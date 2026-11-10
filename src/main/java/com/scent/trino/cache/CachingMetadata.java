package com.scent.trino.cache;

import io.airlift.units.Duration;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;

@Slf4j
public final class CachingMetadata implements InvocationHandler {
    private final ConnectorMetadata delegate;
    private final Map<String, List<String>> partitionColumnsByTable;
    private final boolean captureColumnTypes;
    private final TableMetadataCache metadataCache;
    private final PlanningNotes planningNotes;
    private final Map<String, Duration> refreshByTable;

    public CachingMetadata(
            ConnectorMetadata delegate,
            Map<String, List<String>> partitionColumnsByTable,
            boolean captureColumnTypes,
            TableMetadataCache metadataCache,
            PlanningNotes planningNotes,
            Map<String, Duration> refreshByTable) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.partitionColumnsByTable = Objects.requireNonNull(partitionColumnsByTable, "partitionColumnsByTable is null");
        this.captureColumnTypes = captureColumnTypes;
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache is null");
        this.planningNotes = Objects.requireNonNull(planningNotes, "planningNotes is null");
        this.refreshByTable = Map.copyOf(refreshByTable);
    }

    public static ConnectorMetadata wrap(
            ConnectorMetadata delegate,
            Map<String, List<String>> partitionColumnsByTable,
            boolean captureColumnTypes,
            TableMetadataCache metadataCache,
            PlanningNotes planningNotes,
            Map<String, Duration> refreshByTable) {
        return (ConnectorMetadata) Proxy.newProxyInstance(
                ConnectorMetadata.class.getClassLoader(),
                new Class<?>[] {ConnectorMetadata.class},
                new CachingMetadata(delegate, partitionColumnsByTable, captureColumnTypes, metadataCache, planningNotes, refreshByTable));
    }

    /**
     * Methods whose delegate result replaces the table handle (projection/aggregate/limit pushdown
     * etc.). Forwarding them lets the delegate return a raw handle, which bypasses this wrapper for
     * every later call; declining keeps the rewrite above the scan so slices stay full-row and the
     * handle identity (and cache bookkeeping) stays ours.
     */
    private static final Set<String> HANDLE_REWRITING_METHODS = Set.of(
            "applyProjection",
            "applyAggregation",
            "applyLimit",
            "applySample",
            "applyTopN",
            "applyPartitioning",
            "applyTableScanRedirect",
            "applyJoin",
            "applyTableFunction",
            "applyDelete",
            "applyUpdate");

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
        try {
            return switch (method.getName()) {
                case "getTableHandle" -> getTableHandle(method, arguments);
                case "getColumnHandles" -> getColumnHandles(method, arguments);
                case "getTableSchema" -> getTableSchema(method, arguments);
                case "getTableStatistics" -> getTableStatistics(method, arguments);
                case "applyFilter" -> applyFilter(method, arguments);
                default -> {
                    if (HANDLE_REWRITING_METHODS.contains(method.getName())
                            && containsCachingHandle(arguments)) {
                        // Optional<...>-returning pushdown hooks: decline so Trino plans the
                        // rewrite above the scan instead of folding it into the delegate handle.
                        // applyDelete/applyUpdate are declined too: the wrapper has no invalidation
                        // path, so forwarding a write would serve stale cached rows afterwards.
                        log.debug("[SCENT SPI] Declining ConnectorMetadata.{} to preserve the caching table handle", method.getName());
                        yield Optional.empty();
                    }
                    log.debug("[SCENT SPI] Forwarding ConnectorMetadata.{} to the delegate", method.getName());
                    yield method.invoke(delegate, unwrap(arguments));
                }
            };
        }
        catch (InvocationTargetException exception) {
            throw exception.getCause() != null ? exception.getCause() : exception;
        }
    }

    /**
     * Cache-first (Phase 5, decision 8): a table with a refresh window plans without the delegate
     * while its entry is fresh, in memory or on disk in a note. Stale or unknown entries go to the
     * delegate, and a delegate failure fails the query — the cache is an accelerator, not a
     * replacement. Tables without a refresh entry always plan through the delegate.
     */
    private Object getTableHandle(Method method, Object[] arguments) throws Throwable {
        SchemaTableName tableName = (SchemaTableName) arguments[1];
        Optional<CachingTableHandle> fresh = hasRequestedVersion(arguments)
                ? Optional.empty()
                : freshTableHandle(tableName);
        if (fresh.isPresent()) {
            return fresh.get();
        }
        log.debug("[SCENT SPI] Forwarding ConnectorMetadata.getTableHandle to the delegate");
        Object result = method.invoke(delegate, arguments);
        if (result == null) {
            return null;
        }
        // Every delegate confirmation rewrites the planning note: the note's timestamp means
        // "the delegate last confirmed this metadata", not "the values last changed". A stable
        // table would otherwise freeze its note at first-write time and expire it while the
        // delegate had in fact just answered. Bounded by the refresh cycle, not per query.
        metadataCache.put(tableName, (ConnectorTableHandle) result, null, null, true);
        writeTableNote(tableName);
        return new CachingTableHandle(
                (ConnectorTableHandle) result,
                tableName,
                TupleDomain.all(),
                null,
                null,
                true);
    }

    /** Memory first, then the table note for a coordinator that restarted inside the refresh window. */
    private Optional<CachingTableHandle> freshTableHandle(SchemaTableName tableName) {
        Duration refresh = refreshByTable.get(tableName.toString());
        if (refresh == null) {
            return Optional.empty();
        }
        if (metadataCache.isFresh(tableName, refresh)) {
            log.info("[SCENT SPI] serving table handle for {} from planning cache", tableName);
            return Optional.of(new CachingTableHandle(
                    metadataCache.getHandle(tableName).orElseThrow(),
                    tableName,
                    TupleDomain.all(),
                    metadataCache.getColumns(tableName).orElse(null),
                    metadataCache.getColumnTypes(tableName).orElse(null),
                    metadataCache.refreshedViaDelegate(tableName)));
        }
        if (metadataCache.contains(tableName)) {
            return Optional.empty();
        }
        Optional<PlanningNotes.TableMetadata> noted = planningNotes.readTable(tableName);
        if (noted.isEmpty()) {
            return Optional.empty();
        }
        PlanningNotes.TableMetadata note = noted.get();
        long ageMillis = System.currentTimeMillis() - note.refreshedAtEpochMillis();
        if (ageMillis >= refresh.getValue(java.util.concurrent.TimeUnit.MILLISECONDS)) {
            return Optional.empty();
        }
        metadataCache.put(tableName, note.handle(), note.columns(), note.columnTypes(), false);
        if (note.schema() != null) {
            metadataCache.putSchema(tableName, note.schema(), false);
        }
        log.info("[SCENT SPI] serving table handle for {} from planning note", tableName);
        return Optional.of(new CachingTableHandle(
                note.handle(),
                tableName,
                TupleDomain.all(),
                note.columns(),
                note.columnTypes(),
                false));
    }

    private Object getColumnHandles(Method method, Object[] arguments) throws Throwable {
        CachingTableHandle caching = arguments[1] instanceof CachingTableHandle handle ? handle : null;
        // A handle served fresh already carries the learned layout, so the delegate is not asked again.
        if (caching != null && caching.getColumnHandles() != null) {
            Map<String, ColumnHandle> columns = caching.getColumnHandles();
            // A note-rebuilt handle may carry columns but not their types; re-capture so the
            // slice write has type metadata instead of being rejected as incomplete on every populate.
            if (captureColumnTypes && caching.getColumnTypes() == null) {
                learnColumnTypes((ConnectorSession) arguments[0], caching, columns);
                metadataCache.put(caching.getSchemaTableName(), caching.getDelegateHandle(), columns, caching.getColumnTypes(), true);
                writeTableNote(caching.getSchemaTableName());
            }
            log.debug("[SCENT SPI] serving column handles for {} from planning cache", caching.getSchemaTableName());
            return columns;
        }
        Map<String, ColumnHandle> result;
        try {
            @SuppressWarnings("unchecked")
            Map<String, ColumnHandle> delegateResult =
                    (Map<String, ColumnHandle>) method.invoke(delegate, unwrap(arguments));
            result = delegateResult;
        }
        catch (InvocationTargetException exception) {
            throw unwrapCause(exception);
        }
        if (caching != null && result != null) {
            caching.attachColumns(result);
            Map<String, String> types = null;
            if (captureColumnTypes) {
                learnColumnTypes((ConnectorSession) arguments[0], caching, result);
                types = caching.getColumnTypes();
            }
            metadataCache.put(caching.getSchemaTableName(), caching.getDelegateHandle(), result, types, true);
            writeTableNote(caching.getSchemaTableName());
        }
        return result;
    }

    /** Learns and attaches the delegate's type ids for this table's columns. */
    private void learnColumnTypes(ConnectorSession session, CachingTableHandle caching, Map<String, ColumnHandle> columns) {
        Map<String, String> captured = new HashMap<>();
        columns.forEach((name, column) -> captured.put(
                name,
                delegate.getColumnMetadata(session, caching.getDelegateHandle(), column).getType().getTypeId().getId()));
        caching.attachColumnTypes(captured);
    }

    private Object getTableSchema(Method method, Object[] arguments) throws Throwable {
        CachingTableHandle caching = arguments[1] instanceof CachingTableHandle handle ? handle : null;
        // The handle arrived through the fresh path, so the schema learned in the same refresh
        // cycle is in memory and equally fresh.
        if (caching != null && tableFresh(caching.getSchemaTableName())) {
            Optional<ConnectorTableSchema> cached = metadataCache.getSchema(caching.getSchemaTableName());
            if (cached.isPresent()) {
                log.debug("[SCENT SPI] serving table schema for {} from planning cache", caching.getSchemaTableName());
                return cached.get();
            }
        }
        ConnectorTableSchema result;
        try {
            result = (ConnectorTableSchema) method.invoke(delegate, unwrap(arguments));
        }
        catch (InvocationTargetException exception) {
            throw unwrapCause(exception);
        }
        if (caching != null && result != null) {
            metadataCache.putSchema(caching.getSchemaTableName(), result, true);
            writeTableNote(caching.getSchemaTableName());
        }
        return result;
    }

    /**
     * Known tables never pay a source round-trip for statistics; empty statistics keep
     * planning outage-safe (the source statistics query is one of the calls that fails with
     * the source down).
     */
    private Object getTableStatistics(Method method, Object[] arguments) throws Throwable {
        if (arguments[1] instanceof CachingTableHandle caching
                && metadataCache.contains(caching.getSchemaTableName())) {
            return TableStatistics.empty();
        }
        return method.invoke(delegate, unwrap(arguments));
    }

    private static Throwable unwrapCause(InvocationTargetException exception) {
        return exception.getCause() != null ? exception.getCause() : exception;
    }

    /** Persists the table's current planning entry; nulls inside keep the previously noted fields. */
    private void writeTableNote(SchemaTableName tableName) {
        // The note is only read back in freshTableHandle, which requires a refresh window; for
        // tables without one the delegate is always contacted, so a per-query disk write is wasted.
        if (!refreshByTable.containsKey(tableName.toString())) {
            return;
        }
        Optional<ConnectorTableHandle> handle = metadataCache.getHandle(tableName);
        if (handle.isEmpty()) {
            return;
        }
        planningNotes.writeTableNote(tableName, new PlanningNotes.TableNote(
                handle.get(),
                metadataCache.getColumns(tableName).orElse(null),
                metadataCache.getColumnTypes(tableName).orElse(null),
                metadataCache.getSchema(tableName).map(PlanningNotes::schemaColumns).orElse(null)));
    }

    @SuppressWarnings("unchecked")
    private Object applyFilter(Method method, Object[] arguments) throws Exception {
        if (!(arguments[1] instanceof CachingTableHandle caching)) {
            return method.invoke(delegate, arguments);
        }
        ConnectorSession session = (ConnectorSession) arguments[0];
        Constraint constraint = (Constraint) arguments[2];
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        Map<String, ColumnHandle> partitionHandles = resolvePartitionHandles(session, caching);

        TupleDomain<ColumnHandle> incomingPartition = partitionDomainFor(summary, partitionHandles);
        if (incomingPartition.isAll()) {
            if (partitionColumnsByTable.containsKey(caching.getSchemaTableName().toString())) {
                // Configured table, but none of its grain columns is constrained in this query.
                log.debug("[SCENT SPI] none of the configured partition columns is constrained for {}; forwarding uncached", caching.getSchemaTableName());
            }
            else {
                log.debug("[SCENT SPI] table {} has no configured partition columns; forwarding uncached", caching.getSchemaTableName());
            }
            // No partition predicate in flight: forward the whole constraint so non-partition
            // predicates still reach the delegate, and keep whatever partition domain the
            // scan already carries (all() for a plain scan, so it stays uncached).
            return forwardWithFullConstraint(method, arguments, caching);
        }
        if (incomingPartition.isNone()) {
            // Contradiction on the partition column: let Trino evaluate it; never cache a none() slice.
            return Optional.empty();
        }

        // Push only the partition predicate; non-partition domains remain for Trino (C7).
        TupleDomain<ColumnHandle> merged = caching.getPartitionDomain().intersect(incomingPartition);
        log.debug("[SCENT SPI] applyFilter pushing partition domain {} onto {}", incomingPartition, caching);

        // Cache-first: a fresh table entry plans without the delegate when a trusted filtered
        // handle is remembered for this exact domain. An untrusted one (rebuilt from a note) may
        // serve a cache hit but must never drive a delegate read presented as filtered, so it is
        // upgraded through the delegate whenever this cycle proves the delegate reachable, and
        // served only as an untrusted handle otherwise. Unknown domains must go through, because
        // a slice for them does not exist yet and the source read needs the delegate anyway.
        Optional<TableMetadataCache.RememberedFiltered> remembered = metadataCache.getFilteredHandle(caching.getSchemaTableName(), merged);
        if (remembered.isEmpty() && tableFresh(caching.getSchemaTableName())) {
            remembered = planningNotes.readSliceNote(CacheKey.of(caching.getSchemaTableName(), CacheKey.nameKeyed(merged, partitionHandles)))
                    .map(note -> new TableMetadataCache.RememberedFiltered(note.filteredHandle(), false));
            if (remembered.isPresent()) {
                metadataCache.putFilteredHandle(caching.getSchemaTableName(), merged, remembered.get().handle(), false);
            }
        }
        boolean delegateProvenUp = caching.isDelegateHandleTrusted() && metadataCache.refreshedViaDelegate(caching.getSchemaTableName());
        if (remembered.isPresent() && remembered.get().trusted() && tableFresh(caching.getSchemaTableName())) {
            log.info("[SCENT SPI] serving filtered handle for {} from planning cache", merged);
            return Optional.of(new ConstraintApplicationResult<>(
                    new CachingTableHandle(
                            remembered.get().handle(),
                            caching.getSchemaTableName(),
                            merged,
                            caching.getColumnHandles(),
                            caching.getColumnTypes(),
                            true),
                    withoutPartitionColumns(summary, partitionHandles),
                    Constant.TRUE,
                    false));
        }
        if (remembered.isPresent() && !delegateProvenUp) {
            // Serve the untrusted handle only for its cache-hit chance; the page source refuses
            // to repopulate with it, so a miss degrades to the delegate read's own outcome.
            log.info("[SCENT SPI] serving unverified filtered handle for {} from planning cache", merged);
            return Optional.of(new ConstraintApplicationResult<>(
                    new CachingTableHandle(
                            remembered.get().handle(),
                            caching.getSchemaTableName(),
                            merged,
                            caching.getColumnHandles(),
                            caching.getColumnTypes(),
                            false),
                    withoutPartitionColumns(summary, partitionHandles),
                    Constant.TRUE,
                    false));
        }

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result;
        try {
            result = (Optional<ConstraintApplicationResult<ConnectorTableHandle>>) method.invoke(
                    delegate,
                    session,
                    caching.getDelegateHandle(),
                    new Constraint(incomingPartition));
        }
        catch (InvocationTargetException exception) {
            // The pushdown is declined on a failed delegate call, but a remembered handle still
            // gets its cache-hit chance: the page source refuses to repopulate with it, so a
            // missing slice surfaces the real source error instead of wrong rows. A handle is
            // never paired with a partition domain it was not built for (no populate poisoning).
            if (remembered.isPresent()) {
                log.info("[SCENT SPI] serving unverified filtered handle for {} after a failed delegate call", merged);
                return Optional.of(new ConstraintApplicationResult<>(
                        new CachingTableHandle(
                                remembered.get().handle(),
                                caching.getSchemaTableName(),
                                merged,
                                caching.getColumnHandles(),
                                caching.getColumnTypes(),
                                false),
                        withoutPartitionColumns(summary, partitionHandles),
                        Constant.TRUE,
                        false));
            }
            log.debug("[SCENT SPI] applyFilter declined after a failed delegate call", unwrapCause(exception));
            return Optional.empty();
        }
        if (result.isEmpty()) {
            // Delegate cannot push; Trino keeps the predicate above the scan.
            return result;
        }
        if (!result.get().getRemainingFilter().isAll()) {
            // ponytail: bail instead of merging leftovers — cached slices assume the delegate
            // applied the partition predicate fully; upgrade to remaining-union if that ever bites.
            return Optional.empty();
        }
        if (metadataCache.putFilteredHandle(caching.getSchemaTableName(), merged, result.get().getHandle(), true)) {
            planningNotes.writeSliceNote(CacheKey.of(caching.getSchemaTableName(), CacheKey.nameKeyed(merged, partitionHandles)), result.get().getHandle());
        }
        CachingTableHandle wrapped = new CachingTableHandle(
                result.get().getHandle(),
                caching.getSchemaTableName(),
                merged,
                caching.getColumnHandles(),
                caching.getColumnTypes());
        return Optional.of(new ConstraintApplicationResult<>(
                wrapped,
                withoutPartitionColumns(summary, partitionHandles),
                result.get().getRemainingExpression().orElseGet(() -> Constant.TRUE),
                false));
    }

    /** The table's planning entry is younger than its refresh window (and it has one). */
    private boolean tableFresh(SchemaTableName tableName) {
        Duration refresh = refreshByTable.get(tableName.toString());
        return refresh != null && metadataCache.isFresh(tableName, refresh);
    }

    private Object forwardWithFullConstraint(Method method, Object[] arguments, CachingTableHandle caching) throws Exception {
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                (Optional<ConstraintApplicationResult<ConnectorTableHandle>>) method.invoke(
                        delegate,
                        arguments[0],
                        caching.getDelegateHandle(),
                        arguments[2]);
        if (result.isEmpty()) {
            return result;
        }
        CachingTableHandle wrapped = new CachingTableHandle(
                result.get().getHandle(),
                caching.getSchemaTableName(),
                caching.getPartitionDomain(),
                caching.getColumnHandles(),
                caching.getColumnTypes());
        return Optional.of(new ConstraintApplicationResult<>(
                wrapped,
                result.get().getRemainingFilter(),
                result.get().getRemainingExpression().orElseGet(() -> Constant.TRUE),
                result.get().isPrecalculateStatistics()));
    }

    /**
     * The configured grain columns of this table, resolved against the delegate's column handles.
     * An unconfigured table, or a configured column the table does not have, contributes nothing.
     */
    private Map<String, ColumnHandle> resolvePartitionHandles(ConnectorSession session, CachingTableHandle caching) {
        List<String> configured = partitionColumnsByTable.getOrDefault(
                caching.getSchemaTableName().toString(), List.of());
        if (configured.isEmpty()) {
            return Map.of();
        }
        // Learned columns from the last successful getColumnHandles; the delegate call is the
        // fallback for a table whose columns were never captured (also removes a duplicate
        // source round-trip on the happy path).
        Map<String, ColumnHandle> columns = metadataCache.getColumns(caching.getSchemaTableName())
                .orElseGet(() -> delegate.getColumnHandles(session, caching.getDelegateHandle()));
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (String column : configured) {
            ColumnHandle handle = columns.get(column);
            if (handle != null) {
                handles.put(column, handle);
            }
        }
        return handles;
    }

    /**
     * The conjunction of the grain-column domains present in the summary, or all() when none of
     * them is constrained (unknown column, missing from this table, or simply unconstrained).
     * Partial grains are cached as coarser slices (ADR-013).
     */
    private static TupleDomain<ColumnHandle> partitionDomainFor(TupleDomain<ColumnHandle> summary, Map<String, ColumnHandle> partitionHandles) {
        if (partitionHandles.isEmpty()) {
            return TupleDomain.all();
        }
        Map<ColumnHandle, Domain> domains = summary.getDomains().orElse(null);
        if (domains == null) {
            return TupleDomain.all();
        }
        Map<ColumnHandle, Domain> present = new LinkedHashMap<>();
        for (ColumnHandle handle : partitionHandles.values()) {
            Domain domain = domains.get(handle);
            if (domain != null) {
                present.put(handle, domain);
            }
        }
        if (present.isEmpty()) {
            return TupleDomain.all();
        }
        return withColumnDomains(present);
    }

    private static TupleDomain<ColumnHandle> withoutPartitionColumns(TupleDomain<ColumnHandle> summary, Map<String, ColumnHandle> partitionHandles) {
        Map<ColumnHandle, Domain> domains = summary.getDomains().orElseThrow();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(domains);
        remaining.keySet().removeAll(partitionHandles.values());
        if (remaining.isEmpty()) {
            return TupleDomain.all();
        }
        return withColumnDomains(remaining);
    }

    private static Object[] unwrap(Object[] arguments) {
        if (arguments == null) {
            return null;
        }
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] instanceof CachingTableHandle handle) {
                arguments[i] = handle.getDelegateHandle();
            }
        }
        return arguments;
    }

    /** True when any argument carries our table handle, regardless of its position. */
    private static boolean containsCachingHandle(Object[] arguments) {
        if (arguments == null) {
            return false;
        }
        for (Object argument : arguments) {
            if (argument instanceof CachingTableHandle) {
                return true;
            }
        }
        return false;
    }

    /** True when the request pins a table version (time travel); a versioned scan must not be served from the cache. */
    private static boolean hasRequestedVersion(Object[] arguments) {
        return (arguments.length > 2 && arguments[2] instanceof Optional<?> requestedVersion && requestedVersion.isPresent())
                || (arguments.length > 3 && arguments[3] instanceof Optional<?> retryVersion && retryVersion.isPresent());
    }
}
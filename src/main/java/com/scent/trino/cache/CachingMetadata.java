package com.scent.trino.cache;

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

    public CachingMetadata(
            ConnectorMetadata delegate,
            Map<String, List<String>> partitionColumnsByTable,
            boolean captureColumnTypes,
            TableMetadataCache metadataCache) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.partitionColumnsByTable = Objects.requireNonNull(partitionColumnsByTable, "partitionColumnsByTable is null");
        this.captureColumnTypes = captureColumnTypes;
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache is null");
    }

    public static ConnectorMetadata wrap(
            ConnectorMetadata delegate,
            Map<String, List<String>> partitionColumnsByTable,
            boolean captureColumnTypes,
            TableMetadataCache metadataCache) {
        return (ConnectorMetadata) Proxy.newProxyInstance(
                ConnectorMetadata.class.getClassLoader(),
                new Class<?>[] {ConnectorMetadata.class},
                new CachingMetadata(delegate, partitionColumnsByTable, captureColumnTypes, metadataCache));
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
            "applyTableScanRedirect");

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
                            && arguments[1] instanceof CachingTableHandle) {
                        // Optional<...>-returning pushdown hooks: decline so Trino plans the
                        // rewrite above the scan instead of folding it into the delegate handle.
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

    private Object getTableHandle(Method method, Object[] arguments) throws Throwable {
        SchemaTableName tableName = (SchemaTableName) arguments[1];
        Object result;
        try {
            result = method.invoke(delegate, arguments);
        }
        catch (InvocationTargetException exception) {
            return cachedTableHandle(tableName, unwrapCause(exception));
        }
        if (result == null) {
            return null;
        }
        metadataCache.put(tableName, (ConnectorTableHandle) result, null, null);
        return new CachingTableHandle(
                (ConnectorTableHandle) result,
                tableName,
                TupleDomain.all(),
                null,
                null);
    }

    /**
     * S7: a table seen at least once plans without the source. The cached handle is served
     * fresh-wrapped on source failure only; on a cache miss the source error propagates, so a
     * table never seen before still fails like it does today.
     */
    private Object cachedTableHandle(SchemaTableName tableName, Throwable sourceFailure) throws Throwable {
        Optional<ConnectorTableHandle> cached = metadataCache.getHandle(tableName);
        if (cached.isEmpty()) {
            throw sourceFailure;
        }
        log.info("[SCENT SPI] source outage: serving table handle for {} from metadata cache", tableName);
        return new CachingTableHandle(
                cached.get(),
                tableName,
                TupleDomain.all(),
                metadataCache.getColumns(tableName).orElse(null),
                metadataCache.getColumnTypes(tableName).orElse(null));
    }

    private Object getColumnHandles(Method method, Object[] arguments) throws Throwable {
        CachingTableHandle caching = arguments[1] instanceof CachingTableHandle handle ? handle : null;
        Map<String, ColumnHandle> result;
        try {
            @SuppressWarnings("unchecked")
            Map<String, ColumnHandle> delegateResult =
                    (Map<String, ColumnHandle>) method.invoke(delegate, unwrap(arguments));
            result = delegateResult;
        }
        catch (InvocationTargetException exception) {
            return cachedColumnHandles(caching, unwrapCause(exception));
        }
        if (caching != null && result != null) {
            caching.attachColumns(result);
            Map<String, String> types;
            if (captureColumnTypes) {
                ConnectorSession session = (ConnectorSession) arguments[0];
                Map<String, String> captured = new HashMap<>();
                result.forEach((name, column) -> captured.put(
                        name,
                        delegate.getColumnMetadata(session, caching.getDelegateHandle(), column).getType().getTypeId().getId()));
                caching.attachColumnTypes(captured);
                types = captured;
            }
            else {
                types = null;
            }
            metadataCache.put(caching.getSchemaTableName(), caching.getDelegateHandle(), result, types);
        }
        return result;
    }

    private Object cachedColumnHandles(CachingTableHandle caching, Throwable sourceFailure) throws Throwable {
        if (caching == null) {
            throw sourceFailure;
        }
        Optional<Map<String, ColumnHandle>> cached = metadataCache.getColumns(caching.getSchemaTableName());
        if (cached.isEmpty()) {
            throw sourceFailure;
        }
        log.info("[SCENT SPI] source outage: serving column handles for {} from metadata cache", caching.getSchemaTableName());
        caching.attachColumns(cached.get());
        return cached.get();
    }

    private Object getTableSchema(Method method, Object[] arguments) throws Throwable {
        CachingTableHandle caching = arguments[1] instanceof CachingTableHandle handle ? handle : null;
        ConnectorTableSchema result;
        try {
            result = (ConnectorTableSchema) method.invoke(delegate, unwrap(arguments));
        }
        catch (InvocationTargetException exception) {
            return cachedTableSchema(caching, unwrapCause(exception));
        }
        if (caching != null && result != null) {
            metadataCache.putSchema(caching.getSchemaTableName(), result);
        }
        return result;
    }

    private Object cachedTableSchema(CachingTableHandle caching, Throwable sourceFailure) throws Throwable {
        if (caching == null) {
            throw sourceFailure;
        }
        Optional<ConnectorTableSchema> cached = metadataCache.getSchema(caching.getSchemaTableName());
        if (cached.isEmpty()) {
            throw sourceFailure;
        }
        log.info("[SCENT SPI] source outage: serving table schema for {} from metadata cache", caching.getSchemaTableName());
        return cached.get();
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

    @SuppressWarnings("unchecked")
    private Object applyFilter(Method method, Object[] arguments) throws Exception {
        if (!(arguments[1] instanceof CachingTableHandle caching)) {
            return method.invoke(delegate, arguments);
        }
        ConnectorSession session = (ConnectorSession) arguments[0];
        Constraint constraint = (Constraint) arguments[2];
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        List<ColumnHandle> partitionHandles = resolvePartitionHandles(session, caching);

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
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result;
        try {
            result = (Optional<ConstraintApplicationResult<ConnectorTableHandle>>) method.invoke(
                    delegate,
                    session,
                    caching.getDelegateHandle(),
                    new Constraint(incomingPartition));
        }
        catch (InvocationTargetException exception) {
            return outageFilterResult(caching, merged, summary, partitionHandles);
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
        metadataCache.putFilteredHandle(caching.getSchemaTableName(), merged, result.get().getHandle());
        CachingTableHandle wrapped = new CachingTableHandle(
                result.get().getHandle(),
                caching.getSchemaTableName(),
                merged,
                caching.getColumnHandles(),
                caching.getColumnTypes());
        return Optional.of(new ConstraintApplicationResult<>(
                wrapped,
                withoutPartitionColumns(summary, partitionHandles),
                // remaining domains are authoritative; TRUE is the sanctioned no-expression value
                Constant.TRUE,
                false));
    }

    /**
     * The delegate's applyFilter opens a JDBC connection (toColumnMappings), so a source outage
     * fails the pushdown. The delegate's rewritten handle for this exact partition domain was
     * remembered on the last successful query, so it is replayed; with no remembered handle the
     * pushdown is declined and the predicate stays above the scan. That keeps the populate path
     * filtered: a delegate handle is never paired with a partition domain it was not built for.
     */
    private Object outageFilterResult(
            CachingTableHandle caching,
            TupleDomain<ColumnHandle> merged,
            TupleDomain<ColumnHandle> summary,
            List<ColumnHandle> partitionHandles) {
        Optional<ConnectorTableHandle> remembered = metadataCache.getFilteredHandle(caching.getSchemaTableName(), merged);
        if (remembered.isEmpty()) {
            return Optional.empty();
        }
        log.info("[SCENT SPI] source outage: serving filtered handle for {} {} from metadata cache", caching.getSchemaTableName(), merged);
        CachingTableHandle wrapped = new CachingTableHandle(
                remembered.get(),
                caching.getSchemaTableName(),
                merged,
                caching.getColumnHandles(),
                caching.getColumnTypes());
        return Optional.of(new ConstraintApplicationResult<>(
                wrapped,
                withoutPartitionColumns(summary, partitionHandles),
                Constant.TRUE,
                false));
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
    private List<ColumnHandle> resolvePartitionHandles(ConnectorSession session, CachingTableHandle caching) {
        List<String> configured = partitionColumnsByTable.getOrDefault(
                caching.getSchemaTableName().toString(), List.of());
        if (configured.isEmpty()) {
            return List.of();
        }
        // Learned columns from the last successful getColumnHandles; the delegate call is the
        // fallback for a table whose columns were never captured (also removes a duplicate
        // source round-trip on the happy path).
        Map<String, ColumnHandle> columns = metadataCache.getColumns(caching.getSchemaTableName())
                .orElseGet(() -> delegate.getColumnHandles(session, caching.getDelegateHandle()));
        List<ColumnHandle> handles = new ArrayList<>();
        for (String column : configured) {
            ColumnHandle handle = columns.get(column);
            if (handle != null) {
                handles.add(handle);
            }
        }
        return handles;
    }

    /**
     * The conjunction of the grain-column domains present in the summary, or all() when none of
     * them is constrained (unknown column, missing from this table, or simply unconstrained).
     * Partial grains are cached as coarser slices (ADR-013).
     */
    private static TupleDomain<ColumnHandle> partitionDomainFor(TupleDomain<ColumnHandle> summary, List<ColumnHandle> partitionHandles) {
        if (partitionHandles.isEmpty()) {
            return TupleDomain.all();
        }
        Map<ColumnHandle, Domain> domains = summary.getDomains().orElse(null);
        if (domains == null) {
            return TupleDomain.all();
        }
        Map<ColumnHandle, Domain> present = new LinkedHashMap<>();
        for (ColumnHandle handle : partitionHandles) {
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

    private static TupleDomain<ColumnHandle> withoutPartitionColumns(TupleDomain<ColumnHandle> summary, List<ColumnHandle> partitionHandles) {
        Map<ColumnHandle, Domain> domains = summary.getDomains().orElseThrow();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(domains);
        remaining.keySet().removeAll(partitionHandles);
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
}
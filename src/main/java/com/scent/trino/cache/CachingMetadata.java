package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
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

    public CachingMetadata(ConnectorMetadata delegate, Map<String, List<String>> partitionColumnsByTable, boolean captureColumnTypes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.partitionColumnsByTable = Objects.requireNonNull(partitionColumnsByTable, "partitionColumnsByTable is null");
        this.captureColumnTypes = captureColumnTypes;
    }

    public static ConnectorMetadata wrap(ConnectorMetadata delegate, Map<String, List<String>> partitionColumnsByTable, boolean captureColumnTypes) {
        return (ConnectorMetadata) Proxy.newProxyInstance(
                ConnectorMetadata.class.getClassLoader(),
                new Class<?>[] {ConnectorMetadata.class},
                new CachingMetadata(delegate, partitionColumnsByTable, captureColumnTypes));
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

    private Object getTableHandle(Method method, Object[] arguments) throws Exception {
        Object result = method.invoke(delegate, arguments);
        if (result == null) {
            return null;
        }
        return new CachingTableHandle(
                (ConnectorTableHandle) result,
                (SchemaTableName) arguments[1],
                TupleDomain.all(),
                null,
                null);
    }

    private Object getColumnHandles(Method method, Object[] arguments) throws Exception {
        CachingTableHandle caching = arguments[1] instanceof CachingTableHandle handle ? handle : null;
        Object result = method.invoke(delegate, unwrap(arguments));
        if (caching != null && result != null) {
            @SuppressWarnings("unchecked")
            Map<String, ColumnHandle> columns = (Map<String, ColumnHandle>) result;
            caching.attachColumns(columns);
            if (captureColumnTypes) {
                ConnectorSession session = (ConnectorSession) arguments[0];
                Map<String, String> types = new HashMap<>();
                columns.forEach((name, column) -> types.put(
                        name,
                        delegate.getColumnMetadata(session, caching.getDelegateHandle(), column).getType().getTypeId().getId()));
                caching.attachColumnTypes(types);
            }
        }
        return result;
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
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                (Optional<ConstraintApplicationResult<ConnectorTableHandle>>) method.invoke(
                        delegate,
                        session,
                        caching.getDelegateHandle(),
                        new Constraint(incomingPartition));
        if (result.isEmpty()) {
            // Delegate cannot push; Trino keeps the predicate above the scan.
            return result;
        }
        if (!result.get().getRemainingFilter().isAll()) {
            // ponytail: bail instead of merging leftovers — cached slices assume the delegate
            // applied the partition predicate fully; upgrade to remaining-union if that ever bites.
            return Optional.empty();
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
                // remaining domains are authoritative; TRUE is the sanctioned no-expression value
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
        Map<String, ColumnHandle> columns = delegate.getColumnHandles(session, caching.getDelegateHandle());
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
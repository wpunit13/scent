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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;

@Slf4j
public final class CachingMetadata
        implements InvocationHandler
{
    private final ConnectorMetadata delegate;
    private final String partitionColumn;

    public CachingMetadata(ConnectorMetadata delegate, String partitionColumn)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.partitionColumn = Objects.requireNonNull(partitionColumn, "partitionColumn is null");
    }

    public static ConnectorMetadata wrap(ConnectorMetadata delegate, String partitionColumn)
    {
        return (ConnectorMetadata) Proxy.newProxyInstance(
                ConnectorMetadata.class.getClassLoader(),
                new Class<?>[] {ConnectorMetadata.class},
                new CachingMetadata(delegate, partitionColumn));
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
    public Object invoke(Object proxy, Method method, Object[] arguments)
            throws Throwable
    {
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
                        yield Optional.empty();
                    }
                    yield method.invoke(delegate, unwrap(arguments));
                }
            };
        }
        catch (InvocationTargetException exception) {
            throw exception.getCause() != null ? exception.getCause() : exception;
        }
        finally {
            if (method.getDeclaringClass() != Object.class) {
                log.debug("[SCENT SPI] Forwarded ConnectorMetadata.{}", method.getName());
            }
        }
    }

    private Object getTableHandle(Method method, Object[] arguments)
            throws Exception
    {
        Object result = method.invoke(delegate, arguments);
        if (result == null) {
            return null;
        }
        return new CachingTableHandle(
                (ConnectorTableHandle) result,
                (SchemaTableName) arguments[1],
                TupleDomain.all(),
                null);
    }

    private Object getColumnHandles(Method method, Object[] arguments)
            throws Exception
    {
        CachingTableHandle caching = arguments[1] instanceof CachingTableHandle handle ? handle : null;
        Object result = method.invoke(delegate, unwrap(arguments));
        if (caching != null && result != null) {
            @SuppressWarnings("unchecked")
            Map<String, ColumnHandle> columns = (Map<String, ColumnHandle>) result;
            caching.attachColumns(columns);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Object applyFilter(Method method, Object[] arguments)
            throws Exception
    {
        if (!(arguments[1] instanceof CachingTableHandle caching)) {
            return method.invoke(delegate, arguments);
        }
        ConnectorSession session = (ConnectorSession) arguments[0];
        Constraint constraint = (Constraint) arguments[2];
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        ColumnHandle partitionHandle = resolvePartitionHandle(session, caching);

        TupleDomain<ColumnHandle> incomingPartition = partitionDomainFor(summary, partitionHandle);
        if (incomingPartition.isAll()) {
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
                caching.getColumnHandles());
        return Optional.of(new ConstraintApplicationResult<>(
                wrapped,
                withoutPartitionColumn(summary, partitionHandle),
                // remaining domains are authoritative; TRUE is the sanctioned no-expression value
                Constant.TRUE,
                false));
    }

    private Object forwardWithFullConstraint(Method method, Object[] arguments, CachingTableHandle caching)
            throws Exception
    {
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
                caching.getColumnHandles());
        return Optional.of(new ConstraintApplicationResult<>(
                wrapped,
                result.get().getRemainingFilter(),
                result.get().getRemainingExpression().orElseGet(() -> Constant.TRUE),
                result.get().isPrecalculateStatistics()));
    }

    private ColumnHandle resolvePartitionHandle(ConnectorSession session, CachingTableHandle caching)
            throws Exception
    {
        Map<String, ColumnHandle> columns = delegate.getColumnHandles(session, caching.getDelegateHandle());
        return columns.get(partitionColumn);
    }

    /**
     * The partition column's slice of the summary, or all() when the partition column carries
     * no predicate (unknown column, missing from this table, or simply unconstrained).
     */
    private static TupleDomain<ColumnHandle> partitionDomainFor(TupleDomain<ColumnHandle> summary, ColumnHandle partitionHandle)
    {
        if (partitionHandle == null) {
            return TupleDomain.all();
        }
        return summary.getDomains()
                .flatMap(domains -> Optional.ofNullable(domains.get(partitionHandle)))
                .map(domain -> withColumnDomains(Map.of(partitionHandle, domain)))
                .orElseGet(TupleDomain::all);
    }

    private static TupleDomain<ColumnHandle> withoutPartitionColumn(TupleDomain<ColumnHandle> summary, ColumnHandle partitionHandle)
    {
        Map<ColumnHandle, Domain> domains = summary.getDomains().orElseThrow();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(domains);
        remaining.remove(partitionHandle);
        if (remaining.isEmpty()) {
            return TupleDomain.all();
        }
        return withColumnDomains(remaining);
    }

    private static Object[] unwrap(Object[] arguments)
    {
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
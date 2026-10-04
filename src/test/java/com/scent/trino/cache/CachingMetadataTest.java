package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingMetadataTest {

    private static final String PARTITION_COLUMN = "part";
    private static final Domain PART = Domain.singleValue(BigintType.BIGINT, 1L);
    private static final Domain OTHER = Domain.singleValue(BigintType.BIGINT, 2L);

    private static final class StubMetadata implements ConnectorMetadata {
        final ConnectorTableHandle handle = new ConnectorTableHandle() {};
        Map<String, ColumnHandle> columns;
        ConnectorTableHandle filteredHandle = new ConnectorTableHandle() {};
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> filterResult = Optional.empty();
        boolean pushEverything;

        ConnectorTableHandle columnHandlesHandle;
        ConnectorTableHandle applyFilterHandle;
        Constraint applyFilterConstraint;

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion,
                Optional<ConnectorTableVersion> endVersion) {
            return handle;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
            columnHandlesHandle = tableHandle;
            return columns;
        }

        @Override
        public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return tableHandle == handle ? new SchemaTableName("schema", "table") : null;
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                Constraint constraint) {
            applyFilterHandle = tableHandle;
            applyFilterConstraint = constraint;
            if (pushEverything) {
                return Optional.of(new ConstraintApplicationResult<>(
                        filteredHandle, constraint.getSummary(), Constant.TRUE, false));
            }
            return filterResult;
        }
    }

    private final StubMetadata stub = new StubMetadata();
    private final ColumnHandle partitionHandle = new ColumnHandle() {};
    private final ColumnHandle otherHandle = new ColumnHandle() {};

    private ConnectorMetadata metadata() {
        stub.columns = Map.of(PARTITION_COLUMN, partitionHandle, "other", otherHandle);
        return CachingMetadata.wrap(stub, PARTITION_COLUMN);
    }

    private CachingTableHandle tableHandle(ConnectorMetadata metadata) {
        ConnectorTableHandle result = metadata.getTableHandle(
                null, new SchemaTableName("schema", "table"), Optional.empty(), Optional.empty());
        assertInstanceOf(CachingTableHandle.class, result);
        return (CachingTableHandle) result;
    }

    @Test
    void wrapsTableHandleAndUnwrapsOtherMethods() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);

        assertSame(stub.handle, wrapped.getDelegateHandle());
        assertEquals(new SchemaTableName("schema", "table"), wrapped.getSchemaTableName());
        assertTrue(wrapped.getPartitionDomain().isAll());
        assertNull(wrapped.getColumnHandles());

        // Any other method sees the delegate handle, not the wrapper
        SchemaTableName name = metadata.getTableName(null, wrapped);
        assertEquals(new SchemaTableName("schema", "table"), name);
    }

    @Test
    void getColumnHandlesAttachesColumns() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);

        Map<String, ColumnHandle> result = metadata.getColumnHandles(null, wrapped);

        assertSame(stub.handle, stub.columnHandlesHandle);
        assertEquals(Map.of(PARTITION_COLUMN, partitionHandle, "other", otherHandle), result);
        assertEquals(result, wrapped.getColumnHandles());
    }

    @Test
    void applyFilterSplitsPartitionDomain() {
        ConnectorMetadata metadata = metadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, TupleDomain.all(), Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);

        Constraint constraint = new Constraint(withColumnDomains(Map.of(
                partitionHandle, PART,
                otherHandle, OTHER)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        assertTrue(result.isPresent());
        // Delegate sees its own handle and only the partition domain
        assertSame(stub.handle, stub.applyFilterHandle);
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), stub.applyFilterConstraint.getSummary());
        // Result keeps the wrapper, with the partition domain baked in
        ConnectorTableHandle newHandle = result.get().getHandle();
        assertInstanceOf(CachingTableHandle.class, newHandle);
        CachingTableHandle cached = (CachingTableHandle) newHandle;
        assertSame(stub.filteredHandle, cached.getDelegateHandle());
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), cached.getPartitionDomain());
        assertEquals(wrapped.getColumnHandles(), cached.getColumnHandles());
        // Remaining filter carries only the non-partition domains
        assertEquals(withColumnDomains(Map.of(otherHandle, OTHER)), result.get().getRemainingFilter());
        assertFalse(result.get().isPrecalculateStatistics());
    }

    @Test
    void applyFilterWithoutPartitionPredicateForwardsFullConstraint() {
        ConnectorMetadata metadata = metadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        TupleDomain<ColumnHandle> delegateRemaining = withColumnDomains(Map.of(otherHandle, OTHER));
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, delegateRemaining, Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);

        Constraint constraint = new Constraint(withColumnDomains(Map.of(otherHandle, OTHER)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        // Full constraint reaches the delegate untouched (pushdown preserved)
        assertSame(constraint, stub.applyFilterConstraint);
        assertTrue(result.isPresent());
        CachingTableHandle cached = assertInstanceOf(CachingTableHandle.class, result.get().getHandle());
        assertSame(stub.filteredHandle, cached.getDelegateHandle());
        // Still uncachable: no partition domain
        assertTrue(cached.getPartitionDomain().isAll());
        assertEquals(delegateRemaining, result.get().getRemainingFilter());
    }

    @Test
    void applyFilterDelegateCannotPushReturnsEmpty() {
        ConnectorMetadata metadata = metadata();
        stub.filterResult = Optional.empty();
        CachingTableHandle wrapped = tableHandle(metadata);

        Constraint constraint = new Constraint(withColumnDomains(Map.of(partitionHandle, PART)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        assertTrue(result.isEmpty());
    }

    @Test
    void applyFilterDelegatePartiallyAppliedBails() {
        ConnectorMetadata metadata = metadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle,
                withColumnDomains(Map.of(partitionHandle, Domain.singleValue(BigintType.BIGINT, 99L))),
                Constant.TRUE,
                false));
        CachingTableHandle wrapped = tableHandle(metadata);

        Constraint constraint = new Constraint(withColumnDomains(Map.of(partitionHandle, PART)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        // Delegate did not fully apply the partition predicate: don't cache
        assertTrue(result.isEmpty());
    }

    @Test
    void applyFilterPartitionContradictionReturnsEmpty() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);

        Constraint constraint = new Constraint(withColumnDomains(
                Map.of(partitionHandle, Domain.none(BigintType.BIGINT))));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        assertTrue(result.isEmpty());
    }

    @Test
    void applyFilterMergesPartitionDomainsAcrossCalls() {
        ConnectorMetadata metadata = metadata();
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                new ConnectorTableHandle() {}, TupleDomain.all(), Constant.TRUE, false));
        CachingTableHandle current = tableHandle(metadata);
        metadata.getColumnHandles(null, current);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> first =
                metadata.applyFilter(null, current, new Constraint(withColumnDomains(Map.of(partitionHandle, PART))));
        current = (CachingTableHandle) first.get().getHandle();

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> second =
                metadata.applyFilter(null, current, new Constraint(withColumnDomains(Map.of(partitionHandle, PART))));

        CachingTableHandle merged = (CachingTableHandle) second.get().getHandle();
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), merged.getPartitionDomain());
    }

    @Test
    void applyFilterOnForeignHandleIsPurePassthrough() {
        ConnectorMetadata metadata = metadata();
        stub.pushEverything = true;
        ConnectorTableHandle foreign = new ConnectorTableHandle() {};

        Constraint constraint = new Constraint(withColumnDomains(Map.of(partitionHandle, PART)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, foreign, constraint);

        assertSame(foreign, stub.applyFilterHandle);
        assertTrue(result.isPresent());
        assertSame(stub.filteredHandle, result.get().getHandle());
    }
}
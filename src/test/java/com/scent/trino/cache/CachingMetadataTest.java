package com.scent.trino.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingMetadataTest {

    private static final String PARTITION_COLUMN = "part";
    private static final String PARTITION_COLUMN_2 = "part2";
    private static final String TABLE_ID = "schema.table";
    private static final Domain PART = Domain.singleValue(BigintType.BIGINT, 1L);
    private static final Domain PART2 = Domain.singleValue(BigintType.BIGINT, 3L);
    private static final Domain OTHER = Domain.singleValue(BigintType.BIGINT, 2L);

    /** Value-shaped like a real delegate handle so it survives the note round trip. */
    public static final class FakeDelegateHandle implements ConnectorTableHandle {
        private final String id;

        @JsonCreator
        public FakeDelegateHandle(@JsonProperty("id") String id) {
            this.id = id;
        }

        @JsonProperty
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof FakeDelegateHandle other && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private static final class StubMetadata implements ConnectorMetadata {
        final ConnectorTableHandle handle = new FakeDelegateHandle("schema.table");
        Map<String, ColumnHandle> columns;
        ConnectorTableHandle filteredHandle = new ConnectorTableHandle() {};
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> filterResult = Optional.empty();
        boolean pushEverything;

        ConnectorTableHandle columnHandlesHandle;
        ConnectorTableHandle applyFilterHandle;
        Constraint applyFilterConstraint;
        RuntimeException tableHandleFailure;
        RuntimeException columnHandlesFailure;
        RuntimeException schemaFailure;
        RuntimeException applyFilterFailure;
        int columnHandlesCalls;
        int statisticsCalls;

        void clearFailures() {
            tableHandleFailure = null;
            columnHandlesFailure = null;
            schemaFailure = null;
            applyFilterFailure = null;
            columnHandlesCalls = 0;
            statisticsCalls = 0;
        }

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle tableHandle) {
            if (schemaFailure != null) {
                throw schemaFailure;
            }
            return new ConnectorTableSchema(
                    new SchemaTableName("schema", "table"),
                    List.of(ColumnSchema.builder().setName(PARTITION_COLUMN).setType(BigintType.BIGINT).build()));
        }

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session,
                SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion,
                Optional<ConnectorTableVersion> endVersion) {
            if (tableHandleFailure != null) {
                throw tableHandleFailure;
            }
            return handle;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
            if (columnHandlesFailure != null) {
                throw columnHandlesFailure;
            }
            columnHandlesCalls++;
            columnHandlesHandle = tableHandle;
            return columns;
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
            if (columnHandle == columns.get(PARTITION_COLUMN)) {
                return ColumnMetadata.builder().setName(PARTITION_COLUMN).setType(BigintType.BIGINT).build();
            }
            return ColumnMetadata.builder().setName("other").setType(VarcharType.VARCHAR).build();
        }

        @Override
        public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return tableHandle == handle ? new SchemaTableName("schema", "table") : null;
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle) {
            statisticsCalls++;
            return TableStatistics.empty();
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
                ConnectorSession session,
                ConnectorTableHandle tableHandle,
                Constraint constraint) {
            if (applyFilterFailure != null) {
                throw applyFilterFailure;
            }
            applyFilterHandle = tableHandle;
            applyFilterConstraint = constraint;
            if (pushEverything) {
                return Optional.of(new ConstraintApplicationResult<>(
                        filteredHandle, constraint.getSummary(), Constant.TRUE, false));
            }
            return filterResult;
        }
    }

    /** Value-shaped like a real delegate column so it survives the note round trip. */
    public static final class FakeColumnHandle implements ColumnHandle {
        private final String id;

        @JsonCreator
        public FakeColumnHandle(@JsonProperty("id") String id) {
            this.id = id;
        }

        @JsonProperty
        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof FakeColumnHandle other && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private final StubMetadata stub = new StubMetadata();
    private final ColumnHandle partitionHandle = new FakeColumnHandle("part");
    private final ColumnHandle partitionHandle2 = new FakeColumnHandle("part2");
    private final ColumnHandle otherHandle = new FakeColumnHandle("other");
    private final TableMetadataCache metadataCache = new TableMetadataCache();

    private ConnectorMetadata metadata() {
        stub.columns = Map.of(PARTITION_COLUMN, partitionHandle, "other", otherHandle);
        return CachingMetadata.wrap(stub, Map.of(TABLE_ID, List.of(PARTITION_COLUMN)), false, metadataCache, new PlanningNotes(null, null));
    }

    private ConnectorMetadata metadataWithColumnTypes() {
        stub.columns = Map.of(PARTITION_COLUMN, partitionHandle, "other", otherHandle);
        return CachingMetadata.wrap(stub, Map.of(TABLE_ID, List.of(PARTITION_COLUMN)), true, metadataCache, new PlanningNotes(null, null));
    }

    private ConnectorMetadata multiColumnMetadata() {
        stub.columns = Map.of(
                PARTITION_COLUMN, partitionHandle,
                PARTITION_COLUMN_2, partitionHandle2,
                "other", otherHandle);
        return CachingMetadata.wrap(stub, Map.of(TABLE_ID, List.of(PARTITION_COLUMN, PARTITION_COLUMN_2)), false, metadataCache, new PlanningNotes(null, null));
    }

    private ConnectorMetadata unconfiguredMetadata() {
        stub.columns = Map.of(PARTITION_COLUMN, partitionHandle, "other", otherHandle);
        return CachingMetadata.wrap(stub, Map.of("other.schema.otherTable", List.of(PARTITION_COLUMN)), false, metadataCache, new PlanningNotes(null, null));
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
    void getColumnHandlesCapturesColumnTypesWhenEnabled() {
        ConnectorMetadata metadata = metadataWithColumnTypes();
        CachingTableHandle wrapped = tableHandle(metadata);

        metadata.getColumnHandles(null, wrapped);

        assertSame(stub.handle, stub.columnHandlesHandle);
        assertEquals(Map.of(PARTITION_COLUMN, "bigint", "other", "varchar"), wrapped.getColumnTypes());
    }

    @Test
    void applyFilterSplitsPartitionDomain() {
        ConnectorMetadata metadata = metadataWithColumnTypes();
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
        assertEquals(wrapped.getColumnTypes(), cached.getColumnTypes());
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

    @Test
    void applyFilterPushesConjunctionOfAllConfiguredColumnsPresent() {
        ConnectorMetadata metadata = multiColumnMetadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, TupleDomain.all(), Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);

        Constraint constraint = new Constraint(withColumnDomains(Map.of(
                partitionHandle, PART,
                partitionHandle2, PART2,
                otherHandle, OTHER)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        assertTrue(result.isPresent());
        // Delegate sees the conjunction of both grain columns
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART, partitionHandle2, PART2)),
                stub.applyFilterConstraint.getSummary());
        CachingTableHandle cached = assertInstanceOf(CachingTableHandle.class, result.get().getHandle());
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART, partitionHandle2, PART2)),
                cached.getPartitionDomain());
        // Remaining filter carries only the non-grain domain
        assertEquals(withColumnDomains(Map.of(otherHandle, OTHER)), result.get().getRemainingFilter());
    }

    @Test
    void applyFilterCachesPartialGrainWhenOnlyOneConfiguredColumnIsConstrained() {
        ConnectorMetadata metadata = multiColumnMetadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, TupleDomain.all(), Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);

        // Only the first grain column is constrained; the second stays in the remaining filter
        Constraint constraint = new Constraint(withColumnDomains(Map.of(
                partitionHandle, PART,
                otherHandle, OTHER)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        assertTrue(result.isPresent());
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), stub.applyFilterConstraint.getSummary());
        CachingTableHandle cached = assertInstanceOf(CachingTableHandle.class, result.get().getHandle());
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), cached.getPartitionDomain());
        // Remaining filter carries only what the query constrained and we did not push;
        // part2 was never constrained, so it appears nowhere
        assertEquals(withColumnDomains(Map.of(otherHandle, OTHER)), result.get().getRemainingFilter());
    }

    @Test
    void unconfiguredTableForwardsFullConstraintEvenWhenFilterMatchesAColumn() {
        ConnectorMetadata metadata = unconfiguredMetadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        TupleDomain<ColumnHandle> delegateRemaining = withColumnDomains(Map.of(otherHandle, OTHER));
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, delegateRemaining, Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);

        Constraint constraint = new Constraint(withColumnDomains(Map.of(
                partitionHandle, PART,
                otherHandle, OTHER)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);

        // Full constraint reaches the delegate untouched; nothing is cacheable
        assertSame(constraint, stub.applyFilterConstraint);
        assertTrue(result.isPresent());
        CachingTableHandle cached = assertInstanceOf(CachingTableHandle.class, result.get().getHandle());
        assertTrue(cached.getPartitionDomain().isAll());
        assertEquals(delegateRemaining, result.get().getRemainingFilter());
    }

    @Test
    void outageServesTableHandleAndColumnsFromCache() {
        ConnectorMetadata metadata = metadataWithColumnTypes();
        CachingTableHandle first = tableHandle(metadata);
        metadata.getColumnHandles(null, first);

        RuntimeException outage = new RuntimeException("source down");
        stub.tableHandleFailure = outage;
        stub.columnHandlesFailure = outage;

        ConnectorTableHandle second = metadata.getTableHandle(
                null, new SchemaTableName("schema", "table"), Optional.empty(), Optional.empty());
        CachingTableHandle cached = assertInstanceOf(CachingTableHandle.class, second);
        assertSame(stub.handle, cached.getDelegateHandle());
        assertEquals(first.getColumnHandles(), cached.getColumnHandles());
        assertEquals(first.getColumnTypes(), cached.getColumnTypes());

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(null, cached);
        assertEquals(stub.columns, columns);
        assertEquals(1, stub.columnHandlesCalls);
    }

    @Test
    void outageOnUnknownTableRethrowsSourceError() {
        ConnectorMetadata metadata = metadata();
        RuntimeException outage = new RuntimeException("source down");
        stub.tableHandleFailure = outage;

        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                metadata.getTableHandle(null, new SchemaTableName("schema", "table"), Optional.empty(), Optional.empty()));
        assertSame(outage, thrown);
    }

    @Test
    void outageServesTableSchemaFromCache() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);
        ConnectorTableSchema before = (ConnectorTableSchema) metadata.getTableSchema(null, wrapped);

        RuntimeException outage = new RuntimeException("source down");
        stub.schemaFailure = outage;

        ConnectorTableSchema after = (ConnectorTableSchema) metadata.getTableSchema(null, wrapped);
        assertEquals(before.getTable(), after.getTable());
        assertEquals(before.getColumns(), after.getColumns());
    }

    @Test
    void outageTableSchemaWithoutCachedSchemaRethrowsSourceError() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);

        RuntimeException outage = new RuntimeException("source down");
        stub.schemaFailure = outage;

        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                metadata.getTableSchema(null, wrapped));
        assertSame(outage, thrown);
    }

    @Test
    void applyFilterOutageServesRememberedFilteredHandle() {
        ConnectorMetadata metadata = metadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, TupleDomain.all(), Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);
        Constraint constraint = new Constraint(withColumnDomains(Map.of(partitionHandle, PART)));
        assertTrue(metadata.applyFilter(null, wrapped, constraint).isPresent());

        stub.applyFilterFailure = new RuntimeException("source down");

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, wrapped, constraint);
        assertTrue(result.isPresent());
        CachingTableHandle cached = assertInstanceOf(CachingTableHandle.class, result.get().getHandle());
        assertSame(stub.filteredHandle, cached.getDelegateHandle());
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), cached.getPartitionDomain());
        assertEquals(wrapped.getColumnHandles(), cached.getColumnHandles());
        assertTrue(result.get().getRemainingFilter().isAll());
    }

    @Test
    void applyFilterOutageWithoutRememberedHandleDeclinesPushdown() {
        ConnectorMetadata metadata = metadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);

        stub.applyFilterFailure = new RuntimeException("source down");

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(
                null, wrapped, new Constraint(withColumnDomains(Map.of(partitionHandle, PART))));
        assertTrue(result.isEmpty());
    }

    @Test
    void outageColumnHandlesWithoutCachedColumnsRethrowsSourceError() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);

        RuntimeException outage = new RuntimeException("source down");
        stub.columnHandlesFailure = outage;

        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                metadata.getColumnHandles(null, wrapped));
        assertSame(outage, thrown);
    }

    @Test
    void applyFilterResolvesPartitionColumnsFromCache() {
        ConnectorMetadata metadata = metadata();
        stub.filteredHandle = new ConnectorTableHandle() {};
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, TupleDomain.all(), Constant.TRUE, false));
        CachingTableHandle wrapped = tableHandle(metadata);
        metadata.getColumnHandles(null, wrapped);
        int callsAfterFirstPass = stub.columnHandlesCalls;

        metadata.applyFilter(null, wrapped, new Constraint(withColumnDomains(Map.of(partitionHandle, PART))));

        assertEquals(callsAfterFirstPass, stub.columnHandlesCalls);
    }

    @Test
    void getTableStatisticsSkipsSourceForKnownTables() {
        ConnectorMetadata metadata = metadata();
        CachingTableHandle wrapped = tableHandle(metadata);

        assertEquals(TableStatistics.empty(), metadata.getTableStatistics(null, wrapped));
        assertEquals(0, stub.statisticsCalls);
    }

    @Test
    void getTableStatisticsForwardsForUnknownTables() {
        ConnectorMetadata metadata = metadata();
        ConnectorTableHandle foreign = new ConnectorTableHandle() {};

        assertEquals(TableStatistics.empty(), metadata.getTableStatistics(null, foreign));
        assertEquals(1, stub.statisticsCalls);
    }

    @Test
    void outageAfterRestartServesEveryPlanningCallFromNotes(@TempDir Path notesDir) {
        PlanningNotes notes = new PlanningNotes(notesDir, FixedTypeManager.INSTANCE);
        stub.columns = Map.of(PARTITION_COLUMN, partitionHandle, "other", otherHandle);
        stub.filteredHandle = new FakeDelegateHandle("filtered");
        stub.filterResult = Optional.of(new ConstraintApplicationResult<>(
                stub.filteredHandle, TupleDomain.all(), Constant.TRUE, false));

        ConnectorMetadata healthy = healthyWrapper(notes);
        CachingTableHandle first = tableHandle(healthy);
        healthy.getTableSchema(null, first);
        healthy.getColumnHandles(null, first);
        Constraint constraint = new Constraint(withColumnDomains(Map.of(partitionHandle, PART)));
        assertTrue(healthy.applyFilter(null, first, constraint).isPresent());

        stub.tableHandleFailure = new RuntimeException("source down");
        stub.columnHandlesFailure = stub.tableHandleFailure;
        stub.schemaFailure = stub.tableHandleFailure;
        stub.applyFilterFailure = stub.tableHandleFailure;
        stub.columnHandlesCalls = 0;

        ConnectorMetadata restarted = restartedWrapper(notes);
        CachingTableHandle served = tableHandle(restarted);
        ConnectorTableSchema schema = (ConnectorTableSchema) restarted.getTableSchema(null, served);
        Map<String, ColumnHandle> columns = restarted.getColumnHandles(null, served);
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> applied =
                restarted.applyFilter(null, served, constraint);

        assertInstanceOf(ConnectorTableHandle.class, served.getDelegateHandle());
        assertEquals(1, schema.getColumns().size());
        assertEquals(PARTITION_COLUMN, schema.getColumns().get(0).getName());
        assertEquals(BigintType.BIGINT, schema.getColumns().get(0).getType());
        assertEquals(stub.columns, columns);
        assertTrue(applied.isPresent());
        CachingTableHandle filtered = assertInstanceOf(CachingTableHandle.class, applied.get().getHandle());
        assertEquals(withColumnDomains(Map.of(partitionHandle, PART)), filtered.getPartitionDomain());
        assertEquals(stub.columns, filtered.getColumnHandles());
        assertEquals(0, stub.columnHandlesCalls);
    }

    @Test
    void restartOutageOnTableNeverSeenRethrowsSourceError(@TempDir Path notesDir) {
        PlanningNotes notes = new PlanningNotes(notesDir, FixedTypeManager.INSTANCE);
        RuntimeException outage = new RuntimeException("source down");
        stub.tableHandleFailure = outage;

        ConnectorMetadata restarted = restartedWrapper(notes);
        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                restarted.getTableHandle(null, new SchemaTableName("schema", "table"), Optional.empty(), Optional.empty()));
        assertSame(outage, thrown);
    }

    private ConnectorMetadata healthyWrapper(PlanningNotes notes) {
        stub.clearFailures();
        return CachingMetadata.wrap(stub, Map.of(TABLE_ID, List.of(PARTITION_COLUMN)), true, new TableMetadataCache(), notes);
    }

    private ConnectorMetadata restartedWrapper(PlanningNotes notes) {
        return CachingMetadata.wrap(stub, Map.of(TABLE_ID, List.of(PARTITION_COLUMN)), true, new TableMetadataCache(), notes);
    }
}
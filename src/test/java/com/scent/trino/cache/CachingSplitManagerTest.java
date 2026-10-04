package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingSplitManagerTest {

    private static final class StubSplitManager implements ConnectorSplitManager {
        ConnectorTableHandle receivedTable;
        ConnectorSplitSource source = new StubSplitSource();

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                Set<ColumnHandle> columns,
                Constraint constraint) {
            receivedTable = table;
            return source;
        }

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableFunctionHandle function) {
            return source;
        }
    }

    private static final class StubSplitSource implements ConnectorSplitSource {
        final List<ConnectorSplit> splits = new ArrayList<>();
        boolean closed;

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot) {
            return CompletableFuture.completedFuture(List.copyOf(splits));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isFinished() {
            return splits.isEmpty();
        }
    }

    private final StubSplitManager stub = new StubSplitManager();
    private final ColumnHandle columnA = new ColumnHandle() {};
    private final ColumnHandle columnB = new ColumnHandle() {};
    private final CachingSplitManager manager = new CachingSplitManager(stub);

    @Test
    void wrapsSplitsForCacheableScans() {
        ConnectorTableHandle delegateHandle = new ConnectorTableHandle() {};
        CachingTableHandle table = new CachingTableHandle(
                delegateHandle,
                new SchemaTableName("schema", "table"),
                withColumnDomains(Map.of(columnA, Domain.singleValue(BigintType.BIGINT, 1L))),
                Map.of("b", columnB, "a", columnA));
        ConnectorSplit delegateSplit = new ConnectorSplit() {};
        StubSplitSource source = (StubSplitSource) stub.source;
        source.splits.add(delegateSplit);

        ConnectorSplitSource result = manager.getSplits(null, null, table, Set.of(), Constraint.alwaysTrue());

        assertSame(delegateHandle, stub.receivedTable);
        List<ConnectorSplit> batch = result.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join();
        assertEquals(1, batch.size());
        CachingSplit split = assertInstanceOf(CachingSplit.class, batch.get(0));
        assertSame(delegateSplit, split.getDelegateSplit());
        assertSame(delegateHandle, split.getDelegateTableHandle());
        // Cached layout is deterministic: sorted by column name
        assertEquals(List.of(columnA, columnB), split.cachedChannelOrder());
        assertEquals(0, split.cachedChannelIndex(columnA));
        assertEquals(1, split.cachedChannelIndex(columnB));
        assertTrue(split.getSliceKey().contains("schema.table"));
        assertTrue(split.getSliceKey().contains("|"));
        assertFalse(result.isFinished());
        result.close();
        assertTrue(source.closed);
    }

    @Test
    void passesThroughUncacheableScans() {
        ConnectorTableHandle delegateHandle = new ConnectorTableHandle() {};
        CachingTableHandle table = new CachingTableHandle(
                delegateHandle,
                new SchemaTableName("schema", "table"),
                TupleDomain.all(),
                Map.of("a", columnA, "b", columnB));
        ConnectorSplit delegateSplit = new ConnectorSplit() {};
        StubSplitSource source = (StubSplitSource) stub.source;
        source.splits.add(delegateSplit);

        ConnectorSplitSource result = manager.getSplits(null, null, table, Set.of(), Constraint.alwaysTrue());

        assertSame(delegateHandle, stub.receivedTable);
        List<ConnectorSplit> batch = result.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join();
        assertEquals(1, batch.size());
        assertSame(delegateSplit, batch.get(0));
    }

    @Test
    void passesThroughForeignHandlesAndUnknownColumnLayout() {
        // Foreign handle: straight passthrough
        ConnectorTableHandle foreign = new ConnectorTableHandle() {};
        manager.getSplits(null, null, foreign, Set.of(), Constraint.alwaysTrue());
        assertSame(foreign, stub.receivedTable);

        // Ours but no column layout attached (getColumnHandles never ran): uncached
        ConnectorTableHandle delegateHandle = new ConnectorTableHandle() {};
        CachingTableHandle noColumns = new CachingTableHandle(
                delegateHandle,
                new SchemaTableName("schema", "table"),
                withColumnDomains(Map.of(columnA, Domain.singleValue(BigintType.BIGINT, 1L))),
                null);
        manager.getSplits(null, null, noColumns, Set.of(), Constraint.alwaysTrue());
        assertSame(delegateHandle, stub.receivedTable);
    }

    @Test
    void tableFunctionSplitsPassThrough() {
        ConnectorSplitSource source = manager.getSplits(null, null, new ConnectorTableFunctionHandle() {});
        assertSame(stub.source, source);
    }
}
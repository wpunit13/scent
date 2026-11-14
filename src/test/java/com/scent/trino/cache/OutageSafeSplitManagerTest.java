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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutageSafeSplitManagerTest {

    /** Connectivity-shaped failure: the only kind the outage fallbacks are allowed to react to. */
    private static RuntimeException outage() {
        return new RuntimeException(new java.net.ConnectException("connection refused"));
    }

    private final ConnectorSplit firstSplit = new ConnectorSplit() {};
    private final ColumnHandle column = new ColumnHandle() {};
    private final CachingTableHandle cacheableHandle = new CachingTableHandle(
            new ConnectorTableHandle() {},
            new SchemaTableName("schema", "table"),
            withColumnDomains(Map.of(column, Domain.singleValue(BigintType.BIGINT, 1L))),
            Map.of("part", column),
            Map.of("part", "bigint"));

    private static final class StubSplitManager implements ConnectorSplitManager {
        final Deque<ConnectorSplitSource> sources = new ArrayDeque<>();
        RuntimeException syncFailure;

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableHandle table,
                Set<ColumnHandle> columns,
                Constraint constraint) {
            if (syncFailure != null) {
                throw syncFailure;
            }
            return sources.removeFirst();
        }

        @Override
        public ConnectorSplitSource getSplits(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorTableFunctionHandle function) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * getNextBatch plays the script in order: a List is a successful batch, a RuntimeException
     * is a failed batch. close throws when closeFailure is set.
     */
    private static final class ScriptedSource implements ConnectorSplitSource {
        final Deque<Object> script = new ArrayDeque<>();
        RuntimeException closeFailure;
        boolean finished;

        @Override
        @SuppressWarnings("unchecked")
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot) {
            Object next = script.removeFirst();
            if (next instanceof RuntimeException failure) {
                return CompletableFuture.failedFuture(failure);
            }
            finished = true;
            return CompletableFuture.completedFuture((List<ConnectorSplit>) next);
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void close() {
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    @Test
    void recordsBatchThenReplaysItOnSourceOutage() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource healthy = new ScriptedSource();
        healthy.script.add(List.of(firstSplit));
        ScriptedSource outage = new ScriptedSource();
        RuntimeException sourceError = outage();
        outage.script.add(sourceError);
        outage.closeFailure = new RuntimeException("close down");
        inner.sources.add(healthy);
        inner.sources.add(outage);

        ConnectorSplitSource warmup = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit), warmup.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertTrue(warmup.isFinished());

        ConnectorSplitSource duringOutage = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit), duringOutage.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertTrue(duringOutage.isFinished());
        duringOutage.close();
    }

    @Test
    void syncFailureWhileBuildingTheSourceReplaysRememberedBatch() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource healthy = new ScriptedSource();
        healthy.script.add(List.of(firstSplit));
        inner.sources.add(healthy);

        ConnectorSplitSource warmup = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit), warmup.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());

        inner.syncFailure = outage();
        ConnectorSplitSource duringOutage = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit), duringOutage.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertTrue(duringOutage.isFinished());
    }

    @Test
    void replayRespectsTheCallerBatchSize() {
        ConnectorSplit second = new ConnectorSplit() {};
        ConnectorSplit third = new ConnectorSplit() {};
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource healthy = new ScriptedSource();
        healthy.script.add(List.of(firstSplit, second, third));
        inner.sources.add(healthy);

        ConnectorSplitSource warmup = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit, second, third), warmup.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());

        ScriptedSource outage = new ScriptedSource();
        outage.script.add(outage());
        outage.script.add(outage());
        inner.sources.add(outage);

        ConnectorSplitSource duringOutage = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit, second), duringOutage.getNextBatch(2, DynamicFilterSnapshot.EMPTY).join());
        assertFalse(duringOutage.isFinished());
        assertEquals(List.of(third), duringOutage.getNextBatch(2, DynamicFilterSnapshot.EMPTY).join());
        assertTrue(duringOutage.isFinished());
    }

    @Test
    void firstOutageWithoutRememberedBatchPropagatesSourceError() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource outage = new ScriptedSource();
        outage.script.add(outage());
        inner.sources.add(outage);

        ConnectorSplitSource source = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        CompletionException thrown = assertThrows(CompletionException.class,
                () -> source.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertInstanceOf(java.net.ConnectException.class, rootCause(thrown));
    }

    @Test
    void midStreamFailurePropagatesInsteadOfReplaying() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource partiallyRead = new ScriptedSource();
        partiallyRead.script.add(List.of(firstSplit));
        partiallyRead.script.add(outage());
        inner.sources.add(partiallyRead);

        ConnectorSplitSource source = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit), source.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        CompletionException thrown = assertThrows(CompletionException.class,
                () -> source.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertInstanceOf(java.net.ConnectException.class, rootCause(thrown));
    }

    @Test
    void passthroughScansFailLikeToday() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource outage = new ScriptedSource();
        outage.script.add(outage());
        inner.sources.add(outage);

        CachingTableHandle uncached = new CachingTableHandle(
                new ConnectorTableHandle() {},
                new SchemaTableName("schema", "table"),
                TupleDomain.all(),
                Map.of("part", new ColumnHandle() {}),
                Map.of("part", "bigint"));
        ConnectorSplitSource source = manager.getSplits(null, null, uncached, Set.of(), Constraint.alwaysTrue());
        CompletionException thrown = assertThrows(CompletionException.class,
                () -> source.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertInstanceOf(java.net.ConnectException.class, rootCause(thrown));
    }

    @Test
    void nonConnectivityFailureIsNotMistakenForAnOutage() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource healthy = new ScriptedSource();
        healthy.script.add(List.of(firstSplit));
        ScriptedSource buggy = new ScriptedSource();
        RuntimeException bug = new NullPointerException("delegate bug");
        buggy.script.add(bug);
        inner.sources.add(healthy);
        inner.sources.add(buggy);

        ConnectorSplitSource warmup = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        warmup.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join();

        // A remembered batch exists, but a NullPointerException is a bug, not an outage;
        // the failure must propagate instead of replaying stale work.
        ConnectorSplitSource source = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        CompletionException thrown = assertThrows(CompletionException.class,
                () -> source.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertSame(bug, rootCause(thrown));
    }

    @Test
    void connectivityFailureReplaysTheRememberedBatch() {
        StubSplitManager inner = new StubSplitManager();
        OutageSafeSplitManager manager = new OutageSafeSplitManager(inner);
        ScriptedSource healthy = new ScriptedSource();
        healthy.script.add(List.of(firstSplit));
        ScriptedSource outage = new ScriptedSource();
        RuntimeException connectivity = new RuntimeException(new java.net.ConnectException("connection refused"));
        outage.script.add(connectivity);
        inner.sources.add(healthy);
        inner.sources.add(outage);

        ConnectorSplitSource warmup = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        warmup.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join();

        ConnectorSplitSource duringOutage = manager.getSplits(null, null, cacheableHandle, Set.of(), Constraint.alwaysTrue());
        assertEquals(List.of(firstSplit), duringOutage.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join());
        assertTrue(duringOutage.isFinished());
    }

    private static Throwable rootCause(Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }
}

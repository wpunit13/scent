package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * S7: wraps the caching split manager so a fully cached slice still schedules while the source
 * is down. The delegate's split generation opens a JDBC connection (BaseJdbcClient#toColumnMappings),
 * so on a source failure the last successfully produced batch for the slice is replayed instead.
 * The stale delegate split inside a replayed CachingSplit is reachable only through the populate
 * path, which fails with the source error — stale data is never served. The batch memory is
 * in-JVM only: split generation after a coordinator restart leans on the delegate's own
 * getSplits being source-free (true for postgres), so the batch is not noted to disk.
 */
@Slf4j
public final class OutageSafeSplitManager implements ConnectorSplitManager {

    private final ConnectorSplitManager delegate;
    private final Map<CacheKey, List<ConnectorSplit>> lastBatchBySlice = new ConcurrentHashMap<>();

    public OutageSafeSplitManager(ConnectorSplitManager delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> columns,
            Constraint constraint) {
        if (table instanceof CachingTableHandle caching && isCacheable(caching)) {
            CacheKey cacheKey = CacheKey.of(caching.getSchemaTableName(), caching.getPartitionDomain());
            ConnectorSplitSource source;
            try {
                source = delegate.getSplits(transaction, session, table, columns, constraint);
            }
            catch (RuntimeException e) {
                // The delegate builds its split source eagerly and that path opens a JDBC
                // connection (BaseJdbcClient#toColumnMappings), so the failure can surface
                // before any split source exists.
                List<ConnectorSplit> remembered = lastBatchBySlice.get(cacheKey);
                if (remembered == null) {
                    throw e;
                }
                log.info("[SCENT SPI] source outage: serving splits for {} from the last known batch", cacheKey);
                return new FixedSplitSource(remembered);
            }
            return new RememberingSplitSource(source, cacheKey);
        }
        return delegate.getSplits(transaction, session, table, columns, constraint);
    }

    // Mirrors CachingSplitManager.isCacheable, which is out of bounds for this change.
    private static boolean isCacheable(CachingTableHandle caching) {
        return !caching.getPartitionDomain().isAll() && caching.getColumnHandles() != null;
    }

    private final class RememberingSplitSource implements ConnectorSplitSource {
        private final ConnectorSplitSource delegate;
        private final CacheKey cacheKey;
        // The delegate future can complete on a worker thread; these flags are read by the
        // scheduler thread afterwards.
        private volatile boolean produced;
        private volatile boolean replayed;

        private RememberingSplitSource(ConnectorSplitSource delegate, CacheKey cacheKey) {
            this.delegate = delegate;
            this.cacheKey = cacheKey;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot) {
            return delegate.getNextBatch(maxSize, dynamicFilterSnapshot)
                    .handle((batch, error) -> {
                        if (error == null) {
                            produced = true;
                            if (!batch.isEmpty()) {
                                lastBatchBySlice.put(cacheKey, List.copyOf(batch));
                            }
                            return batch;
                        }
                        // A replay is only safe before this source produced anything; mid-stream
                        // failures must propagate or the remembered batch would double-count.
                        List<ConnectorSplit> remembered = produced ? null : lastBatchBySlice.get(cacheKey);
                        if (remembered == null) {
                            throw unwrap(error);
                        }
                        replayed = true;
                        log.info("[SCENT SPI] source outage: serving splits for {} from the last known batch", cacheKey);
                        return remembered;
                    });
        }

        @Override
        public boolean isFinished() {
            if (replayed) {
                return true;
            }
            return delegate.isFinished();
        }

        @Override
        public void close() {
            try {
                delegate.close();
            }
            catch (RuntimeException e) {
                log.debug("[SCENT SPI] split source close failed during a source outage: {}", e.toString());
            }
        }

        private static RuntimeException unwrap(Throwable error) {
            Throwable cause = error;
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof RuntimeException runtime) {
                return runtime;
            }
            return new RuntimeException(cause);
        }
    }
}

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
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

@Slf4j
public final class CachingSplitManager implements ConnectorSplitManager {
    private final ConnectorSplitManager delegate;

    public CachingSplitManager(ConnectorSplitManager delegate) {
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
            log.debug("[SCENT SPI] getSplits wrapping splits for {}", caching);
            CacheKey cacheKey = CacheKey.of(caching.getSchemaTableName(), caching.getPartitionDomain());
            return new CachingSplitSource(
                    delegate.getSplits(transaction, session, caching.getDelegateHandle(), columns, constraint),
                    split -> new CachingSplit(
                            cacheKey,
                            caching.getColumnHandles(),
                            caching.getColumnTypes(),
                            split,
                            caching.getDelegateHandle()));
        }
        log.debug("[SCENT SPI] getSplits passthrough");
        return delegate.getSplits(transaction, session, unwrap(table), columns, constraint);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function) {
        return delegate.getSplits(transaction, session, function);
    }

    /**
     * Cacheable when a partition predicate was pushed and the column layout is known;
     * otherwise pass through uncached (C7).
     */
    private static boolean isCacheable(CachingTableHandle caching) {
        return !caching.getPartitionDomain().isAll() && caching.getColumnHandles() != null;
    }

    private static ConnectorTableHandle unwrap(ConnectorTableHandle table) {
        return table instanceof CachingTableHandle caching ? caching.getDelegateHandle() : table;
    }

    private record CachingSplitSource(
            ConnectorSplitSource delegate,
            UnaryOperator<ConnectorSplit> wrapper) implements ConnectorSplitSource {
        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot) {
            return delegate.getNextBatch(maxSize, dynamicFilterSnapshot)
                    .thenApply(batch -> batch.stream().map(wrapper).toList());
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }
    }
}
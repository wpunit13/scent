package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class CachingPageSourceProvider implements ConnectorPageSourceProvider {
    private final ConnectorPageSourceProvider delegate;
    private final FileSliceStore sliceStore;

    public CachingPageSourceProvider(
            ConnectorPageSourceProvider delegate,
            FileSliceStore sliceStore) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.sliceStore = Objects.requireNonNull(sliceStore, "sliceStore is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        if (split instanceof CachingSplit caching) {
            int[] channels = channelsFor(columns, caching);
            if (channels != null) {
                Optional<ConnectorPageSource> cached = sliceStore.openRead(
                        caching.getCacheKey(), Arrays.stream(channels).boxed().toList(), caching.cachedChannelTypes());
                if (cached.isPresent()) {
                    log.debug("[SCENT] cache hit for {}", caching.getCacheKey());
                    return cached.get();
                }
                log.debug("[SCENT] cache miss for {}, populating", caching.getCacheKey());
                ConnectorPageSource source = delegate.createPageSource(
                        transaction,
                        session,
                        caching.getDelegateSplit(),
                        caching.getDelegateTableHandle(),
                        tableCredentials,
                        // Raw slice read: all columns, no dynamic filter, so the slice is reusable
                        caching.cachedChannelOrder(),
                        DynamicFilter.EMPTY);
                return new PopulatingPageSource(source, caching, channels, sliceStore);
            }
            // Requested column unknown to the cached layout (e.g. schema changed mid-flight):
            // degrade to the delegate read for this split.
            log.debug("[SCENT] unknown requested column for {}, passing through", caching.getCacheKey());
            return delegate.createPageSource(
                    transaction,
                    session,
                    caching.getDelegateSplit(),
                    caching.getDelegateTableHandle(),
                    tableCredentials,
                    columns,
                    dynamicFilter);
        }
        return delegate.createPageSource(
                transaction,
                session,
                split,
                unwrap(table),
                tableCredentials,
                columns,
                dynamicFilter);
    }

    /**
     * Channel index of each requested column in the cached layout, or null when a requested
     * column is unknown to the layout — the read then degrades to the delegate (and the
     * populate path refreshes the slice).
     */
    private static int[] channelsFor(List<ColumnHandle> columns, CachingSplit split) {
        int[] channels = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            channels[i] = split.cachedChannelIndex(columns.get(i));
            if (channels[i] < 0) {
                return null;
            }
        }
        return channels;
    }

    /**
     * Projects cached pages to the requested channels. Blocks are shared, never copied:
     * Trino blocks are immutable once built.
     */
    private static Page projectPage(Page page, int[] channels) {
        if (channels.length == 0) {
            return new Page(page.getPositionCount());
        }
        Block[] blocks = new Block[channels.length];
        for (int channel = 0; channel < channels.length; channel++) {
            blocks[channel] = page.getBlock(channels[channel]);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static ConnectorTableHandle unwrap(ConnectorTableHandle table) {
        return table instanceof CachingTableHandle caching ? caching.getDelegateHandle() : table;
    }

    /**
     * Streaming tee over the delegate read: emits requested-column pages to the engine while
     * writing the raw slice to a temporary Parquet file. The file is published only on clean
     * exhaustion; an early close or an oversized slice aborts the write.
     */
    private static final class PopulatingPageSource implements ConnectorPageSource {
        private final ConnectorPageSource delegate;
        private final CachingSplit split;
        private final int[] channels;
        private final FileSliceStore sliceStore;
        private FileSliceStore.WriteSession writeSession;
        private boolean cacheable;
        private boolean published;

        PopulatingPageSource(
                ConnectorPageSource delegate,
                CachingSplit split,
                int[] channels,
                FileSliceStore sliceStore) {
            this.delegate = delegate;
            this.split = split;
            this.channels = channels;
            this.sliceStore = sliceStore;
            this.cacheable = sliceStore.isEnabled();
            if (cacheable) {
                try {
                    this.writeSession = sliceStore.beginWrite(
                            split.getCacheKey(), split.cachedChannelNames(), split.cachedChannelTypes());
                    cacheable = writeSession != null;
                }
                catch (Exception e) {
                    cacheable = false;
                    log.warn("[SCENT] failed to start slice write {}", split.getCacheKey(), e);
                }
            }
        }

        @Override
        public SourcePage getNextSourcePage() {
            try {
                SourcePage raw = delegate.getNextSourcePage();
                if (raw == null) {
                    publishIfComplete();
                    return null;
                }
                appendToStore(raw.getPage());
                return SourcePage.create(projectPage(raw.getPage(), channels));
            }
            catch (RuntimeException | Error e) {
                cacheable = false;
                abortWriteSession();
                throw e;
            }
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            try {
                boolean finished = delegate.isFinished();
                if (finished) {
                    publishIfComplete();
                }
                return finished;
            }
            catch (RuntimeException | Error e) {
                cacheable = false;
                abortWriteSession();
                throw e;
            }
        }

        @Override
        public void close() throws IOException {
            // The engine may stop at isFinished() without polling the terminal null,
            // so a completed slice can publish here too; publishIfComplete is idempotent.
            publishIfComplete();
            if (!published) {
                abortWriteSession();
                log.debug("[SCENT] discarded incomplete slice {}", split.getCacheKey());
            }
            delegate.close();
        }

        /**
         * The delegate reports finished only after every page was consumed, so the slice
         * is complete exactly when this succeeds.
         */
        private void publishIfComplete() {
            if (!published && delegate.isFinished()) {
                published = true;
                if (cacheable) {
                    try {
                        if (writeSession != null) {
                            writeSession.complete();
                            log.debug("[SCENT] published slice {}", split.getCacheKey());
                        }
                    }
                    catch (Exception e) {
                        log.warn("[SCENT] failed to publish slice {}", split.getCacheKey(), e);
                        abortWriteSession();
                    }
                }
            }
        }

        private void appendToStore(Page page) {
            if (!cacheable) {
                return;
            }
            try {
                if (writeSession != null && !writeSession.write(page)) {
                    cacheable = false;
                    writeSession = null;
                    log.debug("[SCENT] skipped oversized slice {} (limit {} bytes)", split.getCacheKey(), sliceStore.maxBytes());
                }
            }
            catch (Exception e) {
                log.warn("[SCENT] failed to write slice {}", split.getCacheKey(), e);
                cacheable = false;
                abortWriteSession();
            }
        }

        private void abortWriteSession() {
            if (writeSession != null) {
                try {
                    writeSession.abort();
                }
                catch (Exception e) {
                    log.warn("[SCENT] failed to clean up temporary Parquet slice {}", split.getCacheKey(), e);
                }
                writeSession = null;
            }
        }
    }
}

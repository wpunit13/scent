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
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SourcePage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class CachingPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProvider delegate;
    private final InMemorySliceStore sliceStore;

    public CachingPageSourceProvider(
            ConnectorPageSourceProvider delegate,
            InMemorySliceStore sliceStore)
    {
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
            DynamicFilter dynamicFilter)
    {
        if (split instanceof CachingSplit caching) {
            int[] channels = channelsFor(columns, caching);
            if (channels != null) {
                Optional<List<Page>> cached = sliceStore.get(caching.getSliceKey());
                if (cached.isPresent()) {
                    log.debug("[SCENT] cache hit for {}", caching.getSliceKey());
                    return new FixedPageSource(project(cached.get(), channels));
                }
                log.debug("[SCENT] cache miss for {}, populating", caching.getSliceKey());
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
            log.debug("[SCENT] unknown requested column for {}, passing through", caching.getSliceKey());
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
    private static int[] channelsFor(List<ColumnHandle> columns, CachingSplit split)
    {
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
    private static List<Page> project(List<Page> pages, int[] channels)
    {
        return pages.stream()
                .map(page -> projectPage(page, channels))
                .toList();
    }

    private static Page projectPage(Page page, int[] channels)
    {
        if (channels.length == 0) {
            return new Page(page.getPositionCount());
        }
        Block[] blocks = new Block[channels.length];
        for (int channel = 0; channel < channels.length; channel++) {
            blocks[channel] = page.getBlock(channels[channel]);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static ConnectorTableHandle unwrap(ConnectorTableHandle table)
    {
        return table instanceof CachingTableHandle caching ? caching.getDelegateHandle() : table;
    }

    /**
     * Streaming tee over the delegate read: emits requested-column pages to the engine while
     * buffering the raw slice. The slice is published only on clean exhaustion; an early close
     * (query cancel, error, limit) discards the partial buffer (C4).
     */
    private static final class PopulatingPageSource
            implements ConnectorPageSource
    {
        private final ConnectorPageSource delegate;
        private final CachingSplit split;
        private final int[] channels;
        private final InMemorySliceStore sliceStore;
        private final List<Page> buffer = new ArrayList<>();
        private long bufferedBytes;
        private boolean overCap;
        private boolean published;

        PopulatingPageSource(
                ConnectorPageSource delegate,
                CachingSplit split,
                int[] channels,
                InMemorySliceStore sliceStore)
        {
            this.delegate = delegate;
            this.split = split;
            this.channels = channels;
            this.sliceStore = sliceStore;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            SourcePage raw = delegate.getNextSourcePage();
            if (raw == null) {
                publishIfComplete();
                return null;
            }
            if (bufferedBytes + raw.getSizeInBytes() > sliceStore.maxBytes()) {
                // ponytail: once over cap, drop the rest of the slice instead of evicting
                // everything else to fit it; revisit only if large partitions matter.
                overCap = true;
            }
            else {
                buffer.add(raw.getPage());
                bufferedBytes += raw.getSizeInBytes();
            }
            return SourcePage.create(projectPage(raw.getPage(), channels));
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
            return delegate.isFinished();
        }

        @Override
        public void close()
                throws IOException
        {
            // The engine may stop at isFinished() without polling the terminal null,
            // so a completed buffer can publish here too; publishIfComplete is idempotent.
            publishIfComplete();
            if (!published && !overCap) {
                log.debug("[SCENT] discarded partial slice {}", split.getSliceKey());
            }
            delegate.close();
        }

        /**
         * The delegate reports finished only after every page was consumed, so the buffer
         * is complete exactly when this succeeds.
         */
        private void publishIfComplete()
        {
            if (!published && !overCap && delegate.isFinished()) {
                published = true;
                sliceStore.put(split.getSliceKey(), buffer);
                log.debug("[SCENT] published slice {} ({} bytes)", split.getSliceKey(), bufferedBytes);
            }
        }
    }
}
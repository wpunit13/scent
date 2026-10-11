package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingPageSourceProviderTest {

    private static final class FakePageSource implements ConnectorPageSource {
        private final List<Page> pages;
        private int next;
        boolean closed;

        FakePageSource(List<Page> pages) {
            this.pages = pages;
        }

        @Override
        public SourcePage getNextSourcePage() {
            return next < pages.size() ? SourcePage.create(pages.get(next++)) : null;
        }

        @Override
        public long getCompletedBytes() {
            return 0;
        }

        @Override
        public long getReadTimeNanos() {
            return 0;
        }

        @Override
        public boolean isFinished() {
            return next >= pages.size();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** Records what the delegate was asked for; serves pages per the requested column list. */
    private static final class RecordingDelegate implements ConnectorPageSourceProvider {
        ConnectorSplit lastSplit;
        ConnectorTableHandle lastTable;
        List<ColumnHandle> lastColumns;
        DynamicFilter lastDynamicFilter;
        ConnectorPageSource lastSource;
        List<Page> pagesToServe = List.of();

        @Override
        public ConnectorPageSource createPageSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorSplit split,
                ConnectorTableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter) {
            lastSplit = split;
            lastTable = table;
            lastColumns = List.copyOf(columns);
            lastDynamicFilter = dynamicFilter;
            ConnectorPageSource source = new FixedPageSource(pagesToServe);
            lastSource = source;
            return source;
        }
    }

    private final ColumnHandle columnA = new ColumnHandle() {};
    private final ColumnHandle columnB = new ColumnHandle() {};
    private final RecordingDelegate delegate = new RecordingDelegate();
    private final InMemorySliceStore store = new InMemorySliceStore(1024 * 1024, null, null);
    private final CachingPageSourceProvider provider = new CachingPageSourceProvider(delegate, store);

    private static Block byteBlock(int value) {
        return new ByteArrayBlock(1, Optional.empty(), new byte[] {(byte) value});
    }

    private CachingSplit split() {
        return new CachingSplit(
                sliceKey(),
                Map.of("a", columnA, "b", columnB), // cached layout: [columnA, columnB] (sorted)
                Map.of(),
                new ConnectorSplit() {},
                new ConnectorTableHandle() {});
    }

    private CacheKey sliceKey() {
        return CacheKey.of(
                new SchemaTableName("schema", "table"),
                withColumnDomains(Map.of(columnA, Domain.singleValue(BigintType.BIGINT, 1L))));
    }

    private static List<Page> drain(ConnectorPageSource source) throws IOException {
        List<Page> pages = new ArrayList<>();
        try {
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page != null) {
                    pages.add(page.getPage());
                }
            }
        }
        finally {
            // Mimic the engine lifecycle: the source is always closed
            source.close();
        }
        return pages;
    }

    @Test
    void missPopulatesStoreAndProjectsRequestedColumns() throws IOException {
        // Raw pages in cached layout [a, b]
        Block rawA = byteBlock(1);
        Block rawB = byteBlock(2);
        Block rawA2 = byteBlock(3);
        Block rawB2 = byteBlock(4);
        delegate.pagesToServe = List.of(new Page(rawA, rawB), new Page(rawA2, rawB2));
        CachingSplit split = split();

        ConnectorPageSource source = provider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnB), DynamicFilter.EMPTY);

        // Populate read requests the full raw layout from the delegate split/handle
        assertSame(split.getDelegateSplit(), delegate.lastSplit);
        assertSame(split.getDelegateTableHandle(), delegate.lastTable);
        assertEquals(List.of(columnA, columnB), delegate.lastColumns);
        assertSame(DynamicFilter.EMPTY, delegate.lastDynamicFilter);

        List<Page> emitted = drain(source);
        assertEquals(2, emitted.size());
        assertEquals(1, emitted.get(0).getChannelCount());
        assertSame(rawB, emitted.get(0).getBlock(0));
        assertSame(rawB2, emitted.get(1).getBlock(0));

        // Clean finish publishes the raw slice
        List<Page> cached = store.get(sliceKey()).orElseThrow();
        assertEquals(2, cached.size());
        assertSame(rawA, cached.get(0).getBlock(0));
        assertSame(rawB, cached.get(0).getBlock(1));
        assertTrue(source.isFinished());
    }

    @Test
    void completedPopulateWritesParquetUsingSplitColumnTypes(@TempDir Path cacheDirectory) throws IOException {
        List<String> resolvedTypeNames = new ArrayList<>();
        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> {
                    if (!method.getName().equals("fromSqlType")) {
                        return null;
                    }
                    String typeName = (String) arguments[0];
                    resolvedTypeNames.add(typeName);
                    return switch (typeName) {
                        case "bigint" -> BigintType.BIGINT;
                        case "varchar" -> VarcharType.VARCHAR;
                        default -> throw new IllegalArgumentException("Unexpected type: " + typeName);
                    };
                });
        InMemorySliceStore diskStore = new InMemorySliceStore(1024 * 1024, typeManager, cacheDirectory);
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);
        delegate.pagesToServe = List.of();
        CachingSplit split = new CachingSplit(
                CacheKey.of(new SchemaTableName("schema", "table"), withColumnDomains(Map.of(columnA, Domain.singleValue(BigintType.BIGINT, 1L)))),
                Map.of("a", columnA, "b", columnB),
                Map.of("b", "varchar", "a", "bigint"),
                new ConnectorSplit() {},
                new ConnectorTableHandle() {});

        ConnectorPageSource source = diskProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);
        drain(source);

        assertTrue(diskStore.get(split.getCacheKey()).isPresent());
        assertTrue(Files.size(diskStore.fileFor(split.getCacheKey())) > 0);
        assertEquals(List.of("bigint", "varchar"), resolvedTypeNames);
    }

    @Test
    void hitServesCachedPagesProjectedToRequestedOrder() throws IOException {
        Block rawA = byteBlock(1);
        Block rawB = byteBlock(2);
        store.put(sliceKey(), List.of(new Page(rawA, rawB)), List.of("a", "b"), List.of("bigint", "bigint"));
        CachingSplit split = split();

        // Reversed request order must be projected back correctly; delegate is never touched
        ConnectorPageSource source = provider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnB, columnA), DynamicFilter.EMPTY);

        assertNull(delegate.lastSplit);
        FixedPageSource fixed = assertInstanceOf(FixedPageSource.class, source);
        List<Page> pages = drain(fixed);
        assertEquals(1, pages.size());
        assertEquals(2, pages.get(0).getChannelCount());
        assertSame(rawB, pages.get(0).getBlock(0));
        assertSame(rawA, pages.get(0).getBlock(1));
    }

    @Test
    void hitWithoutTerminalNullStillPublishesOnClose() throws IOException {
        // Delegate serves one page; consumer stops at isFinished() without polling the null
        Block rawA = byteBlock(1);
        delegate.pagesToServe = List.of(new Page(rawA, byteBlock(2)));
        CachingSplit split = split();

        ConnectorPageSource source = provider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);

        SourcePage page = source.getNextSourcePage();
        assertSame(rawA, page.getPage().getBlock(0));
        assertTrue(source.isFinished());
        source.close();

        assertTrue(store.get(sliceKey()).isPresent());
    }

    @Test
    void closeBeforeFinishDiscardsPartialSlice() throws IOException {
        Block rawA = byteBlock(1);
        delegate.pagesToServe = List.of(new Page(rawA, byteBlock(2)), new Page(byteBlock(3), byteBlock(4)));
        CachingSplit split = split();

        ConnectorPageSource source = provider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);

        source.getNextSourcePage();
        source.close();

        assertTrue(store.get(sliceKey()).isEmpty());
        // Delegate still closed (connection returned)
        // (the source is a PopulatingPageSource wrapping the FixedPageSource delegate)
    }

    @Test
    void unknownRequestedColumnDegradesToDelegate() {
        delegate.pagesToServe = List.of();
        CachingSplit split = split();
        ConnectorTableHandle callerTable = new ConnectorTableHandle() {};

        ConnectorPageSource source = provider.createPageSource(
                null, null, split, callerTable,
                Optional.empty(), List.of(columnB, new ColumnHandle() {}), DynamicFilter.EMPTY);

        assertSame(split.getDelegateSplit(), delegate.lastSplit);
        assertSame(split.getDelegateTableHandle(), delegate.lastTable);
        // Degraded read keeps the caller's requested columns
        assertEquals(2, delegate.lastColumns.size());
        assertSame(source, delegate.lastSource);
    }

    @Test
    void plainSplitsPassThroughUnchanged() {
        ConnectorSplit plainSplit = new ConnectorSplit() {};
        ConnectorTableHandle plainTable = new ConnectorTableHandle() {};
        List<ColumnHandle> columns = List.of(columnA);
        delegate.pagesToServe = List.of();
        ConnectorPageSource delegateSource = provider.createPageSource(
                null, null, plainSplit, plainTable,
                Optional.empty(), columns, DynamicFilter.EMPTY);

        assertSame(plainSplit, delegate.lastSplit);
        assertSame(plainTable, delegate.lastTable);
        assertEquals(columns, delegate.lastColumns);
        assertSame(delegateSource, delegate.lastSource);
    }

    @Test
    void oversizeSliceNeverPublishes() throws IOException {
        delegate.pagesToServe = List.of(new Page(byteBlock(1), byteBlock(2)));
        // Cap smaller than one page: everything is over cap from the first page
        InMemorySliceStore tinyStore = new InMemorySliceStore(1, null, null);
        CachingPageSourceProvider tinyProvider = new CachingPageSourceProvider(delegate, tinyStore);
        CachingSplit split = split();

        ConnectorPageSource source = tinyProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);

        drain(source);
        assertTrue(tinyStore.get(sliceKey()).isEmpty());
    }
}
package com.scent.trino.cache;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingPageSourceProviderTest {

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
    private final FileSliceStore store = new FileSliceStore(1024 * 1024, null, null);
    private final CachingPageSourceProvider provider = new CachingPageSourceProvider(delegate, store);

    private static Block byteBlock(int value) {
        return new ByteArrayBlock(1, Optional.empty(), new byte[] {(byte) value});
    }

    private static TypeManager bigintTypeManager() {
        return (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);
    }

    private static Page bigintPage(long... values) {
        Block[] blocks = new Block[values.length];
        for (int channel = 0; channel < values.length; channel++) {
            BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
            BigintType.BIGINT.writeLong(builder, values[channel]);
            blocks[channel] = builder.build();
        }
        return new Page(1, blocks);
    }

    private static Page mixedPage(long number, String text) {
        BlockBuilder numberBlock = BigintType.BIGINT.createBlockBuilder(null, 1);
        BigintType.BIGINT.writeLong(numberBlock, number);
        BlockBuilder textBlock = VarcharType.VARCHAR.createBlockBuilder(null, 1);
        VarcharType.VARCHAR.writeSlice(textBlock, Slices.utf8Slice(text));
        return new Page(numberBlock.build(), textBlock.build());
    }

    private CachingSplit typedSplit() {
        return new CachingSplit(
                sliceKey(),
                Map.of("a", columnA, "b", columnB),
                Map.of("a", "bigint", "b", "bigint"),
                new ConnectorSplit() {},
                new ConnectorTableHandle() {});
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

    private static List<Path> temporaryFiles(Path directory) throws IOException {
        if (Files.notExists(directory)) {
            return List.of();
        }
        try (var paths = Files.walk(directory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".parquet.tmp")).toList();
        }
    }

    @Test
    void missStreamsPagesAndProjectsRequestedColumnsWhenDiskCachingIsDisabled() throws IOException {
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

        assertFalse(store.isEnabled());
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
        FileSliceStore diskStore = new FileSliceStore(1024 * 1024, typeManager, cacheDirectory);
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);
        delegate.pagesToServe = List.of(mixedPage(42, "cached text"));
        CachingSplit split = new CachingSplit(
                CacheKey.of(new SchemaTableName("schema", "table"), withColumnDomains(Map.of(columnA, Domain.singleValue(BigintType.BIGINT, 1L)))),
                Map.of("a", columnA, "b", columnB),
                Map.of("b", "varchar", "a", "bigint"),
                new ConnectorSplit() {},
                new ConnectorTableHandle() {});

        ConnectorPageSource source = diskProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);
        List<Page> emitted = drain(source);
        assertEquals(1, emitted.size());
        assertEquals(42, BigintType.BIGINT.getLong(emitted.get(0).getBlock(0), 0));

        assertTrue(Files.size(diskStore.fileFor(split.getCacheKey())) > 0);
        List<Page> cached = drain(diskStore.openRead(
                split.getCacheKey(), List.of(0, 1), split.cachedChannelTypes()).orElseThrow());
        assertEquals(1, cached.size());
        assertEquals(42, BigintType.BIGINT.getLong(cached.get(0).getBlock(0), 0));
        assertEquals("cached text", VarcharType.VARCHAR.getSlice(cached.get(0).getBlock(1), 0).toStringUtf8());
        assertEquals(List.of("bigint", "varchar", "bigint", "varchar"), resolvedTypeNames);
    }

    @Test
    void populateStreamsRawPagesAndPublishesOnlyAfterDelegateFinishes(@TempDir Path cacheDirectory) throws IOException {
        delegate.pagesToServe = List.of(bigintPage(10, 20), bigintPage(11, 21));
        FileSliceStore diskStore = new FileSliceStore(1024 * 1024, bigintTypeManager(), cacheDirectory);
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);
        CachingSplit split = typedSplit();
        ConnectorPageSource source = diskProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnB), DynamicFilter.EMPTY);

        SourcePage first = source.getNextSourcePage();
        assertEquals(20, BigintType.BIGINT.getLong(first.getPage().getBlock(0), 0));
        assertFalse(Files.exists(diskStore.fileFor(split.getCacheKey())), "temporary output is not visible as a slice");

        SourcePage second = source.getNextSourcePage();
        assertEquals(21, BigintType.BIGINT.getLong(second.getPage().getBlock(0), 0));
        assertTrue(source.isFinished());
        assertTrue(Files.exists(diskStore.fileFor(split.getCacheKey())));
        source.close();

        List<Page> cached = drain(diskStore.openRead(split.getCacheKey(), List.of(0, 1), split.cachedChannelTypes()).orElseThrow());
        assertEquals(2, cached.size());
        assertEquals(10, BigintType.BIGINT.getLong(cached.get(0).getBlock(0), 0));
        assertEquals(20, BigintType.BIGINT.getLong(cached.get(0).getBlock(1), 0));
        assertEquals(11, BigintType.BIGINT.getLong(cached.get(1).getBlock(0), 0));
        assertEquals(21, BigintType.BIGINT.getLong(cached.get(1).getBlock(1), 0));
    }

    @Test
    void hitServesCachedPagesProjectedToRequestedOrder(@TempDir Path cacheDirectory) throws IOException {
        TypeManager typeManager = bigintTypeManager();
        FileSliceStore diskStore = new FileSliceStore(1024 * 1024, typeManager, cacheDirectory);
        try (FileSliceStore.WriteSession session = diskStore.beginWrite(sliceKey(), List.of("a", "b"), List.of("bigint", "bigint"))) {
            session.write(bigintPage(1, 2));
            session.complete();
        }
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);
        CachingSplit split = typedSplit();

        // Reversed request order must be projected back correctly; delegate is never touched
        ConnectorPageSource source = diskProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnB, columnA), DynamicFilter.EMPTY);

        assertNull(delegate.lastSplit);
        List<Page> pages = drain(source);
        assertEquals(1, pages.size());
        assertEquals(2, pages.get(0).getChannelCount());
        assertEquals(2L, BigintType.BIGINT.getLong(pages.get(0).getBlock(0), 0));
        assertEquals(1L, BigintType.BIGINT.getLong(pages.get(0).getBlock(1), 0));
    }

    @Test
    void hitWithNoRequestedColumnsStillReturnsRowCounts(@TempDir Path cacheDirectory) throws IOException {
        FileSliceStore diskStore = new FileSliceStore(1024 * 1024, bigintTypeManager(), cacheDirectory);
        try (FileSliceStore.WriteSession session = diskStore.beginWrite(
                sliceKey(), List.of("a", "b"), List.of("bigint", "bigint"))) {
            session.write(bigintPage(1, 2));
            session.write(bigintPage(3, 4));
            session.complete();
        }
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);

        List<Page> pages = drain(diskProvider.createPageSource(
                null, null, typedSplit(), new ConnectorTableHandle() {},
                Optional.empty(), List.of(), DynamicFilter.EMPTY));

        assertEquals(2, pages.size());
        assertEquals(0, pages.get(0).getChannelCount());
        assertEquals(1, pages.get(0).getPositionCount());
        assertEquals(1, pages.get(1).getPositionCount());
        assertNull(delegate.lastSplit, "cache hit should not read from the delegate");
    }

    @Test
    void hitWithoutTerminalNullStillPublishesOnClose(@TempDir Path cacheDirectory) throws IOException {
        // Delegate serves one page; consumer stops at isFinished() without polling the null
        delegate.pagesToServe = List.of(bigintPage(1, 2));
        FileSliceStore diskStore = new FileSliceStore(1024 * 1024, bigintTypeManager(), cacheDirectory);
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);
        CachingSplit split = typedSplit();

        ConnectorPageSource source = diskProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);

        SourcePage page = source.getNextSourcePage();
        assertEquals(1L, BigintType.BIGINT.getLong(page.getPage().getBlock(0), 0));
        assertTrue(source.isFinished());
        source.close();

        assertTrue(Files.exists(diskStore.fileFor(sliceKey())));
    }

    @Test
    void closeBeforeFinishDiscardsPartialSlice(@TempDir Path cacheDirectory) throws IOException {
        delegate.pagesToServe = List.of(bigintPage(1, 2), bigintPage(3, 4));
        FileSliceStore diskStore = new FileSliceStore(1024 * 1024, bigintTypeManager(), cacheDirectory);
        CachingPageSourceProvider diskProvider = new CachingPageSourceProvider(delegate, diskStore);
        CachingSplit split = typedSplit();

        ConnectorPageSource source = diskProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);

        source.getNextSourcePage();
        source.close();

        assertFalse(Files.exists(diskStore.fileFor(sliceKey())));
        assertTrue(temporaryFiles(cacheDirectory).isEmpty());
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
    void oversizeSliceNeverPublishes(@TempDir Path cacheDirectory) throws IOException {
        delegate.pagesToServe = List.of(bigintPage(1, 2));
        // Cap smaller than one page: everything is over cap from the first page
        FileSliceStore tinyStore = new FileSliceStore(1, bigintTypeManager(), cacheDirectory);
        CachingPageSourceProvider tinyProvider = new CachingPageSourceProvider(delegate, tinyStore);
        CachingSplit split = typedSplit();

        ConnectorPageSource source = tinyProvider.createPageSource(
                null, null, split, new ConnectorTableHandle() {},
                Optional.empty(), List.of(columnA), DynamicFilter.EMPTY);

        List<Page> emitted = drain(source);
        assertEquals(1, emitted.size());
        assertEquals(1L, BigintType.BIGINT.getLong(emitted.get(0).getBlock(0), 0));
        assertFalse(Files.exists(tinyStore.fileFor(sliceKey())));
        assertTrue(temporaryFiles(cacheDirectory).isEmpty());
    }
}

package com.scent.trino.cache;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetSliceReaderTest {
    private static final int PAGE_ROWS = 8192;
    private static final TypeManager TYPE_MANAGER = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
            TypeManager.class.getClassLoader(),
            new Class<?>[] {TypeManager.class},
            (proxy, method, arguments) -> {
                if (!method.getName().equals("fromSqlType")) {
                    return null;
                }
                return switch ((String) arguments[0]) {
                    case "bigint" -> BigintType.BIGINT;
                    case "varchar" -> VarcharType.VARCHAR;
                    case "boolean" -> BooleanType.BOOLEAN;
                    case "double" -> DoubleType.DOUBLE;
                    default -> throw new IllegalArgumentException("Unexpected type: " + arguments[0]);
                };
            });

    private static CacheKey cacheKey(String tableName) {
        return CacheKey.of(new SchemaTableName("schema", tableName), TupleDomain.all());
    }

    private static Page bigintPage(int positions) {
        BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positions);
        for (int position = 0; position < positions; position++) {
            BigintType.BIGINT.writeLong(builder, position);
        }
        return new Page(builder.build());
    }

    private static Page mixedPage() {
        BlockBuilder bigint = BigintType.BIGINT.createBlockBuilder(null, 1);
        BigintType.BIGINT.writeLong(bigint, 42);
        BlockBuilder varchar = VarcharType.VARCHAR.createBlockBuilder(null, 1);
        VarcharType.VARCHAR.writeSlice(varchar, Slices.utf8Slice("cached"));
        BlockBuilder bool = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
        BooleanType.BOOLEAN.writeBoolean(bool, true);
        BlockBuilder doubleValue = DoubleType.DOUBLE.createBlockBuilder(null, 1);
        DoubleType.DOUBLE.writeDouble(doubleValue, 3.5);
        return new Page(bigint.build(), varchar.build(), bool.build(), doubleValue.build());
    }

    private static List<Page> drain(ConnectorPageSource source) throws IOException {
        List<Page> pages = new ArrayList<>();
        try {
            while (true) {
                var next = source.getNextSourcePage();
                if (next == null) {
                    return pages;
                }
                pages.add(next.getPage());
            }
        }
        finally {
            source.close();
        }
    }

    @Test
    void streamsRowsInChunksOfAtMost8192(@TempDir Path directory) throws IOException {
        Path file = directory.resolve("many-rows.parquet");
        try (ParquetSliceWriter.StreamingWriter writer = ParquetSliceWriter.open(
                file, List.of("n"), List.of("bigint"), TYPE_MANAGER)) {
            writer.write(bigintPage(PAGE_ROWS + 17));
            writer.complete();
        }

        List<Page> pages = drain(ParquetSliceReader.open(file, List.of(0), List.of("bigint"), TYPE_MANAGER));

        long nextExpectedValue = 0;
        int totalRows = 0;
        for (Page page : pages) {
            assertTrue(page.getPositionCount() <= PAGE_ROWS, "emitted page must not exceed 8192 rows");
            for (int position = 0; position < page.getPositionCount(); position++) {
                assertEquals(nextExpectedValue++, BigintType.BIGINT.getLong(page.getBlock(0), position));
            }
            totalRows += page.getPositionCount();
        }
        assertEquals(PAGE_ROWS + 17, totalRows);
    }

    @Test
    void emptyParquetFileReturnsNoPages(@TempDir Path directory) throws IOException {
        Path file = directory.resolve("empty.parquet");
        try (ParquetSliceWriter.StreamingWriter writer = ParquetSliceWriter.open(
                file, List.of("n"), List.of("bigint"), TYPE_MANAGER)) {
            writer.complete();
        }

        assertTrue(drain(ParquetSliceReader.open(file, List.of(0), List.of("bigint"), TYPE_MANAGER)).isEmpty());
    }

    @Test
    void invalidChannelIndexIsRejectedAndFileCanBeClosed(@TempDir Path directory) throws IOException {
        Path file = directory.resolve("one-column.parquet");
        try (ParquetSliceWriter.StreamingWriter writer = ParquetSliceWriter.open(
                file, List.of("n"), List.of("bigint"), TYPE_MANAGER)) {
            writer.write(bigintPage(1));
            writer.complete();
        }

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ParquetSliceReader.open(file, List.of(1), List.of("bigint", "bigint"), TYPE_MANAGER));
    }

    @Test
    void matchingSchemaIsReadFromCache(@TempDir Path directory) throws IOException {
        FileSliceStore store = new FileSliceStore(1024 * 1024, TYPE_MANAGER, directory);
        CacheKey key = cacheKey("matching-schema");
        List<String> columnTypes = List.of("bigint", "varchar", "boolean", "double");
        try (FileSliceStore.WriteSession session = store.beginWrite(
                key, List.of("n", "text", "enabled", "ratio"), columnTypes)) {
            session.write(mixedPage());
            session.complete();
        }

        var cachedPages = store.openRead(key, List.of(0, 1, 2, 3), columnTypes);

        assertTrue(cachedPages.isPresent(), "matching Parquet schema should be a cache hit");
        List<Page> pages = drain(cachedPages.orElseThrow());
        assertEquals(1, pages.size());
        assertEquals(42, BigintType.BIGINT.getLong(pages.get(0).getBlock(0), 0));
        assertEquals("cached", VarcharType.VARCHAR.getSlice(pages.get(0).getBlock(1), 0).toStringUtf8());
        assertTrue(BooleanType.BOOLEAN.getBoolean(pages.get(0).getBlock(2), 0));
        assertEquals(3.5, DoubleType.DOUBLE.getDouble(pages.get(0).getBlock(3), 0));
    }

    @Test
    void mismatchedSchemaInvalidatesFileAndReturnsCacheMiss(@TempDir Path directory) throws IOException {
        FileSliceStore store = new FileSliceStore(1024 * 1024, TYPE_MANAGER, directory);
        CacheKey key = cacheKey("mismatched-schema");
        List<String> storedTypes = List.of("bigint", "varchar", "boolean", "double");
        try (FileSliceStore.WriteSession session = store.beginWrite(
                key, List.of("n", "text", "enabled", "ratio"), storedTypes)) {
            session.write(mixedPage());
            session.complete();
        }
        Path cachedFile = store.fileFor(key);
        assertTrue(Files.exists(cachedFile), "test setup should publish the Parquet slice");

        // The first two and last types match; the stored BOOLEAN field is incorrectly claimed as DOUBLE.
        var cachedPages = store.openRead(key, List.of(0, 1, 2, 3), List.of("bigint", "varchar", "double", "double"));

        assertTrue(cachedPages.isEmpty(), "schema mismatch should be treated as a cache miss");
        assertFalse(Files.exists(cachedFile), "schema-mismatched Parquet file should be invalidated");
    }
}

package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetSliceReaderTest {
    private static final int PAGE_ROWS = 8192;
    private static final TypeManager TYPE_MANAGER = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
            TypeManager.class.getClassLoader(),
            new Class<?>[] {TypeManager.class},
            (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);

    private static Page bigintPage(int positions) {
        BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positions);
        for (int position = 0; position < positions; position++) {
            BigintType.BIGINT.writeLong(builder, position);
        }
        return new Page(builder.build());
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
}

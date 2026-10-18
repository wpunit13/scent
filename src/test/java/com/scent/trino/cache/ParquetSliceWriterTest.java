package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetSliceWriterTest
{
    private static final TypeManager UNUSED_TYPE_MANAGER = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
            TypeManager.class.getClassLoader(),
            new Class<?>[] {TypeManager.class},
            (proxy, method, arguments) -> null);

    @Test
    void requiresTypeManagerForTypedParquetSlices()
    {
        assertThrows(IllegalStateException.class, () -> ParquetSliceWriter.open(
                Path.of("unused", "slice.parquet"),
                List.of(),
                List.of(),
                null));
    }

    @Test
    void rejectsColumnAndTypeCountMismatch()
    {
        assertThrows(IllegalArgumentException.class, () -> ParquetSliceWriter.open(
                Path.of("unused", "slice.parquet"),
                List.of("a"),
                List.of(),
                UNUSED_TYPE_MANAGER));
    }

    @Test
    void rejectsMissingColumnType()
    {
        assertThrows(IllegalArgumentException.class, () -> ParquetSliceWriter.open(
                Path.of("unused", "slice.parquet"),
                List.of("a"),
                Arrays.asList((String) null),
                UNUSED_TYPE_MANAGER));
    }

    @Test
    void rejectsPagesWithDifferentChannelCount(@TempDir Path cacheDirectory) throws IOException
    {
        Page twoChannels = new Page(
                new ByteArrayBlock(1, Optional.empty(), new byte[] {1}),
                new ByteArrayBlock(1, Optional.empty(), new byte[] {2}));

        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);

        try (ParquetSliceWriter.StreamingWriter writer = ParquetSliceWriter.open(
                cacheDirectory.resolve("slice.parquet"), List.of("a"), List.of("bigint"), typeManager)) {
            assertThrows(IllegalArgumentException.class, () -> writer.write(twoChannels));
        }
    }

    @Test
    void writesParquetWithSanitizedUniqueFieldNames(@TempDir Path cacheDirectory) throws IOException
    {
        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);

        try (ParquetSliceWriter.StreamingWriter writer = ParquetSliceWriter.open(
                cacheDirectory.resolve("slice.parquet"),
                List.of("sales-region", "sales region", "2nd"),
                List.of("bigint", "bigint", "bigint"),
                typeManager)) {
            writer.complete();
        }

        assertTrue(Files.size(cacheDirectory.resolve("slice.parquet")) > 0);
    }

    @Test
    void verifyReadWriteJsonAsVarcharThroughParquet(@TempDir Path cacheDirectory) throws IOException
    {
        List<String> jsonValues = List.of("{\"k\":1}", "[1,2]");
        Type jsonType = (Type) java.lang.reflect.Proxy.newProxyInstance(
                Type.class.getClassLoader(),
                new Class<?>[] {Type.class},
                (proxy, method, arguments) -> method.getName().equals("getBaseName") ? "json" : null);
        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("fromSqlType")) {
                        return arguments[0].equals("json") ? jsonType : VarcharType.VARCHAR;
                    }
                    return null;
                });

        BlockBuilder jsonBuilder = VarcharType.VARCHAR.createBlockBuilder(null, jsonValues.size());
        jsonValues.forEach(value -> VarcharType.VARCHAR.writeSlice(jsonBuilder, utf8Slice(value)));
        Path file = cacheDirectory.resolve("json.parquet");
        try (ParquetSliceWriter.StreamingWriter writer = ParquetSliceWriter.open(
                file, List.of("payload"), List.of("json"), typeManager)) {
            writer.write(new Page(jsonBuilder.build()));
            writer.complete();
        }

        List<Page> pages = new java.util.ArrayList<>();
        try (var source = ParquetSliceReader.open(file, List.of(0), List.of("json"), typeManager)) {
            while (true) {
                SourcePage sourcePage = source.getNextSourcePage();
                if (sourcePage == null) {
                    break;
                }
                pages.add(sourcePage.getPage());
            }
        }

        int valueIndex = 0;
        for (Page page : pages) {
            var block = page.getBlock(0);
            for (int position = 0; position < block.getPositionCount(); position++) {
                assertTrue(valueIndex < jsonValues.size(), "reader returned more JSON values than were written");
                assertEquals(jsonValues.get(valueIndex++), VarcharType.VARCHAR.getSlice(block, position).toStringUtf8());
            }
        }
        assertEquals(jsonValues.size(), valueIndex, "reader returned all JSON values");
    }
}

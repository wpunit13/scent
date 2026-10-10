package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
        assertThrows(IllegalStateException.class, () -> ParquetSliceWriter.write(
                "slice",
                List.of(),
                List.of(),
                List.of(),
                null,
                Path.of("unused")));
    }

    @Test
    void rejectsColumnAndTypeCountMismatch()
    {
        assertThrows(IllegalArgumentException.class, () -> ParquetSliceWriter.write(
                "slice",
                List.of(),
                List.of("a"),
                List.of(),
                UNUSED_TYPE_MANAGER,
                Path.of("unused")));
    }

    @Test
    void rejectsMissingColumnType()
    {
        assertThrows(IllegalArgumentException.class, () -> ParquetSliceWriter.write(
                "slice",
                List.of(),
                List.of("a"),
                Arrays.asList((String) null),
                UNUSED_TYPE_MANAGER,
                Path.of("unused")));
    }

    @Test
    void rejectsPagesWithDifferentChannelCount()
    {
        Page twoChannels = new Page(
                new ByteArrayBlock(1, Optional.empty(), new byte[] {1}),
                new ByteArrayBlock(1, Optional.empty(), new byte[] {2}));

        assertThrows(IllegalArgumentException.class, () -> ParquetSliceWriter.write(
                "slice",
                List.of(twoChannels),
                List.of("a"),
                List.of("bigint"),
                UNUSED_TYPE_MANAGER,
                Path.of("unused")));
    }

    @Test
    void writesParquetWithSanitizedUniqueFieldNames(@TempDir Path cacheDirectory) throws IOException
    {
        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);

        ParquetSliceWriter.write(
                "slice",
                List.of(),
                List.of("sales-region", "sales region", "2nd"),
                List.of("bigint", "bigint", "bigint"),
                typeManager,
                cacheDirectory);

        assertTrue(Files.size(cacheDirectory.resolve("slice.parquet")) > 0);
    }
}

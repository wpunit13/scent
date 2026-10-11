package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetSliceReaderTest {

    private static final List<Type> TYPES = List.of(
            BooleanType.BOOLEAN,
            IntegerType.INTEGER,
            BigintType.BIGINT,
            RealType.REAL,
            DoubleType.DOUBLE,
            VarcharType.VARCHAR,
            DateType.DATE,
            TimestampType.TIMESTAMP_MICROS,
            VarbinaryType.VARBINARY);
    private static final List<String> NAMES = List.of("b", "i", "l", "r", "d", "v", "dt", "ts", "vb");

    private static final TypeManager TYPE_MANAGER = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
            TypeManager.class.getClassLoader(),
            new Class<?>[] {TypeManager.class},
            (proxy, method, arguments) -> {
                if (!method.getName().equals("fromSqlType")) {
                    return null;
                }
                return switch ((String) arguments[0]) {
                    case "boolean" -> BooleanType.BOOLEAN;
                    case "integer" -> IntegerType.INTEGER;
                    case "bigint" -> BigintType.BIGINT;
                    case "real" -> RealType.REAL;
                    case "double" -> DoubleType.DOUBLE;
                    case "varchar" -> VarcharType.VARCHAR;
                    case "date" -> DateType.DATE;
                    case "timestamp" -> TimestampType.TIMESTAMP_MICROS;
                    case "varbinary" -> VarbinaryType.VARBINARY;
                    default -> null;
                };
            });

    /** Builds a page where position 2 is null in every column and other positions hold per-type values. */
    private static Page sample(int positions)
    {
        BlockBuilder[] builders = new BlockBuilder[TYPES.size()];
        for (int channel = 0; channel < TYPES.size(); channel++) {
            builders[channel] = TYPES.get(channel).createBlockBuilder(null, positions);
        }
        for (int position = 0; position < positions; position++) {
            for (int channel = 0; channel < TYPES.size(); channel++) {
                BlockBuilder builder = builders[channel];
                if (position % 3 == 2) {
                    builder.appendNull();
                }
                else {
                    append(builder, TYPES.get(channel), position);
                }
            }
        }
        Block[] blocks = new Block[TYPES.size()];
        for (int channel = 0; channel < TYPES.size(); channel++) {
            blocks[channel] = builders[channel].build();
        }
        return new Page(blocks);
    }

    private static void append(BlockBuilder builder, Type type, int position)
    {
        switch (type.getBaseName()) {
            case "boolean" -> type.writeBoolean(builder, position % 2 == 0);
            case "integer" -> type.writeLong(builder, position);
            case "bigint" -> type.writeLong(builder, position * 1_000_000L);
            case "real" -> ((RealType) type).writeFloat(builder, position * 1.5f);
            case "double" -> type.writeDouble(builder, position * 2.5);
            case "varchar" -> type.writeSlice(builder, io.airlift.slice.Slices.utf8Slice("row-" + position));
            case "date" -> type.writeLong(builder, position);
            case "timestamp" -> type.writeLong(builder, position * 1_000L);
            case "varbinary" -> type.writeSlice(builder, io.airlift.slice.Slices.utf8Slice("bin-" + position));
            default -> throw new IllegalStateException("unexpected type " + type.getDisplayName());
        }
    }

    @Test
    void roundTripPreservesRowsAcrossAllSupportedTypes(@TempDir Path cacheDirectory) throws IOException
    {
        Page original = sample(25);
        ParquetSliceWriter.write(cacheDirectory.resolve("roundtrip.parquet"), List.of(original), NAMES,
                TYPES.stream().map(Type::getBaseName).toList(), TYPE_MANAGER);

        List<Page> read = ParquetSliceReader.read(
                cacheDirectory.resolve("roundtrip.parquet"), TYPE_MANAGER);

        assertEquals(1, read.size());
        Page back = read.get(0);
        assertEquals(original.getPositionCount(), back.getPositionCount());
        assertEquals(TYPES.size(), back.getChannelCount());

        for (int channel = 0; channel < TYPES.size(); channel++) {
            Type type = TYPES.get(channel);
            Block originalBlock = original.getBlock(channel);
            Block readBlock = back.getBlock(channel);
            for (int position = 0; position < original.getPositionCount(); position++) {
                assertEquals(type.getObjectValue(originalBlock, position),
                        type.getObjectValue(readBlock, position),
                        "channel " + channel + " (" + type.getBaseName() + ") position " + position);
            }
        }
    }

    @Test
    void emptyFileReadsAsNoPages(@TempDir Path cacheDirectory) throws IOException
    {
        ParquetSliceWriter.write(cacheDirectory.resolve("empty.parquet"), List.of(), NAMES,
                TYPES.stream().map(Type::getBaseName).toList(), TYPE_MANAGER);

        assertTrue(Files.size(cacheDirectory.resolve("empty.parquet")) > 0);
        assertEquals(List.of(), ParquetSliceReader.read(cacheDirectory.resolve("empty.parquet"), TYPE_MANAGER));
    }
}

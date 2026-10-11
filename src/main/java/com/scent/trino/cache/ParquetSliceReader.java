package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.airlift.slice.Slices;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Inverse of {@link ParquetSliceWriter}: reads a slice's Parquet file back into pages in the
 * cached layout order. The file is self-describing: the writer embedded each column's Trino
 * type name as a schema prop, so the reader needs no type information from the caller.
 */
final class ParquetSliceReader {
    /** Rows per rebuilt page; keeps a large slice from coming back as one giant page. */
    private static final int PAGE_ROWS = 8192;

    private ParquetSliceReader() {}

    static List<Page> read(Path file, TypeManager typeManager) throws IOException {
        Configuration configuration = new Configuration(false);
        configuration.setClassLoader(ParquetSliceReader.class.getClassLoader());
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");

        List<Page> pages = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                new org.apache.hadoop.fs.Path(file.toUri())).withConf(configuration).build()) {

            GenericRecord record = reader.read();
            if (record == null) {
                return pages;
            }

            Schema schema = record.getSchema();
            int channels = schema.getFields().size();
            List<Type> types = new ArrayList<>(channels);
            for (Schema.Field field : schema.getFields()) {
                types.add(typeManager.fromSqlType(field.getProp("trino.type")));
            }

            BlockBuilder[] builders = builders(types, PAGE_ROWS);
            int positions = 0;
            do {
                for (int channel = 0; channel < channels; channel++) {
                    append(builders[channel], types.get(channel), record.get(channel));
                }
                if (++positions == PAGE_ROWS) {
                    pages.add(page(builders));
                    builders = builders(types, PAGE_ROWS);
                    positions = 0;
                }
            }
            while ((record = reader.read()) != null);
            if (positions > 0) {
                pages.add(page(builders));
            }
        }
        return pages;
    }

    private static BlockBuilder[] builders(List<Type> types, int expectedRows)
    {
        BlockBuilder[] builders = new BlockBuilder[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            builders[channel] = types.get(channel).createBlockBuilder(null, expectedRows);
        }
        return builders;
    }

    private static Page page(BlockBuilder[] builders)
    {
        Block[] blocks = new Block[builders.length];
        for (int channel = 0; channel < builders.length; channel++) {
            blocks[channel] = builders[channel].build();
        }
        return new Page(blocks);
    }

    private static void append(BlockBuilder builder, Type type, Object value)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }
        switch (type.getBaseName()) {
            case "boolean" -> type.writeBoolean(builder, (Boolean) value);

            case "tinyint", "smallint", "integer" -> type.writeLong(builder, ((Number) value).longValue());

            case "date" -> type.writeLong(builder, days(value));

            case "bigint" -> type.writeLong(builder, ((Number) value).longValue());

            case "time" -> type.writeLong(builder, microsOfDay(value));

            case "timestamp" -> type.writeLong(builder, epochMicros(value));

            case "real" -> ((RealType) type).writeFloat(builder, (Float) value);

            case "double" -> type.writeDouble(builder, (Double) value);

            case "varchar", "char", "json" -> type.writeSlice(builder, Slices.utf8Slice(value.toString()));

            case "varbinary" -> type.writeSlice(builder, Slices.wrappedBuffer(bytes((ByteBuffer) value)));

            case "decimal" -> {
                if (!(type instanceof DecimalType decimalType)) {
                    throw new IllegalArgumentException("Unsupported Trino type: " + type.getDisplayName());
                }
                if (!decimalType.isShort()) {
                    throw new IllegalArgumentException("Long decimal not yet supported: " + type.getDisplayName());
                }
                type.writeObject(builder, decimal(value, decimalType));
            }

            default -> throw new IllegalArgumentException("Unsupported Trino type: " + type.getDisplayName());
        }
    }

    /** Avro's GenericData converts logical types on read; the writer stored raw Trino longs. */
    private static long days(Object value)
    {
        if (value instanceof LocalDate date) {
            return date.toEpochDay();
        }
        return ((Number) value).longValue();
    }

    private static long epochMicros(Object value)
    {
        if (value instanceof Instant instant) {
            return Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1000;
        }
        return ((Number) value).longValue();
    }

    private static long microsOfDay(Object value)
    {
        if (value instanceof LocalTime time) {
            return time.toNanoOfDay() / 1000;
        }
        return ((Number) value).longValue();
    }

    private static BigDecimal decimal(Object value, DecimalType type)
    {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        return new BigDecimal(new BigInteger(bytes((ByteBuffer) value)), type.getScale());
    }

    /** Exact copy of the buffer's remaining bytes as an array. */
    private static byte[] bytes(ByteBuffer buffer)
    {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}

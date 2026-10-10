package com.scent.trino.cache;

import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.Conversions;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

final class TrinoParquetValueConverter {
    private TrinoParquetValueConverter() {}

    static Object convert(Type type, Schema schema, Block block, int position) {
        if (type instanceof DecimalType) {
            return decimal(type, schema, block, position);
        }

        return switch (type.getBaseName()) {
            case "boolean" -> type.getBoolean(block, position);

            case "tinyint",
                 "smallint",
                 "integer",
                 "date" -> Math.toIntExact(type.getLong(block, position));

            case "bigint" -> type.getLong(block, position);

            case "real" -> Float.intBitsToFloat(Math.toIntExact(type.getLong(block, position)));

            case "double" -> type.getDouble(block, position);

            case "time",
                 "timestamp" -> type.getLong(block, position);

            case "varchar",
                 "char",
                 "json" -> type.getSlice(block, position).toStringUtf8();

            case "varbinary" -> ByteBuffer.wrap(type.getSlice(block, position).getBytes());

            default -> throw unsupported(type);
        };
    }

    private static Object decimal(Type type, Schema schema, Block block, int position) {
        if (!(type instanceof DecimalType decimalType)) {
            throw unsupported(type);
        }

        if (!decimalType.isShort()) {
            throw new IllegalArgumentException("Long decimal not yet supported: "
                            + type.getDisplayName());
        }

        BigDecimal value = (BigDecimal) type.getObjectValue(block, position);

        return new Conversions.DecimalConversion()
                .toBytes(value,
                        schema,
                        schema.getLogicalType());
    }

    private static IllegalArgumentException unsupported(Type type) {
        return new IllegalArgumentException("Unsupported Trino type: " + type.getDisplayName());
    }
}

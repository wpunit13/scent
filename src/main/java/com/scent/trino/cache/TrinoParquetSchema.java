package com.scent.trino.cache;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

final class TrinoParquetSchema {
    private TrinoParquetSchema() {}

    static Schema create(List<String> columnNames, List<String> fieldNames, List<String> typeNames, List<Type> types) {
        Schema record = Schema.createRecord("ScentSlice", null, "com.scent.cache", false);
        List<Schema.Field> fields = new ArrayList<>();

        for (int i = 0; i < types.size(); i++) {
            Schema valueSchema = valueSchema(types.get(i));
            Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), valueSchema);
            Schema.Field field = new Schema.Field(fieldNames.get(i),
                            nullable,
                            null,
                            org.apache.avro.JsonProperties.NULL_VALUE);

            field.addProp("trino.column.name", columnNames.get(i));
            field.addProp("trino.type", typeNames.get(i));
            fields.add(field);
        }

        record.setFields(fields);
        return record;
    }

    private static Schema valueSchema(Type type) {
        if (type instanceof DecimalType decimalType) {
            return LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                    .addToSchema(Schema.create(Schema.Type.BYTES));
        }

        return switch (type.getBaseName()) {
            case "boolean" -> Schema.create(Schema.Type.BOOLEAN);

            case "tinyint",
                 "smallint",
                 "integer" -> Schema.create(Schema.Type.INT);

            case "bigint" -> Schema.create(Schema.Type.LONG);

            case "real" -> Schema.create(Schema.Type.FLOAT);

            case "double" -> Schema.create(Schema.Type.DOUBLE);

            case "varchar",
                 "char",
                 "json" -> Schema.create(Schema.Type.STRING);

            case "varbinary" -> Schema.create(Schema.Type.BYTES);

            case "date" -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

            case "time" -> LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));

            case "timestamp" -> LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

            default -> throw new IllegalArgumentException("Unsupported Trino type: " + type.getDisplayName());
        };
    }
}

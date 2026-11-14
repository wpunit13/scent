package com.scent.trino.cache;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.Map;

/** Minimal TypeManager for tests that rebuild schemas from note type strings. */
public enum FixedTypeManager implements TypeManager {
    INSTANCE;

    private static final Map<String, Type> TYPES = Map.of(
            "bigint", BigintType.BIGINT,
            "varchar", VarcharType.VARCHAR);

    @Override
    public Type getType(io.trino.spi.type.TypeDescriptor descriptor) {
        return fromSqlType(descriptor.getBase());
    }

    @Override
    public Type fromSqlType(String sqlType) {
        Type type = TYPES.get(sqlType);
        if (type == null) {
            throw new IllegalArgumentException("unknown type " + sqlType);
        }
        return type;
    }

    @Override
    public Type getType(io.trino.spi.type.TypeId typeId) {
        return fromSqlType(typeId.getId());
    }

    @Override
    public boolean isTypeRegistered(String sqlType) {
        return TYPES.containsKey(sqlType);
    }

    @Override
    public TypeOperators getTypeOperators() {
        throw new UnsupportedOperationException();
    }
}

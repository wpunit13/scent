package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CacheKeyTest {
    private final ColumnHandle column = new ColumnHandle() {};

    private TupleDomain<ColumnHandle> partition()
    {
        return withColumnDomains(Map.of(column, Domain.singleValue(BigintType.BIGINT, 1L)));
    }

    @Test
    void equalForSameTablePartitionAndVersion()
    {
        CacheKey a = CacheKey.of(new SchemaTableName("schema", "table"), partition());
        CacheKey b = CacheKey.of(new SchemaTableName("schema", "table"), partition());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void differentVersionRotatesTheKey()
    {
        CacheKey none = CacheKey.of(new SchemaTableName("schema", "table"), partition());
        CacheKey versioned = new CacheKey(new SchemaTableName("schema", "table"), partition(), "2024-03-15");
        assertNotEquals(none, versioned);
    }

    @Test
    void defaultsVersionToNone()
    {
        assertEquals("none", CacheKey.of(new SchemaTableName("schema", "table"), partition()).getVersion());
    }
}

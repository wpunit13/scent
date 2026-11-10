package com.scent.trino.cache;

import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheKeyTest {
    private TupleDomain<String> partition()
    {
        return withColumnDomains(Map.of("region", Domain.singleValue(BigintType.BIGINT, 1L)));
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

    @Test
    void canonicalPartitionSortsColumnsForStableLayout()
    {
        Domain domainA = Domain.singleValue(BigintType.BIGINT, 1L);
        Domain domainB = Domain.singleValue(BigintType.BIGINT, 2L);

        CacheKey aFirst = new CacheKey(new SchemaTableName("schema", "table"), withColumnDomains(Map.of("col-a", domainA, "col-b", domainB)), null);
        CacheKey bFirst = new CacheKey(new SchemaTableName("schema", "table"), withColumnDomains(new LinkedHashMap<>(Map.of("col-b", domainB, "col-a", domainA))), null);

        assertEquals(aFirst.canonicalPartition(), bFirst.canonicalPartition());
        assertTrue(aFirst.canonicalPartition().startsWith("{col-a="));
        assertTrue(aFirst.canonicalPartition().contains("col-b="));
    }
}

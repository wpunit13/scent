package com.scent.trino.cache;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingConnectorFactoryTest {

    @Test
    void parsesPerTableJsonArrayConfig() {
        Map<String, List<String>> parsed = CachingConnectorFactory.parsePartitionColumns(Map.of(
                "cache.partition-column.public.metrics", "[\"region\"]",
                "cache.partition-column.public.sales", "[\"day\", \"region\"]"));

        assertEquals(List.of("region"), parsed.get("public.metrics"));
        assertEquals(List.of("day", "region"), parsed.get("public.sales"));
        assertEquals(2, parsed.size());
    }

    @Test
    void noEntriesMeansNothingIsCached() {
        assertTrue(CachingConnectorFactory.parsePartitionColumns(Map.of(
                "delegate.connector.name", "postgresql",
                "cache.root", "/tmp/scent-cache")).isEmpty());
    }

    @Test
    void rejectsMalformedJson() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parsePartitionColumns(Map.of(
                        "cache.partition-column.public.metrics", "region")));
        assertTrue(e.getMessage().contains("JSON array"));
    }

    @Test
    void rejectsEmptyArray() {
        assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parsePartitionColumns(Map.of(
                        "cache.partition-column.public.metrics", "[]")));
    }

    @Test
    void rejectsBlankColumnName() {
        assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parsePartitionColumns(Map.of(
                        "cache.partition-column.public.metrics", "[\"region\", \" \"]")));
    }

    @Test
    void rejectsEmptyTableName() {
        assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parsePartitionColumns(Map.of(
                        "cache.partition-column.", "[\"region\"]")));
    }
}

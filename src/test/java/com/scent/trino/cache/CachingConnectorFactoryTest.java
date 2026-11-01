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
    void rejectsEmptyPartitionColumnTableName() {
        assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parsePartitionColumns(Map.of(
                        "cache.partition-column.", "[\"region\"]")));
    }

    @Test
    void parsesPerTableRefreshWindows() {
        Map<String, io.airlift.units.Duration> parsed = CachingConnectorFactory.parseRefresh(Map.of(
                "cache.refresh.public.metrics", "2h",
                "cache.refresh.public.sales", "30s"));

        assertEquals(2, parsed.size());
        assertEquals(7200_000.0, parsed.get("public.metrics").getValue(java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(30_000.0, parsed.get("public.sales").getValue(java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void noRefreshEntriesMeansNothingExpiresByTime() {
        assertTrue(CachingConnectorFactory.parseRefresh(Map.of(
                "delegate.connector.name", "postgresql",
                "cache.root", "/tmp/scent-cache")).isEmpty());
    }

    @Test
    void rejectsMalformedRefreshDuration() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parseRefresh(Map.of(
                        "cache.refresh.public.metrics", "2 fortnights")));
        assertTrue(e.getMessage().contains("must be a duration"));
    }

    @Test
    void rejectsRefreshKeyWithoutTableName() {
        assertThrows(IllegalArgumentException.class,
                () -> CachingConnectorFactory.parseRefresh(Map.of("cache.refresh.", "2h")));
    }
}

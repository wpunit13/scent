package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableMetadataCacheTest {

    private final TableMetadataCache cache = new TableMetadataCache();
    private final SchemaTableName table = new SchemaTableName("schema", "table");
    private final ConnectorTableHandle handle = new ConnectorTableHandle() {};
    private final ColumnHandle column = new ColumnHandle() {};

    @Test
    void unknownTableIsAbsent() {
        assertFalse(cache.contains(table));
        assertEquals(Optional.empty(), cache.getHandle(table));
        assertEquals(Optional.empty(), cache.getColumns(table));
        assertEquals(Optional.empty(), cache.getColumnTypes(table));
    }

    @Test
    void handlePutAloneKeepsPreviouslyLearnedColumns() {
        cache.put(table, null, Map.of("region", column), Map.of("region", "varchar"), true);
        cache.put(table, handle, null, null, true);

        assertTrue(cache.contains(table));
        assertEquals(Optional.of(handle), cache.getHandle(table));
        assertEquals(Optional.of(Map.of("region", column)), cache.getColumns(table));
        assertEquals(Optional.of(Map.of("region", "varchar")), cache.getColumnTypes(table));
    }

    @Test
    void columnPutAloneKeepsPreviouslyLearnedHandle() {
        cache.put(table, handle, null, null, true);
        cache.put(table, null, Map.of("region", column), Map.of("region", "varchar"), true);

        assertEquals(Optional.of(handle), cache.getHandle(table));
        assertEquals(Optional.of(Map.of("region", column)), cache.getColumns(table));
    }

    @Test
    void differentTablesAreIndependent() {
        cache.put(table, handle, null, null, true);
        SchemaTableName other = new SchemaTableName("other", "table");

        assertFalse(cache.contains(other));
        assertTrue(cache.contains(table));
    }
}

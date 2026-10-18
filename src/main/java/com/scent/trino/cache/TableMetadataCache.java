package com.scent.trino.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connector-lifetime metadata fallback (S7): the delegate handle, column layout, and column
 * types of every table this connector has served at least once, keyed by table name. Planning
 * consults it when the source is unreachable; every successful delegate call refreshes the
 * entry, so the cache is a fallback, never a freeze.
 */
public final class TableMetadataCache {

    private record Entry(
            ConnectorTableHandle handle,
            Map<String, ColumnHandle> columns,
            Map<String, String> columnTypes,
            ConnectorTableSchema schema,
            long refreshedAtNanos) {}

    private final Map<String, Entry> byTable = new ConcurrentHashMap<>();

    /** The delegate's rewritten handle per pushed partition domain; replayed when the source cannot answer applyFilter. */
    private final Map<String, Map<TupleDomain<ColumnHandle>, ConnectorTableHandle>> filteredHandlesByTable = new ConcurrentHashMap<>();

    /**
     * Null handle, columns, columnTypes, or schema keep the previously learned value for that
     * field; the refresh timestamp always moves. getColumnHandles passes all three, getTableHandle
     * only the handle. The timestamp is monotonic; a wall-clock staleness window arrives with
     * Phase 5 freshness. Returns true when a stored VALUE changed (new entry or a differing
     * field), which is the signal to persist the table note.
     */
    public boolean put(SchemaTableName table, ConnectorTableHandle handle, Map<String, ColumnHandle> columns, Map<String, String> columnTypes) {
        return putEntry(table, new Entry(handle, copy(columns), copy(columnTypes), null, System.nanoTime()));
    }

    public boolean putSchema(SchemaTableName table, ConnectorTableSchema schema) {
        return putEntry(table, new Entry(null, null, null, Objects.requireNonNull(schema, "schema is null"), System.nanoTime()));
    }

    private boolean putEntry(SchemaTableName table, Entry update) {
        boolean[] changed = {false};
        byTable.compute(
                Objects.requireNonNull(table, "table is null").toString(),
                (key, existing) -> {
                    Entry merged = existing == null ? update : merge(existing, update);
                    changed[0] = existing == null || valueChanged(existing, merged);
                    return merged;
                });
        return changed[0];
    }

    private static boolean valueChanged(Entry before, Entry after) {
        return !Objects.equals(before.handle, after.handle)
                || !Objects.equals(before.columns, after.columns)
                || !Objects.equals(before.columnTypes, after.columnTypes)
                || !schemaEquals(before.schema, after.schema);
    }

    /** ConnectorTableSchema has no equals; its identity is the table plus its columns. */
    private static boolean schemaEquals(ConnectorTableSchema before, ConnectorTableSchema after) {
        if (before == after) {
            return true;
        }
        if (before == null || after == null) {
            return false;
        }
        return before.getTable().equals(after.getTable()) && before.getColumns().equals(after.getColumns());
    }

    private static Entry merge(Entry existing, Entry update) {
        return new Entry(
                update.handle != null ? update.handle : existing.handle,
                update.columns != null ? update.columns : existing.columns,
                update.columnTypes != null ? update.columnTypes : existing.columnTypes,
                update.schema != null ? update.schema : existing.schema,
                update.refreshedAtNanos);
    }

    public Optional<ConnectorTableHandle> getHandle(SchemaTableName table) {
        return entryFor(table).map(Entry::handle);
    }

    public Optional<Map<String, ColumnHandle>> getColumns(SchemaTableName table) {
        return entryFor(table).map(Entry::columns);
    }

    public Optional<Map<String, String>> getColumnTypes(SchemaTableName table) {
        return entryFor(table).map(Entry::columnTypes);
    }

    public Optional<ConnectorTableSchema> getSchema(SchemaTableName table) {
        return entryFor(table).map(Entry::schema);
    }

    public boolean contains(SchemaTableName table) {
        return byTable.containsKey(table.toString());
    }

    /** True when the handle for this domain is new or changed; the signal to persist the slice note. */
    public boolean putFilteredHandle(SchemaTableName table, TupleDomain<ColumnHandle> domain, ConnectorTableHandle handle) {
        Objects.requireNonNull(table, "table is null");
        Objects.requireNonNull(domain, "domain is null");
        Objects.requireNonNull(handle, "handle is null");
        Map<TupleDomain<ColumnHandle>, ConnectorTableHandle> byDomain =
                filteredHandlesByTable.computeIfAbsent(table.toString(), key -> new ConcurrentHashMap<>());
        ConnectorTableHandle previous = byDomain.put(domain, handle);
        return previous == null || !previous.equals(handle);
    }

    public Optional<ConnectorTableHandle> getFilteredHandle(SchemaTableName table, TupleDomain<ColumnHandle> domain) {
        Map<TupleDomain<ColumnHandle>, ConnectorTableHandle> byDomain = filteredHandlesByTable.get(table.toString());
        return byDomain == null ? Optional.empty() : Optional.ofNullable(byDomain.get(domain));
    }

    private Optional<Entry> entryFor(SchemaTableName table) {
        return Optional.ofNullable(byTable.get(table.toString()));
    }

    private static <T> Map<String, T> copy(Map<String, T> map) {
        return map == null ? null : Map.copyOf(map);
    }
}

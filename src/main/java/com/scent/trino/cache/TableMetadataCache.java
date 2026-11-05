package com.scent.trino.cache;

import io.airlift.units.Duration;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Connector-lifetime planning metadata for every table this connector has served, keyed by table
 * name. Cache-first consuming paths read it when the entry is younger than the table's refresh
 * window (T9) or, after a restart, from the planning notes that mirror it; consumers also read the
 * remembered filtered handles and their trust flags. Every successful delegate call refreshes the
 * entry, so it is a reflection of the source's last answers, never a freeze.
 */
public final class TableMetadataCache {

    private record Entry(
            ConnectorTableHandle handle,
            Map<String, ColumnHandle> columns,
            Map<String, String> columnTypes,
            ConnectorTableSchema schema,
            boolean viaDelegate,
            long refreshedAtNanos) {}

    private final Map<String, Entry> byTable = new ConcurrentHashMap<>();

    /** The delegate's rewritten handle per pushed partition domain; replayed when the source cannot answer applyFilter. */
    private final Map<String, Map<TupleDomain<ColumnHandle>, RememberedFiltered>> filteredHandlesByTable = new ConcurrentHashMap<>();

    /**
     * Null handle, columns, columnTypes, or schema keep the previously learned value for that
     * field; the refresh timestamp always moves. getColumnHandles passes all three, getTableHandle
     * only the handle. viaDelegate marks an entry learned straight from the delegate, which makes
     * the delegate provably reachable in that cycle. Returns true when a stored VALUE changed (new
     * entry or a differing field), which is the signal to persist the table note.
     */
    public boolean put(SchemaTableName table, ConnectorTableHandle handle, Map<String, ColumnHandle> columns, Map<String, String> columnTypes, boolean viaDelegate) {
        return putEntry(table, new Entry(handle, copy(columns), copy(columnTypes), null, viaDelegate, System.nanoTime()));
    }

    public boolean putSchema(SchemaTableName table, ConnectorTableSchema schema, boolean viaDelegate) {
        return putEntry(table, new Entry(null, null, null, Objects.requireNonNull(schema, "schema is null"), viaDelegate, System.nanoTime()));
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

    private static Entry merge(Entry existing, Entry update) {
        return new Entry(
                update.handle != null ? update.handle : existing.handle,
                update.columns != null ? update.columns : existing.columns,
                update.columnTypes != null ? update.columnTypes : existing.columnTypes,
                update.schema != null ? update.schema : existing.schema,
                update.viaDelegate,
                update.refreshedAtNanos);
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

    /** True when the entry was learned or refreshed more recently than the table's refresh window. */
    public boolean isFresh(SchemaTableName table, Duration refresh) {
        Entry entry = byTable.get(table.toString());
        return entry != null
                && System.nanoTime() - entry.refreshedAtNanos() < refresh.getValue(TimeUnit.MILLISECONDS) * 1_000_000L;
    }

    /** True when the current entry was learned straight from the delegate (the delegate provably answered this cycle). */
    public boolean refreshedViaDelegate(SchemaTableName table) {
        Entry entry = byTable.get(table.toString());
        return entry != null && entry.viaDelegate();
    }

    /** True when the handle for this domain is new or changed; the signal to persist the slice note. The trusted flag is provenance for the consumers: CachingMetadata serves only trusted remembered handles without delegate contact, and CachingPageSourceProvider refuses repopulate reads with an untrusted one (its delegate-side predicate was lost when the note rebuilt it). */
    public boolean putFilteredHandle(SchemaTableName table, TupleDomain<ColumnHandle> domain, ConnectorTableHandle handle, boolean trusted) {
        Objects.requireNonNull(table, "table is null");
        Objects.requireNonNull(domain, "domain is null");
        Objects.requireNonNull(handle, "handle is null");
        Map<TupleDomain<ColumnHandle>, RememberedFiltered> byDomain =
                filteredHandlesByTable.computeIfAbsent(table.toString(), key -> new ConcurrentHashMap<>());
        RememberedFiltered previous = byDomain.put(domain, new RememberedFiltered(handle, trusted));
        return previous == null || !previous.handle().equals(handle);
    }

    public Optional<RememberedFiltered> getFilteredHandle(SchemaTableName table, TupleDomain<ColumnHandle> domain) {
        Map<TupleDomain<ColumnHandle>, RememberedFiltered> byDomain = filteredHandlesByTable.get(table.toString());
        return byDomain == null ? Optional.empty() : Optional.ofNullable(byDomain.get(domain));
    }

    /** A remembered filtered handle plus whether it is known to come from the delegate itself. */
    public record RememberedFiltered(ConnectorTableHandle handle, boolean trusted) {}

    private Optional<Entry> entryFor(SchemaTableName table) {
        return Optional.ofNullable(byTable.get(table.toString()));
    }

    private static <T> Map<String, T> copy(Map<String, T> map) {
        return map == null ? null : Map.copyOf(map);
    }
}

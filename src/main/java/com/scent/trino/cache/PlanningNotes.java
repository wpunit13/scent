package com.scent.trino.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * T17: lazy file sidecars for the planning metadata held in {@link TableMetadataCache}. A table
 * note (delegate handle, columns, types, schema shape) lives inside the table's cache directory;
 * a slice note (the delegate's filtered handle for that partition domain) lives beside the
 * slice's Parquet file, so slice eviction removes them together. Notes are written on change and
 * read on in-memory miss at the outage fallback points only, so boot cost is unchanged. A failed
 * write is logged and swallowed; an unreadable note reads as absent, degrading the fallback to
 * in-memory only.
 *
 * <p>Delegate objects are stored as {type, json} envelopes and read back through their own
 * class, the way the engine serializes them for the wire. A globally polymorphic mapper would
 * misread their delegating creators, which the spike proved with a real JdbcTableHandle.
 */
@Slf4j
public final class PlanningNotes {

    public record SchemaColumn(String name, String typeId, boolean hidden) {}

    /** Envelope for a delegate-owned object: its concrete class plus its own JSON. */
    public record HandleRef(@JsonProperty("type") String type, @JsonProperty("json") String json) {}

    /** Notes are files on disk; reflection/serialization is restricted to the classes these prefixes name. */
    private static final List<String> ALLOWED_NAMES = List.of(
            "com.scent.trino.cache.", "io.trino.plugin.jdbc.", "io.trino.plugin.bigquery.");

    private record TableNoteJson(
            HandleRef handle,
            Map<String, HandleRef> columns,
            Map<String, String> columnTypes,
            List<SchemaColumn> schema,
            long refreshedAtEpochMillis) {}

    private record SliceNoteJson(
            HandleRef filteredHandle,
            long refreshedAtEpochMillis) {}

    public record TableNote(
            ConnectorTableHandle handle,
            Map<String, ColumnHandle> columns,
            Map<String, String> columnTypes,
            List<SchemaColumn> schema) {}

    public record SliceNote(ConnectorTableHandle filteredHandle) {}

    /** Everything the outage fallbacks need, with the schema rebuilt into real Types and the note's age. */
    public record TableMetadata(
            ConnectorTableHandle handle,
            Map<String, ColumnHandle> columns,
            Map<String, String> columnTypes,
            ConnectorTableSchema schema,
            long refreshedAtEpochMillis) {}

    /**
     * Types cannot round trip without the engine's Block and Type serdes, so the note mapper
     * learns the one conversion it can do here: a Type becomes its SQL rendering and comes back
     * through the connector's TypeManager. Unknown properties are tolerated because delegate
     * records expose derived accessors that no creator consumes.
     */
    private final ObjectMapper mapper;
    private final Path root;
    private final TypeManager typeManager;
    private final Object lock = new Object();

    public PlanningNotes(Path root, TypeManager typeManager) {
        this.root = root;
        this.typeManager = typeManager;
        JsonMapper.Builder builder = JsonMapper.builder()
                .addModule(new Jdk8Module())
                .addModule(new GuavaModule())
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        if (typeManager != null) {
            SimpleModule types = new SimpleModule("scent-note-types");
            types.addSerializer(Type.class, new JsonSerializer<>() {
                @Override
                public void serialize(Type value, JsonGenerator generator, SerializerProvider provider) throws IOException {
                    generator.writeString(value.getDisplayName());
                }
            });
            types.addDeserializer(Type.class, new JsonDeserializer<>() {
                @Override
                public Type deserialize(JsonParser parser, DeserializationContext context) throws IOException {
                    return typeManager.fromSqlType(parser.getValueAsString());
                }
            });
            builder.addModule(types);
        }
        this.mapper = builder.build();
    }

    /** Extracts the serializable shape of a live schema; Types become their SQL rendering, so parameters survive. */
    public static List<SchemaColumn> schemaColumns(ConnectorTableSchema schema) {
        return schema.getColumns().stream()
                .map(column -> new SchemaColumn(
                        column.getName(),
                        column.getType().getDisplayName(),
                        column.isHidden()))
                .toList();
    }

    public static ConnectorTableSchema rebuildSchema(SchemaTableName table, List<SchemaColumn> columns, TypeManager typeManager) {
        List<ColumnSchema> rebuilt = columns.stream()
                .map(column -> ColumnSchema.builder()
                        .setName(column.name())
                        .setType(typeManager.fromSqlType(column.typeId()))
                        .setHidden(column.hidden())
                        .build())
                .toList();
        return new ConnectorTableSchema(table, rebuilt);
    }

    public void writeTableNote(SchemaTableName table, TableNote update) {
        if (root == null) {
            return;
        }
        synchronized (lock) {
            TableNoteJson existing = readNote(tableNotePath(table), TableNoteJson.class).orElse(null);
            TableNoteJson merged = new TableNoteJson(
                    envelope(update.handle(), existing == null ? null : existing.handle()),
                    envelopes(update.columns(), existing == null ? null : existing.columns()),
                    update.columnTypes() != null ? update.columnTypes() : existing == null ? null : existing.columnTypes(),
                    update.schema() != null ? update.schema() : existing == null ? null : existing.schema(),
                    System.currentTimeMillis());
            writeNote(tableNotePath(table), merged);
        }
    }

    public Optional<TableMetadata> readTable(SchemaTableName table) {
        if (root == null) {
            return Optional.empty();
        }
        Optional<TableNoteJson> note = readNote(tableNotePath(table), TableNoteJson.class);
        if (note.isEmpty() || note.get().handle() == null) {
            return Optional.empty();
        }
        TableNoteJson value = note.get();
        Map<String, ColumnHandle> columns = readColumns(value.columns());
        if (columns == null) {
            return Optional.empty();
        }
        Optional<ConnectorTableHandle> handle = readHandle(value.handle(), ConnectorTableHandle.class);
        if (handle.isEmpty()) {
            return Optional.empty();
        }
        ConnectorTableSchema schema = null;
        if (value.schema() != null && typeManager != null) {
            try {
                schema = rebuildSchema(table, value.schema(), typeManager);
            }
            catch (RuntimeException e) {
                log.debug("[SCENT] failed to rebuild schema from note for {}: {}", table, e.toString());
            }
        }
        return Optional.of(new TableMetadata(
                handle.get(),
                columns,
                value.columnTypes(),
                schema,
                value.refreshedAtEpochMillis()));
    }

    public void writeSliceNote(CacheKey key, ConnectorTableHandle filteredHandle) {
        if (root == null) {
            return;
        }
        synchronized (lock) {
            writeNote(sliceNotePath(key), new SliceNoteJson(
                    envelope(filteredHandle, null),
                    System.currentTimeMillis()));
        }
    }

    public Optional<SliceNote> readSliceNote(CacheKey key) {
        if (root == null) {
            return Optional.empty();
        }
        Optional<SliceNoteJson> note = readNote(sliceNotePath(key), SliceNoteJson.class);
        if (note.isEmpty() || note.get().filteredHandle() == null) {
            return Optional.empty();
        }
        return readHandle(note.get().filteredHandle(), ConnectorTableHandle.class)
                .map(SliceNote::new);
    }

    private Path tableNotePath(SchemaTableName table) {
        return root.resolve(table.toString()).resolve("table-note.json");
    }

    /** Mirrors FileSliceStore.fileFor so the note lands beside the slice's Parquet file. */
    private Path sliceNotePath(CacheKey key) {
        return root
                .resolve(key.getTable().toString())
                .resolve(key.canonicalPartition().replace('/', '_'))
                .resolve("slice-note.json");
    }

    private <T> Optional<T> readNote(Path path, Class<T> type) {
        if (!Files.isRegularFile(path)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(path.toFile(), type));
        }
        catch (IOException | RuntimeException e) {
            log.debug("[SCENT] unreadable planning note {}: {}", path, e.toString());
            return Optional.empty();
        }
    }

    private void writeNote(Path path, Object note) {
        Path temp = path.resolveSibling(path.getFileName() + ".tmp");
        try {
            Files.createDirectories(path.getParent());
            mapper.writeValue(temp.toFile(), note);
            try {
                Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        catch (IOException | RuntimeException e) {
            log.warn("[SCENT] failed to write planning note {}: {}", path, e.toString());
            try {
                Files.deleteIfExists(temp);
            }
            catch (IOException cleanup) {
                log.debug("[SCENT] failed to clean up temporary note {}: {}", temp, e.toString());
            }
        }
    }

    /**
     * The JDBC delegate's handle carries a QueryParameter whose creator needs the engine's
     * Block serde, so no plugin-side Jackson mapper can read that class back. The note stores
     * the plain relation shape instead (which is what the base handle is) and rebuilds it by
     * reflection; the class is loaded only when the JDBC delegate is actually installed. A
     * filtered handle loses the delegate-side predicate on the round trip, which is safe: the
     * cache key comes from our own partition domain, and the only consumer of a reconstructed
     * handle during an outage is the delegate's split generation, whose result is unused on a
     * cache hit.
     */
    private static final String JDBC_TABLE_HANDLE = "io.trino.plugin.jdbc.JdbcTableHandle";
    private static final String JDBC_NAMED_RELATION_HANDLE = "io.trino.plugin.jdbc.JdbcNamedRelationHandle";
    private static final String REMOTE_TABLE_NAME = "io.trino.plugin.jdbc.RemoteTableName";

    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_TABLE = "table";
    private static final String KEY_REMOTE_CATALOG = "remoteCatalog";
    private static final String KEY_REMOTE_SCHEMA = "remoteSchema";
    private static final String KEY_REMOTE_TABLE = "remoteTable";
    private static final String KEY_AUTHORIZATION = "authorization";

    private HandleRef envelope(Object value, HandleRef fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            if (JDBC_TABLE_HANDLE.equals(value.getClass().getName())) {
                return jdbcTableHandleEnvelope(value);
            }
            return new HandleRef(value.getClass().getName(), mapper.writeValueAsString(value));
        }
        catch (Exception e) {
            // One unusable field must not poison the whole note; the reader treats it as absent.
            log.debug("[SCENT] failed to serialize planning-note field {}: {}", value.getClass().getSimpleName(), e.toString());
            return fallback;
        }
    }

    private HandleRef jdbcTableHandleEnvelope(Object handle) throws Exception {
        Class<?> type = Class.forName(JDBC_TABLE_HANDLE);
        // getRelationHandle instead of asPlainTable: a filtered handle is marked synthetic and
        // asPlainTable rejects it, but the relation itself stays a named relation carrying the
        // table identity the note needs.
        Object relation = type.getMethod("getRelationHandle").invoke(handle);
        if (!JDBC_NAMED_RELATION_HANDLE.equals(relation.getClass().getName())) {
            throw new IllegalStateException("unsupported relation " + relation.getClass().getName());
        }
        SchemaTableName table = (SchemaTableName) relation.getClass().getMethod("getSchemaTableName").invoke(relation);
        Object remoteTable = relation.getClass().getMethod("getRemoteTableName").invoke(relation);
        @SuppressWarnings("unchecked")
        Optional<String> authorization = (Optional<String>) type.getMethod("getAuthorization").invoke(handle);
        Map<String, Object> shape = new LinkedHashMap<>();
        shape.put(KEY_SCHEMA, table.getSchemaName());
        shape.put(KEY_TABLE, table.getTableName());
        shape.put(KEY_REMOTE_CATALOG, remoteCatalogOf(remoteTable));
        shape.put(KEY_REMOTE_SCHEMA, remoteSchemaOf(remoteTable));
        shape.put(KEY_REMOTE_TABLE, remoteNameOf(remoteTable));
        shape.put(KEY_AUTHORIZATION, authorization == null ? null : authorization.orElse(null));
        return new HandleRef(JDBC_TABLE_HANDLE, mapper.writeValueAsString(shape));
    }

    private static String remoteCatalogOf(Object remoteTable) throws Exception {
        return optionalText(remoteTable.getClass().getMethod("getCatalogName").invoke(remoteTable));
    }

    private static String remoteSchemaOf(Object remoteTable) throws Exception {
        return optionalText(remoteTable.getClass().getMethod("getSchemaName").invoke(remoteTable));
    }

    private static String remoteNameOf(Object remoteTable) throws Exception {
        return (String) remoteTable.getClass().getMethod("getTableName").invoke(remoteTable);
    }

    private static String optionalText(Object optional) throws Exception {
        return ((Optional<?>) optional).flatMap(value -> value instanceof String s ? Optional.of(s) : Optional.empty()).orElse(null);
    }

    private Map<String, HandleRef> envelopes(Map<String, ? extends ColumnHandle> columns, Map<String, HandleRef> fallback) {
        if (columns == null) {
            return fallback;
        }
        Map<String, HandleRef> refs = new LinkedHashMap<>();
        columns.forEach((name, column) -> refs.put(name, envelope(column, fallback == null ? null : fallback.get(name))));
        return refs;
    }

    private <T> Optional<T> readHandle(HandleRef ref, Class<T> type) {
        // The type name comes from a file on disk, so it is checked against the allowlist
        // before any class is loaded; anything not allowlisted reads as absent.
        if (ref == null || ref.type() == null || ALLOWED_NAMES.stream().noneMatch(ref.type()::startsWith)) {
            return Optional.empty();
        }
        try {
            if (JDBC_TABLE_HANDLE.equals(ref.type())) {
                return Optional.of(type.cast(jdbcTableHandleFromNote(ref.json())));
            }
            Class<?> named = Class.forName(ref.type());
            return Optional.of(type.cast(mapper.readValue(ref.json(), named.asSubclass(type))));
        }
        catch (Exception e) {
            log.debug("[SCENT] failed to read planning-note object {}: {}", ref.type(), e.toString());
            return Optional.empty();
        }
    }

    private ConnectorTableHandle jdbcTableHandleFromNote(String json) throws Exception {
        JsonNode node = mapper.readTree(json);
        SchemaTableName table = new SchemaTableName(node.path(KEY_SCHEMA).asText(), node.path(KEY_TABLE).asText());
        Class<?> remoteType = Class.forName(REMOTE_TABLE_NAME);
        Constructor<?> remoteConstructor = remoteType.getConstructor(Optional.class, Optional.class, String.class);
        Object remoteTable = remoteConstructor.newInstance(
                optionalOf(node.path(KEY_REMOTE_CATALOG).asText(null)),
                optionalOf(node.path(KEY_REMOTE_SCHEMA).asText(null)),
                node.path(KEY_REMOTE_TABLE).asText());
        Class<?> handleType = Class.forName(JDBC_TABLE_HANDLE);
        Constructor<?> constructor = handleType.getConstructor(SchemaTableName.class, remoteType, Optional.class);
        return (ConnectorTableHandle) constructor.newInstance(table, remoteTable, optionalOf(node.path(KEY_AUTHORIZATION).asText(null)));
    }

    private static Optional<String> optionalOf(String value) {
        return value == null ? Optional.empty() : Optional.of(value);
    }

    private Map<String, ColumnHandle> readColumns(Map<String, HandleRef> refs) {
        if (refs == null) {
            return null;
        }
        Map<String, ColumnHandle> columns = new LinkedHashMap<>();
        for (Map.Entry<String, HandleRef> entry : refs.entrySet()) {
            Optional<ColumnHandle> column = readHandle(entry.getValue(), ColumnHandle.class);
            if (column.isEmpty()) {
                return null;
            }
            columns.put(entry.getKey(), column.get());
        }
        return columns;
    }
}

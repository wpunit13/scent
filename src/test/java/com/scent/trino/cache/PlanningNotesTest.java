package com.scent.trino.cache;

import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlanningNotesTest {

    private static final SchemaTableName TABLE = new SchemaTableName("public", "metrics");

    /**
     * The round trip must survive the real delegate handle classes; these are the shapes the
     * postgres delegate produces and the reason the note keeps a dedicated compatibility path
     * for them.
     */
    private static JdbcTableHandle jdbcHandle() {
        return new JdbcTableHandle(
                TABLE,
                new RemoteTableName(Optional.empty(), Optional.of("public"), "metrics"),
                Optional.empty());
    }

    private static Map<String, ColumnHandle> jdbcColumns() {
        JdbcColumnHandle region = new JdbcColumnHandle(
                "region",
                new JdbcTypeHandle(
                        12,
                        Optional.of("varchar"),
                        Optional.of(255),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                VarcharType.VARCHAR);
        return Map.of("region", region);
    }

    @Test
    void roundTripsRealDelegateHandleThroughJson(@TempDir Path dir) {
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        notes.writeTableNote(TABLE, new PlanningNotes.TableNote(
                jdbcHandle(),
                jdbcColumns(),
                Map.of("region", "varchar"),
                List.of(new PlanningNotes.SchemaColumn("region", "varchar", false))));

        PlanningNotes.TableMetadata readBack = notes.readTable(TABLE).orElseThrow();
        assertEquals(jdbcHandle(), readBack.handle());
        assertEquals(jdbcColumns(), readBack.columns());
        assertEquals(Map.of("region", "varchar"), readBack.columnTypes());
        assertEquals(TABLE, readBack.schema().getTable());
        assertEquals(1, readBack.schema().getColumns().size());
        assertEquals(VarcharType.VARCHAR, readBack.schema().getColumns().get(0).getType());
        assertFalse(readBack.schema().getColumns().get(0).isHidden());
    }

    @Test
    void mergeKeepsUnspecifiedFields(@TempDir Path dir) {
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        notes.writeTableNote(TABLE, new PlanningNotes.TableNote(jdbcHandle(), null, null, null));
        notes.writeTableNote(TABLE, new PlanningNotes.TableNote(null, jdbcColumns(), null, null));

        PlanningNotes.TableMetadata readBack = notes.readTable(TABLE).orElseThrow();
        assertEquals(jdbcHandle(), readBack.handle());
        assertEquals(jdbcColumns(), readBack.columns());
    }

    @Test
    void sliceNoteRoundTripsFilteredHandle(@TempDir Path dir) {
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        CacheKey key = CacheKey.of(TABLE, TupleDomain.all());
        notes.writeSliceNote(key, jdbcHandle());

        PlanningNotes.SliceNote readBack = notes.readSliceNote(key).orElseThrow();
        assertEquals(jdbcHandle(), readBack.filteredHandle());
    }

    @Test
    void filteredHandleNoteKeepsTableIdentityNotThePredicate(@TempDir Path dir) {
        // The delegate's applyFilter returns the base handle with the constraint folded in and
        // the relation marked synthetic; the note round trips the plain shape because the cache
        // key comes from our own partition domain and the delegate side of the handle is only
        // consumed by paths that fail during an outage anyway.
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        JdbcTableHandle base = jdbcHandle();
        JdbcTableHandle filtered = base.intersectedWithConstraint(TupleDomain.withColumnDomains(
                Map.of(jdbcColumns().get("region"), io.trino.spi.predicate.Domain.singleValue(VarcharType.VARCHAR, io.airlift.slice.Slices.utf8Slice("us-east")))));

        CacheKey key = CacheKey.of(TABLE, TupleDomain.all());
        notes.writeSliceNote(key, filtered);

        PlanningNotes.SliceNote readBack = notes.readSliceNote(key).orElseThrow();
        assertInstanceOf(JdbcTableHandle.class, readBack.filteredHandle());
        assertEquals(base, readBack.filteredHandle());
    }

    @Test
    void unreadableNoteReadsAsAbsent(@TempDir Path dir) throws Exception {
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        Path note = dir.resolve(TABLE.toString()).resolve("table-note.json");
        Files.createDirectories(note.getParent());
        Files.writeString(note, "not json at all {");

        assertTrue(notes.readTable(TABLE).isEmpty());
    }

    @Test
    void missingNoteReadsAsAbsent(@TempDir Path dir) {
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        assertTrue(notes.readTable(TABLE).isEmpty());
        assertTrue(notes.readSliceNote(CacheKey.of(TABLE, TupleDomain.all())).isEmpty());
    }

    @Test
    void disabledNotesAreNoOps(@TempDir Path dir) {
        PlanningNotes notes = new PlanningNotes(null, FixedTypeManager.INSTANCE);
        notes.writeTableNote(TABLE, new PlanningNotes.TableNote(jdbcHandle(), null, null, null));
        notes.writeSliceNote(CacheKey.of(TABLE, TupleDomain.all()), jdbcHandle());

        assertTrue(notes.readTable(TABLE).isEmpty());
        assertTrue(notes.readSliceNote(CacheKey.of(TABLE, TupleDomain.all())).isEmpty());
        assertFalse(Files.exists(dir.resolve(TABLE.toString())));
    }

    @Test
    void schemaRebuildResolvesSqlTypeStrings(@TempDir Path dir) {
        PlanningNotes notes = new PlanningNotes(dir, FixedTypeManager.INSTANCE);
        notes.writeTableNote(TABLE, new PlanningNotes.TableNote(
                jdbcHandle(),
                jdbcColumns(),
                null,
                List.of(
                        new PlanningNotes.SchemaColumn("region", "varchar", false),
                        new PlanningNotes.SchemaColumn("value", "bigint", false))));

        PlanningNotes.TableMetadata readBack = notes.readTable(TABLE).orElseThrow();
        assertInstanceOf(io.trino.spi.connector.ConnectorTableSchema.class, readBack.schema());
        assertEquals(VarcharType.VARCHAR, readBack.schema().getColumns().get(0).getType());
        assertEquals(io.trino.spi.type.BigintType.BIGINT, readBack.schema().getColumns().get(1).getType());
    }
}

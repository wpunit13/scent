package com.scent.trino.cache;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSliceStoreTest {
    private static final TypeManager TYPE_MANAGER = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
            TypeManager.class.getClassLoader(),
            new Class<?>[] {TypeManager.class},
            (proxy, method, arguments) -> {
                if (!method.getName().equals("fromSqlType")) {
                    return null;
                }
                return switch ((String) arguments[0]) {
                    case "bigint" -> BigintType.BIGINT;
                    case "varchar" -> VarcharType.VARCHAR;
                    default -> throw new IllegalArgumentException("Unexpected type: " + arguments[0]);
                };
            });

    private static CacheKey key(String table) {
        return CacheKey.of(new SchemaTableName("schema", table), TupleDomain.all());
    }

    private static Page page(long number, String text) {
        BlockBuilder numberBlock = BigintType.BIGINT.createBlockBuilder(null, 1);
        BigintType.BIGINT.writeLong(numberBlock, number);
        BlockBuilder textBlock = VarcharType.VARCHAR.createBlockBuilder(null, 1);
        VarcharType.VARCHAR.writeSlice(textBlock, Slices.utf8Slice(text));
        return new Page(numberBlock.build(), textBlock.build());
    }

    private static List<Page> drain(ConnectorPageSource source) throws IOException {
        List<Page> pages = new ArrayList<>();
        try {
            while (true) {
                var next = source.getNextSourcePage();
                if (next == null) {
                    return pages;
                }
                pages.add(next.getPage());
            }
        }
        finally {
            source.close();
        }
    }

    @Test
    void disabledWithoutDiskDirectoryOrTypeManager() {
        assertFalse(new FileSliceStore(100, TYPE_MANAGER, null).isEnabled());
        assertFalse(new FileSliceStore(100, null, Path.of("unused")).isEnabled());
    }

    @Test
    void usesHierarchicalFileLayout(@TempDir Path directory) {
        FileSliceStore store = new FileSliceStore(100, TYPE_MANAGER, directory);
        Path file = store.fileFor(key("orders"));

        assertEquals(directory.resolve("schema.orders").resolve(key("orders").canonicalPartition().replace('/', '_'))
                .resolve(key("orders").getVersion() + ".parquet"), file);
    }

    @Test
    void completedSessionPublishesParquetAndCanBeReadFromANewStore(@TempDir Path directory) throws IOException {
        CacheKey key = key("persisted");
        FileSliceStore store = new FileSliceStore(1024 * 1024, TYPE_MANAGER, directory);
        Page first = page(42, "hello");
        Page second = page(84, "world");
        try (FileSliceStore.WriteSession session = store.beginWrite(key, List.of("n", "v"), List.of("bigint", "varchar"))) {
            assertTrue(session.write(first));
            assertTrue(Files.notExists(store.fileFor(key)), "slice must stay hidden before complete()");
            assertTrue(session.write(second));
            session.complete();
        }

        assertTrue(Files.size(store.fileFor(key)) > 0);
        FileSliceStore freshStore = new FileSliceStore(1024 * 1024, TYPE_MANAGER, directory);
        List<Page> loaded = drain(freshStore.openRead(key, List.of(0, 1), List.of("bigint", "varchar")).orElseThrow());
        assertEquals(2, loaded.size());
        assertEquals(42, BigintType.BIGINT.getLong(loaded.get(0).getBlock(0), 0));
        assertEquals("hello", VarcharType.VARCHAR.getSlice(loaded.get(0).getBlock(1), 0).toStringUtf8());
        assertEquals(84, BigintType.BIGINT.getLong(loaded.get(1).getBlock(0), 0));
        assertEquals("world", VarcharType.VARCHAR.getSlice(loaded.get(1).getBlock(1), 0).toStringUtf8());
    }

    @Test
    void readLoadsOnlyRequestedColumn(@TempDir Path directory) throws IOException {
        CacheKey key = key("projection");
        FileSliceStore store = new FileSliceStore(1024 * 1024, TYPE_MANAGER, directory);
        try (FileSliceStore.WriteSession session = store.beginWrite(key, List.of("n", "v"), List.of("bigint", "varchar"))) {
            session.write(page(17, "only requested"));
            session.complete();
        }

        List<Page> loaded = drain(store.openRead(key, List.of(1), List.of("bigint", "varchar")).orElseThrow());
        assertEquals(1, loaded.size());
        assertEquals(1, loaded.get(0).getChannelCount());
        assertEquals("only requested", VarcharType.VARCHAR.getSlice(loaded.get(0).getBlock(0), 0).toStringUtf8());
    }

    @Test
    void missingSliceIsCacheMiss(@TempDir Path directory) {
        FileSliceStore store = new FileSliceStore(100, TYPE_MANAGER, directory);
        assertTrue(store.openRead(key("missing"), List.of(0), List.of("bigint")).isEmpty());
    }

    @Test
    void abortDeletesTemporaryFileAndDoesNotPublishSlice(@TempDir Path directory) throws IOException {
        CacheKey key = key("aborted");
        FileSliceStore store = new FileSliceStore(1024 * 1024, TYPE_MANAGER, directory);
        FileSliceStore.WriteSession session = store.beginWrite(key, List.of("n", "v"), List.of("bigint", "varchar"));
        assertTrue(session.write(page(1, "partial")));

        session.abort();

        assertFalse(Files.exists(store.fileFor(key)));
        try (var paths = Files.walk(directory)) {
            assertTrue(paths.noneMatch(path -> path.getFileName().toString().endsWith(".parquet.tmp")));
        }
        assertTrue(store.openRead(key, List.of(0), List.of("bigint")).isEmpty());
    }

    @Test
    void perSliceLimitAbortsOnlyThatSession(@TempDir Path directory) throws IOException {
        CacheKey tooLarge = key("large");
        CacheKey withinLimit = key("small");
        FileSliceStore store = new FileSliceStore(40, TYPE_MANAGER, directory);

        try (FileSliceStore.WriteSession session = store.beginWrite(tooLarge, List.of("n", "v"), List.of("bigint", "varchar"))) {
            assertFalse(session.write(page(1, "this value pushes the page over the limit")));
        }
        try (FileSliceStore.WriteSession session = store.beginWrite(withinLimit, List.of("n", "v"), List.of("bigint", "varchar"))) {
            assertTrue(session.write(page(2, "x")));
            session.complete();
        }

        assertFalse(Files.exists(store.fileFor(tooLarge)));
        assertTrue(Files.exists(store.fileFor(withinLimit)));
    }

    @Test
    void rejectsNonPositiveLimit() {
        assertThrows(IllegalArgumentException.class, () -> new FileSliceStore(0, TYPE_MANAGER, Path.of("unused")));
        assertThrows(IllegalArgumentException.class, () -> new FileSliceStore(-1, TYPE_MANAGER, Path.of("unused")));
    }
}

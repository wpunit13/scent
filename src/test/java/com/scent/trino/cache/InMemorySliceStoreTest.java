package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemorySliceStoreTest {

    private static CacheKey key(String name) {
        return CacheKey.of(new SchemaTableName("schema", name), TupleDomain.all());
    }

    private static Page pageOfBytes(int positions, int bytesPerPosition) {
        byte[] values = new byte[positions * bytesPerPosition];
        Arrays.fill(values, (byte) 7);
        return new Page(new ByteArrayBlock(positions, Optional.empty(), values));
    }

    @Test
    void storesAndReturnsSlices() {
        InMemorySliceStore store = new InMemorySliceStore(1024 * 1024, null, null);
        assertFalse(store.isParquetEnabled());
        Page page = pageOfBytes(10, 4);

        assertTrue(store.get(key("missing")).isEmpty());

        store.put(key("slice"), List.of(page), List.of(), List.of());

        assertEquals(Optional.of(List.of(page)), store.get(key("slice")));
        assertEquals(page.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void typedPutPersistsParquetAlongsideInMemorySlice(@TempDir Path storageDirectory) throws IOException {
        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);
        InMemorySliceStore store = new InMemorySliceStore(1024 * 1024, typeManager, storageDirectory);

        store.put(key("slice"), List.of(), List.of("a"), List.of("bigint"));

        assertTrue(store.isParquetEnabled());
        assertTrue(store.get(key("slice")).isPresent());
        assertTrue(Files.size(storageDirectory.resolve(key("slice").pathSegment() + ".parquet")) > 0);
    }

    @Test
    void oversizedSliceIsNotWrittenToParquet(@TempDir Path storageDirectory) throws IOException {
        TypeManager typeManager = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> method.getName().equals("fromSqlType") ? BigintType.BIGINT : null);
        Page page = pageOfBytes(100, 4);
        InMemorySliceStore store = new InMemorySliceStore(page.getSizeInBytes() - 1, typeManager, storageDirectory);

        store.put(key("oversized"), List.of(page), List.of("a"), List.of("bigint"));

        assertTrue(store.get(key("oversized")).isEmpty());
        assertTrue(Files.notExists(storageDirectory.resolve(key("oversized").pathSegment() + ".parquet")));
    }

    @Test
    void parquetWriteFailureDoesNotPreventInMemoryCaching(@TempDir Path storageDirectory) {
        // A missing TypeManager makes the Parquet write fail, but disk persistence is best-effort.
        InMemorySliceStore store = new InMemorySliceStore(1024 * 1024, null, storageDirectory);
        Page page = pageOfBytes(10, 1);

        store.put(key("slice"), List.of(page), List.of("a"), List.of("bigint"));

        assertEquals(List.of(page), store.get(key("slice")).orElseThrow());
        assertEquals(page.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void evictsOldestUnderByteCap() {
        Page big = pageOfBytes(1000, 4);
        Page small = pageOfBytes(100, 4);
        // Cap holds the small slice but not both
        InMemorySliceStore store = new InMemorySliceStore(big.getSizeInBytes() + small.getSizeInBytes() - 1, null, null);

        store.put(key("big"), List.of(big), List.of(), List.of());
        store.put(key("small"), List.of(small), List.of(), List.of());

        assertTrue(store.get(key("big")).isEmpty());
        assertEquals(List.of(small), store.get(key("small")).orElseThrow());
        assertEquals(small.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void rejectsSlicesLargerThanCap() {
        Page big = pageOfBytes(10_000, 4);
        Page small = pageOfBytes(10, 4);
        InMemorySliceStore store = new InMemorySliceStore(big.getSizeInBytes() - 1, null, null);

        store.put(key("big"), List.of(big), List.of(), List.of());
        store.put(key("small"), List.of(small), List.of(), List.of());

        assertTrue(store.get(key("big")).isEmpty());
        assertEquals(List.of(small), store.get(key("small")).orElseThrow());
    }

    @Test
    void replacesSliceWithSameKeyWithoutDoubleCounting() {
        Page first = pageOfBytes(10, 1);
        Page second = pageOfBytes(20, 1);
        InMemorySliceStore store = new InMemorySliceStore(1024 * 1024, null, null);

        store.put(key("k"), List.of(first), List.of(), List.of());
        store.put(key("k"), List.of(second), List.of(), List.of());

        assertEquals(List.of(second), store.get(key("k")).orElseThrow());
        assertEquals(second.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void rejectsNonPositiveCap() {
        assertThrows(IllegalArgumentException.class, () -> new InMemorySliceStore(0, null, null));
        assertThrows(IllegalArgumentException.class, () -> new InMemorySliceStore(-1, null, null));
    }
}

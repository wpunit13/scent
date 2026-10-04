package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemorySliceStoreTest {

    private static Page pageOfBytes(int positions, int bytesPerPosition) {
        byte[] values = new byte[positions * bytesPerPosition];
        Arrays.fill(values, (byte) 7);
        return new Page(new ByteArrayBlock(positions, Optional.empty(), values));
    }

    @Test
    void storesAndReturnsSlices() {
        InMemorySliceStore store = new InMemorySliceStore(1024 * 1024);
        Page page = pageOfBytes(10, 4);

        assertTrue(store.get("missing").isEmpty());

        store.put("slice", List.of(page));

        assertEquals(Optional.of(List.of(page)), store.get("slice"));
        assertEquals(page.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void evictsOldestUnderByteCap() {
        Page big = pageOfBytes(1000, 4);
        Page small = pageOfBytes(100, 4);
        // Cap holds the small slice but not both
        InMemorySliceStore store = new InMemorySliceStore(big.getSizeInBytes() + small.getSizeInBytes() - 1);

        store.put("big", List.of(big));
        store.put("small", List.of(small));

        assertTrue(store.get("big").isEmpty());
        assertEquals(List.of(small), store.get("small").orElseThrow());
        assertEquals(small.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void rejectsSlicesLargerThanCap() {
        Page big = pageOfBytes(10_000, 4);
        Page small = pageOfBytes(10, 4);
        InMemorySliceStore store = new InMemorySliceStore(big.getSizeInBytes() - 1);

        store.put("big", List.of(big));
        store.put("small", List.of(small));

        assertTrue(store.get("big").isEmpty());
        assertEquals(List.of(small), store.get("small").orElseThrow());
    }

    @Test
    void replacesSliceWithSameKeyWithoutDoubleCounting() {
        Page first = pageOfBytes(10, 1);
        Page second = pageOfBytes(20, 1);
        InMemorySliceStore store = new InMemorySliceStore(1024 * 1024);

        store.put("k", List.of(first));
        store.put("k", List.of(second));

        assertEquals(List.of(second), store.get("k").orElseThrow());
        assertEquals(second.getSizeInBytes(), store.totalBytes());
    }

    @Test
    void rejectsNonPositiveCap() {
        assertThrows(IllegalArgumentException.class, () -> new InMemorySliceStore(0));
        assertThrows(IllegalArgumentException.class, () -> new InMemorySliceStore(-1));
    }
}
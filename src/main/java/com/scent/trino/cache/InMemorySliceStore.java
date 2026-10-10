package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.type.TypeManager;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Worker-local slice cache: keyed slices of raw pages, FIFO-evicted under a byte cap.
 */
@Slf4j
public final class InMemorySliceStore {
    private final long maxBytes;
    private final TypeManager typeManager;
    private final Path storageDirectory;
    private final Map<String, List<Page>> slices = new LinkedHashMap<>();
    private long totalBytes;

    public InMemorySliceStore(long maxBytes, TypeManager typeManager, Path storageDirectory) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got: " + maxBytes);
        }
        this.maxBytes = maxBytes;
        this.typeManager = typeManager;
        this.storageDirectory = storageDirectory;
    }

    public synchronized void put(String key, List<Page> pages, List<String> columnNames, List<String> columnTypes) {
        long bytes = sizeOf(pages);
        if (bytes > maxBytes) {
            log.debug("[SCENT] slice {} ({} bytes) exceeds cache cap ({} bytes), not cached", key, bytes, maxBytes);
            return;
        }
        while (totalBytes + bytes > maxBytes && !slices.isEmpty()) {
            evictOldest();
        }
        List<Page> previous = slices.put(key, List.copyOf(pages));
        if (previous != null) {
            totalBytes -= sizeOf(previous);
        }
        totalBytes += bytes;
        if (storageDirectory != null) {
            try {
                ParquetSliceWriter.write(key, pages, columnNames, columnTypes, typeManager, storageDirectory);
                log.info("[SCENT] Parquet slice {} written successfully", key);
            }
            catch (Exception e) {
                log.warn("[SCENT] failed to write Parquet slice {} to disk", key, e);
            }
        }
    }

    public synchronized Optional<List<Page>> get(String key)
    {
        return Optional.ofNullable(slices.get(key));
    }

    public synchronized long totalBytes()
    {
        return totalBytes;
    }

    private void evictOldest() {
        Iterator<Map.Entry<String, List<Page>>> iterator = slices.entrySet().iterator();
        Map.Entry<String, List<Page>> oldest = iterator.next();
        iterator.remove();
        totalBytes -= sizeOf(oldest.getValue());
        log.debug("[SCENT] evicted slice {} ({} bytes)", oldest.getKey(), sizeOf(oldest.getValue()));
    }

    private static long sizeOf(List<Page> pages)
    {
        return pages.stream().mapToLong(Page::getSizeInBytes).sum();
    }

    /** Cap used by populate reads to decide when a slice outgrows the cache. */
    public long maxBytes()
    {
        return maxBytes;
    }

    public boolean isParquetEnabled()
    {
        return storageDirectory != null;
    }
}
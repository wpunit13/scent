package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.type.TypeManager;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
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
    private final Map<CacheKey, List<Page>> slices = new LinkedHashMap<>();
    private long totalBytes;

    public InMemorySliceStore(long maxBytes, TypeManager typeManager, Path storageDirectory) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got: " + maxBytes);
        }
        this.maxBytes = maxBytes;
        this.typeManager = typeManager;
        this.storageDirectory = storageDirectory;
    }

    public synchronized void put(CacheKey key, List<Page> pages, List<String> columnNames, List<String> columnTypes) {
        long bytes = sizeOf(pages);
        if (bytes > maxBytes) {
            log.debug("[SCENT] slice {} ({} bytes) exceeds cache cap ({} bytes), not cached", key, bytes, maxBytes);
            return;
        }
        storeInMemory(key, pages);
        if (storageDirectory != null) {
            try {
                ParquetSliceWriter.write(fileFor(key), pages, columnNames, columnTypes, typeManager);
                log.info("[SCENT] Parquet slice {} written successfully", key);
            }
            catch (Exception e) {
                log.warn("[SCENT] failed to write Parquet slice {} to disk", key, e);
            }
        }
    }

    /** Memory hit first; a miss falls back to the slice's Parquet file (read-through, ADR-011). */
    public synchronized Optional<List<Page>> get(CacheKey key)
    {
        List<Page> cached = slices.get(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        return readFromDisk(key);
    }

    private Optional<List<Page>> readFromDisk(CacheKey key)
    {
        if (storageDirectory == null || typeManager == null) {
            return Optional.empty();
        }
        Path file = fileFor(key);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        List<Page> pages;
        try {
            pages = ParquetSliceReader.read(file, typeManager);
        }
        catch (Exception e) {
            log.warn("[SCENT] failed to read Parquet slice {} from disk; treating as miss", key, e);
            return Optional.empty();
        }
        if (sizeOf(pages) <= maxBytes) {
            storeInMemory(key, pages);
        }
        log.debug("[SCENT] loaded slice {} from Parquet on disk", key);
        return Optional.of(pages);
    }

    /**
     * Hierarchical layout per ADR-006: root/table/partition/version.parquet. Sharding by table and
     * partition keeps every directory small, so lookups stay cheap and renames stay fast even when
     * the cache holds hundreds of thousands of slices.
     */
    Path fileFor(CacheKey key)
    {
        return storageDirectory
                .resolve(key.getTable().toString())
                .resolve(key.getPartition().toString().replace('/', '_'))
                .resolve(key.getVersion() + ".parquet");
    }

    /** Evicts older slices under the cap, then accounts and stores. Caller must have checked the cap. */
    private void storeInMemory(CacheKey key, List<Page> pages)
    {
        long bytes = sizeOf(pages);
        while (totalBytes + bytes > maxBytes && !slices.isEmpty()) {
            evictOldest();
        }
        List<Page> previous = slices.put(key, List.copyOf(pages));
        if (previous != null) {
            totalBytes -= sizeOf(previous);
        }
        totalBytes += bytes;
    }

    public synchronized long totalBytes()
    {
        return totalBytes;
    }

    private void evictOldest() {
        Iterator<Map.Entry<CacheKey, List<Page>>> iterator = slices.entrySet().iterator();
        Map.Entry<CacheKey, List<Page>> oldest = iterator.next();
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
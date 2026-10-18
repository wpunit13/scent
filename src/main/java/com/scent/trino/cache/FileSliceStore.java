package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.TypeManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Worker-local cache of Parquet slices. */
@Slf4j
public final class FileSliceStore {
    private final long maxBytes;
    private final TypeManager typeManager;
    private final Path storageDirectory;

    public FileSliceStore(long maxBytes, TypeManager typeManager, Path storageDirectory) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got: " + maxBytes);
        }
        this.maxBytes = maxBytes;
        this.typeManager = typeManager;
        this.storageDirectory = storageDirectory;
    }

    long maxBytes() {
        return maxBytes;
    }

    Optional<ConnectorPageSource> openRead(CacheKey key, List<Integer> channelIndexes, List<String> channelTypes) {
        if (storageDirectory == null || typeManager == null) {
            return Optional.empty();
        }
        Path file = fileFor(key);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            ConnectorPageSource pageSource = ParquetSliceReader.open(file, channelIndexes, channelTypes, typeManager);
            log.debug("[SCENT] loaded slice {} from Parquet on disk", key);
            return Optional.of(pageSource);
        }
        catch (Exception e) {
            log.warn("[SCENT] failed to read Parquet slice {}; treating as miss", key, e);
            return Optional.empty();
        }
    }

    WriteSession beginWrite(CacheKey key, List<String> columnNames, List<String> columnTypes) throws IOException {
        if (storageDirectory == null || typeManager == null) {
            return null;
        }
        return new WriteSession(ParquetSliceWriter.open(fileFor(key), columnNames, columnTypes, typeManager), maxBytes);
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
                .resolve(key.canonicalPartition().replace('/', '_'))
                .resolve(key.getVersion() + ".parquet");
    }

    public boolean isEnabled() {
        return storageDirectory != null && typeManager != null;
    }

    static final class WriteSession implements AutoCloseable {
        private final ParquetSliceWriter.StreamingWriter writer;
        private final long maxBytes;
        private long bytes;
        private boolean aborted;
        private boolean finished;

        private WriteSession(ParquetSliceWriter.StreamingWriter writer, long maxBytes) {
            this.writer = writer;
            this.maxBytes = maxBytes;
        }

        boolean write(Page page) throws IOException {
            if (finished || aborted) {
                return false;
            }
            long pageBytes = page.getSizeInBytes();
            if (pageBytes > maxBytes - bytes) {
                abort();
                return false;
            }
            try {
                writer.write(page);
                bytes += pageBytes;
                return true;
            }
            catch (IOException | RuntimeException e) {
                abort();
                throw e;
            }
        }

        //a write session can be completed only if it hasn't already finished or been aborted.
        void complete() throws IOException {
            if (!finished && !aborted) {
                finished = true;
                writer.complete();
            }
        }

        void abort() throws IOException {
            if (!finished && !aborted) {
                aborted = true;
                writer.abort();
            }
        }

        @Override
        public void close() throws IOException {
            abort();
        }
    }
}
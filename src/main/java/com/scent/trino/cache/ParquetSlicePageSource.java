package com.scent.trino.cache;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Streams decoded Parquet pages to the engine in bounded row chunks. */
final class ParquetSlicePageSource implements ConnectorPageSource {
    private static final int PAGE_ROWS = 8192;

    private final ParquetReader reader;
    private final ParquetDataSource dataSource;
    private final AggregatedMemoryContext memoryContext;
    private Page pendingPage;
    private int nextPosition;
    private boolean finished;
    private boolean closed;

    ParquetSlicePageSource(ParquetReader reader, ParquetDataSource dataSource, AggregatedMemoryContext memoryContext) {
        this.reader = reader;
        this.dataSource = dataSource;
        this.memoryContext = memoryContext;
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (closed || finished) {
            return null;
        }
        try {
            while (pendingPage == null || nextPosition >= pendingPage.getPositionCount()) {
                SourcePage sourcePage = reader.nextPage();
                if (sourcePage == null) {
                    finished = true;
                    close();
                    return null;
                }
                pendingPage = sourcePage.getPage();
                nextPosition = 0;
            }

            int length = Math.min(PAGE_ROWS, pendingPage.getPositionCount() - nextPosition);
            Page page = pendingPage.getRegion(nextPosition, length);
            nextPosition += length;
            return SourcePage.create(page);
        }
        catch (IOException e) {
            try {
                close();
            }
            catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw new UncheckedIOException(e);
        }
        catch (RuntimeException | Error e) {
            try {
                close();
            }
            catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    public long getCompletedBytes() {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos() {
        return dataSource.getReadTimeNanos();
    }

    @Override
    public long getMemoryUsage() {
        return memoryContext.getBytes();
    }

    @Override
    public boolean isFinished() {
        return finished || closed;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            reader.close();
        }
        finally {
            memoryContext.close();
        }
    }
}

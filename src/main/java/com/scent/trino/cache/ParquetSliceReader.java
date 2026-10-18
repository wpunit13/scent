package com.scent.trino.cache;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Streams selected Trino columns from a native Parquet slice. */
final class ParquetSliceReader {
    private ParquetSliceReader() {}

    static ConnectorPageSource open(Path file, List<Integer> channelIndexes, List<String> allTypeNames, TypeManager typeManager)
            throws IOException {
        LocalParquetDataSource dataSource = new LocalParquetDataSource(file);
        AggregatedMemoryContext memoryContext = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        try {
            ParquetMetadata metadata = MetadataReader.readFooter(
                    dataSource,
                    ParquetReaderOptions.defaultOptions(),
                    Optional.empty(),
                    Optional.empty());
            FileMetadata fileMetadata = metadata.getFileMetaData();
            MessageType schema = fileMetadata.getSchema();
            MessageColumnIO messageColumnIO = ParquetTypeUtils.getColumnIO(schema, schema);
            List<org.apache.parquet.schema.Type> schemaFields = schema.getFields();

            List<Column> columns = new ArrayList<>(channelIndexes.size());
            for (int channelIndex : channelIndexes) {
                if (channelIndex < 0 || channelIndex >= schemaFields.size()) {
                    throw new IllegalArgumentException("Cached channel index is outside the Parquet schema: " + channelIndex);
                }
                org.apache.parquet.schema.Type schemaField = schemaFields.get(channelIndex);
                String fieldName = schemaField.getName();
                if (channelIndex >= allTypeNames.size() || allTypeNames.get(channelIndex) == null) {
                    throw new IllegalArgumentException("Trino type metadata is missing for cached channel " + channelIndex);
                }
                String typeName = allTypeNames.get(channelIndex);
                ColumnIO columnIO = ParquetTypeUtils.lookupColumnByName(messageColumnIO, fieldName);
                Type type = parquetType(typeManager.fromSqlType(typeName));
                Field field = ParquetTypeUtils.constructField(type, columnIO)
                        .orElseThrow(() -> new IllegalArgumentException("Unsupported Parquet field: " + fieldName));
                columns.add(new Column(fieldName, field));
            }

            var descriptors = ParquetTypeUtils.getDescriptors(schema, schema);
            ImmutableList.Builder<RowGroupInfo> rowGroups = ImmutableList.builder();
            long rowOffset = 0;
            for (BlockMetadata block : metadata.getBlocks()) {
                PrunedBlockMetadata pruned = PrunedBlockMetadata.createPrunedColumnsMetadata(
                        block, dataSource.getId(), descriptors);
                rowGroups.add(new RowGroupInfo(pruned, rowOffset, Optional.empty()));
                rowOffset += block.rowCount();
            }

            ParquetReader reader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    columns,
                    false,
                    rowGroups.build(),
                    dataSource,
                    DateTimeZone.UTC,
                    memoryContext,
                    ParquetReaderOptions.defaultOptions(),
                    exception -> exception instanceof RuntimeException runtimeException
                            ? runtimeException
                            : new RuntimeException(exception),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            return new ParquetSlicePageSource(reader, dataSource, memoryContext);
        }
        catch (IOException | RuntimeException | Error e) {
            try {
                dataSource.close();
            }
            catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            memoryContext.close();
            throw e;
        }
    }

    private static Type parquetType(Type type) {
        return type.getBaseName().equals("json") ? VarcharType.VARCHAR : type;
    }

    private static final class LocalParquetDataSource extends AbstractParquetDataSource {
        private final FileChannel channel;

        private LocalParquetDataSource(Path file) throws IOException {
            super(new ParquetDataSourceId(file.toString()), java.nio.file.Files.size(file), ParquetReaderOptions.defaultOptions());
            channel = FileChannel.open(file, StandardOpenOption.READ);
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int length) throws IOException {
            ByteBuffer target = ByteBuffer.wrap(buffer, bufferOffset, length);
            long currentPosition = position;
            while (target.hasRemaining()) {
                int bytesRead = channel.read(target, currentPosition);
                if (bytesRead < 0) {
                    throw new EOFException("Unexpected end of Parquet file at offset " + currentPosition);
                }
                if (bytesRead == 0) {
                    continue;
                }
                currentPosition += bytesRead;
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}

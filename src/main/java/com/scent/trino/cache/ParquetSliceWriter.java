package com.scent.trino.cache;

import com.scent.trino.BuildInfo;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Writes Trino Pages using Trino's native Parquet writer. */
final class ParquetSliceWriter {
    private ParquetSliceWriter() {}

    static StreamingWriter open(Path target,
            List<String> columnNames,
            List<String> typeNames,
            TypeManager typeManager) throws IOException {
        validate(columnNames, typeNames, typeManager);
        Files.createDirectories(target.getParent());
        Path temporary = target.getParent().resolve("slice-" + UUID.randomUUID() + ".parquet.tmp");

        try {
            List<Type> types = typeNames.stream()
                    .map(typeManager::fromSqlType)
                    .map(ParquetSliceWriter::parquetType)
                    .toList();

            List<String> fieldNames = fieldNames(columnNames);
            ParquetSchemaConverter converter = new ParquetSchemaConverter(types, fieldNames, false, false);
            Converted schema = new Converted(converter.getMessageType(), converter.getPrimitiveTypes(), fieldNames);

            OutputStream output = Files.newOutputStream(temporary);
            ParquetWriter writer;
            try {
                writer = new ParquetWriter(
                            output,
                            schema.messageType(),
                            schema.primitiveTypes(),
                            ParquetWriterOptions.builder().build(),
                            CompressionCodec.GZIP,
                            "SCENT / Trino " + BuildInfo.trinoVersion(),
                            Optional.of(DateTimeZone.UTC),
                            Optional.empty());
            }
            catch (RuntimeException e) {
                output.close();
                throw e;
            }
            return new StreamingWriter(writer, output, temporary, target, columnNames.size());
        }
        catch (RuntimeException | IOException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
    }

    static final class StreamingWriter implements AutoCloseable {
        private final ParquetWriter writer;
        private final OutputStream output;
        private final Path temporary;
        private final Path target;
        private final int channelCount;
        private boolean closed;

        private StreamingWriter(ParquetWriter writer, OutputStream output,
                                Path temporary, Path target, int channelCount) {
            this.writer = writer;
            this.output = output;
            this.temporary = temporary;
            this.target = target;
            this.channelCount = channelCount;
        }

        void write(Page page) throws IOException {
            if (closed) {
                throw new IllegalStateException("Parquet slice writer is already closed");
            }
            if (page.getChannelCount() != channelCount) {
                throw new IllegalArgumentException("Page channel count does not match columns");
            }
            writer.write(page);
        }

        /*closed before attempting to close the underlying resources.
        If writer.close() fails, the catch block attempts to close output and delete the temporary file. But it doesn't retry closing writer.
        More importantly, closed now means that the StreamingWriter lifecycle has ended, not necessarily that all resources were successfully closed.*/
        void complete() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                writer.close();
                output.close();
                moveIntoPlace(temporary, target);
            }
            catch (RuntimeException | IOException e) {
                try {
                    output.close();
                }
                catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                try {
                    Files.deleteIfExists(temporary);
                }
                catch (IOException deleteFailure) {
                    e.addSuppressed(deleteFailure);
                }
                throw e;
            }
        }

        void abort() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    writer.close();
                }
                finally {
                    try {
                        output.close();
                    }
                    finally {
                        Files.deleteIfExists(temporary);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            abort();
        }
    }

    private static Type parquetType(Type type)
    {
        // Trino represents JSON values as slices; using Parquet's UTF-8 string mapping for them.
        return type.getBaseName().equals("json") ? VarcharType.VARCHAR : type;
    }

    private static List<String> fieldNames(List<String> columnNames) {
        List<String> fieldNames = new ArrayList<>();
        Set<String> usedNames = new HashSet<>();
        for (int index = 0; index < columnNames.size(); index++) {
            String fieldName = columnNames.get(index).replaceAll("[^A-Za-z0-9_]", "_");
            if (fieldName.isEmpty() || !Character.isLetter(fieldName.charAt(0)) && fieldName.charAt(0) != '_') {
                fieldName = "_" + fieldName;
            }
            if (!usedNames.add(fieldName)) {
                fieldName += "_" + index;
                while (!usedNames.add(fieldName)) {
                    fieldName += "_";
                }
            }
            fieldNames.add(fieldName);
        }
        return List.copyOf(fieldNames);
    }

    private static void moveIntoPlace(Path temporary, Path target) throws IOException {
        try {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void validate(List<String> columnNames,
            List<String> typeNames,
            TypeManager typeManager) {
        if (typeManager == null) {
            throw new IllegalStateException("TypeManager is required");
        }
        if (columnNames.size() != typeNames.size() || typeNames.stream().anyMatch(typeName -> typeName == null)) {
            throw new IllegalArgumentException("Column/type metadata is incomplete");
        }
    }

    record Converted(MessageType messageType, Map<List<String>, Type> primitiveTypes, List<String> fieldNames) {}
}
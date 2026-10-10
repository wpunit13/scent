package com.scent.trino.cache;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

final class ParquetSliceWriter {
    private ParquetSliceWriter() {}

    static void write(String key,
            List<Page> pages,
            List<String> columnNames,
            List<String> typeNames,
            TypeManager typeManager,
            Path cacheDirectory) throws IOException {

        validate(pages, columnNames, typeNames, typeManager);
        Files.createDirectories(cacheDirectory);
        Path target = cacheDirectory.resolve(key + ".parquet");
        Path temporary = cacheDirectory.resolve("slice-" + UUID.randomUUID() + ".parquet.tmp");

        try {
            List<Type> types = typeNames.stream()
                    .map(typeManager::fromSqlType)
                    .toList();

            List<String> fieldNames = fieldNames(columnNames);

            Schema schema = TrinoParquetSchema.create(
                            columnNames,
                            fieldNames,
                            typeNames,
                            types);

            writePages(temporary, pages, types, fieldNames, schema);
            moveIntoPlace(temporary, target);
        }
        catch (RuntimeException | IOException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
    }

    private static void writePages(Path temporary,
            List<Page> pages,
            List<Type> types,
            List<String> fieldNames,
            Schema schema) throws IOException {

        Configuration configuration = new Configuration(false);
        configuration.setClassLoader(ParquetSliceWriter.class.getClassLoader());
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                                     new org.apache.hadoop.fs.Path(
                                             temporary.toUri()))
                             .withSchema(schema)
                             .withDataModel(GenericData.get())
                             .withCompressionCodec(CompressionCodecName.GZIP)
                             .withConf(configuration)
                             .build()) {

            for (Page page : pages) {
                writePage(writer, page, types, fieldNames, schema);
            }
        }
    }

    private static void writePage(
            ParquetWriter<GenericRecord> writer,
            Page page,
            List<Type> types,
            List<String> fieldNames,
            Schema schema) throws IOException {
        for (int position = 0; position < page.getPositionCount(); position++) {
            GenericRecord row = new GenericData.Record(schema);
            for (int channel = 0; channel < types.size(); channel++) {
                Block block = page.getBlock(channel);
                Object value = block.isNull(position)
                        ? null
                        : TrinoParquetValueConverter.convert(
                        types.get(channel),
                        schema.getFields().get(channel).schema(),
                        block,
                        position);

                row.put(fieldNames.get(channel), value);
            }
            writer.write(row);
        }
    }

    private static void moveIntoPlace(Path temporary, Path target) throws IOException {
        try {
            Files.move(temporary,
                    target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        }
        catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
            Files.move(temporary,
                    target,
                    StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void validate(List<Page> pages,
            List<String> columnNames,
            List<String> typeNames,
            TypeManager typeManager) {
        if (typeManager == null) {
            throw new IllegalStateException("TypeManager is required");
        }

        if (columnNames.size() != typeNames.size()
                || typeNames.stream().anyMatch(typeName -> typeName == null)) {
            throw new IllegalArgumentException("Column/type metadata is incomplete");
        }

        for (Page page : pages) {
            if (page.getChannelCount() != columnNames.size()) {
                throw new IllegalArgumentException("Page channel count does not match columns");
            }
        }
    }

    private static List<String> fieldNames(List<String> columnNames)
    {
        List<String> fieldNames = new ArrayList<>();
        Set<String> usedNames = new HashSet<>();

        for (int index = 0; index < columnNames.size(); index++) {
            String columnName = columnNames.get(index);
            String fieldName = columnName.replaceAll("[^A-Za-z0-9_]", "_");
            if (fieldName.isEmpty() || !Character.isLetter(fieldName.charAt(0)) && fieldName.charAt(0) != '_') {
                fieldName = "_" + fieldName;
            }

            String uniqueName = fieldName;
            if (!usedNames.add(uniqueName)) {
                uniqueName = fieldName + "_" + index;
                while (!usedNames.add(uniqueName)) {
                    uniqueName += "_";
                }
            }
            fieldNames.add(uniqueName);
        }
        return fieldNames;
    }
}
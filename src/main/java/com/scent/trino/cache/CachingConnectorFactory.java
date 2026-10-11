package com.scent.trino.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scent.trino.BuildInfo;
import com.scent.trino.delegate.DelegateConnectorResolver;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

@Slf4j
public class CachingConnectorFactory implements ConnectorFactory {

    private static final String PARTITION_COLUMN_PREFIX = "cache.partition-column.";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DelegateConnectorResolver delegateResolver;

    public CachingConnectorFactory() {
        this.delegateResolver = new DelegateConnectorResolver();
    }

    @Override
    public String getName() {
        return "cache_wrapper";
    }

    @Override
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context) {

        verifyTrinoVersion(catalogName, context);

        String delegateName = config.get("delegate.connector.name");
        if (delegateName == null || delegateName.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration: delegate.connector.name");
        }

        // Opt-in per table (ADR-013): only tables with a configured grain are cached.
        Map<String, List<String>> partitionColumns = parsePartitionColumns(config);
        if (partitionColumns.isEmpty()) {
            log.warn("[SCENT] no cache.partition-column.<schema>.<table> entries configured; no table will be cached");
        }
        else {
            log.info("[SCENT] cacheable tables: {}", partitionColumns);
        }

        long cacheMaxBytes = config.containsKey("cache.max-bytes")
                ? Long.parseLong(config.get("cache.max-bytes"))
                : 256L * 1024 * 1024;

        Path storageDirectory = parseStorageDirectory(config);

        ConnectorFactory delegateFactory = delegateResolver.resolve(delegateName);

        Map<String, String> delegateConfig = new HashMap<>(config);
        delegateConfig.keySet().removeIf(key -> key.startsWith("cache.") || key.startsWith("delegate."));

        Connector delegateConnector =
                delegateFactory.create(
                        catalogName,
                        delegateConfig,
                        context);

        return CachingConnector.wrap(delegateConnector, partitionColumns, new InMemorySliceStore(cacheMaxBytes,
                        context.getTypeManager(),
                        storageDirectory));
    }

    /**
     * The bundled delegate connectors and the plugin's own SPI usage are only valid on the Trino
     * version this plugin was built against. Fail fast with an actionable message instead of
     * surfacing a NoSuchMethodError/AbstractMethodError deep inside a query.
     */
    private static void verifyTrinoVersion(String catalogName, ConnectorContext context) {
        String runtimeVersion = context.getSpiVersion();
        if (!BuildInfo.trinoVersion().equals(runtimeVersion)) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    String.format(
                            "SCENT plugin in catalog '%s' was built for Trino %s but the cluster is running Trino %s. "
                                    + "Rebuild the plugin against the cluster's Trino version.",
                            catalogName, BuildInfo.trinoVersion(), runtimeVersion));
        }
    }

    /**
     * Opt-in per table (ADR-013): every key of the form cache.partition-column.<schema>.<table>
     * declares that table's slice grain as a JSON array of column names. Tables without an entry
     * are never cached.
     */
    static Map<String, List<String>> parsePartitionColumns(Map<String, String> config) {
        Map<String, List<String>> byTable = new HashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PARTITION_COLUMN_PREFIX)) {
                continue;
            }
            String table = key.substring(PARTITION_COLUMN_PREFIX.length()).trim();
            if (table.isEmpty()) {
                throw new IllegalArgumentException(
                        "Config '" + key + "' has no table name; expected cache.partition-column.<schema>.<table>");
            }
            byTable.put(table, parsePartitionColumnList(key, entry.getValue()));
        }
        return Map.copyOf(byTable);
    }

    private static List<String> parsePartitionColumnList(String key, String value) {
        List<String> columns;
        try {
            columns = OBJECT_MAPPER.readValue(value, new TypeReference<List<String>>() {});
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Config '" + key + "' must be a JSON array of column names, e.g. [\"region\"]; got: " + value, e);
        }
        if (columns.isEmpty() || columns.stream().anyMatch(column -> column == null || column.isBlank())) {
            throw new IllegalArgumentException(
                    "Config '" + key + "' must be a non-empty JSON array of column names; got: " + value);
        }
        return List.copyOf(columns);
    }

    private static Path parseStorageDirectory(Map<String, String> config) {
        String directory = config.get("cache.root");

        if (directory == null || directory.isBlank()) {
            return null;
        }

        try {
            return Path.of(directory);
        }
        catch (InvalidPathException e) {
            log.error("Invalid cache.root '{}'; Parquet storage is disabled", directory, e);
            return null;
        }
    }
}
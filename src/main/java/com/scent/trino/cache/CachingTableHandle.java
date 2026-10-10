package com.scent.trino.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNullElse;

/**
 * Trino-facing table handle. Wraps the delegate's handle and carries the cache bookkeeping:
 * the partition predicate that defines the cached slice, and the delegate's column handles
 * (attached by getColumnHandles interception) which fix the channel order of cached pages.
 */
public final class CachingTableHandle implements ConnectorTableHandle {
    private final ConnectorTableHandle delegateHandle;
    private final SchemaTableName schemaTableName;
    private final TupleDomain<ColumnHandle> partitionDomain;
    private Map<String, ColumnHandle> columnHandles;
    private Map<String, String> columnTypes;

    @JsonCreator
    public CachingTableHandle(
            @JsonProperty("delegateHandle") ConnectorTableHandle delegateHandle,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("partitionDomain") TupleDomain<ColumnHandle> partitionDomain,
            @JsonProperty("columnHandles") Map<String, ColumnHandle> columnHandles,
            @JsonProperty("columnTypes") Map<String, String> columnTypes) {
        this.delegateHandle = Objects.requireNonNull(delegateHandle, "delegateHandle is null");
        this.schemaTableName = schemaTableName;
        this.partitionDomain = requireNonNullElse(partitionDomain, TupleDomain.all());
        this.columnHandles = columnHandles;
        this.columnTypes = columnTypes;
    }

    @JsonProperty
    public ConnectorTableHandle getDelegateHandle()
    {
        return delegateHandle;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPartitionDomain()
    {
        return partitionDomain;
    }

    @JsonProperty
    public Map<String, ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public synchronized Map<String, String> getColumnTypes()
    {
        return columnTypes;
    }

    /**
     * Columns are learned exactly once, from the delegate's getColumnHandles; first write wins.
     */
    public synchronized void attachColumns(Map<String, ColumnHandle> columnHandles) {
        if (columnHandles != null && this.columnHandles == null) {
            this.columnHandles = Map.copyOf(columnHandles);
        }
    }

    public synchronized void attachColumnTypes(Map<String, String> columnTypes) {
        if (columnTypes != null && this.columnTypes == null) {
            this.columnTypes = Map.copyOf(columnTypes);
        }
    }

    @Override
    public String toString()
    {
        return schemaTableName + " partition=" + partitionDomain;
    }
}
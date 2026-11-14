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
    private volatile Map<String, ColumnHandle> columnHandles;
    private volatile Map<String, String> columnTypes;
    /**
     * False when the delegate handle was rebuilt from a planning note, whose shape drops the
     * delegate-side predicate. Such a handle may serve a cache hit (the slice bytes are correct)
     * but must never drive a delegate read presented as filtered: the read would be unfiltered
     * while the scan claims a predicate. The page source enforces this.
     */
    private final boolean delegateHandleTrusted;

    public CachingTableHandle(
            ConnectorTableHandle delegateHandle,
            SchemaTableName schemaTableName,
            TupleDomain<ColumnHandle> partitionDomain,
            Map<String, ColumnHandle> columnHandles,
            Map<String, String> columnTypes) {
        this(delegateHandle, schemaTableName, partitionDomain, columnHandles, columnTypes, true);
    }

    @JsonCreator
    public CachingTableHandle(
            @JsonProperty("delegateHandle") ConnectorTableHandle delegateHandle,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("partitionDomain") TupleDomain<ColumnHandle> partitionDomain,
            @JsonProperty("columnHandles") Map<String, ColumnHandle> columnHandles,
            @JsonProperty("columnTypes") Map<String, String> columnTypes,
            @JsonProperty("delegateHandleTrusted") boolean delegateHandleTrusted) {
        this.delegateHandle = Objects.requireNonNull(delegateHandle, "delegateHandle is null");
        this.schemaTableName = schemaTableName;
        this.partitionDomain = requireNonNullElse(partitionDomain, TupleDomain.all());
        this.columnHandles = columnHandles;
        this.columnTypes = columnTypes;
        this.delegateHandleTrusted = delegateHandleTrusted;
    }

    @JsonProperty
    public ConnectorTableHandle getDelegateHandle()
    {
        return delegateHandle;
    }

    @JsonProperty
    public boolean isDelegateHandleTrusted()
    {
        return delegateHandleTrusted;
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
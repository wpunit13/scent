package com.scent.trino.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Split wrapper carrying everything the page source needs to serve or populate a slice:
 * the cache key, the partition predicate, the delegate's full column set (fixing the cached
 * page layout), and the underlying delegate split.
 */
public final class CachingSplit implements ConnectorSplit {
    private final String sliceKey;
    private final TupleDomain<ColumnHandle> partitionDomain;
    private final Map<String, ColumnHandle> columnHandles;
    private final Map<String, String> columnTypes;
    private final ConnectorSplit delegateSplit;
    private final ConnectorTableHandle delegateTableHandle;

    @JsonCreator
    public CachingSplit(
            @JsonProperty("sliceKey") String sliceKey,
            @JsonProperty("partitionDomain") TupleDomain<ColumnHandle> partitionDomain,
            @JsonProperty("columnHandles") Map<String, ColumnHandle> columnHandles,
            @JsonProperty("columnTypes") Map<String, String> columnTypes,
            @JsonProperty("delegateSplit") ConnectorSplit delegateSplit,
            @JsonProperty("delegateTableHandle") ConnectorTableHandle delegateTableHandle) {
        this.sliceKey = Objects.requireNonNull(sliceKey, "sliceKey is null");
        this.partitionDomain = Objects.requireNonNull(partitionDomain, "partitionDomain is null");
        this.columnHandles = Objects.requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnTypes == null ? Map.of() : Map.copyOf(columnTypes);
        this.delegateSplit = Objects.requireNonNull(delegateSplit, "delegateSplit is null");
        this.delegateTableHandle = Objects.requireNonNull(delegateTableHandle, "delegateTableHandle is null");
    }

    @JsonProperty
    public String getSliceKey()
    {
        return sliceKey;
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
    public Map<String, String> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public ConnectorSplit getDelegateSplit()
    {
        return delegateSplit;
    }

    @JsonProperty
    public ConnectorTableHandle getDelegateTableHandle()
    {
        return delegateTableHandle;
    }

    /**
     * Cached pages are stored with channels in this order (column names sorted); the
     * populate read requests columns in the same order. Sorted keys keep the layout
     * deterministic across transactions, which the slice identity depends on.
     */
    public List<ColumnHandle> cachedChannelOrder() {
        return columnHandles.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    public List<String> cachedChannelNames()
    {
        return columnHandles.keySet().stream().sorted().toList();
    }

    public List<String> cachedChannelTypes()
    {
        return cachedChannelNames().stream().map(columnTypes::get).toList();
    }

    /** Position of a requested column in the cached page layout, or -1 if unknown. */
    public int cachedChannelIndex(ColumnHandle columnHandle) {
        List<ColumnHandle> order = cachedChannelOrder();
        return order.indexOf(columnHandle);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return delegateSplit.isRemotelyAccessible();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return delegateSplit.getAddresses();
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return delegateSplit.getSplitWeight();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return delegateSplit.getRetainedSizeInBytes();
    }

    @Override
    public String toString()
    {
        return sliceKey;
    }
}
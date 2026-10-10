package com.scent.trino.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

/**
 * Content-addressable identity of a cached slice: the table, the partition predicate that scopes
 * it, and the source content version. Two slices are the same cached unit only when all three
 * match. A version change rotates the key, which makes the stale entry unreachable instead of
 * needing an explicit invalidation (ADR-004).
 */
public final class CacheKey {
    private final SchemaTableName table;
    private final TupleDomain<ColumnHandle> partition;
    private final String version;

    @JsonCreator
    public CacheKey(
            @JsonProperty("table") SchemaTableName table,
            @JsonProperty("partition") TupleDomain<ColumnHandle> partition,
            @JsonProperty("version") String version) {
        this.table = Objects.requireNonNull(table, "table is null");
        this.partition = Objects.requireNonNull(partition, "partition is null");
        this.version = version == null || version.isBlank() ? "none" : version;
    }

    /** Version defaults to none until ADR-007 freshness supplies a real value. */
    public static CacheKey of(SchemaTableName table, TupleDomain<ColumnHandle> partition) {
        return new CacheKey(table, partition, "none");
    }

    @JsonProperty
    public SchemaTableName getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPartition()
    {
        return partition;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    /**
     * Filesystem-safe identity string, used for the parquet filename. ponytail: the partition half
     * still uses TupleDomain.toString(), which is fragile across predicate-format changes; a
     * structural rendering is a later concern.
     */
    public String pathSegment()
    {
        return table + "|" + partition + "|" + version;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheKey that)) {
            return false;
        }
        return table.equals(that.table) && partition.equals(that.partition) && version.equals(that.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, partition, version);
    }

    @Override
    public String toString()
    {
        return pathSegment();
    }
}

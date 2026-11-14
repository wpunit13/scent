package com.scent.trino.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Content-addressable identity of a cached slice: the table, the partition predicate that scopes
 * it, and the source content version. Two slices are the same cached unit only when all three
 * match. A version change rotates the key, which makes the stale entry unreachable instead of
 * needing an explicit invalidation (ADR-004).
 *
 * <p>The partition is keyed by column name (not ColumnHandle) so on-disk identity and in-memory
 * equality agree: column names are unique and deterministic within a table, whereas a generic
 * ColumnHandle.toString() is not guaranteed to be injective.
 */
public final class CacheKey {
    private final SchemaTableName table;
    private final TupleDomain<String> partition;
    private final String version;

    @JsonCreator
    public CacheKey(
            @JsonProperty("table") SchemaTableName table,
            @JsonProperty("partition") TupleDomain<String> partition,
            @JsonProperty("version") String version) {
        this.table = Objects.requireNonNull(table, "table is null");
        this.partition = Objects.requireNonNull(partition, "partition is null");
        this.version = version == null || version.isBlank() ? "none" : version;
    }

    /** Version defaults to none until ADR-007 freshness supplies a real value. */
    public static CacheKey of(SchemaTableName table, TupleDomain<String> partition) {
        return new CacheKey(table, partition, "none");
    }

    /**
     * Rebuilds a handle-keyed partition domain as a name-keyed one. Column names are unique and
     * deterministic within a table, so this is the canonical identity used for both equality and
     * the on-disk path.
     */
    public static TupleDomain<String> nameKeyed(TupleDomain<ColumnHandle> partition, Map<String, ColumnHandle> columnsByName) {
        if (partition.isAll() || partition.isNone()) {
            return partition.isNone() ? TupleDomain.none() : TupleDomain.all();
        }
        Map<ColumnHandle, String> nameByHandle = new HashMap<>();
        columnsByName.forEach((name, handle) -> nameByHandle.put(handle, name));
        Map<String, Domain> byName = new LinkedHashMap<>();
        partition.getDomains().orElse(Map.of()).forEach((handle, domain) -> {
            String name = nameByHandle.get(handle);
            if (name != null) {
                byName.put(name, domain);
            }
        });
        return TupleDomain.withColumnDomains(byName);
    }

    @JsonProperty
    public SchemaTableName getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<String> getPartition()
    {
        return partition;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    /**
     * Filesystem-safe identity string, used for the parquet filename. Percent-encodes the
     * characters Hadoop's URI-based path handling rejects.
     */
    public String pathSegment()
    {
        return encode(table + "|" + canonicalPartition() + "|" + version);
    }

    /**
     * Canonical rendering of the partition domains, entries sorted by column name so the on-disk
     * layout is identical on every worker regardless of map iteration order (ADR-013).
     */
    public String canonicalPartition()
    {
        Map<String, Domain> domains = partition.getDomains().orElse(Map.of());
        if (domains.isEmpty()) {
            return partition.toString();
        }
        String joined = domains.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
        return "{" + joined + "}";
    }

    private static String encode(String value)
    {
        StringBuilder out = new StringBuilder(value.length() + 16);
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (c == '%' || c == '{' || c == '}' || c == '[' || c == ']' || c == '|'
                    || c == ' ' || c == '?' || c == '#' || c == '"') {
                out.append('%').append(String.format("%02X", (int) c));
            }
            else {
                out.append(c);
            }
        }
        return out.toString();
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

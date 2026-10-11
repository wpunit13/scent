# TRACE: Trino Aware Analytic Cache Engine
### Connector-Agnostic Semantic Partition Cache for Trino

---

## 1. Executive Summary & Problem Statement

Trino is widely used to federate queries across transactional databases (PostgreSQL, MySQL, Oracle), data lakes (Iceberg, Delta Lake, Hive), and external APIs[cite: 2]. However, analytical and business intelligence (BI) workloads suffer from critical inefficiencies[cite: 2]:

1. **Repetitive Temporal Overlap:** BI dashboards repeatedly query overlapping time windows (e.g., "trailing 30 days")[cite: 2]. 29 of those 30 days represent static historical data, yet upstream systems are re-scanned repeatedly[cite: 2].
2. **Upstream Source Contention & Cost:** Running analytical aggregations directly against operational RDBMS instances spikes CPU usage, causes lock contention, or incurs heavy API egress/scan charges[cite: 2].
3. **Fragility of Result Caching:** Traditional query result caches (raw SQL hashing) invalidate on minor changes (e.g., whitespace, column aliases, extending a date range by one day, or adding a column)[cite: 2].
4. **Blindness of File/Block Caches:** File-level block caches (e.g., Alluxio, Rubix) operate strictly on raw byte offsets of object-store files[cite: 2]. They do not understand relational schemas, cannot optimize non-file sources like JDBC/RDBMS, and suffer from cache pollution[cite: 2].

**TRACE** is a partition-aware semantic caching plugin for Trino[cite: 2]. Implemented as an SPI wrapper connector, it intercepts query constraints, decomposes requests into discrete partition slices, reads warm slices directly from worker NVMe storage, and fetches only uncached deltas from source databases[cite: 2].

---

## 2. High-Level Architecture

TRACE sits inside the Trino engine via the standard SPI, wrapping delegate connectors (e.g., PostgreSQL, Iceberg) without requiring engine forks or proxy layers[cite: 2].

```text
                      Client / BI Dashboard
                                |  SQL Query
                                v
                   +--------------------------+
                   |    Trino Coordinator     |
                   +------------+-------------+
                                |
        +-----------------------+-----------------------+
        v                                               v
+-------------------------------+       +-------------------------------+
|        CachingMetadata        |       |      CachingSplitManager      |
|  - Extracts Predicates        |       |  - Checks Partition Registry  |
|  - Resolves Partition Slices  |       |  - Emits Local/Remote Splits  |
+-------------------------------+       +---------------+---------------+
                                                        |
                      +---------------------------------+---------------------------------+
                      | Assigned Splits                                                   | Assigned Splits
                      v (LocalCacheSplit)                                                 v (RemoteFetchSplit)
        +---------------------------+                                       +---------------------------+
        |         Worker A          |                                       |         Worker B          |
        | +-----------------------+ |                                       | +-----------------------+ |
        | |CachingPageSourceProv. | |                                       | |CachingPageSourceProv. | |
        | +-----------+-----------+ |                                       | +-----------+-----------+ |
        |             |             |                                       |             |             |
        |             v             |                                       |             v             |
        | +-----------------------+ |                                       | +-----------------------+ |
        | |    Local NVMe Read    | |                                       | | Upstream Source Read  | |
        | | (Pre-materialized     | |                                       | | (e.g. Postgres JDBC)  | |
        | |  Page/Parquet)        | |                                       | +-----------+-----------+ |
        | +-----------+-----------+ |                                       |             | Tee Stream  |
        |             |             |                                       |      +------+------+      |
        |             |             |                                       |      v             v      |
        |             |             |                                       | +---------+  +----------+ |
        |             |             |                                       | |  Query  |  |NVMe Cache| |
        |             |             |                                       | |  Pipe   |  |  Writer  | |
        |             |             |                                       | +---------+  +----------+ |
        +-------------+-------------+                                       +-------------+-------------+
                      |                                                                   |
                      +-------------------------------+-----------------------------------+
                                                      v
                                       +---------------------------+
                                       |   Trino Execution Engine  |
                                       | (Aggregate, Join, Return) |
                                       +---------------------------+
```

---

## 3. Core Mechanisms

### 3.1 Declarative Semantic Partitioning & Sub-Bucketing
Non-lakehouse sources (e.g., PostgreSQL) lack native partition metadata[cite: 2]. TRACE bridges this gap declaratively at the catalog level[cite: 2]. The catalog config defines which tables to cache, their logical partition keys, bucketing granularity, and invalidation rules[cite: 2].

* **Temporal Range Slicing:** Maps timestamps/dates into discrete uniform buckets (1h, 1d, 1month)[cite: 2]. Bounded pushdown queries target specific missing buckets[cite: 2].
* **Discrete Key Slicing:** Partitions across categorical values (e.g., tenant_id, region, country_code)[cite: 2].
* **Sub-Bucketing (Intra-Slice Parallelism):** To prevent single-worker node hot-spotting on massive daily partitions, TRACE defines a cache unit as a composite key: `(Logical_Slice_ID, Sub_Bucket_ID)`. Tables can define a natural `cluster_by` sub-partition (e.g., `region`) or synthetic hashing (e.g., `hash(session_id)`) to fan out data across multiple workers.
* **Native Lakehouse Pass-Through:** Respects underlying partition manifests (Iceberg/Delta/Hive)[cite: 2].

### 3.2 Coordinator Split Planning & Affinity Routing
1. **Predicate Extraction:** `CachingMetadata.applyFilter()` intercepts the query's `TupleDomain` and extracts partition column constraints[cite: 2].
2. **Slice Calculation:** Slices are resolved (e.g., `order_date BETWEEN '2026-09-20' AND '2026-09-22'` becomes `['2026-09-20', '2026-09-21', '2026-09-22']`)[cite: 2].
3. **Registry Check:** The coordinator queries an internal partition registry[cite: 2]:
   * **Cache Hit (`2026-09-20`):** Emits a `LocalCacheSplit` assigned via node locality (`split.getAddresses()`) strictly to the worker holding that partition[cite: 2].
   * **Cache Miss (`2026-09-21`):** Calls the underlying connector to generate standard splits (e.g., JDBC query with bounded predicate `WHERE order_date >= '2026-09-21' AND order_date < '2026-09-22'`)[cite: 2]. Wraps it in a `RemoteFetchSplit` flagged for caching[cite: 2].

### 3.3 Worker-Side Execution & Asynchronous Tee-Caching
* **On Cache Hit:** The worker’s `CachingPageSourceProvider` bypasses network I/O, reading directly from local storage into Trino `Page` structures[cite: 2].
* **On Cache Miss (Populate):** The worker calls the underlying connector’s `ConnectorPageSource`[cite: 2]. A `TeePageSource` decorator yields `Page` batches immediately downstream to avoid query latency[cite: 2]. To prevent upstream I/O bottlenecks, disk writes to NVMe are fully decoupled and asynchronous via a bounded concurrent queue. If a node's disk experiences latency spikes, the writer drops the cache payload to preserve query speed.
* **Atomic Registration:** Upon stream completion, the worker atomically renames the cache file and registers the new partition with the coordinator registry[cite: 2].

### 3.4 System Resilience & Worker Lifecycle
* **Aggressive Startup Cleanup:** To guarantee strict data freshness, workers aggressively wipe their local TRACE cache directory upon startup or when rejoining the cluster after a failure. This prevents offline nodes from serving stale slices upon reconnection. The cache repopulates organically through normal read-through traffic.
* **Locality Fallbacks:** If a worker receives a `LocalCacheSplit` but the local file is missing (e.g., due to node reassignment mid-query), the `CachingPageSourceProvider` silently suppresses the `FileNotFoundException` and instantly instantiates a `RemoteFetchSplit` to fetch the data directly from the delegate connector.
* **Decentralized State:** The coordinator’s registry acts as a materialized view of the cluster state, updating automatically as workers broadcast their cache inventories.

### 3.5 Eviction: The SIEVE Algorithm
Rather than traditional LRU (which incurs high lock contention and pointer mutation on every read hit), TRACE uses the **SIEVE** cache eviction algorithm[cite: 2]:
* Operates on a single FIFO queue with a single eviction pointer (`hand`)[cite: 2].
* Cache hits flip a single atomic `visited` bit (`visited = 1`) without acquiring write locks or moving nodes[cite: 2].
* On eviction, the `hand` scans nodes: if `visited == 1`, it resets to `0`; if `visited == 0`, it evicts the slice[cite: 2].
* Rapidly filters out "one-hit wonder" full table scans while preserving frequently queried operational slices[cite: 2].

---

## 4. Repository & Packaging Structure

TRACE is developed as an isolated SPI module without cloning or forking Trino. It depends only on standard Trino SPI artifacts (`trino-spi`, `trino-plugin-toolkit`) marked `provided` — the delegate connectors are **not** build-time dependencies, because Trino stopped publishing connector plugin artifacts to Maven Central from 477 onward.

The delegate plugin is resolved reflectively by class name at runtime (§6.2). Trino gives every plugin directory its own classloader, so a separately installed delegate plugin is **not** visible to the wrapper — the delegate's jars must be copied into the wrapper's own plugin directory **at deploy time**. They are already on the cluster (shipped with the Trino distribution), so they are version-matched to the runtime SPI by construction; no build-time fetching or version pinning is needed.

Two Trino constraints the packaging must respect:
- Every plugin directory must contain at least one `io.trino.spi.Plugin` service provider (`PluginManager.loadPlugin` fails otherwise), so the wrapper's generated `-services.jar` must be present.
- Duplicate connector registration is fatal (`DefaultCatalogFactory.addConnectorFactory`), so the delegate `-services.jar` files are excluded from the copy — otherwise the delegate is registered as a top-level connector and collides with its own plugin directory. For the same reason the wrapper is installed in exactly **one** plugin directory, never copied into each delegate's directory.

Delegate plugins installed separately on the cluster are otherwise irrelevant to the wrapper: there is no conflict because the wrapper directory does not register the delegate connector names.

The plugin is **version-locked**: the wrapper's own SPI usage is only valid on the Trino version it was built against (true of any Trino plugin — the SPI is not stable across releases). `CachingConnectorFactory` verifies this at catalog creation via `ConnectorContext.getSpiVersion()` and fails fast with an actionable message instead of a query-time `NoSuchMethodError`. Upgrading Trino means bumping `trino.version`, rebuilding, and redeploying the two wrapper jars.

```text
trino-cache/
|-- pom.xml
|-- README.md
`-- src/
    |-- main/
    |   |-- java/io/trinocache/
    |   |   |-- CachePlugin.java                  # Registered entry point in META-INF
    |   |   |-- CachingConnectorFactory.java      # Instantiates wrapper & delegate connector
    |   |   |-- CachingConnector.java             # Coordinates SPI component wrappers
    |   |   |-- CachingMetadata.java              # Predicate evaluation & slice calculation
    |   |   |-- CachingSplitManager.java          # Hit/miss routing & affinity split emission
    |   |   |-- CachingPageSourceProvider.java    # Local reader and TeePageSource wrapper
    |   |   |-- storage/
    |   |   |   |-- LocalDiskStorage.java         # Serialized Page / Parquet I/O
    |   |   |   `-- SieveEvictionPolicy.java      # Lock-free SIEVE eviction implementation
    |   |   `-- config/
    |   |       |-- CacheConfig.java              # Global catalog cache settings
    |   |       `-- TablePartitionRule.java       # Partition strategy definitions
    |   `-- resources/
    |       `-- META-INF/services/
    |           `-- io.trino.spi.Plugin           # ServiceLoader declaration
    `-- test/
        `-- java/io/trinocache/
            `-- TestCachingConnector.java         # In-memory cluster tests using DistributedQueryRunner
```

---

## 5. Configuration Specification

### 5.1 Catalog Definition (`/etc/trino/catalog/cached_pg.properties`)


```
# Wrapper Connector Configuration
connector.name=cache-wrapper

# Delegate connector shortcut (postgresql, oracle, mysql, sqlserver, iceberg, tpch)
# or fully qualified class name (e.g., com.custom.MyConnectorFactory)
delegate.connector.name=postgresql

# Upstream Connector Credentials (passed directly through to delegate)
connection-url=jdbc:postgresql://postgres.production.internal:5432/analytics
connection-user=trino_readonly
connection-password=secure_password
case-insensitive-name-matching=true

# Cache Engine Settings
cache.storage-dir=/mnt/nvme/trino-cache
cache.max-size=500GB
cache.eviction-policy=sieve
cache.rules-file=/etc/trino/cache-rules.yaml
```


### 5.2 Partition Rules Definition (`/etc/trino/cache-rules.yaml`)

```yaml
rules:
  # Example 1: Natural Composite Sub-Partitioning
  - schema: sales
    table: orders
    partition_strategy:
      type: range
      column: order_date
      granularity: 1d
      sub_partition:
        type: discrete
        columns: [region]        # Distributes discrete region slices to different workers
    freshness:
      immutable_after: 7d        # Slices older than 7 days are static; no invalidation
      hot_ttl: 2h                # Recent partitions refresh every 2 hours

  # Example 2: Synthetic Hash Sub-Bucketing
  - schema: events
    table: clickstream
    partition_strategy:
      type: range
      column: event_date
      granularity: 1d
      sub_partition:
        type: hash
        column: session_id       # High cardinality column for synthetic splitting
        buckets: 16              # Fan-out factor per date slice to prevent hot-spotting
    freshness:
      hot_ttl: 1h

  # Example 3: Coarse Discrete Partitioning
  - schema: crm
    table: customer_events
    partition_strategy:
      type: discrete
      column: tenant_id
    freshness:
      hot_ttl: 12h
```

---

## 6. Implementation Blueprint (Trino SPI)

### 6.1 `pom.xml` Dependencies

```xml
<project xmlns="[http://maven.apache.org/POM/4.0.0](http://maven.apache.org/POM/4.0.0)"
         xmlns:xsi="[http://www.w3.org/2001/XMLSchema-instance](http://www.w3.org/2001/XMLSchema-instance)"
         xsi:schemaLocation="[http://maven.apache.org/POM/4.0.0](http://maven.apache.org/POM/4.0.0) [http://maven.apache.org/xsd/maven-4.0.0.xsd](http://maven.apache.org/xsd/maven-4.0.0.xsd)">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.trinocache</groupId>
    <artifactId>trino-cache</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>trino-plugin</packaging>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <dep.trino.version>440</dep.trino.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-spi</artifactId>
            <version>${dep.trino.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-plugin-toolkit</artifactId>
            <version>${dep.trino.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-postgresql</artifactId>
            <version>${dep.trino.version}</version>
            <scope>compile</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>io.trino</groupId>
                <artifactId>trino-maven-plugin</artifactId>
                <version>14</version>
                <extensions>true</extensions>
            </plugin>
        </plugins>
    </build>
</project>
```

### 6.2 Connector Factory Initialization

```java
package io.trinocache;

import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;

public class CachingConnectorFactory implements ConnectorFactory {

    // Values are Plugin entry points, not ConnectorFactory classes: most connectors do not expose a
    // standalone factory (e.g. PostgreSqlPlugin constructs its factory internally). Resolved
    // reflectively so TRACE has no compile-time dependency on the connector artifacts.
    private static final Map<String, String> BUILTIN_PLUGINS = Map.of(
        "postgresql", "io.trino.plugin.postgresql.PostgreSqlPlugin",
        "oracle", "io.trino.plugin.oracle.OraclePlugin",
        "mysql", "io.trino.plugin.mysql.MySqlPlugin",
        "sqlserver", "io.trino.plugin.sqlserver.SqlServerPlugin",
        "iceberg", "io.trino.plugin.iceberg.IcebergPlugin",
        "tpch", "io.trino.plugin.tpch.TpchPlugin"
    );

    @Override
    public String getName() {
        return "cache-wrapper";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        String delegateName = config.get("delegate.connector.name");

        if (delegateName == null || delegateName.isBlank()) {
            throw new TrinoException(
                INVALID_CATALOG_PROPERTY,
                String.format("TRACE configuration error in catalog '%s': Missing required property 'delegate.connector.name'.", catalogName)
            );
        }

        String pluginClassName = BUILTIN_PLUGINS.getOrDefault(delegateName, delegateName);
        ConnectorFactory delegateFactory = instantiateFactory(catalogName, delegateName, pluginClassName);

        // Instantiate underlying delegate connector with catalog config
        Connector delegateConnector = delegateFactory.create(catalogName, config, context);

        String cacheDir = config.getOrDefault("cache.storage-dir", "/tmp/cache");
        String rulesPath = config.get("cache.rules-file");

        return new CachingConnector(delegateConnector, cacheDir, rulesPath);
    }

    private ConnectorFactory instantiateFactory(String catalogName, String rawInput, String pluginClassName) {
        Plugin plugin;
        try {
            plugin = (Plugin) Class.forName(pluginClassName).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new TrinoException(
                INVALID_CATALOG_PROPERTY,
                String.format("TRACE error in catalog '%s': Unrecognized delegate '%s'. Class '%s' not found.", catalogName, rawInput, pluginClassName),
                e
            );
        } catch (Exception e) {
            throw new TrinoException(
                GENERIC_INTERNAL_ERROR,
                String.format("TRACE failed to instantiate delegate plugin '%s' for catalog '%s'.", pluginClassName, catalogName),
                e
            );
        }

        for (ConnectorFactory factory : plugin.getConnectorFactories()) {
            if (factory.getName().equals(rawInput)) {
                return factory;
            }
        }
        throw new TrinoException(
            INVALID_CATALOG_PROPERTY,
            String.format("TRACE error in catalog '%s': delegate '%s' exposes no connector factory.", catalogName, rawInput)
        );
    }
}
```

### 6.3 The Decorator Connector

```java
package io.trinocache;

import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

public class CachingConnector implements Connector {
    private final Connector delegate;
    private final CacheManager cacheManager;

    public CachingConnector(Connector delegate, String cacheDir, String rulesPath) {
        this.delegate = delegate;
        this.cacheManager = new CacheManager(cacheDir, rulesPath);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
        return new CachingMetadata(delegate.getMetadata(session, transaction), cacheManager);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new CachingSplitManager(delegate.getSplitManager(), cacheManager);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new CachingPageSourceProvider(delegate.getPageSourceProvider(), cacheManager);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return delegate.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction) {
        delegate.commit(transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction) {
        delegate.rollback(transaction);
    }
}
```

---

## 7. Phased Implementation Roadmap

1. **Phase 1: Single-Table Read-Through Prototype**
   * Implement `CachingConnectorFactory` wrapping standard Trino connectors (`tpch` or `postgresql`)[cite: 2].
   * Support a single temporal date range strategy for a pre-configured table[cite: 2].
   * On cache miss, dump serialized Trino `Page` records directly to local disk; on hit, read back via node-affinity splits[cite: 2].
2. **Phase 2: Partition Splitting, Affinity & Sub-Bucketing**
   * Implement automated splitting logic: decompose queries spanning multiple days into separate `LocalCacheSplit` and `RemoteFetchSplit` objects[cite: 2].
   * Coordinate split placement across workers using consistent hashing[cite: 2].
   * Introduce multi-worker fan-out support via `cluster_by` and synthetic hash sub-bucketing for massive partitions.
3. **Phase 3: Production Eviction, Resilience & Invalidation**
   * Integrate asynchronous local disk writers and node-fallback logic for missing files.
   * Add aggressive start-up cleanups to prevent offline workers from rejoining with stale cache files.
   * Integrate the SIEVE eviction policy for local NVMe quota enforcement[cite: 2].
   * Implement TTL-based and watermark-based partition invalidation[cite: 2].
4. **Phase 4: Lakehouse Format Optimization**
   * Extend support to Apache Iceberg and Delta Lake by reading commit snapshot IDs from table manifests to determine partition freshness[cite: 2].

# TRACE Design Review — Issue Tracker

Status: `open` | `wip` | `resolved` | `wontfix` | `deferred`

## Correctness

| ID | Issue | Resolution direction | Status |
|----|-------|---------------------|--------|
| C1 | Cache stores query result, not raw slice → predicate pushdown contaminates it | Resolved: full-slice handle for populate read (see Verified mechanism) | resolved |
| C2 | Column projection mismatch (`select a` vs `select *`) | Resolved: populate read passes all columns; TRACE selects requested columns from pages | resolved |
| C3 | Non-partition predicates (`region='EU'`) | Resolved: partition domain consumed; non-partition domains returned as `remainingFilter` | resolved |
| C4 | Partial partition from `LIMIT`/cancel/failure | Only cache full-slice scans; discard partials explicitly | resolved (POC): publish only when the delegate source finishes cleanly; close-before-finish and over-cap populate discard the buffer |
| C5 | Staleness: `immutable_after: 7d` is an unenforced assumption | Resolved: freshness is a per-table mode, not a TTL (ADR-007) | resolved |
| C6 | Cache bypasses access control (RLS, masking, multi-tenant) | Resolved: cache raw slices; engine applies RLS/masks above the scan (ADR-009) | resolved |
| C7 | Query with no partition-column predicate → no slice to prune | Resolved: pass through to the delegate, uncached | resolved |

## Distributed systems

| ID | Issue | Resolution direction | Status |
|----|-------|---------------------|--------|
| D1 | Coordinator registry = hand-waved consensus, SPOF | Resolved (target): registry optional, routing registry-free (ADR-005). POC: none | resolved |
| D2 | `split.getAddresses()` is advisory, not guaranteed | Resolved: engine-provided `getAffinityKey()` at 483; addresses stay advisory (ADR-005) | resolved |
| D3 | Concurrent population of same slice → duplicate work | Resolved: local mode needs single-flight; shared mode has none (ADR-005/006) | resolved |
| D4 | Startup wipe contradicts immutable slices | Resolved (target): persist and validate (ADR-011). POC: in-memory, so moot | resolved |
| D5 | Sub-bucketing | Resolved (target): range/TupleDomain buckets + affinity (ADR-012). POC: granularity only | resolved |

## Eviction

| ID | Issue | Resolution direction | Status |
|----|-------|---------------------|--------|
| E1 | SIEVE rationale is a strawman; "lock-free" overstated | Resolved (target): keep SIEVE, fix the justification (ADR-010). POC: size cap | resolved |

## SPI implementation

| ID | Issue | Resolution direction | Status |
|----|-------|---------------------|--------|
| S1 | `Connector` delegation incomplete (defaults throw) | Resolved: `Connector`/`ConnectorMetadata` are dynamic proxies; every method forwards exhaustively | resolved |
| S2 | `applyFilter` ≠ split emission; conflated with `getSplits` | Resolved: split logic in `ConnectorSplitManager.getSplits` (Verified mechanism) | resolved |
| S3 | Wrapper config keys leak into delegate → validation failure | Resolved: factory strips `cache.*` / `delegate.*` before delegating | resolved |
| S4 | Bundling delegate connector = classloader minefield | Resolved: resolve the delegate reflectively by class name; co-locate the delegate jars with the wrapper at deploy time (plugin classloaders are isolated) | resolved |
| S5 | POM targets Java 21; project was written for Trino 440 | Resolved: compiles at `release 25` against trino-spi 483 | resolved |
| S6 | Serialized `Page` format is version-fragile | Resolved: slices are Parquet at `cache.root` (ADR-006) | resolved |

## Direction

| ID | Issue | Resolution direction | Status |
|----|-------|---------------------|--------|
| P1 | No "why not materialized views / Warp Speed / Rubix" | Add alternatives-considered section | open |
| P2 | No success criteria / benchmark | Define latency, hit-rate, scan-reduction targets | open |
| P3 | `[cite: 2]` artifacts | Strip | open |

---

## Target vs POC

The ADRs below describe the **target architecture**. The POC ships a subset — deferred items stay in the design.

| Area | Target | POC |
|---|---|---|
| Cache granularity | Full slice, all columns (ADR-001) | yes |
| Populate | widen-on-miss (ADR-002) | yes |
| Storage | One path + `cache.shared` flag; Parquet (ADR-006) | in-memory |
| Freshness | `immutable` / `versioned` modes (ADR-007) | `immutable` + `version: none` |
| Scope | JDBC/RDBMS (ADR-008) | one table, tpch/postgres |
| Access control | Cache raw, engine enforces (ADR-009) | yes |
| Topology | Read-through; `getAffinityKey()` only when local (ADR-005) | read-through only |
| Registry | Optional: dedup + observability (ADR-005) | none |
| Coalescing | Per-worker single-flight, local mode only (D3) | none, instrumented |
| Sub-bucketing | Range buckets via `TupleDomain`, size-gated (ADR-012) | none; granularity only |
| Eviction | SIEVE, local mode only (ADR-010) | simple size cap |
| Persistence | Parquet at `cache.root` (ADR-006) | in-memory |

---

## Architecture Decisions

### ADR-001 — Cache granularity: full partition, all columns
**Status:** accepted
**Context:** C1, C2, C3. Query results are not reusable (predicate pushdown contaminates them); projections vary per query.
**Decision:** The cache unit is the partition slice. Each slice stores **all columns** of the partition. One slice per partition.
**Consequences:**
- Max reuse: any query over the partition is served from one cached slice.
- First read pays full-slice cost (all rows, all columns), even for a single-column query.
- Storage = full table width per slice.
- Cache key = slice ID + source snapshot/watermark (not the query).

### ADR-002 — Populate strategy: widen-on-miss
**Status:** accepted
**Context:** Fork 2.
**Decision:** On a miss, the user's own split reads the **full slice** (partition predicate only, all columns). The engine applies the residual filter and projection above the page source. Tee to cache.
**Consequences:**
- No second execution path; no background fill in the POC.
- First query on a partition pays full-slice latency.
- Requires keeping the pre-pushdown handle for the populate read (see SPI lever below).

### ADR-003 — Storage: engine blob-cache SPI — REJECTED for 483
**Status:** rejected (target is 483)
**Context:** Verified: `io.trino.spi.cache` and `ConnectorContext.getCacheFactory()` exist in **master only**. Absent in 483 (latest release, published 2026-07-18): `spi/cache/` 404s at tag 483, `ConnectorContext` has no `getCacheFactory()`, and no `trino-blob-cache-*` plugin ships in the 483 assets. The SPI is unreleased.
**Decision:** Do not depend on it. TRACE owns its storage for 483.
**Consequences:**
- E1 (SIEVE) and D4 (startup lifecycle) return to TRACE's scope.
- S6 (byte format) is TRACE's decision again.
- Revisit if the SPI ships in a release; it would remove the entire storage layer.
- Supersedes storage options A1–A4; A4 is no longer a shipped plugin, it is TRACE's own in-memory cache.

### ADR-006 — Storage backend: one path, `cache.shared` flag
**Status:** accepted (target)
**Context:** ADR-003 is rejected for 483 — TRACE owns storage. A local NVMe cache buys latency but loses elasticity (owner busy/dead → source read, duplication, hot spots). A shared store removes affinity, duplication and per-worker eviction at the cost of a network hop and an external system.
**Decision:**
- The cache root is a **path**, not a pluggable backend: `cache.root=/mnt/nvme/trino-cache`. A local SSD path and an NFS mount are the same code. "Users provide shared storage" = point the path at a mount.
- `cache.shared` selects the mode. It toggles whether affinity keys are emitted (ADR-005) and who owns eviction (ADR-010). Default `false` (local, no external dependency); **shared is the recommended production mode**.
- Slices are written as **Parquet** at `<root>/<table>/<slice>/<bucket>.parquet`.
- `SliceStore` has exactly two implementations, both needed: `InMemorySliceStore` (POC) and `FileSliceStore(root, shared)` (target). Not speculative.
- Object stores later via `io.trino:trino-filesystem`, not a bespoke S3 client. Noted cost: `trino-filesystem` / `trino-parquet` are libs, not SPI — version coupling.
**Consequences:**
- One binary, two topologies, config-only:
  ```properties
  cache.root=/mnt/nvme/trino-cache
  cache.shared=false
  ```
- Shared mode deletes affinity (ADR-005), local eviction (ADR-010) and duplication (D3) from the picture.
- Resolves S6: the byte format is Parquet.

### ADR-007 — Freshness: immutability vs version
**Status:** accepted
**Context:** C5. `immutable_after: 7d` was applied as a TTL to every table. It is only correct for append-only tables with bounded lateness; for mutating partitions it silently serves stale rows.
**Decision:** Freshness is a per-table **mode**, not a TTL.

**Mode `immutable` — append-only partitions.**
The partition never mutates after a declared bound. `lateness` is the watermark: the max delay between a row's event time and its write. Partitions older than `lateness` are final.
```yaml
freshness:
  mode: immutable
  lateness: 7d
```
Correct only if the invariant holds. Declared by contract; optionally verified (below).

**Mode `versioned` — mutating partitions.**
The partition can change at any time. A version is obtained per slice and encoded in the cache key (ADR-004).
```yaml
freshness:
  mode: versioned
  version: max_updated_at   # or: none
  column: updated_at
  check: refresh            # or: sync
  refresh: 2h
```
- `version: max_updated_at` — `SELECT max(updated_at) FROM t WHERE <partition predicate>` per slice. Batchable per window: `... WHERE date BETWEEN ... GROUP BY date`.
- `version: none` — cache within `refresh`, accept bounded staleness. Honest, not correct.
- `check: sync` — verify on every query. Correct, costs a small round-trip.
- `check: refresh` — background refresh. Bounded staleness.

**Optional verification for `immutable`.**
`SELECT max(updated_at) FROM t WHERE <partition>` ≤ partition end → still final. Cheap if `updated_at` is indexed. If the check fails, demote the slice to `versioned`.

**Examples**

| Table | Partitions mutate? | Mode | Why |
|---|---|---|---|
| `events.clickstream` | No | `immutable`, `lateness: 1h` | Append-only; late arrivals bounded by 1h |
| `audit.login_events` | No | `immutable`, `lateness: 0` | Strictly append-only |
| `sales.orders_snapshot_daily` | No | `immutable`, `lateness: 1d` | Written once per day by ETL |
| `sales.orders` (status updates) | Yes | `versioned`, `max_updated_at` | Refunds/status changes hit old partitions |
| `sales.orders` (late inserts only) | For 3d | `immutable`, `lateness: 3d` | Partition final after 3 days |
| `crm.customer_events` | Yes | `versioned`, `max_updated_at` | Continuous updates |
| `inventory.stock_levels` | Constantly | `versioned`, `none`, `refresh: 5m` | Current-state table; only TTL is sane |

**Sub-partition note.** Bucketing does not create immutability. A late row for an existing key mutates that bucket. Only bucketing on a *monotonic* key (insert id range) makes buckets append-only while the partition is not.

### ADR-008 — Scope: lakehouse sources out, Alluxio covers them
**Status:** accepted
**Context:** Trino's Hive/Iceberg/Delta connectors already support filesystem caching (Alluxio, Rubix). A repeat query prunes to the same files and hits the fs cache.
**Decision:** TRACE targets non-file sources (JDBC/RDBMS). Lakehouse pass-through is out of scope; drop Phase 4.
**Consequences:**
- Removes the snapshot-ID version path. All version strategies must be JDBC-shaped.
- Aligns with the original problem statement (JDBC contention, not object-store scanning).

### ADR-009 — Access control: cache raw, engine enforces
**Status:** accepted
**Context:** C6. Verified against 483 `ConnectorAccessControl`: `getRowFilters` returns `List<ViewExpression>` and `getColumnMasks` returns `Map<ColumnSchema, ViewExpression>` — both are SQL expressions over the table's columns, i.e. plan inputs, not connector-applied transforms.
**Decision:** TRACE caches raw slice bytes. It never pushes an RLS row filter or a column mask into the cache read. The engine's `FilterNode` (row filter) and masking `ProjectNode` sit above TRACE's page source and operate on raw rows.
**Consequences:**
- Already enforced by ADR-001/002 (never push projection or non-partition filters on the populate path).
- An RLS predicate on the partition column is safe: the predicate value is part of the slice ID, hence the cache key → a distinct slice per tenant.
- Remaining risk: filesystem permissions on the cache directory, since raw rows land on disk.

### ADR-004 — Cache key: content version in the key
**Status:** accepted (target)
**Context:** C5. Nothing in the source tells TRACE that a cached slice has gone stale. The engine blob-cache SPI (ADR-003) is out for 483, so TRACE owns the key.
**Decision:** The cache key encodes content identity, not just the partition: `[table, sliceId, bucket, version]`. `version` is the source partition's watermark/version (ADR-007).
**Consequences:**
- An `immutable` slice has a stable version → stable key → hit.
- A `versioned` slice changes version → new key → the old entry ages out via eviction. **No invalidation API is needed**; correctness comes from the key, not from deleting.
- TRACE must obtain a per-slice (or per-bucket) version. For JDBC this is the ADR-007 problem.
- `version` for a bucket can be `max(pk)` over that bucket (ADR-012), so one late row invalidates one bucket.

### ADR-005 — Topology: read-through, affinity only when the cache is local
**Status:** accepted (target)
**Context:** D1, D2, D3. Whether affinity is needed at all depends on where the cache lives (ADR-006).
**Decision:**
- Storage mode decides topology:
  - `cache.shared=false` (local root): the cache is sharded per worker. TRACE emits `split.getAffinityKey()` so the engine routes a `(slice, bucket)` split to the same worker across queries. This is the only reuse mechanism a local cache has.
  - `cache.shared=true` (shared root): any worker reads any bucket. Affinity is **suppressed** — it would concentrate load with no local copy to hit.
- **`getAffinityKey()` (483) is the affinity mechanism**, owned by the engine. No consistent-hash ring in TRACE, no coordinator registry. Verified contract: "splits reading related content are routed to the same worker(s) across queries."
- `getAddresses()` stays advisory. `isRemotelyAccessible()=false` gives a hard pin but is **never used** — it turns a placement miss into a query failure.
- The **coordinator registry is optional**, for cross-worker population dedup and observability only, never for routing.
**Consequences:**
- Local mode: reuse depends on the scheduler honouring affinity; a placement miss falls back to a **source read**, never a peer's disk. No replication (accepted).
- Shared mode: D2, D3 and the local eviction/lifecycle ADRs (ADR-010/011) do not apply.
- POC ships plain read-through with no affinity key.

### ADR-010 — Eviction: SIEVE per worker
**Status:** accepted (target)
**Context:** E1. TRACE owns eviction (ADR-006); the cache is per-worker local (ADR-005).
**Decision:** SIEVE per worker, over the worker's own cache directory. Hits flip a `visited` bit with no lock; the `hand` scans on eviction.
**Consequences:**
- Rationale is empirical (cheap on hit, no pointer mutation), not a claim that LRU is unusable. CLOCK would be a fine alternative.
- "Lock-free" is overstated: the `hand` and queue need atomicity.
- POC ships a simple size cap; SIEVE when eviction is actually exercised.
- Applies only when `cache.shared=false` (ADR-006). A shared root delegates eviction to the store (TTL / lifecycle policy).

### ADR-011 — Lifecycle: persist and validate, do not wipe
**Status:** accepted (target)
**Context:** D4. The original design wiped the cache on worker startup, to avoid serving stale slices after a node rejoins.
**Decision:** Do not wipe. Cache entries are keyed by slice ID + content version (ADR-004), so a stale entry is unreachable, not wrong. On startup, drop entries whose version is no longer accepted and let the rest survive.
**Consequences:**
- A rolling restart no longer cold-starts the cache, which matters most for `immutable` slices that never change.
- Correctness comes from version-in-key, not from wiping — so wiping was solving a problem the key already solves.
- Needs a durable index of what is on disk (rebuild by scanning the cache dir on start).
- Applies only when `cache.shared=false` (ADR-006). A shared store has no per-worker lifecycle.

### ADR-012 — Split granularity: TRACE subdivides coarse delegate splits
**Status:** accepted (target)
**Context:** Trino parallelizes at **split** granularity; the connector decides splits. Verified at 483: `JdbcSplitManager.getSplits` forwards straight to `jdbcClient.getSplits`; `JdbcSplit` carries only `additionalPredicate` and `dynamicFilter` — no range; and base-jdbc has no `JdbcPartitioningHandle`. So a JDBC table scan is **one split → one worker**, which reads the entire slice. TRACE inherits that granularity.
**Decision:** TRACE controls split granularity.
- If the delegate already emits multiple splits for a slice (file connectors), cache each delegate split as its own unit; do not subdivide.
- If the delegate emits one split (JDBC) and the slice is large, TRACE subdivides into N buckets and emits N splits.
- Buckets are **range / `TupleDomain` domains** (PK range, id range, finer time), **not hashes**:
  - A range domain is expressible via `delegate.applyFilter(handle, bucketDomain)` then `delegate.getSplits`.
  - A hash bucket (`hash(col) % N = k`) is **not** expressible as a `TupleDomain`, and is not source-prunable.
- Cache unit becomes `(slice, bucket)`. The affinity key is `(slice, bucket)` when the cache is local (ADR-005).
**Consequences:**
- Without subdivision a JDBC partition is single-worker — storage and CPU on one node, no scan parallelism. Sub-bucketing is therefore **the mechanism that makes a JDBC partition parallel**, not an optional optimization.
- Needs per-slice bucket boundaries: `min(pk)`/`max(pk)` for the slice (one cheap query), or the partition column's natural range.
- Gate subdivision on a row-count estimate so small slices are not chopped.
- Version can be computed **per bucket** (`max(pk)` per bucket), so one late row does not invalidate the whole slice.
- ADR-001's cache unit is the **split's scope**, all columns — not always the whole partition.

### ADR-013 — Slice grain: per-table partition columns, opt-in
**Status:** accepted (2026-10-11)
**Context:** `cache.partition-column` was catalog-wide. A catalog serves tables with different natural
partition keys (metrics by region; sales by day + region under Oracle composite partitioning, where the
partition key and the subpartition key are both real). A catalog-wide column is an invalid assumption:
tables without that column were silently uncached, and tables with a same-named column of different
cardinality were cached under a wrong grain, which is silently expensive (full-slice reads for slices that
never hit). Flagged in design review; confirmed critical.
**Decision:**
- The slice grain is configured per table: `cache.partition-column.<schema>.<table>=<JSON array of columns>`.
- Opt-in: a table with no configured grain is never cached; its queries pass through to the delegate.
  Caching is a capacity decision (populate reads the full partition; slices cost disk and memory), so the
  expensive mistake of a wrong grain must be impossible, not unlikely.
- Multi-column grains are supported. Every configured column whose domain appears in the query's constraint
  contributes to the slice; the slice is the conjunction of the domains present. Partial-grain queries cache
  coarser slices; mixed-grain query patterns may store overlapping data (inherent to the whole-slice design;
  a stricter all-columns-required gate is a one-line change if duplication bites).
- The partition's rendering in the on-disk layout must be canonical (domain entries sorted by column),
  because TupleDomain.toString() order is not stable across workers, and unstable ordering would break
  cross-worker hits in the shared cache. In-memory equality is unaffected (TupleDomain.equals is
  order-independent).
- The catalog-wide key is removed (clean break; the POC has no production users).
**Consequences:**
- Config value format is a JSON array. Unambiguous with legacy column names that may contain commas or
  spaces; Jackson is already a dependency; malformed values fail fast at catalog creation with an actionable
  error. Rejected: YAML (wrong substrate for Trino catalog properties files), repeated properties keys (Java
  Properties collapses duplicates to the last value), bare comma lists (ambiguous with legacy column names).
- README's Out-of-Scope "configurable caching policies" is superseded: choosing which tables are cached and
  their grain is now in scope.
- Tables with no configured grain are already safe today: the null-partition-handle path forwards the full
  constraint to the delegate (uncached passthrough).

### ADR-014 — Memory tier dropped: the disk is the cache
**Status:** accepted (2026-10-11). Supersedes ADR-006's two-implementation clause
(InMemorySliceStore POC + FileSliceStore target).
**Context:** The POC kept an in-memory page tier in front of the Parquet disk tier. In a Trino worker that
tier is hostile to the JVM: cached pages are long-lived heap objects competing with query execution memory,
the cache is invisible to Trino's memory manager, and a large retained live set is old-gen GC pressure on a
heap already sized for query workloads. Trino's own caching direction (the blob-cache SPI, the Hive
file-system cache) is disk-based for the same reason.
**Decision:**
- The disk is the only cache tier. `FileSliceStore` is the one implementation: put writes Parquet, get reads
  Parquet. No in-memory retention, no memory byte cap, no in-memory eviction.
- The OS page cache is the memory tier. Repeated reads of a hot slice are served from the kernel's page cache
  at memcpy speed, with eviction managed by the OS rather than the JVM GC.
- Per-query page rebuilds are short-lived allocations (young-gen, cheap), unlike long-lived retained pages.
  This is the standard JVM data-system pattern (Kafka, Cassandra read paths): rely on the OS page cache, keep
  the heap for short-lived work.
- Populate streams: the tee writes each raw page to the Parquet session as it arrives. The overCap question
  dissolves (there is no memory cap to hit), and the C4 guarantee moves to the session lifecycle: the temp
  file completes only on clean exhaustion and is deleted on abort.
- `cache.max-bytes` becomes a per-slice guard (skip caching slices larger than the cap), not a memory cap.
  A total disk quota arrives with the lifecycle phase (ADR-010/011).
**Consequences:**
- Every hit pays Parquet read plus page rebuild. Mitigations: the OS page cache makes the I/O free for hot
  slices; the compression codec is a read-speed knob (SNAPPY or none; cache files do not need max compression).
- Page-cache hit rates are opaque to us. The T1 harness measures source reads, the metric that matters, and
  stays valid.
- In-memory FIFO/SIEVE eviction (T13) becomes disk-file eviction over the cache directory.
- Restart survival is unchanged: the disk was already the source of truth.

---

## Design: the raw-slice problem (C1/C2/C3)

Cache unit = **partition**. Cache the partition's **physical rows**, not the query's result over the partition.

Trino's pushdown is **partial**: a connector can consume the partition predicate (for split pruning) and leave the rest as a residual filter the engine applies above the page source. Hive does this — partition pruning in the handle, non-partition predicates stay in the plan. Same for projection.

Flow:
- **Split planning**: partition predicate → slice resolution. Non-partition predicates → left in the plan, not pushed.
- **On miss**: populate read = full slice (partition predicate only, all columns). Engine applies residual filter + projection above. Tee to cache.
- **On hit**: cache read returns the full slice. Engine applies the same residual filter + projection. Identical semantics.

### Verified mechanism (Trino master, `PushPredicateIntoTableScan`)

`ConnectorMetadata.applyFilter` returns `Optional<ConstraintApplicationResult<TableHandle>>`, which carries `getRemainingFilter()` (a `TupleDomain`) and `getRemainingExpression()`. The engine re-applies the remaining filter as a `FilterNode` above the scan and computes the enforced part via `computeEnforced(predicate, unenforced)`.

The contract doc is explicit: "The connector is expected to enforce the respective domain entirely on none, some, or all of the columns." Partial pushdown is first-class.

Mapping to TRACE:
- `CachingMetadata.getTableHandle` — stash the delegate's raw handle.
- `CachingMetadata.applyFilter(handle, constraint)` — build a partition-only constraint from the incoming domains; call `delegate.applyFilter(rawHandle, partitionOnly)`; return `ConstraintApplicationResult` with the filtered handle and `remainingFilter` = the non-partition domains. Engine keeps them as a residual filter (C3).
- `CachingSplitManager.getSplits` — resolve slices from the partition constraint; hit → `LocalCacheSplit`, miss → delegate splits wrapped as `RemoteFetchSplit`.
- `CachingPageSourceProvider.createPageSource(..., columns, ...)` — the engine passes the pruned column list. On miss, call the delegate with **all** columns, tee the full slice to cache, then select `columns` from the pages (C2). On hit, read the cached full slice and select `columns`.

Verification items against Trino 440 (master fetched here; confirm 440 parity):
- `applyFilter` signature: `Optional<ConstraintApplicationResult<TableHandle>>` vs the older `ConnectorMetadata` return.
- The delegate's `applyFilter` accepts a partition-only `TupleDomain` and yields a split carrying only that predicate.
- Passing all columns to the delegate's `createPageSource` yields a full-slice read.

Cost: first query on a partition reads the whole partition (all rows, all columns), even for one column. Price of reuse. Acceptable for POC; note the ceiling.

### Forks to settle

**Fork 1 — all columns vs per-column-set.**
- All columns: one slice per partition, max reuse, max first-read cost, max storage.
- Per-column-set: key on projected column set. `select a` / `select b` cache separately. Less reuse, less cost.

POC default: all columns, one slice per partition. Decides cache key + storage format.

**Fork 2 — widen-on-miss vs background fill.**
- Widen-on-miss: user's own split reads the full slice. Simple, no second query. First query pays full-slice latency.
- Background fill: serve query narrow (fast), separately materialize full slice. Better latency, needs a second execution path (can't easily spawn a Trino query from inside a connector).

POC default: widen-on-miss.

**Fork 3 — resolved: use the engine blob-cache SPI.**
Verified against `io.trino.spi.Plugin` (master): no analyzer/optimizer rule hook, so a plugin cannot rewrite a query to read a materialized table. Routing must live in the connector. Separately, Trino ships a blob-cache SPI (`io.trino.spi.cache`) with two implementations: `plugin/trino-blob-cache-memory` and `plugin/trino-blob-cache-alluxio`. A connector obtains the cache via `ConnectorContext.getCacheFactory()` → `ConnectorCacheFactory.createBlobCache(CacheRequirements)`. See ADR-003.

The blob cache stores opaque bytes with positioned reads (`Blob.read(position, buffer, offset, length)`), per-catalog scoping, prefix invalidation, and built-in `cachedSize()`/`loadedSize()` metrics. TRACE supplies the bytes; the manager owns storage, eviction, and lifecycle.

### Access control (C6)
Settled — see ADR-009. Cache raw slices; never push RLS/masking into the cache read; the engine's `FilterNode`/masking `ProjectNode` sit above the scan.

### Sub-bucketing (D5)

Sub-bucketing splits a logical slice into N buckets so N workers each hold 1/N. Two goals: scan fan-out and storage spread.

It only works when the **source can prune the bucket predicate**:
- **Range bucket on an indexed column** (PK range, or a finer time/id column) → source prunes → real fan-out, no amplification.
- **Hash bucket on a non-indexed column** (`hash(session_id) % 16`) → source scans the whole slice per bucket → **N× amplification**. Wrong tool for JDBC sources.

**Lazy answer: use slice granularity instead.** `granularity: 1h` in place of `1d` gives the same storage spread, with a source-prunable predicate and no new concept. Only reach for bucketing when the partition column is too coarse to subdivide by time.

**Affinity tension.** Bucketing + no affinity = poor reuse: a query needs all N buckets, splits land anywhere, each worker caches its bucket, next query lands elsewhere and misses. Fan-out wants many workers; cache reuse wants the same worker. This is exactly what affinity (D2) reconciles.

**POC decision:** no sub-bucketing. Slice granularity is the knob. Reopen D2/D3 if bucketing is added later.

### Concurrent population (D3)

Cold slice + concurrent queries = thundering herd. Two flavors:

- **Same worker.** Two queries (or two splits of one query) miss the same slice. Duplicate source read, duplicate disk write, and a write race on the same file (mitigated by the atomic rename, ADR-001).
- **Cross worker.** With no affinity (ADR-005), split placement is arbitrary, so workers A and B can both get a slice for `d1`. No file conflict — each writes to its own disk — but **two full source reads and two stored copies**. Bounded by worker count.

**Why it matters:** a BI burst (dashboards refreshing on the same schedule) is exactly a cold-cache, concurrent, same-window workload — the spike the design exists to prevent. Steady state is unaffected (cache warm, no misses).

**Why it isn't fatal:** still strictly better than no cache. Without TRACE every query re-reads the source in full; with TRACE only the cold burst duplicates.

**Fixes:**
- **Per-worker single-flight.** `ConcurrentHashMap<CacheKey, CompletableFuture<Slice>>` — first requester populates, others await. ~20 lines. Fixes the same-worker case and the file race. Must propagate failure and clean up the entry on abort.
- **Cross-worker.** Needs affinity (D2, so a slice always lands on one worker) or a coordinator-level in-flight registry (D1). Neither is in the POC.

**Subtlety:** ADR-002 makes population a **tee off the user's own query**, so a population is coupled to that query's lifetime. If the query is cancelled mid-stream (`LIMIT`, disconnect), the population aborts and the slice is partial (C4). A single-flight waiter must therefore handle "the population I awaited was aborted" and fall back to its own read. That coupling is the main reason not to build coalescing first.

**POC decision:** no coalescing. Accept duplicate cold reads. Instrument them (count source reads per slice) so the cost is measurable before adding machinery.

### S7 — Source-outage cache hits: metadata phase is not stale-tolerant (NEXT)

**Observed in e2e (postgres stopped):** a query whose slice is fully cached still fails. The failure is in the **metadata phase**, before any cache lookup: the delegate's `getTableHandle` touches the source even for handle creation (JDBC in 483), and `getColumnHandles`, `applyFilter` → `resolvePartitionHandle`, and `getTableStatistics` would each open a connection too. Cache *serving* is proven correct — this is purely planning not being able to complete without the source.

**Why the current design can't ride through an outage:** `CachingTableHandle.columnHandles` is write-through only (never read back), every metadata call forwards to the delegate, and handles are created per query/transaction — there is no connector-lifetime memory of a table's layout.

**Fix sketch (connector-level metadata staleness):**
- Keep a connector-level map `SchemaTableName → (delegate handle, column map, last-verified timestamp)`.
- `getTableHandle`: on source failure (or within the staleness window), serve the cached handle instead of failing.
- `getColumnHandles`: serve from the map when present; delegate only on miss.
- `applyFilter` → `resolvePartitionHandle`: use the cached column map instead of a second delegate call.
- `getTableStatistics`: return empty/estimated stats for cached-table scans (no source round-trip).
- Invalidate on version change — natural fit for ADR-007 `versioned` mode (the existing freshness story); `immutable` mode can hold entries for the slice's lifetime.

**Scope guard:** hit-serving (page source) needs no change — only the coordinator-side planning calls. Write paths stay strict (no stale metadata for DML). Confirm the JDBC `getTableHandle` DB round-trip before building (it was observed, not source-verified).

**Decision needed:** implement as the next increment, or defer behind the freshness-mode work.

---

## Decisions log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-09-26 | ADR-001: cache full partition, all columns | Query results not reusable; projections vary |
| 2026-09-26 | ADR-002: widen-on-miss | No second execution path; simplest correct populate |
| 2026-09-26 | Fork 3: routing must be connector-level | Plugin SPI has no analyzer-rule hook (verified) |
| 2026-09-26 | Target pinned to Trino 483 (latest release) | User requirement |
| 2026-09-26 | ADR-003 rejected: blob-cache SPI is master-only, absent in 483 | Verified: spi/cache 404s at tag 483 |
| 2026-09-26 | ADR-006: TRACE-owned storage, in-memory first | 483 has no blob-cache SPI |
| 2026-09-26 | C1/C2/C3 mechanism confirmed on 483 | `ConstraintApplicationResult` present at tag 483 |
| 2026-09-26 | D5 resolved: granularity, not bucketing | Hash bucketing = N× amplification; POC: no bucketing |
| 2026-09-26 | ADR-007: freshness is a mode (`immutable`/`versioned`) | `immutable_after` as a TTL silently serves stale rows |
| 2026-09-26 | ADR-008: lakehouse out of scope | Alluxio/Rubix already cache file bytes for Iceberg/Delta/Hive |
| 2026-09-26 | C5 resolved | Freshness mode + per-slice version |
| 2026-09-26 | ADR-009: cache raw, engine enforces access control | RLS/masks are SQL expressions applied as plan nodes above the scan |
| 2026-09-26 | C7 resolved: no partition predicate → pass through uncached | No slice to prune; delegate handles it |
| 2026-09-26 | D3: no coalescing in POC, instrument cold reads | Widen-on-miss couples population to a user query; coalescing is fragile |
| 2026-09-26 | Reframe: ADRs are the target; POC is an explicit subset | User is finalizing target architecture |
| 2026-09-26 | ADR-005 rewritten: affinity by consistent hashing, registry optional | Removes the registry as a correctness dependency |
| 2026-09-26 | ADR-010: SIEVE per worker | TRACE owns eviction |
| 2026-09-26 | ADR-011: persist and validate, do not wipe | Version-in-key already makes stale entries unreachable |
| 2026-09-26 | ADR-012: TRACE subdivides coarse delegate splits | base-jdbc emits one split per scan; hash buckets not TupleDomain-expressible |
| 2026-09-26 | ADR-004: version-in-key | BlobSource immutability contract mandates it |
| 2026-09-26 | ADR-005: per-worker read-through, no registry | POC scope cut; defer D1-D3 |
| 2026-09-26 | ADR-005 rewritten: affinity via engine `getAffinityKey()`, local mode only | 483 provides `getAffinityKey`; no ring or registry needed |
| 2026-09-26 | ADR-006: storage = one path + `cache.shared` flag | Local and shared are the same code; one boolean toggles affinity + eviction |
| 2026-09-26 | Shared store is the recommended production mode | Removes affinity, duplication, per-worker eviction |
| 2026-09-26 | ADR-004 rewritten: key is TRACE-owned `[table, sliceId, bucket, version]` | Blob-cache SPI is master-only; drop its terminology |
| 2026-09-26 | S2 resolved (split logic lives in `getSplits`); S6 resolved (Parquet) | Stale after target moved to 483 |
| 2026-09-30 | S4 resolved: delegate resolved reflectively by class name; no connector artifacts at build time | Trino stopped publishing connector plugins to Maven Central from 477; reflection removes the build-time dependency |
| 2026-09-30 | Packaging: copy the delegate jars from the cluster's own plugin dirs into the wrapper's plugin dir at deploy time; version guard via `ConnectorContext.getSpiVersion()` | Delegate classes must live in the wrapper's plugin dir (classloader isolation); copying from the cluster means version-matched by construction; plugin is version-locked by design |
| 2026-09-30 | CI: add a build job (compile + package) | Deferred — no CI yet |
| 2026-10-04 | C4 resolved (POC): slice published only on clean exhaustion | `PopulatingPageSource` buffers raw pages and publishes when the delegate reports finished (idempotent, covers both null-poll and isFinished-stop); close-before-finish and over-cap populate discard |
| 2026-10-04 | E2E verified on Trino 483 + Postgres (docker, coordinator + 1 worker): wire serialization, miss/populate/publish, hits across projections/reorder/0-col/SELECT *, passthrough for un-partitioned scans | Nested delegate split/handle + `JdbcColumnHandle` equality survive the coordinator→worker round-trip; `getSplits passthrough` observed for no-partition query (C7) |
| 2026-10-04 | Wrapper declines handle-rewriting pushdowns for caching handles (`applyProjection`, `applyAggregation`, `applyLimit`, `applySample`, `applySort`, `applyOffset`, `applyRowFilter`, `applyTableScanRedirect` → `Optional.empty()`) | Engine calls `applyProjection` before `applyFilter`; forwarding let the JDBC delegate return a raw handle that bypassed the wrapper for all later calls (no push, no cache). Declining keeps rewrites above the scan, keeps slices full-row, prevents cache poisoning via limit/aggregate pushdown. Ceiling: cached-catalog queries lose pushdown into the source (POC-accepted) |
| 2026-10-04 | Connector name finalized: `cache_wrapper` (README updated to match) | Code is authoritative; e2e validated the code's name |
| 2026-10-04 | NEXT: metadata staleness for source-outage hits (S7) | See S7 below |
| 2026-10-10 | S6 (Parquet storage) landed: `ParquetSliceWriter`/`TrinoParquetSchema`/`TrinoParquetValueConverter`; config `cache.shared.memory` + `cache.storage.directory`; slices persist as Parquet under the configured dir, shared across workers via a bind mount; e2e 14/14 (joins, masking, parquet-file assertions) | Parquet implementation on top of the 28-test baseline; decline-set fix preserved; 38 unit tests green |
| 2026-10-11 | ADR-013: per-table slice grain, opt-in, JSON-array config; multi-column grains for Oracle composite partitioning; canonical partition rendering for cross-worker determinism | Catalog-wide column is an invalid assumption; wrong-grain caching is silently expensive; unstable toString order would break shared-cache hits |
| 2026-10-11 | ADR-014: memory tier dropped, the disk is the cache (FileSliceStore is the only store; populate streams to Parquet; OS page cache is the memory tier) | Long-lived heap pages compete with query memory invisibly to Trino's memory manager and cause old-gen GC pressure; per-query rebuilds are young-gen; the kernel page cache provides memory-speed reads with OS-managed eviction |
| 2026-10-04 | Column layout fixed by sorted column names (`cachedChannelOrder`) | Map iteration order is not deterministic across transactions; sorted keys keep the cached page layout identical for hits and populate reads |
| 2026-10-04 | applyFilter bails to uncached when the delegate partially applied the partition constraint | Cached slices assume the delegate applied the partition predicate fully; merging delegate leftovers deferred |

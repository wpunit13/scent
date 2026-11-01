# Implementation Plan: SCENT caching connector

## Overview

SCENT is a Trino connector wrapper that caches full partition slices (all columns) in a worker-local
store and serves repeat queries without re-reading the JDBC source. The design is fully specified in
`docs/ISSUES.md` (ADRs 001–012); this plan sequences the remaining work from the verified POC to the
ADR target architecture.

**Where we are (verified this week, e2e 14/14):** correctness core is done — partition-predicate
pushdown, full-row slice populate on miss, projection/reorder/0-column reads from cache, passthrough
for un-partitioned scans, wire serialization coordinator→worker, and a Parquet *writer* (write-only).
**What is not done:** everything that makes the cache durable, correct under staleness, parallel, and
production-ready — the ADR target.

## Current state vs target (gap map)

| Capability | ADR | State |
|---|---|---|
| Full partition slice, all columns | 001 | ✅ done + verified |
| Widen-on-miss populate (tee) | 002 | ✅ done + verified |
| Raw-slice caching, engine enforces RLS/masks | 009 | ✅ by design (C6) |
| Lakehouse sources out of scope | 008 | ✅ scope decision |
| Content version in the cache key | 004 | ❌ key is `table\|domain-string`, no version/bucket |
| Durable storage (`FileSliceStore`) + Parquet read-back | 006 | ❌ parquet is write-only; in-memory is the only read source |
| Persist + validate on startup, do not wipe | 011 | ❌ files never read back → restart cold-starts |
| Freshness modes (immutable/versioned) | 007 | ❌ no staleness handling at all |
| Metadata survives source outage | S7 | ❌ cached hits still fail at `getTableHandle` when source down |
| Subdivide coarse JDBC splits (buckets) | 012 | ❌ one split per partition → single-worker |
| Affinity (`getAffinityKey`) local mode | 005 | ❌ plain read-through, no affinity |
| SIEVE eviction | 010 | ❌ FIFO size-cap only |
| Single-flight on concurrent populate | D3 | ❌ accepted duplicate cold reads |
| Success criteria / benchmark | P2 | ❌ no measurable targets |

## Architecture decisions (settled — do not re-litigate)

- **ADR-004** version-in-key is the correctness backbone: staleness is handled by key rotation, never by
  explicit invalidation. Everything (ADR-007 freshness, ADR-011 lifecycle, ADR-010 eviction) builds on it.
- **ADR-006** storage is one path (`cache.root`), toggled by `cache.shared`; two store impls
  (`InMemorySliceStore` POC, `FileSliceStore` target). Slices are Parquet.
- **ADR-007** freshness is a per-table mode (`immutable`/`versioned`), not a TTL.
- **ADR-012** bucket subdivision is the only mechanism that makes a JDBC partition parallel — not optional
  at scale, but it's the deepest change and sits late in the sequence.

## Success criteria (Phase 1 bar, POC-meaningful)

- **Hit rate.** After a partition warms, a repeated partition-filter query is served from cache with no source read. Target 100% of repeated queries hit.
- **Source reads.** One cold partition produces exactly one source read. N repeats produce N cache hits and 0 further source reads.
- **Correctness.** Cached results are identical to source results for projection, reordering, 0-column, and full-row reads. The e2e suite pins this today with literal values (300, 7425.0, 100).
- **Latency.** Deferred. The 100-row e2e dataset cannot separate cache from source latency. Measured only after Phase 3 on a realistic table.

Measured by `e2e/benchmark.sh`, which reports source-read and cache-hit counts from the worker logs.

## Phases

Dependency order is bottom-up: key (004) → storage (006/011) → freshness (007); source-outage (S7) is
independent; parallelism/affinity/eviction (012/005/010/D3) come last.

### Phase 1 — Baseline & measurement

Lock scope and get a measurable yardstick before building more.

**Task 1: Define success criteria + a benchmark harness.**
- Description: write latency/hit-rate/source-read targets and a repeatable e2e workload that measures them.
- Acceptance: a documented number per target; a script that runs the workload and prints hit-rate + source reads.
- Verification: `e2e/e2e.sh` still green; harness runs end-to-end.
- Dependencies: none.

**Task 2: Align config keys to ADR-006.**
- Description: rename `cache.storage.directory`→`cache.root`; drop the misnamed `cache.shared.memory` flag, since presence of `cache.root` now enables Parquet persistence.
- Acceptance: config keys match ADR-006; e2e + README updated.
- Verification: e2e green; grep finds no stale keys.
- Dependencies: none.

**Task 3: Align README scope to the ADRs.**
- Description: drop "partial cache-hit" from README In-Scope (ADR-001 settled whole-slice); reconcile the out-of-scope list.
- Acceptance: README no longer promises partial hits; in-scope/out-of-scope matches the ADRs.
- Verification: README diff shows the removal; no other references to partial-hit.
- Dependencies: none.

### Checkpoint: Phase 1
- [x] Success criteria written and measurable
- [x] Config keys consistent with ADR-006
- [x] README scope aligned with ADRs

### Phase 2 — Content-addressable cache key (ADR-004)

**Task 4: Replace the string slice key with a structured `CacheKey(table, partition, version)`.**
- Description: introduce a typed key; `version` defaults to `none`. Carry it in `CachingTableHandle`/`CachingSplit`
  instead of deriving from `TupleDomain.toString()` (the current `ponytail:`-marked fragile string).
- Acceptance: key is structured, versioned, and `equals`/`hashCode` correct; no behavior change with `version:none`.
- Verification: unit tests for key equality + round-trip; e2e green.
- Dependencies: none.

**Task 5: Thread version into slice identity and the parquet filename.**
- Description: parquet path becomes `<root>/<table>/<slice>/<version>.parquet` (or filename embeds version).
- Acceptance: files are version-addressed; two versions of a slice don't collide.
- Verification: `ParquetSliceWriterTest` + `InMemorySliceStoreTest` updated; e2e green.
- Dependencies: Task 4.

### Phase 3 — Durable storage (ADR-006 + ADR-011)

**Task 6: Parquet read path (`FileSliceStore`) — serve hits from disk.**
- Description: add an Avro/Parquet reader and a store that reads a slice from `<root>` when not in memory,
  falling back to in-memory first. Keep `InMemorySliceStore` as the in-memory front.
- Acceptance: a slice written then evicted (or on a fresh worker) is readable from disk with identical rows.
- Verification: round-trip unit test (write→read→compare pages); e2e restart test.
- Dependencies: Task 5.

**Task 7: Startup index — scan, validate, do not wipe (ADR-011).**
- Description: on worker start, scan the cache dir to rebuild the slice index; drop entries whose version is
  no longer accepted; keep the rest (no cold start for `immutable` slices).
- Acceptance: after restart, cached slices survive and are served without a source read.
- Verification: e2e restart step (stop/start worker, query, assert no new `cache miss`).
- Dependencies: Task 6.

### Checkpoint: Phase 3
- [x] Cache survives a worker restart; parquet is actually read, not just written
- [x] Stale-version entries are dropped on start

### Phase 4 — Source-outage resilience (S7)

**Task 8: Connector-level metadata cache so cached hits survive a source outage.**
- Description: keep `SchemaTableName → (delegate handle, column map, timestamp)` at connector scope; serve
  `getTableHandle`, `getColumnHandles`, and `resolvePartitionHandle` from it on source failure (or within a
  staleness window); stub `getTableStatistics` for known cached tables.
- Acceptance: with postgres stopped, a fully-cached slice query still returns correct results.
- Verification: e2e adds a `docker compose stop postgres` step followed by a hit assertion.
- Dependencies: none (parallelizable with Phase 2–3).

### Phase 4.5 — Durable planning notes (S7 follow-on)

**Task 17: Lazy file sidecars for planning metadata.**
- Description: the T8 metadata cache is in-JVM only and vanishes on restart, so a restart during a
  source outage fails even fully cached queries until the source returns. Persist a small JSON note
  per table (delegate handle, columns, types, schema shape) inside the table's cache directory, and
  a note per slice (the delegate's filtered handle for that partition domain) beside the slice's
  Parquet file. Written on change, not per query; read lazily on in-memory miss at the existing
  fallback points (never at boot, so startup cost is unchanged). The coordinator already mounts the
  cache directory, so no infra change is required. No store interface: files are the only tier
  (decided 2026-10-18; a remote store such as Redis adds a failure domain and latency without a
  benefit in single-coordinator local mode). A failed note write is logged and swallowed. Trust in a
  note follows Phase 5 freshness once it exists.
- Serialization findings (2026-10-18, spike): delegate objects are stored as {type, json} envelopes
  read back through their own class. A globally polymorphic mapper misreads their delegating
  creators. The JdbcTableHandle note keeps only the plain relation shape (schema, table,
  remote names, authorization) rebuilt via reflection, because the full creator graph needs the
  engine's Block serde; the schema shape is stored as SQL type strings and rebuilt through the
  TypeManager. Deviation: the slice note does not carry the split batch — delegate split objects
  need the same engine serdes, so the split batch stays in-JVM (OutageSafeSplitManager replays it
  within the same coordinator lifetime; after a restart, split generation leans on the delegate's
  getSplits being source-free, which holds for postgres).
- Acceptance: with postgres stopped and the coordinator restarted since the last successful query, a
  fully cached slice query still returns correct results (served from notes plus disk).
- Verification: unit tests for note round-trip with real JdbcTableHandle/JdbcColumnHandle classes,
  merge semantics, unreadable/missing/disabled notes, and a full restart simulation; e2e outage step
  extended with a coordinator restart between populate and the outage query.
- Dependencies: Task 8 (done). Must land before Phase 5 flips the default to cache-first.

### Phase 5 — Freshness (ADR-007)

**Task 9: `version:none` + refresh TTL (bounded staleness MVP).**
- Description: implement the honest "cache within `refresh`, accept staleness" mode first — a per-slice
  TTL that expires the key. Lowest-risk entry to freshness.
- Design update (2026-10-18): the same TTL governs planning metadata (T8 cache + T17 notes). The
  connector flips to cache-first: fresh entries plan with zero source contact; expired entries refresh
  from the source; a failed refresh fails the query, which subsumes the outage fallback as a general
  mechanism. Criterion 3 restated: cache-first with a bounded clock, not a freeze.
- Landed 2026-10-19. Config: per-table `cache.refresh.<schema>.<table>` (airlift Duration; unset means
  planning always contacts the delegate and slices never expire by time). Planning: CachingMetadata
  serves fresh entries (memory, or the table note after a restart) without delegate contact; stale or
  unknown entries go to the delegate and a failure fails the query — the T8 fallback machinery was
  deleted. Slices: FileSliceStore expires a slice past its table's window by file mtime, so the next
  query repopulates it in place. Correctness guard found by the e2e run: a delegate handle rebuilt
  from a note has lost the delegate-side predicate, so CachingTableHandle carries a
  delegateHandleTrusted flag — note-rebuilt handles may serve cache hits but the page source refuses
  to drive a delegate read with them; applyFilter upgrades an untrusted remembered handle through the
  delegate whenever the same cycle proves the delegate reachable. Serialization constraint: the handle
  keeps exactly one @JsonCreator (the 6-arg form carrying the flag); two annotated creators broke the
  engine's typed Jackson module and failed every query at TaskUpdateRequest serialization.
- Acceptance: a slice expires after `refresh` and is re-populated on next query.
- Verification: unit tests for TTL expiry, cache-first planning, stale-entry failure, and the trust
  upgrade; e2e with `cache.refresh.public.metrics=3m` covering outage-without-contact and
  refresh-after-expiry.
- Dependencies: Task 4, Task 17.

**Task 10: `version:max_updated_at` + `immutable`/`versioned` modes.**
- Description: per-slice `SELECT max(updated_at) …` version acquisition, `check: sync|refresh`, and the
  `immutable` (lateness) mode with optional verification query.
- Acceptance: a mutating partition invalidates via key rotation; `immutable` tables stay stable.
- Verification: integration test with a source-side update between queries.
- Dependencies: Task 9.

### Phase 6 — Local-mode production hardening

**Task 11: Bucket subdivision of coarse JDBC splits (ADR-012).**
- Description: query `min/max` of a range column per slice; emit N range-domain splits when the row estimate
  justifies it. Cache unit becomes `(slice, bucket)`.
- Fallback variant (decided 2026-10-11): for tables with no source-prunable column, subdivide by stream
  position instead. Roll the write session at the per-slice cap into numbered chunks
  (root/table/partition/version-N.parquet), cached incrementally, read back concatenated. Chunks are not
  addressable and give no parallelism, so range buckets remain the primary mechanism; chunking covers the
  tables range buckets cannot serve. Either way, bound the chunk or bucket count per slice so disk stays
  finite.
- Acceptance: a large JDBC partition splits into N workers; a small one does not.
- Verification: e2e shows >1 split for a big partition; unit tests for boundary math.
- Dependencies: Task 4 (key carries bucket), Task 15 (streaming session provides the roll point).

**Task 12: Affinity via `getAffinityKey` (ADR-005, local mode).**
- Description: emit `(slice, bucket)` affinity key so the engine routes a bucket to the same worker.
- Acceptance: repeated query lands on the same worker (cache reuse) in local mode.
- Verification: e2e asserts reuse across queries; log evidence of same worker.
- Dependencies: Task 11.

**Task 13: Disk eviction (SIEVE over cache files, ADR-010).**
- Description: with the memory tier gone (ADR-014), eviction operates on the cache directory: SIEVE over
  slice files, flipping a visited bit on hit, scanning the hand when the disk exceeds its quota. A total disk
  quota config arrives with this task.
- Acceptance: eviction happens under the disk quota with bounded cost; hit path stays lock-light.
- Verification: unit test for SIEVE behavior over files; e2e with a tiny disk quota.
- Dependencies: Task 15 (disk-only store).

**Task 14: Single-flight populate (D3).**
- Description: `ConcurrentHashMap<CacheKey, CompletableFuture<Slice>>` so concurrent misses on the same slice
  coalesce; propagate failure; fall back to own read on abort.
- Acceptance: N concurrent cold queries produce one source read.
- Verification: unit test with concurrent threads; instrumented source-read count = 1.
- Dependencies: Task 6.

**Task 15: Disk-only store — FileSliceStore replaces the memory tier (ADR-014).**
- Decision: the user confirmed the memory tier is not viable in a Trino worker (long-lived heap pages are
  invisible to the memory manager and cause old-gen GC pressure; the OS page cache is the memory tier). The
  the overCap question (publishIfComplete) dissolves: there is no memory cap to hit.
- Description: `InMemorySliceStore` becomes `FileSliceStore`, the only implementation. put writes Parquet;
  get reads Parquet and rebuilds pages per query. The in-memory map, byte-cap eviction loop, and read-through
  duality are deleted. The populate tee streams: each raw page goes to the Parquet session as it arrives, the
  projected page goes to the engine, and the session completes on clean exhaustion or aborts (deleting the
  temp) on early close (C4 preserved). The writer grows a session form (open/write/complete-or-abort).
- Config: `cache.max-bytes` becomes a per-slice guard (skip caching slices larger than the cap).
- Acceptance: hits are served from Parquet with correct rows; a cancelled populate leaves no temp file; the
  restart-survival checks still pass.
- Verification: unit tests (store round-trip, abort-on-close, oversize slice skipped); e2e full suite
  including restart survival; benchmark harness for hit rate and source reads.
- Dependencies: Task 6 (done). Compression codec choice (SNAPPY or none) is a read-speed knob to evaluate
  with the harness.

**Task 16: Per-table partition columns (CRITICAL — decided 2026-10-11, next after Phase 3).**
- Decision: confirmed. A catalog-wide `cache.partition-column` is an
  invalid assumption; the partition grain is a per-table property. This supersedes the README's Out-of-Scope
  "configurable caching policies" entry, which must be updated with the change.
- Config: `cache.partition-column.<schema>.<table>=<JSON array>` (for example
  `cache.partition-column.public.sales=["day","region"]`), because real partitioning is hierarchical (Oracle
  range partition + list/hash subpartition) or multi-column. The list defines the table's slice grain. Tables
  without a configured line are never cached (opt-in; decided, see ADR-013). The catalog-wide key is removed
  (clean break).
- Value format: a JSON array, not a bare comma list. Legacy column names can contain commas or spaces, which
  a comma list cannot express; Jackson is already a dependency; malformed values fail fast at catalog creation.
  Rejected: YAML (wrong substrate for catalog properties), repeated properties keys (Java Properties collapses
  duplicates), bare comma lists (ambiguous with legacy names).
- Semantics: every configured column whose domain appears in the query's constraint contributes to the slice;
  the slice is the conjunction of the domains present. A query constraining none of them is uncached
  passthrough (existing path). This generalizes today's single-column behavior exactly.
- Trade-off (documented): mixed-grain query patterns store overlapping slices ({day} and {day,region} are
  distinct keys). A stricter all-columns-required gate is a one-line change if duplication bites.
- Determinism fix required: with multiple columns, the partition's string rendering must be canonical
  (domain entries sorted by column), or worker A and worker B compute different directory names for the same
  slice and the shared cache never hits across workers. In-memory equality is unaffected
  (TupleDomain.equals is order-independent). CacheKey gains a canonical rendering used by fileFor.
- Implementation: factory parses the map (warn on malformed); CachingMetadata loops the configured columns
  and pushes the conjunction of present domains; CacheKey canonical rendering; README + e2e (two tables with
  different grains, one multi-column, plus an unconfigured table staying uncached); unit tests for parsing,
  multi-column keys, partial-grain caching, and the unconfigured fallback.
- Acceptance: two tables in one catalog cache under different partition columns (one multi-column); an
  unconfigured table stays uncached.
- Verification: e2e + unit tests as above.
- Dependencies: none.

### Checkpoint: Complete
- [ ] All acceptance criteria met
- [ ] e2e suite covers: restart survival, source outage, staleness, parallelism, reuse

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Parquet read-back changes hit latency (disk vs memory) | Med | In-memory front (Task 6); measure in Phase 1 harness |
| `getAffinityKey` placement miss → source read, not peer read | Med | Accepted by ADR-005; reuse measured, not correctness |
| `max_updated_at` version adds a source round-trip per slice | Med | `check: refresh` background mode; batchable per window (ADR-007) |
| Bucket subdivision on non-indexed columns → N× amplification | High | Range domains only (ADR-012); never hash buckets |
| Docs drift (README "partial hit" vs ADR whole-slice) | Low | Resolved: dropped partial-hit from README in Task 3 |

## Decisions (resolved 2026-10-10)

1. **Partial cache-hit** — dropped. Whole-slice is the settled design (ADR-001). README updated in Task 3.
2. **Shared vs local first** — build local mode through Phase 6; shared arrives later as a config toggle on
   the same `FileSliceStore`.
3. **Docs tracking** — reversed 2026-10-11: `docs/` is now tracked in git (ADRs, tracker, plan, arch).
4. **Config naming** — align to ADR-006 (`cache.root` / `cache.shared`), renaming the earlier keys in Task 2.
5. **Storage evolution (later)** — migrate the slice format from parquet-avro to `io.trino:trino-parquet` +
   `trino-filesystem` (verified available for 483). Removes ~68MB of hadoop jars (94% of the plugin zip) and
   the Avro conversion layer; keeps standard Parquet files. Deferred until the storage layer gets a dedicated
   phase; measure with the T1 harness first. Cache survival across upgrades is already guaranteed by design:
   stable Parquet spec + version-in-key + miss-on-unreadable. Add a format-version marker to the filename or
   directory when this migration happens, so old-format files can be identified and dropped deliberately.
   Rejected for now: Trino-native block dump (fastest, zero deps, but reimplements block internals and loses
   external inspectability; revisit only if profiling shows conversion overhead is a bottleneck).
6. **Per-table partition columns (2026-10-11)** — confirmed: catalog-wide
   `cache.partition-column` is an invalid assumption; the partition column is a per-table property. Critical,
   next after Phase 3. Supersedes the README's Out-of-Scope "configurable caching policies" entry.
7. **Outage posture and planning-notes persistence (2026-10-18)** — T8's fallback fires only on source
   failure (cache as fallback, not freeze). A remote metadata store (Redis/DB) is rejected for local mode:
   one coordinator is the single client, local files beat the network on latency, and a second live service
   inside the outage-resilience path compounds failure domains. Persistence is lazy file sidecars instead
   (Task 17, zero boot cost). Phase 5's TTL becomes the mechanism that flips planning to cache-first;
   the outage fallback becomes the grace window under it. `OutageSafeSplitManager` stays: it never fires
   for postgres (split generation is pure) but covers delegates whose split discovery queries the source.
8. **Cache is an accelerator, not a delegate replacement (2026-10-18)** — the cache exists to shield
   the delegate from Trino's query load and to cut redundant round-trips; it is not a standby copy of
   the delegate, and queries may fail while the delegate is down. Consequences: the T8/T17 outage
   fallbacks are best-effort behavior on top of this contract, not a promise; the split-batch-after-restart
   gap is accepted behavior with no follow-up work planned; and Phase 5 cache-first ships WITHOUT a
   grace window — an expired entry whose refresh fails fails the query, so staleness is bounded by the
   TTL alone and outage resilience for fresh entries comes free from that bound.

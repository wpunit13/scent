# SCENT implementation todo

Queue order: T17 (durable planning notes, next) → Phase 5 (freshness, reshaped to cache-first) → Phase 6.

## Phase 1 — Baseline & measurement
- [x] T1: Success criteria + benchmark harness
- [x] T2: Align config keys to ADR-006 (`cache.root`; dropped `cache.shared.memory`)
- [x] T3: Align README scope to ADRs (drop partial cache-hit)

### Checkpoint: Phase 1
- [x] Success criteria measurable; config consistent; README scope aligned

## Phase 2 — Content-addressable cache key (ADR-004)
- [x] T4: Structured `CacheKey(table, partition, version)`, default `version:none`
- [x] T5: Version in slice identity + parquet filename

### Checkpoint: Phase 2
- [x] Key structured and versioned, no behavior change

## Phase 3 — Durable storage (ADR-006 + ADR-011)
- [x] T6: Parquet read path — serve hits from disk
- [x] T7: Startup index — scan, validate, do not wipe
- [x] T7b: Hierarchical layout root/table/partition/version.parquet (ADR-006, flat-dir scaling fix)

### Checkpoint: Phase 3
- [x] Cache survives worker restart; parquet actually read

## Phase 4 — Source-outage resilience (S7)
- [x] T8: Connector-level metadata cache (outage-safe planning)

## Phase 4.5 — Durable planning notes (S7 follow-on)
- [ ] T17: Lazy file sidecars for planning metadata — table note (handle, columns, schema,
      timestamp) and slice note (filtered handle, last split batch) written next to the data
      they describe, on change only; read on in-memory miss at the existing fallback points.
      No boot-time preload (zero startup cost); no store interface (files are the only tier);
      note trust follows Phase 5 freshness; unreadable note degrades to in-memory fallback.

## Phase 5 — Freshness (ADR-007)
- [ ] T9: `version:none` + refresh TTL
- [ ] T10: `version:max_updated_at` + `immutable`/`versioned` modes

> Design update (2026-10-18): TTL governs planning metadata as well as data slices. Cache
> becomes the first path tried for planning: fresh entries plan with zero source contact,
> expired entries refresh from the source, and a failed refresh serves stale within a grace
> window. That grace window replaces the outage fallback as a special case; T8's per-call
> fallbacks remain as the net under it. Requires T17 (notes) so the cache-first default
> survives a restart.

## Phase 6 — Local-mode production hardening
- [ ] T11: Bucket subdivision of coarse JDBC splits (ADR-012)
- [ ] T12: Affinity via `getAffinityKey` (ADR-005)
- [ ] T13: SIEVE eviction (ADR-010)
- [ ] T14: Single-flight populate (D3)
- [ ] T15: Disk-only store — FileSliceStore replaces the memory tier; populate streams to Parquet (ADR-014)
- [x] T16: Per-table partition columns (CRITICAL — supersedes README out-of-scope entry; ADR-013)

### Checkpoint: Complete
- [ ] e2e covers restart survival, source outage, staleness, parallelism, reuse

## Later — storage evolution
- [ ] Migrate slice format parquet-avro → trino-parquet + trino-filesystem (drops ~68MB hadoop, keeps Parquet files)
- [ ] Format-version marker in filename/directory for deliberate old-file handling

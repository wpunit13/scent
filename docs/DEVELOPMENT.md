# SCENT — Development Guide

Permanent engineering conventions for building and releasing the SCENT Trino plugin. These are guidelines, not one-off decisions; the rationale lives in the ADRs (`ISSUES.md`).

## Module layout (target)

```
scent/
├── connector-core/          # pure business logic — zero SPI knowledge
└── connector-spi-<min>/     # the thin SPI adapter, named by minimum Trino version
```

- `connector-core`: cache key, Parquet read/write, planning notes, eviction, slice store. Never imports an `io.trino.spi` interface it doesn't have to.
- `connector-spi-<min>`: `CachePlugin`, `CachingConnectorFactory`, and the `Caching*` wrapper classes.

`trino.version` is a **build parameter**, not a source folder. One SPI module builds N artifacts, each version-locked:

```
mvn -Dtrino.version=483 package  → artifact locked to 483
mvn -Dtrino.version=484 package  → artifact locked to 484
```

## Naming convention — the folder name is the contract

`connector-spi-<min>` means: *"this adapter compiles against Trino `<min>` and up, until the next module's min."*

There is **no `version → module` map** anywhere. The mapping is derived from folder names. This is deliberate — a map is a second source of truth that drifts.

## Derivation rules

Two constants, declared once (not per version):

- `LATEST_SUPPORTED_TRINO` — the newest Trino version we've validated (ceiling).
- `SUPPORT_WINDOW_SIZE = 6` — how many versions back we support.

From these and the folder names, everything else is derived:

1. `floor = ceiling − (window − 1)`.
2. A module's range is `[min, next_module.min − 1]`; the highest module's range is `[min, ceiling]`.
3. To build for Trino `X`: use the module with the largest `min ≤ X`, build with `-Dtrino.version=X`.
4. A module is deprecated when its `max < floor`.

## Runbook

### Additive Trino release (no SPI break)

Bump `LATEST_SUPPORTED_TRINO`. No new folder. The matrix picks up the new ceiling.

### Breaking Trino release

1. Detect the break — the SPI-signature snapshot diff, or a compile failure when bumping `trino.version`.
2. Create `connector-spi-<new-version>` (copy the previous module, adapt the divergent surface).
3. Bump `LATEST_SUPPORTED_TRINO`.
4. Leave the old module frozen.

### Deprecate the oldest version

When a module's `max < floor`: delete the folder, drop it from the matrix, and widen the version-guard error message.

## When to create a new SPI module

Only on a **non-additive** SPI change — a method removed, a signature changed, a return type changed, or a default method's behavior changed so it must be overridden.

"Trino shipped" is not the trigger. Steady state is **one** SPI module; a second appears only at a breaking change.

## Anti-patterns

- Do not introduce a `version → module` map.
- Do not create a module per Trino release (additive releases don't need one).
- Do not use git branches per Trino line, except as a last resort when a shim can't stay in one source.

## CI

- Unit-test matrix across the full supported window (derived from floor/ceiling + module folders).
- e2e only on the two edges (oldest + newest supported).

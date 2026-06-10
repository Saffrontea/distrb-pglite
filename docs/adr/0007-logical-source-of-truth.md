# ADR 0007: Logical Layer Is The Source Of Truth

## Decision

The logical layer (event log plus replay) is the authoritative source of semantics, including schema changes, which travel as DDL events. Physical CAS snapshots are a transfer optimization with explicit validity preconditions, never the only recovery path:

1. Every manifest records a manifest format version, the PGlite version, and the Postgres catalog version. Mismatches are rejected, not migrated.
2. A `data-only` snapshot records the `ddl_generation` it was taken at. A receiver may apply it only when its own generation matches; otherwise it must replay DDL events to that generation or fall back to a `catalog-and-data` snapshot. Managed pull orchestration checks this fence before the snapshot fence.
3. CAS blocks are shared only between nodes on the same PGlite/Postgres version. Cross-version physical compatibility is a non-goal: version upgrades re-create snapshots instead of migrating manifests.
4. Every node must remain reconstructible from a version-compatible `catalog-and-data` snapshot plus event replay.

## Consequences

- Schema divergence is detected as a generation mismatch that closes the physical fast path; recovery goes through the logical layer.
- A PGlite version bump can never strand data, because the logical rebuild path is an invariant.
- Cross-version manifest compatibility moves from deferred work to an explicit non-goal.
- The data-directory inventory tool should run as a contract test in CI so PGlite upgrades surface layout changes as test failures, not broken snapshots.
- Manifests without version stamps written before this ADR are treated as incompatible.

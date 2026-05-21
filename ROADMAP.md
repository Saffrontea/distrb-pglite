# Distributed Edge DB Roadmap

## Summary

This roadmap starts with a TypeScript + PGlite MVP, then moves toward the larger architecture in `distributed_edge_db.md`: partition-scoped replication, snapshot pull, CoW/CAS storage, and eventually PGlite/Postgres storage-layer changes.

PGlite is treated as a black box only during the early MVP. That is not the final architecture. The later phases explicitly investigate and modify the PGlite/Postgres storage boundary, including VFS-style interception first and smgr-level work after the invariants are proven.

Replication uses source-node-local event streams. Event IDs are UUID v7, but correctness never depends on a global wall clock. Cursors are tracked per source node as `(source_node_id, event_id)`.

## Phase 0: Planning Artifacts

- Add this roadmap.
- Record ADRs for eventual consistency, transport abstraction, Node adapter boundaries, and per-node UUID v7 cursors.
- Acceptance: implementation choices are documented before feature work.

## Phase 1: Project Foundation

- Initialize pnpm workspace.
- Add TypeScript build and test scripts.
- Create `packages/core`, `packages/node`, and `packages/testkit`.
- Acceptance: `pnpm build` and `pnpm test` are the standard validation commands.

## Phase 2: Core Event And Partition Model

- Define `NodeId`, `PartitionId`, `EventId`, `SnapshotId`, and per-node cursors.
- Implement UUID v7 event generation.
- Implement append-only event logs with idempotent apply keyed by `(source_node_id, event_id)`.
- Implement partition subscription filtering.
- Acceptance: duplicate and out-of-order delivery do not corrupt local state.

## Phase 3: Memory Multi-Node Simulation

- Implement `MemoryTransport`.
- Implement a sync node that can subscribe, publish, receive, and catch up.
- Simulate multiple nodes in one process.
- Acceptance: subscribed nodes converge and unsubscribed nodes ignore unrelated partitions.

## Phase 4: Snapshot Pull MVP

- Store snapshot metadata with per-source-node high-watermarks.
- Implement snapshot import/export API shape.
- Replay events after each high-watermark.
- Acceptance: stale nodes recover from snapshot plus event replay.

## Phase 5: WebSocket Transport

- Add a WebSocket transport behind the same core `Transport` interface.
- Keep wire messages transport-neutral.
- Add Node CLI commands for hub and node processes.
- Acceptance: WebSocket transport passes the same behavioral sync scenarios as memory transport.

## Phase 6: Demo And Docs

- Add a tenant-scoped demo schema or sensor-reading demo.
- Demonstrate local write, local read, partition subscription, offline recovery, and WebSocket sync.
- Provide Node CLI commands for write, list, and catch-up.
- Link the MVP behavior back to `distributed_edge_db.md`.
- Acceptance: `distrb-pglite demo` and the Phase 6 integration test exercise the same PGlite + WebSocket scenario.

## Phase 7: PGlite File Boundary And Snapshot Inventory

- Map how PGlite persists its data directory across Node FS, IndexedDB, OPFS, and custom filesystem hooks.
- Build an inventory tool that lists the files and logical snapshot inputs for a PGlite data directory before and after writes.
- Define the minimum stable file-level contract needed for snapshot export/import without relying on global process state.
- Acceptance: a test can create a PGlite database, write data, inventory the data directory, copy/import it, and prove the imported node can read the same application data and event metadata.

## Phase 8: External CoW/CAS Prototype

- Implement a storage adapter outside PGlite that chunks data-directory files into content-addressed blocks.
- Track manifests per partition snapshot with block hashes, sizes, and parent snapshot references.
- Implement diff transfer as manifest comparison plus missing block fetch.
- Keep this phase outside PGlite internals to validate snapshot, GC, and replication invariants before maintaining a fork.
- Acceptance: two snapshots with overlapping data reuse blocks, transfer only missing blocks, and reconstruct a readable PGlite data directory.

## Phase 9: Partition-Aware Snapshot Semantics

- Move from whole-data-directory snapshots toward partition-scoped snapshot catalogs.
- Record snapshot high-watermarks as `{ source_node_id -> event_id }` and tie them to manifest roots.
- Define how `PartitionId` maps to storage identity: hypertable OID, relation OID, relfilenode, database OID, and tablespace OID.
- Make snapshot scope explicit: `data-only` or `catalog-and-data`.
- Acceptance: a node can pull a partition snapshot, replay only events after the per-source high-watermarks, and avoid receiving unrelated partitions.

## Phase 10: PGlite Fork / Custom Filesystem Integration

- Define a small CAS-backed filesystem contract before binding to PGlite internals.
- Create a PGlite integration branch or package that injects the CoW/CAS storage layer at the closest supported filesystem boundary.
- Replace opaque directory copying with a managed snapshot filesystem adapter.
- Add failure tests for interrupted writes, partial block availability, duplicate block fetches, and manifest corruption.
- Acceptance: the same sync and snapshot tests pass when PGlite persists through the CoW/CAS-backed filesystem adapter.

## Phase 11: Postgres smgr Research Track

- Study where PGlite’s embedded Postgres build exposes or can be patched around storage manager behavior.
- Identify the smallest useful smgr/TableAM/WAL interception point for page/block-level CoW.
- Compare three implementation paths: PGlite filesystem adapter, Postgres extension, and patched Postgres/smgr fork.
- Document the current PGlite public filesystem boundary before patching internals.
- Acceptance: produce a design note with concrete patch points, build implications, test strategy, and a decision on whether smgr-level work is justified.

## Phase 12: smgr-Level CoW Experiment

- If Phase 11 justifies it, prototype a patched storage manager path that writes page/block changes into content-addressed extents.
- Preserve local ACID behavior and let the distributed layer consume snapshots/diffs from the storage layer.
- Validate compatibility with PGlite’s WASM build constraints.
- Acceptance: a patched build can run core SQL workloads, produce deterministic block manifests, restore from manifests, and pass crash/recovery smoke tests.

## Deferred

- Custom WASM beyond PGlite.
- MQTT transport adapter.
- Production-grade GC and vacuum policy.
- Full Postgres extension packaging.
- Cross-version manifest compatibility.

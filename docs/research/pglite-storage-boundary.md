# PGlite Storage Boundary Research

## Current Public Boundary

The installed PGlite package exposes a `Filesystem` option on `PGliteOptions`. The public type surface includes:

- `PGliteOptions.fs?: Filesystem`
- `PGliteOptions.loadDataDir?: Blob | File`
- `PGliteOptions.fsBundle?: Blob | File`
- `PGliteInterface.dumpDataDir()`
- `Filesystem.init`, `syncToFs`, `initialSyncFs`, `dumpTar`, `closeFs`
- `BaseFilesystem`, which models a Node-like file API: `open`, `read`, `write`, `rename`, `truncate`, `readdir`, `stat`, etc.

This means the next integration point should be PGlite custom filesystem injection, not smgr. It is closer, testable from TypeScript, and does not require a Postgres/PGlite fork.

## Recommended Path

1. Implement a PGlite-compatible filesystem adapter that wraps the `CasSnapshotFilesystem` contract.
2. Preserve normal mutable file semantics during a running PGlite instance.
3. Convert the adapter state into CAS manifests only at explicit snapshot boundaries.
4. Use `syncToFs` / `initialSyncFs` as the first durability hooks.
5. Keep `dumpDataDir()` compatibility for debug and migration.

## Partition Identity Must Be Storage-Level

`PartitionId` cannot remain only an application label. For filesystem/CAS diff sync, it must bind to Postgres storage identity:

- root relation or logical partition OID,
- database OID,
- tablespace OID,
- relation OID,
- relfilenode when available,
- snapshot scope: `data-only` or `catalog-and-data`.

TimescaleDB's catalog-backed chunk model is a useful analogy, but the prototype uses the generic term root relation. The application chooses a partition key, but the storage layer needs a catalog-backed mapping from that partition to concrete relation files/pages.

Snapshot scope must be explicit:

- `data-only`: transfer relation data pages when catalog/schema compatibility is already guaranteed.
- `catalog-and-data`: transfer catalog pages plus data pages when a node needs partition definitions, DDL changes, or bootstrap state.

Without this binding, a filesystem-level CAS layer cannot decide which blocks belong to a partition or whether catalog changes must accompany data changes.

The partition snapshot path now uses this binding during CAS acquisition:

- `data-only` includes only files matching `base/<databaseOid>/<relfilenode>` plus segment and fork variants such as `.1`, `_fsm`, `_vm`, and `_init`.
- `catalog-and-data` keeps the full closed data directory in this prototype, including empty directories, because that is the restorable bootstrap format until catalog-page selection is made precise.
- a snapshot request is rejected if the requested `partitionId` and `storageBinding.partitionId` differ.

The Node adapter can now discover this binding from PGlite catalog state. `discoverStoragePartitionBinding` reads `pg_class`, `pg_database`, `pg_relation_filenode()`, and `pg_relation_filepath()` for a relation name such as `public.sensor_readings`. The resulting `storagePath` is preferred by the snapshot filter so CAS acquisition is tied to the actual relation file path rather than only reconstructing a path from OIDs.

The Phase 9 pull path is now explicit for `catalog-and-data` snapshots: restore the snapshot CAS manifest into a new PGlite data directory, open the restored node, then replay only events for the snapshot partition after the per-source high-watermark. This keeps unrelated partition events out of replay even when the source has advanced other partitions after the snapshot.

## smgr Track

smgr-level work remains a research track, not the next implementation step. Before smgr, finish the storage catalog plan in `pre-smgr-storage-catalog.md`.

It becomes justified only if the filesystem adapter cannot provide:

- consistent snapshot boundaries without stopping or quiescing PGlite,
- acceptable diff granularity,
- enough information for GC/vacuum coordination,
- crash recovery semantics compatible with PGlite’s embedded Postgres build.

If those limits appear, the smgr research should identify concrete patch points in the embedded Postgres storage manager and compare them against a TableAM or WAL-oriented approach.

## Decision

Proceed with custom filesystem integration first. Do not fork PGlite or patch smgr until the CAS filesystem adapter has failed a concrete acceptance test.

## Adapter Progress

`PGliteCasFilesystem` now implements the Node-like synchronous methods required by PGlite's `BaseFilesystem` shape and can snapshot/restore its state through CAS blobs. The next step is to pass it into `new PGlite({ fs })` and close any compatibility gaps against PGlite's runtime expectations.

The adapter now runs the same partition snapshot restore/replay flow as the host data-directory path for `catalog-and-data` snapshots. A PGlite database can run on `PGliteCasFilesystem`, produce a filesystem snapshot, restore that snapshot into a second adapter, and replay only same-partition events after the snapshot high-watermark. Restore also fails explicitly when required CAS blocks are unavailable or the filesystem manifest is corrupted.

## Phase 7 File Snapshot Contract

The early file-boundary snapshot contract is intentionally conservative: export only a closed or otherwise quiesced PGlite data directory. The inventory records each regular file by relative path, size, and sha256. A copied import is valid only when the target inventory exactly matches the source inventory before the imported database is opened.

The Phase 7 integration test creates both application data and distributed event metadata, closes the source PGlite instance, copies the data directory, opens the copy, and verifies that both the application table and `distrb_events` high-watermark metadata are readable. This validates the black-box file boundary before moving the same invariants into CAS manifests and custom filesystem work.

## Phase 8 External CAS Contract

The external CAS prototype keeps PGlite unchanged. A snapshot manifest records a snapshot id, parent snapshot ids, empty directories, file hashes, and content-addressed blocks. A receiver compares the next manifest with its local CAS block set, copies only missing blocks, restores the data directory, and opens the restored PGlite database.

The restore path must preserve empty directories as well as file content. PGlite can abort during startup if required runtime directories are missing even when every regular file hash matches.

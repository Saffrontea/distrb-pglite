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

## smgr Track

smgr-level work remains a research track, not the next implementation step.

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

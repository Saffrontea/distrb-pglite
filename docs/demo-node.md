# Node Demo

This demo exercises the current Node adapter layer: PGlite persistence, WebSocket transport, local writes, local reads, and pull catch-up.

## Setup

```bash
mise exec -- pnpm install
mise exec -- pnpm build
```

## Run The End-To-End Demo

The single-process demo starts a hub, writes a `tenant:jp` sensor event through WebSocket sync, verifies a `tenant:us` subscriber ignores it, performs an offline local write, and then recovers the missing event through catch-up.

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite demo \
  --dataDir ./.data/phase6-demo
```

## Start A Hub

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite hub --port 8787
```

## Keep A Subscribed Node Running

In another terminal:

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite node \
  --node node-b \
  --partition tenant:jp \
  --url ws://localhost:8787
```

## Write From Another Node

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite write \
  --node node-a \
  --partition tenant:jp \
  --payload '{"title":"hello from node-a"}' \
  --url ws://localhost:8787
```

## Read Local Event Logs

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite list \
  --node node-a \
  --partition tenant:jp
```

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite list \
  --node node-b \
  --partition tenant:jp
```

## Offline Write And Pull Catch-Up

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite write \
  --node node-a \
  --partition tenant:jp \
  --payload '{"title":"offline event"}' \
  --offline
```

With the hub running and `node-a` reachable, another node can pull missing events:

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite catchup \
  --node node-c \
  --peer node-a \
  --partition tenant:jp \
  --url ws://localhost:8787
```

The CLI stores PGlite data under `./.data/<node-id>` by default.

## Inspect PGlite Data Directory

Phase 7 starts by treating the PGlite data directory as the snapshot boundary. The inventory command lists every regular file with size and sha256.

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite inventory \
  --node node-a
```

## Copy A Closed Data Directory Snapshot

For the Phase 7 file-boundary prototype, the stable import/export contract is a closed or otherwise quiesced PGlite data directory. The copy command records the source inventory, copies the directory, and records the target inventory so the two file sets can be compared before opening the imported database.

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite copy-snapshot \
  --node node-a \
  --targetDir ./.restore/node-a
```

## Create A Partition Snapshot Manifest

This wraps the CAS manifest with the partition id and the per-source-node high-watermark vector used for replay.
When `snapshotScope` is `data-only`, the snapshot includes only relation files that match the storage binding's `databaseOid` and `relfilenode` or `relationOid`. When it is `catalog-and-data`, the current prototype keeps the full closed data directory so the snapshot remains directly restorable.

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite partition-snapshot \
  --node node-a \
  --partition tenant:jp \
  --casDir ./.cas \
  --storageBinding '{"partitionId":"tenant:jp","rootRelationOid":16384,"snapshotScope":"catalog-and-data","relations":[{"databaseOid":5,"tablespaceOid":1668,"relationOid":16384,"relfilenode":16384}]}'
```

The binding can also be discovered from PGlite catalog metadata:

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite storage-binding \
  --node node-a \
  --partition tenant:jp \
  --rootRelation public.sensor_readings \
  --snapshotScope data-only
```

And used directly during snapshot acquisition:

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite partition-snapshot \
  --node node-a \
  --partition tenant:jp \
  --casDir ./.cas \
  --rootRelation public.sensor_readings \
  --snapshotScope data-only
```

For restoreable bootstrap snapshots, use `catalog-and-data`; after restoring the CAS manifest, replay starts from the snapshot high-watermark for the same partition only.

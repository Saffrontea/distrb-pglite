# @distrb-pglite/browser

Browser storage adapters for `distrb-pglite`.

This package keeps the browser path separate from `@distrb-pglite/node` so it can run without `node:fs`, `node:crypto`, `Buffer`, or `ws`.

## Exports

- `OpfsDataDirFilesystem`: OPFS-backed logical data-directory filesystem.
- `OpfsCasBlockStore`: OPFS-backed content-addressed block store.
- `BrowserSha256BlobStore`: in-memory `BlobStore` using Web Crypto SHA-256.
- `BrowserPGliteCasFilesystem`: PGlite `BaseFilesystem` adapter that snapshots and restores through core CAS manifests.
- `BrowserWebSocketTransport`: browser `WebSocket` transport compatible with the relay protocol used by `@distrb-pglite/node`'s `WebSocketHub`.
- `snapshotBrowserFilesystemToCas`, `restoreBrowserFilesystemFromCas`, `transferMissingBrowserCasBlocks`, `gcBrowserCasBlocks`: browser-safe CAS helpers.

## Shape

```ts
import { PGlite } from "@electric-sql/pglite";
import {
  BrowserPGliteCasFilesystem,
  BrowserSha256BlobStore,
  OpfsCasBlockStore,
  snapshotBrowserFilesystemToCas,
} from "@distrb-pglite/browser";

const blobs = new BrowserSha256BlobStore();
const fs = new BrowserPGliteCasFilesystem(blobs);
const db = new PGlite({ fs, relaxedDurability: false });

await db.exec("CREATE TABLE IF NOT EXISTS readings (id text PRIMARY KEY)");
const manifest = await fs.snapshot();

const casStore = await OpfsCasBlockStore.open("distrb-pglite-cas");
await snapshotBrowserFilesystemToCas({
  fs: fs.asSnapshotDataFilesystem(),
  casStore,
});
```

## Relay sync

```ts
import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { BrowserWebSocketTransport } from "@distrb-pglite/browser";

const store = new InMemoryPartitionStore("browser-a");
const sync = new SyncNode("browser-a", store, new BrowserWebSocketTransport("ws://127.0.0.1:4000"));

await sync.start({ autoRecoverSubscriptions: true });
await sync.subscribe("tenant:jp");
await sync.write({
  partitionId: "tenant:jp",
  schemaVersion: 1,
  payload: { sensorId: "s1", reading: 21 },
});
```

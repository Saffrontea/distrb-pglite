# @distrb-pglite/browser

Browser storage adapters for `distrb-pglite`.

This package keeps the browser path separate from `@distrb-pglite/node` so it can run without `node:fs`, `node:crypto`, `Buffer`, or `ws`.

## Exports

- `OpfsDataDirFilesystem`: OPFS-backed logical data-directory filesystem.
- `OpfsCasBlockStore`: OPFS-backed content-addressed block store.
- `BrowserSha256BlobStore`: in-memory `BlobStore` using Web Crypto SHA-256.
- `BrowserPGliteCasFilesystem`: PGlite `BaseFilesystem` adapter that snapshots and restores through core CAS manifests.
- `BrowserWebSocketTransport`: browser `WebSocket` transport compatible with the relay protocol used by `@distrb-pglite/node`'s `WebSocketHub`.
- `DistributedBrowserPGlite`: browser facade that opens PGlite, the event store, transport subscriptions, and event projections together.
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
import { DistributedBrowserPGlite } from "@distrb-pglite/browser";

const db = await DistributedBrowserPGlite.open({
  nodeId: "browser-a",
  schema:
    "CREATE TABLE IF NOT EXISTS readings (id text PRIMARY KEY, sensor_id text, reading numeric)",
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  partitions: [
    {
      id: "tenant:jp",
      project: async (sql, event) => {
        const payload = event.payload as { sensorId: string; reading: number };
        await sql.query(
          "INSERT INTO readings (id, sensor_id, reading) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
          [event.eventId, payload.sensorId, payload.reading],
        );
      },
    },
  ],
});

await db.write("tenant:jp", { sensorId: "s1", reading: 21 });
await db.close();
```

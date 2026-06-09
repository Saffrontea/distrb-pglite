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

## Facade shape

```ts
import { DistributedBrowserPGlite, OpfsCasBlockStore } from "@distrb-pglite/browser";

const casStore = await OpfsCasBlockStore.open("distrb-pglite-cas");
const db = await DistributedBrowserPGlite.open({
  nodeId: "browser-a",
  schema:
    "CREATE TABLE IF NOT EXISTS readings (id text PRIMARY KEY, sensor_id text, reading numeric)",
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  autoStart: false,
});

await db.exec("CREATE TABLE IF NOT EXISTS readings (id text PRIMARY KEY)");
const manifest = await db.createSnapshot({ casStore });
await db.close();
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

`DistributedBrowserPGlite` mirrors the Node facade at the contract level:
SQL access goes through `query` / `exec`, replication through `write` /
`writeLocal` / `catchUpFrom`, event inspection through `listEventsAfter*`, and
snapshot restore through `createSnapshot` / `restoreSnapshot`. The raw PGlite
filesystem, SQL event store, and `SyncNode` are available only under
`internals` for tests and dogfooding code.

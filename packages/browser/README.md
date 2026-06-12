# @distrb-pglite/browser

ブラウザ向けアダプターパッケージです。`DistributedBrowserPGlite` facade を通じて、
ブラウザ内 PGlite・OPFS ファイルシステム・ブラウザ CAS スナップショット/復元・
ブラウザ WebSocket transport を一体で扱えます。

`node:fs`・`node:crypto`・`Buffer`・`ws` への依存を持たず、ブラウザ環境だけで動作します。

> **注意**: このパッケージはプロトタイプです。npm には未公開です。

---

## インストール

npm には未公開のため、pnpm workspace 経由でのみ利用できます。

```jsonc
// package.json
{
  "dependencies": {
    "@distrb-pglite/browser": "workspace:*",
  },
}
```

---

## 最小使用例

```ts
import { DistributedBrowserPGlite } from "@distrb-pglite/browser";

const db = await DistributedBrowserPGlite.open({
  nodeId: "browser-node-a",
  schema: `
    CREATE TABLE IF NOT EXISTS factory_events (
      source_node text NOT NULL,
      event_id    text NOT NULL,
      line_id     text NOT NULL,
      quantity    integer NOT NULL,
      PRIMARY KEY (source_node, event_id)
    );
  `,
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  partitions: [
    {
      id: "line:a",
      project: async (pgDb, event) => {
        const p = event.payload as { lineId: string; quantity: number };
        await pgDb.query(
          `INSERT INTO factory_events (source_node, event_id, line_id, quantity)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (source_node, event_id) DO NOTHING`,
          [event.sourceNodeId, event.eventId, p.lineId, p.quantity],
        );
      },
    },
  ],
});

// イベントを書く
await db.write("line:a", { lineId: "line-1", quantity: 10 });

// 動的に partition を追加購読する
await db.subscribe({ id: "line:b" });

// 離線後の差分を取り込む
await db.catchUpFrom("node-server", "line:a");

// ローカル SQL を実行する
const result = await db.query<{ line_id: string; quantity: number }>(
  "SELECT line_id, quantity FROM factory_events ORDER BY event_id DESC LIMIT 10",
);

await db.close();
```

---

## OPFS と CAS スナップショット

ブラウザ内のデータを OPFS に永続化し、CAS スナップショットで bootstrap できます。

```ts
import {
  DistributedBrowserPGlite,
  BrowserSha256BlobStore,
  BrowserPGliteCasFilesystem,
  OpfsCasBlockStore,
} from "@distrb-pglite/browser";

// OPFS-backed blob store と CAS filesystem を準備する
const blobs = new BrowserSha256BlobStore();
const fs = new BrowserPGliteCasFilesystem(blobs);

const db = await DistributedBrowserPGlite.open({
  nodeId: "browser-node-a",
  fs,
  blobs,
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
});

// CAS スナップショットを作成する
const casStore = new OpfsCasBlockStore("opfs://cas/line-a");
const manifest = await db.createSnapshot({ casStore });

// スナップショットから別インスタンスを bootstrap する
const { handle: restoredDb } = await db.restoreSnapshot({
  manifest,
  sourceCasStore: casStore,
  targetCasStore: new OpfsCasBlockStore("opfs://cas/line-a-clone"),
});
await restoredDb.start();
```

ブロック転送を細かく制御したい場合は `transferMissingBrowserCasBlocks`・
`restoreBrowserFilesystemFromCas`・`snapshotBrowserFilesystemToCas` を直接使えます。

---

## テレメトリ

`telemetry` オプションに `SyncTelemetrySink` を渡します。
sink が throw してもレプリケーションは壊れません。

```ts
const db = await DistributedBrowserPGlite.open({
  nodeId: "browser-node-a",
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  telemetry: (event) => {
    // event.type: sync-started / sync-stopped / local-write /
    //   remote-event / replay-served / catchup /
    //   snapshot-created / snapshot-pulled / subscription-recovered
    console.log(event.type, event);
  },
});
```

OpenTelemetry との接続例は [docs/examples/otel-telemetry.md](../../docs/examples/otel-telemetry.md) を参照してください。

---

## 主要な公開 API

| 名前                              | 種別     | 説明                                           |
| --------------------------------- | -------- | ---------------------------------------------- |
| `DistributedBrowserPGlite`        | class    | ブラウザ用メイン facade                        |
| `BrowserPGliteCasFilesystem`      | class    | PGlite 向け OPFS CAS filesystem adapter        |
| `OpfsCasBlockStore`               | class    | OPFS-backed CAS ブロックストア                 |
| `OpfsDataDirFilesystem`           | class    | OPFS-backed 論理 data directory filesystem     |
| `BrowserSha256BlobStore`          | class    | Web Crypto SHA-256 を使う in-memory blob store |
| `BrowserWebSocketTransport`       | class    | ブラウザ用 WebSocket transport                 |
| `snapshotBrowserFilesystemToCas`  | function | OPFS → CAS manifest に変換                     |
| `restoreBrowserFilesystemFromCas` | function | CAS manifest → OPFS に復元                     |
| `transferMissingBrowserCasBlocks` | function | 差分ブロックだけ転送                           |
| `gcBrowserCasBlocks`              | function | 不要 CAS ブロックの GC                         |

---

## `DistributedBrowserPGliteOpenOptions` の主要フィールド

| フィールド        | 型                                            | 説明                                                       |
| ----------------- | --------------------------------------------- | ---------------------------------------------------------- |
| `nodeId`          | `string`                                      | ノード識別子                                               |
| `fs`              | `BrowserPGliteCasFilesystem?`                 | OPFS-backed CAS filesystem (省略時は揮発性)                |
| `blobs`           | `BrowserSha256BlobStore?`                     | SHA-256 blob store (省略時は揮発性)                        |
| `schema`          | `string?`                                     | 起動時に実行する DDL                                       |
| `migrate`         | `(db: PGlite) => void?`                       | schema マイグレーション関数                                |
| `transport`       | `DistributedBrowserPGliteTransportOptions`    | `{ type: "websocket", url }` / `Transport` インスタンス    |
| `partitions`      | `DistributedBrowserPGlitePartitionOptions[]?` | 購読する partition の設定                                  |
| `autoStart`       | `boolean?`                                    | デフォルト `true`。`false` にすると `start()` を手動で呼ぶ |
| `singleWriter`    | `SingleWriterRegistry \| { owners }?`         | 単一 writer 検出 (ADR 0006)                                |
| `onAppliedRemote` | `(event: EventEnvelope) => void?`             | リモートイベント適用後の hook                              |
| `telemetry`       | `SyncTelemetrySink?`                          | テレメトリ hook                                            |

---

## 参照

- アーキテクチャ全体: [docs/ARCH.md](../../docs/ARCH.md)
- テレメトリ例: [docs/examples/otel-telemetry.md](../../docs/examples/otel-telemetry.md)
- ADR 一覧: [docs/adr/](../../docs/adr/)

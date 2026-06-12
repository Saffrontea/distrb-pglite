# @distrb-pglite/node

Node.js 向けアダプターパッケージです。`DistributedPGlite` facade を通じて、
PGlite・イベントストア・WebSocket transport・CAS スナップショット・
ストレージ orchestration を一体で開けます。

> **注意**: このパッケージはプロトタイプです。npm には未公開です。

---

## インストール

npm には未公開のため、pnpm workspace 経由でのみ利用できます。

```jsonc
// package.json
{
  "dependencies": {
    "@distrb-pglite/node": "workspace:*",
  },
}
```

---

## 最小使用例

```ts
import { DistributedPGlite } from "@distrb-pglite/node";

const db = await DistributedPGlite.open({
  nodeId: "node-a",
  dataDir: "./data/node-a",
  casDir: "./data/node-a-cas", // CAS スナップショットを使う場合は必須
  schema: `
    CREATE TABLE IF NOT EXISTS sensor_readings (
      source_node text NOT NULL,
      event_id    text NOT NULL,
      sensor_id   text NOT NULL,
      reading     numeric NOT NULL,
      PRIMARY KEY (source_node, event_id)
    );
  `,
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  partitions: [
    {
      id: "tenant:jp",
      storage: {
        groupId: "sensors",
        rootRelation: "sensor_readings",
        relations: ["sensor_readings"],
      },
      project: async (sql, event) => {
        const p = event.payload as { sensorId: string; reading: number };
        await sql.query(
          `INSERT INTO sensor_readings (source_node, event_id, sensor_id, reading)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (source_node, event_id) DO NOTHING`,
          [event.sourceNodeId, event.eventId, p.sensorId, p.reading],
        );
      },
    },
  ],
});

// イベントを書く
await db.write("tenant:jp", { sensorId: "tokyo-1", reading: 21.5 });

// 動的に partition を追加購読する
await db.subscribe({ id: "tenant:us" });

// catchup: 離線していた node-b からの差分を取り込む
await db.catchUpFrom("node-b", "tenant:jp");

// CAS スナップショットを作成する (casDir が必要)
const snapshot = await db.createSnapshot("tenant:jp");

await db.close();
```

---

## CAS スナップショットと pullSnapshot

新規 node の bootstrap や大量 event replay の回避に使います。

```ts
// source node でスナップショットを作成
const snapshot = await sourceDb.createSnapshot("tenant:jp");

// source node の snapshot catalog エントリを取得して target node に渡す
// (groupId は partition の storage.groupId)
const snapshotEntries = await sourceDb.listSnapshotEntries("sensors");

// target node でスナップショットを pull して bootstrap
const pulled = await targetDb.pullSnapshot({
  partitionId: "tenant:jp",
  snapshotEntries,
  sourceCasDir: "./data/node-a-cas",
});
// pulled.entry.highWatermark 以降の event は自動で replay されます
```

不要になった snapshot の CAS ブロックは、binding group の retention policy
に従って `runCasGc` で削除できます。

```ts
const execution = await db.runCasGc("sensors", { dryRun: true });
// execution?.gc.deleted / execution?.gc.retained
```

event log は `pruneEvents` で切り詰められます (ADR 0008)。境界は
bootstrap 可能な snapshot の high-watermark に anchor され、prune 済み
範囲への catchup は `SnapshotRequiredError` になるため `pullSnapshot`
で回復します。anchor になった snapshot は `runCasGc` から自動で保護されます。

```ts
await db.createSnapshot("tenant:jp");
const { pruned } = await db.pruneEvents("tenant:jp");
```

回復と圧縮はシステムに任せられます (ADR 0009):

- open 時に `snapshotSource`(物理面の contract。共有ファイルシステムなら
  `SharedDirSnapshotSource`)を渡すと、`catchUpFrom` が prune 済み範囲への
  遭遇時に snapshot pull → replay → catchup を透過的に行います。
- storage の `vacuumPolicy` に `autoPruneEvents: true` を含めると、
  `runCasGc` が CAS block の GC と同じ retention 判断で event log を
  最古の retained anchor まで自動 prune します。

```ts
const db = await DistributedPGlite.open({
  // ...
  snapshotSource: new SharedDirSnapshotSource({
    sourceCasDir: "./shared/node-a-cas",
    targetCasDir: "./data/node-b-cas",
    listEntries: (peer, partitionId) => fetchEntriesSomehow(peer, partitionId),
  }),
  partitions: [
    {
      id: "tenant:jp",
      storage: {
        groupId: "sensors",
        rootRelation: "sensor_readings",
        relations: ["sensor_readings"],
        vacuumPolicy: {
          retainSnapshotGenerations: 1,
          gcAfterGeneration: 0,
          autoPruneEvents: true,
        },
      },
    },
  ],
});
```

snapshot の経路と scope (`catalog-and-data` / `data-only`) については
[docs/ARCH.md](../../docs/ARCH.md) の「CAS スナップショット」節を参照してください。

---

## テレメトリ

`telemetry` オプションに `SyncTelemetrySink` を渡します。
sink が throw してもレプリケーションは壊れません。

```ts
const db = await DistributedPGlite.open({
  nodeId: "node-a",
  dataDir: "./data/node-a",
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  telemetry: (event) => {
    // event.type: sync-started / sync-stopped / local-write /
    //   remote-event / replay-served / catchup /
    //   snapshot-created / snapshot-pulled / cas-gc /
    //   subscription-recovered
    console.log(event.type, event);
  },
});
```

OpenTelemetry との接続例は [docs/examples/otel-telemetry.md](../../docs/examples/otel-telemetry.md) を参照してください。

---

## `DistributedPGliteOpenOptions` の主要フィールド

| フィールド          | 型                                     | 説明                                                                             |
| ------------------- | -------------------------------------- | -------------------------------------------------------------------------------- |
| `nodeId`            | `string`                               | ノード識別子                                                                     |
| `dataDir`           | `string`                               | PGlite data directory のパス                                                     |
| `casDir`            | `string?`                              | CAS ブロックストアのディレクトリ (snapshot を使う場合)                           |
| `schema`            | `string?`                              | 起動時に実行する DDL                                                             |
| `migrate`           | `(db) => void?`                        | schema マイグレーション関数                                                      |
| `transport`         | `DistributedPGliteTransportOptions`    | `{ type: "websocket", url }` / `{ type: "p2p", ... }` / `Transport` インスタンス |
| `partitions`        | `DistributedPGlitePartitionOptions[]?` | 購読する partition の設定                                                        |
| `autoStart`         | `boolean?`                             | デフォルト `true`。`false` にすると `start()` を手動で呼ぶ                       |
| `singleWriter`      | `SingleWriterRegistry \| { owners }?`  | 単一 writer 検出 (ADR 0006)                                                      |
| `telemetry`         | `SyncTelemetrySink?`                   | テレメトリ hook                                                                  |
| `relaxedDurability` | `boolean?`                             | PGlite の relaxed durability モード                                              |

---

## 参照

- アーキテクチャ全体: [docs/ARCH.md](../../docs/ARCH.md)
- テレメトリ例: [docs/examples/otel-telemetry.md](../../docs/examples/otel-telemetry.md)
- ADR 一覧: [docs/adr/](../../docs/adr/)

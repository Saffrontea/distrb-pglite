# distrb-pglite

各 node がローカル PGlite を持ち、必要な partition だけを非同期で同期する
edge database layer のプロトタイプです。

中央 coordinator も global transaction も持ちません。SQL read は完全にローカルで完結し、
一貫性は結果整合性です。

> **プロトタイプ**: このリポジトリは実験的実装です。npm には未公開です。

---

## パッケージ構成

| パッケージ                                   | 説明                                                                                                    |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| [`@distrb-pglite/core`](packages/core)       | ランタイム非依存の同期エンジン。イベントモデル・`SyncNode`・`MemoryTransport`・`InMemoryPartitionStore` |
| [`@distrb-pglite/node`](packages/node)       | Node.js アダプター。`DistributedPGlite` facade・WebSocket transport・CAS スナップショット               |
| [`@distrb-pglite/browser`](packages/browser) | ブラウザアダプター。`DistributedBrowserPGlite` facade・OPFS filesystem・ブラウザ CAS                    |
| [`@distrb-pglite/testkit`](packages/testkit) | テスト共通ヘルパー                                                                                      |
| [`sensor-demo`](packages/sensor-demo)        | Node-backed playground・ブラウザ工場 dashboard デモ                                                     |

---

## クイックスタート

Node.js からの利用は `@distrb-pglite/node` が入口です。

```ts
import { DistributedPGlite } from "@distrb-pglite/node";

const db = await DistributedPGlite.open({
  nodeId: "node-a",
  dataDir: "./data/node-a",
  casDir: "./data/node-a-cas",
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
        const payload = event.payload as { sensorId: string; reading: number };
        await sql.query(
          `INSERT INTO sensor_readings (source_node, event_id, sensor_id, reading)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (source_node, event_id) DO NOTHING`,
          [event.sourceNodeId, event.eventId, payload.sensorId, payload.reading],
        );
      },
    },
  ],
});

await db.write("tenant:jp", { sensorId: "tokyo-1", reading: 21 });
await db.catchUpFrom("node-b", "tenant:jp");
await db.createSnapshot("tenant:jp");
await db.close();
```

各パッケージの詳細は:

- [`packages/core/README.md`](packages/core/README.md)
- [`packages/node/README.md`](packages/node/README.md)
- [`packages/browser/README.md`](packages/browser/README.md)

---

## 何を作っているか

> partitioned PGlite edge nodes のための、賢い Litestream っぽいもの。
> 軽量 ElectricSQL clone ではない。

SQL read はローカルで完結します。同期は非同期の append-only event replication で、
冪等性は `(source_node_id, event_id)` で担保します。snapshot は partition 単位で扱い、
cursor は global clock ではなく source node ごとの HighWatermark vector です。

やらないこと: strong consistency / global transaction / central coordinator /
Postgres `smgr` fork / Auth (use WireGuard or equivalent)。

---

## Playground

```bash
mise exec -- pnpm install
mise exec -- pnpm build
mise exec -- pnpm playground
```

`http://127.0.0.1:3000` で 3 つの PGlite edge node と WebSocket hub が起動します。
event 書き込み・収束の観察・SQL クエリ・snapshot pull と delta catchup を UI で試せます。

ブラウザ専用版 (OPFS/CAS + 工場 dashboard):

```bash
mise exec -- pnpm playground:browser
```

`http://127.0.0.1:5173`

---

## 開発コマンド

```bash
# 依存インストール
pnpm install

# ビルド
pnpm build

# 高速テスト (PGlite integration を除く)
pnpm test

# PGlite / CAS integration テスト
pnpm test:integration

# 全テスト
pnpm test:all
```

---

## ドキュメント

- [docs/ARCH.md](docs/ARCH.md) — アーキテクチャ詳細・コンポーネント図・ADR 要約
- [docs/CONCEPTS.md](docs/CONCEPTS.md) — 用語と概念の説明
- [TODO.md](TODO.md) — ライブラリ化に向けた残作業
- [ROADMAP.old.md](ROADMAP.old.md) — Phase 10.x までの実装履歴
- [docs/adr/](docs/adr/) — アーキテクチャ決定記録
- [docs/examples/otel-telemetry.md](docs/examples/otel-telemetry.md) — OpenTelemetry 接続例

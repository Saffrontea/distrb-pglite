# @distrb-pglite/core

ランタイム非依存の同期エンジンです。イベントモデル・`PartitionStore` インターフェース・
`SyncNode`・`MemoryTransport`・ストレージポリシー型など、Node.js / ブラウザ共通の
基盤部品をまとめています。

> **注意**: このパッケージはプロトタイプです。npm には未公開です。

---

## インストール

npm には未公開のため、pnpm workspace 経由でのみ利用できます。

```jsonc
// package.json
{
  "dependencies": {
    "@distrb-pglite/core": "workspace:*",
  },
}
```

---

## 主要な公開 API

| 名前                       | 種別           | 説明                                                         |
| -------------------------- | -------------- | ------------------------------------------------------------ |
| `SyncNode`                 | class          | partition 購読・書き込み・catchup を管理する同期エンジン本体 |
| `MemoryTransport`          | class          | インプロセス・決定論的なテスト用 transport                   |
| `InMemoryPartitionStore`   | class          | インメモリの `PartitionStore` 実装                           |
| `SqlPartitionStore`        | class          | 任意の SQL クライアントを使う `PartitionStore` 実装          |
| `SingleWriterRegistry`     | class          | partition 所有権の検出レジストリ (ADR 0006)                  |
| `uuidV7` / `UuidV7Factory` | function/class | UUIDv7 イベント ID 生成                                      |
| `compareEventIds`          | function       | UUIDv7 cursor の大小比較                                     |
| `compareHighWatermarks`    | function       | HighWatermark vector の比較                                  |
| `convergenceStatus`        | function       | partition 収束状態の計算                                     |

型は `EventEnvelope`・`PartitionStore`・`Transport`・`TransportConnection`・
`SyncTelemetryEvent`・`SyncTelemetrySink` など多数を re-export しています。

---

## 最小使用例

`MemoryTransport` と `InMemoryPartitionStore` を使ったインプロセス同期の例です。

```ts
import { SyncNode, MemoryTransport, InMemoryPartitionStore } from "@distrb-pglite/core";

// shared transport (in-process hub)
const transport = new MemoryTransport();

// node-a
const storeA = new InMemoryPartitionStore("node-a");
const nodeA = new SyncNode("node-a", storeA, transport);
await nodeA.start({ autoRecoverSubscriptions: false });
await nodeA.subscribe("tenant:jp");

// node-b
const storeB = new InMemoryPartitionStore("node-b");
const nodeB = new SyncNode("node-b", storeB, transport);
await nodeB.start({ autoRecoverSubscriptions: false });
await nodeB.subscribe("tenant:jp");

// node-a に書き込み → node-b に非同期で届く
await nodeA.write({ partitionId: "tenant:jp", payload: { msg: "hello" } });

// catchup (離線後の差分取得)
await nodeB.catchUpFrom("node-a", "tenant:jp");

await nodeA.stop();
await nodeB.stop();
```

---

## テレメトリ

`SyncTelemetrySink` を渡すことで、同期イベントを外部 observability に接続できます。
sink が throw してもレプリケーションは壊れません。

```ts
const nodeA = new SyncNode("node-a", store, transport, {
  telemetry: (event) => {
    // event は判別可能 union
    // sync-started / sync-stopped / local-write /
    // remote-event / replay-served / catchup / subscription-recovered
    console.log(event.type, event);
  },
});
```

OpenTelemetry との接続例は [docs/examples/otel-telemetry.md](../../docs/examples/otel-telemetry.md) を参照してください。

---

## 参照

- アーキテクチャ全体: [docs/ARCH.md](../../docs/ARCH.md)
- ADR 一覧: [docs/adr/](../../docs/adr/)

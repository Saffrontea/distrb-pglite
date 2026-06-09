# distrb-pglite

ローカル PGlite/Postgres ノードのための、パーティション単位レプリケーションと
snapshot pull のプロトタイプです。

This is an early prototype of partition-scoped replication and snapshot pull for local
PGlite/Postgres edge nodes.

## 何を作っているか

このプロジェクトは「Postgres をローカルに保ったまま、必要な partition だけを node 間で
同期・復元できる edge database layer」を試しています。

狙いは、普通の分散 DB のように中央 coordinator や global transaction を置くことではありません。
各 node は自分の PGlite を持ち、SQL read は完全にローカルで完結します。同期は非同期で、
結果整合性を受け入れます。

短く言うと:

> partitioned PGlite edge nodes のための、賢い Litestream っぽいもの。
> 軽量 ElectricSQL clone ではない。

## なぜ必要か

edge で SQL を使いたい時、よくある選択肢には弱点があります。

- local-first にすると SQL や Postgres の表現力が弱くなりがち
- distributed SQL にすると coordinator や global consistency が重くなりがち
- cloud edge 前提だと、ユーザー端末・オンプレ・現場端末まで含む本当の edge になりにくい
- snapshot / restore が DB 全体単位だと、必要な tenant や partition だけを動かしにくい

この prototype は次の割り切りを取ります。

- PGlite/Postgres は各 node のローカル DB として使う
- global transaction は持たない
- consistency は eventual
- event は at-least-once で流す
- 冪等性は `(source_node_id, event_id)` で担保する
- snapshot は partition 単位で扱う
- replay cursor は source node ごとの high-watermark vector にする
- Postgres `smgr` には入らない

`smgr` に触らずにここまで来ているので、今の複雑さで済んでいます。現フェーズでは、
外側の event / snapshot / catalog の不変条件をテストで固めるのが重要です。

## 今できること

- append-only event replication
- `(source_node_id, event_id)` による idempotent apply
- partition subscription
- UUID v7 event id
- source node ごとの cursor / high-watermark
- memory transport
- WebSocket hub transport
- P2P WebSocket transport
- PGlite-backed event store
- PGlite data directory の CAS manifest 化
- partition snapshot cursor
- storage binding metadata
- catalog-and-data snapshot
- data-only snapshot
- managed snapshot 作成
- managed snapshot pull
- snapshot 後の delta event replay
- Browser 用 OPFS data filesystem / CAS block store
- Browser 用 PGlite CAS filesystem adapter
- Browser 用 WebSocket relay transport
- browser playground で SQL / catchup / snapshot bootstrap を試す
- Node 用 `DistributedPGlite` facade で PGlite / event store / transport / partition binding をまとめて開く

## やらないこと

- strong consistency
- global transaction
- central coordinator
- Postgres `smgr` fork

## まだやらないこと

- production-grade GC / vacuum policy
- 任意の既存 data directory に full snapshot を雑に上書きすること

特に最後の点は重要です。`catalog-and-data` snapshot は新規 node の bootstrap 向けです。
既存 node 間の差分同期は event catchup、catalog-compatible な node への data refresh は
`data-only` snapshot の領域です。

## Playground

この prototype の価値は playground が一番わかりやすいです。

```bash
mise exec -- pnpm install
mise exec -- pnpm build
mise exec -- pnpm playground
```

開く URL:

```text
http://127.0.0.1:3000
```

playground は 3 つの PGlite edge node と WebSocket hub を起動します。

UI でできること:

- source node に event を書く
- 他 node が収束する様子を見る
- 任意の node のローカル PGlite に SQL を投げる
- source node で snapshot を作る
- 既存 target node を event catchup で追いつかせる
- snapshot pull で late-joining replica を bootstrap する
- bootstrap 後に delta catchup する

Browser-only 版:

```bash
mise exec -- pnpm playground:browser
```

```text
http://127.0.0.1:5173
```

こちらは Node 側の data directory を使わず、ブラウザ内 PGlite と
`@distrb-pglite/browser` の OPFS/CAS adapter だけで分散工場 dashboard を動かします。
ライン端末、品質ゲート、保全端末、工場管制ノードがそれぞれ
`line:a` / `line:b` / `quality` / `maintenance` / `warehouse` の一部を購読し、
購読 partition のイベントだけを受け取ります。event sync / catchup / snapshot restore で
ローカル SQL view と KPI が収束します。
既定では shift automation が動き、イベント追加・ネットワーク分断・復帰後 catchup・設備停止が
自動で発生します。
dev server は relay 用 `WebSocketHub` も同時に起動するため、ブラウザ node 同士の
event sync / catchup も試せます。snapshot の CAS block transfer は同じ dev server の
HTTP CAS relay を通ります。

SQL パネルでは、各 node のローカル DB に直接 query できます。

例:

```sql
SELECT partition_id, event_type, line_id, station_id, work_order, sku, quantity, severity, status, source_node, recorded_at
FROM factory_events
ORDER BY recorded_at DESC
LIMIT 20;
```

event log も SQL で見られます。

```sql
SELECT source_node_id, event_id::text, partition_id, payload
FROM distrb_events
ORDER BY event_id DESC
LIMIT 20;
```

## Mental Model

各 node はローカル PGlite を持ちます。application write はまず local transaction として完結し、
その後 event として append されます。購読している peer は event を受け取り、冪等に apply します。

```text
local SQL write
  -> local PGlite transaction
  -> event append
  -> async transport
  -> idempotent remote apply
```

Node では低レベル部品を直接組み立てず、`DistributedPGlite` から始められます。
安定した入口は `query` / `exec` / `subscribe` / `write` / `writeLocal` /
`catchUpFrom` / `createSnapshot` / `pullSnapshot` です。PGlite 実体、event store、
`SyncNode` などの低レベル部品は facade の直下には出さず、dogfooding や検証用の
`internals` に隔離しています。

```ts
import { DistributedPGlite } from "@distrb-pglite/node";

const db = await DistributedPGlite.open({
  nodeId: "node-a",
  dataDir: "./data/node-a",
  casDir: "./data/node-a-cas",
  schema: `
    CREATE TABLE IF NOT EXISTS sensor_groups (id serial PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS sensor_readings (
      source_node text NOT NULL,
      event_id text NOT NULL,
      sensor_id text NOT NULL,
      reading numeric NOT NULL,
      PRIMARY KEY (source_node, event_id)
    );
  `,
  transport: { type: "websocket", url: "ws://127.0.0.1:4000" },
  partitions: [
    {
      id: "tenant:jp",
      storage: {
        groupId: "sensors",
        rootRelation: "sensor_groups",
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

大量の event replay が重い場合や、新しい node を初期化する場合は snapshot pull を使います。

```text
source node
  -> partition snapshot を作る
  -> high-watermark vector を保存する
  -> CAS blocks を保存する

target node
  -> catalog manifest を受け取る
  -> 足りない CAS blocks だけ転送する
  -> snapshot を restore する
  -> snapshot high-watermark 以降の events を replay する
```

cursor は global clock ではなく source node ごとの vector です。

```text
tenant:jp snapshot cursor
  node-a -> event 018b...
  node-b -> event 018c...
  node-c -> event 018d...
```

これにより、node 間の clock skew や global ordering に correctness を依存しません。

## Snapshot Scope

### catalog-and-data

新規 node の bootstrap 向けです。復元可能性を優先し、PGlite data directory の catalog と data を
一緒に snapshot します。

playground の `Pull snapshot -> new replica` はこの経路を使います。

### data-only

既に catalog-compatible な node に対して、relation file と visibility に必要な metadata を運ぶための
経路です。

これは「既存 node の data directory に full snapshot を上書きする」ものではありません。

## Repository Layout

```text
packages/core
  event model, partition store interface, UUID v7 cursor, MemoryTransport, SyncNode

packages/node
  PGlite store, WebSocket transport, CAS snapshot, storage binding, orchestration

packages/browser
  OPFS filesystem, browser CAS snapshot/restore, browser PGlite filesystem adapter

packages/testkit
  shared test helpers

packages/sensor-demo
  sensor demo, Node-backed playground, browser-only playground

docs/adr
  architecture decision records

docs/research
  storage boundary and pre-smgr research notes
```

## Development

高速テスト:

```bash
mise exec -- pnpm test
```

重い PGlite/CAS integration test:

```bash
mise exec -- pnpm test:integration
```

全部:

```bash
mise exec -- pnpm test:all
```

build:

```bash
mise exec -- pnpm build
```

playground:

```bash
mise exec -- pnpm playground
```

## Design Notes

詳しい設計メモ:

- [distributed_edge_db.md](./distributed_edge_db.md)
- [ROADMAP.md](./ROADMAP.md)
- [docs/adr/0001-eventual-consistency.md](./docs/adr/0001-eventual-consistency.md)
- [docs/adr/0005-storage-layer-escalation.md](./docs/adr/0005-storage-layer-escalation.md)

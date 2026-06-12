# Architecture

distrb-pglite は「各ノードがローカルの PGlite を持ち、必要な partition だけを非同期で同期する」
edge database layer のプロトタイプです。
中央 coordinator も global transaction も持たず、収束は結果整合性に委ねます。

---

## パッケージ構成

```
packages/
  core/          イベントモデル・PartitionStore インターフェース・SyncNode・MemoryTransport
  node/          PGlite ストア・WebSocket Transport・CAS スナップショット・ストレージ orchestration
  testkit/       共有テストヘルパー
  sensor-demo/   センサーデモ・ブラウザ playground
```

```mermaid
graph TD
  core["@distrb-pglite/core<br/>(transport-agnostic sync engine)"]
  node["@distrb-pglite/node<br/>(Node.js adapters)"]
  demo["sensor-demo<br/>(playground & demo)"]
  testkit["testkit<br/>(test helpers)"]

  node -->|depends on| core
  demo -->|depends on| core
  demo -->|depends on| node
  testkit -->|depends on| core
```

---

## コアコンポーネント

Application code should normally enter through `DistributedPGlite` or
`DistributedBrowserPGlite`. These facades expose the product contract:
local SQL (`query` / `exec`), partition writes, catchup, event inspection, and
snapshot operations. Lower-level building blocks remain available for tests and
research, but facade instances expose them only through `internals`.

### 公開 API と internals の線引き

- **公開 API(後方互換性を意識する面)**: facade のメソッドと open オプション、
  core の型(`EventEnvelope` など)、`Transport` インターフェース、
  `SyncTelemetrySink`、`SingleWriterRegistry`。
- **internals(契約外)**: facade の `internals` getter から触れる
  `SyncNode`・store・PGlite インスタンスと、`@distrb-pglite/node` の
  storage orchestration / CAS / inventory ヘルパー群。テスト・研究・
  上位レイヤー実装のために export しているが、シグネチャは予告なく変わる。
- 迷ったら: facade で表現できないユースケースが出たときは internals に
  手を伸ばすのではなく、facade に何が足りないかを先に問う。

```mermaid
classDiagram
  class SyncNode {
    +nodeId: NodeId
    +start()
    +stop()
    +subscribe(partitionId)
    +write(input) EventEnvelope
    +catchUpFrom(peerNodeId, partitionId)
    +highWatermark(partitionId)
  }

  class PartitionStore {
    <<interface>>
    +appendLocal(input) EventEnvelope
    +applyRemote(event) bool
    +listEventsAfterCursors(partitionId, cursors)
    +getHighWatermark(partitionId) HighWatermark
    +registerPartition(partitionId)
    +createSnapshotMetadata(partitionId)
  }

  class Transport {
    <<interface>>
    +connect(nodeId, handlers) TransportConnection
  }

  class TransportConnection {
    <<interface>>
    +subscribe(partitionId)
    +publish(event)
    +requestReplay(targetNodeId, partitionId, cursors)
    +close()
  }

  class MemoryTransport {
    +connect(nodeId, handlers)
  }

  class WebSocketTransport {
    +connect(nodeId, handlers)
  }

  class P2PTransport {
    +publicUrl: string
    +seeds: string[]
    +connect(nodeId, handlers)
  }

  class PGlitePartitionStore {
    -db: PGliteClient
    +migrate()
    +appendLocal(input)
    +applyRemote(event)
    +listEventsAfterCursors(...)
    +getHighWatermark(...)
  }

  SyncNode --> PartitionStore : uses
  SyncNode --> Transport : uses
  Transport --> TransportConnection : creates
  Transport <|.. MemoryTransport
  Transport <|.. WebSocketTransport
  Transport <|.. P2PTransport
  PartitionStore <|.. PGlitePartitionStore
```

---

## イベントレプリケーションの流れ

```mermaid
sequenceDiagram
  participant App
  participant SyncNodeA as SyncNode (node-a)
  participant StoreA as PGliteStore (node-a)
  participant Hub as WebSocketHub
  participant SyncNodeB as SyncNode (node-b)
  participant StoreB as PGliteStore (node-b)

  App->>SyncNodeA: write({ partitionId, payload })
  SyncNodeA->>StoreA: appendLocal(input)
  StoreA-->>SyncNodeA: EventEnvelope (eventId=UUIDv7)
  SyncNodeA->>Hub: publish(event)
  Hub->>SyncNodeB: onEvent(event)
  SyncNodeB->>StoreB: applyRemote(event)
  Note over StoreB: idempotent: (source_node_id, event_id) で重複排除
  StoreB-->>SyncNodeB: applied=true
```

---

## イベント catchup (cursor による差分取得)

node-b が一時的に切断していた場合など、HighWatermark ベクターで差分だけ取り寄せます。

```mermaid
sequenceDiagram
  participant SyncNodeB as SyncNode (node-b)
  participant StoreB as PGliteStore (node-b)
  participant SyncNodeA as SyncNode (node-a)
  participant StoreA as PGliteStore (node-a)

  SyncNodeB->>StoreB: getHighWatermark(partitionId)
  StoreB-->>SyncNodeB: { "node-a": "018b...", "node-c": "018d..." }
  SyncNodeB->>SyncNodeA: requestReplay(partitionId, cursors)
  SyncNodeA->>StoreA: listEventsAfterCursors(partitionId, cursors)
  StoreA-->>SyncNodeA: [ events newer than watermarks ]
  SyncNodeA-->>SyncNodeB: ReplayResponse
  loop for each event
    SyncNodeB->>StoreB: applyRemote(event)
  end
```

---

## CAS スナップショットと復元の流れ

大量の event replay を避けたい場合や新規ノードの bootstrap に使います。

```mermaid
sequenceDiagram
  participant OrcSrc as StorageOrchestration (source)
  participant CASStore as CAS Block Store
  participant OrcTgt as StorageOrchestration (target)
  participant PGlite as PGlite (target)
  participant StoreSrc as PGliteStore (source)
  participant StoreTgt as PGliteStore (target)

  OrcSrc->>OrcSrc: detectStorageBindingDrift()
  OrcSrc->>OrcSrc: prepareStorageSnapshotFence()
  OrcSrc->>CASStore: snapshotFilesystemToCas()<br/>(PGlite data dir → CAS blocks)
  OrcSrc->>StoreSrc: createSnapshotMetadata(partitionId)
  StoreSrc-->>OrcSrc: HighWatermark (snapshot 時点)

  Note over OrcTgt: --- target node が snapshot を pull ---

  OrcTgt->>CASStore: transferMissingBlocks()<br/>(差分ブロックだけ転送)
  OrcTgt->>OrcTgt: restorePartitionCasSnapshot()<br/>(PGlite data dir を復元)
  OrcTgt->>StoreTgt: replayEventsAfterPartitionSnapshot()<br/>(snapshot HWM 以降の event を適用)
```

---

## ストレージレイヤーの構造

```mermaid
graph TD
  subgraph "packages/node — Storage"
    CAS["CAS Block Store<br/>(NodeCasBlockStore)"]
    Inventory["Inventory<br/>(NodeDataDirFilesystem)"]
    Binding["StorageBinding<br/>(DDL generation, relation OID mapping)"]
    Manifest["CasManifest<br/>(per-file SHA-256, block refs)"]
    Orchestration["StorageOrchestration<br/>(create / pull / restore)"]
    PartitionSnapshot["PartitionCasSnapshot<br/>(snapshot + storageBinding + HWM)"]
  end

  subgraph "PGlite data directory"
    DataDir["PGlite data dir<br/>(base/, global/, pg_wal/, …)"]
  end

  Orchestration --> Binding
  Orchestration --> CAS
  Orchestration --> PartitionSnapshot
  PartitionSnapshot --> Manifest
  Manifest --> CAS
  CAS --> DataDir
  Inventory --> DataDir
```

---

## Transport の切り替え

Transport はインターフェースで抽象化されており、環境に合わせて差し替えられます。

```mermaid
graph LR
  SN[SyncNode]

  subgraph "テスト環境"
    MT[MemoryTransport<br/>インプロセス・決定論的]
  end

  subgraph "クラスター構成"
    WSH[WebSocketHub<br/>中央ハブ経由]
    WSTr["WebSocketTransport<br/>(クライアント)"]
    WSH <-->|ws| WSTr
  end

  subgraph "P2P 構成"
    P2P[P2PTransport<br/>seed peer 発見・自動再接続]
  end

  SN --> MT
  SN --> WSTr
  SN --> P2P
```

---

## データモデル (主要型)

```mermaid
erDiagram
  EventEnvelope {
    string eventId "UUIDv7"
    string sourceNodeId
    string partitionId
    number schemaVersion
    unknown payload
  }

  HighWatermark {
    string sourceNodeId PK
    string eventId "最新の eventId"
  }

  StoragePartitionBinding {
    string partitionId PK
    number rootRelationOid
    string snapshotScope "catalog-and-data | data-only"
    number ddlGeneration
  }

  StorageSnapshotCatalogEntry {
    string snapshotId PK
    string groupId
    string partitionId
    string manifestKind "cas | filesystem"
    array highWatermark
    number ddlGeneration
  }

  CasManifest {
    string snapshotId
    string[] parentSnapshotIds
    string compression "zstd | none"
    array files
  }

  CasFileManifest {
    string path
    number size
    string sha256
    array blocks
  }

  EventEnvelope }o--|| HighWatermark : "tracked by"
  StorageSnapshotCatalogEntry ||--|| CasManifest : "manifest"
  StorageSnapshotCatalogEntry }o--|| StoragePartitionBinding : "scoped by"
  CasManifest ||--|{ CasFileManifest : "contains"
```

---

## Partition 所有権の保証レベル (ADR 0006)

partition は常に単一 writer という前提だが、ライブラリが提供するのは**検出**であって**強制**ではない。

- `singleWriter` オプションは opt-in。未指定なら所有権チェックは一切行われない。
- 所有権はローカル知識。`claimPartition` / `transferPartition` はローカルの
  `SingleWriterRegistry` を更新するだけで、レプリケーションも永続化も
  旧 writer の fence もしない。所有権の配布と fencing は上位レイヤーの責務。
- registry を渡した場合: 違反するリモートイベントは skip され
  `onSingleWriterViolation` と telemetry に報告される。違反するローカル書き込みは throw する。
- 詳細は [ADR 0006](adr/0006-single-writer-partition-ownership.md) の Guarantee Level を参照。

---

## テレメトリ

core は OpenTelemetry に依存しない。代わりに構造化イベントの observer hook
(`SyncTelemetrySink`)を公開し、OTel exporter・ロガー・メトリクスは利用側が
1 つの adapter 関数として被せる。

```ts
const db = await DistributedPGlite.open({
  // ...
  telemetry: (event) => {
    // event は判別可能 union: sync-started / sync-stopped / local-write /
    // remote-event (applied | duplicate | rejected-single-writer) /
    // replay-served / catchup / subscription-recovered
    meter.counterFor(event.type).add(1);
  },
});
```

- sink が throw してもレプリケーションは壊れない(エラーは握りつぶす契約)。
- sink 未指定時はゼロコスト。

---

## Event log compaction (ADR 0008)

event log は append-only だが、snapshot を anchor にして有限長に保てる。

- prune は「retained な bootstrap 可能 snapshot の high-watermark 以下」のみ。
  facade の `pruneEvents` がこの anchor 検証を行う(明示 API のみ、自動 prune はまだない)。
- `pruned_through` は partition × source node の永続 cursor vector。
  冪等性の床も兼ねる: floor 以下のイベントの再配送は duplicate として拒否される。
- prune 済みの範囲に cursor が落ちる replay 要求には、部分的なイベント列ではなく
  `snapshot-required` 応答が返り、要求側は `SnapshotRequiredError` を受けて
  snapshot pull で回復する。wire protocol v2(hello の `protocolVersion`)で運ばれ、
  v1 の peer には silent な空応答ではなく timeout で失敗させる。
- prune 後、anchor snapshot は最適化ではなく回復経路の一部になる
  (ADR 0008 の Relationship to ADR 0007 を参照)。

---

## 物理レプリケーションの contract (ADR 0009)

論理面の `Transport` と対になる物理面の contract が `SnapshotSource`。
責務は**発見**(peer が広告する snapshot catalog entries の列挙)と
**転送**(1 entry の不足 block をローカル block store に揃える)の 2 つだけ。
検証(ADR 0007 の fence)・entry 選択・復元は consumer 側に残る:
source 実装は「list と fetch」より賢くなってはいけない。

- content-addressed なので転送は冪等・再開可能で、提供側を信頼しなくてよい
- facade に `snapshotSource` を構成すると、`catchUpFrom` が
  `SnapshotRequiredError` から透過的に回復する(pull → replay → catchup)
- 実装: `StaticSnapshotSource`(テスト/移行用、core)、
  `SharedDirSnapshotSource`(共有ファイルシステム、node)。
  wire protocol 上の block 転送は将来の実装(contract は変えずに足せる)
- `pullSnapshot` の `snapshotEntries` / `sourceCasDir` 引数は
  「呼び出し側が source を兼ねる」退化ケースとして残る移行経路

また `vacuumPolicy.autoPruneEvents` を有効にすると、`runCasGc` が
CAS block の GC と同じ retention 判断で各 partition の event log を
「最古の retained bootstrap anchor の HWM」まで自動 prune する。
retention window 内の遅れは replay で、それ以遅は snapshot pull で
回復する、という猶予が policy から自動的に導かれる。

---

## エコシステムの責務境界

このライブラリは WireGuard 相当のレイヤーであり、Tailscale 的な
ネットワーキング層(認証・認可・ノード発見・鍵配布・メンバーシップ管理)は
責務外。relay/hub(`WebSocketHub`)は transport と authorization の
入口だけを持ち、ストレージのセマンティクスは manifest・event log・
CAS block store に閉じる。これらの上位機能は別レイヤー/別プロダクトとして
被せる前提で、core の API はそれを妨げない形を保つ。

---

## 主要なアーキテクチャ決定

| ADR                                                   | 決定                                                                         |
| ----------------------------------------------------- | ---------------------------------------------------------------------------- |
| [0001](adr/0001-eventual-consistency.md)              | global transaction を持たず、結果整合性のみ                                  |
| [0002](adr/0002-transport-abstraction.md)             | Transport を VFS 的インターフェースで抽象化                                  |
| [0003](adr/0003-node-adapter-boundary.md)             | core はランタイム非依存、Node.js 依存は packages/node に閉じ込める           |
| [0004](adr/0004-per-node-uuidv7-cursors.md)           | cursor は global clock でなく source node ごとの UUIDv7 HighWatermark vector |
| [0005](adr/0005-storage-layer-escalation.md)          | PGlite を black box として扱い、smgr には入らない                            |
| [0006](adr/0006-single-writer-partition-ownership.md) | partition は常に単一 writer、競合 merge は実装しない                         |
| [0007](adr/0007-logical-source-of-truth.md)           | 論理層が正本、物理 snapshot は version/generation fence 付きの最適化         |
| [0008](adr/0008-event-log-compaction.md)              | snapshot-anchored な event log compaction、replay gap は明示応答             |
| [0009](adr/0009-physical-replication-contract.md)     | 物理面の contract `SnapshotSource`(発見+転送のみ、検証/選択/復元は外)      |

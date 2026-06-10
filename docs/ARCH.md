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

## 主要なアーキテクチャ決定

| ADR                                          | 決定                                                                         |
| -------------------------------------------- | ---------------------------------------------------------------------------- |
| [0001](adr/0001-eventual-consistency.md)     | global transaction を持たず、結果整合性のみ                                  |
| [0002](adr/0002-transport-abstraction.md)    | Transport を VFS 的インターフェースで抽象化                                  |
| [0003](adr/0003-node-adapter-boundary.md)    | core はランタイム非依存、Node.js 依存は packages/node に閉じ込める           |
| [0004](adr/0004-per-node-uuidv7-cursors.md)  | cursor は global clock でなく source node ごとの UUIDv7 HighWatermark vector |
| [0005](adr/0005-storage-layer-escalation.md) | PGlite を black box として扱い、smgr には入らない                            |
| [0006](adr/0006-single-writer-partition-ownership.md) | partition は常に単一 writer、競合 merge は実装しない                |
| [0007](adr/0007-logical-source-of-truth.md)  | 論理層が正本、物理 snapshot は version/generation fence 付きの最適化         |

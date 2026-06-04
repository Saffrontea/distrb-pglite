# Concepts & Use Cases

distrb-pglite が解こうとしている問題と、その中心となる概念・想定ユースケースを説明します。

---

## なぜこのシステムが必要か

edge で SQL を使いたいとき、既存の選択肢にはトレードオフがあります。

| アプローチ         | 問題点                                                          |
| ------------------ | --------------------------------------------------------------- |
| クラウド DB に接続 | オフライン時に読み書きできない                                  |
| SQLite ローカル DB | 複数ノード間の同期が難しい                                      |
| distributed SQL    | coordinator・global lock が重く、本当の edge まで届かない       |
| ElectricSQL 系     | リレーショナルな表現力を犠牲にすることがある                    |
| Litestream         | DB 全体を S3 にレプリカするだけ。partition 単位の制御ができない |

distrb-pglite は次の割り切りをとります。

- **各 node は自分の PGlite を持ち、SQL read は完全ローカルで完結する**
- **global transaction・global lock は持たない**
- **収束は結果整合性。at-least-once + idempotent apply で補償する**
- **同期の単位は partition。必要な tenant・region の data だけを動かす**

---

## コアコンセプト

### 1. Partition — 同期の論理単位

Partition は「どの data を誰に届けるか」を表す論理的なまとまりです。
テナント・リージョン・エンティティグループなどをマッピングします。

```
partition: "tenant:jp"  → JP の sensor_readings だけを JP エッジに届ける
partition: "tenant:us"  → US の sensor_readings だけを US エッジに届ける
```

node は任意の partition を subscribe でき、その partition の event だけを受け取ります。
別 tenant の data は届かないため、不要な同期コストや情報漏洩リスクを下げられます。

---

### 2. Event Envelope — append-only ログ

すべての書き込みは `EventEnvelope` として表現されます。

```typescript
type EventEnvelope = {
  eventId: EventId; // UUIDv7 (時刻ソート可能)
  sourceNodeId: NodeId; // 書いた node
  partitionId: PartitionId;
  schemaVersion: number;
  payload: unknown; // アプリケーション定義の任意 payload
};
```

Event は append-only です。削除・更新は新しい event として表現します。
`payload` は完全にアプリケーション定義であり、ライブラリ側は関与しません。

---

### 3. HighWatermark Vector — clock に依存しない cursor

各 node は「どの source node のどの event まで受け取ったか」を vector として管理します。
global clock や Lamport clock を使わないため、時刻同期の誤差に correctness を依存しません。

```
tenant:jp の HighWatermark (node-b 視点):
  node-a → 018b3f...  (node-a が出した最後の eventId)
  node-c → 018d1a...  (node-c が出した最後の eventId)
```

catchup は「この vector より新しいものをくれ」という一回の request で済みます。

---

### 4. Idempotent Apply — at-least-once 配送との組み合わせ

Transport は at-least-once です。同じ event が複数回届くことがあります。
`(source_node_id, event_id)` をプライマリキーとする `distrb_applied_events` テーブルで
重複を排除し、冪等性を保証します。

```sql
INSERT INTO distrb_applied_events (source_node_id, event_id, partition_id)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
RETURNING event_id
```

`RETURNING` が空なら既適用 → スキップ。これがレプリケーションの正確性を担保します。

---

### 5. CAS Snapshot — ブロック単位の差分転送

大量の event replay を避けたい場合、PGlite の data directory をブロック単位で
Content-Addressable Storage (CAS) に変換してスナップショットします。

```
PGlite data dir
  base/16384/1259  → SHA-256 = a3f1...  [block 0..64KB]
  base/16384/1259  → SHA-256 = b2e0...  [block 64KB..128KB]
  global/pg_auth   → SHA-256 = c9d2...
```

target node はすでに持っているブロックをリストアップし、**差分だけ転送**します。
スナップショットのサイズが大きくても、変更があったブロックのみ流れます。

スナップショットには作成時点の HighWatermark が埋め込まれており、
復元後は「HighWatermark 以降の event を replay する」ことで最新状態に追いつきます。

---

### 6. Snapshot Scope

| Scope              | 内容                                         | 用途                                             |
| ------------------ | -------------------------------------------- | ------------------------------------------------ |
| `catalog-and-data` | PGlite catalog (pg_class 等) + relation data | 新規ノードの bootstrap                           |
| `data-only`        | relation data のみ                           | catalog-compatible な既存ノードへの data refresh |

`catalog-and-data` は新規 node が何も持っていない状態から立ち上がるための経路です。
既存 node を上書きするためのものではありません。

---

### 7. Transport 抽象

`Transport` は小さなインターフェースです。

```typescript
type Transport = {
  connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection>;
};
```

実装は差し替え可能です。

| 実装                                  | 用途                          |
| ------------------------------------- | ----------------------------- |
| `MemoryTransport`                     | テスト・インプロセス同期      |
| `WebSocketTransport` + `WebSocketHub` | ハブ経由のクラスター構成      |
| `P2PTransport`                        | seed peer 発見・直接 P2P 接続 |

Transport が抽象化されているため、MQTT・BLE・NATS などへの差し替えも core の変更なしに行えます。

---

## 想定ユースケース

### ユースケース 1: IoT センサーエッジ

**シナリオ**: 工場や農場に設置されたセンサーノード群が、ネットワークが不安定な環境で
センサー値を記録し、後でダッシュボードに集約する。

```
センサーノード JP (jp-edge)
  ┌──────────────────────────────┐
  │ INSERT INTO sensor_readings  │  ← ローカル SQL、即座に完結
  │ PGlite: sensor_readings_jp  │
  └──────────────┬───────────────┘
                 │ event publish (非同期、切断時はキュー)
                 ▼
        WebSocketHub / P2P
                 │
                 ▼
  ┌──────────────────────────────┐
  │ ダッシュボードノード         │
  │ SELECT avg(reading) FROM ... │  ← ローカル SQL で集計
  └──────────────────────────────┘
```

- センサーノードはオフライン中でもローカルに書き続けられる
- 再接続後に catchup で自動収束する
- ダッシュボードは Postgres SQL (JOIN、集計、window 関数) がそのまま使える
- JP のデータを US ノードは受け取らない (partition 分離)

---

### ユースケース 2: オフラインファーストモバイル

**シナリオ**: 現場作業員のタブレットが、オフラインで作業記録を書き、
Wi-Fi 圏内に戻ったときにサーバー側のノードと同期する。

```
タブレット (field-node)
  ┌──────────────────────────────┐
  │ INSERT INTO work_logs ...    │  ← offline でも書ける
  │ local PGlite                │
  └──────────────┬───────────────┘
      (reconnect 後)
                 │ event catchup
                 ▼
  ┌──────────────────────────────┐
  │ サーバーノード (server-node) │
  │ SELECT * FROM work_logs      │  ← 集計・PDF 出力など
  └──────────────────────────────┘
```

- 複数の現場ノードが同じ partition を書いても、idempotent apply で衝突しない
- 接続が回復した瞬間に自動 catchup
- サーバー側も PGlite または PostgreSQL をバックエンドにできる

---

### ユースケース 3: マルチリージョン edge レプリカ

**シナリオ**: JP リージョンのエッジノードが事前に `tenant:jp` の data を
CAS スナップショットで持ち、ユーザーリクエストはローカル SQL で応答する。

```
Central (source)
  ┌─────────────────────────────────┐
  │ createManagedPartitionCasSnapshot│
  │  → CAS blocks を保存            │
  │  → HighWatermark を記録         │
  └────────────────┬────────────────┘
                   │ manifest + 差分ブロックのみ転送
                   ▼
JP Edge (target)
  ┌─────────────────────────────────┐
  │ pullPartitionCasSnapshot         │
  │  → 足りないブロックだけ取得     │
  │  → PGlite data dir を復元       │
  │ replayEventsAfterSnapshot        │
  │  → HWM 以降の event を追いつく  │
  └─────────────────────────────────┘
         ↓ 以後はローカル SQL
  SELECT * FROM tenants WHERE ...
```

- 初回は CAS snapshot で高速 bootstrap（event を全件 replay する必要がない）
- 以降は event catchup で差分だけ流れる
- スナップショット転送は差分ブロック単位のため、全体を再転送しない

---

### ユースケース 4: 遅延参加レプリカの bootstrap

**シナリオ**: 長時間稼働中のクラスターに新しいノードを追加する。
event ログが長大になっているので、スナップショットから始めて追いつく。

```
既存クラスター (node-a, node-b が数日分の event を持つ)
       │
       │ 1. node-a が catalog-and-data snapshot を作成
       │ 2. CAS blocks を target に転送
       ▼
新規ノード (jp-replica)
  → snapshot 復元 (PGlite が即座に起動できる状態)
  → snapshot の HighWatermark 以降の event を replay
  → 収束完了 → 通常の subscribe モードへ移行
```

playground の `Pull snapshot → new replica` がこの経路を体験できます。

---

## 制約と割り切り

| 項目                            | 方針                                                                             |
| ------------------------------- | -------------------------------------------------------------------------------- |
| 読み取り整合性                  | ローカルのみ。他ノードの最新書き込みは非同期で届く                               |
| 書き込み競合                    | アプリケーション側で partition key を設計して回避する                            |
| GC                              | snapshot の世代管理は今後の課題。現状は手動                                      |
| smgr (Postgres storage manager) | 触らない。file-level CAS で十分か検証中                                          |
| 既存 data directory への上書き  | `catalog-and-data` snapshot は新規 node 向け。既存 node への全体上書きは行わない |

---

## メンタルモデルまとめ

```
アプリケーション書き込み
  → ローカル PGlite トランザクション (即座に完結)
  → event として append (UUIDv7 で時刻ソート可能)
  → Transport を通じて非同期配送
  → 各購読ノードが idempotent に apply
  → HighWatermark vector で収束を追跡

大量 replay が重い場合 / 新規ノード追加:
  → CAS スナップショット作成 (data dir → blocks)
  → 差分ブロックのみ転送
  → snapshot 復元 → delta replay → 完全に追いつく
```

これにより「Postgres の表現力をそのまま edge で使いながら、必要な partition だけを軽量に同期する」
アーキテクチャを実現します。

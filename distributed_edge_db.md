# 分散エッジDBアーキテクチャ 概念モデル

> "賢いlitestream であって、軽いelectric ではない"

---

## 設計思想

### 捨てるもの
- **グローバルトランザクション** — 存在しない。ローカルトランザクションのみ。
- **強整合性** — 最初から諦める。Eventual Consistencyを受け入れる。
- **中央集権的なコーディネーター** — ノード数に比例して状態管理が爆発するため排除。

### 得るもの
- **Postgresの表現力** — JOIN、pgvector、PostGIS、時系列集計が一つのエッジノードで完結。
- **本当のエッジ** — CloudflareのPoPの中ではなく、ユーザーのデバイスやオンプレサーバーまで含む。
- **読み取りレイテンシの最小化** — 読み取りは完全にローカルで完結。

---

## アーキテクチャ概要

```
┌─────────────────────────────────────────────────────┐
│  Application layer                                   │
│  パーティションキーを決める / 結果整合性を受け入れる      │
├─────────────────────────────────────────────────────┤
│  Partition management layer  （ZFS の zfs コマンド相当）│
│  パーティション定義・冪等性・Snapshot/Vacuum管理        │
├─────────────────────────────────────────────────────┤
│  CoW storage layer  （git × btrfs/zfs 相当）          │
│  Content-addressable・Copy-on-Write・差分転送          │
├─────────────────────────────────────────────────────┤
│  Network / pubsub layer                              │
│  Event配信・at-least-once・WS                        │
├─────────────────────────────────────────────────────┤
│  Local storage layer                                 │
│  PGLite（Postgres） — ローカルdisk相当                │
└─────────────────────────────────────────────────────┘
```

---

## 各レイヤーの詳細

### 1. Local storage layer
- **PGLite（Postgres）** をローカルdiskとして使う。
- 分散レイヤーはPostgresの中身を知らなくていい。NFSやCephがローカルFSを意識しないのと同じ。
- ローカルトランザクションはACIDで保証される。
- pgvector・PostGISのインデックスをローカルで保持。

### 2. Network / pubsub layer
- **Event配信はat-least-once**。Exactly-onceは上位レイヤーが担保。
- WebSocketベース。
- ノード間でeventを非同期に配信する。グローバル順序保証は持たない。

### 3. CoW storage layer
- **Content-addressable storage** — ページ/ブロック単位でhash管理。差分転送が効率的になる。
- **Copy-on-Write** — スナップショットがO(1)で取れる。btrfsのextentツリーに相当。
- 参照カウントでGCを行う。バキュームと自然に対応する。
- PGLiteのファイル出力をブラックボックスとして外側からCoW管理する。

### 4. Partition management layer
- **ZFSのzfsコマンド相当**。アプリがパーティションキーを決め、この層がライフサイクルを管理する。
- Postgresのextensionとして実装可能な範囲。TimescaleDBのアーキテクチャが参考になる。
- パーティション単位でSnapshot・Vacuum・配信が独立して動く。
- `PartitionId`はアプリ層のラベルではなく、storage層でOID/relfilenodeに対応する概念として扱う。TimescaleDBのようなcatalog-backed partition管理と同じく、分散差分同期の最小単位はPostgres relation identityに結びつく必要がある。
- Snapshot対象はpartitionごとに選択可能にする。データページのみを運ぶのか、カタログページとデータページを一緒に運ぶのかを明示する。

```sql
-- DDLイメージ
CREATE PARTITION users_jp
  KEY = tenant_region
  VALUE = 'jp'
  REPLICATE TO (node_1, node_3);
```

### 5. Application layer
- パーティションキーの定義はアプリの責任。
- グローバル整合性が必要なユースケースは対象外と明示的に割り切る。

---

## Eventモデル

### 基本原則
```
書き込み = ローカルトランザクション + event append
配信     = pubsub、at-least-once
受信     = UUID v7で冪等性を保証してローカルに適用
```

- **append-only** — Eventは追記のみ。State = fold(events, initialState)。
- **コンフリクトが原理的に起きにくい** — 状態の同期ではなくeventの配信。
- **グローバルトランザクションは存在しない** — カオスでいい。

### 冪等性
```
source_node_id: node_1
event_id: UUID v7  # source node内でソート可能
```
受信側は`(source_node_id, event_id)`をキーに **INSERT OR IGNORE** 的な処理をする。PGLiteのローカルトランザクション内で完結。

### Cursor
Replication cursorは`created_at`ではなく、source nodeごとの`(source_node_id, event_id)`で持つ。

- 各ノードは自分のappend streamの順序だけを所有する。
- ノード間のグローバル順序は推測しない。
- ローカル時刻のずれは、source nodeをまたぐ同期の正しさに影響しない。

---

## Snapshot戦略

### Pull型（明示的）
オフラインから復帰したノードが能動的にスナップショットをpullする。サーバー側は関知しない。

```
1. 「自分はどこまで持ってる？」を自己申告
2. 「最新スナップショット」をpullする
3. スナップショット以降のevent logを追いかける
```

Gitの `clone + rebase` と同じ思想。pullした側が自分で追いつく責任を持つ。

### 暗黙的にやらない理由
- サーバーが全ノードの状態を管理しないといけなくなる
- ノード数が増えるほど状態管理が爆発する
- 「カオスでいい」という設計判断と矛盾する

### Snapshot × Vacuumの合流
UUID v7はsource node内で順序を持つため、SnapshotとVacuumはsource nodeごとのhigh-watermark vectorを境界として共有できる。

```
event log
──────────────────────────────────▶
         ↑snapshot          ↑snapshot
         source nodeごとのwatermarkまでvacuumしていい
```

---

## パーティション戦略

### 基本
- **全データを全ノードに配信しない** — 関心のあるパーティションだけをpull/subscribe。
- GitHubのリポジトリ単位のcloneと同じ思想。

### パーティションキーの例
| ユースケース | パーティションキー | 特性 |
|---|---|---|
| ベクトル系センシング | デバイスID × 時系列 | 書き込み多、読み込みは局所的 |
| マルチテナントSaaS | テナントID | テナント間でデータが交差しない |
| AI RAG | コーパスID × ユーザーID | pgvectorのHNSWインデックスをパーティションごとに保持 |
| GIS | 地理パーティション | PostGIS、位置情報は元々fuzzy |

### CoWレイヤーとの対応
```
パーティション = CoWツリーのサブボリューム単位
スナップショット = パーティションごとに独立
差分配信       = 購読しているパーティションの差分だけ
```

### Storage-level PartitionId

`PartitionId`は最終的に次のようなstorage bindingを持つ。

```ts
PartitionId -> {
  rootRelationOid,
  relations: [
    { databaseOid, tablespaceOid, relationOid, relfilenode, storagePath }
  ],
  snapshotScope: "data-only" | "catalog-and-data"
}
```

- `data-only`は既に同じカタログ定義を持つノードへの差分転送向け。
- `catalog-and-data`は新規ノードのpull、DDL変更後、またはpartition定義自体の同期向け。
- この対応がないと、FS/CAS層はどのページがどのpartitionに属するか判断できず、分散差分同期を正しく切れない。

現在のprototypeでは、このbindingをpartition snapshot取得に接続している。`data-only`では`base/<databaseOid>/<relfilenode>`に一致するrelation本体、segment、FSM/VM/init forkだけをCAS manifestへ入れる。`catalog-and-data`では復元可能性を優先し、closed data directory全体をCAS manifestへ入れる。

また、PGlite上の`pg_class`、`pg_database`、`pg_relation_filenode()`、`pg_relation_filepath()`からbindingを生成できる。`storagePath`が得られる場合は、それを優先して実ファイルと`PartitionId`を結合する。

---

## 実装レイヤーの分離

| 機能 | 実装場所 |
|---|---|
| パーティション管理 | Postgres extension（OID/relfilenode対応を含む） |
| UUID v7冪等性 | Postgres extension（トリガー + uuid型） |
| Snapshotカタログ管理 | Postgres extension |
| ローカルトランザクション | PGLite |
| CoWストレージ | PGLiteのファイルレイヤー外側 |
| ノード間の差分配信 | extensionの外（ネットワーク層） |
| pubsubのネットワーク層 | extensionの外 |

Postgresソースに手を入れる場合、**smgr（Storage Manager）インターフェース**にCoWを差し込む。WAL自体がappend-onlyのevent logとして活用できる。

---

## 先行実装の参考

| プロジェクト | 参考にする点 |
|---|---|
| **litestream** | WALをそのまま流すシンプルさ。「カオスでいい」と通じる割り切り |
| **electric-sql** | PGLite + 同期レイヤーの接合部 |
| **Neon** | smgr差し替えの実装実績。page serverの分離思想 |
| **orioledb** | Table AM完全実装。CoW実験の教科書 |
| **TimescaleDB** | パーティション管理のextension実装 |

> このアーキテクチャは **electric と litestream の間** を狙う。  
> litestreamのシンプルさは維持しつつ、パーティション単位の選択的配信・スナップショットのpull戦略・冪等性保証を加える。

---

## 競合との比較

| | D1 | Turso | Aurora | このアーキテクチャ |
|---|---|---|---|---|
| エッジ | CloudflareのPoP内 | エッジ | リージョン | 真のエッジ（デバイスまで） |
| DB | SQLite | SQLite | MySQL/Postgres | Postgres |
| ベクトル検索 | ✗ | △ | 別サービス | ✓（pgvector） |
| GIS | ✗ | ✗ | 別サービス | ✓（PostGIS） |
| グローバル整合性 | ✓ | ✓ | ✓ | ✗（意図的） |
| ベンダー依存 | Cloudflare | Turso | AWS | なし |

---

## ターゲットユースケース

三つとも**読み取りレイテンシが命でグローバル整合性はそこまで要らない**という共通点を持つ。

1. **ベクトル系センシング** — IoTデバイスのデータをエッジで収集・検索。古いデータでも近似でいい。
2. **マルチテナントエッジサービス** — テナントが独立しているため結果整合性の問題が起きにくい。
3. **AI RAG** — 推論がエッジにあり、ベクトル検索もエッジで完結。レイテンシ最小。

---

## 設計原則まとめ

```
書き込み：ローカルトランザクション + event append
配信    ：pubsub、at-least-once、UUID v7で冪等
Snapshot：明示的pull、ノードの自己責任
Vacuum  ：Snapshotのカットラインに従う
整合性  ：最初から諦める
```

> **結果整合性で十分 × Postgresの表現力が欲しい**  
> このトレードオフが明確で、誰もそこを取りに行っていない。

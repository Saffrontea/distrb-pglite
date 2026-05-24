# sensor-demo

distrb-pglite の動作を検証する dogfooding デモ集です。
センサーデータの収集・分散・スナップショット復元を実際に動かして確認できます。

## 前提

```bash
mise exec -- pnpm install
mise exec -- pnpm build
```

---

## エントリーポイント

### `pnpm playground` — ブラウザ UI

```bash
mise exec -- pnpm --filter @distrb-pglite/sensor-demo playground
```

`http://127.0.0.1:3000` を開く。

3 つの edge node と WebSocket hub が起動し、ブラウザ上でノードの状態・イベント伝播・SQL 集計・スナップショット操作をインタラクティブに試せます。

UI でできること:

- センサー値を書き込む (online / offline)
- ノードを disconnect / connect する
- 任意のノードのローカル DB に SQL を投げる
- CAS スナップショットを作成し、新規レプリカを bootstrap する
- スナップショット後の delta catchup を試す

---

### `pnpm run` — CLI ドッグフーディングデモ

```bash
mise exec -- pnpm --filter @distrb-pglite/sensor-demo run
```

4 ノード構成をインプロセスで立ち上げ、以下を順に実行します。

| ステップ | 内容 |
|---------|------|
| 1 | jp-edge がセンサー値を 3 件書き込む |
| 2 | us-edge が `tenant:us` に書き込む (partition isolation 確認) |
| 3 | dashboard が `tenant:jp` を購読し、初回 catchup で集計 |
| 4 | jp-edge をオフラインにし、ローカルに 2 件書き込む |
| 5 | jp-edge 再接続 → dashboard が差分を catchup |
| 6 | jp-replica が CAS スナップショット + delta replay で参加 |
| 7 | jp-edge と jp-replica のイベント数・SQL 集計が一致するか検証 |

---

### `pnpm scenarios` — HTTP シナリオテスト

```bash
mise exec -- pnpm --filter @distrb-pglite/sensor-demo scenarios
```

`EdgeNodeServer` (HTTP ラッパー) を複数立ち上げ、8 つのシナリオを順に検証します。

| シナリオ | 検証内容 |
|---------|---------|
| A | Basic CRUD — 書き込み・集計・stats |
| B | Cross-node sync — catchup による伝播 |
| C | Concurrent writes — 2 ノードの並列書き込み + WebSocket 伝播 |
| D | Offline write + catchup — disconnect 中の書き込み → 再接続後の収束 |
| E | Partition isolation — `tenant:us` が jp ノードに届かないこと |
| F | Snapshot bootstrap — 新規ノードが CAS snapshot + delta で追いつく |
| G | Stats consistency — 双方向 catchup 後に集計が一致する |
| H | P2P transport smoke — hub なしで P2PTransport が機能する |

全シナリオ PASS で終了コード 0、失敗があれば 1。

---

### `pnpm scale` — スケールテスト

```bash
mise exec -- pnpm --filter @distrb-pglite/sensor-demo scale
```

N ノードが並列書き込みし、全ノードが収束するまでの時間を計測します。
`N = [2, 5, 10, 20]` で繰り返し実行し、`writeMs`・`convergenceMs`・`catchupMs`・`consistent` を出力します。

---

### `pnpm vector` — pgvector ドッグフーディング

```bash
mise exec -- pnpm --filter @distrb-pglite/sensor-demo vector
```

`MemoryTransport` + PGlite + pgvector 拡張を使い、テナントスコープのベクターデータを
distrb イベントログ経由でレプリケートします。受信ノード側で最近傍クエリを実行し、
ベクター検索がローカル SQL として動作することを確認します。

---

## `EdgeNodeServer` — HTTP API リファレンス

`scenarios`・`scale`・`ui` が共通で使うノード実装です。
各ノードは PGlite + SyncNode を内包し、以下の HTTP エンドポイントを持ちます。

| Method | Path | 説明 |
|--------|------|------|
| GET | `/status` | nodeId・online 状態・partitionId・eventCount |
| POST | `/readings` | センサー値を書き込む (online 時) |
| POST | `/readings-offline` | センサー値をローカルのみに書き込む (offline 書き込みシミュレート) |
| GET | `/readings` | `sensor_readings` テーブルを返す |
| GET | `/stats` | sensor_id ごとの avg / min / max / count |
| GET | `/events` | イベントログ |
| POST | `/sql` | ローカル PGlite に任意の SQL を投げる |
| POST | `/catchup` | 指定ピアから catchup する (`{ peerNodeId }`) |
| POST | `/disconnect` | SyncNode を停止してオフラインにする |
| POST | `/connect` | SyncNode を再起動し、既知ピアから自動 catchup する |
| POST | `/create-snapshot` | CAS スナップショットを作成する |
| GET | `/catalog` | ストレージカタログマニフェストを返す |
| GET | `/snapshot-entries` | スナップショットカタログエントリ一覧 |
| POST | `/pull-snapshot` | スナップショットを pull して PGlite を復元・再起動する |

### `SensorPartitionStore` の役割

`PartitionStore` のラッパーで、`applyRemote` が呼ばれたときに
`sensor_readings` テーブルへの INSERT も同時に行います。
「event log → SQL テーブル」の射影をアプリケーション層で実装する例です。

```
applyRemote(event)
  → distrb_events に INSERT (idempotent)
  → sensor_readings に INSERT (ON CONFLICT DO NOTHING)
```

---

## ノード構成図 (playground / scenarios)

```
jp-node-1  ──┐
jp-node-2  ──┤── WebSocketHub ──── jp-node-3 (snapshot bootstrap)
us-node    ──┘                     (tenant:us は jp ノードに届かない)
```

- `jp-node-1`, `jp-node-2`, `jp-node-3` は `tenant:jp` を購読
- `us-node` は `tenant:us` を購読
- `jp-node-3` は初回起動時に `jp-node-1` から CAS snapshot を pull し、その後 delta replay で追いつく

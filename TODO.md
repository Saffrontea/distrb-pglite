# TODO

ライブラリとして使える状態にするための残作業。元のメモを精査して具体化したもの。
ロードマップ本体は ROADMAP.old.md(Phase 10.x まで完了)、設計判断は docs/adr/ を参照。

## 1. Partition 所有権 (ADR 0006)

方針: 「書いてもいいけど、保証しない」— 所有権の強制はしない。検出は opt-in。

- [x] `SingleWriterRegistry` の所有権移譲 (`transfer`) を facade
      (`DistributedPGlite` / `DistributedBrowserPGlite`) から扱えるようにする
      (`partitionOwner` / `claimPartition` / `transferPartition`)
- [x] 所有権の保証レベル(検出のみ・移譲はローカル知識・耐久性なし)を
      ARCH.md / ADR 0006 に明文化する
- [x] 違反検出時の挙動(skip + report / throw)を telemetry に載せる
      (`remote-event` の `rejected-single-writer` outcome)

## 2. テレメトリ / 可観測性

方針: OTel に直接依存しない。構造化された observer hook を core が公開し、
OTel exporter は利用側が被せられるようにする。

- [x] core に `SyncTelemetrySink`(observer interface)を定義する:
      local write / remote apply (applied・duplicate・rejected) /
      replay served / catchup / subscription recovery / start・stop
- [x] `SyncNode` と両 facade に配線する(未指定時はゼロコスト)
- [x] snapshot/CAS イベント(snapshot-created / snapshot-pulled、転送
      block 数、duration)を facade 層から同じ sink に出す
- [x] CAS GC を facade に公開 (`DistributedPGlite.runCasGc`) し、
      `cas-gc` テレメトリイベントを emit する
- [x] OTel を被せる例を docs に置く (docs/examples/otel-telemetry.md)

## 3. ライブラリとしての整理(公開準備)

- [x] 各公開パッケージ (core / node / browser) の package.json 整備:
      `files`, `exports`(`types` condition 込み), `repository`,
      `sideEffects` を追加済み
- [ ] LICENSE ファイルを追加する(2026-06-12 に確認し**意図的に保留**。
      決まるまで `private: true` / `0.0.0` のまま、`license` フィールドも
      未設定。候補: MIT / Apache-2.0(PGlite と同じ)/ MPL-2.0)
- [x] 各パッケージに README(インストール・最小利用例・API の入口)を書く
- [x] ルート README をライブラリ利用者向けに更新する
- [x] `internals` 経由でしか触れない低レベル API と公開 API の線引きを
      ARCH.md に明記する

## 4. Event log compaction (ADR 0008)

設計合意済み(2026-06-12)。0007 は変更しない。実装は 3 段階:

- [x] core: 永続 `pruned_through` vector(冪等性の床を兼ねる)、
      prune 操作、replay serving での gap 拒否
- [x] wire: `snapshot-required` response variant + protocol version field
      (memory / websocket hub / p2p)。v1 peer には timeout で失敗させる
- [x] facade: 明示 prune API (`DistributedPGlite.pruneEvents`、
      bootstrap snapshot anchor 検証つき)
- [ ] catchup の snapshot pull **自動**フォールバック(現状は
      `SnapshotRequiredError` を受けて利用者が `pullSnapshot` する)
- [ ] auto-prune policy(auto CAS GC と並ぶ vacuumPolicy 駆動)
- [x] anchor snapshot の GC 保護: `runStorageCasGcFromCatalog` は
      `pruned_through` を cover する bootstrap 可能 entry を世代 plan に
      関係なく retain する
- [x] telemetry: `events-pruned` / `snapshot-required` イベント
- [ ] subscriber prune(serve 先がないノードの緩い prune)は別クラスとして

## 5. エコシステム(スコープ外として記録)

Tailscale 的なネットワーキング層はこのライブラリの責務ではない。
これは WireGuard 相当のレイヤーであり、認証・認可・ノード発見・鍵配布は
別の責務として切り出す。relay/hub(WebSocketHub)は transport と
authorization の責務だけを持つ、という現行の線引きを維持する。

- [x] この責務分担を ARCH.md に 1 セクションとして明記する(実装はしない)

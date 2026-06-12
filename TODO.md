# TODO

ライブラリとして使える状態にするための残作業。元のメモを精査して具体化したもの。
ロードマップ本体は ROADMAP.old.md(Phase 10.x まで完了)、設計判断は docs/adr/ を参照。

## 1. Partition 所有権 (ADR 0006)

方針: 「書いてもいいけど、保証しない」— 所有権の強制はしない。検出は opt-in。

- [ ] `SingleWriterRegistry` の所有権移譲 (`transfer`) を facade
      (`DistributedPGlite` / `DistributedBrowserPGlite`) から扱えるようにする
- [ ] 所有権の保証レベル(検出のみ・移譲はローカル知識・耐久性なし)を
      ARCH.md / ADR 0006 に明文化する
- [ ] 違反検出時の挙動(skip + report / throw)を telemetry に載せる

## 2. テレメトリ / 可観測性

方針: OTel に直接依存しない。構造化された observer hook を core が公開し、
OTel exporter は利用側が被せられるようにする。

- [ ] core に `SyncTelemetry`(observer interface)を定義する:
      local write / remote apply (applied・duplicate) / replay served /
      catchup applied / single-writer violation / transport
      connect・reconnect・close
- [ ] `SyncNode` と両 facade に配線する(未指定時はゼロコスト)
- [ ] snapshot/CAS 側のイベント(snapshot 作成・pull・block 転送量・GC)を
      orchestration から同じ仕組みで出す
- [ ] OTel を被せる例を docs かサンプルとして 1 つ置く

## 3. ライブラリとしての整理(公開準備)

- [ ] 各公開パッケージ (core / node / browser) の package.json 整備:
      `files`, `exports`(`types` condition 込み), `license`, `repository`,
      `sideEffects`, `private` フラグの扱いを決める
- [ ] LICENSE ファイルを追加する(ライセンス選定は要決定)
- [ ] 各パッケージに README(インストール・最小利用例・API の入口)を書く
- [ ] ルート README をライブラリ利用者向けに更新する
- [ ] `internals` 経由でしか触れない低レベル API と公開 API の線引きを
      docs に明記する

## 4. エコシステム(スコープ外として記録)

Tailscale 的なネットワーキング層はこのライブラリの責務ではない。
これは WireGuard 相当のレイヤーであり、認証・認可・ノード発見・鍵配布は
別の責務として切り出す。relay/hub(WebSocketHub)は transport と
authorization の責務だけを持つ、という現行の線引きを維持する。

- [ ] この責務分担を ARCH.md に 1 セクションとして明記する(実装はしない)

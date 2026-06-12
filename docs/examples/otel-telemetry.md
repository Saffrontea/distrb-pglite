# 例: SyncTelemetrySink に OpenTelemetry を被せる

core は OTel に依存しない。`telemetry` オプションに渡す 1 つの adapter 関数で
`SyncTelemetryEvent`(判別可能 union)を OTel のメトリクスに写像する。

```ts
import { metrics } from "@opentelemetry/api";
import type { SyncTelemetryEvent } from "@distrb-pglite/core";
import { DistributedPGlite } from "@distrb-pglite/node";

const meter = metrics.getMeter("distrb-pglite");

const localWrites = meter.createCounter("distrb.local_writes");
const remoteEvents = meter.createCounter("distrb.remote_events");
const replayServed = meter.createCounter("distrb.replay_served_events");
const catchupDuration = meter.createHistogram("distrb.catchup_duration_ms");
const snapshotDuration = meter.createHistogram("distrb.snapshot_duration_ms");
const transferredBlocks = meter.createCounter("distrb.snapshot_transferred_blocks");

function otelSink(event: SyncTelemetryEvent): void {
  switch (event.type) {
    case "local-write":
      localWrites.add(1, { partition: event.partitionId });
      break;
    case "remote-event":
      // outcome: applied | duplicate | rejected-single-writer
      remoteEvents.add(1, {
        partition: event.partitionId,
        source: event.sourceNodeId,
        outcome: event.outcome,
        context: event.context, // live | replay
      });
      break;
    case "replay-served":
      replayServed.add(event.events, { partition: event.partitionId });
      break;
    case "catchup":
      catchupDuration.record(event.durationMs, {
        partition: event.partitionId,
        mode: event.mode, // catchup | reconcile
      });
      break;
    case "snapshot-created":
    case "snapshot-pulled":
      snapshotDuration.record(event.durationMs, { kind: event.type });
      if (event.type === "snapshot-pulled") {
        transferredBlocks.add(event.transferredBlocks);
      }
      break;
    case "cas-gc":
      snapshotDuration.record(event.durationMs, { kind: event.type });
      // event.deletedBlocks / event.retainedBlocks / event.dryRun
      break;
    case "sync-started":
    case "sync-stopped":
    case "subscription-recovered":
      // 必要ならログやイベントとして送る
      break;
  }
}

const db = await DistributedPGlite.open({
  nodeId: "node-a",
  dataDir: "./data/node-a",
  transport: { type: "websocket", url: "ws://localhost:8080" },
  telemetry: otelSink,
});
```

ポイント:

- sink が throw してもレプリケーションは壊れない(core 側で握りつぶす契約)。
  とはいえ adapter 内で重い処理をしない。export は OTel SDK の
  batch processor に任せる。
- `rejected-single-writer` は ADR 0006 のプロトコル違反検出。alert を
  仕掛けるならまずこれ。
- union に case が増えてもコンパイルエラーにしたい場合は `switch` の
  `default` で `event satisfies never` を使う。

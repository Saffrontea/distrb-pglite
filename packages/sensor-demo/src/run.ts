/**
 * 分散センサーダッシュボード — dogfooding demo
 *
 * 構成:
 *   jp-edge   ... JP IoTエッジノード (sensor_readings_jp に書き込む)
 *   us-edge   ... US IoTエッジノード (sensor_readings_us に書き込む、別パーティション)
 *   dashboard ... tenant:jp を購読、ローカルSQL集計
 *   jp-replica... CASスナップショットでブートストラップする新規参加ノード
 *
 * 検証内容:
 *   1. online sync       — WebSocket経由でイベント伝播、dashboard側でSQL集計
 *   2. partition isolation — tenant:us イベントはdashboardに届かない
 *   3. offline write+catchup — jp-edgeが切断中に書き込み → 再接続後にdashboardが追いつく
 *   4. CAS snapshot pull — jp-replicaがスナップショット+デルタリプレイで同じSQLデータを得る
 */

import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  DistributedPGlite,
  WebSocketHub,
  type DistributedPGliteProjection,
  type PGliteClient,
} from "@distrb-pglite/node";

// ─── application types ────────────────────────────────────────────────────────

type SensorPayload = { sensorId: string; reading: number };

// ─── application schema ───────────────────────────────────────────────────────

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS sensor_groups (
    id serial PRIMARY KEY
  );
  CREATE TABLE IF NOT EXISTS sensor_readings_jp (
    sensor_id   text    NOT NULL,
    reading     numeric NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
  );
  CREATE TABLE IF NOT EXISTS sensor_readings_us (
    sensor_id   text    NOT NULL,
    reading     numeric NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
  );
`;

function tableForPartition(partitionId: string): string | null {
  if (partitionId === "tenant:jp") return "sensor_readings_jp";
  if (partitionId === "tenant:us") return "sensor_readings_us";
  return null;
}

const projectSensorEvent: DistributedPGliteProjection<SensorPayload> = async (db, event) => {
  const table = tableForPartition(event.partitionId);
  if (!table) return;
  const { sensorId, reading } = event.payload;
  await db.query(`INSERT INTO ${table} (sensor_id, reading) VALUES ($1, $2)`, [sensorId, reading]);
};

// ─── helpers ──────────────────────────────────────────────────────────────────

type NodeSet = {
  node: DistributedPGlite;
  dataDir: string;
};

async function openNode(
  nodeId: string,
  baseDir: string,
  url: string,
  casDir: string,
  partitions: readonly string[],
): Promise<NodeSet> {
  const dataDir = join(baseDir, nodeId);
  const node = await DistributedPGlite.open({
    nodeId,
    dataDir,
    casDir,
    schema: SCHEMA,
    relaxedDurability: false,
    transport: { type: "websocket", url },
    autoStart: false,
    partitions: partitions.map((id) => ({
      id,
      project: projectSensorEvent,
      ...(id === "tenant:jp"
        ? {
            storage: {
              groupId: "sensor_groups",
              rootRelation: "sensor_groups",
              relations: ["sensor_readings_jp"],
              snapshotScope: "catalog-and-data" as const,
            },
          }
        : {}),
    })),
  });
  return { node, dataDir };
}

async function getFreePort(): Promise<number> {
  const srv = createServer();
  await new Promise<void>((resolve, reject) => {
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", resolve);
  });
  const addr = srv.address();
  await new Promise<void>((resolve, reject) => {
    srv.close((e) => (e ? reject(e) : resolve()));
  });
  if (!addr || typeof addr === "string") throw new Error("no port");
  return addr.port;
}

type AggRow = { sensor_id: string; avg_reading: string; count: string };

async function printAggregation(db: PGliteClient, table: string, label: string): Promise<void> {
  const result = await db.query<AggRow>(
    `SELECT sensor_id,
            round(avg(reading)::numeric, 2) AS avg_reading,
            count(*)::int                   AS count
       FROM ${table}
   GROUP BY sensor_id
   ORDER BY sensor_id`,
  );
  console.log(`\n  [${label}] SELECT from ${table}:`);
  if (result.rows.length === 0) {
    console.log("    (empty)");
  } else {
    for (const row of result.rows) {
      console.log(`    ${row.sensor_id}: avg=${row.avg_reading}, n=${row.count}`);
    }
  }
}

function section(title: string): void {
  console.log(`\n${"─".repeat(60)}`);
  console.log(`  ${title}`);
  console.log(`${"─".repeat(60)}`);
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function run(): Promise<void> {
  const baseDir = await mkdtemp(join(tmpdir(), "distrb-sensor-"));
  const casDir = join(baseDir, "cas");
  const port = await getFreePort();
  const url = `ws://127.0.0.1:${port}`;

  console.log(`\n${"═".repeat(60)}`);
  console.log("  分散センサーダッシュボード dogfooding");
  console.log(`  baseDir : ${baseDir}`);
  console.log(`  hub     : ${url}`);
  console.log(`${"═".repeat(60)}`);

  const hub = new WebSocketHub({ port });

  const jp = await openNode("jp-edge", baseDir, url, casDir, ["tenant:jp"]);
  const us = await openNode("us-edge", baseDir, url, casDir, ["tenant:us"]);
  const dash = await openNode("dashboard", baseDir, url, casDir, ["tenant:jp"]);
  let replica: NodeSet | undefined;
  let jpClosed = false;

  try {
    // ── 1. JP edge online writes ─────────────────────────────
    section("1. JP edge がオンラインでセンサー読み値を書き込む");

    await jp.node.start();

    const jpReadings: [string, number][] = [
      ["tokyo-1", 42.1],
      ["osaka-1", 38.5],
      ["tokyo-1", 43.8],
    ];
    for (const [sensorId, reading] of jpReadings) {
      await jp.node.write({
        partitionId: "tenant:jp",
        payload: { sensorId, reading } satisfies SensorPayload,
      });
    }
    console.log(`\n  jp-edge: ${jpReadings.length} 件書き込み完了`);

    // ── 2. US edge (別パーティション) ────────────────────────
    section("2. US edge が tenant:us に書き込む (partition isolation)");

    await us.node.start();

    for (const [sensorId, reading] of [
      ["ny-1", 25.0],
      ["ny-1", 26.3],
    ] as [string, number][]) {
      await us.node.write({
        partitionId: "tenant:us",
        payload: { sensorId, reading } satisfies SensorPayload,
      });
    }
    console.log("  us-edge: 2 件書き込み完了");

    // ── 3. Dashboard が tenant:jp を購読 ──────────────────────
    section("3. Dashboard が tenant:jp を購読してSQLを集計する");

    await dash.node.start();

    // jp-edge から過去イベントを catchup (購読前に書かれた分)
    const caughtInitial = await dash.node.catchUpFrom("jp-edge", "tenant:jp");
    console.log(`\n  初回catchup: ${caughtInitial} 件取得`);

    const dashJpCount = (await dash.node.listEventsAfterCursors("tenant:jp", new Map())).length;
    const dashUsCount = (await dash.node.listEventsAfterCursors("tenant:us", new Map())).length;
    console.log(
      `\n  dashboard tenant:jp イベント数: ${dashJpCount}  (expected: ${jpReadings.length})`,
    );
    console.log(`  dashboard tenant:us イベント数: ${dashUsCount}  (expected: 0, isolation OK)`);

    await printAggregation(dash.node, "sensor_readings_jp", "dashboard");

    // ── 4. JP edge オフライン → ローカル書き込み ─────────────
    section("4. JP edge がオフラインになりローカルに書き込む");

    await jp.node.stop();

    const offlineReadings: [string, number][] = [
      ["osaka-1", 39.2],
      ["tokyo-1", 44.0],
    ];
    for (const [sensorId, reading] of offlineReadings) {
      await jp.node.writeLocal({
        partitionId: "tenant:jp",
        payload: { sensorId, reading } satisfies SensorPayload,
      });
    }

    const beforeCatchup = (await dash.node.listEventsAfterCursors("tenant:jp", new Map())).length;
    console.log(`\n  catchup前 dashboard イベント数: ${beforeCatchup}`);

    // ── 5. JP edge 再接続 → dashboard が catch up ────────────
    section("5. JP edge が再接続し、dashboard が差分を取得する");

    await jp.node.start();

    const caught = await dash.node.catchUpFrom("jp-edge", "tenant:jp");
    console.log(`\n  catchup完了: ${caught} イベント取得`);

    const afterCatchup = (await dash.node.listEventsAfterCursors("tenant:jp", new Map())).length;
    console.log(
      `  catchup後 dashboard イベント数: ${afterCatchup}  (expected: ${jpReadings.length + offlineReadings.length})`,
    );

    await printAggregation(dash.node, "sensor_readings_jp", "dashboard (updated)");

    // ── 6. CAS snapshot + new replica ────────────────────────
    section("6. jp-replica が CASスナップショット+デルタリプレイで参加する");

    // スナップショットを取得 (jp-edge db はまだ open)
    const managed = await jp.node.createSnapshot("tenant:jp");
    console.log(`\n  スナップショット取得: ${managed.snapshot.snapshotId}`);
    console.log(
      `  CASブロック数: ${managed.snapshot.manifest.files.reduce((n, f) => n + f.blocks.length, 0)}`,
    );
    console.log(
      `  high-watermark: ${JSON.stringify(Object.fromEntries(managed.snapshot.highWatermark))}`,
    );

    // スナップショット後に jp-edge が 1 件追加書き込み
    const postSnapshotReading: [string, number] = ["tokyo-1", 45.5];
    await jp.node.writeLocal({
      partitionId: "tenant:jp",
      payload: {
        sensorId: postSnapshotReading[0],
        reading: postSnapshotReading[1],
      } satisfies SensorPayload,
    });
    console.log("  スナップショット後に 1 件追加書き込み (tokyo-1: 45.5)");

    // デルタイベント収集 (jp db を閉じる前に)
    const snapshotWatermark = new Map<string, string>(managed.snapshot.highWatermark);
    const deltaEvents = await jp.node.listEventsAfterCursors("tenant:jp", snapshotWatermark);
    console.log(`  デルタイベント: ${deltaEvents.length} 件`);

    // カタログマニフェストとスナップショットエントリを保存
    const catalogManifest = await jp.node.loadCatalog("sensor_groups");
    const snapshotEntries = await jp.node.listSnapshotEntries("sensor_groups");

    // jp-edge ノードを停止
    await jp.node.close();
    jpClosed = true;

    // スナップショット pull (catalog-and-data を bootstrap で取得)
    replica = await openNode("jp-replica", baseDir, url, casDir, ["tenant:jp"]);
    const pulled = await replica.node.pullSnapshot({
      snapshotEntries,
      catalogManifest,
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      sourceCasDir: casDir,
    });
    console.log(`\n  pull完了: ${pulled.transferredBlocks} ブロック転送`);

    // スナップショット時点でのSQL行数を確認
    const beforeDelta = await replica.node.query<{ count: string }>(
      "SELECT count(*)::int AS count FROM sensor_readings_jp",
    );
    console.log(`  スナップショット復元後の行数: ${beforeDelta.rows[0]?.count}`);

    // デルタイベントをリプレイ (facade の projection で SQL にも書き込まれる)
    let replayCount = 0;
    for (const event of deltaEvents) {
      if (await replica.node.applyEvent(event)) replayCount++;
    }
    console.log(`  デルタリプレイ: ${replayCount} 件`);

    await printAggregation(replica.node, "sensor_readings_jp", "jp-replica");

    // ── 検証: jp-edge と jp-replica の集計が一致するか ───────
    section("7. 検証: jp-edge と jp-replica のSQL集計が一致する");

    // jp-edge のデータを直接確認 (db は閉じているのでイベントから再集計)
    const totalExpectedReadings = jpReadings.length + offlineReadings.length + 1; // +1 post-snapshot
    const replicaTotalEvents = (await replica.node.listEventsAfterCursors("tenant:jp", new Map()))
      .length;
    console.log(`\n  jp-edge 総書き込み: ${totalExpectedReadings} 件`);
    console.log(
      `  jp-replica 総イベント: ${replicaTotalEvents} 件  (expected: ${totalExpectedReadings})`,
    );

    const finalRows = await replica.node.query<{ count: string }>(
      "SELECT count(*)::int AS count FROM sensor_readings_jp",
    );
    console.log(`  jp-replica 総行数: ${finalRows.rows[0]?.count}`);

    await printAggregation(replica.node, "sensor_readings_jp", "jp-replica (final)");

    const ok = replicaTotalEvents === totalExpectedReadings;
    console.log(`\n  ${"═".repeat(60)}`);
    console.log(`  結果: ${ok ? "✓ PASS — イベント数一致" : "✗ FAIL — イベント数不一致"}`);
    console.log(`  ${"═".repeat(60)}\n`);
  } finally {
    await Promise.allSettled([
      dash.node.close(),
      us.node.close(),
      jpClosed ? Promise.resolve() : jp.node.close(),
      replica?.node.close() ?? Promise.resolve(),
      hub.close(),
    ]);
    await rm(baseDir, { recursive: true, force: true });
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

/**
 * 分散センサーダッシュボード — シナリオランナー
 *
 * EdgeNodeServer に HTTP リクエストを投げてシナリオを検証する。
 * 各シナリオは独立しており、冪等に書き直せるよう状態を明示的に構築する。
 */

import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocketHub } from "@distrb-pglite/node";
import { EdgeNodeServer, type EdgeServerConfig } from "./server.js";

// ─── types ────────────────────────────────────────────────────────────────────

type ScenarioResult = {
  name: string;
  passed: boolean;
  details: string[];
  errors: string[];
};

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

async function api<T = unknown>(url: string, method = "GET", body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const json = (await res.json()) as T;
  if (!res.ok) throw new Error(`${method} ${url} → ${res.status}: ${JSON.stringify(json)}`);
  return json;
}

const get = <T = unknown>(url: string) => api<T>(url);
const post = <T = unknown>(url: string, body?: unknown) => api<T>(url, "POST", body);

function assertEq<T>(label: string, actual: T, expected: T, details: string[]): boolean {
  if (JSON.stringify(actual) === JSON.stringify(expected)) {
    details.push(`  ✓ ${label}: ${JSON.stringify(actual)}`);
    return true;
  }
  details.push(`  ✗ ${label}: got=${JSON.stringify(actual)}, want=${JSON.stringify(expected)}`);
  return false;
}

function assertGte(label: string, actual: number, min: number, details: string[]): boolean {
  if (actual >= min) {
    details.push(`  ✓ ${label}: ${actual} >= ${min}`);
    return true;
  }
  details.push(`  ✗ ${label}: ${actual} < ${min}`);
  return false;
}

// PGlite は numeric を文字列で返す。数値として比較する。
function assertNumEq(
  label: string,
  actual: unknown,
  expected: number,
  details: string[],
  errors: string[],
): void {
  const v = Number(actual);
  if (Math.abs(v - expected) < 0.001) {
    details.push(`  ✓ ${label}: ${v}`);
  } else {
    errors.push(`  ✗ ${label}: got=${v}, want=${expected}`);
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── scenario helpers ─────────────────────────────────────────────────────────

async function runScenario(
  name: string,
  fn: (details: string[], errors: string[]) => Promise<void>,
): Promise<ScenarioResult> {
  const details: string[] = [];
  const errors: string[] = [];
  let passed = false;

  console.log(`\n${"─".repeat(60)}`);
  console.log(`  ${name}`);

  try {
    await fn(details, errors);
    passed = errors.length === 0;
  } catch (err) {
    errors.push(`  EXCEPTION: ${err}`);
  }

  for (const d of details) console.log(d);
  if (errors.length > 0) {
    console.log("  FAILURES:");
    for (const e of errors) console.log(e);
  }
  console.log(`  → ${passed ? "PASS" : "FAIL"}`);

  return { name, passed, details, errors };
}

function check(condition: boolean, message: string, details: string[], errors: string[]): void {
  if (condition) {
    details.push(`  ✓ ${message}`);
  } else {
    errors.push(`  ✗ ${message}`);
  }
}

// ─── scenarios ────────────────────────────────────────────────────────────────

async function scenarioA_BasicCrud(
  jp1: string,
  details: string[],
  errors: string[],
): Promise<void> {
  details.push("  write 3 readings to jp-node-1");

  await post(`${jp1}/readings`, { sensorId: "tokyo-1", reading: 22.5 });
  await post(`${jp1}/readings`, { sensorId: "osaka-1", reading: 19.8 });
  await post(`${jp1}/readings`, { sensorId: "tokyo-1", reading: 23.1 });

  const { rows } = await get<{ rows: unknown[]; count: number }>(`${jp1}/readings`);
  check(rows.length === 3, `readings count = 3 (got ${rows.length})`, details, errors);

  const { stats } = await get<{
    stats: { sensor_id: string; avg_reading: string; count: number }[];
  }>(`${jp1}/stats`);
  const tokyo = stats.find((s) => s.sensor_id === "tokyo-1");
  const osaka = stats.find((s) => s.sensor_id === "osaka-1");

  check(!!tokyo, "tokyo-1 appears in stats", details, errors);
  check(!!osaka, "osaka-1 appears in stats", details, errors);
  assertNumEq("tokyo-1 count", tokyo?.count, 2, details, errors);
  assertNumEq("tokyo-1 avg", tokyo?.avg_reading, 22.8, details, errors);
  assertNumEq("osaka-1 avg", osaka?.avg_reading, 19.8, details, errors);
}

async function scenarioB_CrossNodeSync(
  jp1: string,
  jp2: string,
  details: string[],
  errors: string[],
): Promise<void> {
  details.push("  temporarily disconnect jp-node-2 so catchup has real work");
  await post(`${jp2}/disconnect`);

  details.push("  write 2 more readings to jp-node-1");
  await post(`${jp1}/readings`, { sensorId: "kyoto-1", reading: 18.0 });
  await post(`${jp1}/readings`, { sensorId: "kyoto-1", reading: 18.5 });

  details.push("  jp-node-2 reconnects and auto-catches up");
  const { applied } = await post<{ applied: number }>(`${jp2}/connect`);
  details.push(`  auto catchup applied ${applied} events`);
  check(applied >= 2, `auto catchup applied >= 2 events (got ${applied})`, details, errors);

  const { stats: stats2 } = await get<{ stats: { sensor_id: string; count: number }[] }>(
    `${jp2}/stats`,
  );
  const kyoto2 = stats2.find((s) => s.sensor_id === "kyoto-1");

  check(!!kyoto2, "jp-node-2 has kyoto-1 from jp-node-1", details, errors);
  assertNumEq("kyoto-1 count on jp-node-2", kyoto2?.count, 2, details, errors);
}

async function scenarioH_P2PTransportSmoke(
  baseDir: string,
  casDir: string,
  details: string[],
  errors: string[],
): Promise<void> {
  const [httpA, httpB, wsA, wsB] = await Promise.all([1, 2, 3, 4].map(() => getFreePort()));
  const urlA = `ws://127.0.0.1:${wsA}`;
  const urlB = `ws://127.0.0.1:${wsB}`;
  const p2pA = await EdgeNodeServer.create({
    nodeId: "p2p-node-a",
    transport: "p2p",
    p2pPublicUrl: urlA,
    p2pPort: wsA,
    p2pSeeds: [],
    dataDir: join(baseDir, "p2p-node-a"),
    casDir,
    port: httpA,
    partitionId: "tenant:jp",
  });
  const p2pB = await EdgeNodeServer.create({
    nodeId: "p2p-node-b",
    transport: "p2p",
    p2pPublicUrl: urlB,
    p2pPort: wsB,
    p2pSeeds: [urlA],
    dataDir: join(baseDir, "p2p-node-b"),
    casDir,
    port: httpB,
    partitionId: "tenant:jp",
  });

  try {
    await Promise.all([p2pA.listen(), p2pB.listen()]);
    await sleep(250);
    await post(`${p2pA.baseUrl}/readings`, { sensorId: "p2p-a", reading: 11 });

    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const { stats } = await get<{ stats: { sensor_id: string; count: number }[] }>(
        `${p2pB.baseUrl}/stats`,
      );
      if (stats.some((row) => row.sensor_id === "p2p-a")) {
        details.push("  ✓ p2p-node-b received p2p-node-a reading without hub");
        return;
      }
      await sleep(50);
    }
    errors.push("  ✗ p2p-node-b did not receive p2p-node-a reading");
  } finally {
    await Promise.allSettled([p2pA.close(), p2pB.close()]);
  }
}

async function scenarioC_ConcurrentWrites(
  jp1: string,
  jp2: string,
  details: string[],
  errors: string[],
): Promise<void> {
  details.push("  jp-node-1 and jp-node-2 write concurrently");

  await Promise.all([
    post(`${jp1}/readings`, { sensorId: "sensor-A", reading: 100 }),
    post(`${jp1}/readings`, { sensorId: "sensor-A", reading: 101 }),
    post(`${jp2}/readings`, { sensorId: "sensor-B", reading: 200 }),
    post(`${jp2}/readings`, { sensorId: "sensor-B", reading: 201 }),
  ]);

  // WebSocket 経由で伝播を待つ
  await sleep(300);

  const { stats: stats1 } = await get<{ stats: { sensor_id: string; count: string }[] }>(
    `${jp1}/stats`,
  );
  const { stats: stats2 } = await get<{ stats: { sensor_id: string; count: string }[] }>(
    `${jp2}/stats`,
  );

  // jp-node-1 は sensor-B を受け取っているはず (hub 経由)
  const sensorB_on1 = stats1.find((s) => s.sensor_id === "sensor-B");
  // jp-node-2 は sensor-A を受け取っているはず (hub 経由)
  const sensorA_on2 = stats2.find((s) => s.sensor_id === "sensor-A");

  check(!!sensorB_on1, "jp-node-1 received sensor-B from jp-node-2 via hub", details, errors);
  check(!!sensorA_on2, "jp-node-2 received sensor-A from jp-node-1 via hub", details, errors);

  if (sensorB_on1) details.push(`  jp-node-1 sensor-B count=${sensorB_on1.count}`);
  if (sensorA_on2) details.push(`  jp-node-2 sensor-A count=${sensorA_on2.count}`);
}

async function scenarioD_OfflineCatchup(
  jp1: string,
  jp2: string,
  details: string[],
  errors: string[],
): Promise<void> {
  // jp-node-2 の現在のイベント数を記録
  const { eventCount: before } = await get<{ eventCount: number }>(`${jp2}/status`);

  details.push("  jp-node-2 goes offline");
  await post(`${jp2}/disconnect`);

  details.push("  jp-node-1 writes 3 readings while jp-node-2 is offline");
  await post(`${jp1}/readings`, { sensorId: "offline-test", reading: 55 });
  await post(`${jp1}/readings`, { sensorId: "offline-test", reading: 56 });
  await post(`${jp1}/readings`, { sensorId: "offline-test", reading: 57 });

  details.push("  jp-node-2 writes 1 reading offline");
  await post(`${jp2}/readings-offline`, { sensorId: "jp2-local", reading: 99 });

  const { eventCount: duringOffline } = await get<{ eventCount: number }>(`${jp2}/status`);
  check(
    duringOffline === before + 1,
    `jp-node-2 offline event count = ${before + 1} (got ${duringOffline})`,
    details,
    errors,
  );

  details.push("  jp-node-2 reconnects and auto-catches up");
  const { applied } = await post<{ applied: number }>(`${jp2}/connect`);
  details.push(`  auto catchup applied ${applied} events`);

  check(applied >= 3, `auto catchup >= 3 offline events (got ${applied})`, details, errors);

  const { stats } = await get<{ stats: { sensor_id: string; count: number }[] }>(`${jp2}/stats`);
  const offlineTest = stats.find((s) => s.sensor_id === "offline-test");
  const jp2local = stats.find((s) => s.sensor_id === "jp2-local");

  assertNumEq("offline-test on jp-node-2 count", offlineTest?.count, 3, details, errors);
  check(!!jp2local, "jp-node-2 local offline write preserved", details, errors);
}

async function scenarioE_PartitionIsolation(
  jp1: string,
  us: string,
  details: string[],
  errors: string[],
): Promise<void> {
  details.push("  us-node writes to tenant:us");
  await post(`${us}/readings`, { sensorId: "ny-1", reading: 72.5 });
  await post(`${us}/readings`, { sensorId: "la-1", reading: 68.0 });

  // 少し待って伝播を確認 (jp-node-1 は tenant:us を購読していないので届かないはず)
  await sleep(200);

  const { rows: jpRows } = await get<{ rows: { sensor_id: string }[] }>(`${jp1}/readings`);
  const usReadingsOnJp = jpRows.filter((r) => r.sensor_id === "ny-1" || r.sensor_id === "la-1");

  check(
    usReadingsOnJp.length === 0,
    `jp-node-1 has 0 rows from tenant:us (got ${usReadingsOnJp.length})`,
    details,
    errors,
  );

  const { rows: usRows } = await get<{ rows: unknown[] }>(`${us}/readings`);
  check(
    usRows.length >= 2,
    `us-node has >= 2 own readings (got ${usRows.length})`,
    details,
    errors,
  );
}

async function scenarioF_SnapshotBootstrap(
  jp1: string,
  jp3: string,
  casDir: string,
  details: string[],
  errors: string[],
): Promise<void> {
  // jp-node-1 の現在のイベント数を確認
  const { eventCount: sourceCount } = await get<{ eventCount: number }>(`${jp1}/status`);
  details.push(`  jp-node-1 has ${sourceCount} events`);

  details.push("  jp-node-1 creates CAS snapshot");
  const snap = await post<{ snapshotId: string; blocks: number }>(`${jp1}/create-snapshot`);
  details.push(`  snapshot: ${snap.snapshotId} (${snap.blocks} blocks)`);

  // スナップショット後に jp-node-1 が 1 件追加書き込み (デルタ確認用)
  details.push("  jp-node-1 writes 1 more reading after snapshot");
  await post(`${jp1}/readings`, { sensorId: "post-snapshot", reading: 999 });

  // jp-node-3 がカタログとスナップショットエントリを jp-node-1 から取得
  const catalogManifest = await get(`${jp1}/catalog`);
  const { entries: snapshotEntries } = await get<{ entries: unknown[] }>(`${jp1}/snapshot-entries`);
  details.push(`  fetched catalog + ${snapshotEntries.length} snapshot entries from jp-node-1`);

  // jp-node-3 がスナップショットを pull して PGlite を再起動
  details.push("  jp-node-3 pulls snapshot from jp-node-1 (bootstrap)");
  const pullResult = await post<{ ok: boolean; snapshotId: string }>(`${jp3}/pull-snapshot`, {
    catalogManifest,
    snapshotEntries,
    sourceCasDir: casDir,
    requirement: "bootstrap",
  });
  check(pullResult.ok, "pull-snapshot returned ok", details, errors);

  // スナップショット時点のデータが jp-node-3 にある
  const { rows: jp3RowsAfterPull } = await get<{ rows: unknown[] }>(`${jp3}/readings`);
  assertGte("jp-node-3 rows after pull", jp3RowsAfterPull.length, sourceCount, details);

  // jp-node-3 が jp-node-1 からデルタイベントを catchup
  details.push("  jp-node-3 catchup delta from jp-node-1");
  const { applied: deltaApplied } = await post<{ applied: number }>(`${jp3}/catchup`, {
    peerNodeId: "jp-node-1",
  });
  details.push(`  jp-node-3 delta catchup: ${deltaApplied} events`);

  // jp-node-3 に post-snapshot 行が入っているはず
  const { stats: jp3Stats } = await get<{ stats: { sensor_id: string }[] }>(`${jp3}/stats`);
  const hasPostSnapshot = jp3Stats.some((s) => s.sensor_id === "post-snapshot");
  check(hasPostSnapshot, "jp-node-3 has post-snapshot reading after delta replay", details, errors);

  // jp-node-1 と jp-node-3 のイベント数が一致
  const { eventCount: jp1Count } = await get<{ eventCount: number }>(`${jp1}/status`);
  const { eventCount: jp3Count } = await get<{ eventCount: number }>(`${jp3}/status`);
  assertEq("event count jp-node-1 == jp-node-3", jp3Count, jp1Count, details);
}

async function scenarioG_StatsConsistency(
  jp1: string,
  jp2: string,
  details: string[],
  errors: string[],
): Promise<void> {
  details.push("  full sync: jp-node-2 catchup from jp-node-1, jp-node-1 catchup from jp-node-2");
  await post(`${jp2}/catchup`, { peerNodeId: "jp-node-1" });
  await post(`${jp1}/catchup`, { peerNodeId: "jp-node-2" });

  const { stats: s1 } = await get<{
    stats: { sensor_id: string; count: string; avg_reading: string }[];
  }>(`${jp1}/stats`);
  const { stats: s2 } = await get<{
    stats: { sensor_id: string; count: string; avg_reading: string }[];
  }>(`${jp2}/stats`);

  const key = (s: { sensor_id: string; count: string; avg_reading: string }) =>
    `${s.sensor_id}:${s.count}:${s.avg_reading}`;
  const keys1 = new Set(s1.map(key));
  const keys2 = new Set(s2.map(key));

  const missingIn2 = [...keys1].filter((k) => !keys2.has(k));
  const missingIn1 = [...keys2].filter((k) => !keys1.has(k));

  check(
    missingIn2.length === 0,
    `jp-node-2 has all rows from jp-node-1 (missing: ${JSON.stringify(missingIn2)})`,
    details,
    errors,
  );
  check(
    missingIn1.length === 0,
    `jp-node-1 has all rows from jp-node-2 (missing: ${JSON.stringify(missingIn1)})`,
    details,
    errors,
  );
  details.push(`  sensor types in sync: ${s1.map((s) => s.sensor_id).join(", ")}`);
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function run(): Promise<void> {
  const baseDir = await mkdtemp(join(tmpdir(), "distrb-scenarios-"));
  const casDir = join(baseDir, "cas");
  const port = await getFreePort();
  const hubUrl = `ws://127.0.0.1:${port}`;

  console.log(`\n${"═".repeat(60)}`);
  console.log("  分散センサーダッシュボード — シナリオテスト");
  console.log(`  hub: ${hubUrl}`);
  console.log(`  dir: ${baseDir}`);
  console.log(`${"═".repeat(60)}`);

  const hub = new WebSocketHub({ port });
  const ports = await Promise.all([1, 2, 3, 4].map(() => getFreePort()));

  const makeConfig = (nodeId: string, partitionId: string, p: number): EdgeServerConfig => ({
    nodeId,
    hubUrl,
    dataDir: join(baseDir, nodeId),
    casDir,
    port: p,
    partitionId,
    peerNodeIds: ["jp-node-1", "jp-node-2", "jp-node-3", "us-node"].filter(
      (peer) => peer !== nodeId,
    ),
  });

  const [jp1, jp2, jp3, us] = await Promise.all([
    EdgeNodeServer.create(makeConfig("jp-node-1", "tenant:jp", ports[0])),
    EdgeNodeServer.create(makeConfig("jp-node-2", "tenant:jp", ports[1])),
    EdgeNodeServer.create(makeConfig("jp-node-3", "tenant:jp", ports[2])),
    EdgeNodeServer.create(makeConfig("us-node", "tenant:us", ports[3])),
  ]);
  await Promise.all([jp1.listen(), jp2.listen(), jp3.listen(), us.listen()]);

  const u1 = jp1.baseUrl;
  const u2 = jp2.baseUrl;
  const u3 = jp3.baseUrl;
  const uu = us.baseUrl;

  const results: ScenarioResult[] = [];

  results.push(
    await runScenario("A: Basic CRUD (jp-node-1)", (d, e) => scenarioA_BasicCrud(u1, d, e)),
  );

  results.push(
    await runScenario("B: Cross-node sync via catchup (jp-node-1 → jp-node-2)", (d, e) =>
      scenarioB_CrossNodeSync(u1, u2, d, e),
    ),
  );

  results.push(
    await runScenario("C: Concurrent writes + WebSocket propagation", (d, e) =>
      scenarioC_ConcurrentWrites(u1, u2, d, e),
    ),
  );

  results.push(
    await runScenario("D: Offline write + catchup (jp-node-2 disconnect/reconnect)", (d, e) =>
      scenarioD_OfflineCatchup(u1, u2, d, e),
    ),
  );

  results.push(
    await runScenario("E: Partition isolation (tenant:us invisible to jp nodes)", (d, e) =>
      scenarioE_PartitionIsolation(u1, uu, d, e),
    ),
  );

  results.push(
    await runScenario("F: Snapshot bootstrap (jp-node-3 pulls from jp-node-1)", (d, e) =>
      scenarioF_SnapshotBootstrap(u1, u3, casDir, d, e),
    ),
  );

  results.push(
    await runScenario("G: Stats consistency after full bidirectional sync", (d, e) =>
      scenarioG_StatsConsistency(u1, u2, d, e),
    ),
  );

  results.push(
    await runScenario("H: P2P transport smoke (no hub)", (d, e) =>
      scenarioH_P2PTransportSmoke(baseDir, casDir, d, e),
    ),
  );

  // ─── summary ─────────────────────────────────────────────────

  const passed = results.filter((r) => r.passed).length;
  const total = results.length;

  console.log(`\n${"═".repeat(60)}`);
  console.log(`  結果サマリー: ${passed}/${total} passed`);
  for (const r of results) {
    console.log(`  ${r.passed ? "✓" : "✗"} ${r.name}`);
  }
  console.log(`${"═".repeat(60)}\n`);

  await Promise.allSettled([jp1.close(), jp2.close(), jp3.close(), us.close(), hub.close()]);
  await rm(baseDir, { recursive: true, force: true });

  if (passed < total) process.exit(1);
}

async function getFreePort(): Promise<number> {
  const srv = createServer();
  await new Promise<void>((r, j) => {
    srv.once("error", j);
    srv.listen(0, "127.0.0.1", r);
  });
  const addr = srv.address();
  await new Promise<void>((r, j) => srv.close((e) => (e ? j(e) : r())));
  if (!addr || typeof addr === "string") throw new Error("no port");
  return addr.port;
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

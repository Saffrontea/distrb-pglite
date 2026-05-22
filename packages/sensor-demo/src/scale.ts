/**
 * スケールテスト
 *
 * N ノードが同時に書き込み、全ノードが収束するまでの挙動を測定する。
 * N = [2, 5, 10, 20] で繰り返し実行し、以下を計測する:
 *
 *   writeMs       — N ノードが writesPerNode 件を並列書き込みする時間
 *   convergenceMs — 最初の書き込みから全ノードが totalEvents 件揃うまでの時間
 *   catchupMs     — 遅延参加ノードが node-0 から catchup する時間
 *   consistent    — 全ノードの stats が一致するか
 */

import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocketHub } from "@distrb-pglite/node";
import { EdgeNodeServer, type EdgeServerConfig } from "./server.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

async function get<T = unknown>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`GET ${url} → ${res.status}`);
  return res.json() as Promise<T>;
}

async function post<T = unknown>(url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`POST ${url} → ${res.status}: ${await res.text()}`);
  return res.json() as Promise<T>;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function getFreePort(): Promise<number> {
  const srv = createServer();
  await new Promise<void>((r, j) => { srv.once("error", j); srv.listen(0, "127.0.0.1", r); });
  const addr = srv.address();
  await new Promise<void>((r, j) => srv.close((e) => (e ? j(e) : r())));
  if (!addr || typeof addr === "string") throw new Error("no port");
  return addr.port;
}

async function waitForConvergence(
  urls: string[],
  expected: number,
  timeoutMs = 15000,
): Promise<number> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const counts = await Promise.all(
      urls.map((u) => get<{ eventCount: number }>(`${u}/status`).then((s) => s.eventCount)),
    );
    if (counts.every((c) => c >= expected)) return Date.now() - start;
    await sleep(30);
  }
  const counts = await Promise.all(
    urls.map((u) => get<{ eventCount: number }>(`${u}/status`).then((s) => s.eventCount)),
  );
  throw new Error(`timeout: counts=${counts.join(",")}, expected=${expected}`);
}

// ─── scale test for one N ─────────────────────────────────────────────────────

type ScaleResult = {
  N: number;
  writesPerNode: number;
  totalEvents: number;
  writeMs: number;
  convergenceMs: number;
  catchupMs: number;
  snapshotMs: number;
  snapshotCatchupMs: number;
  dataOnlySnapMs: number;
  dataOnlyBlocks: number;
  catalogAndDataBlocks: number;
  consistent: boolean;
};

async function runForN(options: {
  N: number;
  writesPerNode: number;
  hubUrl: string;
  baseDir: string;
  casDir: string;
}): Promise<ScaleResult> {
  const { N, writesPerNode, hubUrl, baseDir, casDir } = options;
  const dir = join(baseDir, `run-${N}`);
  const ports = await Promise.all(Array.from({ length: N }, () => getFreePort()));
  const totalEvents = N * writesPerNode;

  // N ノードを起動
  const nodes = await Promise.all(
    Array.from({ length: N }, async (_, i) => {
      const cfg: EdgeServerConfig = {
        nodeId: `node-${i}`,
        hubUrl,
        dataDir: join(dir, `node-${i}`),
        casDir,
        port: ports[i],
        partitionId: "tenant:jp",
      };
      const srv = await EdgeNodeServer.create(cfg);
      await srv.listen();
      return srv;
    }),
  );
  const urls = nodes.map((n) => n.baseUrl);

  // ── 1. 並列書き込み ──────────────────────────────────────────
  const writeStart = Date.now();
  await Promise.all(
    nodes.map((_, i) =>
      Promise.all(
        Array.from({ length: writesPerNode }, (__, j) =>
          post(`${urls[i]}/readings`, { sensorId: `sensor-${i}`, reading: i * 1000 + j }),
        ),
      ),
    ),
  );
  const writeMs = Date.now() - writeStart;

  // ── 2. 全ノード収束待ち ──────────────────────────────────────
  const convergenceMs = await waitForConvergence(urls, totalEvents);

  // ── 3. 整合性確認 ────────────────────────────────────────────
  const allStats = await Promise.all(
    urls.map((u) => get<{ stats: { sensor_id: string; count: number }[] }>(`${u}/stats`)),
  );
  const expected = Array.from({ length: N }, (_, i) => `sensor-${i}`);
  const consistent = allStats.every((s) => {
    const ids = new Set(s.stats.map((r) => r.sensor_id));
    return expected.every((id) => ids.has(id));
  });

  // ── 4. 遅延参加ノード: catchup ───────────────────────────────
  const latePort = await getFreePort();
  const lateCatchup = await EdgeNodeServer.create({
    nodeId: "late-catchup",
    hubUrl,
    dataDir: join(dir, "late-catchup"),
    casDir,
    port: latePort,
    partitionId: "tenant:jp",
  });
  await lateCatchup.listen();

  const catchupStart = Date.now();
  // node-0 はすべてのイベントを持っている (hub 経由で受け取った)
  await post(`${lateCatchup.baseUrl}/catchup`, { peerNodeId: "node-0" });
  await waitForConvergence([lateCatchup.baseUrl], totalEvents);
  const catchupMs = Date.now() - catchupStart;

  // ── 5. 遅延参加ノード: snapshot pull + delta catchup ─────────
  const snapPort = await getFreePort();
  const lateSnap = await EdgeNodeServer.create({
    nodeId: "late-snap",
    hubUrl,
    dataDir: join(dir, "late-snap"),
    casDir,
    port: snapPort,
    partitionId: "tenant:jp",
  });
  await lateSnap.listen();

  const snapshotStart = Date.now();

  // node-0 でスナップショット作成（blocks を記録）
  const snapResult = await post<{ blocks: number }>(`${urls[0]}/create-snapshot`);
  const catalogAndDataBlocks = snapResult.blocks;

  // カタログとエントリを取得して late-snap に pull させる
  const catalogManifest = await get(`${urls[0]}/catalog`);
  const { entries } = await get<{ entries: unknown[] }>(`${urls[0]}/snapshot-entries`);
  await post(`${lateSnap.baseUrl}/pull-snapshot`, {
    catalogManifest,
    snapshotEntries: entries,
    sourceCasDir: casDir,
    requirement: "bootstrap",
  });
  const snapshotMs = Date.now() - snapshotStart;

  // delta catchup (スナップショット後に書かれたイベントがあれば)
  const snapCatchupStart = Date.now();
  await post(`${lateSnap.baseUrl}/catchup`, { peerNodeId: "node-0" });
  await waitForConvergence([lateSnap.baseUrl], totalEvents);
  const snapshotCatchupMs = Date.now() - snapCatchupStart;

  // N 個の元ノードを閉じてメモリを解放してから step 6 へ
  await Promise.all(nodes.map((n) => n.close()));

  // ── 6. data-only スナップショット比較 ───────────────────────────
  // data-only ノードを catchup で bootstrap してからスナップショット作成し、
  // late-snap (既に bootstrap 済み) が data-refresh で pull する。
  const dataOnlyPort = await getFreePort();
  const dataOnlyNode = await EdgeNodeServer.create({
    nodeId: "data-only-src",
    hubUrl,
    dataDir: join(dir, "data-only-src"),
    casDir,
    port: dataOnlyPort,
    partitionId: "tenant:jp",
    snapshotScope: "data-only",
  });
  await dataOnlyNode.listen();

  // late-catchup (全イベント保持) から catchup
  await post(`${dataOnlyNode.baseUrl}/catchup`, { peerNodeId: "late-catchup" });
  await waitForConvergence([dataOnlyNode.baseUrl], totalEvents);

  const dataOnlyStart = Date.now();

  // data-only スナップショット作成
  const dataOnlySnap = await post<{ blocks: number }>(`${dataOnlyNode.baseUrl}/create-snapshot`);

  // late-snap が data-refresh で pull (既に bootstrap 済みなのでカタログ不要)
  const doManifest = await get(`${dataOnlyNode.baseUrl}/catalog`);
  const { entries: doEntries } = await get<{ entries: unknown[] }>(`${dataOnlyNode.baseUrl}/snapshot-entries`);
  await post(`${lateSnap.baseUrl}/pull-snapshot`, {
    catalogManifest: doManifest,
    snapshotEntries: doEntries,
    sourceCasDir: casDir,
    requirement: "data-refresh",
  });

  const dataOnlySnapMs = Date.now() - dataOnlyStart;
  const dataOnlyBlocks = dataOnlySnap.blocks;

  // cleanup (元ノードは既に閉じ済み)
  await Promise.all([lateCatchup, lateSnap, dataOnlyNode].map((n) => n.close()));

  return {
    N,
    writesPerNode,
    totalEvents,
    writeMs,
    convergenceMs,
    catchupMs,
    snapshotMs,
    snapshotCatchupMs,
    dataOnlySnapMs,
    dataOnlyBlocks,
    catalogAndDataBlocks,
    consistent,
  };
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function run(): Promise<void> {
  const NS = [2, 5, 10];  // N=20 は PGlite WASM インスタンスのメモリ制約で除外
  const WRITES_PER_NODE = 10;

  const baseDir = await mkdtemp(join(tmpdir(), "distrb-scale-"));
  const casDir  = join(baseDir, "cas");
  const port    = await getFreePort();
  const hubUrl  = `ws://127.0.0.1:${port}`;

  console.log(`\n${"═".repeat(72)}`);
  console.log("  分散スケールテスト");
  console.log(`  writes/node = ${WRITES_PER_NODE},  N ∈ ${JSON.stringify(NS)}`);
  console.log(`  hub: ${hubUrl}`);
  console.log(`${"═".repeat(72)}`);

  const hub = new WebSocketHub({ port });
  const results: ScaleResult[] = [];

  for (const N of NS) {
    process.stdout.write(`\n  N=${N}  (${N * WRITES_PER_NODE} total events) ... `);
    const t0 = Date.now();

    try {
      const result = await runForN({ N, writesPerNode: WRITES_PER_NODE, hubUrl, baseDir, casDir });
      results.push(result);
      process.stdout.write(`done in ${Date.now() - t0}ms\n`);
    } catch (err) {
      process.stdout.write(`ERROR: ${err}\n`);
    }
  }

  // ─── テーブル出力 ─────────────────────────────────────────────

  const col = (s: string | number, w: number) => String(s).padStart(w);

  console.log(`\n${"═".repeat(88)}`);
  console.log("  結果");
  console.log(`${"─".repeat(88)}`);
  console.log(
    `  ${"N".padEnd(4)} ${"total".padStart(6)} ${"write(ms)".padStart(10)} ${"conv(ms)".padStart(9)} ${"CU(ms)".padStart(7)} ${"snap(ms)".padStart(9)} ${"ΔCU(ms)".padStart(8)} ${"c&d blk".padStart(8)} ${"d-only(ms)".padStart(11)} ${"d-only blk".padStart(11)} ${"OK".padStart(3)}`,
  );
  console.log(`  ${"─".repeat(84)}`);

  for (const r of results) {
    console.log(
      `  ${col(r.N, 4)} ${col(r.totalEvents, 6)} ${col(r.writeMs, 10)} ${col(r.convergenceMs, 9)} ${col(r.catchupMs, 7)} ${col(r.snapshotMs, 9)} ${col(r.snapshotCatchupMs, 8)} ${col(r.catalogAndDataBlocks, 8)} ${col(r.dataOnlySnapMs, 11)} ${col(r.dataOnlyBlocks, 11)} ${r.consistent ? "  ✓" : "  ✗"}`,
    );
  }

  console.log(`${"─".repeat(88)}`);
  console.log("  conv(ms)    = 書き込み開始から全ノードが totalEvents 件揃うまで");
  console.log("  CU(ms)      = 遅延参加ノードが node-0 から catchup する時間");
  console.log("  snap(ms)    = catalog-and-data スナップショット作成 + bootstrap pull");
  console.log("  ΔCU(ms)     = スナップショット後の delta catchup");
  console.log("  c&d blk     = catalog-and-data スナップショットのブロック数");
  console.log("  d-only(ms)  = data-only スナップショット作成 + data-refresh pull");
  console.log("  d-only blk  = data-only スナップショットのブロック数");
  console.log(`${"═".repeat(88)}\n`);

  await hub.close();
  await rm(baseDir, { recursive: true, force: true });
}

run().catch((err) => { console.error(err); process.exit(1); });

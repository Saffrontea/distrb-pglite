import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import * as fc from "fast-check";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketHub } from "@distrb-pglite/node";
import { EdgeNodeServer, type SensorPayload } from "../src/server.js";

type DemoOp =
  | { kind: "write"; node: 0 | 1; sensorId: string; reading: number; unit?: string }
  | { kind: "disconnect"; node: 0 | 1 }
  | { kind: "connect"; node: 0 | 1 }
  | { kind: "catchup"; node: 0 | 1 };

type EventRow = {
  eventId: string;
  sourceNodeId: string;
  payload: SensorPayload;
};

type ReadingRow = {
  event_id: string;
  sensor_id: string;
  reading: string;
  unit: string;
  source_node: string;
};

type StatRow = {
  sensor_id: string;
  avg_reading: string;
  min_reading: string;
  max_reading: string;
  count: number;
};

describe("PBT: sensor-demo EdgeNodeServer", () => {
  const hubs: WebSocketHub[] = [];
  const servers: EdgeNodeServer[] = [];
  const tempDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(servers.splice(0).map((server) => server.close().catch(() => {})));
    await Promise.all(hubs.splice(0).map((hub) => hub.close().catch(() => {})));
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("random online/offline writes converge without drifting from the SQL projection", async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(operationArb, { minLength: 3, maxLength: 8 }), async (ops) => {
        const fixture = await startFixture();
        try {
          const online = [true, true];
          const expectedById = new Map<string, {
            sourceNode: string;
            payload: SensorPayload;
          }>();

          for (const op of ops) {
            switch (op.kind) {
              case "write": {
                const server = fixture.servers[op.node];
                const response = online[op.node]
                  ? await post<{ eventId: string }>(`${server.baseUrl}/readings`, {
                      sensorId: op.sensorId,
                      reading: op.reading,
                      unit: op.unit,
                    })
                  : await post<{ eventId: string }>(`${server.baseUrl}/readings-offline`, {
                      sensorId: op.sensorId,
                      reading: op.reading,
                      unit: op.unit,
                    });
                expectedById.set(response.eventId, {
                  sourceNode: server.nodeId,
                  payload: { sensorId: op.sensorId, reading: op.reading, unit: op.unit },
                });
                break;
              }
              case "disconnect":
                if (online[op.node]) {
                  await post(`${fixture.servers[op.node].baseUrl}/disconnect`, {});
                  online[op.node] = false;
                }
                break;
              case "connect":
                if (!online[op.node]) {
                  await post(`${fixture.servers[op.node].baseUrl}/connect`, {});
                  online[op.node] = true;
                }
                break;
              case "catchup":
                if (online[op.node] && online[1 - op.node]) {
                  await post(`${fixture.servers[op.node].baseUrl}/catchup`, {
                    peerNodeId: fixture.servers[1 - op.node].nodeId,
                  });
                }
                break;
            }
          }

          for (const node of [0, 1] as const) {
            if (!online[node]) {
              await post(`${fixture.servers[node].baseUrl}/connect`, {});
            }
          }

          await post(`${fixture.servers[0].baseUrl}/catchup`, { peerNodeId: fixture.servers[1].nodeId });
          await post(`${fixture.servers[1].baseUrl}/catchup`, { peerNodeId: fixture.servers[0].nodeId });
          await post(`${fixture.servers[0].baseUrl}/catchup`, { peerNodeId: fixture.servers[1].nodeId });
          await post(`${fixture.servers[1].baseUrl}/catchup`, { peerNodeId: fixture.servers[0].nodeId });

          for (const server of fixture.servers) {
            const events = await get<{ events: EventRow[] }>(`${server.baseUrl}/events?limit=1000`);
            const readings = await get<{ rows: ReadingRow[] }>(`${server.baseUrl}/readings?limit=1000`);
            const stats = await get<{ stats: StatRow[] }>(`${server.baseUrl}/stats`);

            expect(eventKey(events.events)).toEqual([...expectedById.keys()].sort());
            expect(readingKey(readings.rows)).toEqual([...expectedById.keys()].sort());
            expect(projectRowsByEvent(readings.rows)).toEqual(projectExpectedRows(expectedById));
            expect(statsBySensor(stats.stats)).toEqual(expectedStats(expectedById));
          }
        } finally {
          await fixture.cleanup();
        }
      }),
      { numRuns: 5 },
    );
  }, 60_000);

  it("bootstraps a late node from a snapshot and catches up post-snapshot readings", async () => {
    const fixture = await startSnapshotFixture();
    try {
      const source = fixture.source;
      await post(`${source.baseUrl}/readings`, { sensorId: "snap-a", reading: 10 });
      await post(`${source.baseUrl}/readings`, { sensorId: "snap-b", reading: 20, unit: "F" });

      const snapshot = await post<{ snapshotId: string; blocks: number }>(`${source.baseUrl}/create-snapshot`, {});
      expect(snapshot.snapshotId).toContain("tenant:jp");
      expect(snapshot.blocks).toBeGreaterThan(0);

      await post(`${source.baseUrl}/readings`, { sensorId: "post-snapshot", reading: 999 });

      const target = await fixture.createTarget();
      const catalogManifest = await get(`${source.baseUrl}/catalog`);
      const { entries } = await get<{ entries: unknown[] }>(`${source.baseUrl}/snapshot-entries`);
      expect(entries).toHaveLength(1);

      const pull = await post<{ ok: boolean; snapshotId: string }>(`${target.baseUrl}/pull-snapshot`, {
        catalogManifest,
        snapshotEntries: entries,
        sourceCasDir: fixture.sourceCasDir,
        requirement: "bootstrap",
      });
      expect(pull).toMatchObject({ ok: true, snapshotId: snapshot.snapshotId });

      await post(`${target.baseUrl}/catchup`, { peerNodeId: source.nodeId });

      const sourceReadings = await get<{ rows: ReadingRow[] }>(`${source.baseUrl}/readings?limit=1000`);
      const targetReadings = await get<{ rows: ReadingRow[] }>(`${target.baseUrl}/readings?limit=1000`);
      const sourceStats = await get<{ stats: StatRow[] }>(`${source.baseUrl}/stats`);
      const targetStats = await get<{ stats: StatRow[] }>(`${target.baseUrl}/stats`);

      expect(projectRowsByEvent(targetReadings.rows)).toEqual(projectRowsByEvent(sourceReadings.rows));
      expect(statsBySensor(targetStats.stats)).toEqual(statsBySensor(sourceStats.stats));
    } finally {
      await fixture.cleanup();
    }
  }, 30_000);

  it("does not project explicitly subscribed foreign partitions into the configured sensor table", async () => {
    const fixture = await startPartitionFixture();
    try {
      await post(`${fixture.jp.baseUrl}/subscribe`, { partitionId: "tenant:us" });

      await post(`${fixture.jp.baseUrl}/readings`, { sensorId: "jp-only", reading: 1 });
      await post(`${fixture.us.baseUrl}/readings`, { sensorId: "us-only", reading: 2 });
      await post(`${fixture.jp.baseUrl}/catchup`, {
        peerNodeId: fixture.us.nodeId,
        partitionId: "tenant:us",
      });

      const jpReadings = await get<{ rows: ReadingRow[] }>(`${fixture.jp.baseUrl}/readings?limit=1000`);
      expect(jpReadings.rows.map((row) => row.sensor_id).sort()).toEqual(["jp-only"]);
    } finally {
      await fixture.cleanup();
    }
  }, 20_000);

  it("rejects malformed readings without appending events or SQL projection rows", async () => {
    const fixture = await startFixture();
    try {
      const [server] = fixture.servers;
      const invalidReadings = [
        {},
        { sensorId: "", reading: 1 },
        { sensorId: "bad-reading", reading: "not-a-number" },
        { sensorId: "nan-reading", reading: Number.NaN },
        { sensorId: "bad-unit", reading: 1, unit: "" },
      ];

      for (const body of invalidReadings) {
        const online = await postRaw(`${server.baseUrl}/readings`, body);
        expect(online.status).toBe(500);

        const offline = await postRaw(`${server.baseUrl}/readings-offline`, body);
        expect(offline.status).toBe(500);
      }

      const status = await get<{ eventCount: number }>(`${server.baseUrl}/status`);
      const readings = await get<{ rows: ReadingRow[] }>(`${server.baseUrl}/readings?limit=1000`);
      expect(status.eventCount).toBe(0);
      expect(readings.rows).toEqual([]);
    } finally {
      await fixture.cleanup();
    }
  }, 20_000);

  it("rejects malformed subscribe and catchup requests without changing local state", async () => {
    const fixture = await startFixture();
    try {
      const [server] = fixture.servers;
      await post(`${server.baseUrl}/readings`, { sensorId: "valid", reading: 1 });

      const beforeStatus = await get<{ eventCount: number }>(`${server.baseUrl}/status`);
      const beforeReadings = await get<{ rows: ReadingRow[] }>(`${server.baseUrl}/readings?limit=1000`);

      for (const body of [{}, { partitionId: "" }, { partitionId: 123 }]) {
        const response = await postRaw(`${server.baseUrl}/subscribe`, body);
        expect(response.status).toBe(500);
      }

      for (const body of [{}, { peerNodeId: "" }, { peerNodeId: 123 }, { peerNodeId: "edge-1", partitionId: "" }]) {
        const response = await postRaw(`${server.baseUrl}/catchup`, body);
        expect(response.status).toBe(500);
      }

      const afterStatus = await get<{ eventCount: number }>(`${server.baseUrl}/status`);
      const afterReadings = await get<{ rows: ReadingRow[] }>(`${server.baseUrl}/readings?limit=1000`);
      expect(afterStatus).toEqual(beforeStatus);
      expect(projectRowsByEvent(afterReadings.rows)).toEqual(projectRowsByEvent(beforeReadings.rows));
    } finally {
      await fixture.cleanup();
    }
  }, 20_000);


  async function startFixture(): Promise<{
    servers: [EdgeNodeServer, EdgeNodeServer];
    cleanup: () => Promise<void>;
  }> {
    const hubPort = await getFreePort();
    const nodePorts = [await getFreePort(), await getFreePort()] as const;
    const hub = new WebSocketHub({ port: hubPort });
    hubs.push(hub);

    const root = await makeTempDir("sensor-demo-pbt");
    const mkServer = async (index: 0 | 1) => {
      const server = await EdgeNodeServer.create({
        nodeId: `edge-${index}`,
        hubUrl: `ws://127.0.0.1:${hubPort}`,
        dataDir: join(root, `edge-${index}`, "data"),
        casDir: join(root, `edge-${index}`, "cas"),
        port: nodePorts[index],
        partitionId: "tenant:jp",
        peerNodeIds: [`edge-${1 - index}`],
      });
      servers.push(server);
      await server.listen();
      return server;
    };

    const activeServers: [EdgeNodeServer, EdgeNodeServer] = [await mkServer(0), await mkServer(1)];
    return {
      servers: activeServers,
      cleanup: async () => {
        await Promise.all(activeServers.map((server) => server.close().catch(() => {})));
        await hub.close().catch(() => {});
        await rm(root, { recursive: true, force: true });
      },
    };
  }

  async function makeTempDir(prefix: string): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), `distrb-pglite-${prefix}-`));
    tempDirs.push(dir);
    return dir;
  }

  async function startSnapshotFixture(): Promise<{
    source: EdgeNodeServer;
    sourceCasDir: string;
    createTarget: () => Promise<EdgeNodeServer>;
    cleanup: () => Promise<void>;
  }> {
    const hubPort = await getFreePort();
    const hub = new WebSocketHub({ port: hubPort });
    hubs.push(hub);

    const root = await makeTempDir("sensor-demo-snapshot");
    const source = await createEdgeServer({
      root,
      hubPort,
      index: 0,
      peerNodeIds: ["edge-1"],
    });
    servers.push(source);
    await source.listen();

    let target: EdgeNodeServer | undefined;
    return {
      source,
      sourceCasDir: join(root, "edge-0", "cas"),
      createTarget: async () => {
        if (target) {
          return target;
        }
        target = await createEdgeServer({
          root,
          hubPort,
          index: 1,
          peerNodeIds: ["edge-0"],
        });
        servers.push(target);
        await target.listen();
        return target;
      },
      cleanup: async () => {
        await Promise.all([target, source].filter((server): server is EdgeNodeServer => !!server)
          .map((server) => server.close().catch(() => {})));
        await hub.close().catch(() => {});
        await rm(root, { recursive: true, force: true });
      },
    };
  }

  async function startPartitionFixture(): Promise<{
    jp: EdgeNodeServer;
    us: EdgeNodeServer;
    cleanup: () => Promise<void>;
  }> {
    const hubPort = await getFreePort();
    const hub = new WebSocketHub({ port: hubPort });
    hubs.push(hub);

    const root = await makeTempDir("sensor-demo-partitions");
    const jp = await createEdgeServer({
      root,
      hubPort,
      index: 0,
      partitionId: "tenant:jp",
      peerNodeIds: ["edge-1"],
    });
    const us = await createEdgeServer({
      root,
      hubPort,
      index: 1,
      partitionId: "tenant:us",
      peerNodeIds: ["edge-0"],
    });
    servers.push(jp, us);
    await jp.listen();
    await us.listen();

    return {
      jp,
      us,
      cleanup: async () => {
        await Promise.all([jp, us].map((server) => server.close().catch(() => {})));
        await hub.close().catch(() => {});
        await rm(root, { recursive: true, force: true });
      },
    };
  }

  async function createEdgeServer(input: {
    root: string;
    hubPort: number;
    index: 0 | 1;
    partitionId?: string;
    peerNodeIds: string[];
  }): Promise<EdgeNodeServer> {
    return await EdgeNodeServer.create({
      nodeId: `edge-${input.index}`,
      hubUrl: `ws://127.0.0.1:${input.hubPort}`,
      dataDir: join(input.root, `edge-${input.index}`, "data"),
      casDir: join(input.root, `edge-${input.index}`, "cas"),
      port: await getFreePort(),
      partitionId: input.partitionId ?? "tenant:jp",
      peerNodeIds: input.peerNodeIds,
    });
  }
});

const operationArb: fc.Arbitrary<DemoOp> = fc.oneof(
  fc.record({
    kind: fc.constant("write"),
    node: fc.constantFrom(0, 1),
    sensorId: fc.constantFrom("tokyo-1", "osaka-1", "kyoto-1", "edge-temp", "edge-humidity"),
    reading: fc.integer({ min: -40, max: 120 }),
    unit: fc.option(fc.constantFrom("C", "F", "%"), { nil: undefined }),
  }),
  fc.record({ kind: fc.constant("disconnect"), node: fc.constantFrom(0, 1) }),
  fc.record({ kind: fc.constant("connect"), node: fc.constantFrom(0, 1) }),
  fc.record({ kind: fc.constant("catchup"), node: fc.constantFrom(0, 1) }),
);

function eventKey(events: EventRow[]): string[] {
  return events.map((event) => event.eventId).sort();
}

function readingKey(rows: ReadingRow[]): string[] {
  return rows.map((row) => row.event_id).sort();
}

function projectRowsByEvent(rows: ReadingRow[]): Record<string, unknown> {
  return Object.fromEntries(
    rows
      .map((row) => [
        row.event_id,
        {
          sourceNode: row.source_node,
          sensorId: row.sensor_id,
          reading: Number(row.reading),
          unit: row.unit,
        },
      ])
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}

function projectExpectedRows(
  expectedById: Map<string, { sourceNode: string; payload: SensorPayload }>,
): Record<string, unknown> {
  return Object.fromEntries(
    [...expectedById.entries()]
      .map(([eventId, expected]) => [
        eventId,
        {
          sourceNode: expected.sourceNode,
          sensorId: expected.payload.sensorId,
          reading: expected.payload.reading,
          unit: expected.payload.unit ?? "C",
        },
      ])
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}

function statsBySensor(stats: StatRow[]): Record<string, unknown> {
  return Object.fromEntries(
    stats.map((row) => [
      row.sensor_id,
      {
        count: Number(row.count),
        avg: Number(row.avg_reading),
        min: Number(row.min_reading),
        max: Number(row.max_reading),
      },
    ]),
  );
}

function expectedStats(
  expectedById: Map<string, { payload: SensorPayload }>,
): Record<string, unknown> {
  const readingsBySensor = new Map<string, number[]>();
  for (const { payload } of expectedById.values()) {
    const readings = readingsBySensor.get(payload.sensorId) ?? [];
    readings.push(payload.reading);
    readingsBySensor.set(payload.sensorId, readings);
  }

  return Object.fromEntries(
    [...readingsBySensor.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([sensorId, readings]) => {
        const sum = readings.reduce((total, reading) => total + reading, 0);
        return [
          sensorId,
          {
            count: readings.length,
            avg: round2(sum / readings.length),
            min: Math.min(...readings),
            max: Math.max(...readings),
          },
        ];
      }),
  );
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

async function post<T = unknown>(url: string, body: unknown): Promise<T> {
  return parseJsonResponse<T>(await postRaw(url, body));
}

async function postRaw(url: string, body: unknown): Promise<Response> {
  return await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function get<T>(url: string): Promise<T> {
  return parseJsonResponse<T>(await fetch(url));
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  const json = await response.json() as T | { error: string };
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${JSON.stringify(json)}`);
  }
  return json as T;
}

async function getFreePort(): Promise<number> {
  const server = createServer();
  const port = await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("failed to allocate test port"));
        return;
      }
      resolve(address.port);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
  return port;
}

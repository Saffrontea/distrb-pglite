import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import * as fc from "fast-check";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketHub } from "@distrb-pglite/node";
import { PGlite } from "@electric-sql/pglite";
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
          const expectedById = new Map<
            string,
            {
              sourceNode: string;
              payload: SensorPayload;
            }
          >();

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

          await post(`${fixture.servers[0].baseUrl}/catchup`, {
            peerNodeId: fixture.servers[1].nodeId,
          });
          await post(`${fixture.servers[1].baseUrl}/catchup`, {
            peerNodeId: fixture.servers[0].nodeId,
          });
          await post(`${fixture.servers[0].baseUrl}/catchup`, {
            peerNodeId: fixture.servers[1].nodeId,
          });
          await post(`${fixture.servers[1].baseUrl}/catchup`, {
            peerNodeId: fixture.servers[0].nodeId,
          });

          for (const server of fixture.servers) {
            const events = await get<{ events: EventRow[] }>(`${server.baseUrl}/events?limit=1000`);
            const readings = await get<{ rows: ReadingRow[] }>(
              `${server.baseUrl}/readings?limit=1000`,
            );
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

      const snapshot = await post<{ snapshotId: string; blocks: number }>(
        `${source.baseUrl}/create-snapshot`,
        {},
      );
      expect(snapshot.snapshotId).toContain("tenant:jp");
      expect(snapshot.blocks).toBeGreaterThan(0);

      await post(`${source.baseUrl}/readings`, { sensorId: "post-snapshot", reading: 999 });

      const target = await fixture.createTarget();
      const catalogManifest = await get(`${source.baseUrl}/catalog`);
      const { entries } = await get<{ entries: unknown[] }>(`${source.baseUrl}/snapshot-entries`);
      expect(entries).toHaveLength(1);

      const pull = await post<{ ok: boolean; snapshotId: string }>(
        `${target.baseUrl}/pull-snapshot`,
        {
          catalogManifest,
          snapshotEntries: entries,
          sourceCasDir: fixture.sourceCasDir,
          requirement: "bootstrap",
        },
      );
      expect(pull).toMatchObject({ ok: true, snapshotId: snapshot.snapshotId });

      await post(`${target.baseUrl}/catchup`, { peerNodeId: source.nodeId });

      const sourceReadings = await get<{ rows: ReadingRow[] }>(
        `${source.baseUrl}/readings?limit=1000`,
      );
      const targetReadings = await get<{ rows: ReadingRow[] }>(
        `${target.baseUrl}/readings?limit=1000`,
      );
      const sourceStats = await get<{ stats: StatRow[] }>(`${source.baseUrl}/stats`);
      const targetStats = await get<{ stats: StatRow[] }>(`${target.baseUrl}/stats`);

      expect(projectRowsByEvent(targetReadings.rows)).toEqual(
        projectRowsByEvent(sourceReadings.rows),
      );
      expect(statsBySensor(targetStats.stats)).toEqual(statsBySensor(sourceStats.stats));
    } finally {
      await fixture.cleanup();
    }
  }, 30_000);

  it("migrates old snapshot projection keys after pull before applying catchup", async () => {
    const fixture = await startSnapshotFixture();
    try {
      const source = fixture.source;
      await post(`${source.baseUrl}/readings`, { sensorId: "snapshot-old-key", reading: 10 });
      const sourceEvents = await get<{ events: EventRow[] }>(`${source.baseUrl}/events?limit=1000`);
      const [snapshotEvent] = sourceEvents.events;
      expect(snapshotEvent).toBeDefined();

      await downgradeSensorReadingsPrimaryKey(source);
      const snapshot = await post<{ snapshotId: string }>(`${source.baseUrl}/create-snapshot`, {});

      const target = await fixture.createTarget();
      const catalogManifest = await get(`${source.baseUrl}/catalog`);
      const { entries } = await get<{ entries: unknown[] }>(`${source.baseUrl}/snapshot-entries`);
      await post(`${target.baseUrl}/pull-snapshot`, {
        catalogManifest,
        snapshotEntries: entries,
        sourceCasDir: fixture.sourceCasDir,
        requirement: "bootstrap",
      });

      const peer = await fixture.createPeer(2);
      await seedStoredEvent(peer, {
        sourceNodeId: peer.nodeId,
        eventId: snapshotEvent!.eventId,
        payload: { sensorId: "same-id-new-source", reading: 20 },
      });

      await expect(
        post<{ applied: number }>(`${target.baseUrl}/catchup`, { peerNodeId: peer.nodeId }),
      ).resolves.toEqual({ applied: 1 });

      const readings = await get<{ rows: ReadingRow[] }>(`${target.baseUrl}/readings?limit=1000`);
      expect(readingSourceEventKey(readings.rows)).toContain(
        `${source.nodeId}/${snapshotEvent!.eventId}`,
      );
      expect(readingSourceEventKey(readings.rows)).toContain(
        `${peer.nodeId}/${snapshotEvent!.eventId}`,
      );
      expect(snapshot.snapshotId).toContain("tenant:jp");
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

      const jpReadings = await get<{ rows: ReadingRow[] }>(
        `${fixture.jp.baseUrl}/readings?limit=1000`,
      );
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

  it("rolls back local events when SQL projection fails", async () => {
    const fixture = await startFixture();
    try {
      const [server] = fixture.servers;
      await post(`${server.baseUrl}/sql`, {
        sql: "ALTER TABLE sensor_readings ADD CONSTRAINT reject_projection_sensor CHECK (sensor_id <> 'reject-projection')",
      });

      const response = await postRaw(`${server.baseUrl}/readings`, {
        sensorId: "reject-projection",
        reading: 1,
      });
      expect(response.status).toBe(500);

      const offline = await postRaw(`${server.baseUrl}/readings-offline`, {
        sensorId: "reject-projection",
        reading: 2,
      });
      expect(offline.status).toBe(500);

      const status = await get<{ eventCount: number }>(`${server.baseUrl}/status`);
      const events = await get<{ events: EventRow[] }>(`${server.baseUrl}/events?limit=1000`);
      const readings = await get<{ rows: ReadingRow[] }>(`${server.baseUrl}/readings?limit=1000`);
      expect(status.eventCount).toBe(0);
      expect(events.events).toEqual([]);
      expect(readings.rows).toEqual([]);
    } finally {
      await fixture.cleanup();
    }
  }, 20_000);

  it("rolls back remote catchup events when SQL projection fails so retry can apply them", async () => {
    const fixture = await startFixture();
    try {
      const [source, target] = fixture.servers;
      await post(`${target.baseUrl}/sql`, {
        sql: "ALTER TABLE sensor_readings ADD CONSTRAINT reject_remote_projection CHECK (sensor_id <> 'remote-reject')",
      });
      const sourceEvent = await post<{ eventId: string }>(`${source.baseUrl}/readings-offline`, {
        sensorId: "remote-reject",
        reading: 1,
      });

      const failedCatchup = await postRaw(`${target.baseUrl}/catchup`, {
        peerNodeId: source.nodeId,
      });
      expect(failedCatchup.status).toBe(500);

      const failedStatus = await get<{ eventCount: number }>(`${target.baseUrl}/status`);
      const failedEvents = await get<{ events: EventRow[] }>(`${target.baseUrl}/events?limit=1000`);
      const failedReadings = await get<{ rows: ReadingRow[] }>(
        `${target.baseUrl}/readings?limit=1000`,
      );
      expect(failedStatus.eventCount).toBe(0);
      expect(failedEvents.events).toEqual([]);
      expect(failedReadings.rows).toEqual([]);

      await post(`${target.baseUrl}/sql`, {
        sql: "ALTER TABLE sensor_readings DROP CONSTRAINT reject_remote_projection",
      });
      await expect(
        post<{ applied: number }>(`${target.baseUrl}/catchup`, {
          peerNodeId: source.nodeId,
        }),
      ).resolves.toEqual({ applied: 1 });

      const retriedEvents = await get<{ events: EventRow[] }>(
        `${target.baseUrl}/events?limit=1000`,
      );
      const retriedReadings = await get<{ rows: ReadingRow[] }>(
        `${target.baseUrl}/readings?limit=1000`,
      );
      expect(retriedEvents.events.map((event) => event.eventId)).toEqual([sourceEvent.eventId]);
      expect(readingKey(retriedReadings.rows)).toEqual([sourceEvent.eventId]);
    } finally {
      await fixture.cleanup();
    }
  }, 20_000);

  it("migrates old event-id-only projection keys to source-scoped event identity", async () => {
    const hubPort = await getFreePort();
    const hub = new WebSocketHub({ port: hubPort });
    hubs.push(hub);
    const root = await makeTempDir("sensor-demo-old-projection-key");
    const dataDir = join(root, "edge-0", "data");
    await mkdir(dataDir, { recursive: true });
    const oldDb = new PGlite(dataDir, { relaxedDurability: false });
    await oldDb.exec(`
      CREATE TABLE sensor_groups (id serial PRIMARY KEY);
      CREATE TABLE sensor_readings (
        event_id text PRIMARY KEY,
        sensor_id text NOT NULL,
        reading numeric NOT NULL,
        unit text NOT NULL DEFAULT 'C',
        recorded_at timestamptz NOT NULL DEFAULT now(),
        source_node text NOT NULL
      );
    `);
    await oldDb.close();

    const server = await EdgeNodeServer.create({
      nodeId: "edge-0",
      hubUrl: `ws://127.0.0.1:${hubPort}`,
      dataDir,
      casDir: join(root, "edge-0", "cas"),
      port: await getFreePort(),
      partitionId: "tenant:jp",
      peerNodeIds: [],
    });
    servers.push(server);
    await server.listen();

    const eventId = "018bcfe5-6800-7001-8000-000000000001";
    await post(`${server.baseUrl}/sql`, {
      sql: `
        INSERT INTO sensor_readings (event_id, sensor_id, reading, source_node)
        VALUES
          (${sqlString(eventId)}, 'same-id-a', 1, 'edge-a'),
          (${sqlString(eventId)}, 'same-id-b', 2, 'edge-b')
      `,
    });

    const result = await post<{ rows: Array<{ count: number }> }>(`${server.baseUrl}/sql`, {
      sql: "SELECT count(*)::int AS count FROM sensor_readings",
    });
    expect(result.rows).toEqual([{ count: 2 }]);
  }, 20_000);

  it("projects same event ids from different source nodes independently", async () => {
    const fixture = await startThreeNodeFixture();
    try {
      const [sourceA, sourceB, target] = fixture.servers;
      const eventId = "018bcfe5-6800-7001-8000-000000000001";
      await seedStoredEvent(sourceA, {
        sourceNodeId: sourceA.nodeId,
        eventId,
        payload: { sensorId: "same-id-a", reading: 1 },
      });
      await seedStoredEvent(sourceB, {
        sourceNodeId: sourceB.nodeId,
        eventId,
        payload: { sensorId: "same-id-b", reading: 2 },
      });

      await expect(
        post<{ applied: number }>(`${target.baseUrl}/catchup`, { peerNodeId: sourceA.nodeId }),
      ).resolves.toEqual({ applied: 1 });
      await expect(
        post<{ applied: number }>(`${target.baseUrl}/catchup`, { peerNodeId: sourceB.nodeId }),
      ).resolves.toEqual({ applied: 1 });

      const events = await get<{ events: EventRow[] }>(`${target.baseUrl}/events?limit=1000`);
      const readings = await get<{ rows: ReadingRow[] }>(
        `${target.baseUrl}/readings?limit=1000`,
      );

      expect(sourceEventKey(events.events)).toEqual([
        `${sourceA.nodeId}/${eventId}`,
        `${sourceB.nodeId}/${eventId}`,
      ]);
      expect(readingSourceEventKey(readings.rows)).toEqual([
        `${sourceA.nodeId}/${eventId}`,
        `${sourceB.nodeId}/${eventId}`,
      ]);
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
      const beforeReadings = await get<{ rows: ReadingRow[] }>(
        `${server.baseUrl}/readings?limit=1000`,
      );

      for (const body of [{}, { partitionId: "" }, { partitionId: 123 }]) {
        const response = await postRaw(`${server.baseUrl}/subscribe`, body);
        expect(response.status).toBe(500);
      }

      for (const body of [
        {},
        { peerNodeId: "" },
        { peerNodeId: 123 },
        { peerNodeId: "edge-1", partitionId: "" },
      ]) {
        const response = await postRaw(`${server.baseUrl}/catchup`, body);
        expect(response.status).toBe(500);
      }

      const afterStatus = await get<{ eventCount: number }>(`${server.baseUrl}/status`);
      const afterReadings = await get<{ rows: ReadingRow[] }>(
        `${server.baseUrl}/readings?limit=1000`,
      );
      expect(afterStatus).toEqual(beforeStatus);
      expect(projectRowsByEvent(afterReadings.rows)).toEqual(
        projectRowsByEvent(beforeReadings.rows),
      );
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
    createPeer: (index: number) => Promise<EdgeNodeServer>;
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
    const peers: EdgeNodeServer[] = [];
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
      createPeer: async (index) => {
        const peer = await createEdgeServer({
          root,
          hubPort,
          index,
          peerNodeIds: ["edge-0", "edge-1"],
        });
        peers.push(peer);
        servers.push(peer);
        await peer.listen();
        return peer;
      },
      cleanup: async () => {
        await Promise.all(
          [...peers, target, source]
            .filter((server): server is EdgeNodeServer => !!server)
            .map((server) => server.close().catch(() => {})),
        );
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

  async function startThreeNodeFixture(): Promise<{
    servers: [EdgeNodeServer, EdgeNodeServer, EdgeNodeServer];
    cleanup: () => Promise<void>;
  }> {
    const hubPort = await getFreePort();
    const hub = new WebSocketHub({ port: hubPort });
    hubs.push(hub);

    const root = await makeTempDir("sensor-demo-three-node");
    const sourceA = await createEdgeServer({
      root,
      hubPort,
      index: 0,
      peerNodeIds: ["edge-1", "edge-2"],
    });
    const sourceB = await createEdgeServer({
      root,
      hubPort,
      index: 1,
      peerNodeIds: ["edge-0", "edge-2"],
    });
    const target = await createEdgeServer({
      root,
      hubPort,
      index: 2,
      peerNodeIds: ["edge-0", "edge-1"],
    });
    servers.push(sourceA, sourceB, target);
    await sourceA.listen();
    await sourceB.listen();
    await target.listen();

    return {
      servers: [sourceA, sourceB, target],
      cleanup: async () => {
        await Promise.all([sourceA, sourceB, target].map((server) => server.close().catch(() => {})));
        await hub.close().catch(() => {});
        await rm(root, { recursive: true, force: true });
      },
    };
  }

  async function createEdgeServer(input: {
    root: string;
    hubPort: number;
    index: number;
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

  async function seedStoredEvent(
    server: EdgeNodeServer,
    event: { sourceNodeId: string; eventId: string; payload: SensorPayload },
  ): Promise<void> {
    await post(`${server.baseUrl}/sql`, {
      sql: `
        INSERT INTO distrb_events (
          source_node_id,
          event_id,
          partition_id,
          schema_version,
          payload
        )
        VALUES (
          ${sqlString(event.sourceNodeId)},
          ${sqlString(event.eventId)}::uuid,
          'tenant:jp',
          1,
          ${sqlString(JSON.stringify(event.payload))}::jsonb
        );
      `,
    });
  }

  async function downgradeSensorReadingsPrimaryKey(server: EdgeNodeServer): Promise<void> {
    const primaryKey = await post<{
      rows: Array<{ constraint_name: string }>;
    }>(`${server.baseUrl}/sql`, {
      sql: `
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = 'sensor_readings'
          AND constraint_type = 'PRIMARY KEY'
      `,
    });
    const constraintName = primaryKey.rows[0]?.constraint_name;
    if (!constraintName) {
      throw new Error("sensor_readings primary key not found");
    }

    await post(`${server.baseUrl}/sql`, {
      sql: `
        ALTER TABLE sensor_readings DROP CONSTRAINT ${quoteSqlIdentifier(constraintName)};
        ALTER TABLE sensor_readings ADD PRIMARY KEY (event_id);
      `,
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

function sourceEventKey(events: EventRow[]): string[] {
  return events.map((event) => `${event.sourceNodeId}/${event.eventId}`).sort();
}

function readingSourceEventKey(rows: ReadingRow[]): string[] {
  return rows.map((row) => `${row.source_node}/${row.event_id}`).sort();
}

function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function quoteSqlIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
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
  const json = (await response.json()) as T | { error: string };
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
    server.close((error) => (error ? reject(error) : resolve()));
  });
  return port;
}

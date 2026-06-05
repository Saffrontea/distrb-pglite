import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryTransport } from "@distrb-pglite/core";
import { afterEach, describe, expect, it } from "vitest";
import { VectorNode } from "../src/vector.js";

describe("VectorNode", () => {
  const nodes: VectorNode[] = [];
  const tempDirs: string[] = [];

  afterEach(async () => {
    await Promise.all(nodes.splice(0).map((node) => node.close().catch(() => {})));
    await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  });

  it("rolls back local vector events when SQL projection fails", async () => {
    const dir = await makeTempDir("vector-local-projection");
    const node = await VectorNode.create("vector-node-a", dir, new MemoryTransport(), ["jp"]);
    nodes.push(node);

    await node.db.exec(
      "ALTER TABLE sensor_profiles_jp ADD CONSTRAINT reject_local_vector CHECK (label <> 'reject-local')",
    );

    await expect(
      node.writeProfile({
        tenantId: "jp",
        sensorId: "local-reject",
        label: "reject-local",
        embedding: [1, 0, 0],
      }),
    ).rejects.toThrow("reject_local_vector");

    await expect(
      node.store.listEventsAfterCursors("tenant:jp:vectors", new Map()),
    ).resolves.toHaveLength(0);
    await expect(countProfiles(node, "jp")).resolves.toBe(0);
  });

  it("rolls back remote vector catchup events when SQL projection fails so retry can apply them", async () => {
    const root = await makeTempDir("vector-remote-projection");
    const transport = new MemoryTransport();
    const source = await VectorNode.create(
      "vector-node-source",
      join(root, "source"),
      transport,
      ["jp"],
    );
    nodes.push(source);

    await source.writeProfile({
      tenantId: "jp",
      sensorId: "remote-reject",
      label: "reject-remote",
      embedding: [0, 1, 0],
    });

    const target = await VectorNode.create(
      "vector-node-target",
      join(root, "target"),
      transport,
      ["jp"],
    );
    nodes.push(target);
    await target.db.exec(
      "ALTER TABLE sensor_profiles_jp ADD CONSTRAINT reject_remote_vector CHECK (label <> 'reject-remote')",
    );

    await expect(target.catchUpTenantFrom("jp", source.nodeId)).rejects.toThrow(
      "reject_remote_vector",
    );
    await expect(
      target.store.listEventsAfterCursors("tenant:jp:vectors", new Map()),
    ).resolves.toHaveLength(0);
    await expect(countProfiles(target, "jp")).resolves.toBe(0);

    await target.db.exec("ALTER TABLE sensor_profiles_jp DROP CONSTRAINT reject_remote_vector");
    await expect(target.catchUpTenantFrom("jp", source.nodeId)).resolves.toBe(1);
    await expect(
      target.store.listEventsAfterCursors("tenant:jp:vectors", new Map()),
    ).resolves.toHaveLength(1);
    await expect(countProfiles(target, "jp")).resolves.toBe(1);
  });

  it("rejects vector payloads whose tenant does not match the event partition", async () => {
    const dir = await makeTempDir("vector-tenant-mismatch");
    const node = await VectorNode.create("vector-node-a", dir, new MemoryTransport(), ["jp", "eu"]);
    nodes.push(node);

    await expect(
      node.store.applyRemote({
        eventId: "018bcfe5-6800-7001-8000-000000000001",
        sourceNodeId: "vector-node-b",
        partitionId: "tenant:jp:vectors",
        schemaVersion: 1,
        payload: {
          tenantId: "eu",
          sensorId: "cross-tenant",
          label: "cross tenant",
          embedding: [0, 0, 1],
        },
      }),
    ).rejects.toThrow("vector payload tenant mismatch");

    await expect(
      node.store.listEventsAfterCursors("tenant:jp:vectors", new Map()),
    ).resolves.toHaveLength(0);
    await expect(countProfiles(node, "jp")).resolves.toBe(0);
    await expect(countProfiles(node, "eu")).resolves.toBe(0);
  });

  it("does not let delayed older vector events roll back the latest sensor profile", async () => {
    const dir = await makeTempDir("vector-delayed-older-event");
    const node = await VectorNode.create("vector-node-a", dir, new MemoryTransport(), ["jp"]);
    nodes.push(node);

    await expect(
      node.store.applyRemote({
        eventId: "018bcfe5-6800-7002-8000-000000000002",
        sourceNodeId: "vector-node-b",
        partitionId: "tenant:jp:vectors",
        schemaVersion: 1,
        payload: {
          tenantId: "jp",
          sensorId: "same-sensor",
          label: "newer",
          embedding: [0, 1, 0],
        },
      }),
    ).resolves.toBe(true);
    await expect(
      node.store.applyRemote({
        eventId: "018bcfe5-6800-7001-8000-000000000001",
        sourceNodeId: "vector-node-b",
        partitionId: "tenant:jp:vectors",
        schemaVersion: 1,
        payload: {
          tenantId: "jp",
          sensorId: "same-sensor",
          label: "older",
          embedding: [1, 0, 0],
        },
      }),
    ).resolves.toBe(true);

    await expect(
      node.store.listEventsAfterCursors("tenant:jp:vectors", new Map()),
    ).resolves.toHaveLength(2);
    await expect(profileLabel(node, "jp", "same-sensor")).resolves.toBe("newer");
  });

  async function makeTempDir(prefix: string): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), `distrb-pglite-${prefix}-`));
    tempDirs.push(dir);
    return dir;
  }
});

async function countProfiles(node: VectorNode, tenantId: "jp" | "eu"): Promise<number> {
  const table = tenantId === "jp" ? "sensor_profiles_jp" : "sensor_profiles_eu";
  const result = await node.db.query<{ count: number | string }>(
    `SELECT count(*)::int AS count FROM ${table}`,
  );
  return Number(result.rows[0]?.count ?? 0);
}

async function profileLabel(
  node: VectorNode,
  tenantId: "jp" | "eu",
  sensorId: string,
): Promise<string | undefined> {
  const table = tenantId === "jp" ? "sensor_profiles_jp" : "sensor_profiles_eu";
  const result = await node.db.query<{ label: string }>(
    `SELECT label FROM ${table} WHERE sensor_id = $1`,
    [sensorId],
  );
  return result.rows[0]?.label;
}

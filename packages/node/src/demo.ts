import { PGlite } from "@electric-sql/pglite";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import { SyncNode, type EventEnvelope } from "@distrb-pglite/core";
import { PGlitePartitionStore } from "./pglite-store.js";
import { WebSocketHub, WebSocketTransport } from "./websocket-transport.js";

export type NodeDemoSummary = {
  baseDir: string;
  port: number;
  onlineEvent: EventEnvelope;
  offlineEvent: EventEnvelope;
  nodeBEventsBeforeCatchup: number;
  nodeBEventsAfterCatchup: number;
  nodeCApplied: number;
  nodeCEvents: number;
  ignoredPartitionEvents: number;
};

export async function runNodeDemo(options: {
  baseDir?: string;
  port?: number;
} = {}): Promise<NodeDemoSummary> {
  const baseDir = options.baseDir ?? await mkdtemp(join(tmpdir(), "distrb-pglite-demo-"));
  const port = options.port ?? await getFreePort();
  const hub = new WebSocketHub({ port });
  const url = `ws://127.0.0.1:${port}`;

  const storeA = await openDemoStore("node-a", join(baseDir, "node-a"));
  const storeB = await openDemoStore("node-b", join(baseDir, "node-b"));
  const storeC = await openDemoStore("node-c", join(baseDir, "node-c"));
  const storeIgnored = await openDemoStore("node-us", join(baseDir, "node-us"));

  let nodeA = new SyncNode("node-a", storeA, new WebSocketTransport(url));
  const nodeB = new SyncNode("node-b", storeB, new WebSocketTransport(url));
  const nodeC = new SyncNode("node-c", storeC, new WebSocketTransport(url));
  const nodeIgnored = new SyncNode("node-us", storeIgnored, new WebSocketTransport(url));

  try {
    await nodeA.start();
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");
    await nodeIgnored.start();
    await nodeIgnored.subscribe("tenant:us");

    const onlineEvent = await nodeA.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensor: "tokyo-1", reading: 42 },
    });

    await waitFor(async () => {
      const events = await storeB.listEventsAfterCursors("tenant:jp", new Map());
      if (events.length !== 1) {
        throw new Error(`expected node-b to receive one event, got ${events.length}`);
      }
    });

    const ignoredPartitionEvents = (
      await storeIgnored.listEventsAfterCursors("tenant:jp", new Map())
    ).length;

    await nodeA.stop();
    const offlineEvent = await storeA.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensor: "tokyo-1", reading: 43, mode: "offline" },
    });

    const nodeBEventsBeforeCatchup = (
      await storeB.listEventsAfterCursors("tenant:jp", new Map())
    ).length;

    nodeA = new SyncNode("node-a", storeA, new WebSocketTransport(url));
    await nodeA.start();

    await nodeC.start();
    await nodeC.subscribe("tenant:jp");
    const nodeCApplied = await nodeC.catchUpFrom("node-a", "tenant:jp");
    const nodeCEvents = (
      await storeC.listEventsAfterCursors("tenant:jp", new Map())
    ).length;

    const nodeBApplied = await nodeB.catchUpFrom("node-a", "tenant:jp");
    if (nodeBApplied !== 1) {
      throw new Error(`expected node-b to catch up one offline event, got ${nodeBApplied}`);
    }

    const nodeBEventsAfterCatchup = (
      await storeB.listEventsAfterCursors("tenant:jp", new Map())
    ).length;

    return {
      baseDir,
      port,
      onlineEvent,
      offlineEvent,
      nodeBEventsBeforeCatchup,
      nodeBEventsAfterCatchup,
      nodeCApplied,
      nodeCEvents,
      ignoredPartitionEvents,
    };
  } finally {
    await Promise.allSettled([
      nodeA.stop(),
      nodeB.stop(),
      nodeC.stop(),
      nodeIgnored.stop(),
      hub.close(),
    ]);
  }
}

async function openDemoStore(nodeId: string, dataDir: string): Promise<PGlitePartitionStore> {
  const db = new PGlite(dataDir);
  const store = new PGlitePartitionStore(nodeId, db);
  await store.migrate();
  return store;
}

async function getFreePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });

  if (!address || typeof address === "string") {
    throw new Error("failed to allocate demo port");
  }

  return address.port;
}

async function waitFor(assertion: () => Promise<void>, timeoutMs = 1000): Promise<void> {
  const startedAt = Date.now();
  let lastError: unknown;

  while (Date.now() - startedAt < timeoutMs) {
    try {
      await assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  throw lastError;
}

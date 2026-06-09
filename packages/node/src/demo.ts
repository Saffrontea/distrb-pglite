import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import type { EventEnvelope, PartitionId } from "@distrb-pglite/core";
import { DistributedPGlite } from "./distributed-pglite.js";
import { WebSocketHub } from "./websocket-transport.js";

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

export async function runNodeDemo(
  options: {
    baseDir?: string;
    port?: number;
  } = {},
): Promise<NodeDemoSummary> {
  const baseDir = options.baseDir ?? (await mkdtemp(join(tmpdir(), "distrb-pglite-demo-")));
  const port = options.port ?? (await getFreePort());
  const hub = new WebSocketHub({ port });
  const url = `ws://127.0.0.1:${port}`;

  const nodeA = await openDemoNode("node-a", join(baseDir, "node-a"), url);
  const nodeB = await openDemoNode("node-b", join(baseDir, "node-b"), url, ["tenant:jp"]);
  const nodeC = await openDemoNode("node-c", join(baseDir, "node-c"), url, ["tenant:jp"], false);
  const nodeIgnored = await openDemoNode("node-us", join(baseDir, "node-us"), url, ["tenant:us"]);

  try {
    const onlineEvent = await nodeA.write({
      partitionId: "tenant:jp",
      payload: { sensor: "tokyo-1", reading: 42 },
    });

    await waitFor(async () => {
      const events = await nodeB.store.listEventsAfterCursors("tenant:jp", new Map());
      if (events.length !== 1) {
        throw new Error(`expected node-b to receive one event, got ${events.length}`);
      }
    });

    const ignoredPartitionEvents = (
      await nodeIgnored.store.listEventsAfterCursors("tenant:jp", new Map())
    ).length;

    await nodeA.stop();
    const offlineEvent = await nodeA.writeLocal({
      partitionId: "tenant:jp",
      payload: { sensor: "tokyo-1", reading: 43, mode: "offline" },
    });

    const nodeBEventsBeforeCatchup = (
      await nodeB.store.listEventsAfterCursors("tenant:jp", new Map())
    ).length;

    await nodeA.start();

    await nodeC.start();
    const nodeCApplied = await nodeC.catchUpFrom("node-a", "tenant:jp");
    const nodeCEvents = (await nodeC.store.listEventsAfterCursors("tenant:jp", new Map())).length;

    const nodeBApplied = await nodeB.catchUpFrom("node-a", "tenant:jp");
    if (nodeBApplied !== 1) {
      throw new Error(`expected node-b to catch up one offline event, got ${nodeBApplied}`);
    }

    const nodeBEventsAfterCatchup = (
      await nodeB.store.listEventsAfterCursors("tenant:jp", new Map())
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
      nodeA.close(),
      nodeB.close(),
      nodeC.close(),
      nodeIgnored.close(),
      hub.close(),
    ]);
  }
}

async function openDemoNode(
  nodeId: string,
  dataDir: string,
  url: string,
  partitions: readonly PartitionId[] = [],
  autoStart = true,
): Promise<DistributedPGlite> {
  return DistributedPGlite.open({
    nodeId,
    dataDir,
    transport: { type: "websocket", url },
    autoStart,
    partitions: partitions.map((id) => ({ id })),
  });
}

async function getFreePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
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

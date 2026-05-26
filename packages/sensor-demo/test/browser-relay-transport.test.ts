import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { BrowserWebSocketTransport } from "@distrb-pglite/browser";
import { WebSocketHub } from "@distrb-pglite/node";
import { afterEach, describe, expect, it } from "vitest";

describe("browser relay transport compatibility", () => {
  const hubs: WebSocketHub[] = [];
  const nodes: SyncNode[] = [];

  afterEach(async () => {
    await Promise.all(nodes.splice(0).map((node) => node.stop().catch(() => {})));
    await Promise.all(hubs.splice(0).map((hub) => hub.close()));
  });

  it("replicates browser transport events through the Node WebSocketHub", async () => {
    const url = await startHub();
    const storeB = new InMemoryPartitionStore("browser-b");
    const nodeA = new SyncNode(
      "browser-a",
      new InMemoryPartitionStore("browser-a"),
      new BrowserWebSocketTransport(url),
    );
    const nodeB = new SyncNode(
      "browser-b",
      storeB,
      new BrowserWebSocketTransport(url),
    );
    nodes.push(nodeA, nodeB);

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    await nodeA.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "browser-temp", reading: 23 },
    });

    await waitFor(async () => {
      const events = await storeB.listEventsAfterCursors("tenant:jp", new Map());
      expect(events).toHaveLength(1);
      expect(events[0]?.payload).toEqual({ sensorId: "browser-temp", reading: 23 });
    });
  });

  it("replays missed browser transport events through the Node WebSocketHub", async () => {
    const url = await startHub();
    const storeC = new InMemoryPartitionStore("browser-c");
    const nodeA = new SyncNode(
      "browser-a",
      new InMemoryPartitionStore("browser-a"),
      new BrowserWebSocketTransport(url),
    );
    const nodeC = new SyncNode(
      "browser-c",
      storeC,
      new BrowserWebSocketTransport(url),
    );
    nodes.push(nodeA, nodeC);

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { x: 1 } });
    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { x: 2 } });

    await nodeC.start();
    await nodeC.subscribe("tenant:jp");

    await expect(nodeC.catchUpFrom("browser-a", "tenant:jp")).resolves.toBe(2);
    expect(await storeC.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(2);
  });

  async function startHub(): Promise<string> {
    const port = await getFreePort();
    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    return `ws://127.0.0.1:${port}`;
  }
});

async function waitFor(assertion: () => Promise<void>, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  let last: unknown;
  while (Date.now() - start < timeoutMs) {
    try {
      await assertion();
      return;
    } catch (error) {
      last = error;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  throw last;
}

async function getFreePort(): Promise<number> {
  const { createServer } = await import("node:net");
  const server = createServer();
  const port = await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("no address"));
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

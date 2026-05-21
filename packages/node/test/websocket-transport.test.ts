import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { createServer } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketHub, WebSocketTransport } from "../src/index.js";

describe("WebSocketTransport", () => {
  const hubs: WebSocketHub[] = [];

  afterEach(async () => {
    await Promise.all(hubs.map((hub) => hub.close()));
    hubs.length = 0;
  });

  it("replicates and catches up through the same SyncNode transport contract", async () => {
    const port = await getFreePort();
    if (port === undefined) {
      return;
    }

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const url = `ws://127.0.0.1:${port}`;
    const nodeA = new SyncNode(
      "node-a",
      new InMemoryPartitionStore("node-a"),
      new WebSocketTransport(url),
    );
    const nodeBStore = new InMemoryPartitionStore("node-b");
    const nodeB = new SyncNode("node-b", nodeBStore, new WebSocketTransport(url));

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    await nodeA.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "over ws" },
    });

    await waitFor(async () => {
      const events = await nodeBStore.listEventsAfterCursors("tenant:jp", new Map());
      expect(events).toHaveLength(1);
      expect(events[0]?.payload).toEqual({ title: "over ws" });
    });

    const nodeCStore = new InMemoryPartitionStore("node-c");
    const nodeC = new SyncNode("node-c", nodeCStore, new WebSocketTransport(url));
    await nodeC.start();
    await nodeC.subscribe("tenant:jp");

    await expect(nodeC.catchUpFrom("node-a", "tenant:jp")).resolves.toBe(1);
    await expect(nodeCStore.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(1);

    await nodeA.stop();
    await nodeB.stop();
    await nodeC.stop();
  });
});

async function getFreePort(): Promise<number | undefined> {
  const server = createServer();
  const canListen = await new Promise<boolean>((resolve, reject) => {
    server.once("error", (error: NodeJS.ErrnoException) => {
      if (error.code === "EPERM" || error.code === "EACCES") {
        resolve(false);
        return;
      }
      reject(error);
    });
    server.listen(0, "127.0.0.1", () => resolve(true));
  });

  if (!canListen) {
    return undefined;
  }

  const address = server.address();
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));

  if (!address || typeof address === "string") {
    throw new Error("failed to allocate test port");
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

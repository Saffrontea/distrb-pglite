import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketHub, WebSocketTransport } from "../src/index.js";
import {
  describeTransportContract,
  getFreePort,
  type TransportSetup,
} from "./transport-contract.js";

// ─── contract テスト ──────────────────────────────────────────────────────────

describeTransportContract("WebSocketHub+WebSocketTransport", async () => {
  const port = await getFreePort();
  const hub = new WebSocketHub({ port });
  const url = `ws://127.0.0.1:${port}`;
  const nodes: SyncNode[] = [];

  const setup: TransportSetup = {
    createNode: async (nodeId) => {
      const store = new InMemoryPartitionStore(nodeId);
      const node  = new SyncNode(nodeId, store, new WebSocketTransport(url));
      nodes.push(node);
      return { node, store };
    },
    teardown: async () => {
      await Promise.all(nodes.splice(0).map((n) => n.stop().catch(() => {})));
      await hub.close();
    },
  };
  return setup;
});

// ─── Hub 固有テスト ───────────────────────────────────────────────────────────

describe("WebSocketTransport (Hub 固有)", () => {
  afterEach(async () => {
    await Promise.all(hubs.map((hub) => hub.close()));
    hubs.length = 0;
  });

  it("zstd 圧縮を有効にしてもイベントが届く", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port, compress: true });
    hubs.push(hub);

    const url = `ws://127.0.0.1:${port}`;
    const storeB = new InMemoryPartitionStore("node-b");
    const nodeA  = new SyncNode("node-a", new InMemoryPartitionStore("node-a"), new WebSocketTransport(url, true));
    const nodeB  = new SyncNode("node-b", storeB, new WebSocketTransport(url, true));

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { compressed: true } });

    await waitFor(async () => {
      const events = await storeB.listEventsAfterCursors("tenant:jp", new Map());
      expect(events).toHaveLength(1);
      expect(events[0]?.payload).toEqual({ compressed: true });
    });

    await nodeA.stop();
    await nodeB.stop();
  });
});

const hubs: WebSocketHub[] = [];

async function waitFor(fn: () => Promise<void>, ms = 2000): Promise<void> {
  const start = Date.now();
  let last: unknown;
  while (Date.now() - start < ms) {
    try { await fn(); return; } catch (e) { last = e; }
    await new Promise((r) => setTimeout(r, 20));
  }
  throw last;
}

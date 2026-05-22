/**
 * Transport contract テスト共有ヘルパー
 *
 * P2PWebSocketTransport が WebSocketHub+WebSocketTransport と同等の動作をすることを証明する。
 * describeTransportContract に setup 関数を渡すことで、任意の Transport 実装に対して
 * 同じシナリオを実行できる。
 */

import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { describe, expect, it } from "vitest";

export type CreatedNode = {
  node: SyncNode;
  store: InMemoryPartitionStore;
};

export type TransportSetup = {
  /** 新しいノードを作成して返す。transport は内部で設定済み */
  createNode(nodeId: string): Promise<CreatedNode>;
  /** subscribe 後、write/requestReplay 前に P2P メッシュ確立を待つ（Hub では不要） */
  settle?(): Promise<void>;
  teardown(): Promise<void>;
};

export function describeTransportContract(
  label: string,
  buildSetup: () => Promise<TransportSetup>,
): void {
  describe(label, () => {
    it("購読パーティションのイベントを全ノードにレプリケートする", async () => {
      const { createNode, teardown, settle } = await buildSetup();
      try {
        const { node: nodeA } = await createNode("node-a");
        const { node: nodeB, store: storB } = await createNode("node-b");

        await nodeA.start();
        await nodeA.subscribe("tenant:jp");
        await nodeB.start();
        await nodeB.subscribe("tenant:jp");
        await settle?.();

        await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { v: 1 } });

        await waitFor(async () => {
          const events = await storB.listEventsAfterCursors("tenant:jp", new Map());
          expect(events).toHaveLength(1);
          expect(events[0]?.payload).toEqual({ v: 1 });
        });

        await nodeA.stop();
        await nodeB.stop();
      } finally {
        await teardown();
      }
    });

    it("未購読パーティションのイベントは届かない", async () => {
      const { createNode, teardown, settle } = await buildSetup();
      try {
        const { node: nodeA } = await createNode("node-a");
        const { node: nodeB, store: storB } = await createNode("node-b");

        await nodeA.start();
        await nodeB.start();
        await nodeA.subscribe("tenant:jp");
        await nodeB.subscribe("tenant:jp");
        await settle?.();

        await nodeA.write({ partitionId: "tenant:us", schemaVersion: 1, payload: { ignored: true } });
        await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { visible: true } });

        await waitFor(async () => {
          expect(await storB.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
        });
        expect(await storB.listEventsAfterCursors("tenant:us", new Map())).toHaveLength(0);

        await nodeA.stop();
        await nodeB.stop();
      } finally {
        await teardown();
      }
    });

    it("遅延参加ノードが requestReplay で過去イベントを取得できる", async () => {
      const { createNode, teardown, settle } = await buildSetup();
      try {
        const { node: nodeA } = await createNode("node-a");
        const { node: nodeC, store: storC } = await createNode("node-c");

        await nodeA.start();
        await nodeA.subscribe("tenant:jp");
        await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { x: 1 } });
        await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { x: 2 } });

        await nodeC.start();
        await nodeC.subscribe("tenant:jp");
        await settle?.();

        await expect(nodeC.catchUpFrom("node-a", "tenant:jp")).resolves.toBe(2);
        await expect(nodeC.catchUpFrom("node-a", "tenant:jp")).resolves.toBe(0);
        expect(await storC.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(2);

        await nodeA.stop();
        await nodeC.stop();
      } finally {
        await teardown();
      }
    });

    it("同じイベントを複数回 catchup しても冪等である", async () => {
      const { createNode, teardown, settle } = await buildSetup();
      try {
        const { node: nodeA } = await createNode("node-a");
        const { node: nodeB, store: storB } = await createNode("node-b");

        await nodeA.start();
        await nodeA.subscribe("tenant:jp");
        await nodeB.start();
        await nodeB.subscribe("tenant:jp");
        await settle?.();

        await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { once: true } });

        await waitFor(async () => {
          expect(await storB.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
        });

        await nodeB.catchUpFrom("node-a", "tenant:jp");
        expect(await storB.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);

        await nodeA.stop();
        await nodeB.stop();
      } finally {
        await teardown();
      }
    });

    it("3ノード間で両方向に収束する", async () => {
      const { createNode, teardown, settle } = await buildSetup();
      try {
        const { node: nodeA } = await createNode("node-a");
        const { node: nodeB } = await createNode("node-b");
        const { node: nodeC, store: storC } = await createNode("node-c");

        await nodeA.start();
        await nodeA.subscribe("tenant:jp");
        await nodeB.start();
        await nodeB.subscribe("tenant:jp");
        await nodeC.start();
        await nodeC.subscribe("tenant:jp");
        await settle?.();

        await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { from: "a" } });
        await nodeB.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { from: "b" } });

        await waitFor(async () => {
          expect(await storC.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(2);
        });

        await nodeA.stop();
        await nodeB.stop();
        await nodeC.stop();
      } finally {
        await teardown();
      }
    });
  });
}

// ─── helpers ──────────────────────────────────────────────────────────────────

export async function waitFor(assertion: () => Promise<void>, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  let last: unknown;
  while (Date.now() - start < timeoutMs) {
    try { await assertion(); return; } catch (e) { last = e; }
    await new Promise((r) => setTimeout(r, 20));
  }
  throw last;
}

export async function getFreePort(): Promise<number> {
  const { createServer } = await import("node:net");
  const server = createServer();
  const port = await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") { reject(new Error("no addr")); return; }
      resolve(addr.port);
    });
  });
  await new Promise<void>((r, j) => server.close((e) => e ? j(e) : r()));
  return port;
}

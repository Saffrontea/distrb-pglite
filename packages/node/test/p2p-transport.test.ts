import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { describe, expect, it } from "vitest";
import { P2PWebSocketTransport } from "../src/index.js";
import {
  describeTransportContract,
  getFreePort,
  waitFor,
  type TransportSetup,
} from "./transport-contract.js";

const meshSettleMs = 50;
const restartSettleMs = 20;

// ─── contract テスト ──────────────────────────────────────────────────────────

describeTransportContract("P2PWebSocketTransport", async () => {
  const allocatedPorts: number[] = [];
  const nodes: SyncNode[] = [];

  const setup: TransportSetup = {
    createNode: async (nodeId) => {
      const port = await getFreePort();
      const myUrl = `ws://127.0.0.1:${port}`;
      // すでに割り当てられたポートを seed として指定（gossip でメッシュが拡大）
      const seeds = allocatedPorts.map((p) => `ws://127.0.0.1:${p}`);
      allocatedPorts.push(port);

      const transport = new P2PWebSocketTransport({ publicUrl: myUrl, port, seeds });
      const store = new InMemoryPartitionStore(nodeId);
      const node  = new SyncNode(nodeId, store, transport);
      nodes.push(node);
      return { node, store };
    },
    settle: () => new Promise<void>((r) => setTimeout(r, meshSettleMs)),
    teardown: async () => {
      await Promise.all(nodes.splice(0).map((n) => n.stop().catch(() => {})));
      allocatedPorts.length = 0;
    },
  };
  return setup;
});

// ─── P2P 固有テスト ───────────────────────────────────────────────────────────

describe("P2PWebSocketTransport (P2P 固有)", () => {
  it("gossip で seed を知らないノードもメッシュに参加できる", async () => {
    // A を起動 → B が A を seed にして参加 → C は A だけを seed にするが gossip で B も発見
    const portA = await getFreePort();
    const portB = await getFreePort();
    const portC = await getFreePort();
    const urlA  = `ws://127.0.0.1:${portA}`;
    const urlB  = `ws://127.0.0.1:${portB}`;
    const urlC  = `ws://127.0.0.1:${portC}`;

    const storeA = new InMemoryPartitionStore("node-a");
    const storeB = new InMemoryPartitionStore("node-b");
    const storeC = new InMemoryPartitionStore("node-c");

    const nodeA = new SyncNode("node-a", storeA, new P2PWebSocketTransport({ publicUrl: urlA, port: portA }));
    const nodeB = new SyncNode("node-b", storeB, new P2PWebSocketTransport({ publicUrl: urlB, port: portB, seeds: [urlA] }));
    const nodeC = new SyncNode("node-c", storeC, new P2PWebSocketTransport({ publicUrl: urlC, port: portC, seeds: [urlA] }));

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");
    await nodeC.start();
    await nodeC.subscribe("tenant:jp");

    // gossip が落ち着くまで少し待つ
    await new Promise((r) => setTimeout(r, meshSettleMs));

    // B からイベントを書く → C（B を seed にしていない）にも届く
    await nodeB.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { gossip: true } });

    await waitFor(async () => {
      expect(await storeC.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
    });

    await nodeA.stop();
    await nodeB.stop();
    await nodeC.stop();
  });

  it("ノード切断後に再接続してイベントが再び届く", async () => {
    const portA = await getFreePort();
    const portB = await getFreePort();
    const urlA  = `ws://127.0.0.1:${portA}`;
    const urlB  = `ws://127.0.0.1:${portB}`;

    const storeA = new InMemoryPartitionStore("node-a");
    const storeB = new InMemoryPartitionStore("node-b");

    const nodeA = new SyncNode("node-a", storeA, new P2PWebSocketTransport({
      publicUrl: urlA, port: portA, seeds: [urlB],
      reconnectBaseDelayMs: 10, reconnectMaxDelayMs: 50,
    }));
    const nodeB = new SyncNode("node-b", storeB, new P2PWebSocketTransport({
      publicUrl: urlB, port: portB, seeds: [urlA],
      reconnectBaseDelayMs: 10, reconnectMaxDelayMs: 50,
    }));

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");
    await new Promise((r) => setTimeout(r, meshSettleMs));

    // 初回イベント
    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 1 } });
    await waitFor(async () => {
      expect(await storeB.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
    });

    // B を一度停止して再起動
    await nodeB.stop();
    await new Promise((r) => setTimeout(r, restartSettleMs));

    const storeBNew = new InMemoryPartitionStore("node-b");
    const nodeBNew  = new SyncNode("node-b", storeBNew, new P2PWebSocketTransport({
      publicUrl: urlB, port: portB, seeds: [urlA],
      reconnectBaseDelayMs: 10, reconnectMaxDelayMs: 50,
    }));
    await nodeBNew.start();
    await nodeBNew.subscribe("tenant:jp");
    // 再接続ハンドシェイク完了を待つ
    await new Promise((r) => setTimeout(r, meshSettleMs));

    // 再接続後のイベント
    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 2 } });
    await waitFor(async () => {
      expect(await storeBNew.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
    });

    await nodeA.stop();
    await nodeBNew.stop();
  });

  it("target が未接続の場合 requestReplay は空を返す", async () => {
    const portA = await getFreePort();
    const urlA  = `ws://127.0.0.1:${portA}`;
    const storeA = new InMemoryPartitionStore("node-a");

    const nodeA = new SyncNode("node-a", storeA, new P2PWebSocketTransport({ publicUrl: urlA, port: portA }));
    await nodeA.start();
    await nodeA.subscribe("tenant:jp");

    // node-x は存在しない → 空応答
    const count = await nodeA.catchUpFrom("node-x", "tenant:jp");
    expect(count).toBe(0);

    await nodeA.stop();
  });

  it("requestReplay は応答がない場合にタイムアウトする", async () => {
    const portA = await getFreePort();
    const portB = await getFreePort();
    const urlA  = `ws://127.0.0.1:${portA}`;
    const urlB  = `ws://127.0.0.1:${portB}`;

    const requester = await new P2PWebSocketTransport({
      publicUrl: urlA,
      port: portA,
      seeds: [urlB],
      reconnectBaseDelayMs: 10,
      reconnectMaxDelayMs: 50,
    }).connect("node-a", { onEvent: () => {} });
    const target = await new P2PWebSocketTransport({
      publicUrl: urlB,
      port: portB,
      seeds: [urlA],
      reconnectBaseDelayMs: 10,
      reconnectMaxDelayMs: 50,
    }).connect("node-b", {
      onEvent: () => {},
      onReplayRequest: () => new Promise(() => {}),
    });
    await new Promise((r) => setTimeout(r, meshSettleMs));

    await expect(requester.requestReplay("node-b", "tenant:jp", new Map()))
      .rejects.toThrow("replay request timed out after 1000ms");

    await requester.close();
    await target.close();
  });

  it("publish は connection close 後に失敗する", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    await connection.close();

    await expect(connection.publish({
      sourceNodeId: "node-a",
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("p2p connection is closed");
  });

  it("zstd 圧縮を有効にしてもP2Pイベントが届く", async () => {
    const portA = await getFreePort();
    const portB = await getFreePort();
    const urlA  = `ws://127.0.0.1:${portA}`;
    const urlB  = `ws://127.0.0.1:${portB}`;

    const storeA = new InMemoryPartitionStore("node-a");
    const storeB = new InMemoryPartitionStore("node-b");

    const nodeA = new SyncNode("node-a", storeA, new P2PWebSocketTransport({
      publicUrl: urlA,
      port: portA,
      seeds: [urlB],
      compress: true,
    }));
    const nodeB = new SyncNode("node-b", storeB, new P2PWebSocketTransport({
      publicUrl: urlB,
      port: portB,
      seeds: [urlA],
      compress: true,
    }));

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");
    await new Promise((r) => setTimeout(r, meshSettleMs));

    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { compressed: true } });

    await waitFor(async () => {
      expect(await storeB.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
    });

    await nodeA.stop();
    await nodeB.stop();
  });

  it("A→B と B→A の同時接続でも重複せず1接続になる", async () => {
    const portA = await getFreePort();
    const portB = await getFreePort();
    const urlA  = `ws://127.0.0.1:${portA}`;
    const urlB  = `ws://127.0.0.1:${portB}`;

    const storeA = new InMemoryPartitionStore("node-a");
    const storeB = new InMemoryPartitionStore("node-b");

    // 両方が互いを seed に指定して同時起動 → deduplication
    const nodeA = new SyncNode("node-a", storeA, new P2PWebSocketTransport({ publicUrl: urlA, port: portA, seeds: [urlB] }));
    const nodeB = new SyncNode("node-b", storeB, new P2PWebSocketTransport({ publicUrl: urlB, port: portB, seeds: [urlA] }));

    // 同時 start
    await Promise.all([nodeA.start(), nodeB.start()]);
    await Promise.all([nodeA.subscribe("tenant:jp"), nodeB.subscribe("tenant:jp")]);

    // gossip の落ち着きを待ってからイベント送受信確認
    await new Promise((r) => setTimeout(r, meshSettleMs));

    await nodeA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { dedup: true } });
    await waitFor(async () => {
      expect(await storeB.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(1);
    });

    // B→A 方向も確認
    await nodeB.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { dedup: "b" } });
    await waitFor(async () => {
      expect(await storeA.listEventsAfterCursors("tenant:jp", new Map())).toHaveLength(2);
    });

    await nodeA.stop();
    await nodeB.stop();
  });
});

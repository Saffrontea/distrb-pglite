import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { describe, expect, it, vi } from "vitest";
import WebSocket from "ws";
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
  it("rejects empty node ids before opening a server", async () => {
    const port = await getFreePort();

    await expect(new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("", { onEvent: () => {} }))
      .rejects.toThrow("nodeId must be a non-empty string");
  });

  it("rejects invalid local peer urls before opening a server", async () => {
    const port = await getFreePort();

    expect(() => new P2PWebSocketTransport({
      publicUrl: `http://127.0.0.1:${port}`,
      port,
    })).toThrow("publicUrl must use ws or wss protocol");
    expect(() => new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
      seeds: ["not-a-url"],
    })).toThrow("seeds[0] must be a valid websocket URL");
  });

  it("closes raw peers that send an empty hello node id", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send unknown message types", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({ type: "mystery", value: 1 }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send ping messages with invalid sentAt values", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({ type: "ping", sentAt: "bad" }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send pong messages with invalid sentAt values", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({ type: "pong", sentAt: "bad" }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that try to replace their hello identity on the same socket", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "other-peer",
        publicUrl: "ws://127.0.0.1:2",
        subscriptions: [],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send invalid hello public urls", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "http://127.0.0.1:1",
        subscriptions: [],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that gossip invalid peer urls", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "peers",
        urls: ["http://127.0.0.1:2"],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send an empty subscription partition", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({ type: "subscribe", partitionId: "" }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send invalid replay cursors", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "bad-cursor",
        requesterNodeId: "raw-peer",
        targetNodeId: "node-a",
        partitionId: "tenant:jp",
        cursors: [["raw-peer", "not-a-uuid"]],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send replay requests targeted at another node", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "wrong-target",
        requesterNodeId: "raw-peer",
        targetNodeId: "node-b",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that send an empty replay request id", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "",
        requesterNodeId: "raw-peer",
        targetNodeId: "node-a",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that spoof event source node ids", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "event",
        event: {
          eventId: "018bcfe5-6800-7001-8000-000000000001",
          sourceNodeId: "other-peer",
          partitionId: "tenant:jp",
          schemaVersion: 1,
          payload: {},
        },
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

  it("closes raw peers that spoof replay requester node ids", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "spoofed-requester",
        requesterNodeId: "other-peer",
        targetNodeId: "node-a",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await expect(waitForSocketClose(socket)).resolves.toBeUndefined();
      expect(errorSpy).toHaveBeenCalledWith(
        "[p2p] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
      await connection.close();
    }
  });

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

  it("rejects replay responses that do not match the pending p2p request", async () => {
    const port = await getFreePort();
    const publicUrl = `ws://127.0.0.1:${port}`;
    const connection = await new P2PWebSocketTransport({
      publicUrl,
      port,
    }).connect("node-a", { onEvent: () => {} });
    const socket = await rawPeer(publicUrl);
    try {
      socket.send(JSON.stringify({
        type: "hello",
        nodeId: "raw-peer",
        publicUrl: "ws://127.0.0.1:1",
        subscriptions: [],
      }));
      await new Promise((resolve) => setTimeout(resolve, meshSettleMs));

      const response = connection.requestReplay("raw-peer", "tenant:jp", new Map());
      const request = await nextRawPeerMessage(socket, "replay-request");
      socket.send(JSON.stringify({
        type: "replay-response",
        requestId: request.requestId,
        requesterNodeId: "node-a",
        partitionId: "tenant:us",
        events: [],
      }));

      await expect(response).rejects.toThrow("replay response mismatch");
    } finally {
      socket.terminate();
      await connection.close();
    }
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

  it("rejects invalid events before publishing", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });

    await expect(connection.publish({
      sourceNodeId: "node-a",
      eventId: "not-a-uuid",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("eventId must be a UUIDv7");

    await connection.close();
  });

  it("rejects events whose source does not match the p2p connection", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });

    await expect(connection.publish({
      sourceNodeId: "node-b",
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("event source mismatch");

    await connection.close();
  });

  it("subscribe は connection close 後に失敗する", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    await connection.close();

    await expect(connection.subscribe("tenant:jp"))
      .rejects.toThrow("p2p connection is closed");
  });

  it("rejects empty partition ids before sending subscribe or replay requests", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });

    await expect(connection.subscribe(""))
      .rejects.toThrow("partitionId must be a non-empty string");
    await expect(connection.requestReplay("node-b", "", new Map()))
      .rejects.toThrow("partitionId must be a non-empty string");
    await expect(connection.requestReplay("", "tenant:jp", new Map()))
      .rejects.toThrow("nodeId must be a non-empty string");
    await expect(connection.requestReplay(
      "node-b",
      "tenant:jp",
      new Map([["node-a", "not-a-uuid"]]),
    )).rejects.toThrow("cursor eventId must be a UUIDv7");

    await connection.close();
  });

  it("requestReplay は connection close 後に失敗する", async () => {
    const port = await getFreePort();
    const connection = await new P2PWebSocketTransport({
      publicUrl: `ws://127.0.0.1:${port}`,
      port,
    }).connect("node-a", { onEvent: () => {} });
    await connection.close();

    await expect(connection.requestReplay("node-b", "tenant:jp", new Map()))
      .rejects.toThrow("p2p connection is closed");
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

async function rawPeer(url: string): Promise<WebSocket> {
  const socket = new WebSocket(url);
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  return socket;
}

async function nextRawPeerMessage(socket: WebSocket, type: string, timeoutMs = 1000): Promise<Record<string, unknown>> {
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for ${type}`));
    }, timeoutMs);
    const onMessage = (data: WebSocket.RawData) => {
      const message = JSON.parse(data.toString()) as Record<string, unknown>;
      if (message.type !== type) {
        return;
      }
      cleanup();
      resolve(message);
    };
    const cleanup = () => {
      clearTimeout(timer);
      socket.off("message", onMessage);
    };
    socket.on("message", onMessage);
  });
}

async function waitForSocketClose(socket: WebSocket): Promise<void> {
  if (socket.readyState === WebSocket.CLOSED) {
    return;
  }
  await new Promise<void>((resolve) => socket.once("close", resolve));
}

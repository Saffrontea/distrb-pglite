import { InMemoryPartitionStore, SyncNode } from "@distrb-pglite/core";
import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket, { WebSocketServer } from "ws";
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

  it("rejects empty node ids before opening a websocket", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    await expect(new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("", {
      onEvent: () => {},
    })).rejects.toThrow("nodeId must be a non-empty string");
  });

  it("requestReplay は応答がない場合にタイムアウトする", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const url = `ws://127.0.0.1:${port}`;
    const requester = await new WebSocketTransport(url).connect("node-a", {
      onEvent: () => {},
    });
    const target = await new WebSocketTransport(url).connect("node-b", {
      onEvent: () => {},
      onReplayRequest: () => new Promise(() => {}),
    });

    await expect(requester.requestReplay("node-b", "tenant:jp", new Map()))
      .rejects.toThrow("replay request timed out after 1000ms");

    await requester.close();
    await target.close();
  });

  it("requestReplay は target handler が失敗した場合も未処理 rejection を出さずタイムアウトする", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const url = `ws://127.0.0.1:${port}`;
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const requester = await new WebSocketTransport(url).connect("node-a", {
        onEvent: () => {},
      });
      const target = await new WebSocketTransport(url).connect("node-b", {
        onEvent: () => {},
        onReplayRequest: async () => {
          throw new Error("handler failed");
        },
      });

      await expect(requester.requestReplay("node-b", "tenant:jp", new Map()))
        .rejects.toThrow("replay request timed out after 1000ms");
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket] message handling error",
        expect.any(Error),
      );

      await requester.close();
      await target.close();
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("rejects replay responses that do not match the pending websocket request", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const server = new WebSocketServer({ port });
    try {
      const accepted = new Promise<WebSocket>((resolve) => {
        server.once("connection", resolve);
      });
      const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
        onEvent: () => {},
      });
      const serverSocket = await accepted;

      const response = connection.requestReplay("node-b", "tenant:jp", new Map());
      const request = await nextRawMessageOfType(serverSocket, "replay-request");
      serverSocket.send(JSON.stringify({
        type: "replay-response",
        requestId: request.requestId,
        requesterNodeId: "node-a",
        partitionId: "tenant:us",
        events: [],
      }));

      await expect(response).rejects.toThrow("replay response mismatch");

      await connection.close();
    } finally {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
    }
  });

  it("publish は connection close 後に失敗する", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
      onEvent: () => {},
    });
    await connection.close();

    await expect(connection.publish({
      sourceNodeId: "node-a",
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("websocket connection is closed");
  });

  it("rejects invalid events before publishing", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
      onEvent: () => {},
    });

    await expect(connection.publish({
      sourceNodeId: "node-a",
      eventId: "not-a-uuid",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("eventId must be a UUIDv7");

    await connection.close();
  });

  it("rejects events whose source does not match the websocket connection", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
      onEvent: () => {},
    });

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
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
      onEvent: () => {},
    });
    await connection.close();

    await expect(connection.subscribe("tenant:jp"))
      .rejects.toThrow("websocket connection is closed");
  });

  it("rejects empty partition ids before sending subscribe or replay requests", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
      onEvent: () => {},
    });

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
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
      onEvent: () => {},
    });
    await connection.close();

    await expect(connection.requestReplay("node-b", "tenant:jp", new Map()))
      .rejects.toThrow("websocket connection is closed");
  });

  it("replay-response is routed only to the requester", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const requester = await rawClient(`ws://127.0.0.1:${port}`, "requester");
    const bystander = await rawClient(`ws://127.0.0.1:${port}`, "bystander");
    const target = await rawClient(`ws://127.0.0.1:${port}`, "target");

    try {
      requester.send(JSON.stringify({
        type: "replay-request",
        requestId: "fixed-request",
        requesterNodeId: "requester",
        targetNodeId: "target",
        partitionId: "tenant:jp",
        cursors: [],
      }));
      await expect(nextRawMessage(target)).resolves.toMatchObject({
        type: "replay-request",
        requestId: "fixed-request",
      });

      target.send(JSON.stringify({
        type: "replay-response",
        requestId: "fixed-request",
        requesterNodeId: "requester",
        partitionId: "tenant:jp",
        events: [],
      }));

      await expect(nextRawMessage(requester)).resolves.toMatchObject({
        type: "replay-response",
        requestId: "fixed-request",
      });
      await expect(nextRawMessage(bystander, 100)).rejects.toThrow("timed out waiting for message");
    } finally {
      requester.close();
      bystander.close();
      target.close();
    }
  });

  it("closes raw clients that spoof replay responses for another target", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const url = `ws://127.0.0.1:${port}`;
    const requester = await rawClient(url, "requester");
    const target = await rawClient(url, "target");
    const bystander = await rawClient(url, "bystander");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      requester.send(JSON.stringify({
        type: "replay-request",
        requestId: "spoofed-response",
        requesterNodeId: "requester",
        targetNodeId: "target",
        partitionId: "tenant:jp",
        cursors: [],
      }));
      await expect(nextRawMessage(target)).resolves.toMatchObject({
        type: "replay-request",
        requestId: "spoofed-response",
      });

      bystander.send(JSON.stringify({
        type: "replay-response",
        requestId: "spoofed-response",
        requesterNodeId: "requester",
        partitionId: "tenant:jp",
        events: [],
      }));

      await expect(waitForSocketCloseCode(bystander)).resolves.toBe(1003);
      await expect(nextRawMessage(requester, 100)).rejects.toThrow("timed out waiting for message");
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      requester.terminate();
      target.terminate();
      bystander.terminate();
    }
  });

  it("closes raw clients that reuse an outstanding replay request id", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);

    const url = `ws://127.0.0.1:${port}`;
    const requester = await rawClient(url, "requester");
    const target = await rawClient(url, "target");
    const bystander = await rawClient(url, "bystander");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      requester.send(JSON.stringify({
        type: "replay-request",
        requestId: "duplicate-request",
        requesterNodeId: "requester",
        targetNodeId: "target",
        partitionId: "tenant:jp",
        cursors: [],
      }));
      await expect(nextRawMessage(target)).resolves.toMatchObject({
        type: "replay-request",
        requestId: "duplicate-request",
      });

      bystander.send(JSON.stringify({
        type: "replay-request",
        requestId: "duplicate-request",
        requesterNodeId: "bystander",
        targetNodeId: "target",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await expect(waitForSocketCloseCode(bystander)).resolves.toBe(1003);
      await expect(nextRawMessage(target, 100)).rejects.toThrow("timed out waiting for message");
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      requester.terminate();
      target.terminate();
      bystander.terminate();
    }
  });

  it("keeps the latest connection when an older duplicate node id socket closes", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const url = `ws://127.0.0.1:${port}`;

    const oldTarget = await rawClient(url, "target");
    const newTarget = await rawClient(url, "target");
    const requester = await rawClient(url, "requester");

    try {
      oldTarget.close();
      await waitForSocketClose(oldTarget);

      requester.send(JSON.stringify({
        type: "replay-request",
        requestId: "after-duplicate-close",
        requesterNodeId: "requester",
        targetNodeId: "target",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      const forwarded = await nextRawMessage(newTarget);
      expect(forwarded).toMatchObject({
        type: "replay-request",
        requestId: "after-duplicate-close",
        requesterNodeId: "requester",
        targetNodeId: "target",
      });
    } finally {
      oldTarget.terminate();
      newTarget.terminate();
      requester.terminate();
    }
  });

  it("closes raw clients that send an empty hello node id", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });

      socket.send(JSON.stringify({ type: "hello", nodeId: "" }));

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("closes raw clients that send an empty subscribe partition", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = await rawClient(`ws://127.0.0.1:${port}`, "node-a");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({ type: "subscribe", partitionId: "" }));

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("closes raw clients that send invalid replay cursors", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = await rawClient(`ws://127.0.0.1:${port}`, "node-a");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "bad-cursor",
        requesterNodeId: "node-a",
        targetNodeId: "node-b",
        partitionId: "tenant:jp",
        cursors: [["node-a", "not-a-uuid"]],
      }));

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("closes raw clients that send an empty replay request id", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = await rawClient(`ws://127.0.0.1:${port}`, "node-a");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "",
        requesterNodeId: "node-a",
        targetNodeId: "node-b",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("closes raw clients that spoof event source node ids", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = await rawClient(`ws://127.0.0.1:${port}`, "node-a");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "event",
        event: {
          eventId: "018bcfe5-6800-7001-8000-000000000001",
          sourceNodeId: "node-b",
          partitionId: "tenant:jp",
          schemaVersion: 1,
          payload: {},
        },
      }));

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("closes raw clients that spoof replay requester node ids", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = await rawClient(`ws://127.0.0.1:${port}`, "node-a");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      socket.send(JSON.stringify({
        type: "replay-request",
        requestId: "spoofed-requester",
        requesterNodeId: "node-b",
        targetNodeId: "node-c",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("closes malformed raw messages without an unhandled rejection", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const hub = new WebSocketHub({ port });
    hubs.push(hub);
    const socket = new WebSocket(`ws://127.0.0.1:${port}`);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });

      socket.send("{");

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1003);
      expect(errorSpy).toHaveBeenCalledWith(
        "[websocket-hub] message decode error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
      socket.terminate();
    }
  });

  it("captures malformed server messages without throwing from the client listener", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const server = new WebSocketServer({ port });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const accepted = new Promise<WebSocket>((resolve) => {
        server.once("connection", resolve);
      });
      const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
        onEvent: () => {},
      });
      const serverSocket = await accepted;

      serverSocket.send("{");

      await waitFor(async () => {
        expect(errorSpy).toHaveBeenCalledWith(
          "[websocket] message handling error",
          expect.any(Error),
        );
      });

      await connection.close();
    } finally {
      errorSpy.mockRestore();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
    }
  });

  it("rejects server replay requests targeted at another node", async () => {
    const port = await getFreePort();
    if (port === undefined) return;

    const server = new WebSocketServer({ port });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const accepted = new Promise<WebSocket>((resolve) => {
        server.once("connection", resolve);
      });
      const onReplayRequest = vi.fn();
      const connection = await new WebSocketTransport(`ws://127.0.0.1:${port}`).connect("node-a", {
        onEvent: () => {},
        onReplayRequest,
      });
      const serverSocket = await accepted;

      serverSocket.send(JSON.stringify({
        type: "replay-request",
        requestId: "wrong-target",
        requesterNodeId: "node-b",
        targetNodeId: "node-c",
        partitionId: "tenant:jp",
        cursors: [],
      }));

      await waitFor(async () => {
        expect(errorSpy).toHaveBeenCalledWith(
          "[websocket] message handling error",
          expect.any(Error),
        );
      });
      expect(onReplayRequest).not.toHaveBeenCalled();

      await connection.close();
    } finally {
      errorSpy.mockRestore();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
      });
    }
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

async function rawClient(url: string, nodeId: string): Promise<WebSocket> {
  const socket = new WebSocket(url);
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  socket.send(JSON.stringify({ type: "hello", nodeId }));
  return socket;
}

async function nextRawMessage(socket: WebSocket, timeoutMs = 1000): Promise<Record<string, unknown>> {
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for message"));
    }, timeoutMs);
    const onMessage = (data: WebSocket.RawData) => {
      cleanup();
      resolve(JSON.parse(data.toString()) as Record<string, unknown>);
    };
    const cleanup = () => {
      clearTimeout(timer);
      socket.off("message", onMessage);
    };
    socket.on("message", onMessage);
  });
}

async function nextRawMessageOfType(
  socket: WebSocket,
  type: string,
  timeoutMs = 1000,
): Promise<Record<string, unknown>> {
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

async function waitForSocketCloseCode(socket: WebSocket): Promise<number> {
  if (socket.readyState === WebSocket.CLOSED) {
    throw new Error("socket was already closed before close code could be observed");
  }
  return await new Promise<number>((resolve) => {
    socket.once("close", (code) => resolve(code));
  });
}

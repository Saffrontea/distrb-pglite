import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { EventEnvelope } from "@distrb-pglite/core";
import { BrowserWebSocketTransport } from "../src/index.js";

const originalWebSocket = globalThis.WebSocket;

describe("BrowserWebSocketTransport", () => {
  beforeEach(() => {
    FakeWebSocket.instances = [];
    vi.useFakeTimers();
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  });

  afterEach(() => {
    vi.useRealTimers();
    globalThis.WebSocket = originalWebSocket;
  });

  it("sends hello, subscribe, and event messages over the browser WebSocket API", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    await connection.subscribe("tenant:jp");
    const event = sampleEvent("browser-a", "01");
    await connection.publish(event);

    expect(socket.sent.map(parse)).toMatchObject([
      { type: "hello", nodeId: "browser-a" },
      { type: "subscribe", partitionId: "tenant:jp" },
      { type: "event", event },
    ]);
  });

  it("rejects empty node ids before opening a browser websocket", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");

    await expect(transport.connect("", { onEvent: vi.fn() })).rejects.toThrow(
      "nodeId must be a non-empty string",
    );
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it("rejects invalid relay urls before opening a browser websocket", () => {
    expect(() => new BrowserWebSocketTransport("http://relay.test")).toThrow(
      "url must use ws or wss protocol",
    );
    expect(
      () =>
        new BrowserWebSocketTransport({
          url: "",
        }),
    ).toThrow("url must be a non-empty websocket URL");
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it("rejects browser websocket connections that close before opening", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;

    const rejected = expect(
      withTimeout(connected, "browser websocket close before open", 100),
    ).rejects.toThrow("websocket connection closed before open");
    socket.closeFromServer();
    await vi.advanceTimersByTimeAsync(100);

    await rejected;
  });

  it("handles replay requests and resolves replay responses", async () => {
    const event = sampleEvent("browser-b", "02");
    const onReplayRequest = vi.fn(async () => ({
      partitionId: "tenant:jp",
      events: [event],
    }));
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", {
      onEvent: vi.fn(),
      onReplayRequest,
    });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    socket.receive({
      type: "replay-request",
      requestId: "request-from-peer",
      requesterNodeId: "browser-b",
      targetNodeId: "browser-a",
      partitionId: "tenant:jp",
      cursors: [["browser-b", sampleEventId("01")]],
    });
    await Promise.resolve();

    expect(onReplayRequest).toHaveBeenCalledWith({
      requesterNodeId: "browser-b",
      partitionId: "tenant:jp",
      cursors: new Map([["browser-b", sampleEventId("01")]]),
    });
    expect(parse(socket.sent.at(-1)!)).toMatchObject({
      type: "replay-response",
      requestId: "request-from-peer",
      requesterNodeId: "browser-b",
      partitionId: "tenant:jp",
      events: [event],
    });

    const response = connection.requestReplay(
      "browser-b",
      "tenant:jp",
      new Map([["browser-a", sampleEventId("01")]]),
    );
    const request = parse(socket.sent.at(-1)!) as { requestId: string };
    socket.receive({
      type: "replay-response",
      requestId: request.requestId,
      requesterNodeId: "browser-a",
      partitionId: "tenant:jp",
      events: [event],
    });

    await expect(response).resolves.toEqual({
      partitionId: "tenant:jp",
      events: [event],
    });
  });

  it("rejects replay responses that do not match the pending browser request", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    const response = connection.requestReplay("browser-b", "tenant:jp", new Map());
    const request = parse(socket.sent.at(-1)!) as { requestId: string };
    socket.receive({
      type: "replay-response",
      requestId: request.requestId,
      requesterNodeId: "browser-a",
      partitionId: "tenant:us",
      events: [],
    });

    await expect(response).rejects.toThrow("replay response mismatch");
  });

  it("captures replay handler failures without leaking unhandled rejections", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const transport = new BrowserWebSocketTransport("ws://relay.test");
      const connected = transport.connect("browser-a", {
        onEvent: vi.fn(),
        onReplayRequest: async () => {
          throw new Error("handler failed");
        },
      });
      const socket = FakeWebSocket.instances[0]!;
      socket.open();
      await connected;

      socket.receive({
        type: "replay-request",
        requestId: "request-from-peer",
        requesterNodeId: "browser-b",
        targetNodeId: "browser-a",
        partitionId: "tenant:jp",
        cursors: [],
      });
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();

      expect(errorSpy).toHaveBeenCalledWith(
        "[browser-websocket] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("rejects replay requests targeted at another node", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const onReplayRequest = vi.fn();
      const transport = new BrowserWebSocketTransport("ws://relay.test");
      const connected = transport.connect("browser-a", {
        onEvent: vi.fn(),
        onReplayRequest,
      });
      const socket = FakeWebSocket.instances[0]!;
      socket.open();
      await connected;

      socket.receive({
        type: "replay-request",
        requestId: "wrong-target",
        requesterNodeId: "browser-b",
        targetNodeId: "browser-c",
        partitionId: "tenant:jp",
        cursors: [],
      });
      await Promise.resolve();

      expect(onReplayRequest).not.toHaveBeenCalled();
      expect(errorSpy).toHaveBeenCalledWith(
        "[browser-websocket] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("rejects replay requests with an empty request id", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const onReplayRequest = vi.fn();
      const transport = new BrowserWebSocketTransport("ws://relay.test");
      const connected = transport.connect("browser-a", {
        onEvent: vi.fn(),
        onReplayRequest,
      });
      const socket = FakeWebSocket.instances[0]!;
      socket.open();
      await connected;

      socket.receive({
        type: "replay-request",
        requestId: "",
        requesterNodeId: "browser-b",
        targetNodeId: "browser-a",
        partitionId: "tenant:jp",
        cursors: [],
      });
      await Promise.resolve();

      expect(onReplayRequest).not.toHaveBeenCalled();
      expect(errorSpy).toHaveBeenCalledWith(
        "[browser-websocket] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("captures malformed server messages without throwing from the browser listener", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const transport = new BrowserWebSocketTransport("ws://relay.test");
      const connected = transport.connect("browser-a", { onEvent: vi.fn() });
      const socket = FakeWebSocket.instances[0]!;
      socket.open();
      await connected;

      expect(() => socket.receiveRaw("{")).not.toThrow();
      await Promise.resolve();

      expect(errorSpy).toHaveBeenCalledWith(
        "[browser-websocket] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("captures unknown server message types without ignoring them", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const transport = new BrowserWebSocketTransport("ws://relay.test");
      const connected = transport.connect("browser-a", { onEvent: vi.fn() });
      const socket = FakeWebSocket.instances[0]!;
      socket.open();
      await connected;

      socket.receive({ type: "mystery", value: 1 });
      await Promise.resolve();

      expect(errorSpy).toHaveBeenCalledWith(
        "[browser-websocket] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("captures server pong messages with invalid sentAt values", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const transport = new BrowserWebSocketTransport("ws://relay.test");
      const connected = transport.connect("browser-a", { onEvent: vi.fn() });
      const socket = FakeWebSocket.instances[0]!;
      socket.open();
      await connected;

      socket.receive({ type: "pong", sentAt: "bad" });
      await Promise.resolve();

      expect(errorSpy).toHaveBeenCalledWith(
        "[browser-websocket] message handling error",
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("resubscribes and reports reconnect after a browser socket reconnect", async () => {
    const onReconnect = vi.fn();
    const transport = new BrowserWebSocketTransport({
      url: "ws://relay.test",
      reconnect: { maxAttempts: 1, baseDelayMs: 10, maxDelayMs: 10 },
    });
    const connected = transport.connect("browser-a", { onEvent: vi.fn(), onReconnect });
    const first = FakeWebSocket.instances[0]!;
    first.open();
    const connection = await connected;
    await connection.subscribe("tenant:jp");

    first.closeFromServer();
    await vi.advanceTimersByTimeAsync(10);
    const second = FakeWebSocket.instances[1]!;
    second.open();
    await Promise.resolve();

    expect(second.sent.map(parse)).toMatchObject([
      { type: "hello", nodeId: "browser-a" },
      { type: "subscribe", partitionId: "tenant:jp" },
    ]);
    expect(onReconnect).toHaveBeenCalledWith(new Set(["tenant:jp"]));
  });

  it("ignores messages and duplicate close events from stale browser sockets after reconnect", async () => {
    const onEvent = vi.fn();
    const transport = new BrowserWebSocketTransport({
      url: "ws://relay.test",
      reconnect: { maxAttempts: 1, baseDelayMs: 10, maxDelayMs: 10 },
    });
    const connected = transport.connect("browser-a", { onEvent });
    const first = FakeWebSocket.instances[0]!;
    first.open();
    const connection = await connected;

    const response = connection.requestReplay("browser-b", "tenant:jp", new Map());
    const rejected = expect(response).rejects.toThrow("websocket connection closed");
    first.closeFromServer();
    await vi.advanceTimersByTimeAsync(10);
    const second = FakeWebSocket.instances[1]!;
    second.open();
    await Promise.resolve();

    first.receive({
      type: "event",
      event: sampleEvent("browser-b", "02"),
    });
    first.closeFromServer();

    await rejected;
    expect(onEvent).not.toHaveBeenCalled();
    await expect(connection.subscribe("tenant:jp")).resolves.toBeUndefined();
  });

  it("rejects replay requests after the browser connection is closed", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    await connection.close();

    await expect(connection.requestReplay("browser-b", "tenant:jp", new Map())).rejects.toThrow(
      "websocket connection is closed",
    );
  });

  it("rejects pending replay requests when the browser socket closes", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    const response = connection.requestReplay("browser-b", "tenant:jp", new Map());
    socket.closeFromServer();

    await expect(response).rejects.toThrow("websocket connection closed");
  });

  it("rejects subscriptions after the browser connection is closed", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    await connection.close();

    await expect(connection.subscribe("tenant:jp")).rejects.toThrow(
      "websocket connection is closed",
    );
  });

  it("rejects invalid events before publishing", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    await expect(
      connection.publish({
        eventId: "not-a-uuid",
        sourceNodeId: "browser-a",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: {},
      }),
    ).rejects.toThrow("eventId must be a UUIDv7");

    expect(socket.sent).toHaveLength(1);
  });

  it("rejects events whose source does not match the browser connection", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    await expect(
      connection.publish({
        eventId: sampleEventId("01"),
        sourceNodeId: "browser-b",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: {},
      }),
    ).rejects.toThrow("event source mismatch");

    expect(socket.sent).toHaveLength(1);
  });

  it("rejects empty partition ids before sending subscribe or replay requests", async () => {
    const transport = new BrowserWebSocketTransport("ws://relay.test");
    const connected = transport.connect("browser-a", { onEvent: vi.fn() });
    const socket = FakeWebSocket.instances[0]!;
    socket.open();
    const connection = await connected;

    await expect(connection.subscribe("")).rejects.toThrow(
      "partitionId must be a non-empty string",
    );
    await expect(connection.requestReplay("browser-b", "", new Map())).rejects.toThrow(
      "partitionId must be a non-empty string",
    );
    await expect(connection.requestReplay("", "tenant:jp", new Map())).rejects.toThrow(
      "nodeId must be a non-empty string",
    );
    await expect(
      connection.requestReplay("browser-b", "tenant:jp", new Map([["browser-a", "not-a-uuid"]])),
    ).rejects.toThrow("cursor eventId must be a UUIDv7");
  });
});

function sampleEvent(sourceNodeId: string, suffix: string): EventEnvelope {
  return {
    eventId: sampleEventId(suffix),
    sourceNodeId,
    partitionId: "tenant:jp",
    schemaVersion: 1,
    payload: { sensorId: "s1", reading: 21 },
  };
}

function sampleEventId(suffix: string): string {
  return `018bcfe5-6800-7001-8000-${suffix.padStart(12, "0")}`;
}

function parse(data: string): unknown {
  return JSON.parse(data);
}

async function withTimeout<T>(promise: Promise<T>, label: string, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out: ${label}`)), ms);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

type Listener = (event: unknown) => void;

class FakeWebSocket {
  static readonly OPEN = 1;
  static instances: FakeWebSocket[] = [];

  readyState = 0;
  readonly sent: string[] = [];
  private readonly listeners = new Map<string, Set<Listener>>();

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: Listener): void {
    const listeners = this.listeners.get(type) ?? new Set<Listener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = 3;
    this.emit("close", {});
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  closeFromServer(): void {
    this.readyState = 3;
    this.emit("close", {});
  }

  receive(message: unknown): void {
    this.emit("message", { data: JSON.stringify(message) });
  }

  receiveRaw(data: string): void {
    this.emit("message", { data });
  }

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}

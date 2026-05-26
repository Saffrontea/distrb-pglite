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
      cursors: [["browser-b", "01"]],
    });
    await Promise.resolve();

    expect(onReplayRequest).toHaveBeenCalledWith({
      requesterNodeId: "browser-b",
      partitionId: "tenant:jp",
      cursors: new Map([["browser-b", "01"]]),
    });
    expect(parse(socket.sent.at(-1)!)).toMatchObject({
      type: "replay-response",
      requestId: "request-from-peer",
      partitionId: "tenant:jp",
      events: [event],
    });

    const response = connection.requestReplay(
      "browser-b",
      "tenant:jp",
      new Map([["browser-a", "01"]]),
    );
    const request = parse(socket.sent.at(-1)!) as { requestId: string };
    socket.receive({
      type: "replay-response",
      requestId: request.requestId,
      partitionId: "tenant:jp",
      events: [event],
    });

    await expect(response).resolves.toEqual({
      partitionId: "tenant:jp",
      events: [event],
    });
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
});

function sampleEvent(sourceNodeId: string, eventId: string): EventEnvelope {
  return {
    eventId,
    sourceNodeId,
    partitionId: "tenant:jp",
    schemaVersion: 1,
    payload: { sensorId: "s1", reading: 21 },
  };
}

function parse(data: string): unknown {
  return JSON.parse(data);
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

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}

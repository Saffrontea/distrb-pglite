import {
  assertValidEventEnvelope,
  assertValidNodeId,
  assertValidPartitionId,
  uuidV7,
  validateEventCursorEntries,
  type EventEnvelope,
  type EventId,
  type NodeId,
  type PartitionId,
  type ReplayResponse,
  type Transport,
  type TransportConnection,
  type TransportHandlers,
} from "@distrb-pglite/core";
import WebSocket, { WebSocketServer } from "ws";
import { loadZstd } from "./zstd.js";

type WireMessage =
  | { type: "hello"; nodeId: NodeId }
  | { type: "subscribe"; partitionId: PartitionId }
  | { type: "ping"; sentAt: number }
  | { type: "pong"; sentAt: number }
  | { type: "event"; event: EventEnvelope }
  | {
      type: "replay-request";
      requestId: string;
      requesterNodeId: NodeId;
      targetNodeId: NodeId;
      partitionId: PartitionId;
      cursors: [NodeId, EventId][];
    }
  | {
      type: "replay-response";
      requestId: string;
      requesterNodeId: NodeId;
      partitionId: PartitionId;
      events: EventEnvelope[];
    };

type ClientRecord = {
  nodeId: NodeId;
  socket: WebSocket;
  subscriptions: Set<PartitionId>;
};

type PendingHubReplay = {
  requesterNodeId: NodeId;
  targetNodeId: NodeId;
  partitionId: PartitionId;
  timer: ReturnType<typeof setTimeout>;
};

const REPLAY_REQUEST_FALLBACK_TIMEOUT_MS = 1_000;
const REPLAY_REQUEST_MIN_TIMEOUT_MS = 250;
const REPLAY_REQUEST_MAX_TIMEOUT_MS = 5_000;
const REPLAY_REQUEST_BASE_TIMEOUT_MS = 250;
const REPLAY_REQUEST_RTT_MULTIPLIER = 4;
const HUB_REPLAY_PENDING_TIMEOUT_MS = REPLAY_REQUEST_MAX_TIMEOUT_MS;
const PING_INTERVAL_MS = 500;
const RTT_EWMA_ALPHA = 0.2;

export class WebSocketHub {
  private readonly server: WebSocketServer;
  private readonly clients = new Map<NodeId, ClientRecord>();
  private readonly pendingReplay = new Map<string, PendingHubReplay>();
  private readonly compress: boolean;

  constructor(options: { port: number; compress?: boolean }) {
    this.compress = options.compress ?? false;
    this.server = new WebSocketServer({ port: options.port });
    this.server.on("connection", (socket) => this.handleConnection(socket));
  }

  async close(): Promise<void> {
    this.clearPendingReplay();
    for (const client of this.clients.values()) {
      client.socket.terminate();
    }
    this.clients.clear();

    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }

  private handleConnection(socket: WebSocket): void {
    let record: ClientRecord | undefined;

    socket.on("message", (data) => {
      void (async () => {
        try {
          const message = await this.decode(data as Buffer);
          if (message.type === "hello") {
            if (record) {
              throw new Error(`hello already received for socket: ${record.nodeId}`);
            }
            assertValidNodeId(message.nodeId);
            record = {
              nodeId: message.nodeId,
              socket,
              subscriptions: new Set(),
            };
            this.clients.set(message.nodeId, record);
            return;
          }

          if (!record) {
            socket.close(1008, "hello required before other messages");
            return;
          }

          this.handleMessage(record, message);
        } catch (error) {
          console.error("[websocket-hub] message decode error", error);
          socket.close(1003, "invalid message");
        }
      })();
    });

    socket.on("close", () => {
      if (record) {
        if (this.clients.get(record.nodeId)?.socket === socket) {
          this.clients.delete(record.nodeId);
        }
        for (const [requestId, pending] of this.pendingReplay) {
          if (pending.requesterNodeId === record.nodeId || pending.targetNodeId === record.nodeId) {
            this.deletePendingReplay(requestId);
          }
        }
      }
    });
  }

  private handleMessage(source: ClientRecord, message: WireMessage): void {
    switch (message.type) {
      case "subscribe":
        assertValidPartitionId(message.partitionId);
        source.subscriptions.add(message.partitionId);
        break;
      case "event":
        assertValidEventEnvelope(message.event);
        assertNodeMatches("event source", source.nodeId, message.event.sourceNodeId);
        this.broadcastEvent(source.nodeId, message.event);
        break;
      case "ping":
        void this.encode(source.socket, { type: "pong", sentAt: message.sentAt });
        break;
      case "replay-request":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertValidPartitionId(message.partitionId);
        assertNodeMatches("replay requester", source.nodeId, message.requesterNodeId);
        validateEventCursorEntries(message.cursors);
        this.forwardReplayRequest(message);
        break;
      case "replay-response":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidPartitionId(message.partitionId);
        for (const event of message.events) {
          assertValidEventEnvelope(event);
        }
        this.forwardReplayResponse(source, message);
        break;
      case "hello":
      case "pong":
        break;
    }
  }

  private broadcastEvent(sourceNodeId: NodeId, event: EventEnvelope): void {
    for (const client of this.clients.values()) {
      if (client.nodeId === sourceNodeId || !client.subscriptions.has(event.partitionId)) {
        continue;
      }

      void this.encode(client.socket, { type: "event", event });
    }
  }

  private forwardReplayRequest(message: Extract<WireMessage, { type: "replay-request" }>): void {
    if (this.pendingReplay.has(message.requestId)) {
      throw new Error(`duplicate replay request id: ${message.requestId}`);
    }
    const target = this.clients.get(message.targetNodeId);
    if (!target) {
      const requester = this.clients.get(message.requesterNodeId);
      if (requester) {
        void this.encode(requester.socket, {
          type: "replay-response",
          requestId: message.requestId,
          requesterNodeId: message.requesterNodeId,
          partitionId: message.partitionId,
          events: [],
        });
      }
      return;
    }

    const timer = setTimeout(() => {
      this.pendingReplay.delete(message.requestId);
    }, HUB_REPLAY_PENDING_TIMEOUT_MS);
    timer.unref?.();
    this.pendingReplay.set(message.requestId, {
      requesterNodeId: message.requesterNodeId,
      targetNodeId: message.targetNodeId,
      partitionId: message.partitionId,
      timer,
    });
    void this.encode(target.socket, message);
  }

  private forwardReplayResponse(
    source: ClientRecord,
    message: Extract<WireMessage, { type: "replay-response" }>,
  ): void {
    const pending = this.pendingReplay.get(message.requestId);
    if (!pending) {
      throw new Error(`unexpected replay response: ${message.requestId}`);
    }
    if (
      source.nodeId !== pending.targetNodeId
      || message.requesterNodeId !== pending.requesterNodeId
      || message.partitionId !== pending.partitionId
    ) {
      throw new Error(`replay response mismatch: ${message.requestId}`);
    }
    this.deletePendingReplay(message.requestId);
    const requester = this.clients.get(message.requesterNodeId);
    if (requester) {
      void this.encode(requester.socket, message);
    }
  }

  private deletePendingReplay(requestId: string): void {
    const pending = this.pendingReplay.get(requestId);
    if (!pending) {
      return;
    }
    this.pendingReplay.delete(requestId);
    clearTimeout(pending.timer);
  }

  private clearPendingReplay(): void {
    for (const requestId of this.pendingReplay.keys()) {
      this.deletePendingReplay(requestId);
    }
  }

  private async encode(socket: WebSocket, message: WireMessage): Promise<void> {
    if (socket.readyState !== WebSocket.OPEN) {
      return;
    }
    if (this.compress) {
      const zstd = await loadZstd();
      socket.send(zstd.compress(Buffer.from(JSON.stringify(message))));
    } else {
      socket.send(JSON.stringify(message));
    }
  }

  private async decode(data: Buffer): Promise<WireMessage> {
    const message = this.compress
      ? await this.decodeCompressed(data)
      : JSON.parse(data.toString());
    assertKnownWireMessage(message);
    return message;
  }

  private async decodeCompressed(data: Buffer): Promise<unknown> {
    if (this.compress) {
      const zstd = await loadZstd();
      return JSON.parse(Buffer.from(zstd.decompress(new Uint8Array(data))).toString());
    }
    return JSON.parse(data.toString());
  }
}
export type WebSocketTransportReconnectOptions = {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
};

export type WebSocketTransportOptions = {
  url: string;
  compress?: boolean;
  reconnect?: WebSocketTransportReconnectOptions;
};

export class WebSocketTransport implements Transport {
  private readonly url: string;
  private readonly compress: boolean;
  private readonly reconnectOptions?: WebSocketTransportReconnectOptions;

  constructor(urlOrOptions: string | WebSocketTransportOptions, compress?: boolean) {
    if (typeof urlOrOptions === "string") {
      this.url = urlOrOptions;
      this.compress = compress ?? false;
    } else {
      this.url = urlOrOptions.url;
      this.compress = urlOrOptions.compress ?? false;
      this.reconnectOptions = urlOrOptions.reconnect;
    }
  }

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    assertValidNodeId(nodeId);
    const socket = new WebSocket(this.url);
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });

    const zstd = this.compress ? await loadZstd() : undefined;
    return new WebSocketTransportConnection(nodeId, socket, handlers, zstd, this.reconnectOptions, this.url);
  }
}

class WebSocketTransportConnection implements TransportConnection {
  private socket: WebSocket;
  private readonly subscriptions = new Set<PartitionId>();
  private readonly pendingReplay = new Map<string, {
    partitionId: PartitionId;
    resolve: (response: ReplayResponse) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }>();
  private pingInterval?: ReturnType<typeof setInterval>;
  private rttEwmaMs: number | undefined;
  private closed = false;

  constructor(
    private readonly nodeId: NodeId,
    socket: WebSocket,
    private readonly handlers: TransportHandlers,
    private readonly zstd?: Awaited<ReturnType<typeof loadZstd>>,
    private readonly reconnectOptions?: WebSocketTransportReconnectOptions,
    private readonly url?: string,
  ) {
    this.socket = socket;
    this.attach(socket);
    this.send({ type: "hello", nodeId });
    this.startPing();
  }

  private attach(socket: WebSocket): void {
    socket.on("message", (data) => {
      void (async () => {
        try {
          await this.handleMessage(this.decode(data as Buffer));
        } catch (error) {
          console.error("[websocket] message handling error", error);
        }
      })();
    });
    socket.on("close", () => {
      this.rejectAllPendingReplay("websocket connection closed");
      if (!this.closed && this.reconnectOptions && this.url) {
        void this.attemptReconnect();
      }
    });
  }

  private async attemptReconnect(): Promise<void> {
    const maxAttempts = this.reconnectOptions?.maxAttempts ?? 8;
    const baseDelayMs = this.reconnectOptions?.baseDelayMs ?? 500;
    const maxDelayMs = this.reconnectOptions?.maxDelayMs ?? 30_000;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      await sleep(Math.min(baseDelayMs * 2 ** attempt, maxDelayMs));
      if (this.closed) return;

      try {
        const socket = new WebSocket(this.url!);
        await new Promise<void>((resolve, reject) => {
          socket.once("open", resolve);
          socket.once("error", reject);
        });

        this.socket = socket;
        this.attach(socket);
        this.sendIfOpen({ type: "hello", nodeId: this.nodeId });
        for (const partitionId of this.subscriptions) {
          this.sendIfOpen({ type: "subscribe", partitionId });
        }

        if (this.handlers.onReconnect) {
          await this.handlers.onReconnect(new Set(this.subscriptions));
        }
        return;
      } catch {
        // next attempt
      }
    }
  }

  async subscribe(partitionId: PartitionId): Promise<void> {
    if (this.closed) {
      throw new Error("websocket connection is closed");
    }
    assertValidPartitionId(partitionId);
    this.subscriptions.add(partitionId);
    this.sendIfOpen({ type: "subscribe", partitionId });
  }

  async publish(event: EventEnvelope): Promise<void> {
    if (this.closed) {
      throw new Error("websocket connection is closed");
    }
    assertValidEventEnvelope(event);
    assertNodeMatches("event source", this.nodeId, event.sourceNodeId);
    this.send({ type: "event", event });
  }

  async requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    if (this.closed) {
      throw new Error("websocket connection is closed");
    }
    assertValidNodeId(targetNodeId);
    assertValidPartitionId(partitionId);
    validateEventCursorEntries([...cursors]);

    const requestId = uuidV7();
    const timeoutMs = this.replayRequestTimeoutMs();
    const response = new Promise<ReplayResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingReplay.delete(requestId)) {
          reject(new Error(`replay request timed out after ${timeoutMs}ms`));
        }
      }, timeoutMs);
      this.pendingReplay.set(requestId, { partitionId, resolve, reject, timer });
    });

    try {
      this.send({
        type: "replay-request",
        requestId,
        requesterNodeId: this.nodeId,
        targetNodeId,
        partitionId,
        cursors: [...cursors],
      });
    } catch (error) {
      const pending = this.pendingReplay.get(requestId);
      if (pending) {
        this.pendingReplay.delete(requestId);
        clearTimeout(pending.timer);
      }
      throw error;
    }

    return response;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.stopPing();
    this.rejectAllPendingReplay("websocket connection closed");
    this.socket.close();
  }

  private send(message: WireMessage): void {
    if (this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("websocket connection is not open");
    }
    this.sendIfOpen(message);
  }

  private sendIfOpen(message: WireMessage): boolean {
    if (this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    if (this.zstd) {
      this.socket.send(this.zstd.compress(Buffer.from(JSON.stringify(message))));
    } else {
      this.socket.send(JSON.stringify(message));
    }
    return true;
  }

  private decode(data: Buffer): WireMessage {
    const message = this.zstd
      ? JSON.parse(Buffer.from(this.zstd.decompress(new Uint8Array(data))).toString())
      : JSON.parse(data.toString());
    assertKnownWireMessage(message);
    return message;
  }

  private async handleMessage(message: WireMessage): Promise<void> {
    switch (message.type) {
      case "event":
        assertValidEventEnvelope(message.event);
        await this.handlers.onEvent(message.event);
        break;
      case "replay-request": {
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertValidPartitionId(message.partitionId);
        if (message.requesterNodeId === this.nodeId) {
          throw new Error(`replay requester must not be the local node: ${this.nodeId}`);
        }
        if (message.targetNodeId !== this.nodeId) {
          throw new Error(
            `replay request target mismatch: local=${this.nodeId} target=${message.targetNodeId}`,
          );
        }
        const cursors = validateEventCursorEntries(message.cursors);
        if (!this.handlers.onReplayRequest) {
          this.send({
            type: "replay-response",
            requestId: message.requestId,
            requesterNodeId: message.requesterNodeId,
            partitionId: message.partitionId,
            events: [],
          });
          return;
        }

        const response = await this.handlers.onReplayRequest({
          requesterNodeId: message.requesterNodeId,
          partitionId: message.partitionId,
          cursors: new Map(cursors),
        });
        this.send({
          type: "replay-response",
          requestId: message.requestId,
          requesterNodeId: message.requesterNodeId,
          ...response,
        });
        break;
      }
      case "replay-response": {
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidPartitionId(message.partitionId);
        for (const event of message.events) {
          assertValidEventEnvelope(event);
        }
        const pending = this.pendingReplay.get(message.requestId);
        if (pending) {
          if (message.requesterNodeId !== this.nodeId || message.partitionId !== pending.partitionId) {
            this.rejectPendingReplay(
              message.requestId,
              `replay response mismatch: ${message.requestId}`,
            );
            break;
          }
          const mismatchedEvent = message.events.find((event) => event.partitionId !== message.partitionId);
          if (mismatchedEvent) {
            this.rejectPendingReplay(
              message.requestId,
              `replay event partition mismatch: response=${message.partitionId} event=${mismatchedEvent.partitionId}`,
            );
            break;
          }
          this.pendingReplay.delete(message.requestId);
          clearTimeout(pending.timer);
          pending.resolve({ partitionId: message.partitionId, events: message.events });
        }
        break;
      }
      case "ping":
        this.sendIfOpen({ type: "pong", sentAt: message.sentAt });
        break;
      case "pong":
        this.recordRtt(Date.now() - message.sentAt);
        break;
      case "hello":
      case "subscribe":
        break;
    }
  }

  private rejectAllPendingReplay(reason: string): void {
    for (const [requestId, pending] of this.pendingReplay) {
      this.pendingReplay.delete(requestId);
      clearTimeout(pending.timer);
      pending.reject(new Error(reason));
    }
  }

  private rejectPendingReplay(requestId: string, reason: string): void {
    const pending = this.pendingReplay.get(requestId);
    if (!pending) {
      return;
    }
    this.pendingReplay.delete(requestId);
    clearTimeout(pending.timer);
    pending.reject(new Error(reason));
  }

  private startPing(): void {
    if (this.pingInterval) {
      return;
    }
    this.pingInterval = setInterval(() => {
      this.sendIfOpen({ type: "ping", sentAt: Date.now() });
    }, PING_INTERVAL_MS);
    this.pingInterval.unref?.();
  }

  private stopPing(): void {
    if (!this.pingInterval) {
      return;
    }
    clearInterval(this.pingInterval);
    this.pingInterval = undefined;
  }

  private recordRtt(rttMs: number): void {
    if (!Number.isFinite(rttMs) || rttMs < 0) {
      return;
    }
    this.rttEwmaMs = this.rttEwmaMs === undefined
      ? rttMs
      : this.rttEwmaMs * (1 - RTT_EWMA_ALPHA) + rttMs * RTT_EWMA_ALPHA;
  }

  private replayRequestTimeoutMs(): number {
    if (this.rttEwmaMs === undefined) {
      return REPLAY_REQUEST_FALLBACK_TIMEOUT_MS;
    }
    return clampTimeout(
      Math.ceil(REPLAY_REQUEST_BASE_TIMEOUT_MS + this.rttEwmaMs * REPLAY_REQUEST_RTT_MULTIPLIER),
    );
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampTimeout(ms: number): number {
  return Math.max(REPLAY_REQUEST_MIN_TIMEOUT_MS, Math.min(REPLAY_REQUEST_MAX_TIMEOUT_MS, ms));
}

function assertValidRequestId(requestId: unknown): asserts requestId is string {
  if (typeof requestId !== "string" || requestId.length === 0) {
    throw new Error("requestId must be a non-empty string");
  }
}

function assertKnownWireMessage(message: unknown): asserts message is WireMessage {
  if (typeof message !== "object" || message === null || !("type" in message)) {
    throw new Error("wire message must be an object with a type");
  }
  switch ((message as { type: unknown }).type) {
    case "hello":
    case "subscribe":
    case "ping":
    case "pong":
    case "event":
    case "replay-request":
    case "replay-response":
      return;
    default:
      throw new Error(`unknown wire message type: ${String((message as { type: unknown }).type)}`);
  }
}

function assertNodeMatches(name: string, expected: NodeId, actual: NodeId): void {
  if (actual !== expected) {
    throw new Error(`${name} mismatch: expected=${expected} actual=${actual}`);
  }
}

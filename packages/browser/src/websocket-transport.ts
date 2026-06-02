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

export type BrowserWebSocketTransportReconnectOptions = {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
};

export type BrowserWebSocketTransportOptions = {
  url: string;
  reconnect?: BrowserWebSocketTransportReconnectOptions;
};

const REPLAY_REQUEST_FALLBACK_TIMEOUT_MS = 1_000;
const REPLAY_REQUEST_MIN_TIMEOUT_MS = 250;
const REPLAY_REQUEST_MAX_TIMEOUT_MS = 5_000;
const REPLAY_REQUEST_BASE_TIMEOUT_MS = 250;
const REPLAY_REQUEST_RTT_MULTIPLIER = 4;
const PING_INTERVAL_MS = 500;
const RTT_EWMA_ALPHA = 0.2;

export class BrowserWebSocketTransport implements Transport {
  private readonly url: string;
  private readonly reconnectOptions?: BrowserWebSocketTransportReconnectOptions;

  constructor(urlOrOptions: string | BrowserWebSocketTransportOptions) {
    if (typeof urlOrOptions === "string") {
      this.url = urlOrOptions;
    } else {
      this.url = urlOrOptions.url;
      this.reconnectOptions = urlOrOptions.reconnect;
    }
  }

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    assertValidNodeId(nodeId);
    const socket = new WebSocket(this.url);
    await waitForOpen(socket);
    return new BrowserWebSocketTransportConnection(
      nodeId,
      socket,
      handlers,
      this.reconnectOptions,
      this.url,
    );
  }
}

class BrowserWebSocketTransportConnection implements TransportConnection {
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
    private readonly reconnectOptions?: BrowserWebSocketTransportReconnectOptions,
    private readonly url?: string,
  ) {
    this.socket = socket;
    this.attach(socket);
    this.send({ type: "hello", nodeId });
    this.startPing();
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

  private attach(socket: WebSocket): void {
    socket.addEventListener("message", (event) => {
      void (async () => {
        try {
          await this.handleMessage(decodeMessage(event.data));
        } catch (error) {
          console.error("[browser-websocket] message handling error", error);
        }
      })();
    });
    socket.addEventListener("close", () => {
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
        await waitForOpen(socket);

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
        // try the next reconnect attempt
      }
    }
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
    this.socket.send(JSON.stringify(message));
    return true;
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

function waitForOpen(socket: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = () => {
      cleanup();
      reject(new Error("websocket connection failed"));
    };
    const cleanup = () => {
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("error", onError);
    };
    socket.addEventListener("open", onOpen);
    socket.addEventListener("error", onError);
  });
}

function decodeMessage(data: MessageEvent["data"]): WireMessage {
  if (typeof data === "string") {
    const message = JSON.parse(data);
    assertKnownWireMessage(message);
    return message;
  }
  throw new Error("binary browser websocket messages are not supported");
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

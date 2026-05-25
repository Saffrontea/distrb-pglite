import {
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
      partitionId: PartitionId;
      events: EventEnvelope[];
    };

type ClientRecord = {
  nodeId: NodeId;
  socket: WebSocket;
  subscriptions: Set<PartitionId>;
};

const REPLAY_REQUEST_TIMEOUT_MS = 1_000;

export class WebSocketHub {
  private readonly server: WebSocketServer;
  private readonly clients = new Map<NodeId, ClientRecord>();
  private readonly compress: boolean;

  constructor(options: { port: number; compress?: boolean }) {
    this.compress = options.compress ?? false;
    this.server = new WebSocketServer({ port: options.port });
    this.server.on("connection", (socket) => this.handleConnection(socket));
  }

  async close(): Promise<void> {
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
        const message = await this.decode(data as Buffer);
        if (message.type === "hello") {
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
      })();
    });

    socket.on("close", () => {
      if (record) {
        this.clients.delete(record.nodeId);
      }
    });
  }

  private handleMessage(source: ClientRecord, message: WireMessage): void {
    switch (message.type) {
      case "subscribe":
        source.subscriptions.add(message.partitionId);
        break;
      case "event":
        this.broadcastEvent(source.nodeId, message.event);
        break;
      case "replay-request":
        this.forwardReplayRequest(message);
        break;
      case "replay-response":
        this.forwardReplayResponse(message);
        break;
      case "hello":
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
    const target = this.clients.get(message.targetNodeId);
    if (!target) {
      const requester = this.clients.get(message.requesterNodeId);
      if (requester) {
        void this.encode(requester.socket, {
          type: "replay-response",
          requestId: message.requestId,
          partitionId: message.partitionId,
          events: [],
        });
      }
      return;
    }

    void this.encode(target.socket, message);
  }

  private forwardReplayResponse(message: Extract<WireMessage, { type: "replay-response" }>): void {
    for (const client of this.clients.values()) {
      void this.encode(client.socket, message);
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
    if (this.compress) {
      const zstd = await loadZstd();
      return JSON.parse(Buffer.from(zstd.decompress(new Uint8Array(data))).toString()) as WireMessage;
    }
    return JSON.parse(data.toString()) as WireMessage;
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
    resolve: (response: ReplayResponse) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }>();
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
  }

  private attach(socket: WebSocket): void {
    socket.on("message", (data) => {
      void this.handleMessage(this.decode(data as Buffer));
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
    this.subscriptions.add(partitionId);
    this.sendIfOpen({ type: "subscribe", partitionId });
  }

  async publish(event: EventEnvelope): Promise<void> {
    if (this.closed) {
      throw new Error("websocket connection is closed");
    }
    this.send({ type: "event", event });
  }

  async requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    const requestId = crypto.randomUUID();
    const response = new Promise<ReplayResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingReplay.delete(requestId)) {
          reject(new Error(`replay request timed out after ${REPLAY_REQUEST_TIMEOUT_MS}ms`));
        }
      }, REPLAY_REQUEST_TIMEOUT_MS);
      this.pendingReplay.set(requestId, { resolve, reject, timer });
    });

    this.send({
      type: "replay-request",
      requestId,
      requesterNodeId: this.nodeId,
      targetNodeId,
      partitionId,
      cursors: [...cursors],
    });

    return response;
  }

  async close(): Promise<void> {
    this.closed = true;
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
    if (this.zstd) {
      return JSON.parse(Buffer.from(this.zstd.decompress(new Uint8Array(data))).toString()) as WireMessage;
    }
    return JSON.parse(data.toString()) as WireMessage;
  }

  private async handleMessage(message: WireMessage): Promise<void> {
    switch (message.type) {
      case "event":
        await this.handlers.onEvent(message.event);
        break;
      case "replay-request": {
        if (!this.handlers.onReplayRequest) {
          this.send({
            type: "replay-response",
            requestId: message.requestId,
            partitionId: message.partitionId,
            events: [],
          });
          return;
        }

        const response = await this.handlers.onReplayRequest({
          requesterNodeId: message.requesterNodeId,
          partitionId: message.partitionId,
          cursors: new Map(message.cursors),
        });
        this.send({ type: "replay-response", requestId: message.requestId, ...response });
        break;
      }
      case "replay-response": {
        const pending = this.pendingReplay.get(message.requestId);
        if (pending) {
          this.pendingReplay.delete(message.requestId);
          clearTimeout(pending.timer);
          pending.resolve({ partitionId: message.partitionId, events: message.events });
        }
        break;
      }
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
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

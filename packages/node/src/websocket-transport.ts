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

export class WebSocketHub {
  private readonly server: WebSocketServer;
  private readonly clients = new Map<NodeId, ClientRecord>();

  constructor(options: { port: number }) {
    this.server = new WebSocketServer({ port: options.port });
    this.server.on("connection", (socket) => this.handleConnection(socket));
  }

  async close(): Promise<void> {
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
      const message = JSON.parse(data.toString()) as WireMessage;
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

      send(client.socket, { type: "event", event });
    }
  }

  private forwardReplayRequest(message: Extract<WireMessage, { type: "replay-request" }>): void {
    const target = this.clients.get(message.targetNodeId);
    if (!target) {
      const requester = this.clients.get(message.requesterNodeId);
      if (requester) {
        send(requester.socket, {
          type: "replay-response",
          requestId: message.requestId,
          partitionId: message.partitionId,
          events: [],
        });
      }
      return;
    }

    send(target.socket, message);
  }

  private forwardReplayResponse(message: Extract<WireMessage, { type: "replay-response" }>): void {
    for (const client of this.clients.values()) {
      send(client.socket, message);
    }
  }
}

export class WebSocketTransport implements Transport {
  constructor(private readonly url: string) {}

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    const socket = new WebSocket(this.url);
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });

    return new WebSocketTransportConnection(nodeId, socket, handlers);
  }
}

class WebSocketTransportConnection implements TransportConnection {
  private readonly pendingReplay = new Map<
    string,
    (response: ReplayResponse) => void
  >();

  constructor(
    private readonly nodeId: NodeId,
    private readonly socket: WebSocket,
    private readonly handlers: TransportHandlers,
  ) {
    this.socket.on("message", (data) => {
      void this.handleMessage(JSON.parse(data.toString()) as WireMessage);
    });
    send(this.socket, { type: "hello", nodeId });
  }

  async subscribe(partitionId: PartitionId): Promise<void> {
    send(this.socket, { type: "subscribe", partitionId });
  }

  async publish(event: EventEnvelope): Promise<void> {
    send(this.socket, { type: "event", event });
  }

  async requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    const requestId = crypto.randomUUID();
    const response = new Promise<ReplayResponse>((resolve) => {
      this.pendingReplay.set(requestId, resolve);
    });

    send(this.socket, {
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
    this.socket.close();
  }

  private async handleMessage(message: WireMessage): Promise<void> {
    switch (message.type) {
      case "event":
        await this.handlers.onEvent(message.event);
        break;
      case "replay-request": {
        if (!this.handlers.onReplayRequest) {
          send(this.socket, {
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
        send(this.socket, { type: "replay-response", requestId: message.requestId, ...response });
        break;
      }
      case "replay-response": {
        const resolve = this.pendingReplay.get(message.requestId);
        if (resolve) {
          this.pendingReplay.delete(message.requestId);
          resolve({ partitionId: message.partitionId, events: message.events });
        }
        break;
      }
      case "hello":
      case "subscribe":
        break;
    }
  }
}

function send(socket: WebSocket, message: WireMessage): void {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

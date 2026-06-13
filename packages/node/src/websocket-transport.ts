import { createHash } from "node:crypto";
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
  type ReplaySnapshotRequired,
  type SnapshotSource,
  type StorageCatalogManifest,
  type StorageSnapshotCatalogEntry,
  type Transport,
  type TransportConnection,
  type TransportHandlers,
} from "@distrb-pglite/core";
import WebSocket, { WebSocketServer } from "ws";
import {
  diffMissingBlocks,
  NodeCasBlockStore,
  validateCasManifest,
  type CasBlockStore,
  type CasManifest,
} from "./cas.js";
import { loadZstd } from "./zstd.js";

// ADR 0008 introduced protocolVersion (2). Pre-versioned peers (absent
// field = 1) are assumed to predate pruning; the hub refuses to forward a
// snapshot-required response to them so they fail by timeout, never by a
// silently empty replay.
export const WIRE_PROTOCOL_VERSION = 2;

type WireMessage =
  | { type: "hello"; nodeId: NodeId; protocolVersion?: number }
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
      snapshotRequired?: ReplaySnapshotRequired;
    }
  | {
      type: "snapshot-list-request";
      requestId: string;
      requesterNodeId: NodeId;
      targetNodeId: NodeId;
      partitionId: PartitionId;
    }
  | {
      type: "snapshot-list-response";
      requestId: string;
      requesterNodeId: NodeId;
      partitionId: PartitionId;
      entries: StorageSnapshotCatalogEntry<CasManifest>[];
    }
  | {
      type: "snapshot-catalog-request";
      requestId: string;
      requesterNodeId: NodeId;
      targetNodeId: NodeId;
      groupId: string;
    }
  | {
      type: "snapshot-catalog-response";
      requestId: string;
      requesterNodeId: NodeId;
      groupId: string;
      manifest?: StorageCatalogManifest;
    }
  | {
      type: "snapshot-block-request";
      requestId: string;
      requesterNodeId: NodeId;
      targetNodeId: NodeId;
      hash: string;
    }
  | {
      type: "snapshot-block-response";
      requestId: string;
      requesterNodeId: NodeId;
      hash: string;
      contentBase64: string;
    };

type ClientRecord = {
  nodeId: NodeId;
  socket: WebSocket;
  subscriptions: Set<PartitionId>;
  protocolVersion: number;
};

type PendingHubReplay = {
  requesterNodeId: NodeId;
  requesterSocket: WebSocket;
  targetNodeId: NodeId;
  targetSocket: WebSocket;
  partitionId: PartitionId;
  timer: ReturnType<typeof setTimeout>;
};

type PendingHubSnapshotRequest = {
  requesterNodeId: NodeId;
  requesterSocket: WebSocket;
  targetNodeId: NodeId;
  targetSocket: WebSocket;
  kind: "list" | "catalog" | "block";
  partitionId?: PartitionId;
  groupId?: string;
  hash?: string;
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
  private readonly pendingSnapshot = new Map<string, PendingHubSnapshotRequest>();
  private readonly compress: boolean;
  private closing = false;

  constructor(options: { port: number; compress?: boolean }) {
    this.compress = options.compress ?? false;
    this.server = new WebSocketServer({ port: options.port });
    this.server.on("connection", (socket) => this.handleConnection(socket));
  }

  async close(): Promise<void> {
    this.closing = true;
    this.clearPendingReplay();
    this.clearPendingSnapshot();
    for (const client of this.clients.values()) {
      client.socket.terminate();
    }
    for (const socket of this.server.clients) {
      socket.terminate();
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
    if (this.closing) {
      socket.terminate();
      return;
    }

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
              protocolVersion: validateProtocolVersion(message.protocolVersion),
            };
            const previous = this.clients.get(message.nodeId);
            if (previous && previous.socket !== socket) {
              this.deletePendingReplayForSocket(previous.socket);
              previous.socket.close(1008, "duplicate node id connection replaced");
            }
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
        this.deletePendingReplayForSocket(socket);
        this.deletePendingSnapshotForSocket(socket);
        if (this.clients.get(record.nodeId)?.socket !== socket) {
          return;
        }
        this.clients.delete(record.nodeId);
      }
    });
  }

  private handleMessage(source: ClientRecord, message: WireMessage): void {
    if (this.clients.get(source.nodeId)?.socket !== source.socket) {
      source.socket.close(1008, "stale connection replaced");
      return;
    }

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
        assertValidSentAt(message.sentAt);
        void this.encode(source.socket, { type: "pong", sentAt: message.sentAt });
        break;
      case "replay-request":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertValidPartitionId(message.partitionId);
        assertNodeMatches("replay requester", source.nodeId, message.requesterNodeId);
        validateEventCursorEntries(message.cursors);
        this.forwardReplayRequest(source, message);
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
      case "snapshot-list-request":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertValidPartitionId(message.partitionId);
        assertNodeMatches("snapshot list requester", source.nodeId, message.requesterNodeId);
        this.forwardSnapshotRequest(source, message, "list");
        break;
      case "snapshot-list-response":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidPartitionId(message.partitionId);
        this.forwardSnapshotResponse(source, message, "list");
        break;
      case "snapshot-catalog-request":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertNonEmptyString(message.groupId, "snapshot catalog groupId");
        assertNodeMatches("snapshot catalog requester", source.nodeId, message.requesterNodeId);
        this.forwardSnapshotRequest(source, message, "catalog");
        break;
      case "snapshot-catalog-response":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertNonEmptyString(message.groupId, "snapshot catalog groupId");
        this.forwardSnapshotResponse(source, message, "catalog");
        break;
      case "snapshot-block-request":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertCasBlockHash(message.hash, "snapshot block hash");
        assertNodeMatches("snapshot block requester", source.nodeId, message.requesterNodeId);
        this.forwardSnapshotRequest(source, message, "block");
        break;
      case "snapshot-block-response":
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertCasBlockHash(message.hash, "snapshot block hash");
        assertNonEmptyString(message.contentBase64, "snapshot block content");
        this.forwardSnapshotResponse(source, message, "block");
        break;
      case "hello":
      case "pong":
        if (message.type === "pong") {
          assertValidSentAt(message.sentAt);
        }
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

  private forwardReplayRequest(
    source: ClientRecord,
    message: Extract<WireMessage, { type: "replay-request" }>,
  ): void {
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
      requesterSocket: source.socket,
      targetNodeId: message.targetNodeId,
      targetSocket: target.socket,
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
      source.nodeId !== pending.targetNodeId ||
      source.socket !== pending.targetSocket ||
      message.requesterNodeId !== pending.requesterNodeId ||
      message.partitionId !== pending.partitionId
    ) {
      throw new Error(`replay response mismatch: ${message.requestId}`);
    }
    this.deletePendingReplay(message.requestId);
    // A pre-ADR-0008 requester would misread snapshot-required as "caught
    // up" (empty events). Dropping the response makes it time out instead.
    if (message.snapshotRequired) {
      const requester = this.clients.get(message.requesterNodeId);
      if ((requester?.protocolVersion ?? 1) < 2) {
        return;
      }
    }
    void this.encode(pending.requesterSocket, message);
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

  private deletePendingReplayForSocket(socket: WebSocket): void {
    for (const [requestId, pending] of this.pendingReplay) {
      if (pending.requesterSocket === socket || pending.targetSocket === socket) {
        this.deletePendingReplay(requestId);
      }
    }
  }

  private forwardSnapshotRequest(
    source: ClientRecord,
    message: Extract<
      WireMessage,
      | { type: "snapshot-list-request" }
      | { type: "snapshot-catalog-request" }
      | { type: "snapshot-block-request" }
    >,
    kind: PendingHubSnapshotRequest["kind"],
  ): void {
    if (this.pendingSnapshot.has(message.requestId)) {
      throw new Error(`duplicate snapshot request id: ${message.requestId}`);
    }
    const target = this.clients.get(message.targetNodeId);
    if (!target) {
      return;
    }
    if (target.protocolVersion < 2) {
      return;
    }

    const timer = setTimeout(() => {
      this.pendingSnapshot.delete(message.requestId);
    }, HUB_REPLAY_PENDING_TIMEOUT_MS);
    timer.unref?.();
    this.pendingSnapshot.set(message.requestId, {
      requesterNodeId: message.requesterNodeId,
      requesterSocket: source.socket,
      targetNodeId: message.targetNodeId,
      targetSocket: target.socket,
      kind,
      partitionId: "partitionId" in message ? message.partitionId : undefined,
      groupId: "groupId" in message ? message.groupId : undefined,
      hash: "hash" in message ? message.hash : undefined,
      timer,
    });
    void this.encode(target.socket, message);
  }

  private forwardSnapshotResponse(
    source: ClientRecord,
    message: Extract<
      WireMessage,
      | { type: "snapshot-list-response" }
      | { type: "snapshot-catalog-response" }
      | { type: "snapshot-block-response" }
    >,
    kind: PendingHubSnapshotRequest["kind"],
  ): void {
    const pending = this.pendingSnapshot.get(message.requestId);
    if (!pending) {
      throw new Error(`unexpected snapshot response: ${message.requestId}`);
    }
    if (
      source.nodeId !== pending.targetNodeId ||
      source.socket !== pending.targetSocket ||
      message.requesterNodeId !== pending.requesterNodeId ||
      pending.kind !== kind ||
      ("partitionId" in message && message.partitionId !== pending.partitionId) ||
      ("groupId" in message && message.groupId !== pending.groupId) ||
      ("hash" in message && message.hash !== pending.hash)
    ) {
      throw new Error(`snapshot response mismatch: ${message.requestId}`);
    }
    this.deletePendingSnapshot(message.requestId);
    void this.encode(pending.requesterSocket, message);
  }

  private deletePendingSnapshot(requestId: string): void {
    const pending = this.pendingSnapshot.get(requestId);
    if (!pending) {
      return;
    }
    this.pendingSnapshot.delete(requestId);
    clearTimeout(pending.timer);
  }

  private clearPendingSnapshot(): void {
    for (const requestId of this.pendingSnapshot.keys()) {
      this.deletePendingSnapshot(requestId);
    }
  }

  private deletePendingSnapshotForSocket(socket: WebSocket): void {
    for (const [requestId, pending] of this.pendingSnapshot) {
      if (pending.requesterSocket === socket || pending.targetSocket === socket) {
        this.deletePendingSnapshot(requestId);
      }
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
    const message = this.compress ? await this.decodeCompressed(data) : JSON.parse(data.toString());
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
  snapshot?: WebSocketSnapshotOptions;
};

export type WebSocketSnapshotOptions = {
  casDir?: string;
  casStore?: CasBlockStore;
  listSnapshotEntries?: (
    peerNodeId: NodeId,
    partitionId: PartitionId,
  ) => Promise<StorageSnapshotCatalogEntry<CasManifest>[]>;
  loadCatalogManifest?: (
    peerNodeId: NodeId,
    groupId: string,
  ) => Promise<StorageCatalogManifest | undefined>;
};

export class WebSocketTransport implements Transport, SnapshotSource<CasManifest> {
  private readonly url: string;
  private readonly compress: boolean;
  private readonly reconnectOptions?: WebSocketTransportReconnectOptions;
  private readonly snapshotOptions?: WebSocketSnapshotOptions;
  private connection?: WebSocketTransportConnection;

  constructor(urlOrOptions: string | WebSocketTransportOptions, compress?: boolean) {
    if (typeof urlOrOptions === "string") {
      this.url = urlOrOptions;
      this.compress = compress ?? false;
    } else {
      this.url = urlOrOptions.url;
      this.compress = urlOrOptions.compress ?? false;
      this.reconnectOptions = urlOrOptions.reconnect;
      this.snapshotOptions = urlOrOptions.snapshot;
    }
    assertValidWebSocketUrl(this.url, "url");
    if (this.snapshotOptions?.casDir && this.snapshotOptions.casStore) {
      throw new Error("snapshot options must pass casDir or casStore, not both");
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
    this.connection = new WebSocketTransportConnection(
      nodeId,
      socket,
      handlers,
      zstd,
      this.reconnectOptions,
      this.url,
      this.snapshotOptions,
    );
    return this.connection;
  }

  listSnapshotEntries(
    peerNodeId: NodeId,
    partitionId: PartitionId,
  ): Promise<StorageSnapshotCatalogEntry<CasManifest>[]> {
    return this.requireSnapshotConnection().requestSnapshotEntries(peerNodeId, partitionId);
  }

  fetchBlocks(
    peerNodeId: NodeId,
    entry: StorageSnapshotCatalogEntry<CasManifest>,
  ): Promise<number> {
    return this.requireSnapshotConnection().fetchSnapshotBlocks(peerNodeId, entry);
  }

  loadCatalogManifest(
    peerNodeId: NodeId,
    groupId: string,
  ): Promise<StorageCatalogManifest | undefined> {
    return this.requireSnapshotConnection().requestSnapshotCatalog(peerNodeId, groupId);
  }

  private requireSnapshotConnection(): WebSocketTransportConnection {
    if (!this.snapshotOptions) {
      throw new Error("websocket snapshot source is not enabled");
    }
    if (!this.connection) {
      throw new Error("websocket transport is not connected");
    }
    return this.connection;
  }
}

class WebSocketTransportConnection implements TransportConnection {
  private socket: WebSocket;
  private readonly subscriptions = new Set<PartitionId>();
  private readonly pendingReplay = new Map<
    string,
    {
      partitionId: PartitionId;
      resolve: (response: ReplayResponse) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private readonly pendingSnapshot = new Map<
    string,
    {
      kind: "list" | "catalog" | "block";
      partitionId?: PartitionId;
      groupId?: string;
      hash?: string;
      resolve: (response: WireMessage) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private pingInterval?: ReturnType<typeof setInterval>;
  private rttEwmaMs: number | undefined;
  private closed = false;
  private readonly snapshotCasStore?: CasBlockStore;

  constructor(
    private readonly nodeId: NodeId,
    socket: WebSocket,
    private readonly handlers: TransportHandlers,
    private readonly zstd?: Awaited<ReturnType<typeof loadZstd>>,
    private readonly reconnectOptions?: WebSocketTransportReconnectOptions,
    private readonly url?: string,
    private readonly snapshotOptions?: WebSocketSnapshotOptions,
  ) {
    this.snapshotCasStore =
      snapshotOptions?.casStore ??
      (snapshotOptions?.casDir ? new NodeCasBlockStore(snapshotOptions.casDir) : undefined);
    this.socket = socket;
    this.attach(socket);
    this.send({ type: "hello", nodeId, protocolVersion: WIRE_PROTOCOL_VERSION });
    this.startPing();
  }

  private attach(socket: WebSocket): void {
    socket.on("message", (data) => {
      void (async () => {
        try {
          if (this.closed || socket !== this.socket) {
            return;
          }
          await this.handleMessage(this.decode(data as Buffer));
        } catch (error) {
          console.error("[websocket] message handling error", error);
        }
      })();
    });
    socket.on("close", () => {
      if (socket !== this.socket) {
        return;
      }
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
        this.sendIfOpen({
          type: "hello",
          nodeId: this.nodeId,
          protocolVersion: WIRE_PROTOCOL_VERSION,
        });
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
    this.rejectAllPendingSnapshot("websocket connection closed");
    this.socket.close();
  }

  async requestSnapshotEntries(
    targetNodeId: NodeId,
    partitionId: PartitionId,
  ): Promise<StorageSnapshotCatalogEntry<CasManifest>[]> {
    assertValidNodeId(targetNodeId);
    assertValidPartitionId(partitionId);
    const response = await this.requestSnapshot(targetNodeId, "list", { partitionId });
    if (response.type !== "snapshot-list-response") {
      throw new Error(`snapshot list response type mismatch: ${response.type}`);
    }
    return response.entries.map(validateSnapshotEntry);
  }

  async requestSnapshotCatalog(
    targetNodeId: NodeId,
    groupId: string,
  ): Promise<StorageCatalogManifest | undefined> {
    assertValidNodeId(targetNodeId);
    assertNonEmptyString(groupId, "snapshot catalog groupId");
    const response = await this.requestSnapshot(targetNodeId, "catalog", { groupId });
    if (response.type !== "snapshot-catalog-response") {
      throw new Error(`snapshot catalog response type mismatch: ${response.type}`);
    }
    return response.manifest;
  }

  async fetchSnapshotBlocks(
    targetNodeId: NodeId,
    entry: StorageSnapshotCatalogEntry<CasManifest>,
  ): Promise<number> {
    assertValidNodeId(targetNodeId);
    const casStore = this.requireSnapshotCasStore();
    const manifest = validateCasManifest(entry.manifest, entry.snapshotId);
    const missing = diffMissingBlocks(manifest, await casStore.listBlockHashes());
    for (const block of missing) {
      const response = await this.requestSnapshot(targetNodeId, "block", { hash: block.hash });
      if (response.type !== "snapshot-block-response") {
        throw new Error(`snapshot block response type mismatch: ${response.type}`);
      }
      const content = Buffer.from(response.contentBase64, "base64");
      if (content.length !== block.size || sha256(content) !== block.hash) {
        throw new Error(`snapshot block integrity check failed: ${block.hash}`);
      }
      await casStore.writeBlock(block.hash, content);
    }
    return missing.length;
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
          if (
            message.requesterNodeId !== this.nodeId ||
            message.partitionId !== pending.partitionId
          ) {
            this.rejectPendingReplay(
              message.requestId,
              `replay response mismatch: ${message.requestId}`,
            );
            break;
          }
          const mismatchedEvent = message.events.find(
            (event) => event.partitionId !== message.partitionId,
          );
          if (mismatchedEvent) {
            this.rejectPendingReplay(
              message.requestId,
              `replay event partition mismatch: response=${message.partitionId} event=${mismatchedEvent.partitionId}`,
            );
            break;
          }
          if (message.snapshotRequired) {
            try {
              validateEventCursorEntries(message.snapshotRequired.prunedThrough);
              if (message.events.length > 0) {
                throw new Error("snapshot-required replay response must not carry events");
              }
            } catch (error) {
              this.rejectPendingReplay(message.requestId, String(error));
              break;
            }
          }
          this.pendingReplay.delete(message.requestId);
          clearTimeout(pending.timer);
          pending.resolve({
            partitionId: message.partitionId,
            events: message.events,
            snapshotRequired: message.snapshotRequired,
          });
        }
        break;
      }
      case "snapshot-list-request": {
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertValidPartitionId(message.partitionId);
        if (message.targetNodeId !== this.nodeId) {
          throw new Error(
            `snapshot list target mismatch: local=${this.nodeId} target=${message.targetNodeId}`,
          );
        }
        const entries =
          (await this.snapshotOptions?.listSnapshotEntries?.(
            message.requesterNodeId,
            message.partitionId,
          )) ?? [];
        this.send({
          type: "snapshot-list-response",
          requestId: message.requestId,
          requesterNodeId: message.requesterNodeId,
          partitionId: message.partitionId,
          entries,
        });
        break;
      }
      case "snapshot-catalog-request": {
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertNonEmptyString(message.groupId, "snapshot catalog groupId");
        if (message.targetNodeId !== this.nodeId) {
          throw new Error(
            `snapshot catalog target mismatch: local=${this.nodeId} target=${message.targetNodeId}`,
          );
        }
        const manifest = await this.snapshotOptions?.loadCatalogManifest?.(
          message.requesterNodeId,
          message.groupId,
        );
        this.send({
          type: "snapshot-catalog-response",
          requestId: message.requestId,
          requesterNodeId: message.requesterNodeId,
          groupId: message.groupId,
          ...(manifest ? { manifest } : {}),
        });
        break;
      }
      case "snapshot-block-request": {
        assertValidRequestId(message.requestId);
        assertValidNodeId(message.requesterNodeId);
        assertValidNodeId(message.targetNodeId);
        assertCasBlockHash(message.hash, "snapshot block hash");
        if (message.targetNodeId !== this.nodeId) {
          throw new Error(
            `snapshot block target mismatch: local=${this.nodeId} target=${message.targetNodeId}`,
          );
        }
        const content = await this.requireSnapshotCasStore().readBlock(message.hash);
        if (sha256(content) !== message.hash) {
          throw new Error(`local CAS block integrity check failed: ${message.hash}`);
        }
        this.send({
          type: "snapshot-block-response",
          requestId: message.requestId,
          requesterNodeId: message.requesterNodeId,
          hash: message.hash,
          contentBase64: Buffer.from(content).toString("base64"),
        });
        break;
      }
      case "snapshot-list-response":
      case "snapshot-catalog-response":
      case "snapshot-block-response":
        this.resolveSnapshotResponse(message);
        break;
      case "ping":
        assertValidSentAt(message.sentAt);
        this.sendIfOpen({ type: "pong", sentAt: message.sentAt });
        break;
      case "pong":
        assertValidSentAt(message.sentAt);
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

  private async requestSnapshot(
    targetNodeId: NodeId,
    kind: "list",
    payload: { partitionId: PartitionId },
  ): Promise<Extract<WireMessage, { type: "snapshot-list-response" }>>;
  private async requestSnapshot(
    targetNodeId: NodeId,
    kind: "catalog",
    payload: { groupId: string },
  ): Promise<Extract<WireMessage, { type: "snapshot-catalog-response" }>>;
  private async requestSnapshot(
    targetNodeId: NodeId,
    kind: "block",
    payload: { hash: string },
  ): Promise<Extract<WireMessage, { type: "snapshot-block-response" }>>;
  private async requestSnapshot(
    targetNodeId: NodeId,
    kind: "list" | "catalog" | "block",
    payload: { partitionId?: PartitionId; groupId?: string; hash?: string },
  ): Promise<WireMessage> {
    if (this.closed) {
      throw new Error("websocket connection is closed");
    }
    const requestId = uuidV7();
    const timeoutMs = this.replayRequestTimeoutMs();
    const response = new Promise<WireMessage>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingSnapshot.delete(requestId)) {
          reject(new Error(`snapshot request timed out after ${timeoutMs}ms`));
        }
      }, timeoutMs);
      this.pendingSnapshot.set(requestId, { kind, ...payload, resolve, reject, timer });
    });

    try {
      if (kind === "list") {
        this.send({
          type: "snapshot-list-request",
          requestId,
          requesterNodeId: this.nodeId,
          targetNodeId,
          partitionId: payload.partitionId!,
        });
      } else if (kind === "catalog") {
        this.send({
          type: "snapshot-catalog-request",
          requestId,
          requesterNodeId: this.nodeId,
          targetNodeId,
          groupId: payload.groupId!,
        });
      } else {
        this.send({
          type: "snapshot-block-request",
          requestId,
          requesterNodeId: this.nodeId,
          targetNodeId,
          hash: payload.hash!,
        });
      }
    } catch (error) {
      this.rejectPendingSnapshot(requestId, String(error));
      throw error;
    }

    return response;
  }

  private resolveSnapshotResponse(
    message: Extract<
      WireMessage,
      | { type: "snapshot-list-response" }
      | { type: "snapshot-catalog-response" }
      | { type: "snapshot-block-response" }
    >,
  ): void {
    const pending = this.pendingSnapshot.get(message.requestId);
    if (!pending) {
      return;
    }
    if (message.requesterNodeId !== this.nodeId) {
      this.rejectPendingSnapshot(
        message.requestId,
        `snapshot response mismatch: ${message.requestId}`,
      );
      return;
    }
    if (
      (message.type === "snapshot-list-response" &&
        (pending.kind !== "list" || message.partitionId !== pending.partitionId)) ||
      (message.type === "snapshot-catalog-response" &&
        (pending.kind !== "catalog" || message.groupId !== pending.groupId)) ||
      (message.type === "snapshot-block-response" &&
        (pending.kind !== "block" || message.hash !== pending.hash))
    ) {
      this.rejectPendingSnapshot(
        message.requestId,
        `snapshot response mismatch: ${message.requestId}`,
      );
      return;
    }

    this.pendingSnapshot.delete(message.requestId);
    clearTimeout(pending.timer);
    pending.resolve(message);
  }

  private rejectAllPendingSnapshot(reason: string): void {
    for (const [requestId, pending] of this.pendingSnapshot) {
      this.pendingSnapshot.delete(requestId);
      clearTimeout(pending.timer);
      pending.reject(new Error(reason));
    }
  }

  private rejectPendingSnapshot(requestId: string, reason: string): void {
    const pending = this.pendingSnapshot.get(requestId);
    if (!pending) {
      return;
    }
    this.pendingSnapshot.delete(requestId);
    clearTimeout(pending.timer);
    pending.reject(new Error(reason));
  }

  private requireSnapshotCasStore(): CasBlockStore {
    if (!this.snapshotCasStore) {
      throw new Error("websocket snapshot CAS store is not configured");
    }
    return this.snapshotCasStore;
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
    this.rttEwmaMs =
      this.rttEwmaMs === undefined
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

function validateProtocolVersion(value: unknown): number {
  if (value === undefined) {
    return 1;
  }
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new Error("protocolVersion must be a positive integer when present");
  }
  return value;
}

function assertValidRequestId(requestId: unknown): asserts requestId is string {
  if (typeof requestId !== "string" || requestId.length === 0) {
    throw new Error("requestId must be a non-empty string");
  }
}

function assertValidSentAt(sentAt: unknown): asserts sentAt is number {
  if (typeof sentAt !== "number" || !Number.isFinite(sentAt) || sentAt < 0) {
    throw new Error("sentAt must be a non-negative finite number");
  }
}

function assertValidWebSocketUrl(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty websocket URL`);
  }

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`${name} must be a valid websocket URL`);
  }
  if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") {
    throw new Error(`${name} must use ws or wss protocol`);
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
    case "snapshot-list-request":
    case "snapshot-list-response":
    case "snapshot-catalog-request":
    case "snapshot-catalog-response":
    case "snapshot-block-request":
    case "snapshot-block-response":
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

function assertNonEmptyString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function assertCasBlockHash(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new Error(`${name} must be a 64-character lowercase sha256 hash`);
  }
}

function validateSnapshotEntry(
  entry: StorageSnapshotCatalogEntry<CasManifest>,
): StorageSnapshotCatalogEntry<CasManifest> {
  assertNonEmptyString(entry.groupId, "snapshot entry groupId");
  assertValidPartitionId(entry.partitionId);
  assertNonEmptyString(entry.snapshotId, "snapshot entry snapshotId");
  validateEventCursorEntries(entry.highWatermark);
  if (entry.manifestKind !== "cas") {
    throw new Error(`wire snapshot source only supports cas manifests: ${entry.manifestKind}`);
  }
  validateCasManifest(entry.manifest, entry.snapshotId);
  return entry;
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

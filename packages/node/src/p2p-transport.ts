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

const REPLAY_REQUEST_FALLBACK_TIMEOUT_MS = 1_000;
const REPLAY_REQUEST_MIN_TIMEOUT_MS = 250;
const REPLAY_REQUEST_MAX_TIMEOUT_MS = 5_000;
const REPLAY_REQUEST_BASE_TIMEOUT_MS = 250;
const REPLAY_REQUEST_RTT_MULTIPLIER = 4;
const PING_INTERVAL_MS = 500;
const RTT_EWMA_ALPHA = 0.2;

// ─── options ──────────────────────────────────────────────────────────────────

export type P2PTransportOptions = {
  /** 他ノードに通知する自分の WebSocket URL (例: "ws://192.168.1.10:9000") */
  publicUrl: string;
  /** listen するポート番号 */
  port: number;
  /** 起動時に接続する既知ピアの URL 一覧 (ユーザーが指定) */
  seeds?: string[];
  compress?: boolean;
  /** 再接続の最大試行回数 (デフォルト 8) */
  maxReconnectAttempts?: number;
  /** 再接続の初期待機ミリ秒 (デフォルト 500) */
  reconnectBaseDelayMs?: number;
  /** 再接続の最大待機ミリ秒 (デフォルト 30000) */
  reconnectMaxDelayMs?: number;
};

// ─── wire protocol ────────────────────────────────────────────────────────────

type P2PWireMessage =
  | { type: "hello"; nodeId: NodeId; publicUrl: string; subscriptions: PartitionId[] }
  | { type: "peers"; urls: string[] }
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

// ─── PeerSocket ───────────────────────────────────────────────────────────────

type PeerRecord = {
  nodeId: NodeId | undefined;
  publicUrl: string | undefined;
  subscriptions: Set<PartitionId>;
  readonly direction: "inbound" | "outbound";
};

type PeerSocketCallbacks = {
  onHello(peer: PeerSocket): void;
  onPeers(peer: PeerSocket, urls: string[]): void;
  onMessage(peer: PeerSocket, msg: P2PWireMessage): void;
  onClose(peer: PeerSocket): void;
};

class PeerSocket {
  readonly record: PeerRecord;
  private closed = false;
  private closeResolve?: () => void;
  private pingInterval?: ReturnType<typeof setInterval>;
  private rttEwmaMs: number | undefined;
  readonly closePromise: Promise<void>;

  constructor(
    readonly ws: WebSocket,
    direction: "inbound" | "outbound",
    private readonly callbacks: PeerSocketCallbacks,
    private readonly zstd?: Awaited<ReturnType<typeof loadZstd>>,
    knownPublicUrl?: string,
  ) {
    this.record = {
      nodeId: undefined,
      publicUrl: knownPublicUrl,
      subscriptions: new Set(),
      direction,
    };
    this.closePromise = new Promise<void>((r) => {
      this.closeResolve = r;
    });
    this.startPing();

    this.ws.on("message", (data) => {
      void (async () => {
        try {
          const msg = this.decode(data as Buffer);
          if (msg.type === "hello") {
            if (this.record.nodeId) {
              throw new Error(`hello already received for peer: ${this.record.nodeId}`);
            }
            assertValidNodeId(msg.nodeId);
            assertValidWebSocketUrl(msg.publicUrl, "hello publicUrl");
            assertPartitionArray(msg.subscriptions, "hello subscriptions");
            for (const partitionId of msg.subscriptions) {
              assertValidPartitionId(partitionId);
            }
            this.record.nodeId = msg.nodeId;
            this.record.publicUrl ??= msg.publicUrl;
            for (const p of msg.subscriptions) this.record.subscriptions.add(p);
            this.callbacks.onHello(this);
          } else if (msg.type === "peers") {
            assertWebSocketUrlArray(msg.urls, "peers urls");
            this.callbacks.onPeers(this, msg.urls);
          } else if (msg.type === "ping") {
            assertValidSentAt(msg.sentAt);
            this.send({ type: "pong", sentAt: msg.sentAt });
          } else if (msg.type === "pong") {
            assertValidSentAt(msg.sentAt);
            this.recordRtt(Date.now() - msg.sentAt);
          } else {
            if (!this.record.nodeId) return; // hello 未受信 → 無視
            this.callbacks.onMessage(this, msg);
          }
        } catch (err) {
          console.error("[p2p] message decode error", err);
          this.close();
        }
      })();
    });

    this.ws.on("close", () => {
      this.closed = true;
      this.stopPing();
      this.closeResolve?.();
      this.callbacks.onClose(this);
    });

    this.ws.on("error", (err) => {
      if (!this.closed) {
        console.error("[p2p] socket error", err.message);
      }
    });
  }

  send(msg: P2PWireMessage): boolean {
    if (this.ws.readyState !== WebSocket.OPEN) return false;
    if (this.zstd) {
      this.ws.send(this.zstd.compress(Buffer.from(JSON.stringify(msg))));
    } else {
      this.ws.send(JSON.stringify(msg));
    }
    return true;
  }

  close(): void {
    if (!this.closed) {
      this.closed = true;
      this.ws.terminate();
    }
  }

  replayRequestTimeoutMs(): number {
    if (this.rttEwmaMs === undefined) {
      return REPLAY_REQUEST_FALLBACK_TIMEOUT_MS;
    }
    return clampTimeout(
      Math.ceil(REPLAY_REQUEST_BASE_TIMEOUT_MS + this.rttEwmaMs * REPLAY_REQUEST_RTT_MULTIPLIER),
    );
  }

  private decode(data: Buffer): P2PWireMessage {
    const message = this.zstd
      ? JSON.parse(Buffer.from(this.zstd.decompress(new Uint8Array(data))).toString())
      : JSON.parse(data.toString());
    assertKnownP2PWireMessage(message);
    return message;
  }

  private startPing(): void {
    if (this.pingInterval) {
      return;
    }
    this.pingInterval = setInterval(() => {
      this.send({ type: "ping", sentAt: Date.now() });
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
}

// ─── P2PWebSocketConnection ───────────────────────────────────────────────────

class P2PWebSocketConnection implements TransportConnection {
  private readonly peers = new Map<NodeId, PeerSocket>();
  private readonly peerSockets = new Set<PeerSocket>();
  private readonly knownUrls = new Set<string>();
  private readonly reconnecting = new Set<string>();
  private readonly localSubscriptions = new Set<PartitionId>();
  private readonly pendingReplay = new Map<
    string,
    {
      targetNodeId: NodeId;
      partitionId: PartitionId;
      resolve: (response: ReplayResponse) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private closed = false;

  private server!: WebSocketServer;
  private zstd: Awaited<ReturnType<typeof loadZstd>> | undefined;

  private readonly maxAttempts: number;
  private readonly baseDelay: number;
  private readonly maxDelay: number;

  constructor(
    private readonly nodeId: NodeId,
    private readonly handlers: TransportHandlers,
    private readonly options: Required<P2PTransportOptions>,
  ) {
    assertValidWebSocketUrl(options.publicUrl, "publicUrl");
    assertWebSocketUrlArray(options.seeds, "seeds");
    this.maxAttempts = options.maxReconnectAttempts;
    this.baseDelay = options.reconnectBaseDelayMs;
    this.maxDelay = options.reconnectMaxDelayMs;
  }

  async start(): Promise<void> {
    if (this.options.compress) {
      this.zstd = await loadZstd();
    }

    await new Promise<void>((resolve, reject) => {
      this.server = new WebSocketServer({ port: this.options.port });
      this.server.once("listening", resolve);
      this.server.once("error", reject);
    });

    this.server.on("connection", (ws) => {
      if (this.closed) {
        ws.terminate();
        return;
      }
      const peer = this.registerPeerSocket(ws, "inbound");
      this.sendHello(peer);
    });

    this.knownUrls.add(this.options.publicUrl);
    for (const url of this.options.seeds) {
      this.knownUrls.add(url);
      this.connectToPeer(url);
    }
  }

  async subscribe(partitionId: PartitionId): Promise<void> {
    if (this.closed) {
      throw new Error("p2p connection is closed");
    }
    assertValidPartitionId(partitionId);
    this.localSubscriptions.add(partitionId);
    for (const peer of this.peers.values()) {
      peer.send({ type: "subscribe", partitionId });
    }
  }

  async publish(event: EventEnvelope): Promise<void> {
    if (this.closed) {
      throw new Error("p2p connection is closed");
    }
    assertValidEventEnvelope(event);
    assertNodeMatches("event source", this.nodeId, event.sourceNodeId);
    for (const peer of this.peers.values()) {
      if (peer.record.subscriptions.has(event.partitionId)) {
        peer.send({ type: "event", event });
      }
    }
  }

  async requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    if (this.closed) {
      throw new Error("p2p connection is closed");
    }
    assertValidNodeId(targetNodeId);
    assertValidPartitionId(partitionId);
    validateEventCursorEntries([...cursors]);

    const peer = this.peers.get(targetNodeId);
    if (!peer) return { partitionId, events: [] };

    const requestId = uuidV7();
    const timeoutMs = peer.replayRequestTimeoutMs();
    const response = new Promise<ReplayResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingReplay.delete(requestId)) {
          reject(new Error(`replay request timed out after ${timeoutMs}ms`));
        }
      }, timeoutMs);
      this.pendingReplay.set(requestId, { targetNodeId, partitionId, resolve, reject, timer });
    });
    const sent = peer.send({
      type: "replay-request",
      requestId,
      requesterNodeId: this.nodeId,
      targetNodeId,
      partitionId,
      cursors: [...cursors],
    });
    if (!sent) {
      const pending = this.pendingReplay.get(requestId);
      if (pending) {
        this.pendingReplay.delete(requestId);
        clearTimeout(pending.timer);
      }
      throw new Error("p2p peer connection is not open");
    }
    return response;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.rejectAllPendingReplay("p2p connection closed");
    for (const peer of this.peerSockets) peer.close();
    for (const socket of this.server.clients) socket.terminate();
    this.peers.clear();
    this.peerSockets.clear();
    await new Promise<void>((r, j) => this.server.close((e) => (e ? j(e) : r())));
  }

  // ─── peer lifecycle ──────────────────────────────────────────────────────

  private registerPeerSocket(
    ws: WebSocket,
    direction: "inbound" | "outbound",
    knownUrl?: string,
  ): PeerSocket {
    const peer = new PeerSocket(
      ws,
      direction,
      {
        onHello: (p) => this.onPeerHello(p),
        onPeers: (p, urls) => this.onPeerPeers(p, urls),
        onMessage: (p, msg) => this.onPeerMessage(p, msg),
        onClose: (p) => this.onPeerClose(p),
      },
      this.zstd,
      knownUrl,
    );
    this.peerSockets.add(peer);
    return peer;
  }

  private connectToPeer(url: string): void {
    if (this.closed) return;
    assertValidWebSocketUrl(url, "peer url");
    void (async () => {
      const ws = new WebSocket(url);
      // ハンドラを open より先に登録し、サーバーが即座に送る hello を取りこぼさない
      const peer = this.registerPeerSocket(ws, "outbound", url);
      try {
        await new Promise<void>((resolve, reject) => {
          ws.once("open", resolve);
          ws.once("error", reject);
        });
        this.sendHello(peer);
        await peer.closePromise;
      } catch {
        // 接続失敗 → scheduleReconnect で再試行
      }
      if (!this.closed) this.scheduleReconnect(url);
    })();
  }

  private sendHello(peer: PeerSocket): void {
    peer.send({
      type: "hello",
      nodeId: this.nodeId,
      publicUrl: this.options.publicUrl,
      subscriptions: [...this.localSubscriptions],
    });
  }

  // ─── peer event handlers ─────────────────────────────────────────────────

  private onPeerHello(peer: PeerSocket): void {
    const nodeId = peer.record.nodeId!;
    if (nodeId === this.nodeId) {
      peer.close();
      return;
    }

    const existing = this.peers.get(nodeId);
    if (existing) {
      // 重複接続: nodeId 辞書順で小さい方が outbound を保持する
      const keepOutboundOf = this.nodeId < nodeId ? this.nodeId : nodeId;
      let keep: PeerSocket;
      let discard: PeerSocket;
      if (keepOutboundOf === this.nodeId) {
        keep = existing.record.direction === "outbound" ? existing : peer;
        discard = existing.record.direction === "outbound" ? peer : existing;
      } else {
        keep = existing.record.direction === "inbound" ? existing : peer;
        discard = existing.record.direction === "inbound" ? peer : existing;
      }
      this.peers.set(nodeId, keep);
      discard.close();
      // keep がすでに peers に登録済みなら追加処理不要
      if (keep === existing) return;
    } else {
      this.peers.set(nodeId, peer);
    }

    // 既知 URL を gossip
    if (peer.record.publicUrl) this.knownUrls.add(peer.record.publicUrl);
    // 送信先URL と 自分のURL を両方除外してゴシップ（自分のURLを含めると相手が重複接続を試みる）
    peer.send({
      type: "peers",
      urls: [...this.knownUrls].filter(
        (u) => u !== peer.record.publicUrl && u !== this.options.publicUrl,
      ),
    });
  }

  private onPeerPeers(_peer: PeerSocket, urls: string[]): void {
    for (const url of urls) {
      if (url === this.options.publicUrl) continue;
      if (this.knownUrls.has(url)) continue;
      this.knownUrls.add(url);
      this.connectToPeer(url);
    }
  }

  private onPeerMessage(peer: PeerSocket, msg: P2PWireMessage): void {
    void (async () => {
      try {
        const nodeId = peer.record.nodeId;
        if (nodeId && this.peers.get(nodeId) !== peer) {
          peer.close();
          return;
        }

        switch (msg.type) {
          case "subscribe":
            assertValidPartitionId(msg.partitionId);
            peer.record.subscriptions.add(msg.partitionId);
            break;
          case "event":
            assertValidEventEnvelope(msg.event);
            assertNodeMatches("event source", peer.record.nodeId!, msg.event.sourceNodeId);
            await this.handlers.onEvent(msg.event);
            break;
          case "replay-request": {
            assertValidRequestId(msg.requestId);
            assertValidNodeId(msg.requesterNodeId);
            assertValidNodeId(msg.targetNodeId);
            assertValidPartitionId(msg.partitionId);
            assertNodeMatches("replay requester", peer.record.nodeId!, msg.requesterNodeId);
            if (msg.targetNodeId !== this.nodeId) {
              throw new Error(
                `replay request target mismatch: local=${this.nodeId} target=${msg.targetNodeId}`,
              );
            }
            const cursors = validateEventCursorEntries(msg.cursors);
            if (!this.handlers.onReplayRequest) {
              peer.send({
                type: "replay-response",
                requestId: msg.requestId,
                requesterNodeId: msg.requesterNodeId,
                partitionId: msg.partitionId,
                events: [],
              });
              return;
            }
            const response = await this.handlers.onReplayRequest({
              requesterNodeId: msg.requesterNodeId,
              partitionId: msg.partitionId,
              cursors: new Map(cursors),
            });
            peer.send({
              type: "replay-response",
              requestId: msg.requestId,
              requesterNodeId: msg.requesterNodeId,
              ...response,
            });
            break;
          }
          case "replay-response": {
            assertValidRequestId(msg.requestId);
            assertValidNodeId(msg.requesterNodeId);
            assertValidPartitionId(msg.partitionId);
            for (const event of msg.events) {
              assertValidEventEnvelope(event);
            }
            const pending = this.pendingReplay.get(msg.requestId);
            if (pending) {
              if (peer.record.nodeId !== pending.targetNodeId) {
                throw new Error(`replay response source mismatch: ${msg.requestId}`);
              }
              if (msg.requesterNodeId !== this.nodeId || msg.partitionId !== pending.partitionId) {
                this.rejectPendingReplay(
                  msg.requestId,
                  `replay response mismatch: ${msg.requestId}`,
                );
                break;
              }
              const mismatchedEvent = msg.events.find(
                (event) => event.partitionId !== msg.partitionId,
              );
              if (mismatchedEvent) {
                this.rejectPendingReplay(
                  msg.requestId,
                  `replay event partition mismatch: response=${msg.partitionId} event=${mismatchedEvent.partitionId}`,
                );
                break;
              }
              this.pendingReplay.delete(msg.requestId);
              clearTimeout(pending.timer);
              pending.resolve({ partitionId: msg.partitionId, events: msg.events });
            }
            break;
          }
        }
      } catch (err) {
        console.error("[p2p] message handling error", err);
        peer.close();
      }
    })();
  }

  private onPeerClose(peer: PeerSocket): void {
    this.peerSockets.delete(peer);
    const nodeId = peer.record.nodeId;
    if (nodeId) {
      if (this.peers.get(nodeId) === peer) {
        this.peers.delete(nodeId);
        this.rejectPendingReplayForPeer(nodeId, `p2p peer disconnected: ${nodeId}`);
      }
    }
    const url = peer.record.publicUrl;
    // dedup で別方向を残して close した場合は再接続しない
    const hasLiveConnection = nodeId ? this.peers.has(nodeId) : false;
    if (url && !this.closed && !hasLiveConnection) this.scheduleReconnect(url);
  }

  private rejectPendingReplayForPeer(nodeId: NodeId, reason: string): void {
    for (const [requestId, pending] of this.pendingReplay) {
      if (pending.targetNodeId !== nodeId) continue;
      this.pendingReplay.delete(requestId);
      clearTimeout(pending.timer);
      pending.reject(new Error(reason));
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

  // ─── reconnection ────────────────────────────────────────────────────────

  private scheduleReconnect(url: string): void {
    if (url === this.options.publicUrl) return;
    if (this.reconnecting.has(url)) return;
    this.reconnecting.add(url);
    void this.reconnectLoop(url).finally(() => this.reconnecting.delete(url));
  }

  private async reconnectLoop(url: string): Promise<void> {
    let attempt = 0;
    while (!this.closed && attempt < this.maxAttempts) {
      const jitter = 0.75 + Math.random() * 0.5;
      const delay = Math.min(this.baseDelay * 2 ** attempt, this.maxDelay) * jitter;
      await new Promise<void>((r) => setTimeout(r, delay));
      if (this.closed) return;

      const ws = new WebSocket(url);
      const peer = this.registerPeerSocket(ws, "outbound", url);
      try {
        await new Promise<void>((resolve, reject) => {
          ws.once("open", resolve);
          ws.once("error", reject);
        });
        this.sendHello(peer);
        attempt = 0;
        await peer.closePromise;
      } catch {
        attempt++;
      }
    }
  }
}

function clampTimeout(ms: number): number {
  return Math.max(REPLAY_REQUEST_MIN_TIMEOUT_MS, Math.min(REPLAY_REQUEST_MAX_TIMEOUT_MS, ms));
}

function assertValidRequestId(requestId: unknown): asserts requestId is string {
  if (typeof requestId !== "string" || requestId.length === 0) {
    throw new Error("requestId must be a non-empty string");
  }
}

function assertPartitionArray(value: unknown, name: string): asserts value is PartitionId[] {
  if (!Array.isArray(value)) {
    throw new Error(`${name} must be an array`);
  }
}

function assertWebSocketUrlArray(value: unknown, name: string): asserts value is string[] {
  if (!Array.isArray(value)) {
    throw new Error(`${name} must be an array`);
  }
  for (const [index, url] of value.entries()) {
    assertValidWebSocketUrl(url, `${name}[${index}]`);
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

function assertKnownP2PWireMessage(message: unknown): asserts message is P2PWireMessage {
  if (typeof message !== "object" || message === null || !("type" in message)) {
    throw new Error("p2p wire message must be an object with a type");
  }
  switch ((message as { type: unknown }).type) {
    case "hello":
    case "peers":
    case "subscribe":
    case "ping":
    case "pong":
    case "event":
    case "replay-request":
    case "replay-response":
      return;
    default:
      throw new Error(
        `unknown p2p wire message type: ${String((message as { type: unknown }).type)}`,
      );
  }
}

function assertValidSentAt(sentAt: unknown): asserts sentAt is number {
  if (typeof sentAt !== "number" || !Number.isFinite(sentAt) || sentAt < 0) {
    throw new Error("sentAt must be a non-negative finite number");
  }
}

function assertNodeMatches(name: string, expected: NodeId, actual: NodeId): void {
  if (actual !== expected) {
    throw new Error(`${name} mismatch: expected=${expected} actual=${actual}`);
  }
}

// ─── P2PWebSocketTransport ────────────────────────────────────────────────────

export class P2PWebSocketTransport implements Transport {
  private readonly opts: Required<P2PTransportOptions>;

  constructor(options: P2PTransportOptions) {
    this.opts = {
      seeds: [],
      compress: false,
      maxReconnectAttempts: 8,
      reconnectBaseDelayMs: 500,
      reconnectMaxDelayMs: 30_000,
      ...options,
    };
    assertValidWebSocketUrl(this.opts.publicUrl, "publicUrl");
    assertWebSocketUrlArray(this.opts.seeds, "seeds");
  }

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    assertValidNodeId(nodeId);
    const conn = new P2PWebSocketConnection(nodeId, handlers, this.opts);
    await conn.start();
    return conn;
  }
}

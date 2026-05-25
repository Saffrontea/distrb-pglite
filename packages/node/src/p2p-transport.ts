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

const REPLAY_REQUEST_TIMEOUT_MS = 1_000;

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
  | { type: "event"; event: EventEnvelope }
  | {
      type: "replay-request";
      requestId: string;
      requesterNodeId: NodeId;
      targetNodeId: NodeId;
      partitionId: PartitionId;
      cursors: [NodeId, EventId][];
    }
  | { type: "replay-response"; requestId: string; partitionId: PartitionId; events: EventEnvelope[] };

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
    this.closePromise = new Promise<void>((r) => { this.closeResolve = r; });

    this.ws.on("message", (data) => {
      void (async () => {
        try {
          const msg = this.decode(data as Buffer);
          if (msg.type === "hello") {
            this.record.nodeId = msg.nodeId;
            this.record.publicUrl ??= msg.publicUrl;
            for (const p of msg.subscriptions) this.record.subscriptions.add(p);
            this.callbacks.onHello(this);
          } else if (msg.type === "peers") {
            this.callbacks.onPeers(this, msg.urls);
          } else {
            if (!this.record.nodeId) return; // hello 未受信 → 無視
            this.callbacks.onMessage(this, msg);
          }
        } catch (err) {
          console.error("[p2p] message decode error", err);
        }
      })();
    });

    this.ws.on("close", () => {
      this.closed = true;
      this.closeResolve?.();
      this.callbacks.onClose(this);
    });

    this.ws.on("error", (err) => {
      console.error("[p2p] socket error", err.message);
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
    if (!this.closed) this.ws.terminate();
  }

  private decode(data: Buffer): P2PWireMessage {
    if (this.zstd) {
      return JSON.parse(Buffer.from(this.zstd.decompress(new Uint8Array(data))).toString()) as P2PWireMessage;
    }
    return JSON.parse(data.toString()) as P2PWireMessage;
  }
}

// ─── P2PWebSocketConnection ───────────────────────────────────────────────────

class P2PWebSocketConnection implements TransportConnection {
  private readonly peers = new Map<NodeId, PeerSocket>();
  private readonly knownUrls = new Set<string>();
  private readonly reconnecting = new Set<string>();
  private readonly localSubscriptions = new Set<PartitionId>();
  private readonly pendingReplay = new Map<string, {
    targetNodeId: NodeId;
    resolve: (response: ReplayResponse) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }>();
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
    this.maxAttempts = options.maxReconnectAttempts;
    this.baseDelay   = options.reconnectBaseDelayMs;
    this.maxDelay    = options.reconnectMaxDelayMs;
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
    this.localSubscriptions.add(partitionId);
    for (const peer of this.peers.values()) {
      peer.send({ type: "subscribe", partitionId });
    }
  }

  async publish(event: EventEnvelope): Promise<void> {
    if (this.closed) {
      throw new Error("p2p connection is closed");
    }
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
    const peer = this.peers.get(targetNodeId);
    if (!peer) return { partitionId, events: [] };

    const requestId = crypto.randomUUID();
    const response = new Promise<ReplayResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pendingReplay.delete(requestId)) {
          reject(new Error(`replay request timed out after ${REPLAY_REQUEST_TIMEOUT_MS}ms`));
        }
      }, REPLAY_REQUEST_TIMEOUT_MS);
      this.pendingReplay.set(requestId, { targetNodeId, resolve, reject, timer });
    });
    peer.send({
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
    this.rejectAllPendingReplay("p2p connection closed");
    for (const peer of this.peers.values()) peer.close();
    this.peers.clear();
    await new Promise<void>((r, j) => this.server.close((e) => e ? j(e) : r()));
  }

  // ─── peer lifecycle ──────────────────────────────────────────────────────

  private registerPeerSocket(ws: WebSocket, direction: "inbound" | "outbound", knownUrl?: string): PeerSocket {
    const peer = new PeerSocket(ws, direction, {
      onHello:   (p) => this.onPeerHello(p),
      onPeers:   (p, urls) => this.onPeerPeers(p, urls),
      onMessage: (p, msg) => this.onPeerMessage(p, msg),
      onClose:   (p) => this.onPeerClose(p),
    }, this.zstd, knownUrl);
    return peer;
  }

  private connectToPeer(url: string): void {
    if (this.closed) return;
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
        keep    = existing.record.direction === "outbound" ? existing : peer;
        discard = existing.record.direction === "outbound" ? peer : existing;
      } else {
        keep    = existing.record.direction === "inbound" ? existing : peer;
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
    peer.send({ type: "peers", urls: [...this.knownUrls].filter((u) => u !== peer.record.publicUrl && u !== this.options.publicUrl) });
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
        switch (msg.type) {
          case "subscribe":
            peer.record.subscriptions.add(msg.partitionId);
            break;
          case "event":
            await this.handlers.onEvent(msg.event);
            break;
          case "replay-request": {
            if (!this.handlers.onReplayRequest) {
              peer.send({ type: "replay-response", requestId: msg.requestId, partitionId: msg.partitionId, events: [] });
              return;
            }
            const response = await this.handlers.onReplayRequest({
              requesterNodeId: msg.requesterNodeId,
              partitionId: msg.partitionId,
              cursors: new Map(msg.cursors),
            });
            peer.send({ type: "replay-response", requestId: msg.requestId, ...response });
            break;
          }
          case "replay-response": {
            const pending = this.pendingReplay.get(msg.requestId);
            if (pending) {
              this.pendingReplay.delete(msg.requestId);
              clearTimeout(pending.timer);
              pending.resolve({ partitionId: msg.partitionId, events: msg.events });
            }
            break;
          }
        }
      } catch (err) {
        console.error("[p2p] message handling error", err);
      }
    })();
  }

  private onPeerClose(peer: PeerSocket): void {
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
      const delay  = Math.min(this.baseDelay * 2 ** attempt, this.maxDelay) * jitter;
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
  }

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    const conn = new P2PWebSocketConnection(nodeId, handlers, this.opts);
    await conn.start();
    return conn;
  }
}

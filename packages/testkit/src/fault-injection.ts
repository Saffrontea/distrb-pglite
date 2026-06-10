import type {
  EventEnvelope,
  NodeId,
  PartitionId,
  PartitionStore,
  Transport,
  TransportConnection,
  TransportHandlers,
} from "@distrb-pglite/core";

// Deterministic PRNG so fault scenarios shrink and replay under fast-check.
export function mulberry32(seed: number): () => number {
  let s = seed;
  return () => {
    s |= 0;
    s = (s + 0x6d2b79f5) | 0;
    let z = Math.imul(s ^ (s >>> 15), 1 | s);
    z = (z + Math.imul(z ^ (z >>> 7), 61 | z)) ^ z;
    return ((z ^ (z >>> 14)) >>> 0) / 0x1_0000_0000;
  };
}

export type TransportFaultOptions = {
  // Probability that an inbound event delivery is silently dropped.
  dropEventRate?: number;
  // Probability that an inbound event is delivered twice.
  duplicateEventRate?: number;
  // Probability that an outbound replay request fails with an injected error.
  failReplayRate?: number;
  random?: () => number;
};

export type TransportFaultStats = {
  delivered: number;
  dropped: number;
  duplicated: number;
  failedReplays: number;
};

// Wraps any Transport and injects at-least-once delivery faults on the
// receiving side: drops, duplicates, and replay request failures. Storage
// semantics stay in the wrapped transport; this only perturbs delivery.
export class FaultInjectionTransport implements Transport {
  readonly stats: TransportFaultStats = {
    delivered: 0,
    dropped: 0,
    duplicated: 0,
    failedReplays: 0,
  };
  private readonly random: () => number;

  constructor(
    private readonly inner: Transport,
    private readonly options: TransportFaultOptions = {},
  ) {
    this.random = options.random ?? Math.random;
  }

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    const faultyHandlers: TransportHandlers = {
      ...handlers,
      onEvent: async (event) => {
        if (this.random() < (this.options.dropEventRate ?? 0)) {
          this.stats.dropped += 1;
          return;
        }
        await handlers.onEvent(event);
        this.stats.delivered += 1;
        if (this.random() < (this.options.duplicateEventRate ?? 0)) {
          this.stats.duplicated += 1;
          await handlers.onEvent(event);
        }
      },
    };

    const connection = await this.inner.connect(nodeId, faultyHandlers);
    return {
      subscribe: (partitionId) => connection.subscribe(partitionId),
      publish: (event) => connection.publish(event),
      close: () => connection.close(),
      requestReplay: async (targetNodeId, partitionId, cursors) => {
        if (this.random() < (this.options.failReplayRate ?? 0)) {
          this.stats.failedReplays += 1;
          throw new Error(`injected transport fault: replay ${nodeId} -> ${targetNodeId}`);
        }
        return connection.requestReplay(targetNodeId, partitionId, cursors);
      },
    };
  }
}

// Canonical, order-independent identity of a partition's applied event set.
export async function partitionEventKey(
  store: PartitionStore,
  partitionId: PartitionId,
): Promise<string> {
  const events = await store.listEventsAfterCursors(partitionId, new Map());
  return events
    .map((event: EventEnvelope) => `${event.sourceNodeId}/${event.eventId}`)
    .sort()
    .join(",");
}

export async function expectStoresConverged(
  stores: readonly PartitionStore[],
  partitionId: PartitionId,
): Promise<string> {
  const keys = await Promise.all(stores.map((store) => partitionEventKey(store, partitionId)));
  const reference = keys[0] ?? "";
  for (const [index, key] of keys.entries()) {
    if (key !== reference) {
      throw new Error(
        `stores did not converge for ${partitionId}: stores[0]=${reference} stores[${index}]=${key}`,
      );
    }
  }
  return reference;
}

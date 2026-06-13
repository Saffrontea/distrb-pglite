import type { EventId, HighWatermark, NodeId, PartitionId } from "./types.js";
import { compareEventIds } from "./uuid-v7.js";

// ADR 0008: thrown by catchup/reconcile when the peer has pruned events the
// requester still needs. Recovery is snapshot pull plus replay after the
// snapshot high-watermark, never partial replay.
export class SnapshotRequiredError extends Error {
  constructor(
    readonly partitionId: PartitionId,
    readonly peerNodeId: NodeId,
    readonly prunedThrough: ReadonlyMap<NodeId, EventId>,
  ) {
    super(
      `peer ${peerNodeId} has pruned events for partition ${partitionId}; ` +
        `recover via snapshot pull (pruned through ${prunedThrough.size} source cursor(s))`,
    );
    this.name = "SnapshotRequiredError";
  }
}

// A replay request has a gap when the responder pruned past any cursor the
// requester depends on: a missing cursor asks for a source's full history,
// which a pruned log can no longer serve.
export function hasReplayGap(
  cursors: ReadonlyMap<NodeId, EventId>,
  prunedThrough: HighWatermark,
): boolean {
  for (const [sourceNodeId, floor] of prunedThrough) {
    const cursor = cursors.get(sourceNodeId);
    if (!cursor || compareEventIds(cursor, floor) < 0) {
      return true;
    }
  }
  return false;
}

// ADR 0008: a prune boundary must be covered by a single bootstrap-capable
// snapshot's high-watermark, because recovery below the boundary uses one
// snapshot, not a combination. Returns the index of the first anchor that
// covers `through`, or undefined.
export function selectPruneAnchor(
  through: HighWatermark,
  anchors: readonly HighWatermark[],
): number | undefined {
  for (let index = 0; index < anchors.length; index += 1) {
    const anchor = anchors[index];
    let covered = true;
    for (const [sourceNodeId, eventId] of through) {
      const anchorCursor = anchor.get(sourceNodeId);
      if (!anchorCursor || compareEventIds(anchorCursor, eventId) < 0) {
        covered = false;
        break;
      }
    }
    if (covered) {
      return index;
    }
  }
  return undefined;
}

export function assertPruneAnchored(
  partitionId: PartitionId,
  through: HighWatermark,
  anchors: readonly HighWatermark[],
): number {
  const anchor = selectPruneAnchor(through, anchors);
  if (anchor === undefined) {
    throw new Error(
      `prune boundary for partition ${partitionId} is not covered by any retained ` +
        `bootstrap-capable snapshot high-watermark (ADR 0008)`,
    );
  }
  return anchor;
}

export type SubscriberPrunePlan = {
  partitionId: PartitionId;
  through: HighWatermark;
  subscriberCount: number;
};

// ADR 0008 follow-up: a deliberately separate, loose pruning policy for
// deployments that only promise replay to known subscribers. Unknown or
// offline nodes may be forced to recover by snapshot. With no subscribers,
// the local high-watermark is prunable. With subscribers, each source can be
// pruned only through the minimum cursor reported by every subscriber that
// has a cursor for that source; a missing cursor blocks that source.
export class SubscriberPrunePlanner {
  private readonly subscribers = new Map<PartitionId, Map<NodeId, HighWatermark>>();

  observeSubscriber(
    partitionId: PartitionId,
    subscriberNodeId: NodeId,
    cursors: HighWatermark,
  ): void {
    let partitionSubscribers = this.subscribers.get(partitionId);
    if (!partitionSubscribers) {
      partitionSubscribers = new Map();
      this.subscribers.set(partitionId, partitionSubscribers);
    }
    partitionSubscribers.set(subscriberNodeId, new Map(cursors));
  }

  removeSubscriber(partitionId: PartitionId, subscriberNodeId: NodeId): void {
    const partitionSubscribers = this.subscribers.get(partitionId);
    if (!partitionSubscribers) {
      return;
    }
    partitionSubscribers.delete(subscriberNodeId);
    if (partitionSubscribers.size === 0) {
      this.subscribers.delete(partitionId);
    }
  }

  plan(partitionId: PartitionId, localHighWatermark: HighWatermark): SubscriberPrunePlan {
    const partitionSubscribers = this.subscribers.get(partitionId);
    if (!partitionSubscribers || partitionSubscribers.size === 0) {
      return {
        partitionId,
        through: new Map(localHighWatermark),
        subscriberCount: 0,
      };
    }

    const through = new Map<NodeId, EventId>();
    for (const [sourceNodeId, localCursor] of localHighWatermark) {
      let floor: EventId | undefined = localCursor;
      for (const subscriberCursors of partitionSubscribers.values()) {
        const subscriberCursor = subscriberCursors.get(sourceNodeId);
        if (!subscriberCursor) {
          floor = undefined;
          break;
        }
        if (compareEventIds(subscriberCursor, floor) < 0) {
          floor = subscriberCursor;
        }
      }
      if (floor) {
        through.set(sourceNodeId, floor);
      }
    }

    return {
      partitionId,
      through,
      subscriberCount: partitionSubscribers.size,
    };
  }
}

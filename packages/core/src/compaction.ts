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

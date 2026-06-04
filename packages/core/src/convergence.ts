import type { EventId, HighWatermark, NodeId, PartitionId } from "./types.js";
import { compareEventIds } from "./uuid-v7.js";

export type CursorComparison = "equal" | "local-behind" | "local-ahead" | "diverged";

export type SourceCursorComparison = {
  sourceNodeId: NodeId;
  localEventId?: EventId;
  remoteEventId?: EventId;
  comparison: CursorComparison;
};

export type PartitionConvergenceStatus = {
  partitionId: PartitionId;
  local: HighWatermark;
  remote?: HighWatermark;
  comparisons: SourceCursorComparison[];
  missingSources: NodeId[];
  staleSources: NodeId[];
  aheadSources: NodeId[];
  converged: boolean;
};

export type CatchupPlan = {
  partitionId: PartitionId;
  shouldRequestReplay: boolean;
  missingSources: NodeId[];
  staleSources: NodeId[];
  cursors: HighWatermark;
};

export function compareHighWatermarks(
  local: HighWatermark,
  remote: HighWatermark,
): SourceCursorComparison[] {
  return [...new Set([...local.keys(), ...remote.keys()])].sort().map((sourceNodeId) => {
    const localEventId = local.get(sourceNodeId);
    const remoteEventId = remote.get(sourceNodeId);
    return {
      sourceNodeId,
      localEventId,
      remoteEventId,
      comparison: compareSourceCursor(localEventId, remoteEventId),
    };
  });
}

export function convergenceStatus(options: {
  partitionId: PartitionId;
  local: HighWatermark;
  remote?: HighWatermark;
}): PartitionConvergenceStatus {
  const comparisons = options.remote
    ? compareHighWatermarks(options.local, options.remote)
    : [...options.local.keys()].sort().map((sourceNodeId) => ({
        sourceNodeId,
        localEventId: options.local.get(sourceNodeId),
        comparison: "local-ahead" as const,
      }));
  const missingSources = comparisons
    .filter((comparison) => comparison.comparison === "local-behind" && !comparison.localEventId)
    .map((comparison) => comparison.sourceNodeId);
  const staleSources = comparisons
    .filter((comparison) => comparison.comparison === "local-behind" && comparison.localEventId)
    .map((comparison) => comparison.sourceNodeId);
  const aheadSources = comparisons
    .filter((comparison) => comparison.comparison === "local-ahead")
    .map((comparison) => comparison.sourceNodeId);

  return {
    partitionId: options.partitionId,
    local: options.local,
    remote: options.remote,
    comparisons,
    missingSources,
    staleSources,
    aheadSources,
    converged:
      options.remote !== undefined &&
      missingSources.length === 0 &&
      staleSources.length === 0 &&
      aheadSources.length === 0,
  };
}

export function planCatchupFromStatus(status: PartitionConvergenceStatus): CatchupPlan {
  return {
    partitionId: status.partitionId,
    shouldRequestReplay: status.missingSources.length > 0 || status.staleSources.length > 0,
    missingSources: status.missingSources,
    staleSources: status.staleSources,
    cursors: status.local,
  };
}

function compareSourceCursor(
  localEventId: EventId | undefined,
  remoteEventId: EventId | undefined,
): CursorComparison {
  if (!localEventId && !remoteEventId) {
    return "equal";
  }
  if (!localEventId) {
    return "local-behind";
  }
  if (!remoteEventId) {
    return "local-ahead";
  }

  const order = compareEventIds(localEventId, remoteEventId);
  if (order === 0) {
    return "equal";
  }
  return order < 0 ? "local-behind" : "local-ahead";
}

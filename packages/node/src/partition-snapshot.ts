import type {
  EventEnvelope,
  EventId,
  NodeId,
  PartitionId,
  PartitionStore,
  SnapshotId,
} from "@distrb-pglite/core";
import { UuidV7Factory } from "@distrb-pglite/core";
import { snapshotToCas, type CasManifest } from "./cas.js";

export type PartitionCasSnapshot = {
  version: 1;
  snapshotId: SnapshotId;
  partitionId: PartitionId;
  highWatermark: [NodeId, EventId][];
  manifest: CasManifest;
};

export async function createPartitionCasSnapshot(options: {
  store: PartitionStore;
  partitionId: PartitionId;
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  ids?: UuidV7Factory;
}): Promise<PartitionCasSnapshot> {
  const ids = options.ids ?? new UuidV7Factory();
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
  const manifest = await snapshotToCas({
    dataDir: options.dataDir,
    casDir: options.casDir,
    chunkSize: options.chunkSize,
  });

  return {
    version: 1,
    snapshotId: `partition-snapshot:${options.partitionId}:${ids.next()}`,
    partitionId: options.partitionId,
    highWatermark: [...highWatermark].sort(([left], [right]) => left.localeCompare(right)),
    manifest,
  };
}

export function highWatermarkMap(snapshot: PartitionCasSnapshot): ReadonlyMap<NodeId, EventId> {
  return new Map(snapshot.highWatermark);
}

export async function listEventsAfterPartitionSnapshot(
  store: PartitionStore,
  snapshot: PartitionCasSnapshot,
): Promise<EventEnvelope[]> {
  return store.listEventsAfterCursors(
    snapshot.partitionId,
    highWatermarkMap(snapshot),
  );
}

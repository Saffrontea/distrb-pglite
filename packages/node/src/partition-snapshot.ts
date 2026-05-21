import type {
  EventEnvelope,
  EventId,
  NodeId,
  PartitionId,
  PartitionStore,
  SnapshotId,
  StoragePartitionBinding,
  StorageRelationLocator,
} from "@distrb-pglite/core";
import { UuidV7Factory } from "@distrb-pglite/core";
import {
  restoreFromCas,
  snapshotToCas,
  type CasManifest,
  type CasSnapshotFilter,
} from "./cas.js";
import type { FileInventoryEntry } from "./inventory.js";

export type PartitionCasSnapshot = {
  version: 1;
  snapshotId: SnapshotId;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  highWatermark: [NodeId, EventId][];
  manifest: CasManifest;
};

export async function createPartitionCasSnapshot(options: {
  store: PartitionStore;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  ids?: UuidV7Factory;
  beforeStorageSnapshot?: () => Promise<void>;
}): Promise<PartitionCasSnapshot> {
  if (options.storageBinding.partitionId !== options.partitionId) {
    throw new Error(
      `storage binding partition mismatch: ${options.storageBinding.partitionId} !== ${options.partitionId}`,
    );
  }

  const ids = options.ids ?? new UuidV7Factory();
  const snapshotId = `partition-snapshot:${options.partitionId}:${ids.next()}`;
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
  await options.beforeStorageSnapshot?.();
  const manifest = await snapshotToCas({
    dataDir: options.dataDir,
    casDir: options.casDir,
    chunkSize: options.chunkSize,
    snapshotId,
    filter: storageBindingSnapshotFilter(options.storageBinding),
  });

  return {
    version: 1,
    snapshotId,
    partitionId: options.partitionId,
    storageBinding: options.storageBinding,
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

export async function restorePartitionCasSnapshot(options: {
  snapshot: PartitionCasSnapshot;
  casDir: string;
  targetDir: string;
}): Promise<void> {
  if (options.snapshot.storageBinding.snapshotScope !== "catalog-and-data") {
    throw new Error("only catalog-and-data partition snapshots can restore a full data directory");
  }

  await restoreFromCas({
    manifest: options.snapshot.manifest,
    casDir: options.casDir,
    targetDir: options.targetDir,
  });
}

export async function replayEventsAfterPartitionSnapshot(options: {
  sourceStore: PartitionStore;
  targetStore: PartitionStore;
  snapshot: PartitionCasSnapshot;
}): Promise<number> {
  await options.targetStore.registerPartition(options.snapshot.partitionId);
  const events = await listEventsAfterPartitionSnapshot(
    options.sourceStore,
    options.snapshot,
  );

  let applied = 0;
  for (const event of events) {
    if (await options.targetStore.applyRemote(event)) {
      applied += 1;
    }
  }

  return applied;
}

export function storageBindingSnapshotFilter(
  binding: StoragePartitionBinding,
): CasSnapshotFilter | undefined {
  if (binding.snapshotScope === "catalog-and-data") {
    return undefined;
  }

  const relationFiles = new Set(
    binding.relations.flatMap((relation) => relationFileStems(relation)),
  );

  return {
    includeFile(file: FileInventoryEntry): boolean {
      return relationFiles.has(relationFileStem(file.path));
    },
  };
}

function relationFileStems(relation: StorageRelationLocator): string[] {
  const relfilenode = relation.relfilenode ?? relation.relationOid;
  return [
    relation.storagePath,
    `base/${relation.databaseOid}/${relfilenode}`,
    `pg_tblspc/${relation.tablespaceOid}/${relation.databaseOid}/${relfilenode}`,
  ].filter((path): path is string => typeof path === "string" && path.length > 0);
}

function relationFileStem(path: string): string {
  return path
    .replace(/_(fsm|vm|init)(\.\d+)?$/, "")
    .replace(/\.\d+$/, "");
}

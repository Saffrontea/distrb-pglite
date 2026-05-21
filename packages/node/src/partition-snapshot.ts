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
import { snapshotToCas, type CasManifest, type CasSnapshotFilter } from "./cas.js";
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
}): Promise<PartitionCasSnapshot> {
  if (options.storageBinding.partitionId !== options.partitionId) {
    throw new Error(
      `storage binding partition mismatch: ${options.storageBinding.partitionId} !== ${options.partitionId}`,
    );
  }

  const ids = options.ids ?? new UuidV7Factory();
  const snapshotId = `partition-snapshot:${options.partitionId}:${ids.next()}`;
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
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
    `base/${relation.databaseOid}/${relfilenode}`,
    `pg_tblspc/${relation.tablespaceOid}/${relation.databaseOid}/${relfilenode}`,
  ];
}

function relationFileStem(path: string): string {
  return path
    .replace(/_(fsm|vm|init)(\.\d+)?$/, "")
    .replace(/\.\d+$/, "");
}

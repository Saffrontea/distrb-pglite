import type {
  EventEnvelope,
  EventId,
  NodeId,
  PartitionId,
  PartitionStore,
  SnapshotId,
  SnapshotFilesystem,
  SnapshotFilesystemManifest,
  StoragePartitionBinding,
  StorageRelationLocator,
} from "@distrb-pglite/core";
import { UuidV7Factory } from "@distrb-pglite/core";
import {
  restoreFromCas,
  snapshotToCas,
  type CasChunking,
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

export type PartitionSnapshotCursor = {
  partitionId: PartitionId;
  highWatermark: [NodeId, EventId][];
};

export type PartitionFilesystemSnapshot = PartitionSnapshotCursor & {
  version: 1;
  snapshotId: SnapshotId;
  storageBinding: StoragePartitionBinding;
  manifest: SnapshotFilesystemManifest;
};

export async function createPartitionCasSnapshot(options: {
  store: PartitionStore;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  chunking?: CasChunking;
  compress?: boolean;
  ids?: UuidV7Factory;
  beforeStorageSnapshot?: () => Promise<void>;
}): Promise<PartitionCasSnapshot> {
  validateStorageBinding(options.partitionId, options.storageBinding);

  const ids = options.ids ?? new UuidV7Factory();
  const snapshotId = `partition-snapshot:${options.partitionId}:${ids.next()}`;
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
  await options.beforeStorageSnapshot?.();
  const manifest = await snapshotToCas({
    dataDir: options.dataDir,
    casDir: options.casDir,
    chunkSize: options.chunkSize,
    chunking: options.chunking,
    compress: options.compress,
    snapshotId,
    filter: storageBindingSnapshotFilter(options.storageBinding),
  });

  return {
    version: 1,
    snapshotId,
    partitionId: options.partitionId,
    storageBinding: options.storageBinding,
    highWatermark: sortedHighWatermark(highWatermark),
    manifest,
  };
}

export async function createPartitionFilesystemSnapshot(options: {
  store: PartitionStore;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  fs: Pick<SnapshotFilesystem, "snapshot">;
  ids?: UuidV7Factory;
  beforeStorageSnapshot?: () => Promise<void>;
}): Promise<PartitionFilesystemSnapshot> {
  validateStorageBinding(options.partitionId, options.storageBinding);
  if (options.storageBinding.snapshotScope !== "catalog-and-data") {
    throw new Error("filesystem snapshots currently require catalog-and-data scope");
  }

  const ids = options.ids ?? new UuidV7Factory();
  const snapshotId = `partition-fs-snapshot:${options.partitionId}:${ids.next()}`;
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
  await options.beforeStorageSnapshot?.();
  const manifest = await options.fs.snapshot();

  return {
    version: 1,
    snapshotId,
    partitionId: options.partitionId,
    storageBinding: options.storageBinding,
    highWatermark: sortedHighWatermark(highWatermark),
    manifest,
  };
}

export function highWatermarkMap(snapshot: PartitionSnapshotCursor): ReadonlyMap<NodeId, EventId> {
  return new Map(snapshot.highWatermark);
}

export async function listEventsAfterPartitionSnapshot(
  store: PartitionStore,
  snapshot: PartitionSnapshotCursor,
): Promise<EventEnvelope[]> {
  return store.listEventsAfterCursors(
    snapshot.partitionId,
    highWatermarkMap(snapshot),
  );
}

export async function restorePartitionFilesystemSnapshot(options: {
  snapshot: PartitionFilesystemSnapshot;
  fs: Pick<SnapshotFilesystem, "restore">;
}): Promise<void> {
  if (options.snapshot.storageBinding.snapshotScope !== "catalog-and-data") {
    throw new Error("only catalog-and-data partition filesystem snapshots can restore a full filesystem");
  }

  await options.fs.restore(options.snapshot.manifest);
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

export async function restorePartitionDataOnlyCasSnapshot(options: {
  snapshot: PartitionCasSnapshot;
  casDir: string;
  targetDir: string;
  targetStorageBinding?: StoragePartitionBinding;
}): Promise<void> {
  if (options.snapshot.storageBinding.snapshotScope !== "data-only") {
    throw new Error("only data-only partition snapshots can restore relation files into an existing data directory");
  }

  await restoreFromCas({
    manifest: options.targetStorageBinding
      ? remapDataOnlyManifest(
          options.snapshot.manifest,
          options.snapshot.storageBinding,
          options.targetStorageBinding,
        )
      : options.snapshot.manifest,
    casDir: options.casDir,
    targetDir: options.targetDir,
  });
}

export async function replayEventsAfterPartitionSnapshot(options: {
  sourceStore: PartitionStore;
  targetStore: PartitionStore;
  snapshot: PartitionSnapshotCursor;
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
      return relationFiles.has(relationFileStem(file.path)) || isDataComponentMetadataFile(file.path);
    },
  };
}

function validateStorageBinding(
  partitionId: PartitionId,
  storageBinding: StoragePartitionBinding,
): void {
  if (storageBinding.partitionId !== partitionId) {
    throw new Error(
      `storage binding partition mismatch: ${storageBinding.partitionId} !== ${partitionId}`,
    );
  }
}

function sortedHighWatermark(highWatermark: ReadonlyMap<NodeId, EventId>): [NodeId, EventId][] {
  return [...highWatermark].sort(([left], [right]) => left.localeCompare(right));
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

function remapDataOnlyManifest(
  manifest: CasManifest,
  sourceBinding: StoragePartitionBinding,
  targetBinding: StoragePartitionBinding,
): CasManifest {
  return {
    ...manifest,
    directories: [],
    files: manifest.files.map((file) => ({
      ...file,
      path: isDataComponentMetadataFile(file.path)
        ? file.path
        : remapRelationFilePath(file.path, sourceBinding, targetBinding),
    })),
  };
}

function remapRelationFilePath(
  path: string,
  sourceBinding: StoragePartitionBinding,
  targetBinding: StoragePartitionBinding,
): string {
  const stem = relationFileStem(path);
  for (const [index, sourceRelation] of sourceBinding.relations.entries()) {
    const sourceStem = relationFileStems(sourceRelation).find((candidate) => candidate === stem);
    if (!sourceStem) {
      continue;
    }

    const targetRelation = targetBinding.relations[index];
    if (!targetRelation) {
      throw new Error(`target storage binding is missing relation at index ${index}`);
    }

    const targetStem = relationFileStems(targetRelation)[0];
    if (!targetStem) {
      throw new Error(`target storage binding is missing a storage path for relation at index ${index}`);
    }
    return `${targetStem}${path.slice(sourceStem.length)}`;
  }

  throw new Error(`data-only snapshot file does not match source storage binding: ${path}`);
}

function isDataComponentMetadataFile(path: string): boolean {
  return (
    path.startsWith("pg_xact/")
    || path.startsWith("pg_subtrans/")
    || path.startsWith("pg_multixact/")
    || path.startsWith("pg_commit_ts/")
    || path.startsWith("pg_serial/")
    || path.startsWith("pg_twophase/")
  );
}

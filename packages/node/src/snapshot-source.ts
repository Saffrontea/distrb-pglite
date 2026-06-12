import type {
  NodeId,
  PartitionId,
  SnapshotSource,
  StorageCatalogManifest,
  StorageSnapshotCatalogEntry,
} from "@distrb-pglite/core";
import { transferMissingBlocks, type CasManifest } from "./cas.js";

export type SharedDirSnapshotSourceOptions = {
  // The peer's CAS directory, visible over a shared filesystem.
  sourceCasDir: string;
  // Where fetched blocks land; normally the local facade's casDir.
  targetCasDir: string;
  // Discovery: how this deployment learns what the peer advertises. Often
  // wraps the peer facade's listSnapshotEntries or a serialized catalog.
  listEntries: (
    peerNodeId: NodeId,
    partitionId: PartitionId,
  ) => Promise<StorageSnapshotCatalogEntry<CasManifest>[]>;
  loadCatalogManifest?: (
    peerNodeId: NodeId,
    groupId: string,
  ) => Promise<StorageCatalogManifest | undefined>;
};

// ADR 0009 SnapshotSource over a shared filesystem: the simplest non-trivial
// implementation. Discovery is delegated to the deployment (shared catalogs
// are deployment-specific); transfer is content-addressed copy of missing
// blocks, idempotent by construction.
export class SharedDirSnapshotSource implements SnapshotSource<CasManifest> {
  constructor(private readonly options: SharedDirSnapshotSourceOptions) {}

  listSnapshotEntries(
    peerNodeId: NodeId,
    partitionId: PartitionId,
  ): Promise<StorageSnapshotCatalogEntry<CasManifest>[]> {
    return this.options.listEntries(peerNodeId, partitionId);
  }

  async fetchBlocks(
    _peerNodeId: NodeId,
    entry: StorageSnapshotCatalogEntry<CasManifest>,
  ): Promise<number> {
    const transferred = await transferMissingBlocks({
      manifest: entry.manifest,
      sourceCasDir: this.options.sourceCasDir,
      targetCasDir: this.options.targetCasDir,
    });
    return transferred.length;
  }

  async loadCatalogManifest(
    peerNodeId: NodeId,
    groupId: string,
  ): Promise<StorageCatalogManifest | undefined> {
    return this.options.loadCatalogManifest?.(peerNodeId, groupId);
  }
}

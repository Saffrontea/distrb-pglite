import { selectStorageSnapshotCatalogEntryForPull } from "@distrb-pglite/core";
import type {
  PartitionId,
  StorageBindingGroup,
  StoragePartitionBinding,
  StoragePullRequirement,
  StorageRelationLocator,
  StorageRelationRole,
  StorageSnapshotCatalogEntry,
  StorageSnapshotComponent,
  StorageSnapshotScope,
} from "@distrb-pglite/core";
import type { PartitionCasSnapshot, PartitionFilesystemSnapshot } from "./partition-snapshot.js";
import type { PGliteClient } from "./pglite-store.js";

type RelationIdentityRow = {
  database_oid: number;
  tablespace_oid: number;
  relation_oid: number;
  relfilenode: number | null;
  storage_path: string | null;
};

type BindingGroupRow = {
  group_id: string;
  root_relation_oid: number;
  ddl_generation: number;
  vacuum_policy: unknown;
};

type BindingRow = {
  partition_id: string;
  binding: unknown;
};

type SnapshotCatalogRow = {
  group_id: string;
  partition_id: string;
  snapshot_id: string;
  components: unknown;
  high_watermark: unknown;
  ddl_generation: number;
  manifest_kind: string;
  manifest: unknown;
};

export async function migrateStorageBindingCatalog(db: PGliteClient): Promise<void> {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS distrb_storage_binding_groups (
      group_id text PRIMARY KEY,
      root_relation_oid integer NOT NULL,
      ddl_generation integer NOT NULL,
      vacuum_policy jsonb
    );

    CREATE TABLE IF NOT EXISTS distrb_storage_bindings (
      group_id text NOT NULL REFERENCES distrb_storage_binding_groups(group_id) ON DELETE CASCADE,
      partition_id text NOT NULL,
      binding jsonb NOT NULL,
      PRIMARY KEY (group_id, partition_id)
    );

    CREATE TABLE IF NOT EXISTS distrb_storage_snapshots (
      group_id text NOT NULL REFERENCES distrb_storage_binding_groups(group_id) ON DELETE CASCADE,
      partition_id text NOT NULL,
      snapshot_id text NOT NULL,
      components jsonb NOT NULL,
      high_watermark jsonb NOT NULL,
      ddl_generation integer NOT NULL,
      manifest_kind text NOT NULL,
      manifest jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (group_id, partition_id, snapshot_id)
    );
  `);
}

export async function saveStorageBindingGroup(
  db: PGliteClient,
  group: StorageBindingGroup,
): Promise<void> {
  await migrateStorageBindingCatalog(db);
  await db.query(
    `
      INSERT INTO distrb_storage_binding_groups (
        group_id,
        root_relation_oid,
        ddl_generation,
        vacuum_policy
      )
      VALUES ($1, $2, $3, $4::jsonb)
      ON CONFLICT (group_id) DO UPDATE SET
        root_relation_oid = EXCLUDED.root_relation_oid,
        ddl_generation = EXCLUDED.ddl_generation,
        vacuum_policy = EXCLUDED.vacuum_policy
    `,
    [
      group.groupId,
      group.rootRelationOid,
      group.ddlGeneration,
      JSON.stringify(group.vacuumPolicy ?? null),
    ],
  );

  for (const binding of group.bindings) {
    await db.query(
      `
        INSERT INTO distrb_storage_bindings (group_id, partition_id, binding)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (group_id, partition_id) DO UPDATE SET
          binding = EXCLUDED.binding
      `,
      [group.groupId, binding.partitionId, JSON.stringify(binding)],
    );
  }
}

export async function loadStorageBindingGroup(
  db: PGliteClient,
  groupId: string,
): Promise<StorageBindingGroup | undefined> {
  await migrateStorageBindingCatalog(db);
  const groupResult = await db.query<BindingGroupRow>(
    `
      SELECT
        group_id,
        root_relation_oid,
        ddl_generation,
        vacuum_policy
      FROM distrb_storage_binding_groups
      WHERE group_id = $1
    `,
    [groupId],
  );
  const row = groupResult.rows[0];
  if (!row) {
    return undefined;
  }

  const bindingsResult = await db.query<BindingRow>(
    `
      SELECT partition_id, binding
      FROM distrb_storage_bindings
      WHERE group_id = $1
      ORDER BY partition_id
    `,
    [groupId],
  );

  return {
    groupId: row.group_id,
    rootRelationOid: row.root_relation_oid,
    ddlGeneration: row.ddl_generation,
    vacuumPolicy: row.vacuum_policy
      ? row.vacuum_policy as StorageBindingGroup["vacuumPolicy"]
      : undefined,
    bindings: bindingsResult.rows.map((bindingRow) =>
      bindingRow.binding as StoragePartitionBinding
    ),
  };
}

export async function loadStoragePartitionBinding(options: {
  db: PGliteClient;
  groupId: string;
  partitionId: PartitionId;
}): Promise<StoragePartitionBinding | undefined> {
  const group = await loadStorageBindingGroup(options.db, options.groupId);
  return group?.bindings.find((binding) => binding.partitionId === options.partitionId);
}

export async function saveStorageSnapshotCatalogEntry<TManifest>(
  db: PGliteClient,
  entry: StorageSnapshotCatalogEntry<TManifest>,
): Promise<void> {
  await migrateStorageBindingCatalog(db);
  await db.query(
    `
      INSERT INTO distrb_storage_snapshots (
        group_id,
        partition_id,
        snapshot_id,
        components,
        high_watermark,
        ddl_generation,
        manifest_kind,
        manifest
      )
      VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8::jsonb)
      ON CONFLICT (group_id, partition_id, snapshot_id) DO UPDATE SET
        components = EXCLUDED.components,
        high_watermark = EXCLUDED.high_watermark,
        ddl_generation = EXCLUDED.ddl_generation,
        manifest_kind = EXCLUDED.manifest_kind,
        manifest = EXCLUDED.manifest
    `,
    [
      entry.groupId,
      entry.partitionId,
      entry.snapshotId,
      JSON.stringify(entry.components),
      JSON.stringify(entry.highWatermark),
      entry.ddlGeneration,
      entry.manifestKind,
      JSON.stringify(entry.manifest),
    ],
  );
}

export async function savePartitionCasSnapshotCatalogEntry(options: {
  db: PGliteClient;
  groupId: string;
  snapshot: PartitionCasSnapshot;
  components?: StorageSnapshotComponent[];
}): Promise<void> {
  await saveStorageSnapshotCatalogEntry(options.db, {
    groupId: options.groupId,
    partitionId: options.snapshot.partitionId,
    snapshotId: options.snapshot.snapshotId,
    components: options.components ?? defaultComponents(options.snapshot.storageBinding.snapshotScope),
    highWatermark: options.snapshot.highWatermark,
    ddlGeneration: options.snapshot.storageBinding.ddlGeneration ?? 0,
    manifestKind: "cas",
    manifest: options.snapshot.manifest,
  });
}

export async function savePartitionFilesystemSnapshotCatalogEntry(options: {
  db: PGliteClient;
  groupId: string;
  snapshot: PartitionFilesystemSnapshot;
  components?: StorageSnapshotComponent[];
}): Promise<void> {
  await saveStorageSnapshotCatalogEntry(options.db, {
    groupId: options.groupId,
    partitionId: options.snapshot.partitionId,
    snapshotId: options.snapshot.snapshotId,
    components: options.components ?? defaultComponents(options.snapshot.storageBinding.snapshotScope),
    highWatermark: options.snapshot.highWatermark,
    ddlGeneration: options.snapshot.storageBinding.ddlGeneration ?? 0,
    manifestKind: "filesystem",
    manifest: options.snapshot.manifest,
  });
}

export async function loadStorageSnapshotCatalogEntry<TManifest = unknown>(options: {
  db: PGliteClient;
  groupId: string;
  partitionId: PartitionId;
  snapshotId: string;
}): Promise<StorageSnapshotCatalogEntry<TManifest> | undefined> {
  await migrateStorageBindingCatalog(options.db);
  const result = await options.db.query<SnapshotCatalogRow>(
    `
      SELECT
        group_id,
        partition_id,
        snapshot_id,
        components,
        high_watermark,
        ddl_generation,
        manifest_kind,
        manifest
      FROM distrb_storage_snapshots
      WHERE group_id = $1
        AND partition_id = $2
        AND snapshot_id = $3
    `,
    [options.groupId, options.partitionId, options.snapshotId],
  );

  return result.rows[0] ? snapshotCatalogEntryFromRow<TManifest>(result.rows[0]) : undefined;
}

export async function listStorageSnapshotCatalogEntries<TManifest = unknown>(options: {
  db: PGliteClient;
  groupId: string;
  partitionId?: PartitionId;
}): Promise<StorageSnapshotCatalogEntry<TManifest>[]> {
  await migrateStorageBindingCatalog(options.db);
  const result = await options.db.query<SnapshotCatalogRow>(
    `
      SELECT
        group_id,
        partition_id,
        snapshot_id,
        components,
        high_watermark,
        ddl_generation,
        manifest_kind,
        manifest
      FROM distrb_storage_snapshots
      WHERE group_id = $1
        AND ($2::text IS NULL OR partition_id = $2)
      ORDER BY partition_id, ddl_generation, snapshot_id
    `,
    [options.groupId, options.partitionId ?? null],
  );

  return result.rows.map((row) => snapshotCatalogEntryFromRow<TManifest>(row));
}

export async function loadStorageSnapshotCatalogEntryForPull<TManifest = unknown>(options: {
  db: PGliteClient;
  groupId: string;
  partitionId?: PartitionId;
  requirement: StoragePullRequirement;
}): Promise<StorageSnapshotCatalogEntry<TManifest> | undefined> {
  const entries = await listStorageSnapshotCatalogEntries<TManifest>({
    db: options.db,
    groupId: options.groupId,
    partitionId: options.partitionId,
  });
  return selectStorageSnapshotCatalogEntryForPull(entries, options.requirement);
}

export async function createStorageBindingGroup(options: {
  db: PGliteClient;
  groupId: string;
  rootRelation: string;
  ddlGeneration?: number;
  vacuumPolicy?: StorageBindingGroup["vacuumPolicy"];
}): Promise<StorageBindingGroup> {
  const root = await discoverRelationLocator(options.db, options.rootRelation, "root");
  const group: StorageBindingGroup = {
    groupId: options.groupId,
    rootRelationOid: root.relationOid,
    ddlGeneration: options.ddlGeneration ?? 1,
    vacuumPolicy: options.vacuumPolicy,
    bindings: [],
  };
  await saveStorageBindingGroup(options.db, group);
  return group;
}

export async function bindStoragePartition(options: {
  db: PGliteClient;
  groupId: string;
  partitionId: PartitionId;
  relations: string[];
  snapshotScope: StorageSnapshotScope;
  snapshotComponents?: StorageSnapshotComponent[];
  pullRequires?: StorageSnapshotComponent[];
}): Promise<StorageBindingGroup> {
  if (options.relations.length === 0) {
    throw new Error("at least one relation is required to bind a storage partition");
  }

  const group = await loadStorageBindingGroup(options.db, options.groupId);
  if (!group) {
    throw new Error(`storage binding group not found: ${options.groupId}`);
  }

  const nextGeneration = group.ddlGeneration + 1;
  const relations: StorageRelationLocator[] = [];
  for (const relationName of options.relations) {
    relations.push(await discoverRelationLocator(
      options.db,
      relationName,
      "child",
      group.rootRelationOid,
    ));
  }

  const binding: StoragePartitionBinding = {
    partitionId: options.partitionId,
    rootRelationOid: group.rootRelationOid,
    relations,
    snapshotScope: options.snapshotScope,
    snapshotComponents: options.snapshotComponents ?? defaultComponents(options.snapshotScope),
    pullRequires: options.pullRequires ?? defaultComponents(options.snapshotScope),
    ddlGeneration: nextGeneration,
  };

  const bindings = [
    ...group.bindings.filter((existing) => existing.partitionId !== options.partitionId),
    binding,
  ].sort((left, right) => left.partitionId.localeCompare(right.partitionId));

  const updated: StorageBindingGroup = {
    ...group,
    ddlGeneration: nextGeneration,
    bindings,
  };
  await saveStorageBindingGroup(options.db, updated);
  return updated;
}

export async function discoverStoragePartitionBinding(options: {
  db: PGliteClient;
  partitionId: PartitionId;
  rootRelation: string;
  relations?: string[];
  snapshotScope: StorageSnapshotScope;
}): Promise<StoragePartitionBinding> {
  const relationNames = options.relations?.length
    ? options.relations
    : [options.rootRelation];
  const relations: StorageRelationLocator[] = [];

  for (const relationName of relationNames) {
    relations.push(await discoverRelationLocator(options.db, relationName));
  }

  return {
    partitionId: options.partitionId,
    rootRelationOid: relations[0]?.relationOid ?? 0,
    relations,
    snapshotScope: options.snapshotScope,
  };
}

async function discoverRelationLocator(
  db: PGliteClient,
  relationName: string,
  role?: StorageRelationRole,
  parentRelationOid?: number,
): Promise<StorageRelationLocator> {
  const result = await db.query<RelationIdentityRow>(
    `
      SELECT
        d.oid::int AS database_oid,
        COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::int AS tablespace_oid,
        c.oid::int AS relation_oid,
        NULLIF(pg_relation_filenode(c.oid), 0)::int AS relfilenode,
        pg_relation_filepath(c.oid) AS storage_path
      FROM pg_class c
      JOIN pg_database d ON d.datname = current_database()
      WHERE c.oid = to_regclass($1)
    `,
    [relationName],
  );
  const row = result.rows[0];
  if (!row) {
    throw new Error(`relation not found for storage binding: ${relationName}`);
  }

  return {
    databaseOid: row.database_oid,
    tablespaceOid: row.tablespace_oid,
    relationOid: row.relation_oid,
    relfilenode: row.relfilenode ?? row.relation_oid,
    storagePath: row.storage_path ?? undefined,
    role,
    parentRelationOid,
  };
}

function defaultComponents(scope: StorageSnapshotScope): StorageSnapshotComponent[] {
  return scope === "catalog-and-data" ? ["catalog", "data"] : ["data"];
}

function snapshotCatalogEntryFromRow<TManifest>(
  row: SnapshotCatalogRow,
): StorageSnapshotCatalogEntry<TManifest> {
  return {
    groupId: row.group_id,
    partitionId: row.partition_id,
    snapshotId: row.snapshot_id,
    components: row.components as StorageSnapshotComponent[],
    highWatermark: row.high_watermark as StorageSnapshotCatalogEntry<TManifest>["highWatermark"],
    ddlGeneration: row.ddl_generation,
    manifestKind: row.manifest_kind as StorageSnapshotCatalogEntry<TManifest>["manifestKind"],
    manifest: row.manifest as TManifest,
  };
}

import type {
  PartitionId,
  StorageBindingGroup,
  StoragePartitionBinding,
  StorageSnapshotScope,
} from "@distrb-pglite/core";
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
  const relations = [];

  for (const relationName of relationNames) {
    const result = await options.db.query<RelationIdentityRow>(
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

    relations.push({
      databaseOid: row.database_oid,
      tablespaceOid: row.tablespace_oid,
      relationOid: row.relation_oid,
      relfilenode: row.relfilenode ?? row.relation_oid,
      storagePath: row.storage_path ?? undefined,
    });
  }

  return {
    partitionId: options.partitionId,
    rootRelationOid: relations[0]?.relationOid ?? 0,
    relations,
    snapshotScope: options.snapshotScope,
  };
}

#!/usr/bin/env node
import { PGlite } from "@electric-sql/pglite";
import { SyncNode } from "@distrb-pglite/core";
import { parseArgs } from "node:util";
import { restoreFromCas, snapshotToCas } from "./cas.js";
import { copyDataDirSnapshot, inventoryDataDir } from "./inventory.js";
import { createPartitionCasSnapshot } from "./partition-snapshot.js";
import { PGlitePartitionStore } from "./pglite-store.js";
import { WebSocketHub, WebSocketTransport } from "./websocket-transport.js";
import { runNodeDemo } from "./demo.js";

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    offline: { type: "boolean", default: false },
    port: { type: "string", default: "8787" },
    url: { type: "string", default: "ws://localhost:8787" },
    node: { type: "string" },
    peer: { type: "string" },
    dataDir: { type: "string" },
    casDir: { type: "string" },
    targetDir: { type: "string" },
    manifest: { type: "string" },
    partition: { type: "string" },
    payload: { type: "string" },
    storageBinding: { type: "string" },
  },
});

const command = positionals[0];

if (command === "hub") {
  const port = Number(values.port);
  new WebSocketHub({ port });
  console.log(`distrb-pglite hub listening on ws://localhost:${port}`);
} else if (command === "node") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition) {
    throw new Error("--node and --partition are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  const db = new PGlite(dataDir);
  const store = new PGlitePartitionStore(nodeId, db);
  await store.migrate();

  const node = new SyncNode(nodeId, store, new WebSocketTransport(values.url));
  await node.start();
  await node.subscribe(partition);

  console.log(`${nodeId} subscribed to ${partition} via ${values.url}`);
} else if (command === "write") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition || !values.payload) {
    throw new Error("--node, --partition, and --payload are required");
  }

  const store = await openStore(nodeId, values.dataDir);
  const payload = JSON.parse(values.payload);

  if (values.offline) {
    const event = await store.appendLocal({
      partitionId: partition,
      schemaVersion: 1,
      payload,
    });
    console.log(JSON.stringify(event, null, 2));
  } else {
    const node = new SyncNode(nodeId, store, new WebSocketTransport(values.url));
    await node.start();
    const event = await node.write({
      partitionId: partition,
      schemaVersion: 1,
      payload,
    });
    await node.stop();
    console.log(JSON.stringify(event, null, 2));
  }
} else if (command === "list") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition) {
    throw new Error("--node and --partition are required");
  }

  const store = await openStore(nodeId, values.dataDir);
  const events = await store.listEventsAfterCursors(partition, new Map());
  console.log(JSON.stringify(events, null, 2));
} else if (command === "catchup") {
  const nodeId = values.node;
  const peerNodeId = values.peer;
  const partition = values.partition;
  if (!nodeId || !peerNodeId || !partition) {
    throw new Error("--node, --peer, and --partition are required");
  }

  const store = await openStore(nodeId, values.dataDir);
  const node = new SyncNode(nodeId, store, new WebSocketTransport(values.url));
  await node.start();
  await node.subscribe(partition);
  const applied = await node.catchUpFrom(peerNodeId, partition);
  await node.stop();
  console.log(JSON.stringify({ applied }, null, 2));
} else if (command === "inventory") {
  const nodeId = values.node;
  if (!nodeId) {
    throw new Error("--node is required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  console.log(JSON.stringify(await inventoryDataDir(dataDir), null, 2));
} else if (command === "copy-snapshot") {
  const nodeId = values.node;
  if (!nodeId || !values.targetDir) {
    throw new Error("--node and --targetDir are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  console.log(JSON.stringify(await copyDataDirSnapshot({
    sourceDir: dataDir,
    targetDir: values.targetDir,
  }), null, 2));
} else if (command === "cas-snapshot") {
  const nodeId = values.node;
  if (!nodeId || !values.casDir) {
    throw new Error("--node and --casDir are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  const manifest = await snapshotToCas({ dataDir, casDir: values.casDir });
  console.log(JSON.stringify(manifest, null, 2));
} else if (command === "partition-snapshot") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition || !values.casDir || !values.storageBinding) {
    throw new Error("--node, --partition, --casDir, and --storageBinding are required");
  }

  const store = await openStore(nodeId, values.dataDir);
  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  const snapshot = await createPartitionCasSnapshot({
    store,
    partitionId: partition,
    storageBinding: JSON.parse(values.storageBinding),
    dataDir,
    casDir: values.casDir,
  });
  console.log(JSON.stringify(snapshot, null, 2));
} else if (command === "cas-restore") {
  if (!values.casDir || !values.targetDir || !values.manifest) {
    throw new Error("--casDir, --targetDir, and --manifest are required");
  }

  await restoreFromCas({
    casDir: values.casDir,
    targetDir: values.targetDir,
    manifest: JSON.parse(values.manifest),
  });
} else if (command === "demo") {
  const summary = await runNodeDemo({
    baseDir: values.dataDir,
    port: Number(values.port),
  });
  console.log(JSON.stringify(summary, null, 2));
} else {
  console.log(`Usage:
  distrb-pglite hub --port 8787
  distrb-pglite demo --dataDir ./.data/phase6-demo --port 8787
  distrb-pglite node --node node-a --partition tenant:jp --url ws://localhost:8787
  distrb-pglite write --node node-a --partition tenant:jp --payload '{"title":"hello"}' --url ws://localhost:8787
  distrb-pglite write --node node-a --partition tenant:jp --payload '{"title":"hello"}' --offline
  distrb-pglite list --node node-a --partition tenant:jp
  distrb-pglite catchup --node node-b --peer node-a --partition tenant:jp --url ws://localhost:8787
  distrb-pglite inventory --node node-a
  distrb-pglite copy-snapshot --node node-a --targetDir ./.restore/node-a
  distrb-pglite cas-snapshot --node node-a --casDir ./.cas
  distrb-pglite partition-snapshot --node node-a --partition tenant:jp --casDir ./.cas --storageBinding '<json>'
  distrb-pglite cas-restore --casDir ./.cas --targetDir ./.restore/node-a --manifest '<json>'
`);
}

async function openStore(
  nodeId: string,
  dataDir = `./.data/${nodeId}`,
): Promise<PGlitePartitionStore> {
  const db = new PGlite(dataDir);
  const store = new PGlitePartitionStore(nodeId, db);
  await store.migrate();
  return store;
}

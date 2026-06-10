import { describe, expect, it } from "vitest";
import {
  InMemoryPartitionStore,
  MemoryTransport,
  SingleWriterRegistry,
  SingleWriterViolationError,
  SyncNode,
} from "../src/index.js";

describe("SingleWriterRegistry", () => {
  it("learns the writer from the first observed event", () => {
    const registry = new SingleWriterRegistry();
    registry.observe({ partitionId: "tenant:jp", sourceNodeId: "node-a" });

    expect(registry.ownerOf("tenant:jp")).toBe("node-a");
    expect(() =>
      registry.observe({ partitionId: "tenant:jp", sourceNodeId: "node-a" }),
    ).not.toThrow();
  });

  it("treats a second writer without a transfer as a protocol violation", () => {
    const registry = new SingleWriterRegistry();
    registry.assign("tenant:jp", "node-a");

    expect(() => registry.observe({ partitionId: "tenant:jp", sourceNodeId: "node-b" })).toThrow(
      SingleWriterViolationError,
    );
    expect(registry.isViolation({ partitionId: "tenant:jp", sourceNodeId: "node-b" })).toBe(true);
    expect(registry.ownerOf("tenant:jp")).toBe("node-a");
  });

  it("allows the new writer only after an explicit ownership transfer", () => {
    const registry = new SingleWriterRegistry();
    registry.assign("tenant:jp", "node-a");
    registry.transfer({ partitionId: "tenant:jp", fromNodeId: "node-a", toNodeId: "node-b" });

    expect(registry.ownerOf("tenant:jp")).toBe("node-b");
    expect(() =>
      registry.observe({ partitionId: "tenant:jp", sourceNodeId: "node-b" }),
    ).not.toThrow();
    expect(() => registry.observe({ partitionId: "tenant:jp", sourceNodeId: "node-a" })).toThrow(
      SingleWriterViolationError,
    );
  });

  it("rejects transfers that do not start from the current writer", () => {
    const registry = new SingleWriterRegistry();
    registry.assign("tenant:jp", "node-a");

    expect(() =>
      registry.transfer({ partitionId: "tenant:jp", fromNodeId: "node-b", toNodeId: "node-c" }),
    ).toThrow("current writer is node-a");
    expect(() =>
      registry.transfer({ partitionId: "tenant:us", fromNodeId: "node-a", toNodeId: "node-b" }),
    ).toThrow("no writer to transfer from");
  });

  it("rejects re-assignment without a transfer", () => {
    const registry = new SingleWriterRegistry();
    registry.assign("tenant:jp", "node-a");

    expect(() => registry.assign("tenant:jp", "node-b")).toThrow("use transfer");
    expect(() => registry.assign("tenant:jp", "node-a")).not.toThrow();
  });
});

describe("SyncNode single-writer enforcement", () => {
  it("skips and reports remote events from a non-owner writer", async () => {
    const transport = new MemoryTransport();
    const violations: SingleWriterViolationError[] = [];

    const ownerStore = new InMemoryPartitionStore("node-owner");
    const owner = new SyncNode("node-owner", ownerStore, transport);
    const rogueStore = new InMemoryPartitionStore("node-rogue");
    const rogue = new SyncNode("node-rogue", rogueStore, transport);

    const subscriberRegistry = new SingleWriterRegistry();
    subscriberRegistry.assign("tenant:jp", "node-owner");
    const subscriberStore = new InMemoryPartitionStore("node-subscriber");
    const subscriber = new SyncNode("node-subscriber", subscriberStore, transport, {
      singleWriter: subscriberRegistry,
      onSingleWriterViolation: (error) => violations.push(error),
    });

    await owner.start();
    await rogue.start();
    await subscriber.start();
    await owner.subscribe("tenant:jp");
    await rogue.subscribe("tenant:jp");
    await subscriber.subscribe("tenant:jp");

    await owner.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { value: "ok" } });
    await rogue.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { value: "bad" } });

    const applied = await subscriberStore.listEventsAfterCursors("tenant:jp", new Map());
    expect(applied.map((event) => event.payload)).toEqual([{ value: "ok" }]);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toMatchObject({
      partitionId: "tenant:jp",
      ownerNodeId: "node-owner",
      violatingNodeId: "node-rogue",
    });

    await subscriber.stop();
    await rogue.stop();
    await owner.stop();
  });

  it("throws on violating remote events when no violation handler is configured", async () => {
    const transport = new MemoryTransport();

    const registry = new SingleWriterRegistry();
    registry.assign("tenant:jp", "node-owner");
    const subscriberStore = new InMemoryPartitionStore("node-subscriber");
    const subscriber = new SyncNode("node-subscriber", subscriberStore, transport, {
      singleWriter: registry,
    });
    const rogueStore = new InMemoryPartitionStore("node-rogue");
    const rogue = new SyncNode("node-rogue", rogueStore, transport);

    await subscriber.start();
    await rogue.start();
    await rogue.subscribe("tenant:jp");
    // Register locally without a transport subscription so the violating
    // event arrives through the replay path, not live delivery.
    await subscriberStore.registerPartition("tenant:jp");
    await rogue.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { value: "x" } });

    await expect(subscriber.catchUpFrom("node-rogue", "tenant:jp")).rejects.toThrow(
      SingleWriterViolationError,
    );

    await rogue.stop();
    await subscriber.stop();
  });

  it("rejects local writes into a partition owned by another node", async () => {
    const transport = new MemoryTransport();
    const registry = new SingleWriterRegistry();
    registry.assign("tenant:jp", "node-owner");

    const store = new InMemoryPartitionStore("node-b");
    const node = new SyncNode("node-b", store, transport, { singleWriter: registry });
    await node.start();
    await node.subscribe("tenant:jp");

    await expect(
      node.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { value: "x" } }),
    ).rejects.toThrow(SingleWriterViolationError);
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);

    registry.transfer({ partitionId: "tenant:jp", fromNodeId: "node-owner", toNodeId: "node-b" });
    await expect(
      node.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { value: "y" } }),
    ).resolves.toMatchObject({ partitionId: "tenant:jp" });

    await node.stop();
  });
});

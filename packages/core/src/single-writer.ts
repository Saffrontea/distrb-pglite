import type { EventEnvelope, NodeId, PartitionId } from "./types.js";
import { assertValidNodeId, assertValidPartitionId } from "./store.js";

// ADR 0006: each partition has exactly one writer at a time. Events for one
// partition from two source nodes without an intervening ownership transfer
// are a protocol violation, not a conflict to resolve. This registry is the
// local, opt-in detector for that invariant; the durable transfer protocol
// stays out of scope until ownership transfer is designed end to end.
export class SingleWriterViolationError extends Error {
  constructor(
    readonly partitionId: PartitionId,
    readonly ownerNodeId: NodeId,
    readonly violatingNodeId: NodeId,
  ) {
    super(
      `single-writer violation for partition ${partitionId}: ` +
        `owner=${ownerNodeId} violating-writer=${violatingNodeId}`,
    );
    this.name = "SingleWriterViolationError";
  }
}

export type PartitionOwnershipTransfer = {
  partitionId: PartitionId;
  fromNodeId: NodeId;
  toNodeId: NodeId;
};

export class SingleWriterRegistry {
  private readonly owners = new Map<PartitionId, NodeId>();

  ownerOf(partitionId: PartitionId): NodeId | undefined {
    assertValidPartitionId(partitionId);
    return this.owners.get(partitionId);
  }

  assign(partitionId: PartitionId, ownerNodeId: NodeId): void {
    assertValidPartitionId(partitionId);
    assertValidNodeId(ownerNodeId);
    const current = this.owners.get(partitionId);
    if (current !== undefined && current !== ownerNodeId) {
      throw new Error(
        `partition ${partitionId} already has writer ${current}; use transfer to change ownership`,
      );
    }
    this.owners.set(partitionId, ownerNodeId);
  }

  transfer(transfer: PartitionOwnershipTransfer): void {
    assertValidPartitionId(transfer.partitionId);
    assertValidNodeId(transfer.fromNodeId);
    assertValidNodeId(transfer.toNodeId);
    const current = this.owners.get(transfer.partitionId);
    if (current === undefined) {
      throw new Error(`partition ${transfer.partitionId} has no writer to transfer from`);
    }
    if (current !== transfer.fromNodeId) {
      throw new Error(
        `partition ${transfer.partitionId} ownership transfer from ${transfer.fromNodeId} ` +
          `rejected: current writer is ${current}`,
      );
    }
    this.owners.set(transfer.partitionId, transfer.toNodeId);
  }

  // Learns the writer from the first observed event and rejects any later
  // event for the same partition from a different source node.
  observe(event: Pick<EventEnvelope, "partitionId" | "sourceNodeId">): void {
    assertValidPartitionId(event.partitionId);
    assertValidNodeId(event.sourceNodeId);
    const owner = this.owners.get(event.partitionId);
    if (owner === undefined) {
      this.owners.set(event.partitionId, event.sourceNodeId);
      return;
    }
    if (owner !== event.sourceNodeId) {
      throw new SingleWriterViolationError(event.partitionId, owner, event.sourceNodeId);
    }
  }

  isViolation(event: Pick<EventEnvelope, "partitionId" | "sourceNodeId">): boolean {
    const owner = this.owners.get(event.partitionId);
    return owner !== undefined && owner !== event.sourceNodeId;
  }
}

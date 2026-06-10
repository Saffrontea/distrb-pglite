# ADR 0006: Single-Writer Partition Ownership

## Decision

Each partition has exactly one writer node at a time. Conflict resolution between concurrent writers is out of scope: the system does not implement LWW, CRDTs, or merge procedures. Other nodes replicate a partition as read-only subscribers until ownership is explicitly transferred.

## Consequences

- Eventual consistency means "subscribers converge to the owner's stream", not "concurrent writes merge".
- Idempotent apply keyed by `(source_node_id, event_id)` remains sufficient; no merge logic is layered on top.
- Ownership transfer is an explicit operation and must fence the old writer before the new writer starts emitting.
- Events for one partition from two source nodes without an intervening transfer indicate a protocol violation, not a conflict to resolve.
- Application schemas must choose partition keys that match real write ownership (per tenant, per device, per site).

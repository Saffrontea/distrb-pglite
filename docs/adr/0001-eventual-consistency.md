# ADR 0001: Eventual Consistency Only

## Decision

The MVP provides local transactions plus asynchronous event replication. It does not implement global transactions, global locks, or strong cross-node consistency.

## Consequences

- Reads are local.
- Writes are accepted locally and emitted as events.
- Convergence is validated through idempotent event application.
- Application schemas must choose partition keys that tolerate eventual consistency.

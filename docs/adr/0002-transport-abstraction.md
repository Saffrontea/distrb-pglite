# ADR 0002: Transport As A VFS-Like Boundary

## Decision

The sync engine depends on a small `Transport` interface instead of a concrete network implementation. Memory, WebSocket, and future MQTT transports share the same publish/subscribe contract.

## Consequences

- Core sync tests can run deterministically with `MemoryTransport`.
- WebSocket is an adapter, not a dependency of the core package.
- Browser and worker runtimes can add adapters without changing sync semantics.

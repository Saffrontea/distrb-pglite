# ADR 0003: Node APIs Stay Outside Core

## Decision

Node-specific filesystem, process, and WebSocket code belongs in `packages/node`. `packages/core` only uses platform-neutral TypeScript APIs and injected dependencies.

## Consequences

- The initial CLI can run on Node.
- The core can later be reused in browsers, workers, or embedded runtimes.
- Runtime-specific persistence and network concerns remain replaceable.

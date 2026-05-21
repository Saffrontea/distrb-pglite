# Node Demo

This demo exercises the current Node adapter layer: PGlite persistence, WebSocket transport, local writes, local reads, and pull catch-up.

## Setup

```bash
mise exec -- pnpm install
mise exec -- pnpm build
```

## Start A Hub

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite hub --port 8787
```

## Keep A Subscribed Node Running

In another terminal:

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite node \
  --node node-b \
  --partition tenant:jp \
  --url ws://localhost:8787
```

## Write From Another Node

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite write \
  --node node-a \
  --partition tenant:jp \
  --payload '{"title":"hello from node-a"}' \
  --url ws://localhost:8787
```

## Read Local Event Logs

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite list \
  --node node-a \
  --partition tenant:jp
```

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite list \
  --node node-b \
  --partition tenant:jp
```

## Offline Write And Pull Catch-Up

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite write \
  --node node-a \
  --partition tenant:jp \
  --payload '{"title":"offline event"}' \
  --offline
```

With the hub running and `node-a` reachable, another node can pull missing events:

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite catchup \
  --node node-c \
  --peer node-a \
  --partition tenant:jp \
  --url ws://localhost:8787
```

The CLI stores PGlite data under `./.data/<node-id>` by default.

## Inspect PGlite Data Directory

Phase 7 starts by treating the PGlite data directory as the snapshot boundary. The inventory command lists every regular file with size and sha256.

```bash
mise exec -- pnpm --filter @distrb-pglite/node exec distrb-pglite inventory \
  --node node-a
```

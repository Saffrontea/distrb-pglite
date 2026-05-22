import { Zstd } from "@hpcc-js/wasm-zstd";

let pending: Promise<Zstd> | undefined;

export function loadZstd(): Promise<Zstd> {
  pending ??= Zstd.load();
  return pending;
}

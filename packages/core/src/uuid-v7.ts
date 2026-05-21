import type { EventId } from "./types.js";

export type RandomBytes = (length: number) => Uint8Array;
export type Now = () => number;

const UUID_V7_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export class UuidV7Factory {
  private lastTimestamp = 0;
  private sequence = 0;

  constructor(
    private readonly now: Now = () => Date.now(),
    private readonly randomBytes: RandomBytes = defaultRandomBytes,
  ) {}

  next(): EventId {
    const timestamp = Math.max(Math.trunc(this.now()), this.lastTimestamp);
    if (timestamp === this.lastTimestamp) {
      this.sequence = (this.sequence + 1) & 0x0fff;
    } else {
      this.sequence = this.randomBytes(2)[1] & 0x0fff;
      this.lastTimestamp = timestamp;
    }

    const bytes = this.randomBytes(16);
    bytes[0] = (timestamp / 0x10000000000) & 0xff;
    bytes[1] = (timestamp / 0x100000000) & 0xff;
    bytes[2] = (timestamp / 0x1000000) & 0xff;
    bytes[3] = (timestamp / 0x10000) & 0xff;
    bytes[4] = (timestamp / 0x100) & 0xff;
    bytes[5] = timestamp & 0xff;
    bytes[6] = 0x70 | ((this.sequence >> 8) & 0x0f);
    bytes[7] = this.sequence & 0xff;
    bytes[8] = 0x80 | (bytes[8] & 0x3f);

    return formatUuid(bytes);
  }
}

export function isUuidV7(value: string): boolean {
  return UUID_V7_PATTERN.test(value);
}

export function compareEventIds(left: EventId, right: EventId): number {
  return left.localeCompare(right);
}

function defaultRandomBytes(length: number): Uint8Array {
  const crypto = globalThis.crypto;
  if (!crypto?.getRandomValues) {
    throw new Error("globalThis.crypto.getRandomValues is required");
  }

  return crypto.getRandomValues(new Uint8Array(length));
}

function formatUuid(bytes: Uint8Array): string {
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0"));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join(""),
  ].join("-");
}


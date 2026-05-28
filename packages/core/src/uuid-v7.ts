import type { EventId } from "./types.js";
import { v7 as uuidV7FromLibrary } from "uuid";

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
    let timestamp = Math.max(Math.trunc(this.now()), this.lastTimestamp);
    if (timestamp === this.lastTimestamp) {
      if (this.sequence === 0xffff_ffff) {
        timestamp = this.lastTimestamp + 1;
        this.sequence = 0;
        this.lastTimestamp = timestamp;
      } else {
        this.sequence += 1;
      }
    } else {
      this.sequence = randomSequence(this.randomBytes(4));
      this.lastTimestamp = timestamp;
    }

    return uuidV7FromLibrary({
      msecs: timestamp,
      random: this.randomBytes(16),
      seq: this.sequence,
    });
  }
}

export function isUuidV7(value: string): boolean {
  return UUID_V7_PATTERN.test(value);
}

export function compareEventIds(left: EventId, right: EventId): number {
  return left.localeCompare(right);
}

const defaultUuidV7Factory = new UuidV7Factory();

export function uuidV7(): EventId {
  return defaultUuidV7Factory.next();
}

function defaultRandomBytes(length: number): Uint8Array {
  const crypto = globalThis.crypto;
  if (!crypto?.getRandomValues) {
    throw new Error("globalThis.crypto.getRandomValues is required");
  }

  return crypto.getRandomValues(new Uint8Array(length));
}

function randomSequence(bytes: Uint8Array): number {
  if (bytes.length < 4) {
    throw new Error("randomBytes must return at least 4 bytes");
  }
  return (
    (bytes[0]! * 0x1000000)
    + (bytes[1]! << 16)
    + (bytes[2]! << 8)
    + bytes[3]!
  ) >>> 0;
}

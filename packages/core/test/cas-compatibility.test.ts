import { describe, expect, it } from "vitest";
import {
  assertCasCompatible,
  CAS_MANIFEST_FORMAT,
  isCasCompatible,
  validateCasCompatibility,
  type CasCompatibility,
} from "../src/index.js";

const stamp = (overrides: Partial<CasCompatibility> = {}): CasCompatibility => ({
  manifestFormat: CAS_MANIFEST_FORMAT,
  pgMajorVersion: "17",
  pgliteVersion: "0.3.16",
  ...overrides,
});

describe("CAS compatibility stamps", () => {
  it("accepts a well-formed stamp", () => {
    expect(validateCasCompatibility(stamp())).toEqual(stamp());
    expect(validateCasCompatibility({ manifestFormat: 1, pgMajorVersion: "17" })).toEqual({
      manifestFormat: 1,
      pgMajorVersion: "17",
    });
  });

  it("rejects unknown manifest formats", () => {
    expect(() => validateCasCompatibility({ ...stamp(), manifestFormat: 2 })).toThrow(
      "manifestFormat is unsupported",
    );
  });

  it("rejects malformed Postgres versions", () => {
    expect(() => validateCasCompatibility(stamp({ pgMajorVersion: "seventeen" }))).toThrow(
      "pgMajorVersion",
    );
    expect(() => validateCasCompatibility(stamp({ pgMajorVersion: "" }))).toThrow("pgMajorVersion");
  });

  it("rejects empty pglite versions", () => {
    expect(() => validateCasCompatibility(stamp({ pgliteVersion: "" }))).toThrow("pgliteVersion");
  });

  it("treats unstamped manifests as incompatible", () => {
    expect(isCasCompatible(undefined, stamp())).toBe(false);
    expect(() => assertCasCompatible(undefined, stamp(), "test")).toThrow("no compatibility stamp");
  });

  it("rejects Postgres major version mismatches", () => {
    expect(isCasCompatible(stamp({ pgMajorVersion: "16" }), stamp())).toBe(false);
    expect(() => assertCasCompatible(stamp({ pgMajorVersion: "16" }), stamp(), "test")).toThrow(
      "compatibility mismatch",
    );
  });

  it("rejects pglite version mismatches only when both sides know their version", () => {
    expect(isCasCompatible(stamp({ pgliteVersion: "0.3.15" }), stamp())).toBe(false);
    expect(isCasCompatible(stamp({ pgliteVersion: undefined }), stamp())).toBe(true);
    expect(isCasCompatible(stamp(), stamp({ pgliteVersion: undefined }))).toBe(true);
  });

  it("accepts matching stamps", () => {
    expect(isCasCompatible(stamp(), stamp())).toBe(true);
    expect(() => assertCasCompatible(stamp(), stamp(), "test")).not.toThrow();
  });
});

import { generateKsuid } from "../src/utils/ksuid";

describe("generateKsuid", () => {
  it("produces a 26-character string", () => {
    expect(generateKsuid()).toHaveLength(26);
  });

  it("uses only Crockford Base32 characters (uppercase, no I/L/O/U)", () => {
    const VALID = /^[0-9A-HJKMNP-TV-Z]{26}$/;
    for (let i = 0; i < 100; i++) {
      expect(generateKsuid()).toMatch(VALID);
    }
  });

  it("generates unique IDs", () => {
    const ids = new Set(Array.from({ length: 1000 }, () => generateKsuid()));
    expect(ids.size).toBe(1000);
  });

  it("is lexicographically sortable by creation time", () => {
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      ids.push(generateKsuid());
    }
    const sorted = [...ids].sort();
    expect(sorted).toEqual(ids);
  });

  it("is monotonic within the same millisecond", () => {
    // Capture a burst of IDs that are very likely to land in the same ms
    // (generating 50 in a tight loop virtually guarantees same-ms collisions)
    const ids: string[] = Array.from({ length: 50 }, () => generateKsuid());

    // All IDs with the same time prefix must be strictly increasing
    for (let i = 1; i < ids.length; i++) {
      if (ids[i - 1]!.slice(0, 10) === ids[i]!.slice(0, 10)) {
        // Same millisecond — random suffix must be strictly greater
        expect(ids[i]! > ids[i - 1]!).toBe(true);
      }
    }
  });
});

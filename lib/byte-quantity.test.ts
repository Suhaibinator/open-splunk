import assert from "node:assert/strict";
import test from "node:test";

import { describeByteQuantity, formatByteQuantity, parseByteQuantity } from "./byte-quantity";

const KIB = 1n << 10n;
const MIB = 1n << 20n;
const GIB = 1n << 30n;
const TIB = 1n << 40n;

test("a bare number is bytes, which is the one reading a unitless field can have", () => {
  // The defect this module replaces was a field whose bare number meant
  // mebibytes on one admin page and bytes on the next. There is one answer now.
  assert.equal(parseByteQuantity("0"), 0n);
  assert.equal(parseByteQuantity("1048576"), MIB);
  assert.equal(parseByteQuantity("  65536  "), 65_536n);
});

test("the suffix table is the collector configuration's, decimal and binary alike", () => {
  // `byteSizeUnit` in internal/collector/config/config.go, which
  // docs/collector-configuration.md documents for `max_event_bytes` and
  // `state.max_queue_bytes`. A console that read `MB` as 1,048,576 would be a
  // second dialect for the same kind of limit in the same product.
  const decimal = [["KB", 1_000n], ["MB", 1_000_000n], ["GB", 1_000_000_000n],
    ["TB", 1_000_000_000_000n], ["PB", 1_000_000_000_000_000n]] as const;
  const binary = [["KiB", 1n << 10n], ["MiB", 1n << 20n], ["GiB", 1n << 30n],
    ["TiB", 1n << 40n], ["PiB", 1n << 50n]] as const;
  for (const [suffix, multiplier] of [...decimal, ...binary]) {
    assert.equal(parseByteQuantity(`3${suffix}`), 3n * multiplier, suffix);
    assert.equal(parseByteQuantity(`3 ${suffix.toLowerCase()}`), 3n * multiplier, suffix);
  }
  for (const spelling of ["", "B", " b"]) {
    assert.equal(parseByteQuantity(`7${spelling}`), 7n, spelling);
  }
});

test("a decimal suffix is not the binary one, which is why the field echoes what it read", () => {
  assert.equal(parseByteQuantity("64MB"), 64_000_000n);
  assert.equal(parseByteQuantity("64MiB"), 67_108_864n);
  assert.equal(describeByteQuantity(parseByteQuantity("64MB")!), "64,000,000 bytes");
  assert.equal(describeByteQuantity(parseByteQuantity("64MiB")!), "67,108,864 bytes");
});

test("a suffix the collector would not take is not taken here either", () => {
  // `parseByteSize` has no bare-initial spellings, so neither does this.
  for (const candidate of ["3k", "3m", "3g", "3t", "3 byte", "3 bytes", "3kbs", "3 mib b"]) {
    assert.equal(parseByteQuantity(candidate), null, candidate);
  }
});

test("a fraction is accepted only when it lands on a whole byte", () => {
  assert.equal(parseByteQuantity("1.5 GiB"), GIB + (GIB / 2n));
  assert.equal(parseByteQuantity("0.25MiB"), MIB / 4n);
  assert.equal(parseByteQuantity("1.0001 KiB"), null);
  assert.equal(parseByteQuantity("0.5 B"), null);
  assert.equal(parseByteQuantity("0.5"), null);
});

test("anything that is not a quantity is rejected rather than coerced", () => {
  for (const candidate of ["", "   ", "-1", "1e6", "1.", ".5", "12 MiBs", "NaN", "1,048,576"]) {
    assert.equal(parseByteQuantity(candidate), null, candidate);
  }
  // A leading `+` is the one sign the collector's parser trims, so it is the one
  // sign this accepts.
  assert.equal(parseByteQuantity("+1 MiB"), 1n << 20n);
});

test("range is the field's business, not the notation's", () => {
  // Nothing here knows a minimum or a maximum: a limit that is legal for the
  // byte-rate ceiling is far past what a per-event ceiling accepts, and a parser
  // that guessed would have to be told which field it was reading.
  assert.equal(parseByteQuantity("1024 TiB"), 1024n * TIB);
});

test("formatting picks the largest unit that divides the value exactly", () => {
  assert.equal(formatByteQuantity(TIB), "1 TiB");
  assert.equal(formatByteQuantity(64n * GIB), "64 GiB");
  assert.equal(formatByteQuantity(512n * MIB), "512 MiB");
  assert.equal(formatByteQuantity(64n * KIB), "64 KiB");
  assert.equal(formatByteQuantity(1023n), "1023 B");
  assert.equal(formatByteQuantity(0n), "0 B");
  assert.equal(formatByteQuantity(1n), "1 B");
  // Formatting only ever writes the binary ladder, which is what the rest of the
  // console displays; the decimal suffixes exist for reading, not for writing.
  assert.equal(formatByteQuantity(1_000_000n), "1000000 B");
});

test("a value that sits on no unit is shown in bytes rather than rounded onto one", () => {
  // This is what makes the round trip lossless, and it is why the search-limits
  // form no longer carries a second copy of the server's value to restore the
  // precision a MiB division threw away.
  assert.equal(formatByteQuantity(MIB + 1n), "1048577 B");
  assert.equal(formatByteQuantity(GIB - 1n), "1073741823 B");
});

test("every formatted quantity parses back to the value it was written from", () => {
  const values = [
    0n, 1n, 1023n, KIB, KIB + 1n, MIB, MIB + 17n, GIB, 64n * GIB, TIB, 1024n * TIB,
    1n << 63n, (1n << 64n) - 1n,
  ];
  for (const value of values) {
    assert.equal(parseByteQuantity(formatByteQuantity(value)), value, value.toString());
  }
});

test("the echo groups digits without Intl, which cannot hold these limits", () => {
  // The byte-rate ceiling is a tebibyte per second, well past
  // Number.MAX_SAFE_INTEGER, so a formatter that took a number would print a
  // rounded count beside the exact value it claims to explain.
  assert.equal(describeByteQuantity(0n), "0 bytes");
  assert.equal(describeByteQuantity(1n), "1 byte");
  assert.equal(describeByteQuantity(999n), "999 bytes");
  assert.equal(describeByteQuantity(MIB), "1,048,576 bytes");
  assert.equal(describeByteQuantity(TIB), "1,099,511,627,776 bytes");
  assert.equal(describeByteQuantity((1n << 64n) - 1n), "18,446,744,073,709,551,615 bytes");
});

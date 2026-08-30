/**
 * The one way the product reads and writes a byte quantity in a form field.
 *
 * The admin console used to hold two opposite conventions at once. Search
 * limits labelled four fields "…bytes" and then divided the protobuf value by a
 * mebibyte before showing it, so "Retained bytes per job" meant mebibytes and
 * only the grey hint underneath said so. The index and token policy fields did
 * the reverse: they took a raw byte count while their hints spoke of "1 MiB" and
 * "1 TiB/s", so setting the documented maximum meant typing 1048576 by hand. One
 * page therefore read 64 as 67108864 and the next read it as 64, under labels
 * that were spelled the same way.
 *
 * Neither half was fixable on its own, because the two shapes want different
 * things: a per-query memory ceiling is naturally stated in gibibytes, and a
 * per-event ceiling of 1 MiB has to be able to say 512 KiB. So the unit moved
 * into the value. A field holds a quantity -- `512 MiB`, `1.5 GiB`, `65536` --
 * `parseByteQuantity` is the only thing that reads one, and `formatByteQuantity`
 * is the only thing that writes one. The unit is visible in the field it applies
 * to, and no label has to promise a scale the parser does not enforce.
 *
 * Two properties make that safe to put in front of an administrator:
 *
 * 1. The round trip is lossless. `formatByteQuantity` picks a unit only when it
 *    divides the value exactly, so a limit the server reports as 67108865 comes
 *    back as `67108865 bytes` rather than as a rounded `64 MiB` that would write
 *    a different number back on the next save. Before this module, search limits
 *    needed a whole second copy of the server's value -- `exactBase` -- carried
 *    through the form purely to restore the precision the MiB division threw
 *    away.
 * 2. The notation is the one the product already had. The collector's
 *    configuration file has taken a `byte size` since before this module
 *    existed -- `max_event_bytes: 1MiB`, `state.max_queue_bytes: 1GiB` -- parsed
 *    by `parseByteSize` in internal/collector/config/config.go and documented in
 *    docs/collector-configuration.md. These fields accept exactly that: the same
 *    suffixes, the same decimal-versus-binary split (`MB` is 1,000,000 and `MiB`
 *    is 1,048,576, which is what those spellings mean), and the same refusal of a
 *    fraction that does not land on a whole byte. Inventing a second dialect for
 *    the console would have replaced one unit ambiguity with another, across the
 *    two places an operator states the same kind of limit.
 *
 * Because `MB` and `MiB` are genuinely different numbers, the caller echoes
 * `describeByteQuantity` back under the field: the exact byte count the product
 * read is on screen beside the text that produced it, so a suffix is never a
 * guess about what somebody meant.
 */

/** Binary units, largest first, so formatting picks the largest that divides exactly. */
const BYTE_UNITS: readonly { label: string; multiplier: bigint }[] = [
  { label: "TiB", multiplier: 1n << 40n },
  { label: "GiB", multiplier: 1n << 30n },
  { label: "MiB", multiplier: 1n << 20n },
  { label: "KiB", multiplier: 1n << 10n },
];

/**
 * Every suffix a field accepts, lowercased, to the number of bytes it means.
 *
 * This table is `byteSizeUnit` in internal/collector/config/config.go, in
 * TypeScript. The decimal spellings are powers of a thousand and the `i`
 * spellings are powers of 1024, because that is what those suffixes mean and
 * what the collector's configuration file has always read them as. Formatting
 * only ever writes the binary ladder, which is the one the rest of the console
 * displays.
 */
const UNIT_MULTIPLIERS = new Map<string, bigint>([
  ["", 1n],
  ["b", 1n],
  ["kb", 1_000n],
  ["mb", 1_000_000n],
  ["gb", 1_000_000_000n],
  ["tb", 1_000_000_000_000n],
  ["pb", 1_000_000_000_000_000n],
  ["kib", 1n << 10n],
  ["mib", 1n << 20n],
  ["gib", 1n << 30n],
  ["tib", 1n << 40n],
  ["pib", 1n << 50n],
]);

const DIGIT_GROUPS = /\B(?=(\d{3})+(?!\d))/gu;

/**
 * A quantity, its optional fraction, and its optional unit; nothing else.
 *
 * The leading `+` is accepted because `parseByteSize` trims one, and a notation
 * shared with the collector's configuration file is only shared if it is shared
 * at the edges too.
 */
const QUANTITY = /^\+?(\d+)(?:\.(\d+))?\s*([a-z]*)$/iu;

/**
 * Reads a byte quantity, or `null` when the text is not one.
 *
 * `null` covers every rejection a field needs to report as "this is not a
 * quantity": empty text, a negative sign, an exponent, a unit nothing
 * recognises, and a fraction that does not land on a whole byte (`0.5 B`,
 * `1.0001 KiB`). Range is
 * deliberately not checked here -- a minimum and a maximum belong to the field,
 * not to the notation -- so a caller compares the returned value itself.
 */
export function parseByteQuantity(text: string): bigint | null {
  const match = QUANTITY.exec(text.trim());
  if (match === null) return null;
  const multiplier = UNIT_MULTIPLIERS.get(match[3].toLowerCase());
  if (multiplier === undefined) return null;
  const whole = BigInt(match[1]) * multiplier;
  const fraction = match[2];
  if (fraction === undefined) return whole;
  const scale = 10n ** BigInt(fraction.length);
  const scaled = BigInt(fraction) * multiplier;
  // A fraction of a unit that is not a whole number of bytes is a typo, not a
  // value to round: `1.5 KiB` is 1536 and `0.5 B` is nothing the server can hold.
  if (scaled % scale !== 0n) return null;
  return whole + scaled / scale;
}

/**
 * Writes a byte count as the shortest quantity that reads back as itself.
 *
 * The exact-division test is what makes the round trip lossless, and it is also
 * what makes the result honest: a value that is not a whole number of mebibytes
 * is shown in bytes rather than rounded into a unit it does not sit on. Only the
 * binary ladder is ever written; the decimal suffixes exist for reading.
 */
export function formatByteQuantity(bytes: bigint): string {
  for (const { label, multiplier } of BYTE_UNITS) {
    if (bytes >= multiplier && bytes % multiplier === 0n) {
      return `${(bytes / multiplier).toString()} ${label}`;
    }
  }
  // `B`, not `bytes`: the round trip has to land back inside the notation, and
  // `parseByteSize` has no long spelling for the unitless case.
  return `${bytes.toString()} B`;
}

/**
 * The same count in bytes, grouped, for the echo a field prints under itself.
 *
 * This is the half of the contract that keeps a suffix from being a guess:
 * `64MB` and `64MiB` are different numbers, and whichever was typed appears
 * beside the text that produced it as 64,000,000 or 67,108,864 bytes.
 *
 * `Intl.NumberFormat` is not used because it takes a `number`, and these limits
 * run past `Number.MAX_SAFE_INTEGER` -- the byte-rate ceiling alone is a
 * tebibyte per second.
 */
export function describeByteQuantity(bytes: bigint): string {
  return `${bytes.toString().replace(DIGIT_GROUPS, ",")} ${bytes === 1n ? "byte" : "bytes"}`;
}

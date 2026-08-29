/**
 * Static audit of the token sweep: which literals survive, and where.
 *
 * Phase 2 replaced the colour, spacing, radius, type, stacking and elevation
 * literals in `app/globals.css`, the feature stylesheets -- CSS modules until
 * Phase 4 colocated them as plain CSS -- and the two TypeScript palettes with
 * semantic tokens. Nothing in the shipped test suite could see
 * whether that job is finished: `npm run test:visual` proves no layout moved
 * and, at the harness's default per-pixel tolerance, says almost nothing about
 * colour, while `npm run lint:css` reports a warning count rather than an
 * invariant. This module supplies the reading those invariants need.
 *
 * It is a plain library, not a test file, for the same reason
 * `scripts/css-inventory.mjs` is: `scripts/css-invariants.test.mjs` forbids a
 * test file from opening a stylesheet, and that rule is worth keeping. Every
 * `readFile` of a `.css` path in the sweep lives here.
 *
 * The parsing is borrowed from `css-inventory.mjs` rather than rewritten --
 * `cssBlocks` and `cssDeclarations` already survive nested at-rules, comments
 * and declaration values full of punctuation -- so this file only adds the
 * value-level analysis the sweep needs: what a literal is, what kind of value
 * a token holds, and which of the two a property is allowed to carry.
 */
import { readFile } from "node:fs/promises";
import path from "node:path";

import {
  cssBlocks,
  cssDeclarations,
  isDarkThemeContext,
  listRepositoryFiles,
  listStylesheets,
  listTokenStylesheets,
  relativePosix,
} from "./css-inventory.mjs";

/** Sources that can carry a colour into the DOM through an inline style. */
const TYPESCRIPT_EXTENSIONS = new Set([".ts", ".tsx"]);

/**
 * A colour written as a literal rather than as a role.
 *
 * Hex covers `#rgb` through `#rrggbbaa`; the function list covers every colour
 * function CSS has, because "we only ever write `rgb()`" is a convention and
 * this is meant to be a rule. The lookbehind keeps `--purple-500` and
 * `translate(...)` out: a colour function is a bare word, never the tail of an
 * identifier.
 */
const COLOUR_LITERAL = /#[0-9a-fA-F]{3,8}\b|(?<![\w-])(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\(/u;

/**
 * CSS's named colours.
 *
 * A named colour is a literal exactly as much as a hex is, and it is the one
 * shape a search for `#` or `rgb(` misses entirely. `transparent` and
 * `currentcolor` are deliberately absent: neither names a colour, they name
 * "no paint" and "whatever the ink already is", and both survive a retheme.
 */
const NAMED_COLOURS = new Set([
  "aliceblue", "antiquewhite", "aqua", "aquamarine", "azure", "beige", "bisque", "black",
  "blanchedalmond", "blue", "blueviolet", "brown", "burlywood", "cadetblue", "chartreuse",
  "chocolate", "coral", "cornflowerblue", "cornsilk", "crimson", "cyan", "darkblue", "darkcyan",
  "darkgoldenrod", "darkgray", "darkgreen", "darkgrey", "darkkhaki", "darkmagenta", "darkolivegreen",
  "darkorange", "darkorchid", "darkred", "darksalmon", "darkseagreen", "darkslateblue",
  "darkslategray", "darkslategrey", "darkturquoise", "darkviolet", "deeppink", "deepskyblue",
  "dimgray", "dimgrey", "dodgerblue", "firebrick", "floralwhite", "forestgreen", "fuchsia",
  "gainsboro", "ghostwhite", "gold", "goldenrod", "gray", "green", "greenyellow", "grey",
  "honeydew", "hotpink", "indianred", "indigo", "ivory", "khaki", "lavender", "lavenderblush",
  "lawngreen", "lemonchiffon", "lightblue", "lightcoral", "lightcyan", "lightgoldenrodyellow",
  "lightgray", "lightgreen", "lightgrey", "lightpink", "lightsalmon", "lightseagreen",
  "lightskyblue", "lightslategray", "lightslategrey", "lightsteelblue", "lightyellow", "lime",
  "limegreen", "linen", "magenta", "maroon", "mediumaquamarine", "mediumblue", "mediumorchid",
  "mediumpurple", "mediumseagreen", "mediumslateblue", "mediumspringgreen", "mediumturquoise",
  "mediumvioletred", "midnightblue", "mintcream", "mistyrose", "moccasin", "navajowhite", "navy",
  "oldlace", "olive", "olivedrab", "orange", "orangered", "orchid", "palegoldenrod", "palegreen",
  "paleturquoise", "palevioletred", "papayawhip", "peachpuff", "peru", "pink", "plum", "powderblue",
  "purple", "rebeccapurple", "red", "rosybrown", "royalblue", "saddlebrown", "salmon", "sandybrown",
  "seagreen", "seashell", "sienna", "silver", "skyblue", "slateblue", "slategray", "slategrey",
  "snow", "springgreen", "steelblue", "tan", "teal", "thistle", "tomato", "turquoise", "violet",
  "wheat", "white", "whitesmoke", "yellow", "yellowgreen",
]);

/** Properties that measure a gap and are therefore on the spacing scale. */
const SPACING_PROPERTY =
  /^(?:padding|margin|gap|row-gap|column-gap)(?:-(?:top|bottom|left|right|block|inline|block-start|block-end|inline-start|inline-end))?$/u;

/** The four scale families the phase bar names explicitly, beside spacing. */
const SCALE_PROPERTY = /^(?:font-size|z-index|border-radius|box-shadow)$/u;

/**
 * Which token families a property is allowed to name.
 *
 * The key is the property; the value is the set of *kinds* -- derived from what
 * a token resolves to, not from how it is spelled -- that the property accepts.
 * Longhands are listed rather than shorthands wherever the shorthand mixes
 * kinds, because `border: 1px solid var(--x)` legitimately carries a length and
 * a colour and can prove nothing on its own. `background` is the exception that
 * has to be listed anyway: it is the only way to paint a gradient, and a
 * gradient's stops are inks drawn as a ground.
 */
const PROPERTY_KINDS = new Map(Object.entries({
  "accent-color": ["colour"],
  "animation": ["duration", "easing"],
  "animation-duration": ["duration"],
  "animation-timing-function": ["easing"],
  "background": ["colour", "length"],
  "background-color": ["colour"],
  "border": ["colour", "length"],
  "border-block": ["colour", "length"],
  "border-block-end": ["colour", "length"],
  "border-block-start": ["colour", "length"],
  "border-bottom": ["colour", "length"],
  "border-bottom-color": ["colour"],
  "border-bottom-left-radius": ["length"],
  "border-bottom-right-radius": ["length"],
  "border-color": ["colour"],
  "border-inline": ["colour", "length"],
  "border-inline-end": ["colour", "length"],
  "border-inline-start": ["colour", "length"],
  "border-left": ["colour", "length"],
  "border-left-color": ["colour"],
  "border-radius": ["length"],
  "border-right": ["colour", "length"],
  "border-right-color": ["colour"],
  "border-top": ["colour", "length"],
  "border-top-color": ["colour"],
  "border-top-left-radius": ["length"],
  "border-top-right-radius": ["length"],
  "box-shadow": ["colour", "length", "shadow"],
  "caret-color": ["colour"],
  "color": ["colour"],
  "column-gap": ["length"],
  "column-rule-color": ["colour"],
  "fill": ["colour"],
  "font-family": ["font-stack"],
  "font-size": ["length"],
  "gap": ["length"],
  "margin": ["length"],
  "margin-bottom": ["length"],
  "margin-left": ["length"],
  "margin-right": ["length"],
  "margin-top": ["length"],
  "outline": ["colour", "length"],
  "outline-color": ["colour"],
  "padding": ["length"],
  "padding-block": ["length"],
  "padding-bottom": ["length"],
  "padding-inline": ["length"],
  "padding-left": ["length"],
  "padding-right": ["length"],
  "padding-top": ["length"],
  "row-gap": ["length"],
  "scrollbar-color": ["colour"],
  "stroke": ["colour"],
  "text-decoration": ["colour", "length"],
  "text-decoration-color": ["colour"],
  "transition": ["duration", "easing"],
  "transition-duration": ["duration"],
  "transition-timing-function": ["easing"],
  "z-index": ["number"],
}));

/** Repository-relative paths of every stylesheet that is not the token layer. */
export async function listApplicationStylesheets(root) {
  const layer = new Set((await listTokenStylesheets(root)).map((file) => relativePosix(root, file)));
  return (await listStylesheets(root))
    .filter((file) => !layer.has(relativePosix(root, file)));
}

/**
 * Every declaration the application stylesheets make, with its context.
 *
 * One pass, reused by every collector below, so a stylesheet is read once and
 * every invariant sees exactly the same declarations. The selector is
 * whitespace-collapsed because a selector list spans lines in the source and a
 * failure message has to fit on one.
 */
export async function collectApplicationDeclarations(root) {
  const files = await listApplicationStylesheets(root);
  const perFile = await Promise.all(files.map(async (file) => {
    const css = await readFile(file, "utf8");
    return cssBlocks(css).flatMap((block) => cssDeclarations(block.body).map((declaration) => ({
      ancestors: block.ancestors,
      file: relativePosix(root, file),
      property: declaration.property,
      selector: block.prelude.replaceAll(/\s+/gu, " "),
      value: declaration.value,
    })));
  }));
  return perFile.flat();
}

/** Splits a declaration value into its top-level, comma- and space-free parts. */
export function valueComponents(value) {
  const parts = [];
  let buffer = "";
  let depth = 0;
  for (const character of value) {
    if (character === "(") depth += 1;
    if (character === ")") depth = Math.max(0, depth - 1);
    if (depth === 0 && /[\s,]/u.test(character)) {
      if (buffer.length > 0) parts.push(buffer);
      buffer = "";
      continue;
    }
    buffer += character;
  }
  if (buffer.length > 0) parts.push(buffer);
  return parts;
}

/** A declaration value with any `!important` flag removed. */
export function withoutImportant(value) {
  return value.replace(/\s*!\s*important\s*$/iu, "").trim();
}

/**
 * True when a value paints a colour the token layer did not name.
 *
 * A named colour only counts in a component of its own: `white-space: nowrap`
 * and `linear-gradient(…)` both contain colour words inside longer
 * identifiers, and neither paints anything.
 */
export function hasColourLiteral(value) {
  if (COLOUR_LITERAL.test(value)) return true;
  return valueComponents(value).some((part) => NAMED_COLOURS.has(part.toLowerCase()));
}

/**
 * The light-theme value of every custom property the token layer declares.
 *
 * Dark blocks are skipped: a theme restates tier 2, and the question every
 * caller here asks -- "does this literal already have a name?" -- is asked
 * against the theme the product actually ships. `var()` chains are followed to
 * the primitive, so a tier-2 token and the literal it replaced are directly
 * comparable.
 */
export async function collectTokenValues(root) {
  const files = await listTokenStylesheets(root);
  const sources = await Promise.all(files.map((file) => readFile(file, "utf8")));
  const declared = new Map();
  for (const css of sources) {
    for (const block of cssBlocks(css)) {
      if (block.prelude.startsWith("@") || isDarkThemeContext(block)) continue;
      for (const { property, value } of cssDeclarations(block.body)) {
        if (property.startsWith("--")) declared.set(property, value);
      }
    }
  }
  const resolved = new Map();
  function resolve(name, seen) {
    if (resolved.has(name)) return resolved.get(name);
    if (seen.has(name)) return declared.get(name);
    seen.add(name);
    const value = (declared.get(name) ?? "").replaceAll(
      /var\(\s*(--[\w-]+)\s*\)/gu,
      (whole, reference) => (declared.has(reference) ? resolve(reference, seen) : whole),
    );
    resolved.set(name, value);
    return value;
  }
  for (const name of declared.keys()) resolve(name, new Set());
  return { declared, resolved };
}

/** `#abc` and `#AABBCC` written the one way, so two spellings compare equal. */
export function normaliseHex(hex) {
  const body = hex.slice(1).toLowerCase();
  if (body.length === 3) return `#${body[0]}${body[0]}${body[1]}${body[1]}${body[2]}${body[2]}`;
  if (body.length === 4) {
    return `#${body[0]}${body[0]}${body[1]}${body[1]}${body[2]}${body[2]}${body[3]}${body[3]}`;
  }
  return `#${body}`;
}

/**
 * What kind of value a token holds, read off the value rather than the name.
 *
 * Deriving the kind from the value is what makes the property/token pairing
 * check survive a rename: `--type-md` is a length because it resolves to
 * `12px`, not because it is spelled `--type-`. The shadow case is the one that
 * needs structure as well as shape -- an elevation is several lengths and a
 * colour -- so it is recognised last, as "more than one component, at least one
 * of which paints".
 */
export function tokenKind(value) {
  const text = value.trim();
  if (/^#[0-9a-fA-F]{3,8}$/u.test(text)) return "colour";
  if (/^(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\([^()]*\)$/u.test(text)) return "colour";
  if (NAMED_COLOURS.has(text.toLowerCase())) return "colour";
  if (/^-?\d+(?:\.\d+)?(?:px|rem|em|ch|vh|vw|%)$/u.test(text)) return "length";
  if (/^-?\d+(?:\.\d+)?$/u.test(text)) return "number";
  if (/^\d+(?:\.\d+)?m?s$/u.test(text)) return "duration";
  if (/^(?:linear|ease|ease-in|ease-out|ease-in-out|steps\(.*\)|cubic-bezier\(.*\))$/u.test(text)) {
    return "easing";
  }
  if (/(?:sans-serif|serif|monospace|system-ui|cursive|fantasy)$/u.test(text)) return "font-stack";
  if (valueComponents(text).length > 1 && COLOUR_LITERAL.test(text)) return "shadow";
  return "unknown";
}

/** Kinds a property accepts, or `null` when the sweep makes no claim about it. */
export function acceptedKinds(property) {
  return PROPERTY_KINDS.get(property) ?? null;
}

/**
 * Declarations outside the token layer that still paint a literal colour.
 *
 * Reported as `property: value` per file rather than per rule: two rules that
 * share a literal share one entry, because what the ledger records is a colour
 * the palette has no name for, not a line number that moves whenever anything
 * above it is edited.
 */
export async function collectColourLiterals(root) {
  const grouped = new Map();
  for (const declaration of await collectApplicationDeclarations(root)) {
    if (!hasColourLiteral(declaration.value)) continue;
    const perFile = grouped.get(declaration.file) ?? new Map();
    const perProperty = perFile.get(declaration.property) ?? new Set();
    perProperty.add(declaration.value);
    perFile.set(declaration.property, perProperty);
    grouped.set(declaration.file, perFile);
  }
  return groupedToLedger(grouped);
}

/**
 * Colours hidden inside a percent-encoded `data:` URI.
 *
 * `fill='%23526068'` is a hex that no search for `#` will ever find and that no
 * `var()` can reach, because the SVG is a separate document the browser parses
 * out of a URL. It is the one shape that can grow without any of the checks
 * above noticing, so it is collected on its own and pinned to the sites that
 * are known and written down.
 */
export async function collectEncodedColourLiterals(root) {
  const found = [];
  for (const declaration of await collectApplicationDeclarations(root)) {
    for (const match of declaration.value.matchAll(/%23[0-9a-fA-F]{3,8}\b/gu)) {
      found.push(`${declaration.file} | ${declaration.selector} | ${declaration.property}: ${match[0]}`);
    }
  }
  return found.toSorted();
}

/**
 * True when a value component is a measurement the scale layer could name.
 *
 * Components, not whole values, because a `box-shadow` is several lengths and
 * an ink and a `border-radius` can be four corners: recording the whole string
 * would file `0 0 0 1px var(--border-focus)` -- a ring whose only colour is
 * already a token -- as unmigrated, and the ledger would stop meaning anything.
 * Zero is excluded because it measures nothing and is the same length on every
 * scale; a `var()` is excluded because it is the migrated case; a colour is
 * excluded because the colour ledger already carries it with its full context.
 */
export function isScaleLiteral(component) {
  if (component.startsWith("var(")) return false;
  if (hasColourLiteral(component)) return false;
  if (/^0(?:px|rem|em|%)?$/u.test(component)) return false;
  return /\d/u.test(component);
}

/**
 * The measurements outside the token layer that are still written as numbers.
 *
 * Recorded per file and property as the distinct components, so
 * `font-size: 13px` in twelve rules is one row and the ledger reads as "these
 * numbers are still hard-coded here" rather than as a transcript.
 */
export async function collectScaleLiterals(root) {
  const grouped = new Map();
  for (const declaration of await collectApplicationDeclarations(root)) {
    if (!SCALE_PROPERTY.test(declaration.property)) continue;
    const literals = valueComponents(withoutImportant(declaration.value)).filter(isScaleLiteral);
    if (literals.length === 0) continue;
    const perFile = grouped.get(declaration.file) ?? new Map();
    const perProperty = perFile.get(declaration.property) ?? new Set();
    for (const literal of literals) perProperty.add(literal);
    perFile.set(declaration.property, perProperty);
    grouped.set(declaration.file, perFile);
  }
  return groupedToLedger(grouped);
}

/** Turns the nested Maps the collectors build into sorted, comparable JSON. */
function groupedToLedger(grouped) {
  const ledger = {};
  for (const [file, perFile] of [...grouped].toSorted()) {
    ledger[file] = Object.fromEntries(
      [...perFile].toSorted().map(([property, values]) => [property, [...values].toSorted()]),
    );
  }
  return ledger;
}

/**
 * Literals a token already spells exactly, which are therefore pure misses.
 *
 * This is the sharpest thing the sweep can be asked, and the only one whose
 * answer needs no judgement: if a declaration writes `8px` where `--space-2`
 * resolves to `8px`, substituting the token moves nothing at all. Anything
 * further away is a decision about hue or density and belongs in the ledger.
 *
 * Two scopes, because the families differ in how a value is built. Spacing is
 * checked component by component -- `padding: 3px 8px` is two lengths and the
 * second one has a name -- while the rest are checked whole, because a length
 * inside `box-shadow` or `border-radius: 1px 1px 0 0` measures a geometry the
 * spacing scale makes no claim about.
 *
 * `z-index` is excluded by name. docs/theming.md keeps twenty-three of them
 * literal on purpose: they order an element inside a stacking context its
 * parent already opened, so `--z-base` would promise an escape they cannot
 * make, and five of those happen to be `1`.
 */
export async function collectExactTokenMisses(root) {
  const { resolved } = await collectTokenValues(root);
  /** The family a property measures on, so `width: 14px` never meets `--type-lg`. */
  const families = new Map([
    ["border-bottom-left-radius", "--radius-"],
    ["border-bottom-right-radius", "--radius-"],
    ["border-radius", "--radius-"],
    ["border-top-left-radius", "--radius-"],
    ["border-top-right-radius", "--radius-"],
    ["box-shadow", "--shadow-"],
    ["font-size", "--type-"],
  ]);
  const steps = new Map();
  for (const [name, value] of resolved) {
    const prefix = /^(--[a-z]+-)/u.exec(name)?.[1];
    if (prefix === undefined) continue;
    const perFamily = steps.get(prefix) ?? new Map();
    perFamily.set(value.trim(), name);
    steps.set(prefix, perFamily);
  }
  const spacingSteps = steps.get("--space-") ?? new Map();
  const misses = [];
  for (const declaration of await collectApplicationDeclarations(root)) {
    const { file, property, selector, value } = declaration;
    const bare = withoutImportant(value);
    if (SPACING_PROPERTY.test(property)) {
      // One entry per length, not per position: `padding: 8px 10px 8px 13px`
      // is one substitution to make, written twice.
      for (const component of new Set(valueComponents(bare))) {
        const token = spacingSteps.get(component);
        if (token !== undefined) {
          misses.push({ file, property, selector, token, value, wrote: component });
        }
      }
      continue;
    }
    const family = families.get(property);
    if (family === undefined) continue;
    const token = (steps.get(family) ?? new Map()).get(bare);
    if (token !== undefined) misses.push({ file, property, selector, token, value, wrote: bare });
  }
  return misses.toSorted((left, right) => (
    `${left.file}${left.selector}${left.property}`.localeCompare(`${right.file}${right.selector}${right.property}`)
  ));
}

/**
 * Declarations that name a token of a kind the property cannot carry.
 *
 * The sweep's stated method was to substitute per property role -- a border
 * literal may only become a border token, a background only a ground. Nothing
 * enforced the weaker half of that claim: that a `font-size` never reads a
 * colour and a `color` never reads a step. A mistake of that shape does not
 * render as a wrong colour, it renders as no rule at all, which no screenshot
 * of a page full of text reliably shows.
 */
export async function collectPropertyKindMismatches(root) {
  const { resolved } = await collectTokenValues(root);
  const mismatches = [];
  for (const declaration of await collectApplicationDeclarations(root)) {
    const accepted = acceptedKinds(declaration.property);
    if (accepted === null) continue;
    for (const match of declaration.value.matchAll(/var\(\s*(--[\w-]+)/gu)) {
      const value = resolved.get(match[1]);
      if (value === undefined) continue;
      const kind = tokenKind(value);
      if (kind === "unknown" || accepted.includes(kind)) continue;
      mismatches.push(
        `${declaration.file} | ${declaration.selector} | ${declaration.property}: ${match[1]} `
        + `is a ${kind} (${value.trim()}), and ${declaration.property} accepts ${accepted.join(" or ")}`,
      );
    }
  }
  return mismatches.toSorted();
}

/** Every TypeScript module the application ships, tests excluded. */
async function listApplicationTypeScript(root) {
  return (await listRepositoryFiles(root)).filter((file) => (
    TYPESCRIPT_EXTENSIONS.has(path.extname(file)) && !/\.(?:test|spec)\.tsx?$/u.test(file)
  ));
}

/**
 * Colour literals TypeScript carries, with the comments stripped.
 *
 * A hex in a component is worse than a hex in a stylesheet: it reaches the DOM
 * through an inline `style`, where it outranks every rule and no theme can
 * touch it. Comments are removed first so prose about a colour -- which the
 * visual harness has, explaining why two blues compare equal -- is not read as
 * one.
 */
export async function collectTypeScriptColourLiterals(root) {
  const files = await listApplicationTypeScript(root);
  const perFile = await Promise.all(files.map(async (file) => {
    const source = (await readFile(file, "utf8"))
      .replaceAll(/\/\*[\s\S]*?\*\//gu, " ")
      .replaceAll(/(^|[^:])\/\/[^\n]*/gu, "$1");
    const found = new Set();
    for (const match of source.matchAll(/#[0-9a-fA-F]{3,8}\b/gu)) found.add(match[0]);
    for (const match of source.matchAll(/(?<![\w-])(?:rgba?|hsla?)\([^)]*\)/gu)) found.add(match[0]);
    return [...found].toSorted().map((literal) => `${relativePosix(root, file)}: ${literal}`);
  }));
  return perFile.flat().toSorted();
}

/** Every `var(--token)` TypeScript reads, with the file that reads it. */
export async function collectTypeScriptTokenReferences(root) {
  const files = await listApplicationTypeScript(root);
  const perFile = await Promise.all(files.map(async (file) => {
    const source = await readFile(file, "utf8");
    return [...source.matchAll(/var\(\s*(--[\w-]+)\s*\)/gu)]
      .map((match) => ({ file: relativePosix(root, file), name: match[1] }));
  }));
  return perFile.flat();
}

/**
 * The token each event severity is painted with, read from both languages.
 *
 * The legend swatch is a CSS class and the chart swatch is an inline style
 * built in TypeScript, so the same four levels are spelled twice in two
 * different files. Nothing but this comparison makes them agree: a legend that
 * disagrees with its own chart is a wrong colour, not a missing one, and the
 * page still renders.
 */
export async function collectSeverityPalette(root) {
  const stylesheet = {};
  for (const declaration of await collectApplicationDeclarations(root)) {
    if (declaration.property !== "background" && declaration.property !== "background-color") continue;
    const token = /^var\(\s*(--[\w-]+)\s*\)$/u.exec(declaration.value);
    if (token === null) continue;
    for (const match of declaration.selector.matchAll(/\.(?:severity|legend|level)-(info|warn|error|debug)\b/gu)) {
      const seen = stylesheet[match[1]] ?? new Set();
      seen.add(token[1]);
      stylesheet[match[1]] = seen;
    }
  }
  const panel = path.join(root, "app", "search-workspace", "panels", "visualization-panel.tsx");
  const source = await readFile(panel, "utf8");
  const body = /function categoryColor\([\s\S]*?\n\}/u.exec(source);
  const typescript = {};
  for (const match of (body?.[0] ?? "").matchAll(/(\w+):\s*"var\(\s*(--[\w-]+)\s*\)"/gu)) {
    typescript[match[1]] = match[2];
  }
  return {
    stylesheet: Object.fromEntries(
      Object.entries(stylesheet).toSorted().map(([level, names]) => [level, [...names].toSorted()]),
    ),
    typescript,
  };
}

/**
 * The categorical ramp as `time-series-line-chart.tsx` declares it.
 *
 * Returned as written, in order, so a caller can insist it is the chart-series
 * family and nothing else: a single stray hex in that array reaches every chart
 * in the product through an inline style.
 */
export async function collectSeriesPalette(root) {
  const chart = path.join(root, "app", "search-workspace", "charts", "time-series-line-chart.tsx");
  const source = await readFile(chart, "utf8");
  const array = /export const TIME_SERIES_COLORS = \[([\s\S]*?)\]/u.exec(source);
  const entries = [...(array?.[1] ?? "").matchAll(/"([^"]*)"/gu)].map((match) => match[1]);
  const panel = path.join(root, "app", "search-workspace", "panels", "visualization-panel.tsx");
  const panelSource = await readFile(panel, "utf8");
  const slice = /const CATEGORY_COLORS = TIME_SERIES_COLORS\.slice\(0,\s*(\d+)\)/u.exec(panelSource);
  return { categoryCount: slice === null ? null : Number(slice[1]), series: entries };
}

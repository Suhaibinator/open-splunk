// Semantic invariants for the two-tier token layer in `app/styles`.
//
// `token-layer.test.mjs` asserts the layer's *structure*: one declaration site
// per name, no literal inside a semantic token, no dark-only name. Those hold
// perfectly well for a layer whose palette is unordered, whose dark theme is
// half dead code, and whose chrome text is black on black. This file asserts
// what the names and the values are supposed to *mean*, against the contract
// written down in `docs/theming.md`:
//
//   - every name parses under the documented naming grammar, and the frozen
//     legacy block never grows;
//   - a step number really does say how light a primitive is;
//   - a name family really does hold one kind of value;
//   - a theme restates only what it changes, and changes everything it must;
//   - the pairings the tokens' own comments promise stay legible;
//   - and the whole move out of `app/globals.css` resolves to the same colours
//     it resolved to before the refactor.
//
// Like every other test in this directory it never opens a stylesheet: the
// reading and parsing live in `scripts/css-inventory.mjs`.
import assert from "node:assert/strict";
import process from "node:process";
import test from "node:test";

import {
  collectDeclarationComments,
  collectTokenBlocks,
  collectTokenComments,
  collectTokenLayer,
} from "./css-inventory.mjs";

const workspace = process.cwd();

/**
 * The values `app/globals.css` shipped at 7459a0cc, the commit before the token
 * layer landed, each against the name that carries it today.
 *
 * Phase 1 moved these declarations into `app/styles` and rewrote each as a
 * chain of `var()` references; Phase 2 rewrote the call sites and deleted the
 * pre-refactor aliases, so the left-hand side is now the role rather than the
 * retired name. The contract is unchanged and is the reason the table survives
 * the deletion: if a chain resolves anywhere but here, the refactor moved a
 * pixel, and it moved it in a place no screenshot may happen to cover.
 * docs/theming.md tabulates which retired name each role replaced.
 *
 * `--orange` and `--yellow` named no role, so the primitive each resolved to is
 * pinned directly; every other row is a tier-2 token.
 */
const PRE_REFACTOR_VALUES = {
  "--accent": "#477f2b",
  "--accent-hover": "#376a20",
  "--accent-soft": "#e8f2e1",
  "--amber-500": "#d2a600",
  "--bg-canvas": "#f6f6f4",
  "--bg-inverse": "#161b1f",
  "--bg-raised": "#fbfbfa",
  "--bg-subtle": "#f2f3f3",
  "--bg-surface": "#ffffff",
  "--border": "#cfd4d7",
  "--border-strong": "#aeb6bb",
  "--chrome-appbar": "#3f464c",
  "--chrome-bar": "#1e252b",
  "--chrome-hover": "#4b535a",
  "--fg-faint": "#89949b",
  "--fg-muted": "#64717a",
  "--fg-strong": "#19252d",
  "--fg-text": "#28343d",
  "--font-mono": '"SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace',
  "--font-sans": "Arial, Helvetica, sans-serif",
  "--orange-400": "#d97a23",
  "--shadow-lg": "0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%)",
  "--status-error": "#c93c37",
  "--status-error-soft": "#fff0ee",
  "--status-info": "#2878a8",
  "--status-info-soft": "#e8f3f9",
};

/**
 * The pre-refactor alias block, now empty.
 *
 * `docs/theming.md`: "nothing may be added to that block, and each one
 * disappears as its call sites are rewritten". Phase 2 rewrote the last call
 * site, so the block is gone and the set below is the assertion that it stays
 * gone. The test it feeds still earns its runtime for the other shape it
 * catches: a tier-2 token the dark block forgot to restate looks exactly like a
 * new alias from here.
 */
const LEGACY_ALIASES = new Set([]);

/** Group prefixes the semantic tier is allowed to use, from docs/theming.md. */
const SEMANTIC_GROUPS = ["accent", "bg", "border", "chart", "chrome", "fg", "level", "status"];

/** The three interaction tokens the documented grammar leaves ungrouped. */
const INTERACTION_TOKENS = new Set(["--focus-ring", "--highlight", "--selection"]);

/** Name families `app/styles/tokens-scale.css` is allowed to use. */
const SCALE_FAMILIES = ["dur", "ease", "font", "radius", "shadow", "space", "type", "z"];

/** The ladder a primitive step number may sit on: light at 0, darkest at 950. */
const PRIMITIVE_STEPS = new Set([0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 900, 950]);

/**
 * Pairings the tokens' own role comments promise, as `[foreground, background]`.
 *
 * Only the pairings the layer commits to in writing are here: body and heading
 * text on each of the four surfaces, `--fg-inverse`, whose comment reads "Text
 * on --bg-inverse", and `--chrome-fg`, whose comment reads "Text and icons on
 * either bar" and which is a separate role precisely because the inverse
 * surface flips between themes and the two chrome bars do not. Every ground a
 * foreground token's comment names is still checked, in both themes; what
 * changed is which token owns the bars. Secondary and tertiary text
 * are left out on purpose -- `--fg-faint` is placeholder ink and its ratio is
 * inherited from the pre-refactor palette, so pinning it here would assert a
 * decision this layer did not make.
 */
const MANDATED_TEXT_PAIRS = [
  ["--fg-text", "--bg-canvas"],
  ["--fg-text", "--bg-surface"],
  ["--fg-text", "--bg-subtle"],
  ["--fg-text", "--bg-raised"],
  ["--fg-strong", "--bg-canvas"],
  ["--fg-strong", "--bg-surface"],
  ["--fg-strong", "--bg-subtle"],
  ["--fg-strong", "--bg-raised"],
  ["--fg-inverse", "--bg-inverse"],
  ["--chrome-fg", "--chrome-bar"],
  ["--chrome-fg", "--chrome-appbar"],
  ["--chrome-fg", "--chrome-hover"],
];

/** WCAG 2.2 AA for text below 18.66px, which is every size this product ships. */
const AA_CONTRAST = 4.5;

/** Role groups whose members must stay visually distinct from one another. */
const ROLE_GROUPS = {
  accent: /^--accent(?:-|$)/u,
  background: /^--bg-/u,
  border: /^--border(?:-|$)/u,
  "chart series": /^--chart-series-\d+$/u,
  chrome: /^--chrome-/u,
  foreground: /^--fg-/u,
  interaction: /^--(?:focus-ring|highlight|selection)$/u,
  "severity level": /^--level-/u,
  status: /^--status-(?!.*-soft$)/u,
  "status wash": /^--status-.*-soft$/u,
};

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

/** Reads the layer once and indexes it the way every assertion below needs. */
async function readTokenLayer() {
  const layer = await collectTokenLayer(workspace);
  const light = new Map();
  const dark = new Map();
  const fileOf = new Map();
  for (const entry of layer) {
    for (const token of entry.light) {
      light.set(token.name, token.value);
      fileOf.set(token.name, entry.file);
    }
    for (const token of entry.dark) dark.set(token.name, token.value);
  }
  const primitives = new Map([...light].filter(([, value]) => value.startsWith("#")));
  return { dark, fileOf, light, primitives };
}

/** Follows a chain of bare `var()` indirections down to the value it lands on. */
function resolve(name, scope) {
  const seen = new Set([name]);
  let value = scope.get(name);
  while (value !== undefined) {
    const reference = /^var\(\s*(--[\w-]+)\s*\)$/u.exec(value);
    if (reference === null) return value;
    if (seen.has(reference[1])) return `cycle through ${reference[1]}`;
    seen.add(reference[1]);
    value = scope.get(reference[1]);
  }
  return undefined;
}

/** sRGB channels of a six-digit hex literal. */
function channels(hex) {
  return [1, 3, 5].map((offset) => Number.parseInt(hex.slice(offset, offset + 2), 16));
}

/** WCAG relative luminance. */
function luminance(hex) {
  const [red, green, blue] = channels(hex).map((channel) => {
    const scaled = channel / 255;
    return scaled <= 0.040_45 ? scaled / 12.92 : ((scaled + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
}

/**
 * CIE L*, the perceptual lightness a reader means by "one step darker".
 *
 * Relative luminance would do for ordering a single hue, but the palette mixes
 * hues of very different saturation and L* is the axis the step numbers claim
 * to walk.
 */
function lightness(hex) {
  const y = luminance(hex);
  return y > 216 / 24_389 ? 116 * Math.cbrt(y) - 16 : (y * 24_389) / 27;
}

function contrastRatio(first, second) {
  const [darker, lighter] = [luminance(first), luminance(second)].toSorted((a, b) => a - b);
  return (lighter + 0.05) / (darker + 0.05);
}

/** The kind of value a token holds, which every name family has to agree on. */
function valueKind(value) {
  if (value === undefined) return "undefined";
  if (/^#[0-9a-f]{3,8}$/iu.test(value) || /^(?:rgba?|hsla?|oklch|color)\(/iu.test(value)) return "colour";
  if (/^-?[\d.]+(?:px|rem|em|%)$/u.test(value)) return "length";
  if (/^-?[\d.]+m?s$/u.test(value)) return "duration";
  if (/^-?\d+$/u.test(value)) return "number";
  if (/\brgba?\(/u.test(value)) return "shadow";
  return "keyword or stack";
}

/** The first `--`-delimited segment of a name, which is its family. */
function family(name) {
  return /^--([a-z0-9]+)/u.exec(name)?.[1] ?? name;
}

test("every token name parses under the documented naming grammar", async () => {
  const { light, primitives } = await readTokenLayer();
  const offenders = [];
  for (const name of light.keys()) {
    if (!/^--[a-z0-9]+(?:-[a-z0-9]+)*$/u.test(name)) {
      offenders.push(`${name} is not lowercase kebab-case`);
      continue;
    }
    if (LEGACY_ALIASES.has(name)) continue;
    if (primitives.has(name)) {
      const parsed = /^--[a-z]+-(\d+)$/u.exec(name);
      if (parsed === null) offenders.push(`${name} holds a literal but is not named --<hue>-<step>`);
      else if (!PRIMITIVE_STEPS.has(Number(parsed[1]))) {
        offenders.push(`${name} sits on step ${parsed[1]}, which is not on the 0-950 ladder`);
      }
      continue;
    }
    if (INTERACTION_TOKENS.has(name)) continue;
    if (SEMANTIC_GROUPS.some((group) => name.startsWith(`--${group}-`) || name === `--${group}`)) continue;
    if (SCALE_FAMILIES.some((scale) => name.startsWith(`--${scale}-`) || name === `--${scale}`)) continue;
    offenders.push(`${name} belongs to no documented group; docs/theming.md lists ${SEMANTIC_GROUPS.join(", ")}`);
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "A token name is outside the grammar docs/theming.md documents. Either it needs a documented\n"
      + `group prefix, or the document needs the new group written into it:\n${describeList(offenders.toSorted())}`,
  );
});

test("no semantic or scale token name mentions a hue", async () => {
  const { light, primitives } = await readTokenLayer();
  const hues = new Set([...primitives.keys()].map((name) => family(name)));
  const offenders = [];
  for (const name of light.keys()) {
    if (primitives.has(name) || LEGACY_ALIASES.has(name)) continue;
    const mentioned = name.slice(2).split("-").filter((segment) => hues.has(segment));
    if (mentioned.length > 0) offenders.push(`${name} names the hue ${mentioned.join(", ")}`);
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "docs/theming.md: \"No token name mentions a hue. --status-error, never --status-red.\" A hue in\n"
      + "the name freezes the colour into every call site and a theme can no longer move it:\n"
      + `${describeList(offenders.toSorted())}`,
  );
});

test("the frozen legacy alias block gains no new members", async () => {
  const { dark, fileOf, light, primitives } = await readTokenLayer();
  // An alias is a colour token no theme can move: it exists to keep a
  // pre-refactor call site working, so the dark block deliberately leaves it
  // alone and it follows whatever semantic token it points at. The chart ramp
  // is the one themeable token that is also unrestated, and is documented so.
  const aliases = [...light]
    .filter(([name, value]) => (
      fileOf.get(name) === "app/styles/tokens-color.css"
      && !primitives.has(name)
      && value.startsWith("var(")
      && !dark.has(name)
      && !/^--chart-series-\d+$/u.test(name)
    ))
    .map(([name]) => name);
  assert.deepEqual(
    aliases.toSorted(),
    [...LEGACY_ALIASES].toSorted(),
    "The set of pre-refactor aliases changed. The block is empty and stays empty: a new token needs\n"
      + "a documented role name, never a legacy one, and a deleted alias never comes back. A tier-2\n"
      + "token that the dark block forgot to restate also lands here -- give it a dark value and it\n"
      + "leaves the list again.",
  );
});

test("every role that replaced a pre-refactor custom property resolves to its original value", async () => {
  const { light } = await readTokenLayer();
  const drift = [];
  for (const [name, expected] of Object.entries(PRE_REFACTOR_VALUES)) {
    const actual = resolve(name, light);
    if (actual !== expected) drift.push(`${name} resolves to ${actual ?? "nothing"}, was ${expected}`);
  }
  assert.deepEqual(
    drift,
    [],
    "The move off literals is a rename, not a recolour: the role that replaced each name\n"
      + "app/globals.css declared before the token layer has to resolve to the byte that name\n"
      + "resolved to at 7459a0cc. A screenshot only covers the pixels a\n"
      + `spec happens to visit, so the whole table is checked here:\n${describeList(drift)}`,
  );
});

test("every primitive is a distinct colour", async () => {
  const { primitives } = await readTokenLayer();
  const byColour = new Map();
  for (const [name, value] of primitives) {
    const key = value.toLowerCase();
    byColour.set(key, [...(byColour.get(key) ?? []), name]);
  }
  const shared = [...byColour]
    .filter(([, names]) => names.length > 1)
    .map(([value, names]) => `${names.join(" and ")} are both ${value}`);
  assert.deepEqual(
    shared.toSorted(),
    [],
    "Two primitives hold the same literal, so the palette has a step that says nothing. Point the\n"
      + `semantic tokens at one of them and delete the other:\n${describeList(shared.toSorted())}`,
  );
});

test("a primitive family runs light to dark as its step number rises", async () => {
  const { primitives } = await readTokenLayer();
  const families = new Map();
  for (const [name, value] of primitives) {
    const parsed = /^--([a-z]+)-(\d+)$/u.exec(name);
    if (parsed === null) continue;
    families.set(parsed[1], [...(families.get(parsed[1]) ?? []), { hex: value, name, step: Number(parsed[2]) }]);
  }
  const inversions = [];
  for (const members of families.values()) {
    const ordered = members.toSorted((first, second) => first.step - second.step);
    for (let index = 1; index < ordered.length; index += 1) {
      const previous = ordered[index - 1];
      const current = ordered[index];
      if (lightness(current.hex) >= lightness(previous.hex)) {
        inversions.push(
          `${current.name} (${current.hex}, L* ${lightness(current.hex).toFixed(1)}) is not darker than `
          + `${previous.name} (${previous.hex}, L* ${lightness(previous.hex).toFixed(1)})`,
        );
      }
    }
  }
  assert.deepEqual(
    inversions.toSorted(),
    [],
    "docs/theming.md: a primitive is \"a hue family plus a lightness step, where a lower number is\n"
      + "lighter\". Where that is false the ladder lies, and \"one step darker\" during the migration\n"
      + `picks a lighter colour. Renumber the step:\n${describeList(inversions.toSorted())}`,
  );
});

test("every token in a name family holds the same kind of value", async () => {
  const { fileOf, light } = await readTokenLayer();
  const families = new Map();
  for (const name of light.keys()) {
    families.set(family(name), [...(families.get(family(name)) ?? []), name]);
  }
  const mixed = [];
  for (const [group, names] of families) {
    const kinds = new Map();
    for (const name of names) {
      const kind = valueKind(resolve(name, light));
      kinds.set(kind, [...(kinds.get(kind) ?? []), `${name} (${fileOf.get(name)})`]);
    }
    if (kinds.size === 1) continue;
    mixed.push(`--${group}-*: ${[...kinds].map(([kind, members]) => `${kind} = ${members.join(", ")}`).join("; ")}`);
  }
  assert.deepEqual(
    mixed.toSorted(),
    [],
    "One name family means two different things, so nothing about var(--family-x) tells a reader\n"
      + "whether it is a colour or a length. Rename the family that has no consumers yet -- the scale\n"
      + `tiers are still unread -- rather than the one with call sites:\n${describeList(mixed.toSorted())}`,
  );
});

test("a semantic token points at a primitive and a primitive holds a literal", async () => {
  const { dark, light, primitives } = await readTokenLayer();
  const offenders = [];
  for (const [theme, declarations] of [["light", light], ["dark", dark]]) {
    for (const [name, value] of declarations) {
      const references = [...value.matchAll(/var\(\s*(--[\w-]+)/gu)].map((match) => match[1]);
      if (primitives.has(name) && theme === "light") {
        if (references.length > 0) offenders.push(`${name} is a primitive but reads ${references.join(", ")}`);
        continue;
      }
      if (LEGACY_ALIASES.has(name)) continue;
      for (const reference of references) {
        if (!primitives.has(reference)) {
          offenders.push(`${theme} ${name} reads ${reference}, which is not a tier-1 primitive`);
        }
      }
    }
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "docs/theming.md: \"Point each token at a primitive, not at a literal.\" A semantic token that\n"
      + "reads another semantic token inherits a role it was not given, and a theme that moves one\n"
      + `moves both:\n${describeList(offenders.toSorted())}`,
  );
});

test("no two tokens in a role group resolve to the same colour in either theme", async () => {
  const { dark, light, primitives } = await readTokenLayer();
  const themed = new Map([...light, ...dark]);
  const collisions = [];
  for (const [theme, scope] of [["light", light], ["dark", themed]]) {
    for (const [group, pattern] of Object.entries(ROLE_GROUPS)) {
      const byColour = new Map();
      for (const name of light.keys()) {
        if (primitives.has(name) || LEGACY_ALIASES.has(name) || !pattern.test(name)) continue;
        const value = resolve(name, scope);
        byColour.set(value, [...(byColour.get(value) ?? []), name]);
      }
      for (const [value, names] of byColour) {
        if (names.length > 1) collisions.push(`${theme} ${group}: ${names.join(" and ")} are both ${value}`);
      }
    }
  }
  assert.deepEqual(
    collisions.toSorted(),
    [],
    "Two roles in the same group render identically, so the interface cannot tell them apart. Either\n"
      + `they are one role and want one token, or one of them needs its own step:\n${describeList(collisions.toSorted())}`,
  );
});

test("the dark theme restates nothing it does not change", async () => {
  const { dark, light } = await readTokenLayer();
  const themed = new Map([...light, ...dark]);
  const inert = [];
  for (const [name, value] of dark) {
    const after = resolve(name, themed);
    if (after === resolve(name, light)) inert.push(`${name}: ${value} resolves to ${after} in both themes`);
  }
  assert.deepEqual(
    inert.toSorted(),
    [],
    "docs/theming.md, Adding a theme, step 4: \"Restate only what changes.\" A restatement that changes\n"
      + "nothing is dead weight a future theme has to keep in step with the light default, and it hides\n"
      + `whether the equality was decided or accidental. Delete it:\n${describeList(inert.toSorted())}`,
  );
});

test("the dark theme restates every themeable semantic token", async () => {
  const { dark, light, primitives } = await readTokenLayer();
  const missing = [];
  for (const name of light.keys()) {
    if (primitives.has(name) || LEGACY_ALIASES.has(name) || dark.has(name)) continue;
    // The categorical ramp is documented as theme-independent: those twelve
    // hues separate from each other, not from the background.
    if (/^--chart-series-\d+$/u.test(name)) continue;
    if (!light.get(name).startsWith("var(")) continue;
    missing.push(name);
  }
  assert.deepEqual(
    missing.toSorted(),
    [],
    "A semantic colour token has no dark value, so it keeps its light primitive on a dark ground.\n"
      + "Either restate it in the dark block, or document it beside the chart ramp as deliberately\n"
      + `theme-independent:\n${describeList(missing.toSorted())}`,
  );
});

test("text keeps AA contrast against every ground its role comment promises", async () => {
  const { dark, light } = await readTokenLayer();
  const themed = new Map([...light, ...dark]);
  const failures = [];
  for (const [theme, scope] of [["light", light], ["dark", themed]]) {
    for (const [foreground, background] of MANDATED_TEXT_PAIRS) {
      const ink = resolve(foreground, scope);
      const ground = resolve(background, scope);
      assert.match(ink, /^#[0-9a-f]{6}$/iu, `${theme} ${foreground} is not a six-digit hex`);
      assert.match(ground, /^#[0-9a-f]{6}$/iu, `${theme} ${background} is not a six-digit hex`);
      const ratio = contrastRatio(ink, ground);
      if (ratio < AA_CONTRAST) {
        failures.push(`${theme}: ${foreground} (${ink}) on ${background} (${ground}) is ${ratio.toFixed(2)}:1`);
      }
    }
  }
  assert.deepEqual(
    failures.toSorted(),
    [],
    `Text falls below WCAG AA (${AA_CONTRAST}:1) on a ground its own role comment names. A theme that\n`
      + "ships this renders those labels unreadable, and a ratio near 1 renders them invisible:\n"
      + `${describeList(failures.toSorted())}`,
  );
});

test("every token outside the frozen legacy block states its role in one line", async () => {
  const files = await collectTokenComments(workspace);
  const undocumented = [];
  for (const { declarations, file } of files) {
    for (const { comment, name } of declarations) {
      if (LEGACY_ALIASES.has(name)) continue;
      // The scale file states each family's rationale in a banner above it,
      // which is the same promise made once for eight steps rather than eight
      // times; only the colour tiers carry a comment per token.
      if (file !== "app/styles/tokens-color.css") continue;
      if (comment === null || comment.length === 0) undocumented.push(`${file}: ${name}`);
    }
  }
  assert.deepEqual(
    undocumented.toSorted(),
    [],
    "docs/theming.md: \"Every token carries a one-line comment naming its role. A token whose role\n"
      + "cannot be stated in one line is usually two tokens.\" These have no comment on their\n"
      + `declaration line:\n${describeList(undocumented.toSorted())}`,
  );
});

test("the token layer declares tokens and nothing else", async () => {
  const files = await collectTokenBlocks(workspace);
  const offenders = [];
  for (const { blocks, file } of files) {
    for (const { ancestors, declarations, prelude } of blocks) {
      if (ancestors.length > 0) offenders.push(`${file}: ${prelude} is nested inside ${ancestors.join(" > ")}`);
      if (!/^:root(?:\[data-theme="[a-z-]+"\])?$/u.test(prelude)) {
        offenders.push(`${file}: ${prelude} is a rule, not a theme block`);
      }
      for (const { property } of declarations) {
        // `color-scheme` is the one non-token declaration a theme block owes
        // the browser: without it form controls and scrollbars keep the other
        // theme's colours.
        if (property.startsWith("--") || property === "color-scheme") continue;
        offenders.push(`${file}: ${prelude} sets ${property}`);
      }
    }
  }
  assert.deepEqual(
    offenders.toSorted(),
    [],
    "A token file grew something that is not a token. Rules belong in app/globals.css or a module;\n"
      + `keeping the layer declaration-only is what lets a theme be read at a glance:\n${describeList(offenders.toSorted())}`,
  );
});

test("every theme block sets color-scheme", async () => {
  const files = await collectTokenBlocks(workspace);
  const missing = [];
  for (const { blocks, file } of files) {
    // Only a file that defines colours has a theme to declare. The scale tier
    // is shared by every theme, so a `color-scheme` there would be a claim it
    // is not entitled to make.
    const paintsColour = blocks.some(({ declarations }) => (
      declarations.some(({ value }) => /^#[0-9a-f]{3,8}$/iu.test(value))
    ));
    if (!paintsColour) continue;
    for (const { declarations, prelude } of blocks) {
      if (!declarations.some(({ property }) => property.startsWith("--"))) continue;
      if (!declarations.some(({ property }) => property === "color-scheme")) {
        missing.push(`${file}: ${prelude}`);
      }
    }
  }
  assert.deepEqual(
    missing.toSorted(),
    [],
    "A theme block declares colours but no color-scheme, so the browser keeps painting scrollbars,\n"
      + `form controls and the canvas behind the page in the other theme:\n${describeList(missing.toSorted())}`,
  );
});

// The invariants above are only worth their runtime if the parsing underneath
// them can see a violation, so the one parser this file adds is pinned against
// the shapes the token files actually use.

test("collectDeclarationComments pairs a comment with the declaration it trails", () => {
  const found = collectDeclarationComments([
    ":root {",
    "  --gray-0: #ffffff; /* Paper white. */",
    "  --bare: #000000;",
    "  --shadow-lg: 0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%); /* Deep lift. */",
    "  /* A banner above the next token, which is not that token's own comment. */",
    "  --after-banner: 1px;",
    "}",
  ].join("\n"));
  assert.deepEqual(found, [
    { comment: "Paper white.", name: "--gray-0" },
    { comment: null, name: "--bare" },
    { comment: "Deep lift.", name: "--shadow-lg" },
    { comment: null, name: "--after-banner" },
  ]);
});

test("collectDeclarationComments reports a name once per declaration site", () => {
  const found = collectDeclarationComments(
    ':root { --focus-ring: var(--blue-450); /* Light. */ }\n'
    + ':root[data-theme="dark"] { --focus-ring: var(--blue-300); /* Dark. */ }',
  );
  assert.deepEqual(found.map(({ comment }) => comment), ["Light.", "Dark."]);
});

// The guardrails' own guardrails.
//
// `scripts/style-invariants.test.mjs` asserts things about the styling layer.
// This file asserts things about the machinery that asserts them, and about the
// four spellings that machinery cannot see. Both halves come out of the same
// exercise: taking the finished Phase 5 tree and trying to land a violation of
// its own bar -- "theme changes are a one-file edit, and nothing can regress
// that" -- with `npm run lint` and `npm run test:frontend` both green.
//
// Two kinds of hole turned up, and they need different assertions.
//
// The first is a value written where the property that names it never appears.
// `declaration-property-value-allowed-list` keys on the property, so it reads
// `font-size` and never `font`; `collectScaleLiterals` keys on the same four
// property names; the breakpoint rules read `@media` and never `@container`;
// and the colour sweep splits a value at depth zero, so a keyword inside
// `color-mix(...)` is a component it never looks at. Each of those is a real
// path a size, a face, a step or a colour can take into the layer with every
// gate green, and one of them is not hypothetical: two `font:` shorthands in
// `app/dashboards/operations-dashboard.css` carry a raw size and a raw stack
// today.
//
// The second is the wiring. Nothing in the tree notices if `npm run lint`
// stops calling `lint:css`, if a rule in `.stylelintrc.json` is set to null, if
// a third file joins either exemption list, or if a documented gate leaves CI --
// and each of those makes the phase bar quietly false while every test still
// passes. A guardrail whose own removal is silent is a convention, not a
// ratchet, so the wiring is pinned here in the shape the documentation claims
// for it.
//
// Every read of a stylesheet goes through `scripts/style-inventory.mjs`, like
// the suite beside this one, so this file never opens a `.css` itself. What it
// does read directly are the three tool configurations -- `package.json`,
// `.stylelintrc.json` and the CI workflow -- because those are the subject.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import {
  collectContainerQueries,
  collectFontShorthandLiterals,
  collectImportantCounts,
  collectInlineStyleColourLiterals,
  collectNestedColourLiterals,
  flattenValueComponents,
} from "./style-inventory.mjs";

const workspace = process.cwd();
const packagePath = path.join(workspace, "package.json");
const stylelintPath = path.join(workspace, ".stylelintrc.json");
const workflowPath = path.join(workspace, ".github", "workflows", "ci.yml");

/** The four max-width steps docs/theming.md lists, largest first. */
const BREAKPOINT_CANON = [1240, 980, 760, 480];

/** The two files `.stylelintrc.json` documents as allowed to say `!important`. */
const IMPORTANT_EXEMPT = Object.freeze({
  "app/reports/reports.css": 9,
  "app/styles/interaction.css": 5,
});

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

function readJson(file) {
  return readFile(file, "utf8").then((text) => JSON.parse(text));
}

/* == 1. The gates stay wired ================================================== */

test("npm run lint runs the stylesheet gate as well as the script gate", async () => {
  const manifest = await readJson(packagePath);
  const lint = manifest.scripts.lint;
  assert.match(
    lint,
    /\boxlint\b/u,
    `package.json "lint" no longer runs oxlint; it reads ${lint}`,
  );
  assert.match(
    lint,
    /npm run lint:css\b/u,
    "package.json \"lint\" no longer chains npm run lint:css, so `make lint`, `make test` and the CI Lint\n"
      + "step all stop checking the stylesheets while reporting success. Every value rule in\n"
      + `.stylelintrc.json goes quiet at once when this is dropped, and it currently reads: ${lint}`,
  );
  assert.match(
    manifest.scripts["lint:css"],
    /^stylelint "app\/\*\*\/\*\.css"$/u,
    "npm run lint:css must run stylelint over every stylesheet under app/. Narrowing the glob is the one\n"
      + "edit that silences the gate for a file without changing a single rule, and it looks like a\n"
      + `refactor in review. It currently reads: ${manifest.scripts["lint:css"]}`,
  );
});

test("no guardrail rule is switched off at the top level of .stylelintrc.json", async () => {
  const config = await readJson(stylelintPath);
  assert.equal(
    config.defaultSeverity,
    undefined,
    "defaultSeverity is set again. Phase 5 removed it so stylelint's own `error` applies; setting it to\n"
      + "\"warning\" leaves every rule below reporting and nothing failing, which is the shape this file\n"
      + "exists to catch.",
  );
  const required = [
    "color-named",
    "color-no-hex",
    "declaration-no-important",
    "declaration-property-value-allowed-list",
    "declaration-property-value-disallowed-list",
    "media-feature-name-allowed-list",
    "media-feature-name-value-allowed-list",
    "selector-class-pattern",
  ];
  const disabled = required.filter((rule) => (config.rules[rule] ?? null) === null);
  assert.deepEqual(
    disabled,
    [],
    "These rules carry the phase bar -- a colour, a size, a radius, a shadow, a stacking layer, a font\n"
      + "stack, a breakpoint or an !important written outside the token layer fails the build because of\n"
      + "them. Setting one to null is a one-word edit that removes a whole family of checks and leaves the\n"
      + `file looking configured:\n${describeList(disabled)}`,
  );
  const properties = config.rules["declaration-property-value-allowed-list"];
  assert.deepEqual(
    Object.keys(properties).toSorted(),
    ["/^border-(?:[a-z]+-){0,2}radius$/", "box-shadow", "font", "font-family", "font-size", "z-index"],
    "The allow-list no longer keys the same six properties. Each one is a scale a retheme has to be able\n"
      + "to move; dropping a key stops that scale being checked anywhere. Two of the keys are shaped rather\n"
      + "than literal and that shape is the check: the radius key is a pattern so the four `border-*-radius`\n"
      + "longhands are held to the same token list as the shorthand, and `font` is keyed at all so the\n"
      + "shorthand cannot state a size or a face past the `font-size` and `font-family` entries.",
  );
  assert.deepEqual(
    config.rules["media-feature-name-value-allowed-list"]["max-width"],
    [`/^(?:${BREAKPOINT_CANON.join("|")})px$/`],
    "The max-width allow-list no longer pins exactly the four documented steps. Adding a fifth here is\n"
      + "how a breakpoint ladder grows without anybody deciding to grow it.",
  );
});

test("the two stylelint exemptions name exactly the files they document", async () => {
  const config = await readJson(stylelintPath);
  const exemptions = config.overrides.map((entry) => ({
    files: entry.files.toSorted(),
    rules: Object.keys(entry.rules).toSorted(),
  }));
  assert.deepEqual(
    exemptions,
    [
      {
        files: ["app/styles/tokens-*.css"],
        rules: [
          // `color-named` is deliberately absent, and adding it here would be a
          // real loss rather than a formality: the palette writes every
          // primitive as a six-digit hex because style-invariants.test.mjs
          // parses those digits to compare lightness, so `--fg-text: white` is
          // a spelling the invariants cannot read. The rule fires inside the
          // token layer -- verified by writing one there -- and that is the
          // only value rule that does.
          "color-no-hex",
          "declaration-property-value-allowed-list",
          "declaration-property-value-disallowed-list",
        ],
      },
      { files: ["app/reports/reports.css", "app/styles/interaction.css"], rules: ["declaration-no-important"] },
    ],
    "An override is a file the value rules do not apply to, and adding a path to one is the cheapest way\n"
      + "to make a violation legal: the offending line stays exactly as written and the build goes green.\n"
      + "The two that exist are argued for in the file's own comments -- the token layer is the one place a\n"
      + "value belongs, and two stylesheets fight a cascade specificity cannot win. A third needs the same\n"
      + "argument made in review, which is what changing this list forces.",
  );
});

test("CI runs every gate the guardrail layer is made of", async () => {
  const workflow = await readFile(workflowPath, "utf8");
  const missing = ["npm run check:docs", "npx --no-install oxlint .", "npm run lint:css", "npm run typecheck",
    "npm run test:contracts", "npm run test:frontend"]
    .filter((command) => !workflow.includes(`run: ${command}`));
  assert.deepEqual(
    missing,
    [],
    "A gate that does not run in CI is a gate one developer's machine enforces. These are named in\n"
      + `docs/theming.md as the checks that hold the cleanup in place:\n${describeList(missing)}`,
  );
});

/* == 2. Values written where the property that names them never appears ======== */

test("no font shorthand states a size or a face the token layer cannot reach", async () => {
  const hidden = await collectFontShorthandLiterals(workspace);
  assert.deepEqual(
    hidden,
    [],
    "`font: 12px/1.4 system-ui, sans-serif` sets font-size and font-family without writing either word, so\n"
      + "stylelint's per-property allow-lists never fire and the scale sweep -- which also keys on\n"
      + "`font-size` -- never sees it. The size and the face are then exactly as unreachable as the hex\n"
      + "literals this phase removed. Write the shorthand through the tokens, or split it into the\n"
      + `longhands the allow-list already checks:\n${describeList(hidden)}`,
  );
});

test("every width a container query states is on the documented breakpoint canon", async () => {
  const queries = await collectContainerQueries(workspace);
  const offCanon = queries.filter((entry) => {
    const widths = [...entry.matchAll(/(\d+)px/gu)].map((match) => Number(match[1]));
    return widths.some((width) => !BREAKPOINT_CANON.includes(width));
  });
  assert.deepEqual(
    offCanon,
    [],
    "A container query steps a layout at a width exactly as a media query does, and neither gate that\n"
      + "polices the canon can see one: stylelint's media-feature rules read `@media` preludes only, and\n"
      + "`collectMediaQueryRuns` drops any prelude that does not start with `@media`. A second ladder here\n"
      + "is invisible until somebody measures the page. Use one of the four documented steps\n"
      + `(${BREAKPOINT_CANON.join("px, ")}px) or add the new one to docs/theming.md and to the\n`
      + `.stylelintrc.json value list first:\n${describeList(offCanon)}`,
  );
});

test("no colour literal hides inside a function's arguments", async () => {
  const nested = await collectNestedColourLiterals(workspace);
  assert.deepEqual(
    nested,
    [],
    "`hasColourLiteral` splits a value at depth zero, so a named colour one level down --\n"
      + "`color-mix(in srgb, rebeccapurple 30%, transparent)`, `linear-gradient(black, transparent)` -- is\n"
      + "never a component it examines, and stylelint has no rule for a keyword at all. Phase 5 made\n"
      + "color-mix the idiom for every translucent colour in the layer, which makes the inside of a\n"
      + `function the likeliest place for the next literal to land:\n${describeList(nested)}`,
  );
});

test("an inline style may not write a colour the stylesheet sweep would reject", async () => {
  const inline = await collectInlineStyleColourLiterals(workspace);
  assert.deepEqual(
    inline,
    [],
    "`collectTypeScriptColourLiterals` looks for `#` and `rgb(`; the stylesheet sweep beside it also counts\n"
      + "a named colour and every modern colour function. The weaker of the two definitions guards the\n"
      + "stronger position: an inline style outranks every rule in the cascade, so a colour written here\n"
      + `survives a retheme that repaints the whole stylesheet layer:\n${describeList(inline)}`,
  );
});

/* == 3. The exemptions do not grow ============================================ */

test("the two files exempt from declaration-no-important say it the recorded number of times", async () => {
  const counts = await collectImportantCounts(workspace);
  assert.deepEqual(
    counts,
    IMPORTANT_EXEMPT,
    "stylelint is told to ignore `!important` in these two files, which makes them the one place the flag\n"
      + "can spread with nothing reporting it -- and an `!important` is the single hardest thing in CSS for\n"
      + "a later rule to override, so each new one narrows what a theme can change. A file appearing here\n"
      + "that is not exempt means the lint has stopped running at all. Changing a count is fine; doing it\n"
      + "deliberately, with the reason written at the site, is the point.",
  );
});

/* == 4. The parsers these assertions stand on ================================= */

test("flattenValueComponents returns arguments at every depth and drops function names", () => {
  assert.deepEqual(
    flattenValueComponents("color-mix(in srgb, var(--accent) 12%, transparent)"),
    ["in", "srgb", "--accent", "12%", "transparent"],
    "the flattener must reach the arguments a nested function holds, or a colour inside color-mix stays"
      + " invisible",
  );
  assert.deepEqual(
    flattenValueComponents("linear-gradient(black, transparent)"),
    ["black", "transparent"],
    "a function name is the operation, not an argument: returning `linear-gradient` would file every"
      + " gradient as a colour literal",
  );
  assert.deepEqual(
    flattenValueComponents("nowrap"),
    ["nowrap"],
    "a single identifier is one token; `white-space: nowrap` must not read as the colour `white`",
  );
  assert.deepEqual(
    flattenValueComponents("0 0 0 3px color-mix(in srgb, var(--accent-bright) 12%, transparent)"),
    ["0", "0", "0", "3px", "in", "srgb", "--accent-bright", "12%", "transparent"],
    "a shadow's geometry and its ink are separate tokens at two different depths",
  );
});

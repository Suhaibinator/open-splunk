// One implementation of each primitive.
//
// Phase 3 folded parallel families into single primitives: eight status chips
// into `.status`, eight badges into `.badge`, three button vocabularies into
// `.button`, five tables into `.table`, four chrome bars into ProductShell's
// own, and six keyframe blocks into `spin` and `pulse-ring`. Nothing in the
// toolchain notices when a second implementation grows back -- CSS has no
// duplicate-rule error, and a screenshot of a page that uses only one of the
// two copies is identical either way -- so these are the assertions that make
// the fold stick.
//
// Reading and parsing live in `scripts/css-inventory.mjs`, which is a library
// rather than a test file, so nothing here opens a stylesheet and the
// "no test reads stylesheet text" invariant in css-invariants.test.mjs stays
// true of this file too.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import {
  collectAnimationReferences,
  collectBaseRuleSites,
  collectDeclarationBlocks,
  collectKeyframeSites,
  cssDeclarations,
  declarationSignature,
  describeRuleSite,
} from "./css-inventory.mjs";

const workspace = process.cwd();
const duplicateAllowlistPath = path.join(workspace, "scripts", "css-duplicate-blocks.json");

/**
 * The primitives Phase 3 consolidated, and what each one replaced.
 *
 * The count is the point: every one of these names was, before the phase, two
 * or more rules describing the same thing. Adding a name here is a claim that
 * the product has exactly one of it.
 */
const CONSOLIDATED_PRIMITIVES = {
  badge: "eight chips: .mode-pill, .role-pill, .severity-badge, two .previewBadge module rules, .readOnlyBadge, .liveBadge/.partialBadge, .availableBadge/.unavailableBadge",
  button: "three vocabularies: .button, .suite-button, .icon-button/.close-button",
  drawer: "two mobile drawers: .suite-mobile-drawer and .search-mobile-drawer",
  "drawer-backdrop": "three identical scrims: .suite-mobile-backdrop, .search-mobile-backdrop, .time-picker-mobile-backdrop",
  "modal-card": "the one dialog surface, after Modal moved to app/_components",
  status: "eight families: .status-icon, .status-dot, .status-label, .mini-status, .job-state-icon, .job-card-state, .inspector-state, and reports' .status/.runStatus",
  table: "five tables: .product-table and four card-mode reimplementations",
  "table-wrap": "two scroll containers: .responsive-table-wrap and .product-table's own wrapper",
  wordmark: "three blocks and four markup copies of the product wordmark",
};

/** Animations more than one family used to declare a private copy of. */
const SHARED_ANIMATIONS = {
  "pulse-ring": "folded from .pulse, .status-pulse and .backend-preview-pulse",
  spin: "folded from .app-icon-spin, .spinner and .backend-state-spin",
};

/** Keyframe blocks the fold above deleted; none may come back under its old name. */
const RETIRED_KEYFRAMES = [
  "app-icon-spin",
  "backend-preview-pulse",
  "backend-state-spin",
  "pulse",
  "spinner",
  "status-pulse",
];

/** Two rules stating this many declarations identically is a restatement, not a coincidence. */
const DUPLICATE_DECLARATION_THRESHOLD = 4;

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

/** Reads the justified record of duplication this phase deliberately left. */
async function readDuplicateAllowlist() {
  const parsed = JSON.parse(await readFile(duplicateAllowlistPath, "utf8"));
  return new Map((parsed.groups ?? []).map((group) => [
    JSON.stringify({ declarations: group.declarations, sites: group.sites }),
    group.why,
  ]));
}

/** Groups every rule of at least `minimum` declarations by what it declares. */
async function duplicateDeclarationGroups(root, minimum) {
  const groups = new Map();
  for (const block of await collectDeclarationBlocks(root, minimum)) {
    const declarations = declarationSignature(block.declarations);
    const key = declarations.join("; ");
    const group = groups.get(key) ?? { declarations, sites: [] };
    group.sites.push(describeRuleSite(block));
    groups.set(key, group);
  }
  return [...groups.values()]
    .filter((group) => group.sites.length > 1)
    .map((group) => ({ declarations: group.declarations, sites: group.sites.toSorted() }))
    .toSorted((left, right) => left.sites[0].localeCompare(right.sites[0]));
}

test("every consolidated primitive has exactly one base rule", async () => {
  const sites = await collectBaseRuleSites(workspace, Object.keys(CONSOLIDATED_PRIMITIVES));
  const problems = [];
  for (const [name, replaced] of Object.entries(CONSOLIDATED_PRIMITIVES).toSorted()) {
    // An at-rule ancestor makes a rule a responsive or theme override of the
    // base, not a second base: it can only apply where the base already does.
    const bases = sites.get(name).filter((site) => site.ancestors.length === 0);
    if (bases.length === 1) continue;
    problems.push(
      bases.length === 0
        ? `.${name} has no unconditional base rule at all, so ${replaced} were folded into nothing`
        : `.${name} has ${bases.length} base rules, which is ${bases.length} implementations of one primitive:\n`
          + bases.map((site) => `      ${site.file} :: ${site.prelude}`).join("\n"),
    );
  }
  assert.deepEqual(
    problems,
    [],
    "A primitive is defined more than once. Whichever rule loses the cascade is dead weight, and\n"
      + "whichever wins depends on file order rather than on intent -- which is the state Phase 3\n"
      + `removed. Fold the extra rules into the one base:\n${describeList(problems)}`,
  );
});

test("every shared animation is defined exactly once and its retired names stay gone", async () => {
  const sites = await collectKeyframeSites(workspace);
  const problems = [];
  for (const [name, folded] of Object.entries(SHARED_ANIMATIONS).toSorted()) {
    const defined = sites.filter((site) => site.name === name);
    if (defined.length === 1) continue;
    problems.push(
      `@keyframes ${name} (${folded}) is defined ${defined.length} times`
      + (defined.length === 0 ? "" : `: ${defined.map((site) => site.file).join(", ")}`),
    );
  }
  for (const name of RETIRED_KEYFRAMES) {
    const defined = sites.filter((site) => site.name === name);
    if (defined.length > 0) {
      problems.push(`@keyframes ${name} is back in ${defined.map((site) => site.file).join(", ")}`);
    }
  }
  assert.deepEqual(
    problems,
    [],
    "Two copies of one animation is the defect this phase removed, and CSS reports it as nothing:\n"
      + `the later block simply wins.\n${describeList(problems)}`,
  );
});

test("every animation a rule plays names a keyframe block that exists", async () => {
  const defined = new Set((await collectKeyframeSites(workspace)).map((site) => site.name));
  const dangling = (await collectAnimationReferences(workspace))
    .filter((reference) => !defined.has(reference.name))
    .map((reference) => `${reference.file} :: ${reference.selector} plays ${reference.name}`)
    .toSorted();
  assert.deepEqual(
    dangling,
    [],
    "A rule asks for a keyframe block nothing declares, which renders as no animation at all and\n"
      + `raises no error anywhere. This is how folding duplicate keyframes goes wrong:\n${describeList(dangling)}`,
  );
});

test("no two rules state the same four or more declarations", async () => {
  const allowlist = await readDuplicateAllowlist();
  const unjustified = (await duplicateDeclarationGroups(workspace, DUPLICATE_DECLARATION_THRESHOLD))
    .filter((group) => !allowlist.has(JSON.stringify(group)))
    .map((group) => `${group.declarations.length} declarations restated by:\n`
      + group.sites.map((site) => `      ${site}`).join("\n")
      + `\n      { ${group.declarations.join("; ")} }`);
  assert.deepEqual(
    unjustified,
    [],
    "Two rules describe the same thing in the same words. Either one of them should be using the\n"
      + "other -- a primitive, a modifier, a shared selector list -- or the duplication is deliberate\n"
      + "and belongs in scripts/css-duplicate-blocks.json with the reason and the primitive that\n"
      + `would otherwise own it:\n${describeList(unjustified)}`,
  );
});

test("the duplicate-block allowlist carries no stale entries", async () => {
  const allowlist = await readDuplicateAllowlist();
  const live = new Set(
    (await duplicateDeclarationGroups(workspace, DUPLICATE_DECLARATION_THRESHOLD))
      .map((group) => JSON.stringify(group)),
  );
  const stale = [];
  for (const [key, why] of allowlist) {
    if (live.has(key)) {
      if (typeof why === "string" && why.trim().length > 0) continue;
      stale.push(`an entry for ${JSON.parse(key).sites[0]} carries no justification`);
      continue;
    }
    const { sites } = JSON.parse(key);
    stale.push(`${sites[0]} no longer restates the same declarations as ${sites.slice(1).join(", ")}`);
  }
  assert.deepEqual(
    stale.toSorted(),
    [],
    "scripts/css-duplicate-blocks.json describes duplication that has changed or been paid off.\n"
      + "Delete the entry when the duplication is gone; rewrite it -- declarations, sites and reason\n"
      + `-- when it has moved, so the record can never drift into a blanket exemption:\n${describeList(stale.toSorted())}`,
  );
});

// The invariants above are only as good as the parsing under them. What follows
// pins that parsing against the shapes a simpler implementation gets wrong.

test("declarationSignature is order-independent and keeps property and value together", () => {
  assert.deepEqual(
    declarationSignature(cssDeclarations("gap: 4px; color: red;")),
    declarationSignature(cssDeclarations("color: red; gap: 4px;")),
  );
  assert.notDeepEqual(
    declarationSignature(cssDeclarations("gap: 4px; color: red;")),
    declarationSignature(cssDeclarations("gap: red; color: 4px;")),
  );
});

test("collectDeclarationBlocks counts a rule's own declarations, not a nested rule's", async () => {
  const blocks = await collectDeclarationBlocks(workspace, 1);
  assert.ok(blocks.length > 100, `only ${blocks.length} rules were found; the walk is broken`);
  assert.ok(
    blocks.every((block) => !block.prelude.startsWith("@")),
    "an at-rule prelude was counted as a selector, so @keyframes steps would be compared as rules",
  );
  assert.ok(
    blocks.every((block) => block.declarations.every(({ property }) => !property.includes("{"))),
    "a nested rule's selector was parsed as a declaration property",
  );
});

test("describeRuleSite names the file, the at-rules around a rule, and its selector list", () => {
  assert.equal(
    describeRuleSite({
      ancestors: ["@media (max-width: 760px)"],
      file: "app/globals.css",
      prelude: ".table, .other",
    }),
    "app/globals.css :: @media (max-width: 760px) :: .table, .other",
  );
});

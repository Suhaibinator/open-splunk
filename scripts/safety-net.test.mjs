// Invariants over the Phase 0 safety net itself.
//
// The net is only worth trusting if it cannot stop running without saying so.
// Two mechanisms can fail silently: `scripts/test-frontend.mjs` names every
// unit test in a hardcoded list, so a new test file that nobody adds there is
// simply never executed; and the visual baselines are stored per project, so a
// screenshot whose baseline was never committed only fails on the machine that
// runs the suite -- while a baseline whose test was deleted lingers forever.
//
// The repository walk is borrowed from `scripts/css-inventory.mjs` rather than
// duplicated; it skips dependency and build directories, which is exactly what
// these checks need too.
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import { BACKEND_VISUAL_EXPORT, DEMO_VISUAL_EXPORT } from "./build-visual-exports.mjs";
import { listRepositoryFiles, relativePosix } from "./css-inventory.mjs";
import { countRecordedScreenshots, parseDeterminismArguments } from "./visual-determinism.mjs";

const workspace = process.cwd();
const runnerPath = path.join(workspace, "scripts", "test-frontend.mjs");
const visualConfigPath = path.join(workspace, "playwright.visual.config.ts");
const serversPath = path.join(workspace, "integration", "visual", "visual-servers.ts");
const baselineRoot = path.join(workspace, "integration", "visual", "__screenshots__");

/** Paths `scripts/test-frontend.mjs` names, in the two shapes it uses. */
function registeredTestPaths(runner) {
  const registered = new Set();
  for (const match of runner.matchAll(/path\.join\(([^)]*)\)/gu)) {
    const segments = [...match[1].matchAll(/"([^"]*)"/gu)].map((segment) => segment[1]);
    if (segments.length > 0) registered.add(segments.join("/"));
  }
  for (const match of runner.matchAll(/"([\w.-]+\.test\.mjs)"/gu)) registered.add(`scripts/${match[1]}`);
  return registered;
}

function describeList(items) {
  return items.map((item) => `  ${item}`).join("\n");
}

test("every unit test file is named in the hardcoded runner list", async () => {
  const registered = registeredTestPaths(await readFile(runnerPath, "utf8"));
  const discovered = (await listRepositoryFiles(workspace))
    .map((file) => relativePosix(workspace, file))
    // Playwright owns integration/visual; `node --test` never runs it.
    .filter((file) => /\.test\.(?:ts|tsx|mjs)$/u.test(file) && !file.startsWith("integration/visual/"))
    .toSorted();
  assert.ok(discovered.length > 40, `only ${discovered.length} test files were discovered; the walk is broken`);
  const unregistered = discovered.filter((file) => !registered.has(file));
  assert.deepEqual(
    unregistered,
    [],
    "These test files exist but scripts/test-frontend.mjs never runs them, so they pass by\n"
      + `not being executed. Add each one to its list:\n${describeList(unregistered)}`,
  );
});

test("the runner list names no test file that has been deleted", async () => {
  const registered = registeredTestPaths(await readFile(runnerPath, "utf8"));
  const present = new Set(
    (await listRepositoryFiles(workspace)).map((file) => relativePosix(workspace, file)),
  );
  const missing = [...registered]
    .filter((file) => /\.test\.(?:ts|tsx|mjs)$/u.test(file) && !present.has(file))
    .toSorted();
  assert.deepEqual(
    missing,
    [],
    `scripts/test-frontend.mjs names test files that no longer exist:\n${describeList(missing)}`,
  );
});

test("every visual screenshot has a baseline in every project, and none is orphaned", async () => {
  const specs = (await listRepositoryFiles(workspace)).filter((file) => file.endsWith(".visual.spec.ts"));
  assert.ok(specs.length > 0, "no *.visual.spec.ts files were found; the visual suite is not being scanned");
  const specSources = await Promise.all(specs.map((spec) => readFile(spec, "utf8")));
  const expected = new Set();
  for (const source of specSources) {
    for (const match of source.matchAll(/expect\w*Screenshot\([^,]*,\s*"([^"]*)"/gu)) expected.add(match[1]);
  }
  assert.ok(expected.size > 0, "no screenshot names were parsed out of the visual specs");

  const configuration = await readFile(visualConfigPath, "utf8");
  const projects = [...configuration.matchAll(/name:\s*"([^"]*)"/gu)].map((match) => match[1]);
  assert.ok(projects.length > 0, "no projects were parsed out of playwright.visual.config.ts");

  const platforms = (await readdir(baselineRoot, { withFileTypes: true }))
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name);
  assert.ok(platforms.length > 0, `no baselines are committed under ${relativePosix(workspace, baselineRoot)}`);

  const sets = platforms.flatMap((platform) => projects.map((project) => ({ platform, project })));
  const listings = await Promise.all(sets.map(async ({ platform, project }) => ({
    files: await readdir(path.join(baselineRoot, platform, project)).catch(() => null),
    platform,
    project,
  })));
  const problems = [];
  for (const { files, platform, project } of listings) {
    if (files === null) {
      problems.push(`${platform}/${project}: no baselines are committed for this project`);
      continue;
    }
    const recorded = new Set(files.filter((file) => file.endsWith(".png")).map((file) => file.slice(0, -4)));
    for (const name of [...expected].toSorted()) {
      if (!recorded.has(name)) problems.push(`${platform}/${project}/${name}.png: a spec pins it, but it is missing`);
    }
    for (const name of [...recorded].toSorted()) {
      if (!expected.has(name)) problems.push(`${platform}/${project}/${name}.png: no spec pins it any more`);
    }
  }
  assert.deepEqual(
    problems,
    [],
    "The committed baselines and the visual specs disagree. A missing baseline only fails on the\n"
      + "machine that runs the suite; an orphaned one is never compared against anything:\n"
      + describeList(problems),
  );
});

test("the builder and the visual configuration name the same export directories", async () => {
  const declared = Object.fromEntries(
    [...(await readFile(serversPath, "utf8"))
      .matchAll(/export const (\w+_EXPORT_ROOT) = "([^"]*)";/gu)]
      .map((match) => [match[1], match[2]]),
  );
  assert.deepEqual(
    declared,
    {
      BACKEND_EXPORT_ROOT: relativePosix(workspace, BACKEND_VISUAL_EXPORT),
      DEMO_EXPORT_ROOT: relativePosix(workspace, DEMO_VISUAL_EXPORT),
    },
    "scripts/build-visual-exports.mjs writes the exports somewhere integration/visual/visual-servers.ts\n"
      + "does not serve them from, so the visual suite would render a stale directory or none at all.",
  );
});

test("parseDeterminismArguments accepts its flags and rejects everything else", () => {
  assert.deepEqual(parseDeterminismArguments([]), { port: 43380, skipBuild: false });
  assert.deepEqual(
    parseDeterminismArguments(["--skip-build", "--port", "45000"]),
    { port: 45000, skipBuild: true },
  );
  assert.throws(() => parseDeterminismArguments(["--port", "0"]), /between 1 and 65533/u);
  assert.throws(() => parseDeterminismArguments(["--port", "65534"]), /between 1 and 65533/u);
  assert.throws(() => parseDeterminismArguments(["--update"]), /unknown argument --update/u);
});

test("countRecordedScreenshots reports zero for a pass that recorded nothing", async () => {
  assert.equal(await countRecordedScreenshots(path.join(workspace, "no-such-directory")), 0);
  assert.ok(await countRecordedScreenshots(baselineRoot) > 0);
});

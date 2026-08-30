// Invariants over the hardcoded frontend test runner.
//
// The runner is only worth trusting if a test cannot stop running without
// saying so. The repository walk is borrowed from `scripts/style-inventory.mjs`
// rather than duplicated; it skips dependency and build directories, which is
// exactly what these checks need too.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import { listRepositoryFiles, relativePosix } from "./style-inventory.mjs";

const workspace = process.cwd();
const runnerPath = path.join(workspace, "scripts", "test-frontend.mjs");

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
    .filter((file) => /\.test\.(?:ts|tsx|mjs)$/u.test(file))
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

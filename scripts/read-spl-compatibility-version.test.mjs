/* eslint-disable no-await-in-loop */
// Each hostile source fixture is intentionally materialized and checked in sequence.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";

const workspace = process.cwd();
const reader = path.join(
  workspace,
  "scripts",
  "read-spl-compatibility-version.mjs",
);

function readCompatibilityVersion(cwd, arguments_ = []) {
  return spawnSync(process.execPath, [reader, ...arguments_], {
    cwd,
    encoding: "utf8",
  });
}

async function sourceFixture(t, source) {
  const fixture = await mkdtemp(
    path.join(tmpdir(), "open-splunk-spl-compatibility-version-"),
  );
  t.after(() => rm(fixture, { force: true, recursive: true }));
  const sourceDirectory = path.join(fixture, "internal", "spl");
  await mkdir(sourceDirectory, { recursive: true });
  await writeFile(path.join(sourceDirectory, "doc.go"), source);
  return fixture;
}

test("SPL compatibility identity reader returns the exact source declaration", async () => {
  const source = await readFile(
    path.join(workspace, "internal", "spl", "doc.go"),
    "utf8",
  );
  const declarations = [
    ...source.matchAll(/^const CompatibilityVersion = "([0-9]+\.[0-9]+)"$/gm),
  ];
  assert.equal(declarations.length, 1);

  const result = readCompatibilityVersion(workspace);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, `${declarations[0][1]}\n`);
});

test("SPL compatibility identity reader follows an exact future source identity", async (t) => {
  const fixture = await sourceFixture(
    t,
    "package spl\n\nconst CompatibilityVersion = \"9.9\"\n",
  );
  const result = readCompatibilityVersion(fixture);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, "9.9\n");
});

test("SPL compatibility identity reader rejects missing, malformed, and duplicate declarations", async (t) => {
  const sources = [
    "package spl\n",
    "package spl\n\nconst CompatibilityVersion=\"9.9\"\n",
    "package spl\n\nconst CompatibilityVersion = \"9.9.0\"\n",
    "package spl\n\nconst CompatibilityVersion = \"9.8\"\nconst CompatibilityVersion = \"9.9\"\n",
  ];
  for (const source of sources) {
    const fixture = await sourceFixture(t, source);
    const result = readCompatibilityVersion(fixture);
    assert.notEqual(result.status, 0, source);
    assert.equal(result.stdout, "");
  }
});

test("SPL compatibility identity reader rejects arguments", () => {
  const result = readCompatibilityVersion(workspace, ["unexpected"]);
  assert.equal(result.status, 2);
  assert.match(result.stderr, /^usage:/);
});

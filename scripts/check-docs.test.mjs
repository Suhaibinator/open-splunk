import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";

const workspace = process.cwd();
const checker = path.join(workspace, "scripts", "check-docs.mjs");
const ownedMarkdownPaths = [
  "README.md",
  "AGENTS.md",
  "CLAUDE.md",
  "docs/README.md",
  "docs/architecture.md",
  "docs/api.md",
  "docs/spl.md",
  "docs/knowledge.md",
  "docs/theming.md",
  "docs/ingestion.md",
  "docs/collector-configuration.md",
  "docs/hec.md",
  "docs/auditing.md",
  "docs/roadmap.md",
  "docs/releasing.md",
  "deploy/README.md",
  "integration/README.md",
  "scripts/README.md",
  "migrations/README.md",
  "internal/hec/testdata/compatibility/README.md",
  "gen/go/README.md",
  "gen/ts/README.md",
];

async function fixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-docs-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  await Promise.all(ownedMarkdownPaths.map(async (relativePath) => {
    const filename = path.join(root, relativePath);
    await mkdir(path.dirname(filename), { recursive: true });
    await writeFile(filename, `# ${path.basename(relativePath, ".md")}\n`);
  }));
  return root;
}

function check(root, arguments_ = []) {
  return spawnSync(process.execPath, [checker, "--root", root, ...arguments_], {
    encoding: "utf8",
  });
}

test("documentation checker accepts unversioned local links and duplicate anchors", async (t) => {
  const root = await fixture(t);
  await writeFile(
    path.join(root, "docs", "README.md"),
    [
      "# Documentation",
      "",
      "[API](api.md#contract-1)",
      "[Root](../README.md)",
      "[External](https://example.com/reference)",
      "",
    ].join("\n"),
  );
  await writeFile(
    path.join(root, "docs", "api.md"),
    "# Contract\n\n## Contract\n",
  );

  const result = check(root);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /^documentation check passed/u);
});

test("documentation checker rejects stale versioned contract identifiers", async (t) => {
  const staleValues = [
    "open_splunk.v1",
    "proto/open_splunk/v04",
    "/api/v1/search/jobs",
    "SPL-V03-REGEX-001",
    "/services/collector/event/1.0",
    "docs/spl-compatibility-v0.2.md",
    "0004_open_splunk.sql",
    "format version 4",
    "OPEN_SPLUNK_APPLICATION_VERSION",
  ];

  const results = await Promise.all(staleValues.map(async (stale) => {
    const root = await fixture(t);
    await writeFile(path.join(root, "docs", "api.md"), `# API\n\n${stale}\n`);
    return { result: check(root), stale };
  }));

  for (const { result, stale } of results) {
    assert.equal(result.status, 1, stale);
    assert.match(result.stderr, /documentation check failed/u, stale);
    assert.match(result.stderr, /docs\/api\.md:3/u, stale);
  }
});

test("documentation checker rejects missing files and anchors", async (t) => {
  const root = await fixture(t);
  await writeFile(
    path.join(root, "docs", "README.md"),
    "# Documentation\n\n[Missing](absent.md)\n[Anchor](api.md#absent)\n",
  );

  const result = check(root);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /missing local link target/u);
  assert.match(result.stderr, /missing Markdown anchor/u);
});

test("documentation checker rejects extra historical documents", async (t) => {
  const root = await fixture(t);
  await writeFile(path.join(root, "docs", "implementation-history.md"), "# History\n");

  const result = check(root);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /unexpected non-canonical documentation file/u);
});

test("documentation checker ignores negative-support fixtures outside owned Markdown", async (t) => {
  const root = await fixture(t);
  const negativeFixture = path.join(root, "internal", "hec", "route_negative_test.go");
  await mkdir(path.dirname(negativeFixture), { recursive: true });
  await writeFile(
    negativeFixture,
    "package hec\n\nconst unsupported = \"/services/collector/event/1.0\"\n",
  );

  const result = check(root);
  assert.equal(result.status, 0, result.stderr);
});

test("documentation checker rejects unexpected arguments", () => {
  const result = spawnSync(process.execPath, [checker, "unexpected"], {
    encoding: "utf8",
  });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /^usage:/u);
});

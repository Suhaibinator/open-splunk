import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { OWNED_MARKDOWN_PATHS } from "../lib/help/documentation-registry.mjs";
import { checkDocumentation, parseDocumentationArguments } from "./check-docs.mjs";

async function fixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-docs-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  await Promise.all(OWNED_MARKDOWN_PATHS.map(async (relativePath) => {
    const filename = path.join(root, relativePath);
    await mkdir(path.dirname(filename), { recursive: true });
    await writeFile(filename, `# ${path.basename(relativePath, ".md")}\n`);
  }));
  return root;
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

  assert.deepEqual(await checkDocumentation(root), []);
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
    return { failures: await checkDocumentation(root), stale };
  }));

  for (const { failures, stale } of results) {
    assert.ok(failures.some((failure) => failure.includes("docs/api.md:3")), stale);
  }
});

test("documentation checker rejects missing files and anchors", async (t) => {
  const root = await fixture(t);
  await writeFile(
    path.join(root, "docs", "README.md"),
    "# Documentation\n\n[Missing](absent.md)\n[Anchor](api.md#absent)\n",
  );

  const failures = await checkDocumentation(root);
  assert.ok(failures.some((failure) => failure.includes("missing local link target")));
  assert.ok(failures.some((failure) => failure.includes("missing Markdown anchor")));
});

test("documentation checker rejects extra historical documents", async (t) => {
  const root = await fixture(t);
  await writeFile(path.join(root, "docs", "implementation-history.md"), "# History\n");

  const failures = await checkDocumentation(root);
  assert.ok(failures.some((failure) => failure.includes("unexpected non-canonical documentation file")));
});

test("documentation checker ignores negative-support fixtures outside owned Markdown", async (t) => {
  const root = await fixture(t);
  const negativeFixture = path.join(root, "internal", "hec", "route_negative_test.go");
  await mkdir(path.dirname(negativeFixture), { recursive: true });
  await writeFile(
    negativeFixture,
    "package hec\n\nconst unsupported = \"/services/collector/event/1.0\"\n",
  );

  assert.deepEqual(await checkDocumentation(root), []);
});

test("documentation checker rejects unexpected arguments", () => {
  assert.throws(() => parseDocumentationArguments(["unexpected"]), /^Error: usage:/u);
});

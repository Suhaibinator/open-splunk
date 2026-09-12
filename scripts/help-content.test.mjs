import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";

import { buildHelpDocumentation } from "./build-help.mjs";

test("checked-in Help content is the deterministic output of the canonical registry", async () => {
  const root = process.cwd();
  const generated = JSON.parse(await readFile(path.join(root, "app/help/help-content.generated.json"), "utf8"));
  const rebuilt = JSON.parse(JSON.stringify(await buildHelpDocumentation({ root })));
  assert.deepEqual(generated, rebuilt);
});

test("the Help content revision covers published sources only", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-help-revision-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  await mkdir(path.join(root, "docs"), { recursive: true });
  await writeFile(path.join(root, "docs", "README.md"), "# Documentation\n");
  await writeFile(path.join(root, "AGENTS.md"), "first contributor instruction\n");
  const registry = [
    { kind: "markdown", published: true, section: "Guides", slug: "", sourcePath: "docs/README.md", title: "Documentation" },
    { kind: "markdown", published: false, section: "Internal", slug: "", sourcePath: "AGENTS.md", title: "Agent guidelines" },
  ];
  const before = await buildHelpDocumentation({ registry, root });
  await writeFile(path.join(root, "AGENTS.md"), "changed contributor instruction\n");
  const after = await buildHelpDocumentation({ registry, root });
  assert.equal(after.contentRevision, before.contentRevision);
});

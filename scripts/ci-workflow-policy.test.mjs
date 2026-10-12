import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

const workspace = process.cwd();
const workflowPath = path.join(workspace, ".github", "workflows", "ci.yml");

test("external actions use immutable commit references", async () => {
  const workflow = await readFile(workflowPath, "utf8");
  const actions = [...workflow.matchAll(/^\s+uses:\s+(.+)$/gmu)].map((match) => match[1].trim());
  assert.ok(actions.length > 0);
  assert.equal(actions.length, (workflow.match(/^\s+uses:/gmu) ?? []).length);
  for (const action of actions) {
    if (action.startsWith("./")) {
      assert.match(action, /^\.\/[^\s#]+$/u);
      continue;
    }
    assert.match(action, /^[^@\s]+@[0-9a-f]{40}$/u);
  }
});

test("ClickHouse jobs share one pinned image and one pull policy", async () => {
  const workflow = await readFile(workflowPath, "utf8");
  assert.equal((workflow.match(/^\s+OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE:/gmu) ?? []).length, 1);
  assert.match(workflow, /^\s+OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE: \S+@sha256:[0-9a-f]{64}$/mu);
  assert.equal((workflow.match(/run: scripts\/pull-clickhouse-test-image\.sh/gmu) ?? []).length, 5);
  assert.doesNotMatch(workflow, /docker pull "\$OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"/u);
});

test("the full local verification target covers release-equivalent checks", async () => {
  const makefile = await readFile(path.join(workspace, "Makefile"), "utf8");
  const verify = makefile.slice(makefile.indexOf("verify:\n"), makefile.indexOf("\nclean:\n"));
  for (const command of [
    "scripts/verify-protobuf-generation.sh $(MAKE) build",
    "$(MAKE) test",
    "OPEN_SPLUNK_DATA_MODE=demo npm run build",
    "npm run test:workspace",
    "go build ./...",
    "go vet ./...",
    "$(MAKE) go-lint-tools",
  ]) {
    assert.match(verify, new RegExp(command.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&"), "u"));
  }
  assert.equal((verify.match(/scripts\/verify-protobuf-generation\.sh \$\(MAKE\) build/gu) ?? []).length, 1);
  assert.doesNotMatch(verify, /^\t\$\(MAKE\) build$/mu);
  assert.match(verify, /golangci-lint" run/u);
  assert.match(verify, /GOOS=linux GOARCH=amd64/u);
  assert.match(makefile, /go install github\.com\/golangci\/golangci-lint\/v2\/cmd\/golangci-lint@\$\(GOLANGCI_LINT_VERSION\)/u);
  const lintVersion = makefile.match(/^override GOLANGCI_LINT_VERSION := (v\d+\.\d+\.\d+)$/mu)?.[1];
  assert.ok(lintVersion);
  assert.match(await readFile(workflowPath, "utf8"), new RegExp(`version: ${lintVersion}$`, "mu"));
});

test("frontend CI runs code and stylesheet lint once each", async () => {
  const workflow = await readFile(workflowPath, "utf8");
  assert.equal((workflow.match(/run: npx --no-install oxlint \./gu) ?? []).length, 1);
  assert.equal((workflow.match(/run: npm run lint:css/gu) ?? []).length, 1);
  assert.doesNotMatch(workflow, /run: npm run lint$/mu);
});


test("Go lint scope covers every repository-owned main-module source tree", async () => {
  const [makefile, workflow] = await Promise.all([
    readFile(path.join(workspace, "Makefile"), "utf8"),
    readFile(workflowPath, "utf8"),
  ]);
  const declared = makefile.match(/^override GO_LINT_PACKAGES := (.+)$/mu)?.[1];
  assert.ok(declared, "Make must declare its first-party package scope");
  assert.equal(workflow.match(/^  GO_LINT_PACKAGES: (.+)$/mu)?.[1], declared);
  assert.match(makefile, /--max-issues-per-linter=0 --max-same-issues=0 \$\(GO_LINT_PACKAGES\)/u);
  assert.match(workflow, /args: --timeout=10m --max-issues-per-linter=0 --max-same-issues=0 \$\{\{ env\.GO_LINT_PACKAGES \}\}/u);

  // Source control identifies repository-owned files; nested modules retain
  // their own package boundaries. Neither npm installation contents nor a
  // formatter exclusion determines coverage of the main module's Go sources.
  const discovery = spawnSync("git", ["ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.go"], {
    cwd: workspace, encoding: "utf8",
  });
  assert.equal(discovery.status, 0, discovery.stderr);
  const sources = [...new Set(discovery.stdout.split("\0").filter(Boolean))];
  const ancestors = new Set();
  for (const source of sources) {
    let directory = path.posix.dirname(source);
    while (directory !== ".") {
      ancestors.add(directory);
      directory = path.posix.dirname(directory);
    }
  }
  const nestedModules = (await Promise.all([...ancestors].map(async (directory) => {
    try {
      await readFile(path.join(workspace, directory, "go.mod"));
      return directory;
    } catch (error) {
      if (error.code === "ENOENT") return undefined;
      throw error;
    }
  }))).filter(Boolean);
  const mainSources = sources.filter((source) => !nestedModules.some((directory) => source.startsWith(`${directory}/`)));
  assert.ok(mainSources.length > 0);
  const patterns = declared.trim().split(/\s+/u);
  for (const source of mainSources) {
    assert.ok(patterns.some((pattern) => goLintPatternCoversSource(pattern, source)), `Go lint omits repository source ${source}`);
  }
  for (const pattern of patterns) {
    assert.ok(mainSources.some((source) => goLintPatternCoversSource(pattern, source)), `Go lint pattern has no repository-owned sources: ${pattern}`);
  }
});

function goLintPatternCoversSource(pattern, source) {
  const directory = path.posix.dirname(source);
  if (pattern === ".") return directory === ".";
  assert.match(pattern, /^\.\/[^\s]+\/\.\.\.$/u);
  const base = pattern.slice(2, -4);
  return directory === base || directory.startsWith(`${base}/`);
}

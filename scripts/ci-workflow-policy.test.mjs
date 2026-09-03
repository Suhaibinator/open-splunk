import assert from "node:assert/strict";
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

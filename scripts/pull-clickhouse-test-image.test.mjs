import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const workspace = process.cwd();
const pullScript = path.join(workspace, "scripts", "pull-clickhouse-test-image.sh");

function run(environment) {
  return new Promise((resolve, reject) => {
    const child = spawn(pullScript, [], {
      cwd: workspace,
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("exit", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

async function harness(t, failures) {
  const directory = await mkdtemp(path.join(tmpdir(), "open-splunk-clickhouse-pull-test-"));
  const attempts = path.join(directory, "attempts");
  const sleeps = path.join(directory, "sleeps");
  await writeFile(path.join(directory, "docker"), `#!/usr/bin/env bash
set -euo pipefail
count=0
if [[ -f "$FAKE_DOCKER_ATTEMPTS" ]]; then count="$(cat "$FAKE_DOCKER_ATTEMPTS")"; fi
count="$((count + 1))"
printf '%s' "$count" >"$FAKE_DOCKER_ATTEMPTS"
printf '%s\\n' "$*" >>"$FAKE_DOCKER_COMMANDS"
if [[ "$count" -le "$FAKE_DOCKER_FAILURES" ]]; then exit 1; fi
`, { mode: 0o755 });
  await writeFile(path.join(directory, "sleep"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$1" >>"$FAKE_SLEEP_CALLS"
`, { mode: 0o755 });
  await chmod(path.join(directory, "docker"), 0o755);
  await chmod(path.join(directory, "sleep"), 0o755);
  t.after(async () => { await rm(directory, { recursive: true, force: true }); });
  return {
    attempts,
    commands: path.join(directory, "commands"),
    sleeps,
    environment: {
      ...process.env,
      PATH: `${directory}${path.delimiter}${process.env.PATH}`,
      OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE: `clickhouse.example/test@sha256:${"a".repeat(64)}`,
      FAKE_DOCKER_ATTEMPTS: attempts,
      FAKE_DOCKER_COMMANDS: path.join(directory, "commands"),
      FAKE_DOCKER_FAILURES: String(failures),
      FAKE_SLEEP_CALLS: sleeps,
    },
  };
}

async function optionalFile(file) {
  try {
    return await readFile(file, "utf8");
  } catch (error) {
    if (error?.code === "ENOENT") return "";
    throw error;
  }
}

test("pulls the configured image once after immediate success", async (t) => {
  const fixture = await harness(t, 0);
  const result = await run(fixture.environment);
  assert.equal(result.code, 0, result.stderr);
  assert.equal(await readFile(fixture.attempts, "utf8"), "1");
  assert.equal(await readFile(fixture.commands, "utf8"), `pull clickhouse.example/test@sha256:${"a".repeat(64)}\n`);
  assert.equal(await optionalFile(fixture.sleeps), "");
});

test("retries twice with bounded backoff", async (t) => {
  const fixture = await harness(t, 2);
  const result = await run(fixture.environment);
  assert.equal(result.code, 0, result.stderr);
  assert.equal(await readFile(fixture.attempts, "utf8"), "3");
  assert.equal(await readFile(fixture.sleeps, "utf8"), "5\n10\n");
});

test("fails after the third pull attempt", async (t) => {
  const fixture = await harness(t, 3);
  const result = await run(fixture.environment);
  assert.equal(result.code, 1);
  assert.equal(await readFile(fixture.attempts, "utf8"), "3");
  assert.equal(await readFile(fixture.sleeps, "utf8"), "5\n10\n");
});

test("requires an explicit pinned image", async () => {
  const environment = { ...process.env };
  delete environment.OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE;
  const result = await run(environment);
  assert.notEqual(result.code, 0);
  assert.match(result.stderr, /OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE is required/);
});

test("rejects a mutable image tag", async () => {
  const result = await run({
    ...process.env,
    OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE: "clickhouse.example/test:latest",
  });
  assert.notEqual(result.code, 0);
  assert.match(result.stderr, /must use an exact sha256 digest/u);
});

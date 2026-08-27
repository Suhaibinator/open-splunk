import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";

const workspace = process.cwd();
const runner = path.join(workspace, "scripts", "run-development.sh");

test("development runner passes the complete host-server contract", async (t) => {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-development-runner-"));
  t.after(() => rm(fixture, { force: true, recursive: true }));
  const invocation = path.join(fixture, "invocation");
  const binary = path.join(fixture, "open-splunk-server");
  await writeFile(binary, `#!/bin/sh\nprintf '%s\\n' "$@" >${JSON.stringify(invocation)}\nprintf 'lock=%s\\n' "$OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH" >>${JSON.stringify(invocation)}\nprintf 'clickhouse_secret=%s\\n' "\${OPEN_SPLUNK_CLICKHOUSE_PASSWORD:-}" >>${JSON.stringify(invocation)}\nprintf 'administrator_token=%s\\n' "\${OPEN_SPLUNK_ADMINISTRATOR_TOKEN:-}" >>${JSON.stringify(invocation)}\n`);
  await chmod(binary, 0o755);

  const environment = path.join(fixture, "development.env");
  await writeFile(environment, [
    `OPEN_SPLUNK_DEVELOPMENT_SERVER_BINARY=${JSON.stringify(binary)}`,
    `OPEN_SPLUNK_DEVELOPMENT_STATE_ROOT=${JSON.stringify(path.join(fixture, "state"))}`,
    `OPEN_SPLUNK_DEVELOPMENT_EXPORT_ROOT=${JSON.stringify(path.join(fixture, "exports"))}`,
    "OPEN_SPLUNK_ADMINISTRATOR_TOKEN=administrator-secret",
    "OPEN_SPLUNK_CLICKHOUSE_PASSWORD=clickhouse-secret",
    "OPEN_SPLUNK_CLICKHOUSE_USERNAME=clickhouse",
    "OPEN_SPLUNK_CLICKHOUSE_ADDRESS=127.0.0.1:19000",
    "OPEN_SPLUNK_SERVER_HTTP_PORT=18080",
    "",
  ].join("\n"), { mode: 0o600 });

  const result = spawnSync("sh", [runner, environment], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /http:\/\/127\.0\.0\.1:18080\/signin\//);
  const argumentsText = await readFile(invocation, "utf8");
  for (const expected of [
    "-http-address",
    "127.0.0.1:18080",
    "-control-db",
    "open-splunk.db",
    "-master-key",
    "master.key",
    "-export-artifact-dir",
    "-clickhouse-address",
    "127.0.0.1:19000",
    "-clickhouse-username",
    "clickhouse",
    "lock=",
  ]) {
    assert.match(argumentsText, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  }
  assert.match(argumentsText, /^clickhouse_secret=clickhouse-secret$/m);
  assert.match(argumentsText, /^administrator_token=administrator-secret$/m);
  const privateDirectories = [
    path.join(fixture, "state", "private"),
    path.join(fixture, "exports", "private"),
  ];
  for (const info of await Promise.all(privateDirectories.map((directory) => stat(directory)))) {
    assert.equal(info.mode & 0o777, 0o700);
  }
});

test("development runner fails clearly before initialization", () => {
  const result = spawnSync("sh", [runner, "/definitely/missing/open-splunk.env"], {
    encoding: "utf8",
  });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /make dev-clickhouse/);
});

test("Make exposes the isolated persistent development lifecycle", async () => {
  const makefile = await readFile(path.join(workspace, "Makefile"), "utf8");
  assert.match(makefile, /^dev-clickhouse:/m);
  assert.match(makefile, /--project-name open-splunk-development/);
  assert.match(makefile, /docker-compose\.development\.yaml/);
  assert.match(makefile, /up --detach --wait clickhouse/);
  assert.match(makefile, /^dev-down:[\s\S]*?\$\(DEVELOPMENT_COMPOSE\) down$/m);
  assert.doesNotMatch(makefile.match(/^dev-down:[\s\S]*?\n\n/m)?.[0] ?? "", /--volumes/);
  assert.match(makefile, /^run: dev-tools dev-build-server$/m);
  assert.match(makefile, /development tools are missing; run 'make proto-tools' first/);
});

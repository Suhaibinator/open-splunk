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
  await writeFile(binary, `#!/bin/sh\nprintf 'args=%s\\n' "$*" >${JSON.stringify(invocation)}\nprintf 'listen=%s\\n' "$OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS" >>${JSON.stringify(invocation)}\nprintf 'control_database=%s\\n' "$OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE" >>${JSON.stringify(invocation)}\nprintf 'master_key=%s\\n' "$OPEN_SPLUNK_SERVER_MASTER_KEY_FILE" >>${JSON.stringify(invocation)}\nprintf 'lock=%s\\n' "$OPEN_SPLUNK_SERVER_LOCK_FILE" >>${JSON.stringify(invocation)}\nprintf 'exports=%s\\n' "$OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY" >>${JSON.stringify(invocation)}\nprintf 'clickhouse_address=%s\\n' "$OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS" >>${JSON.stringify(invocation)}\nprintf 'clickhouse_username=%s\\n' "$OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME" >>${JSON.stringify(invocation)}\nprintf 'clickhouse_secret=%s\\n' "$OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD" >>${JSON.stringify(invocation)}\nprintf 'administrator_token=%s\\n' "$OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN" >>${JSON.stringify(invocation)}\n`);
  await chmod(binary, 0o755);

  const environment = path.join(fixture, "development.env");
  await writeFile(environment, [
    `OPEN_SPLUNK_DEVELOPMENT_SERVER_BINARY=${JSON.stringify(binary)}`,
    `OPEN_SPLUNK_DEVELOPMENT_STATE_ROOT=${JSON.stringify(path.join(fixture, "state"))}`,
    `OPEN_SPLUNK_DEVELOPMENT_EXPORT_ROOT=${JSON.stringify(path.join(fixture, "exports"))}`,
    "OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN=administrator-secret",
    "OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD=clickhouse-secret",
    "OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME=clickhouse",
    "OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS=127.0.0.1:19000",
    "OPEN_SPLUNK_DEPLOY_HTTP_PORT=18080",
    "",
  ].join("\n"), { mode: 0o600 });

  const result = spawnSync("sh", [runner, environment], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /http:\/\/127\.0\.0\.1:18080\/signin\//);
  const argumentsText = await readFile(invocation, "utf8");
  for (const expected of [
    "args=",
    "listen=127.0.0.1:18080",
    "control_database=",
    "open-splunk.db",
    "master_key=",
    "master.key",
    "lock=",
    "open-splunk-server-open_splunk.server.lock",
    "exports=",
    "clickhouse_address=127.0.0.1:19000",
    "clickhouse_username=clickhouse",
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

test("make clean removes repository build outputs and restores the export placeholder", async () => {
  const makefile = await readFile(path.join(workspace, "Makefile"), "utf8");
  const clean = makefile.match(/^clean:\n(?:(?:\t|  ).*\n?)+/m)?.[0] ?? "";
  assert.match(clean, /\$\(GO_TOOL_ENV\) go clean/);
  assert.match(clean, /rm -rf -- build \.cache \.next node_modules\/\.cache out test-results coverage\.out/);
  assert.match(clean, /mkdir -p out/);
  assert.match(clean, /touch out\/\.gitkeep/);
});

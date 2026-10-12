import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { closeSync, openSync } from "node:fs";
import { chmod, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const workspace = process.cwd();
const runner = path.join(workspace, "scripts", "start-ci-native-services.sh");

async function fixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-native-ci-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const calls = path.join(root, "docker-calls");
  const docker = path.join(root, "docker");
  const toolScript = [
    "#!/bin/sh",
    'if [ "$1" = "--version" ]; then',
    '  [ "$CI_FAIL_CLIENT" != true ] || exit 1',
    '  echo "PostgreSQL) 18.0"',
    'elif [ "$1" = "/dev/fd/3" ]; then',
    '  cat "$1"',
    'else',
    '  [ "$PGPASSWORD" = connection-secret ] || exit 1',
    'fi', "",
  ].join("\n");
  const loaderScript = '#!/bin/sh\n[ "$1" = --library-path ] || exit 1\nshift 2\nexec "$@"\n';
  await writeFile(docker, `#!/usr/bin/env node
import fs from "node:fs";
const args = process.argv.slice(2);
fs.appendFileSync(process.env.CI_DOCKER_CALLS, JSON.stringify(args) + "\\n");
const mount = args.find((arg) => arg.startsWith("type=bind,src=") && arg.endsWith(",dst=/native-tools"));
if (mount) {
  const root = mount.slice("type=bind,src=".length, -",dst=/native-tools".length);
  fs.mkdirSync(root + "/lib", { recursive: true });
  fs.mkdirSync(root + "/usr/local/bin", { recursive: true });
  fs.writeFileSync(root + "/lib/ld-musl-test.so.1", ${JSON.stringify(loaderScript)}, { mode: 0o755 });
  for (const tool of ["pg_dump", "pg_restore"]) fs.writeFileSync(root + "/usr/local/bin/" + tool, ${JSON.stringify(toolScript)}, { mode: 0o755 });
}
`);
  const curl = path.join(root, "curl");
  await writeFile(curl, "#!/bin/sh\nexit 0\n");
  await chmod(docker, 0o755);
  await chmod(curl, 0o755);
  const environmentOutput = path.join(root, "environment");
  const pathOutput = path.join(root, "path");
  return {
    root, calls, environmentOutput, pathOutput,
    env: {
      ...process.env,
      PATH: `${root}:${process.env.PATH}`,
      GITHUB_ENV: environmentOutput,
      GITHUB_PATH: pathOutput,
      GITHUB_WORKSPACE: workspace,
      RUNNER_TEMP: root,
      CI_DOCKER_CALLS: calls,
      OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE: "approved.example/minio:fixture-2026",
    },
  };
}

test("native CI requires an approved exact MinIO image before touching Docker", async (t) => {
  const f = await fixture(t);
  for (const image of ["", "approved.example/minio", "approved.example/minio:latest", "approved.example/minio:", "approved.example/minio@sha256:bad", "--privileged", "bad image:tag"]) {
    const result = spawnSync("bash", [runner], {
      encoding: "utf8", env: { ...f.env, OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE: image },
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE/);
  }
  await assert.rejects(readFile(f.calls), { code: "ENOENT" });
});

test("native CI exports isolated credentials and matching PostgreSQL 18 tools", async (t) => {
  const f = await fixture(t);
  const result = spawnSync("bash", [runner], { encoding: "utf8", env: f.env });
  assert.equal(result.status, 0, result.stderr);
  const environment = await readFile(f.environmentOutput, "utf8");
  const variables = Object.fromEntries(environment.trim().split("\n").map((row) => {
    const separator = row.indexOf("=");
    return [row.slice(0, separator), row.slice(separator + 1)];
  }));
  assert.match(variables.OPEN_SPLUNK_TEST_POSTGRES_DSN, /^postgres:\/\/postgres:[a-f0-9]{64}@127\.0\.0\.1:55432\/postgres\?sslmode=disable$/);
  assert.match(variables.OPEN_SPLUNK_TEST_MINIO_SECRET_KEY, /^[a-f0-9]{64}$/);
  assert.equal(variables.OPEN_SPLUNK_TEST_MINIO_ENDPOINT, "127.0.0.1:59000");
  assert.equal(variables.OPEN_SPLUNK_TEST_MINIO_USE_TLS, "false");
  const tools = (await readFile(f.pathOutput, "utf8")).trim();
  assert.equal((await stat(tools)).mode & 0o777, 0o700);
  const toolNames = ["pg_dump", "pg_restore"];
  const toolModes = await Promise.all(toolNames.map((tool) => stat(path.join(tools, tool))));
  for (const mode of toolModes) assert.equal(mode.mode & 0o777, 0o700);
  for (const tool of toolNames) {
    const invocation = spawnSync(path.join(tools, tool), ["--file", path.join(f.root, "private backup.dump")], {
      encoding: "utf8", env: { ...f.env, PGPASSWORD: "connection-secret" },
    });
    assert.equal(invocation.status, 0, invocation.stderr);
  }
  const calls = (await readFile(f.calls, "utf8")).trim().split("\n").map(JSON.parse);
  const pgImage = calls.find((args) => args[0] === "pull" && args[1].startsWith("postgres:"))[1];
  assert.match(pgImage, /^postgres:18-alpine@sha256:[a-f0-9]{64}$/);
  const serverRuns = calls.filter((args) => args.includes("--detach"));
  assert.equal(serverRuns.length, 2);
  assert.ok(serverRuns.some((args) => args.includes("127.0.0.1:55432:5432")));
  assert.ok(serverRuns.some((args) => args.includes("127.0.0.1:59000:9000")));
  const extraction = calls.find((args) => args.includes("--interactive"));
  assert.ok(extraction.includes(pgImage));
  assert.ok(extraction.includes("--user"));
  assert.ok(extraction.includes(`type=bind,src=${tools}/postgres-tools,dst=/native-tools`));
  assert.doesNotMatch(JSON.stringify(calls), /connection-secret/);
  assert.doesNotMatch(JSON.stringify(calls), new RegExp(variables.OPEN_SPLUNK_TEST_MINIO_SECRET_KEY));
  const archive = path.join(f.root, "pinned-archive");
  await writeFile(archive, "pinned descriptor content");
  const descriptor = openSync(archive, "r");
  try {
    const inspection = spawnSync(path.join(tools, "pg_restore"), ["/dev/fd/3"], {
      encoding: "utf8", env: f.env, stdio: ["ignore", "pipe", "pipe", descriptor],
    });
    assert.equal(inspection.status, 0, inspection.stderr);
    assert.equal(inspection.stdout, "pinned descriptor content");
  } finally {
    closeSync(descriptor);
  }
  const afterInspection = await readFile(f.calls, "utf8");
  assert.equal(afterInspection.trim().split("\n").length, calls.length, "native tools must execute on the host to preserve inherited descriptors");
  assert.equal(calls.some((args) => args[0] === "rm"), false);
});

test("native CI removes its disposable services if client qualification fails", async (t) => {
  const f = await fixture(t);
  const result = spawnSync("bash", [runner], {
    encoding: "utf8", env: { ...f.env, CI_FAIL_CLIENT: "true" },
  });
  assert.notEqual(result.status, 0);
  const calls = (await readFile(f.calls, "utf8")).trim().split("\n").map(JSON.parse);
  const cleanup = calls.find((args) => args[0] === "rm");
  assert.ok(cleanup);
  assert.equal(cleanup[1], "--force");
  assert.ok(cleanup[2].startsWith("open-splunk-ci-postgres-"));
  assert.ok(cleanup[3].startsWith("open-splunk-ci-minio-"));
  await assert.rejects(readFile(f.environmentOutput), { code: "ENOENT" });
});

test("every native CI test job prepares services before its Go toolchain", async () => {
  const workflow = await readFile(path.join(workspace, ".github", "workflows", "ci.yml"), "utf8");
  const jobs = ["go-test", "knowledge-object-fuzz", "gradethis-corpus", "knowledge-runtime-clickhouse", "spl-pipeline-clickhouse", "hec-durable-load", "backend-vertical", "release-oci"];
  for (const job of jobs) {
    const start = workflow.indexOf(`\n  ${job}:\n`);
    assert.ok(start >= 0, job);
    const nextJob = workflow.slice(start + 1).search(/\n  [a-z][a-z-]+:\n/);
    const body = workflow.slice(start, nextJob < 0 ? undefined : start + 1 + nextJob);
    assert.match(body, /OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE: \$\{\{ vars\.OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE \}\}/);
    assert.ok(body.indexOf("run: scripts/start-ci-native-services.sh") < body.indexOf("uses: actions/setup-go@"), job);
  }
});

import assert from "node:assert/strict";
import {
  access,
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { constants } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import { spawn, spawnSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

const workspace = process.cwd();

async function executable(file, contents) {
  await writeFile(file, contents);
  await chmod(file, 0o755);
}

async function createFixture(t, prefix) {
  const fixture = await mkdtemp(path.join(tmpdir(), prefix));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const fakeBin = path.join(fixture, "fake-bin");
  const nodeBin = path.join(fixture, "node_modules", ".bin");
  const goBin = path.join(fixture, ".cache", "proto-tools");
  const directories = [
    path.join(fixture, "scripts"),
    path.join(fixture, "proto", "open_splunk"),
    path.join(fixture, "gen", "go", "open_splunk"),
    path.join(fixture, "gen", "ts", "open_splunk"),
    fakeBin,
    nodeBin,
    goBin,
  ];
  await Promise.all(directories.map((directory) => mkdir(directory, { recursive: true })));
  await copyFile(
    path.join(workspace, "scripts", "compile-protos.sh"),
    path.join(fixture, "scripts", "compile-protos.sh"),
  );
  await writeFile(
    path.join(fixture, "proto", "open_splunk", "contract.proto"),
    'syntax = "proto3";\n',
  );
  await writeFile(path.join(fixture, "buf.gen.yaml"), "version: v2\nplugins: []\n");

  const oldGo = path.join(fixture, "gen", "go", "open_splunk", "contract.pb.go");
  const oldTypeScript = path.join(fixture, "gen", "ts", "open_splunk", "contract.ts");
  await writeFile(oldGo, "previous Go output\n");
  await writeFile(oldTypeScript, "previous TypeScript output\n");
  await executable(
    path.join(nodeBin, "protoc-gen-ts_proto"),
    "#!/usr/bin/env bash\nexit 0\n",
  );
  await Promise.all(["protoc-gen-go", "protoc-gen-go-grpc"].map((plugin) =>
    executable(path.join(goBin, plugin), "#!/usr/bin/env bash\nexit 0\n")));
  return { fixture, fakeBin, nodeBin, oldGo, oldTypeScript };
}

function compilerEnvironment({ fixture, fakeBin }, extraEnvironment = {}) {
  return {
    ...process.env,
    BUF_CACHE_DIR: path.join(fixture, ".cache", "buf"),
    PATH: `${fakeBin}${path.delimiter}${process.env.PATH ?? ""}`,
    ...extraEnvironment,
  };
}

function runCompiler(fixture, extraEnvironment = {}) {
  return spawnSync(
    "bash",
    [path.join(fixture.fixture, "scripts", "compile-protos.sh")],
    {
      cwd: tmpdir(),
      encoding: "utf8",
      env: compilerEnvironment(fixture, extraEnvironment),
    },
  );
}

function startCompiler(fixture, extraEnvironment = {}) {
  const child = spawn(
    "bash",
    [path.join(fixture.fixture, "scripts", "compile-protos.sh")],
    {
      cwd: tmpdir(),
      env: compilerEnvironment(fixture, extraEnvironment),
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const completed = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (status, signal) => {
      resolve({ status, signal, stdout, stderr });
    });
  });
  return { child, completed };
}

async function waitForPath(target) {
  // Poll serially so the timeout and filesystem observation remain ordered.
  /* eslint-disable no-await-in-loop */
  for (let attempt = 0; attempt < 500; attempt += 1) {
    try {
      await access(target, constants.F_OK);
      return;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    await delay(10);
  }
  /* eslint-enable no-await-in-loop */
  throw new Error(`timed out waiting for ${target}`);
}

async function assertOldOutputs(fixture) {
  assert.equal(await readFile(fixture.oldGo, "utf8"), "previous Go output\n");
  assert.equal(await readFile(fixture.oldTypeScript, "utf8"), "previous TypeScript output\n");
  assert.deepEqual(
    (await readdir(path.join(fixture.fixture, "gen"))).toSorted(),
    ["go", "ts"],
    "protobuf transaction files must be cleaned",
  );
}

async function installSuccessfulBuf(fixture) {
  await executable(
    path.join(fixture.nodeBin, "buf"),
    `#!/usr/bin/env node
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
if (process.env.PROTO_TEST_HOLD_READY) {
  writeFileSync(process.env.PROTO_TEST_HOLD_READY, "ready\\n");
  for (let attempt = 0; attempt < 1000; attempt += 1) {
    if (existsSync(process.env.PROTO_TEST_HOLD_RELEASE)) break;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
  if (!existsSync(process.env.PROTO_TEST_HOLD_RELEASE)) process.exit(75);
}
const outputIndex = process.argv.indexOf("--output");
if (outputIndex < 0 || outputIndex + 1 >= process.argv.length) process.exit(64);
const output = process.argv[outputIndex + 1];
const goDirectory = path.join(output, "gen", "go", "open_splunk");
const tsDirectory = path.join(output, "gen", "ts", "open_splunk");
mkdirSync(goDirectory, { recursive: true });
mkdirSync(tsDirectory, { recursive: true });
writeFileSync(path.join(goDirectory, "contract.pb.go"), "replacement Go output\\n");
writeFileSync(path.join(tsDirectory, "contract.ts"), "replacement TypeScript output\\n");
for (const relativePath of [
  "google/protobuf/duration.ts",
  "google/protobuf/field_mask.ts",
  "google/protobuf/timestamp.ts",
  "index.google.protobuf.ts",
  "index.google.ts",
  "index.open_splunk.ts",
  "index.ts",
]) {
  const destination = path.join(output, "gen", "ts", relativePath);
  mkdirSync(path.dirname(destination), { recursive: true });
  writeFileSync(destination, "replacement support output\\n");
}
`,
  );
}

async function installFailingMove(fixture) {
  await executable(
    path.join(fixture.fakeBin, "mv"),
    `#!/usr/bin/env bash
set -euo pipefail
count=0
if [[ -f "\${PROTO_TEST_MOVE_COUNT}" ]]; then
  read -r count < "\${PROTO_TEST_MOVE_COUNT}"
fi
count=$((count + 1))
printf '%s\\n' "\${count}" > "\${PROTO_TEST_MOVE_COUNT}"
case ",\${PROTO_TEST_MOVE_FAILURES}," in
  *,\${count},*) exit 73 ;;
esac
if [[ "\${PROTO_TEST_MOVE_HOLD_AT:-}" == "\${count}" ]]; then
  /bin/mv "$@"
  printf 'ready\\n' > "\${PROTO_TEST_MOVE_HOLD_READY}"
  for _ in {1..1000}; do
    if [[ -f "\${PROTO_TEST_MOVE_HOLD_RELEASE}" ]]; then break; fi
    sleep 0.01
  done
  test -f "\${PROTO_TEST_MOVE_HOLD_RELEASE}"
  exit 0
fi
exec /bin/mv "$@"
`,
  );
}

test("compile-protos preserves published outputs when generation fails", async (t) => {
  if (process.platform === "win32") {
    t.skip("the protobuf compiler entrypoint is a bash script");
    return;
  }

  const fixture = await createFixture(t, "open-splunk-proto-generate-failure-");
  await executable(
    path.join(fixture.nodeBin, "buf"),
    "#!/usr/bin/env bash\nexit 37\n",
  );

  const result = runCompiler(fixture);

  assert.equal(result.status, 37, result.stderr);
  await assertOldOutputs(fixture);
});

test("compile-protos replaces generated files while preserving handwritten files", async (t) => {
  if (process.platform === "win32") {
    t.skip("the protobuf compiler entrypoint is a bash script");
    return;
  }

  const fixture = await createFixture(t, "open-splunk-proto-success-");
  await installSuccessfulBuf(fixture);
  const goReadme = path.join(fixture.fixture, "gen", "go", "README.md");
  const typeScriptReadme = path.join(
    fixture.fixture,
    "gen",
    "ts",
    "README.md",
  );
  await writeFile(goReadme, "handwritten Go guidance\n");
  await writeFile(typeScriptReadme, "handwritten TypeScript guidance\n");

  const result = runCompiler(fixture);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(await readFile(fixture.oldGo, "utf8"), "replacement Go output\n");
  assert.equal(
    await readFile(fixture.oldTypeScript, "utf8"),
    "replacement TypeScript output\n",
  );
  assert.equal(await readFile(goReadme, "utf8"), "handwritten Go guidance\n");
  assert.equal(
    await readFile(typeScriptReadme, "utf8"),
    "handwritten TypeScript guidance\n",
  );
  assert.deepEqual(
    (await readdir(path.join(fixture.fixture, "gen"))).toSorted(),
    ["go", "ts"],
  );
});

test("concurrent protobuf generation fails without mixing output trees", async (t) => {
  if (process.platform === "win32") {
    t.skip("the protobuf compiler entrypoint is a bash script");
    return;
  }

  const fixture = await createFixture(t, "open-splunk-proto-concurrent-");
  await installSuccessfulBuf(fixture);
  const holdReady = path.join(fixture.fixture, "hold-ready");
  const holdRelease = path.join(fixture.fixture, "hold-release");
  const first = startCompiler(fixture, {
    PROTO_TEST_HOLD_READY: holdReady,
    PROTO_TEST_HOLD_RELEASE: holdRelease,
  });
  let second;
  let firstResult;
  try {
    await waitForPath(holdReady);
    second = runCompiler(fixture);
  } finally {
    await writeFile(holdRelease, "release\n");
    firstResult = await first.completed;
  }

  assert.equal(firstResult.status, 0, firstResult.stderr);
  assert.equal(second.status, 1);
  assert.match(second.stderr, /another protobuf generation is running/);
  assert.equal(await readFile(fixture.oldGo, "utf8"), "replacement Go output\n");
  assert.equal(
    await readFile(fixture.oldTypeScript, "utf8"),
    "replacement TypeScript output\n",
  );
  await assert.rejects(
    access(
      path.join(fixture.fixture, "gen", ".proto-generation.lock"),
      constants.F_OK,
    ),
  );
});

test("protobuf lock precedes inspection during another publisher's directory swap", async (t) => {
  if (process.platform === "win32") {
    t.skip("the protobuf compiler entrypoint is a bash script");
    return;
  }

  const fixture = await createFixture(t, "open-splunk-proto-swap-concurrent-");
  await installSuccessfulBuf(fixture);
  await installFailingMove(fixture);
  const holdReady = path.join(fixture.fixture, "move-hold-ready");
  const holdRelease = path.join(fixture.fixture, "move-hold-release");
  const first = startCompiler(fixture, {
    PROTO_TEST_MOVE_COUNT: path.join(fixture.fixture, "move-count"),
    PROTO_TEST_MOVE_FAILURES: "",
    PROTO_TEST_MOVE_HOLD_AT: "1",
    PROTO_TEST_MOVE_HOLD_READY: holdReady,
    PROTO_TEST_MOVE_HOLD_RELEASE: holdRelease,
  });
  let second;
  let firstResult;
  try {
    await waitForPath(holdReady);
    second = runCompiler(fixture);
  } finally {
    await writeFile(holdRelease, "release\n");
    firstResult = await first.completed;
  }

  assert.equal(firstResult.status, 0, firstResult.stderr);
  assert.equal(second.status, 1);
  assert.match(second.stderr, /another protobuf generation is running/);
  assert.equal(await readFile(fixture.oldGo, "utf8"), "replacement Go output\n");
  assert.equal(
    await readFile(fixture.oldTypeScript, "utf8"),
    "replacement TypeScript output\n",
  );
  assert.deepEqual(
    (await readdir(path.join(fixture.fixture, "gen"))).toSorted(),
    ["go", "ts"],
  );
});

test("compile-protos rolls back both trees when the final publish move fails", async (t) => {
  if (process.platform === "win32") {
    t.skip("the protobuf compiler entrypoint is a bash script");
    return;
  }

  const fixture = await createFixture(t, "open-splunk-proto-publish-failure-");
  await installSuccessfulBuf(fixture);
  const moveCount = path.join(fixture.fixture, "move-count");
  await installFailingMove(fixture);

  const result = runCompiler(fixture, {
    PROTO_TEST_MOVE_COUNT: moveCount,
    PROTO_TEST_MOVE_FAILURES: "4",
  });

  assert.equal(result.status, 73, result.stderr);
  await assertOldOutputs(fixture);
});

test("compile-protos preserves recovery data when rollback itself fails", async (t) => {
  if (process.platform === "win32") {
    t.skip("the protobuf compiler entrypoint is a bash script");
    return;
  }

  const fixture = await createFixture(t, "open-splunk-proto-rollback-failure-");
  await installSuccessfulBuf(fixture);
  await installFailingMove(fixture);
  const result = runCompiler(fixture, {
    PROTO_TEST_MOVE_COUNT: path.join(fixture.fixture, "move-count"),
    PROTO_TEST_MOVE_FAILURES: "4,5",
  });

  assert.equal(result.status, 73, result.stderr);
  assert.match(result.stderr, /rollback is incomplete; preserved recovery data at /);
  const transaction = (await readdir(path.join(fixture.fixture, "gen")))
    .find((entry) => entry.startsWith(".proto-generation."));
  assert.ok(transaction, "failed rollback must preserve its transaction");
  assert.equal(
    await readFile(
      path.join(
        fixture.fixture,
        "gen",
        transaction,
        "old-ts",
        "open_splunk",
        "contract.ts",
      ),
      "utf8",
    ),
    "previous TypeScript output\n",
  );
  assert.equal(await readFile(fixture.oldGo, "utf8"), "previous Go output\n");
});

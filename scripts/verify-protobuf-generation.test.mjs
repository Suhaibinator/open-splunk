import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execute = promisify(execFile);
const workspace = process.cwd();
const verifier = path.join(workspace, "scripts", "verify-protobuf-generation.sh");

async function fixture(t, makeBody) {
  const directory = await mkdtemp(path.join(tmpdir(), "open-splunk-protobuf-verify-test-"));
  await mkdir(path.join(directory, "proto"));
  await mkdir(path.join(directory, "gen"));
  await writeFile(path.join(directory, "proto", "schema.proto"), "edited schema\n");
  await writeFile(path.join(directory, "gen", "schema.pb.go"), "edited output\n");
  await writeFile(path.join(directory, "make"), `#!/usr/bin/env bash
set -euo pipefail
${makeBody}
`);
  await chmod(path.join(directory, "make"), 0o755);
  t.after(async () => { await rm(directory, { recursive: true, force: true }); });
  try {
    const result = await execute(verifier, [], {
      cwd: directory,
      env: { ...process.env, PATH: `${directory}${path.delimiter}${process.env.PATH}` },
    });
    return { ...result, code: 0 };
  } catch (error) {
    return { code: error.code, stderr: error.stderr, stdout: error.stdout };
  }
}

test("accepts already-edited protobuf sources and matching generated output", async (t) => {
  const result = await fixture(t, ":");
  assert.equal(result.code, 0);
});

test("rejects generation that changes the current protobuf snapshot", async (t) => {
  const result = await fixture(t, "printf '%s\\n' changed >gen/schema.pb.go");
  assert.notEqual(result.code, 0);
  assert.match(result.stdout, /edited output/u);
  assert.match(result.stdout, /changed/u);
});

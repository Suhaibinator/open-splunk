import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

test("runtime config digest ignores changing attestations but detects config and filesystem changes", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "oci-config-fixture-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const binary = path.join(root, "bin");
  await mkdir(binary);
  await writeFile(path.join(binary, "docker"), '#!/bin/sh\nset -eu\ntest "$1 $2 $3 $4 $5" = "image save --platform linux/arm64 --output"\ncp "$OCI_TEST_ARCHIVE" "$6"\n');
  await chmod(path.join(binary, "docker"), 0o755);
  const run = async (config, attestation, legacy = false) => {
    const content = JSON.stringify(config);
    const digest = createHash("sha256").update(content).digest("hex");
    await mkdir(path.join(root, "blobs", "sha256"), { recursive: true });
    const member = legacy ? `${digest}.json` : `blobs/sha256/${digest}`;
    await writeFile(path.join(root, member), content);
    await writeFile(path.join(root, "manifest.json"), JSON.stringify([{ Config: member }]));
    await writeFile(path.join(root, "index.json"), JSON.stringify({ attestation }));
    const archive = path.join(root, "image.tar");
    execFileSync("tar", ["-cf", archive, "-C", root, "manifest.json", "index.json", member]);
    return spawnSync(process.execPath, ["scripts/oci-runtime-config-digest.mjs", "linux/arm64", "fixture:image"], {
      cwd: process.cwd(), encoding: "utf8",
      env: { ...process.env, PATH: `${binary}:${process.env.PATH}`, OCI_TEST_ARCHIVE: archive },
    });
  };
  const config = { os: "linux", architecture: "arm64", config: { User: "65532:65532" }, rootfs: { type: "layers", diff_ids: [`sha256:${"a".repeat(64)}`] } };
  const first = await run(config, "first");
  assert.equal(first.status, 0, first.stderr);
  assert.equal((await run(config, "second")).stdout, first.stdout);
  assert.equal((await run(config, "legacy", true)).stdout, first.stdout);
  assert.notEqual((await run({ ...config, config: { User: "0" } }, "first")).stdout, first.stdout);
  assert.notEqual((await run({ ...config, rootfs: { type: "layers", diff_ids: [`sha256:${"b".repeat(64)}`] } }, "first")).stdout, first.stdout);
  assert.notEqual((await run({ ...config, architecture: "amd64" }, "first")).status, 0);
});

import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { cleanUIOutput } from "./build-ui-output.mjs";

test("cleanUIOutput replaces stale entries with a canonical placeholder", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-ui-clean-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const output = path.join(root, "out");
  await mkdir(path.join(output, "stale"), { recursive: true });
  await writeFile(path.join(output, "stale", "bundle.js"), "stale");
  await writeFile(path.join(output, ".gitkeep"), "\r\n");

  await cleanUIOutput(output);

  assert.equal(await readFile(path.join(output, ".gitkeep"), "utf8"), "\n");
  await assert.rejects(readFile(path.join(output, "stale", "bundle.js")), { code: "ENOENT" });
});

test("cleanUIOutput rejects a symlink without touching its target", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-ui-symlink-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const target = path.join(root, "valuable");
  const output = path.join(root, "out");
  await mkdir(target);
  const valuable = path.join(target, "keep.txt");
  await writeFile(valuable, "keep");
  try {
    await symlink(target, output, "dir");
  } catch (error) {
    t.skip(`symbolic links are unavailable: ${error}`);
    return;
  }

  await assert.rejects(cleanUIOutput(output), /symbolic-link UI output/);
  assert.equal(await readFile(valuable, "utf8"), "keep");
});

test("cleanUIOutput restores a rejected non-directory output", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-ui-file-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const output = path.join(root, "out");
  await writeFile(output, "valuable output\n");

  await assert.rejects(cleanUIOutput(output), /non-directory UI output/);
  assert.equal(await readFile(output, "utf8"), "valuable output\n");
});

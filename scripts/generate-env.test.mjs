import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { chmod, copyFile, lstat, mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";

const workspace = process.cwd();

async function fixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-generate-env-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const deploy = path.join(root, "deploy");
  await mkdir(deploy);
  await chmod(deploy, 0o755);
  await Promise.all(["generate-env.sh", ".env.example"].map((name) =>
    copyFile(path.join(workspace, "deploy", name), path.join(deploy, name))));
  return { root, deploy, generator: path.join(deploy, "generate-env.sh") };
}

function invocation(generator, mode, output, umask = "022") {
  return ["-c", 'umask "$1"; shift; exec sh "$@"', "generate-env", umask,
    generator, mode, ...(output === undefined ? [] : [output])];
}

function run(generator, mode, output, options = {}) {
  return spawnSync("sh", invocation(generator, mode, output, options.umask), {
    cwd: options.cwd,
    env: { ...process.env, ...options.env },
    encoding: "utf8",
    timeout: 30_000,
  });
}

function runAsync(generator, mode, output) {
  return new Promise((resolve, reject) => {
    const child = spawn("sh", invocation(generator, mode, output, "000"), {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (data) => { stdout += data; });
    child.stderr.on("data", (data) => { stderr += data; });
    child.on("error", reject);
    child.on("close", (status) => resolve({ status, stdout, stderr }));
  });
}

for (const mode of ["--production", "--development"]) {
  for (const umask of ["000", "022", "777"]) {
    test(`${mode} initializes an owner-only file under umask ${umask}`, async (t) => {
      const { deploy, generator } = await fixture(t);
      const result = run(generator, mode, undefined, { umask });
      assert.equal(result.status, 0, result.stderr);
      assert.equal(result.stdout, "");
      assert.equal(result.stderr, "");
      const output = path.join(deploy, mode === "--production" ? ".env" : ".env.development");
      const info = await stat(output);
      assert.equal(info.mode & 0o7777, 0o600);
      assert.equal(info.uid, process.getuid());
      assert.equal(info.nlink, 1);
      assert.equal((await stat(deploy)).mode & 0o777, mode === "--production" ? 0o755 : 0o700);
      const contents = await readFile(output, "utf8");
      if (mode === "--production") {
        assert.equal(contents, await readFile(path.join(deploy, ".env.example"), "utf8"));
      } else {
        assert.match(contents, /^OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD=[0-9a-f]{64}$/m);
        assert.match(contents, /^OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN=[A-Za-z0-9+/]{64}$/m);
      }
      assert.equal((await readdir(deploy)).some((name) => name.startsWith(".open-splunk-env.")), false);
    });
  }

  test(`${mode} preserves an existing environment and rejects other destination entries`, async (t) => {
    const { root, generator } = await fixture(t);
    const output = path.join(root, "existing.env");
    await writeFile(output, "existing-settings-and-credentials\n", { mode: 0o600 });
    const before = await stat(output);
    assert.notEqual(run(generator, mode, output).status, 0);
    assert.equal(await readFile(output, "utf8"), "existing-settings-and-credentials\n");
    assert.equal((await stat(output)).ino, before.ino);
    await Promise.all([["dangling.env", "missing"], ["linked.env", output], ["directory-link.env", root]].map(async ([name, target]) => {
      const link = path.join(root, name);
      await symlink(target, link);
      assert.notEqual(run(generator, mode, link).status, 0);
      assert.equal((await lstat(link)).isSymbolicLink(), true);
    }));
    const directory = path.join(root, "directory.env");
    await mkdir(directory);
    assert.notEqual(run(generator, mode, directory).status, 0);
    assert.deepEqual(await readdir(directory), []);
  });
}

test("production creation supports another project's relative path without OpenSSL", async (t) => {
  const { root, generator } = await fixture(t);
  const bin = path.join(root, "bin");
  await mkdir(bin);
  await writeFile(path.join(bin, "openssl"), "#!/bin/sh\nexit 99\n", { mode: 0o755 });
  const result = run(generator, "--production", "project with spaces/.env", {
    cwd: root,
    env: { PATH: `${bin}:${process.env.PATH}` },
    umask: "000",
  });
  assert.equal(result.status, 0, result.stderr);
  assert.equal((await stat(path.join(root, "project with spaces", ".env"))).mode & 0o777, 0o600);
  assert.equal((await stat(path.join(root, "project with spaces"))).mode & 0o777, 0o700);
});

test("concurrent setup publishes exactly one complete private file", async (t) => {
  const { root, deploy, generator } = await fixture(t);
  const output = path.join(deploy, ".env");
  const results = await Promise.all(Array.from({ length: 12 }, () => runAsync(generator, "--production", output)));
  assert.equal(results.filter((result) => result.status === 0).length, 1);
  assert.equal(results.every((result) => result.stdout === ""), true);
  assert.equal(await readFile(output, "utf8"), await readFile(path.join(deploy, ".env.example"), "utf8"));
  assert.equal((await stat(output)).mode & 0o777, 0o600);
  assert.equal((await stat(output)).nlink, 1);
  assert.deepEqual((await readdir(deploy)).toSorted(), [".env", ".env.example", "generate-env.sh"]);
  assert.deepEqual((await readdir(root)).toSorted(), ["deploy"]);
});

for (const entry of ["file", "symlink", "directory"]) {
  test(`publication refuses a ${entry} appearing after staging`, async (t) => {
    const { root, deploy, generator } = await fixture(t);
    const bin = path.join(root, "bin");
    const other = path.join(root, "other");
    await mkdir(bin);
    await mkdir(other);
    const realLn = spawnSync("sh", ["-c", "command -v ln"], { encoding: "utf8" });
    assert.equal(realLn.status, 0, realLn.stderr);
    await writeFile(path.join(bin, "ln"), `#!/bin/sh
set -eu
destination="$2/$(basename -- "$1")"
case "$TEST_ENTRY" in
    file) printf 'concurrent-settings\\n' >"$destination" ;;
    symlink) "$TEST_REAL_LN" -s "$TEST_OTHER" "$destination" ;;
    directory) mkdir "$destination" ;;
esac
exec "$TEST_REAL_LN" "$@"
`, { mode: 0o755 });
    const result = run(generator, "--production", undefined, { env: {
      PATH: `${bin}:${process.env.PATH}`,
      TEST_ENTRY: entry,
      TEST_OTHER: other,
      TEST_REAL_LN: realLn.stdout.trim(),
    } });
    assert.notEqual(result.status, 0);
    assert.equal(result.stdout, "");
    const output = path.join(deploy, ".env");
    if (entry === "file") assert.equal(await readFile(output, "utf8"), "concurrent-settings\n");
    if (entry === "symlink") assert.equal((await lstat(output)).isSymbolicLink(), true);
    if (entry === "directory") assert.deepEqual(await readdir(output), []);
    assert.deepEqual(await readdir(other), []);
    assert.equal((await readdir(deploy)).some((name) => name.startsWith(".open-splunk-env.")), false);
  });
}

test("a failed template copy leaves no output or staging files", async (t) => {
  const { root, deploy, generator } = await fixture(t);
  const bin = path.join(root, "bin");
  await mkdir(bin);
  await writeFile(path.join(bin, "cat"), "#!/bin/sh\nprintf 'partial-template\\n'\nexit 1\n", { mode: 0o755 });
  const result = run(generator, "--production", undefined, { env: { PATH: `${bin}:${process.env.PATH}` } });
  assert.notEqual(result.status, 0);
  assert.equal(result.stdout, "");
  assert.deepEqual((await readdir(deploy)).toSorted(), [".env.example", "generate-env.sh"]);
});

test("documented upgrade restricts permissions without replacing credentials", async (t) => {
  const { deploy } = await fixture(t);
  const readme = await readFile(path.join(workspace, "deploy", "README.md"), "utf8");
  assert.doesNotMatch(readme, /\bcp\s+\.env\.example\s+\.env\b/);
  assert.match(readme, /\.\/generate-env\.sh --production/);
  const upgrade = readme.match(/### Existing environment files and upgrades[\s\S]*?```sh\n([\s\S]*?)```/u)?.[1];
  assert.ok(upgrade);
  const output = path.join(deploy, ".env");
  const contents = "OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD=retained-example-password\nOPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN=retained-example-token\n";
  await writeFile(output, contents);
  await chmod(output, 0o644);
  const before = await stat(output);
  const result = spawnSync("sh", ["-eu", "-c", upgrade], { cwd: deploy, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const after = await stat(output);
  assert.equal(after.mode & 0o777, 0o600);
  assert.equal(after.uid, before.uid);
  assert.equal(after.ino, before.ino);
  assert.equal(await readFile(output, "utf8"), contents);
  assert.equal(result.stdout.includes("retained-example"), false);
});

test("documented upgrade refuses a symlink without changing its target", async (t) => {
  const { root, deploy } = await fixture(t);
  const readme = await readFile(path.join(workspace, "deploy", "README.md"), "utf8");
  const upgrade = readme.match(/### Existing environment files and upgrades[\s\S]*?```sh\n([\s\S]*?)```/u)?.[1];
  assert.ok(upgrade);
  const target = path.join(root, "other.env");
  await writeFile(target, "other-settings\n");
  await chmod(target, 0o644);
  await symlink(target, path.join(deploy, ".env"));
  const result = spawnSync("sh", ["-c", upgrade], { cwd: deploy, encoding: "utf8" });
  assert.notEqual(result.status, 0);
  assert.equal((await stat(target)).mode & 0o777, 0o644);
  assert.equal(await readFile(target, "utf8"), "other-settings\n");
});

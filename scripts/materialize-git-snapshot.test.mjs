import assert from "node:assert/strict";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";
import test from "node:test";

const workspace = process.cwd();

function git(fixture, args, options = {}) {
  const result = spawnSync("git", ["-C", fixture, ...args], {
    encoding: "utf8",
    ...options,
  });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}

async function snapshotFixture(t) {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-snapshot-"));
  const destination = `${fixture}-output`;
  t.after(() => rm(fixture, { recursive: true, force: true }));
  t.after(() => rm(destination, { recursive: true, force: true }));
  await mkdir(path.join(fixture, "scripts"));
  await mkdir(path.join(fixture, "lib", "api"), { recursive: true });
  await mkdir(path.join(fixture, "public"));
  await copyFile(
    path.join(workspace, "scripts", "materialize-git-snapshot.mjs"),
    path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
  );
  await writeFile(path.join(fixture, ".gitignore"), "ignored/\n");
  await writeFile(path.join(fixture, "lib", "api", "index.ts"), "committed api\n");
  await writeFile(path.join(fixture, "public", ".gitkeep"), "");
  await writeFile(path.join(fixture, "tool.sh"), "#!/usr/bin/env bash\nexit 0\n");
  await chmod(path.join(fixture, "tool.sh"), 0o755);
  git(fixture, ["init", "--quiet"]);
  git(fixture, ["config", "user.email", "tests@open-splunk.invalid"]);
  git(fixture, ["config", "user.name", "Open Splunk Tests"]);
  git(fixture, ["add", "."]);
  git(fixture, ["commit", "--quiet", "-m", "fixture"]);
  return { fixture, destination };
}

function materialize(fixture, destination, options = {}) {
  return spawnSync(
    process.execPath,
    [
      path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
      fixture,
      "HEAD",
      destination,
    ],
    { cwd: tmpdir(), encoding: "utf8", ...options },
  );
}

test("materializer uses raw committed bytes and modes only", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  await writeFile(path.join(fixture, ".git", "info", "exclude"), "lib/api.ts\n");
  await writeFile(path.join(fixture, "lib", "api.ts"), "hidden resolver override\n");
  await writeFile(path.join(fixture, "lib", "api", "index.ts"), "dirty worktree api\n");
  await mkdir(path.join(fixture, "ignored"));
  await writeFile(path.join(fixture, "ignored", "source.go"), "package poison\n");

  const result = materialize(fixture, destination);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(destination, "lib", "api", "index.ts"), "utf8"),
    "committed api\n",
  );
  await assert.rejects(readFile(path.join(destination, "lib", "api.ts")));
  await assert.rejects(readFile(path.join(destination, "ignored", "source.go")));
  await assert.rejects(stat(path.join(destination, ".git")));
  assert.equal((await stat(path.join(destination, "tool.sh"))).mode & 0o777, 0o755);
  assert.equal(
    (await stat(path.join(destination, "lib", "api", "index.ts"))).mode & 0o777,
    0o644,
  );
});

function commitPublicTree(fixture, namedEntries, message) {
  const existingBlob = git(fixture, ["rev-parse", "HEAD:public/.gitkeep"]);
  const records = [
    { mode: "100644", object: existingBlob, name: ".gitkeep" },
    ...namedEntries,
  ].toSorted((left, right) =>
    Buffer.compare(Buffer.from(left.name), Buffer.from(right.name)),
  );
  const publicTree = git(
    fixture,
    ["mktree", "-z"],
    {
      input: Buffer.from(
        records
          .map(
            (entry) =>
              `${entry.mode} blob ${entry.object}\t${entry.name}\0`,
          )
          .join(""),
      ),
    },
  );
  const rootEntries = git(fixture, ["ls-tree", "HEAD"])
    .split("\n")
    .map((line) =>
      line.endsWith("\tpublic")
        ? `040000 tree ${publicTree}\tpublic`
        : line,
    );
  const rootTree = git(fixture, ["mktree"], {
    input: `${rootEntries.join("\n")}\n`,
  });
  const commit = git(fixture, ["commit-tree", rootTree, "-p", "HEAD"], {
    input: `${message}\n`,
  });
  git(fixture, ["update-ref", "HEAD", commit]);
}

for (const collision of [
  {
    name: "case-folding",
    paths: ["Logo.svg", "logo.svg"],
  },
  {
    name: "Unicode-normalization",
    paths: ["café.svg", "cafe\u0301.svg"],
  },
  {
    name: "Unicode-case-folding",
    paths: ["Σ.svg", "ς.svg"],
  },
]) {
  test(`materializer rejects ${collision.name} path collisions`, async (t) => {
    const { fixture, destination } = await snapshotFixture(t);
    const object = git(fixture, ["hash-object", "-w", "--stdin"], {
      input: "identical bytes\n",
    });
    commitPublicTree(
      fixture,
      collision.paths.map((name) => ({ mode: "100644", object, name })),
      `${collision.name} collision`,
    );

    const result = materialize(fixture, destination);

    assert.equal(result.status, 1);
    assert.match(result.stderr, /cross-platform path collision/);
    await assert.rejects(stat(destination));
  });
}

test("materializer ignores skip-worktree and clean-filter lies", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  git(fixture, ["update-index", "--skip-worktree", "lib/api/index.ts"]);
  await writeFile(path.join(fixture, "lib", "api", "index.ts"), "skip-worktree poison\n");
  await mkdir(path.join(fixture, ".git", "info"), { recursive: true });
  await writeFile(path.join(fixture, ".git", "info", "attributes"), "tool.sh filter=lie\n");
  git(fixture, ["config", "filter.lie.clean", "sed 's/poison/exit 0/'"]);
  await writeFile(path.join(fixture, "tool.sh"), "#!/usr/bin/env bash\npoison\n");

  const result = materialize(fixture, destination);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(destination, "lib", "api", "index.ts"), "utf8"),
    "committed api\n",
  );
  assert.equal(
    await readFile(path.join(destination, "tool.sh"), "utf8"),
    "#!/usr/bin/env bash\nexit 0\n",
  );
});

test("materializer anchors Git operations to its own repository", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const other = await mkdtemp(path.join(tmpdir(), "open-splunk-other-git-"));
  t.after(() => rm(other, { recursive: true, force: true }));
  git(other, ["init", "--quiet"]);

  const result = materialize(fixture, destination, {
    env: {
      ...process.env,
      GIT_DIR: path.join(other, ".git"),
      GIT_WORK_TREE: other,
    },
  });

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(destination, "lib", "api", "index.ts"), "utf8"),
    "committed api\n",
  );
});

test("materializer uses one batch extractor and no per-file Git verifier", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const wrapperDirectory = path.join(fixture, "ignored", "bin");
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "previous=\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$argument\" == hash-object ]]; then exit 91; fi\n" +
      "  if [[ \"$previous\" == cat-file && \"$argument\" == blob ]]; then exit 92; fi\n" +
      "  previous=\"$argument\"\n" +
      "done\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);

  const result = materialize(fixture, destination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(destination, "lib", "api", "index.ts"), "utf8"),
    "committed api\n",
  );
});

test("materializer rejects an oversized batch blob before reading it", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const wrapperDirectory = path.join(fixture, "ignored", "oversized-bin");
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "previous=\n" +
      "batch=false\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$previous\" == cat-file && \"$argument\" == --batch ]]; then batch=true; fi\n" +
      "  previous=\"$argument\"\n" +
      "done\n" +
      "if [[ \"$batch\" == true ]]; then\n" +
      "  read -r object\n" +
      "  printf '%s blob 134217729\\n' \"$object\"\n" +
      "  exit 0\n" +
      "fi\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);

  const result = materialize(fixture, destination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /exceeds 134217728 bytes/);
  await assert.rejects(stat(destination));
});

test("materializer rejects paths with excessive directory depth", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const components = Array.from({ length: 65 }, (_, index) => `d${index}`);
  const deepDirectory = path.join(fixture, "public", ...components);
  await mkdir(deepDirectory, { recursive: true });
  await writeFile(path.join(deepDirectory, "payload.txt"), "bounded\n");
  git(fixture, ["add", "public"]);
  git(fixture, ["commit", "--quiet", "-m", "add excessively deep path"]);

  const result = materialize(fixture, destination);

  assert.equal(result.status, 1);
  assert.match(result.stderr, /more than 64 components/);
  await assert.rejects(stat(destination));
});

test("materializer bounds distinct directory prefixes", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const object = git(fixture, ["rev-parse", "HEAD:public/.gitkeep"]);
  const wrapperDirectory = path.join(fixture, "ignored", "directory-bound-bin");
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "listing=false\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$argument\" == ls-tree ]]; then listing=true; fi\n" +
      "done\n" +
      "if [[ \"$listing\" == true ]]; then\n" +
      `  object=${JSON.stringify(object)}\n` +
      "  for ((index = 0; index <= 10000; index += 1)); do\n" +
      "    printf '100644 blob %s\\tdirectory-%05d/file\\0' \"$object\" \"$index\"\n" +
      "  done\n" +
      "  exit 0\n" +
      "fi\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);

  const result = materialize(fixture, destination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /more than 10000 directories/);
  await assert.rejects(stat(destination));
});

test("materializer stops parsing at the file-count bound", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const object = git(fixture, ["rev-parse", "HEAD:public/.gitkeep"]);
  const wrapperDirectory = path.join(fixture, "ignored", "file-bound-bin");
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "listing=false\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$argument\" == ls-tree ]]; then listing=true; fi\n" +
      "done\n" +
      "if [[ \"$listing\" == true ]]; then\n" +
      `  object=${JSON.stringify(object)}\n` +
      "  for ((index = 0; index <= 100000; index += 1)); do\n" +
      "    printf '100644 blob %s\\tfile-%06d\\0' \"$object\" \"$index\"\n" +
      "  done\n" +
      "  exit 0\n" +
      "fi\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);

  const result = materialize(fixture, destination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /more than 100000 files/);
  await assert.rejects(stat(destination));
});

test("materializer rejects a non-portable relative path length", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const object = git(fixture, ["rev-parse", "HEAD:public/.gitkeep"]);
  const treePath = [
    "a".repeat(200),
    "b".repeat(200),
    "c".repeat(200),
    "d".repeat(200),
    "file",
  ].join("/");
  const wrapperDirectory = path.join(fixture, "ignored", "path-bound-bin");
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "listing=false\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$argument\" == ls-tree ]]; then listing=true; fi\n" +
      "done\n" +
      "if [[ \"$listing\" == true ]]; then\n" +
      `  object=${JSON.stringify(object)}\n` +
      `  tree_path=${JSON.stringify(treePath)}\n` +
      "  printf '100644 blob %s\\t%s\\0' \"$object\" \"$tree_path\"\n" +
      "  exit 0\n" +
      "fi\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);

  const result = materialize(fixture, destination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /path exceeds 768 bytes/);
  await assert.rejects(stat(destination));
});

test("materializer bounds the combined temporary destination path", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  const object = git(fixture, ["rev-parse", "HEAD:public/.gitkeep"]);
  const treePath = [
    "a".repeat(240),
    "b".repeat(240),
    "c".repeat(240),
    "file",
  ].join("/");
  const wrapperDirectory = path.join(
    fixture,
    "ignored",
    "destination-bound-bin",
  );
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "listing=false\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$argument\" == ls-tree ]]; then listing=true; fi\n" +
      "done\n" +
      "if [[ \"$listing\" == true ]]; then\n" +
      `  object=${JSON.stringify(object)}\n` +
      `  tree_path=${JSON.stringify(treePath)}\n` +
      "  printf '100644 blob %s\\t%s\\0' \"$object\" \"$tree_path\"\n" +
      "  exit 0\n" +
      "fi\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);
  const longDestination = path.join(
    destination,
    ...Array.from({ length: 30 }, () => "abcdefghij"),
  );
  await mkdir(path.dirname(longDestination), { recursive: true });

  const result = materialize(fixture, longDestination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /destination path exceeds 1023 bytes/);
  await assert.rejects(stat(longDestination));
});

test("materializer measures a symlinked destination parent physically", async (t) => {
  const { fixture } = await snapshotFixture(t);
  const object = git(fixture, ["rev-parse", "HEAD:public/.gitkeep"]);
  const treePath = `${"a".repeat(240)}/file`;
  const physicalParent = path.join(
    fixture,
    "ignored",
    "physical-destination",
    ...Array.from({ length: 65 }, () => "abcdefghij"),
  );
  await mkdir(physicalParent, { recursive: true });
  const shortParent = path.join(fixture, "short-destination");
  await symlink(physicalParent, shortParent, "dir");
  const requestedDestination = path.join(shortParent, "snapshot");
  const wrapperDirectory = path.join(
    fixture,
    "ignored",
    "physical-path-bound-bin",
  );
  await mkdir(wrapperDirectory, { recursive: true });
  const wrapper = path.join(wrapperDirectory, "git");
  await writeFile(
    wrapper,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "listing=false\n" +
      "for argument in \"$@\"; do\n" +
      "  if [[ \"$argument\" == ls-tree ]]; then listing=true; fi\n" +
      "done\n" +
      "if [[ \"$listing\" == true ]]; then\n" +
      `  object=${JSON.stringify(object)}\n` +
      `  tree_path=${JSON.stringify(treePath)}\n` +
      "  printf '100644 blob %s\\t%s\\0' \"$object\" \"$tree_path\"\n" +
      "  exit 0\n" +
      "fi\n" +
      "PATH=\"${PATH#*:}\" exec git \"$@\"\n",
  );
  await chmod(wrapper, 0o755);

  const result = materialize(fixture, requestedDestination, {
    env: {
      ...process.env,
      PATH: `${wrapperDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /destination path exceeds 1023 bytes/);
  await assert.rejects(stat(path.join(physicalParent, "snapshot")));
});

test("materializer rejects committed symbolic links", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  await symlink("lib/api/index.ts", path.join(fixture, "linked-api"));
  git(fixture, ["add", "linked-api"]);
  git(fixture, ["commit", "--quiet", "-m", "add symlink"]);

  const result = materialize(fixture, destination);

  assert.equal(result.status, 1);
  assert.match(result.stderr, /cannot contain links/);
  await assert.rejects(stat(destination));
});

test("materializer rejects embedded VCS administrative directories", async (t) => {
  const { fixture, destination } = await snapshotFixture(t);
  await mkdir(path.join(fixture, "public", ".svn"), { recursive: true });
  await writeFile(path.join(fixture, "public", ".svn", "entries"), "metadata\n");
  git(fixture, ["add", "public/.svn/entries"]);
  git(fixture, ["commit", "--quiet", "-m", "add administrative data"]);

  const result = materialize(fixture, destination);

  assert.equal(result.status, 1);
  assert.match(result.stderr, /unsafe path component "\.svn"/);
  await assert.rejects(stat(destination));
});

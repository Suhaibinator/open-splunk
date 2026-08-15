import assert from "node:assert/strict";
import {
  access,
  chmod,
  cp,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
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

function git(fixture, args) {
  const result = spawnSync("git", ["-C", fixture, ...args], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}

async function releaseFixture(t) {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-release-"));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  await mkdir(path.join(fixture, "scripts"));
  await mkdir(path.join(fixture, "fixtures"));
  await copyFile(
    path.join(workspace, "scripts", "materialize-git-snapshot.mjs"),
    path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
  );
  await copyFile(
    path.join(workspace, "scripts", "build-release.sh"),
    path.join(fixture, "scripts", "build-release.sh"),
  );
  await writeFile(
    path.join(fixture, ".gitignore"),
    "/build/\n/.cache/\n",
  );
  await writeFile(
    path.join(fixture, "fixtures", "server"),
    "#!/usr/bin/env bash\n" +
      "test \"${1:-}\" = -verify-embedded-release\n" +
      "printf 'application_version=%s\\nsource_revision=%s\\nspl_compatibility_version=0.2\\nui_build_id=fixture\\nui_sha256=fixture\\n' " +
      "\"$OPEN_SPLUNK_APPLICATION_VERSION\" \"$OPEN_SPLUNK_SOURCE_REVISION\"\n",
  );
  await chmod(path.join(fixture, "fixtures", "server"), 0o755);
  await writeFile(
    path.join(fixture, "fixtures", "tool"),
    "#!/usr/bin/env bash\n" +
      "case \"${1:-}\" in\n" +
      "  version|-version)\n" +
      "    printf 'application_version=%s\\nsource_revision=%s\\n' " +
      "\"$OPEN_SPLUNK_APPLICATION_VERSION\" \"$OPEN_SPLUNK_SOURCE_REVISION\"\n" +
      "    ;;\n" +
      "  *) exit 0 ;;\n" +
      "esac\n",
  );
  await chmod(path.join(fixture, "fixtures", "tool"), 0o755);
  await writeFile(
    path.join(fixture, "Makefile"),
    ".PHONY: proto-tools release-go-deps build build-loggen\n" +
      "proto-tools release-go-deps:\n" +
      "\ttest \"$$npm_config_include\" = dev\n" +
      "\ttest \"$$npm_config_globalconfig\" = /dev/null\n" +
      "\ttest \"$$GIT_CONFIG_NOSYSTEM\" = 1\n" +
      "build:\n" +
      "\tmkdir -p build out\n" +
      "\tcp fixtures/server build/open-splunk-server\n" +
      "\tcp fixtures/tool build/open-splunk-collector\n" +
      "\tprintf '{\"node_env\":\"%s\",\"deployment\":\"%s\"}\\n' \"$$NODE_ENV\" \"$${NEXT_DEPLOYMENT_ID:-}\" > out/asset-manifest.json\n" +
      "build-loggen:\n" +
      "\tmkdir -p build\n" +
      "\tcp fixtures/tool build/open-splunk-loggen\n",
  );
  git(fixture, ["init", "--quiet"]);
  git(fixture, ["config", "user.email", "tests@open-splunk.invalid"]);
  git(fixture, ["config", "user.name", "Open Splunk Tests"]);
  git(fixture, ["add", "."]);
  git(fixture, ["commit", "--quiet", "-m", "fixture"]);
  return fixture;
}

async function installRenameShim(fixture, mode) {
  const shimDirectory = path.join(fixture, ".cache", "rename-shim");
  const binaryDirectory = path.join(shimDirectory, "bin");
  const state = path.join(shimDirectory, "rename-count");
  const holdReady = path.join(shimDirectory, "hold-ready");
  const holdRelease = path.join(shimDirectory, "hold-release");
  await mkdir(binaryDirectory, { recursive: true });
  const shim = path.join(binaryDirectory, "node");
  await writeFile(
    shim,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      `real_node=${JSON.stringify(process.execPath)}\n` +
      `state=${JSON.stringify(state)}\n` +
      `mode=${JSON.stringify(mode)}\n` +
      `hold_ready=${JSON.stringify(holdReady)}\n` +
      `hold_release=${JSON.stringify(holdRelease)}\n` +
      "if [[ \"$mode\" == hold-materializer && " +
      "\"${1:-}\" == */materialize-git-snapshot.mjs ]]; then\n" +
      "  \"$real_node\" \"$@\"\n" +
      "  printf 'ready\\n' > \"$hold_ready\"\n" +
      "  for _ in {1..1000}; do\n" +
      "    if [[ -f \"$hold_release\" ]]; then break; fi\n" +
      "    sleep 0.01\n" +
      "  done\n" +
      "  test -f \"$hold_release\"\n" +
      "  exit 0\n" +
      "fi\n" +
      "if [[ \"${1:-}\" == -e && \"${2:-}\" == *fs.renameSync* ]]; then\n" +
      "  count=0\n" +
      "  if [[ -f \"$state\" ]]; then count=\"$(<\"$state\")\"; fi\n" +
      "  count=$((count + 1))\n" +
      "  printf '%s\\n' \"$count\" > \"$state\"\n" +
      "  if [[ \"$count\" -eq 2 && \"$mode\" == fail-publish ]]; then\n" +
      "    exit 73\n" +
      "  fi\n" +
      "  if [[ \"$count\" -eq 2 && \"$mode\" == occupy-destination ]]; then\n" +
      "    mkdir -p \"$4\"\n" +
      "    printf 'concurrent output\\n' > \"$4/concurrent\"\n" +
      "  fi\n" +
      "  if [[ \"$mode\" == tamper-proof-after-publish && " +
      "\"$3\" == */.cache/release-output.* && \"$4\" == */build ]]; then\n" +
      "    \"$real_node\" \"$@\"\n" +
      "    printf 'tampered\\n' > \"$4/binary-identities.txt\"\n" +
      "    exit 0\n" +
      "  fi\n" +
      "  if [[ \"$mode\" == hold-publish && " +
      "\"$3\" == */.cache/release-output.* && \"$4\" == */build ]]; then\n" +
      "    printf 'ready\\n' > \"$hold_ready\"\n" +
      "    for _ in {1..1000}; do\n" +
      "      if [[ -f \"$hold_release\" ]]; then break; fi\n" +
      "      sleep 0.01\n" +
      "    done\n" +
      "    test -f \"$hold_release\"\n" +
      "  fi\n" +
      "fi\n" +
      "exec \"$real_node\" \"$@\"\n",
  );
  await chmod(shim, 0o755);
  return binaryDirectory;
}

async function installReleaseToolShims(fixture) {
  const binaryDirectory = path.join(
    fixture,
    ".cache",
    "release-tool-shims",
    "bin",
  );
  await mkdir(binaryDirectory, { recursive: true });
  await writeFile(
    path.join(binaryDirectory, "node"),
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "if [[ \"${1:-}\" == --version ]]; then\n" +
      "  printf 'v24.19.0\\n'\n" +
      "  exit 0\n" +
      "fi\n" +
      `exec ${JSON.stringify(process.execPath)} "$@"\n`,
  );
  await writeFile(
    path.join(binaryDirectory, "go"),
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "test \"${1:-}\" = env\n" +
      "test \"${2:-}\" = GOVERSION\n" +
      "printf 'go1.26.6\\n'\n",
  );
  await writeFile(
    path.join(binaryDirectory, "npm"),
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      "test \"${1:-}\" = --version\n" +
      "printf '11.17.0\\n'\n",
  );
  await Promise.all(
    ["node", "go", "npm"].map((name) =>
      chmod(path.join(binaryDirectory, name), 0o755),
    ),
  );
  return binaryDirectory;
}

async function installLauncherRetargetShim(
  binaryDirectory,
  temporaryLink,
  alternateRoot,
  sentinel,
) {
  const shim = path.join(binaryDirectory, "chmod");
  await writeFile(
    shim,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      `temporary_link=${JSON.stringify(temporaryLink)}\n` +
      `alternate_root=${JSON.stringify(alternateRoot)}\n` +
      `sentinel=${JSON.stringify(sentinel)}\n` +
      "if [[ \"$#\" -eq 2 && \"$1\" == 0500 && " +
      "\"$2\" == */open-splunk-release-launcher.*/build-release.sh ]]; then\n" +
      "  /bin/chmod \"$@\"\n" +
      "  launcher_directory=\"${2%/*}\"\n" +
      "  launcher_name=\"${launcher_directory##*/}\"\n" +
      "  /bin/rm -f \"$temporary_link\"\n" +
      "  /bin/ln -s \"$alternate_root\" \"$temporary_link\"\n" +
      "  /bin/mkdir -p \"$alternate_root/$launcher_name\"\n" +
      "  printf '#!/usr/bin/env bash\\nprintf executed > \"%s\"\\n' \"$sentinel\" " +
      "> \"$alternate_root/$launcher_name/build-release.sh\"\n" +
      "  /bin/chmod 0500 \"$alternate_root/$launcher_name/build-release.sh\"\n" +
      "  exit 0\n" +
      "fi\n" +
      "exec /bin/chmod \"$@\"\n",
  );
  await chmod(shim, 0o755);
}

async function installCleanupFailureShim(fixture, targetPattern) {
  const binaryDirectory = path.join(
    fixture,
    ".cache",
    "cleanup-failure-shim",
    "bin",
  );
  await mkdir(binaryDirectory, { recursive: true });
  const shim = path.join(binaryDirectory, "rm");
  await writeFile(
    shim,
    "#!/usr/bin/env bash\n" +
      "set -euo pipefail\n" +
      `target_pattern=${JSON.stringify(targetPattern)}\n` +
      "if [[ \"$#\" -eq 2 && \"$1\" == -rf && " +
      "\"$2\" == $target_pattern ]]; then\n" +
      "  exit 74\n" +
      "fi\n" +
      "exec /bin/rm \"$@\"\n",
  );
  await chmod(shim, 0o755);
  return binaryDirectory;
}

function releaseArguments(fixture) {
  return [
    "-u",
    "BASH_ENV",
    "-u",
    "ENV",
    "BASH_ENV=",
    "ENV=",
    "bash",
    path.join(fixture, "scripts", "build-release.sh"),
  ];
}

function releaseEnvironment(revision, extraEnvironment = {}) {
  return {
    ...process.env,
    OPEN_SPLUNK_APPLICATION_VERSION: "1.2.3",
    OPEN_SPLUNK_SOURCE_REVISION: revision,
    OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: "0.2",
    ...extraEnvironment,
  };
}

function buildRelease(fixture, revision, options = {}) {
  return spawnSync("env", releaseArguments(fixture), {
    cwd: tmpdir(),
    encoding: "utf8",
    env: releaseEnvironment(revision, options.env),
  });
}

function startBuildRelease(fixture, revision, options = {}) {
  const child = spawn("env", releaseArguments(fixture), {
    cwd: tmpdir(),
    env: releaseEnvironment(revision, options.env),
    stdio: ["ignore", "pipe", "pipe"],
  });
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
  for (let attempt = 0; attempt < 1000; attempt += 1) {
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

async function waitForPathOrProcess(target, completed) {
  await Promise.race([
    waitForPath(target),
    completed.then((result) => {
      throw new Error(
        `process exited before creating ${target}: status=${result.status} signal=${result.signal}\n${result.stderr}`,
      );
    }),
  ]);
}

test("release build publishes only verified HEAD artifacts", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  await mkdir(path.join(fixture, "build"));
  await writeFile(path.join(fixture, "build", "stale"), "stale output\n");

  const result = buildRelease(fixture, revision, {
    env: {
      NODE_ENV: "development",
      NEXT_DEPLOYMENT_ID: "ambient-review",
    },
  });

  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, new RegExp(`committed snapshot ${revision}`));
  assert.equal(
    await readFile(path.join(fixture, "build", "asset-manifest.json"), "utf8"),
    '{"node_env":"production","deployment":""}\n',
  );
  assert.equal(
    await readFile(path.join(fixture, "build", "release-verification.txt"), "utf8"),
    "application_version=1.2.3\n" +
      `source_revision=${revision}\n` +
      "spl_compatibility_version=0.2\n" +
      "ui_build_id=fixture\n" +
      "ui_sha256=fixture\n",
  );
  await assert.rejects(access(path.join(fixture, "build", "stale"), constants.F_OK));
  const binaryModes = await Promise.all(
    [
      "open-splunk-server",
      "open-splunk-collector",
      "open-splunk-loggen",
    ].map(async (name) => (await stat(path.join(fixture, "build", name))).mode & 0o777),
  );
  assert.deepEqual(binaryModes, [0o755, 0o755, 0o755]);
  assert.equal(
    (await stat(path.join(fixture, "build", "asset-manifest.json"))).mode & 0o777,
    0o644,
  );
});

test("release build rejects dirty tracked or untracked state", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  await writeFile(path.join(fixture, "unexpected.go"), "package poison\n");

  const result = buildRelease(fixture, revision);

  assert.equal(result.status, 1);
  assert.match(result.stderr, /clean worktree/);
});

test("release build bootstraps the materializer from HEAD", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  git(fixture, [
    "update-index",
    "--skip-worktree",
    "scripts/materialize-git-snapshot.mjs",
  ]);
  await writeFile(
    path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
    "throw new Error('worktree materializer must not execute');\n",
  );

  const result = buildRelease(fixture, revision);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(fixture, "build", "release-verification.txt"), "utf8"),
    "application_version=1.2.3\n" +
      `source_revision=${revision}\n` +
      "spl_compatibility_version=0.2\n" +
      "ui_build_id=fixture\n" +
      "ui_sha256=fixture\n",
  );
});

test("release launcher and bootstrap ignore shell and Node preloads", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const hostileDirectory = path.join(fixture, ".cache", "hostile-environment");
  await mkdir(hostileDirectory, { recursive: true });
  const shellSentinel = path.join(hostileDirectory, "shell-preload-ran");
  const nodeSentinel = path.join(hostileDirectory, "node-preload-ran");
  const shellEnvironment = path.join(hostileDirectory, "hostile-bash-env");
  const nodePreload = path.join(hostileDirectory, "hostile-node-preload.cjs");
  await writeFile(shellEnvironment, `printf poison > ${JSON.stringify(shellSentinel)}\n`);
  await writeFile(
    nodePreload,
    `require("node:fs").writeFileSync(${JSON.stringify(nodeSentinel)}, "poison");\n`,
  );

  const result = buildRelease(fixture, revision, {
    env: {
      BASH_ENV: shellEnvironment,
      ENV: shellEnvironment,
      NODE_OPTIONS: `--require=${nodePreload}`,
    },
  });

  assert.equal(result.status, 0, result.stderr);
  await assert.rejects(access(shellSentinel, constants.F_OK));
  await assert.rejects(access(nodeSentinel, constants.F_OK));
});

test("release build rejects revisions other than anchored HEAD", async (t) => {
  const fixture = await releaseFixture(t);

  const result = buildRelease(
    fixture,
    "0123456789abcdef0123456789abcdef01234567",
  );

  assert.equal(result.status, 1);
  assert.match(result.stderr, /must equal the current HEAD/);
});

test("release build rejects unsafe application versions before Make expansion", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const sentinel = path.join(fixture, ".cache", "version-injection-ran");
  await mkdir(path.dirname(sentinel), { recursive: true });

  const result = buildRelease(fixture, revision, {
    env: {
      OPEN_SPLUNK_APPLICATION_VERSION: `1.2.3"; touch ${sentinel}; #`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /unsafe or unsupported/);
  await assert.rejects(access(sentinel, constants.F_OK));
});

test("release build requires an explicit expected SPL compatibility identity", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const environment = releaseEnvironment(revision);
  delete environment.OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION;

  const result = spawnSync("env", releaseArguments(fixture), {
    cwd: tmpdir(),
    encoding: "utf8",
    env: environment,
  });

  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION is required/,
  );
});

test("release build rejects unsafe expected SPL compatibility identities", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);

  const result = buildRelease(fixture, revision, {
    env: {
      OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: "0.3\nforged",
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /unsafe or unsupported/);
});

test("release build rejects an embedded SPL compatibility identity mismatch", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);

  const result = buildRelease(fixture, revision, {
    env: {
      OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: "0.3",
    },
  });

  assert.equal(result.status, 1);
  await assert.rejects(
    access(path.join(fixture, "build", "release-verification.txt"), constants.F_OK),
  );
});

test("release Make target treats identity text as opaque before validation", async (t) => {
  const temporaryRoot = await mkdtemp(
    path.join(tmpdir(), "open-splunk-make-identity-"),
  );
  t.after(() => rm(temporaryRoot, { force: true, recursive: true }));
  const versionSentinel = path.join(temporaryRoot, "version-expanded");
  const revisionSentinel = path.join(temporaryRoot, "revision-expanded");
  const compatibilitySentinel = path.join(
    temporaryRoot,
    "compatibility-expanded",
  );

  const result = spawnSync("make", ["-n", "release"], {
    cwd: workspace,
    encoding: "utf8",
    env: {
      ...process.env,
      OPEN_SPLUNK_APPLICATION_VERSION:
        `$(shell touch ${versionSentinel})`,
      OPEN_SPLUNK_SOURCE_REVISION:
        `$(shell touch ${revisionSentinel})`,
      OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION:
        `$(shell touch ${compatibilitySentinel})`,
    },
  });

  assert.equal(result.status, 0, result.stderr);
  await assert.rejects(access(versionSentinel, constants.F_OK));
  await assert.rejects(access(revisionSentinel, constants.F_OK));
  await assert.rejects(access(compatibilitySentinel, constants.F_OK));
});

test("make release uses the committed launcher and a fixed umask", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const toolDirectory = await installReleaseToolShims(fixture);
  const sentinel = path.join(fixture, ".cache", "worktree-launcher-ran");
  const temporaryRoot = await mkdtemp(
    path.join(tmpdir(), "open-splunk-launcher-retarget-"),
  );
  t.after(() => rm(temporaryRoot, { force: true, recursive: true }));
  const firstTarget = path.join(temporaryRoot, "first");
  const secondTarget = path.join(temporaryRoot, "second");
  const temporaryLink = path.join(temporaryRoot, "tmp");
  const retargetSentinel = path.join(temporaryRoot, "retargeted-launcher-ran");
  await mkdir(firstTarget);
  await mkdir(secondTarget);
  await symlink(firstTarget, temporaryLink, "dir");
  await installLauncherRetargetShim(
    toolDirectory,
    temporaryLink,
    secondTarget,
    retargetSentinel,
  );
  await writeFile(
    path.join(fixture, "scripts", "build-release.sh"),
    "#!/usr/bin/env bash\n" +
      `touch ${JSON.stringify(sentinel)}\n` +
      "exit 97\n",
  );
  await chmod(path.join(fixture, "scripts", "build-release.sh"), 0o755);
  git(fixture, [
    "update-index",
    "--skip-worktree",
    "scripts/build-release.sh",
  ]);
  assert.equal(git(fixture, ["status", "--porcelain=v1"]), "");

  const result = spawnSync(
    "bash",
    [
      "-c",
      'umask 0777; exec make "$@"',
      "make",
      "-f",
      path.join(workspace, "Makefile"),
      "release",
    ],
    {
      cwd: fixture,
      encoding: "utf8",
      env: {
        ...process.env,
        OPEN_SPLUNK_APPLICATION_VERSION: "1.2.3",
        OPEN_SPLUNK_SOURCE_REVISION: revision,
        OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: "0.2",
        PATH: `${toolDirectory}:${process.env.PATH}`,
        TMPDIR: temporaryLink,
      },
    },
  );

  assert.equal(result.status, 0, result.stderr);
  await assert.rejects(access(sentinel, constants.F_OK));
  await assert.rejects(access(retargetSentinel, constants.F_OK));
  assert.equal(
    await readFile(path.join(fixture, "build", "asset-manifest.json"), "utf8"),
    '{"node_env":"production","deployment":""}\n',
  );
  assert.equal(
    (await stat(path.join(fixture, "build", "open-splunk-server"))).mode &
      0o777,
    0o755,
  );
  assert.equal(
    (await stat(path.join(fixture, "build", "asset-manifest.json"))).mode &
      0o777,
    0o644,
  );
});

test("supported binary recipes disable ambient VCS stamping", async () => {
  const makefile = await readFile(path.join(workspace, "Makefile"), "utf8");
  const releaseBuildCommands = makefile
    .split("\n")
    .filter((line) => line.includes("go build") && line.includes("./cmd/open-splunk-"));
  assert.equal(releaseBuildCommands.length, 3);
  for (const command of releaseBuildCommands) {
    assert.match(command, /go build -buildvcs=false /);
  }
});

test("release launcher pins match the canonical tool-version files", async () => {
  const [makefile, goModule, nodeVersion, packageText] = await Promise.all([
    readFile(path.join(workspace, "Makefile"), "utf8"),
    readFile(path.join(workspace, "go.mod"), "utf8"),
    readFile(path.join(workspace, ".node-version"), "utf8"),
    readFile(path.join(workspace, "package.json"), "utf8"),
  ]);
  const packageJSON = JSON.parse(packageText);
  const makeVersion = (name) => {
    const match = new RegExp(`^override ${name} := ([^\\s]+)$`, "m").exec(
      makefile,
    );
    assert.ok(match, `${name} must be pinned in Makefile`);
    return match[1];
  };
  const goVersion = /^go ([^\s]+)$/m.exec(goModule)?.[1];

  assert.ok(goVersion, "go.mod must pin a Go version");
  assert.equal(makeVersion("RELEASE_GO_VERSION"), goVersion);
  assert.equal(makeVersion("RELEASE_NODE_VERSION"), nodeVersion.trim());
  assert.equal(makeVersion("RELEASE_NODE_VERSION"), packageJSON.engines.node);
  assert.equal(makeVersion("RELEASE_NPM_VERSION"), packageJSON.engines.npm);
  assert.equal(
    `npm@${makeVersion("RELEASE_NPM_VERSION")}`,
    packageJSON.packageManager,
  );
});

test("release build works when its temporary root contains spaces", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const temporaryRoot = await mkdtemp(path.join(tmpdir(), "open splunk release tmp "));
  t.after(() => rm(temporaryRoot, { force: true, recursive: true }));

  const result = buildRelease(fixture, revision, {
    env: { TMPDIR: temporaryRoot },
  });

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(fixture, "build", "asset-manifest.json"), "utf8"),
    '{"node_env":"production","deployment":""}\n',
  );
});

test("release build pins its physical work root across TMPDIR retargeting", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const shimDirectory = await installRenameShim(fixture, "hold-materializer");
  const holdReady = path.join(
    fixture,
    ".cache",
    "rename-shim",
    "hold-ready",
  );
  const holdRelease = path.join(
    fixture,
    ".cache",
    "rename-shim",
    "hold-release",
  );
  const temporaryRoot = await mkdtemp(
    path.join(tmpdir(), "open-splunk-work-root-retarget-"),
  );
  t.after(() => rm(temporaryRoot, { force: true, recursive: true }));
  const firstTarget = path.join(temporaryRoot, "first");
  const secondTarget = path.join(temporaryRoot, "second");
  const temporaryLink = path.join(temporaryRoot, "tmp");
  await mkdir(firstTarget);
  await mkdir(secondTarget);
  await symlink(firstTarget, temporaryLink, "dir");
  const release = startBuildRelease(fixture, revision, {
    env: {
      PATH: `${shimDirectory}:${process.env.PATH}`,
      TMPDIR: temporaryLink,
    },
  });
  let result;
  try {
    await waitForPathOrProcess(holdReady, release.completed);
    const workRoots = (await readdir(firstTarget)).filter((entry) =>
      entry.startsWith("open-splunk-release."));
    assert.equal(workRoots.length, 1);
    const workRoot = workRoots[0];
    await cp(
      path.join(firstTarget, workRoot),
      path.join(secondTarget, workRoot),
      { recursive: true },
    );
    const attackerMakefile = path.join(
      secondTarget,
      workRoot,
      "source",
      "Makefile",
    );
    const originalMakefile = await readFile(attackerMakefile, "utf8");
    const changedMakefile = originalMakefile.replace(
      '{"node_env":"%s","deployment":"%s"}',
      '{"source":"retargeted","node_env":"%s","deployment":"%s"}',
    );
    assert.notEqual(changedMakefile, originalMakefile);
    await writeFile(attackerMakefile, changedMakefile);
    await rm(temporaryLink);
    await symlink(secondTarget, temporaryLink, "dir");
  } finally {
    await writeFile(holdRelease, "release\n");
    result = await release.completed;
  }

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(path.join(fixture, "build", "asset-manifest.json"), "utf8"),
    '{"node_env":"production","deployment":""}\n',
  );
});

test("release cleanup removes read-only dependency-cache directories", async (t) => {
  const fixture = await releaseFixture(t);
  const makefilePath = path.join(fixture, "Makefile");
  const originalMakefile = await readFile(makefilePath, "utf8");
  const changedMakefile = originalMakefile.replace(
    "proto-tools release-go-deps:\n",
    "proto-tools release-go-deps:\n" +
      "\tmkdir -p \"$$GOMODCACHE/read-only/child\"\n" +
      "\tprintf 'cached\\n' > \"$$GOMODCACHE/read-only/child/module\"\n" +
      "\tchmod 0555 \"$$GOMODCACHE/read-only\" \"$$GOMODCACHE/read-only/child\"\n",
  );
  assert.notEqual(changedMakefile, originalMakefile);
  await writeFile(makefilePath, changedMakefile);
  git(fixture, ["add", "Makefile"]);
  git(fixture, ["commit", "--quiet", "-m", "create read-only dependency cache"]);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const temporaryRoot = await mkdtemp(
    path.join(tmpdir(), "open-splunk-read-only-cleanup-"),
  );
  t.after(() => rm(temporaryRoot, { force: true, recursive: true }));

  const result = buildRelease(fixture, revision, {
    env: { TMPDIR: temporaryRoot },
  });

  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(
    (await readdir(temporaryRoot)).filter((entry) =>
      entry.startsWith("open-splunk-release.")),
    [],
  );
});

test("release cleanup failure changes the command status", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const shimDirectory = await installCleanupFailureShim(
    fixture,
    "*/open-splunk-release.*",
  );
  const temporaryRoot = await mkdtemp(
    path.join(tmpdir(), "open-splunk-failed-cleanup-"),
  );
  t.after(() => rm(temporaryRoot, { force: true, recursive: true }));

  const result = buildRelease(fixture, revision, {
    env: {
      PATH: `${shimDirectory}:${process.env.PATH}`,
      TMPDIR: temporaryRoot,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /could not remove release work root/);
  assert.match(result.stderr, /release work root remains after cleanup/);
  const remainingWorkRoots = (await readdir(temporaryRoot)).filter((entry) =>
    entry.startsWith("open-splunk-release."));
  assert.equal(remainingWorkRoots.length, 1);
  await assert.rejects(
    access(path.join(fixture, ".cache", "release.lock"), constants.F_OK),
  );
});

test("release cleanup removes a read-only previous build", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const readOnlyDirectory = path.join(fixture, "build", "cache", "child");
  await mkdir(readOnlyDirectory, { recursive: true });
  await writeFile(path.join(readOnlyDirectory, "artifact"), "previous\n");
  await chmod(readOnlyDirectory, 0o555);
  await chmod(path.dirname(readOnlyDirectory), 0o555);

  const result = buildRelease(fixture, revision);

  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(
    (await readdir(path.join(fixture, ".cache"))).filter((entry) =>
      entry.startsWith("release-previous.")),
    [],
  );
});

test("previous-build cleanup failure changes the command status", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  await mkdir(path.join(fixture, "build"));
  await writeFile(path.join(fixture, "build", "previous"), "previous\n");
  const shimDirectory = await installCleanupFailureShim(
    fixture,
    "*/.cache/release-previous.*",
  );

  const result = buildRelease(fixture, revision, {
    env: {
      PATH: `${shimDirectory}:${process.env.PATH}`,
    },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /could not remove previous release build/);
  assert.match(result.stderr, /previous release build remains after cleanup/);
  assert.equal(
    (await readdir(path.join(fixture, ".cache"))).filter((entry) =>
      entry.startsWith("release-previous.")).length,
    1,
  );
  assert.equal(
    await readFile(path.join(fixture, "build", "asset-manifest.json"), "utf8"),
    '{"node_env":"production","deployment":""}\n',
  );
  await assert.rejects(
    access(path.join(fixture, ".cache", "release.lock"), constants.F_OK),
  );
});

test("release build rejects a collector with contradictory linked identity", async (t) => {
  const fixture = await releaseFixture(t);
  await writeFile(
    path.join(fixture, "fixtures", "tool"),
    "#!/usr/bin/env bash\n" +
      "case \"${1:-}\" in\n" +
      "  version) printf 'application_version=1.2.3\\nsource_revision=0000000000000000000000000000000000000000\\n' ;;\n" +
      "  -version) printf 'application_version=%s\\nsource_revision=%s\\n' " +
      "\"$OPEN_SPLUNK_APPLICATION_VERSION\" \"$OPEN_SPLUNK_SOURCE_REVISION\" ;;\n" +
      "  *) exit 0 ;;\n" +
      "esac\n",
  );
  await chmod(path.join(fixture, "fixtures", "tool"), 0o755);
  git(fixture, ["add", "fixtures/tool"]);
  git(fixture, ["commit", "--quiet", "-m", "wrong collector identity"]);
  const revision = git(fixture, ["rev-parse", "HEAD"]);

  const result = buildRelease(fixture, revision);

  assert.equal(result.status, 1);
  await assert.rejects(access(path.join(fixture, "build"), constants.F_OK));
});

test("concurrent release publisher fails without disturbing the lock holder", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const shimDirectory = await installRenameShim(fixture, "hold-publish");
  const holdReady = path.join(
    fixture,
    ".cache",
    "rename-shim",
    "hold-ready",
  );
  const holdRelease = path.join(
    fixture,
    ".cache",
    "rename-shim",
    "hold-release",
  );
  const first = startBuildRelease(fixture, revision, {
    env: { PATH: `${shimDirectory}:${process.env.PATH}` },
  });
  let second;
  let firstResult;
  try {
    await waitForPath(holdReady);
    second = buildRelease(fixture, revision);
  } finally {
    await writeFile(holdRelease, "release\n");
    firstResult = await first.completed;
  }

  assert.equal(firstResult.status, 0, firstResult.stderr);
  assert.equal(second.status, 1);
  assert.match(second.stderr, /another release is publishing/);
  assert.equal(
    await readFile(path.join(fixture, "build", "release-verification.txt"), "utf8"),
    "application_version=1.2.3\n" +
      `source_revision=${revision}\n` +
      "spl_compatibility_version=0.2\n" +
      "ui_build_id=fixture\n" +
      "ui_sha256=fixture\n",
  );
  await assert.rejects(
    access(path.join(fixture, ".cache", "release.lock"), constants.F_OK),
  );
});

test("an older slow release cannot replace a newer committed release", async (t) => {
  const fixture = await releaseFixture(t);
  const olderRevision = git(fixture, ["rev-parse", "HEAD"]);
  const shimDirectory = await installRenameShim(fixture, "hold-materializer");
  const holdReady = path.join(
    fixture,
    ".cache",
    "rename-shim",
    "hold-ready",
  );
  const holdRelease = path.join(
    fixture,
    ".cache",
    "rename-shim",
    "hold-release",
  );
  const older = startBuildRelease(fixture, olderRevision, {
    env: {
      PATH: `${shimDirectory}:${process.env.PATH}`,
    },
  });
  let newer;
  let newerRevision;
  let olderResult;
  try {
    await waitForPathOrProcess(holdReady, older.completed);
    await writeFile(
      path.join(fixture, "fixtures", "revision-marker"),
      "newer source\n",
    );
    git(fixture, ["add", "fixtures/revision-marker"]);
    git(fixture, ["commit", "--quiet", "-m", "newer release source"]);
    newerRevision = git(fixture, ["rev-parse", "HEAD"]);
    newer = buildRelease(fixture, newerRevision);
  } finally {
    await writeFile(holdRelease, "release\n");
    olderResult = await older.completed;
  }

  assert.equal(newer.status, 0, newer.stderr);
  assert.equal(olderResult.status, 1);
  assert.match(olderResult.stderr, /HEAD changed after release snapshot/);
  assert.equal(
    await readFile(path.join(fixture, "build", "release-verification.txt"), "utf8"),
    "application_version=1.2.3\n" +
      `source_revision=${newerRevision}\n` +
      "spl_compatibility_version=0.2\n" +
      "ui_build_id=fixture\n" +
      "ui_sha256=fixture\n",
  );
});

test("failed atomic publication restores the previous build", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  await mkdir(path.join(fixture, "build"));
  await writeFile(path.join(fixture, "build", "stale"), "previous build\n");
  const shimDirectory = await installRenameShim(fixture, "fail-publish");

  const result = buildRelease(fixture, revision, {
    env: { PATH: `${shimDirectory}:${process.env.PATH}` },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /could not atomically publish/);
  assert.equal(
    await readFile(path.join(fixture, "build", "stale"), "utf8"),
    "previous build\n",
  );
});

test("publication race preserves the previous build when rollback is blocked", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  await mkdir(path.join(fixture, "build"));
  await writeFile(path.join(fixture, "build", "stale"), "previous build\n");
  const shimDirectory = await installRenameShim(fixture, "occupy-destination");

  const result = buildRelease(fixture, revision, {
    env: { PATH: `${shimDirectory}:${process.env.PATH}` },
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /rollback is incomplete/);
  assert.equal(
    await readFile(path.join(fixture, "build", "concurrent"), "utf8"),
    "concurrent output\n",
  );
  const previousMatch = /previous build preserved at (.+)/.exec(result.stderr);
  assert.ok(previousMatch, result.stderr);
  assert.equal(
    await readFile(path.join(previousMatch[1], "stale"), "utf8"),
    "previous build\n",
  );
});

test("post-publication verification failure removes an unbacked canonical build", async (t) => {
  const fixture = await releaseFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const shimDirectory = await installRenameShim(
    fixture,
    "tamper-proof-after-publish",
  );

  const result = buildRelease(fixture, revision, {
    env: { PATH: `${shimDirectory}:${process.env.PATH}` },
  });

  assert.equal(result.status, 1);
  await assert.rejects(access(path.join(fixture, "build"), constants.F_OK));
});

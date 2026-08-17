import assert from "node:assert/strict";
import { createHash } from "node:crypto";
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
import test from "node:test";

const workspace = process.cwd();

function git(fixture, args) {
  const result = spawnSync("git", ["-C", fixture, ...args], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}

async function ociFixture(t) {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-oci-"));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  await mkdir(path.join(fixture, "scripts"));
  await copyFile(
    path.join(workspace, "scripts", "build-oci.sh"),
    path.join(fixture, "scripts", "build-oci.sh"),
  );
  await copyFile(
    path.join(workspace, "scripts", "materialize-git-snapshot.mjs"),
    path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
  );
  await copyFile(
    path.join(workspace, "Dockerfile"),
    path.join(fixture, "Dockerfile"),
  );
  await copyFile(
    path.join(workspace, ".dockerignore"),
    path.join(fixture, ".dockerignore"),
  );
  await writeFile(
    path.join(fixture, "package.json"),
    '{"name":"open-splunk-oci-fixture","version":"0.4.0"}\n',
  );
  await mkdir(path.join(fixture, "oci", "rootfs", "etc"), { recursive: true });
  await copyFile(
    path.join(workspace, "oci", "rootfs", "etc", "passwd"),
    path.join(fixture, "oci", "rootfs", "etc", "passwd"),
  );
  await copyFile(
    path.join(workspace, "oci", "rootfs", "etc", "group"),
    path.join(fixture, "oci", "rootfs", "etc", "group"),
  );
  await mkdir(path.join(fixture, "internal"));
  await writeFile(
    path.join(fixture, "internal", "fixture_context.go"),
    "package fixture // committed snapshot\n",
  );
  await chmod(path.join(fixture, "scripts", "build-oci.sh"), 0o755);
  await chmod(
    path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
    0o755,
  );
  git(fixture, ["init", "--quiet"]);
  git(fixture, ["config", "user.email", "tests@open-splunk.invalid"]);
  git(fixture, ["config", "user.name", "Open Splunk Tests"]);
  git(fixture, ["add", "."]);
  git(fixture, ["commit", "--quiet", "-m", "fixture"]);
  return fixture;
}

async function installDockerShim(fixture) {
  const binaryDirectory = path.join(fixture, ".git", "test-bin");
  const log = path.join(fixture, ".git", "docker-invocations");
  const contextRecord = path.join(fixture, ".git", "docker-context");
  const state = path.join(fixture, ".git", "docker-state");
  await mkdir(binaryDirectory);
  await mkdir(state);
  await writeFile(
    path.join(binaryDirectory, "docker"),
    [
      "#!/usr/bin/env bash",
      "set -euo pipefail",
      'state_root="$OPEN_SPLUNK_TEST_DOCKER_STATE"',
      'container_root="$state_root/containers"',
      'server_state="$state_root/server-final-image-id"',
      'collector_state="$state_root/collector-final-image-id"',
      'temporary_server_id="${OPEN_SPLUNK_TEST_TEMP_SERVER_IMAGE_ID:-sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}"',
      'temporary_collector_id="${OPEN_SPLUNK_TEST_TEMP_COLLECTOR_IMAGE_ID:-sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc}"',
      'mkdir -p "$container_root"',
      'printf \'SOURCE_DATE_EPOCH=%q DOCKER_BUILDKIT=%q \' "${SOURCE_DATE_EPOCH:-}" "${DOCKER_BUILDKIT:-}" >> "$OPEN_SPLUNK_TEST_DOCKER_LOG"',
      'printf \'%q \' "$@" >> "$OPEN_SPLUNK_TEST_DOCKER_LOG"',
      'printf \'\\n\' >> "$OPEN_SPLUNK_TEST_DOCKER_LOG"',
      'if [[ "${1:-}" == build ]]; then',
      '  context="${!#}"',
      '  printf \'%s\\n\' "$context" > "$OPEN_SPLUNK_TEST_CONTEXT_RECORD.root"',
      '  IFS= read -r marker < "$context/internal/fixture_context.go"',
      '  printf \'%s\\n\' "$marker" > "$OPEN_SPLUNK_TEST_CONTEXT_RECORD"',
      '  if [[ -e "$context/internal/hidden_uncommitted.go" ]]; then',
      '    printf \'present\\n\' > "$OPEN_SPLUNK_TEST_CONTEXT_RECORD.hidden"',
      "  else",
      '    printf \'absent\\n\' > "$OPEN_SPLUNK_TEST_CONTEXT_RECORD.hidden"',
      "  fi",
      "fi",
      'if [[ "${1:-}" == container && "${2:-}" == create ]]; then',
      '  lock_name=""',
      '  lock_owner=""',
      '  lock_reference=""',
      "  shift 2",
      '  while [[ $# -gt 0 ]]; do',
      '    case "$1" in',
      "      --name)",
      '        lock_name=$2',
      "        shift 2",
      "        ;;",
      "      --label)",
      '        if [[ "$2" == org.open-splunk.oci-publication-lock.owner=* ]]; then',
      '          lock_owner=${2#org.open-splunk.oci-publication-lock.owner=}',
      '        elif [[ "$2" == org.open-splunk.oci-publication-lock.reference=* ]]; then',
      '          lock_reference=${2#org.open-splunk.oci-publication-lock.reference=}',
      "        fi",
      "        shift 2",
      "        ;;",
      "      *)",
      "        shift",
      "        ;;",
      "    esac",
      "  done",
      '  if [[ -z "$lock_name" || -z "$lock_owner" || -z "$lock_reference" ]] || ! mkdir "$container_root/$lock_name" 2>/dev/null; then',
      "    exit 1",
      "  fi",
      '  lock_id=${lock_name##*-}',
      '  printf \'%s\\n\' "$lock_id" > "$container_root/$lock_name/id"',
      '  printf \'%s\\n\' "$lock_owner" > "$container_root/$lock_name/owner"',
      '  printf \'%s\\n\' "$lock_reference" > "$container_root/$lock_name/reference"',
      '  if [[ "${OPEN_SPLUNK_TEST_SIGNAL_AFTER_LOCK_CREATE:-}" == TERM ]]; then',
      '    kill -TERM "$PPID"',
      "    exit 130",
      "  fi",
      '  printf \'%s\\n\' "$lock_id"',
      "  exit 0",
      "fi",
      'if [[ "${1:-}" == container && "${2:-}" == inspect ]]; then',
      '  lock_name="${!#}"',
      '  [[ -f "$container_root/$lock_name/id" && -f "$container_root/$lock_name/owner" && -f "$container_root/$lock_name/reference" ]] || exit 1',
      '  IFS= read -r lock_id < "$container_root/$lock_name/id"',
      '  IFS= read -r lock_owner < "$container_root/$lock_name/owner"',
      '  IFS= read -r lock_reference < "$container_root/$lock_name/reference"',
      '  printf \'%s|%s|%s\\n\' "$lock_id" "$lock_owner" "$lock_reference"',
      "  exit 0",
      "fi",
      'if [[ "${1:-}" == container && "${2:-}" == rm ]]; then',
      '  requested_id="${!#}"',
      '  lock_name=""',
      '  for candidate in "$container_root"/*; do',
      '    [[ -d "$candidate" && -f "$candidate/id" ]] || continue',
      '    IFS= read -r candidate_id < "$candidate/id"',
      '    if [[ "$candidate_id" == "$requested_id" ]]; then',
      '      lock_name=${candidate##*/}',
      "      break",
      "    fi",
      "  done",
      '  [[ -n "$lock_name" ]] || exit 1',
      '  rm -f "$container_root/$lock_name/id" "$container_root/$lock_name/owner" "$container_root/$lock_name/reference"',
      '  rmdir "$container_root/$lock_name"',
      '  printf \'%s\\n\' "$requested_id"',
      "  exit 0",
      "fi",
      'if [[ "${1:-}" == image && "${2:-}" == inspect ]]; then',
      '  inspected_image="${!#}"',
      '  if [[ "$inspected_image" == *-server ]]; then',
      '    printf \'%s\\n\' "$temporary_server_id"',
      "  else",
      '    printf \'%s\\n\' "$temporary_collector_id"',
      "  fi",
      "  exit 0",
      "fi",
      'if [[ "${1:-}" == image && "${2:-}" == ls ]]; then',
      '  reference="${!#}"',
      '  reference=${reference#reference=}',
      '  if [[ "${OPEN_SPLUNK_TEST_REQUIRE_FAMILIAR_DOCKER_HUB_FILTER:-0}" == 1 ]]; then',
      '    if [[ "$reference" == docker.io/* ]]; then',
      '      exit 0',
      '    elif [[ "$reference" == */* ]]; then',
      '      reference="docker.io/$reference"',
      '    else',
      '      reference="docker.io/library/$reference"',
      '    fi',
      '  fi',
      '  if [[ "$reference" == "$OPEN_SPLUNK_SERVER_IMAGE" ]]; then',
      '    if [[ "${OPEN_SPLUNK_TEST_TAMPER_LOCK_OWNER:-0}" == 1 ]]; then',
      '      for candidate in "$container_root"/*; do',
      '        [[ -d "$candidate" ]] || continue',
      '        printf \'%s\\n\' eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee > "$candidate/owner"',
      "        break",
      "      done",
      "    fi",
      '    if [[ -n "${OPEN_SPLUNK_TEST_HOLD_MARKER:-}" ]]; then',
      '      : > "$OPEN_SPLUNK_TEST_HOLD_MARKER"',
      '      while [[ ! -e "$OPEN_SPLUNK_TEST_HOLD_RELEASE" ]]; do sleep 0.05; done',
      "    fi",
      '    if [[ -f "$server_state" ]]; then',
      '      cat "$server_state"',
      "    else",
      '      printf \'%s\' "${OPEN_SPLUNK_TEST_PREVIOUS_SERVER_IMAGE_ID:-}"',
      "    fi",
      '  elif [[ "$reference" == "$OPEN_SPLUNK_COLLECTOR_IMAGE" ]]; then',
      '    if [[ -f "$collector_state" ]]; then',
      '      cat "$collector_state"',
      "    else",
      '      printf \'%s\' "${OPEN_SPLUNK_TEST_PREVIOUS_COLLECTOR_IMAGE_ID:-}"',
      "    fi",
      "  fi",
      "  exit 0",
      "fi",
      'if [[ "${1:-}" == image && "${2:-}" == tag ]]; then',
      '  source_image=$3',
      '  target_image=$4',
      '  if [[ "${OPEN_SPLUNK_TEST_FAIL_COLLECTOR_TAG:-0}" == 1 && "$target_image" == "$OPEN_SPLUNK_COLLECTOR_IMAGE" && "$source_image" != sha256:* ]]; then',
      "    exit 42",
      "  fi",
      '  if [[ "$target_image" == "$OPEN_SPLUNK_SERVER_IMAGE" ]]; then',
      '    if [[ "$source_image" == sha256:* ]]; then',
      '      printf \'%s\\n\' "$source_image" > "$server_state"',
      "    else",
      '      printf \'%s\\n\' "$temporary_server_id" > "$server_state"',
      "    fi",
      '    if [[ "${OPEN_SPLUNK_TEST_SIGNAL_AFTER_SERVER_TAG:-}" == TERM ]]; then',
      '      kill -TERM "$PPID"',
      "    fi",
      '  elif [[ "$target_image" == "$OPEN_SPLUNK_COLLECTOR_IMAGE" ]]; then',
      '    if [[ "$source_image" == sha256:* ]]; then',
      '      printf \'%s\\n\' "$source_image" > "$collector_state"',
      "    else",
      '      printf \'%s\\n\' "$temporary_collector_id" > "$collector_state"',
      "    fi",
      "  fi",
      "  exit 0",
      "fi",
      'if [[ "${1:-}" == image && "${2:-}" == rm ]]; then',
      "  shift 2",
      '  for image_reference in "$@"; do',
      '    if [[ "$image_reference" == --force ]]; then',
      "      continue",
      "    fi",
      '    if [[ "$image_reference" == "$OPEN_SPLUNK_SERVER_IMAGE" ]]; then',
      '      rm -f "$server_state"',
      '    elif [[ "$image_reference" == "$OPEN_SPLUNK_COLLECTOR_IMAGE" ]]; then',
      '      rm -f "$collector_state"',
      "    fi",
      "  done",
      "  exit 0",
      "fi",
    ].join("\n") + "\n",
  );
  await chmod(path.join(binaryDirectory, "docker"), 0o755);
  return { binaryDirectory, contextRecord, log, state };
}

function buildOCIEnvironment(revision, docker, extraEnvironment = {}) {
  return {
    ...process.env,
    OPEN_SPLUNK_APPLICATION_VERSION: "0.4.0",
    OPEN_SPLUNK_SOURCE_REVISION: revision,
    OPEN_SPLUNK_SERVER_IMAGE: "registry.invalid/open-splunk/server:test",
    OPEN_SPLUNK_COLLECTOR_IMAGE: "registry.invalid/open-splunk/collector:test",
    OPEN_SPLUNK_OCI_PLATFORM: "linux/amd64",
    OPEN_SPLUNK_OCI_NO_CACHE: "",
    OPEN_SPLUNK_TEST_DOCKER_LOG: docker.log,
    OPEN_SPLUNK_TEST_DOCKER_STATE: docker.state,
    OPEN_SPLUNK_TEST_CONTEXT_RECORD: docker.contextRecord,
    PATH: `${docker.binaryDirectory}:${process.env.PATH}`,
    ...extraEnvironment,
  };
}

function runBuildOCI(fixture, revision, docker, extraEnvironment = {}) {
  return spawnSync("bash", [path.join(fixture, "scripts", "build-oci.sh")], {
    cwd: tmpdir(),
    encoding: "utf8",
    env: buildOCIEnvironment(revision, docker, extraEnvironment),
  });
}

function startBuildOCI(fixture, revision, docker, extraEnvironment = {}) {
  const child = spawn("bash", [path.join(fixture, "scripts", "build-oci.sh")], {
    cwd: tmpdir(),
    env: buildOCIEnvironment(revision, docker, extraEnvironment),
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const completed = new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (status, signal) => resolve({ signal, status, stderr, stdout }));
  });
  return { child, completed };
}

function waitForPath(target, timeoutMilliseconds = 5000) {
  return new Promise((resolve, reject) => {
    let checking = false;
    const finish = (error) => {
      clearInterval(interval);
      clearTimeout(timeout);
      if (error) reject(error);
      else resolve();
    };
    const interval = setInterval(async () => {
      if (checking) return;
      checking = true;
      try {
        await access(target, constants.F_OK);
        finish();
      } catch (error) {
        if (error.code !== "ENOENT") finish(error);
      } finally {
        checking = false;
      }
    }, 25);
    const timeout = setTimeout(() => {
      finish(new Error(`timed out waiting for ${target}`));
    }, timeoutMilliseconds);
  });
}

function publicationLockName(reference) {
  const digest = createHash("sha256").update(reference, "utf8").digest("hex");
  return `open-splunk-oci-publication-lock-${digest}`;
}

function runMakeOCI(fixture, revision, docker, extraEnvironment = {}) {
  return spawnSync(
    "make",
    ["-f", path.join(workspace, "Makefile"), "oci"],
    {
      cwd: fixture,
      encoding: "utf8",
      env: {
        ...process.env,
        OPEN_SPLUNK_APPLICATION_VERSION: "0.4.0",
        OPEN_SPLUNK_SOURCE_REVISION: revision,
        OPEN_SPLUNK_SERVER_IMAGE: "registry.invalid/open-splunk/server:test",
        OPEN_SPLUNK_COLLECTOR_IMAGE:
          "registry.invalid/open-splunk/collector:test",
        OPEN_SPLUNK_OCI_PLATFORM: "linux/amd64",
        OPEN_SPLUNK_OCI_NO_CACHE: "",
        OPEN_SPLUNK_TEST_DOCKER_LOG: docker.log,
        OPEN_SPLUNK_TEST_DOCKER_STATE: docker.state,
        OPEN_SPLUNK_TEST_CONTEXT_RECORD: docker.contextRecord,
        PATH: `${docker.binaryDirectory}:${process.env.PATH}`,
        ...extraEnvironment,
      },
    },
  );
}

test("OCI targets are pinned scratch runtimes with a minimal non-root contract", async () => {
  const dockerfile = await readFile(path.join(workspace, "Dockerfile"), "utf8");

  assert.match(
    dockerfile,
    /node:26\.7\.0-bookworm-slim@sha256:cd565714d4da3e84bfd341e31448f81d47c6362198f152345297c9c1154e6341/,
  );
  assert.match(
    dockerfile,
    /golang:1\.26\.6-bookworm@sha256:116d58cbd88c1297624acc6e967a060012422bacf9930927e23fb719189c6f36/,
  );
  assert.match(dockerfile, /^FROM scratch AS server$/m);
  assert.match(dockerfile, /^FROM scratch AS collector$/m);
  assert.equal((dockerfile.match(/^USER 65532:65532$/gm) ?? []).length, 2);
  assert.equal((dockerfile.match(/^STOPSIGNAL SIGTERM$/gm) ?? []).length, 2);
  assert.match(
    dockerfile,
    /^ENTRYPOINT \["\/usr\/local\/bin\/open-splunk-server"\]$/m,
  );
  assert.match(
    dockerfile,
    /^ENTRYPOINT \["\/usr\/local\/bin\/open-splunk-collector"\]$/m,
  );
  assert.match(
    dockerfile,
    /HEALTHCHECK[^\n]*\n\s*CMD \["\/usr\/local\/bin\/open-splunk-server", "healthcheck", "-url", "https:\/\/127\.0\.0\.1:8080\/readyz", "-ca-cert", "\/run\/open-splunk\/tls\/ca\.crt", "-server-name", "open-splunk-server"\]/,
  );
  assert.match(dockerfile, /\/var\/lib\/open-splunk\/state/);
  assert.match(dockerfile, /\/var\/lib\/open-splunk\/exports/);
  assert.match(dockerfile, /\/var\/lib\/open-splunk\/lock/);
  assert.match(dockerfile, /\/var\/lib\/open-splunk-collector/);
  assert.match(dockerfile, /install -d -o 0 -g 0 -m 0555/);
  assert.equal(
    (dockerfile.match(/^COPY --from=binaries \/image-rootfs\/(?:server|collector)\/ \/$/gm) ?? [])
      .length,
    2,
  );
  assert.match(
    dockerfile,
    /COPY internal\/spl\/completion_catalog\.json \.\/internal\/spl\/completion_catalog\.json/,
  );
  assert.match(dockerfile, /^EXPOSE 8080 4317$/m);
  assert.equal(
    (dockerfile.match(/GOOS="\$\{TARGETOS\}" GOARCH="\$\{TARGETARCH\}" go build/g) ?? [])
      .length,
    2,
  );
  assert.doesNotMatch(
    dockerfile,
    /^ARG (?:BUILDPLATFORM|TARGETOS|TARGETARCH)=/m,
    "BuildKit automatic platform arguments must not have local defaults",
  );
  assert.match(
    dockerfile,
    /test "\$\{TARGETOS\}" = "\$\{OPEN_SPLUNK_EXPECTED_TARGETOS\}"/,
  );
  assert.match(
    dockerfile,
    /test "\$\{TARGETARCH\}" = "\$\{OPEN_SPLUNK_EXPECTED_TARGETARCH\}"/,
  );
  assert.match(
    dockerfile,
    /actual_spl_compatibility_version=.*internal\/spl\/doc\.go/s,
  );
  assert.match(
    dockerfile,
    /printf '%s\\n' "\$\{actual_spl_compatibility_version\}" > \.spl-compatibility-version/,
  );
  assert.match(
    dockerfile,
    /spl_compatibility_version=%s[\s\S]*cat \.spl-compatibility-version/,
  );
  assert.match(
    dockerfile,
    /server_identity=.*sed -n '1,3p'/,
  );
  assert.match(
    dockerfile,
    /OPEN_SPLUNK_APPLICATION_VERSION.*does not match package\.json/s,
  );
  assert.doesNotMatch(
    dockerfile,
    /OPEN_SPLUNK_EXPECTED_(?:SPL|KNOWLEDGE)_COMPATIBILITY_VERSION/,
  );
  assert.doesNotMatch(
    dockerfile,
    /internal\/knowledge\/doc\.go|knowledge_compatibility_version/,
  );
  assert.match(
    dockerfile,
    /test "\$\{server_identity\}" = "\$\{expected_server\}"; \\\n+\s+test "\$\{collector_identity\}" = "\$\{expected_base\}";/,
  );
  assert.ok(
    (dockerfile.match(/^ARG SOURCE_DATE_EPOCH(?:=.*)?$/gm) ?? []).length >= 3,
    "Dockerfile must expose Docker/BuildKit's standard reproducibility argument",
  );
  assert.match(
    dockerfile,
    /^ARG SOURCE_DATE_EPOCH=\$\{OPEN_SPLUNK_SOURCE_DATE_EPOCH\}$/m,
  );
  assert.match(
    dockerfile,
    /test "\$\{SOURCE_DATE_EPOCH\}" = "\$\{OPEN_SPLUNK_SOURCE_DATE_EPOCH\}"/,
  );
  assert.match(
    dockerfile,
    /find \/image-rootfs -exec touch -h -d "@\$\{SOURCE_DATE_EPOCH\}" \{\} \+/,
  );
  assert.doesNotMatch(dockerfile, /^ADD /m);

  const serverTarget = dockerfile.split("FROM scratch AS server")[1]
    .split("FROM scratch AS collector")[0];
  const collectorTarget = dockerfile.split("FROM scratch AS collector")[1];
  assert.match(serverTarget, /open-splunk-server/);
  assert.doesNotMatch(serverTarget, /open-splunk-collector/);
  assert.match(collectorTarget, /open-splunk-collector/);
  assert.doesNotMatch(collectorTarget, /open-splunk-server/);
  for (const target of [serverTarget, collectorTarget]) {
    assert.doesNotMatch(target, /^RUN /m);
    assert.equal(
      (target.match(/^COPY /gm) ?? []).length,
      1,
      "scratch targets must copy a complete, timestamp-normalized rootfs",
    );
    assert.match(target, /org\.opencontainers\.image\.version/);
    assert.match(target, /org\.opencontainers\.image\.revision/);
    assert.match(target, /org\.opencontainers\.image\.created/);
  }
});

test("release publication creates immutable amd64 and arm64 GHCR images", async () => {
  const workflowDirectory = path.join(workspace, ".github", "workflows");
  const publicationWorkflows = (await readdir(workflowDirectory))
    .filter((name) => /^publish.*\.ya?ml$/.test(name))
    .toSorted();
  assert.deepEqual(publicationWorkflows, ["publish-images.yml"]);

  const workflow = await readFile(
    path.join(workflowDirectory, "publish-images.yml"),
    "utf8",
  );

  assert.match(workflow, /tags:\n\s+- "v\*"/);
  assert.match(
    workflow,
    /registry="ghcr\.io\/\$\{GITHUB_REPOSITORY_OWNER,,\}"/,
  );
  assert.match(workflow, /image_name: open-splunk-server/);
  assert.match(workflow, /image_name: open-splunk-collector/);
  assert.match(workflow, /target: server/);
  assert.match(workflow, /target: collector/);
  assert.equal(
    (workflow.match(/platform: linux\/amd64\n\s+architecture: amd64/g) ?? [])
      .length,
    2,
  );
  assert.equal(
    (workflow.match(/platform: linux\/arm64\n\s+architecture: arm64/g) ?? [])
      .length,
    2,
  );
  assert.match(workflow, /uses: docker\/build-push-action@v7/);
  assert.match(workflow, /GITHUB_TOKEN: \$\{\{ github\.token \}\}/);
  assert.match(workflow, /permissions:\n\s+actions: read\n\s+contents: read/);
  assert.match(
    workflow,
    /if \[\[ ! "\$RELEASE_TAG" =~ \^v\[0-9\]\+\\\.\[0-9\]\+\\\.\[0-9\]\+\$ \]\]/,
  );
  assert.match(
    workflow,
    /git merge-base --is-ancestor "\$GITHUB_SHA" refs\/remotes\/origin\/main/,
  );
  assert.match(workflow, /-f head_sha="\$GITHUB_SHA"/);
  assert.match(workflow, /select\(\.name == "CI" and \.conclusion == "success"\)/);
  assert.match(
    workflow,
    /ref: \$\{\{ needs\.verify\.outputs\.release_revision \}\}/,
  );
  assert.match(
    workflow,
    /OPEN_SPLUNK_SOURCE_REVISION=\$\{\{ needs\.verify\.outputs\.release_revision \}\}/,
  );
  assert.match(
    workflow,
    /package_version="\$\(node -p 'require\("\.\/package\.json"\)\.version'\)"/,
  );
  assert.match(
    workflow,
    /compatibility_version="\$\(node scripts\/read-spl-compatibility-version\.mjs\)"/,
  );
  assert.match(
    workflow,
    /package_version" != "0\.4\.0" \|\| "\$compatibility_version" != "0\.4"/,
  );
  assert.match(
    workflow,
    /steps\.release\.outputs\.application_version.*package_version/,
  );
  assert.doesNotMatch(
    workflow,
    /OPEN_SPLUNK_EXPECTED_(?:SPL|KNOWLEDGE)_COMPATIBILITY_VERSION/,
  );
  assert.doesNotMatch(
    workflow,
    /read-knowledge-compatibility-version\.mjs|knowledge_compatibility_version/,
  );
  assert.doesNotMatch(workflow, /runtime_revision/);
  assert.match(workflow, /push-by-digest=true/);
  assert.match(workflow, /provenance: mode=max/);
  assert.match(workflow, /sbom: true/);
  assert.match(workflow, /docker buildx imagetools create --tag/);
  assert.match(workflow, /expected_platforms=\$'linux\/amd64\\nlinux\/arm64'/);
  assert.doesNotMatch(workflow, /IMAGE_TAG.*latest/);

  const ciWorkflow = await readFile(
    path.join(workflowDirectory, "ci.yml"),
    "utf8",
  );
  assert.ok(
    (ciWorkflow.match(
      /application_version="\$\(node -p 'require\("\.\/package\.json"\)\.version'\)"/g,
    ) ?? []).length >= 2,
  );
  assert.ok(
    (ciWorkflow.match(
      /application_version" != "0\.4\.0" \|\| "\$compatibility_version" != "0\.4"/g,
    ) ?? []).length >= 2,
  );
  assert.doesNotMatch(
    ciWorkflow,
    /OPEN_SPLUNK_EXPECTED_(?:SPL|KNOWLEDGE)_COMPATIBILITY_VERSION/,
  );
  assert.doesNotMatch(
    ciWorkflow,
    /read-knowledge-compatibility-version\.mjs|knowledge_compatibility_version/,
  );
  assert.doesNotMatch(ciWorkflow, /if: env\.OPEN_SPLUNK_SPL_COMPATIBILITY_VERSION/);
});

test("images seed secure writable paths in their normalized rootfs trees", async () => {
  const dockerfile = await readFile(path.join(workspace, "Dockerfile"), "utf8");
  const serverTarget = dockerfile.split("FROM scratch AS server")[1]
    .split("FROM scratch AS collector")[0];
  const collectorTarget = dockerfile.split("FROM scratch AS collector")[1];

  assert.match(
    dockerfile,
    /install -d -o 65532 -g 65532 -m 0700 \\\n+\s*\/image-rootfs\/server\/var\/lib\/open-splunk\/state\/private \\\n+\s*\/image-rootfs\/server\/var\/lib\/open-splunk\/exports\/private \\\n+\s*\/image-rootfs\/server\/var\/lib\/open-splunk\/recovery\/private \\\n+\s*\/image-rootfs\/server\/var\/lib\/open-splunk\/lock\/private \\\n+\s*\/image-rootfs\/collector\/var\/lib\/open-splunk-collector;/,
  );
  assert.match(
    serverTarget,
    /^COPY --from=binaries \/image-rootfs\/server\/ \/$/m,
  );
  assert.match(
    serverTarget,
    /^WORKDIR \/var\/lib\/open-splunk\/state\/private$/m,
  );
  assert.match(
    collectorTarget,
    /^WORKDIR \/var\/lib\/open-splunk-collector$/m,
  );
});

test("Docker context is a deny-by-default source allowlist", async () => {
  const dockerignore = await readFile(
    path.join(workspace, ".dockerignore"),
    "utf8",
  );
  const rules = dockerignore
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "" && !line.startsWith("#"));

  assert.equal(rules[0], "**");
  for (const required of [
    "!Dockerfile",
    "!go.mod",
    "!go.sum",
    "!package.json",
    "!package-lock.json",
    "!app/**/*.tsx",
    "!cmd/**/*.go",
    "!gen/**/*.go",
    "!internal/**/*.go",
    "!internal/spl/completion_catalog.json",
    "!migrations/clickhouse/*.sql",
    "!migrations/sqlite/*.sql",
    "!proto/**/*.proto",
    "**/*_test.go",
    "**/*.test.ts",
  ]) {
    assert.ok(rules.includes(required), `missing Docker context rule ${required}`);
  }
  for (const forbidden of [
    "!.git/**",
    "!build/**",
    "!deploy/**",
    "!.env",
    "!.env.*",
    "!app/**",
    "!cmd/**",
    "!internal/**",
  ]) {
    assert.ok(!rules.includes(forbidden), `unsafe Docker context rule ${forbidden}`);
  }
});

test("ELF verifier checks the extracted binary machine architecture", async (t) => {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-elf-"));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const binaryPath = path.join(fixture, "open-splunk-server");
  const header = Buffer.alloc(64);
  header.set([0x7f, 0x45, 0x4c, 0x46], 0);
  header[4] = 2;
  header[5] = 1;
  header[6] = 1;
  header.writeUInt16LE(2, 16);
  header.writeUInt16LE(183, 18);
  header.writeUInt32LE(1, 20);
  await writeFile(binaryPath, header);
  const verifier = path.join(workspace, "scripts", "verify-elf-architecture.mjs");

  const arm64 = spawnSync(process.execPath, [verifier, "arm64", binaryPath], {
    encoding: "utf8",
  });
  assert.equal(arm64.status, 0, arm64.stderr);
  assert.match(arm64.stdout, /ELF64 AArch64/);

  const amd64 = spawnSync(process.execPath, [verifier, "amd64", binaryPath], {
    encoding: "utf8",
  });
  assert.equal(amd64.status, 1);
  assert.match(amd64.stderr, /does not match x86-64/);
});

test("OCI build anchors both local image tags to clean HEAD", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const sourceDateEpoch = git(fixture, ["show", "-s", "--format=%ct", revision]);
  const docker = await installDockerShim(fixture);
  const privateTemporaryRoot = await mkdtemp(
    path.join(fixture, ".git", "oci-private-root-"),
  );

  const result = runBuildOCI(fixture, revision, docker, {
    TMPDIR: privateTemporaryRoot,
  });

  assert.equal(result.status, 0, result.stderr);
  const invocations = await readFile(docker.log, "utf8");
  assert.match(invocations, /build .*--platform linux\/amd64 .*--target server /);
  assert.match(invocations, /build .*--platform linux\/amd64 .*--target collector /);
  assert.doesNotMatch(invocations, /build --no-cache/);
  assert.match(invocations, /--build-arg OPEN_SPLUNK_APPLICATION_VERSION=0\.4\.0/);
  assert.doesNotMatch(
    invocations,
    /OPEN_SPLUNK_EXPECTED_(?:SPL|KNOWLEDGE)_COMPATIBILITY_VERSION/,
  );
  assert.match(
    invocations,
    new RegExp(`--build-arg OPEN_SPLUNK_SOURCE_REVISION=${revision}`),
  );
  assert.match(
    invocations,
    new RegExp(`SOURCE_DATE_EPOCH=${sourceDateEpoch} DOCKER_BUILDKIT=1 build`),
  );
  assert.match(
    invocations,
    new RegExp(`--build-arg SOURCE_DATE_EPOCH=${sourceDateEpoch}`),
  );
  assert.match(
    invocations,
    new RegExp(`--build-arg OPEN_SPLUNK_SOURCE_DATE_EPOCH=${sourceDateEpoch}`),
  );
  assert.match(invocations, /image tag .*registry\.invalid\/open-splunk\/server:test/);
  assert.match(invocations, /image tag .*registry\.invalid\/open-splunk\/collector:test/);
  assert.doesNotMatch(invocations, /push/);
  assert.deepEqual(
    (await readdir(privateTemporaryRoot)).filter((entry) =>
      entry.startsWith("open-splunk-oci."),
    ),
    [],
  );
});

test("OCI build rejects an application version that differs from package.json", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);

  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_APPLICATION_VERSION: "0.4.1",
  });

  assert.equal(result.status, 1);
  assert.match(
    result.stderr,
    /does not match committed package version 0\.4\.0/,
  );
  await assert.rejects(access(docker.log, constants.F_OK));
});

test("OCI cold rebuild bypasses cache for both targets and rejects unsafe values", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);

  const invalid = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_OCI_NO_CACHE: "true",
  });
  assert.equal(invalid.status, 1);
  assert.match(invalid.stderr, /OPEN_SPLUNK_OCI_NO_CACHE must be empty or 1/);
  await assert.rejects(access(docker.log, constants.F_OK));

  const cold = runMakeOCI(fixture, revision, docker, {
    OPEN_SPLUNK_OCI_NO_CACHE: "1",
  });
  assert.equal(cold.status, 0, cold.stderr);
  const buildInvocations = (await readFile(docker.log, "utf8"))
    .split("\n")
    .filter((line) => line.includes("DOCKER_BUILDKIT=1 build "));
  assert.equal(buildInvocations.length, 2);
  for (const invocation of buildInvocations) {
    assert.match(invocation, /build --no-cache --file /);
  }
});

test("Make propagates the ARM64 target into Docker and the binary contract", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);

  const result = runMakeOCI(fixture, revision, docker, {
    OPEN_SPLUNK_OCI_PLATFORM: "linux/arm64",
  });

  assert.equal(result.status, 0, result.stderr);
  const buildInvocations = (await readFile(docker.log, "utf8"))
    .split("\n")
    .filter((line) => line.includes("DOCKER_BUILDKIT=1 build "));
  assert.equal(buildInvocations.length, 2);
  for (const invocation of buildInvocations) {
    assert.match(invocation, /--platform linux\/arm64/);
    assert.match(
      invocation,
      /--build-arg OPEN_SPLUNK_EXPECTED_TARGETOS=linux/,
    );
    assert.match(
      invocation,
      /--build-arg OPEN_SPLUNK_EXPECTED_TARGETARCH=arm64/,
    );
    assert.doesNotMatch(invocation, /OPEN_SPLUNK_EXPECTED_TARGETARCH=amd64/);
  }
});

test("OCI publication resolves Docker Hub tags with Linux familiar-name filters", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_SERVER_IMAGE: "docker.io/library/open-splunk-server:test",
    OPEN_SPLUNK_COLLECTOR_IMAGE:
      "docker.io/open-splunk/collector:test",
    OPEN_SPLUNK_TEST_REQUIRE_FAMILIAR_DOCKER_HUB_FILTER: "1",
  });

  assert.equal(result.status, 0, result.stderr);
  const invocations = await readFile(docker.log, "utf8");
  assert.match(
    invocations,
    /image ls --quiet --no-trunc --filter reference=open-splunk-server:test/,
  );
  assert.match(
    invocations,
    /image ls --quiet --no-trunc --filter reference=open-splunk\/collector:test/,
  );
  assert.match(
    invocations,
    /image tag .* docker\.io\/library\/open-splunk-server:test/,
  );
  assert.match(
    invocations,
    /image tag .* docker\.io\/open-splunk\/collector:test/,
  );
});

test("OCI build sends Docker only committed materialized source", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  git(fixture, [
    "update-index",
    "--assume-unchanged",
    "internal/fixture_context.go",
  ]);
  git(fixture, [
    "update-index",
    "--skip-worktree",
    "scripts/materialize-git-snapshot.mjs",
  ]);
  await writeFile(
    path.join(fixture, "internal", "fixture_context.go"),
    "package fixture // poisoned worktree\n",
  );
  await writeFile(
    path.join(fixture, "scripts", "materialize-git-snapshot.mjs"),
    "throw new Error('worktree materializer must not execute');\n",
  );
  await writeFile(
    path.join(fixture, ".git", "info", "exclude"),
    "internal/hidden_uncommitted.go\n",
    { flag: "a" },
  );
  await writeFile(
    path.join(fixture, "internal", "hidden_uncommitted.go"),
    "package fixture // hidden uncommitted source\n",
  );
  assert.equal(git(fixture, ["status", "--porcelain=v1"]), "");

  const result = runBuildOCI(fixture, revision, docker);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    await readFile(docker.contextRecord, "utf8"),
    "package fixture // committed snapshot\n",
  );
  assert.equal(
    await readFile(`${docker.contextRecord}.hidden`, "utf8"),
    "absent\n",
  );
  const contextRoot = (
    await readFile(`${docker.contextRecord}.root`, "utf8")
  ).trim();
  assert.notEqual(contextRoot, fixture);
  await assert.rejects(access(contextRoot, constants.F_OK));
});

test("make oci executes the committed launcher", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  const sentinel = path.join(fixture, ".git", "worktree-launcher-ran");
  await writeFile(
    path.join(fixture, "scripts", "build-oci.sh"),
    "#!/usr/bin/env bash\n" +
      `touch ${JSON.stringify(sentinel)}\n` +
      "exit 97\n",
  );
  await chmod(path.join(fixture, "scripts", "build-oci.sh"), 0o755);
  git(fixture, ["update-index", "--skip-worktree", "scripts/build-oci.sh"]);
  assert.equal(git(fixture, ["status", "--porcelain=v1"]), "");

  const result = runMakeOCI(fixture, revision, docker);

  assert.equal(result.status, 0, result.stderr);
  await assert.rejects(access(sentinel, constants.F_OK));
  assert.match(
    await readFile(docker.log, "utf8"),
    /image tag .*registry\.invalid\/open-splunk\/server:test/,
  );
});

test("OCI publication locks coordinate independent clones through one daemon", async (t) => {
  const firstFixture = await ociFixture(t);
  const secondFixture = await ociFixture(t);
  const firstRevision = git(firstFixture, ["rev-parse", "HEAD"]);
  const secondRevision = git(secondFixture, ["rev-parse", "HEAD"]);
  const firstDocker = await installDockerShim(firstFixture);
  const secondDocker = await installDockerShim(secondFixture);
  const sharedDaemonState = await mkdtemp(
    path.join(tmpdir(), "open-splunk-shared-docker-state-"),
  );
  const holdMarker = path.join(sharedDaemonState, "publication-held");
  const holdRelease = path.join(sharedDaemonState, "publication-release");
  const firstBuild = startBuildOCI(firstFixture, firstRevision, firstDocker, {
    OPEN_SPLUNK_TEST_DOCKER_STATE: sharedDaemonState,
    OPEN_SPLUNK_TEST_HOLD_MARKER: holdMarker,
    OPEN_SPLUNK_TEST_HOLD_RELEASE: holdRelease,
  });
  t.after(async () => {
    await writeFile(holdRelease, "release\n");
    if (firstBuild.child.exitCode === null && firstBuild.child.signalCode === null) {
      firstBuild.child.kill("SIGTERM");
    }
    await firstBuild.completed;
    await rm(sharedDaemonState, { recursive: true, force: true });
  });

  await waitForPath(holdMarker);
  const lockNames = (await readdir(path.join(sharedDaemonState, "containers")))
    .toSorted();
  assert.deepEqual(lockNames, [
    publicationLockName("registry.invalid/open-splunk/collector:test"),
    publicationLockName("registry.invalid/open-splunk/server:test"),
  ].toSorted());

  const competing = runBuildOCI(secondFixture, secondRevision, secondDocker, {
    OPEN_SPLUNK_TEST_DOCKER_STATE: sharedDaemonState,
  });

  assert.equal(competing.status, 1);
  assert.match(competing.stderr, /another OCI publication is active|stale daemon lock/);
  assert.ok(competing.stderr.includes(lockNames[0]), competing.stderr);
  assert.deepEqual(
    (await readdir(path.join(sharedDaemonState, "containers"))).toSorted(),
    lockNames,
  );
  assert.doesNotMatch(
    await readFile(secondDocker.log, "utf8"),
    /image tag .*registry\.invalid\/open-splunk\/(server|collector):test/,
  );

  await writeFile(holdRelease, "release\n");
  const completed = await firstBuild.completed;
  assert.equal(completed.status, 0, completed.stderr);
  assert.deepEqual(await readdir(path.join(sharedDaemonState, "containers")), []);
});

test("OCI publication fails closed when a daemon lock identity changes", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);

  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_TEST_TAMPER_LOCK_OWNER: "1",
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /publication lock identity changed/);
  assert.match(result.stderr, /publication lock ownership changed while held/);
  assert.doesNotMatch(
    await readFile(docker.log, "utf8"),
    /image tag .*registry\.invalid\/open-splunk\/(server|collector):test/,
  );
  assert.equal(
    (await readdir(path.join(docker.state, "containers"))).length,
    1,
    "the identity-changing lock must be left in place for explicit inspection",
  );
});

test("OCI termination reconciles a daemon lock created after CLI interruption", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);

  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_TEST_SIGNAL_AFTER_LOCK_CREATE: "TERM",
  });

  assert.equal(result.status, 143, result.stderr);
  assert.deepEqual(await readdir(path.join(docker.state, "containers")), []);
  assert.doesNotMatch(
    await readFile(docker.log, "utf8"),
    /image tag .*registry\.invalid\/open-splunk\/(server|collector):test/,
  );
});

test("OCI termination after server publication rolls back and releases state", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  const privateTemporaryRoot = await mkdtemp(
    path.join(fixture, ".git", "oci-signal-root-"),
  );

  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_TEST_SIGNAL_AFTER_SERVER_TAG: "TERM",
    TMPDIR: privateTemporaryRoot,
  });

  assert.equal(result.status, 143, result.stderr);
  assert.deepEqual(await readdir(path.join(docker.state, "containers")), []);
  await assert.rejects(
    access(path.join(docker.state, "server-final-image-id"), constants.F_OK),
  );
  assert.deepEqual(
    (await readdir(privateTemporaryRoot)).filter((entry) =>
      entry.startsWith("open-splunk-oci."),
    ),
    [],
  );
  const invocations = await readFile(docker.log, "utf8");
  assert.match(invocations, /image rm --force open-splunk-build:/);
  assert.match(
    invocations,
    /image tag .* registry\.invalid\/open-splunk\/server:test/,
  );
  assert.match(
    invocations,
    /image rm --force registry\.invalid\/open-splunk\/server:test/,
  );
  assert.doesNotMatch(
    invocations,
    /image tag .*registry\.invalid\/open-splunk\/collector:test/,
  );
});

test("OCI build restores the server tag when collector publication fails", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  const previousServerImage = `sha256:${"a".repeat(64)}`;
  const previousCollectorImage = `sha256:${"d".repeat(64)}`;

  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_TEST_FAIL_COLLECTOR_TAG: "1",
    OPEN_SPLUNK_TEST_PREVIOUS_COLLECTOR_IMAGE_ID: previousCollectorImage,
    OPEN_SPLUNK_TEST_PREVIOUS_SERVER_IMAGE_ID: previousServerImage,
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /restored the previous publication state/);
  const invocations = await readFile(docker.log, "utf8");
  assert.ok(invocations.includes(
    "image ls --quiet --no-trunc --filter reference=registry.invalid/open-splunk/server:test",
  ));
  assert.match(invocations, /image tag .* registry\.invalid\/open-splunk\/server:test/);
  assert.match(
    invocations,
    new RegExp(`image tag ${previousServerImage} registry\\.invalid/open-splunk/server:test`),
  );
  assert.match(
    invocations,
    new RegExp(`image tag ${previousCollectorImage} registry\\.invalid/open-splunk/collector:test`),
  );
  assert.equal(
    (await readFile(path.join(docker.state, "collector-final-image-id"), "utf8")).trim(),
    previousCollectorImage,
  );
  assert.doesNotMatch(result.stdout, /^built /m);
});

test("OCI build removes a newly introduced server tag when collector publication fails", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);

  const result = runBuildOCI(fixture, revision, docker, {
    OPEN_SPLUNK_TEST_FAIL_COLLECTOR_TAG: "1",
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /restored the previous publication state/);
  const invocations = await readFile(docker.log, "utf8");
  assert.ok(invocations.includes(
    "image rm --force registry.invalid/open-splunk/server:test",
  ));
  assert.doesNotMatch(result.stdout, /^built /m);
});

test("OCI build rejects dirty or falsely labeled source before Docker", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  await writeFile(path.join(fixture, "untracked-secret.env"), "secret\n");

  const dirty = runBuildOCI(fixture, revision, docker);

  assert.equal(dirty.status, 1);
  assert.match(dirty.stderr, /clean worktree/);
  await assert.rejects(access(docker.log, constants.F_OK));

  await rm(path.join(fixture, "untracked-secret.env"));
  const mismatch = runBuildOCI(
    fixture,
    "0123456789abcdef0123456789abcdef01234567",
    docker,
  );
  assert.equal(mismatch.status, 1);
  assert.match(mismatch.stderr, /must equal the current HEAD/);
  await assert.rejects(access(docker.log, constants.F_OK));
});

test("OCI build rejects unsafe identity, platform, and image references", async (t) => {
  const fixture = await ociFixture(t);
  const revision = git(fixture, ["rev-parse", "HEAD"]);
  const docker = await installDockerShim(fixture);
  const cases = [
    {
      environment: { OPEN_SPLUNK_APPLICATION_VERSION: "1.2.3;touch-pwned" },
      message: /semantic version/,
    },
    {
      environment: { OPEN_SPLUNK_OCI_PLATFORM: "linux/386" },
      message: /OCI platform/,
    },
    {
      environment: { OPEN_SPLUNK_SERVER_IMAGE: "--output=type=local" },
      message: /server image/,
    },
    {
      environment: {
        OPEN_SPLUNK_SERVER_IMAGE: "alias-image:test",
        OPEN_SPLUNK_COLLECTOR_IMAGE: "docker.io/library/alias-image:test",
      },
      message: /distinct Docker names/,
    },
    {
      environment: {
        OPEN_SPLUNK_SERVER_IMAGE: "library/legacy-alias:test",
        OPEN_SPLUNK_COLLECTOR_IMAGE:
          "index.docker.io/library/legacy-alias:test",
      },
      message: /distinct Docker names/,
    },
  ];

  for (const fixtureCase of cases) {
    const result = runBuildOCI(
      fixture,
      revision,
      docker,
      fixtureCase.environment,
    );
    assert.equal(result.status, 1);
    assert.match(result.stderr, fixtureCase.message);
  }
  await assert.rejects(access(docker.log, constants.F_OK));
});

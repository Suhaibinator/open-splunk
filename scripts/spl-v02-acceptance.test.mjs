/* eslint-disable no-await-in-loop */
// The synthetic Git history and receipt files must be built in deterministic order.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const workspace = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const verifier = path.join(workspace, "scripts/verify-spl-v02-acceptance.mjs");
const manifestPath = path.join(
  workspace,
  "docs/evidence/spl-v0.2-activation/manifest.json",
);
const reportPath = path.join(workspace, "docs/spl-compatibility-v0.2-acceptance.md");
const strictJSONPath = path.join(workspace, "scripts/strict-json.mjs");
const currentManifestBytes = await readFile(manifestPath);
const currentManifest = JSON.parse(currentManifestBytes.toString("utf8"));
const currentPhase = currentManifest.phase;
let acceptedEvidenceRevision = null;
try {
  const v03Manifest = JSON.parse(await readFile(path.join(
    workspace,
    "docs/evidence/spl-v0.3/manifest.json",
  ), "utf8"));
  if (currentPhase === "accepted" &&
      v03Manifest.prerequisite?.v02_status === "accepted") {
    acceptedEvidenceRevision = v03Manifest.prerequisite.evidence_revision;
  }
} catch (error) {
  if (error?.code !== "ENOENT") throw error;
}

function runNode(argumentsList, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, argumentsList, {
      cwd: options.cwd ?? workspace,
      env: options.env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("close", (status) => resolve({ status, stdout, stderr }));
  });
}

function runProcess(command, argumentsList, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, argumentsList, {
      cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("close", (status) => resolve({ status, stdout, stderr }));
  });
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function v02OperatorBytes(relative, source) {
  if (relative === "README.md") {
    return source
      .replace(
        "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
        "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
      )
      .replace(
        "[`docs/spl-compatibility-v0.3.md`](docs/spl-compatibility-v0.3.md)",
        "[`docs/spl-compatibility-v0.2.md`](docs/spl-compatibility-v0.2.md)",
      )
      .replace(
        "[`v0.3 migration and read-only audit guide`](docs/spl-compatibility-v0.3-migration.md)",
        "[`v0.2 migration and read-only audit guide`](docs/spl-compatibility-v0.2-migration.md)",
      )
      .replace(
        "[`v0.3 acceptance report`](docs/spl-compatibility-v0.3-acceptance.md)",
        "[`v0.2 acceptance report`](docs/spl-compatibility-v0.2-acceptance.md)",
      );
  }
  if (relative === "deploy/generate-env.sh") {
    return source.replace(
      "application_version=${OPEN_SPLUNK_APPLICATION_VERSION:-0.2.0}",
      "application_version=${OPEN_SPLUNK_APPLICATION_VERSION:-0.1.0}",
    );
  }
  if (relative === "deploy/README.md") {
    return source
      .replace(
        "export OPEN_SPLUNK_APPLICATION_VERSION=0.2.0\n" +
          "export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3\n",
        "export OPEN_SPLUNK_APPLICATION_VERSION=0.1.0\n" +
          "export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2\n",
      )
      .replace(
        "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 ./generate-env.sh",
        "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 ./generate-env.sh",
      );
  }
  if (relative === "docs/collector-deployment.md") {
    return source.replace(
      "export OPEN_SPLUNK_COLLECTOR_VERSION=0.2.0",
      "export OPEN_SPLUNK_COLLECTOR_VERSION=0.1.0",
    );
  }
  return source.replace(
    "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
  );
}

async function historicalV02EvidenceRevision() {
  const relative = path.relative(workspace, manifestPath);
  const result = await runProcess(
    "git",
    ["log", "-1", "--format=%H", "--", relative],
    workspace,
  );
  assert.equal(result.status, 0, result.stderr);
  const revision = result.stdout.trim();
  assert.match(revision, /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/);
  return revision;
}

function qualificationCandidateManifest(source) {
  const candidate = structuredClone(source);
  candidate.phase = "qualification-candidate";
  candidate.runtime = {
    revision_binding: "containing-commit",
    revision: null,
    tree: null,
    candidate_manifest_sha256: null,
    candidate_acceptance_sha256: null,
    remote_ref: null,
    remote_readback_revision: null,
    remote_readback_at_utc: null,
  };
  candidate.ci = {
    repository: null,
    workflow: "CI",
    run_id: null,
    url: null,
    event: null,
    head_branch: null,
    head_revision: null,
    checkout_revision: null,
    checkout_tree: null,
    release_artifact_source_revision: null,
    status: null,
    conclusion: null,
    completed_at_utc: null,
    job_set_complete: false,
    jobs: [],
  };
  candidate.release = {
    source_revision: null,
    spl_compatibility_version: null,
    application_version: null,
    binary_identities: [],
    artifact_digests: [],
  };
  candidate.receipts = [];
  candidate.decision = {
    status: "pending",
    stable_publication_authorized: false,
    reason: "Synthetic candidate remains distribution-blocked.",
  };
  return candidate;
}

test("the current v0.2 activation bundle verifies its exact declared phase", async () => {
  if (currentPhase === "accepted") {
    const historicalRevision = await historicalV02EvidenceRevision();
    if (acceptedEvidenceRevision !== null) {
      assert.equal(acceptedEvidenceRevision, historicalRevision);
    }
    const evidenceRevision = acceptedEvidenceRevision ?? historicalRevision;
    const result = await runNode([
      verifier,
      "--phase", "accepted",
      "--evidence-revision", evidenceRevision,
      "--print-evidence-binding",
    ]);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(
      result.stdout,
      `${evidenceRevision} ${sha256(currentManifestBytes)}\n`,
    );
    return;
  }
  const argumentsList = [
    verifier,
    "--phase",
    currentPhase,
  ];
  if (currentPhase === "implementation-checkpoint") argumentsList.push("--allow-dirty");
  const result = await runNode(argumentsList);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, `SPL v0.2 ${currentPhase} evidence verified\n`);
});

test("the bundle cannot be confused with a different evidence phase", async () => {
  const differentPhase = currentPhase === "accepted" ?
    "qualification-candidate" : "accepted";
  const result = await runNode([verifier, "--phase", differentPhase]);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, new RegExp(
    `manifest phase ${currentPhase} does not equal expected ${differentPhase}|` +
    "git show .*spl-v0\\.2-activation/manifest",
  ));
});

test("publication output is available only for strict accepted verification", async () => {
  for (const argumentsList of [
    [verifier, "--phase", "implementation-checkpoint", "--publication"],
    [verifier, "--phase", "qualification-candidate", "--print-runtime-revision"],
    [verifier, "--phase", "accepted", "--allow-dirty"],
    [verifier, "--phase", "accepted", "--print-evidence-binding"],
    [verifier, "--phase", "accepted", "--evidence-revision", "not-a-revision"],
  ]) {
    const result = await runNode(argumentsList);
    assert.equal(result.status, 2, `${argumentsList.join(" ")}\n${result.stderr}`);
    assert.match(result.stderr, /^usage:/);
  }
});

test("accepted ancestor mode is exact-revision and machine-binding scoped", async () => {
  const source = await readFile(verifier, "utf8");
  assert.match(source, /--evidence-revision <E> --print-evidence-binding/);
  assert.match(source, /accepted v0\.2 evidence revision E must be an ancestor/);
  assert.match(source, /showBytes\(evidenceRevision, manifestRelative\)/);
  assert.match(source, /process\.stdout\.write\(`\$\{evidenceRevision\} \$\{sha256\(manifestBytes\)\}\\n`\)/);
});

test("frontend CI retains the accepted v0.2 R/E history", async () => {
  const workflow = await readFile(
    path.join(workspace, ".github/workflows/ci.yml"),
    "utf8",
  );
  const start = workflow.indexOf("  frontend:");
  const end = workflow.indexOf("\n  go-lint:", start);
  assert.ok(start >= 0 && end > start, "frontend job is missing");
  const frontend = workflow.slice(start, end);
  assert.match(frontend, /uses: actions\/checkout@v7\n\s+with:\n\s+fetch-depth: 0/);
  assert.match(frontend, /run: npm run test:frontend/);
});

test("schema and verifier pin the strict R/E acceptance invariants", async () => {
  const [schema, verifierSource, workflow, report] = await Promise.all([
    readFile(path.join(
      workspace,
      "docs/evidence/spl-v0.2-activation/manifest.schema.json",
    ), "utf8"),
    readFile(verifier, "utf8"),
    readFile(path.join(workspace, ".github/workflows/publish-images.yml"), "utf8"),
    readFile(reportPath, "utf8"),
  ]);
  assert.match(schema, /"additionalProperties": false/);
  assert.match(schema, /"recorded-runtime-parent"/);
  assert.match(schema, /"spl_compatibility_version": \{ "enum": \["0\.2", null\] \}/);
  assert.match(verifierSource, /E must be a non-merge direct child of R/);
  assert.match(verifierSource, /E must be documentation-only and confined to acceptance evidence paths/);
  assert.match(verifierSource, /live CI job set is incomplete, duplicated, or paginated/);
  assert.match(verifierSource, /immutable runtime tag remote readback does not equal R/);
  assert.match(verifierSource, /server binary identity must report SPL 0\.2/);
  assert.match(verifierSource, /const EXPECTED_JOBS = \[/);
  assert.match(verifierSource, /ids\.size === EXPECTED_JOBS\.length/);
  assert.match(verifierSource, /job\.status === "completed" && job\.conclusion === "success"/);
  assert.match(verifierSource, /release tag remote readback does not equal E/);
  assert.match(verifierSource, /oldMode !== "160000" && entry\.newMode !== "160000"/);
  assert.match(verifierSource, /accepted receipt IDs and paths must equal the exact required evidence set/);
  assert.match(workflow, /node scripts\/verify-spl-v02-acceptance\.mjs/);
  assert.doesNotMatch(workflow, /SPL compatibility 0\.2 has no strict accepted evidence bundle/);
  assert.match(report, new RegExp(
    `^\\*\\*Evidence phase:\\*\\* \`${currentPhase}\`$`,
    "m",
  ));
});

test("strict JSON rejects duplicate keys at every nesting level", async () => {
  const probe = `
    import { parseStrictJSON } from ${JSON.stringify(strictJSONPath)};
    for (const source of [
      '{"phase":"accepted","phase":"implementation-checkpoint"}',
      '{"runtime":{"tree":null,"tree":"${"0".repeat(40)}"}}',
      '[{"id":1,"id":2}]',
    ]) {
      try {
        parseStrictJSON(source, "probe");
        process.exit(3);
      } catch (error) {
        if (!String(error).includes("duplicate JSON object key")) process.exit(4);
      }
    }
  `;
  const result = await runNode(["--input-type=module", "--eval", probe]);
  assert.equal(result.status, 0, result.stderr);
});

test("strict JSON retains JSON.parse syntax and value semantics", async () => {
  const probe = `
    import assert from "node:assert/strict";
    import { parseStrictJSON } from ${JSON.stringify(strictJSONPath)};
    const source = JSON.stringify({
      "quoted\\key": { unicode: "☃", array: [true, false, null, -1.25e3] },
    });
    assert.deepEqual(parseStrictJSON(source, "probe"), JSON.parse(source));
    for (const invalid of ['{"a":}', '{"a":1} trailing', '[1,]']) {
      assert.throws(() => parseStrictJSON(invalid, "probe"), SyntaxError);
    }
  `;
  const result = await runNode(["--input-type=module", "--eval", probe]);
  assert.equal(result.status, 0, result.stderr);
});

test("the exact accepted CI inventory tracks every expanded current job", async () => {
  const source = await readFile(verifier, "utf8");
  const block = source.match(/const EXPECTED_JOBS = \[(.*?)\];/s)?.[1] ?? "";
  const jobs = [...block.matchAll(/^  "([^"]+)",$/gm)].map((match) => match[1]);
  assert.equal(jobs.length, 28);
  assert.equal(new Set(jobs).size, 28);
  assert.ok(jobs.includes("Go tests (atomic coverage)"));
  assert.ok(jobs.includes("Knowledge object fuzz (hec-protocol)"));
  assert.ok(jobs.includes("Production binaries (macos)"));
  assert.ok(jobs.includes("SPL compatibility pinned ClickHouse verticals"));
  assert.ok(!jobs.some((name) => name.includes("linux-arm64")));
});

test("frontend runner requires v0.2 tests and discovers v0.3 tests only when present", async () => {
  const runner = await readFile(path.join(workspace, "scripts/test-frontend.mjs"), "utf8");
  assert.match(runner, /"spl-v02-acceptance\.test\.mjs"/);
  assert.match(runner, /"spl-v02-stats-by-ci\.test\.mjs"/);
  assert.match(runner, /for \(const optional of \[/);
  assert.match(runner, /"spl-v03-acceptance\.test\.mjs"/);
  assert.match(runner, /"spl-v03-ci\.test\.mjs"/);
  assert.match(runner, /scriptTests\.push\(optional\)/);
  assert.match(runner, /if \(error\?\.code !== "ENOENT"\) throw error/);
});

test("the current phase has the exact permitted provenance shape", async () => {
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  assert.equal(manifest.phase, currentPhase);
  if (currentPhase !== "accepted") {
    assert.equal(manifest.runtime.revision, null);
    assert.equal(manifest.runtime.tree, null);
    assert.equal(manifest.ci.run_id, null);
    assert.equal(manifest.ci.head_branch, null);
    assert.equal(manifest.ci.job_set_complete, false);
    assert.deepEqual(manifest.ci.jobs, []);
    assert.equal(manifest.release.spl_compatibility_version, null);
    assert.deepEqual(manifest.receipts, []);
    assert.equal(manifest.decision.status, "pending");
    assert.equal(manifest.decision.stable_publication_authorized, false);
  } else {
    assert.equal(manifest.runtime.revision_binding, "recorded-runtime-parent");
    assert.equal(manifest.ci.job_set_complete, true);
    assert.equal(manifest.release.spl_compatibility_version, "0.2");
    assert.equal(manifest.decision.status, "accepted");
    assert.equal(manifest.decision.stable_publication_authorized, true);
  }
});

test("checkpoint verification rejects unknown manifest fields", async (t) => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), "open-splunk-v02-schema-"));
  t.after(() => rm(temporary, { recursive: true, force: true }));
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  manifest.unreviewed = true;
  const mutated = path.join(temporary, "manifest.json");
  await writeFile(mutated, `${JSON.stringify(manifest, null, 2)}\n`);

  // The verifier intentionally does not accept an alternate manifest path.
  // This isolated mutation therefore exercises the same closed schema with a
  // tiny module importing no repository state.
  const probe = path.join(temporary, "probe.mjs");
  await writeFile(probe, `
    import fs from "node:fs";
    const schema = JSON.parse(fs.readFileSync(${JSON.stringify(path.join(
      workspace,
      "docs/evidence/spl-v0.2-activation/manifest.schema.json",
    ))}, "utf8"));
    const manifest = JSON.parse(fs.readFileSync(${JSON.stringify(mutated)}, "utf8"));
    if (schema.additionalProperties !== false ||
        Object.keys(manifest).some((key) => !Object.hasOwn(schema.properties, key))) {
      process.exit(1);
    }
  `);
  const result = await runNode([probe]);
  assert.equal(result.status, 1);
});

test("synthetic clean R/E lineage passes accepted and ancestor verification", async (t) => {
  const fixture = await mkdtemp(path.join(os.tmpdir(), "open-splunk-v02-accepted-"));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  async function put(relative, contents) {
    const absolute = path.join(fixture, relative);
    await mkdir(path.dirname(absolute), { recursive: true });
    await writeFile(absolute, contents);
  }
  async function git(...argumentsList) {
    const result = await runProcess("git", argumentsList, fixture);
    assert.equal(result.status, 0, `${argumentsList.join(" ")}\n${result.stderr}`);
    return result.stdout.trim();
  }
  await mkdir(path.join(fixture, "scripts"), { recursive: true });
  await copyFile(verifier, path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"));
  await copyFile(strictJSONPath, path.join(fixture, "scripts/strict-json.mjs"));
  await mkdir(path.join(fixture, "docs/evidence/spl-v0.2-activation"), {
    recursive: true,
  });
  await copyFile(
    path.join(workspace, "docs/evidence/spl-v0.2-activation/manifest.schema.json"),
    path.join(fixture, "docs/evidence/spl-v0.2-activation/manifest.schema.json"),
  );
  const candidate = qualificationCandidateManifest(currentManifest);
  const fixtureArtifacts = new Map(candidate.artifacts.map((artifact) =>
    [artifact.id, artifact.path]));
  for (const [id, relative] of fixtureArtifacts) {
    const absolute = path.join(fixture, relative);
    try {
      await readFile(absolute);
    } catch {
      await put(relative, `${id} synthetic fixture\n`);
    }
  }
  await put("internal/spl/doc.go", 'package spl\n\nconst CompatibilityVersion = "0.2"\n');
  for (const relative of [
    "README.md", "deploy/generate-env.sh", "deploy/README.md",
    "docs/collector-deployment.md", "integration/README.md",
  ]) {
    await put(relative, v02OperatorBytes(
      relative,
      await readFile(path.join(workspace, relative), "utf8"),
    ));
  }
  await chmod(path.join(fixture, "deploy/generate-env.sh"), 0o755);
  for (const artifact of candidate.artifacts) {
    if (["scripts/build-release.sh", "scripts/build-oci.sh"].includes(artifact.path)) {
      await chmod(path.join(fixture, artifact.path), 0o755);
    }
  }
  for (const artifact of candidate.artifacts) {
    artifact.sha256 = sha256(await readFile(path.join(fixture, artifact.path)));
  }
  const candidateReport = [
    "# Synthetic SPL v0.2 acceptance",
    "",
    "**Status:** pending",
    "",
    "**Evidence phase:** `qualification-candidate`",
    "",
    "**Decision:** pending",
    "",
    "**Stable publication authorized:** no",
    "",
    "**Target authored-search identity:** `0.2`",
    "",
    "**Knowledge-expression identity:** `0.1`",
    "",
  ].join("\n");
  await put("docs/spl-compatibility-v0.2-acceptance.md", candidateReport);
  await put(
    "docs/evidence/spl-v0.2-activation/manifest.json",
    `${JSON.stringify(candidate, null, 2)}\n`,
  );
  await git("init", "--quiet");
  await git("config", "user.email", "fixture@example.invalid");
  await git("config", "user.name", "Fixture");
  await git("add", ".");
  await git("commit", "--quiet", "-m", "v0.2 runtime candidate R");
  let runtime = await git("rev-parse", "HEAD");
  const runtimeTree = await git("rev-parse", "HEAD^{tree}");
  const candidateVerification = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "qualification-candidate",
  ], { cwd: fixture });
  assert.equal(candidateVerification.status, 0, candidateVerification.stderr);

  for (const [relative, stale, pattern] of [
    ["deploy/generate-env.sh", "application_version=${OPEN_SPLUNK_APPLICATION_VERSION:-0.2.0}\n", /deployment generator/],
    ["deploy/README.md", "```sh\nexport OPEN_SPLUNK_APPLICATION_VERSION=0.2.0\n```\n", /deployment guide/],
    ["docs/collector-deployment.md", "```sh\nexport OPEN_SPLUNK_COLLECTOR_VERSION=0.2.0\n```\n", /collector deployment guide/],
    ["integration/README.md", "```sh\nOPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n```\n", /integration guide/],
    ["integration/README.md", "```sh\nOPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n```\n", /integration guide/],
    ["integration/README.md", "```sh\nOPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n```\n", /integration guide/],
  ]) {
    const exact = await readFile(path.join(fixture, relative), "utf8");
    await put(relative, `${exact}${stale}`);
    await git("add", relative);
    await git("commit", "--quiet", "--amend", "--no-edit");
    const result = await runNode([
      path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
      "--phase", "qualification-candidate",
    ], { cwd: fixture });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, new RegExp(`${pattern.source}|artifact .* hash does not match`));
    await put(relative, exact);
    await git("add", relative);
    await git("commit", "--quiet", "--amend", "--no-edit");
  }
  runtime = await git("rev-parse", "HEAD");

  const readmePath = "README.md";
  const exactReadme = await readFile(path.join(fixture, readmePath), "utf8");
  await put(readmePath, exactReadme.replace(
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0",
    "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0",
  ));
  await git("add", readmePath);
  await git("commit", "--quiet", "--amend", "--no-edit");
  const wrongReadmeVerification = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "qualification-candidate",
  ], { cwd: fixture });
  assert.notEqual(wrongReadmeVerification.status, 0);
  assert.match(wrongReadmeVerification.stderr,
    /canonical release command block|exact v0\.2 release authority|artifact .* hash does not match/);
  await put(readmePath, exactReadme);
  await git("add", readmePath);
  await git("commit", "--quiet", "--amend", "--no-edit");
  runtime = await git("rev-parse", "HEAD");
  const restoredCandidateVerification = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "qualification-candidate",
  ], { cwd: fixture });
  assert.equal(restoredCandidateVerification.status, 0, restoredCandidateVerification.stderr);

  for (const stale of [
    "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
  ]) {
    await put(readmePath, `${exactReadme}\n\`\`\`sh\n${stale}\`\`\`\n`);
    await git("add", readmePath);
    await git("commit", "--quiet", "--amend", "--no-edit");
    const result = await runNode([
      path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
      "--phase", "qualification-candidate",
    ], { cwd: fixture });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr,
      /README .* (?:contains an additional or malformed OPEN_SPLUNK_|does not equal the exact v0\.2 release authority)/);
  }
  await put(readmePath, exactReadme);
  await git("add", readmePath);
  await git("commit", "--quiet", "--amend", "--no-edit");
  runtime = await git("rev-parse", "HEAD");

  const oversizedCandidate = structuredClone(candidate);
  oversizedCandidate.artifacts.push({
    id: "unreviewed-extra-artifact",
    path: "docs/unreviewed-extra-artifact.txt",
    sha256: "0".repeat(64),
  });
  await put(
    "docs/evidence/spl-v0.2-activation/manifest.json",
    `${JSON.stringify(oversizedCandidate, null, 2)}\n`,
  );
  await put("docs/unreviewed-extra-artifact.txt", "unreviewed\n");
  await git("add", ".");
  await git("commit", "--quiet", "-m", "invalid oversized manifest");
  const oversizedVerification = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "qualification-candidate",
  ], { cwd: fixture });
  assert.notEqual(oversizedVerification.status, 0);
  assert.match(oversizedVerification.stderr, /manifest\.artifacts has too many items/);
  await git("switch", "--quiet", "--detach", runtime);

  await rm(path.join(fixture, "README.md"));
  await symlink("docs/spl-compatibility-v0.2.md", path.join(fixture, "README.md"));
  await git("add", "README.md");
  await git("commit", "--quiet", "-m", "invalid symlinked stable artifact");
  const symlinkVerification = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "qualification-candidate",
  ], { cwd: fixture });
  assert.notEqual(symlinkVerification.status, 0);
  assert.match(symlinkVerification.stderr,
    /artifact public-readme must be a regular Git blob/);
  await git("switch", "--quiet", "--detach", runtime);

  const candidateManifestBytes = await readFile(path.join(
    fixture,
    "docs/evidence/spl-v0.2-activation/manifest.json",
  ));
  const runID = 42;
  const verifierSource = await readFile(verifier, "utf8");
  const jobBlock = verifierSource.match(/const EXPECTED_JOBS = \[(.*?)\];/s)?.[1] ?? "";
  const jobNames = [...jobBlock.matchAll(/^  "([^"]+)",$/gm)]
    .map((match) => match[1]);
  const jobs = jobNames.map((name, index) => ({
    id: 1000 + index,
    name,
    url: `https://github.com/Suhaibinator/open-splunk/actions/runs/${runID}/job/${1000 + index}`,
    status: "completed",
    conclusion: "success",
  }));
  const release = {
    source_revision: runtime,
    spl_compatibility_version: "0.2",
    application_version: "0.1.0",
    binary_identities: [
      { name: "open-splunk-server", application_version: "0.1.0", source_revision: runtime, spl_compatibility_version: "0.2" },
      { name: "open-splunk-collector", application_version: "0.1.0", source_revision: runtime, spl_compatibility_version: null },
      { name: "open-splunk-loggen", application_version: "0.1.0", source_revision: runtime, spl_compatibility_version: null },
    ],
    artifact_digests: [
      "asset-manifest.json", "open-splunk-collector", "open-splunk-loggen",
      "open-splunk-server", "ui",
    ].map((name, index) => ({ name, sha256: String(index + 1).repeat(64), bytes: index + 1 })),
  };
  const ci = {
    repository: "Suhaibinator/open-splunk",
    workflow: "CI",
    run_id: runID,
    url: `https://github.com/Suhaibinator/open-splunk/actions/runs/${runID}`,
    event: "workflow_dispatch",
    head_branch: "v02-candidate",
    head_revision: runtime,
    checkout_revision: runtime,
    checkout_tree: runtimeTree,
    release_artifact_source_revision: runtime,
    status: "completed",
    conclusion: "success",
    completed_at_utc: "2026-08-12T12:00:00Z",
    job_set_complete: true,
    jobs,
  };
  const accepted = structuredClone(candidate);
  accepted.phase = "accepted";
  accepted.runtime = {
    revision_binding: "recorded-runtime-parent",
    revision: runtime,
    tree: runtimeTree,
    candidate_manifest_sha256: sha256(candidateManifestBytes),
    candidate_acceptance_sha256: sha256(Buffer.from(candidateReport)),
    remote_ref: `refs/tags/spl-v0.2-evidence-${runtime}`,
    remote_readback_revision: runtime,
    remote_readback_at_utc: "2026-08-12T12:00:00Z",
  };
  accepted.ci = ci;
  accepted.release = release;

  const wrongVersion = structuredClone(accepted);
  wrongVersion.release.application_version = "0.2.0";
  for (const identity of wrongVersion.release.binary_identities) {
    identity.application_version = "0.2.0";
  }
  const wrongVersionBytes = Buffer.from(`${JSON.stringify(wrongVersion, null, 2)}\n`);
  await put("docs/evidence/spl-v0.2-activation/manifest.json", wrongVersionBytes);
  await git("add", "docs/evidence/spl-v0.2-activation/manifest.json");
  await git("commit", "--quiet", "-m", "invalid coherent v0.2 application version");
  const wrongVersionResult = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "accepted",
  ], { cwd: fixture });
  assert.notEqual(wrongVersionResult.status, 0);
  assert.match(wrongVersionResult.stderr, /application_version has an unsupported value/);
  await git("switch", "--quiet", "--detach", runtime);

  accepted.decision = {
    status: "accepted",
    stable_publication_authorized: true,
    reason: "Synthetic exact R/E evidence passed.",
  };
  const ciRun = { ...ci };
  delete ciRun.jobs;
  const releaseIdentity = { ...release };
  delete releaseIdentity.artifact_digests;
  const receiptValues = new Map([
    ["source-identity", { runtime_revision: runtime, runtime_tree: runtimeTree, remote_ref: accepted.runtime.remote_ref, remote_readback_revision: runtime, result: "pass" }],
    ["quality-gates", { runtime_revision: runtime, runtime_tree: runtimeTree, result: "pass" }],
    ["clickhouse-gates", { runtime_revision: runtime, runtime_tree: runtimeTree, image: "clickhouse/clickhouse-server:26.3.17.56@sha256:422be85ae7344058369cdd366ac0efea9daa8428b55c9cf50258e83a7d12fcb3", result: "pass" }],
    ["compatibility-audit", { runtime_revision: runtime, runtime_tree: runtimeTree, compatibility_version: "0.2", unresolved_findings: 0, result: "pass" }],
    ["ci-run", ciRun],
    ["ci-jobs", jobs],
    ["release-identity", releaseIdentity],
    ["release-artifacts", release.artifact_digests],
  ]);
  accepted.receipts = [];
  for (const [id, value] of receiptValues) {
    const bytes = Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
    const relative = `receipts/${id}.json`;
    await put(`docs/evidence/spl-v0.2-activation/${relative}`, bytes);
    accepted.receipts.push({ id, path: relative, sha256: sha256(bytes), bytes: bytes.length });
  }
  const acceptedReport = candidateReport
    .replace("**Status:** pending", "**Status:** accepted")
    .replace("`qualification-candidate`", "`accepted`")
    .replace("**Decision:** pending", "**Decision:** accepted")
    .replace("**Stable publication authorized:** no", "**Stable publication authorized:** yes");
  await put("docs/spl-compatibility-v0.2-acceptance.md", acceptedReport);
  const acceptedManifestBytes = Buffer.from(`${JSON.stringify(accepted, null, 2)}\n`);
  await put("docs/evidence/spl-v0.2-activation/manifest.json", acceptedManifestBytes);
  await git("add", ".");
  await git("commit", "--quiet", "-m", "v0.2 evidence E");
  const strict = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "accepted",
  ], { cwd: fixture });
  assert.equal(strict.status, 0, strict.stderr);
  const forgedRuntimeRef = structuredClone(accepted);
  forgedRuntimeRef.runtime.remote_ref += "-forged";
  await put(
    "docs/evidence/spl-v0.2-activation/manifest.json",
    `${JSON.stringify(forgedRuntimeRef, null, 2)}\n`,
  );
  await git("add", "docs/evidence/spl-v0.2-activation/manifest.json");
  await git("commit", "--quiet", "--amend", "--no-edit");
  const forgedRuntimeRefResult = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "accepted",
  ], { cwd: fixture });
  assert.notEqual(forgedRuntimeRefResult.status, 0);
  assert.match(forgedRuntimeRefResult.stderr, /immutable full-R v0\.2 evidence tag/);
  await put(
    "docs/evidence/spl-v0.2-activation/manifest.json",
    acceptedManifestBytes,
  );
  await git("add", "docs/evidence/spl-v0.2-activation/manifest.json");
  await git("commit", "--quiet", "--amend", "--no-edit");
  const restoredEvidence = await git("rev-parse", "HEAD");
  const ancestor = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "accepted", "--evidence-revision", restoredEvidence,
    "--print-evidence-binding",
  ], { cwd: fixture });
  assert.equal(ancestor.status, 0, ancestor.stderr);
  assert.equal(ancestor.stdout, `${restoredEvidence} ${sha256(acceptedManifestBytes)}\n`);

  const graftsPath = path.join(fixture, ".git", "info", "grafts");
  await writeFile(graftsPath, `${restoredEvidence} ${runtime}\n`);
  const grafted = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "accepted", "--evidence-revision", restoredEvidence,
    "--print-evidence-binding",
  ], { cwd: fixture });
  assert.notEqual(grafted.status, 0);
  assert.match(grafted.stderr, /legacy Git grafts can forge R\/E topology/);
  await rm(graftsPath);

  await git("switch", "--quiet", "--detach", runtime);
  await chmod(path.join(fixture, "deploy/generate-env.sh"), 0o644);
  await git("add", "deploy/generate-env.sh");
  await git("commit", "--quiet", "--amend", "--no-edit");
  const wrongMode = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "qualification-candidate",
  ], { cwd: fixture });
  assert.notEqual(wrongMode.status, 0);
  assert.match(wrongMode.stderr, /exact Git mode 100755/);
  await git("switch", "--quiet", "--detach", runtime);

  // Accepted verification must replay every candidate invariant from R, not
  // merely trust E's hashes and accepted authorities.
  await git("switch", "--quiet", "--detach", runtime);
  const malformedCandidate = structuredClone(candidate);
  malformedCandidate.runtime.revision_binding = "unbound";
  const malformedCandidateBytes = Buffer.from(
    `${JSON.stringify(malformedCandidate, null, 2)}\n`,
  );
  await put(
    "docs/evidence/spl-v0.2-activation/manifest.json",
    malformedCandidateBytes,
  );
  await git("add", ".");
  await git("commit", "--quiet", "-m", "malformed v0.2 candidate R");
  const malformedRuntime = await git("rev-parse", "HEAD");
  const malformedRuntimeTree = await git("rev-parse", "HEAD^{tree}");
  const malformedAccepted = structuredClone(accepted);
  malformedAccepted.runtime = {
    ...accepted.runtime,
    revision: malformedRuntime,
    tree: malformedRuntimeTree,
    candidate_manifest_sha256: sha256(malformedCandidateBytes),
    remote_readback_revision: malformedRuntime,
  };
  malformedAccepted.ci = {
    ...accepted.ci,
    head_revision: malformedRuntime,
    checkout_revision: malformedRuntime,
    checkout_tree: malformedRuntimeTree,
    release_artifact_source_revision: malformedRuntime,
  };
  malformedAccepted.release = {
    ...accepted.release,
    source_revision: malformedRuntime,
    binary_identities: structuredClone(accepted.release.binary_identities),
  };
  for (const identity of malformedAccepted.release.binary_identities) {
    identity.source_revision = malformedRuntime;
  }
  const malformedCIRun = { ...malformedAccepted.ci };
  delete malformedCIRun.jobs;
  const malformedReleaseIdentity = { ...malformedAccepted.release };
  delete malformedReleaseIdentity.artifact_digests;
  const malformedReceiptValues = new Map([
    ["source-identity", {
      runtime_revision: malformedRuntime,
      runtime_tree: malformedRuntimeTree,
      remote_ref: malformedAccepted.runtime.remote_ref,
      remote_readback_revision: malformedRuntime,
      result: "pass",
    }],
    ["quality-gates", {
      runtime_revision: malformedRuntime,
      runtime_tree: malformedRuntimeTree,
      result: "pass",
    }],
    ["clickhouse-gates", {
      runtime_revision: malformedRuntime,
      runtime_tree: malformedRuntimeTree,
      image: "clickhouse/clickhouse-server:26.3.17.56@sha256:422be85ae7344058369cdd366ac0efea9daa8428b55c9cf50258e83a7d12fcb3",
      result: "pass",
    }],
    ["compatibility-audit", {
      runtime_revision: malformedRuntime,
      runtime_tree: malformedRuntimeTree,
      compatibility_version: "0.2",
      unresolved_findings: 0,
      result: "pass",
    }],
    ["ci-run", malformedCIRun],
    ["ci-jobs", jobs],
    ["release-identity", malformedReleaseIdentity],
    ["release-artifacts", malformedAccepted.release.artifact_digests],
  ]);
  malformedAccepted.receipts = [];
  for (const [id, value] of malformedReceiptValues) {
    const bytes = Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
    const relative = `receipts/${id}.json`;
    await put(`docs/evidence/spl-v0.2-activation/${relative}`, bytes);
    malformedAccepted.receipts.push({
      id,
      path: relative,
      sha256: sha256(bytes),
      bytes: bytes.length,
    });
  }
  await put("docs/spl-compatibility-v0.2-acceptance.md", acceptedReport);
  await put(
    "docs/evidence/spl-v0.2-activation/manifest.json",
    `${JSON.stringify(malformedAccepted, null, 2)}\n`,
  );
  await git("add", ".");
  await git("commit", "--quiet", "-m", "evidence for malformed v0.2 R");
  const malformedAcceptedVerification = await runNode([
    path.join(fixture, "scripts/verify-spl-v02-acceptance.mjs"),
    "--phase", "accepted",
  ], { cwd: fixture });
  assert.notEqual(malformedAcceptedVerification.status, 0);
  assert.match(malformedAcceptedVerification.stderr,
    /candidate runtime binding must be containing-commit/);
});

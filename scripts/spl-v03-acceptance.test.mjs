/* eslint-disable no-await-in-loop, unicorn/consistent-function-scoping */
// Synthetic R/E histories require ordered filesystem and Git mutations.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { chmod, copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

import {
  REQUIRED_CI_JOBS,
  REQUIRED_RELEASE_DIGESTS,
  REQUIRED_SPL_TESTS,
  collectSchemaErrors,
  expectedUIBuildID,
  parseArguments,
  rejectDuplicateJSONNames,
  remoteRefCommit,
  verifyArtifactInventory,
  verifyCandidateInvariants,
  verifyCheckpoint,
  verifyCI,
  verifyManifestSchema,
  verifyOperatorReleaseIdentitySources,
  verifyPublicREADME,
  verifyRelease,
  verifyReleaseReadback,
  verifyReleaseReadbackReceiptDigest,
} from "./verify-spl-v03-acceptance.mjs";

const workspace = process.cwd();
const currentManifestBytes = await readFile(
  path.join(workspace, "docs/evidence/spl-v0.3/manifest.json"),
);
const manifest = JSON.parse(currentManifestBytes.toString("utf8"));
const manifestSchema = JSON.parse(await readFile(
  path.join(workspace, "docs/evidence/spl-v0.3/manifest.schema.json"),
  "utf8",
));
const verifierSource = await readFile(
  path.join(workspace, "scripts/verify-spl-v03-acceptance.mjs"),
  "utf8",
);

function currentEvidenceRevision() {
  const result = spawnSync("git", [
    "log", "-1", "--format=%H", "--",
    "docs/evidence/spl-v0.3/manifest.json",
  ], { cwd: workspace, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const revision = result.stdout.trim();
  assert.match(revision, /^[0-9a-f]{40}$/);
  return revision;
}

function clone(value) {
  return structuredClone(value);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

test("release readback preserves the literal deterministic UI build identity", () => {
  const runtime = "1".repeat(40);
  const applicationVersion = "0.2.0";
  const buildID = expectedUIBuildID(applicationVersion, runtime);
  assert.match(buildID, /^r[0-9ghjkmn]{64}$/);
  assert.equal(buildID.length, 65);
  const valid = [
    `application_version=${applicationVersion}`,
    `source_revision=${runtime}`,
    "spl_compatibility_version=0.3",
    `ui_build_id=${buildID}`,
    `ui_sha256=${"2".repeat(64)}`,
    "",
  ].join("\n");
  assert.doesNotThrow(() =>
    verifyReleaseReadback(valid, applicationVersion, runtime, sha256(valid)));

  const substituted = valid.replace(
    `ui_build_id=${buildID}`,
    `ui_build_id=${sha256(buildID)}`,
  );
  assert.throws(
    () => verifyReleaseReadback(
      substituted,
      applicationVersion,
      runtime,
      sha256(substituted),
    ),
    /does not exactly bind application\/source\/SPL\/UI identities/,
  );
  assert.throws(
    () => verifyReleaseReadback(
      valid,
      applicationVersion,
      runtime,
      "3".repeat(64),
    ),
    /does not equal the release-verification artifact/,
  );
  assert.doesNotThrow(() =>
    verifyReleaseReadbackReceiptDigest(sha256(valid), sha256(valid)));
  assert.throws(
    () => verifyReleaseReadbackReceiptDigest(sha256(valid), "3".repeat(64)),
    /receipt digest does not equal the release-verification artifact/,
  );

  const lines = valid.split("\n");
  for (const nonCanonical of [
    valid.slice(0, -1),
    `${valid}\n`,
    `${valid.slice(0, -1)} `,
    `${valid.slice(0, -1)}\t`,
    valid.replaceAll("\n", "\r\n"),
    `\n${valid}`,
    lines.filter((_, index) => index !== 2).join("\n"),
    [lines[1], lines[0], ...lines.slice(2)].join("\n"),
    valid.replace("application_version=0.2.0", "application_version=0.2.1"),
    valid.replace(`source_revision=${runtime}`, `source_revision=${"3".repeat(40)}`),
    valid.replace("spl_compatibility_version=0.3", "spl_compatibility_version=0.2"),
    valid.replace(`ui_build_id=${buildID}`, `ui_build_id=${buildID.toUpperCase()}`),
    valid.replace(`ui_build_id=${buildID}`, `ui_build_id=a${buildID.slice(1)}`),
    valid.replace(`ui_sha256=${"2".repeat(64)}`, `ui_sha256=${"A".repeat(64)}`),
    valid.replace("ui_sha256=", "ui_sha256=\0"),
    valid.replace("ui_sha256=", "ui_sha256=é"),
  ]) {
    assert.throws(
      () => verifyReleaseReadback(
        nonCanonical,
        applicationVersion,
        runtime,
        sha256(nonCanonical),
      ),
      /does not exactly bind application\/source\/SPL\/UI identities/,
    );
  }
});

function checkpointManifestFixture() {
  const value = clone(manifest);
  value.phase = "implementation-checkpoint";
  value.compatibility.runtime_authored_search = "0.2";
  value.prerequisite = {
    v02_status: "pending",
    evidence_path: null,
    evidence_sha256: null,
    evidence_revision: null,
    runtime_revision: null,
  };
  value.runtime = {
    revision_binding: "unbound",
    revision: null,
    tree: null,
    candidate_acceptance_sha256: null,
    remote_ref: null,
    remote_readback_revision: null,
  };
  for (const artifact of value.artifacts) artifact.sha256 = null;
  value.ci = {
    repository: null,
    workflow: "CI",
    run_id: null,
    url: null,
    event: null,
    head_revision: null,
    status: null,
    conclusion: null,
    completed_at_utc: null,
    jobs: [],
  };
  value.release = {
    source_revision: null,
    spl_compatibility_version: null,
    application_version: null,
    binary_identities: [],
    artifact_digests: [],
  };
  value.receipts = [];
  value.decision = {
    status: "pending",
    stable_publication_authorized: false,
    reason: "Synthetic checkpoint fixture remains distribution-blocked.",
  };
  return value;
}

function checkpointOperatorBytes(relative, source) {
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
      .replace("export OPEN_SPLUNK_APPLICATION_VERSION=0.2.0", "export OPEN_SPLUNK_APPLICATION_VERSION=0.1.0")
      .replace("export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3", "export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2")
      .replace("OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 ./generate-env.sh", "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 ./generate-env.sh");
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

function candidateOperatorBytes(relative, source) {
  const checkpoint = checkpointOperatorBytes(relative, source);
  if (relative === "README.md") {
    return checkpoint
      .replace(
        "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
        "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
      )
      .replace(
        "[`docs/spl-compatibility-v0.2.md`](docs/spl-compatibility-v0.2.md)",
        "[`docs/spl-compatibility-v0.3.md`](docs/spl-compatibility-v0.3.md)",
      )
      .replace(
        "[`v0.2 migration and read-only audit guide`](docs/spl-compatibility-v0.2-migration.md)",
        "[`v0.3 migration and read-only audit guide`](docs/spl-compatibility-v0.3-migration.md)",
      )
      .replace(
        "[`v0.2 acceptance report`](docs/spl-compatibility-v0.2-acceptance.md)",
        "[`v0.3 acceptance report`](docs/spl-compatibility-v0.3-acceptance.md)",
      );
  }
  if (relative === "deploy/generate-env.sh") {
    return checkpoint.replace(
      "application_version=${OPEN_SPLUNK_APPLICATION_VERSION:-0.1.0}",
      "application_version=${OPEN_SPLUNK_APPLICATION_VERSION:-0.2.0}",
    );
  }
  if (relative === "deploy/README.md") {
    return checkpoint
      .replace(
        "export OPEN_SPLUNK_APPLICATION_VERSION=0.1.0\n" +
          "export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2\n",
        "export OPEN_SPLUNK_APPLICATION_VERSION=0.2.0\n" +
          "export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3\n",
      )
      .replace(
        "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 ./generate-env.sh",
        "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 ./generate-env.sh",
      );
  }
  if (relative === "docs/collector-deployment.md") {
    return checkpoint.replace(
      "export OPEN_SPLUNK_COLLECTOR_VERSION=0.1.0",
      "export OPEN_SPLUNK_COLLECTOR_VERSION=0.2.0",
    );
  }
  return checkpoint.replace(
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
    "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
  );
}

const checkpointReportFixture = [
  "**Acceptance phase:** `implementation-checkpoint`",
  "**Status:** implementation checkpoint; activation provenance pending",
  "**Target authored-search identity:** `0.3`",
  "**Knowledge-expression identity:** `0.1`",
  "**v0.2 prerequisite:** pending",
  "**Activation decision:** pending; distribution blocked",
  "",
].join("\n");

test("current SPL v0.3 checkpoint is strict and deliberately unaccepted", () => {
  const phase = manifest.phase;
  const argumentsList = [
    "scripts/verify-spl-v03-acceptance.mjs",
    "--phase", phase,
  ];
  let evidenceRevision = null;
  if (phase === "implementation-checkpoint") {
    argumentsList.push("--allow-dirty");
  } else if (phase === "accepted") {
    evidenceRevision = currentEvidenceRevision();
    argumentsList.push(
      "--evidence-revision", evidenceRevision,
      "--print-evidence-binding",
    );
  }
  const result = spawnSync(process.execPath, [
    ...argumentsList,
  ], { cwd: workspace, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  if (phase === "implementation-checkpoint") {
    assert.equal(
      result.stdout,
      "SPL v0.3 implementation checkpoint is internally consistent and not accepted\n",
    );
  } else if (phase === "qualification-candidate") {
    assert.equal(
      result.stdout,
      "SPL v0.3 qualification candidate is internally consistent; stable publication remains blocked\n",
    );
  } else {
    assert.equal(
      result.stdout,
      `${evidenceRevision} ${sha256(currentManifestBytes)}\n`,
    );
  }
});

test("v0.3 verifier CLI forbids publication shortcuts", () => {
  assert.throws(() => parseArguments([]), /--phase must be/);
  assert.throws(
    () => parseArguments(["--phase", "qualification-candidate", "--allow-dirty"]),
    /--allow-dirty/,
  );
  assert.throws(
    () => parseArguments(["--phase", "qualification-candidate", "--publication"]),
    /requires --phase accepted/,
  );
  assert.throws(
    () => parseArguments(["--phase", "accepted", "--print-runtime-revision"]),
    /requires --publication/,
  );
  assert.throws(
    () => parseArguments(["--phase", "accepted", "--print-evidence-binding"]),
    /requires --evidence-revision/,
  );
  assert.throws(
    () => parseArguments(["--phase", "accepted", "--evidence-revision"]),
    /requires an exact revision/,
  );
  assert.throws(
    () => parseArguments([
      "--phase", "accepted", "--phase", "qualification-candidate",
    ]),
    /duplicate argument/,
  );
  assert.throws(
    () => parseArguments([
      "--phase", "accepted", "--evidence-revision", "a".repeat(40),
    ]),
    /requires --print-evidence-binding/,
  );
  assert.throws(
    () => parseArguments([
      "--phase", "accepted", "--evidence-revision", "a".repeat(40),
      "--print-evidence-binding", "--publication",
    ]),
    /without publication/,
  );
  assert.doesNotThrow(() => parseArguments([
    "--phase", "accepted", "--evidence-revision", "a".repeat(40),
    "--print-evidence-binding",
  ]));
});

test("accepted ancestor mode is exact-E scoped and keeps publication HEAD-only", async () => {
  const workflow = await readFile(
    path.join(workspace, ".github/workflows/ci.yml"),
    "utf8",
  );
  assert.match(
    verifierSource,
    /--evidence-revision <E> --print-evidence-binding/,
  );
  assert.match(
    verifierSource,
    /accepted v0\.3 evidence revision E must be an ancestor of the current checkout/,
  );
  assert.match(
    verifierSource,
    /accepted v0\.3 ancestor verification requires a non-shallow repository/,
  );
  assert.match(
    verifierSource,
    /legacy Git grafts can forge R\/E topology and must be absent/,
  );
  assert.match(
    verifierSource,
    /current \$\{id\} bytes must equal the authority qualified by v0\.3 R/,
  );
  for (const setting of [
    'GIT_CONFIG_GLOBAL: "/dev/null"',
    'GIT_CONFIG_NOSYSTEM: "1"',
    'GIT_NO_REPLACE_OBJECTS: "1"',
    'GIT_OPTIONAL_LOCKS: "0"',
    'GIT_TERMINAL_PROMPT: "0"',
  ]) {
    assert.ok(verifierSource.includes(setting), `missing hardened Git setting ${setting}`);
  }
  assert.match(
    verifierSource,
    /options\.publication[\s\S]*--evidence-revision requires an exact accepted E without publication/,
  );
  assert.match(workflow, /git log -1 --format=%H -- "\$manifest_path"/);
  assert.match(
    workflow,
    /--evidence-revision "\$evidence_revision" \\\n\s+--print-evidence-binding/,
  );
});

test("strict JSON rejects duplicate names at every nesting level", () => {
  assert.throws(
    () => rejectDuplicateJSONNames('{"phase":"accepted","phase":"candidate"}', "fixture"),
    /duplicates object key "phase"/,
  );
  assert.throws(
    () => rejectDuplicateJSONNames('{"outer":{"status":1,"status":2}}', "fixture"),
    /duplicates object key "status"/,
  );
});

test("manifest schema validation is complete for every vocabulary used by the bundle", () => {
  verifyManifestSchema(manifestSchema, manifest);
  for (const [name, mutate, pattern] of [
    ["unknown nested field", (value) => { value.runtime.forged = true; }, /not allowed/],
    ["target identity", (value) => { value.compatibility.target_authored_search = "0.4"; }, /must equal/],
    ["artifact minimum", (value) => { value.artifacts.pop(); }, /too few items/],
    ["artifact maximum", (value) => { value.artifacts.push(clone(value.artifacts[0])); }, /too many items/],
    ["minimum", (value) => { value.release.artifact_digests = [{ name: "x", sha256: "0".repeat(64) }]; value.release.source_revision = "a".repeat(40); value.release.spl_compatibility_version = "0.3"; value.release.application_version = "0.2.0"; value.release.binary_identities = []; value.receipts = [{ id: "x", path: "receipts/x", sha256: "0".repeat(64), bytes: 0 }]; }, /below its schema minimum/],
    ["wrong valid application version", (value) => { value.release.application_version = "0.3.0"; }, /unsupported value/],
  ]) {
    const fixture = clone(manifest);
    mutate(fixture);
    assert.throws(
      () => verifyManifestSchema(manifestSchema, fixture),
      pattern,
      name,
    );
  }
  assert.deepEqual(collectSchemaErrors(manifestSchema, manifest), []);
});

test("candidate invariants pin complete report and manifest identities for accepted replay", () => {
  const candidate = checkpointManifestFixture();
  candidate.phase = "qualification-candidate";
  candidate.compatibility.runtime_authored_search = "0.3";
  candidate.runtime.revision_binding = "containing-commit";
  const candidateReport = [
    "**Acceptance phase:** `qualification-candidate`",
    "**Status:** qualification candidate; final evidence pending",
    "**Target authored-search identity:** `0.3`",
    "**Knowledge-expression identity:** `0.1`",
    "**v0.2 prerequisite:** accepted",
    "**Activation decision:** pending; stable publication blocked",
    "",
  ].join("\n");
  verifyCandidateInvariants(candidate, candidateReport, "0.3");

  for (const [name, mutate, pattern] of [
    ["target", (value) => { value.compatibility.target_authored_search = "0.2"; }, /compatibility identities/],
    ["knowledge", (value) => { value.compatibility.knowledge_expression = "0.2"; }, /compatibility identities/],
    ["format", (value) => { value.format_version = "forged"; }, /manifest identity/],
    ["phase", (value) => { value.phase = "accepted"; }, /manifest identity/],
  ]) {
    const fixture = clone(candidate);
    mutate(fixture);
    assert.throws(
      () => verifyCandidateInvariants(fixture, candidateReport, "0.3"),
      pattern,
      name,
    );
  }
});

test("remote runtime tag readback peels an annotated tag to its commit", async (t) => {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-v03-annotated-tag-"));
  t.after(() => rm(fixture, { recursive: true, force: true }));
  const remote = path.join(fixture, "remote.git");
  const checkout = path.join(fixture, "checkout");
  const runGit = (cwd, argumentsList) => {
    const result = spawnSync("git", argumentsList, { cwd, encoding: "utf8" });
    assert.equal(result.status, 0, `${argumentsList.join(" ")}\n${result.stderr}`);
    return result.stdout;
  };

  runGit(fixture, ["init", "--quiet", "--bare", remote]);
  runGit(fixture, ["init", "--quiet", checkout]);
  runGit(checkout, ["config", "user.email", "fixture@example.invalid"]);
  runGit(checkout, ["config", "user.name", "Fixture"]);
  await writeFile(path.join(checkout, "proof"), "annotated tag proof\n");
  runGit(checkout, ["add", "proof"]);
  runGit(checkout, ["commit", "--quiet", "-m", "runtime R"]);
  const runtime = runGit(checkout, ["rev-parse", "HEAD"]).trim();
  const reference = "refs/tags/spl-v0.3-runtime-annotated";
  runGit(checkout, ["tag", "-a", reference.slice("refs/tags/".length), "-m", "runtime", runtime]);
  runGit(checkout, ["remote", "add", "origin", remote]);
  runGit(checkout, ["push", "--quiet", "origin", reference]);

  const direct = runGit(checkout, ["ls-remote", "origin", reference])
    .trim().split(/\s+/)[0];
  assert.notEqual(direct, runtime, "fixture must prove the direct ref is a tag object");
  assert.equal(
    remoteRefCommit("origin", reference, (argumentsList) =>
      runGit(checkout, argumentsList)),
    runtime,
  );
});

test("v0.3 prerequisite reuses byte-identical v0.2 accepted authorities", () => {
  for (const relative of [
    "docs/spl-compatibility-v0.2-acceptance.md",
    "docs/evidence/spl-v0.2-activation/manifest.json",
    "docs/evidence/spl-v0.2-activation/manifest.schema.json",
    "scripts/verify-spl-v02-acceptance.mjs",
    "scripts/spl-v02-acceptance.test.mjs",
    "scripts/strict-json.mjs",
  ]) {
    assert.ok(
      verifierSource.includes(JSON.stringify(relative)),
      `missing accepted-E byte-identity check for ${relative}`,
    );
  }
  assert.match(verifierSource, /Buffer\.compare\(currentBytes, acceptedBytes\) === 0/);
  assert.match(verifierSource, /--print-evidence-binding/);
});

test("checkpoint validation rejects every provenance family", () => {
  const checkpoint = checkpointManifestFixture();
  verifyArtifactInventory(checkpoint);
  verifyCheckpoint(checkpoint, checkpointReportFixture, "0.2");
  for (const [name, mutate, pattern] of [
    ["identity", (value) => { value.compatibility.runtime_authored_search = "0.3"; }, /identit|compatibility/],
    ["v0.2 evidence path", (value) => { value.prerequisite.evidence_path = "forged.json"; }, /v0\.2 evidence provenance/],
    ["v0.2 evidence hash", (value) => { value.prerequisite.evidence_sha256 = "0".repeat(64); }, /v0\.2 evidence provenance/],
    ["runtime binding", (value) => { value.runtime.revision_binding = "containing-commit"; }, /runtime revision/],
    ["runtime revision", (value) => { value.runtime.revision = "a".repeat(40); }, /runtime revision/],
    ["runtime tree", (value) => { value.runtime.tree = "b".repeat(40); }, /runtime revision/],
    ["candidate hash", (value) => { value.runtime.candidate_acceptance_sha256 = "c".repeat(64); }, /runtime revision/],
    ["runtime ref", (value) => { value.runtime.remote_ref = "refs/tags/forged"; }, /runtime revision/],
    ["runtime readback", (value) => { value.runtime.remote_readback_revision = "d".repeat(40); }, /runtime revision/],
    ["artifact hash", (value) => { value.artifacts[0].sha256 = "e".repeat(64); }, /artifact hashes/],
    ["CI repository", (value) => { value.ci.repository = "owner/repo"; }, /CI provenance/],
    ["CI run", (value) => { value.ci.run_id = 1; }, /CI provenance/],
    ["CI URL", (value) => { value.ci.url = "https://example.invalid"; }, /CI provenance/],
    ["CI event", (value) => { value.ci.event = "push"; }, /CI provenance/],
    ["CI head", (value) => { value.ci.head_revision = "f".repeat(40); }, /CI provenance/],
    ["CI status", (value) => { value.ci.status = "completed"; }, /CI provenance/],
    ["CI conclusion", (value) => { value.ci.conclusion = "success"; }, /CI provenance/],
    ["CI completion", (value) => { value.ci.completed_at_utc = "2026-08-12T00:00:00Z"; }, /CI provenance/],
    ["CI jobs", (value) => { value.ci.jobs = [{}]; }, /CI provenance/],
    ["release source", (value) => { value.release.source_revision = "a".repeat(40); }, /release provenance/],
    ["release SPL", (value) => { value.release.spl_compatibility_version = "0.3"; }, /release provenance/],
    ["release app", (value) => { value.release.application_version = "0.1.0"; }, /release provenance/],
    ["release binaries", (value) => { value.release.binary_identities = [{}]; }, /release provenance/],
    ["release digests", (value) => { value.release.artifact_digests = [{}]; }, /release provenance/],
    ["receipts", (value) => { value.receipts = [{}]; }, /receipts/],
    ["decision", (value) => { value.decision.status = "accepted"; }, /publication/],
    ["publication", (value) => { value.decision.stable_publication_authorized = true; }, /publication/],
  ]) {
    const fixture = clone(checkpoint);
    mutate(fixture);
    assert.throws(
      () => verifyCheckpoint(fixture, checkpointReportFixture, "0.2"),
      pattern,
      name,
    );
  }
});

test("artifact inventory is exact, unique, and path-bound", () => {
  for (const mutate of [
    (value) => value.artifacts.pop(),
    (value) => value.artifacts.push(clone(value.artifacts[0])),
    (value) => { value.artifacts[0].path = "docs/forged.md"; },
    (value) => { value.artifacts[0].id = value.artifacts[1].id; },
  ]) {
    const fixture = clone(manifest);
    mutate(fixture);
    assert.throws(() => verifyArtifactInventory(fixture), /artifact/i);
  }
});

test("public README follows the exact activation phase", async () => {
  const checkpoint = checkpointOperatorBytes("README.md",
    await readFile(path.join(workspace, "README.md"), "utf8"));
  verifyPublicREADME(checkpoint, "implementation-checkpoint");
  assert.throws(
    () => verifyPublicREADME(checkpoint, "qualification-candidate"),
    /public v0\.3/,
  );
  const candidate = candidateOperatorBytes("README.md", checkpoint);
  verifyPublicREADME(candidate, "qualification-candidate");
  verifyPublicREADME(candidate, "accepted");
  assert.throws(
    () => verifyPublicREADME(`${candidate}\n${checkpoint}`, "accepted"),
    /stale public v0\.2/,
  );
  assert.throws(
    () => verifyPublicREADME(candidate.replace(
      "APPLICATION_VERSION=0.2.0",
      "APPLICATION_VERSION=0.1.0",
    ), "qualification-candidate"),
    /canonical release command block|additional or malformed/,
  );
  for (const stale of [
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
    "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
  ]) {
    assert.throws(
      () => verifyPublicREADME(`${candidate}\n\`\`\`sh\n${stale}\`\`\`\n`, "qualification-candidate"),
      /canonical release command block|additional or malformed/,
    );
  }
});

test("operator release defaults advance atomically with the activation phase", async () => {
  const checkpoint = {
    deployGenerator: checkpointOperatorBytes("deploy/generate-env.sh",
      await readFile(path.join(workspace, "deploy/generate-env.sh"), "utf8")),
    deployGuide: checkpointOperatorBytes("deploy/README.md",
      await readFile(path.join(workspace, "deploy/README.md"), "utf8")),
    collectorGuide: checkpointOperatorBytes("docs/collector-deployment.md",
      await readFile(path.join(workspace, "docs/collector-deployment.md"), "utf8")),
    integrationGuide: checkpointOperatorBytes("integration/README.md",
      await readFile(path.join(workspace, "integration/README.md"), "utf8")),
  };
  verifyOperatorReleaseIdentitySources(checkpoint, "implementation-checkpoint");
  const candidate = Object.fromEntries(Object.entries({
    deployGenerator: "deploy/generate-env.sh",
    deployGuide: "deploy/README.md",
    collectorGuide: "docs/collector-deployment.md",
    integrationGuide: "integration/README.md",
  }).map(([key, relative]) => [key, candidateOperatorBytes(relative, checkpoint[key])]));
  verifyOperatorReleaseIdentitySources(candidate, "qualification-candidate");
  verifyOperatorReleaseIdentitySources(candidate, "accepted");
  for (const suffix of [" $(evil)", " ${EVIL}", "; $(evil)", " \\\nevil", "#evil"]) {
    const hostile = clone(candidate);
    hostile.collectorGuide = hostile.collectorGuide.replace(
      "COLLECTOR_VERSION=0.2.0", `COLLECTOR_VERSION=0.2.0${suffix}`,
    );
    assert.throws(
      () => verifyOperatorReleaseIdentitySources(hostile, "qualification-candidate"),
      /canonical release command block|additional or malformed/,
    );
  }
  const exportedPair = clone(candidate);
  exportedPair.integrationGuide = exportedPair.integrationGuide
    .replace("OPEN_SPLUNK_APPLICATION_VERSION=", "export OPEN_SPLUNK_APPLICATION_VERSION=");
  assert.throws(
    () => verifyOperatorReleaseIdentitySources(exportedPair, "qualification-candidate"),
    /canonical release command block|additional or malformed/,
  );
  for (const invalid of [
    "export OPEN_SPLUNK_COLLECTOR_VERSION =0.2.0",
    "export OPEN_SPLUNK_COLLECTOR_VERSION= 0.2.0",
  ]) {
    const hostile = clone(candidate);
    hostile.collectorGuide = `\`\`\`sh\n${invalid}\n\`\`\`\n`;
    assert.throws(
      () => verifyOperatorReleaseIdentitySources(hostile, "qualification-candidate"),
      /canonical release command block|additional or malformed/,
    );
  }
  for (const key of Object.keys(candidate)) {
    const stale = clone(candidate);
    stale[key] = checkpoint[key];
    assert.throws(
      () => verifyOperatorReleaseIdentitySources(stale, "qualification-candidate"),
      /must (?:default|contain)|canonical release command block|additional or malformed/,
      key,
    );
    const ambiguous = clone(candidate);
    ambiguous[key] += `\n${checkpoint[key]}`;
    assert.throws(
      () => verifyOperatorReleaseIdentitySources(ambiguous, "qualification-candidate"),
      /must (?:default|contain)|canonical release command block|additional or malformed/,
      `${key} accepts simultaneous stale identity`,
    );
  }
  for (const stale of [
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
    "OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2 \\\n",
  ]) {
    const ambiguous = clone(candidate);
    ambiguous.integrationGuide += `\n\`\`\`sh\n${stale}\`\`\`\n`;
    assert.throws(
      () => verifyOperatorReleaseIdentitySources(ambiguous, "qualification-candidate"),
      /canonical release command block|additional or malformed/,
    );
  }
  for (const stale of [
    "OPEN_SPLUNK_APPLICATION_VERSION='0.1.0' \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
    'OPEN_SPLUNK_APPLICATION_VERSION=0.2.0 \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION="0.2" \\\n',
    "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0\\evil \\\nOPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.3 \\\n",
  ]) {
    const ambiguous = clone(candidate);
    ambiguous.integrationGuide += `\n\`\`\`sh\n${stale}\`\`\`\n`;
    assert.throws(
      () => verifyOperatorReleaseIdentitySources(ambiguous, "qualification-candidate"),
      /canonical release command block|additional or malformed/,
    );
  }
});

function acceptedCI(runtime) {
  return {
    repository: "Suhaibinator/open-splunk",
    workflow: "CI",
    run_id: 123,
    url: "https://github.com/Suhaibinator/open-splunk/actions/runs/123",
    event: "workflow_dispatch",
    head_revision: runtime,
    status: "completed",
    conclusion: "success",
    completed_at_utc: "2026-08-12T00:00:00Z",
    jobs: REQUIRED_CI_JOBS.map((name, index) => ({
      id: index + 1,
      name,
      url: `https://github.com/Suhaibinator/open-splunk/actions/runs/123/job/${index + 1}`,
      status: "completed",
      conclusion: "success",
    })),
  };
}

test("accepted CI requires the exact 28 terminal-success jobs bound to R", () => {
  const runtime = "a".repeat(40);
  const fixture = { ci: acceptedCI(runtime) };
  verifyCI(fixture, runtime);
  fixture.ci.jobs.pop();
  assert.throws(() => verifyCI(fixture, runtime), /exactly 28 jobs/);
  fixture.ci = acceptedCI(runtime);
  fixture.ci.jobs[0].conclusion = "skipped";
  assert.throws(() => verifyCI(fixture, runtime), /not terminal-success/);
});

test("accepted release requires exact R, SPL identity, and digest set", () => {
  const runtime = "a".repeat(40);
  const lines = [];
  const artifact_digests = [...REQUIRED_RELEASE_DIGESTS].map((name, index) => {
    const digest = index.toString(16).padStart(64, "0");
    lines.push(`${digest}  ${name}`);
    return { name, sha256: digest };
  });
  const fixture = { release: {
    source_revision: runtime,
    spl_compatibility_version: "0.3",
    application_version: "0.2.0",
    binary_identities: [
      { name: "open-splunk-server", application_version: "0.2.0", source_revision: runtime, spl_compatibility_version: "0.3" },
      { name: "open-splunk-collector", application_version: "0.2.0", source_revision: runtime, spl_compatibility_version: null },
      { name: "open-splunk-loggen", application_version: "0.2.0", source_revision: runtime, spl_compatibility_version: null },
    ],
    artifact_digests,
  } };
  verifyRelease(fixture, runtime, new Map([["artifact-digests", lines.join("\n")]]), "0.1.0");
  fixture.release.spl_compatibility_version = "0.2";
  assert.throws(
    () => verifyRelease(fixture, runtime, new Map([["artifact-digests", lines.join("\n")]]), "0.1.0"),
    /release identity/,
  );
  fixture.release.spl_compatibility_version = "0.3";
  fixture.release.application_version = "release";
  assert.throws(
    () => verifyRelease(fixture, runtime, new Map([["artifact-digests", lines.join("\n")]]), "0.1.0"),
    /release identity/,
  );
  fixture.release.application_version = "0.2.0";
  assert.throws(
    () => verifyRelease(fixture, runtime, new Map([["artifact-digests", lines.join("\n")]]), "0.2.0"),
    /advance beyond/,
  );
});

test("synthetic clean v0.3 R/E lineage passes candidate and accepted verification", async (t) => {
  const fixture = await mkdtemp(path.join(tmpdir(), "open-splunk-v03-accepted-"));
  t.after(() => rm(fixture, { recursive: true, force: true }));

  const put = async (relative, contents) => {
    const absolute = path.join(fixture, relative);
    await mkdir(path.dirname(absolute), { recursive: true });
    await writeFile(absolute, contents);
  };
  const git = (...argumentsList) => {
    const result = spawnSync("git", argumentsList, {
      cwd: fixture,
      encoding: "utf8",
    });
    assert.equal(result.status, 0, `${argumentsList.join(" ")}\n${result.stderr}`);
    return result.stdout.trim();
  };
  const runVerifier = (relative, ...argumentsList) => spawnSync(
    process.execPath,
    [relative, ...argumentsList],
    { cwd: fixture, encoding: "utf8" },
  );
  const json = (value) => `${JSON.stringify(value, null, 2)}\n`;

  for (const relative of [
    "scripts/verify-spl-v02-acceptance.mjs",
    "scripts/verify-spl-v03-acceptance.mjs",
    "scripts/strict-json.mjs",
    "docs/evidence/spl-v0.2-activation/manifest.schema.json",
    "docs/evidence/spl-v0.3/manifest.schema.json",
  ]) {
    const absolute = path.join(fixture, relative);
    await mkdir(path.dirname(absolute), { recursive: true });
    await copyFile(path.join(workspace, relative), absolute);
  }
  await put("package.json", '{"type":"module"}\n');

  git("init", "--quiet");
  git("config", "user.email", "fixture@example.invalid");
  git("config", "user.name", "Fixture");
  await put("docs/synthetic-prehistory.txt", "prehistory outside the evidence chain\n");
  git("add", "docs/synthetic-prehistory.txt");
  git("commit", "--quiet", "-m", "synthetic prehistory");

  // Build a strict accepted v0.2 R/E pair first. The v0.3 verifier must replay
  // this exact ancestor authority rather than trusting mutable current files.
  const v02 = JSON.parse(await readFile(
    path.join(workspace, "docs/evidence/spl-v0.2-activation/manifest.json"),
    "utf8",
  ));
  v02.phase = "qualification-candidate";
  v02.runtime = {
    revision_binding: "containing-commit",
    revision: null,
    tree: null,
    candidate_manifest_sha256: null,
    candidate_acceptance_sha256: null,
    remote_ref: null,
    remote_readback_revision: null,
    remote_readback_at_utc: null,
  };
  v02.ci = {
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
  v02.release = {
    source_revision: null,
    spl_compatibility_version: null,
    application_version: null,
    binary_identities: [],
    artifact_digests: [],
  };
  v02.receipts = [];
  v02.decision = {
    status: "pending",
    stable_publication_authorized: false,
    reason: "Synthetic v0.2 candidate remains distribution-blocked.",
  };
  for (const artifact of v02.artifacts) {
    const absolute = path.join(fixture, artifact.path);
    try {
      await readFile(absolute);
    } catch {
      await put(artifact.path, `${artifact.id} synthetic v0.2 fixture\n`);
    }
  }
  for (const relative of [
    "README.md", "deploy/generate-env.sh", "deploy/README.md",
    "docs/collector-deployment.md", "integration/README.md",
  ]) {
    const source = await readFile(path.join(workspace, relative), "utf8");
    await put(relative, checkpointOperatorBytes(relative, source));
  }
  await chmod(path.join(fixture, "deploy/generate-env.sh"), 0o755);
  for (const artifact of v02.artifacts) {
    if (["scripts/build-release.sh", "scripts/build-oci.sh"].includes(artifact.path)) {
      await chmod(path.join(fixture, artifact.path), 0o755);
    }
  }
  await put("internal/spl/doc.go", 'package spl\n\nconst CompatibilityVersion = "0.2"\n');
  for (const artifact of v02.artifacts) {
    artifact.sha256 = sha256(await readFile(path.join(fixture, artifact.path)));
  }
  const v02CandidateReport = [
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
  await put("docs/spl-compatibility-v0.2-acceptance.md", v02CandidateReport);
  await put("docs/evidence/spl-v0.2-activation/manifest.json", json(v02));
  git("add", ".");
  git("commit", "--quiet", "-m", "synthetic v0.2 candidate R");
  const v02Runtime = git("rev-parse", "HEAD");
  const v02Tree = git("rev-parse", "HEAD^{tree}");
  const v02CandidateManifest = await readFile(path.join(
    fixture,
    "docs/evidence/spl-v0.2-activation/manifest.json",
  ));

  const v02VerifierSource = await readFile(
    path.join(workspace, "scripts/verify-spl-v02-acceptance.mjs"),
    "utf8",
  );
  const v02JobBlock = v02VerifierSource.match(/const EXPECTED_JOBS = \[(.*?)\];/s)?.[1] ?? "";
  const v02JobNames = [...v02JobBlock.matchAll(/^  "([^"]+)",$/gm)]
    .map((match) => match[1]);
  const v02RunID = 20;
  const v02Jobs = v02JobNames.map((name, index) => ({
    id: 2000 + index,
    name,
    url: `https://github.com/Suhaibinator/open-splunk/actions/runs/${v02RunID}/job/${2000 + index}`,
    status: "completed",
    conclusion: "success",
  }));
  const v02Release = {
    source_revision: v02Runtime,
    spl_compatibility_version: "0.2",
    application_version: "0.1.0",
    binary_identities: [
      { name: "open-splunk-server", application_version: "0.1.0", source_revision: v02Runtime, spl_compatibility_version: "0.2" },
      { name: "open-splunk-collector", application_version: "0.1.0", source_revision: v02Runtime, spl_compatibility_version: null },
      { name: "open-splunk-loggen", application_version: "0.1.0", source_revision: v02Runtime, spl_compatibility_version: null },
    ],
    artifact_digests: [
      "asset-manifest.json", "open-splunk-collector", "open-splunk-loggen",
      "open-splunk-server", "ui",
    ].map((name, index) => ({
      name,
      sha256: (index + 1).toString(16).repeat(64),
      bytes: index + 1,
    })),
  };
  const v02CI = {
    repository: "Suhaibinator/open-splunk",
    workflow: "CI",
    run_id: v02RunID,
    url: `https://github.com/Suhaibinator/open-splunk/actions/runs/${v02RunID}`,
    event: "workflow_dispatch",
    head_branch: "synthetic-v02",
    head_revision: v02Runtime,
    checkout_revision: v02Runtime,
    checkout_tree: v02Tree,
    release_artifact_source_revision: v02Runtime,
    status: "completed",
    conclusion: "success",
    completed_at_utc: "2026-08-12T12:00:00Z",
    job_set_complete: true,
    jobs: v02Jobs,
  };
  const v02Accepted = clone(v02);
  v02Accepted.phase = "accepted";
  v02Accepted.runtime = {
    revision_binding: "recorded-runtime-parent",
    revision: v02Runtime,
    tree: v02Tree,
    candidate_manifest_sha256: sha256(v02CandidateManifest),
    candidate_acceptance_sha256: sha256(v02CandidateReport),
    remote_ref: `refs/tags/spl-v0.2-evidence-${v02Runtime}`,
    remote_readback_revision: v02Runtime,
    remote_readback_at_utc: "2026-08-12T12:00:00Z",
  };
  v02Accepted.ci = v02CI;
  v02Accepted.release = v02Release;
  v02Accepted.decision = {
    status: "accepted",
    stable_publication_authorized: true,
    reason: "Synthetic v0.2 prerequisite accepted.",
  };
  const v02CIRun = { ...v02CI };
  delete v02CIRun.jobs;
  const v02ReleaseIdentity = { ...v02Release };
  delete v02ReleaseIdentity.artifact_digests;
  const v02ReceiptValues = new Map([
    ["source-identity", { runtime_revision: v02Runtime, runtime_tree: v02Tree, remote_ref: v02Accepted.runtime.remote_ref, remote_readback_revision: v02Runtime, result: "pass" }],
    ["quality-gates", { runtime_revision: v02Runtime, runtime_tree: v02Tree, result: "pass" }],
    ["clickhouse-gates", { runtime_revision: v02Runtime, runtime_tree: v02Tree, image: "clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49", result: "pass" }],
    ["compatibility-audit", { runtime_revision: v02Runtime, runtime_tree: v02Tree, compatibility_version: "0.2", unresolved_findings: 0, result: "pass" }],
    ["ci-run", v02CIRun],
    ["ci-jobs", v02Jobs],
    ["release-identity", v02ReleaseIdentity],
    ["release-artifacts", v02Release.artifact_digests],
  ]);
  v02Accepted.receipts = [];
  for (const [id, value] of v02ReceiptValues) {
    const bytes = Buffer.from(json(value));
    const relative = `receipts/${id}.json`;
    await put(`docs/evidence/spl-v0.2-activation/${relative}`, bytes);
    v02Accepted.receipts.push({ id, path: relative, sha256: sha256(bytes), bytes: bytes.length });
  }
  const v02AcceptedReport = v02CandidateReport
    .replace("**Status:** pending", "**Status:** accepted")
    .replace("`qualification-candidate`", "`accepted`")
    .replace("**Decision:** pending", "**Decision:** accepted")
    .replace("**Stable publication authorized:** no", "**Stable publication authorized:** yes");
  await put("docs/spl-compatibility-v0.2-acceptance.md", v02AcceptedReport);
  await put("docs/evidence/spl-v0.2-activation/manifest.json", json(v02Accepted));
  git("add", ".");
  git("commit", "--quiet", "-m", "synthetic v0.2 evidence E");
  const v02Evidence = git("rev-parse", "HEAD");
  const v02EvidenceManifest = await readFile(path.join(
    fixture,
    "docs/evidence/spl-v0.2-activation/manifest.json",
  ));

  // Create v0.3 candidate R on top of accepted v0.2 E and prove that its clean
  // self-unbound state is executable before adding post-commit evidence.
  const v03 = checkpointManifestFixture();
  v03.phase = "qualification-candidate";
  v03.compatibility.runtime_authored_search = "0.3";
  v03.prerequisite = {
    v02_status: "accepted",
    evidence_path: "docs/evidence/spl-v0.2-activation/manifest.json",
    evidence_sha256: sha256(v02EvidenceManifest),
    evidence_revision: v02Evidence,
    runtime_revision: v02Runtime,
  };
  v03.runtime.revision_binding = "containing-commit";
  v03.decision.reason = "Synthetic v0.3 candidate remains distribution-blocked.";
  for (const artifact of v03.artifacts) {
    const absolute = path.join(fixture, artifact.path);
    try {
      await readFile(absolute);
    } catch {
      await put(artifact.path, `${artifact.id} synthetic v0.3 fixture\n`);
    }
  }
  for (const relative of [
    "README.md", "deploy/generate-env.sh", "deploy/README.md",
    "docs/collector-deployment.md", "integration/README.md",
  ]) {
    const source = await readFile(path.join(workspace, relative), "utf8");
    await put(relative, candidateOperatorBytes(relative, source));
  }
  await put("internal/spl/doc.go", 'package spl\n\nconst CompatibilityVersion = "0.3"\n');
  for (const artifact of v03.artifacts) {
    artifact.sha256 = sha256(await readFile(path.join(fixture, artifact.path)));
  }
  const v03CandidateReport = [
    "# Synthetic SPL v0.3 acceptance",
    "",
    "**Acceptance phase:** `qualification-candidate`",
    "",
    "**Status:** qualification candidate; final evidence pending",
    "",
    "**Target authored-search identity:** `0.3`",
    "",
    "**Knowledge-expression identity:** `0.1`",
    "",
    "**v0.2 prerequisite:** accepted",
    "",
    "**Activation decision:** pending; stable publication blocked",
    "",
  ].join("\n");
  await put("docs/spl-compatibility-v0.3-acceptance.md", v03CandidateReport);
  await put("docs/evidence/spl-v0.3/manifest.json", json(v03));
  git("add", ".");
  git("commit", "--quiet", "-m", "synthetic v0.3 candidate R");
  const v03Runtime = git("rev-parse", "HEAD");
  const v03Tree = git("rev-parse", "HEAD^{tree}");
  const candidateResult = runVerifier(
    "scripts/verify-spl-v03-acceptance.mjs",
    "--phase", "qualification-candidate",
  );
  assert.equal(candidateResult.status, 0, candidateResult.stderr);
  assert.equal(
    candidateResult.stdout,
    "SPL v0.3 qualification candidate is internally consistent; stable publication remains blocked\n",
  );

  const v03Accepted = clone(v03);
  v03Accepted.phase = "accepted";
  v03Accepted.runtime = {
    revision_binding: "recorded-runtime-parent",
    revision: v03Runtime,
    tree: v03Tree,
    candidate_acceptance_sha256: sha256(v03CandidateReport),
    remote_ref: `refs/tags/spl-v0.3-runtime-${v03Runtime}`,
    remote_readback_revision: v03Runtime,
  };
  v03Accepted.ci = acceptedCI(v03Runtime);
  const applicationVersion = "0.2.0";
  const binaryIdentities = [
    { name: "open-splunk-server", application_version: applicationVersion, source_revision: v03Runtime, spl_compatibility_version: "0.3" },
    { name: "open-splunk-collector", application_version: applicationVersion, source_revision: v03Runtime, spl_compatibility_version: null },
    { name: "open-splunk-loggen", application_version: applicationVersion, source_revision: v03Runtime, spl_compatibility_version: null },
  ];
  const artifactDigests = [...REQUIRED_RELEASE_DIGESTS].map((name, index) => ({
    name,
    sha256: index.toString(16).padStart(64, "0"),
  }));
  v03Accepted.release = {
    source_revision: v03Runtime,
    spl_compatibility_version: "0.3",
    application_version: applicationVersion,
    binary_identities: binaryIdentities,
    artifact_digests: artifactDigests,
  };
  v03Accepted.decision = {
    status: "accepted",
    stable_publication_authorized: true,
    reason: "Synthetic exact v0.3 R/E evidence passed.",
  };
  const releaseReadback = [
    `application_version=${applicationVersion}`,
    `source_revision=${v03Runtime}`,
    "spl_compatibility_version=0.3",
    `ui_build_id=${expectedUIBuildID(applicationVersion, v03Runtime)}`,
    `ui_sha256=${"b".repeat(64)}`,
    "",
  ].join("\n");
  artifactDigests.find(
    (entry) => entry.name === "release-verification.txt",
  ).sha256 = sha256(releaseReadback);
  const binaryIdentityReceipt = [
    "open-splunk-server",
    `application_version=${applicationVersion}`,
    `source_revision=${v03Runtime}`,
    "spl_compatibility_version=0.3",
    "open-splunk-collector",
    `application_version=${applicationVersion}`,
    `source_revision=${v03Runtime}`,
    "open-splunk-loggen",
    `application_version=${applicationVersion}`,
    `source_revision=${v03Runtime}`,
    "",
  ].join("\n");
  const digestReceipt = `${artifactDigests
    .map((entry) => `${entry.sha256}  ${entry.name}`)
    .join("\n")}\n`;
  const v03ReceiptValues = new Map([
    ["source-identity", json({ runtime_revision: v03Runtime, runtime_tree: v03Tree, compatibility_version: "0.3", knowledge_expression_version: "0.1", result: "pass" })],
    ["go-test-all", json({ runtime_revision: v03Runtime, runtime_tree: v03Tree, command: "go test ./... -count=1 -timeout=15m", result: "pass" })],
    ["go-race-focused", json({ runtime_revision: v03Runtime, runtime_tree: v03Tree, packages: [
      "./internal/spl", "./internal/plan", "./internal/clickhouse",
      "./internal/queryexec", "./internal/searchjobs", "./internal/server",
      "./internal/searchsnapshot", "./internal/searchinspection",
      "./internal/searchanalysis", "./internal/export",
    ], result: "pass" })],
    ["frontend-gates", json({ runtime_revision: v03Runtime, runtime_tree: v03Tree, node: "v24.18.0", npm: "11.16.0", commands: [
      "npm ci", "npm audit --omit=dev --audit-level=critical",
      "npm run typecheck", "npm run lint", "npm run test:frontend",
      "npm run build",
    ], result: "pass" })],
    ["clickhouse-v03", json({ runtime_revision: v03Runtime, runtime_tree: v03Tree, image: "clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49", tests: REQUIRED_SPL_TESTS, result: "pass" })],
    ["release-readback", releaseReadback],
    ["binary-identities", binaryIdentityReceipt],
    ["artifact-digests", digestReceipt],
    ["ci-readback", json(v03Accepted.ci)],
    ["remote-readback", json({ runtime_revision: v03Runtime, runtime_tree: v03Tree, remote_ref: v03Accepted.runtime.remote_ref, remote_readback_revision: v03Runtime, result: "pass" })],
  ]);
  v03Accepted.receipts = [];
  for (const [id, contents] of v03ReceiptValues) {
    const bytes = Buffer.from(contents);
    const relative = `receipts/${id}.txt`;
    await put(`docs/evidence/spl-v0.3/${relative}`, bytes);
    v03Accepted.receipts.push({ id, path: relative, sha256: sha256(bytes), bytes: bytes.length });
  }
  const v03AcceptedReport = v03CandidateReport
    .replace("`qualification-candidate`", "`accepted`")
    .replace("qualification candidate; final evidence pending", "accepted")
    .replace("pending; stable publication blocked", "accepted");
  await put("docs/spl-compatibility-v0.3-acceptance.md", v03AcceptedReport);
  const v03AcceptedManifest = Buffer.from(json(v03Accepted));
  await put("docs/evidence/spl-v0.3/manifest.json", v03AcceptedManifest);
  git("add", ".");
  git("commit", "--quiet", "-m", "synthetic v0.3 evidence E");
  const v03RuntimeTag = `spl-v0.3-runtime-${v03Runtime}`;
  git("tag", v03RuntimeTag, v03Runtime);

  const acceptedResult = runVerifier(
    "scripts/verify-spl-v03-acceptance.mjs",
    "--phase", "accepted",
  );
  assert.equal(acceptedResult.status, 0, acceptedResult.stderr);
  assert.equal(
    acceptedResult.stdout,
    "SPL v0.3 accepted evidence is internally consistent\n",
  );
  const forgedRuntimeRef = clone(v03Accepted);
  forgedRuntimeRef.runtime.remote_ref += "-forged";
  await put("docs/evidence/spl-v0.3/manifest.json", json(forgedRuntimeRef));
  git("add", "docs/evidence/spl-v0.3/manifest.json");
  git("commit", "--quiet", "--amend", "--no-edit");
  const forgedRuntimeRefResult = runVerifier(
    "scripts/verify-spl-v03-acceptance.mjs",
    "--phase", "accepted",
  );
  assert.notEqual(forgedRuntimeRefResult.status, 0);
  assert.match(forgedRuntimeRefResult.stderr, /immutable full-R runtime qualification tag/);
  await put("docs/evidence/spl-v0.3/manifest.json", v03AcceptedManifest);
  git("add", "docs/evidence/spl-v0.3/manifest.json");
  git("commit", "--quiet", "--amend", "--no-edit");
  const restoredV03Evidence = git("rev-parse", "HEAD");

  // Later source can retain this accepted authority without pretending that
  // the descendant itself is E. Direct accepted/publication verification must
  // remain HEAD-scoped, while exact-E ancestor replay requires full history.
  await put("docs/synthetic-v0.3-descendant.txt", "harmless descendant\n");
  git("add", "docs/synthetic-v0.3-descendant.txt");
  git("commit", "--quiet", "-m", "synthetic post-acceptance descendant");
  const directDescendant = runVerifier(
    "scripts/verify-spl-v03-acceptance.mjs",
    "--phase", "accepted",
  );
  assert.notEqual(directDescendant.status, 0);
  assert.match(directDescendant.stderr, /accepted E must be a non-merge commit/);
  const ancestorArguments = [
    "--phase", "accepted",
    "--evidence-revision", restoredV03Evidence,
    "--print-evidence-binding",
  ];
  const ancestorResult = runVerifier(
    "scripts/verify-spl-v03-acceptance.mjs",
    ...ancestorArguments,
  );
  assert.equal(ancestorResult.status, 0, ancestorResult.stderr);
  assert.equal(
    ancestorResult.stdout,
    `${restoredV03Evidence} ${sha256(v03AcceptedManifest)}\n`,
  );
  const unrelatedEvidence = git(
    "commit-tree", v03Tree, "-m", "synthetic unrelated evidence object",
  );
  const unrelatedResult = runVerifier(
    "scripts/verify-spl-v03-acceptance.mjs",
    "--phase", "accepted",
    "--evidence-revision", unrelatedEvidence,
    "--print-evidence-binding",
  );
  assert.notEqual(unrelatedResult.status, 0);
  assert.match(unrelatedResult.stderr, /must be an ancestor of the current checkout/);

  const protocolRoot = await mkdtemp(path.join(tmpdir(), "open-splunk-v03-history-"));
  t.after(() => rm(protocolRoot, { recursive: true, force: true }));
  const remote = path.join(protocolRoot, "remote.git");
  const full = path.join(protocolRoot, "full");
  const shallow = path.join(protocolRoot, "shallow");
  const deepShallow = path.join(protocolRoot, "deep-shallow");
  const runGit = (cwd, ...argumentsList) => {
    const result = spawnSync("git", argumentsList, { cwd, encoding: "utf8" });
    assert.equal(result.status, 0, `${argumentsList.join(" ")}\n${result.stderr}`);
    return result.stdout.trim();
  };
  runGit(protocolRoot, "init", "--quiet", "--bare", remote);
  runGit(fixture, "remote", "add", "protocol-origin", remote);
  runGit(
    fixture,
    "push", "--quiet", "protocol-origin",
    "HEAD:refs/heads/descendant", `refs/tags/${v03RuntimeTag}`,
  );
  const remoteURL = `file://${remote}`;
  runGit(protocolRoot, "clone", "--quiet", "--branch", "descendant", remoteURL, full);
  runGit(
    protocolRoot,
    "clone", "--quiet", "--depth=1", "--branch", "descendant", remoteURL, shallow,
  );
  runGit(
    protocolRoot,
    "clone", "--quiet", "--depth=5", "--branch", "descendant", remoteURL, deepShallow,
  );
  const fullResult = spawnSync(process.execPath, [
    "scripts/verify-spl-v03-acceptance.mjs",
    ...ancestorArguments,
  ], { cwd: full, encoding: "utf8" });
  assert.equal(fullResult.status, 0, fullResult.stderr);
  assert.equal(fullResult.stdout, ancestorResult.stdout);

  assert.equal(runGit(deepShallow, "rev-parse", "--is-shallow-repository"), "true");
  runGit(deepShallow, "cat-file", "-e", `${restoredV03Evidence}^{commit}`);
  runGit(deepShallow, "cat-file", "-e", `${v03Runtime}^{commit}`);
  runGit(deepShallow, "cat-file", "-e", `${v02Evidence}^{commit}`);
  runGit(deepShallow, "cat-file", "-e", `${v02Runtime}^{commit}`);
  const deepShallowResult = spawnSync(process.execPath, [
    "scripts/verify-spl-v03-acceptance.mjs",
    ...ancestorArguments,
  ], { cwd: deepShallow, encoding: "utf8" });
  assert.notEqual(deepShallowResult.status, 0);
  assert.match(deepShallowResult.stderr, /requires a non-shallow repository/);

  const shallowResult = spawnSync(process.execPath, [
    "scripts/verify-spl-v03-acceptance.mjs",
    ...ancestorArguments,
  ], { cwd: shallow, encoding: "utf8" });
  assert.notEqual(shallowResult.status, 0);
  assert.match(shallowResult.stderr, /requires a non-shallow repository/);

  const graftsPath = path.join(full, ".git", "info", "grafts");
  await writeFile(graftsPath, `${restoredV03Evidence} ${v03Runtime}\n`);
  const graftedResult = spawnSync(process.execPath, [
    "scripts/verify-spl-v03-acceptance.mjs",
    ...ancestorArguments,
  ], { cwd: full, encoding: "utf8" });
  assert.notEqual(graftedResult.status, 0);
  assert.match(graftedResult.stderr, /legacy Git grafts can forge R\/E topology/);
  await rm(graftsPath);

  runGit(shallow, "fetch", "--quiet", "--unshallow", "--tags");
  const restoredResult = spawnSync(process.execPath, [
    "scripts/verify-spl-v03-acceptance.mjs",
    ...ancestorArguments,
  ], { cwd: shallow, encoding: "utf8" });
  assert.equal(restoredResult.status, 0, restoredResult.stderr);
  assert.equal(restoredResult.stdout, ancestorResult.stdout);
});

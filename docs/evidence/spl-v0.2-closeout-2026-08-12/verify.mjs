#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const bundleRoot = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(bundleRoot, "../../..");
const manifestPath = path.join(bundleRoot, "manifest.json");
const schemaPath = path.join(bundleRoot, "manifest.schema.json");
const failures = [];

function check(condition, message) {
  if (!condition) failures.push(message);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

async function parseJSON(file, label) {
  try {
    return JSON.parse(await readFile(file, "utf8"));
  } catch (error) {
    failures.push(`${label} is not valid JSON: ${error.message}`);
    return null;
  }
}

function git(args, encoding = "utf8") {
  const result = spawnSync("git", args, {
    cwd: repositoryRoot,
    encoding,
    maxBuffer: 32 * 1024 * 1024,
  });
  check(result.status === 0,
    `git ${args.join(" ")} failed: ${String(result.stderr).trim()}`);
  return result.status === 0 ? result.stdout : null;
}

const manifest = await parseJSON(manifestPath, "manifest");
const schema = await parseJSON(schemaPath, "schema");
if (!manifest || !schema) {
  process.stderr.write(`${failures.join("\n")}\n`);
  process.exit(1);
}

check(schema.$schema === "https://json-schema.org/draft/2020-12/schema",
  "schema draft is not pinned");
check(schema.additionalProperties === false,
  "schema root must reject additional properties");
check(manifest.$schema === "./manifest.schema.json",
  "manifest schema binding is invalid");
check(manifest.format_version ===
  "open-splunk-spl-v0.2-reachable-closeout-v1",
"manifest format is invalid");
check(manifest.evidence_state === "incomplete",
  "closeout manifest must remain incomplete until every gate closes");
check(manifest.decision?.status === "pending" &&
  manifest.decision?.v02_acceptance_closed === false &&
  manifest.decision?.v03_prerequisite_satisfied === false,
"manifest must not advertise acceptance");
check(Array.isArray(manifest.pending_gates) && manifest.pending_gates.length > 0,
  "manifest must enumerate pending gates");
check(manifest.semantic_reconciliation?.production_semantics_changed === true,
  "stats-BY atomic publication must be recorded as a production semantic change");
const requiredSemanticEvidence = [
  "internal/clickhouse.TestStatsByDeferredValidationAdversarialAgainstClickHouse",
  "internal/clickhouse.TestStatsMultivalueByExpansionLimitAgainstClickHouse",
  "internal/queryexec.TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse",
  "internal/queryexec.TestStatsByLateUnsupportedValueIsAtomicAndRedacted",
  "internal/queryexec.TestStatsByLateExpansionLimitIsAtomicAndRedacted",
  "internal/searchjobs.TestStatsByAtomicManagerHidesStagedPrefixAndClearsItOnFailure",
  "internal/searchjobs.TestStatsByExpansionLimitManagerHidesStagedPrefixAndClearsItOnFailure",
];
for (const identity of requiredSemanticEvidence) {
  check(manifest.semantic_reconciliation?.evidence_tests?.includes(identity),
    `semantic reconciliation is missing evidence ${identity}`);
}

const target = manifest.reachable_target;
const targetTree = git(["rev-parse", `${target.revision}^{tree}`]);
if (targetTree !== null) {
  check(targetTree.trim() === target.tree,
    "reachable target tree does not match Git readback");
}

await Promise.all([
  ["corrected contract", "docs/spl-compatibility-v0.2.md",
    manifest.contracts.corrected_carrier_contract_sha256],
  ["corrected corpus", "internal/spl/testdata/compatibility-v0.2.json",
    manifest.contracts.corrected_carrier_corpus_sha256],
  ["corrected migration guide", "docs/spl-compatibility-v0.2-migration.md",
    manifest.contracts.corrected_carrier_migration_sha256],
].map(async ([name, relative, expected]) => {
  const bytes = await readFile(path.join(repositoryRoot, relative));
  check(sha256(bytes) === expected, `${name} SHA-256 does not match`);
}));

for (const [name, relative, expected] of [
  ["target contract", "docs/spl-compatibility-v0.2.md",
    manifest.contracts.target_contract_sha256],
  ["target corpus", "internal/spl/testdata/compatibility-v0.2.json",
    manifest.contracts.target_corpus_sha256],
]) {
  const bytes = git(["show", `${target.revision}:${relative}`], null);
  if (bytes !== null) {
    check(sha256(bytes) === expected, `${name} SHA-256 does not match Git`);
  }
}

const oldManifest = await readFile(
  path.join(repositoryRoot, "docs/evidence/spl-v0.2/manifest.json"),
);
check(sha256(oldManifest) === manifest.supersedes.manifest_sha256,
  "historical manifest changed");
git(["diff", "--exit-code", target.revision, "--", "docs/evidence/spl-v0.2"]);

check(manifest.supersedes.terminal_status === "completed" &&
  manifest.supersedes.terminal_conclusion === "cancelled",
"superseded CI terminal state is not recorded honestly");
const olderCI = manifest.older_cancelled_ci_readback;
check(olderCI?.run_id === 31540820337 &&
  olderCI?.head_revision === "dffde13c84d9a2ef0567e89dd527ec4776f5ca42" &&
  olderCI?.status === "completed" &&
  olderCI?.conclusion === "cancelled" &&
  olderCI?.release_jobs === "skipped",
"older cancelled CI readback is inconsistent");

const reachableCI = manifest.reachable_ci_readback;
check(reachableCI?.run_id === 31571852036 &&
  reachableCI?.url ===
    "https://github.com/Suhaibinator/open-splunk/actions/runs/31571852036" &&
  reachableCI?.workflow_name === "CI" &&
  reachableCI?.event === "push" &&
  reachableCI?.head_branch === "main" &&
  reachableCI?.head_revision === target.revision &&
  reachableCI?.created_at_utc === "2026-08-12T06:55:15Z" &&
  reachableCI?.started_at_utc === "2026-08-12T06:55:15Z" &&
  reachableCI?.updated_at_utc === "2026-08-12T07:46:59Z" &&
  reachableCI?.status === "completed" &&
  reachableCI?.conclusion === "failure",
"reachable-target CI run identity or terminal failure is inconsistent");

const backendVertical = reachableCI?.backend_vertical_job;
check(backendVertical?.job_id === 94035340634 &&
  backendVertical?.url ===
    "https://github.com/Suhaibinator/open-splunk/actions/runs/31571852036/job/94035340634" &&
  backendVertical?.started_at_utc === "2026-08-12T06:57:34Z" &&
  backendVertical?.completed_at_utc === "2026-08-12T07:07:30Z" &&
  backendVertical?.status === "completed" &&
  backendVertical?.conclusion === "failure" &&
  backendVertical?.failed_step?.number === 8 &&
  backendVertical?.failed_step?.conclusion === "failure" &&
  Array.isArray(backendVertical?.observed_failures) &&
  backendVertical.observed_failures.length > 0,
"reachable-target backend vertical failure is inconsistent");

check(reachableCI?.release_oci_contract_job?.job_id === 94035340572 &&
  reachableCI?.release_oci_contract_job?.conclusion === "success",
"reachable-target OCI contract result is inconsistent");
check(reachableCI?.production_binaries_job?.job_id === 94046353594 &&
  reachableCI?.production_binaries_job?.conclusion === "skipped" &&
  reachableCI?.release_asset_consistency_job?.job_id === 94046354835 &&
  reachableCI?.release_asset_consistency_job?.conclusion === "skipped",
"reachable-target release jobs were not both recorded as skipped");
check(reachableCI?.head_revision === target.remote_readback_revision,
  "reachable-target CI head does not match origin/main readback");

const schemaRequired = new Set(schema.required ?? []);
check(schemaRequired.has("older_cancelled_ci_readback") &&
  schemaRequired.has("reachable_ci_readback") &&
  !schemaRequired.has("historical_ci_readback"),
"schema does not distinguish the older cancelled and reachable failed runs");
check(manifest.contracts.final_revision_binding ===
  "pending_new_reachable_commit",
"corrected contracts must remain pending a reachable revision binding");

if (failures.length > 0) {
  process.stderr.write(`${failures.join("\n")}\n`);
  process.exit(1);
}
process.stdout.write(
  "v0.2 reachable-target closeout evidence is internally consistent and deliberately incomplete\n",
);

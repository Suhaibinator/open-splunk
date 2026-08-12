#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  lstat,
  readFile,
  readdir,
} from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const TARGET_REVISION = "dffde13c84d9a2ef0567e89dd527ec4776f5ca42";
const TARGET_TREE = "70a99f34c187c6f1c46fa06549af78f33ed2e017";
const BASE_REVISION = "c5440b96248c68a9b58d10ebaf08eaef5345b61a";
const BASE_TARGET_DIFF_SHA256 =
  "ba796b30a6d67bd6f08ec56c8e2deeab553e13086294d710141f77d0e0437cac";
const REPORT_SHA256 =
  "23f91f67d935026d69313b5e9f4552cc328bc44aff7461f445411052cbc8d50c";
const PLACEHOLDER_PATTERN = /^PENDING::[A-Z0-9_]+$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/;

const argumentsList = process.argv.slice(2);
const allowPlaceholders = argumentsList.length === 1 &&
  argumentsList[0] === "--allow-placeholders";
if (argumentsList.length !== 0 && !allowPlaceholders) {
  process.stderr.write(
    "usage: node docs/evidence/spl-v0.2/verify.mjs [--allow-placeholders]\n",
  );
  process.exit(2);
}

const bundleRoot = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(bundleRoot, "../../..");
const failures = [];
const notes = [];

function check(condition, message) {
  if (!condition) {
    failures.push(message);
  }
}

function note(message) {
  notes.push(message);
}

async function readBytes(relative) {
  checkSafeRelativePath(relative);
  const absolute = path.resolve(bundleRoot, relative);
  const metadata = await lstat(absolute);
  check(metadata.isFile(), `${relative} must be a regular file`);
  check(!metadata.isSymbolicLink(), `${relative} must not be a symbolic link`);
  return readFile(absolute);
}

async function readJSON(relative) {
  const bytes = await readBytes(relative);
  try {
    return JSON.parse(bytes.toString("utf8"));
  } catch (error) {
    failures.push(`${relative} is not valid JSON: ${error.message}`);
    return null;
  }
}

function checkSafeRelativePath(relative) {
  check(typeof relative === "string" && relative.length > 0,
    "bundle paths must be nonempty strings");
  check(!path.isAbsolute(relative), `${relative} must be relative`);
  check(relative === relative.replaceAll("\\", "/"),
    `${relative} must use slash separators`);
  const normalized = path.posix.normalize(relative);
  check(normalized === relative && normalized !== ".." &&
    !normalized.startsWith("../"), `${relative} must be a clean in-bundle path`);
  const resolved = path.resolve(bundleRoot, relative);
  check(resolved.startsWith(`${bundleRoot}${path.sep}`),
    `${relative} must resolve inside the bundle`);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

async function checkDigestedFile(entry, label) {
  check(entry && typeof entry === "object" && !Array.isArray(entry),
    `${label} must be an object`);
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    return null;
  }
  checkSafeRelativePath(entry.path);
  check(SHA256_PATTERN.test(entry.sha256), `${label} has an invalid SHA-256`);
  check(Number.isSafeInteger(entry.bytes) && entry.bytes > 0,
    `${label} has an invalid byte count`);
  const bytes = await readBytes(entry.path);
  check(bytes.length === entry.bytes,
    `${label} byte count is ${bytes.length}, expected ${entry.bytes}`);
  check(sha256(bytes) === entry.sha256, `${label} SHA-256 does not match`);
  return bytes;
}

function findPlaceholders(value, currentPath = "manifest", found = []) {
  if (typeof value === "string" && value.startsWith("PENDING::")) {
    check(PLACEHOLDER_PATTERN.test(value),
      `${currentPath} contains a malformed placeholder ${JSON.stringify(value)}`);
    found.push({ path: currentPath, value });
    return found;
  }
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      findPlaceholders(entry, `${currentPath}[${index}]`, found));
    return found;
  }
  if (value && typeof value === "object") {
    for (const [key, entry] of Object.entries(value)) {
      findPlaceholders(entry, `${currentPath}.${key}`, found);
    }
  }
  return found;
}

function ensureNoAuthoredSourceMarkers(value, label) {
  const encoded = JSON.stringify(value).toLowerCase();
  check(!encoded.includes("| eval "),
    `${label} contains an authored-source eval marker`);
  check(!encoded.includes("| where "),
    `${label} contains an authored-source where marker`);
}

function git(argumentsForGit, options = {}) {
  const result = spawnSync("git", argumentsForGit, {
    cwd: repositoryRoot,
    encoding: options.encoding ?? "utf8",
    maxBuffer: 128 * 1024 * 1024,
  });
  check(result.status === 0,
    `git ${argumentsForGit.join(" ")} failed: ${String(result.stderr).trim()}`);
  return result.status === 0 ? result.stdout : null;
}

async function listBundleFiles(directory = bundleRoot, prefix = "") {
  const result = [];
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => left.name.localeCompare(right.name, "en"));
  for (const entry of entries) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = path.join(directory, entry.name);
    check(!entry.isSymbolicLink(), `${relative} must not be a symbolic link`);
    if (entry.isDirectory()) {
      result.push(...await listBundleFiles(absolute, relative));
    } else {
      check(entry.isFile(), `${relative} must be a regular file`);
      result.push(relative);
    }
  }
  return result;
}

function tuple(objectID, sourceLocation) {
  return `${objectID}\u0000${sourceLocation}`;
}

function parseIdentityLines(text) {
  const result = new Map();
  for (const line of text.split("\n")) {
    const separator = line.indexOf("=");
    if (separator > 0) {
      result.set(line.slice(0, separator), line.slice(separator + 1));
    }
  }
  return result;
}

const manifest = await readJSON("manifest.json");
const schema = await readJSON("manifest.schema.json");
if (!manifest || !schema) {
  process.stderr.write(`${failures.join("\n")}\n`);
  process.exit(1);
}

check(schema.$schema === "https://json-schema.org/draft/2020-12/schema",
  "manifest schema must declare JSON Schema draft 2020-12");
check(schema.$id === "manifest.schema.json", "manifest schema ID is invalid");
check(schema.type === "object" && schema.additionalProperties === false,
  "manifest schema root must be a closed object");
check(Array.isArray(schema.required) && schema.required.includes("runtime_target") &&
  schema.required.includes("gates") && schema.required.includes("audit") &&
  schema.required.includes("release") && schema.required.includes("integrity"),
"manifest schema must require every evidence authority");

check(manifest.$schema === "./manifest.schema.json", "manifest schema binding is invalid");
check(manifest.format_version ===
  "open-splunk-spl-v0.2-acceptance-evidence-v1",
"manifest format version is invalid");
check(manifest.evidence_state === "pending_ci" ||
  manifest.evidence_state === "complete", "manifest evidence state is invalid");

const placeholders = findPlaceholders(manifest);
const distinctPlaceholders = [...new Set(placeholders.map((entry) => entry.value))].sort();
const declaredPending = [...new Set(manifest.pending ?? [])].sort();
check(JSON.stringify(distinctPlaceholders) === JSON.stringify(declaredPending),
  "manifest.pending must enumerate every distinct placeholder exactly once");
if (placeholders.length > 0) {
  if (allowPlaceholders) {
    for (const entry of placeholders) {
      note(`placeholder ${entry.value} at ${entry.path}`);
    }
  } else {
    failures.push(
      `strict verification rejects ${distinctPlaceholders.length} pending placeholder(s)`,
    );
  }
}

ensureNoAuthoredSourceMarkers(manifest, "manifest");

check(manifest.compatibility?.authored_search === "0.2",
  "authored-search compatibility identity must be 0.2");
check(manifest.compatibility?.knowledge_expression === "0.1",
  "knowledge-expression compatibility identity must remain 0.1");
check(manifest.compatibility?.application_version === "0.1.0",
  "application version must remain distinct at 0.1.0");
check(manifest.decision?.unresolved_implementation_findings === 0,
  "implementation finding count must be zero");
check(manifest.decision?.unresolved_audit_findings === 0,
  "audit finding count must be zero");

const target = manifest.runtime_target;
check(target?.revision === TARGET_REVISION, "runtime target revision is invalid");
check(target?.tree === TARGET_TREE, "runtime target tree is invalid");
check(target?.base_revision === BASE_REVISION, "runtime base revision is invalid");
check(target?.base_target_binary_diff_sha256 === BASE_TARGET_DIFF_SHA256,
  "base-to-target diff digest is invalid");
check(target?.remote_readback_revision === TARGET_REVISION,
  "remote readback must equal the runtime target");
check(target?.git_status_porcelain_bytes_before === 0 &&
  target?.git_status_porcelain_bytes_after === 0,
"target checkout must be clean before and after local gates");
check(manifest.evidence_publication?.embedded_revision === null,
  "publication commit must not be recursively embedded");
check(manifest.evidence_publication?.expected_first_parent === TARGET_REVISION,
  "publication carrier must descend directly from the runtime target");
check(manifest.evidence_publication?.is_runtime_activation_target === false,
  "publication carrier must not be labeled as the runtime target");

const targetTree = git(["rev-parse", `${TARGET_REVISION}^{tree}`]);
if (targetTree !== null) {
  check(targetTree.trim() === TARGET_TREE, "Git target tree readback does not match");
}
const diff = git([
  "diff",
  "--binary",
  "--no-ext-diff",
  BASE_REVISION,
  TARGET_REVISION,
], { encoding: null });
if (diff !== null) {
  check(sha256(diff) === BASE_TARGET_DIFF_SHA256,
    "Git base-to-target binary diff digest does not match");
}

for (const contract of manifest.contracts ?? []) {
  check(GIT_OBJECT_PATTERN.test(target.revision), "target revision is not canonical");
  check(typeof contract.path === "string" && SHA256_PATTERN.test(contract.sha256),
    "contract digest entry is invalid");
  const absolute = path.resolve(repositoryRoot, contract.path);
  check(absolute.startsWith(`${repositoryRoot}${path.sep}`),
    `contract ${contract.path} escapes the repository`);
  const bytes = await readFile(absolute);
  check(sha256(bytes) === contract.sha256,
    `contract ${contract.path} digest does not match`);
}

check(Array.isArray(manifest.gates) && manifest.gates.length === 16,
  "manifest must contain exactly 16 local receipt groups");
const gateIDs = new Set();
const receiptPaths = new Set();
for (const gate of manifest.gates ?? []) {
  check(typeof gate.id === "string" && !gateIDs.has(gate.id),
    `gate ID ${JSON.stringify(gate.id)} must be unique`);
  gateIDs.add(gate.id);
  check(gate.result === "pass", `gate ${gate.id} did not pass`);
  check(Array.isArray(gate.commands) && gate.commands.length > 0 &&
    gate.commands.every((command) => typeof command === "string" && command.length > 0),
  `gate ${gate.id} must record its exact command list`);
  const receipt = gate.receipt;
  check(!receiptPaths.has(receipt.path), `receipt ${receipt.path} is referenced twice`);
  receiptPaths.add(receipt.path);
  await checkDigestedFile(receipt, `gate ${gate.id} receipt`);
}

const durablePaths = new Set();
for (const artifact of manifest.durable_artifacts ?? []) {
  check(!durablePaths.has(artifact.path),
    `durable artifact ${artifact.path} is listed twice`);
  durablePaths.add(artifact.path);
  await checkDigestedFile(artifact, `durable artifact ${artifact.path}`);
}

const report = await readJSON(manifest.audit.report);
const ledger = await readJSON(manifest.audit.dispositions);
const ledgerSchema = await readJSON(manifest.audit.dispositions_schema);
if (report && ledger && ledgerSchema) {
  ensureNoAuthoredSourceMarkers(report, "audit report");
  ensureNoAuthoredSourceMarkers(ledger, "audit ledger");
  check(report.compatibility_version === "0.2",
    "audit report compatibility version is invalid");
  check(report.scanned_objects === 186, "audit report scanned-object count is invalid");
  check(Array.isArray(report.findings) && report.findings.length === 214,
    "audit report finding count is invalid");
  const reportTuples = new Set();
  for (const finding of report.findings ?? []) {
    check(Object.keys(finding).sort().join(",") === "kind,object_id,source_location",
      "audit findings must contain only redacted identity/location fields");
    check(finding.kind === "ambiguous_unspaced_scalar_operator",
      "audit finding kind is invalid");
    const key = tuple(finding.object_id, finding.source_location);
    check(!reportTuples.has(key), `audit report duplicates tuple ${key}`);
    reportTuples.add(key);
  }

  check(ledger.$schema === "spl-v0.2-audit-dispositions.schema.json",
    "audit ledger schema binding is invalid");
  check(ledgerSchema.$id === "spl-v0.2-audit-dispositions.schema.json",
    "audit ledger schema ID is invalid");
  check(ledger.target_revision === TARGET_REVISION && ledger.target_tree === TARGET_TREE,
    "audit ledger target identity is invalid");
  check(ledger.audit_report_sha256 === REPORT_SHA256,
    "audit ledger report binding is invalid");
  check(ledger.finding_count === 214 && ledger.unresolved_count === 0 &&
    Array.isArray(ledger.unresolved) && ledger.unresolved.length === 0,
  "audit ledger counts are invalid");

  const ledgerTuples = new Set();
  let categoryFindingCount = 0;
  const categoryByID = new Map();
  for (const category of ledger.categories ?? []) {
    check(!categoryByID.has(category.id),
      `audit ledger duplicates category ${category.id}`);
    categoryByID.set(category.id, category);
    check(category.finding_count === category.findings.length,
      `audit category ${category.id} finding_count is inconsistent`);
    const objects = new Set();
    for (const findingTuple of category.findings) {
      check(Array.isArray(findingTuple) && findingTuple.length === 2,
        `audit category ${category.id} has an invalid tuple`);
      if (!Array.isArray(findingTuple) || findingTuple.length !== 2) {
        continue;
      }
      const key = tuple(findingTuple[0], findingTuple[1]);
      check(!ledgerTuples.has(key), `audit ledger classifies tuple ${key} twice`);
      ledgerTuples.add(key);
      objects.add(findingTuple[0]);
    }
    check(category.object_count === objects.size,
      `audit category ${category.id} object_count is inconsistent`);
    categoryFindingCount += category.finding_count;
  }
  check(categoryFindingCount === 214, "audit ledger categories do not total 214");
  check(ledgerTuples.size === reportTuples.size &&
    [...reportTuples].every((key) => ledgerTuples.has(key)),
  "audit ledger does not classify every report tuple exactly once");

  for (const summary of manifest.audit.categories ?? []) {
    const category = categoryByID.get(summary.id);
    check(Boolean(category), `manifest names unknown audit category ${summary.id}`);
    if (category) {
      check(summary.finding_count === category.finding_count &&
        summary.object_count === category.object_count &&
        summary.disposition === category.disposition,
      `manifest summary for audit category ${summary.id} is inconsistent`);
    }
  }
}

check(manifest.audit.target_revision === TARGET_REVISION &&
  manifest.audit.target_tree === TARGET_TREE,
"manifest audit target identity is invalid");
check(manifest.audit.auditing_binary_source_revision === TARGET_REVISION,
  "auditing binary source revision is invalid");
check(manifest.audit.report_sha256 === REPORT_SHA256 &&
  manifest.audit.report_bytes === 42991,
"canonical audit report identity is invalid");
check(manifest.audit.fixture_sha256_before ===
  manifest.audit.fixture_sha256_after, "audit mutated the fixture database");
check(manifest.audit.fixture_cross_generation_bytes_claimed_deterministic === false,
  "fixture bytes must not be claimed deterministic across generations");
check(manifest.audit.repeat_count === 4 &&
  manifest.audit.reports_byte_identical === true,
"audit repeatability receipt is invalid");
check(manifest.audit.disposition_count === 214 &&
  manifest.audit.unresolved_count === 0,
"audit disposition decision is incomplete");
check(manifest.audit.contains_authored_spl === false,
  "audit evidence must not contain authored source");
check(manifest.audit.input_database_mutated === false,
  "audit evidence must be read-only");

const releaseVerification = (await readBytes(manifest.release.verification)).toString("utf8");
const binaryIdentities = (await readBytes(manifest.release.binary_identities)).toString("utf8");
const assetManifest = await readJSON(manifest.release.asset_manifest);
const verification = parseIdentityLines(releaseVerification);
check(verification.get("application_version") === "0.1.0",
  "release verification application version is invalid");
check(verification.get("source_revision") === TARGET_REVISION,
  "release verification source revision is invalid");
check(verification.get("ui_build_id") === manifest.release.ui_build_id &&
  verification.get("ui_sha256") === manifest.release.ui_sha256,
"release verification UI identity is invalid");
check((binaryIdentities.match(/application_version=0\.1\.0/g) ?? []).length === 3,
  "binary identity proof must contain three application identities");
check((binaryIdentities.match(new RegExp(`source_revision=${TARGET_REVISION}`, "g")) ?? [])
  .length === 3, "binary identity proof must contain three target revisions");
if (assetManifest) {
  check(assetManifest.application_version === "0.1.0" &&
    assetManifest.source_revision === TARGET_REVISION,
  "asset manifest release identity is invalid");
  check(assetManifest.ui_build_id === manifest.release.ui_build_id &&
    assetManifest.ui?.sha256 === manifest.release.ui_sha256 &&
    assetManifest.ui?.file_count === manifest.release.ui_file_count &&
    assetManifest.ui?.byte_count === manifest.release.ui_byte_count,
  "asset manifest UI identity is invalid");
}
check(manifest.release.source_revision === TARGET_REVISION,
  "local release source revision is not the runtime target");
check(manifest.release.application_version === "0.1.0",
  "local release application version is invalid");
check(manifest.release.clean_before === true && manifest.release.clean_after === true,
  "release checkout was not clean before and after publication");

const releaseReceipt = (await readBytes("receipts/release.txt")).toString("utf8");
const releaseArtifactNames = new Set();
for (const artifact of manifest.release.artifacts ?? []) {
  check(!releaseArtifactNames.has(artifact.name),
    `release artifact ${artifact.name} is listed twice`);
  releaseArtifactNames.add(artifact.name);
  check(SHA256_PATTERN.test(artifact.sha256) &&
    Number.isSafeInteger(artifact.bytes) && artifact.bytes > 0 &&
    artifact.mode === "0755" && artifact.committed === false,
  `release artifact ${artifact.name} metadata is invalid`);
  check(releaseReceipt.includes(`${artifact.sha256}  build/${artifact.name}`),
    `release receipt lacks ${artifact.name} digest`);
  check(releaseReceipt.includes(
    `bytes=${artifact.bytes} mode=755 path=build/${artifact.name}`,
  ), `release receipt lacks ${artifact.name} size/mode`);
}
check(releaseArtifactNames.size === 3,
  "release manifest must describe exactly three uncommitted binaries");

const ci = manifest.ci;
check(ci.provider === "github_actions" && ci.workflow === "CI" &&
  ci.run_id === 31540820337,
"CI workflow identity is invalid");
check(ci.head_revision === TARGET_REVISION && ci.head_tree === TARGET_TREE,
  "CI head identity is not the runtime target");
if (placeholders.length === 0) {
  check(ci.status === "completed" && ci.conclusion === "success",
    "CI must be terminal success in a complete bundle");
  check(GIT_OBJECT_PATTERN.test(ci.checkout_revision) &&
    GIT_OBJECT_PATTERN.test(ci.checkout_tree), "CI checkout identity is invalid");
  check(ci.tree_equivalent === true && ci.checkout_tree === TARGET_TREE,
    "CI checkout tree must equal the runtime target tree");
  check(ci.release_artifact_source_revision === ci.checkout_revision,
    "CI release artifacts must retain the actual checkout revision");
  check(Array.isArray(ci.jobs) && ci.jobs.length > 0 &&
    ci.jobs.every((job) => job.conclusion === "success"),
  "every recorded CI job must succeed");
  await checkDigestedFile(ci.receipt, "CI receipt");
  receiptPaths.add(ci.receipt.path);
  check(manifest.evidence_state === "complete" &&
    manifest.decision.status === "accepted" && manifest.pending.length === 0,
  "placeholder-free evidence must carry the accepted decision");
}

const receiptDirectoryEntries = (await readdir(path.join(bundleRoot, "receipts")))
  .filter((entry) => entry.endsWith(".txt"))
  .map((entry) => `receipts/${entry}`)
  .sort();
check(receiptDirectoryEntries.every((entry) => receiptPaths.has(entry)),
  "every durable text receipt must be referenced by a gate or CI");
check([...receiptPaths].every((entry) => receiptDirectoryEntries.includes(entry)),
  "every referenced receipt must exist in the receipt directory");

const bundleFiles = (await listBundleFiles()).sort();
for (const relative of bundleFiles) {
  check(!relative.endsWith(".log"), `${relative} must not use the ignored log extension`);
  check(!/\.(?:db|db-shm|db-wal|sqlite|sqlite3)$/.test(relative),
    `${relative} must not retain a database`);
  check(!/(?:^|\/)open-splunk-(?:server|collector|loggen)$/.test(relative),
    `${relative} must not retain a release binary`);
}

const checksumPath = path.join(bundleRoot, manifest.integrity.index_path);
let checksumBytes = null;
try {
  checksumBytes = await readFile(checksumPath);
} catch (error) {
  if (!(allowPlaceholders && placeholders.some((entry) =>
    entry.value === "PENDING::SHA256SUMS_AFTER_CI_FINALIZATION"))) {
    failures.push(`could not read SHA256SUMS: ${error.message}`);
  } else {
    note("placeholder PENDING::SHA256SUMS_AFTER_CI_FINALIZATION leaves SHA256SUMS absent");
  }
}

if (checksumBytes !== null) {
  const indexed = new Map();
  for (const line of checksumBytes.toString("utf8").trimEnd().split("\n")) {
    const match = /^([0-9a-f]{64})  ([^\n]+)$/.exec(line);
    check(Boolean(match), `invalid SHA256SUMS line ${JSON.stringify(line)}`);
    if (!match) {
      continue;
    }
    const [, digest, relative] = match;
    checkSafeRelativePath(relative);
    check(relative !== manifest.integrity.index_path,
      "SHA256SUMS must exclude itself");
    check(!indexed.has(relative), `SHA256SUMS repeats ${relative}`);
    indexed.set(relative, digest);
  }
  const expected = bundleFiles.filter((relative) =>
    relative !== manifest.integrity.index_path);
  check(expected.every((relative) => indexed.has(relative)) &&
    [...indexed].every(([relative]) => expected.includes(relative)),
  "SHA256SUMS must index every bundle file except itself exactly once");
  for (const relative of expected) {
    const bytes = await readBytes(relative);
    check(indexed.get(relative) === sha256(bytes),
      `SHA256SUMS digest does not match ${relative}`);
  }
  if (placeholders.length === 0) {
    check(manifest.integrity.status === "complete",
      "placeholder-free checksum index must be marked complete");
  }
}

for (const message of notes) {
  process.stdout.write(`note: ${message}\n`);
}
if (failures.length > 0) {
  for (const failure of failures) {
    process.stderr.write(`error: ${failure}\n`);
  }
  process.exit(1);
}

process.stdout.write(
  `verified SPL v0.2 evidence for ${TARGET_REVISION}` +
  (placeholders.length > 0 ? ` with ${distinctPlaceholders.length} pending placeholder(s)` :
    " with no placeholders") + "\n",
);

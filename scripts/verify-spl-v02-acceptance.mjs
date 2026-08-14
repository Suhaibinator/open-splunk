#!/usr/bin/env node

/* eslint-disable no-await-in-loop, unicorn/prefer-set-has */
// Verification is deliberately sequential so every authority is checked before reuse.

import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { lstatSync, readFileSync } from "node:fs";
import { lstat, readFile, readdir } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";
import { parseStrictJSON } from "./strict-json.mjs";

const EXPECTED_JOBS = [
  "Backend vertical",
  "Frontend checks",
  "Go lint",
  "Go tests (atomic coverage)",
  "Go tests (race detector, knowledge catalog shard 1/4)",
  "Go tests (race detector, knowledge catalog shard 2/4)",
  "Go tests (race detector, knowledge catalog shard 3/4)",
  "Go tests (race detector, knowledge catalog shard 4/4)",
  "Go tests (race detector, non-catalog packages shard 1/4)",
  "Go tests (race detector, non-catalog packages shard 2/4)",
  "Go tests (race detector, non-catalog packages shard 3/4)",
  "Go tests (race detector, non-catalog packages shard 4/4)",
  "GradeThis compatibility corpus",
  "HEC durable (observational 50 requests/s small-only correctness/backpressure saturation and outage recovery)",
  "HEC durable (rate-gated 1,000 EPS batch-only throughput and outage recovery)",
  "Knowledge object fuzz (catalog)",
  "Knowledge object fuzz (definitions)",
  "Knowledge object fuzz (hec-protocol)",
  "Knowledge object fuzz (selectors)",
  "Knowledge object fuzz (spl-parser)",
  "Knowledge runtime ClickHouse matrix",
  "Linux/macOS release asset consistency",
  "Production binaries (linux-amd64)",
  "Production binaries (macos)",
  "Protobuf contracts",
  "Release OCI contract",
  "SPL compatibility pinned ClickHouse verticals",
  "Go vulnerability scan",
];
const EXPECTED_ARTIFACTS = new Map([
  ["public-readme", "README.md"],
  ["deploy-env-generator", "deploy/generate-env.sh"],
  ["deploy-guide", "deploy/README.md"],
  ["collector-deployment-guide", "docs/collector-deployment.md"],
  ["integration-guide", "integration/README.md"],
  ["contract", "docs/spl-compatibility-v0.2.md"],
  ["corpus", "internal/spl/testdata/compatibility-v0.2.json"],
  ["migration", "docs/spl-compatibility-v0.2-migration.md"],
  ["benchmark", "docs/spl-compatibility-v0.2-benchmark.md"],
  ["manifest-schema", "docs/evidence/spl-v0.2-activation/manifest.schema.json"],
  ["acceptance-verifier", "scripts/verify-spl-v02-acceptance.mjs"],
  ["acceptance-verifier-tests", "scripts/spl-v02-acceptance.test.mjs"],
  ["ci-structural-tests", "scripts/spl-v02-stats-by-ci.test.mjs"],
  ["strict-json-parser", "scripts/strict-json.mjs"],
  ["ci-workflow", ".github/workflows/ci.yml"],
  ["publication-workflow", ".github/workflows/publish-images.yml"],
  ["release-makefile", "Makefile"],
  ["release-launcher", "scripts/build-release.sh"],
  ["oci-launcher", "scripts/build-oci.sh"],
  ["identity-reader", "scripts/read-spl-compatibility-version.mjs"],
  ["identity-reader-tests", "scripts/read-spl-compatibility-version.test.mjs"],
  ["runtime-identity", "internal/spl/doc.go"],
]);
const EXPECTED_ARTIFACT_MODES = new Map([
  ["deploy/generate-env.sh", "100755"],
  ["scripts/build-release.sh", "100755"],
  ["scripts/build-oci.sh", "100755"],
]);
const REQUIRED_RECEIPTS = new Map([
  ["ci-jobs", "receipts/ci-jobs.json"],
  ["ci-run", "receipts/ci-run.json"],
  ["clickhouse-gates", "receipts/clickhouse-gates.json"],
  ["compatibility-audit", "receipts/compatibility-audit.json"],
  ["quality-gates", "receipts/quality-gates.json"],
  ["release-artifacts", "receipts/release-artifacts.json"],
  ["release-identity", "receipts/release-identity.json"],
  ["source-identity", "receipts/source-identity.json"],
]);
const EVIDENCE_ALLOWLIST = [
  "docs/spl-compatibility-v0.2-acceptance.md",
  "docs/evidence/spl-v0.2-activation/manifest.json",
];
const PHASES = new Set([
  "implementation-checkpoint",
  "qualification-candidate",
  "accepted",
]);
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const OBJECT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const RECEIPT_PATH_PATTERN = /^receipts\/[A-Za-z0-9._/-]+$/;
const TIMESTAMP_PATTERN = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$/;
const V02_APPLICATION_VERSION = "0.1.0";

const scriptPath = fileURLToPath(import.meta.url);
const repositoryRoot = path.resolve(path.dirname(scriptPath), "..");
const bundleRoot = path.join(repositoryRoot, "docs/evidence/spl-v0.2-activation");
const manifestRelative = "docs/evidence/spl-v0.2-activation/manifest.json";
const reportRelative = "docs/spl-compatibility-v0.2-acceptance.md";
const failures = [];

function fail(message) {
  failures.push(message);
}

function check(condition, message) {
  if (!condition) fail(message);
}

function usage() {
  return "usage: node scripts/verify-spl-v02-acceptance.mjs " +
    "--phase <implementation-checkpoint|qualification-candidate|accepted> " +
    "[--allow-dirty] [--publication] [--print-runtime-revision] " +
    "[--evidence-revision <E> --print-evidence-binding]\n";
}

function parseArguments(argumentsList) {
  let phase = null;
  let allowDirty = false;
  let publication = false;
  let printRuntimeRevision = false;
  let evidenceRevision = null;
  let printEvidenceBinding = false;
  for (let index = 0; index < argumentsList.length; index++) {
    const argument = argumentsList[index];
    if (argument === "--phase" && index + 1 < argumentsList.length) {
      phase = argumentsList[++index];
    } else if (argument === "--allow-dirty") {
      allowDirty = true;
    } else if (argument === "--publication") {
      publication = true;
    } else if (argument === "--print-runtime-revision") {
      printRuntimeRevision = true;
    } else if (argument === "--evidence-revision" && index + 1 < argumentsList.length) {
      evidenceRevision = argumentsList[++index];
    } else if (argument === "--print-evidence-binding") {
      printEvidenceBinding = true;
    } else {
      process.stderr.write(usage());
      process.exit(2);
    }
  }
  if (!PHASES.has(phase) || (allowDirty && phase !== "implementation-checkpoint") ||
      (publication && phase !== "accepted") ||
      (printRuntimeRevision && !publication) ||
      (evidenceRevision !== null &&
        (phase !== "accepted" || publication || allowDirty || printRuntimeRevision ||
          !OBJECT_PATTERN.test(evidenceRevision))) ||
      (printEvidenceBinding && evidenceRevision === null)) {
    process.stderr.write(usage());
    process.exit(2);
  }
  return {
    phase,
    allowDirty,
    publication,
    printRuntimeRevision,
    evidenceRevision,
    printEvidenceBinding,
  };
}

const options = parseArguments(process.argv.slice(2));

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function parseJSONText(text, label) {
  try {
    return parseStrictJSON(text, label);
  } catch (error) {
    fail(`${label} is not valid JSON: ${error.message}`);
    return null;
  }
}

async function parseJSONFile(absolute, label) {
  try {
    return parseJSONText(await readFile(absolute, "utf8"), label);
  } catch (error) {
    fail(`${label} cannot be read: ${error.message}`);
    return null;
  }
}

function git(argumentsForGit, encoding = "utf8") {
  const result = spawnSync("git", argumentsForGit, {
    cwd: repositoryRoot,
    encoding,
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_NO_REPLACE_OBJECTS: "1",
      GIT_OPTIONAL_LOCKS: "0",
      GIT_TERMINAL_PROMPT: "0",
    },
    maxBuffer: 128 * 1024 * 1024,
  });
  if (result.status !== 0) {
    fail(`git ${argumentsForGit.join(" ")} failed: ${String(result.stderr).trim()}`);
    return null;
  }
  return result.stdout;
}

function gitText(argumentsForGit) {
  const output = git(argumentsForGit);
  return output === null ? null : output.trim();
}

function gitIsAncestor(ancestor, descendant) {
  const result = spawnSync("git", ["merge-base", "--is-ancestor", ancestor, descendant], {
    cwd: repositoryRoot,
    encoding: "utf8",
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_NO_REPLACE_OBJECTS: "1",
      GIT_OPTIONAL_LOCKS: "0",
      GIT_TERMINAL_PROMPT: "0",
    },
  });
  if (result.status !== 0 && result.status !== 1) {
    fail(`git merge-base --is-ancestor failed: ${result.stderr.trim()}`);
  }
  return result.status === 0;
}

function checkSafeRepositoryPath(relative, label) {
  check(typeof relative === "string" && relative.length > 0,
    `${label} must be a nonempty path`);
  if (typeof relative !== "string" || relative.length === 0) return null;
  check(!path.isAbsolute(relative), `${label} must be relative`);
  check(relative === relative.replaceAll("\\", "/"),
    `${label} must use slash separators`);
  const normalized = path.posix.normalize(relative);
  check(normalized === relative && normalized !== ".." &&
    !normalized.startsWith("../"), `${label} must be a clean repository path`);
  const absolute = path.resolve(repositoryRoot, relative);
  check(absolute.startsWith(`${repositoryRoot}${path.sep}`),
    `${label} must remain inside the repository`);
  return absolute;
}

async function checkRegularFile(absolute, label) {
  try {
    const metadata = await lstat(absolute);
    check(metadata.isFile(), `${label} must be a regular file`);
    check(!metadata.isSymbolicLink(), `${label} must not be a symbolic link`);
    return metadata.isFile() && !metadata.isSymbolicLink();
  } catch (error) {
    fail(`${label} cannot be inspected: ${error.message}`);
    return false;
  }
}

async function listFiles(directory, prefix = "") {
  let entries;
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (error) {
    fail(`cannot enumerate ${directory}: ${error.message}`);
    return [];
  }
  const files = [];
  for (const entry of entries.toSorted((left, right) =>
    left.name.localeCompare(right.name, "en"))) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = path.join(directory, entry.name);
    check(!entry.isSymbolicLink(), `${relative} must not be a symbolic link`);
    if (entry.isDirectory()) {
      files.push(...await listFiles(absolute, relative));
    } else {
      check(entry.isFile(), `${relative} must be a regular file`);
      files.push(relative);
    }
  }
  return files;
}

async function verifyGitHistoryIntegrity(requireCompleteHistory = false) {
  const commonDirectory = gitText([
    "rev-parse", "--path-format=absolute", "--git-common-dir",
  ]);
  check(typeof commonDirectory === "string" && path.isAbsolute(commonDirectory),
    "Git common directory must resolve to an absolute path");
  if (typeof commonDirectory !== "string") return;
  const graftsPath = path.join(commonDirectory, "info", "grafts");
  try {
    const metadata = await lstat(graftsPath);
    check(metadata.isFile() && !metadata.isSymbolicLink(),
      "Git graft authority must not be a symlink or special file");
    check((await readFile(graftsPath)).length === 0,
      "legacy Git grafts can forge R/E topology and must be absent");
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  if (requireCompleteHistory) {
    check(gitText(["rev-parse", "--is-shallow-repository"]) === "false",
      "accepted exact-E verification requires complete non-shallow Git history");
  }
}

function collectSchemaErrors(schema, value, location = "manifest") {
  const errors = [];
  function visit(node, candidate, current, root) {
    if (node.$ref) {
      const segments = node.$ref.replace(/^#\//, "").split("/");
      let target = root;
      for (const segment of segments) target = target?.[segment];
      if (!target) errors.push(`${current} has an unresolved schema reference`);
      else visit(target, candidate, current, root);
      return;
    }
    if (node.anyOf) {
      const branches = node.anyOf.map((branch) => {
        const prior = errors.length;
        visit(branch, candidate, current, root);
        const branchErrors = errors.splice(prior);
        return branchErrors;
      });
      if (!branches.some((branch) => branch.length === 0)) {
        errors.push(`${current} does not match any allowed schema shape`);
      }
      return;
    }
    if (Object.hasOwn(node, "const") && candidate !== node.const) {
      errors.push(`${current} must equal ${JSON.stringify(node.const)}`);
      return;
    }
    if (node.enum && !node.enum.some((entry) => entry === candidate)) {
      errors.push(`${current} has an unsupported value`);
      return;
    }
    const types = Array.isArray(node.type) ? node.type : node.type ? [node.type] : [];
    if (types.length > 0) {
      const matchesType = types.some((type) => {
        if (type === "null") return candidate === null;
        if (type === "array") return Array.isArray(candidate);
        if (type === "object") return candidate !== null &&
          typeof candidate === "object" && !Array.isArray(candidate);
        if (type === "integer") return Number.isSafeInteger(candidate);
        return typeof candidate === type;
      });
      if (!matchesType) {
        errors.push(`${current} has the wrong JSON type`);
        return;
      }
    }
    if (typeof candidate === "string") {
      if (Object.hasOwn(node, "minLength") && candidate.length < node.minLength) {
        errors.push(`${current} is too short`);
      }
      if (Object.hasOwn(node, "maxLength") && candidate.length > node.maxLength) {
        errors.push(`${current} is too long`);
      }
      if (node.pattern && !new RegExp(node.pattern, "u").test(candidate)) {
        errors.push(`${current} does not match its schema pattern`);
      }
    }
    if (Number.isSafeInteger(candidate) && Object.hasOwn(node, "minimum") &&
        candidate < node.minimum) {
      errors.push(`${current} is below its schema minimum`);
    }
    if (Array.isArray(candidate)) {
      if (Object.hasOwn(node, "minItems") && candidate.length < node.minItems) {
        errors.push(`${current} has too few items`);
      }
      if (Object.hasOwn(node, "maxItems") && candidate.length > node.maxItems) {
        errors.push(`${current} has too many items`);
      }
      if (node.items) {
        candidate.forEach((entry, index) =>
          visit(node.items, entry, `${current}[${index}]`, root));
      }
    } else if (candidate && typeof candidate === "object") {
      for (const required of node.required ?? []) {
        if (!Object.hasOwn(candidate, required)) {
          errors.push(`${current}.${required} is required`);
        }
      }
      if (node.additionalProperties === false) {
        for (const key of Object.keys(candidate)) {
          if (!Object.hasOwn(node.properties ?? {}, key)) {
            errors.push(`${current}.${key} is not allowed`);
          }
        }
      }
      for (const [key, property] of Object.entries(node.properties ?? {})) {
        if (Object.hasOwn(candidate, key)) {
          visit(property, candidate[key], `${current}.${key}`, root);
        }
      }
    }
  }
  visit(schema, value, location, schema);
  return errors;
}

function parseReportPhase(text) {
  const matches = [...text.matchAll(/^\*\*Evidence phase:\*\* `([^`]+)`$/gm)];
  check(matches.length === 1,
    "acceptance report must contain exactly one machine-readable Evidence phase line");
  return matches.length === 1 ? matches[0][1] : null;
}

function checkReportState(text, phase) {
  const expected = phase === "accepted" ? {
    status: "accepted",
    decision: "accepted",
    publication: "yes",
  } : {
    status: "pending",
    decision: "pending",
    publication: "no",
  };
  const fields = [
    ["Status", expected.status],
    ["Evidence phase", phase],
    ["Decision", expected.decision],
    ["Stable publication authorized", expected.publication],
    ["Target authored-search identity", "0.2"],
    ["Knowledge-expression identity", "0.1"],
  ];
  for (const [name, value] of fields) {
    const escapedName = name.replaceAll(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const matches = [...text.matchAll(new RegExp(
      `^\\*\\*${escapedName}:\\*\\* (?:\`${value.replaceAll(".", "\\.")}\`|${value})$`,
      "gm",
    ))];
    check(matches.length === 1,
      `acceptance report must contain exactly one ${name} field equal to ${value}`);
  }
}

function showBytes(revision, relative) {
  if (revision === "WORKTREE") {
    try {
      const absolute = path.join(repositoryRoot, relative);
      let current = repositoryRoot;
      for (const segment of relative.split("/")) {
        current = path.join(current, segment);
        const component = lstatSync(current);
        check(!component.isSymbolicLink(),
          `worktree path ${relative} must not traverse a symbolic link`);
      }
      const metadata = lstatSync(absolute);
      check(metadata.isFile() && !metadata.isSymbolicLink(),
        `worktree path ${relative} must be a regular non-symlink file`);
      return metadata.isFile() && !metadata.isSymbolicLink()
        ? readFileSync(absolute)
        : null;
    } catch {
      fail(`worktree path ${relative} cannot be read`);
      return null;
    }
  }
  const output = git(["show", `${revision}:${relative}`], null);
  if (output === null) return null;
  return Buffer.from(output);
}

function checkRegularGitBlob(revision, relative, label) {
  if (revision === "WORKTREE") {
    try {
      const metadata = lstatSync(path.join(repositoryRoot, relative));
      check(metadata.isFile() && !metadata.isSymbolicLink(),
        `${label} must be a regular non-symlink file in the worktree`);
    } catch (error) {
      fail(`${label} cannot be inspected in the worktree: ${error.message}`);
    }
    return;
  }
  const output = gitText(["ls-tree", revision, "--", relative]);
  const match = output?.match(/^([0-9]{6}) blob ([0-9a-f]+)\t([^\n]+)$/u);
  check(match !== null && match !== undefined && match[3] === relative,
    `${label} must resolve to one exact Git blob at ${revision}`);
  if (!match) return;
  check(match[1] === "100644" || match[1] === "100755",
    `${label} must be a regular Git blob, not a symlink or special entry`);
}

function checkExpectedGitMode(revision, relative, expected, label) {
  if (revision === "WORKTREE") {
    try {
      let current = repositoryRoot;
      for (const segment of relative.split("/")) {
        current = path.join(current, segment);
        const component = lstatSync(current);
        check(!component.isSymbolicLink(),
          `${label} path must not traverse a symbolic link`);
      }
      const metadata = lstatSync(path.join(repositoryRoot, relative));
      const executable = (metadata.mode & 0o111) !== 0;
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        executable === (expected === "100755"),
      `${label} must have exact mode ${expected} in the worktree`);
    } catch (error) {
      fail(`${label} mode cannot be inspected in the worktree: ${error.message}`);
    }
    return;
  }
  const output = gitText(["ls-tree", revision, "--", relative]);
  const match = output?.match(/^([0-9]{6}) blob [0-9a-f]+\t([^\n]+)$/u);
  check(match?.[1] === expected && match?.[2] === relative,
    `${label} must have exact Git mode ${expected} at ${revision}`);
}

function commitParents(revision) {
  const output = gitText(["show", "-s", "--format=%P", revision]);
  return output === null || output === "" ? [] : output.split(" ");
}

function checkCommitObject(revision, label) {
  check(OBJECT_PATTERN.test(revision ?? ""), `${label} is not a canonical object ID`);
  if (!OBJECT_PATTERN.test(revision ?? "")) return false;
  check(gitText(["cat-file", "-t", revision]) === "commit",
    `${label} must identify a commit object`);
  check(gitText(["rev-parse", `${revision}^{commit}`]) === revision,
    `${label} must be the canonical commit ID`);
  return true;
}

function changedEntries(from, to) {
  const output = git(["diff", "--raw", "--no-renames", "-z", from, to], null);
  if (output === null || output.length === 0) return [];
  const fields = output.toString("utf8").split("\0").filter(Boolean);
  const entries = [];
  for (let index = 0; index < fields.length; index += 2) {
    const header = fields[index];
    const relative = fields[index + 1];
    const match = header.match(/^:([0-7]{6}) ([0-7]{6}) ([0-9a-f]+) ([0-9a-f]+) ([A-Z])$/u);
    if (!match || relative === undefined) {
      fail("E diff contains an unparseable raw Git entry");
      continue;
    }
    entries.push({
      oldMode: match[1],
      newMode: match[2],
      status: match[5],
      path: relative,
    });
  }
  return entries;
}

function isAllowedEvidencePath(relative) {
  return EVIDENCE_ALLOWLIST.includes(relative) ||
    /^docs\/evidence\/spl-v0\.2-activation\/receipts\/[A-Za-z0-9._/-]+$/.test(relative);
}

function readCompatibilityAt(revision) {
  const bytes = showBytes(revision, "internal/spl/doc.go");
  if (bytes === null) return null;
  const matches = [...bytes.toString("utf8").matchAll(
    /^const CompatibilityVersion = "([0-9]+\.[0-9]+)"$/gm,
  )];
  check(matches.length === 1,
    `internal/spl/doc.go at ${revision} must declare one compatibility identity`);
  return matches.length === 1 ? matches[0][1] : null;
}

function shellBlocks(source, label) {
  const blocks = [...source.matchAll(
    /^```(?:sh|bash|shell)\r?\n([\s\S]*?)^```[ \\t]*\r?$/gm,
  )].map((match) => match[1]);
  check(blocks.length > 0, `${label} must contain an explicit shell code fence`);
  return blocks;
}

function requireCanonicalShellBlock(source, block, count, label) {
  const blocks = shellBlocks(source, label);
  check(blocks.filter((candidate) => candidate === `${block}\n`).length === count,
    `${label} must contain exactly ${count} canonical release command block${count === 1 ? "" : "s"}`);
}

function requireAuthorityOccurrences(source, expected, label) {
  const code = shellBlocks(source, label).join("\n");
  for (const [name, count] of Object.entries(expected)) {
    check(code.split(name).length - 1 === count,
      `${label} contains an additional or malformed ${name} authority`);
  }
}

function requireParameterDefault(source, name, expected, label) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const assignmentHeads = [...source.matchAll(new RegExp(
    `^application_version=(.*)$`,
    "gm",
  ))];
  check(assignmentHeads.length === 1,
    `${label} must contain exactly one executable application_version assignment`);
  if (assignmentHeads.length !== 1) return;
  const matches = [...assignmentHeads[0][1].matchAll(new RegExp(
    `^\\$\\{${escaped}:-([^}]*)\\}$`,
    "g",
  ))];
  const values = matches.map((match) => {
    const raw = match[1];
    if ((raw.startsWith('"') && raw.endsWith('"')) ||
        (raw.startsWith("'") && raw.endsWith("'"))) {
      return raw.slice(1, -1);
    }
    return raw;
  });
  check(values.length === 1 && values[0] === expected,
    `${label} must default ${name} to ${expected} exactly once`);
}

function requireReleasePair(source, applicationVersion, compatibilityVersion, label,
  commandPattern) {
  const pattern = new RegExp(
    `^OPEN_SPLUNK_APPLICATION_VERSION=${applicationVersion.replaceAll(".", "\\.")} \\\\r?\\n` +
    `OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=${compatibilityVersion.replaceAll(".", "\\.")} \\\\r?\\n` +
    commandPattern,
    "gm",
  );
  const code = shellBlocks(source, label).join("\n");
  check([...code.matchAll(pattern)].length === 1,
    `${label} must contain one adjacent application ${applicationVersion}/SPL ${compatibilityVersion} command pair`);
  requireAuthorityOccurrences(source, {
    OPEN_SPLUNK_APPLICATION_VERSION: 1,
    OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: 1,
  }, label);
}

function checkPublicREADMEAt(revision) {
  checkExpectedGitMode(revision, "README.md", "100644", "public README");
  const bytes = showBytes(revision, "README.md");
  if (bytes === null) return;
  const source = bytes.toString("utf8");
  for (const suffix of [".md", "-migration.md", "-acceptance.md"]) {
    check(source.includes(`(docs/spl-compatibility-v0.2${suffix})`),
      `README at ${revision} must link the public v0.2${suffix} authority`);
    check(!source.includes(`(docs/spl-compatibility-v0.3${suffix})`),
      `README at ${revision} must not advertise v0.3 before its own accepted activation`);
  }
  requireReleasePair(source, "0.1.0", "0.2", `README at ${revision}`,
    "OPEN_SPLUNK_SOURCE_REVISION=.*make[ \\t]+release[ \\t]*$");
  check(sha256(bytes) === "42b175ea79965ddb2bd9a7546480a3906f9b1f811cfb4da96c8145cbec2c28db",
    `README at ${revision} does not equal the exact v0.2 release authority`);
}

function checkOperatorReleaseIdentityAt(revision) {
  const sources = {};
  for (const [key, relative, mode] of [
    ["deployGenerator", "deploy/generate-env.sh", "100755"],
    ["deployGuide", "deploy/README.md", "100644"],
    ["collectorGuide", "docs/collector-deployment.md", "100644"],
    ["integrationGuide", "integration/README.md", "100644"],
  ]) {
    checkExpectedGitMode(revision, relative, mode, key);
    const bytes = showBytes(revision, relative);
    if (bytes === null) return;
    sources[key] = bytes.toString("utf8");
  }
  requireParameterDefault(sources.deployGenerator,
    "OPEN_SPLUNK_APPLICATION_VERSION", "0.1.0", `deployment generator at ${revision}`);
  requireCanonicalShellBlock(sources.deployGuide, [
    "cd ..", "git status --short",
    "export OPEN_SPLUNK_APPLICATION_VERSION=0.1.0",
    "export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2",
    'export OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)"', "make oci",
  ].join("\n"), 1, `deployment guide at ${revision}`);
  requireCanonicalShellBlock(sources.deployGuide, [
    "cd deploy", "OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 ./generate-env.sh",
    "docker compose up --detach --wait",
  ].join("\n"), 1, `deployment guide at ${revision}`);
  requireAuthorityOccurrences(sources.deployGuide, {
    OPEN_SPLUNK_APPLICATION_VERSION: 2,
    OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: 1,
  }, `deployment guide at ${revision}`);
  requireCanonicalShellBlock(sources.collectorGuide, [
    "export OPEN_SPLUNK_COLLECTOR_VERSION=0.1.0",
    'export OPEN_SPLUNK_COLLECTOR_IMAGE="ghcr.io/suhaibinator/open-splunk-collector:${OPEN_SPLUNK_COLLECTOR_VERSION}"',
    'docker pull "$OPEN_SPLUNK_COLLECTOR_IMAGE"',
    "docker image inspect --format '{{.Os}}/{{.Architecture}}' \"$OPEN_SPLUNK_COLLECTOR_IMAGE\"",
  ].join("\n"), 1, `collector deployment guide at ${revision}`);
  requireAuthorityOccurrences(sources.collectorGuide, {
    "OPEN_SPLUNK_COLLECTOR_VERSION=": 1,
    "OPEN_SPLUNK_COLLECTOR_IMAGE=": 1,
  }, `collector deployment guide at ${revision}`);
  requireReleasePair(sources.integrationGuide, "0.1.0", "0.2",
    `integration guide at ${revision}`,
    "OPEN_SPLUNK_OCI_INTEGRATION=1 \\\\r?\\n" +
    "OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26\\.3\\.17\\.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \\\\r?\\n" +
    "  go test \\.\\/integration -run '\\^TestReleaseOCIComposeContract\\$' \\\\r?\\n" +
    "    -count=1 -timeout=25m -v$");
  for (const [id, source, digest] of [
    ["deployGenerator", sources.deployGenerator, "5f6c3a95aa720acb7769431b3b54930c9deb7f124c2d0a09c6966a72a708828b"],
    ["deployGuide", sources.deployGuide, "b94a4361edc5e8f04e2482c5722675e11cb803aecbc240c0cfacccf61e19eaf1"],
    ["collectorGuide", sources.collectorGuide, "e4b38c4539cfe4be60e538fbe408bc9fce0d7e35e464e520da058784fa9324c0"],
    ["integrationGuide", sources.integrationGuide, "f297f0936822ce53b05c8b2ff84ada8a4fa363974cc0570dd21b1880342ee569"],
  ]) {
    check(sha256(Buffer.from(source, "utf8")) === digest,
      `${id} at ${revision} does not equal the exact v0.2 release authority`);
  }
}

function checkStableArtifacts(manifest, revision) {
  const ids = new Set();
  const paths = new Set();
  for (const artifact of manifest.artifacts ?? []) {
    check(!ids.has(artifact.id), `artifact id ${artifact.id} is duplicated`);
    check(!paths.has(artifact.path), `artifact path ${artifact.path} is duplicated`);
    ids.add(artifact.id);
    paths.add(artifact.path);
    const absolute = checkSafeRepositoryPath(artifact.path, `artifact ${artifact.id}`);
    check(HASH_PATTERN.test(artifact.sha256 ?? ""),
      `artifact ${artifact.id} must have an exact SHA-256 in ${manifest.phase}`);
    checkRegularGitBlob(revision, artifact.path, `artifact ${artifact.id}`);
    checkExpectedGitMode(
      revision,
      artifact.path,
      EXPECTED_ARTIFACT_MODES.get(artifact.path) ?? "100644",
      `artifact ${artifact.id}`,
    );
    const bytes = revision === "WORKTREE" ? null : showBytes(revision, artifact.path);
    if (revision !== "WORKTREE" && bytes !== null) {
      check(sha256(bytes) === artifact.sha256,
        `artifact ${artifact.id} hash does not match runtime revision`);
    } else if (revision === "WORKTREE" && absolute) {
      // The checkpoint intentionally permits null hashes; this branch is used
      // only when a caller passes a fully bound checkpoint fixture.
    }
  }
  const exact = [...EXPECTED_ARTIFACTS.entries()]
    .every(([id, expectedPath]) =>
      ids.has(id) && (manifest.artifacts ?? []).some((artifact) =>
        artifact.id === id && artifact.path === expectedPath));
  check(ids.size === EXPECTED_ARTIFACTS.size &&
    paths.size === EXPECTED_ARTIFACTS.size && exact,
  "artifact inventory must equal the exact required ID/path set");
}

function checkCheckpointArtifactInventory(manifest) {
  const supplied = new Map((manifest.artifacts ?? [])
    .map((artifact) => [artifact.id, artifact.path]));
  check(supplied.size === EXPECTED_ARTIFACTS.size &&
    [...EXPECTED_ARTIFACTS].every(([id, relative]) => supplied.get(id) === relative),
  "checkpoint artifact inventory must equal the exact required ID/path set");
}

function checkUnboundQualificationEvidence(manifest, label) {
  const ci = manifest.ci;
  check(ci?.repository === null && ci?.workflow === "CI" &&
    ci?.run_id === null && ci?.url === null && ci?.event === null &&
    ci?.head_branch === null && ci?.head_revision === null &&
    ci?.checkout_revision === null && ci?.checkout_tree === null &&
    ci?.release_artifact_source_revision === null && ci?.status === null &&
    ci?.conclusion === null && ci?.completed_at_utc === null &&
    ci?.job_set_complete === false && Array.isArray(ci?.jobs) && ci.jobs.length === 0,
  `${label} must leave every post-commit CI field unbound`);
  const release = manifest.release;
  check(release?.source_revision === null &&
    release?.spl_compatibility_version === null &&
    release?.application_version === null &&
    Array.isArray(release?.binary_identities) &&
    release.binary_identities.length === 0 &&
    Array.isArray(release?.artifact_digests) &&
    release.artifact_digests.length === 0,
  `${label} must leave every post-commit release field unbound`);
}

function validateCheckpoint(manifest, head, reportPhase) {
  check(reportPhase === "implementation-checkpoint",
    "checkpoint report phase is invalid");
  check(manifest.runtime?.revision_binding === "unbound" &&
    manifest.runtime?.revision === null && manifest.runtime?.tree === null,
  "checkpoint runtime identity must remain unbound");
  check(manifest.runtime?.candidate_manifest_sha256 === null &&
    manifest.runtime?.candidate_acceptance_sha256 === null &&
    manifest.runtime?.remote_ref === null &&
    manifest.runtime?.remote_readback_revision === null &&
    manifest.runtime?.remote_readback_at_utc === null,
  "checkpoint must not claim runtime or remote provenance");
  check((manifest.artifacts ?? []).every((artifact) => artifact.sha256 === null),
    "checkpoint artifacts must remain unhashed until R is frozen");
  checkCheckpointArtifactInventory(manifest);
  for (const relative of EXPECTED_ARTIFACTS.values()) {
    checkExpectedGitMode(
      head,
      relative,
      EXPECTED_ARTIFACT_MODES.get(relative) ?? "100644",
      `checkpoint artifact ${relative}`,
    );
  }
  checkExpectedGitMode(head, manifestRelative, "100644", "checkpoint manifest");
  checkExpectedGitMode(head, reportRelative, "100644", "checkpoint acceptance report");
  checkPublicREADMEAt(head);
  checkOperatorReleaseIdentityAt(head);
  checkUnboundQualificationEvidence(manifest, "checkpoint");
  check(manifest.receipts?.length === 0,
    "checkpoint must not contain accepted receipts");
  check(manifest.decision?.status === "pending" &&
    manifest.decision?.stable_publication_authorized === false,
  "checkpoint decision must remain pending and distribution-blocked");
  check(readCompatibilityAt(head) === "0.2",
    "current runtime identity must remain 0.2 at the checkpoint");
}

function validateCandidate(manifest, head, reportPhase) {
  check(reportPhase === "qualification-candidate",
    "candidate report phase is invalid");
  check(manifest.runtime?.revision_binding === "containing-commit",
    "candidate runtime binding must be containing-commit");
  check(manifest.runtime?.revision === null && manifest.runtime?.tree === null &&
    manifest.runtime?.candidate_manifest_sha256 === null &&
    manifest.runtime?.candidate_acceptance_sha256 === null,
  "candidate cannot recursively embed its own revision, tree, or report hashes");
  check(manifest.runtime?.remote_ref === null &&
    manifest.runtime?.remote_readback_revision === null &&
    manifest.runtime?.remote_readback_at_utc === null,
  "candidate cannot claim post-commit remote provenance");
  checkUnboundQualificationEvidence(manifest, "candidate");
  check(manifest.receipts?.length === 0,
    "candidate cannot contain post-commit receipts");
  check(manifest.decision?.status === "pending" &&
    manifest.decision?.stable_publication_authorized === false,
  "candidate decision must remain pending and distribution-blocked");
  check(readCompatibilityAt(head) === "0.2",
    "candidate runtime identity must be exactly 0.2");
  checkRegularGitBlob(head, manifestRelative, "candidate manifest");
  checkRegularGitBlob(head, reportRelative, "candidate acceptance report");
  checkExpectedGitMode(head, manifestRelative, "100644", "candidate manifest");
  checkExpectedGitMode(head, reportRelative, "100644", "candidate acceptance report");
  checkPublicREADMEAt(head);
  checkOperatorReleaseIdentityAt(head);
  checkStableArtifacts(manifest, head);
}

async function validateReceipts(manifest, revision = null) {
  const ids = new Set();
  const paths = new Set();
  const receiptJSON = new Map();
  for (const receipt of manifest.receipts ?? []) {
    check(!ids.has(receipt.id), `receipt id ${receipt.id} is duplicated`);
    check(!paths.has(receipt.path), `receipt path ${receipt.path} is duplicated`);
    ids.add(receipt.id);
    paths.add(receipt.path);
    check(RECEIPT_PATH_PATTERN.test(receipt.path),
      `receipt ${receipt.id} has an unsafe path`);
    const relative = `docs/evidence/spl-v0.2-activation/${receipt.path}`;
    const absolute = checkSafeRepositoryPath(relative, `receipt ${receipt.id}`);
    let bytes;
    if (revision !== null) {
      checkExpectedGitMode(revision, relative, "100644", `receipt ${receipt.id}`);
      bytes = showBytes(revision, relative);
      if (bytes === null) continue;
    } else {
      checkExpectedGitMode("WORKTREE", relative, "100644", `receipt ${receipt.id}`);
      if (!absolute || !await checkRegularFile(absolute, `receipt ${receipt.id}`)) continue;
      bytes = await readFile(absolute);
    }
    check(bytes.length === receipt.bytes,
      `receipt ${receipt.id} byte count does not match`);
    check(sha256(bytes) === receipt.sha256,
      `receipt ${receipt.id} hash does not match`);
    const parsed = parseJSONText(bytes.toString("utf8"), `receipt ${receipt.id}`);
    if (parsed !== null) receiptJSON.set(receipt.id, parsed);
  }
  let presentReceipts;
  if (revision !== null) {
    const output = gitText([
      "ls-tree", "-r", "--name-only", revision,
      "docs/evidence/spl-v0.2-activation/receipts",
    ]);
    presentReceipts = (output ?? "").split("\n").filter(Boolean)
      .map((relative) => relative.replace(
        "docs/evidence/spl-v0.2-activation/",
        "",
      ));
  } else {
    const bundleFiles = await listFiles(bundleRoot);
    presentReceipts = bundleFiles.filter((relative) => relative.startsWith("receipts/"));
  }
  check(presentReceipts.length === paths.size,
    "every receipt file must be indexed exactly once");
  for (const relative of presentReceipts) {
    check(paths.has(relative), `receipt file ${relative} is not indexed`);
  }
  check(ids.size === REQUIRED_RECEIPTS.size &&
    [...REQUIRED_RECEIPTS].every(([id, relative]) =>
      paths.has(relative) && (manifest.receipts ?? []).some((receipt) =>
        receipt.id === id && receipt.path === relative)),
  "accepted receipt IDs and paths must equal the exact required evidence set");

  const runtime = manifest.runtime.revision;
  const runtimeTree = manifest.runtime.tree;
  const ciRun = { ...manifest.ci };
  delete ciRun.jobs;
  const releaseIdentity = { ...manifest.release };
  delete releaseIdentity.artifact_digests;
  const expectedJSON = new Map([
    ["source-identity", {
      runtime_revision: runtime,
      runtime_tree: runtimeTree,
      remote_ref: manifest.runtime.remote_ref,
      remote_readback_revision: manifest.runtime.remote_readback_revision,
      result: "pass",
    }],
    ["quality-gates", {
      runtime_revision: runtime,
      runtime_tree: runtimeTree,
      result: "pass",
    }],
    ["clickhouse-gates", {
      runtime_revision: runtime,
      runtime_tree: runtimeTree,
      image: "clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49",
      result: "pass",
    }],
    ["compatibility-audit", {
      runtime_revision: runtime,
      runtime_tree: runtimeTree,
      compatibility_version: "0.2",
      unresolved_findings: 0,
      result: "pass",
    }],
    ["ci-run", ciRun],
    ["ci-jobs", manifest.ci.jobs],
    ["release-identity", releaseIdentity],
    ["release-artifacts", manifest.release.artifact_digests],
  ]);
  for (const [id, expected] of expectedJSON) {
    check(isDeepStrictEqual(receiptJSON.get(id), expected),
      `receipt ${id} does not exactly reproduce its accepted authority fields`);
  }
}

function checkCI(manifest, runtime) {
  const ci = manifest.ci;
  check(ci?.repository === "Suhaibinator/open-splunk",
    "CI repository must be the canonical repository");
  check(ci?.workflow === "CI" && Number.isSafeInteger(ci?.run_id),
    "accepted evidence must name one CI run");
  check(ci?.url === `https://github.com/Suhaibinator/open-splunk/actions/runs/${ci?.run_id}`,
    "accepted CI URL must be canonical for the recorded run");
  check(ci?.event === "workflow_dispatch" || ci?.event === "push",
    "accepted CI event must be workflow_dispatch or exact-main push");
  if (ci?.event === "push") {
    check(ci?.head_branch === "main",
      "push qualification is allowed only for the exact main branch");
  } else {
    check(typeof ci?.head_branch === "string" && ci.head_branch.length > 0,
      "workflow_dispatch qualification must record its exact head branch or tag");
  }
  check(ci?.head_revision === runtime && ci?.checkout_revision === runtime &&
    ci?.release_artifact_source_revision === runtime,
  "CI head, checkout, and release artifact must all equal R");
  check(ci?.checkout_tree === manifest.runtime?.tree,
    "CI checkout tree must equal the exact tree of R");
  check(ci?.status === "completed" && ci?.conclusion === "success",
    "accepted CI run must be terminal-success");
  check(TIMESTAMP_PATTERN.test(ci?.completed_at_utc ?? ""),
    "accepted CI completion time is invalid");
  check(ci?.job_set_complete === true,
    "accepted evidence must claim a complete CI job set");
  const names = (ci?.jobs ?? []).map((job) => job.name).toSorted();
  const ids = new Set((ci?.jobs ?? []).map((job) => job.id));
  for (const job of ci?.jobs ?? []) {
    check(Object.keys(job).toSorted().join(",") ===
      "conclusion,id,name,status,url",
    `CI job ${job?.name} must use the exact closed field set`);
    check(Number.isSafeInteger(job.id) && job.id > 0 &&
      typeof job.name === "string" && job.name.length > 0 &&
      job.status === "completed" && job.conclusion === "success",
    `CI job ${job?.name} must be an exact terminal-success job`);
    check(job.url ===
      `https://github.com/Suhaibinator/open-splunk/actions/runs/${ci.run_id}/job/${job.id}`,
    `CI job ${job?.name} URL must be canonical for its run and ID`);
  }
  check(ids.size === (ci?.jobs ?? []).length && ids.size === EXPECTED_JOBS.length,
    "accepted CI job IDs must be unique and complete");
  check(JSON.stringify(names) === JSON.stringify(EXPECTED_JOBS.toSorted()),
    "accepted CI jobs must equal the complete expected terminal-success set");
}

function checkRelease(manifest, runtime) {
  const release = manifest.release;
  check(release?.source_revision === runtime,
    "release source revision must equal R");
  check(release?.spl_compatibility_version === "0.2",
    "release server identity must report spl_compatibility_version=0.2");
  check(release?.application_version === V02_APPLICATION_VERSION,
    `release application version must equal ${V02_APPLICATION_VERSION}`);
  const expectedBinaries = [
    "open-splunk-collector",
    "open-splunk-loggen",
    "open-splunk-server",
  ];
  const binaryNames = (release?.binary_identities ?? [])
    .map((identity) => identity.name).toSorted();
  check(JSON.stringify(binaryNames) === JSON.stringify(expectedBinaries),
    "release must record exactly the three production binary identities");
  for (const identity of release?.binary_identities ?? []) {
    check(identity.application_version === release.application_version &&
      identity.source_revision === runtime,
    `binary ${identity.name} identity does not match the release`);
    if (identity.name === "open-splunk-server") {
      check(identity.spl_compatibility_version === "0.2",
        "server binary identity must report SPL 0.2");
    } else {
      check(identity.spl_compatibility_version === null,
        `${identity.name} must not invent an SPL runtime identity`);
    }
  }
  const digests = release?.artifact_digests ?? [];
  const requiredDigestNames = [
    "asset-manifest.json",
    "open-splunk-collector",
    "open-splunk-loggen",
    "open-splunk-server",
    "ui",
  ];
  check(digests.length === requiredDigestNames.length,
    "release must record exactly the production binaries, asset manifest, and UI digests");
  const digestNames = new Set();
  for (const digest of digests) {
    check(!digestNames.has(digest.name),
      `release artifact ${digest.name} is duplicated`);
    digestNames.add(digest.name);
    check(HASH_PATTERN.test(digest.sha256) && Number.isSafeInteger(digest.bytes) &&
      digest.bytes > 0, `release artifact ${digest.name} has invalid identity`);
  }
  for (const required of requiredDigestNames) {
    check(digestNames.has(required), `release artifact ${required} is missing`);
  }
}

function validateAccepted(manifest, head, reportPhase) {
  check(reportPhase === "accepted", "accepted report phase is invalid");
  check(manifest.runtime?.revision_binding === "recorded-runtime-parent",
    "accepted runtime binding must be recorded-runtime-parent");
  const runtime = manifest.runtime?.revision;
  checkCommitObject(runtime, "accepted runtime revision R");
  check(OBJECT_PATTERN.test(manifest.runtime?.tree ?? ""),
    "accepted evidence must record the tree of R");
  check(HASH_PATTERN.test(manifest.runtime?.candidate_manifest_sha256 ?? "") &&
    HASH_PATTERN.test(manifest.runtime?.candidate_acceptance_sha256 ?? ""),
  "accepted evidence must bind the candidate manifest and report hashes");
  check(manifest.runtime?.remote_ref === `refs/tags/spl-v0.2-evidence-${runtime}`,
    "accepted runtime ref must be the immutable full-R v0.2 evidence tag");
  check(manifest.runtime?.remote_readback_revision === runtime,
    "accepted remote readback must equal R");
  check(TIMESTAMP_PATTERN.test(manifest.runtime?.remote_readback_at_utc ?? ""),
    "accepted remote readback time is invalid");
  const parents = commitParents(head);
  check(parents.length === 1 && parents[0] === runtime,
    "E must be a non-merge direct child of R");
  const changes = changedEntries(runtime, head);
  check(changes.length > 0 && changes.every((entry) => isAllowedEvidencePath(entry.path)),
    "E must be documentation-only and confined to acceptance evidence paths");
  check(changes.every((entry) => entry.status !== "D" &&
    entry.status !== "T" && entry.status !== "U" &&
    entry.oldMode !== "160000" && entry.newMode !== "160000" &&
    entry.newMode !== "120000" && entry.newMode === "100644"),
  "E must not delete, rename, type-change, symlink, submodule, or add executable files");
  check(changes.some((entry) => entry.path === manifestRelative) &&
    changes.some((entry) => entry.path === reportRelative),
    "E must update both the manifest and acceptance report");
  if (runtime && OBJECT_PATTERN.test(runtime)) {
    check(gitText(["rev-parse", `${runtime}^{tree}`]) === manifest.runtime.tree,
      "recorded runtime tree does not equal the Git tree of R");
    const candidateManifest = showBytes(runtime, manifestRelative);
    const candidateReport = showBytes(runtime, reportRelative);
    if (candidateManifest !== null) {
      check(sha256(candidateManifest) === manifest.runtime.candidate_manifest_sha256,
        "candidate manifest hash does not match R");
      const parsedCandidate = parseJSONText(candidateManifest.toString("utf8"),
        "candidate manifest at R");
      if (parsedCandidate) {
        const candidateSchemaBytes = showBytes(
          runtime,
          "docs/evidence/spl-v0.2-activation/manifest.schema.json",
        );
        const candidateSchema = candidateSchemaBytes === null ? null :
          parseJSONText(candidateSchemaBytes.toString("utf8"), "candidate schema at R");
        if (candidateSchema) {
          for (const error of collectSchemaErrors(candidateSchema, parsedCandidate,
            "candidate manifest")) fail(error);
        }
        const candidateReportPhase = candidateReport === null ? null :
          parseReportPhase(candidateReport.toString("utf8"));
        validateCandidate(parsedCandidate, runtime, candidateReportPhase);
        check(JSON.stringify(parsedCandidate.artifacts) ===
          JSON.stringify(manifest.artifacts),
        "E must preserve the exact stable artifact inventory and hashes from R");
      }
    }
    if (candidateReport !== null) {
      check(sha256(candidateReport) === manifest.runtime.candidate_acceptance_sha256,
        "candidate report hash does not match R");
      check(parseReportPhase(candidateReport.toString("utf8")) ===
        "qualification-candidate", "R report must be a qualification candidate");
      checkReportState(candidateReport.toString("utf8"), "qualification-candidate");
    }
    check(readCompatibilityAt(runtime) === "0.2",
      "R runtime identity must be exactly 0.2");
    checkPublicREADMEAt(runtime);
    checkOperatorReleaseIdentityAt(runtime);
    checkStableArtifacts(manifest, runtime);
  }
  check(manifest.decision?.status === "accepted" &&
    manifest.decision?.stable_publication_authorized === true,
  "accepted decision must explicitly authorize stable publication");
  checkCI(manifest, runtime);
  checkRelease(manifest, runtime);
}

async function githubJSON(endpoint) {
  const token = process.env.GITHUB_TOKEN;
  const repository = process.env.GITHUB_REPOSITORY;
  check(typeof token === "string" && token.length > 0,
    "publication verification requires GITHUB_TOKEN");
  check(repository === "Suhaibinator/open-splunk",
    "publication verification requires the canonical GITHUB_REPOSITORY");
  if (!token || repository !== "Suhaibinator/open-splunk") return null;
  const url = `https://api.github.com/repos/${repository}${endpoint}`;
  const result = spawnSync("curl", [
    "--fail-with-body", "--silent", "--show-error",
    "--location", "--proto", "=https", "--tlsv1.2",
    "--header", "Accept: application/vnd.github+json",
    "--header", `Authorization: Bearer ${token}`,
    "--header", "X-GitHub-Api-Version: 2022-11-28",
    url,
  ], { encoding: "utf8", maxBuffer: 128 * 1024 * 1024 });
  if (result.status !== 0) {
    fail(`GitHub readback failed for ${endpoint}: ${result.stderr.trim()}`);
    return null;
  }
  return parseJSONText(result.stdout, `GitHub response ${endpoint}`);
}

function remoteRefCommit(remoteName, reference) {
  const output = gitText([
    "ls-remote",
    remoteName,
    reference,
    `${reference}^{}`,
  ]);
  if (output === null) return null;
  const lines = output.split("\n").filter(Boolean);
  check(lines.length >= 1 && lines.length <= 2,
    `remote reference ${reference} returned an ambiguous object set`);
  const parsed = lines.map((line) => line.split(/\s+/));
  check(parsed.every((fields) => fields.length === 2 &&
    (fields[1] === reference || fields[1] === `${reference}^{}`)),
  `remote reference ${reference} returned an unexpected name`);
  const peeled = parsed.find((fields) => fields[1] === `${reference}^{}`);
  const direct = parsed.find((fields) => fields[1] === reference);
  return (peeled ?? direct)?.[0] ?? null;
}

async function validatePublication(manifest, head) {
  const runtime = manifest.runtime.revision;
  check(remoteRefCommit("origin", manifest.runtime.remote_ref) === runtime,
    "immutable runtime tag remote readback does not equal R");
  const expectedReleaseTag = `v${manifest.release.application_version}`;
  const exactReleaseTags = (gitText(["tag", "--points-at", head]) ?? "")
    .split("\n").filter((tag) => tag.length > 0 && tag.startsWith("v"));
  check(exactReleaseTags.length === 1 && exactReleaseTags[0] === expectedReleaseTag,
    "accepted evidence commit E must have exactly its application-version release tag");
  const releaseRef = `refs/tags/${expectedReleaseTag}`;
  check(remoteRefCommit("origin", releaseRef) === head,
    "release tag remote readback does not equal E");
  if (process.env.GITHUB_SHA) {
    check(process.env.GITHUB_SHA === head,
      "publication checkout HEAD must equal GITHUB_SHA for E");
  }
  if (process.env.GITHUB_REF_NAME) {
    check(process.env.GITHUB_REF_NAME === expectedReleaseTag,
      "publication release tag must equal the manifest application version");
  }

  const run = await githubJSON(`/actions/runs/${manifest.ci.run_id}`);
  if (run) {
    check(run.name === manifest.ci.workflow && run.event === manifest.ci.event &&
      run.head_branch === manifest.ci.head_branch,
      "live CI workflow or event does not match the manifest");
    check(run.head_sha === runtime && run.status === "completed" &&
      run.conclusion === "success" && run.updated_at === manifest.ci.completed_at_utc,
    "live CI run is not the exact terminal-success R run");
    check(run.html_url === manifest.ci.url,
      "live CI URL does not match the manifest");
  }
  const jobsResponse = await githubJSON(
    `/actions/runs/${manifest.ci.run_id}/jobs?filter=latest&per_page=100`,
  );
  if (jobsResponse) {
    check(jobsResponse.total_count === EXPECTED_JOBS.length &&
      jobsResponse.jobs?.length === EXPECTED_JOBS.length,
    "live CI job set is incomplete, duplicated, or paginated beyond the manifest gate");
    const liveJobs = (jobsResponse.jobs ?? []).map((job) => ({
      id: job.id,
      name: job.name,
      url: job.html_url,
      status: job.status,
      conclusion: job.conclusion,
    })).toSorted((left, right) => left.name.localeCompare(right.name, "en"));
    const recordedJobs = [...manifest.ci.jobs]
      .toSorted((left, right) => left.name.localeCompare(right.name, "en"));
    check(JSON.stringify(liveJobs) === JSON.stringify(recordedJobs),
      "live CI job readback does not exactly match the recorded job set");
  }
}

const evidenceRevision = options.evidenceRevision;
await verifyGitHistoryIntegrity(evidenceRevision !== null);
let manifest;
let schema;
let reportText;
const currentHead = gitText(["rev-parse", "HEAD"]);
const authorityRevision = evidenceRevision ??
  (options.phase === "implementation-checkpoint" ? null : currentHead);
if (authorityRevision !== null) {
  checkCommitObject(authorityRevision,
    evidenceRevision === null ? "current phase revision" : "accepted evidence revision E");
  const manifestBytes = showBytes(authorityRevision, manifestRelative);
  const schemaBytes = showBytes(
    authorityRevision,
    "docs/evidence/spl-v0.2-activation/manifest.schema.json",
  );
  const reportBytes = showBytes(authorityRevision, reportRelative);
  manifest = manifestBytes === null ? null :
    parseJSONText(manifestBytes.toString("utf8"), `manifest at ${authorityRevision}`);
  schema = schemaBytes === null ? null :
    parseJSONText(schemaBytes.toString("utf8"), `schema at ${authorityRevision}`);
  reportText = reportBytes === null ? null : reportBytes.toString("utf8");
} else {
  manifest = await parseJSONFile(path.join(bundleRoot, "manifest.json"), "manifest");
  schema = await parseJSONFile(path.join(bundleRoot, "manifest.schema.json"), "schema");
  try {
    reportText = await readFile(path.join(repositoryRoot, reportRelative), "utf8");
  } catch (error) {
    fail(`acceptance report cannot be read: ${error.message}`);
    reportText = null;
  }
}

if (manifest && schema && reportText !== null) {
  check(schema.$schema === "https://json-schema.org/draft/2020-12/schema",
    "manifest schema must pin JSON Schema draft 2020-12");
  check(schema.$id === "manifest.schema.json" && schema.additionalProperties === false,
    "manifest schema root identity or closure is invalid");
  for (const error of collectSchemaErrors(schema, manifest)) fail(error);
  check(manifest.phase === options.phase,
    `manifest phase ${manifest.phase} does not equal expected ${options.phase}`);
  check(manifest.compatibility?.authored_search === "0.2" &&
    manifest.compatibility?.knowledge_expression === "0.1",
  "manifest compatibility identities are invalid");

  const head = evidenceRevision ??
    (options.phase === "implementation-checkpoint" ? "WORKTREE" : currentHead);
  const status = evidenceRevision === null ?
    git(["status", "--porcelain=v1", "-z", "--untracked-files=all"], null) : null;
  if (!options.allowDirty && evidenceRevision === null && status !== null) {
    check(status.length === 0,
      `${options.phase} verification requires a clean tracked and untracked tree`);
  }
  if (evidenceRevision !== null && currentHead !== null) {
    check(gitIsAncestor(evidenceRevision, currentHead),
      "accepted v0.2 evidence revision E must be an ancestor of the current checkout");
    for (const [id, relative] of [
      ["acceptance-verifier", "scripts/verify-spl-v02-acceptance.mjs"],
      ["strict-json-parser", "scripts/strict-json.mjs"],
    ]) {
      const artifact = manifest.artifacts?.find((entry) => entry.id === id);
      try {
        checkExpectedGitMode("WORKTREE", relative, "100644", `current ${id}`);
        const absolute = checkSafeRepositoryPath(relative, `current ${id}`);
        check(artifact && absolute && sha256(await readFile(absolute)) === artifact.sha256,
          `current ${id} bytes must equal the verifier qualified by v0.2 R`);
      } catch (error) {
        fail(`current ${id} cannot be read: ${error.message}`);
      }
    }
  }
  const reportPhase = parseReportPhase(reportText);
  checkReportState(reportText, options.phase);
  check(reportPhase === options.phase,
    `acceptance report phase ${reportPhase} does not equal expected ${options.phase}`);
  if (head !== null) {
    if (options.phase === "implementation-checkpoint") {
      validateCheckpoint(manifest, head, reportPhase);
    } else if (options.phase === "qualification-candidate") {
      validateCandidate(manifest, head, reportPhase);
    } else {
      validateAccepted(manifest, head, reportPhase);
      await validateReceipts(manifest, authorityRevision);
    }
    if (options.publication) await validatePublication(manifest, head);
  }
}

if (failures.length > 0) {
  process.stderr.write(`${failures.join("\n")}\n`);
  process.exit(1);
}

if (options.printEvidenceBinding) {
  const manifestBytes = showBytes(evidenceRevision, manifestRelative);
  process.stdout.write(`${evidenceRevision} ${sha256(manifestBytes)}\n`);
} else if (options.printRuntimeRevision) {
  process.stdout.write(`${manifest.runtime.revision}\n`);
} else {
  process.stdout.write(`SPL v0.2 ${options.phase} evidence verified\n`);
}

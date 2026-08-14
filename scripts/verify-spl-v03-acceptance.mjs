#!/usr/bin/env node

/* eslint-disable no-await-in-loop, preserve-caught-error, unicorn/consistent-function-scoping */
// Fail-closed verification preserves strict ordering and replaces low-level diagnostics.

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { lstat, readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";

const PHASES = new Set([
  "implementation-checkpoint",
  "qualification-candidate",
  "accepted",
]);
const repositoryRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const manifestRelative = "docs/evidence/spl-v0.3/manifest.json";
const schemaRelative = "docs/evidence/spl-v0.3/manifest.schema.json";
const acceptanceRelative = "docs/spl-compatibility-v0.3-acceptance.md";
const identityRelative = "internal/spl/doc.go";
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const OBJECT_ID_PATTERN = /^[0-9a-f]{40}$/;
const V02_APPLICATION_VERSION = "0.1.0";
const V03_APPLICATION_VERSION = "0.2.0";
const EXPECTED_ARTIFACTS = new Map([
  ["public-readme", "README.md"],
  ["deploy-env-generator", "deploy/generate-env.sh"],
  ["deploy-guide", "deploy/README.md"],
  ["collector-deployment-guide", "docs/collector-deployment.md"],
  ["integration-guide", "integration/README.md"],
  ["contract", "docs/spl-compatibility-v0.3.md"],
  ["corpus", "internal/spl/testdata/compatibility-v0.3.json"],
  ["corpus-oracle", "internal/spl/v03_executable_corpus_test.go"],
  ["reference-model", "internal/spl/v03_reference_model_test.go"],
  ["migration", "docs/spl-compatibility-v0.3-migration.md"],
  ["plan", "docs/spl-compatibility-v0.3-plan.md"],
  ["v02-acceptance", "docs/spl-compatibility-v0.2-acceptance.md"],
  ["v02-activation-manifest", "docs/evidence/spl-v0.2-activation/manifest.json"],
  ["v02-activation-schema", "docs/evidence/spl-v0.2-activation/manifest.schema.json"],
  ["v02-acceptance-verifier", "scripts/verify-spl-v02-acceptance.mjs"],
  ["v02-acceptance-verifier-tests", "scripts/spl-v02-acceptance.test.mjs"],
  ["strict-json-parser", "scripts/strict-json.mjs"],
  ["manifest-schema", "docs/evidence/spl-v0.3/manifest.schema.json"],
  ["acceptance-verifier", "scripts/verify-spl-v03-acceptance.mjs"],
  ["acceptance-verifier-tests", "scripts/spl-v03-acceptance.test.mjs"],
  ["ci-structural-tests", "scripts/spl-v03-ci.test.mjs"],
  ["v02-ci-structural-tests", "scripts/spl-v02-stats-by-ci.test.mjs"],
  ["release-artifact-gate", "internal/spl/v03_release_artifacts_adversarial_test.go"],
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
const REQUIRED_CI_JOBS = [
  "Protobuf contracts",
  "Frontend checks",
  "Go lint",
  "Go tests (race detector, non-catalog packages shard 1/4)",
  "Go tests (race detector, non-catalog packages shard 2/4)",
  "Go tests (race detector, non-catalog packages shard 3/4)",
  "Go tests (race detector, non-catalog packages shard 4/4)",
  "Go tests (race detector, knowledge catalog shard 1/4)",
  "Go tests (race detector, knowledge catalog shard 2/4)",
  "Go tests (race detector, knowledge catalog shard 3/4)",
  "Go tests (race detector, knowledge catalog shard 4/4)",
  "Go tests (atomic coverage)",
  "Knowledge object fuzz (selectors)",
  "Knowledge object fuzz (definitions)",
  "Knowledge object fuzz (catalog)",
  "Knowledge object fuzz (spl-parser)",
  "Knowledge object fuzz (hec-protocol)",
  "GradeThis compatibility corpus",
  "Knowledge runtime ClickHouse matrix",
  "SPL compatibility pinned ClickHouse verticals",
  "HEC durable (observational 50 requests/s small-only correctness/backpressure saturation and outage recovery)",
  "HEC durable (rate-gated 1,000 EPS batch-only throughput and outage recovery)",
  "Backend vertical",
  "Release OCI contract",
  "Go vulnerability scan",
  "Production binaries (linux-amd64)",
  "Production binaries (macos)",
  "Linux/macOS release asset consistency",
];
const REQUIRED_RECEIPTS = new Set([
  "source-identity",
  "go-test-all",
  "go-race-focused",
  "frontend-gates",
  "clickhouse-v03",
  "release-readback",
  "binary-identities",
  "artifact-digests",
  "ci-readback",
  "remote-readback",
]);
const REQUIRED_RELEASE_DIGESTS = new Set([
  "open-splunk-server",
  "open-splunk-collector",
  "open-splunk-loggen",
  "asset-manifest.json",
  "binary-identities.txt",
  "release-verification.txt",
]);

function expectedUIBuildID(applicationVersion, sourceRevision) {
  check(applicationVersion === V03_APPLICATION_VERSION,
    `UI build identity requires application ${V03_APPLICATION_VERSION}`);
  check(OBJECT_ID_PATTERN.test(sourceRevision),
    "UI build identity requires a full lowercase runtime revision");
  const digest = createHash("sha256")
    .update("open-splunk-ui-build-v1\0")
    .update(applicationVersion)
    .update("\0")
    .update(sourceRevision)
    .digest("hex");
  const adBlockerSafe = digest.replaceAll(/[a-f]/g, (character) => ({
    a: "g",
    b: "h",
    c: "j",
    d: "k",
    e: "m",
    f: "n",
  })[character]);
  return `r${adBlockerSafe}`;
}

function verifyReleaseReadback(
  source,
  applicationVersion,
  runtimeRevision,
  expectedArtifactSHA256,
) {
  const releaseLines = source.split("\n");
  check(releaseLines.length === 6 && releaseLines[5] === "" &&
    releaseLines[0] === `application_version=${applicationVersion}` &&
    releaseLines[1] === `source_revision=${runtimeRevision}` &&
    releaseLines[2] === "spl_compatibility_version=0.3" &&
    releaseLines[3] ===
      `ui_build_id=${expectedUIBuildID(applicationVersion, runtimeRevision)}` &&
    /^ui_sha256=[0-9a-f]{64}$/.test(releaseLines[4]),
  "release readback receipt does not exactly bind application/source/SPL/UI identities");
  check(SHA256_PATTERN.test(expectedArtifactSHA256) &&
    sha256(Buffer.from(source, "utf8")) === expectedArtifactSHA256,
  "release readback receipt does not equal the release-verification artifact");
}

function verifyReleaseReadbackReceiptDigest(receiptSHA256, artifactSHA256) {
  check(SHA256_PATTERN.test(receiptSHA256) &&
    SHA256_PATTERN.test(artifactSHA256) &&
    receiptSHA256 === artifactSHA256,
  "release-readback receipt digest does not equal the release-verification artifact");
}
const REQUIRED_SPL_TESTS = [
  "internal/clickhouse.TestStatsByDeferredValidationAdversarialAgainstClickHouse",
  "internal/clickhouse.TestStatsMultivalueByAgainstClickHouse",
  "internal/clickhouse.TestStatsMultivalueByExpansionLimitAgainstClickHouse",
  "internal/clickhouse.TestStatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse",
  "internal/queryexec.TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse",
  "internal/queryexec.TestSemanticBytesV02ManagerAgainstClickHouse",
  "internal/queryexec.TestSemanticBytesModeManagerAgainstClickHouse",
  "internal/clickhouse.TestV03AdversarialAgainstClickHouse",
  "internal/clickhouse.TestV03FillNullDynamicThenMVExpandAgainstClickHouse",
  "internal/clickhouse.TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse",
  "internal/export.TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport",
  "internal/clickhouse.TestV03PinnedClickHouseRetainedTupleHasCanonicalBytes",
  "internal/queryexec.TestSemanticBytesLineageManagerAgainstClickHouse",
  "internal/queryexec.TestSparklineFeedsStatsByThroughManagerAgainstClickHouse",
  "internal/queryexec.TestV03AllTenPreserveUntouchedSemanticBytesThroughManagerAgainstClickHouse",
];

function usage() {
  return "usage: node scripts/verify-spl-v03-acceptance.mjs --phase PHASE " +
    "[--allow-dirty] [--publication] [--print-runtime-revision] " +
    "[--evidence-revision <E> --print-evidence-binding]";
}

function parseArguments(argumentsList) {
  const options = {
    phase: null,
    allowDirty: false,
    publication: false,
    printRuntimeRevision: false,
    evidenceRevision: null,
    printEvidenceBinding: false,
  };
  const seen = new Set();
  for (let index = 0; index < argumentsList.length; index += 1) {
    const argument = argumentsList[index];
    if (seen.has(argument)) {
      throw new Error(`duplicate argument ${JSON.stringify(argument)}`);
    }
    seen.add(argument);
    if (argument === "--phase") {
      options.phase = argumentsList[index + 1] ?? null;
      index += 1;
    } else if (argument === "--allow-dirty") {
      options.allowDirty = true;
    } else if (argument === "--publication") {
      options.publication = true;
    } else if (argument === "--print-runtime-revision") {
      options.printRuntimeRevision = true;
    } else if (argument === "--evidence-revision") {
      const revision = argumentsList[index + 1];
      if (revision === undefined) {
        throw new Error("--evidence-revision requires an exact revision E");
      }
      options.evidenceRevision = revision;
      index += 1;
    } else if (argument === "--print-evidence-binding") {
      options.printEvidenceBinding = true;
    } else {
      throw new Error(`unknown argument ${JSON.stringify(argument)}`);
    }
  }
  if (!PHASES.has(options.phase)) {
    throw new Error("--phase must be implementation-checkpoint, qualification-candidate, or accepted");
  }
  if (options.allowDirty && options.phase !== "implementation-checkpoint") {
    throw new Error("--allow-dirty is permitted only for implementation-checkpoint");
  }
  if (options.publication && options.phase !== "accepted") {
    throw new Error("publication/runtime output requires --phase accepted");
  }
  if (options.printRuntimeRevision && !options.publication) {
    throw new Error("--print-runtime-revision requires --publication");
  }
  if (options.evidenceRevision !== null &&
      (options.phase !== "accepted" || options.allowDirty || options.publication ||
        options.printRuntimeRevision ||
        !OBJECT_ID_PATTERN.test(options.evidenceRevision))) {
    throw new Error(
      "--evidence-revision requires an exact accepted E without publication/runtime options",
    );
  }
  if (options.printEvidenceBinding && options.evidenceRevision === null) {
    throw new Error("--print-evidence-binding requires --evidence-revision <E>");
  }
  if (options.evidenceRevision !== null && !options.printEvidenceBinding) {
    throw new Error("--evidence-revision <E> requires --print-evidence-binding");
  }
  return options;
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

const gitEnvironment = {
  ...process.env,
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_NO_REPLACE_OBJECTS: "1",
  GIT_OPTIONAL_LOCKS: "0",
  GIT_TERMINAL_PROMPT: "0",
};

function git(argumentsList) {
  const result = spawnSync("git", argumentsList, {
    cwd: repositoryRoot,
    encoding: "utf8",
    env: gitEnvironment,
    maxBuffer: 32 * 1024 * 1024,
  });
  check(result.status === 0,
    `git ${argumentsList.join(" ")} failed: ${String(result.stderr).trim()}`);
  return result.stdout;
}

function gitBytes(argumentsList) {
  const result = spawnSync("git", argumentsList, {
    cwd: repositoryRoot,
    encoding: null,
    env: gitEnvironment,
    maxBuffer: 128 * 1024 * 1024,
  });
  check(result.status === 0,
    `git ${argumentsList.join(" ")} failed: ${String(result.stderr).trim()}`);
  return result.stdout;
}

function gitIsAncestor(ancestor, descendant) {
  const result = spawnSync(
    "git",
    ["merge-base", "--is-ancestor", ancestor, descendant],
    {
      cwd: repositoryRoot,
      encoding: "utf8",
      env: gitEnvironment,
      maxBuffer: 32 * 1024 * 1024,
    },
  );
  check(result.status === 0 || result.status === 1,
    `git merge-base --is-ancestor failed: ${String(result.stderr).trim()}`);
  return result.status === 0;
}

function revisionBytes(revision, relative, label) {
  try {
    return gitBytes(["show", `${revision}:${relative}`]);
  } catch (error) {
    throw new Error(`${label} cannot be read from ${revision}: ${error.message}`);
  }
}

function verifyRevisionMode(revision, relative, expectedMode, label) {
  const entry = git(["ls-tree", revision, "--", relative]).trim();
  const match = entry.match(/^([0-9]{6}) blob ([0-9a-f]+)\t([^\n]+)$/u);
  check(match?.[1] === expectedMode && match?.[3] === relative,
    `${label} must resolve to one exact Git blob with mode ${expectedMode} at ${revision}`);
}

async function verifyGitHistoryIntegrity(requireCompleteHistory = false) {
  const commonDirectory = git([
    "rev-parse", "--path-format=absolute", "--git-common-dir",
  ]).trim();
  check(path.isAbsolute(commonDirectory),
    "Git common directory must resolve to an absolute path");
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
    check(git(["rev-parse", "--is-shallow-repository"]).trim() === "false",
      "accepted v0.3 ancestor verification requires a non-shallow repository");
  }
}

function parseJSONObject(source, label) {
  rejectDuplicateJSONNames(source, label);
  let value;
  try {
    value = JSON.parse(source);
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
  check(value && typeof value === "object" && !Array.isArray(value),
    `${label} must be a JSON object`);
  return value;
}

function rejectDuplicateJSONNames(source, label) {
  let offset = 0;
  const skipWhitespace = () => {
    while (/\s/.test(source[offset] ?? "")) offset += 1;
  };
  const parseString = () => {
    check(source[offset] === '"', `${label} contains malformed JSON`);
    const start = offset;
    offset += 1;
    while (offset < source.length) {
      if (source[offset] === "\\") {
        offset += 2;
      } else if (source[offset] === '"') {
        offset += 1;
        return JSON.parse(source.slice(start, offset));
      } else {
        offset += 1;
      }
    }
    throw new Error(`${label} contains an unterminated JSON String`);
  };
  const parseValue = () => {
    skipWhitespace();
    if (source[offset] === "{") {
      offset += 1;
      skipWhitespace();
      const names = new Set();
      if (source[offset] === "}") {
        offset += 1;
        return;
      }
      while (true) {
        skipWhitespace();
        const name = parseString();
        check(!names.has(name), `${label} duplicates object key ${JSON.stringify(name)}`);
        names.add(name);
        skipWhitespace();
        check(source[offset] === ":", `${label} contains malformed JSON`);
        offset += 1;
        parseValue();
        skipWhitespace();
        if (source[offset] === "}") {
          offset += 1;
          return;
        }
        check(source[offset] === ",", `${label} contains malformed JSON`);
        offset += 1;
      }
    }
    if (source[offset] === "[") {
      offset += 1;
      skipWhitespace();
      if (source[offset] === "]") {
        offset += 1;
        return;
      }
      while (true) {
        parseValue();
        skipWhitespace();
        if (source[offset] === "]") {
          offset += 1;
          return;
        }
        check(source[offset] === ",", `${label} contains malformed JSON`);
        offset += 1;
      }
    }
    if (source[offset] === '"') {
      parseString();
      return;
    }
    const remainder = source.slice(offset);
    const primitive = /^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(remainder);
    check(primitive !== null, `${label} contains malformed JSON`);
    offset += primitive[0].length;
  };
  parseValue();
  skipWhitespace();
  check(offset === source.length, `${label} contains trailing JSON data`);
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function exactKeys(value, keys, label) {
  check(value && typeof value === "object" && !Array.isArray(value),
    `${label} must be an object`);
  const actual = Object.keys(value).toSorted();
  const expected = [...keys].toSorted();
  check(JSON.stringify(actual) === JSON.stringify(expected),
    `${label} fields are ${actual.join(", ")}; expected ${expected.join(", ")}`);
}

function safeRepositoryPath(relative, label) {
  check(typeof relative === "string" && relative.length > 0,
    `${label} path must be nonempty`);
  check(relative === relative.replaceAll("\\", "/") && !path.isAbsolute(relative),
    `${label} path must be a slash-separated repository-relative path`);
  check(path.posix.normalize(relative) === relative && relative !== ".." &&
    !relative.startsWith("../"), `${label} path is unsafe`);
  return path.join(repositoryRoot, relative);
}

async function requireRegularFile(relative, label) {
  const absolute = safeRepositoryPath(relative, label);
  let current = repositoryRoot;
  for (const segment of relative.split("/")) {
    current = path.join(current, segment);
    const component = await lstat(current);
    check(!component.isSymbolicLink(),
      `${label} path must not traverse a symbolic link`);
  }
  const metadata = await lstat(absolute);
  check(metadata.isFile() && !metadata.isSymbolicLink(),
    `${label} must be a regular non-symlink file`);
  return readFile(absolute);
}

async function authorityBytes(relative, label, sourceRevision = null) {
  return sourceRevision === null
    ? requireRegularFile(relative, label)
    : revisionBytes(sourceRevision, relative, label);
}

function markdownField(source, name) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const matches = [...source.matchAll(
    new RegExp(`^\\*\\*${escaped}:\\*\\*\\s*(.+)$`, "gmi"),
  )];
  check(matches.length === 1,
    `acceptance report must contain exactly one ${name} field`);
  return matches[0][1].trim().replace(/^`|`$/g, "");
}

function readCompatibilityIdentity(source) {
  const matches = [...source.matchAll(
    /^const CompatibilityVersion = "([0-9]+\.[0-9]+)"$/gm,
  )];
  check(matches.length === 1,
    "internal/spl/doc.go must declare exactly one literal CompatibilityVersion");
  return matches[0][1];
}

function shellBlocks(source, label) {
  const blocks = [...source.matchAll(
    /^```(?:sh|bash|shell)\r?\n([\s\S]*?)^```[ \t]*\r?$/gm,
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
  const blocks = shellBlocks(source, label);
  for (const [name, count] of Object.entries(expected)) {
    const occurrences = blocks.reduce((total, candidate) =>
      total + candidate.split(name).length - 1, 0);
    check(occurrences === count,
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
  command) {
  requireCanonicalShellBlock(source, [
    `OPEN_SPLUNK_APPLICATION_VERSION=${applicationVersion} \\`,
    `OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=${compatibilityVersion} \\`,
    command,
  ].join("\n"), 1, label);
  requireAuthorityOccurrences(source, {
    OPEN_SPLUNK_APPLICATION_VERSION: 1,
    OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: 1,
  }, label);
}

function verifyPublicREADME(source, phase) {
  const expectedVersion = phase === "implementation-checkpoint" ? "0.2" : "0.3";
  const otherVersion = expectedVersion === "0.2" ? "0.3" : "0.2";
  const expectedApplicationVersion = phase === "implementation-checkpoint"
    ? V02_APPLICATION_VERSION
    : V03_APPLICATION_VERSION;
  for (const suffix of ["md", "migration.md", "acceptance.md"]) {
    const expected = `docs/spl-compatibility-v${expectedVersion}-${suffix}`
      .replace(`v${expectedVersion}-md`, `v${expectedVersion}.md`);
    check(source.includes(`(${expected})`),
      `README must link the public v${expectedVersion} ${suffix} authority in ${phase}`);
    const stale = `docs/spl-compatibility-v${otherVersion}-${suffix}`
      .replace(`v${otherVersion}-md`, `v${otherVersion}.md`);
    check(!source.includes(`(${stale})`),
      `README still links the stale public v${otherVersion} ${suffix} authority in ${phase}`);
  }
  requireReleasePair(source, expectedApplicationVersion, expectedVersion,
    `README in ${phase}`, 'OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" make release');
}

function verifyOperatorReleaseIdentitySources(sources, phase) {
  const checkpoint = phase === "implementation-checkpoint";
  const applicationVersion = checkpoint ? V02_APPLICATION_VERSION : V03_APPLICATION_VERSION;
  const compatibilityVersion = checkpoint ? "0.2" : "0.3";
  requireParameterDefault(sources.deployGenerator,
    "OPEN_SPLUNK_APPLICATION_VERSION", applicationVersion,
    `deployment generator in ${phase}`);
  requireCanonicalShellBlock(sources.deployGuide, [
    "cd ..",
    "git status --short",
    `export OPEN_SPLUNK_APPLICATION_VERSION=${applicationVersion}`,
    `export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=${compatibilityVersion}`,
    'export OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)"',
    "make oci",
  ].join("\n"), 1, `deployment guide in ${phase}`);
  requireCanonicalShellBlock(sources.deployGuide, [
    "cd deploy",
    `OPEN_SPLUNK_APPLICATION_VERSION=${applicationVersion} ./generate-env.sh`,
    "docker compose up --detach --wait",
  ].join("\n"), 1, `deployment guide in ${phase}`);
  requireAuthorityOccurrences(sources.deployGuide, {
    OPEN_SPLUNK_APPLICATION_VERSION: 2,
    OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION: 1,
  }, `deployment guide in ${phase}`);
  requireCanonicalShellBlock(sources.collectorGuide, [
    `export OPEN_SPLUNK_COLLECTOR_VERSION=${applicationVersion}`,
    'export OPEN_SPLUNK_COLLECTOR_IMAGE="ghcr.io/suhaibinator/open-splunk-collector:${OPEN_SPLUNK_COLLECTOR_VERSION}"',
    'docker pull "$OPEN_SPLUNK_COLLECTOR_IMAGE"',
    "docker image inspect --format '{{.Os}}/{{.Architecture}}' \"$OPEN_SPLUNK_COLLECTOR_IMAGE\"",
  ].join("\n"), 1, `collector deployment guide in ${phase}`);
  requireAuthorityOccurrences(sources.collectorGuide, {
    "OPEN_SPLUNK_COLLECTOR_VERSION=": 1,
    "OPEN_SPLUNK_COLLECTOR_IMAGE=": 1,
  }, `collector deployment guide in ${phase}`);
  requireReleasePair(sources.integrationGuide, applicationVersion, compatibilityVersion,
    `integration guide in ${phase}`, [
      "OPEN_SPLUNK_OCI_INTEGRATION=1 \\",
      "OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \\",
      "  go test ./integration -run '^TestReleaseOCIComposeContract$' \\",
      "    -count=1 -timeout=25m -v",
    ].join("\n"));
}

const OPERATOR_AUTHORITY_SHA256 = Object.freeze({
  "implementation-checkpoint": Object.freeze({
    publicREADME: "42b175ea79965ddb2bd9a7546480a3906f9b1f811cfb4da96c8145cbec2c28db",
    deployGenerator: "5f6c3a95aa720acb7769431b3b54930c9deb7f124c2d0a09c6966a72a708828b",
    deployGuide: "b94a4361edc5e8f04e2482c5722675e11cb803aecbc240c0cfacccf61e19eaf1",
    collectorGuide: "e4b38c4539cfe4be60e538fbe408bc9fce0d7e35e464e520da058784fa9324c0",
    integrationGuide: "f297f0936822ce53b05c8b2ff84ada8a4fa363974cc0570dd21b1880342ee569",
  }),
  candidate: Object.freeze({
    publicREADME: "f8628fe543e7433442bc7a86e7df711a595e74a033261b0cd01b47b05895e2bf",
    deployGenerator: "66e581e7745d44ad6a051454984f4359d33cc301a4bf229feb9f184cae4ea680",
    deployGuide: "a94f23e45ad87dff733fb2b8af8cf3a7daf793707350a4557fdd6e71ebb2b41e",
    collectorGuide: "1b1f9f264b2e0a02795410d8f01d275d4f54f367a956d366040fdd65b0dc3ce8",
    integrationGuide: "7eb5b01858866ddcc1c12beed8b1afc8477c33c3efa69bd38a41d1350ed2f874",
  }),
});

function verifyOperatorAuthorityHashes(sources, phase) {
  const expected = phase === "implementation-checkpoint"
    ? OPERATOR_AUTHORITY_SHA256["implementation-checkpoint"]
    : OPERATOR_AUTHORITY_SHA256.candidate;
  for (const [id, digest] of Object.entries(expected)) {
    check(sha256(Buffer.from(sources[id], "utf8")) === digest,
      `${id} bytes do not equal the exact ${phase} release authority`);
  }
}

async function verifyOperatorReleaseIdentity(phase, sourceRevision = null) {
  for (const [relative, expectedMode] of [
    ["README.md", "100644"],
    ["deploy/generate-env.sh", "100755"],
    ["deploy/README.md", "100644"],
    ["docs/collector-deployment.md", "100644"],
    ["integration/README.md", "100644"],
  ]) {
    if (sourceRevision === null) {
      const absolute = safeRepositoryPath(relative, relative);
      let current = repositoryRoot;
      for (const segment of relative.split("/")) {
        current = path.join(current, segment);
        const component = await lstat(current);
        check(!component.isSymbolicLink(),
          `${relative} path must not traverse a symbolic link`);
      }
      const metadata = await lstat(absolute);
      const executable = (metadata.mode & 0o111) !== 0;
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        executable === (expectedMode === "100755"),
      `${relative} must have exact worktree mode ${expectedMode}`);
    } else {
      verifyRevisionMode(sourceRevision, relative, expectedMode, relative);
    }
  }
  const [deployGenerator, deployGuide, collectorGuide, integrationGuide] =
    await Promise.all([
      authorityBytes("deploy/generate-env.sh", "deployment environment generator", sourceRevision),
      authorityBytes("deploy/README.md", "deployment guide", sourceRevision),
      authorityBytes("docs/collector-deployment.md", "collector deployment guide", sourceRevision),
      authorityBytes("integration/README.md", "integration guide", sourceRevision),
    ]);
  const sources = {
    deployGenerator: deployGenerator.toString("utf8"),
    deployGuide: deployGuide.toString("utf8"),
    collectorGuide: collectorGuide.toString("utf8"),
    integrationGuide: integrationGuide.toString("utf8"),
  };
  verifyOperatorReleaseIdentitySources(sources, phase);
  const publicREADME = await authorityBytes("README.md", "public README", sourceRevision);
  verifyOperatorAuthorityHashes({
    publicREADME: publicREADME.toString("utf8"),
    ...sources,
  }, phase);
}

function verifySchemaEnvelope(schema) {
  check(schema.$schema === "https://json-schema.org/draft/2020-12/schema",
    "manifest schema draft is not pinned");
  check(schema.$id === "manifest.schema.json",
    "manifest schema ID is invalid");
  check(schema.type === "object" && schema.additionalProperties === false,
    "manifest schema root must be a closed object");
  const required = new Set(schema.required ?? []);
  for (const field of [
    "phase", "compatibility", "prerequisite", "runtime", "artifacts",
    "ci", "release", "receipts", "decision",
  ]) {
    check(required.has(field), `manifest schema does not require ${field}`);
  }
}

// The activation manifest is deliberately small enough to validate without a
// package-manager dependency. Keep this validator limited to, but complete
// for, the JSON Schema vocabulary used by manifest.schema.json. Validation is
// applied both to the checked-out E manifest and to the candidate manifest
// read directly from R, so a syntactically valid but schema-invalid evidence
// object cannot bypass the R/E replay checks.
function collectSchemaErrors(schema, value, location = "manifest") {
  const errors = [];

  function resolveReference(root, reference, current, branchErrors) {
    if (typeof reference !== "string" || !reference.startsWith("#/")) {
      branchErrors.push(`${current} has an unsupported schema reference`);
      return null;
    }
    let target = root;
    for (const encoded of reference.slice(2).split("/")) {
      const segment = encoded.replaceAll("~1", "/").replaceAll("~0", "~");
      target = target?.[segment];
    }
    if (!target || typeof target !== "object" || Array.isArray(target)) {
      branchErrors.push(`${current} has an unresolved schema reference`);
      return null;
    }
    return target;
  }

  function visit(node, candidate, current, root, branchErrors = errors) {
    if (!node || typeof node !== "object" || Array.isArray(node)) {
      branchErrors.push(`${current} has an invalid schema node`);
      return;
    }
    if (node.$ref !== undefined) {
      const target = resolveReference(root, node.$ref, current, branchErrors);
      if (target !== null) visit(target, candidate, current, root, branchErrors);
      return;
    }
    if (node.anyOf !== undefined) {
      if (!Array.isArray(node.anyOf) || node.anyOf.length === 0) {
        branchErrors.push(`${current} has an invalid anyOf schema`);
        return;
      }
      const alternatives = node.anyOf.map((branch) => {
        const alternativeErrors = [];
        visit(branch, candidate, current, root, alternativeErrors);
        return alternativeErrors;
      });
      if (!alternatives.some((alternative) => alternative.length === 0)) {
        branchErrors.push(`${current} does not match any allowed schema shape`);
      }
      return;
    }
    if (Object.hasOwn(node, "const") && candidate !== node.const) {
      branchErrors.push(`${current} must equal ${JSON.stringify(node.const)}`);
      return;
    }
    if (node.enum !== undefined) {
      if (!Array.isArray(node.enum) ||
          !node.enum.some((entry) => isDeepStrictEqual(entry, candidate))) {
        branchErrors.push(`${current} has an unsupported value`);
        return;
      }
    }

    const types = Array.isArray(node.type)
      ? node.type
      : node.type === undefined ? [] : [node.type];
    if (types.length > 0) {
      const matchesType = types.some((type) => {
        if (type === "null") return candidate === null;
        if (type === "array") return Array.isArray(candidate);
        if (type === "object") {
          return candidate !== null && typeof candidate === "object" &&
            !Array.isArray(candidate);
        }
        if (type === "integer") return Number.isSafeInteger(candidate);
        if (type === "number") {
          return typeof candidate === "number" && Number.isFinite(candidate);
        }
        return typeof candidate === type;
      });
      if (!matchesType) {
        branchErrors.push(`${current} has the wrong JSON type`);
        return;
      }
    }

    if (typeof candidate === "string") {
      if (node.minLength !== undefined && candidate.length < node.minLength) {
        branchErrors.push(`${current} is too short`);
      }
      if (node.maxLength !== undefined && candidate.length > node.maxLength) {
        branchErrors.push(`${current} is too long`);
      }
      if (node.pattern !== undefined &&
          !new RegExp(node.pattern, "u").test(candidate)) {
        branchErrors.push(`${current} does not match its schema pattern`);
      }
    }
    if (Number.isSafeInteger(candidate) && node.minimum !== undefined &&
        candidate < node.minimum) {
      branchErrors.push(`${current} is below its schema minimum`);
    }
    if (Array.isArray(candidate)) {
      if (node.minItems !== undefined && candidate.length < node.minItems) {
        branchErrors.push(`${current} has too few items`);
      }
      if (node.maxItems !== undefined && candidate.length > node.maxItems) {
        branchErrors.push(`${current} has too many items`);
      }
      if (node.items !== undefined) {
        candidate.forEach((entry, index) =>
          visit(node.items, entry, `${current}[${index}]`, root, branchErrors));
      }
      return;
    }
    if (candidate === null || typeof candidate !== "object") return;

    const properties = node.properties ?? {};
    for (const required of node.required ?? []) {
      if (!Object.hasOwn(candidate, required)) {
        branchErrors.push(`${current}.${required} is required`);
      }
    }
    if (node.additionalProperties === false) {
      for (const key of Object.keys(candidate)) {
        if (!Object.hasOwn(properties, key)) {
          branchErrors.push(`${current}.${key} is not allowed`);
        }
      }
    }
    for (const [key, property] of Object.entries(properties)) {
      if (Object.hasOwn(candidate, key)) {
        visit(property, candidate[key], `${current}.${key}`, root, branchErrors);
      }
    }
  }

  visit(schema, value, location, schema);
  return errors;
}

function verifyManifestSchema(schema, manifest, label = "manifest") {
  const errors = collectSchemaErrors(schema, manifest, label);
  check(errors.length === 0,
    `${label} violates manifest.schema.json: ${errors.join("; ")}`);
}

function verifyManifestEnvelope(manifest) {
  exactKeys(manifest, [
    "$schema", "format_version", "phase", "compatibility", "prerequisite",
    "runtime", "artifacts", "ci", "release", "receipts", "decision",
  ], "manifest");
  exactKeys(manifest.compatibility, [
    "runtime_authored_search", "target_authored_search", "knowledge_expression",
  ], "manifest.compatibility");
  exactKeys(manifest.prerequisite,
    [
      "v02_status", "evidence_path", "evidence_sha256",
      "evidence_revision", "runtime_revision",
    ],
    "manifest.prerequisite");
  exactKeys(manifest.runtime, [
    "revision_binding", "revision", "tree", "candidate_acceptance_sha256",
    "remote_ref", "remote_readback_revision",
  ], "manifest.runtime");
  exactKeys(manifest.ci, [
    "repository", "workflow", "run_id", "url", "event", "head_revision",
    "status", "conclusion", "completed_at_utc", "jobs",
  ], "manifest.ci");
  exactKeys(manifest.release, [
    "source_revision", "spl_compatibility_version", "application_version",
    "binary_identities", "artifact_digests",
  ], "manifest.release");
  exactKeys(manifest.decision,
    ["status", "stable_publication_authorized", "reason"],
    "manifest.decision");
}

async function verifyV02Prerequisite(manifest, report, carrierRevision = null) {
  check(markdownField(report, "v0.2 prerequisite") === "accepted",
    "qualification requires the report to mark v0.2 accepted");
  check(manifest.prerequisite?.v02_status === "accepted",
    "qualification requires accepted v0.2 manifest provenance");
  check(manifest.prerequisite.evidence_path ===
    "docs/evidence/spl-v0.2-activation/manifest.json",
  "v0.2 evidence must use the strict activation manifest");
  check(SHA256_PATTERN.test(manifest.prerequisite.evidence_sha256),
    "v0.2 evidence SHA-256 is invalid");
  check(OBJECT_ID_PATTERN.test(manifest.prerequisite.evidence_revision) &&
    OBJECT_ID_PATTERN.test(manifest.prerequisite.runtime_revision),
  "v0.2 prerequisite must bind exact evidence and runtime revisions");
  const v02Artifact = manifest.artifacts.find((artifact) =>
    artifact.id === "v02-activation-manifest");
  check(v02Artifact?.sha256 === manifest.prerequisite.evidence_sha256,
    "v0.2 prerequisite hash must equal the pinned activation-manifest artifact hash");
  const bytes = await authorityBytes(
    manifest.prerequisite.evidence_path,
    "v0.2 evidence",
    carrierRevision,
  );
  check(sha256(bytes) === manifest.prerequisite.evidence_sha256,
    "v0.2 evidence SHA-256 does not match");
  rejectDuplicateJSONNames(bytes.toString("utf8"), "v0.2 evidence");
  const evidence = JSON.parse(bytes);
  check(evidence.$schema === "./manifest.schema.json" &&
    evidence.format_version === "open-splunk-spl-v0.2-activation-evidence-v1" &&
    evidence.phase === "accepted" &&
    evidence.decision?.status === "accepted" &&
    evidence.decision?.stable_publication_authorized === true &&
    evidence.compatibility?.authored_search === "0.2" &&
    evidence.compatibility?.knowledge_expression === "0.1" &&
    OBJECT_ID_PATTERN.test(evidence.runtime?.revision) &&
    evidence.runtime?.revision_binding === "recorded-runtime-parent" &&
    evidence.ci?.status === "completed" && evidence.ci?.conclusion === "success" &&
    evidence.release?.source_revision === evidence.runtime.revision &&
    evidence.release?.spl_compatibility_version === "0.2" &&
    evidence.release?.application_version === V02_APPLICATION_VERSION,
  "v0.2 evidence does not prove completed authored-search 0.2 acceptance");
  const evidenceRevision = manifest.prerequisite.evidence_revision;
  const runtimeRevision = manifest.prerequisite.runtime_revision;
  check(git(["rev-parse", `${evidenceRevision}^{commit}`]).trim() === evidenceRevision,
    "v0.2 evidence revision does not resolve exactly");
  const parents = git(["rev-list", "--parents", "-n", "1", evidenceRevision])
    .trim().split(" ");
  check(parents.length === 2 && parents[1] === runtimeRevision,
    "v0.2 accepted E must be a direct non-merge child of its runtime R");
  check(evidence.runtime?.revision === runtimeRevision &&
    evidence.runtime?.revision_binding === "recorded-runtime-parent",
  "v0.2 accepted manifest runtime does not equal the prerequisite R");
  check(sha256(gitBytes(["show", `${evidenceRevision}:${manifest.prerequisite.evidence_path}`])) ===
    manifest.prerequisite.evidence_sha256,
  "v0.2 evidence hash does not match the exact accepted E object");
  const carrier = carrierRevision ?? "HEAD";
  check(gitIsAncestor(evidenceRevision, carrier),
  "accepted v0.2 evidence carrier must be an ancestor of the v0.3 carrier");

  for (const relative of [
    "docs/spl-compatibility-v0.2-acceptance.md",
    "docs/evidence/spl-v0.2-activation/manifest.json",
    "docs/evidence/spl-v0.2-activation/manifest.schema.json",
    "scripts/verify-spl-v02-acceptance.mjs",
    "scripts/spl-v02-acceptance.test.mjs",
    "scripts/strict-json.mjs",
  ]) {
    const currentBytes = await authorityBytes(
      relative,
      `v0.2 authority ${relative}`,
      carrierRevision,
    );
    const acceptedBytes = gitBytes(["show", `${evidenceRevision}:${relative}`]);
    check(Buffer.compare(currentBytes, acceptedBytes) === 0,
      `v0.2 authority ${relative} differs from exact accepted evidence revision E`);
  }

  for (const relative of [
    "scripts/verify-spl-v02-acceptance.mjs",
    "scripts/strict-json.mjs",
  ]) {
    const absolute = safeRepositoryPath(relative, `current v0.2 authority ${relative}`);
    let current = repositoryRoot;
    for (const segment of relative.split("/")) {
      current = path.join(current, segment);
      const component = await lstat(current);
      check(!component.isSymbolicLink(),
        `current v0.2 authority ${relative} path must not traverse a symbolic link`);
    }
    const metadata = await lstat(absolute);
    check(metadata.isFile() && !metadata.isSymbolicLink() &&
      (metadata.mode & 0o111) === 0,
    `current v0.2 authority ${relative} must have exact worktree mode 100644`);
    const worktreeBytes = await readFile(absolute);
    const acceptedBytes = gitBytes(["show", `${evidenceRevision}:${relative}`]);
    check(Buffer.compare(worktreeBytes, acceptedBytes) === 0,
      `executed v0.2 authority ${relative} differs from accepted E`);
  }

  const prerequisiteVerifier = spawnSync(process.execPath, [
    "scripts/verify-spl-v02-acceptance.mjs",
    "--phase", "accepted",
    "--evidence-revision", evidenceRevision,
    "--print-evidence-binding",
  ], {
    cwd: repositoryRoot,
    encoding: "utf8",
    env: gitEnvironment,
    maxBuffer: 128 * 1024 * 1024,
  });
  check(prerequisiteVerifier.status === 0,
    `strict v0.2 prerequisite verification failed: ${String(prerequisiteVerifier.stderr).trim()}`);
  check(prerequisiteVerifier.stdout ===
    `${evidenceRevision} ${manifest.prerequisite.evidence_sha256}\n`,
  "strict v0.2 prerequisite verifier returned a different E/manifest binding");
  return evidence.release.application_version;
}

function verifyArtifactInventory(manifest) {
  check(Array.isArray(manifest.artifacts) &&
    manifest.artifacts.length === EXPECTED_ARTIFACTS.size,
  "manifest must contain the exact stable artifact inventory");
  const seenIDs = new Set();
  const seenPaths = new Set();
  for (const artifact of manifest.artifacts) {
    exactKeys(artifact, ["id", "path", "sha256"], "manifest artifact");
    check(!seenIDs.has(artifact.id) && !seenPaths.has(artifact.path),
      `artifact ${artifact.id} is duplicated`);
    check(EXPECTED_ARTIFACTS.get(artifact.id) === artifact.path,
      `artifact ${artifact.id} has unexpected path ${artifact.path}`);
    seenIDs.add(artifact.id);
    seenPaths.add(artifact.path);
  }
  check(JSON.stringify([...seenIDs].toSorted()) ===
    JSON.stringify([...EXPECTED_ARTIFACTS.keys()].toSorted()),
  "manifest artifact IDs do not match the exact required inventory");
}

async function verifyArtifacts(manifest, sourceRevision = null) {
  verifyArtifactInventory(manifest);
  const seen = new Set();
  for (const artifact of manifest.artifacts) {
    exactKeys(artifact, ["id", "path", "sha256"], "manifest artifact");
    check(!seen.has(artifact.id), `duplicate artifact ID ${artifact.id}`);
    seen.add(artifact.id);
    check(EXPECTED_ARTIFACTS.get(artifact.id) === artifact.path,
      `artifact ${artifact.id} has unexpected path ${artifact.path}`);
    check(SHA256_PATTERN.test(artifact.sha256),
      `artifact ${artifact.id} has invalid SHA-256`);
    const expectedMode = EXPECTED_ARTIFACT_MODES.get(artifact.path) ?? "100644";
    let bytes;
    if (sourceRevision === null) {
      const absolute = safeRepositoryPath(artifact.path, `artifact ${artifact.id}`);
      let current = repositoryRoot;
      for (const segment of artifact.path.split("/")) {
        current = path.join(current, segment);
        const component = await lstat(current);
        check(!component.isSymbolicLink(),
          `artifact ${artifact.id} path must not traverse a symbolic link`);
      }
      const metadata = await lstat(absolute);
      const executable = (metadata.mode & 0o111) !== 0;
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        executable === (expectedMode === "100755"),
      `artifact ${artifact.id} must have exact worktree mode ${expectedMode}`);
      bytes = await readFile(absolute);
    } else {
      verifyRevisionMode(sourceRevision, artifact.path, expectedMode,
        `artifact ${artifact.id}`);
      bytes = Buffer.from(git(["show", `${sourceRevision}:${artifact.path}`]), "utf8");
    }
    check(sha256(bytes) === artifact.sha256,
      `artifact ${artifact.id} SHA-256 does not match ${sourceRevision ?? "the checkout"}`);
  }
  for (const id of EXPECTED_ARTIFACTS.keys()) {
    check(seen.has(id), `manifest is missing artifact ${id}`);
  }
}

async function verifyCurrentAncestorAuthority(manifest) {
  for (const id of [
    "acceptance-verifier",
    "v02-acceptance-verifier",
    "strict-json-parser",
  ]) {
    const artifact = manifest.artifacts.find((entry) => entry.id === id);
    check(artifact && SHA256_PATTERN.test(artifact.sha256),
      `accepted ancestor mode requires qualified ${id} identity`);
    const absolute = safeRepositoryPath(artifact.path, `current ${id}`);
    const metadata = await lstat(absolute);
    check(metadata.isFile() && !metadata.isSymbolicLink() &&
      (metadata.mode & 0o111) === 0,
    `current ${id} must have exact worktree mode 100644`);
    const bytes = await requireRegularFile(artifact.path, `current ${id}`);
    check(sha256(bytes) === artifact.sha256,
      `current ${id} bytes must equal the authority qualified by v0.3 R`);
  }
}

function verifyCheckpoint(manifest, report, identity) {
  check(identity === "0.2", "implementation checkpoint must retain runtime identity 0.2");
  check(markdownField(report, "Status") ===
    "implementation checkpoint; activation provenance pending",
  "checkpoint status is not exact");
  check(markdownField(report, "v0.2 prerequisite") === "pending",
    "checkpoint v0.2 prerequisite must be pending");
  check(markdownField(report, "Activation decision") ===
    "pending; distribution blocked",
  "checkpoint activation decision must be pending and blocked");
  check(manifest.compatibility?.runtime_authored_search === "0.2" &&
    manifest.compatibility?.target_authored_search === "0.3" &&
    manifest.compatibility?.knowledge_expression === "0.1",
  "checkpoint compatibility identities are inconsistent");
  check(manifest.prerequisite?.v02_status === "pending",
    "checkpoint manifest must retain the pending v0.2 prerequisite");
  check(manifest.prerequisite?.evidence_path === null &&
    manifest.prerequisite?.evidence_sha256 === null &&
    manifest.prerequisite?.evidence_revision === null &&
    manifest.prerequisite?.runtime_revision === null,
  "checkpoint must not claim v0.2 evidence provenance");
  check(manifest.runtime?.revision_binding === "unbound" &&
    manifest.runtime?.revision === null && manifest.runtime?.tree === null &&
    manifest.runtime?.candidate_acceptance_sha256 === null &&
    manifest.runtime?.remote_ref === null &&
    manifest.runtime?.remote_readback_revision === null,
  "checkpoint manifest must not claim a runtime revision");
  check(manifest.artifacts.every((artifact) => artifact.sha256 === null),
    "checkpoint artifact hashes must all remain null");
  check(manifest.decision?.status === "pending" &&
    manifest.decision?.stable_publication_authorized === false,
  "checkpoint manifest must block stable publication");
  check(manifest.ci?.repository === null && manifest.ci?.run_id === null &&
    manifest.ci?.url === null && manifest.ci?.event === null &&
    manifest.ci?.head_revision === null && manifest.ci?.status === null &&
    manifest.ci?.conclusion === null && manifest.ci?.completed_at_utc === null &&
    Array.isArray(manifest.ci?.jobs) && manifest.ci.jobs.length === 0,
  "checkpoint manifest must not claim CI provenance");
  check(manifest.release?.source_revision === null &&
    manifest.release?.spl_compatibility_version === null &&
    manifest.release?.application_version === null &&
    Array.isArray(manifest.release?.binary_identities) &&
    manifest.release.binary_identities.length === 0 &&
    Array.isArray(manifest.release?.artifact_digests) &&
    manifest.release.artifact_digests.length === 0,
  "checkpoint manifest must not claim CI or release provenance");
  check(Array.isArray(manifest.receipts) && manifest.receipts.length === 0,
    "checkpoint manifest must not contain receipts");
}

function verifyCandidateInvariants(manifest, report, identity) {
  verifyManifestEnvelope(manifest);
  verifyArtifactInventory(manifest);
  check(manifest.$schema === "./manifest.schema.json" &&
    manifest.format_version === "open-splunk-spl-v0.3-activation-evidence-v1" &&
    manifest.phase === "qualification-candidate",
  "qualification candidate manifest identity or phase is invalid");
  check(markdownField(report, "Acceptance phase") === "qualification-candidate" &&
    markdownField(report, "Target authored-search identity") === "0.3" &&
    markdownField(report, "Knowledge-expression identity") === "0.1" &&
    markdownField(report, "v0.2 prerequisite") === "accepted",
  "qualification candidate report identity, phase, or prerequisite is invalid");
  check(identity === "0.3", "qualification candidate must embed runtime identity 0.3");
  check(markdownField(report, "Status") ===
    "qualification candidate; final evidence pending",
  "candidate status is not exact");
  check(markdownField(report, "Activation decision") ===
    "pending; stable publication blocked",
  "candidate activation decision must remain pending and blocked");
  check(manifest.compatibility?.runtime_authored_search === "0.3" &&
    manifest.compatibility?.target_authored_search === "0.3" &&
    manifest.compatibility?.knowledge_expression === "0.1",
  "candidate manifest compatibility identities must be exactly 0.3/0.3/0.1");
  check(manifest.runtime?.revision_binding === "containing-commit" &&
    manifest.runtime?.revision === null && manifest.runtime?.tree === null &&
    manifest.runtime?.candidate_acceptance_sha256 === null &&
    manifest.runtime?.remote_ref === null &&
    manifest.runtime?.remote_readback_revision === null,
  "candidate derives its containing commit and must not guess its own object identity");
  check(manifest.ci?.repository === null && manifest.ci?.workflow === "CI" &&
    manifest.ci?.run_id === null &&
    manifest.ci?.url === null && manifest.ci?.event === null &&
    manifest.ci?.head_revision === null && manifest.ci?.status === null &&
    manifest.ci?.conclusion === null && manifest.ci?.completed_at_utc === null &&
    Array.isArray(manifest.ci?.jobs) && manifest.ci.jobs.length === 0,
  "candidate must not claim post-commit CI evidence");
  check(manifest.release?.source_revision === null &&
    manifest.release?.spl_compatibility_version === null &&
    manifest.release?.application_version === null &&
    Array.isArray(manifest.release?.binary_identities) &&
    manifest.release.binary_identities.length === 0 &&
    Array.isArray(manifest.release?.artifact_digests) &&
    manifest.release.artifact_digests.length === 0,
  "candidate must not claim post-commit CI or release evidence");
  check(manifest.decision?.status === "pending" &&
    manifest.decision?.stable_publication_authorized === false &&
    typeof manifest.decision?.reason === "string" &&
    manifest.decision.reason.length > 0,
  "candidate must block stable publication");
  check(Array.isArray(manifest.receipts) && manifest.receipts.length === 0,
    "candidate receipts belong in the later evidence revision");
}

async function verifyCandidate(manifest, report, identity, sourceRevision = null) {
  verifyCandidateInvariants(manifest, report, identity);
  if (sourceRevision === null) {
    for (const relative of [manifestRelative, acceptanceRelative]) {
      const absolute = safeRepositoryPath(relative, relative);
      const metadata = await lstat(absolute);
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        (metadata.mode & 0o111) === 0,
      `${relative} must be a regular non-executable worktree file`);
    }
  } else {
    for (const relative of [manifestRelative, acceptanceRelative]) {
      verifyRevisionMode(sourceRevision, relative, "100644", relative);
    }
  }
  await verifyV02Prerequisite(manifest, report, sourceRevision);
  await verifyArtifacts(manifest, sourceRevision);
}

function parseJSONDocument(source, label) {
  rejectDuplicateJSONNames(source, label);
  try {
    return JSON.parse(source);
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
}

function parseJSONSource(source, label) {
  const value = parseJSONDocument(source, label);
  verifyManifestEnvelope(value);
  return value;
}

function verifyCI(manifest, runtimeRevision) {
  const ci = manifest.ci;
  check(typeof ci.repository === "string" &&
    /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(ci.repository),
  "accepted CI repository is invalid");
  check(ci.workflow === "CI" && Number.isSafeInteger(ci.run_id) && ci.run_id > 0,
    "accepted CI workflow/run identity is invalid");
  check(ci.url === `https://github.com/${ci.repository}/actions/runs/${ci.run_id}`,
    "accepted CI URL does not bind repository and run ID");
  check(ci.event === "push" || ci.event === "workflow_dispatch",
    "accepted CI event must be push or workflow_dispatch");
  check(ci.head_revision === runtimeRevision && ci.status === "completed" &&
    ci.conclusion === "success" &&
    /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$/.test(
      ci.completed_at_utc,
    ),
  "accepted CI must be terminal-success and bound to R");
  check(Array.isArray(ci.jobs) && ci.jobs.length === REQUIRED_CI_JOBS.length,
    `accepted CI must record exactly ${REQUIRED_CI_JOBS.length} jobs`);
  const names = new Set();
  const ids = new Set();
  for (const job of ci.jobs) {
    exactKeys(job, ["id", "name", "url", "status", "conclusion"], "CI job");
    check(Number.isSafeInteger(job.id) && job.id > 0 && !ids.has(job.id),
      "CI job IDs must be unique positive integers");
    check(typeof job.name === "string" && !names.has(job.name),
      "CI job names must be unique strings");
    check(job.url ===
      `https://github.com/${ci.repository}/actions/runs/${ci.run_id}/job/${job.id}`,
    `CI job ${job.name} URL is not canonical`);
    check(job.status === "completed" && job.conclusion === "success",
      `CI job ${job.name} is not terminal-success`);
    ids.add(job.id);
    names.add(job.name);
  }
  check(JSON.stringify([...names].toSorted()) ===
    JSON.stringify(REQUIRED_CI_JOBS.toSorted()),
  "accepted CI job inventory does not match the exact required 28 jobs");
}

async function verifyReceipts(manifest, runtimeRevision, evidenceRevision = null) {
  check(Array.isArray(manifest.receipts) &&
    manifest.receipts.length === REQUIRED_RECEIPTS.size,
  "accepted evidence must contain the exact required receipt inventory");
  const seenIDs = new Set();
  const seenPaths = new Set();
  const contents = new Map();
  for (const receipt of manifest.receipts) {
    exactKeys(receipt, ["id", "path", "sha256", "bytes"], "receipt");
    check(/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(receipt.id) &&
      !seenIDs.has(receipt.id), `duplicate or invalid receipt ID ${receipt.id}`);
    check(/^receipts\/[A-Za-z0-9._/-]+$/.test(receipt.path) &&
      path.posix.normalize(receipt.path) === receipt.path &&
      !seenPaths.has(receipt.path), `duplicate or unsafe receipt path ${receipt.path}`);
    check(SHA256_PATTERN.test(receipt.sha256) &&
      Number.isSafeInteger(receipt.bytes) && receipt.bytes > 0,
    `receipt ${receipt.id} has invalid integrity metadata`);
    const relative = `docs/evidence/spl-v0.3/${receipt.path}`;
    if (evidenceRevision === null) {
      const absolute = safeRepositoryPath(relative, `receipt ${receipt.id}`);
      let current = repositoryRoot;
      for (const segment of relative.split("/")) {
        current = path.join(current, segment);
        const component = await lstat(current);
        check(!component.isSymbolicLink(),
          `receipt ${receipt.id} path must not traverse a symbolic link`);
      }
      const metadata = await lstat(absolute);
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        (metadata.mode & 0o111) === 0,
      `receipt ${receipt.id} must be a regular non-executable file`);
    } else {
      verifyRevisionMode(evidenceRevision, relative, "100644", `receipt ${receipt.id}`);
    }
    const bytes = await authorityBytes(
      relative,
      `receipt ${receipt.id}`,
      evidenceRevision,
    );
    check(bytes.length === receipt.bytes && sha256(bytes) === receipt.sha256,
      `receipt ${receipt.id} integrity does not match`);
    seenIDs.add(receipt.id);
    seenPaths.add(receipt.path);
    contents.set(receipt.id, bytes.toString("utf8"));
  }
  for (const id of REQUIRED_RECEIPTS) {
    check(seenIDs.has(id), `accepted evidence is missing receipt ${id}`);
  }
  check(JSON.stringify([...seenIDs].toSorted()) ===
    JSON.stringify([...REQUIRED_RECEIPTS].toSorted()),
  "accepted receipt IDs do not equal the exact required inventory");

  const runtimeTree = manifest.runtime.tree;
  const expectedJSON = new Map([
    ["source-identity", {
      runtime_revision: runtimeRevision,
      runtime_tree: runtimeTree,
      compatibility_version: "0.3",
      knowledge_expression_version: "0.1",
      result: "pass",
    }],
    ["go-test-all", {
      runtime_revision: runtimeRevision,
      runtime_tree: runtimeTree,
      command: "go test ./... -count=1 -timeout=15m",
      result: "pass",
    }],
    ["go-race-focused", {
      runtime_revision: runtimeRevision,
      runtime_tree: runtimeTree,
      packages: [
        "./internal/spl", "./internal/plan", "./internal/clickhouse",
        "./internal/queryexec", "./internal/searchjobs", "./internal/server",
        "./internal/searchsnapshot", "./internal/searchinspection",
        "./internal/searchanalysis", "./internal/export",
      ],
      result: "pass",
    }],
    ["frontend-gates", {
      runtime_revision: runtimeRevision,
      runtime_tree: runtimeTree,
      node: "v24.18.0",
      npm: "11.16.0",
      commands: [
        "npm ci", "npm audit --omit=dev --audit-level=critical",
        "npm run typecheck", "npm run lint", "npm run test:frontend",
        "npm run build",
      ],
      result: "pass",
    }],
    ["clickhouse-v03", {
      runtime_revision: runtimeRevision,
      runtime_tree: runtimeTree,
      image: "clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49",
      tests: REQUIRED_SPL_TESTS,
      result: "pass",
    }],
    ["ci-readback", manifest.ci],
    ["remote-readback", {
      runtime_revision: runtimeRevision,
      runtime_tree: runtimeTree,
      remote_ref: manifest.runtime.remote_ref,
      remote_readback_revision: manifest.runtime.remote_readback_revision,
      result: "pass",
    }],
  ]);
  for (const [id, expected] of expectedJSON) {
    const source = contents.get(id);
    rejectDuplicateJSONNames(source, `receipt ${id}`);
    let value;
    try {
      value = JSON.parse(source);
    } catch (error) {
      throw new Error(`receipt ${id} is not valid JSON: ${error.message}`);
    }
    check(isDeepStrictEqual(value, expected),
      `receipt ${id} does not exactly reproduce its accepted authority fields`);
  }
  const applicationVersion = manifest.release.application_version;
  const verificationDigest = manifest.release.artifact_digests.find(
    (entry) => entry.name === "release-verification.txt",
  )?.sha256;
  verifyReleaseReadbackReceiptDigest(
    manifest.receipts.find((entry) => entry.id === "release-readback")?.sha256,
    verificationDigest,
  );
  verifyReleaseReadback(
    contents.get("release-readback"),
    applicationVersion,
    runtimeRevision,
    verificationDigest,
  );

  const expectedBinaryIdentities = [
    "open-splunk-server",
    `application_version=${applicationVersion}`,
    `source_revision=${runtimeRevision}`,
    "spl_compatibility_version=0.3",
    "open-splunk-collector",
    `application_version=${applicationVersion}`,
    `source_revision=${runtimeRevision}`,
    "open-splunk-loggen",
    `application_version=${applicationVersion}`,
    `source_revision=${runtimeRevision}`,
    "",
  ].join("\n");
  check(contents.get("binary-identities") === expectedBinaryIdentities,
    "binary-identities receipt does not exactly bind all production binaries");

  const tracked = git(["ls-tree", "-r", "--name-only", evidenceRevision ?? "HEAD", "--",
    "docs/evidence/spl-v0.3/receipts"]).trim().split("\n").filter(Boolean)
    .map((entry) => entry.slice("docs/evidence/spl-v0.3/".length)).toSorted();
  check(JSON.stringify(tracked) === JSON.stringify([...seenPaths].toSorted()),
    "tracked receipt files must equal the manifest receipt inventory exactly");
  return contents;
}

function verifyRelease(
  manifest,
  runtimeRevision,
  receiptContents,
  prerequisiteApplicationVersion = V02_APPLICATION_VERSION,
) {
  const release = manifest.release;
  check(release.source_revision === runtimeRevision &&
    release.spl_compatibility_version === "0.3" &&
    release.application_version === V03_APPLICATION_VERSION,
  `release identity must bind R, SPL 0.3, and application ${V03_APPLICATION_VERSION}`);
  check(prerequisiteApplicationVersion === V02_APPLICATION_VERSION &&
    release.application_version !== prerequisiteApplicationVersion,
  "v0.3 release application version must advance beyond the accepted v0.2 release");
  const expectedBinaryNames = [
    "open-splunk-collector", "open-splunk-loggen", "open-splunk-server",
  ];
  check(Array.isArray(release.binary_identities) &&
    release.binary_identities.length === expectedBinaryNames.length,
  "release must record exactly the three production binary identities");
  const binaryNames = new Set();
  for (const identity of release.binary_identities) {
    exactKeys(identity, [
      "name", "application_version", "source_revision", "spl_compatibility_version",
    ], "release binary identity");
    check(expectedBinaryNames.includes(identity.name) && !binaryNames.has(identity.name) &&
      identity.application_version === release.application_version &&
      identity.source_revision === runtimeRevision,
    `binary identity ${identity.name} does not match the release`);
    check(identity.name === "open-splunk-server"
      ? identity.spl_compatibility_version === "0.3"
      : identity.spl_compatibility_version === null,
    `binary identity ${identity.name} has an invalid SPL identity`);
    binaryNames.add(identity.name);
  }
  check(JSON.stringify([...binaryNames].toSorted()) === JSON.stringify(expectedBinaryNames),
    "release binary identity names do not match the exact inventory");
  check(Array.isArray(release.artifact_digests) &&
    release.artifact_digests.length === REQUIRED_RELEASE_DIGESTS.size,
  "release must record the exact artifact digest inventory");
  const names = new Set();
  const digestReceipt = receiptContents.get("artifact-digests");
  const digestLines = digestReceipt.trimEnd().split("\n");
  check(digestLines.length === REQUIRED_RELEASE_DIGESTS.size,
    "artifact digest receipt must contain the exact digest line count");
  for (const digest of release.artifact_digests) {
    exactKeys(digest, ["name", "sha256"], "release artifact digest");
    check(REQUIRED_RELEASE_DIGESTS.has(digest.name) && !names.has(digest.name) &&
      SHA256_PATTERN.test(digest.sha256),
    `release digest ${digest.name} is missing, duplicate, or invalid`);
    check(digestReceipt.split("\n").includes(`${digest.sha256}  ${digest.name}`),
      `artifact digest receipt does not contain ${digest.name}`);
    names.add(digest.name);
  }
  check(names.size === REQUIRED_RELEASE_DIGESTS.size,
    "release digest names do not match the required inventory");
  check(new Set(digestLines).size === REQUIRED_RELEASE_DIGESTS.size,
    "artifact digest receipt contains duplicate or extra lines");
}

function verifyEvidenceCommit(runtimeRevision, evidenceRevision = "HEAD") {
  const parents = git(["rev-list", "--parents", "-n", "1", evidenceRevision])
    .trim().split(" ");
  check(parents.length === 2 && parents[1] === runtimeRevision,
    "accepted E must be a non-merge commit whose only parent is R");
  const deleted = gitBytes([
    "diff", "--no-renames", "--diff-filter=D", "--name-only", "-z",
    runtimeRevision, evidenceRevision,
  ]);
  check(deleted.length === 0, "accepted E must not delete any path");
  const paths = gitBytes([
    "diff", "--no-renames", "--name-only", "-z", runtimeRevision, evidenceRevision,
  ]).toString("utf8").split("\0").filter(Boolean);
  check(paths.includes("docs/spl-compatibility-v0.3-acceptance.md") &&
    paths.includes("docs/evidence/spl-v0.3/manifest.json"),
  "accepted E must change both the report and manifest");
  for (const changed of paths) {
    check(changed === "docs/spl-compatibility-v0.3-acceptance.md" ||
      changed === "docs/evidence/spl-v0.3/manifest.json" ||
      changed.startsWith("docs/evidence/spl-v0.3/receipts/"),
    `accepted E changes non-evidence path ${changed}`);
    verifyRevisionMode(
      evidenceRevision,
      changed,
      "100644",
      `accepted E path ${changed}`,
    );
  }
}

async function verifyAccepted(manifest, report, identity, evidenceRevision = "HEAD") {
  check(identity === "0.3" &&
    manifest.compatibility?.runtime_authored_search === "0.3",
  "accepted evidence must inherit runtime identity 0.3");
  check(markdownField(report, "Status") === "accepted",
    "accepted report status is not exact");
  check(markdownField(report, "Activation decision") === "accepted",
    "accepted report decision is not exact");
  const prerequisiteApplicationVersion = await verifyV02Prerequisite(
    manifest,
    report,
    evidenceRevision,
  );
  const runtime = manifest.runtime;
  check(runtime.revision_binding === "recorded-runtime-parent" &&
    OBJECT_ID_PATTERN.test(runtime.revision) && OBJECT_ID_PATTERN.test(runtime.tree) &&
    SHA256_PATTERN.test(runtime.candidate_acceptance_sha256),
  "accepted runtime binding is incomplete");
  check(git(["rev-parse", `${runtime.revision}^{commit}`]).trim() === runtime.revision &&
    git(["rev-parse", `${runtime.revision}^{tree}`]).trim() === runtime.tree,
  "accepted R object/tree does not match Git readback");
  verifyEvidenceCommit(runtime.revision, evidenceRevision);

  const candidateReport = git([
    "show", `${runtime.revision}:docs/spl-compatibility-v0.3-acceptance.md`,
  ]);
  const candidateManifestSource = git([
    "show", `${runtime.revision}:docs/evidence/spl-v0.3/manifest.json`,
  ]);
  const candidateSchemaSource = git([
    "show", `${runtime.revision}:docs/evidence/spl-v0.3/manifest.schema.json`,
  ]);
  const candidateManifest = parseJSONSource(candidateManifestSource, "candidate manifest at R");
  const candidateSchema = parseJSONDocument(candidateSchemaSource, "candidate schema at R");
  verifySchemaEnvelope(candidateSchema);
  verifyManifestSchema(candidateSchema, candidateManifest, "candidate manifest at R");
  check(sha256(Buffer.from(candidateReport)) === runtime.candidate_acceptance_sha256,
    "accepted manifest does not bind the candidate report at R");
  const candidateIdentity = readCompatibilityIdentity(git([
    "show", `${runtime.revision}:internal/spl/doc.go`,
  ]));
  await verifyCandidate(
    candidateManifest,
    candidateReport,
    candidateIdentity,
    runtime.revision,
  );
  await verifyArtifacts(manifest, runtime.revision);
  if (evidenceRevision !== "HEAD") {
    await verifyCurrentAncestorAuthority(manifest);
  }

  check(runtime.remote_ref === `refs/tags/spl-v0.3-runtime-${runtime.revision}` &&
    runtime.remote_readback_revision === runtime.revision,
  "accepted R requires the immutable full-R runtime qualification tag/readback");
  check(git(["rev-parse", `${runtime.remote_ref}^{commit}`]).trim() === runtime.revision,
    "local runtime qualification tag does not resolve to R");
  verifyCI(manifest, runtime.revision);
  const receipts = await verifyReceipts(manifest, runtime.revision, evidenceRevision);
  verifyRelease(
    manifest,
    runtime.revision,
    receipts,
    prerequisiteApplicationVersion,
  );
  check(manifest.decision?.status === "accepted" &&
    manifest.decision?.stable_publication_authorized === true,
  "accepted manifest must explicitly authorize stable publication");
  return runtime.revision;
}

function remoteRefCommit(remoteName, reference, runGit = git) {
  const source = runGit([
    "ls-remote", remoteName, reference, `${reference}^{}`,
  ]).trim();
  const lines = source === "" ? [] : source.split("\n");
  check(lines.length >= 1 && lines.length <= 2,
    `remote reference ${reference} returned an ambiguous object set`);
  const parsed = lines.map((line) => line.trim().split(/\s+/));
  check(parsed.every((fields) => fields.length === 2 &&
    OBJECT_ID_PATTERN.test(fields[0]) &&
    (fields[1] === reference || fields[1] === `${reference}^{}`)),
  `remote reference ${reference} returned an unexpected object or name`);
  const direct = parsed.filter((fields) => fields[1] === reference);
  const peeled = parsed.filter((fields) => fields[1] === `${reference}^{}`);
  check(direct.length === 1 && peeled.length <= 1,
    `remote reference ${reference} returned duplicate or incomplete identities`);
  return (peeled[0] ?? direct[0])?.[0] ?? null;
}

function remoteReadback(reference, expectedRevision, label) {
  check(remoteRefCommit("origin", reference) === expectedRevision,
    `${label} remote readback does not resolve exactly to ${expectedRevision}`);
}

function parseGitHubJSON(source, label) {
  rejectDuplicateJSONNames(source, label);
  try {
    return JSON.parse(source);
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
}

function githubJSON(repository, endpoint) {
  const token = process.env.GITHUB_TOKEN;
  check(typeof token === "string" && token.length > 0,
    "publication verification requires GITHUB_TOKEN");
  const url = `https://api.github.com/repos/${repository}${endpoint}`;
  const result = spawnSync("curl", [
    "--fail-with-body", "--silent", "--show-error", "--location",
    "--proto", "=https", "--tlsv1.2",
    "--header", "Accept: application/vnd.github+json",
    "--header", `Authorization: Bearer ${token}`,
    "--header", "X-GitHub-Api-Version: 2022-11-28",
    url,
  ], { encoding: "utf8", maxBuffer: 128 * 1024 * 1024 });
  check(result.status === 0,
    `GitHub readback failed for ${endpoint}: ${String(result.stderr).trim()}`);
  return parseGitHubJSON(result.stdout, `GitHub response ${endpoint}`);
}

function verifyPublication(manifest, evidenceRevision, runtimeRevision) {
  check(manifest.phase === "accepted" &&
    manifest.decision?.stable_publication_authorized === true,
  "stable publication requires accepted v0.3 evidence");
  check(process.env.GITHUB_REF_TYPE === "tag" &&
    typeof process.env.GITHUB_REF_NAME === "string" &&
    /^v[0-9]/.test(process.env.GITHUB_REF_NAME),
  "publication must run for an exact v-prefixed release tag");
  check(process.env.GITHUB_REF_NAME === `v${manifest.release.application_version}`,
    "release tag must equal v plus the qualified application version");
  check(process.env.GITHUB_SHA === evidenceRevision,
    "publication GITHUB_SHA must equal accepted evidence revision E");
  check(process.env.GITHUB_REPOSITORY === manifest.ci.repository &&
    manifest.ci.repository === "Suhaibinator/open-splunk",
  "publication repository must equal the canonical recorded repository");
  const releaseRef = `refs/tags/${process.env.GITHUB_REF_NAME}`;
  check(git(["rev-parse", `${releaseRef}^{commit}`]).trim() === evidenceRevision,
    "local release tag does not resolve to E");
  remoteReadback(releaseRef, evidenceRevision, "release tag");
  remoteReadback(manifest.runtime.remote_ref, runtimeRevision, "runtime tag");

  const run = githubJSON(manifest.ci.repository,
    `/actions/runs/${manifest.ci.run_id}`);
  check(run.name === "CI" && run.event === manifest.ci.event &&
    run.head_sha === runtimeRevision && run.status === "completed" &&
    run.conclusion === "success" && run.updated_at === manifest.ci.completed_at_utc &&
    run.html_url === manifest.ci.url,
  "live CI run does not exactly match terminal-success R provenance");
  if (manifest.ci.event === "push") {
    check(run.head_branch === "main",
      "push qualification is accepted only for exact main");
    remoteReadback("refs/heads/main", runtimeRevision, "main branch");
  } else {
    check(manifest.ci.event === "workflow_dispatch",
      "non-main R qualification must use workflow_dispatch");
  }

  const jobs = githubJSON(manifest.ci.repository,
    `/actions/runs/${manifest.ci.run_id}/jobs?filter=latest&per_page=100`);
  check(jobs.total_count === REQUIRED_CI_JOBS.length &&
    Array.isArray(jobs.jobs) && jobs.jobs.length === REQUIRED_CI_JOBS.length,
  "live CI job inventory is incomplete, duplicated, or paginated");
  const liveJobs = jobs.jobs.map((job) => ({
    id: job.id,
    name: job.name,
    url: job.html_url,
    status: job.status,
    conclusion: job.conclusion,
  })).toSorted((left, right) => left.name.localeCompare(right.name, "en"));
  const recordedJobs = [...manifest.ci.jobs]
    .toSorted((left, right) => left.name.localeCompare(right.name, "en"));
  check(JSON.stringify(liveJobs) === JSON.stringify(recordedJobs),
    "live CI jobs do not exactly match the recorded 28-job set");
}

async function main() {
  let options;
  try {
    options = parseArguments(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error.message}\n${usage()}\n`);
    process.exitCode = 2;
    return;
  }

  const evidenceRevision = options.evidenceRevision;
  await verifyGitHistoryIntegrity(evidenceRevision !== null);
  const currentHead = git(["rev-parse", "HEAD"]).trim();
  if (evidenceRevision !== null) {
    const resolvedEvidence = spawnSync(
      "git",
      ["rev-parse", `${evidenceRevision}^{commit}`],
      {
        cwd: repositoryRoot,
        encoding: "utf8",
        env: gitEnvironment,
        maxBuffer: 32 * 1024 * 1024,
      },
    );
    check(resolvedEvidence.status === 0 &&
      resolvedEvidence.stdout.trim() === evidenceRevision,
      "accepted v0.3 evidence revision E does not resolve exactly");
    check(gitIsAncestor(evidenceRevision, currentHead),
      "accepted v0.3 evidence revision E must be an ancestor of the current checkout");
  }
  if (options.phase === "implementation-checkpoint") {
    for (const [id, relative] of EXPECTED_ARTIFACTS) {
      const expectedMode = EXPECTED_ARTIFACT_MODES.get(relative) ?? "100644";
      const absolute = safeRepositoryPath(relative, `checkpoint artifact ${id}`);
      let current = repositoryRoot;
      for (const segment of relative.split("/")) {
        current = path.join(current, segment);
        const component = await lstat(current);
        check(!component.isSymbolicLink(),
          `checkpoint artifact ${id} path must not traverse a symbolic link`);
      }
      const metadata = await lstat(absolute);
      const executable = (metadata.mode & 0o111) !== 0;
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        executable === (expectedMode === "100755"),
      `checkpoint artifact ${id} must have exact worktree mode ${expectedMode}`);
    }
    for (const relative of [manifestRelative, acceptanceRelative]) {
      const absolute = safeRepositoryPath(relative, relative);
      let current = repositoryRoot;
      for (const segment of relative.split("/")) {
        current = path.join(current, segment);
        const component = await lstat(current);
        check(!component.isSymbolicLink(),
          `${relative} path must not traverse a symbolic link`);
      }
      const metadata = await lstat(absolute);
      check(metadata.isFile() && !metadata.isSymbolicLink() &&
        (metadata.mode & 0o111) === 0,
      `${relative} must have exact worktree mode 100644`);
    }
  }
  const authorityRevision = evidenceRevision ??
    (options.phase === "implementation-checkpoint" ? null : currentHead);
  const [manifestBytes, schemaBytes, reportBytes, identityBytes, publicREADMEBytes] =
    await Promise.all([
      authorityBytes(manifestRelative, "v0.3 manifest", authorityRevision),
      authorityBytes(schemaRelative, "v0.3 manifest schema", authorityRevision),
      authorityBytes(acceptanceRelative, "v0.3 acceptance report", authorityRevision),
      authorityBytes(identityRelative, "v0.3 runtime identity", authorityRevision),
      authorityBytes("README.md", "public README", authorityRevision),
    ]);
  const manifestSource = manifestBytes.toString("utf8");
  const schemaSource = schemaBytes.toString("utf8");
  const report = reportBytes.toString("utf8");
  const identitySource = identityBytes.toString("utf8");
  const publicREADME = publicREADMEBytes.toString("utf8");
  const manifest = parseJSONObject(manifestSource, "v0.3 manifest");
  const schema = parseJSONObject(schemaSource, "v0.3 manifest schema");
  verifySchemaEnvelope(schema);
  verifyManifestEnvelope(manifest);
  verifyManifestSchema(schema, manifest);
  verifyArtifactInventory(manifest);
  check(manifest.$schema === "./manifest.schema.json",
    "manifest schema binding is invalid");
  check(manifest.format_version ===
    "open-splunk-spl-v0.3-activation-evidence-v1",
  "manifest format version is invalid");
  check(manifest.phase === options.phase,
    `manifest phase ${JSON.stringify(manifest.phase)} does not equal expected ${options.phase}`);
  check(markdownField(report, "Acceptance phase") === options.phase,
    "acceptance report phase does not equal the manifest phase");
  check(markdownField(report, "Target authored-search identity") === "0.3" &&
    markdownField(report, "Knowledge-expression identity") === "0.1",
  "acceptance report compatibility identities are invalid");
  verifyPublicREADME(publicREADME, options.phase);
  await verifyOperatorReleaseIdentity(options.phase, authorityRevision);
  if (!options.allowDirty && evidenceRevision === null) {
    check(git(["status", "--porcelain=v1", "--untracked-files=all"]) === "",
      "v0.3 phase verification requires a clean worktree");
  }

  const identity = readCompatibilityIdentity(identitySource);
  let runtimeRevision = null;
  if (options.phase === "implementation-checkpoint") {
    verifyCheckpoint(manifest, report, identity);
  } else if (options.phase === "qualification-candidate") {
    await verifyCandidate(manifest, report, identity, authorityRevision);
  } else {
    runtimeRevision = await verifyAccepted(
      manifest,
      report,
      identity,
      authorityRevision,
    );
    if (options.publication) {
      verifyPublication(
        manifest,
        git(["rev-parse", "HEAD"]).trim(),
        runtimeRevision,
      );
    }
  }
  if (options.printEvidenceBinding) {
    process.stdout.write(`${evidenceRevision} ${sha256(manifestBytes)}\n`);
  } else if (options.printRuntimeRevision) {
    process.stdout.write(`${runtimeRevision}\n`);
  } else if (options.phase === "implementation-checkpoint") {
    process.stdout.write(
      "SPL v0.3 implementation checkpoint is internally consistent and not accepted\n",
    );
  } else if (options.phase === "qualification-candidate") {
    process.stdout.write(
      "SPL v0.3 qualification candidate is internally consistent; stable publication remains blocked\n",
    );
  } else {
    process.stdout.write("SPL v0.3 accepted evidence is internally consistent\n");
  }
}

export {
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
  verifyRelease,
  verifyReleaseReadback,
  verifyReleaseReadbackReceiptDigest,
  verifyPublicREADME,
};

if (path.resolve(process.argv[1] ?? "") === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}

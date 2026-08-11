#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

export const waiverBaseCommit = "c5440b96248c68a9b58d10ebaf08eaef5345b61a";

export const waivedViolations = Object.freeze([
  {
    path: "proto/open_splunk/v1/knowledge_api.proto",
    type: "FIELD_NO_DELETE",
    message:
      'Previously present field "11" with name "estimated_generated_sql_bytes" on message "KnowledgeResourceEstimate" was deleted.',
  },
  {
    path: "proto/open_splunk/v1/knowledge_api.proto",
    type: "FIELD_NO_DELETE",
    message:
      'Previously present field "6" with name "diagnostics" on message "KnowledgeValidationResult" was deleted.',
  },
  {
    path: "proto/open_splunk/v1/knowledge_api.proto",
    type: "FIELD_NO_DELETE",
    message:
      'Previously present field "7" with name "dependencies" on message "KnowledgeValidationResult" was deleted.',
  },
  {
    path: "proto/open_splunk/v1/knowledge_api.proto",
    type: "FIELD_SAME_TYPE",
    message:
      'Field "1" with name "dependencies" on message "ListKnowledgeObjectDependenciesResponse" changed type from "open_splunk.v1.KnowledgeObjectDependency" to "open_splunk.v1.KnowledgeManagementDependencyEdge".',
  },
  {
    path: "proto/open_splunk/v1/knowledge_api.proto",
    type: "FIELD_SAME_TYPE",
    message:
      'Field "1" with name "dependents" on message "ListKnowledgeObjectDependentsResponse" changed type from "open_splunk.v1.KnowledgeObjectDependency" to "open_splunk.v1.KnowledgeManagementDependencyEdge".',
  },
]);

function violationIdentity(violation) {
  return JSON.stringify([violation.path, violation.type, violation.message]);
}

export function parseBufViolations(output) {
  return output
    .split(/\r?\n/u)
    .filter((line) => line.trim() !== "")
    .map((line) => {
      let violation;
      try {
        violation = JSON.parse(line);
      } catch (error) {
        throw new Error(`Buf returned a non-JSON diagnostic: ${line}`, { cause: error });
      }
      for (const field of ["path", "type", "message"]) {
        if (typeof violation[field] !== "string" || violation[field] === "") {
          throw new Error(`Buf diagnostic is missing ${field}: ${line}`);
        }
      }
      return violation;
    });
}

export function classifyBufViolations(againstCommit, violations) {
  const allowed = againstCommit === waiverBaseCommit
    ? new Set(waivedViolations.map(violationIdentity))
    : new Set();
  const waived = [];
  const unexpected = [];
  for (const violation of violations) {
    if (allowed.has(violationIdentity(violation))) waived.push(violation);
    else unexpected.push(violation);
  }
  return { waived, unexpected };
}

function escapeWorkflowCommand(value, property = false) {
  let escaped = String(value)
    .replaceAll("%", "%25")
    .replaceAll("\r", "%0D")
    .replaceAll("\n", "%0A");
  if (property) {
    escaped = escaped.replaceAll(":", "%3A").replaceAll(",", "%2C");
  }
  return escaped;
}

export function githubAnnotation(level, violation) {
  const properties = [
    `file=${escapeWorkflowCommand(violation.path, true)}`,
    Number.isInteger(violation.start_line) ? `line=${violation.start_line}` : null,
    Number.isInteger(violation.start_column) ? `col=${violation.start_column}` : null,
  ].filter(Boolean).join(",");
  return `::${level} ${properties}::${escapeWorkflowCommand(violation.message)}`;
}

function checkedSpawn(command, arguments_, options = {}) {
  const result = spawnSync(command, arguments_, {
    encoding: "utf8",
    ...options,
  });
  if (result.error) throw result.error;
  return result;
}

function resolveCommit(repositoryRoot, reference) {
  const valid = checkedSpawn("git", ["check-ref-format", "--branch", reference], {
    cwd: repositoryRoot,
  });
  if (valid.status !== 0) throw new Error(`invalid against branch: ${reference}`);

  const resolved = checkedSpawn(
    "git",
    ["rev-parse", "--verify", "--end-of-options", `${reference}^{commit}`],
    { cwd: repositoryRoot },
  );
  if (resolved.status !== 0) {
    throw new Error(`cannot resolve against branch ${reference}: ${resolved.stderr.trim()}`);
  }
  return resolved.stdout.trim().toLowerCase();
}

function parseArguments(arguments_) {
  if (arguments_.length !== 2 || arguments_[0] !== "--against-ref" || arguments_[1] === "") {
    throw new Error("usage: node scripts/check-buf-breaking.mjs --against-ref <branch>");
  }
  return arguments_[1];
}

export function run(arguments_ = process.argv.slice(2)) {
  const againstReference = parseArguments(arguments_);
  const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const againstCommit = resolveCommit(repositoryRoot, againstReference);
  const executable = path.join(
    repositoryRoot,
    "node_modules",
    ".bin",
    process.platform === "win32" ? "buf.cmd" : "buf",
  );
  const result = checkedSpawn(
    executable,
    [
      "breaking",
      "--against", `.git#branch=${againstReference}`,
      "--error-format", "json",
    ],
    { cwd: repositoryRoot, env: process.env },
  );

  if (result.status === 0) {
    console.log(`Buf FILE compatibility passed against ${againstReference} (${againstCommit}).`);
    return 0;
  }
  if (result.status !== 100) {
    if (result.stdout !== "") process.stdout.write(result.stdout);
    if (result.stderr !== "") process.stderr.write(result.stderr);
    throw new Error(`buf breaking exited with status ${result.status}`);
  }

  const violations = parseBufViolations(result.stdout);
  if (violations.length === 0) {
    if (result.stderr !== "") process.stderr.write(result.stderr);
    throw new Error("buf breaking failed without a structured diagnostic");
  }
  const { waived, unexpected } = classifyBufViolations(againstCommit, violations);
  for (const violation of waived) {
    console.log(githubAnnotation("notice", violation));
  }
  for (const violation of unexpected) {
    console.error(githubAnnotation("error", violation));
  }
  if (unexpected.length !== 0) {
    console.error(
      `Buf FILE compatibility found ${unexpected.length} unexpected violation(s) against ${againstReference} (${againstCommit}).`,
    );
    return 1;
  }
  console.log(
    `Allowed ${waived.length} exact pre-activation protobuf migration(s) against ${againstReference} (${againstCommit}).`,
  );
  return 0;
}

const invokedPath = process.argv[1] === undefined ? "" : path.resolve(process.argv[1]);
if (invokedPath === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = run();
  } catch (error) {
    console.error(`error: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}

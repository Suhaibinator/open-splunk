#!/usr/bin/env node

import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const ownedMarkdownPaths = Object.freeze([
  "README.md",
  "AGENTS.md",
  "CLAUDE.md",
  "docs/README.md",
  "docs/architecture.md",
  "docs/api.md",
  "docs/spl.md",
  "docs/knowledge.md",
  "docs/theming.md",
  "docs/ingestion.md",
  "docs/collector-configuration.md",
  "docs/hec.md",
  "docs/auditing.md",
  "docs/roadmap.md",
  "docs/releasing.md",
  "deploy/README.md",
  "integration/README.md",
  "scripts/README.md",
  "migrations/README.md",
  "internal/hec/testdata/compatibility/README.md",
  "gen/go/README.md",
  "gen/ts/README.md",
]);

const canonicalDocumentationNames = Object.freeze([
  "README.md",
  "architecture.md",
  "api.md",
  "spl.md",
  "knowledge.md",
  "theming.md",
  "ingestion.md",
  "collector-configuration.md",
  "hec.md",
  "auditing.md",
  "roadmap.md",
  "releasing.md",
]);

async function forEachSequential(values, visit) {
  const iterator = values[Symbol.iterator]();
  async function visitNext() {
    const next = iterator.next();
    if (next.done) return;
    await visit(next.value);
    await visitNext();
  }
  await visitNext();
}

const stalePatterns = Object.freeze([
  {
    label: "versioned identifier suffix",
    expression: /\bv0\d+\b/giu,
  },
  {
    label: "versioned protobuf package",
    expression: /\b(?:open_splunk\.v\d+|opensplunkv\d+|open_splunk\/v\d+)\b/giu,
  },
  {
    label: "versioned API route",
    expression: /\/api\/(?:v?\d+(?:\.\d+)*)(?=\/|\b)/giu,
  },
  {
    label: "versioned HEC route",
    expression: /\/services\/collector(?:\/[a-z0-9_-]+)?\/(?:v?\d+(?:\.\d+)*)(?=\/|\b)/giu,
  },
  {
    label: "release-era SPL rule identifier",
    expression: /\bSPL-V\d+-[A-Z0-9-]+\b/gu,
  },
  {
    label: "versioned documentation filename",
    expression: /\b[a-z0-9-]+-v\d+(?:\.\d+)*(?:-[a-z0-9-]+)?\.md\b/giu,
  },
  {
    label: "retired migration baseline filename",
    expression: /\b0004[_-][a-z0-9_.-]+\b/giu,
  },
  {
    label: "retired publication identity",
    expression: /\b(?:OPEN_SPLUNK_APPLICATION_VERSION|application_version|api_version|server_version|spl_compatibility_version)\b/gu,
  },
  {
    label: "public version-floor language",
    expression: /\b(?:format\s+version|migration|revision|catalog\s+revision|state_version|current_version)\s+(?:is\s+|starts?\s+(?:at\s+)?|=\s*)?4\b/giu,
  },
]);

function usage() {
  return "usage: node scripts/check-docs.mjs [--root <repository-root>]";
}

function parseArguments(arguments_) {
  if (arguments_.length === 0) return process.cwd();
  if (arguments_.length === 2 && arguments_[0] === "--root") {
    return path.resolve(arguments_[1]);
  }
  throw new Error(usage());
}

function lineNumberAt(source, index) {
  let line = 1;
  for (let cursor = 0; cursor < index; cursor += 1) {
    if (source.charCodeAt(cursor) === 10) line += 1;
  }
  return line;
}

function markdownLinesOutsideFences(source) {
  const lines = source.split(/\r?\n/u);
  const visible = [];
  let fence = null;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const marker = line.match(/^\s{0,3}(`{3,}|~{3,})/u)?.[1];
    if (marker !== undefined) {
      if (fence === null) fence = marker[0];
      else if (marker[0] === fence) fence = null;
      continue;
    }
    if (fence === null) visible.push({ line, number: index + 1 });
  }
  return visible;
}

function headingSlug(heading) {
  return heading
    .replace(/<[^>]*>/gu, "")
    .replace(/!\[([^\]]*)\]\([^)]*\)/gu, "$1")
    .replace(/\[([^\]]+)\]\([^)]*\)/gu, "$1")
    .replace(/[`*_~]/gu, "")
    .trim()
    .toLocaleLowerCase("en-US")
    .replace(/[^\p{L}\p{M}\p{N}\s_-]/gu, "")
    .replace(/\s+/gu, "-");
}

function markdownAnchors(source) {
  const anchors = new Set();
  const occurrences = new Map();
  for (const { line } of markdownLinesOutsideFences(source)) {
    const match = line.match(/^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$/u);
    if (match === null) continue;
    const base = headingSlug(match[1]);
    if (base.length === 0) continue;
    const occurrence = occurrences.get(base) ?? 0;
    occurrences.set(base, occurrence + 1);
    anchors.add(occurrence === 0 ? base : `${base}-${occurrence}`);
  }
  return anchors;
}

function markdownDestinations(source) {
  const destinations = [];
  const inline = /!?\[[^\]\n]*\]\(\s*(<[^>\n]+>|[^)\s]+)(?:\s+["'][^)\n]*["'])?\s*\)/gu;
  for (const { line, number } of markdownLinesOutsideFences(source)) {
    for (const match of line.matchAll(inline)) {
      destinations.push({ destination: match[1], line: number });
    }
    const reference = line.match(/^\s{0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)/u);
    if (reference !== null) {
      destinations.push({ destination: reference[1], line: number });
    }
  }
  return destinations;
}

function isExternal(destination) {
  return /^[a-z][a-z0-9+.-]*:/iu.test(destination) || destination.startsWith("//");
}

function decodeComponent(value, description) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new Error(`invalid percent encoding in ${description}: ${value}`);
  }
}

function relativeDisplay(root, filename) {
  return path.relative(root, filename).split(path.sep).join("/");
}

async function validateLink(root, sourceFile, link, fileCache, failures) {
  let destination = link.destination;
  if (destination.startsWith("<") && destination.endsWith(">")) {
    destination = destination.slice(1, -1);
  }
  if (destination.length === 0 || isExternal(destination)) return;

  const hash = destination.indexOf("#");
  const pathAndQuery = hash === -1 ? destination : destination.slice(0, hash);
  const fragment = hash === -1 ? "" : destination.slice(hash + 1);
  const question = pathAndQuery.indexOf("?");
  const encodedPath = question === -1 ? pathAndQuery : pathAndQuery.slice(0, question);

  let decodedPath;
  let decodedFragment;
  try {
    decodedPath = decodeComponent(encodedPath, "link path");
    decodedFragment = decodeComponent(fragment, "link fragment");
  } catch (error) {
    failures.push(`${relativeDisplay(root, sourceFile)}:${link.line}: ${error.message}`);
    return;
  }

  const target = decodedPath.length === 0
    ? sourceFile
    : path.resolve(path.dirname(sourceFile), decodedPath);
  const relativeTarget = path.relative(root, target);
  if (relativeTarget === ".." || relativeTarget.startsWith(`..${path.sep}`)) {
    failures.push(`${relativeDisplay(root, sourceFile)}:${link.line}: link escapes repository: ${destination}`);
    return;
  }

  let metadata;
  try {
    metadata = await stat(target);
  } catch (error) {
    if (error?.code === "ENOENT") {
      failures.push(`${relativeDisplay(root, sourceFile)}:${link.line}: missing local link target: ${destination}`);
      return;
    }
    throw error;
  }
  if (!metadata.isFile()) {
    failures.push(`${relativeDisplay(root, sourceFile)}:${link.line}: local link target is not a file: ${destination}`);
    return;
  }

  if (decodedFragment.length === 0) return;
  if (path.extname(target).toLocaleLowerCase("en-US") !== ".md") {
    failures.push(`${relativeDisplay(root, sourceFile)}:${link.line}: fragment targets a non-Markdown file: ${destination}`);
    return;
  }
  let targetSource = fileCache.get(target);
  if (targetSource === undefined) {
    targetSource = await readFile(target, "utf8");
    fileCache.set(target, targetSource);
  }
  if (!markdownAnchors(targetSource).has(decodedFragment)) {
    failures.push(`${relativeDisplay(root, sourceFile)}:${link.line}: missing Markdown anchor: ${destination}`);
  }
}

async function checkDocumentation(root) {
  const failures = [];
  const fileCache = new Map();
  const expectedDocumentationNames = new Set(canonicalDocumentationNames);
  let documentationEntries;
  try {
    documentationEntries = await readdir(path.join(root, "docs"), { withFileTypes: true });
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
    documentationEntries = [];
  }
  for (const entry of documentationEntries) {
    if (!entry.isFile() || path.extname(entry.name).toLocaleLowerCase("en-US") !== ".md") continue;
    if (!expectedDocumentationNames.has(entry.name)) {
      failures.push(`docs/${entry.name}: unexpected non-canonical documentation file`);
    }
  }

  await forEachSequential(ownedMarkdownPaths, async (relativePath) => {
    const filename = path.join(root, relativePath);
    let source;
    try {
      source = await readFile(filename, "utf8");
    } catch (error) {
      if (error?.code === "ENOENT") {
        failures.push(`${relativePath}: required documentation file is missing`);
        return;
      }
      throw error;
    }
    fileCache.set(filename, source);

    for (const pattern of stalePatterns) {
      pattern.expression.lastIndex = 0;
      for (const match of source.matchAll(pattern.expression)) {
        failures.push(
          `${relativePath}:${lineNumberAt(source, match.index)}: ${pattern.label}: ${JSON.stringify(match[0])}`,
        );
      }
    }

    await forEachSequential(markdownDestinations(source), async (link) => {
      await validateLink(root, filename, link, fileCache, failures);
    });
  });
  return failures;
}

let root;
try {
  root = parseArguments(process.argv.slice(2));
} catch (error) {
  process.stderr.write(`${error.message}\n`);
  process.exit(2);
}

const failures = await checkDocumentation(root);
if (failures.length > 0) {
  process.stderr.write(`documentation check failed:\n${failures.map((failure) => `- ${failure}`).join("\n")}\n`);
  process.exit(1);
}
process.stdout.write(`documentation check passed (${ownedMarkdownPaths.length} files)\n`);

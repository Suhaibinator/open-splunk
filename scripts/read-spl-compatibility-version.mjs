#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

if (process.argv.length !== 2) {
  process.stderr.write("usage: node scripts/read-spl-compatibility-version.mjs\n");
  process.exit(2);
}

const sourcePath = path.join(
  process.cwd(),
  "internal",
  "spl",
  "doc.go",
);
const source = await readFile(sourcePath, "utf8");
const declaration = /^const CompatibilityVersion = "([0-9]+\.[0-9]+)"$/gm;
const matches = [...source.matchAll(declaration)];
if (matches.length !== 1) {
  throw new Error(
    `expected exactly one canonical CompatibilityVersion declaration; found ${matches.length}`,
  );
}
process.stdout.write(`${matches[0][1]}\n`);

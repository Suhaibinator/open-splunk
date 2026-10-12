// Snapshot only GoCommon module sources and embedded resources, never local
// credentials, Git internals, node_modules, build output, or test fixtures.
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";

const source = path.resolve(process.argv[2] ?? "../go-common");
const destination = path.resolve("third_party/go-common");
const allowedRoots = ["pkg", "internal", "migrations", "proto"];
const files = ["go.mod", "go.sum", "README.md"];
function visit(directory) {
  for (const entry of readdirSync(path.join(source, directory), { withFileTypes: true }).toSorted((a, b) => a.name.localeCompare(b.name))) {
    const relative = path.posix.join(directory, entry.name);
    if (entry.isDirectory()) visit(relative);
    else if (entry.isFile() && !entry.name.endsWith("_test.go") && !relative.includes("/testdata/") && /\.(?:go|sql|proto)$/u.test(entry.name)) files.push(relative);
  }
}
for (const directory of allowedRoots) if (existsSync(path.join(source, directory))) visit(directory);
const hashes = Object.fromEntries(files.map((file) => [file, createHash("sha256").update(readFileSync(path.join(source, file))).digest("hex")]));
const revision = spawnSync("git", ["rev-parse", "HEAD"], { cwd: source, encoding: "utf8" });
if (revision.status !== 0) throw new Error("Cannot identify the GoCommon checkout");
rmSync(destination, { recursive: true, force: true });
mkdirSync(destination, { recursive: true });
for (const file of files) { const target = path.join(destination, file); mkdirSync(path.dirname(target), { recursive: true }); cpSync(path.join(source, file), target); }
writeFileSync(path.resolve("third_party/go-common-provenance.json"), `${JSON.stringify({ module: "github.com/Suhaibinator/go-common", source: "https://github.com/Suhaibinator/go-common", sourceRevision: revision.stdout.trim(), license: "Upstream Go module has no repository license file; upstream README documents the current scope.", includesLocalIntegrationChanges: true, files: hashes }, null, 2)}\n`);

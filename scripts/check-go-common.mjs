import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";

const root = process.cwd();
const provenance = JSON.parse(readFileSync(path.join(root, "third_party/go-common-provenance.json"), "utf8"));
for (const [file, expected] of Object.entries(provenance.files)) {
  const actual = createHash("sha256").update(readFileSync(path.join(root, "third_party/go-common", file))).digest("hex");
  if (actual !== expected) throw new Error(`GoCommon source differs from its pinned snapshot: ${file}`);
}
process.stdout.write(`GoCommon source snapshot verified (${Object.keys(provenance.files).length} files)\n`);

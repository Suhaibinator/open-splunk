// Rebuild the actual GoCommon frontend client from a supplied source checkout.
// Its Node16 output works in the static Next bundle and the Node test runner.
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";

const repository = process.cwd();
const source = path.resolve(process.argv[2] ?? "../go-common");
const workspace = path.join(source, "typescript");
const temporary = mkdtempSync(path.join(tmpdir(), "open-splunk-go-common-"));
function run(command, args, cwd = workspace) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit", env: { ...process.env, BUF_CACHE_DIR: path.join(repository, ".cache", "buf") } });
  if (result.status !== 0) throw new Error(`${command} failed`);
}
try {
  const template = path.join(temporary, "generate.json");
  writeFileSync(template, JSON.stringify({ version: "v2", plugins: [{ local: path.join(workspace, "node_modules", ".bin", "protoc-gen-ts_proto"), out: path.join(workspace, "packages", "core", "src", "generated"), opt: ["esModuleInterop=true", "forceLong=bigint", "useOptionals=messages", "exportCommonSymbols=false", "snakeToCamel=true", "outputEncodeMethods=true", "outputJsonMethods=true", "outputPartialMethods=true", "stringEnums=false", "useDate=true", "outputIndex=true"], strategy: "all" }] }));
  run(path.join(repository, "node_modules", ".bin", "buf"), ["generate", path.join(source, "proto"), "--template", template]);
  const config = path.join(temporary, "tsconfig.json");
  writeFileSync(config, JSON.stringify({ extends: path.join(workspace, "packages", "core", "tsconfig.json"), compilerOptions: { module: "Node16", moduleResolution: "Node16", typeRoots: [path.join(workspace, "node_modules", "@types")], paths: { "next/headers": [path.join(repository, "node_modules", "next", "headers.d.ts")] } } }));
  run(path.join(workspace, "node_modules", ".bin", "tsc"), ["--project", config]);
  run("npm", ["pack", "--ignore-scripts", "--workspace=@suhaibinator/go-common-core", "--pack-destination", path.join(repository, "third_party", "go-common-core"), "--silent"]);
  const archive = path.join(repository, "third_party", "go-common-core", "suhaibinator-go-common-core-1.0.0.tgz");
  const revision = spawnSync("git", ["rev-parse", "HEAD"], { cwd: source, encoding: "utf8" });
  if (revision.status !== 0) throw new Error("Cannot record GoCommon source revision");
  writeFileSync(path.join(repository, "third_party", "go-common-core", "go-common-core-provenance.json"), `${JSON.stringify({ package: "@suhaibinator/go-common-core", version: "1.0.0", source: "https://github.com/Suhaibinator/go-common", sourceRevision: revision.stdout.trim(), declaredLicense: "ISC", compiler: "Node16", archiveSha256: createHash("sha256").update(readFileSync(archive)).digest("hex") }, null, 2)}\n`);
} finally { rmSync(temporary, { recursive: true, force: true }); }

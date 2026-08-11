import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

const workspace = process.cwd();
const outputDirectory = await mkdtemp(path.join(tmpdir(), "open-splunk-frontend-tests-"));
const testFiles = [
  path.join("app", "admin", "admin-resource-data.test.ts"),
  path.join("app", "admin", "backend-admin-console-hec.test.ts"),
  path.join("app", "admin", "knowledge-manager-data.test.ts"),
  path.join("app", "admin", "knowledge-manager-preview-data.test.ts"),
  path.join("app", "activity", "backend-audit-data.test.ts"),
  path.join("app", "search-workspace", "live-preview.test.ts"),
  path.join("app", "search-workspace", "progress-revision.test.ts"),
  path.join("app", "search-workspace", "categorical-interaction.test.ts"),
  path.join("app", "search-workspace", "time-range.test.ts"),
  path.join("app", "search-workspace", "workspace-utils.test.ts"),
  path.join("app", "search-workspace", "virtual-table.test.ts"),
  path.join("app", "datasets", "index-observability-data.test.ts"),
  path.join("lib", "api", "pagination.test.ts"),
  path.join("lib", "api", "administrator-session.test.ts"),
  path.join("lib", "api", "protobuf-forward-compat.test.ts"),
  path.join("lib", "api", "search-websocket.test.ts"),
  path.join("lib", "api", "system-bootstrap.test.ts"),
  path.join("lib", "search", "backend-data.test.ts"),
  path.join("lib", "search", "server-timeline.test.ts"),
  path.join("lib", "search", "server-exports.test.ts"),
  path.join("lib", "search", "example-drafts.test.ts"),
  path.join("lib", "search", "saved-search-names.test.ts"),
  path.join("lib", "search", "server-inspection.test.ts"),
  path.join("lib", "search", "streamstats-surface.test.ts"),
  path.join("integration", "browser_harness.test.ts"),
];

function run(command, arguments_, environment = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, arguments_, {
      cwd: workspace,
      env: environment,
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited with ${code ?? signal ?? "an unknown status"}`));
    });
  });
}

try {
  await run(process.execPath, [
    "--test",
    path.join(workspace, "scripts", "build-ui-output.test.mjs"),
    path.join(workspace, "scripts", "compile-protos.test.mjs"),
    path.join(workspace, "scripts", "materialize-git-snapshot.test.mjs"),
    path.join(workspace, "scripts", "build-release.test.mjs"),
    path.join(workspace, "scripts", "build-oci.test.mjs"),
  ]);
  const compilerConfig = path.join(outputDirectory, "tsconfig.json");
  await writeFile(compilerConfig, JSON.stringify({
    extends: path.join(workspace, "tsconfig.json"),
    compilerOptions: {
      incremental: false,
      module: "Node16",
      moduleResolution: "Node16",
      noEmit: false,
      outDir: outputDirectory,
      plugins: [],
      rootDir: workspace,
      target: "ES2023",
      typeRoots: [path.join(workspace, "node_modules", "@types")],
      types: ["node"],
    },
    files: testFiles.map((file) => path.join(workspace, file)),
    include: [],
  }));
  await run(path.join(workspace, "node_modules", ".bin", "tsc"), [
    "--project", compilerConfig,
    "--pretty", "false",
  ]);

  const aliasDirectory = path.join(outputDirectory, "node_modules", "@");
  const directoryLinkType = process.platform === "win32" ? "junction" : "dir";
  await mkdir(aliasDirectory, { recursive: true });
  await Promise.all([
    symlink(path.join(outputDirectory, "gen"), path.join(aliasDirectory, "gen"), directoryLinkType),
    symlink(path.join(outputDirectory, "lib"), path.join(aliasDirectory, "lib"), directoryLinkType),
  ]);
  await run(process.execPath, [
    "--test",
    ...testFiles.map((file) => path.join(outputDirectory, file.replace(/\.ts$/, ".js"))),
  ], {
    ...process.env,
    NODE_PATH: path.join(workspace, "node_modules"),
  });
} finally {
  await rm(outputDirectory, { recursive: true, force: true });
}

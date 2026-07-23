import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

const workspace = process.cwd();
const outputDirectory = await mkdtemp(path.join(tmpdir(), "open-splunk-frontend-tests-"));

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
  await run(path.join(workspace, "node_modules", ".bin", "tsc"), [
    "--ignoreConfig",
    "--pretty", "false",
    "--strict", "true",
    "--skipLibCheck", "true",
    "--types", "node",
    "--target", "ES2022",
    "--module", "Node16",
    "--moduleResolution", "Node16",
    "--esModuleInterop", "true",
    "--rootDir", workspace,
    "--outDir", outputDirectory,
    path.join(workspace, "app", "search-workspace", "live-preview.test.ts"),
  ]);
  await run(process.execPath, [
    "--test",
    path.join(outputDirectory, "app", "search-workspace", "live-preview.test.js"),
  ], {
    ...process.env,
    NODE_PATH: path.join(workspace, "node_modules"),
  });
} finally {
  await rm(outputDirectory, { recursive: true, force: true });
}

#!/usr/bin/env node
// Builds the static exports the visual-regression suite renders.
//
// Almost every surface is reachable from the demo data mode, which needs no
// backend at all. The bootstrap-advertised Knowledge Manager only renders in
// backend mode, so a second export is produced beside it and the spec supplies
// its protobuf responses through request interception.
import { cp, mkdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const demoExport = path.join(repository, "out");
export const BACKEND_VISUAL_EXPORT = path.join(repository, ".cache", "visual", "backend-export");

function runBuild(dataMode) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [path.join(repository, "scripts", "build-ui.mjs")],
      {
        cwd: repository,
        env: { ...process.env, OPEN_SPLUNK_DATA_MODE: dataMode },
        stdio: "inherit",
      },
    );
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${dataMode} UI build exited with ${code ?? signal ?? "an unknown status"}`));
    });
  });
}

async function requireExport(directory) {
  const metadata = await stat(path.join(directory, "index.html")).catch(() => null);
  if (metadata === null || !metadata.isFile()) {
    throw new Error(`the UI build produced no index.html in ${directory}`);
  }
}

// The backend export is built first so `out/` is left holding the demo export,
// which is what every other repository workflow expects to find there.
await runBuild("backend");
await requireExport(demoExport);
await rm(BACKEND_VISUAL_EXPORT, { force: true, recursive: true });
await mkdir(path.dirname(BACKEND_VISUAL_EXPORT), { recursive: true });
await cp(demoExport, BACKEND_VISUAL_EXPORT, { recursive: true });

await runBuild("demo");
await requireExport(demoExport);

process.stdout.write(`visual exports ready: ${demoExport} (demo), ${BACKEND_VISUAL_EXPORT} (backend)\n`);

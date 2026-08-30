#!/usr/bin/env node
// Builds the static exports the visual-regression suite renders.
//
// Almost every surface is reachable from the demo data mode, which needs no
// backend at all. The bootstrap-advertised Knowledge Manager only renders in
// backend mode, so a second export is produced beside it and the spec supplies
// its protobuf responses through request interception.
//
// Next always exports into `out/`, which is the directory `webui.go` embeds, so
// each build is moved into `.cache/visual/` and `out/` is restored to the state
// Git tracks. A test target must never leave a release build input holding a
// demo-mode export with no `out/asset-manifest.json`: `go build` would embed it
// silently. `make build-ui` remains the only producer of a release `out/`.
//
// The builds run only when this file is executed. Importing it for one of its
// path constants must not start a six-minute build.
import { cp, mkdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { cleanUIOutput } from "./build-ui-output.mjs";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const nextExport = path.join(repository, "out");
export const DEMO_VISUAL_EXPORT = path.join(repository, ".cache", "visual", "demo-export");
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

/** Builds one data mode and moves the export out of the embedded directory. */
async function buildInto(dataMode, destination) {
  await runBuild(dataMode);
  await requireExport(nextExport);
  await rm(destination, { force: true, recursive: true });
  await mkdir(path.dirname(destination), { recursive: true });
  await cp(nextExport, destination, { recursive: true });
  await requireExport(destination);
}

async function main() {
  await buildInto("backend", BACKEND_VISUAL_EXPORT);
  await buildInto("demo", DEMO_VISUAL_EXPORT);
  // Leaves `out/` holding exactly the tracked `.gitkeep`, so a later `go build`
  // fails on a missing release payload instead of embedding a demo export.
  await cleanUIOutput(nextExport);
  process.stdout.write(
    `visual exports ready: ${DEMO_VISUAL_EXPORT} (demo), ${BACKEND_VISUAL_EXPORT} (backend); `
      + "out/ reset -- run `make build-ui` before building the server\n",
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}

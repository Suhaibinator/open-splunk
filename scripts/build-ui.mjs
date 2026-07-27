import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import { cleanUIOutput } from "./build-ui-output.mjs";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const output = path.join(repository, "out");

await cleanUIOutput(output);

const nextExecutable = path.join(
  repository,
  "node_modules",
  "next",
  "dist",
  "bin",
  "next",
);
const child = spawn(process.execPath, [nextExecutable, "build", ...process.argv.slice(2)], {
  cwd: repository,
  env: process.env,
  stdio: "inherit",
});
child.on("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});
child.on("exit", (code, signal) => {
  if (signal !== null) {
    process.kill(process.pid, signal);
    return;
  }
  process.exitCode = code ?? 1;
});

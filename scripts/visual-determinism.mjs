#!/usr/bin/env node
// Runs the visual suite twice over one build and fails on any difference.
//
// The committed baselines allow a small per-pixel tolerance so a platform's
// antialiasing does not turn the suite red. That same tolerance hides a page
// that paints something slightly different every time, and a baseline set that
// has quietly stopped describing a fixed rendering is a safety net with a hole
// in it. Here both passes render one build through one pair of servers and
// compare with no tolerance at all, so any drift belongs to the page rather
// than to the machine.
//
// The first pass records every screenshot into a temporary directory; the
// second pass compares against what the first recorded, so Playwright names the
// surface that moved and writes the diff image beside it.
//
// Usage:
//   node scripts/visual-determinism.mjs [--skip-build] [--port <number>]
//
// `--skip-build` reuses the exports already in `.cache/visual`, which is what
// you want while iterating on a spec; the default rebuilds so a stale export
// can never be measured.
import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

// `build-visual-exports.mjs` runs its builds only when it is executed, so its
// export paths are imported rather than duplicated here. They must stay equal
// to `DEMO_EXPORT_ROOT` and `BACKEND_EXPORT_ROOT` in
// `integration/visual/visual-servers.ts`, which the visual configuration reads;
// the readiness check below fails loudly if either directory is missing.
import { BACKEND_VISUAL_EXPORT, DEMO_VISUAL_EXPORT } from "./build-visual-exports.mjs";
import { createStaticServer } from "./serve-static.mjs";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Default base port.
 *
 * Deliberately not the visual suite's own default, so a determinism run and an
 * ordinary `npm run test:visual` can be in flight at the same time.
 */
const DEFAULT_PORT = 43380;

export function parseDeterminismArguments(arguments_) {
  const options = { port: DEFAULT_PORT, skipBuild: false };
  for (let index = 0; index < arguments_.length; index += 1) {
    const argument = arguments_[index];
    if (argument === "--skip-build") {
      options.skipBuild = true;
    } else if (argument === "--port") {
      index += 1;
      const port = Number(arguments_[index]);
      if (!Number.isInteger(port) || port < 1 || port > 65533) {
        throw new Error("--port needs an integer between 1 and 65533.");
      }
      options.port = port;
    } else {
      throw new Error(`unknown argument ${argument}; usage: [--skip-build] [--port <number>]`);
    }
  }
  return options;
}

function run(command, arguments_, environment) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, arguments_, {
      cwd: repository,
      env: { ...process.env, ...environment },
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("exit", (code, signal) => resolve(code ?? (signal === null ? 0 : 1)));
  });
}

function listen(root, port) {
  const server = createStaticServer(root);
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve(server));
  });
}

function close(server) {
  return new Promise((resolve) => server.close(resolve));
}

/** Counts the screenshots a pass recorded, so a no-op run cannot look clean. */
export async function countRecordedScreenshots(root) {
  let total = 0;
  const entries = await readdir(root, { recursive: true, withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    if (entry.isFile() && entry.name.endsWith(".png")) total += 1;
  }
  return total;
}

async function requireExport(directory) {
  const metadata = await stat(path.join(directory, "index.html")).catch(() => null);
  if (metadata === null || !metadata.isFile()) {
    throw new Error(
      `${directory} holds no index.html. Drop --skip-build, or run `
        + "`node scripts/build-visual-exports.mjs` first.",
    );
  }
}

async function main() {
  const options = parseDeterminismArguments(process.argv.slice(2));
  if (!options.skipBuild) {
    const built = await run(process.execPath, [path.join(repository, "scripts", "build-visual-exports.mjs")]);
    if (built !== 0) throw new Error(`building the visual exports exited with ${built}`);
  }
  await requireExport(DEMO_VISUAL_EXPORT);
  await requireExport(BACKEND_VISUAL_EXPORT);

  const recorded = await mkdtemp(path.join(tmpdir(), "open-splunk-visual-determinism-"));
  const servers = [
    await listen(DEMO_VISUAL_EXPORT, options.port),
    await listen(BACKEND_VISUAL_EXPORT, options.port + 1),
  ];
  const environment = {
    OPEN_SPLUNK_VISUAL_PORT: String(options.port),
    OPEN_SPLUNK_VISUAL_SNAPSHOT_ROOT: recorded,
  };
  const playwright = path.join(repository, "node_modules", ".bin", "playwright");
  const invocation = ["test", "--config=playwright.visual-determinism.config.ts"];
  try {
    process.stdout.write("visual determinism: recording the first pass\n");
    const first = await run(playwright, [...invocation, "--update-snapshots=all"], environment);
    if (first !== 0) throw new Error(`the recording pass exited with ${first}`);
    const screenshots = await countRecordedScreenshots(recorded);
    if (screenshots === 0) {
      throw new Error("the recording pass captured no screenshots, so the second pass would prove nothing");
    }

    process.stdout.write(`visual determinism: replaying ${screenshots} screenshots exactly\n`);
    const second = await run(playwright, invocation, environment);
    if (second !== 0) {
      process.stderr.write(
        "Two renderings of one build disagree, so the baselines do not pin a fixed appearance.\n"
          + "Pin whatever varies -- a clock, a random value, an animation phase -- rather than\n"
          + "widening a tolerance. The failures above name each surface that moved.\n",
      );
      process.exitCode = 1;
      return;
    }
    process.stdout.write(`visual determinism: ${screenshots} screenshots matched exactly\n`);
  } finally {
    await Promise.all(servers.map((server) => close(server)));
    await rm(recorded, { force: true, recursive: true });
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}

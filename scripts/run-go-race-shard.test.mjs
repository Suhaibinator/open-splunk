import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const workspace = process.cwd();
const shardScript = path.join(workspace, "scripts", "run-go-race-shard.sh");
const catalogPackage = "example.com/open-splunk/internal/knowledgecatalog";

function run(command, args, environment = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: workspace,
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("exit", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

async function fakeGoHarness(t, overrides = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), "open-splunk-race-shard-test-"));
  const executable = path.join(directory, "go");
  const log = path.join(directory, "final-commands.ndjson");
  const source = `#!/usr/bin/env node
const { appendFileSync } = require("node:fs");

const args = process.argv.slice(2);
const fail = process.env.FAKE_GO_FAIL ?? "";
const exact = (...expected) => JSON.stringify(args) === JSON.stringify(expected);
const stop = (name) => {
  if (fail === name) process.exit(9);
};

if (exact("list", "-race", "./internal/knowledgecatalog")) {
  stop("catalog-package");
  process.stdout.write((process.env.FAKE_CATALOG_PACKAGE ?? "") + "\\n");
} else if (exact("list", "-race", "./...")) {
  stop("packages");
  process.stdout.write(process.env.FAKE_PACKAGES ?? "");
} else if (exact("test", "-race", "-list", "^(Test|Example|Fuzz)", "./internal/knowledgecatalog")) {
  stop("runnables");
  const inventory = process.env.FAKE_RUNNABLES ?? "";
  if (inventory.length > 0) process.stdout.write(inventory + "\\n");
  const trailer = process.env.FAKE_DISCOVERY_TRAILER ?? "ok";
  if (trailer === "ok") {
    process.stdout.write("ok  \\t" + (process.env.FAKE_CATALOG_PACKAGE ?? "") + "\\t0.001s\\n");
  } else if (trailer === "cached") {
    process.stdout.write("ok  \\t" + (process.env.FAKE_CATALOG_PACKAGE ?? "") + "\\t(cached)\\n");
  } else if (trailer !== "none") {
    process.stdout.write(trailer + "\\n");
  }
} else if (args[0] === "test") {
  stop("final");
  appendFileSync(process.env.FAKE_GO_LOG, JSON.stringify(args) + "\\n");
} else {
  process.stderr.write("unexpected fake go command: " + JSON.stringify(args) + "\\n");
  process.exit(10);
}
`;
  await writeFile(executable, source, { mode: 0o755 });
  await chmod(executable, 0o755);
  t.after(async () => { await rm(directory, { recursive: true, force: true }); });
  return {
    log,
    environment: {
      ...process.env,
      PATH: `${directory}${path.delimiter}${process.env.PATH}`,
      FAKE_GO_LOG: log,
      FAKE_CATALOG_PACKAGE: catalogPackage,
      FAKE_PACKAGES: [
        "example.com/open-splunk",
        catalogPackage,
        "example.com/open-splunk/internal/zeta",
      ].join("\n") + "\n",
      FAKE_RUNNABLES: "TestAlpha\nTestBeta\nExampleGamma\nFuzzDelta",
      ...overrides,
    },
  };
}

async function readFinalCommands(log) {
  const content = await readFile(log, "utf8");
  return content.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
}

test("core race scope excludes exactly the catalog package", async (t) => {
  const harness = await fakeGoHarness(t);
  const result = await run(shardScript, ["core"], harness.environment);
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /repository_package_count=3/);
  assert.match(result.stdout, /selected_package_count=2/);
  assert.match(result.stdout, new RegExp(`excluded_package=${catalogPackage}`));
  assert.deepEqual(await readFinalCommands(harness.log), [[
    "test",
    "-p=1",
    "-race",
    "-shuffle=on",
    "-count=1",
    "-parallel=2",
    "-timeout=40m",
    "example.com/open-splunk",
    "example.com/open-splunk/internal/zeta",
  ]]);
});

test("catalog race shards reconstruct every safe runnable exactly once", async (t) => {
  const runnables = [
    "TestZulu",
    "FuzzDelta",
    "ExampleGamma",
    "TestAlpha",
    "TestEta",
    "FuzzTheta",
    "ExampleIota",
    "TestBeta",
    "TestKappa",
  ];
  const harness = await fakeGoHarness(t, {
    FAKE_DISCOVERY_TRAILER: "cached",
    FAKE_RUNNABLES: runnables.join("\n"),
  });
  const observed = [];
  await Promise.all(Array.from({ length: 4 }, async (_, index) => {
    const result = await run(shardScript, ["catalog", String(index), "4"], harness.environment);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, new RegExp(`catalog_shard_index=${index}`));
    assert.match(result.stdout, /catalog_shard_count=4/);
    assert.equal((result.stdout.match(/catalog_assignment shard=/g) ?? []).length, runnables.length);
  }));

  const commands = await readFinalCommands(harness.log);
  assert.equal(commands.length, 4);
  for (const command of commands) {
    assert.equal(command.length, 9);
    assert.deepEqual(command.slice(0, 7), [
      "test",
      "-race",
      "-shuffle=on",
      "-count=1",
      "-parallel=2",
      "-timeout=40m",
      "-run",
    ]);
    assert.equal(command[8], "./internal/knowledgecatalog");
    assert.match(command[7], /^\^\([A-Za-z0-9_|]+\)\$$/);
    observed.push(...command[7].slice(2, -2).split("|"));
  }
  assert.deepEqual(observed.toSorted(), runnables.toSorted());
  assert.equal(new Set(observed).size, runnables.length);
});

test("race shard argument bounds reject before invoking Go", async () => {
  const invalid = [
    [],
    ["unknown"],
    ["core", "extra"],
    ["catalog"],
    ["catalog", "-1", "4"],
    ["catalog", "01", "4"],
    ["catalog", "4", "4"],
    ["catalog", "0", "1"],
    ["catalog", "0", "17"],
  ];
  await Promise.all(invalid.map(async (args) => {
    const result = await run(shardScript, args, { ...process.env, PATH: "/usr/bin:/bin" });
    assert.notEqual(result.code, 0, `unexpected success for ${JSON.stringify(args)}`);
  }));
});

test("race shard discovery fails closed on missing and malformed inventory", async (t) => {
  const scenarios = [
    { args: ["core"], overrides: { FAKE_GO_FAIL: "packages" } },
    { args: ["core"], overrides: { FAKE_PACKAGES: `${catalogPackage}\n${catalogPackage}\n` } },
    { args: ["core"], overrides: { FAKE_PACKAGES: "example.com/open-splunk\n" } },
    { args: ["core"], overrides: { FAKE_PACKAGES: `${catalogPackage}\n` } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_GO_FAIL: "runnables" } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: "" } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: "TestAlpha\nTestAlpha\nTestBeta\nTestGamma" } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: "TestAlpha\nunsafe-output\nTestBeta\nTestGamma" } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: "TestAlpha\nTestBeta\nTestGamma" } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: "TestAlpha\nTestBeta\nTestGamma\nTestDelta", FAKE_DISCOVERY_TRAILER: "none" } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: "TestAlpha\nTestBeta\nTestGamma\nTestDelta", FAKE_DISCOVERY_TRAILER: `ok  \t${catalogPackage}\tbanana` } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: `TestAlpha\nTestBeta\nTestGamma\nTestDelta\nok  \t${catalogPackage}\t0.001s` } },
    { args: ["catalog", "0", "4"], overrides: { FAKE_RUNNABLES: `TestAlpha\nTestBeta\nok  \t${catalogPackage}\t0.001s\nTestGamma\nTestDelta` } },
  ];
  await Promise.all(scenarios.map(async (scenario) => {
    const harness = await fakeGoHarness(t, scenario.overrides);
    const result = await run(shardScript, scenario.args, harness.environment);
    assert.notEqual(result.code, 0, `unexpected success for ${JSON.stringify(scenario)}`);
  }));
});

test("race shard propagates the selected Go test command failure", async (t) => {
  const harness = await fakeGoHarness(t, { FAKE_GO_FAIL: "final" });
  const result = await run(shardScript, ["catalog", "0", "4"], harness.environment);
  assert.equal(result.code, 9);
});

test("CI declares one core row and every catalog shard exactly once", async () => {
  const workflow = await readFile(path.join(workspace, ".github", "workflows", "ci.yml"), "utf8");
  const goTestStart = workflow.indexOf("  go-test:\n");
  const goTestEnd = workflow.indexOf("\n  knowledge-object-fuzz:\n", goTestStart);
  assert.ok(goTestStart >= 0 && goTestEnd > goTestStart);
  const goTestWorkflow = workflow.slice(goTestStart, goTestEnd);
  const rows = [];
  const rowPattern = /^ {10}- label: (.+)\n((?:^ {12}.+\n)*)/gm;
  for (const match of goTestWorkflow.matchAll(rowPattern)) {
    const properties = Object.fromEntries(match[2].trim().split("\n").filter(Boolean).map((line) => {
      const separator = line.indexOf(":");
      return [line.slice(0, separator).trim(), line.slice(separator + 1).trim()];
    }));
    rows.push({ label: match[1], ...properties });
  }

  const core = rows.filter((row) => row.mode === "race-core");
  assert.equal(core.length, 1);
  assert.equal(core[0].job_timeout_minutes, "90");
  const catalog = rows.filter((row) => row.mode === "race-catalog");
  assert.equal(catalog.length, 4);
  assert.deepEqual(catalog.map((row) => Number(row.shard_index)).toSorted(), [0, 1, 2, 3]);
  assert.ok(catalog.every((row) => row.shard_count === "4"));
  assert.ok(catalog.every((row) => row.job_timeout_minutes === "45"));
  assert.equal(rows.filter((row) => row.mode === "coverage").length, 1);
  assert.match(goTestWorkflow, /strategy:\n {6}fail-fast: false/);
  assert.match(goTestWorkflow, /scripts\/run-go-race-shard\.sh core/);
  assert.match(goTestWorkflow, /scripts\/run-go-race-shard\.sh catalog/);
  assert.equal((goTestWorkflow.match(/GOMAXPROCS: "2"/g) ?? []).length, 2);
  assert.match(workflow, /build:\n(?:.|\n)*?needs:\n(?: {6}- .+\n)* {6}- go-test\n/);
});

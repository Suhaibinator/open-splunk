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

async function assertNoFinalCommand(log) {
  try {
    assert.deepEqual(await readFinalCommands(log), []);
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}

const coreCommandPrefix = [
  "test",
  "-p=1",
  "-race",
  "-shuffle=on",
  "-count=1",
  "-parallel=2",
  "-timeout=40m",
];

test("core race shards reconstruct an unsorted package inventory exactly once", async (t) => {
  const corePackages = [
    "example.com/open-splunk/internal/zulu",
    "example.com/open-splunk",
    "example.com/open-splunk/internal/alpha",
    "example.com/open-splunk/cmd/server",
    "example.com/open-splunk/internal/eta",
    "example.com/open-splunk/internal/bravo",
    "example.com/open-splunk/internal/theta",
    "example.com/open-splunk/internal/delta",
    "example.com/open-splunk/internal/gamma",
  ];
  const harness = await fakeGoHarness(t, {
    FAKE_PACKAGES: [
      corePackages[0],
      catalogPackage,
      ...corePackages.slice(1),
    ].join("\n") + "\n",
  });
  const sorted = corePackages.toSorted();
  const results = await Promise.all(Array.from({ length: 4 }, (_, index) =>
    run(shardScript, ["core", String(index), "4"], harness.environment)));

  for (const [index, result] of results.entries()) {
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, /repository_package_count=10/);
    assert.match(result.stdout, /core_package_count=9/);
    assert.match(result.stdout, new RegExp(`core_shard_index=${index}`));
    assert.match(result.stdout, /core_shard_count=4/);
    assert.match(result.stdout, new RegExp(`excluded_package=${catalogPackage}`));
    const assignments = [...result.stdout.matchAll(/^core_assignment shard=(\d+) package=(.+)$/gm)]
      .map((match) => ({ shard: Number(match[1]), package: match[2] }));
    assert.deepEqual(assignments, sorted.map((packageName, ordinal) => ({
      shard: ordinal % 4,
      package: packageName,
    })));
  }

  const commands = await readFinalCommands(harness.log);
  assert.equal(commands.length, 4);
  const observed = [];
  for (const command of commands) {
    assert.deepEqual(command.slice(0, coreCommandPrefix.length), coreCommandPrefix);
    assert.ok(!command.includes("-run"), "core package shards must execute whole packages");
    observed.push(...command.slice(coreCommandPrefix.length));
  }
  assert.deepEqual(observed.toSorted(), sorted);
  assert.equal(new Set(observed).size, sorted.length);

  const expectedCommands = Array.from({ length: 4 }, (_, shard) => [
    ...coreCommandPrefix,
    ...sorted.filter((_, ordinal) => ordinal % 4 === shard),
  ]).toSorted((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
  assert.deepEqual(
    commands.toSorted((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right))),
    expectedCommands,
  );
});

test("core shard selection is stable across discovery order permutations", async (t) => {
  const packages = [
    "example.com/open-splunk/internal/charlie",
    "example.com/open-splunk/internal/alpha",
    "example.com/open-splunk/internal/echo",
    "example.com/open-splunk/internal/bravo",
    "example.com/open-splunk/internal/foxtrot",
    "example.com/open-splunk/internal/delta",
    "example.com/open-splunk/internal/golf",
    "example.com/open-splunk/internal/hotel",
  ];
  const first = await fakeGoHarness(t, {
    FAKE_PACKAGES: [catalogPackage, ...packages].join("\n") + "\n",
  });
  const second = await fakeGoHarness(t, {
    FAKE_PACKAGES: [...packages.toReversed(), catalogPackage].join("\n") + "\n",
  });
  const [firstResult, secondResult] = await Promise.all([
    run(shardScript, ["core", "2", "4"], first.environment),
    run(shardScript, ["core", "2", "4"], second.environment),
  ]);
  assert.equal(firstResult.code, 0, firstResult.stderr);
  assert.equal(secondResult.code, 0, secondResult.stderr);
  assert.deepEqual(await readFinalCommands(first.log), await readFinalCommands(second.log));
});

test("a newly discovered core package is assigned to exactly one shard", async (t) => {
  const futurePackage = "example.com/open-splunk/internal/futurepackage";
  const packages = [
    "example.com/open-splunk/internal/alpha",
    "example.com/open-splunk/internal/bravo",
    "example.com/open-splunk/internal/charlie",
    "example.com/open-splunk/internal/delta",
    futurePackage,
    "example.com/open-splunk/internal/golf",
  ];
  const harness = await fakeGoHarness(t, {
    FAKE_PACKAGES: [...packages.toReversed(), catalogPackage].join("\n") + "\n",
  });
  await Promise.all(Array.from({ length: 4 }, async (_, index) => {
    const result = await run(shardScript, ["core", String(index), "4"], harness.environment);
    assert.equal(result.code, 0, result.stderr);
  }));
  const occurrences = (await readFinalCommands(harness.log))
    .flatMap((command) => command.slice(coreCommandPrefix.length))
    .filter((packageName) => packageName === futurePackage);
  assert.deepEqual(occurrences, [futurePackage]);
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
    { args: [], code: 64 },
    { args: ["unknown"], code: 64 },
    { args: ["core"], code: 64 },
    { args: ["core", "0"], code: 64 },
    { args: ["core", "0", "4", "extra"], code: 64 },
    { args: ["core", "-1", "4"], code: 65 },
    { args: ["core", "01", "4"], code: 65 },
    { args: ["core", "4", "4"], code: 65 },
    { args: ["core", "0", "1"], code: 65 },
    { args: ["core", "0", "17"], code: 65 },
    { args: ["core", "18446744073709551620", "4"], code: 65 },
    { args: ["core", "0", "18446744073709551620"], code: 65 },
    { args: ["catalog"], code: 64 },
    { args: ["catalog", "-1", "4"], code: 65 },
    { args: ["catalog", "01", "4"], code: 65 },
    { args: ["catalog", "4", "4"], code: 65 },
    { args: ["catalog", "0", "1"], code: 65 },
    { args: ["catalog", "0", "17"], code: 65 },
    { args: ["catalog", "18446744073709551620", "4"], code: 65 },
    { args: ["catalog", "0", "18446744073709551620"], code: 65 },
  ];
  await Promise.all(invalid.map(async ({ args, code }) => {
    const result = await run(shardScript, args, { ...process.env, PATH: "/usr/bin:/bin" });
    assert.equal(result.code, code, `wrong rejection for ${JSON.stringify(args)}: ${result.stderr}`);
  }));
});

test("race shard discovery fails closed on missing and malformed inventory", async (t) => {
  const scenarios = [
    { args: ["core", "0", "4"], overrides: { FAKE_GO_FAIL: "catalog-package" } },
    { args: ["core", "0", "4"], overrides: { FAKE_GO_FAIL: "packages" } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: "" } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: `${catalogPackage}\nunsafe package\n` } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: `${catalogPackage}\nexample.com/open-splunk\nexample.com/open-splunk\n` } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: `${catalogPackage}\n${catalogPackage}\nexample.com/open-splunk\n` } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: "example.com/open-splunk\nexample.com/open-splunk/internal/alpha\nexample.com/open-splunk/internal/bravo\nexample.com/open-splunk/internal/charlie\n" } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: `${catalogPackage}\n` } },
    { args: ["core", "0", "4"], overrides: { FAKE_PACKAGES: `${catalogPackage}\nexample.com/open-splunk\nexample.com/open-splunk/internal/alpha\nexample.com/open-splunk/internal/bravo\n` } },
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
    await assertNoFinalCommand(harness.log);
  }));
});

test("race shard propagates the selected Go test command failure", async (t) => {
  const [core, catalog] = await Promise.all([
    (async () => {
      const harness = await fakeGoHarness(t, {
        FAKE_GO_FAIL: "final",
        FAKE_PACKAGES: `${catalogPackage}\nexample.com/open-splunk/a\nexample.com/open-splunk/b\nexample.com/open-splunk/c\nexample.com/open-splunk/d\n`,
      });
      return run(shardScript, ["core", "0", "4"], harness.environment);
    })(),
    (async () => {
      const harness = await fakeGoHarness(t, { FAKE_GO_FAIL: "final" });
      return run(shardScript, ["catalog", "0", "4"], harness.environment);
    })(),
  ]);
  assert.equal(core.code, 9);
  assert.equal(catalog.code, 9);
});

test("CI declares every core and catalog shard exactly once", async () => {
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
  assert.equal(core.length, 4);
  assert.deepEqual(core.map((row) => Number(row.shard_index)).toSorted(), [0, 1, 2, 3]);
  assert.ok(core.every((row) => row.shard_count === "4"));
  assert.ok(core.every((row) => row.job_timeout_minutes === "45"));
  const catalog = rows.filter((row) => row.mode === "race-catalog");
  assert.equal(catalog.length, 4);
  assert.deepEqual(catalog.map((row) => Number(row.shard_index)).toSorted(), [0, 1, 2, 3]);
  assert.ok(catalog.every((row) => row.shard_count === "4"));
  assert.ok(catalog.every((row) => row.job_timeout_minutes === "45"));
  assert.equal(rows.filter((row) => row.mode === "coverage").length, 1);
  assert.equal(rows.length, 9);
  assert.match(goTestWorkflow, /strategy:\n {6}fail-fast: false/);
  assert.match(goTestWorkflow, /strategy:\n {6}fail-fast: false\n {6}max-parallel: 5/);
  assert.match(goTestWorkflow, /scripts\/run-go-race-shard\.sh core\n {10}\$\{\{ matrix\.shard_index \}\}\n {10}\$\{\{ matrix\.shard_count \}\}/);
  assert.doesNotMatch(goTestWorkflow, /^ {8}run: scripts\/run-go-race-shard\.sh core$/m);
  assert.match(goTestWorkflow, /scripts\/run-go-race-shard\.sh catalog/);
  assert.equal((goTestWorkflow.match(/GOMAXPROCS: "2"/g) ?? []).length, 2);
  assert.match(workflow, /build:\n(?:.|\n)*?needs:\n(?: {6}- .+\n)* {6}- go-test\n/);
});

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

const workspace = process.cwd();

function workflowJob(source, name) {
  const startMarker = `  ${name}:\n`;
  const start = source.indexOf(startMarker);
  assert.ok(start >= 0, `missing ${name} CI job`);
  const remainder = source.slice(start + startMarker.length);
  const nextMatch = /\n  [a-z][a-z0-9-]*:\n/.exec(remainder);
  const end = nextMatch === null
    ? source.length
    : start + startMarker.length + nextMatch.index;
  return source.slice(start, end);
}

test("terminal CI requires every pinned SPL v0.3 ClickHouse vertical", async () => {
  const workflow = await readFile(
    path.join(workspace, ".github", "workflows", "ci.yml"),
    "utf8",
  );
  const job = workflowJob(workflow, "spl-compatibility-clickhouse");
  assert.match(
    job,
    /uses: actions\/checkout@v7\n\s+with:\n\s+fetch-depth: 0/,
    "SPL compatibility qualification must retain the complete R/E prerequisite history",
  );
  assert.match(job, /git log -1 --format=%H -- "\$manifest_path"/);
  assert.match(
    job,
    /--phase accepted \\\n\s+--evidence-revision "\$evidence_revision" \\\n\s+--print-evidence-binding/,
    "accepted compatibility checks must replay exact E instead of treating a descendant as E",
  );

  assert.match(
    job,
    /OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE: clickhouse\/clickhouse-server:26\.3\.17\.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49/,
  );
  const singletonTests = [
    ["./internal/clickhouse", "TestV03AdversarialAgainstClickHouse"],
    ["./internal/export", "TestV03PinnedClickHousePublishesNullableListsThroughPagingAndExport"],
    ["./internal/clickhouse", "TestV03PinnedClickHouseRetainedTupleHasCanonicalBytes"],
  ];
  for (const [, name] of singletonTests) {
    assert.ok(job.includes(`-run '^${name}$'`), `missing exact ${name} CI run`);
  }
  const fillNullTests = [
    "TestV03FillNullDynamicThenMVExpandAgainstClickHouse",
    "TestV03FillNullAfterPrivatePhysicalProducersAgainstClickHouse",
  ];
  assert.ok(
    job.includes(`-run '^(${fillNullTests.join("|")})$'`),
    "missing exact fillnull materialization composition CI run",
  );
  const semanticTests = [
    "TestSemanticBytesLineageManagerAgainstClickHouse",
    "TestV03AllTenPreserveUntouchedSemanticBytesThroughManagerAgainstClickHouse",
  ];
  assert.ok(
    job.includes(
      `-run '^(${semanticTests.join("|")})$'`,
    ),
    "missing exact semantic String/Bytes manager CI run",
  );
  assert.match(job, /name: Verify exact SPL compatibility ClickHouse test inventory/);
  assert.match(job, /if: env\.OPEN_SPLUNK_SPL_COMPATIBILITY_VERSION == '0\.3'/);
  assert.ok(
    job.includes('output="$(go test "$package" -list "^${name}$")"'),
    "v0.3 inventory must use go test -list before live execution",
  );
  assert.match(job, /if \[\[ "\$match_count" -ne 1 \]\]/);
  const inventory = [
    ...singletonTests,
    ...fillNullTests.map((name) => ["./internal/clickhouse", name]),
    ["./internal/queryexec", "TestSemanticBytesV02ManagerAgainstClickHouse"],
    ["./internal/queryexec", "TestSemanticBytesModeManagerAgainstClickHouse"],
    ["./internal/queryexec", "TestSparklineFeedsStatsByThroughManagerAgainstClickHouse"],
    ...semanticTests.map((name) => ["./internal/queryexec", name]),
  ];
  for (const [packageName, name] of inventory) {
    assert.ok(
      job.includes(`${packageName} ${name}`),
      `missing fail-closed discovery for ${packageName}.${name}`,
    );
  }
  assert.equal(
    (job.match(/OPEN_SPLUNK_CLICKHOUSE_INTEGRATION: "1"/g) ?? []).length,
    6,
  );
  assert.equal((job.match(/-p=1/g) ?? []).length, 6);

  const build = workflowJob(workflow, "build");
  assert.match(build, /^ {6}- spl-compatibility-clickhouse$/m);
  assert.match(
    build,
    /compatibility_version="\$\(node scripts\/read-spl-compatibility-version\.mjs\)"/,
  );
  assert.match(
    build,
    /OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=\$compatibility_version/,
  );
  for (const jobName of ["release-oci", "build"]) {
    const releaseJob = workflowJob(workflow, jobName);
    assert.match(releaseJob, /0\.2\) application_version=0\.1\.0/);
    assert.match(releaseJob, /0\.3\) application_version=0\.2\.0/);
    assert.match(
      releaseJob,
      /OPEN_SPLUNK_APPLICATION_VERSION=\$application_version/,
      `${jobName} must bind the release application identity from authored SPL compatibility`,
    );
  }
  assert.doesNotMatch(
    workflowJob(workflow, "release-oci"),
    /OPEN_SPLUNK_APPLICATION_VERSION:\s*0\.1\.0/,
    "release OCI qualification must not silently hard-code the v0.2 application identity",
  );
  assert.ok(
    build.includes('"application_version=${OPEN_SPLUNK_APPLICATION_VERSION}"'),
    "production binary CI must assert the exact application identity",
  );
  assert.ok(
    build.includes(
      '"spl_compatibility_version=${OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION}"',
    ),
    "production binary CI must assert the exact expected SPL identity",
  );
});

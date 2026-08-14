import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const workflow = await readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");

test("backend vertical executes the complete v0.2 multivalue stats-BY evidence", () => {
  const jobStart = workflow.indexOf("  backend-vertical:");
  const jobEnd = workflow.indexOf("\n  go-vulnerability:", jobStart);
  assert.ok(jobStart >= 0 && jobEnd > jobStart, "backend-vertical job is missing");
  const job = workflow.slice(jobStart, jobEnd);

  assert.match(job, /OPEN_SPLUNK_CLICKHOUSE_INTEGRATION:\s*"1"/);
  assert.match(job, /StatsByDeferredValidationAdversarialAgainstClickHouse/);
  assert.match(job, /StatsMultivalueByAgainstClickHouse/);
  assert.match(job, /StatsMultivalueByExpansionLimitAgainstClickHouse/);
  assert.match(job, /StatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse/);
  assert.match(job, /StatsByFixedMultivalueStringOrBytesAgainstClickHouse/);
  assert.match(job, /-p=1/);
  assert.match(job, /name: Verify exact SPL v0\.2 stats-BY test inventory/);
  assert.ok(
    job.includes('output="$(go test "$package" -list "^${name}$")"'),
    "stats-BY inventory must use go test -list before live execution",
  );
  assert.match(job, /if \[\[ "\$match_count" -ne 1 \]\]/);
  for (const [packageName, name] of [
    ["./internal/clickhouse", "TestStatsByDeferredValidationAdversarialAgainstClickHouse"],
    ["./internal/clickhouse", "TestStatsMultivalueByAgainstClickHouse"],
    ["./internal/clickhouse", "TestStatsMultivalueByExpansionLimitAgainstClickHouse"],
    ["./internal/clickhouse", "TestStatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse"],
    ["./internal/queryexec", "TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse"],
  ]) {
    assert.ok(
      job.includes(`${packageName} ${name}`),
      `missing fail-closed discovery for ${packageName}.${name}`,
    );
  }
  assert.match(
    workflow,
    /OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE:\s*clickhouse\/clickhouse-server:26\.3\.17\.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49/,
  );
});

test("production binaries remain gated on the backend vertical", () => {
  const buildStart = workflow.indexOf("  build:");
  assert.ok(buildStart >= 0, "production build job is missing");
  const build = workflow.slice(buildStart);
  assert.match(build, /needs:[\s\S]*?- backend-vertical/);
  assert.match(build, /needs:[\s\S]*?- spl-compatibility-clickhouse/);
  assert.match(build, /0\.2\) application_version=0\.1\.0/);
  assert.match(build, /0\.3\) application_version=0\.2\.0/);
  assert.match(build, /OPEN_SPLUNK_APPLICATION_VERSION=\$application_version/);
  assert.ok(build.includes('"application_version=${OPEN_SPLUNK_APPLICATION_VERSION}"'));

  const releaseStart = workflow.indexOf("  release-oci:");
  const releaseEnd = workflow.indexOf("\n  go-vulnerability:", releaseStart);
  assert.ok(releaseStart >= 0 && releaseEnd > releaseStart, "release-oci job is missing");
  const releaseOCI = workflow.slice(releaseStart, releaseEnd);
  assert.match(releaseOCI, /0\.2\) application_version=0\.1\.0/);
  assert.match(releaseOCI, /0\.3\) application_version=0\.2\.0/);
  assert.match(releaseOCI, /OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=\$compatibility_version/);
  assert.match(releaseOCI, /OPEN_SPLUNK_APPLICATION_VERSION=\$application_version/);
  assert.doesNotMatch(releaseOCI, /OPEN_SPLUNK_APPLICATION_VERSION:\s*0\.1\.0/);
});

test("stable SPL compatibility job qualifies v0.2 stats-BY and semantic Bytes", () => {
  const jobStart = workflow.indexOf("  spl-compatibility-clickhouse:");
  const jobEnd = workflow.indexOf("\n  hec-durable-load:", jobStart);
  assert.ok(jobStart >= 0 && jobEnd > jobStart,
    "spl-compatibility-clickhouse job is missing");
  const job = workflow.slice(jobStart, jobEnd);
  assert.match(job, /name: SPL compatibility pinned ClickHouse verticals/);
  assert.match(job, /compatibility_version="\$\(node scripts\/read-spl-compatibility-version\.mjs\)"/);
  assert.match(job, /0\.2\|0\.3/);
  assert.match(job, /TestStatsByDeferredValidationAdversarialAgainstClickHouse/);
  assert.match(job, /TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse/);
  assert.match(job, /TestSemanticBytesV02ManagerAgainstClickHouse/);
  assert.match(job, /TestSemanticBytesLineageManagerAgainstClickHouse/);
  assert.match(job, /TestSemanticBytesModeManagerAgainstClickHouse/);
  assert.match(job, /TestSparklineFeedsStatsByThroughManagerAgainstClickHouse/);
  const baseline = job.split("-run '^(TestStatsByDeferredValidation")[1]
    .split("-count=1")[0];
  assert.match(baseline, /SemanticBytesV02Manager/);
  assert.doesNotMatch(baseline, /SemanticBytesLineageManager/);
  assert.match(baseline, /SparklineFeedsStatsBy/);
  assert.match(job, /if \[\[ "\$OPEN_SPLUNK_SPL_COMPATIBILITY_VERSION" = "0\.3" \]\]/);
  assert.match(job, /if: env\.OPEN_SPLUNK_SPL_COMPATIBILITY_VERSION == '0\.3'/);
});

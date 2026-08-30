import assert from "node:assert/strict";
import test from "node:test";

import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { SearchJobOrigin } from "@/gen/ts/open_splunk/search";

import { getExactRetainedSearchJob, serverSearchJobOriginLabel } from "./server-jobs";

test("exact retained-job restoration reads the linked job without creating a replacement", async () => {
  const reads: unknown[] = [];
  const client = {
    search: {
      get: async (request: unknown) => {
        reads.push(request);
        return {
          searchJob: {
            searchJobId: "job-exact",
            definition: { spl: "index=main | stats count" },
          },
        };
      },
    },
  } as unknown as OpenSplunkApiClient;

  const job = await getExactRetainedSearchJob(client, " job-exact ");

  assert.equal(job.searchJobId, "job-exact");
  assert.deepEqual(reads, [{
    searchJobId: "job-exact",
    includePlan: false,
    includeGeneratedSql: false,
  }]);
});

test("exact retained-job restoration rejects a mismatched server identity", async () => {
  const client = {
    search: {
      get: async () => ({
        searchJob: {
          searchJobId: "job-other",
          definition: { spl: "index=main" },
        },
      }),
    },
  } as unknown as OpenSplunkApiClient;

  await assert.rejects(
    getExactRetainedSearchJob(client, "job-exact"),
    /invalid retained search job/u,
  );
});

test("search job origins distinguish explicit ad hoc jobs from unknown sources", () => {
  assert.equal(serverSearchJobOriginLabel({
    origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC,
  }), "Ad hoc search");
  assert.equal(serverSearchJobOriginLabel({
    origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_SCHEDULED_REPORT,
  }), "Scheduled report");
  assert.equal(serverSearchJobOriginLabel({
    origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_ALERT,
  }), "Alert");
  assert.equal(serverSearchJobOriginLabel({
    origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_UNSPECIFIED,
  }), "Unknown");
  assert.equal(serverSearchJobOriginLabel(null), "Unknown");
});

import assert from "node:assert/strict";
import test from "node:test";

import {
  RetainedResultStatus,
  SearchJobRetentionClass,
  SearchJobVisibility,
} from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { SystemBootstrapModel } from "@/lib/api/system-bootstrap";

import { shareSearchJobForLink } from "./use-search-sharing";

test("job-link sharing atomically uses the returned shared job identity and revision", async () => {
  const requests: unknown[] = [];
  const client = {
    search: {
      share: async (request: unknown) => {
        requests.push(request);
        return {
          searchJob: {
            searchJobId: "job-1",
            stateVersion: 8n,
            visibility: SearchJobVisibility.SEARCH_JOB_VISIBILITY_EVERYONE,
            retentionClass: SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_SHARED,
            retentionLifetime: { seconds: 604_800n, nanos: 0 },
            retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_AVAILABLE,
          },
        };
      },
    },
  } as unknown as OpenSplunkApiClient;
  const bootstrap = {
    features: new Set([ServerFeature.SERVER_FEATURE_DURABLE_SEARCH_JOBS]),
  } as unknown as SystemBootstrapModel;

  const result = await shareSearchJobForLink(
    client,
    bootstrap,
    " job-1 ",
    7n,
    "https://splunk.example.test/base",
  );

  assert.deepEqual(requests, [{ searchJobId: "job-1", expectedStateVersion: 7n }]);
  assert.equal(result.settings.visibility, "everyone");
  assert.equal(result.settings.lifetimeMs, 7 * 24 * 60 * 60 * 1_000);
  assert.equal(result.href, "https://splunk.example.test/search/events/?searchJobId=job-1&run=0");
});

import assert from "node:assert/strict";
import test from "node:test";
import { ExportJob, ExportJobState } from "@/gen/ts/open_splunk/export";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { SystemBootstrapModel } from "@/lib/api/system-bootstrap";
import { clearIdentitySession, publishIdentity } from "@/lib/api/identity-session";
import { downloadServerExport, nativeExportDownloadUrl } from "./server-exports";

test.afterEach(clearIdentitySession);
test("native artifact downloads preserve cookie organization context without fetching the body", async () => {
  publishIdentity({ status: "authenticated", user: { id: 1n, email: "a@example.com", name: "A", userType: 1 }, organizations: [{ id: 9n, name: "Org", role: 1 }], organizationId: 9n, error: null });
  let requestedGrant: boolean | undefined;
  const job = ExportJob.fromPartial({ exportJobId: "export/one", state: ExportJobState.EXPORT_JOB_STATE_COMPLETED, artifact: { fileName: "results.csv", mediaType: "text/csv", sizeBytes: 100_000_000_000n, rowCount: 9_000_000n } });
  const client = { exports: { get: async (request: { issueDownloadGrant: boolean }) => { requestedGrant = request.issueDownloadGrant; return { exportJob: job }; } } } as unknown as OpenSplunkApiClient;
  const bootstrap = { features: new Set([ServerFeature.SERVER_FEATURE_EXPORT_CSV]) } as unknown as SystemBootstrapModel;
  let navigation: string | null = null;
  const response = await downloadServerExport(client, bootstrap, job.exportJobId, { apiBaseUrl: "https://splunk.example", navigate: (url) => { navigation = url; } });
  assert.equal(response.status, "available");
  assert.equal(requestedGrant, false);
  assert.equal(navigation, "https://splunk.example/api/search/exports/download?export_job_id=export%2Fone&organization_id=9");
  if (response.status === "available") {
    assert.equal(response.value.sizeBytes, 100_000_000_000n);
    assert.equal("blob" in response.value, false);
  }
});

test("native download links encode IDs and never contain authentication tokens", () => {
  const url = new URL(nativeExportDownloadUrl("job?a&b", "https://splunk.example"));
  assert.equal(url.searchParams.get("export_job_id"), "job?a&b");
  assert.equal(url.searchParams.has("download_token"), false);
  assert.equal(url.username, "");
});

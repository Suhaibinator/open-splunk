import assert from "node:assert/strict";
import test from "node:test";

import { SharingScope } from "../../gen/ts/open_splunk/common";
import { ScheduledSearchOutcome } from "../../gen/ts/open_splunk/saved_search";
import {
  ScheduleValidationCode,
  ScheduleValidationField,
  ScheduleValidationMode,
} from "../../gen/ts/open_splunk/schedule_api";
import { RetainedResultStatus, SearchResultTab } from "../../gen/ts/open_splunk/search";
import { ServerFeature } from "../../gen/ts/open_splunk/system_api";
import { OpenSplunkApiClient } from "../api/open-splunk-client";
import type { ProtobufTransport } from "../api/protobuf-transport";
import type { SystemBootstrapModel } from "../api/system-bootstrap";
import {
  adaptScheduledReportRun,
  scheduledReportRetainedResultPresentation,
  scheduledReportResultExpired,
  scheduledReportOutcomePresentation,
  setServerSavedSearchSchedule,
  validateServerSchedule,
  validateScheduledReportConfiguration,
} from "./server-scheduled-reports";
import { adaptSavedSearch, type ServerSavedSearch } from "./server-objects";

function scheduledReportsBootstrap(): SystemBootstrapModel {
  return {
    build: null,
    searchWebsocketPath: null,
    features: new Set([ServerFeature.SERVER_FEATURE_SCHEDULED_SEARCHES]),
    limits: {
      maximumPageSize: 100,
      maximumPreviewRows: 100,
      maximumWebsocketSubscriptions: 4,
      maximumWebsocketFrameBytes: 1_048_576n,
      maximumExportRows: 10_000n,
      maximumExportBytes: 10_000_000n,
      defaultSearchTimeoutMs: 30_000,
      searchResultRetentionMs: 600_000,
      maximumTimelineBuckets: 100,
      maximumFieldSummaryValues: 10,
    },
    apps: [],
    indexes: [],
    selectedAppId: null,
    serverTime: new Date("2026-08-29T12:00:00.000Z"),
    palette: "classic",
  };
}

test("scheduled report local validation only enforces required fields", () => {
  assert.equal(validateScheduledReportConfiguration({ enabled: true, cron: "*/5 * * * *", timezone: "America/Los_Angeles", dispatchTtl: "2p" }), null);
  assert.match(validateScheduledReportConfiguration({ enabled: true, cron: "", timezone: "UTC", dispatchTtl: "2p" }) ?? "", /required/);
  assert.match(validateScheduledReportConfiguration({ enabled: true, cron: "0 9 * * *", timezone: "", dispatchTtl: "2p" }) ?? "", /required/);
  assert.match(validateScheduledReportConfiguration({ enabled: true, cron: "0 9 * * *", timezone: "UTC", dispatchTtl: "" }) ?? "", /required/);
});

test("server schedule validation maps stable field-coded violations", async () => {
  const requests: unknown[] = [];
  const transport = {
    post(_route: unknown, request: unknown) {
      requests.push(request);
      return Promise.resolve({
        valid: false,
        violations: [
          {
            field: ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_CRON,
            code: ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_INVALID,
            message: "Weekday 7 is not supported.",
          },
          {
            field: ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_DISPATCH_TTL,
            code: ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_TOO_LARGE,
            message: "Retention cannot exceed ten years.",
          },
        ],
      });
    },
  } as unknown as ProtobufTransport;
  const errors = await validateServerSchedule(new OpenSplunkApiClient(transport), {
    mode: ScheduleValidationMode.SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
    cron: "0 0 * * 7",
    timezone: "UTC",
    dispatchTtl: "315360001",
  });
  assert.deepEqual(errors, {
    cron: "Weekday 7 is not supported.",
    dispatchTtl: "Retention cannot exceed ten years.",
  });
  assert.deepEqual(requests, [{
    mode: ScheduleValidationMode.SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
    cron: "0 0 * * 7",
    timezone: "UTC",
    dispatchTtl: "315360001",
    webhookTtl: "",
  }]);
});

test("scheduled report results expire at the exact retained-results deadline", () => {
  const expiresAt = new Date("2026-08-29T12:10:00.000Z");
  assert.equal(scheduledReportResultExpired(expiresAt, new Date("2026-08-29T12:09:59.999Z")), false);
  assert.equal(scheduledReportResultExpired(expiresAt, new Date("2026-08-29T12:10:00.000Z")), true);
  assert.equal(scheduledReportResultExpired(null, new Date("2026-08-29T12:10:00.000Z")), false);
  assert.equal(scheduledReportRetainedResultPresentation("pending", new Date("0001-01-01T00:00:00.000Z"), new Date()), "pending");
  assert.equal(scheduledReportRetainedResultPresentation("available", expiresAt, new Date("2026-08-29T12:10:00.000Z")), "expired");
});

test("schedule mutations carry the independent optimistic config version", async () => {
  const requests: unknown[] = [];
  const search = {
    spl: "search index=main",
    timeRange: undefined,
    appId: undefined,
    indexScope: ["main"],
    preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS,
    selectedFields: [],
    visualization: undefined,
  };
  const savedSearch: ServerSavedSearch = {
    id: "saved-1",
    version: 11n,
    name: "Scheduled report",
    description: "",
    search,
    sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
    ownerId: "owner-1",
    schedule: { enabled: true, cron: "0 * * * *", timezone: "UTC", dispatchTtl: "2p", configVersion: 7n },
    scheduleStatus: null,
    createdAt: null,
    updatedAt: null,
  };
  const transport = {
    post(_route: unknown, request: unknown) {
      requests.push(request);
      return Promise.resolve({
        savedSearch: {
          savedSearchId: savedSearch.id,
          version: savedSearch.version,
          definition: {
            name: savedSearch.name,
            description: savedSearch.description,
            search,
            sharingScope: savedSearch.sharingScope,
            ownerId: savedSearch.ownerId ?? undefined,
            schedule: { enabled: false, cron: "0 * * * *", timezone: "UTC", dispatchTtl: "2p", configVersion: 8n },
          },
          createdAt: undefined,
          updatedAt: undefined,
          scheduleStatus: undefined,
        },
      });
    },
  } as unknown as ProtobufTransport;

  const result = await setServerSavedSearchSchedule(
    new OpenSplunkApiClient(transport),
    scheduledReportsBootstrap(),
    savedSearch,
    { enabled: false, cron: "0 * * * *", timezone: "UTC", dispatchTtl: "2p" },
  );

  assert.equal(result.status, "available");
  assert.deepEqual(requests, [{
    savedSearchId: "saved-1",
    expectedVersion: 11n,
    expectedScheduleVersion: 7n,
    schedule: { enabled: false, cron: "0 * * * *", timezone: "UTC", dispatchTtl: "2p", configVersion: 7n },
  }]);
  if (result.status === "available") assert.equal(result.value.schedule?.configVersion, 8n);
});

test("scheduled run adapter preserves lifecycle and bounded count metadata", () => {
  const scheduledAt = new Date("2026-08-29T12:00:00.000Z");
  const run = adaptScheduledReportRun({
    scheduledSearchRunId: "run-1",
    savedSearchId: "saved-1",
    scheduledAt,
    startedAt: scheduledAt,
    finishedAt: undefined,
    outcome: ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_RUNNING,
    searchJobId: "job-1",
    searchJobExpiresAt: undefined,
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_PENDING,
    skippedOccurrenceCount: 3,
  });
  assert.equal(run.id, "run-1");
  assert.equal(run.searchJobId, "job-1");
  assert.equal(run.retainedResultState, "pending");
  assert.equal(run.searchJobExpiresAt, null);
  assert.equal(run.skippedOccurrenceCount, 3);
  assert.equal(run.scheduledAt?.toISOString(), scheduledAt.toISOString());
  assert.deepEqual(scheduledReportOutcomePresentation(run.outcome), { label: "Running", tone: "progress" });
});

test("scheduled response adapters reject missing optimistic and lifecycle metadata", () => {
  assert.throws(() => adaptSavedSearch({
    savedSearchId: "saved-1",
    version: 1n,
    definition: {
      name: "Broken schedule",
      search: {
        spl: "search index=main",
        timeRange: undefined,
        indexScope: ["main"],
        selectedFields: [],
        preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS,
      },
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
      schedule: { enabled: true, cron: "0 * * * *", timezone: "UTC", dispatchTtl: "2p", configVersion: 0n },
    },
    createdAt: undefined,
    updatedAt: undefined,
    scheduleStatus: undefined,
  }), /invalid schedule projection/);
  assert.throws(() => adaptScheduledReportRun({
    scheduledSearchRunId: "run-1",
    savedSearchId: "saved-1",
    scheduledAt: undefined,
    startedAt: undefined,
    finishedAt: undefined,
    outcome: ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_RUNNING,
    searchJobId: undefined,
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_UNSPECIFIED,
    skippedOccurrenceCount: 0,
  }), /incomplete or unsupported/);
  assert.throws(() => adaptSavedSearch({
    savedSearchId: "saved-1",
    version: 1n,
    definition: {
      name: "Broken status",
      search: { spl: "search index=main", indexScope: ["main"], selectedFields: [], preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS },
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
    },
    scheduleStatus: { lastOutcome: ScheduledSearchOutcome.UNRECOGNIZED },
  } as never), /unsupported schedule outcome/);
  assert.throws(() => adaptScheduledReportRun({
    scheduledSearchRunId: "run-1",
    savedSearchId: "saved-1",
    scheduledAt: new Date("2026-08-29T12:00:00.000Z"),
    startedAt: undefined,
    finishedAt: undefined,
    outcome: ScheduledSearchOutcome.UNRECOGNIZED,
    searchJobId: undefined,
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_UNSPECIFIED,
    skippedOccurrenceCount: 0,
  }), /incomplete or unsupported/);
});

test("scheduled report outcomes are exhaustively rendered", () => {
  assert.deepEqual(scheduledReportOutcomePresentation(ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_COMPLETED), { label: "Succeeded", tone: "success" });
  assert.deepEqual(scheduledReportOutcomePresentation(ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_FAILED), { label: "Failed", tone: "error" });
  assert.deepEqual(scheduledReportOutcomePresentation(ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_SKIPPED_OVERLAP), { label: "Skipped overlap", tone: "warning" });
  assert.deepEqual(scheduledReportOutcomePresentation(ScheduledSearchOutcome.SCHEDULED_SEARCH_OUTCOME_UNSPECIFIED), { label: "Not run", tone: "neutral" });
  assert.throws(() => scheduledReportOutcomePresentation(ScheduledSearchOutcome.UNRECOGNIZED), /unsupported/);
});

import assert from "node:assert/strict";
import test from "node:test";

import {
  AlertConditionOperator,
  AlertRunOutcome,
  Alert as AlertMessage,
  AlertDefinition as AlertDefinitionMessage,
  AlertRun as AlertRunMessage,
} from "@/gen/ts/open_splunk/alert";
import { RetainedResultStatus } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { ProtobufTransport } from "@/lib/api/protobuf-transport";
import type { SystemBootstrapModel } from "@/lib/api/system-bootstrap";

import {
  adaptServerAlert,
  adaptServerAlertRun,
  alertRunOutcomeLabel,
  AlertCreateAttemptTracker,
  AlertCreationSession,
  AlertSecretIssuanceController,
  createServerAlert,
} from "./server-alerts";

const now = new Date("2026-08-29T12:00:00Z");

function rawAlert() {
  return AlertMessage.fromPartial({
    alertId: "alert-1",
    version: 1n,
    enabled: false,
    createdAt: now,
    updatedAt: now,
    definition: {
      name: "Errors",
      search: { spl: "index=main", indexScope: ["main"], selectedFields: [] },
      cron: "*/5 * * * *",
      timezone: "UTC",
      condition: { operator: AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN, threshold: 0n },
      webhook: { sampleRowCount: 5, ttl: "10p", hostname: "alerts.example.test", secretGeneration: 1n, secretRotatedAt: now },
      dispatchTtl: "2p",
    },
  });
}

function alertsBootstrap(): SystemBootstrapModel {
  return {
    build: null,
    searchWebsocketPath: null,
    features: new Set([ServerFeature.SERVER_FEATURE_ALERTS]),
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
    serverTime: now,
    palette: "classic",
  };
}

test("alert adapter accepts redacted webhook metadata and rejects returned URLs", () => {
  const raw = rawAlert();
  assert.equal(adaptServerAlert(raw).webhookHostname, "alerts.example.test");
  if (raw.definition?.webhook !== undefined) raw.definition.webhook.url = "https://secret.example.test";
  assert.throws(() => adaptServerAlert(raw), /secret-bearing/);
});

test("alert create attempts reuse one request ID for identical retries and rotate after edits", () => {
  const ids = ["alert-request-0000000000000001", "alert-request-0000000000000002", "alert-request-0000000000000003"];
  const tracker = new AlertCreateAttemptTracker(() => ids.shift() ?? "unexpected-request-id");
  const definition = rawAlert().definition!;
  assert.equal(tracker.requestIdFor(definition), "alert-request-0000000000000001");
  assert.equal(tracker.requestIdFor(AlertDefinitionMessage.fromPartial(definition)), "alert-request-0000000000000001");
  assert.equal(tracker.requestIdFor(AlertDefinitionMessage.fromPartial({ ...definition, name: "Edited" })), "alert-request-0000000000000002");
  tracker.reset();
  assert.equal(tracker.requestIdFor(definition), "alert-request-0000000000000003");
});

test("replayed alert creation never exposes a signing secret and preserves the caller request ID", async () => {
  const requests: unknown[] = [];
  const transport = {
    post(_route: unknown, request: unknown) {
      requests.push(request);
      return Promise.resolve({ alert: rawAlert(), replayed: true, signingSecret: "must-not-be-reissued" });
    },
  } as unknown as ProtobufTransport;
  const definition = rawAlert().definition!;
  const result = await createServerAlert(
    new OpenSplunkApiClient(transport),
    alertsBootstrap(),
    definition,
    "alert-request-0000000000000001",
  );
  assert.equal(result.status, "available");
  if (result.status === "available") {
    assert.equal(result.value.replayed, true);
    assert.equal(result.value.signingSecret, null);
  }
  assert.equal((requests[0] as { clientRequestId: string }).clientRequestId, "alert-request-0000000000000001");
});

test("alert creation session preserves retry identity and centralizes replay recovery", async () => {
  const requests: unknown[] = [];
  let attempt = 0;
  const transport = {
    post(_route: unknown, request: unknown) {
      requests.push(request);
      attempt += 1;
      if (attempt === 1) return Promise.reject(new Error("connection reset"));
      return Promise.resolve({ alert: rawAlert(), replayed: true, signingSecret: "must-not-be-reissued" });
    },
  } as unknown as ProtobufTransport;
  const session = new AlertCreationSession(() => "alert-request-0000000000000001");
  const client = new OpenSplunkApiClient(transport);
  const definition = rawAlert().definition!;

  await assert.rejects(session.create(client, alertsBootstrap(), definition), /connection reset/);
  const result = await session.create(client, alertsBootstrap(), definition);

  assert.equal(result.status, "available");
  if (result.status === "available") {
    assert.equal(result.value.secret, null);
    assert.equal(result.value.noticeTone, "warning");
    assert.match(result.value.notice, /cannot be reissued/);
  }
  assert.deepEqual(
    requests.map((request) => (request as { clientRequestId: string }).clientRequestId),
    ["alert-request-0000000000000001", "alert-request-0000000000000001"],
  );
});

test("alert secret issuance remains exclusive through recovery acknowledgement", () => {
  const issuance = new AlertSecretIssuanceController();
  assert.deepEqual(issuance.state(), { phase: "idle" });
  assert.equal(issuance.begin("create"), true);
  assert.equal(issuance.begin("rotate"), false);
  assert.deepEqual(issuance.state(), { operation: "create", phase: "issuing" });

  issuance.recover({ name: " Errors ", value: "one-time-secret" });
  assert.deepEqual(issuance.state(), {
    operation: "create",
    phase: "recovery",
    secret: { name: "Errors", value: "one-time-secret" },
  });
  assert.equal(issuance.begin("rotate"), false);
  issuance.finishIssuing();
  assert.equal(issuance.state().phase, "recovery");

  issuance.acknowledgeRecovery();
  assert.deepEqual(issuance.state(), { phase: "idle" });
  assert.equal(issuance.begin("rotate"), true);
  issuance.failIssuing();
  assert.deepEqual(issuance.state(), { operation: "rotate", phase: "failed" });
  assert.equal(issuance.begin("create"), true);
  issuance.finishIssuing();
  assert.deepEqual(issuance.state(), { phase: "idle" });
});

test("alert run adapter preserves retained job and explicit outcomes", () => {
  const run = adaptServerAlertRun(AlertRunMessage.fromPartial({
    alertRunId: "run-1",
    alertId: "alert-1",
    alertVersion: 2n,
    scheduledAt: now,
    outcome: AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_FAILED,
    searchJobId: "job-1",
    searchJobExpiresAt: new Date(now.valueOf() + 60_000),
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_AVAILABLE,
    failureCategory: "timeout",
  }));
  assert.equal(run.searchJobId, "job-1");
  assert.equal(run.retainedResultState, "available");
  assert.equal(alertRunOutcomeLabel(run.outcome), "Delivery failed");
});

test("alert run adapter requires coherent retained-result metadata", () => {
  assert.throws(() => adaptServerAlertRun(AlertRunMessage.fromPartial({
    alertRunId: "run-1",
    alertId: "alert-1",
    alertVersion: 2n,
    scheduledAt: now,
    outcome: AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING,
    searchJobId: "job-1",
    searchJobExpiresAt: new Date(now.valueOf() + 60_000),
  })), /invalid alert-run/);
  assert.throws(() => adaptServerAlertRun(AlertRunMessage.fromPartial({
    alertRunId: "run-1",
    alertId: "alert-1",
    alertVersion: 2n,
    scheduledAt: now,
    outcome: AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING,
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_PENDING,
  })), /invalid alert-run/);
});

test("alert run adapter accepts a pending retained job before expiry is assigned", () => {
  const run = adaptServerAlertRun(AlertRunMessage.fromPartial({
    alertRunId: "run-pending",
    alertId: "alert-1",
    alertVersion: 2n,
    scheduledAt: now,
    outcome: AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING,
    searchJobId: "job-pending",
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_PENDING,
  }));
  assert.equal(run.searchJobId, "job-pending");
  assert.equal(run.searchJobExpiresAt, null);
  assert.equal(run.retainedResultState, "pending");
});

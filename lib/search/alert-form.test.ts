import assert from "node:assert/strict";
import test from "node:test";

import { AlertConditionOperator } from "@/gen/ts/open_splunk/alert";
import { ScheduleValidationCode, ScheduleValidationField } from "@/gen/ts/open_splunk/schedule_api";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";
import { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { ProtobufTransport } from "@/lib/api/protobuf-transport";

import {
  alertFormIsValid,
  alertWebhookURLValidationError,
  type AlertFormValue,
  validateAlertForm,
  validateServerAlertSchedule,
} from "./alert-form";

const valid: AlertFormValue = {
  name: "Errors detected",
  description: "",
  spl: "index=main level=error",
  earliest: "-5m",
  latest: "now",
  searchTimezone: "Pacific/Chatham",
  appId: "search",
  indexScope: ["main"],
  preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS,
  selectedFields: ["host"],
  visualization: undefined,
  cron: "*/5 * * * *",
  scheduleTimezone: "America/Los_Angeles",
  operator: AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN,
  threshold: "0",
  webhookEndpointMode: "replace",
  webhookUrl: "https://alerts.example.test/open-splunk",
  sampleRows: 5,
  dispatchTtl: "2p",
  webhookTtl: "10p",
};

test("valid alert form accepts strict cron, IANA timezone, and HTTPS webhook", () => {
  assert.equal(alertFormIsValid(valid), true);
  assert.deepEqual(validateAlertForm(valid), {});
});

test("editing may preserve the encrypted webhook without receiving its URL", () => {
  const errors = validateAlertForm({
    ...valid,
    webhookEndpointMode: "preserve",
    webhookUrl: "",
  });
  assert.equal(errors.webhookUrl, undefined);
});

test("webhook validation rejects insecure and ambiguous URLs", () => {
  assert.match(alertWebhookURLValidationError("http://example.test/hook") ?? "", /HTTPS/);
  assert.match(alertWebhookURLValidationError("https://user@example.test/hook") ?? "", /credentials/);
  assert.match(alertWebhookURLValidationError("https://example.test/hook#token") ?? "", /fragment/);
});

test("alert form bounds sample rows and threshold while deferring TTL syntax to the server", () => {
  const errors = validateAlertForm({
    ...valid,
    threshold: "-1",
    sampleRows: 11,
    dispatchTtl: "0p",
    webhookTtl: "1.5p",
  });
  assert.match(errors.threshold ?? "", /non-negative/);
  assert.match(errors.sampleRows ?? "", /between 0 and 10/);
  assert.equal(errors.dispatchTtl, undefined);
  assert.equal(errors.webhookTtl, undefined);
});

test("server alert schedule validation maps schedule and retention fields", async () => {
  const transport = {
    post() {
      return Promise.resolve({
        valid: false,
        violations: [
          {
            field: ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_TIMEZONE,
            code: ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_INVALID,
            message: "Choose a valid IANA timezone.",
          },
          {
            field: ScheduleValidationField.SCHEDULE_VALIDATION_FIELD_WEBHOOK_TTL,
            code: ScheduleValidationCode.SCHEDULE_VALIDATION_CODE_TOO_LARGE,
            message: "Retention cannot exceed ten years.",
          },
        ],
      });
    },
  } as unknown as ProtobufTransport;
  assert.deepEqual(await validateServerAlertSchedule(new OpenSplunkApiClient(transport), valid), {
    cron: undefined,
    scheduleTimezone: "Choose a valid IANA timezone.",
    dispatchTtl: undefined,
    webhookTtl: "Retention cannot exceed ten years.",
  });
});

test("alert form matches backend UTF-8 and search-authority limits", () => {
  const errors = validateAlertForm({
    ...valid,
    name: "é".repeat(65),
    description: "x".repeat(2_049),
    spl: "x".repeat(64 * 1_024 + 1),
    appId: "",
    indexScope: [],
    threshold: "18446744073709551616",
  });
  assert.match(errors.name ?? "", /128 UTF-8 bytes/);
  assert.match(errors.description ?? "", /2,048 UTF-8 bytes/);
  assert.match(errors.spl ?? "", /64 KiB/);
  assert.match(errors.appId ?? "", /required/);
  assert.match(errors.indexScope ?? "", /between 1 and 256/);
  assert.match(errors.threshold ?? "", /unsigned 64-bit/);
});

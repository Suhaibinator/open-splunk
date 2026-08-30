import { AlertConditionOperator } from "@/gen/ts/open_splunk/alert";
import { ScheduleValidationMode } from "@/gen/ts/open_splunk/schedule_api";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";
import type { VisualizationSpec } from "@/gen/ts/open_splunk/result";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import {
  isIanaTimezone,
  validateServerSchedule,
} from "@/lib/search/server-scheduled-reports";

export const DEFAULT_ALERT_DISPATCH_TTL = "2p";
export const DEFAULT_ALERT_WEBHOOK_TTL = "10p";
export const DEFAULT_ALERT_SAMPLE_ROWS = 5;
export const MAXIMUM_ALERT_SAMPLE_ROWS = 10;

const MAXIMUM_ALERT_NAME_BYTES = 128;
const MAXIMUM_ALERT_DESCRIPTION_BYTES = 2_048;
const MAXIMUM_ALERT_APPLICATION_BYTES = 128;
const MAXIMUM_ALERT_SPL_BYTES = 64 * 1_024;
const MAXIMUM_ALERT_TIME_RANGE_BYTES = 256;
const MAXIMUM_ALERT_INDEXES = 256;
const MAXIMUM_ALERT_INDEX_NAME_BYTES = 255;
const MAXIMUM_ALERT_SELECTED_FIELDS = 256;
const MAXIMUM_ALERT_FIELD_NAME_BYTES = 256;
const MAXIMUM_ALERT_VISUALIZATION_BYTES = 64 * 1_024;
const MAXIMUM_ALERT_WEBHOOK_URL_BYTES = 16 * 1_024 - 16;
const MAXIMUM_ALERT_THRESHOLD = 18_446_744_073_709_551_615n;

export type AlertWebhookEndpointMode = "preserve" | "replace";

export interface AlertFormValue {
  name: string;
  description: string;
  spl: string;
  earliest: string;
  latest: string;
  searchTimezone: string;
  appId?: string;
  indexScope: string[];
  preferredResultTab: SearchResultTab;
  selectedFields: string[];
  visualization?: VisualizationSpec;
  cron: string;
  scheduleTimezone: string;
  operator: AlertConditionOperator;
  threshold: string;
  webhookEndpointMode: AlertWebhookEndpointMode;
  webhookUrl: string;
  sampleRows: number;
  dispatchTtl: string;
  webhookTtl: string;
}

export type AlertFormErrors = Partial<Record<keyof AlertFormValue, string>>;

export function alertWebhookURLValidationError(value: string): string | null {
  if (utf8Bytes(value.trim()) > MAXIMUM_ALERT_WEBHOOK_URL_BYTES) return "Webhook URL is too long.";
  try {
    const url = new URL(value.trim());
    if (url.protocol !== "https:") return "Webhook URL must use HTTPS.";
    if (url.username || url.password) return "Webhook URL cannot contain credentials.";
    if (url.hash) return "Webhook URL cannot contain a fragment.";
    if (url.hostname.length === 0) return "Webhook URL must include a hostname.";
    return null;
  } catch {
    return "Enter a valid absolute webhook URL.";
  }
}

function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function exceedsBytes(value: string, maximum: number): boolean {
  return utf8Bytes(value) > maximum;
}

export function validateAlertForm(value: AlertFormValue): AlertFormErrors {
  const errors: AlertFormErrors = {};
  if (value.name.trim().length === 0) errors.name = "Name is required.";
  if (exceedsBytes(value.name, MAXIMUM_ALERT_NAME_BYTES)) errors.name = "Name cannot exceed 128 UTF-8 bytes.";
  if (exceedsBytes(value.description, MAXIMUM_ALERT_DESCRIPTION_BYTES)) errors.description = "Description cannot exceed 2,048 UTF-8 bytes.";
  if (value.spl.trim().length === 0) errors.spl = "SPL is required.";
  if (exceedsBytes(value.spl, MAXIMUM_ALERT_SPL_BYTES)) errors.spl = "SPL cannot exceed 64 KiB.";
  if (value.earliest.trim().length === 0) errors.earliest = "Earliest time is required.";
  if (exceedsBytes(value.earliest, MAXIMUM_ALERT_TIME_RANGE_BYTES)) errors.earliest = "Earliest time is too long.";
  if (value.latest.trim().length === 0) errors.latest = "Latest time is required.";
  if (exceedsBytes(value.latest, MAXIMUM_ALERT_TIME_RANGE_BYTES)) errors.latest = "Latest time is too long.";
  if (!value.appId || value.appId.trim().length === 0) errors.appId = "Application is required.";
  if (value.appId && exceedsBytes(value.appId, MAXIMUM_ALERT_APPLICATION_BYTES)) errors.appId = "Application ID is too long.";
  if (value.indexScope.length < 1 || value.indexScope.length > MAXIMUM_ALERT_INDEXES) {
    errors.indexScope = `Choose an application with between 1 and ${MAXIMUM_ALERT_INDEXES} searchable indexes.`;
  } else if (value.indexScope.some((index) => index.trim().length === 0 || exceedsBytes(index, MAXIMUM_ALERT_INDEX_NAME_BYTES))) {
    errors.indexScope = "The application contains an invalid index name.";
  }
  if (value.selectedFields.length > MAXIMUM_ALERT_SELECTED_FIELDS || value.selectedFields.some((field) => field.trim().length === 0 || exceedsBytes(field, MAXIMUM_ALERT_FIELD_NAME_BYTES))) {
    errors.selectedFields = "Selected field metadata exceeds the alert limits.";
  }
  if (value.visualization !== undefined && utf8Bytes(JSON.stringify(value.visualization)) > MAXIMUM_ALERT_VISUALIZATION_BYTES) {
    errors.visualization = "Visualization metadata cannot exceed 64 KiB.";
  }
  if (value.cron.trim().length === 0) errors.cron = "Cron schedule is required.";
  if (!isIanaTimezone(value.searchTimezone)) errors.searchTimezone = "Enter a valid IANA search timezone.";
  if (value.scheduleTimezone.trim().length === 0) errors.scheduleTimezone = "Schedule timezone is required.";
  if (![
    AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN,
    AlertConditionOperator.ALERT_CONDITION_OPERATOR_LESS_THAN,
    AlertConditionOperator.ALERT_CONDITION_OPERATOR_EQUAL,
    AlertConditionOperator.ALERT_CONDITION_OPERATOR_NOT_EQUAL,
  ].includes(value.operator)) errors.operator = "Choose a supported result-count operator.";
  if (!/^\d+$/u.test(value.threshold)) {
    errors.threshold = "Threshold must be a non-negative integer.";
  } else if (BigInt(value.threshold) > MAXIMUM_ALERT_THRESHOLD) {
    errors.threshold = "Threshold cannot exceed the unsigned 64-bit limit.";
  }
  if (value.webhookEndpointMode === "replace") {
    const webhookError = alertWebhookURLValidationError(value.webhookUrl);
    if (webhookError !== null) errors.webhookUrl = webhookError;
  }
  if (!Number.isInteger(value.sampleRows) || value.sampleRows < 0 || value.sampleRows > MAXIMUM_ALERT_SAMPLE_ROWS) {
    errors.sampleRows = `Sample rows must be between 0 and ${MAXIMUM_ALERT_SAMPLE_ROWS}.`;
  }
  if (value.dispatchTtl.trim().length === 0) errors.dispatchTtl = "Dispatch retention is required.";
  if (value.webhookTtl.trim().length === 0) errors.webhookTtl = "Webhook retention is required.";
  return errors;
}

export function alertFormIsValid(value: AlertFormValue): boolean {
  return Object.keys(validateAlertForm(value)).length === 0;
}

export async function validateServerAlertSchedule(
  client: OpenSplunkApiClient,
  value: AlertFormValue,
  options?: ProtobufRequestOptions,
): Promise<AlertFormErrors> {
  const errors = await validateServerSchedule(client, {
    mode: ScheduleValidationMode.SCHEDULE_VALIDATION_MODE_WEBHOOK_ALERT,
    cron: value.cron,
    timezone: value.scheduleTimezone,
    dispatchTtl: value.dispatchTtl,
    webhookTtl: value.webhookTtl,
  }, options);
  return {
    cron: errors.cron,
    scheduleTimezone: errors.timezone,
    dispatchTtl: errors.dispatchTtl,
    webhookTtl: errors.webhookTtl,
  };
}

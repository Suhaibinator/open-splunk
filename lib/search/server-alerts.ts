import {
  AlertConditionOperator,
  AlertDefinition as AlertDefinitionMessage,
  AlertRunOutcome,
  type Alert,
  type AlertDefinition,
  type AlertRun,
} from "@/gen/ts/open_splunk/alert";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { validDate } from "@/lib/api/duration";
import { collectCursorPages, type CursorPageCollection } from "@/lib/api/pagination";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import {
  featureNotAdvertised,
  isOptionalRouteUnavailable,
  optionalRouteUnavailable,
  type OptionalFeatureResult,
} from "@/lib/api/optional-feature";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import { supportsServerFeature, type SystemBootstrapModel } from "@/lib/api/system-bootstrap";
import { adaptRetainedJobReference, type RetainedJobReference, type RetainedResultState } from "./server-job-settings";

const CONDITION_OPERATORS = new Set([
  AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN,
  AlertConditionOperator.ALERT_CONDITION_OPERATOR_LESS_THAN,
  AlertConditionOperator.ALERT_CONDITION_OPERATOR_EQUAL,
  AlertConditionOperator.ALERT_CONDITION_OPERATOR_NOT_EQUAL,
]);

const RUN_OUTCOMES = new Set([
  AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING,
  AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_FAILED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_NOT_TRIGGERED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_INDETERMINATE,
  AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_FAILED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_SKIPPED_OVERLAP,
  AlertRunOutcome.ALERT_RUN_OUTCOME_INTERRUPTED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_SKIPPED_SECRET_ROTATED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_CANCELED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_EXPIRED,
  AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_UNKNOWN,
]);

export interface ServerAlert {
  id: string;
  version: bigint;
  definition: AlertDefinition;
  enabled: boolean;
  webhookHostname: string;
  secretGeneration: bigint;
  secretRotatedAt: Date;
  nextRunAt: Date | null;
  lastEvaluatedAt: Date | null;
  lastDeliveredAt: Date | null;
  lastOutcome: AlertRunOutcome | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface ServerAlertRun {
  id: string;
  alertId: string;
  alertVersion: bigint;
  scheduledAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  outcome: AlertRunOutcome;
  missedOccurrenceCount: number;
  searchJobId: string | null;
  searchJobExpiresAt: Date | null;
  retainedResultState: RetainedResultState | null;
  failureCategory: string | null;
  deliveryId: string | null;
}

export function adaptServerAlert(alert: Alert): ServerAlert {
  const id = alert.alertId.trim();
  const definition = alert.definition;
  const webhook = definition?.webhook;
  const createdAt = validDate(alert.createdAt);
  const updatedAt = validDate(alert.updatedAt);
  const rotatedAt = validDate(webhook?.secretRotatedAt);
  if (
    id.length === 0
    || alert.version <= 0n
    || definition === undefined
    || definition.name.trim().length === 0
    || definition.search?.spl.trim().length === 0
    || definition.condition === undefined
    || !CONDITION_OPERATORS.has(definition.condition.operator)
    || webhook === undefined
    || webhook.url !== undefined
    || webhook.hostname?.trim().length === 0
    || webhook.secretGeneration <= 0n
    || createdAt === null
    || updatedAt === null
    || rotatedAt === null
  ) {
    throw new TypeError("The server returned an invalid or secret-bearing alert projection.");
  }
  const lastOutcome = alert.status?.lastOutcome;
  if (
    lastOutcome !== undefined
    && lastOutcome !== AlertRunOutcome.ALERT_RUN_OUTCOME_UNSPECIFIED
    && !RUN_OUTCOMES.has(lastOutcome)
  ) {
    throw new TypeError(`Alert ${id} returned an unsupported outcome.`);
  }
  return {
    id,
    version: alert.version,
    definition,
    enabled: alert.enabled,
    webhookHostname: webhook.hostname?.trim() ?? "",
    secretGeneration: webhook.secretGeneration,
    secretRotatedAt: rotatedAt,
    nextRunAt: validDate(alert.status?.nextRunAt),
    lastEvaluatedAt: validDate(alert.status?.lastEvaluatedAt),
    lastDeliveredAt: validDate(alert.status?.lastDeliveredAt),
    lastOutcome: lastOutcome === undefined || lastOutcome === AlertRunOutcome.ALERT_RUN_OUTCOME_UNSPECIFIED
      ? null
      : lastOutcome,
    createdAt,
    updatedAt,
  };
}

export function adaptServerAlertRun(run: AlertRun): ServerAlertRun {
  const id = run.alertRunId.trim();
  const alertId = run.alertId.trim();
  const scheduledAt = validDate(run.scheduledAt);
  let retainedJob: RetainedJobReference;
  try {
    retainedJob = adaptRetainedJobReference(run);
  } catch {
    throw new TypeError("The server returned an invalid alert-run projection.");
  }
  if (
    id.length === 0
    || alertId.length === 0
    || run.alertVersion <= 0n
    || scheduledAt === null
    || !RUN_OUTCOMES.has(run.outcome)
  ) {
    throw new TypeError("The server returned an invalid alert-run projection.");
  }
  return {
    id,
    alertId,
    alertVersion: run.alertVersion,
    scheduledAt,
    startedAt: validDate(run.startedAt),
    finishedAt: validDate(run.finishedAt),
    outcome: run.outcome,
    missedOccurrenceCount: run.missedOccurrenceCount,
    ...retainedJob,
    failureCategory: run.failureCategory?.trim() || null,
    deliveryId: run.deliveryId?.trim() || null,
  };
}

export interface IssuedServerAlert {
  alert: ServerAlert;
  /** Replays deliberately never recover or reissue the one-time secret. */
  signingSecret: string | null;
  replayed: boolean;
}

export interface AlertSigningSecret {
  name: string;
  value: string;
}

export type AlertSecretIssuanceOperation = "create" | "rotate";

export type AlertSecretIssuanceState =
  | { phase: "idle" }
  | { operation: AlertSecretIssuanceOperation; phase: "issuing" }
  | { operation: AlertSecretIssuanceOperation; phase: "recovery"; secret: AlertSigningSecret }
  | { operation: AlertSecretIssuanceOperation; phase: "failed" };

/**
 * Owns the security-sensitive interval from starting an issuance request until
 * the one-time secret is acknowledged. The synchronous transition methods let
 * callers reject competing actions before React has rendered the new state.
 */
export class AlertSecretIssuanceController {
  private current: AlertSecretIssuanceState = { phase: "idle" };

  public state(): AlertSecretIssuanceState {
    return this.current;
  }

  public begin(operation: AlertSecretIssuanceOperation): boolean {
    if (this.current.phase !== "idle" && this.current.phase !== "failed") return false;
    this.current = { operation, phase: "issuing" };
    return true;
  }

  public recover(secret: AlertSigningSecret): void {
    if (this.current.phase !== "issuing") {
      throw new TypeError("An alert secret can only be recovered after issuance starts.");
    }
    const name = secret.name.trim();
    if (name.length === 0 || secret.value.length === 0) {
      throw new TypeError("The issued alert secret is incomplete.");
    }
    this.current = {
      operation: this.current.operation,
      phase: "recovery",
      secret: { name, value: secret.value },
    };
  }

  public finishIssuing(): void {
    if (this.current.phase === "issuing") this.current = { phase: "idle" };
  }

  public failIssuing(): void {
    if (this.current.phase === "issuing") {
      this.current = { operation: this.current.operation, phase: "failed" };
    }
  }

  public acknowledgeRecovery(): void {
    if (this.current.phase === "recovery") this.current = { phase: "idle" };
  }
}

export interface AlertCreationOutcome {
  alert: ServerAlert;
  notice: string;
  noticeTone: "success" | "warning";
  replayed: boolean;
  secret: AlertSigningSecret | null;
}

const MINIMUM_ALERT_CLIENT_REQUEST_ID_BYTES = 16;
const MAXIMUM_ALERT_CLIENT_REQUEST_ID_BYTES = 128;

export function newAlertClientRequestId(): string {
  return crypto.randomUUID();
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

/** Keeps one idempotency key for byte-identical retries and rotates it after edits. */
export class AlertCreateAttemptTracker {
  private definitionBytes: Uint8Array | null = null;
  private requestId: string | null = null;

  public constructor(private readonly generateRequestId: () => string = newAlertClientRequestId) {}

  public requestIdFor(definition: AlertDefinition): string {
    const definitionBytes = AlertDefinitionMessage.encode(definition).finish();
    if (this.requestId === null || this.definitionBytes === null || !equalBytes(this.definitionBytes, definitionBytes)) {
      this.requestId = this.generateRequestId();
      this.definitionBytes = definitionBytes;
    }
    return this.requestId;
  }

  public reset(): void {
    this.definitionBytes = null;
    this.requestId = null;
  }
}

/** Owns one alert-create retry identity and its one-time-secret interpretation. */
export class AlertCreationSession {
  private readonly attempt: AlertCreateAttemptTracker;

  public constructor(generateRequestId: () => string = newAlertClientRequestId) {
    this.attempt = new AlertCreateAttemptTracker(generateRequestId);
  }

  public reset(): void {
    this.attempt.reset();
  }

  public async create(
    client: OpenSplunkApiClient,
    bootstrap: SystemBootstrapModel,
    definition: AlertDefinition,
    options?: ProtobufRequestOptions,
  ): Promise<OptionalFeatureResult<AlertCreationOutcome>> {
    const result = await createServerAlert(
      client,
      bootstrap,
      definition,
      this.attempt.requestIdFor(definition),
      options,
    );
    if (result.status === "unavailable") return result;
    this.attempt.reset();
    const { alert, replayed, signingSecret } = result.value;
    return {
      status: "available",
      value: replayed
        ? {
          alert,
          notice: "The alert was already created, but its one-time secret cannot be reissued. Rotate the secret before enabling it.",
          noticeTone: "warning",
          replayed,
          secret: null,
        }
        : {
          alert,
          notice: "Alert created disabled. Save its signing secret, test the webhook, then enable it.",
          noticeTone: "success",
          replayed,
          secret: signingSecret === null ? null : { name: alert.definition.name, value: signingSecret },
        },
    };
  }
}

function alertClientRequestId(value: string): string {
  const byteLength = new TextEncoder().encode(value).byteLength;
  if (
    byteLength < MINIMUM_ALERT_CLIENT_REQUEST_ID_BYTES
    || byteLength > MAXIMUM_ALERT_CLIENT_REQUEST_ID_BYTES
  ) {
    throw new TypeError("Alert client request ID must contain between 16 and 128 bytes.");
  }
  return value;
}

export async function createServerAlert(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  definition: AlertDefinition,
  clientRequestId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<IssuedServerAlert>> {
  return requiredAlertFeature(bootstrap, async () => {
    const response = await client.alerts.create({
      definition,
      clientRequestId: alertClientRequestId(clientRequestId),
    }, options);
    if (response.alert === undefined) {
      throw new TypeError("The server did not return the created alert.");
    }
    if (!response.replayed && response.signingSecret.length === 0) {
      throw new TypeError("The server did not return the one-time signing secret for the new alert.");
    }
    return {
      alert: adaptServerAlert(response.alert),
      signingSecret: response.replayed ? null : response.signingSecret,
      replayed: response.replayed,
    };
  });
}

export async function listServerAlerts(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  options: ProtobufRequestOptions & { appId?: string; text?: string; pageSize?: number } = {},
): Promise<OptionalFeatureResult<CursorPageCollection<ServerAlert>>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_ALERTS)) return featureNotAdvertised;
  const pageSize = options.pageSize ?? Math.min(50, bootstrap.limits.maximumPageSize || 50);
  try {
    const value = await collectCursorPages<ServerAlert>({
      label: "Alerts",
      maximumPages: 256,
      fetchPage: async ({ pageToken, includeTotalSize }) => {
        const response = await client.alerts.list({
          page: { pageSize, pageToken, includeTotalSize },
          appIdFilter: options.appId?.trim() || undefined,
          textFilter: options.text?.trim() || undefined,
        }, options);
        return { items: response.alerts.map(adaptServerAlert), page: response.page };
      },
    });
    return { status: "available", value };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

async function requiredAlertFeature<T>(
  bootstrap: SystemBootstrapModel,
  operation: () => Promise<T>,
): Promise<OptionalFeatureResult<T>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_ALERTS)) return featureNotAdvertised;
  try {
    return { status: "available", value: await operation() };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

async function requiredAlertMutation(
  bootstrap: SystemBootstrapModel,
  operation: () => Promise<Alert | undefined>,
): Promise<OptionalFeatureResult<ServerAlert>> {
  return requiredAlertFeature(bootstrap, async () => {
    const alert = await operation();
    if (alert === undefined) throw new TypeError("The server returned an empty alert.");
    return adaptServerAlert(alert);
  });
}

export function setServerAlertEnabled(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alert: ServerAlert,
  enabled: boolean,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerAlert>> {
  return requiredAlertMutation(bootstrap, async () => (
    await client.alerts.setState({ alertId: alert.id, expectedVersion: alert.version, enabled }, options)
  ).alert);
}

export function updateServerAlert(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alert: ServerAlert,
  definition: AlertDefinition,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerAlert>> {
  return requiredAlertMutation(bootstrap, async () => (
    await client.alerts.update({
      alertId: alert.id,
      expectedVersion: alert.version,
      definition,
      updateMask: [],
    }, options)
  ).alert);
}

export async function runServerAlert(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alertId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<ServerAlertRun>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_ALERTS)) return featureNotAdvertised;
  try {
    const response = await client.alerts.run({ alertId }, options);
    if (response.run === undefined) throw new TypeError("The server returned an empty alert run.");
    return { status: "available", value: adaptServerAlertRun(response.run) };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export interface TestWebhookResult {
  delivered: boolean;
  deliveryId: string;
  failureCategory: string | null;
}

export async function testServerAlertWebhook(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alertId: string,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<TestWebhookResult>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_ALERTS)) return featureNotAdvertised;
  try {
    const response = await client.alerts.testWebhook({ alertId }, options);
    return { status: "available", value: {
      delivered: response.delivered,
      deliveryId: response.deliveryId,
      failureCategory: response.failureCategory?.trim() || null,
    } };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function rotateServerAlertSecret(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alert: ServerAlert,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<IssuedServerAlert>> {
  return requiredAlertFeature(bootstrap, async () => {
    const response = await client.alerts.rotateSecret({ alertId: alert.id, expectedVersion: alert.version }, options);
    if (response.alert === undefined || response.signingSecret.length === 0) {
      throw new TypeError("The server did not return the rotated alert and one-time signing secret.");
    }
    return { alert: adaptServerAlert(response.alert), signingSecret: response.signingSecret, replayed: false };
  });
}

export async function deleteServerAlert(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alert: ServerAlert,
  options?: ProtobufRequestOptions,
): Promise<OptionalFeatureResult<string>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_ALERTS)) return featureNotAdvertised;
  try {
    const response = await client.alerts.delete({ alertId: alert.id, expectedVersion: alert.version }, options);
    if (response.alertId !== alert.id) throw new TypeError("The server deleted a different alert.");
    return { status: "available", value: response.alertId };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export async function listServerAlertRuns(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  alertId: string,
  options: ProtobufRequestOptions & { pageSize?: number } = {},
): Promise<OptionalFeatureResult<CursorPageCollection<ServerAlertRun>>> {
  if (!supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_ALERTS)) return featureNotAdvertised;
  const pageSize = options.pageSize ?? Math.min(100, bootstrap.limits.maximumPageSize || 100);
  try {
    const value = await collectCursorPages<ServerAlertRun>({
      label: "Alert runs",
      maximumPages: 128,
      fetchPage: async ({ pageToken, includeTotalSize }) => {
        const response = await client.alerts.listRuns({ alertId, page: { pageSize, pageToken, includeTotalSize } }, options);
        return { items: response.runs.map(adaptServerAlertRun), page: response.page };
      },
    });
    return { status: "available", value };
  } catch (error) {
    if (isOptionalRouteUnavailable(error)) return optionalRouteUnavailable;
    throw error;
  }
}

export function alertRunOutcomeLabel(outcome: AlertRunOutcome): string {
  switch (outcome) {
    case AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING: return "Running";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_FAILED: return "Search failed";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_NOT_TRIGGERED: return "Not triggered";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_INDETERMINATE: return "Indeterminate";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERED: return "Triggered / delivered";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_FAILED: return "Delivery failed";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SKIPPED_OVERLAP: return "Skipped overlap";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_INTERRUPTED: return "Interrupted";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_SKIPPED_SECRET_ROTATED: return "Skipped after secret rotation";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_CANCELED: return "Search canceled";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_EXPIRED: return "Search expired";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_UNKNOWN: return "Delivery unknown";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_UNSPECIFIED:
    case AlertRunOutcome.UNRECOGNIZED:
    default: return "Unknown";
  }
}

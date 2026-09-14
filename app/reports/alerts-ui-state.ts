import { AlertConditionOperator, AlertRunOutcome, type AlertDefinition } from "@/gen/ts/open_splunk/alert";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";
import type { StatusTone } from "@/app/_components/status";
import type { ServerAlert, ServerAlertRun } from "@/lib/search/server-alerts";
import {
  DEFAULT_ALERT_DISPATCH_TTL,
  DEFAULT_ALERT_SAMPLE_ROWS,
  DEFAULT_ALERT_WEBHOOK_TTL,
  type AlertFormValue,
} from "@/lib/search/alert-form";

/** Shown while the one-time secret guard is swallowing link and history navigation. */
export const ONE_TIME_SECRET_NAVIGATION_BLOCKED_MESSAGE =
  "Navigation is blocked while a one-time signing secret is being issued. Finish this step before leaving the page.";

export function defaultAlertForm(overrides: Partial<AlertFormValue> = {}): AlertFormValue {
  return {
    name: "",
    description: "",
    spl: "",
    earliest: "-5m",
    latest: "now",
    searchTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
    appId: "search",
    indexScope: [],
    preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_UNSPECIFIED,
    selectedFields: [],
    visualization: undefined,
    cron: "*/5 * * * *",
    scheduleTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
    operator: AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN,
    threshold: "0",
    webhookEndpointMode: "replace",
    webhookUrl: "",
    sampleRows: DEFAULT_ALERT_SAMPLE_ROWS,
    dispatchTtl: DEFAULT_ALERT_DISPATCH_TTL,
    webhookTtl: DEFAULT_ALERT_WEBHOOK_TTL,
    ...overrides,
  };
}

export function alertDefinitionFromForm(value: AlertFormValue): AlertDefinition {
  return {
    name: value.name.trim(),
    description: value.description.trim() || undefined,
    search: {
      spl: value.spl,
      timeRange: { earliest: value.earliest.trim(), latest: value.latest.trim(), timezone: value.searchTimezone.trim() },
      appId: value.appId?.trim() || undefined,
      indexScope: [...value.indexScope],
      preferredResultTab: value.preferredResultTab,
      selectedFields: [...value.selectedFields],
      visualization: value.visualization,
    },
    cron: value.cron.trim(),
    timezone: value.scheduleTimezone.trim(),
    condition: { operator: value.operator, threshold: BigInt(value.threshold) },
    webhook: {
      url: value.webhookEndpointMode === "replace" ? value.webhookUrl.trim() : undefined,
      sampleRowCount: value.sampleRows,
      ttl: value.webhookTtl.trim(),
      hostname: undefined,
      secretRotatedAt: undefined,
      secretGeneration: 0n,
    },
    dispatchTtl: value.dispatchTtl.trim(),
  };
}

export function alertFormFromServer(alert: ServerAlert): AlertFormValue {
  const search = alert.definition.search;
  return defaultAlertForm({
    name: alert.definition.name,
    description: alert.definition.description ?? "",
    spl: search?.spl ?? "",
    earliest: search?.timeRange?.earliest ?? "-5m",
    latest: search?.timeRange?.latest ?? "now",
    searchTimezone: search?.timeRange?.timezone ?? alert.definition.timezone,
    appId: search?.appId,
    indexScope: [...(search?.indexScope ?? [])],
    preferredResultTab: search?.preferredResultTab ?? SearchResultTab.SEARCH_RESULT_TAB_UNSPECIFIED,
    selectedFields: [...(search?.selectedFields ?? [])],
    visualization: search?.visualization,
    cron: alert.definition.cron,
    scheduleTimezone: alert.definition.timezone,
    operator: alert.definition.condition?.operator ?? AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN,
    threshold: alert.definition.condition?.threshold.toString() ?? "0",
    webhookEndpointMode: "preserve",
    webhookUrl: "",
    sampleRows: alert.definition.webhook?.sampleRowCount ?? DEFAULT_ALERT_SAMPLE_ROWS,
    dispatchTtl: alertEffectiveDispatchTTL(alert.definition),
    webhookTtl: alertEffectiveWebhookTTL(alert.definition),
  });
}

export function alertEffectiveDispatchTTL(definition: AlertDefinition): string {
  return definition.dispatchTtl.trim() || DEFAULT_ALERT_DISPATCH_TTL;
}

export function alertEffectiveWebhookTTL(definition: AlertDefinition): string {
  return definition.webhook?.ttl.trim() || DEFAULT_ALERT_WEBHOOK_TTL;
}

export function alertOutcomeTone(outcome: AlertRunOutcome | null): StatusTone {
  switch (outcome) {
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERED: return "success";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING: return "running";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_FAILED:
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_EXPIRED:
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_FAILED: return "error";
    case AlertRunOutcome.ALERT_RUN_OUTCOME_INDETERMINATE:
    case AlertRunOutcome.ALERT_RUN_OUTCOME_INTERRUPTED:
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_SKIPPED_SECRET_ROTATED:
    case AlertRunOutcome.ALERT_RUN_OUTCOME_SEARCH_CANCELED:
    case AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_UNKNOWN: return "warning";
    default: return "neutral";
  }
}

export type AlertRunResultPresentation = "available" | "expired" | "pending" | "unavailable";

export function alertRunResultPresentation(
  run: ServerAlertRun,
  now = new Date(),
): AlertRunResultPresentation {
  if (run.searchJobId === null || run.retainedResultState === null) return "unavailable";
  if (run.retainedResultState === "pending") return "pending";
  if (
    run.retainedResultState === "expired"
    || (run.searchJobExpiresAt !== null && run.searchJobExpiresAt.valueOf() <= now.valueOf())
  ) return "expired";
  if (run.retainedResultState === "available") return "available";
  return "unavailable";
}

export function alertWizardInitialFocus(
  step: number,
  endpointMode: AlertFormValue["webhookEndpointMode"],
): string {
  if (step === 0) return "#alerts-name";
  if (step === 1) return "#alerts-cron";
  return endpointMode === "preserve" ? "#alerts-endpoint-preserve" : "#alerts-webhook";
}

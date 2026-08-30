import assert from "node:assert/strict";
import test from "node:test";

import { AlertConditionOperator, AlertRunOutcome } from "@/gen/ts/open_splunk/alert";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AlertSecretRecovery } from "./alert-secret-recovery";
import { alertWizardStepIsValid } from "./alert-wizard";
import { installOneTimeSecretNavigationProtection } from "./use-one-time-secret-unload-protection";
import {
  alertDefinitionFromForm,
  alertEffectiveDispatchTTL,
  alertEffectiveWebhookTTL,
  alertFormFromServer,
  alertOutcomeTone,
  alertRunResultPresentation,
  alertWizardInitialFocus,
  defaultAlertForm,
} from "./alerts-ui-state";

test("alert wizard defaults match Splunk-compatible retention behavior", () => {
  const form = defaultAlertForm({ name: "Errors", spl: "index=main error", webhookUrl: "https://hooks.example.test", indexScope: ["main"], selectedFields: ["host"], preferredResultTab: SearchResultTab.SEARCH_RESULT_TAB_EVENTS });
  assert.equal(form.dispatchTtl, "2p");
  assert.equal(form.webhookTtl, "10p");
  assert.equal(form.sampleRows, 5);
  assert.equal(form.operator, AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN);
  const definition = alertDefinitionFromForm(form);
  assert.equal(definition.condition?.threshold, 0n);
  assert.equal(definition.webhook?.url, "https://hooks.example.test");
  assert.deepEqual(definition.search?.indexScope, ["main"]);
  assert.deepEqual(definition.search?.selectedFields, ["host"]);
  assert.equal(definition.search?.preferredResultTab, SearchResultTab.SEARCH_RESULT_TAB_EVENTS);
  assert.equal(definition.search?.timeRange?.timezone, form.searchTimezone);
  assert.equal(definition.timezone, form.scheduleTimezone);
});

test("one-time secret recovery has no escape controls before acknowledgement", () => {
  const markup = renderToStaticMarkup(createElement(AlertSecretRecovery, {
    alertName: "Errors",
    secret: "one-time-secret",
    onClose: () => {},
  }));
  assert.doesNotMatch(markup, /aria-label="Close dialog"/u);
  assert.equal([...markup.matchAll(/type="checkbox"/gu)].length, 2);
  assert.match(markup, /I confirm that I saved this secret/u);
  assert.match(markup, />I’m done<\/button>/u);
  assert.match(markup, /<button[^>]*disabled=""[^>]*>I’m done<\/button>/u);
});

test("each alert wizard step moves focus to its first relevant control", () => {
  assert.equal(alertWizardInitialFocus(0, "replace"), "#alerts-name");
  assert.equal(alertWizardInitialFocus(1, "replace"), "#alerts-cron");
  assert.equal(alertWizardInitialFocus(2, "replace"), "#alerts-webhook");
  assert.equal(alertWizardInitialFocus(2, "preserve"), "#alerts-endpoint-preserve");
});

test("alert wizard surfaces bounded presentation metadata on the search step", () => {
  const form = defaultAlertForm({
    name: "Errors",
    spl: "index=main error",
    indexScope: ["main"],
    selectedFields: Array.from({ length: 257 }, (_, index) => `field-${index}`),
    webhookUrl: "https://hooks.example.test/alert",
  });
  assert.equal(alertWizardStepIsValid(form, 0), false);
  assert.equal(alertWizardStepIsValid({ ...form, selectedFields: [] }, 0), true);
});

test("editing an alert preserves its redacted webhook unless replacement is requested", () => {
  const form = alertFormFromServer({
    id: "alert-1",
    version: 2n,
    definition: alertDefinitionFromForm(defaultAlertForm({
      name: "Errors",
      spl: "index=main error",
      webhookUrl: "https://secret.example.test/hook",
    })),
    enabled: true,
    webhookHostname: "secret.example.test",
    secretGeneration: 1n,
    secretRotatedAt: new Date(0),
    nextRunAt: null,
    lastEvaluatedAt: null,
    lastDeliveredAt: null,
    lastOutcome: null,
    createdAt: new Date(0),
    updatedAt: new Date(0),
  });
  assert.equal(form.webhookUrl, "");
  assert.equal(form.webhookEndpointMode, "preserve");
  assert.equal(form.name, "Errors");
  assert.equal(alertDefinitionFromForm(form).webhook?.url, undefined);
});

test("editing and presenting persisted default TTL intent uses effective Splunk defaults", () => {
  const definition = alertDefinitionFromForm(defaultAlertForm({
    name: "Default retention",
    spl: "index=main",
    webhookUrl: "https://hooks.example.test/alert",
  }));
  definition.dispatchTtl = "";
  if (definition.webhook === undefined) throw new Error("webhook fixture is missing");
  definition.webhook.ttl = "";
  const alert = {
    id: "alert-default-ttl",
    version: 1n,
    definition,
    enabled: false,
    webhookHostname: "hooks.example.test",
    secretGeneration: 1n,
    secretRotatedAt: new Date(0),
    nextRunAt: null,
    lastEvaluatedAt: null,
    lastDeliveredAt: null,
    lastOutcome: null,
    createdAt: new Date(0),
    updatedAt: new Date(0),
  };
  const form = alertFormFromServer(alert);
  assert.equal(form.dispatchTtl, "2p");
  assert.equal(form.webhookTtl, "10p");
  assert.equal(alertEffectiveDispatchTTL(definition), "2p");
  assert.equal(alertEffectiveWebhookTTL(definition), "10p");
  assert.equal(alertWizardStepIsValid(form, 1), true);
  assert.equal(alertWizardStepIsValid(form, 2), true);
});

test("editing preserves independent search and cron timezones", () => {
  const source = defaultAlertForm({
    name: "Cross-zone alert",
    spl: "index=main",
    searchTimezone: "Pacific/Chatham",
    scheduleTimezone: "America/Los_Angeles",
    webhookUrl: "https://hooks.example.test/alert",
  });
  const form = alertFormFromServer({
    id: "alert-zones",
    version: 1n,
    definition: alertDefinitionFromForm(source),
    enabled: false,
    webhookHostname: "hooks.example.test",
    secretGeneration: 1n,
    secretRotatedAt: new Date(0),
    nextRunAt: null,
    lastEvaluatedAt: null,
    lastDeliveredAt: null,
    lastOutcome: null,
    createdAt: new Date(0),
    updatedAt: new Date(0),
  });
  assert.equal(form.searchTimezone, "Pacific/Chatham");
  assert.equal(form.scheduleTimezone, "America/Los_Angeles");
});

test("alert outcome presentation is exhaustive for operational states", () => {
  assert.equal(alertOutcomeTone(AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERED), "success");
  assert.equal(alertOutcomeTone(AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING), "running");
  assert.equal(alertOutcomeTone(AlertRunOutcome.ALERT_RUN_OUTCOME_DELIVERY_FAILED), "error");
  assert.equal(alertOutcomeTone(AlertRunOutcome.ALERT_RUN_OUTCOME_INDETERMINATE), "warning");
  assert.equal(alertOutcomeTone(AlertRunOutcome.ALERT_RUN_OUTCOME_NOT_TRIGGERED), "neutral");
});

test("alert retained-result presentation trusts availability and exact expiry", () => {
  const run = {
    id: "run-1",
    alertId: "alert-1",
    alertVersion: 1n,
    scheduledAt: new Date(0),
    startedAt: null,
    finishedAt: null,
    outcome: AlertRunOutcome.ALERT_RUN_OUTCOME_RUNNING,
    missedOccurrenceCount: 0,
    searchJobId: "job-1",
    searchJobExpiresAt: new Date(2_000),
    retainedResultState: "available" as const,
    failureCategory: null,
    deliveryId: null,
  };
  assert.equal(alertRunResultPresentation(run, new Date(1_000)), "available");
  assert.equal(alertRunResultPresentation(run, new Date(2_000)), "expired");
  assert.equal(alertRunResultPresentation({ ...run, retainedResultState: "pending" }, new Date(1_000)), "pending");
  assert.equal(alertRunResultPresentation({ ...run, retainedResultState: "pending" }, new Date(3_000)), "pending");
  assert.equal(alertRunResultPresentation({ ...run, retainedResultState: "missing" }, new Date(1_000)), "unavailable");
  assert.equal(alertRunResultPresentation({ ...run, searchJobId: null, searchJobExpiresAt: null, retainedResultState: null }, new Date(1_000)), "unavailable");
});

test("secret issuance navigation protection covers unload, captured links, and history", () => {
  const windowListeners = new Map<string, (event: Event) => void>();
  const documentListeners = new Map<string, (event: Event) => void>();
  let backCalls = 0;
  let blockedCalls = 0;
  const history = {
    state: null as unknown,
    back: () => { backCalls += 1; },
    pushState(state: unknown) { this.state = state; },
  };
  const fakeWindow = {
    location: { href: "https://open-splunk.example/reports/" },
    history,
    addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
      if (typeof listener === "function") windowListeners.set(type, listener);
    },
    removeEventListener(type: string) { windowListeners.delete(type); },
  } as unknown as Window;
  const fakeDocument = {
    addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
      if (typeof listener === "function") documentListeners.set(type, listener);
    },
    removeEventListener(type: string) { documentListeners.delete(type); },
  } as unknown as Document;

  const cleanup = installOneTimeSecretNavigationProtection(
    fakeWindow,
    fakeDocument,
    () => { blockedCalls += 1; },
  );
  let unloadPrevented = false;
  windowListeners.get("beforeunload")?.({
    preventDefault: () => { unloadPrevented = true; },
    returnValue: undefined,
  } as unknown as Event);
  let linkPrevented = false;
  documentListeners.get("click")?.({
    defaultPrevented: false,
    preventDefault: () => { linkPrevented = true; },
    stopPropagation: () => {},
    target: {
      closest: () => ({ target: "", hasAttribute: () => false }),
    },
  } as unknown as Event);
  windowListeners.get("popstate")?.({} as Event);

  assert.equal(unloadPrevented, true);
  assert.equal(linkPrevented, true);
  assert.equal(blockedCalls, 2);
  cleanup();
  assert.equal(backCalls, 1);
  assert.equal(windowListeners.size, 0);
  assert.equal(documentListeners.size, 0);
});

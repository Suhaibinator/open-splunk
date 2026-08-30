import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import Link from "next/link";

import { AlertConditionOperator } from "@/gen/ts/open_splunk/alert";
import { type AlertFormErrors, type AlertFormValue, validateAlertForm } from "@/lib/search/alert-form";

import { FieldNote, fieldControlProps } from "../_components/field-validation";
import { Modal } from "../_components/modal";
import { alertWizardInitialFocus } from "./alerts-ui-state";

export interface AlertApplicationOption {
  defaultIndexNames: readonly string[];
  id: string;
  name: string;
}

interface AlertWizardProps {
  applications: readonly AlertApplicationOption[];
  administratorSignInRequired?: boolean;
  existingWebhookHostname?: string;
  initialValue: AlertFormValue;
  pending: boolean;
  submitError: string | null;
  validateSchedule: (value: AlertFormValue, signal: AbortSignal) => Promise<AlertFormErrors>;
  onClose: () => void;
  onSubmit: (value: AlertFormValue) => void;
  returnFocus?: HTMLElement | null;
  title?: string;
  submitLabel?: string;
}

const STEP_FIELDS: Array<Array<keyof AlertFormValue>> = [
  ["name", "description", "appId", "indexScope", "spl", "earliest", "latest", "searchTimezone", "selectedFields", "visualization"],
  ["cron", "scheduleTimezone", "operator", "threshold"],
  ["webhookUrl", "sampleRows", "dispatchTtl", "webhookTtl"],
];

function alertWizardStepHasNoErrors(
  errors: ReturnType<typeof validateAlertForm>,
  step: number,
): boolean {
  return STEP_FIELDS[step]?.every((field) => errors[field] === undefined) ?? false;
}

function focusFirstInvalidControl() {
  window.requestAnimationFrame(() => {
    document
      .getElementById("alerts-create-form")
      ?.querySelector<HTMLElement>('[aria-invalid="true"]')
      ?.focus();
  });
}

export function alertWizardStepIsValid(value: AlertFormValue, step: number): boolean {
  return alertWizardStepHasNoErrors(validateAlertForm(value), step);
}

export function AlertWizard({
  applications,
  administratorSignInRequired = false,
  existingWebhookHostname,
  initialValue,
  pending,
  submitError,
  validateSchedule,
  onClose,
  onSubmit,
  returnFocus = null,
  title = "Save as alert",
  submitLabel = "Create disabled alert",
}: AlertWizardProps) {
  const [value, setValue] = useState(initialValue);
  const [step, setStep] = useState(0);
  const [submitted, setSubmitted] = useState(false);
  const [scheduleErrors, setScheduleErrors] = useState<AlertFormErrors>({});
  const [scheduleValidationError, setScheduleValidationError] = useState<string | null>(null);
  const [validatingSchedule, setValidatingSchedule] = useState(false);
  const validationRef = useRef<AbortController | null>(null);
  const errors = useMemo(() => ({ ...scheduleErrors, ...validateAlertForm(value) }), [scheduleErrors, value]);
  const visibleErrors = submitted ? errors : {};
  const stepValid = alertWizardStepHasNoErrors(errors, step);
  const presentationError = visibleErrors.selectedFields ?? visibleErrors.visualization;
  const applicationOptions = useMemo(() => {
    const options = [...applications];
    if (value.appId && !options.some((option) => option.id === value.appId)) {
      options.push({ defaultIndexNames: value.indexScope, id: value.appId, name: value.appId });
    }
    return options.toSorted((left, right) => left.name.localeCompare(right.name));
  }, [applications, value.appId, value.indexScope]);
  const update = <K extends keyof AlertFormValue>(field: K, next: AlertFormValue[K]) => {
    validationRef.current?.abort();
    setScheduleErrors({});
    setScheduleValidationError(null);
    setValue((current) => ({ ...current, [field]: next }));
  };

  useEffect(() => () => validationRef.current?.abort(), []);

  async function authoritativeScheduleErrors(): Promise<AlertFormErrors | null> {
    validationRef.current?.abort();
    const controller = new AbortController();
    validationRef.current = controller;
    setValidatingSchedule(true);
    setScheduleValidationError(null);
    try {
      const nextErrors = await validateSchedule(value, controller.signal);
      if (controller.signal.aborted || validationRef.current !== controller) return null;
      setScheduleErrors(nextErrors);
      return nextErrors;
    } catch (reason) {
      if (controller.signal.aborted || validationRef.current !== controller) return null;
      setScheduleValidationError(reason instanceof Error && reason.message.trim().length > 0
        ? reason.message
        : "The schedule could not be validated.");
      return null;
    } finally {
      if (validationRef.current === controller) {
        validationRef.current = null;
        setValidatingSchedule(false);
      }
    }
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    setSubmitted(true);
    if (!stepValid) {
      focusFirstInvalidControl();
      return;
    }
    if (step === 1) {
      const nextErrors = await authoritativeScheduleErrors();
      if (nextErrors === null) return;
      if (nextErrors.cron !== undefined || nextErrors.scheduleTimezone !== undefined) {
        focusFirstInvalidControl();
        return;
      }
    }
    if (step < STEP_FIELDS.length - 1) {
      setSubmitted(false);
      setStep((current) => current + 1);
      return;
    }
    const nextErrors = await authoritativeScheduleErrors();
    if (nextErrors === null) return;
    const invalidStep = STEP_FIELDS.findIndex((fields) => fields.some((field) => nextErrors[field] !== undefined));
    if (invalidStep >= 0) {
      setStep(invalidStep);
      focusFirstInvalidControl();
      return;
    }
    onSubmit(value);
  }

  function close() {
    validationRef.current?.abort();
    onClose();
  }

  const controlsPending = pending || validatingSchedule;

  return (
    <Modal
      title={title}
      subtitle={`Step ${step + 1} of ${STEP_FIELDS.length}`}
      initialFocus={alertWizardInitialFocus(step, value.webhookEndpointMode)}
      wide
      dismissible={!pending}
      onClose={close}
      returnFocus={returnFocus}
      footer={(
        <>
          <button className="button button--secondary" type="button" disabled={controlsPending} onClick={() => step === 0 ? close() : setStep((current) => current - 1)}>{step === 0 ? "Cancel" : "Back"}</button>
          <button className="button button--primary" type="submit" form="alerts-create-form" disabled={controlsPending}>{pending ? "Saving…" : validatingSchedule ? "Validating…" : step === STEP_FIELDS.length - 1 ? submitLabel : "Continue"}</button>
        </>
      )}
    >
      <form className="form-stack alerts-wizard" id="alerts-create-form" onSubmit={(event) => void submit(event)}>
        <ol className="alerts-wizard-steps" aria-label="Alert setup progress">
          <li aria-current={step === 0 ? "step" : undefined}>Search</li>
          <li aria-current={step === 1 ? "step" : undefined}>Trigger</li>
          <li aria-current={step === 2 ? "step" : undefined}>Webhook</li>
        </ol>

        {step === 0 ? (
          <>
            <label htmlFor="alerts-name">
              <span>Name</span>
              <input id="alerts-name" value={value.name} onChange={(event) => update("name", event.target.value)} {...fieldControlProps("alerts-name", visibleErrors.name ?? null)} />
              <FieldNote fieldId="alerts-name" error={visibleErrors.name ?? null}>A unique name in this app.</FieldNote>
            </label>
            <label htmlFor="alerts-description">
              <span>Description</span>
              <textarea id="alerts-description" rows={2} value={value.description} onChange={(event) => update("description", event.target.value)} {...fieldControlProps("alerts-description", visibleErrors.description ?? null)} />
              <FieldNote fieldId="alerts-description" error={visibleErrors.description ?? null}>Optional operational context.</FieldNote>
            </label>
            <label htmlFor="alerts-application">
              <span>Application</span>
              <select
                className="button button--secondary button--block"
                id="alerts-application"
                value={value.appId ?? ""}
                onChange={(event) => {
                  const application = applicationOptions.find((option) => option.id === event.target.value);
                  validationRef.current?.abort();
                  setScheduleErrors({});
                  setScheduleValidationError(null);
                  setValue((current) => ({
                    ...current,
                    appId: event.target.value,
                    indexScope: [...(application?.defaultIndexNames ?? [])],
                  }));
                }}
                {...fieldControlProps("alerts-application", visibleErrors.appId ?? visibleErrors.indexScope ?? null)}
              >
                {applicationOptions.length === 0 ? <option value={value.appId ?? "search"}>{value.appId ?? "search"}</option> : null}
                {applicationOptions.map((option) => <option key={option.id} value={option.id}>{option.name}</option>)}
              </select>
              <FieldNote fieldId="alerts-application" error={visibleErrors.appId ?? visibleErrors.indexScope ?? null}>
                {value.indexScope.length === 0 ? "No searchable indexes are available for this application." : `Searches ${value.indexScope.join(", ")}.`}
              </FieldNote>
            </label>
            <label htmlFor="alerts-spl">
              <span>SPL</span>
              <textarea id="alerts-spl" rows={5} value={value.spl} onChange={(event) => update("spl", event.target.value)} {...fieldControlProps("alerts-spl", visibleErrors.spl ?? null)} />
              <FieldNote fieldId="alerts-spl" error={visibleErrors.spl ?? null} />
            </label>
            <div className="alerts-form-grid">
              <label htmlFor="alerts-earliest">
                <span>Earliest</span>
                <input id="alerts-earliest" value={value.earliest} onChange={(event) => update("earliest", event.target.value)} {...fieldControlProps("alerts-earliest", visibleErrors.earliest ?? null)} />
                <FieldNote fieldId="alerts-earliest" error={visibleErrors.earliest ?? null} />
              </label>
              <label htmlFor="alerts-latest">
                <span>Latest</span>
                <input id="alerts-latest" value={value.latest} onChange={(event) => update("latest", event.target.value)} {...fieldControlProps("alerts-latest", visibleErrors.latest ?? null)} />
                <FieldNote fieldId="alerts-latest" error={visibleErrors.latest ?? null} />
              </label>
              <label htmlFor="alerts-search-timezone">
                <span>Search timezone</span>
                <input id="alerts-search-timezone" value={value.searchTimezone} onChange={(event) => update("searchTimezone", event.target.value)} {...fieldControlProps("alerts-search-timezone", visibleErrors.searchTimezone ?? null)} />
                <FieldNote fieldId="alerts-search-timezone" error={visibleErrors.searchTimezone ?? null}>Used to resolve the search time range.</FieldNote>
              </label>
            </div>
            {presentationError === undefined ? null : (
              <p className="alerts-inline-error" role="alert">
                {presentationError} Adjust the search workspace presentation before creating this alert.
              </p>
            )}
          </>
        ) : null}

        {step === 1 ? (
          <>
            <div className="alerts-form-grid">
              <label htmlFor="alerts-cron">
                <span>Five-field cron</span>
                <input id="alerts-cron" value={value.cron} onChange={(event) => update("cron", event.target.value)} {...fieldControlProps("alerts-cron", visibleErrors.cron ?? null)} />
                <FieldNote fieldId="alerts-cron" error={visibleErrors.cron ?? null}>Minute, hour, day, month, weekday.</FieldNote>
              </label>
              <label htmlFor="alerts-schedule-timezone">
                <span>Schedule timezone</span>
                <input id="alerts-schedule-timezone" value={value.scheduleTimezone} onChange={(event) => update("scheduleTimezone", event.target.value)} {...fieldControlProps("alerts-schedule-timezone", visibleErrors.scheduleTimezone ?? null)} />
                <FieldNote fieldId="alerts-schedule-timezone" error={visibleErrors.scheduleTimezone ?? null}>IANA timezone used for cron occurrences.</FieldNote>
              </label>
            </div>
            <div className="alerts-form-grid">
              <label htmlFor="alerts-operator">
                <span>Result count condition</span>
                <select className="button button--secondary button--block" id="alerts-operator" value={value.operator} onChange={(event) => update("operator", Number(event.target.value) as AlertConditionOperator)} {...fieldControlProps("alerts-operator", visibleErrors.operator ?? null)}>
                  <option value={AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN}>Greater than</option>
                  <option value={AlertConditionOperator.ALERT_CONDITION_OPERATOR_LESS_THAN}>Less than</option>
                  <option value={AlertConditionOperator.ALERT_CONDITION_OPERATOR_EQUAL}>Equal to</option>
                  <option value={AlertConditionOperator.ALERT_CONDITION_OPERATOR_NOT_EQUAL}>Not equal to</option>
                </select>
                <FieldNote fieldId="alerts-operator" error={visibleErrors.operator ?? null} />
              </label>
              <label htmlFor="alerts-threshold">
                <span>Threshold</span>
                <input id="alerts-threshold" inputMode="numeric" value={value.threshold} onChange={(event) => update("threshold", event.target.value)} {...fieldControlProps("alerts-threshold", visibleErrors.threshold ?? null)} />
                <FieldNote fieldId="alerts-threshold" error={visibleErrors.threshold ?? null} />
              </label>
            </div>
          </>
        ) : null}

        {step === 2 ? (
          <>
            {initialValue.webhookEndpointMode === "preserve" ? (
              <fieldset className="alerts-endpoint-choice segmented-fieldset">
                <legend>Webhook endpoint</legend>
                <label htmlFor="alerts-endpoint-preserve"><input id="alerts-endpoint-preserve" type="radio" name="alerts-endpoint-mode" checked={value.webhookEndpointMode === "preserve"} onChange={() => update("webhookEndpointMode", "preserve")} /> Keep existing endpoint ({existingWebhookHostname ?? "encrypted hostname"})</label>
                <label htmlFor="alerts-endpoint-replace"><input id="alerts-endpoint-replace" type="radio" name="alerts-endpoint-mode" checked={value.webhookEndpointMode === "replace"} onChange={() => update("webhookEndpointMode", "replace")} /> Replace endpoint</label>
              </fieldset>
            ) : null}
            {value.webhookEndpointMode === "replace" ? (
              <label htmlFor="alerts-webhook">
                <span>Webhook HTTPS URL</span>
                <input id="alerts-webhook" type="url" value={value.webhookUrl} onChange={(event) => update("webhookUrl", event.target.value)} {...fieldControlProps("alerts-webhook", visibleErrors.webhookUrl ?? null)} />
                <FieldNote fieldId="alerts-webhook" error={visibleErrors.webhookUrl ?? null}>Stored encrypted and never shown again.</FieldNote>
              </label>
            ) : <p className="alerts-wizard-note">The encrypted endpoint is unchanged. Choose Replace endpoint to configure a different receiver.</p>}
            <div className="alerts-form-grid">
              <label htmlFor="alerts-sample">
                <span>Sample rows</span>
                <input id="alerts-sample" type="number" min="0" max="10" value={value.sampleRows} onChange={(event) => update("sampleRows", Number(event.target.value))} {...fieldControlProps("alerts-sample", visibleErrors.sampleRows ?? null)} />
                <FieldNote fieldId="alerts-sample" error={visibleErrors.sampleRows ?? null}>0–10 rows; default 5.</FieldNote>
              </label>
              <label htmlFor="alerts-dispatch-ttl">
                <span>Dispatch TTL</span>
                <input id="alerts-dispatch-ttl" value={value.dispatchTtl} onChange={(event) => update("dispatchTtl", event.target.value)} {...fieldControlProps("alerts-dispatch-ttl", visibleErrors.dispatchTtl ?? null)} />
                <FieldNote fieldId="alerts-dispatch-ttl" error={visibleErrors.dispatchTtl ?? null}>Defaults to 2p.</FieldNote>
              </label>
              <label htmlFor="alerts-webhook-ttl">
                <span>Webhook TTL</span>
                <input id="alerts-webhook-ttl" value={value.webhookTtl} onChange={(event) => update("webhookTtl", event.target.value)} {...fieldControlProps("alerts-webhook-ttl", visibleErrors.webhookTtl ?? null)} />
                <FieldNote fieldId="alerts-webhook-ttl" error={visibleErrors.webhookTtl ?? null}>Defaults to 10p.</FieldNote>
              </label>
            </div>
            <p className="alerts-wizard-note">New alerts are disabled. Test the receiver and save the one-time secret before enabling the schedule.</p>
          </>
        ) : null}
        {scheduleValidationError ? <p className="alerts-inline-error" role="alert">{scheduleValidationError}</p> : null}
        {submitError ? <p className="alerts-inline-error" role="alert">{submitError}{administratorSignInRequired ? <> <Link href="/signin/">Administrator sign in</Link></> : null}</p> : null}
      </form>
    </Modal>
  );
}

"use client";

import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  GetServerSettingsResponse,
  SearchLimits,
  UpdateServerSettingsResponse,
} from "@/gen/ts/open_splunk/server_settings_api";
import { isHttpStatus, type OpenSplunkApiClient } from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import {
  parsePositiveInteger,
  sameSearchLimits,
  searchLimitsFromForm,
  searchLimitsToForm,
  type SearchLimitForm,
} from "./search-limits-form";

const fields = {
  runtimeSeconds: { label: "Maximum runtime", unit: "seconds", group: "Execution" },
  memoryMiB: { label: "Per-query memory", unit: "MiB", group: "Execution" },
  rowsRead: { label: "Rows read", unit: "rows", group: "Execution" },
  bytesReadMiB: { label: "Bytes read", unit: "MiB", group: "Execution" },
  groupedRows: { label: "Grouped rows", unit: "rows", group: "Execution" },
  threads: { label: "ClickHouse threads", unit: "threads", group: "Execution" },
  resultRows: { label: "Retained rows per job", unit: "rows", group: "Results and retention" },
  resultBytesMiB: { label: "Retained bytes per job", unit: "MiB", group: "Results and retention" },
  totalResultBytesMiB: { label: "Total retained result bytes", unit: "MiB", group: "Results and retention" },
  retentionMinutes: { label: "Result retention", unit: "minutes", group: "Results and retention" },
  concurrency: { label: "Concurrent searches", unit: "searches", group: "Scheduling" },
} as const;

const groups = ["Execution", "Results and retention", "Scheduling"] as const;
const mebibyteFields = new Set<keyof SearchLimitForm>([
  "memoryMiB",
  "bytesReadMiB",
  "resultBytesMiB",
  "totalResultBytesMiB",
]);

function gibibyteHint(value: string): string {
  const mebibytes = parsePositiveInteger(value);
  if (mebibytes === null || mebibytes < 1024n) return "";
  const whole = mebibytes / 1024n;
  const remainder = mebibytes % 1024n;
  return remainder === 0n
    ? ` (${whole.toLocaleString()} GiB)`
    : ` (${whole.toLocaleString()} GiB + ${remainder.toLocaleString()} MiB)`;
}

function compareFormValue(
  key: keyof SearchLimitForm,
  form: SearchLimitForm,
  minimums: SearchLimitForm,
  maximums: SearchLimitForm,
): string | null {
  const value = parsePositiveInteger(form[key]);
  const minimum = parsePositiveInteger(minimums[key]);
  const maximum = parsePositiveInteger(maximums[key]);
  if (value === null || minimum === null || maximum === null) return "Enter a whole number greater than zero.";
  if (value < minimum || value > maximum) {
    return `Enter ${minimum.toLocaleString()}–${maximum.toLocaleString()} ${fields[key].unit}.`;
  }
  return null;
}

type CompleteSettingsEnvelope = GetServerSettingsResponse & {
  current: NonNullable<GetServerSettingsResponse["current"]> & { limits: SearchLimits };
  defaults: SearchLimits;
  minimums: SearchLimits;
  maximums: SearchLimits;
};

function completeEnvelope(
  response: GetServerSettingsResponse | UpdateServerSettingsResponse,
): response is CompleteSettingsEnvelope {
  return response.current?.limits !== undefined
    && response.defaults !== undefined
    && response.minimums !== undefined
    && response.maximums !== undefined;
}

export function SearchLimitsSettings({
  client,
  onStatus,
  onDirtyChange,
}: {
  client: OpenSplunkApiClient;
  onStatus: (message: string, kind: "success" | "warning") => void;
  onDirtyChange: (dirty: boolean) => void;
}) {
  const [response, setResponse] = useState<GetServerSettingsResponse | null>(null);
  const [form, setForm] = useState<SearchLimitForm | null>(null);
  const [exactBase, setExactBase] = useState<SearchLimits | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "saving" | "error">("loading");
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setState("loading");
    setError(null);
    try {
      const next = await client.serverSettings.get({});
      if (!completeEnvelope(next)) throw new Error("The server returned incomplete search settings.");
      setResponse(next);
      setForm(searchLimitsToForm(next.current.limits));
      setExactBase(next.current.limits);
      setState("ready");
      return true;
    } catch (cause) {
      setError(createErrorMessage("Search settings could not be loaded.")(cause));
      setState("error");
      return false;
    }
  }, [client]);

  useEffect(() => { void load(); }, [load]);

  const savedForm = useMemo(
    () => response?.current?.limits === undefined ? null : searchLimitsToForm(response.current.limits),
    [response],
  );
  const formCandidate = form === null ? null : searchLimitsFromForm(form, exactBase ?? undefined);
  const currentLimits = response?.current?.limits;
  const dirty = form !== null && savedForm !== null && (
    JSON.stringify(form) !== JSON.stringify(savedForm)
    || (formCandidate !== null && currentLimits !== undefined && !sameSearchLimits(formCandidate, currentLimits))
  );
  useEffect(() => {
    onDirtyChange(dirty);
    return () => onDirtyChange(false);
  }, [dirty, onDirtyChange]);
  useEffect(() => {
    if (!dirty) return;
    const protect = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", protect);
    return () => window.removeEventListener("beforeunload", protect);
  }, [dirty]);

  if (state === "error") {
    return <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Search limits could not be loaded</strong><p>{error}</p><button type="button" onClick={() => void load()}>Retry</button></div></div>;
  }
  if (state === "loading" || form === null || response === null) {
    return <output className="access-mode-notice"><span>i</span><div><strong>Loading search limits</strong><p>Reading the current version and supported ranges…</p></div></output>;
  }
  if (!completeEnvelope(response)) return null;

  const minimums = searchLimitsToForm(response.minimums);
  const maximums = searchLimitsToForm(response.maximums);
  const defaultsForm = searchLimitsToForm(response.defaults);
  const errors = Object.fromEntries(
    (Object.keys(fields) as (keyof SearchLimitForm)[]).map((key) => [
      key,
      compareFormValue(key, form, minimums, maximums),
    ]),
  ) as Record<keyof SearchLimitForm, string | null>;
  const parsed = searchLimitsFromForm(form, exactBase ?? undefined);
  const atDefaults = parsed !== null && sameSearchLimits(parsed, response.defaults);
  if (parsed !== null && parsed.maximumResultBytes > parsed.maximumTotalResultBytes) {
    errors.totalResultBytesMiB = "Total retained bytes must be at least the per-job limit.";
  }
  const valid = parsed !== null && Object.values(errors).every((item) => item === null);

  const save = async (event: FormEvent) => {
    event.preventDefault();
    if (!valid || parsed === null || response.current === undefined) return;
    setState("saving");
    try {
      const next = await client.serverSettings.update({
        expectedVersion: response.current.version,
        limits: parsed,
      });
      if (!completeEnvelope(next)) throw new Error("The server returned incomplete search settings.");
      setResponse(next);
      setForm(searchLimitsToForm(next.current.limits));
      setExactBase(next.current.limits);
      setState("ready");
      onStatus("Search limits were updated. New searches now use version " + next.current.version.toString() + ".", "success");
    } catch (cause) {
      if (isHttpStatus(cause, 409)) {
        const reloaded = await load();
        onStatus(reloaded
          ? "Search limits changed on the server. The latest version was reloaded; review it before applying again."
          : "Search limits changed on the server, and the latest version could not be reloaded.", "warning");
        return;
      }
      setState("ready");
      onStatus(createErrorMessage("Search limits could not be updated.")(cause), "warning");
    }
  };

  return (
    <form className="admin-section-stack" onSubmit={(event) => void save(event)}>
      <div className="access-mode-notice" role="note"><span>i</span><div><strong>Applies to newly admitted searches</strong><p>Running and queued jobs keep their captured execution, result, and retention limits. Scheduling concurrency changes immediately. Command-specific safety limits and export re-execution limits remain fixed.</p></div></div>
      {groups.map((group) => (
        <section className="suite-card settings-group" key={group}>
          <header><h3>{group}</h3><p>{group === "Execution" ? "Controls ClickHouse work allowed for each new query." : group === "Scheduling" ? "Controls how many queued jobs may execute at once." : "Controls result memory and how long completed jobs remain available."}</p></header>
          <div className="settings-form-grid">
            {(Object.keys(fields) as (keyof SearchLimitForm)[]).filter((key) => fields[key].group === group).map((key) => (
              <label key={key}>
                <span>{fields[key].label}</span>
                <input
                  inputMode="numeric"
                  value={form[key]}
                  aria-invalid={errors[key] !== null}
                  disabled={state === "saving"}
                  onChange={(event) => setForm((current) => current === null ? current : { ...current, [key]: event.target.value })}
                />
                <small>{errors[key] ?? `${minimums[key]}–${maximums[key]} ${fields[key].unit}${mebibyteFields.has(key) ? gibibyteHint(maximums[key]) : ""}; default ${defaultsForm[key]}${mebibyteFields.has(key) ? gibibyteHint(defaultsForm[key]) : ""}.`}</small>
              </label>
            ))}
          </div>
        </section>
      ))}
      <div className="settings-actions">
        <button className="button secondary" type="button" disabled={state === "saving" || atDefaults} onClick={() => { setForm(defaultsForm); setExactBase(response.defaults); }}>Reset to defaults</button>
        <button className="button primary" type="submit" disabled={state === "saving" || !dirty || !valid}>{state === "saving" ? "Applying…" : "Apply"}</button>
      </div>
    </form>
  );
}

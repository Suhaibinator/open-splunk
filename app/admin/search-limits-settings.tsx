"use client";

import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  GetServerSettingsResponse,
  SearchLimits,
  UpdateServerSettingsResponse,
} from "@/gen/ts/open_splunk/server_settings_api";
import { isHttpStatus, type OpenSplunkApiClient } from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import { FieldNote, fieldControlProps } from "../_components/field-validation";
import {
  SEARCH_LIMIT_FIELDS,
  SEARCH_LIMIT_GROUPS,
  SEARCH_LIMIT_KEYS,
  sameSearchLimits,
  searchLimitErrors,
  searchLimitFieldHint,
  searchLimitsFromForm,
  searchLimitsToForm,
  type SearchLimitForm,
} from "./search-limits-form";

const GROUP_DESCRIPTIONS: Record<typeof SEARCH_LIMIT_GROUPS[number], string> = {
  Execution: "Controls ClickHouse work allowed for each new query.",
  "Results and retention": "Controls result memory and how long completed jobs remain available.",
  Scheduling: "Controls how many queued jobs may execute at once.",
};

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
  const [activeClient, setActiveClient] = useState(client);
  if (activeClient !== client) {
    setActiveClient(client);
    setState("loading");
    setError(null);
  }

  const load = useCallback(async () => {
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

  const reload = useCallback(() => {
    setState("loading");
    setError(null);
    return load();
  }, [load]);

  useEffect(() => {
    if (activeClient !== client) return;
    let current = true;
    void client.serverSettings.get({}).then((next) => {
      if (!current) return;
      if (!completeEnvelope(next)) throw new Error("The server returned incomplete search settings.");
      setResponse(next);
      setForm(searchLimitsToForm(next.current.limits));
      setExactBase(next.current.limits);
      setState("ready");
    }).catch((cause: unknown) => {
      if (!current) return;
      setError(createErrorMessage("Search settings could not be loaded.")(cause));
      setState("error");
    });
    return () => {
      current = false;
    };
  }, [activeClient, client]);

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
    return <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Search limits could not be loaded</strong><p>{error}</p><button type="button" onClick={() => void reload()}>Retry</button></div></div>;
  }
  if (state === "loading" || form === null || response === null) {
    return <output className="access-mode-notice"><span>i</span><div><strong>Loading search limits</strong><p>Reading the current version and supported ranges…</p></div></output>;
  }
  if (!completeEnvelope(response)) return null;

  const defaultsForm = searchLimitsToForm(response.defaults);
  const errors = searchLimitErrors(form, response.minimums, response.maximums);
  const parsed = searchLimitsFromForm(form, exactBase ?? undefined);
  const atDefaults = parsed !== null && sameSearchLimits(parsed, response.defaults);
  const invalid = SEARCH_LIMIT_KEYS.filter((key) => errors[key] !== null);
  const valid = parsed !== null && invalid.length === 0;

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
        const reloaded = await reload();
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
    <form className="admin-section-stack" onSubmit={(event) => void save(event)} noValidate>
      <div className="access-mode-notice" role="note"><span>i</span><div><strong>Applies to newly admitted searches</strong><p>Running and queued jobs keep their captured execution, result, and retention limits. Scheduling concurrency changes immediately. Command-specific safety limits and export re-execution limits remain fixed.</p></div></div>
      {SEARCH_LIMIT_GROUPS.map((group) => (
        <section className="suite-card settings-group" key={group}>
          <header><h3>{group}</h3><p>{GROUP_DESCRIPTIONS[group]}</p></header>
          <div className="settings-form-grid">
            {SEARCH_LIMIT_KEYS.filter((key) => SEARCH_LIMIT_FIELDS[key].group === group).map((key) => {
              const fieldId = `search-limit-${key}`;
              return (
                <label key={key} htmlFor={fieldId}>
                  <span>{SEARCH_LIMIT_FIELDS[key].label}</span>
                  <input
                    autoComplete="off"
                    disabled={state === "saving"}
                    id={fieldId}
                    inputMode={SEARCH_LIMIT_FIELDS[key].kind === "bytes" ? "text" : "numeric"}
                    onChange={(event) => setForm((current) => current === null ? current : { ...current, [key]: event.target.value })}
                    spellCheck={false}
                    value={form[key]}
                    {...fieldControlProps(fieldId, errors[key])}
                  />
                  <FieldNote error={errors[key]} fieldId={fieldId}>
                    {searchLimitFieldHint(key, form, response.defaults, response.minimums, response.maximums)}
                  </FieldNote>
                </label>
              );
            })}
          </div>
        </section>
      ))}
      <div className="settings-actions">
        {invalid.length === 0 ? null : (
          <output className="settings-invalid-summary">
            {invalid.length === 1
              ? `${SEARCH_LIMIT_FIELDS[invalid[0]].label} needs attention.`
              : `${invalid.length.toString()} fields need attention: ${invalid.map((key) => SEARCH_LIMIT_FIELDS[key].label).join(", ")}.`}
          </output>
        )}
        <button className="button button--secondary" type="button" disabled={state === "saving" || atDefaults} onClick={() => { setForm(defaultsForm); setExactBase(response.defaults); }}>Reset to defaults</button>
        <button className="button button--primary" type="submit" disabled={state === "saving" || !dirty || !valid}>{state === "saving" ? "Applying…" : "Apply"}</button>
      </div>
    </form>
  );
}

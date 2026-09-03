"use client";

import type { FormEvent } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";

import {
  SharingScope,
  type FieldViolation,
} from "@/gen/ts/open_splunk/common";
import {
  KnowledgeOverwriteBehavior,
  KnowledgeSelector,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/knowledge";
import {
  LookupDefinition,
  LookupState,
  type Lookup,
  type LookupFieldMapping,
} from "@/gen/ts/open_splunk/lookup";
import type { PreviewLookupResponse } from "@/gen/ts/open_splunk/lookup_api";
import { isOptionalRouteUnavailable } from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";

import { BackendResourceState } from "../_components/backend-resource-state";
import { StatusLabel } from "../_components/status";
import { AppIcon } from "../_components/app-icon";
import { formatMediumDateTime } from "../_components/date-format";
import { Modal } from "../_components/modal";
import { joinedPatterns, lines } from "./knowledge-lookup-text";
import type { KnowledgeManagerAppOption } from "./knowledge-manager-feature";
import {
  createLookupManagerClient,
  type LookupManagerClient,
} from "./lookup-manager-data";
import {
  LOOKUP_MANAGER_CONTRACT,
  hasUnpairedSurrogate,
  isCanonicallyAuthorableLookupDefinition,
  isExactEventField,
  isExactLookupColumn,
  isExactPublicField,
  isLookupOutputMarker,
  selectorPatternKind,
  textBytes,
} from "./lookup-manager-contract";

export {
  isExactEventField,
  isExactLookupColumn,
  isExactPublicField,
} from "./lookup-manager-contract";
import { summarizeByteQuantity } from "@/lib/byte-quantity";

type LoadState = "loading" | "available" | "unavailable" | "error";
type LookupModal = "create" | "replace" | "delete";
type NoticeKind = "success" | "error";

export type LookupDraftSharingScope = "private" | "app" | "global";
export type LookupDraftOverwrite = "preserve" | "replace";

export interface LookupDraft {
  appId: string;
  name: string;
  description: string;
  sharingScope: LookupDraftSharingScope;
  automatic: boolean;
  indexPatterns: string;
  hostPatterns: string;
  sourcePatterns: string;
  sourcetypePatterns: string;
  keyMappings: string;
  outputMappings: string;
  overwrite: LookupDraftOverwrite;
}

export interface SafeLookupPreview {
  columns: readonly string[];
  rows: ReadonlyArray<readonly string[]>;
  totalRows: bigint;
  truncated: boolean;
  violations: readonly FieldViolation[];
}

interface LookupManagerPanelProps {
  apiBaseUrl: string;
  apps: readonly KnowledgeManagerAppOption[];
  initialAppId: string | null;
}

interface LookupEditorProps {
  client: LookupManagerClient;
  apps: readonly KnowledgeManagerAppOption[];
  initialAppId: string;
  currentLookup: Lookup | null;
  busy: boolean;
  error: string | null;
  onBusyChange: (busy: boolean) => void;
  onSubmit: (
    definition: LookupDefinition,
    csvData: Uint8Array | undefined,
  ) => Promise<void>;
}

const errorMessage = createErrorMessage("The server did not return a usable lookup response.");

function mappingLine(mapping: LookupFieldMapping, allowImplicit: boolean): string {
  return allowImplicit && mapping.lookupField === mapping.eventField
    ? mapping.lookupField
    : `${mapping.lookupField} AS ${mapping.eventField}`;
}

/** Parses the exact, unquoted mapping form exposed by the public SPL grammar. */
export function parseLookupMappings(
  value: string,
  kind: "key" | "output",
): LookupFieldMapping[] {
  const entries = lines(value);
  const maximum = kind === "key"
    ? LOOKUP_MANAGER_CONTRACT.maximumKeyMappings
    : LOOKUP_MANAGER_CONTRACT.maximumOutputMappings;
  if (entries.length < 1 || entries.length > maximum) {
    throw new TypeError(`${kind === "key" ? "Key" : "Output"} mappings must contain between 1 and ${maximum} lines.`);
  }
  const lookupFields = new Set<string>();
  const eventFields = new Set<string>();
  return entries.map((entry, index) => {
    const parts = entry.split(/[\t\n\v\f\r ]+/u);
    let lookupField: string;
    let eventField: string;
    if (parts.length === 1 && kind === "output") {
      lookupField = parts[0] ?? "";
      eventField = lookupField;
    } else if (parts.length === 3 && parts[1]?.toUpperCase() === "AS") {
      lookupField = parts[0] ?? "";
      eventField = parts[2] ?? "";
    } else {
      throw new TypeError(
        `${kind === "key" ? "Key" : "Output"} mapping ${index + 1} must use “lookup_column AS event_field”${kind === "output" ? " or one lookup column" : ""}.`,
      );
    }
    if (
      !isExactLookupColumn(lookupField)
      || !isExactEventField(eventField)
      || (kind === "key" && (isLookupOutputMarker(lookupField) || isLookupOutputMarker(eventField)))
    ) {
      throw new TypeError(`${kind === "key" ? "Key" : "Output"} mapping ${index + 1} must contain exact unquoted public field names.`);
    }
    if (lookupFields.has(lookupField)) {
      throw new TypeError(`${kind === "key" ? "Key" : "Output"} lookup column “${lookupField}” is repeated.`);
    }
    if (eventFields.has(eventField)) {
      throw new TypeError(`${kind === "key" ? "Key" : "Output"} event field “${eventField}” is repeated.`);
    }
    lookupFields.add(lookupField);
    eventFields.add(eventField);
    return { lookupField, eventField };
  });
}

function selectorPatterns(value: string) {
  return lines(value).map((pattern) => ({
    matchKind: selectorPatternKind(pattern),
    value: pattern,
  }));
}

function sharingScopeFromDraft(value: LookupDraftSharingScope): SharingScope {
  if (value === "private") return SharingScope.SHARING_SCOPE_PRIVATE;
  if (value === "app") return SharingScope.SHARING_SCOPE_APP;
  if (value === "global") return SharingScope.SHARING_SCOPE_GLOBAL;
  throw new TypeError("Lookup sharing scope is invalid.");
}

function sharingScopeToDraft(value: SharingScope): LookupDraftSharingScope {
  if (value === SharingScope.SHARING_SCOPE_PRIVATE) return "private";
  if (value === SharingScope.SHARING_SCOPE_APP) return "app";
  if (value === SharingScope.SHARING_SCOPE_GLOBAL) return "global";
  throw new TypeError("Lookup sharing scope is invalid.");
}

function overwriteFromDraft(value: LookupDraftOverwrite): KnowledgeOverwriteBehavior {
  if (value === "preserve") {
    return KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING;
  }
  if (value === "replace") {
    return KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING;
  }
  throw new TypeError("Lookup overwrite behavior is invalid.");
}

function overwriteToDraft(value: KnowledgeOverwriteBehavior): LookupDraftOverwrite {
  if (value === KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING) {
    return "preserve";
  }
  if (value === KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING) {
    return "replace";
  }
  throw new TypeError("Lookup overwrite behavior is invalid.");
}

export function createLookupDraft(appId: string): LookupDraft {
  return {
    appId,
    name: "",
    description: "",
    sharingScope: "app",
    automatic: false,
    indexPatterns: "",
    hostPatterns: "",
    sourcePatterns: "",
    sourcetypePatterns: "",
    keyMappings: "",
    outputMappings: "",
    overwrite: "preserve",
  };
}

export function lookupDefinitionFromDraft(draft: LookupDraft): LookupDefinition {
  if (!isExactPublicField(draft.name) || textBytes(draft.name) > LOOKUP_MANAGER_CONTRACT.maximumNameBytes) {
    throw new TypeError(`Lookup name must be an exact unquoted public name within ${LOOKUP_MANAGER_CONTRACT.maximumNameBytes} UTF-8 bytes.`);
  }
  if (
    textBytes(draft.description) > LOOKUP_MANAGER_CONTRACT.maximumDescriptionBytes
    || hasUnpairedSurrogate(draft.description)
    || /[\p{Cc}\p{Cf}]/u.test(draft.description)
  ) {
    throw new TypeError(`Lookup description must be control-free UTF-8 within ${LOOKUP_MANAGER_CONTRACT.maximumDescriptionBytes / 1024} KiB.`);
  }
  if (
    textBytes(draft.appId) > LOOKUP_MANAGER_CONTRACT.maximumAppIdBytes
    || !/^[A-Za-z0-9](?:[A-Za-z0-9._:-]*)$/u.test(draft.appId)
  ) throw new TypeError("A canonical app scope is required.");
  const selector = KnowledgeSelector.fromPartial({
    indexPatterns: selectorPatterns(draft.indexPatterns),
    hostPatterns: selectorPatterns(draft.hostPatterns),
    sourcePatterns: selectorPatterns(draft.sourcePatterns),
    sourcetypePatterns: selectorPatterns(draft.sourcetypePatterns),
  });
  if ([
    ...selector.indexPatterns,
    ...selector.hostPatterns,
    ...selector.sourcePatterns,
    ...selector.sourcetypePatterns,
  ].some((pattern) => pattern.matchKind === KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED)) {
    throw new TypeError("Lookup selector patterns contain an invalid escape sequence or non-canonical value.");
  }
  const definition = LookupDefinition.fromPartial({
    appId: draft.appId,
    name: draft.name,
    description: draft.description.length === 0 ? undefined : draft.description,
    sharingScope: sharingScopeFromDraft(draft.sharingScope),
    selector,
    automatic: draft.automatic,
    keyMappings: parseLookupMappings(draft.keyMappings, "key"),
    outputMappings: parseLookupMappings(draft.outputMappings, "output"),
    overwriteBehavior: overwriteFromDraft(draft.overwrite),
  });
  if (!isCanonicallyAuthorableLookupDefinition(definition)) {
    throw new TypeError(`Lookup mappings exceed the ${LOOKUP_MANAGER_CONTRACT.maximumAuthoredSourceBytes / 1024} KiB authored SPL ceiling.`);
  }
  return definition;
}

export function lookupDraftFromLookup(lookup: Lookup): LookupDraft {
  const definition = lookup.definition;
  if (definition === undefined) throw new TypeError("Lookup definition is unavailable.");
  return {
    appId: definition.appId,
    name: definition.name,
    description: definition.description ?? "",
    sharingScope: sharingScopeToDraft(definition.sharingScope),
    automatic: definition.automatic,
    indexPatterns: joinedPatterns(definition.selector?.indexPatterns ?? []),
    hostPatterns: joinedPatterns(definition.selector?.hostPatterns ?? []),
    sourcePatterns: joinedPatterns(definition.selector?.sourcePatterns ?? []),
    sourcetypePatterns: joinedPatterns(definition.selector?.sourcetypePatterns ?? []),
    keyMappings: definition.keyMappings.map((mapping) => mappingLine(mapping, false)).join("\n"),
    outputMappings: definition.outputMappings.map((mapping) => mappingLine(mapping, true)).join("\n"),
    overwrite: overwriteToDraft(definition.overwriteBehavior),
  };
}

/** Detaches and bounds a preview before any server-controlled arrays reach the DOM. */
export function normalizeLookupPreview(response: PreviewLookupResponse): SafeLookupPreview {
  if (
    response.columns.length > LOOKUP_MANAGER_CONTRACT.maximumColumns
    || response.rows.length > LOOKUP_MANAGER_CONTRACT.maximumPreviewRows
    || response.violations.length > LOOKUP_MANAGER_CONTRACT.maximumPreviewViolations
    || response.totalRows < 0n
    || response.totalRows > BigInt(LOOKUP_MANAGER_CONTRACT.maximumAssetRows)
    || response.sourceSha256.byteLength !== LOOKUP_MANAGER_CONTRACT.sha256Bytes
  ) throw new TypeError("Lookup preview response is outside the bounded contract.");
  const columns = [...response.columns];
  if (
    columns.some((column) => (
      column.length === 0
      || textBytes(column) > LOOKUP_MANAGER_CONTRACT.maximumHeaderBytes
      || column.trim() !== column
      || column.includes("\0")
      || [...column].some((character) => /[\p{Cc}\p{Cf}]/u.test(character))
      || hasUnpairedSurrogate(column)
    ))
    || new Set(columns).size !== columns.length
  ) {
    throw new TypeError("Lookup preview columns are invalid.");
  }
  const violations = response.violations.map((violation) => ({ ...violation }));
  if (violations.length > 0) {
    if (
      response.rows.length !== 0
      || response.truncated
      || (response.contentSha256.byteLength !== 0
        && response.contentSha256.byteLength !== LOOKUP_MANAGER_CONTRACT.sha256Bytes)
      || violations.some((violation) => (
        violation.fieldPath.length === 0
        || textBytes(violation.fieldPath) > LOOKUP_MANAGER_CONTRACT.maximumViolationFieldPathBytes
        || violation.code.length === 0
        || textBytes(violation.code) > LOOKUP_MANAGER_CONTRACT.maximumViolationCodeBytes
        || violation.message.length === 0
        || textBytes(violation.message) > LOOKUP_MANAGER_CONTRACT.maximumViolationMessageBytes
        || hasUnpairedSurrogate(violation.fieldPath)
        || hasUnpairedSurrogate(violation.code)
        || hasUnpairedSurrogate(violation.message)
      ))
    ) throw new TypeError("Lookup preview violations are outside the bounded contract.");
    if (
      (columns.length === 0 && (response.totalRows !== 0n || response.contentSha256.byteLength !== 0))
      || (columns.length > 0 && response.contentSha256.byteLength !== LOOKUP_MANAGER_CONTRACT.sha256Bytes)
    ) throw new TypeError("Lookup preview violation authority is inconsistent.");
    return {
      columns,
      rows: [],
      totalRows: response.totalRows,
      truncated: false,
      violations,
    };
  }
  if (
    columns.length === 0
    || response.contentSha256.byteLength !== LOOKUP_MANAGER_CONTRACT.sha256Bytes
    || response.totalRows < BigInt(response.rows.length)
    || response.truncated !== (response.totalRows > BigInt(response.rows.length))
  ) throw new TypeError("Lookup preview success authority is invalid.");
  let totalCellBytes = 0;
  const rows = response.rows.map((row) => {
    if (row.values.length !== columns.length) {
      throw new TypeError("Lookup preview row width does not match its columns.");
    }
    let rowBytes = 0;
    for (const value of row.values) {
      const bytes = textBytes(value);
      rowBytes += bytes;
      totalCellBytes += bytes;
      if (
        bytes > LOOKUP_MANAGER_CONTRACT.maximumCellBytes
        || rowBytes > LOOKUP_MANAGER_CONTRACT.maximumRowBytes
        || totalCellBytes > LOOKUP_MANAGER_CONTRACT.maximumUploadBytes
        || value.includes("\0")
        || hasUnpairedSurrogate(value)
      ) throw new TypeError("Lookup preview cell is outside its bounded contract.");
    }
    return [...row.values];
  });
  return {
    columns,
    rows,
    totalRows: response.totalRows,
    truncated: response.truncated,
    violations,
  };
}

export function lookupStateLabel(state: LookupState): string {
  if (state === LookupState.LOOKUP_STATE_ACTIVE) return "Active";
  if (state === LookupState.LOOKUP_STATE_DISABLED) return "Disabled";
  if (state === LookupState.LOOKUP_STATE_DELETED) return "Deleted";
  return "Unknown";
}

function sharingScopeLabel(scope: SharingScope): string {
  if (scope === SharingScope.SHARING_SCOPE_PRIVATE) return "Private";
  if (scope === SharingScope.SHARING_SCOPE_APP) return "App";
  if (scope === SharingScope.SHARING_SCOPE_GLOBAL) return "Global";
  return "Unknown";
}

function formatDate(value: Date | undefined): string {
  return formatMediumDateTime(value, "Unavailable");
}

export function LookupManagerPanel({
  apiBaseUrl,
  apps,
  initialAppId,
}: LookupManagerPanelProps) {
  const client = useMemo(
    () => createLookupManagerClient({ baseUrl: apiBaseUrl }),
    [apiBaseUrl],
  );
  const defaultAppId = apps.some((app) => app.appId === initialAppId)
    ? initialAppId ?? ""
    : apps[0]?.appId ?? "";
  const [state, setState] = useState<LoadState>("loading");
  const [lookups, setLookups] = useState<readonly Lookup[]>([]);
  const [appId, setAppId] = useState<string>(defaultAppId);
  const [filter, setFilter] = useState("");
  const [generation, setGeneration] = useState(0);
  const activeLoadGenerationRef = useRef(0);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [modal, setModal] = useState<LookupModal | null>(null);
  const [target, setTarget] = useState<Lookup | null>(null);
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ kind: NoticeKind; message: string } | null>(null);

  const reload = useCallback(() => setGeneration((value) => value + 1), []);

  useEffect(() => {
    activeLoadGenerationRef.current = generation;
    const controller = new AbortController();
    queueMicrotask(() => {
      if (controller.signal.aborted || activeLoadGenerationRef.current !== generation) return;
      setState("loading");
      setLoadError(null);
    });
    void client.list(appId || undefined, { signal: controller.signal }).then((loaded) => {
      if (controller.signal.aborted) return;
      if (loaded.length > LOOKUP_MANAGER_CONTRACT.maximumManagedLookups) {
        throw new TypeError("Lookup list exceeds its managed-object limit.");
      }
      const ids = new Set<string>();
      for (const lookup of loaded) {
        if (ids.has(lookup.lookupId)) {
          throw new TypeError("Lookup list contains a repeated identifier.");
        }
        ids.add(lookup.lookupId);
      }
      setLookups(loaded);
      setState("available");
    }).catch((error: unknown) => {
      if (controller.signal.aborted) return;
      setLookups([]);
      if (isOptionalRouteUnavailable(error)) {
        setState("unavailable");
      } else {
        setLoadError(errorMessage(error));
        setState("error");
      }
    });
    return () => controller.abort();
  }, [appId, client, generation]);

  const visibleLookups = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (needle.length === 0) return lookups;
    return lookups.filter((lookup) => {
      const definition = lookup.definition;
      return definition !== undefined && (
        definition.name.toLowerCase().includes(needle)
        || definition.description?.toLowerCase().includes(needle) === true
      );
    });
  }, [filter, lookups]);

  function closeModal(): void {
    if (busy) return;
    setModal(null);
    setTarget(null);
    setConfirmation("");
  }

  async function openReplace(lookup: Lookup): Promise<void> {
    setBusy(true);
    setNotice(null);
    try {
      const current = await client.get(lookup.lookupId);
      setTarget(current);
      setModal("replace");
    } catch (error) {
      setNotice({ kind: "error", message: errorMessage(error) });
    } finally {
      setBusy(false);
    }
  }

  async function createLookup(
    definition: LookupDefinition,
    csvData: Uint8Array | undefined,
  ): Promise<void> {
    if (csvData === undefined) throw new TypeError("Choose a CSV file before creating a lookup.");
    const created = await client.create(definition, csvData);
    setModal(null);
    setNotice({ kind: "success", message: `Lookup “${created.definition?.name ?? definition.name}” was created.` });
    reload();
  }

  async function replaceLookup(
    definition: LookupDefinition,
    csvData: Uint8Array | undefined,
  ): Promise<void> {
    if (target === null) throw new TypeError("The lookup replacement target is unavailable.");
    const replaced = await client.replace(
      target.lookupId,
      target.version,
      definition,
      csvData,
    );
    setModal(null);
    setTarget(null);
    setNotice({ kind: "success", message: `Lookup “${replaced.definition?.name ?? definition.name}” was replaced at version ${replaced.version.toLocaleString()}.` });
    reload();
  }

  async function changeState(lookup: Lookup): Promise<void> {
    const next = lookup.state === LookupState.LOOKUP_STATE_ACTIVE
      ? LookupState.LOOKUP_STATE_DISABLED
      : LookupState.LOOKUP_STATE_ACTIVE;
    setBusy(true);
    setNotice(null);
    try {
      const updated = await client.setState(lookup.lookupId, lookup.version, next);
      setNotice({
        kind: "success",
        message: `Lookup “${updated.definition?.name ?? lookup.definition?.name ?? lookup.lookupId}” is now ${lookupStateLabel(updated.state).toLowerCase()}.`,
      });
      reload();
    } catch (error) {
      setNotice({ kind: "error", message: errorMessage(error) });
    } finally {
      setBusy(false);
    }
  }

  async function deleteLookup(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    const name = target?.definition?.name;
    if (target === null || name === undefined || confirmation !== name) return;
    setBusy(true);
    setNotice(null);
    try {
      await client.delete(target.lookupId, target.version, confirmation);
      setModal(null);
      setTarget(null);
      setConfirmation("");
      setNotice({ kind: "success", message: `Lookup “${name}” was permanently deleted.` });
      reload();
    } catch (error) {
      setNotice({ kind: "error", message: errorMessage(error) });
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="admin-section-stack lookup-manager">
      <header className="admin-section-header">
        <div>
          <h2>Lookup tables</h2>
          <p>Publish bounded immutable CSV versions for exact SPL and automatic enrichment.</p>
        </div>
        <button
          className="button button--primary"
          type="button"
          disabled={state !== "available" || apps.length === 0 || busy}
          aria-describedby={state === "available" && apps.length === 0 ? "lookup-create-unavailable-reason" : undefined}
          onClick={() => {
            setTarget(null);
            setNotice(null);
            setModal("create");
          }}
        ><AppIcon name="plus" size="sm" /> Create lookup</button>
      </header>

      {state === "available" && apps.length === 0 ? (
        <p id="lookup-create-unavailable-reason" className="lookup-manager__create-reason" role="note">Create an app workspace before publishing a lookup table.</p>
      ) : null}

      {notice === null ? null : (
        <output
          className={`lookup-manager__notice lookup-manager__notice--${notice.kind}`}
          role={notice.kind === "error" ? "alert" : "status"}
        >
          <strong>{notice.kind === "error" ? "Lookup operation failed" : "Lookup catalog updated"}</strong>
          <span>{notice.message}</span>
        </output>
      )}

      <div className="lookup-manager__contract" role="note">
        <span aria-hidden="true">i</span>
        <p><strong>Exact lookup contract.</strong> CSV uploads are limited to {(LOOKUP_MANAGER_CONTRACT.maximumUploadBytes / (1024 * 1024)).toLocaleString()} MiB, {LOOKUP_MANAGER_CONTRACT.maximumAssetRows.toLocaleString()} rows, and {LOOKUP_MANAGER_CONTRACT.maximumColumns.toLocaleString()} columns. Keys match case-sensitive scalar strings; one lookup cannot fan out an event.</p>
      </div>

      <div className="lookup-manager__toolbar">
        <label htmlFor="lookup-app-filter">
          <span>App scope</span>
          <select
            id="lookup-app-filter"
            value={appId}
            disabled={state === "loading"}
            onChange={(event) => setAppId(event.currentTarget.value)}
          >
            <option value="">All managed apps</option>
            {apps.map((app) => <option value={app.appId} key={app.appId}>{app.label}</option>)}
          </select>
        </label>
        <label htmlFor="lookup-text-filter">
          <span>Filter loaded lookups</span>
          <input
            id="lookup-text-filter"
            type="search"
            value={filter}
            placeholder="Name or description"
            onChange={(event) => setFilter(event.currentTarget.value)}
          />
        </label>
        <button type="button" disabled={state === "loading"} onClick={reload}>Refresh</button>
      </div>

      {state === "loading" ? (
        <BackendResourceState kind="loading" title="Loading lookup tables" message="Reading the bounded lookup catalog pages…" />
      ) : null}
      {state === "unavailable" ? (
        <BackendResourceState kind="unavailable" title="Lookup management is unavailable" message="The connected server does not expose the complete lookup management API." action={<button type="button" onClick={reload}>Retry</button>} />
      ) : null}
      {state === "error" ? (
        <BackendResourceState kind="error" title="Lookup tables could not be loaded" message={loadError ?? "The lookup catalog request failed."} action={<button type="button" onClick={reload}>Retry</button>} />
      ) : null}
      {state === "available" && visibleLookups.length === 0 ? (
        <BackendResourceState kind="empty" title={lookups.length === 0 ? "No lookup tables" : "No matching lookup tables"} message={lookups.length === 0 ? apps.length === 0 ? "An app workspace is required before the first lookup can be published." : "Create a lookup or select a different app scope." : "Clear the local name filter to show loaded lookups."} action={apps.length === 0 ? <Link href="/admin/?section=apps">Manage apps</Link> : filter.length > 0 ? <button type="button" onClick={() => setFilter("")}>Clear filter</button> : undefined} />
      ) : null}
      {state === "available" && visibleLookups.length > 0 ? (
        <LookupManagerTable
          lookups={visibleLookups}
          busy={busy}
          onReplace={(lookup) => void openReplace(lookup)}
          onChangeState={(lookup) => void changeState(lookup)}
          onDelete={(lookup) => {
            setTarget(lookup);
            setConfirmation("");
            setNotice(null);
            setModal("delete");
          }}
        />
      ) : null}

      {state === "available" ? (
        <p className="lookup-manager__count" aria-live="polite">
          {visibleLookups.length.toLocaleString()} of {lookups.length.toLocaleString()} loaded lookup{lookups.length === 1 ? "" : "s"}
        </p>
      ) : null}

      {modal === "create" ? (
        <Modal
          wide
          dismissible={!busy}
          title="Create lookup table"
          subtitle="Upload one bounded CSV and publish its first immutable version."
          onClose={closeModal}
        >
          <LookupEditor
            client={client}
            apps={apps}
            initialAppId={appId || defaultAppId}
            currentLookup={null}
            busy={busy}
            error={notice?.kind === "error" ? notice.message : null}
            onBusyChange={setBusy}
            onSubmit={createLookup}
          />
        </Modal>
      ) : null}

      {modal === "replace" && target !== null ? (
        <Modal
          wide
          dismissible={!busy}
          title={`Replace ${target.definition?.name ?? "lookup"}`}
          subtitle={`Publish version ${(target.version + 1n).toLocaleString()}; omit the CSV to retain the current immutable asset.`}
          onClose={closeModal}
        >
          <LookupEditor
            client={client}
            apps={apps}
            initialAppId={target.definition?.appId ?? defaultAppId}
            currentLookup={target}
            busy={busy}
            error={notice?.kind === "error" ? notice.message : null}
            onBusyChange={setBusy}
            onSubmit={replaceLookup}
          />
        </Modal>
      ) : null}

      {modal === "delete" && target?.definition !== undefined ? (
        <Modal
          dismissible={!busy}
          title={`Delete ${target.definition.name}`}
          subtitle="Only a disabled lookup can be permanently deleted."
          onClose={closeModal}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={busy} onClick={closeModal}>Cancel</button>
              <button
                className="button button--danger"
                type="submit"
                form="delete-lookup-form"
                disabled={busy || confirmation !== target.definition.name}
              >{busy ? "Deleting…" : "Delete permanently"}</button>
            </>
          )}
        >
          <form id="delete-lookup-form" className="admin-form" onSubmit={(event) => void deleteLookup(event)}>
            {notice?.kind !== "error" ? null : (
              <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Lookup could not be deleted</strong><p>{notice.message}</p></div></div>
            )}
            <div className="access-mode-notice" role="alert"><span>!</span><div><strong>This cannot be undone</strong><p>Existing pinned search jobs retain their immutable version authority; new resolution will no longer find this lookup.</p></div></div>
            <label htmlFor="delete-lookup-confirmation">
              <span>Type <code>{target.definition.name}</code> to confirm</span>
              <input id="delete-lookup-confirmation" value={confirmation} autoComplete="off" onChange={(event) => setConfirmation(event.currentTarget.value)} />
            </label>
          </form>
        </Modal>
      ) : null}
    </div>
  );
}

export function LookupManagerTable({
  lookups,
  busy,
  onReplace,
  onChangeState,
  onDelete,
}: {
  lookups: readonly Lookup[];
  busy: boolean;
  onReplace: (lookup: Lookup) => void;
  onChangeState: (lookup: Lookup) => void;
  onDelete: (lookup: Lookup) => void;
}) {
  return (
    <div className="suite-card resource-table-card lookup-manager__table-card">
      <div className="table-wrap">
        <table className="table admin-resource-table lookup-manager__table">
          <caption className="sr-only">Lookup tables</caption>
          <thead><tr><th scope="col">Lookup</th><th scope="col">Shape</th><th scope="col">Matching</th><th scope="col">State</th><th scope="col">Updated</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
          <tbody>{lookups.map((lookup) => {
            const definition = lookup.definition;
            const stateLabel = lookupStateLabel(lookup.state);
            const lookupName = definition?.name ?? lookup.lookupId;
            return (
              <tr key={lookup.lookupId}>
                <td className="table-long-value">
                  <strong>{lookupName}</strong>
                  <small className="table-secondary">{definition?.appId ?? "Unknown app"} · v{lookup.version.toLocaleString()} · {sharingScopeLabel(definition?.sharingScope ?? SharingScope.SHARING_SCOPE_UNSPECIFIED)}</small>
                  {definition?.description ? <small className="table-secondary">{definition.description}</small> : null}
                </td>
                <td>{lookup.rowCount.toLocaleString()} rows<small className="table-secondary">{lookup.columns.length.toLocaleString()} columns · {summarizeByteQuantity(lookup.canonicalSizeBytes)}</small></td>
                <td>{definition?.automatic ? "Automatic + explicit" : "Explicit only"}<small className="table-secondary">{definition?.keyMappings.length ?? 0} key · {definition?.outputMappings.length ?? 0} output</small></td>
                <td><StatusLabel tone={stateLabel === "Active" ? "success" : "neutral"}>{stateLabel}</StatusLabel></td>
                <td>{formatDate(lookup.updatedAt)}</td>
                <td><div className="row-actions">
                  <button className="table-action" type="button" aria-label={`Replace lookup ${lookupName}`} disabled={busy || lookup.state === LookupState.LOOKUP_STATE_DELETED} onClick={() => onReplace(lookup)}>Replace</button>
                  <button className="table-action" type="button" aria-label={`${lookup.state === LookupState.LOOKUP_STATE_ACTIVE ? "Disable" : "Enable"} lookup ${lookupName}`} disabled={busy || (lookup.state !== LookupState.LOOKUP_STATE_ACTIVE && lookup.state !== LookupState.LOOKUP_STATE_DISABLED)} onClick={() => onChangeState(lookup)}>{lookup.state === LookupState.LOOKUP_STATE_ACTIVE ? "Disable" : "Enable"}</button>
                  {lookup.state === LookupState.LOOKUP_STATE_DISABLED ? <button className="table-action table-action--danger" type="button" aria-label={`Delete lookup ${lookupName}`} disabled={busy} onClick={() => onDelete(lookup)}>Delete</button> : null}
                </div></td>
              </tr>
            );
          })}</tbody>
        </table>
      </div>
    </div>
  );
}

function LookupEditor({
  client,
  apps,
  initialAppId,
  currentLookup,
  busy,
  error,
  onBusyChange,
  onSubmit,
}: LookupEditorProps) {
  const [draft, setDraft] = useState<LookupDraft>(() => currentLookup === null
    ? createLookupDraft(initialAppId)
    : lookupDraftFromLookup(currentLookup));
  const [csvData, setCSVData] = useState<Uint8Array | undefined>();
  const [csvName, setCSVName] = useState<string | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const [preview, setPreview] = useState<SafeLookupPreview | null>(null);
  const [previewBusy, setPreviewBusy] = useState(false);

  function update<Key extends keyof LookupDraft>(key: Key, value: LookupDraft[Key]): void {
    setDraft((current) => ({ ...current, [key]: value }));
    setPreview(null);
    setLocalError(null);
  }

  async function chooseCSV(file: File | undefined): Promise<void> {
    setPreview(null);
    setLocalError(null);
    setCSVData(undefined);
    setCSVName(null);
    if (file === undefined) return;
    if (file.size < 1 || file.size > LOOKUP_MANAGER_CONTRACT.maximumUploadBytes) {
      setLocalError(`CSV files must contain between 1 byte and ${LOOKUP_MANAGER_CONTRACT.maximumUploadBytes.toLocaleString()} bytes.`);
      return;
    }
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      if (bytes.byteLength !== file.size) throw new TypeError("The selected CSV changed while it was being read.");
      setCSVData(bytes);
      setCSVName(file.name);
    } catch (readError) {
      setLocalError(errorMessage(readError));
    }
  }

  async function requestPreview(): Promise<void> {
    if (csvData === undefined) {
      setLocalError("Choose a CSV file to preview this definition.");
      return;
    }
    setPreviewBusy(true);
    setLocalError(null);
    setPreview(null);
    try {
      const definition = lookupDefinitionFromDraft(draft);
      const result = await client.preview(definition, csvData, LOOKUP_MANAGER_CONTRACT.maximumPreviewRows);
      setPreview(normalizeLookupPreview(result));
    } catch (previewError) {
      setLocalError(errorMessage(previewError));
    } finally {
      setPreviewBusy(false);
    }
  }

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setLocalError(null);
    onBusyChange(true);
    try {
      const definition = lookupDefinitionFromDraft(draft);
      if (currentLookup === null && csvData === undefined) {
        throw new TypeError("Choose a CSV file before creating a lookup.");
      }
      await onSubmit(definition, csvData);
    } catch (submitError) {
      setLocalError(errorMessage(submitError));
    } finally {
      onBusyChange(false);
    }
  }

  return (
    <form className="lookup-manager__editor" id="lookup-editor-form" onSubmit={(event) => void submit(event)} noValidate>
      {error === null && localError === null ? null : (
        <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Lookup definition is not ready</strong><p>{localError ?? error}</p></div></div>
      )}

      <fieldset>
        <legend>Identity and visibility</legend>
        <div className="lookup-manager__editor-grid">
          <label htmlFor="lookup-editor-app"><span>App scope</span><select id="lookup-editor-app" value={draft.appId} onChange={(event) => update("appId", event.currentTarget.value)}>{apps.map((app) => <option value={app.appId} key={app.appId}>{app.label}</option>)}</select></label>
      <label htmlFor="lookup-editor-name"><span>Lookup name</span><input id="lookup-editor-name" value={draft.name} maxLength={LOOKUP_MANAGER_CONTRACT.maximumNameBytes} autoComplete="off" placeholder="service_catalog" onChange={(event) => update("name", event.currentTarget.value)} /><small>Exact unquoted name used by the SPL <code>lookup</code> command.</small></label>
          <label htmlFor="lookup-editor-scope"><span>Sharing</span><select id="lookup-editor-scope" value={draft.sharingScope} onChange={(event) => update("sharingScope", event.currentTarget.value as LookupDraftSharingScope)}><option value="private">Private</option><option value="app">App</option><option value="global">Global</option></select></label>
          <label className="lookup-manager__editor-wide" htmlFor="lookup-editor-description"><span>Description <small>(optional)</small></span><input id="lookup-editor-description" value={draft.description} onChange={(event) => update("description", event.currentTarget.value)} /></label>
          <label className="admin-checkbox lookup-manager__editor-wide"><input type="checkbox" aria-label="Apply lookup automatically" checked={draft.automatic} onChange={(event) => update("automatic", event.currentTarget.checked)} /><span><strong>Apply automatically</strong><small>Run after Tier-1 calculated fields and before the authored base-search predicate when selectors match.</small></span></label>
        </div>
      </fieldset>

      <fieldset>
        <legend>Exact field mappings</legend>
        <div className="lookup-manager__mapping-grid">
          <label htmlFor="lookup-editor-keys"><span>Key mappings <small>(1–{LOOKUP_MANAGER_CONTRACT.maximumKeyMappings})</small></span><textarea id="lookup-editor-keys" value={draft.keyMappings} placeholder={"service_id AS service_key\nregion AS event_region"} onChange={(event) => update("keyMappings", event.currentTarget.value)} /><small>One <code>lookup_column AS event_field</code> per line. Key AS is required.</small></label>
          <label htmlFor="lookup-editor-outputs"><span>Output mappings <small>(1–{LOOKUP_MANAGER_CONTRACT.maximumOutputMappings})</small></span><textarea id="lookup-editor-outputs" value={draft.outputMappings} placeholder={"owner AS service_owner\ntier"} onChange={(event) => update("outputMappings", event.currentTarget.value)} /><small>One mapping per line. A column without AS writes to the same event-field name.</small></label>
        </div>
        <label className="lookup-manager__overwrite" htmlFor="lookup-editor-overwrite"><span>On output collision</span><select id="lookup-editor-overwrite" value={draft.overwrite} onChange={(event) => update("overwrite", event.currentTarget.value as LookupDraftOverwrite)}><option value="preserve">Preserve existing (OUTPUTNEW)</option><option value="replace">Replace existing (OUTPUT)</option></select></label>
      </fieldset>

      <fieldset>
        <legend>Automatic lookup selectors <small>(optional)</small></legend>
        <p className="lookup-manager__fieldset-note">One exact or wildcard pattern per line. Empty dimensions do not restrict matching.</p>
        <div className="lookup-manager__selector-grid">
          <label htmlFor="lookup-editor-indexes"><span>Indexes</span><textarea id="lookup-editor-indexes" value={draft.indexPatterns} onChange={(event) => update("indexPatterns", event.currentTarget.value)} /></label>
          <label htmlFor="lookup-editor-hosts"><span>Hosts</span><textarea id="lookup-editor-hosts" value={draft.hostPatterns} onChange={(event) => update("hostPatterns", event.currentTarget.value)} /></label>
          <label htmlFor="lookup-editor-sources"><span>Sources</span><textarea id="lookup-editor-sources" value={draft.sourcePatterns} onChange={(event) => update("sourcePatterns", event.currentTarget.value)} /></label>
          <label htmlFor="lookup-editor-sourcetypes"><span>Sourcetypes</span><textarea id="lookup-editor-sourcetypes" value={draft.sourcetypePatterns} onChange={(event) => update("sourcetypePatterns", event.currentTarget.value)} /></label>
        </div>
      </fieldset>

      <fieldset>
        <legend>Immutable CSV asset</legend>
        <div className="lookup-manager__upload">
          <label htmlFor="lookup-editor-csv"><span>{currentLookup === null ? "CSV file" : "Replacement CSV (optional)"}</span><input id="lookup-editor-csv" type="file" accept=".csv,text/csv" onChange={(event) => void chooseCSV(event.currentTarget.files?.[0])} /><small>{csvName === null ? currentLookup === null ? `Choose a UTF-8 RFC 4180-style CSV up to ${(LOOKUP_MANAGER_CONTRACT.maximumUploadBytes / (1024 * 1024)).toLocaleString()} MiB.` : "No file selected; saving will retain the current asset." : `${csvName} · ${csvData?.byteLength.toLocaleString() ?? 0} bytes`}</small></label>
          <button type="button" disabled={busy || previewBusy || csvData === undefined} onClick={() => void requestPreview()}>{previewBusy ? "Validating preview…" : "Validate and preview"}</button>
        </div>
        {preview === null ? null : <LookupPreviewTable preview={preview} />}
      </fieldset>

      <div className="lookup-manager__editor-actions">
        <p>{currentLookup === null ? "Create publishes version 1 only after the complete definition and CSV validate." : "Replace uses the current version as optimistic concurrency authority."}</p>
        <button className="button button--primary" type="submit" disabled={busy || previewBusy || (preview !== null && preview.violations.length !== 0) || (currentLookup === null && csvData === undefined)}>{busy ? "Publishing…" : currentLookup === null ? "Create lookup" : "Publish replacement"}</button>
      </div>
    </form>
  );
}

export function LookupPreviewTable({ preview }: { preview: SafeLookupPreview }) {
  const keyedViolations = withOccurrenceKeys(
    preview.violations,
    (violation) => JSON.stringify(violation),
  );
  const keyedRows = withOccurrenceKeys(preview.rows, (row) => JSON.stringify(row));
  return (
    <section className="lookup-manager__preview" aria-labelledby="lookup-preview-title">
      <header>
        <div><h3 id="lookup-preview-title">{preview.violations.length === 0 ? "Validated CSV preview" : "CSV preview needs attention"}</h3><p>{preview.violations.length === 0 ? `${preview.rows.length.toLocaleString()} of ${preview.totalRows.toLocaleString()} rows shown${preview.truncated ? " · preview truncated" : ""}` : "No data rows were returned because publication validation failed."}</p></div>
        <span>{preview.columns.length.toLocaleString()} columns</span>
      </header>
      {preview.violations.length === 0 ? null : (
        <ul className="lookup-manager__violations" aria-label="Lookup validation violations">
          {keyedViolations.map(({ key, value: violation }) => <li key={key}><code>{violation.code}</code> · {violation.fieldPath}: {violation.message}</li>)}
        </ul>
      )}
      {preview.violations.length > 0 ? null : preview.rows.length === 0 ? <p className="lookup-manager__empty-preview">The CSV contains a header and no data rows.</p> : (
        <div className="table-wrap">
          <table className="table"><caption className="sr-only">Validated lookup CSV preview</caption><thead><tr>{preview.columns.map((column) => <th key={column} scope="col">{column}</th>)}</tr></thead><tbody>{keyedRows.map(({ key, value: row }) => <tr key={key}>{row.map((value, columnIndex) => <td key={preview.columns[columnIndex]}>{value === "" ? <i aria-label="empty string">empty</i> : value}</td>)}</tr>)}</tbody></table>
        </div>
      )}
    </section>
  );
}

function withOccurrenceKeys<Value>(
  values: readonly Value[],
  fingerprint: (value: Value) => string,
): Array<{ key: string; value: Value }> {
  const counts = new Map<string, number>();
  return values.map((value) => {
    const base = fingerprint(value);
    const occurrence = counts.get(base) ?? 0;
    counts.set(base, occurrence + 1);
    return { key: `${base}\0${occurrence}`, value };
  });
}

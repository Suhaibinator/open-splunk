import Link from "next/link";

import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  IngestionTokenPurpose,
  IngestionTokenState,
  type IngestionToken,
} from "@/gen/ts/open_splunk/collector_admin";
import type { GetHECOperationalSnapshotResponse } from "@/gen/ts/open_splunk/hec_admin_api";
import {
  IndexAccessState,
  IndexState,
  type Index,
} from "@/gen/ts/open_splunk/index";
import {
  supportsServerFeature,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import { boundedIndexSearchQuery, searchLaunchHref } from "@/lib/search/launch-url";

import { AppIcon } from "../_components/app-icon";
import { BackendResourceState } from "../_components/backend-resource-state";
import { formatMediumDateTime } from "../_components/date-format";
import { FieldNote, fieldControlProps } from "../_components/field-validation";
import { StatusLabel, type StatusTone } from "../_components/status";
import {
  INDEX_POLICY_FIELDS,
  INDEX_POLICY_KEYS,
  TOKEN_POLICY_FIELDS,
  TOKEN_POLICY_KEYS,
  indexPolicyErrors,
  indexPolicyFieldHint,
  policyFieldInputMode,
  tokenPolicyErrors,
  tokenPolicyFieldHint,
  type IndexPolicyForm,
  type PolicyFieldKind,
  type TokenPolicyForm,
} from "./ingestion-policy-form";
import { SearchLimitsSettings } from "./search-limits-settings";
import {
  hecProfileSummary,
  tokenPurposeLabel,
  tokenUsesHEC,
  validHECMetadataDefault,
} from "./token-creation";
import type { BackendAdminSection as AdminSection } from "./knowledge-manager-feature";

export type ResourceState = "loading" | "available" | "unavailable" | "error";

export interface TokenIndexScopeOption {
  id: string;
  name: string;
  displayName: string;
  ingestible: boolean;
}

export type TokenScopeSource = "index-admin" | "bootstrap" | "unavailable";

export function formatDate(value: Date | undefined): string {
  return formatMediumDateTime(value, "Never");
}

export function formatDuration(seconds: bigint | undefined): string {
  if (seconds === undefined || seconds <= 0n) return "Forever";
  const days = seconds / 86_400n;
  if (days > 0n && seconds % 86_400n === 0n) return `${days.toLocaleString()} days`;
  const hours = seconds / 3_600n;
  if (hours > 0n && seconds % 3_600n === 0n) return `${hours.toLocaleString()} hours`;
  return `${seconds.toLocaleString()} seconds`;
}

function formatOperationalDuration(
  duration: { seconds: bigint; nanos: number } | undefined,
): string {
  if (duration === undefined) return "Not reported";
  if (duration.nanos === 0) return `${duration.seconds.toLocaleString()} seconds`;
  const fractional = duration.nanos.toString().padStart(9, "0").replace(/0+$/, "");
  return `${duration.seconds.toLocaleString()}.${fractional} seconds`;
}

function countLabel(
  loaded: number,
  totalSize: bigint | null,
  totalSizeExact: boolean,
  singular: string,
  plural: string,
): string {
  const loadedLabel = loaded === 1 ? singular : plural;
  if (totalSize !== null && totalSizeExact) {
    const totalLabel = totalSize === 1n ? singular : plural;
    return BigInt(loaded) < totalSize
      ? `${loaded.toLocaleString()} of ${totalSize.toLocaleString()} ${totalLabel} loaded`
      : `${totalSize.toLocaleString()} ${totalLabel}`;
  }
  if (totalSize !== null) {
    return `${loaded.toLocaleString()} ${loadedLabel} loaded · server estimate ${totalSize.toLocaleString()}`;
  }
  return `${loaded.toLocaleString()} ${loadedLabel} loaded`;
}

export function indexStateLabel(state: IndexState): string {
  if (state === IndexState.INDEX_STATE_ACTIVE) return "Active";
  if (state === IndexState.INDEX_STATE_ARCHIVED) return "Archived";
  if (state === IndexState.INDEX_STATE_DELETING) return "Deleting";
  return "Unknown";
}

function indexAccessLabel(state: IndexAccessState | undefined): string {
  if (state === IndexAccessState.INDEX_ACCESS_STATE_ENABLED) return "Enabled";
  if (state === IndexAccessState.INDEX_ACCESS_STATE_DISABLED) return "Disabled";
  return "Unknown";
}

export function tokenStateLabel(state: IngestionTokenState): string {
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE) return "Active";
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED) return "Disabled";
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED) return "Revoked";
  if (state === IngestionTokenState.INGESTION_TOKEN_STATE_EXPIRED) return "Expired";
  return "Unknown";
}

export function tokenCanBeRevoked(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
}

export function tokenCanSetEnabled(token: IngestionToken): boolean {
  return token.state === IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE
    || token.state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
}

export function statusTone(label: string): StatusTone {
  if (label === "Active") return "success";
  if (label === "Deleting") return "running";
  if (label === "Unknown") return "warning";
  return "neutral";
}

/**
 * The numeric policy fields, one shape for the index form and the token form.
 *
 * The field id is derived from the form key, so the control and its note cannot
 * be given different names -- that pairing is what lets `fieldControlProps`
 * point `aria-describedby` at the message the field is showing.
 */
export function PolicyNumberField({
  error,
  field,
  fieldKey,
  hint,
  idPrefix,
  onChange,
  value,
}: {
  error: string | null;
  field: { kind: PolicyFieldKind; label: string; placeholder: string };
  fieldKey: string;
  hint: string;
  idPrefix: string;
  onChange: (value: string) => void;
  value: string;
}) {
  const fieldId = `${idPrefix}-${fieldKey.replaceAll(/(?<=[a-z])(?=[A-Z])/gu, "-").toLowerCase()}`;
  return (
    <label htmlFor={fieldId}>
      <span>{field.label}</span>
      <input
        autoComplete="off"
        id={fieldId}
        // A quantity carries its own unit, so it is a text field: `type="number"`
        // would refuse "512 KiB" by silently keeping the control empty, and a
        // silent rejection is what the visible validation here exists to replace.
        inputMode={policyFieldInputMode(field.kind)}
        onChange={(event) => onChange(event.target.value)}
        placeholder={field.placeholder}
        spellCheck={false}
        value={value}
        {...fieldControlProps(fieldId, error)}
      />
      <FieldNote error={error} fieldId={fieldId}>{hint}</FieldNote>
    </label>
  );
}

export function IndexPolicyFields({
  idPrefix,
  value,
  onChange,
}: {
  idPrefix: string;
  value: IndexPolicyForm;
  onChange: (value: Partial<IndexPolicyForm>) => void;
}) {
  const errors = indexPolicyErrors(value);
  const sourcetypeId = `${idPrefix}-default-sourcetype`;
  return (
    <fieldset>
      <legend>Ingestion policy <small>(optional)</small></legend>
      <div className="admin-policy-grid">
        <label htmlFor={sourcetypeId}>
          <span>Default sourcetype</span>
          <input aria-describedby={`${sourcetypeId}-note`} id={sourcetypeId} maxLength={255} onChange={(event) => onChange({ defaultSourcetype: event.target.value })} placeholder="_json" value={value.defaultSourcetype} />
          <small id={`${sourcetypeId}-note`}>Applied when an admitted event does not provide a sourcetype.</small>
        </label>
        {INDEX_POLICY_KEYS.map((key) => (
          <PolicyNumberField
            error={errors[key]}
            field={INDEX_POLICY_FIELDS[key]}
            fieldKey={key}
            hint={indexPolicyFieldHint(key, value)}
            idPrefix={idPrefix}
            key={key}
            onChange={(next) => onChange({ [key]: next })}
            value={value[key]}
          />
        ))}
      </div>
    </fieldset>
  );
}

export function TokenPolicyFields({
  idPrefix,
  value,
  onChange,
}: {
  idPrefix: string;
  value: TokenPolicyForm;
  onChange: (value: Partial<TokenPolicyForm>) => void;
}) {
  const errors = tokenPolicyErrors(value);
  const patternFields = [
    {
      error: errors.allowedHostRegexes,
      hint: "One complete-value Go/RE2 pattern per line. Empty means any host.",
      id: `${idPrefix}-host-patterns`,
      label: "Allowed host patterns",
      onChange: (next: string) => onChange({ allowedHostRegexes: next }),
      placeholder: "api-[0-9]+\nworker-[0-9]+",
      value: value.allowedHostRegexes,
    },
    {
      error: errors.allowedSourceRegexes,
      hint: "One complete-value Go/RE2 pattern per line. Empty means any source.",
      id: `${idPrefix}-source-patterns`,
      label: "Allowed source patterns",
      onChange: (next: string) => onChange({ allowedSourceRegexes: next }),
      placeholder: "/var/log/application\\.log",
      value: value.allowedSourceRegexes,
    },
  ];
  return (
    <fieldset>
      <legend>Admission policy <small>(optional)</small></legend>
      <div className="admin-policy-grid">
        {patternFields.map((field) => (
          <label htmlFor={field.id} key={field.id}>
            <span>{field.label}</span>
            <textarea
              id={field.id}
              onChange={(event) => field.onChange(event.target.value)}
              placeholder={field.placeholder}
              rows={3}
              spellCheck={false}
              value={field.value}
              {...fieldControlProps(field.id, field.error)}
            />
            <FieldNote error={field.error} fieldId={field.id}>{field.hint}</FieldNote>
          </label>
        ))}
        {TOKEN_POLICY_KEYS.map((key) => (
          <PolicyNumberField
            error={errors[key]}
            field={TOKEN_POLICY_FIELDS[key]}
            fieldKey={key}
            hint={tokenPolicyFieldHint(key, value)}
            idPrefix={idPrefix}
            key={key}
            onChange={(next) => onChange({ [key]: next })}
            value={value[key]}
          />
        ))}
      </div>
    </fieldset>
  );
}

interface TokenScopePickerProps {
  idPrefix: string;
  options: TokenIndexScopeOption[];
  selected: Set<string>;
  onChange: (value: Set<string>) => void;
  disabled?: boolean;
}

interface HECTokenProfileFieldsProps {
  idPrefix: string;
  selectedIndexes: Set<string>;
  defaultIndex: string;
  onDefaultIndexChange: (value: string) => void;
  defaultHost: string;
  onDefaultHostChange: (value: string) => void;
  defaultSource: string;
  onDefaultSourceChange: (value: string) => void;
  defaultSourcetype: string;
  onDefaultSourcetypeChange: (value: string) => void;
  indexerAcknowledgment: boolean;
  onIndexerAcknowledgmentChange: (value: boolean) => void;
  acknowledgmentReadOnly?: boolean;
}

export function HECTokenProfileFields(props: HECTokenProfileFieldsProps) {
  const metadataFields = [
    {
      key: "host",
      label: "Default host",
      value: props.defaultHost,
      placeholder: "api.example.com",
      onChange: props.onDefaultHostChange,
    },
    {
      key: "source",
      label: "Default source",
      value: props.defaultSource,
      placeholder: "http:orders",
      onChange: props.onDefaultSourceChange,
    },
    {
      key: "sourcetype",
      label: "Default sourcetype",
      value: props.defaultSourcetype,
      placeholder: "_json",
      onChange: props.onDefaultSourcetypeChange,
    },
  ] as const;
  return (
    <fieldset>
      <legend>HEC profile</legend>
      <label htmlFor={`${props.idPrefix}-hec-default-index`}>
        <span>Default index <small>(optional)</small></span>
        <select id={`${props.idPrefix}-hec-default-index`} value={props.defaultIndex} onChange={(event) => props.onDefaultIndexChange(event.target.value)} aria-invalid={props.defaultIndex.length > 0 && !props.selectedIndexes.has(props.defaultIndex)}>
          <option value="">No token default (requests must provide an index)</option>
          {[...props.selectedIndexes].toSorted().map((name) => <option value={name} key={name}>{name}</option>)}
        </select>
        <small>When set, this index must remain in the token&apos;s allowed scope. Allowed scope alone is never an implicit default.</small>
      </label>
      {metadataFields.map((field) => {
        const valid = validHECMetadataDefault(field.value);
        const bytes = new TextEncoder().encode(field.value).byteLength;
        return (
          <label htmlFor={`${props.idPrefix}-hec-default-${field.key}`} key={field.key}>
            <span>{field.label} <small>(optional)</small></span>
            <input id={`${props.idPrefix}-hec-default-${field.key}`} value={field.value} onChange={(event) => field.onChange(event.target.value)} placeholder={field.placeholder} autoComplete="off" spellCheck={false} aria-invalid={!valid} />
            <small>{bytes.toLocaleString()} / 255 UTF-8 bytes. Values are preserved exactly and cannot contain controls or surrounding ASCII whitespace.</small>
          </label>
        );
      })}
      <label className="admin-checkbox" htmlFor={`${props.idPrefix}-hec-indexer-acknowledgment`} aria-label="Enable HEC indexer acknowledgment">
        <input id={`${props.idPrefix}-hec-indexer-acknowledgment`} type="checkbox" checked={props.indexerAcknowledgment} disabled={props.acknowledgmentReadOnly} onChange={(event) => props.onIndexerAcknowledgmentChange(event.target.checked)} />
        <span><strong>Indexer acknowledgment</strong><small>{props.acknowledgmentReadOnly ? "This setting is immutable. Rotate the token to change acknowledgment mode." : "Enable channel-scoped acknowledgment IDs. This choice cannot be changed after creation."}</small></span>
      </label>
    </fieldset>
  );
}

export function TokenScopePicker({ idPrefix, options, selected, onChange, disabled = false }: TokenScopePickerProps) {
  const optionByName = new Map(options.map((option) => [option.name, option]));
  const ingestibleNames = options.filter((option) => option.ingestible).map((option) => option.name);
  const ingestibleSet = new Set(ingestibleNames);
  const choices = [...ingestibleNames, ...[...selected].filter((name) => !ingestibleSet.has(name))];

  return (
    <fieldset>
      <legend>Allowed indexes</legend>
      {choices.map((name) => {
        const option = optionByName.get(name);
        const available = ingestibleSet.has(name);
        const inputId = `${idPrefix}-index-${option?.id ?? name}`;
        return (
          <label className="admin-checkbox" htmlFor={inputId} aria-label={`Allow ingestion to ${name}`} key={name}>
            <input
              id={inputId}
              type="checkbox"
              checked={selected.has(name)}
              disabled={disabled}
              onChange={(event) => {
                const next = new Set(selected);
                if (event.target.checked) next.add(name);
                else next.delete(name);
                onChange(next);
              }}
            />
            <span>
              <strong>{name}</strong>
              <small>{available
                ? option?.displayName || "Ingestion enabled"
                : disabled
                  ? "Current scope · index eligibility unavailable"
                  : "Unavailable for ingestion · remove to save"}</small>
            </span>
          </label>
        );
      })}
      {choices.length === 0 ? <p className="resource-footnote">No active, ingestion-enabled indexes are available.</p> : null}
    </fieldset>
  );
}

interface BackendOverviewProps {
  bootstrap: SystemBootstrapModel | null;
  bootstrapError: string | null;
  indexState: ResourceState;
  indexCount: number;
  indexTotalSize: bigint | null;
  indexTotalSizeExact: boolean;
  activeIndexes: number;
  tokenState: ResourceState;
  tokenCount: number;
  tokenTotalSize: bigint | null;
  tokenTotalSizeExact: boolean;
  activeTokens: number;
  onNavigate: (section: AdminSection) => void;
  onReload: () => void;
}

export function BackendOverview(props: BackendOverviewProps) {
  const { bootstrap } = props;
  const indexCount = countLabel(
    props.indexCount,
    props.indexTotalSize,
    props.indexTotalSizeExact,
    "index",
    "indexes",
  );
  const tokenCount = countLabel(
    props.tokenCount,
    props.tokenTotalSize,
    props.tokenTotalSizeExact,
    "token",
    "tokens",
  );
  const indexDetail = props.indexState === "available"
    ? `${props.activeIndexes.toLocaleString()} active in loaded records`
    : props.indexState === "loading"
      ? "Loading catalog…"
      : props.indexState === "error"
        ? "Load failed"
        : "Route unavailable";
  const tokenDetail = props.tokenState === "available"
    ? `${props.activeTokens.toLocaleString()} active in loaded records`
    : props.tokenState === "loading"
      ? "Loading tokens…"
      : props.tokenState === "error"
        ? "Load failed"
        : "Route unavailable";
  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>System overview</h2><p>Capabilities reported by the available server routes.</p></div><button className="button" type="button" onClick={props.onReload}>Refresh</button></header>
      <div className="admin-summary-grid">
        <article><span className="summary-icon summary-icon--green" aria-hidden="true">▦</span><div><small>Indexes</small><strong>{props.indexState === "available" ? indexCount : "—"}</strong><p>{indexDetail}</p></div><button type="button" onClick={() => props.onNavigate("indexes")}>Manage</button></article>
        <article><span className="summary-icon summary-icon--blue" aria-hidden="true">⇣</span><div><small>Ingestion tokens</small><strong>{props.tokenState === "available" ? tokenCount : "—"}</strong><p>{tokenDetail}</p></div><button type="button" onClick={() => props.onNavigate("collectors")}>Inspect</button></article>
        <article><span className="summary-icon summary-icon--violet" aria-hidden="true">⌕</span><div><small>Source revision</small><strong>{bootstrap?.build?.sourceRevision.slice(0, 12) || "—"}</strong><p>{bootstrap === null ? "Bootstrap unavailable" : bootstrap.build === null ? "Not reported" : "Build identity"}</p></div><Link href="/search/events/">Search</Link></article>
        <article><span className="summary-icon summary-icon--orange" aria-hidden="true">↻</span><div><small>Result retention</small><strong>{bootstrap !== null && bootstrap.limits.searchResultRetentionMs > 0 ? `${Math.round(bootstrap.limits.searchResultRetentionMs / 60_000)}m` : "—"}</strong><p>{bootstrap === null ? "Bootstrap unavailable" : "Read-only server limit"}</p></div><button type="button" onClick={() => props.onNavigate("server")}>Limits</button></article>
      </div>
      {bootstrap === null ? (
        <BackendResourceState
          kind="error"
          title="System bootstrap could not be loaded"
          message={`${props.bootstrapError ?? "The bootstrap route did not return a usable response."} Index and token routes were checked independently and remain available where shown.`}
          action={<button type="button" onClick={props.onReload}>Retry bootstrap</button>}
        />
      ) : (
        <section className="suite-card">
          <header className="suite-card-header"><div><h3>Connection details</h3><p>Values returned by system bootstrap.</p></div><StatusLabel tone="success">Connected</StatusLabel></header>
          <dl className="backend-definition-list">
            <div><dt>Source revision</dt><dd>{bootstrap.build?.sourceRevision || "Not reported"}</dd></div>
            <div><dt>UI build ID</dt><dd>{bootstrap.build?.uiBuildId || "Not reported"}</dd></div>
            <div><dt>Server time</dt><dd>{formatDate(bootstrap.serverTime)}</dd></div>
            <div><dt>Feature flags</dt><dd>{bootstrap.features.size.toLocaleString()}</dd></div>
          </dl>
        </section>
      )}
    </div>
  );
}

interface BackendIndexesProps {
  state: ResourceState;
  error: string | null;
  filter: string;
  indexes: Index[];
  totalIndexes: number;
  totalSize: bigint | null;
  totalSizeExact: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  paginationError: string | null;
  busy: string | null;
  onFilterChange: (value: string) => void;
  onLoadMore: () => void;
  onReload: () => void;
  onEdit: (index: Index) => void;
  onChangeState: (index: Index) => void;
  onDelete: (index: Index) => void;
}

export function BackendIndexes(props: BackendIndexesProps) {
  if (props.state === "loading") return <BackendResourceState kind="loading" title="Loading indexes" message="Reading the server index catalog…" />;
  if (props.state === "unavailable") return <BackendResourceState kind="unavailable" title="Index administration is unavailable" message="The connected server did not register the index administration routes." action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  if (props.state === "error") return <BackendResourceState kind="error" title="Indexes could not be loaded" message={props.error ?? "The server rejected the index catalog request."} action={<button type="button" onClick={props.onReload}>Retry</button>} />;

  const loadedCount = countLabel(
    props.totalIndexes,
    props.totalSize,
    props.totalSizeExact,
    "index",
    "indexes",
  );

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Indexes</h2><p>Authoritative index definitions from the connected server.</p></div><span>{loadedCount}</span></header>
      <div className="resource-toolbar"><label><span className="sr-only">Filter loaded indexes</span><i aria-hidden="true"><AppIcon name="search" size="sm" /></i><input value={props.filter} onChange={(event) => props.onFilterChange(event.target.value)} placeholder="Filter loaded indexes" /></label><button className="button button--toolbar" type="button" onClick={props.onReload}><AppIcon name="refresh" size="sm" /> Refresh</button></div>
      {props.indexes.length === 0 ? (
        <BackendResourceState kind="empty" title={props.totalIndexes === 0 ? "No indexes configured" : "No matching indexes"} message={props.totalIndexes === 0 ? "Create an index to begin accepting and searching data." : "Try another index name or description."} action={props.totalIndexes > 0 && props.filter.trim().length > 0 ? <button type="button" onClick={() => props.onFilterChange("")}>Clear filter</button> : undefined} />
      ) : (
        <div className="suite-card resource-table-card">
          <div className="table-wrap">
            <table className="table admin-resource-table">
              <caption className="sr-only">Configured indexes</caption>
              <thead><tr><th scope="col">Name</th><th scope="col">State</th><th scope="col">Ingestion</th><th scope="col">Search</th><th scope="col">Retention</th><th scope="col">Updated</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
              <tbody>{props.indexes.map((index) => {
                const definition = index.definition;
                const name = definition?.name || index.indexId;
                const state = indexStateLabel(index.state);
                const canChange = index.state === IndexState.INDEX_STATE_ACTIVE || index.state === IndexState.INDEX_STATE_ARCHIVED;
                const canEdit = index.state !== IndexState.INDEX_STATE_DELETING && definition !== undefined;
                const canSearch = index.state === IndexState.INDEX_STATE_ACTIVE
                  && definition?.searchAccess === IndexAccessState.INDEX_ACCESS_STATE_ENABLED;
                const nameContent = <><span aria-hidden="true">▦</span><div><strong>{definition?.displayName || name}</strong><small>index={name}{definition?.description ? ` · ${definition.description}` : ""}</small></div></>;
                return (
                  <tr key={index.indexId}>
                    <td className="table-long-value">{canSearch
                      ? <Link className="resource-name" href={searchLaunchHref(boundedIndexSearchQuery(name))} aria-label={`Search index ${name}`}>{nameContent}</Link>
                      : <div className="resource-name" aria-label={`Index ${name} is not currently searchable`}>{nameContent}</div>}
                    </td>
                    <td><StatusLabel tone={statusTone(state)}>{state}</StatusLabel></td>
                    <td>{indexAccessLabel(definition?.ingestionAccess)}</td>
                    <td>{indexAccessLabel(definition?.searchAccess)}</td>
                    <td>{formatDuration(definition?.retentionPeriod?.seconds)}</td>
                    <td>{formatDate(index.updatedAt)}</td>
                    <td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit index ${name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onEdit(index)}>{props.busy === `read-index-${index.indexId}` ? "Loading…" : "Edit"}</button><button className="table-action" type="button" aria-label={`${index.state === IndexState.INDEX_STATE_ACTIVE ? "Archive" : "Reactivate"} index ${name}`} disabled={!canChange || props.busy !== null} onClick={() => props.onChangeState(index)}>{props.busy === `index-${index.indexId}` ? "Updating…" : index.state === IndexState.INDEX_STATE_ACTIVE ? "Archive" : "Reactivate"}</button><button className="table-action table-action--danger" type="button" aria-label={`Delete index ${name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onDelete(index)}>Delete</button></div></td>
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
        </div>
      )}
      <div className="admin-pagination-footer" aria-live="polite">
        <div>
          <strong>{loadedCount}</strong>
          {props.filter.trim().length === 0 ? null : <small>{props.indexes.length.toLocaleString()} matching loaded records</small>}
          {props.paginationError === null ? null : <small className="table-warning-detail">{props.paginationError}</small>}
        </div>
        {props.hasMore
          ? <button className="button button--secondary" type="button" disabled={props.loadingMore || props.busy !== null} onClick={props.onLoadMore}>{props.loadingMore ? "Loading…" : "Load more indexes"}</button>
          : null}
      </div>
      <p className="resource-footnote">Event counts, storage use, and the bounded field catalog are available from the Datasets page. Delete uses a current version, an exact-name confirmation, and an explicit physical-data mode.</p>
    </div>
  );
}

interface BackendTokensProps {
  state: ResourceState;
  error: string | null;
  indexState: ResourceState;
  indexError: string | null;
  scopeSource: TokenScopeSource;
  tokens: IngestionToken[];
  totalSize: bigint | null;
  totalSizeExact: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  paginationError: string | null;
  busy: string | null;
  canCreate: boolean;
  createBlockReason: string | null;
  recoveryActionLabel: string | null;
  onResolveRecovery: () => void;
  onEdit: (token: IngestionToken) => void;
  onLoadMore: () => void;
  onReload: () => void;
  onRevoke: (token: IngestionToken) => void;
  onSetEnabled: (token: IngestionToken, enabled: boolean) => void;
}

export function BackendTokens(props: BackendTokensProps) {
  if (props.state === "loading") return <BackendResourceState kind="loading" title="Loading ingestion tokens" message="Reading token metadata from the server…" />;
  if (props.state === "unavailable") return <BackendResourceState kind="unavailable" title="Ingestion tokens are unavailable" message="The connected server did not register the ingestion-token routes. Collector fleet status is loaded independently from its own capability-gated panel." action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  if (props.state === "error") return <BackendResourceState kind="error" title="Ingestion tokens could not be loaded" message={props.error ?? "The server rejected the token list request."} action={<button type="button" onClick={props.onReload}>Retry</button>} />;
  const loadedCount = countLabel(
    props.tokens.length,
    props.totalSize,
    props.totalSizeExact,
    "token",
    "tokens",
  );
  const indexAdminDetail = props.indexState === "loading"
    ? "The versioned index catalog is still loading."
    : props.indexError ?? "The versioned index catalog route is unavailable.";

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Ingestion tokens</h2><p>Manage server-issued ingestion credentials and their index scopes.</p></div></header>
      {props.createBlockReason === null ? null : (
        <div id="ingestion-token-create-disabled-reason" className="access-mode-notice token-create-disabled-reason" role="note">
          <span>!</span>
          <div>
            <strong>Token generation is locked</strong>
            <p>{props.createBlockReason}</p>
            {props.recoveryActionLabel === null ? null : (
              <button className="button button--secondary" type="button" onClick={props.onResolveRecovery}>
                {props.recoveryActionLabel}
              </button>
            )}
          </div>
        </div>
      )}
      {props.indexState === "available" ? null : (
        <div className="access-mode-notice" role="note">
          <span>!</span>
          <div>
            <strong>{props.scopeSource === "bootstrap" ? "Using bootstrap index summaries" : "Index scope data unavailable"}</strong>
            <p>{props.scopeSource === "bootstrap"
              ? `${indexAdminDetail} Token generation and scope edits remain available using bootstrap eligibility data.`
              : props.indexError === null
                ? "Existing tokens can still be inspected, edited, and revoked. Token generation and index-scope changes require an authoritative index summary."
                : `${props.indexError} Existing tokens remain available, but token generation and index-scope changes are disabled.`}</p>
          </div>
        </div>
      )}
      <section className="suite-card token-section token-section--credentials">
        <header className="suite-card-header"><div><h3>Issued credentials</h3><p>Token secrets are never returned after creation. {loadedCount}.</p></div><button type="button" onClick={props.onReload}>Refresh</button></header>
        {props.tokens.length === 0 ? (
          <BackendResourceState
            kind="empty"
            title="No ingestion tokens"
            message={props.canCreate
              ? "Generate a token scoped to an active, ingestible index."
              : props.scopeSource !== "unavailable"
                ? "No active, ingestion-enabled index is currently available for a new token."
                : "The token route is available, but generation is disabled until an authoritative index summary loads."}
          />
        ) : (
          <div className="table-wrap"><table className="table"><caption className="sr-only">Issued ingestion credentials</caption><thead><tr><th scope="col">Name</th><th scope="col">Purpose</th><th scope="col">Prefix</th><th scope="col">Allowed indexes</th><th scope="col">Expires</th><th scope="col">Last used</th><th scope="col">State</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead><tbody>{props.tokens.map((token) => {
            const state = tokenStateLabel(token.state);
            const canRevoke = tokenCanBeRevoked(token);
            const canSetEnabled = tokenCanSetEnabled(token);
            const enable = token.state === IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED;
            const hecToken = tokenUsesHEC(token.purpose);
            const nativeToken = token.purpose === IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR;
            const canEdit = canRevoke && (hecToken || nativeToken);
            return <tr key={token.ingestionTokenId}><td><strong>{token.name}</strong>{token.description ? <small className="table-secondary">{token.description}</small> : null}</td><td><strong>{tokenPurposeLabel(token.purpose)}</strong><small className="table-secondary">{hecToken ? `Indexer ACK ${token.hecProfile?.indexerAcknowledgment ? "enabled" : "disabled"}` : nativeToken ? "gRPC ingestion" : "Transport unavailable"}</small>{hecToken ? <small className="table-secondary">{hecProfileSummary(token.hecProfile)}</small> : null}</td><td><code>{token.tokenPrefix}</code></td><td className="table-long-value">{token.constraints?.allowedIndexNames.join(", ") || "None"}<small className="table-secondary">{hecToken ? token.hecProfile?.defaultIndexName ? `Default ${token.hecProfile.defaultIndexName}` : "No token default index" : nativeToken ? token.constraints?.boundCollectorId === undefined ? "Native collector binding required" : `Collector ${token.constraints.boundCollectorId}` : "Purpose unavailable"}</small></td><td>{formatDate(token.expiresAt)}</td><td>{formatDate(token.lastUsedAt)}</td><td><StatusLabel tone={statusTone(state)}>{state}</StatusLabel></td><td><div className="row-actions"><button className="table-action" type="button" aria-label={`Edit token ${token.name}`} disabled={!canEdit || props.busy !== null} onClick={() => props.onEdit(token)}>{props.busy === `read-token-${token.ingestionTokenId}` ? "Loading…" : "Edit"}</button><button className="table-action" type="button" aria-label={`${enable ? "Enable" : "Disable"} token ${token.name}`} disabled={!canSetEnabled || props.busy !== null} onClick={() => props.onSetEnabled(token, enable)}>{props.busy === `token-state-${token.ingestionTokenId}` ? enable ? "Enabling…" : "Disabling…" : canSetEnabled ? enable ? "Enable" : "Disable" : "—"}</button><button className="table-action" type="button" aria-label={`Revoke token ${token.name}`} disabled={!canRevoke || props.busy !== null} onClick={() => props.onRevoke(token)}>{props.busy === `token-${token.ingestionTokenId}` ? "Revoking…" : canRevoke ? "Revoke" : "—"}</button></div></td></tr>;
          })}</tbody></table></div>
        )}
        <div className="admin-pagination-footer admin-pagination-footer--inset" aria-live="polite">
          <div>
            <strong>{loadedCount}</strong>
            {props.paginationError === null ? null : <small className="table-warning-detail">{props.paginationError}</small>}
          </div>
          {props.hasMore
            ? <button className="button button--secondary" type="button" disabled={props.loadingMore || props.busy !== null} onClick={props.onLoadMore}>{props.loadingMore ? "Loading…" : "Load more tokens"}</button>
            : null}
        </div>
      </section>
    </div>
  );
}

export function BackendServerSettings({
  client,
  bootstrap,
  error,
  hecState,
  hecSnapshot,
  hecError,
  onReload,
  onStatus,
  onDirtyChange,
}: {
  client: OpenSplunkApiClient;
  bootstrap: SystemBootstrapModel | null;
  error: string | null;
  hecState: ResourceState;
  hecSnapshot: GetHECOperationalSnapshotResponse | null;
  hecError: string | null;
  onReload: () => void;
  onStatus: (message: string, kind: "success" | "warning") => void;
  onDirtyChange: (dirty: boolean) => void;
}) {
  if (bootstrap === null) {
    return (
      <BackendResourceState
        kind="error"
        title="Server limits could not be loaded"
        message={error ?? "The system bootstrap route did not return a usable response."}
        action={<button type="button" onClick={onReload}>Retry bootstrap</button>}
      />
    );
  }
  const limits = bootstrap.limits;
  const editable = supportsServerFeature(bootstrap, ServerFeature.SERVER_FEATURE_SERVER_SETTINGS_ADMIN);
  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Server settings</h2><p>{editable ? "Persistent node-wide search resource limits." : "Read-only limits advertised to this browser."}</p></div><span>{editable ? "Administrator settings" : "Bootstrap values"}</span></header>
      {editable ? <SearchLimitsSettings client={client} onStatus={onStatus} onDirtyChange={onDirtyChange} /> : <><div className="access-mode-notice" role="note"><span>i</span><div><strong>Configuration writes are unavailable</strong><p>The backend does not advertise editable server settings. These values cannot be changed from this page.</p></div></div>
      <section className="suite-card settings-group">
        <header><h3>Search and result limits</h3><p>Authoritative limits returned by system bootstrap.</p></header>
        <dl className="backend-definition-list">
          <div><dt>Maximum page size</dt><dd>{limits.maximumPageSize.toLocaleString()}</dd></div>
          <div><dt>Default search timeout</dt><dd>{limits.defaultSearchTimeoutMs > 0 ? `${(limits.defaultSearchTimeoutMs / 1_000).toLocaleString()} seconds` : "Not reported"}</dd></div>
          <div><dt>Result retention</dt><dd>{limits.searchResultRetentionMs > 0 ? `${(limits.searchResultRetentionMs / 60_000).toLocaleString()} minutes` : "Not reported"}</dd></div>
          <div><dt>Maximum export rows</dt><dd>{limits.maximumExportRows > 0n ? limits.maximumExportRows.toLocaleString() : "Not reported"}</dd></div>
          <div><dt>Maximum export bytes</dt><dd>{limits.maximumExportBytes > 0n ? limits.maximumExportBytes.toLocaleString() : "Not reported"}</dd></div>
          <div><dt>Maximum timeline buckets</dt><dd>{limits.maximumTimelineBuckets > 0 ? limits.maximumTimelineBuckets.toLocaleString() : "Not available"}</dd></div>
        </dl>
      </section></>}
      {hecState === "unavailable" ? (
        <div className="access-mode-notice" role="note"><span>i</span><div><strong>HTTP Event Collector is disabled</strong><p>The server does not advertise HEC ingestion. HEC token creation and test commands remain unavailable until the data-plane feature is enabled.</p></div></div>
      ) : hecState === "loading" ? (
        <BackendResourceState kind="loading" title="Loading HEC operations" message="Reading the administrator operational snapshot…" />
      ) : hecState === "error" || hecSnapshot === null ? (
        <BackendResourceState kind="error" title="HEC operations could not be loaded" message={hecError ?? "The operational snapshot was empty."} action={<button type="button" onClick={onReload}>Retry</button>} />
      ) : (
        <>
          <section className="suite-card settings-group">
            <header><h3>HTTP Event Collector operations</h3><p>Process-wide counters observed {formatDate(hecSnapshot.observedAt)}.</p></header>
            <dl className="backend-definition-list">
              <div><dt>Requests</dt><dd>{hecSnapshot.request?.requests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Accepted requests</dt><dd>{hecSnapshot.request?.acceptedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Events</dt><dd>{hecSnapshot.request?.events.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Uncompressed bytes</dt><dd>{hecSnapshot.request?.uncompressedBytes.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Authentication failures</dt><dd>{hecSnapshot.request?.authenticationFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Decode failures</dt><dd>{hecSnapshot.request?.decodeFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Event-policy failures</dt><dd>{hecSnapshot.request?.eventPolicyFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Rate-limited requests</dt><dd>{hecSnapshot.request?.rateLimitedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Staging failures</dt><dd>{hecSnapshot.request?.stagingFailures.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Staging duration</dt><dd>{formatOperationalDuration(hecSnapshot.request?.stagingDuration)}</dd></div>
              <div><dt>Shutdown rejections</dt><dd>{hecSnapshot.request?.shutdownRejections.toLocaleString() ?? "Not reported"}</dd></div>
            </dl>
          </section>
          <section className="suite-card settings-group">
            <header><h3>Durability and acknowledgment</h3><p>Queue capacity, reconciliation, and indexer-acknowledgment health.</p></header>
            <dl className="backend-definition-list">
              <div><dt>Durable queue</dt><dd>{hecSnapshot.durable?.queueAvailable ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Request capacity</dt><dd>{hecSnapshot.durable?.requestCapacityAvailable ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Pending outbox reservations</dt><dd>{hecSnapshot.durable?.pendingOutboxReservations.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Pending outbox bytes</dt><dd>{hecSnapshot.durable?.pendingOutboxBytes.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Oldest pending age</dt><dd>{formatOperationalDuration(hecSnapshot.durable?.oldestPendingOutboxAge)}</dd></div>
              <div><dt>Retained requests</dt><dd>{hecSnapshot.durable?.retainedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Reconciliation</dt><dd>{hecSnapshot.reconciliation?.available ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Reconciliation successes</dt><dd>{hecSnapshot.reconciliation?.successes.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Reconciliation retries</dt><dd>{hecSnapshot.reconciliation?.retries.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Reconciliation ambiguities</dt><dd>{hecSnapshot.reconciliation?.ambiguities.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK service</dt><dd>{hecSnapshot.acknowledgments?.available ? "Available" : "Unavailable"}</dd></div>
              <div><dt>Active ACK channels</dt><dd>{hecSnapshot.acknowledgments?.activeChannels.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Retained ACK channels</dt><dd>{hecSnapshot.acknowledgments?.retainedChannels.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Pending ACK rows</dt><dd>{hecSnapshot.acknowledgments?.pendingRows.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Indexed ACK rows</dt><dd>{hecSnapshot.acknowledgments?.indexedRows.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Expired ACK rows</dt><dd>{hecSnapshot.acknowledgments?.expiredRows.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>Terminal failed requests</dt><dd>{hecSnapshot.acknowledgments?.terminalFailedRequests.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK queries</dt><dd>{hecSnapshot.acknowledgments?.queries.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK IDs queried</dt><dd>{hecSnapshot.acknowledgments?.idsQueried.toLocaleString() ?? "Not reported"}</dd></div>
              <div><dt>ACK query misses</dt><dd>{hecSnapshot.acknowledgments?.misses.toLocaleString() ?? "Not reported"}</dd></div>
            </dl>
          </section>
          <section className="suite-card settings-group">
            <header><h3>HEC protocol failures</h3><p>Bounded non-success response codes reported by the HEC compatibility layer.</p></header>
            {hecSnapshot.protocolFailures.length === 0 ? <p className="settings-group__empty">No protocol failures have been observed.</p> : (
              <dl className="backend-definition-list">
                {hecSnapshot.protocolFailures.map((metric) => <div key={metric.code}><dt>Response code {metric.code}</dt><dd>{metric.count.toLocaleString()}</dd></div>)}
              </dl>
            )}
          </section>
        </>
      )}
    </div>
  );
}

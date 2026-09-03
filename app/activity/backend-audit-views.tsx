"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from "react";

import {
  AuditTargetKind,
  type AuditAction,
  type AuditEvent,
} from "@/gen/ts/open_splunk/audit";
import type { SearchAttemptAuditEvent } from "@/gen/ts/open_splunk/search_attempt_audit";
import {
  createOpenSplunkApiClient,
  recordNextPageToken,
  RepeatedPageCursorError,
} from "@/lib/api";

import { BackendResourceState } from "../_components/backend-resource-state";
import { formatActivityCount, formatActivityDate } from "./backend-activity-shared";
import {
  auditActionLabel,
  auditActorKindLabel,
  auditActorRoleLabel,
  auditErrorPresentation,
  auditIdentifierFromDraft,
  auditTargetDetails,
  auditTargetKindLabel,
  listMutationAuditEvents,
  listSearchAttemptAuditEvents,
  mutationAuditActionOptions,
  mutationAuditTargetOptions,
  type AuditErrorPresentation,
  type AuditListClient,
  type AuditPage,
  type MutationAuditFilters,
  type SearchAttemptAuditFilters,
} from "./backend-audit-data";

interface AuditViewProps {
  apiBaseUrl: string;
  maximumPageSize: number;
}

type AuditLoadState = "loading" | "available" | "error";

function pageSizeOptions(maximumPageSize: number): number[] {
  const maximum = Math.max(1, Math.min(maximumPageSize || 50, 200));
  return [...new Set([25, 50, 100, 200, maximum].filter((value) => value <= maximum))].toSorted((a, b) => a - b);
}

function auditClient(apiBaseUrl: string): AuditListClient {
  return createOpenSplunkApiClient({ baseUrl: apiBaseUrl });
}

function sameMutationFilters(left: MutationAuditFilters, right: MutationAuditFilters): boolean {
  return left.actorId === right.actorId
    && left.targetKind === right.targetKind
    && left.actions.length === right.actions.length
    && left.actions.every((value, index) => value === right.actions[index]);
}

function sameAttemptFilters(left: SearchAttemptAuditFilters, right: SearchAttemptAuditFilters): boolean {
  return left.actorId === right.actorId && left.ownerId === right.ownerId;
}

function useAuditTraversal<T extends { sequence: bigint }>(
  label: string,
  loadPage: (pageToken: string | undefined, signal: AbortSignal) => Promise<AuditPage<T>>,
) {
  const [state, setState] = useState<AuditLoadState>("loading");
  const [items, setItems] = useState<T[]>([]);
  const [totalSize, setTotalSize] = useState(0n);
  const [nextPageToken, setNextPageToken] = useState<string | null>(null);
  const [error, setError] = useState<AuditErrorPresentation | null>(null);
  const [loadMoreError, setLoadMoreError] = useState<AuditErrorPresentation | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [generation, setGeneration] = useState(0);
  const activeLoadGenerationRef = useRef(0);
  const initialAbortRef = useRef<AbortController | null>(null);
  const moreAbortRef = useRef<AbortController | null>(null);
  const tokensSeenRef = useRef(new Set<string>());
  const sequencesSeenRef = useRef(new Set<bigint>());
  const reload = useCallback(() => setGeneration((value) => value + 1), []);

  useEffect(() => {
    activeLoadGenerationRef.current = generation;
    initialAbortRef.current?.abort();
    moreAbortRef.current?.abort();
    moreAbortRef.current = null;
    const controller = new AbortController();
    initialAbortRef.current = controller;
    let current = true;
    queueMicrotask(() => {
      if (!current) return;
      setState("loading");
      setItems([]);
      setTotalSize(0n);
      setNextPageToken(null);
      setError(null);
      setLoadMoreError(null);
      setLoadingMore(false);
    });
    tokensSeenRef.current.clear();
    sequencesSeenRef.current.clear();
    void (async () => {
      try {
        const page = await loadPage(undefined, controller.signal);
        if (!current || controller.signal.aborted || activeLoadGenerationRef.current !== generation) return;
        for (const item of page.items) sequencesSeenRef.current.add(item.sequence);
        setItems(page.items);
        setTotalSize(page.totalSize);
        setNextPageToken(recordNextPageToken(tokensSeenRef.current, page.nextPageToken, label));
        setState("available");
      } catch (reason) {
        if (!current || controller.signal.aborted) return;
        setError(auditErrorPresentation(reason, label));
        setState("error");
      }
    })();
    return () => {
      current = false;
      controller.abort();
    };
  }, [generation, label, loadPage]);

  useEffect(() => () => {
    initialAbortRef.current?.abort();
    moreAbortRef.current?.abort();
  }, []);

  const loadMore = useCallback(async () => {
    if (nextPageToken === null || moreAbortRef.current !== null) return;
    const controller = new AbortController();
    moreAbortRef.current = controller;
    setLoadingMore(true);
    setLoadMoreError(null);
    try {
      const page = await loadPage(nextPageToken, controller.signal);
      if (controller.signal.aborted) return;
      if (page.totalSize !== totalSize) {
        throw new TypeError(`${label} changed its exact total during one traversal.`);
      }
      const duplicate = page.items.find((item) => sequencesSeenRef.current.has(item.sequence));
      if (duplicate !== undefined) throw new TypeError(`${label} repeated sequence ${duplicate.sequence.toString()} across pages.`);
      const last = items.at(-1);
      const first = page.items[0];
      if (last !== undefined && first !== undefined && last.sequence <= first.sequence) {
        throw new TypeError(`${label} continued outside descending sequence order.`);
      }
      const combinedItemCount = BigInt(items.length + page.items.length);
      if (
        combinedItemCount > page.totalSize
        || (page.nextPageToken === null && combinedItemCount !== page.totalSize)
        || (page.nextPageToken !== null && combinedItemCount >= page.totalSize)
      ) {
        throw new TypeError(`${label} continuation did not match its retained exact total.`);
      }
      const validatedNextPageToken = recordNextPageToken(
        tokensSeenRef.current,
        page.nextPageToken,
        label,
      );
      for (const item of page.items) sequencesSeenRef.current.add(item.sequence);
      setItems((current) => [...current, ...page.items]);
      setNextPageToken(validatedNextPageToken);
    } catch (reason) {
      if (!controller.signal.aborted) {
        const presentation = auditErrorPresentation(reason, label);
        setLoadMoreError(presentation);
        if (presentation.invalidTraversal || reason instanceof RepeatedPageCursorError || reason instanceof TypeError) {
          setNextPageToken(null);
        }
      }
    } finally {
      if (moreAbortRef.current === controller) {
        moreAbortRef.current = null;
        setLoadingMore(false);
      }
    }
  }, [items, label, loadPage, nextPageToken, totalSize]);

  return { state, items, totalSize, nextPageToken, error, loadMoreError, loadingMore, loadMore, reload };
}

function AuditPaging({
  loaded,
  total,
  hasNext,
  loading,
  error,
  onLoadMore,
  onRefresh,
}: {
  loaded: number;
  total: bigint;
  hasNext: boolean;
  loading: boolean;
  error: AuditErrorPresentation | null;
  onLoadMore(): void;
  onRefresh(): void;
}) {
  return (
    <>
      <output className="backend-list-notice backend-list-notice--action">
        <span>Showing {loaded.toLocaleString()} of {formatActivityCount(total)} exact matches.</span>
        <div className="audit-paging-actions">
          <button type="button" onClick={onRefresh}>Refresh</button>
          {hasNext ? <button type="button" aria-busy={loading} disabled={loading} onClick={onLoadMore}>{loading ? "Loading more…" : "Load more"}</button> : null}
        </div>
      </output>
      {error === null ? null : <div className="backend-inline-error" role="alert"><strong>{error.title}.</strong> {error.message} {error.invalidTraversal ? <button type="button" onClick={onRefresh}>Start again</button> : null}</div>}
    </>
  );
}

function PageSizeField({ maximumPageSize, value, onChange }: { maximumPageSize: number; value: number; onChange(value: number): void }) {
  return (
    <label>
      <span>Rows per page</span>
      <select value={value} onChange={(event) => onChange(Number(event.target.value))}>
        {pageSizeOptions(maximumPageSize).map((option) => <option key={option} value={option}>{option}</option>)}
      </select>
    </label>
  );
}

export function MutationAuditTargetProjection({ event }: { event: AuditEvent }) {
  const details = auditTargetDetails(event);
  return (
    <>
      <strong>{event.targetId}</strong>
      <small>{auditTargetKindLabel(event.targetKind)}</small>
      {details === null ? null : <small>{details}</small>}
    </>
  );
}

export function BackendMutationAudit({ apiBaseUrl, maximumPageSize }: AuditViewProps) {
  const client = useMemo(() => auditClient(apiBaseUrl), [apiBaseUrl]);
  const initialPageSize = Math.min(50, Math.max(maximumPageSize, 1), 200);
  const [draftActions, setDraftActions] = useState<AuditAction[]>([]);
  const [draftActorId, setDraftActorId] = useState("");
  const [draftTargetKind, setDraftTargetKind] = useState<AuditTargetKind | undefined>();
  const [draftPageSize, setDraftPageSize] = useState(initialPageSize);
  const [filters, setFilters] = useState<MutationAuditFilters>({ actions: [] });
  const [pageSize, setPageSize] = useState(initialPageSize);
  const loadPage = useCallback((pageToken: string | undefined, signal: AbortSignal) => listMutationAuditEvents(
    client,
    filters,
    { pageSize, pageToken, signal },
  ), [client, filters, pageSize]);
  const traversal = useAuditTraversal("Mutation audit", loadPage);
  const normalizedDraftActorId = auditIdentifierFromDraft(draftActorId);
  const dirty = !sameMutationFilters(filters, {
    actions: draftActions,
    actorId: normalizedDraftActorId,
    targetKind: draftTargetKind,
  }) || pageSize !== draftPageSize;

  function applyFilters(event: FormEvent) {
    event.preventDefault();
    setFilters({
      actions: [...draftActions].toSorted((left, right) => left - right),
      actorId: normalizedDraftActorId,
      targetKind: draftTargetKind,
    });
    setPageSize(draftPageSize);
    if (!dirty) traversal.reload();
  }

  function clearFilters() {
    setDraftActions([]);
    setDraftActorId("");
    setDraftTargetKind(undefined);
    setFilters({ actions: [] });
  }

  return (
    <div className="backend-activity-view">
      {traversal.state === "loading" ? <BackendResourceState kind="loading" title="Loading mutation audit" message="Reading successful token, index, app, saved-search, and knowledge-object mutations…" /> : null}
      {traversal.state === "error" && traversal.error !== null ? <BackendResourceState kind={traversal.error.title.endsWith("unavailable") ? "unavailable" : "error"} title={traversal.error.title} message={traversal.error.message} action={<button type="button" onClick={traversal.reload}>Retry</button>} /> : null}
      {traversal.state === "available" ? (
        <>
          <output className="live-jobs-snapshot"><span><i aria-hidden="true" />This journal contains successful mutations only; it does not include rejected changes, authentication, collectors, or search activity.</span></output>
          <form className="suite-card audit-filter-card" onSubmit={applyFilters}>
            <header><div><h2>Mutation filters</h2><p>Every filter is exact and runs on the server.</p></div><div><button type="button" disabled={!dirty && filters.actions.length === 0 && filters.actorId === undefined && filters.targetKind === undefined} onClick={clearFilters}>Clear</button><button className="button button--primary" type="submit">{dirty ? "Apply filters" : "Refresh"}</button></div></header>
            <div className="audit-filter-grid">
              <label className="audit-action-filter"><span>Actions</span><select multiple size={5} value={draftActions.map(String)} onChange={(event) => setDraftActions([...event.currentTarget.selectedOptions].map((option) => Number(option.value) as AuditAction))}>{mutationAuditActionOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select><small>Select one or more; no selection means every action.</small></label>
              <label><span>Actor ID</span><input value={draftActorId} onChange={(event) => setDraftActorId(event.target.value)} placeholder="Exact actor ID" /></label>
              <label><span>Target kind</span><select value={draftTargetKind ?? ""} onChange={(event) => setDraftTargetKind(event.target.value === "" ? undefined : Number(event.target.value) as AuditTargetKind)}><option value="">Every target kind</option>{mutationAuditTargetOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
              <PageSizeField maximumPageSize={maximumPageSize} value={draftPageSize} onChange={setDraftPageSize} />
            </div>
          </form>
          <AuditPaging loaded={traversal.items.length} total={traversal.totalSize} hasNext={traversal.nextPageToken !== null} loading={traversal.loadingMore} error={traversal.loadMoreError} onLoadMore={() => void traversal.loadMore()} onRefresh={traversal.reload} />
          <section className="suite-card activity-jobs-card">
            {traversal.items.length === 0 ? <BackendResourceState kind="empty" title="No mutation events" message="No retained successful mutations match these exact filters." /> : (
              <div className="table-wrap"><table className="table audit-table"><caption className="sr-only">Successful mutation audit events</caption><thead><tr><th scope="col">Occurred</th><th scope="col">Action</th><th scope="col">Actor</th><th scope="col">Target</th><th scope="col">Version</th><th scope="col">Sequence</th></tr></thead><tbody>{traversal.items.map((event: AuditEvent) => <tr key={event.sequence.toString()}><td data-label="Occurred"><time dateTime={event.occurredAt?.toISOString()}>{formatActivityDate(event.occurredAt ?? null)}</time></td><td data-label="Action"><strong>{auditActionLabel(event.action)}</strong></td><td data-label="Actor"><strong>{event.actorId}</strong><small>{auditActorKindLabel(event.actorKind)} · {auditActorRoleLabel(event.actorRole)}</small></td><td data-label="Target"><MutationAuditTargetProjection event={event} /></td><td data-label="Version" className="numeric-data">{formatActivityCount(event.targetVersion)}</td><td data-label="Sequence" className="numeric-data">{formatActivityCount(event.sequence)}</td></tr>)}</tbody></table></div>
            )}
          </section>
        </>
      ) : null}
    </div>
  );
}

export function BackendSearchAttemptAudit({ apiBaseUrl, maximumPageSize }: AuditViewProps) {
  const client = useMemo(() => auditClient(apiBaseUrl), [apiBaseUrl]);
  const initialPageSize = Math.min(50, Math.max(maximumPageSize, 1), 200);
  const [draftActorId, setDraftActorId] = useState("");
  const [draftOwnerId, setDraftOwnerId] = useState("");
  const [draftPageSize, setDraftPageSize] = useState(initialPageSize);
  const [filters, setFilters] = useState<SearchAttemptAuditFilters>({});
  const [pageSize, setPageSize] = useState(initialPageSize);
  const loadPage = useCallback((pageToken: string | undefined, signal: AbortSignal) => listSearchAttemptAuditEvents(
    client,
    filters,
    { pageSize, pageToken, signal },
  ), [client, filters, pageSize]);
  const traversal = useAuditTraversal("Search-attempt audit", loadPage);
  const nextFilters = {
    actorId: auditIdentifierFromDraft(draftActorId),
    ownerId: auditIdentifierFromDraft(draftOwnerId),
  };
  const dirty = !sameAttemptFilters(filters, nextFilters) || pageSize !== draftPageSize;

  function applyFilters(event: FormEvent) {
    event.preventDefault();
    setFilters(nextFilters);
    setPageSize(draftPageSize);
    if (!dirty) traversal.reload();
  }

  function clearFilters() {
    setDraftActorId("");
    setDraftOwnerId("");
    setFilters({});
  }

  return (
    <div className="backend-activity-view">
      {traversal.state === "loading" ? <BackendResourceState kind="loading" title="Loading search-attempt audit" message="Reading retained admitted-search metadata…" /> : null}
      {traversal.state === "error" && traversal.error !== null ? <BackendResourceState kind={traversal.error.title.endsWith("unavailable") ? "unavailable" : "error"} title={traversal.error.title} message={traversal.error.message} action={<button type="button" onClick={traversal.reload}>Retry</button>} /> : null}
      {traversal.state === "available" ? (
        <>
          <output className="live-jobs-snapshot"><span><i aria-hidden="true" />This rolling journal records admitted attempts only. It deliberately contains no SPL, result content, or terminal outcome.</span></output>
          <form className="suite-card audit-filter-card" onSubmit={applyFilters}>
            <header><div><h2>Search-attempt filters</h2><p>Actor and owner filters are exact and run on the server.</p></div><div><button type="button" disabled={!dirty && filters.actorId === undefined && filters.ownerId === undefined} onClick={clearFilters}>Clear</button><button className="button button--primary" type="submit">{dirty ? "Apply filters" : "Refresh"}</button></div></header>
            <div className="audit-filter-grid audit-filter-grid--attempts">
              <label><span>Actor ID</span><input value={draftActorId} onChange={(event) => setDraftActorId(event.target.value)} placeholder="Exact actor ID" /></label>
              <label><span>Owner ID</span><input value={draftOwnerId} onChange={(event) => setDraftOwnerId(event.target.value)} placeholder="Exact owner ID" /></label>
              <PageSizeField maximumPageSize={maximumPageSize} value={draftPageSize} onChange={setDraftPageSize} />
            </div>
          </form>
          <AuditPaging loaded={traversal.items.length} total={traversal.totalSize} hasNext={traversal.nextPageToken !== null} loading={traversal.loadingMore} error={traversal.loadMoreError} onLoadMore={() => void traversal.loadMore()} onRefresh={traversal.reload} />
          <section className="suite-card activity-jobs-card">
            {traversal.items.length === 0 ? <BackendResourceState kind="empty" title="No search attempts" message="No retained admitted searches match these exact filters." /> : (
              <div className="table-wrap"><table className="table audit-table"><caption className="sr-only">Admitted search-attempt audit events</caption><thead><tr><th scope="col">Occurred</th><th scope="col">Actor</th><th scope="col">Owner</th><th scope="col">Search job ID</th><th scope="col">Sequence</th></tr></thead><tbody>{traversal.items.map((event: SearchAttemptAuditEvent) => <tr key={event.sequence.toString()}><td data-label="Occurred"><time dateTime={event.occurredAt?.toISOString()}>{formatActivityDate(event.occurredAt ?? null)}</time></td><td data-label="Actor"><strong>{event.actorId}</strong><small>{auditActorKindLabel(event.actorKind)} · {auditActorRoleLabel(event.actorRole)}</small></td><td data-label="Owner"><strong>{event.ownerId}</strong></td><td data-label="Search job ID"><code>{event.searchJobId}</code></td><td data-label="Sequence" className="numeric-data">{formatActivityCount(event.sequence)}</td></tr>)}</tbody></table></div>
            )}
          </section>
        </>
      ) : null}
    </div>
  );
}

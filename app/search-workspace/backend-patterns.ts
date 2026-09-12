import type {
  ListSearchPatternMembersRequest,
  ListSearchPatternMembersResponse,
  ListSearchPatternsRequest,
  ListSearchPatternsResponse,
} from "@/gen/ts/open_splunk/patterns_api";
import { PatternSensitivity as WireSensitivity } from "@/gen/ts/open_splunk/patterns_api";
import type { ResultPage } from "@/gen/ts/open_splunk/result";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import type { PatternSensitivity } from "./model";

const MAXIMUM_PATTERN_ROWS = 100_000;

export interface PatternRow {
  patternId?: string;
  signature: string;
  count: number;
  percent: number;
}

export interface PatternCoverage {
  retainedRows: number;
  eligibleRows: number;
  excludedRows: number;
  totalGroups: number;
  retainedTruncated: boolean;
  snapshotComplete: boolean;
  algorithmVersion: string;
}

export interface PatternContext {
  searchJobId: string;
  snapshotRef: string;
  sensitivity: PatternSensitivity;
}

export interface PatternClient {
  patterns: (request: ListSearchPatternsRequest, options?: ProtobufRequestOptions) => Promise<ListSearchPatternsResponse>;
  patternMembers: (request: ListSearchPatternMembersRequest, options?: ProtobufRequestOptions) => Promise<ListSearchPatternMembersResponse>;
}

export interface PatternMemberView {
  pattern: PatternRow;
  page: ResultPage | null;
  pageNumber: number;
  pageSize: number;
  pageStart: number;
  hasNextPage: boolean;
  loading: boolean;
  error: string | null;
}

export interface BackendPatternsState {
  rows: PatternRow[];
  coverage: PatternCoverage | null;
  pageNumber: number;
  pageSize: number;
  hasNextPage: boolean;
  loading: boolean;
  error: string | null;
  members: PatternMemberView | null;
}

export function patternWireSensitivity(sensitivity: PatternSensitivity): WireSensitivity {
  switch (sensitivity) {
    case "Precise": return WireSensitivity.PATTERN_SENSITIVITY_PRECISE;
    case "Balanced": return WireSensitivity.PATTERN_SENSITIVITY_BALANCED;
    case "Broad": return WireSensitivity.PATTERN_SENSITIVITY_BROAD;
  }
}

function boundedCount(value: bigint | undefined, label: string): number {
  if (value === undefined || value < 0n || value > BigInt(MAXIMUM_PATTERN_ROWS)) {
    throw new Error(`Patterns returned an invalid ${label}.`);
  }
  return Number(value);
}

function validatePageSize(pageSize: number): void {
  if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > MAXIMUM_PATTERN_ROWS) {
    throw new Error("Patterns page size is invalid.");
  }
}

function nextToken(token: string | undefined, tokens: Map<number, string | undefined>, pageNumber: number): string | undefined {
  const normalized = token === "" ? undefined : token;
  if (tokens.has(pageNumber + 1) && tokens.get(pageNumber + 1) !== normalized) {
    throw new Error("Patterns changed an immutable page cursor.");
  }
  if (normalized === undefined) return undefined;
  for (const [page, previous] of tokens) {
    if (previous === token && page !== pageNumber + 1) throw new Error("Patterns returned a repeated page cursor.");
  }
  return token;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : "Patterns could not be loaded. Try again.";
}

/** Owns only one immutable retained relation; changing it creates a fresh controller. */
export class BackendPatterns {
  private state: BackendPatternsState;
  private readonly listeners = new Set<() => void>();
  private readonly groupTokens = new Map<number, string | undefined>([[1, undefined]]);
  private memberTokens = new Map<number, string | undefined>([[1, undefined]]);
  private memberStarts = new Map<number, number>([[1, 1]]);
  private memberLastOrdinals = new Map<number, bigint>();
  private groupRequest: AbortController | null = null;
  private memberRequest: AbortController | null = null;
  private disposed = false;

  constructor(readonly context: PatternContext, private readonly client: PatternClient, pageSize = 20) {
    validatePageSize(pageSize);
    this.state = { rows: [], coverage: null, pageNumber: 1, pageSize, hasNextPage: false, loading: false, error: null, members: null };
  }

  getSnapshot = (): BackendPatternsState => this.state;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  private update(patch: Partial<BackendPatternsState>): void {
    if (this.disposed) return;
    this.state = { ...this.state, ...patch };
    for (const listener of this.listeners) listener();
  }

  cancelPending(): void {
    this.groupRequest?.abort();
    this.memberRequest?.abort();
    this.update({ loading: false, members: this.state.members ? { ...this.state.members, loading: false } : null });
  }

  dispose(): void {
    this.disposed = true;
    this.groupRequest?.abort();
    this.memberRequest?.abort();
    this.listeners.clear();
  }

  async loadGroups(pageNumber = this.state.pageNumber): Promise<void> {
    if (this.disposed || !this.groupTokens.has(pageNumber)) return;
    this.groupRequest?.abort();
    const request = new AbortController();
    this.groupRequest = request;
    this.update({ loading: true, error: null });
    try {
      const response = await this.client.patterns({
        searchJobId: this.context.searchJobId,
        snapshotRef: this.context.snapshotRef,
        sensitivity: patternWireSensitivity(this.context.sensitivity),
        page: { pageSize: this.state.pageSize, pageToken: this.groupTokens.get(pageNumber), includeTotalSize: true },
      }, { signal: request.signal });
      if (request.signal.aborted || this.disposed) return;
      if (response.snapshotRef !== this.context.snapshotRef || response.algorithmVersion !== "1") {
        throw new Error("Patterns returned a different result snapshot or algorithm.");
      }
      const coverage: PatternCoverage = {
        retainedRows: boundedCount(response.retainedEventCount, "retained count"),
        eligibleRows: boundedCount(response.eligibleEventCount, "eligible count"),
        excludedRows: boundedCount(response.excludedEventCount, "excluded count"),
        totalGroups: boundedCount(response.page?.totalSize, "group count"),
        retainedTruncated: response.retainedTruncated,
        snapshotComplete: response.snapshotComplete,
        algorithmVersion: response.algorithmVersion,
      };
      if (!response.page?.totalSizeExact || coverage.eligibleRows + coverage.excludedRows !== coverage.retainedRows
        || coverage.totalGroups > coverage.eligibleRows || response.patterns.length > this.state.pageSize
        || (coverage.totalGroups === 0) !== (coverage.eligibleRows === 0)
        || (this.state.coverage !== null && JSON.stringify(this.state.coverage) !== JSON.stringify(coverage))) {
        throw new Error("Patterns returned inconsistent retained counts.");
      }
      const ids = new Set<string>();
      let pageCount = 0;
      const rows = response.patterns.map((pattern) => {
        const count = boundedCount(pattern.eventCount, "event count");
        if (!pattern.patternId || ids.has(pattern.patternId) || count < 1) throw new Error("Patterns returned an invalid group.");
        ids.add(pattern.patternId);
        pageCount += count;
        return { patternId: pattern.patternId, signature: pattern.signature, count, percent: count / coverage.eligibleRows * 100 };
      });
      if (pageCount > coverage.eligibleRows) throw new Error("Patterns returned inconsistent group counts.");
      const next = nextToken(response.page.nextPageToken, this.groupTokens, pageNumber);
      const groupEnd = (pageNumber - 1) * this.state.pageSize + rows.length;
      if (groupEnd > coverage.totalGroups || (next !== undefined ? rows.length !== this.state.pageSize || groupEnd >= coverage.totalGroups : groupEnd !== coverage.totalGroups)) {
        throw new Error("Patterns returned an inconsistent page extent.");
      }
      if (next) this.groupTokens.set(pageNumber + 1, next);
      this.update({ rows, coverage, pageNumber, hasNextPage: next !== undefined, loading: false });
    } catch (error) {
      if (!request.signal.aborted && !this.disposed) this.update({ loading: false, error: message(error) });
    }
  }

  async selectPattern(pattern: PatternRow, pageSize = this.state.pageSize): Promise<void> {
    validatePageSize(pageSize);
    if (!pattern.patternId || !this.state.rows.some((row) => row.patternId === pattern.patternId)) {
      throw new Error("Select a pattern from this retained result snapshot.");
    }
    this.memberRequest?.abort();
    this.memberTokens = new Map([[1, undefined]]);
    this.memberStarts = new Map([[1, 1]]);
    this.memberLastOrdinals.clear();
    this.update({ members: { pattern, page: null, pageNumber: 1, pageSize, pageStart: 1, hasNextPage: false, loading: false, error: null } });
    await this.loadMembers(1);
  }

  clearPattern(): void {
    this.memberRequest?.abort();
    this.memberTokens.clear();
    this.memberStarts.clear();
    this.memberLastOrdinals.clear();
    this.update({ members: null });
  }

  async loadMembers(pageNumber = this.state.members?.pageNumber ?? 1): Promise<void> {
    const members = this.state.members;
    if (this.disposed || !members || !this.memberTokens.has(pageNumber)) return;
    this.memberRequest?.abort();
    const request = new AbortController();
    this.memberRequest = request;
    this.update({ members: { ...members, loading: true, error: null } });
    try {
      const response = await this.client.patternMembers({
        searchJobId: this.context.searchJobId,
        snapshotRef: this.context.snapshotRef,
        sensitivity: patternWireSensitivity(this.context.sensitivity),
        patternId: members.pattern.patternId!,
        columns: [],
        page: { pageSize: members.pageSize, pageToken: this.memberTokens.get(pageNumber), includeTotalSize: true },
      }, { signal: request.signal });
      if (request.signal.aborted || this.disposed) return;
      const page = response.resultPage;
      if (response.patternId !== members.pattern.patternId || !page?.schema || page.snapshotRef !== this.context.snapshotRef
        || !page.page?.totalSizeExact || boundedCount(page.page.totalSize, "member count") !== members.pattern.count
        || page.rows.length > members.pageSize) throw new Error("Pattern events returned inconsistent snapshot metadata.");
      let previousOrdinal = this.memberLastOrdinals.get(pageNumber - 1) ?? -1n;
      const rowIds = new Set<string>();
      for (const row of page.rows) {
        if (!row.rowId || rowIds.has(row.rowId) || row.ordinal <= previousOrdinal || row.cells.length !== page.schema.columns.length) {
          throw new Error("Pattern events returned an invalid retained row.");
        }
        previousOrdinal = row.ordinal;
        rowIds.add(row.rowId);
      }
      const next = nextToken(page.page.nextPageToken, this.memberTokens, pageNumber);
      const pageStart = this.memberStarts.get(pageNumber)!;
      const memberEnd = pageStart + page.rows.length - 1;
      if (page.rows.length === 0 || memberEnd > members.pattern.count || (next !== undefined ? memberEnd >= members.pattern.count : memberEnd !== members.pattern.count)) {
        throw new Error("Pattern events returned an inconsistent page extent.");
      }
      this.memberLastOrdinals.set(pageNumber, previousOrdinal);
      if (next) {
        this.memberTokens.set(pageNumber + 1, next);
        this.memberStarts.set(pageNumber + 1, pageStart + page.rows.length);
      }
      this.update({ members: { ...members, page, pageNumber, pageStart, hasNextPage: next !== undefined, loading: false, error: null } });
    } catch (error) {
      if (!request.signal.aborted && !this.disposed) this.update({ members: { ...members, loading: false, error: message(error) } });
    }
  }
}

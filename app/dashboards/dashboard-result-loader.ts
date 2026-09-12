import {
  ResultSchema as ResultSchemaCodec,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/result";
import type { SearchJob } from "@/gen/ts/open_splunk/search";
import {
  assertBrowserResultPageBounds,
  recordNextPageToken,
} from "@/lib/api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";

export const DASHBOARD_TABLE_PAGE_SIZE = 20;
export const DASHBOARD_CHART_PAGE_SIZE = 1_000;
export const DASHBOARD_CHART_MAXIMUM_ROWS = 10_000;
const MAXIMUM_DASHBOARD_PIPELINES = 4;

export interface DashboardResultPage {
  nextPageToken?: string;
  pageNumber: number;
  rowStart: number;
  rows: ResultRow[];
  schema: ResultSchema;
  snapshotComplete: boolean;
  snapshotRef?: string;
}

export interface DashboardChartResult {
  capped: boolean;
  complete: boolean;
  rows: ResultRow[];
  schema: ResultSchema;
  snapshotComplete: boolean;
  snapshotRef?: string;
}

interface DashboardResultLoaderOptions {
  client: OpenSplunkApiClient;
  isCurrent: () => boolean;
  job: SearchJob;
  signal: AbortSignal;
}

interface RawDashboardResultPage {
  nextPageToken?: string;
  rows: ResultRow[];
  schema: ResultSchema;
  snapshotComplete: boolean;
  snapshotRef?: string;
}

function schemasEqual(left: ResultSchema, right: ResultSchema): boolean {
  const leftBytes = ResultSchemaCodec.encode(left).finish();
  const rightBytes = ResultSchemaCodec.encode(right).finish();
  return leftBytes.length === rightBytes.length
    && leftBytes.every((value, index) => value === rightBytes[index]);
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("The dashboard result request was aborted.", "AbortError");
}

function throwIfStale(signal: AbortSignal, isCurrent: () => boolean): void {
  if (signal.aborted) throw abortReason(signal);
  if (!isCurrent()) throw new DOMException("The dashboard panel run was superseded.", "AbortError");
}

export class DashboardResultLoader {
  private authoritativeSchema: ResultSchema | undefined;
  private readonly client: OpenSplunkApiClient;
  private readonly isCurrent: () => boolean;
  private readonly job: SearchJob;
  private readonly pages: DashboardResultPage[] = [];
  private activePageIndex = -1;
  private readonly seenOrdinals = new Set<bigint>();
  private readonly seenRowIDs = new Set<string>();
  private readonly seenCursors = new Set<string>();
  private readonly signal: AbortSignal;
  private snapshotIdentity: { kind: "legacy" } | { kind: "retained"; value: string } | undefined;

  public constructor(options: DashboardResultLoaderOptions) {
    this.authoritativeSchema = options.job.resultSchema;
    this.client = options.client;
    this.isCurrent = options.isCurrent;
    this.job = options.job;
    this.signal = options.signal;
  }

  public currentTablePage(): DashboardResultPage | undefined {
    return this.pages[this.activePageIndex];
  }

  public previousTablePage(): DashboardResultPage | undefined {
    if (this.activePageIndex <= 0) return undefined;
    this.activePageIndex -= 1;
    return this.pages[this.activePageIndex];
  }

  public async firstTablePage(): Promise<DashboardResultPage> {
    if (this.pages.length > 0) {
      this.activePageIndex = 0;
      return this.pages[0];
    }
    const page = await this.request(undefined, DASHBOARD_TABLE_PAGE_SIZE);
    const result = { ...page, pageNumber: 1, rowStart: 1 };
    this.pages.push(result);
    this.activePageIndex = 0;
    return result;
  }

  public async nextTablePage(): Promise<DashboardResultPage> {
    const current = this.pages[this.activePageIndex];
    if (current === undefined) return this.firstTablePage();
    const cached = this.pages[this.activePageIndex + 1];
    if (cached !== undefined) {
      this.activePageIndex += 1;
      return cached;
    }
    if (current.nextPageToken === undefined) throw new Error("The dashboard result has no next page.");
    const page = await this.request(current.nextPageToken, DASHBOARD_TABLE_PAGE_SIZE);
    const result = {
      ...page,
      pageNumber: current.pageNumber + 1,
      rowStart: current.rowStart + current.rows.length,
    };
    this.pages.push(result);
    this.activePageIndex += 1;
    return result;
  }

  public async collectChartRows(): Promise<DashboardChartResult> {
    const rows: ResultRow[] = [];
    let pageToken: string | undefined;
    let lastPage: RawDashboardResultPage | undefined;
    let allPagesSnapshotComplete = true;
    const collect = async (): Promise<void> => {
      const pageSize = Math.min(DASHBOARD_CHART_PAGE_SIZE, DASHBOARD_CHART_MAXIMUM_ROWS - rows.length);
      if (pageSize <= 0) return;
      lastPage = await this.request(pageToken, pageSize);
      rows.push(...lastPage.rows);
      allPagesSnapshotComplete &&= lastPage.snapshotComplete;
      pageToken = lastPage.nextPageToken;
      if (pageToken !== undefined && rows.length < DASHBOARD_CHART_MAXIMUM_ROWS) await collect();
    };
    await collect();
    if (lastPage === undefined || this.authoritativeSchema === undefined) {
      throw new Error("The dashboard search completed without result rows or a schema.");
    }
    return {
      capped: pageToken !== undefined,
      complete: pageToken === undefined && allPagesSnapshotComplete,
      rows,
      schema: this.authoritativeSchema,
      snapshotComplete: allPagesSnapshotComplete,
      snapshotRef: this.snapshotIdentity?.kind === "retained" ? this.snapshotIdentity.value : undefined,
    };
  }

  private async request(pageToken: string | undefined, pageSize: number): Promise<RawDashboardResultPage> {
    throwIfStale(this.signal, this.isCurrent);
    const response = await awaitDashboardOperation(this.client.search.results({
      searchJobId: this.job.searchJobId,
      page: { pageSize, pageToken, includeTotalSize: pageToken === undefined },
      columns: [],
      allowPartialResults: false,
    }, { signal: this.signal }), this.signal);
    throwIfStale(this.signal, this.isCurrent);
    if (response.searchJobId !== this.job.searchJobId) {
      throw new Error("The dashboard results response belongs to a different search job.");
    }
    const resultPage = response.resultPage;
    if (resultPage === undefined) throw new Error("The dashboard search completed without a result page.");
    const schema = resultPage.schema ?? this.authoritativeSchema;
    if (schema === undefined) throw new Error("The dashboard search completed without a result schema.");
    if (schema.schemaId.trim().length === 0 || schema.revision <= 0n) {
      throw new Error("The dashboard result page returned an invalid schema identity or revision.");
    }
    assertBrowserResultPageBounds({ columnCount: schema.columns.length, pageSize, rowCount: resultPage.rows.length });
    if (resultPage.rows.some((row) => row.cells.length !== schema.columns.length)) {
      throw new Error("A dashboard result row does not match the result schema column count.");
    }
    for (const row of resultPage.rows) {
      if (row.rowId.trim().length === 0) throw new Error("A dashboard result row has an empty identity.");
      if (this.seenRowIDs.has(row.rowId) || this.seenOrdinals.has(row.ordinal)) {
        throw new Error("Dashboard result pages repeated a row identity or ordinal.");
      }
      this.seenRowIDs.add(row.rowId);
      this.seenOrdinals.add(row.ordinal);
    }
    if (this.authoritativeSchema !== undefined) {
      if (
        schema.schemaId !== this.authoritativeSchema.schemaId
        || schema.revision !== this.authoritativeSchema.revision
      ) {
        throw new Error("The dashboard result schema changed while paging through one retained snapshot.");
      }
      if (!schemasEqual(schema, this.authoritativeSchema)) {
        throw new Error("The dashboard result schema mutated without changing its identity or revision.");
      }
    }
    this.authoritativeSchema = schema;
    this.recordSnapshotIdentity(resultPage.snapshotRef);
    const nextPageToken = recordNextPageToken(
      this.seenCursors,
      resultPage.page?.nextPageToken,
      "Dashboard results",
    ) ?? undefined;
    if (nextPageToken !== undefined && resultPage.rows.length === 0) {
      throw new Error("Dashboard results returned a continuation cursor without making row progress.");
    }
    return {
      nextPageToken,
      rows: resultPage.rows,
      schema,
      snapshotComplete: resultPage.snapshotComplete,
      snapshotRef: this.snapshotIdentity?.kind === "retained" ? this.snapshotIdentity.value : undefined,
    };
  }

  private recordSnapshotIdentity(rawSnapshotRef: string): void {
    const snapshotRef = rawSnapshotRef.trim();
    if (this.snapshotIdentity === undefined) {
      this.snapshotIdentity = snapshotRef.length === 0
        ? { kind: "legacy" }
        : { kind: "retained", value: snapshotRef };
      return;
    }
    if (this.snapshotIdentity.kind === "legacy" && snapshotRef.length !== 0) {
      throw new Error("The dashboard result snapshot identity appeared partway through paging.");
    }
    if (this.snapshotIdentity.kind === "retained" && snapshotRef !== this.snapshotIdentity.value) {
      throw new Error("The dashboard result snapshot identity changed while paging.");
    }
  }
}

export function awaitDashboardOperation<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(abortReason(signal));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(abortReason(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

interface PipelineWaiter {
  reject: (reason: unknown) => void;
  resolve: (release: () => void) => void;
  signal: AbortSignal;
}

class DashboardPipelineSlots {
  private active = 0;
  private readonly maximum: number;
  private readonly waiters: PipelineWaiter[] = [];

  public constructor(maximum: number) {
    this.maximum = maximum;
  }

  public acquire(signal: AbortSignal): Promise<() => void> {
    if (signal.aborted) return Promise.reject(abortReason(signal));
    return new Promise((resolve, reject) => {
      const waiter = { reject, resolve, signal };
      const onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(abortReason(signal));
      };
      signal.addEventListener("abort", onAbort, { once: true });
      waiter.resolve = (release) => {
        signal.removeEventListener("abort", onAbort);
        resolve(release);
      };
      this.waiters.push(waiter);
      this.dispatch();
    });
  }

  private dispatch(): void {
    while (this.active < this.maximum && this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      if (waiter.signal.aborted) {
        waiter.reject(abortReason(waiter.signal));
        continue;
      }
      this.active += 1;
      let released = false;
      waiter.resolve(() => {
        if (released) return;
        released = true;
        this.active -= 1;
        this.dispatch();
      });
    }
  }
}

const dashboardPipelineSlots = new DashboardPipelineSlots(MAXIMUM_DASHBOARD_PIPELINES);

export function acquireDashboardPanelPipeline(signal: AbortSignal): Promise<() => void> {
  return dashboardPipelineSlots.acquire(signal);
}

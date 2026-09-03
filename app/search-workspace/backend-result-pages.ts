import {
  ResultSchema as ResultSchemaCodec,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/result";
import type { SearchJob } from "@/gen/ts/open_splunk/search";
import {
  assertBrowserResultPageBounds,
  pruneCursorChainFrom,
  recordNextPageToken,
} from "@/lib/api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";

const MAX_CACHED_RESULT_PAGES = 8;

export interface BackendResultPage {
  schema: ResultSchema;
  rows: ResultRow[];
  nextPageToken: string | undefined;
  totalSize: number | undefined;
  totalSizeExact: boolean;
  snapshotComplete: boolean;
}

interface BackendResultPageResponse {
  schema: ResultSchema;
  rows: ResultRow[];
  rawNextPageToken: string | null;
  totalSize: number | undefined;
  totalSizeExact: boolean;
  snapshotComplete: boolean;
}

interface ResultPageRequest {
  client: OpenSplunkApiClient;
  job: SearchJob;
  pageSize: number;
  pageToken: string | undefined;
  includeTotalSize: boolean;
  signal: AbortSignal;
  isCurrent: () => boolean;
}

interface ResultPageFetch extends Omit<ResultPageRequest, "pageToken" | "includeTotalSize"> {
  pageNumber: number;
  apply: boolean;
  onApply: (page: BackendResultPage) => void;
  onNotice: (message: string) => void;
}

function equalResultSchemas(left: ResultSchema, right: ResultSchema): boolean {
  const leftBytes = ResultSchemaCodec.encode(left).finish();
  const rightBytes = ResultSchemaCodec.encode(right).finish();
  return leftBytes.length === rightBytes.length
    && leftBytes.every((value, index) => value === rightBytes[index]);
}

export class BackendResultPages {
  private authoritativeSchema: ResultSchema | null = null;
  private displayedKey: string | null = null;
  private readonly pageStarts = new Map<string, number>();
  private readonly pageTokens = new Map<string, string | undefined>();
  private readonly pages = new Map<string, BackendResultPage>();
  private readonly tokensSeen = new Set<string>();

  pageSize = 20;

  resetForJob(pageSize: number) {
    this.authoritativeSchema = null;
    this.resetPages(pageSize);
  }

  resetForPageSize(pageSize: number) {
    this.resetPages(pageSize);
  }

  private resetPages(pageSize: number) {
    this.displayedKey = null;
    this.pageSize = pageSize;
    this.pageStarts.clear();
    this.pageTokens.clear();
    this.pages.clear();
    this.tokensSeen.clear();
    this.pageTokens.set(`${pageSize}:1`, undefined);
    this.pageStarts.set(`${pageSize}:1`, 1);
  }

  pageStart(pageSize: number, pageNumber: number): number | null {
    const key = `${pageSize}:${pageNumber}`;
    return this.pages.has(key) ? this.pageStarts.get(key) ?? null : null;
  }

  prepareFirstPage(pageSize: number) {
    this.pageSize = pageSize;
    this.pageTokens.set(`${pageSize}:1`, undefined);
    this.pageStarts.set(`${pageSize}:1`, 1);
  }

  pageTokenKeys(): IterableIterator<string> {
    return this.pageTokens.keys();
  }

  canOpen(pageSize: number, pageNumber: number): boolean {
    const key = `${pageSize}:${pageNumber}`;
    return this.pageTokens.has(key) || this.pages.has(key);
  }

  display(pageSize: number, pageNumber: number, page: BackendResultPage) {
    this.displayedKey = `${pageSize}:${pageNumber}`;
    return page;
  }

  async request({
    client,
    job,
    pageSize,
    pageToken,
    includeTotalSize,
    signal,
    isCurrent,
  }: ResultPageRequest): Promise<BackendResultPageResponse> {
    const response = await client.search.results({
      searchJobId: job.searchJobId,
      page: {
        pageSize,
        pageToken,
        includeTotalSize,
      },
      columns: [],
      allowPartialResults: false,
    }, { signal });
    if (!isCurrent()) throw new DOMException("Search was superseded.", "AbortError");
    if (response.searchJobId !== job.searchJobId) {
      throw new Error("The search results response belongs to a different search job.");
    }
    const resultPage = response.resultPage;
    if (resultPage === undefined) throw new Error("The search completed without a result page.");
    const schema = resultPage.schema ?? job.resultSchema;
    if (schema === undefined) throw new Error("The search completed without a result schema.");
    if (schema.schemaId.trim().length === 0 || schema.revision <= 0n) {
      throw new Error("The search result page returned an invalid schema identity or revision.");
    }
    assertBrowserResultPageBounds({
      columnCount: schema.columns.length,
      pageSize,
      rowCount: resultPage.rows.length,
    });
    const expectedSchema = this.authoritativeSchema ?? job.resultSchema;
    if (expectedSchema !== undefined && expectedSchema !== null) {
      if (
        schema.schemaId !== expectedSchema.schemaId
        || schema.revision !== expectedSchema.revision
      ) {
        throw new Error("The search result schema changed while paging through one retained snapshot.");
      }
      if (!equalResultSchemas(schema, expectedSchema)) {
        throw new Error("The search result schema mutated without changing its identity or revision.");
      }
    }
    this.authoritativeSchema = schema;
    const totalSize = resultPage.page?.totalSize;
    return {
      schema,
      rows: resultPage.rows,
      rawNextPageToken: resultPage.page?.nextPageToken?.trim() || null,
      totalSize: totalSize === undefined ? undefined : Math.min(Number.MAX_SAFE_INTEGER, Number(totalSize)),
      totalSizeExact: (resultPage.page?.totalSizeExact ?? false)
        && (totalSize === undefined || totalSize <= BigInt(Number.MAX_SAFE_INTEGER)),
      snapshotComplete: resultPage.snapshotComplete,
    };
  }

  async fetch({
    client,
    job,
    pageNumber,
    pageSize,
    signal,
    isCurrent,
    apply,
    onApply,
    onNotice,
  }: ResultPageFetch): Promise<BackendResultPage> {
    if (!isCurrent()) throw new DOMException("Search was superseded.", "AbortError");
    const cacheKey = `${pageSize}:${pageNumber}`;
    const cached = this.pages.get(cacheKey);
    if (cached !== undefined) {
      this.pages.delete(cacheKey);
      this.pages.set(cacheKey, cached);
      if (apply) {
        this.displayedKey = cacheKey;
        onApply(cached);
      }
      return cached;
    }
    if (!this.pageTokens.has(cacheKey)) {
      throw new Error("That result page cannot be opened until the preceding cursor page has loaded.");
    }
    const response = await this.request({
      client,
      job,
      pageSize,
      pageToken: this.pageTokens.get(cacheKey),
      includeTotalSize: pageNumber === 1,
      signal,
      isCurrent,
    });
    const { schema, rawNextPageToken, totalSize } = response;
    let nextPageToken: string | undefined;
    const nextPageKey = `${pageSize}:${pageNumber + 1}`;
    const knownNextPageToken = this.pageTokens.get(nextPageKey);
    if (this.pageTokens.has(nextPageKey)) {
      if (rawNextPageToken === knownNextPageToken) {
        nextPageToken = knownNextPageToken;
      } else {
        pruneCursorChainFrom(
          this.pages,
          this.pageTokens,
          this.pageStarts,
          this.tokensSeen,
          pageSize,
          pageNumber + 1,
        );
        onNotice("The retained result cursor changed while revisiting a page. Further paging was stopped.");
      }
    } else {
      try {
        nextPageToken = recordNextPageToken(
          this.tokensSeen,
          rawNextPageToken,
          "Search results",
        ) ?? undefined;
      } catch (error) {
        pruneCursorChainFrom(
          this.pages,
          this.pageTokens,
          this.pageStarts,
          this.tokensSeen,
          pageSize,
          pageNumber + 1,
        );
        onNotice(`${error instanceof Error ? error.message : "Search results returned an invalid page cursor."} Further paging was stopped.`);
      }
    }
    const page: BackendResultPage = {
      schema,
      rows: response.rows,
      nextPageToken,
      totalSize,
      totalSizeExact: response.totalSizeExact,
      snapshotComplete: response.snapshotComplete,
    };
    this.pages.set(cacheKey, page);
    while (this.pages.size > MAX_CACHED_RESULT_PAGES) {
      const evictable = [...this.pages.keys()]
        .find((key) => key !== this.displayedKey && key !== cacheKey);
      if (evictable === undefined) break;
      this.pages.delete(evictable);
    }
    if (page.nextPageToken !== undefined) {
      this.pageTokens.set(`${pageSize}:${pageNumber + 1}`, page.nextPageToken);
      const currentStart = this.pageStarts.get(cacheKey) ?? 1;
      this.pageStarts.set(`${pageSize}:${pageNumber + 1}`, currentStart + page.rows.length);
    }
    if (apply) {
      this.displayedKey = cacheKey;
      onApply(page);
    }
    return page;
  }
}

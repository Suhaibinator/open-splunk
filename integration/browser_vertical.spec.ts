import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import {
  expect,
  test,
  type CDPSession,
  type Locator,
  type Page,
  type Response,
  type Route,
  type WebSocket,
  type WebSocketRoute,
} from "@playwright/test";

import {
  CancelSearchJobResponse,
  CreateSearchJobRequest,
  CreateSearchJobResponse,
  GetSearchJobResponse,
  GetSearchResultsRequest,
  GetSearchResultsResponse,
  ValidateSearchRequest,
  ValidateSearchResponse,
} from "../gen/ts/open_splunk/search_api";
import {
  AuditAction,
  AuditActorKind,
  AuditActorRole,
  AuditTargetKind,
} from "../gen/ts/open_splunk/audit";
import {
  ListAuditEventsRequest,
  ListAuditEventsResponse,
} from "../gen/ts/open_splunk/audit_api";
import { AppState } from "../gen/ts/open_splunk/app";
import { IndexAccessState, IndexState } from "../gen/ts/open_splunk/index";
import {
  DeleteSearchHistoryEntryRequest,
  DeleteSearchHistoryEntryResponse,
  ListSearchHistoryResponse,
} from "../gen/ts/open_splunk/history_api";
import {
  GetSystemBootstrapResponse,
  ServerFeature,
} from "../gen/ts/open_splunk/system_api";
import {
  DiagnosticSeverity,
  SharingScope,
  SortDirection,
} from "../gen/ts/open_splunk/common";
import {
  KnowledgeDependencyRole,
  KnowledgeObject,
  KnowledgeObjectDefinition,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSearchStage,
  KnowledgeSelectorMatchKind,
} from "../gen/ts/open_splunk/knowledge";
import {
  InspectSearchJobRequest,
  InspectSearchJobResponse,
  SearchInspectionOutputKind,
} from "../gen/ts/open_splunk/search_inspection_api";
import {
  CreateKnowledgeObjectRequest,
  CreateKnowledgeObjectResponse,
  DeleteKnowledgeObjectRequest,
  DeleteKnowledgeObjectResponse,
  GetKnowledgeObjectRequest,
  GetKnowledgeObjectResponse,
  KnowledgeValidationIntent,
  KnowledgeObjectSortBy,
  ListKnowledgeObjectDependenciesRequest,
  ListKnowledgeObjectDependenciesResponse,
  ListKnowledgeObjectDependentsRequest,
  ListKnowledgeObjectDependentsResponse,
  ListKnowledgeObjectsRequest,
  ListKnowledgeObjectsResponse,
  PreviewKnowledgeObjectRequest,
  PreviewKnowledgeObjectResponse,
  SetKnowledgeObjectStateRequest,
  SetKnowledgeObjectStateResponse,
  UpdateKnowledgeObjectRequest,
  UpdateKnowledgeObjectResponse,
  ValidateKnowledgeObjectRequest,
  ValidateKnowledgeObjectResponse,
} from "../gen/ts/open_splunk/knowledge_api";
import { ListIndexesResponse } from "../gen/ts/open_splunk/index_api";
import { ListIngestionTokensResponse } from "../gen/ts/open_splunk/collector_admin_api";
import {
  SearchExecutionPhase,
  SearchFailureCode,
  SearchJobOrigin,
  SearchJobState,
  type SearchProgress,
} from "../gen/ts/open_splunk/search";
import {
  ColumnSemanticType,
  ResultRow,
  ResultSchema,
  ResultSetKind,
} from "../gen/ts/open_splunk/result";
import { TypedValue, ValueType } from "../gen/ts/open_splunk/value";
import {
  ResynchronizationReason,
  SearchWebSocketCommand,
  SearchWebSocketEvent,
} from "../gen/ts/open_splunk/search_ws";
import { MAXIMUM_BROWSER_RESULT_COLUMNS } from "../lib/api/pagination";
import {
  BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX,
  BoundedObservationRegistry,
  MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
  MAXIMUM_OBSERVED_MATCHING_WEBSOCKETS,
  MAXIMUM_RECORDED_DIAGNOSTICS,
  boundedDiagnostic,
  boundedRecorder,
  type BoundedRecorder,
} from "./browser_harness";

const baseURL = requiredEnvironment("OPEN_SPLUNK_E2E_BASE_URL");
const searchSPL = requiredEnvironment("OPEN_SPLUNK_E2E_SPL");
const earliest = requiredEnvironment("OPEN_SPLUNK_E2E_EARLIEST");
const latest = requiredEnvironment("OPEN_SPLUNK_E2E_LATEST");
const expectedText = requiredEnvironment("OPEN_SPLUNK_E2E_EXPECTED_TEXT");
const expectedRows = parsePositiveInteger(requiredEnvironment("OPEN_SPLUNK_E2E_EXPECTED_ROWS"));
const browserExecutable = process.env.OPEN_SPLUNK_BROWSER_EXECUTABLE?.trim();
const ignoreHTTPSErrors = process.env.OPEN_SPLUNK_E2E_IGNORE_HTTPS_ERRORS === "1";
const recoveryControlURL = optionalLoopbackURL(process.env.OPEN_SPLUNK_E2E_RECOVERY_CONTROL_URL);
const recoveryControlToken = process.env.OPEN_SPLUNK_E2E_RECOVERY_CONTROL_TOKEN?.trim();
const recoveryInitialText = process.env.OPEN_SPLUNK_E2E_RECOVERY_INITIAL_TEXT?.trim();
const cancellationTest = process.env.OPEN_SPLUNK_E2E_CANCELLATION_TEST === "1";
const renderingTest = process.env.OPEN_SPLUNK_E2E_RENDERING_TEST === "1";
const renderingArtifactDirectory =
  process.env.OPEN_SPLUNK_E2E_RENDERING_ARTIFACT_DIRECTORY?.trim();
const renderingMetricsPath = process.env.OPEN_SPLUNK_E2E_RENDERING_METRICS_PATH?.trim();
const browserRenderingJobID = "browser-fixed-result-rendering";
const sequenceExpirationTest = process.env.OPEN_SPLUNK_E2E_SEQUENCE_EXPIRATION_TEST === "1";
const sequenceGapTest = process.env.OPEN_SPLUNK_E2E_SEQUENCE_GAP_TEST === "1";
const sequenceGapRESTTerminalTest =
  process.env.OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_TERMINAL_TEST === "1";
const sequenceGapRESTFirstProgressTest =
  process.env.OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_FIRST_PROGRESS_TEST === "1";
const origin = validatedOrigin(baseURL);
const timeout = 45_000;

function tierOneObjectType(definition: KnowledgeObjectDefinition): KnowledgeObjectType {
  switch (definition.body?.$case) {
    case "fieldExtraction": return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION;
    case "fieldAlias": return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS;
    case "calculatedField": return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD;
    default: return KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED;
  }
}

function tierOneDefinitionDigest(definition: KnowledgeObjectDefinition): Uint8Array {
  return createHash("sha256")
    .update(KnowledgeObjectDefinition.encode(definition).finish())
    .digest();
}
const maximumMaterializedRows = 32;
const maximumSpacerRows = 2;
const maximumTableBodyRows = maximumMaterializedRows + maximumSpacerRows;
const maximumRecordedBrowserFrameBytes = 16_384;
const maximumRecordedBrowserFrames = 64;
let browserRecorderSelfTestCompleted = false;

interface RequestGate {
  release: (() => void) | null;
  started: boolean;
  settled: Promise<void>;
  markSettled: () => void;
}

function createRequestGate(): RequestGate {
  let resolveSettled: (() => void) | null = null;
  const settled = new Promise<void>((resolve) => {
    resolveSettled = resolve;
  });
  return {
    release: null,
    started: false,
    settled,
    markSettled: () => resolveSettled?.(),
  };
}

test.use({
  launchOptions: browserExecutable ? { executablePath: browserExecutable } : {},
  ignoreHTTPSErrors,
  screenshot: "only-on-failure",
  trace: "retain-on-failure",
});

test("collector event is visible through the compiled backend UI", async ({ page }) => {
  test.setTimeout(60_000);
  const safety = observeBrowserSafety(page);
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);
  const protocolObservation = observeSearchProtocol(page, origin, timeout, true);
  try {
    await runSearch.click();

    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const browserSearchJobID = decodeCreateSearchJobID(await createResponse.body());
    const [resultsResponse] = await Promise.all([
      resultsResponsePromise,
      protocolObservation.waitForJob(browserSearchJobID),
    ]);
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
    protocolObservation.dispose();
  }

  const jobStrip = page.getByTestId("job-strip");
  await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(jobStrip).toContainText("Completed", { timeout });
  await expect(jobStrip).toContainText(`${expectedRows} events`, { timeout });
  await expect(page.getByTestId("timeline")).toHaveAttribute("aria-label", "Event timeline", { timeout });

  const eventList = page.getByTestId("event-list");
  const finalRows = eventList.locator('[data-testid^="event-row-"]:not(.event-row--preview)');
  await expect(finalRows).toHaveCount(expectedRows, { timeout });
  // Final backend results intentionally expand the first event so typed fields
  // are immediately visible; wait for that committed state instead of toggling
  // a row that may already be expanded.
  await expect(finalRows.first()).toHaveClass(/\bexpanded\b/u, { timeout });
  await expect(eventList).toContainText(expectedText, { timeout });
  await expect(eventList.locator(".event-row--preview")).toHaveCount(0);

  const previewStatuses = await collectPreviewStatuses(page);
  expect(
    previewStatuses.some((status) => status === "live" || status === "finalizing"),
    `UI preview status transitions: ${JSON.stringify(previewStatuses)}`,
  ).toBe(true);
  expect(
    previewStatuses.filter((status) =>
      status === "paused" || status === "resyncing" || status === "finalization-error"),
    `UI preview status transitions: ${JSON.stringify(previewStatuses)}`,
  ).toEqual([]);
  await expect(page.locator("body")).not.toContainText(
    /Live job updates failed|Live job updates skipped a sequence|resynchronizing from the server/i,
  );
  assertBrowserSafety(safety);
});

test("backend diagnostics remain authoritative and prevent browser dispatch", async ({
  page,
}) => {
  const protobufHeaders = { "content-type": "application/x-protobuf" };
  const indexMatch = /\bindex=(?:"([^"]+)"|([^\s|]+))/u.exec(searchSPL);
  const indexName = indexMatch?.[1] ?? indexMatch?.[2] ?? "main";
  const source = `index=${JSON.stringify(indexName)} message="🟢" | eval rejected=status IN (500, 503)`;
  const membershipOffset = source.indexOf("IN (500");
  if (membershipOffset < 0) throw new Error("diagnostic membership operator is missing");
  const startByteOffset = BigInt(Buffer.byteLength(source.slice(0, membershipOffset), "utf8"));
  const startColumn = Array.from(source.slice(0, membershipOffset)).length + 1;
  const diagnosticMessage =
    "Membership expressions are Boolean and cannot be assigned directly by eval.";
  const validated: ValidateSearchRequest[] = [];
  const safety = observeBrowserSafety(page);

  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/validate",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("diagnostic Validate omitted its protobuf body");
      const request = ValidateSearchRequest.decode(wire);
      if (request.definition?.spl !== source) {
        throw new Error("diagnostic Validate did not preserve the authored source");
      }
      validated.push(request);
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ValidateSearchResponse.encode(
          ValidateSearchResponse.fromPartial({
            valid: false,
            diagnostics: [{
              code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
              severity: DiagnosticSeverity.DIAGNOSTIC_SEVERITY_ERROR,
              message: diagnosticMessage,
              sourceRange: {
                start: { byteOffset: startByteOffset, line: 1, column: startColumn },
                end: { byteOffset: startByteOffset + 2n, line: 1, column: startColumn + 2 },
              },
              suggestions: ["Use membership inside where, if, or case."],
            }],
          }),
        ).finish()),
      });
    },
  );

  await openSearchWorkspace(page);
  await page.getByTestId("search-input").fill(source);
  await page.getByTestId("run-search").click();

  await expect(page.getByTestId("toast")).toContainText(diagnosticMessage, { timeout });
  expect(validated).toHaveLength(1);
  expect(safety.createRequests()).toBe(0);
  expect(safety.resultsRequests()).toBe(0);
  assertBrowserSafety(safety);
});

test("Search Job Inspector renders only capability-gated redacted Knowledge authority", async ({
  page,
}) => {
  test.setTimeout(90_000);
  const safety = observeBrowserSafety(page);
  const protobufHeaders = { "content-type": "application/x-protobuf" };
  const selectedAppId = "inspection-app";
  const indexMatch = /\bindex=(?:"([^"]+)"|([^\s|]+))/u.exec(searchSPL);
  const indexName = indexMatch?.[1] ?? indexMatch?.[2] ?? "main";
  const maliciousField =
    "generated-<img src=x onerror=globalThis.__inspectionExecuted=true>";
  const longGeneratedSQL = `SELECT ${"x".repeat(4_096)}`;
  const capabilityOffSecret = "SECRET_CAPABILITY_OFF_KNOWLEDGE_IDENTITY";
  const foreignSecret = "SECRET_FOREIGN_KNOWLEDGE_IDENTITY";
  const staleInspectionMarker = "STALE_ABORTED_INSPECTION_QUERY";
  const resultSchema = {
    schemaId: "inspection-statistics-v1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [{
      fieldName: "message",
      displayName: "Message",
      valueType: ValueType.VALUE_TYPE_STRING,
      semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_DIMENSION,
      nullable: false,
      multivalue: false,
      hiddenByDefault: false,
    }],
  };
  const inspectRequests: InspectSearchJobRequest[] = [];
  const resultRequests: GetSearchResultsRequest[] = [];
  const apiTraffic: Array<{ method: string; pathname: string }> = [];
  const heldInspection = createRequestGate();
  let knowledgeAdvertised = false;
  let serveForeignIdentity = true;
  let holdNextInspection = false;
  let createdJobs = 0;

  const expectCompletedResultRequest = async (
    expectedCount: number,
    searchJobId: string,
  ): Promise<void> => {
    await expect.poll(() => resultRequests.length, {
      message: `completed job ${searchJobId} loaded one authoritative result snapshot`,
      timeout,
    }).toBe(expectedCount);
    expect(resultRequests[expectedCount - 1]).toEqual({
      searchJobId,
      page: { pageSize: 20, pageToken: undefined, includeTotalSize: true },
      columns: [],
      allowPartialResults: false,
    });
    await waitForBrowserRender(page);
    expect(resultRequests).toHaveLength(expectedCount);
  };

  const inspectionResponse = (responseJobId: string): InspectSearchJobResponse => {
    const provenance = {
      source: {
        $case: "redactedObject" as const,
        value: {
          redactedObjectOrdinal: 0,
          objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
          stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
        },
      },
    };
    return InspectSearchJobResponse.fromPartial({
      searchJobId: responseJobId,
      logicalPlan: {
        stages: [{
          stageIndex: 0,
          operator: "Scan",
          inputFields: ["message"],
          outputFields: ["message"],
          sourceRange: {
            start: { byteOffset: 0n, line: 1, column: 1 },
            end: { byteOffset: 1n, line: 1, column: 2 },
          },
          operatorProvenance: [],
          outputProvenance: [],
        }, {
          stageIndex: 1,
          operator: "ParallelExtend",
          inputFields: ["message"],
          outputFields: [maliciousField],
          operatorProvenance: [provenance],
          outputProvenance: [{ outputField: maliciousField, provenance }],
        }],
        referencedFields: ["message"],
        output: {
          kind: SearchInspectionOutputKind.SEARCH_INSPECTION_OUTPUT_KIND_STATIC,
          fields: ["message"],
        },
      },
      physicalPlan: {
        nodeTypes: ["ReadFromMergeTree"],
        reads: [],
      },
      generatedSql: longGeneratedSQL,
      explainText: "ReadFromMergeTree",
      diagnosticQueryId: "inspection-diagnostic-query",
      knowledgeSnapshot: {
        ref: {
          snapshotSha256: new Uint8Array(32).fill(0x42),
          tenantCatalogRevision: 7n,
          tenantCatalogStateToken: new Uint8Array(32).fill(0x24),
          objectCount: 1,
        },
        objects: [{
          resolutionOrdinal: 0,
          objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
          stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
          disclosure: { $case: "redacted", value: true },
        }],
        objectsTruncated: false,
      },
    });
  };

  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.origin === origin && url.pathname.startsWith("/api/")) {
      apiTraffic.push({ method: request.method(), pathname: url.pathname });
    }
  });
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/system/bootstrap",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(
        GetSystemBootstrapResponse.fromPartial({
          searchWebsocketPath: "/api/search/ws",
          features: [
            ServerFeature.SERVER_FEATURE_SEARCH,
            ServerFeature.SERVER_FEATURE_PLAN_INSPECTION,
            ...(knowledgeAdvertised
              ? [ServerFeature.SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS]
              : []),
          ],
          limits: { maximumPageSize: 20 },
          apps: [{
            appId: selectedAppId,
            slug: "inspection-app",
            displayName: "Inspection app",
            defaultIndexNames: [indexName],
            state: AppState.APP_STATE_ACTIVE,
          }],
          indexes: [{
            indexId: "inspection-index-id",
            name: indexName,
            displayName: "Inspection index",
            state: IndexState.INDEX_STATE_ACTIVE,
            ingestionAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
            searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
          }],
          selectedAppId,
          serverTime: new Date("2026-08-10T12:00:00.000Z"),
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/validate",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("inspection search Validate omitted its protobuf body");
      const request = ValidateSearchRequest.decode(wire);
      if (request.definition === undefined) {
        throw new Error("inspection search Validate omitted its definition");
      }
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ValidateSearchResponse.encode(
          ValidateSearchResponse.fromPartial({
            valid: true,
            normalizedSpl: request.definition.spl,
            referencedIndexes: [indexName],
            referencedFields: ["message"],
            predictedResultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/create",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("inspection search Create omitted its protobuf body");
      const request = CreateSearchJobRequest.decode(wire);
      if (request.definition === undefined) {
        throw new Error("inspection search Create omitted its definition");
      }
      createdJobs += 1;
      const searchJobId = `inspection-job-${createdJobs}`;
      const completedAt = new Date(`2026-08-10T12:00:0${createdJobs}.000Z`);
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(CreateSearchJobResponse.encode(
          CreateSearchJobResponse.fromPartial({
            searchJob: {
              searchJobId,
              stateVersion: 1n,
              definition: request.definition,
              source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
              effectiveIndexScope: [indexName],
              resolvedTimeRange: {
                earliest: new Date("2026-08-10T11:00:00.000Z"),
                latest: completedAt,
                timezone: "UTC",
              },
              state: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
              resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
              resultSchema,
              progress: {
                phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE,
                percentComplete: 100,
                elapsed: { seconds: 0n, nanos: 1_000_000 },
                queueWait: { seconds: 0n, nanos: 0 },
                updatedAt: completedAt,
                stateVersion: 1n,
              },
              createdAt: completedAt,
              startedAt: completedAt,
              finishedAt: completedAt,
            },
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/results",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("inspection search Results omitted its protobuf body");
      const request = GetSearchResultsRequest.decode(wire);
      resultRequests.push(request);
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(GetSearchResultsResponse.encode(
          GetSearchResultsResponse.fromPartial({
            searchJobId: request.searchJobId,
            resultPage: {
              schema: resultSchema,
              rows: [],
              page: {
                totalSize: 0n,
                totalSizeExact: true,
              },
              snapshotComplete: true,
            },
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/inspect",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("inspection request omitted its protobuf body");
      const request = InspectSearchJobRequest.decode(wire);
      inspectRequests.push(request);
      if (holdNextInspection) {
        holdNextInspection = false;
        heldInspection.started = true;
        await new Promise<void>((resolve) => {
          heldInspection.release = resolve;
        });
        heldInspection.release = null;
        const staleResponse = inspectionResponse(request.searchJobId);
        staleResponse.diagnosticQueryId = staleInspectionMarker;
        try {
          await route.fulfill({
            status: 200,
            headers: protobufHeaders,
            body: Buffer.from(InspectSearchJobResponse.encode(staleResponse).finish()),
          });
        } catch {
          // The superseding inspection intentionally aborts only this held request.
        } finally {
          heldInspection.markSettled();
        }
        return;
      }
      const response = inspectionResponse(
        knowledgeAdvertised && serveForeignIdentity
          ? `foreign-${foreignSecret}`
          : request.searchJobId,
      );
      if (!knowledgeAdvertised) {
        const authorizedProvenance = {
          source: {
            $case: "authorizedObject" as const,
            value: {
              knowledgeObjectId: capabilityOffSecret,
              knowledgeObjectVersion: 8n,
              objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
              objectName: capabilityOffSecret,
              definitionLocation: capabilityOffSecret,
              stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
            },
          },
        };
        response.logicalPlan!.stages[1]!.operatorProvenance = [authorizedProvenance];
        response.logicalPlan!.stages[1]!.outputProvenance = [{
          outputField: maliciousField,
          provenance: authorizedProvenance,
        }];
        response.knowledgeSnapshot!.objects[0]!.disclosure = {
          $case: "authorizedObject",
          value: {
            knowledgeObjectId: capabilityOffSecret,
            version: 8n,
            name: capabilityOffSecret,
          },
        };
      }
      if (knowledgeAdvertised && serveForeignIdentity) {
        serveForeignIdentity = false;
        response.knowledgeSnapshot!.objects[0]!.disclosure = {
          $case: "authorizedObject",
          value: {
            knowledgeObjectId: foreignSecret,
            version: 9n,
            name: foreignSecret,
          },
        };
      }
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(InspectSearchJobResponse.encode(response).finish()),
      });
    },
  );

  let inspectorURL = "";
  let inspectorStorage: { local: [string, string][]; session: [string, string][] };
  await test.step("capability-off strips forged Knowledge from one exact inspect response", async () => {
    const runSearch = await openSearchWorkspace(page);
    await runSearch.click();
    await expect(page.getByTestId("job-strip")).toContainText("Completed", { timeout });
    await expectCompletedResultRequest(1, "inspection-job-1");
    expect(inspectRequests, "inspection must not start automatically").toHaveLength(0);
    const inspectTrigger = page.getByRole("button", { name: "Inspect search job" });
    inspectorURL = page.url();
    inspectorStorage = await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }));
    const trafficBeforeInspect = apiTraffic.length;
    await inspectTrigger.click();
    const dialog = page.getByRole("dialog", { name: "Search job inspector" });
    await expect(dialog.getByText("ParallelExtend", { exact: true })).toBeVisible({ timeout });
    await expect(dialog).toContainText(maliciousField);
    await expect(dialog.getByRole("region", { name: "Knowledge authority" })).toHaveCount(0);
    await expect(dialog).not.toContainText(capabilityOffSecret);
    await expect(page.locator("body")).not.toContainText(capabilityOffSecret);
    await expect(dialog).not.toContainText("Calculated field");
    await expect(dialog).not.toContainText(/redacted ordinal/iu);
    await expect(dialog.locator("script, img")).toHaveCount(0);
    expect(await page.evaluate(() => Reflect.get(globalThis, "__inspectionExecuted")))
      .toBeUndefined();
    await waitForBrowserRender(page);
    expect(inspectRequests).toEqual([{ searchJobId: "inspection-job-1" }]);
    expect(apiTraffic.slice(trafficBeforeInspect)).toEqual([{
      method: "POST",
      pathname: "/api/search/jobs/inspect",
    }]);
    await page.keyboard.press("Escape");
    await expect(dialog).toHaveCount(0);
    await expect(inspectTrigger).toBeFocused();
  });

  await test.step("foreign identity fails generically before a valid redacted retry", async () => {
    knowledgeAdvertised = true;
    const runSearch = await openSearchWorkspace(page);
    await runSearch.click();
    await expect(page.getByTestId("job-strip")).toContainText("Completed", { timeout });
    await expectCompletedResultRequest(2, "inspection-job-2");
    expect(inspectRequests, "the second job must not be inspected automatically").toHaveLength(1);
    const inspectTrigger = page.getByRole("button", { name: "Inspect search job" });
    const trafficBeforeInspect = apiTraffic.length;
    await inspectTrigger.click();
    let dialog = page.getByRole("dialog", { name: "Search job inspector" });
    await expect(dialog.getByRole("alert")).toHaveText(
      "Inspection unavailable: The server could not inspect this search job.",
      { timeout },
    );
    await expect(dialog).not.toContainText(foreignSecret);
    await dialog.getByRole("button", { name: "Done" }).click();
    await expect(inspectTrigger).toBeFocused();

    await inspectTrigger.click();
    dialog = page.getByRole("dialog", { name: "Search job inspector" });
    const knowledgeRegion = dialog.getByRole("region", { name: "Knowledge authority" });
    await expect(knowledgeRegion).toContainText("42".repeat(32), { timeout });
    await expect(knowledgeRegion).toContainText("Catalog revision7");
    await expect(knowledgeRegion).toContainText("Applicable objects1");
    await expect(knowledgeRegion).toContainText("Redacted ordinal 0 · Calculated field");
    await expect(dialog).toContainText("Calculated field · ordinal 0");
    await expect(dialog).toContainText(
      `${maliciousField} ← redacted ordinal 0`,
    );
    await expect(dialog.getByText("Generated SQL", { exact: true }).locator("..")).toContainText(
      longGeneratedSQL,
    );
    await expect(dialog).not.toContainText(foreignSecret);
    await expect(dialog).not.toContainText("24".repeat(32));
    await expect(dialog.locator("script, img, a[href]")).toHaveCount(0);
    await expect(dialog.getByRole("button", {
      name: /create|update|enable|disable|delete knowledge/i,
    })).toHaveCount(0);
    expect(await page.evaluate(() => Reflect.get(globalThis, "__inspectionExecuted")))
      .toBeUndefined();
    await waitForBrowserRender(page);
    expect(inspectRequests.slice(1)).toEqual([
      { searchJobId: "inspection-job-2" },
      { searchJobId: "inspection-job-2" },
    ]);
    expect(apiTraffic.slice(trafficBeforeInspect)).toEqual([
      { method: "POST", pathname: "/api/search/jobs/inspect" },
      { method: "POST", pathname: "/api/search/jobs/inspect" },
    ]);

    await page.setViewportSize({ width: 375, height: 812 });
    expect(await dialog.evaluate((element) => {
      const bounds = element.getBoundingClientRect();
      const body = element.querySelector<HTMLElement>(".modal-body");
      const knowledge = element.querySelector<HTMLElement>("[aria-label='Knowledge authority']");
      const logicalLabel = Array.from(element.querySelectorAll("strong"))
        .find((candidate) => candidate.textContent === "Logical plan");
      const logical = logicalLabel?.parentElement ?? null;
      if (body === null) {
        return {
          bodyNoHorizontalOverflow: false,
          dialogContained: false,
          dialogNoHorizontalOverflow: false,
          knowledgeContained: false,
          logicalContained: false,
        };
      }
      const bodyBounds = body.getBoundingClientRect();
      const horizontallyContained = (candidate: HTMLElement | null): boolean => {
        if (candidate === null) return false;
        const candidateBounds = candidate.getBoundingClientRect();
        return candidateBounds.left >= bodyBounds.left - 1
          && candidateBounds.right <= bodyBounds.right + 1;
      };
      return {
        bodyNoHorizontalOverflow: body.scrollWidth <= body.clientWidth,
        dialogContained: bounds.left >= -1
          && bounds.right <= window.innerWidth + 1
          && bounds.top >= -1
          && bounds.bottom <= window.innerHeight + 1,
        dialogNoHorizontalOverflow: element.scrollWidth <= element.clientWidth,
        knowledgeContained: horizontallyContained(knowledge),
        logicalContained: horizontallyContained(logical),
      };
    })).toEqual({
      bodyNoHorizontalOverflow: true,
      dialogContained: true,
      dialogNoHorizontalOverflow: true,
      knowledgeContained: true,
      logicalContained: true,
    });
    expect(page.url()).toBe(inspectorURL);
    expect(await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }))).toEqual(inspectorStorage);
    expect(apiTraffic.filter(({ pathname }) => pathname.startsWith("/api/knowledge/")))
      .toEqual([]);
    assertBrowserSafety(safety);
    await dialog.getByRole("button", { name: "Done" }).click();
    await expect(dialog).toHaveCount(0);
    await page.setViewportSize({ width: 1_280, height: 720 });
    await expect(inspectTrigger).toBeVisible();
  });

  await test.step("a superseding inspection prevents a held stale response from committing", async () => {
    const inspectTrigger = page.getByRole("button", { name: "Inspect search job" });
    const trafficBeforeRace = apiTraffic.length;
    const requestCountBeforeRace = inspectRequests.length;
    holdNextInspection = true;
    let dialog = page.getByRole("dialog", { name: "Search job inspector" });
    try {
      await inspectTrigger.click();
      await expect.poll(() => heldInspection.started, {
        message: "the first racing inspection reached its deterministic response barrier",
        timeout,
      }).toBe(true);
      await expect(dialog).toContainText("Loading the administrator inspection plan…");
      await page.keyboard.press("Escape");
      await expect(dialog).toHaveCount(0);
      await expect(inspectTrigger).toBeFocused();

      await inspectTrigger.click();
      dialog = page.getByRole("dialog", { name: "Search job inspector" });
      await expect(dialog).toContainText("inspection-diagnostic-query", { timeout });
      await expect(dialog).not.toContainText(staleInspectionMarker);

      heldInspection.release?.();
      await heldInspection.settled;
      await waitForBrowserRender(page);
      await expect(dialog).toContainText("inspection-diagnostic-query");
      await expect(dialog).not.toContainText(staleInspectionMarker);
      expect(inspectRequests.slice(requestCountBeforeRace)).toEqual([
        { searchJobId: "inspection-job-2" },
        { searchJobId: "inspection-job-2" },
      ]);
      expect(apiTraffic.slice(trafficBeforeRace)).toEqual([
        { method: "POST", pathname: "/api/search/jobs/inspect" },
        { method: "POST", pathname: "/api/search/jobs/inspect" },
      ]);
      expect(page.url()).toBe(inspectorURL);
      expect(await page.evaluate(() => ({
        local: Object.entries(localStorage),
        session: Object.entries(sessionStorage),
      }))).toEqual(inspectorStorage);
      expect(apiTraffic.filter(({ pathname }) => pathname.startsWith("/api/knowledge/")))
        .toEqual([]);
      assertBrowserSafety(safety, [
        /^POST \/api\/search\/jobs\/inspect: net::ERR_ABORTED$/u,
      ]);
      await page.keyboard.press("Escape");
      await expect(dialog).toHaveCount(0);
      await expect(inspectTrigger).toBeFocused();
    } finally {
      heldInspection.release?.();
      if (heldInspection.started) await heldInspection.settled;
    }
  });
});

test("Mutation Audit renders historical Knowledge events without the Knowledge feature", async ({
  page,
}) => {
  const protobufHeaders = { "content-type": "application/x-protobuf" };
  const maliciousTargetId = "ko-<script>globalThis.__auditScriptExecuted=true</script>";
  const maliciousAppId = "app-<img src=x onerror=globalThis.__auditScriptExecuted=true>";
  const legacyTargetId = "saved-search-legacy";
  const auditRequests: ListAuditEventsRequest[] = [];
  const apiTraffic: Array<{ method: string; pathname: string }> = [];

  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.origin === origin && url.pathname.startsWith("/api/")) {
      apiTraffic.push({ method: request.method(), pathname: url.pathname });
    }
  });
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/system/bootstrap",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(
        GetSystemBootstrapResponse.fromPartial({
          searchWebsocketPath: "/api/search/ws",
          // Historical journal visibility deliberately omits the dormant Knowledge feature.
          features: [ServerFeature.SERVER_FEATURE_AUDIT_SEARCH],
          limits: { maximumPageSize: 25 },
          serverTime: new Date("2026-08-09T12:00:00.000Z"),
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/audit/events/list",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("audit List request omitted its protobuf body");
      auditRequests.push(ListAuditEventsRequest.decode(wire));
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ListAuditEventsResponse.encode(
          ListAuditEventsResponse.fromPartial({
            auditEvents: [{
              sequence: 2n,
              occurredAt: new Date("2026-08-09T11:59:00.000Z"),
              actorKind: AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
              actorId: "admin-audit",
              actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
              action: AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE,
              targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT,
              targetId: maliciousTargetId,
              targetVersion: 2n,
              appId: maliciousAppId,
              objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
              sharingScope: SharingScope.SHARING_SCOPE_APP,
            }, {
              sequence: 1n,
              occurredAt: new Date("2026-08-09T11:58:00.000Z"),
              actorKind: AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
              actorId: "admin-audit",
              actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
              action: AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE,
              targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH,
              targetId: legacyTargetId,
              targetVersion: 2n,
            }],
            page: { totalSize: 2n, totalSizeExact: true },
          }),
        ).finish()),
      });
    },
  );

  let activityURL = "";
  let storageBefore: { local: [string, string][]; session: [string, string][] };
  let trafficBeforeAudit = 0;
  await test.step("audit-only bootstrap exposes the Mutation Audit tab", async () => {
    activityURL = new URL("/activity/", origin).href;
    await page.goto(activityURL, { waitUntil: "domcontentloaded", timeout });
    const mutationTab = page.getByRole("tab", { name: /Mutation audit/ });
    await expect(mutationTab).toBeVisible({ timeout });
    await expect(page.getByRole("tab", { name: /Search attempts/ })).toHaveCount(0);
    storageBefore = await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }));
    trafficBeforeAudit = apiTraffic.length;
    await mutationTab.click();
  });

  const mutationPanel = page.locator("#activity-mutations-panel");
  await test.step("Knowledge metadata is escaped into the read-only journal", async () => {
    const row = mutationPanel.getByRole("row").filter({ hasText: maliciousTargetId });
    await expect(row.getByText("Knowledge object · scope change", { exact: true }))
      .toBeVisible({ timeout });
    await expect(row).toContainText(`App: ${maliciousAppId}`);
    await expect(row).toContainText("Type: Field alias");
    await expect(row).toContainText("Sharing: App");
    await expect(row.locator("script, img, a, button, input, textarea, select")).toHaveCount(0);
    const legacyRow = mutationPanel.getByRole("row").filter({ hasText: legacyTargetId });
    await expect(legacyRow).toContainText("Saved search");
    await expect(legacyRow).not.toContainText(/App:|Type:|Sharing:/);
    await expect(mutationPanel.getByLabel("Target kind").locator("option").filter({
      hasText: "Knowledge object",
    })).toHaveCount(1);
    await expect(mutationPanel.getByLabel("Actions").locator("option").filter({
      hasText: /^Knowledge object ·/,
    })).toHaveCount(6);
    expect(await page.evaluate(() => Reflect.get(globalThis, "__auditScriptExecuted")))
      .toBeUndefined();
  });

  await test.step("the read-only projection emits only exact audit List traffic", async () => {
    await page.evaluate(() => new Promise<void>((resolve) => {
      requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
    }));
    expect(auditRequests.length).toBeGreaterThan(0);
    expect(auditRequests.length).toBeLessThanOrEqual(2);
    const expectedRequest: ListAuditEventsRequest = {
      page: { pageSize: 25, pageToken: undefined, includeTotalSize: true },
      actionFilters: [],
      actorIdFilter: undefined,
      targetKindFilter: undefined,
    };
    expect(auditRequests).toEqual(Array.from(
      { length: auditRequests.length },
      () => expectedRequest,
    ));
    expect(apiTraffic.slice(trafficBeforeAudit)).toEqual(Array.from(
      { length: auditRequests.length },
      () => ({ method: "POST", pathname: "/api/audit/events/list" }),
    ));
    expect(apiTraffic.filter(({ pathname }) => pathname.startsWith("/api/knowledge/")))
      .toEqual([]);
    expect(page.url()).toBe(activityURL);
    expect(await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }))).toEqual(storageBefore);
  });
});

test("bootstrap-advertised Knowledge Manager keeps advanced filters in one exact cursor tuple", async ({
  page,
}) => {
  test.setTimeout(90_000);
  const protobufHeaders = { "content-type": "application/x-protobuf" };
  const appId = "app-observability";
  const cursor = "knowledge-cursor-1";
  const dependencyCursor = "knowledge-dependencies-cursor-1";
  const dependentCursor = "knowledge-dependents-cursor-1";
  const maliciousDependencyId = "ko-dependency-<script>";
  const dependencyBId = "ko-dependency-b";
  const maliciousDependentId =
    "ko-dependent-<img onerror=globalThis.__knowledgeScriptExecuted=true>";
  const maliciousName = "<script>globalThis.__knowledgeScriptExecuted=true</script>";
  let knowledgeAdvertised = false;
  let serveMismatchedDetail = true;
  let serveMismatchedRelatedObject = true;
  let maliciousDependencyGetCount = 0;
  let holdNextDependencyB = true;
  const dependencyRetryGate = createRequestGate();
  const heldDependencyB = createRequestGate();
  let heldDependencyBReplacementStarted = false;
  const requestedURLs: string[] = [];
  const listRequests: ListKnowledgeObjectsRequest[] = [];
  const rootGetRequests: GetKnowledgeObjectRequest[] = [];
  const relatedGetRequests: GetKnowledgeObjectRequest[] = [];
  const dependencyRequests: ListKnowledgeObjectDependenciesRequest[] = [];
  const dependentRequests: ListKnowledgeObjectDependentsRequest[] = [];
  const createRequests: CreateKnowledgeObjectRequest[] = [];
  const validateRequests: ValidateKnowledgeObjectRequest[] = [];
  const updateRequests: UpdateKnowledgeObjectRequest[] = [];
  const stateRequests: SetKnowledgeObjectStateRequest[] = [];
  const deleteRequests: DeleteKnowledgeObjectRequest[] = [];
  const previewRequests: PreviewKnowledgeObjectRequest[] = [];
  const mutationAuthorizations: string[] = [];
  const administratorToken = "K".repeat(32);
  const catalogStateToken = new Uint8Array(32).fill(0x35);
  let malformedValidationResponses = 1;
  let holdNextValidation = true;
  const staleValidationGate = createRequestGate();
  let knowledgeDeleted = false;
  let mutationClock = 0;

  const knowledgeObject = (id: string, name: string, version: bigint): KnowledgeObject => {
    const definition = KnowledgeObjectDefinition.fromPartial({
      appId,
      name,
      description: "Rendered text only: <img src=x onerror=globalThis.__knowledgeScriptExecuted=true>",
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
      selector: {
        sourcePatterns: [{
          matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
          value: "source::api",
        }],
      },
      body: {
        $case: "fieldAlias",
        value: {
          sourceField: "status",
          destinationField: "http_status",
          overwriteBehavior:
            KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
        },
      },
    });
    return KnowledgeObject.fromPartial({
      knowledgeObjectId: id,
      tenantId: "tenant-local",
      appId,
      ownerId: "owner-7",
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      name,
      version,
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
      definition,
      definitionSha256: createHash("sha256")
        .update(KnowledgeObjectDefinition.encode(definition).finish())
        .digest(),
      createdAt: new Date("2026-08-08T10:00:00.000Z"),
      updatedAt: new Date("2026-08-08T10:01:00.000Z"),
    });
  };
  let firstObject = knowledgeObject("ko-malicious", maliciousName, 2n);
  const mismatchedDetailObject = knowledgeObject("ko-malicious", maliciousName, 3n);
  const continuationObject = knowledgeObject("ko-continuation", "continued_alias", 3n);
  const maliciousDependencyObject = knowledgeObject(
    maliciousDependencyId,
    "<img src=x onerror=globalThis.__knowledgeScriptExecuted=true>",
    1n,
  );
  const dependencyBObject = knowledgeObject(dependencyBId, "dependency_b", 3n);
  const maliciousDependentObject = knowledgeObject(
    maliciousDependentId,
    "<script>globalThis.__knowledgeScriptExecuted=true</script>",
    3n,
  );
  const mismatchedRelatedObject = knowledgeObject(
    maliciousDependencyId,
    "SECRET_MISMATCHED_RELATED_OBJECT",
    9n,
  );

  page.on("request", (request) => requestedURLs.push(request.url()));
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/system/bootstrap",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(
        GetSystemBootstrapResponse.fromPartial({
          searchWebsocketPath: "/api/search/ws",
          features: knowledgeAdvertised
            ? [ServerFeature.SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS]
            : [],
          limits: { maximumPageSize: 2 },
          apps: [{
            appId,
            slug: "observability",
            displayName: "Observability",
            state: AppState.APP_STATE_ACTIVE,
          }],
          selectedAppId: appId,
          serverTime: new Date("2026-08-08T12:00:00.000Z"),
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/indexes/list",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(ListIndexesResponse.encode(
        ListIndexesResponse.fromPartial({
          page: { totalSize: 0n, totalSizeExact: true },
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/ingestion-tokens/list",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(ListIngestionTokensResponse.encode(
        ListIngestionTokensResponse.fromPartial({
          page: { totalSize: 0n, totalSizeExact: true },
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/list",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge List request omitted its protobuf body");
      const request = ListKnowledgeObjectsRequest.decode(wire);
      listRequests.push(request);
      const continuation = request.page?.pageToken === cursor;
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ListKnowledgeObjectsResponse.encode(
          ListKnowledgeObjectsResponse.fromPartial({
            knowledgeObjects: knowledgeDeleted
              ? []
              : continuation ? [continuationObject] : [firstObject],
            page: {
              nextPageToken: knowledgeDeleted || continuation ? undefined : cursor,
              totalSize: knowledgeDeleted ? 0n : 2n,
              totalSizeExact: true,
            },
            // Continuations deliberately become stale; Apply/Clear must discard them.
            tenantCatalogRevision: continuation ? 8n : 7n,
          }),
        ).finish()),
      });
    },
  );

  function mutationAuthorization(route: Route): void {
    const authorization = route.request().headers()["authorization"] ?? "";
    mutationAuthorizations.push(authorization);
    if (authorization !== `Bearer ${administratorToken}`) {
      throw new Error(`knowledge mutation omitted its administrator bearer: ${authorization}`);
    }
  }

  function nextMutationDate(): Date {
    mutationClock += 1;
    return new Date(`2026-08-08T10:${String(1 + mutationClock).padStart(2, "0")}:00.000Z`);
  }

  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/validate",
    async (route) => {
      mutationAuthorization(route);
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Validate request omitted its protobuf body");
      const request = ValidateKnowledgeObjectRequest.decode(wire);
      validateRequests.push(request);
      if (malformedValidationResponses > 0) {
        malformedValidationResponses -= 1;
        await route.fulfill({
          status: 200,
          headers: protobufHeaders,
          body: Buffer.from(ValidateKnowledgeObjectResponse.encode(
            ValidateKnowledgeObjectResponse.fromPartial({ tenantCatalogRevision: 40n }),
          ).finish()),
        });
        return;
      }
      const heldForStaleReplacement = holdNextValidation;
      if (heldForStaleReplacement) {
        holdNextValidation = false;
        staleValidationGate.started = true;
        await new Promise<void>((resolve) => {
          staleValidationGate.release = resolve;
        });
        staleValidationGate.release = null;
      }
      if (request.definition === undefined) {
        throw new Error("knowledge Validate omitted its definition");
      }
      const selector = request.definition.selector;
      const selectorPatterns = (selector?.indexPatterns.length ?? 0)
        + (selector?.hostPatterns.length ?? 0)
        + (selector?.sourcePatterns.length ?? 0)
        + (selector?.sourcetypePatterns.length ?? 0);
      const response = {
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ValidateKnowledgeObjectResponse.encode(
          ValidateKnowledgeObjectResponse.fromPartial({
            result: {
              valid: true,
              objectType: tierOneObjectType(request.definition),
              normalizedDefinition: request.definition,
              definitionSha256: tierOneDefinitionDigest(request.definition),
              resources: {
                selectorPatterns,
                normalizedDefinitionBytes: BigInt(
                  KnowledgeObjectDefinition.encode(request.definition).finish().byteLength,
                ),
              },
            },
            tenantCatalogRevision: 40n,
          }),
        ).finish()),
      };
      try {
        await route.fulfill(response);
      } catch (error) {
        if (!heldForStaleReplacement) throw error;
        // Editing the candidate aborts this deliberately held Validate request.
      } finally {
        if (heldForStaleReplacement) staleValidationGate.markSettled();
      }
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/create",
    async (route) => {
      mutationAuthorization(route);
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Create request omitted its protobuf body");
      const request = CreateKnowledgeObjectRequest.decode(wire);
      createRequests.push(request);
      if (request.definition === undefined) throw new Error("knowledge Create omitted definition");
      const occurredAt = nextMutationDate();
      const created = KnowledgeObject.fromPartial({
        knowledgeObjectId: "ko-browser-created",
        tenantId: "tenant-local",
        appId: request.definition.appId,
        ownerId: "owner-7",
        objectType: tierOneObjectType(request.definition),
        name: request.definition.name,
        version: 1n,
        sharingScope: request.definition.sharingScope,
        state: request.initialState,
        definition: request.definition,
        definitionSha256: tierOneDefinitionDigest(request.definition),
        createdAt: occurredAt,
        updatedAt: occurredAt,
      });
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(CreateKnowledgeObjectResponse.encode(
          CreateKnowledgeObjectResponse.fromPartial({
            knowledgeObject: created,
            tenantCatalogRevision: 41n,
            tenantCatalogStateToken: catalogStateToken,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/update",
    async (route) => {
      mutationAuthorization(route);
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Update request omitted its protobuf body");
      const request = UpdateKnowledgeObjectRequest.decode(wire);
      updateRequests.push(request);
      if (request.definition === undefined) throw new Error("knowledge Update omitted definition");
      const updatedAt = nextMutationDate();
      firstObject = KnowledgeObject.fromPartial({
        ...firstObject,
        appId: request.definition.appId,
        name: request.definition.name,
        sharingScope: request.definition.sharingScope,
        version: request.expectedVersion + 1n,
        definition: request.definition,
        definitionSha256: tierOneDefinitionDigest(request.definition),
        updatedAt,
      });
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(UpdateKnowledgeObjectResponse.encode(
          UpdateKnowledgeObjectResponse.fromPartial({
            knowledgeObject: firstObject,
            tenantCatalogRevision: 42n,
            tenantCatalogStateToken: catalogStateToken,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/set-state",
    async (route) => {
      mutationAuthorization(route);
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge SetState request omitted its protobuf body");
      const request = SetKnowledgeObjectStateRequest.decode(wire);
      stateRequests.push(request);
      const updatedAt = nextMutationDate();
      firstObject = KnowledgeObject.fromPartial({
        ...firstObject,
        version: request.expectedVersion + 1n,
        state: request.state,
        updatedAt,
        disabledAt: request.state === KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED
          ? updatedAt
          : undefined,
      });
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(SetKnowledgeObjectStateResponse.encode(
          SetKnowledgeObjectStateResponse.fromPartial({
            knowledgeObject: firstObject,
            tenantCatalogRevision: 43n + BigInt(stateRequests.length),
            tenantCatalogStateToken: catalogStateToken,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/delete",
    async (route) => {
      mutationAuthorization(route);
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Delete request omitted its protobuf body");
      const request = DeleteKnowledgeObjectRequest.decode(wire);
      deleteRequests.push(request);
      knowledgeDeleted = true;
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(DeleteKnowledgeObjectResponse.encode(
          DeleteKnowledgeObjectResponse.fromPartial({
            knowledgeObjectId: request.knowledgeObjectId,
            deletedVersion: request.expectedVersion + 1n,
            tenantCatalogRevision: 50n,
            tenantCatalogStateToken: catalogStateToken,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/preview",
    async (route) => {
      mutationAuthorization(route);
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Preview request omitted its protobuf body");
      const request = PreviewKnowledgeObjectRequest.decode(wire);
      previewRequests.push(request);
      if (request.definition === undefined) {
        throw new Error("knowledge Preview omitted its candidate definition");
      }
      const schema = ResultSchema.fromPartial({
        schemaId: request.retainedSearchJobId,
        revision: 1n,
        resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
        columns: [{
          fieldName: "http_status",
          displayName: "http_status",
          valueType: ValueType.VALUE_TYPE_STRING,
          semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_DIMENSION,
        }],
      });
      const previewRow = (value: string) => ResultRow.fromPartial({
        rowId: `${request.retainedSearchJobId}:0`,
        ordinal: 0n,
        cells: [TypedValue.fromPartial({
          kind: { $case: "stringValue", value },
        })],
      });
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(PreviewKnowledgeObjectResponse.encode(
          PreviewKnowledgeObjectResponse.fromPartial({
            validation: {
              valid: true,
              objectType: tierOneObjectType(request.definition),
              normalizedDefinition: request.definition,
              definitionSha256: tierOneDefinitionDigest(request.definition),
              resources: {
                selectorPatterns: 1,
                normalizedDefinitionBytes: BigInt(
                  KnowledgeObjectDefinition.encode(request.definition).finish().byteLength,
                ),
              },
            },
            beforeSchema: schema,
            afterSchema: schema,
            beforeRows: [previewRow("before-status")],
            afterRows: [previewRow("after-http-status")],
            tenantCatalogRevision: 40n,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/get",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Get request omitted its protobuf body");
      const request = GetKnowledgeObjectRequest.decode(wire);
      let responseObject: KnowledgeObject | undefined;
      let requestGate: ReturnType<typeof createRequestGate> | null = null;
      if (request.knowledgeObjectId === firstObject.knowledgeObjectId) {
        rootGetRequests.push(request);
        responseObject = serveMismatchedDetail ? mismatchedDetailObject : firstObject;
      } else {
        relatedGetRequests.push(request);
        if (
          request.knowledgeObjectId === maliciousDependencyId
          && request.version === maliciousDependencyObject.version
        ) {
          maliciousDependencyGetCount += 1;
          responseObject = serveMismatchedRelatedObject
            ? mismatchedRelatedObject
            : maliciousDependencyObject;
          serveMismatchedRelatedObject = false;
          if (maliciousDependencyGetCount === 2) {
            const gate = dependencyRetryGate;
            requestGate = gate;
            gate.started = true;
            await new Promise<void>((resolve) => {
              gate.release = resolve;
            });
            gate.release = null;
          }
        } else if (
          request.knowledgeObjectId === dependencyBId
          && request.version === dependencyBObject.version
        ) {
          responseObject = dependencyBObject;
          if (holdNextDependencyB) {
            holdNextDependencyB = false;
            const gate = heldDependencyB;
            requestGate = gate;
            gate.started = true;
            await new Promise<void>((resolve) => {
              gate.release = resolve;
            });
            gate.release = null;
          }
        } else if (
          request.knowledgeObjectId === maliciousDependentId
          && request.version === maliciousDependentObject.version
        ) {
          responseObject = maliciousDependentObject;
        }
      }
      const response = {
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(GetKnowledgeObjectResponse.encode(
          GetKnowledgeObjectResponse.fromPartial({
            knowledgeObject: responseObject,
          }),
        ).finish()),
      };
      if (requestGate !== null) {
        try {
          await route.fulfill(response);
        } catch (error) {
          if (
            requestGate !== heldDependencyB
            || !heldDependencyBReplacementStarted
          ) throw error;
          // Replacing inspector A aborts this deliberately held transport.
        } finally {
          requestGate.markSettled();
        }
        return;
      }
      await route.fulfill(response);
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/dependencies",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Dependencies request omitted its protobuf body");
      const request = ListKnowledgeObjectDependenciesRequest.decode(wire);
      dependencyRequests.push(request);
      const continuation = request.page?.pageToken === dependencyCursor;
      const dependencies = continuation
        ? [{
          source: { knowledgeObjectId: firstObject.knowledgeObjectId, version: firstObject.version },
          target: { knowledgeObjectId: "ko-dependency-z", version: 4n },
          role: KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
        }]
        : [{
          source: { knowledgeObjectId: firstObject.knowledgeObjectId, version: firstObject.version },
          target: { knowledgeObjectId: maliciousDependencyId, version: 1n },
          role: KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
        }, {
          source: { knowledgeObjectId: firstObject.knowledgeObjectId, version: firstObject.version },
          target: { knowledgeObjectId: dependencyBId, version: 3n },
          role: KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
        }];
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ListKnowledgeObjectDependenciesResponse.encode(
          ListKnowledgeObjectDependenciesResponse.fromPartial({
            dependencies,
            page: {
              nextPageToken: continuation ? undefined : dependencyCursor,
              totalSize: 3n,
              totalSizeExact: true,
            },
            tenantCatalogRevision: 11n,
            resolvedObject: {
              knowledgeObjectId: firstObject.knowledgeObjectId,
              version: firstObject.version,
            },
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/knowledge/objects/dependents",
    async (route) => {
      const wire = route.request().postDataBuffer();
      if (wire === null) throw new Error("knowledge Dependents request omitted its protobuf body");
      const request = ListKnowledgeObjectDependentsRequest.decode(wire);
      dependentRequests.push(request);
      const continuation = request.page?.pageToken === dependentCursor;
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ListKnowledgeObjectDependentsResponse.encode(
          ListKnowledgeObjectDependentsResponse.fromPartial({
            dependents: [{
              source: {
                knowledgeObjectId: continuation
                  ? "ko-dependent-z"
                  : maliciousDependentId,
                version: continuation ? 5n : 3n,
              },
              target: {
                knowledgeObjectId: firstObject.knowledgeObjectId,
                version: firstObject.version,
              },
              role: KnowledgeDependencyRole.KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
            }],
            page: {
              nextPageToken: continuation ? undefined : dependentCursor,
              totalSize: 2n,
              totalSizeExact: true,
            },
            // The continuation is well-formed but belongs to a newer catalog,
            // so the UI must keep the first page and offer a bounded retry.
            tenantCatalogRevision: continuation ? 13n : 12n,
            resolvedObject: {
              knowledgeObjectId: firstObject.knowledgeObjectId,
              version: firstObject.version,
            },
          }),
        ).finish()),
      });
    },
  );

  const adminURL = new URL("/admin/", origin).href;
  await page.goto(adminURL, { waitUntil: "domcontentloaded", timeout });
  await expect(page.getByRole("heading", { name: "Administration" })).toBeVisible({ timeout });
  await expect(page.locator(".admin-sidebar")).toBeVisible({ timeout });
  await expect(page.getByText("API connected", { exact: true })).toBeVisible({ timeout });
  await expect(page.locator(".admin-sidebar button").filter({ hasText: "Knowledge Manager" }))
    .toHaveCount(0);
  expect(requestedURLs.filter((value) => value.includes("/api/knowledge/"))).toEqual([]);

  knowledgeAdvertised = true;
  await page.goto(new URL("/signin/", origin).href, {
    waitUntil: "domcontentloaded",
    timeout,
  });
  await page.getByLabel("Administrator bearer token").fill(administratorToken);
  await page.getByRole("button", { name: "Open administrator session" }).click();
  await expect(page.getByRole("heading", { name: "Administration" })).toBeVisible({ timeout });
  const knowledgeNavigation = page.locator(".admin-sidebar button").filter({
    hasText: "Knowledge Manager",
  });
  await expect(knowledgeNavigation).toBeVisible({ timeout });
  await knowledgeNavigation.click();
  const manager = page.locator(".knowledge-manager");
  await expect(manager.getByRole("heading", { name: "Knowledge Manager" })).toBeVisible({ timeout });
  await expect(manager.getByText(maliciousName, { exact: true })).toBeVisible({ timeout });
  // React development strict effects may replay only the initial mount request.
  const initialListRequestCount = listRequests.length;
  expect(initialListRequestCount).toBeGreaterThan(0);
  expect(initialListRequestCount).toBeLessThanOrEqual(2);
  const initialListRequest: ListKnowledgeObjectsRequest = {
    page: { pageSize: 2, pageToken: undefined, includeTotalSize: true },
    appIdFilter: appId,
    ownerIdFilter: undefined,
    textFilter: undefined,
    objectTypeFilters: [],
    stateFilters: [],
    sharingScopeFilters: [],
    selectorTextFilter: undefined,
    sortBy: KnowledgeObjectSortBy.KNOWLEDGE_OBJECT_SORT_BY_NAME,
    sortDirection: SortDirection.SORT_DIRECTION_ASCENDING,
  };
  expect(listRequests).toEqual(Array.from(
    { length: initialListRequestCount },
    () => initialListRequest,
  ));
  let expectedListRequestCount = initialListRequestCount;
  async function expectNextListRequest(
    phase: string,
    expected: ListKnowledgeObjectsRequest,
  ): Promise<void> {
    expectedListRequestCount += 1;
    await expect.poll(() => listRequests.length, {
      message: `${phase} issued exactly one List request`,
      timeout,
    }).toBe(expectedListRequestCount);
    expect(listRequests.at(-1), `${phase} List request tuple`).toEqual(expected);
    await waitForBrowserRender(page);
    expect(listRequests, `${phase} emitted no delayed duplicate List request`)
      .toHaveLength(expectedListRequestCount);
  }
  let expectedRelatedGetRequestCount = 0;
  async function expectNextRelatedGetRequest(
    phase: string,
    expected: GetKnowledgeObjectRequest,
  ): Promise<void> {
    expectedRelatedGetRequestCount += 1;
    await expect.poll(() => relatedGetRequests.length, {
      message: `${phase} issued exactly one endpoint Get request`,
      timeout,
    }).toBe(expectedRelatedGetRequestCount);
    expect(relatedGetRequests.at(-1), `${phase} endpoint Get tuple`).toEqual(expected);
    await waitForBrowserRender(page);
    expect(relatedGetRequests, `${phase} emitted no delayed endpoint Get request`)
      .toHaveLength(expectedRelatedGetRequestCount);
  }
  await expect(manager.getByLabel("Tier-1 management surface")).toBeVisible();
  await expect(manager.getByRole("button", { name: "Create knowledge object" })).toBeVisible();
  await Promise.all(["Edit", "Delete", "Activate", "Disable", "Save"].map(
    (mutationLabel) => expect(manager.getByRole("button", {
      name: new RegExp(`^${mutationLabel}(?:\\s|$)`, "i"),
    })).toHaveCount(0),
  ));
  await expect(manager.locator("script, img")).toHaveCount(0);
  expect(await page.evaluate(() => Reflect.get(globalThis, "__knowledgeScriptExecuted")))
    .toBeUndefined();

  await page.setViewportSize({ width: 1280, height: 800 });
  expect((await manager.locator(".knowledge-manager__advanced-filter-grid").evaluate(
    (element) => getComputedStyle(element).gridTemplateColumns.split(" ").length,
  ))).toBe(4);

  const ownerFilter = manager.getByLabel("Owner ID");
  const textFilter = manager.getByLabel("Name or description");
  const sharingFilter = manager.getByLabel("Sharing scope");
  const selectorFilter = manager.getByLabel("Selector text");
  await ownerFilter.fill(" \towner-7 ");
  await textFilter.fill(" latency error ");
  await sharingFilter.selectOption("private");
  await selectorFilter.fill(" source::api ");
  await waitForBrowserRender(page);
  expect(listRequests).toHaveLength(expectedListRequestCount);
  const appliedListRequest: ListKnowledgeObjectsRequest = {
    ...initialListRequest,
    ownerIdFilter: "owner-7",
    textFilter: "latency error",
    sharingScopeFilters: [SharingScope.SHARING_SCOPE_PRIVATE],
    selectorTextFilter: "source::api",
  };
  await selectorFilter.press("Enter");
  await expectNextListRequest("advanced-filter Apply", appliedListRequest);
  await expect(ownerFilter).toHaveValue("owner-7");
  await expect(textFilter).toHaveValue("latency error");
  await expect(selectorFilter).toHaveValue("source::api");
  expect(new URL(page.url()).search).toBe("");

  const continuationListRequest: ListKnowledgeObjectsRequest = {
    ...appliedListRequest,
    page: { pageSize: 2, pageToken: cursor, includeTotalSize: true },
  };
  await manager.getByRole("button", { name: "Load next page" }).click();
  await expectNextListRequest("continuation", continuationListRequest);
  await expect(manager.getByRole("alert")).toContainText("Catalog page changed", { timeout });

  await textFilter.fill("latency warning");
  await waitForBrowserRender(page);
  expect(listRequests).toHaveLength(expectedListRequestCount);
  const committedListRequest: ListKnowledgeObjectsRequest = {
    ...appliedListRequest,
    textFilter: "latency warning",
  };
  await manager.getByRole("button", { name: "Apply filters" }).click();
  await expectNextListRequest("stale-state reset Apply", committedListRequest);
  await expect(manager.getByText("Catalog page changed")).toHaveCount(0);

  const maliciousRow = manager.getByRole("button", { name: new RegExp("globalThis") });
  await maliciousRow.focus();
  await maliciousRow.press("Enter");
  await expect(manager.getByText("Knowledge object unavailable", { exact: true }))
    .toBeVisible({ timeout });
  expect(rootGetRequests).toEqual([{
    knowledgeObjectId: "ko-malicious",
    version: 2n,
  }]);
  await waitForBrowserRender(page);
  expect(dependencyRequests).toHaveLength(0);
  expect(dependentRequests).toHaveLength(0);

  await manager.getByRole("button", { name: "Close knowledge object details" }).click();
  serveMismatchedDetail = false;
  await maliciousRow.press("Enter");
  await expect(manager.getByRole("heading", { name: maliciousName })).toBeVisible({ timeout });
  await expect(manager.getByRole("button", { name: "Edit" })).toBeVisible();
  await expect(manager.getByRole("button", { name: "Disable" })).toBeVisible();
  await expect(manager.getByRole("button", { name: "Delete" })).toBeVisible();
  expect(rootGetRequests).toEqual(Array.from({ length: 2 }, () => ({
    knowledgeObjectId: "ko-malicious",
    version: 2n,
  })));
  await expect(manager.locator("script, img")).toHaveCount(0);
  const escapedDetailMarkup = await manager.locator(".knowledge-manager__detail").evaluate(
    (element) => element.innerHTML,
  );
  expect(escapedDetailMarkup).toContain("&lt;img");

  const preview = manager.locator(".knowledge-preview");
  await expect(preview.getByText("No Preview request has been sent.", { exact: true }))
    .toBeVisible();
  expect(previewRequests).toHaveLength(0);
  const retainedPreviewJobID = "knowledge-preview-retained-job";
  await preview.getByLabel("Retained search job ID").fill(retainedPreviewJobID);
  await preview.getByLabel("Maximum rows per side").fill("7");
  await preview.getByRole("button", { name: "Compare before and after" }).click();
  await expect(preview.getByRole("heading", { name: "Before", exact: true })).toBeVisible({
    timeout,
  });
  await expect(preview.getByRole("heading", { name: "After", exact: true })).toBeVisible();
  await expect(preview.getByText("before-status", { exact: true })).toBeVisible();
  await expect(preview.getByText("after-http-status", { exact: true })).toBeVisible();
  expect(previewRequests).toHaveLength(1);
  expect(previewRequests[0]).toEqual({
    retainedSearchJobId: retainedPreviewJobID,
    definition: firstObject.definition,
    knowledgeObjectId: firstObject.knowledgeObjectId,
    expectedVersion: firstObject.version,
    updateMask: ["app_id", "description", "field_alias", "name", "selector", "sharing_scope"],
    maximumRows: 7,
  });

  const dependenciesSection = manager.locator(
    '.knowledge-manager__relationship-section[aria-labelledby="knowledge-dependencies-title"]',
  );
  const dependentsSection = manager.locator(
    '.knowledge-manager__relationship-section[aria-labelledby="knowledge-dependents-title"]',
  );
  const initialRelationshipPage = {
    pageSize: 2,
    pageToken: undefined,
    includeTotalSize: true,
  };
  const initialRelationshipRequest = {
    knowledgeObjectId: "ko-malicious",
    version: 2n,
    page: initialRelationshipPage,
  };
  await expect(dependenciesSection.getByText("ko-dependency-<script>", { exact: true }))
    .toBeVisible({ timeout });
  await expect(dependentsSection.getByText(
    "ko-dependent-<img onerror=globalThis.__knowledgeScriptExecuted=true>",
    { exact: true },
  )).toBeVisible({ timeout });
  await expect(dependenciesSection).toContainText("3 visible · revision 11");
  await expect(dependentsSection).toContainText("2 visible · revision 12");
  const initialDependencyRequestCount = dependencyRequests.length;
  const initialDependentRequestCount = dependentRequests.length;
  expect(initialDependencyRequestCount).toBeGreaterThan(0);
  expect(initialDependencyRequestCount).toBeLessThanOrEqual(2);
  expect(initialDependentRequestCount).toBeGreaterThan(0);
  expect(initialDependentRequestCount).toBeLessThanOrEqual(2);
  expect(dependencyRequests).toEqual(Array.from(
    { length: initialDependencyRequestCount },
    () => initialRelationshipRequest,
  ));
  expect(dependentRequests).toEqual(Array.from(
    { length: initialDependentRequestCount },
    () => initialRelationshipRequest,
  ));
  const escapedRelationshipsMarkup = await manager.locator(
    ".knowledge-manager__relationships",
  ).evaluate((element) => element.innerHTML);
  expect(escapedRelationshipsMarkup).toContain("ko-dependency-&lt;script&gt;");
  expect(escapedRelationshipsMarkup).toContain("ko-dependent-&lt;img");
  await expect(manager.locator("script, img")).toHaveCount(0);
  await waitForBrowserRender(page);
  expect(relatedGetRequests, "relationship paint never inspects an endpoint automatically")
    .toHaveLength(0);
  const inspectorURL = page.url();
  const inspectorStorage = await page.evaluate(() => ({
    local: Object.entries(localStorage),
    session: Object.entries(sessionStorage),
  }));
  const dependencyInspector = dependenciesSection.getByRole("region", {
    name: "Dependency object inspector",
  });
  await test.step("endpoint inspection is explicit, exact, uniform, and escaped", async () => {
    await dependenciesSection.getByRole("button", {
      name: `Inspect dependency ${maliciousDependencyId}, version 1`,
    }).click();
    await expectNextRelatedGetRequest("mismatched dependency inspection", {
      knowledgeObjectId: maliciousDependencyId,
      version: 1n,
    });
    await expect(dependencyInspector).toContainText(
      "Related object unavailable. This object cannot be inspected.",
      { timeout },
    );
    await expect(dependencyInspector).not.toContainText("SECRET_MISMATCHED_RELATED_OBJECT");
    const dependencyTrigger = dependenciesSection.getByRole("button", {
      name: `Close dependency ${maliciousDependencyId}, version 1`,
    });
    await expect(dependencyTrigger).toBeFocused();
    try {
      await dependencyInspector.getByRole("button", {
        name: `Retry dependency ${maliciousDependencyId}, version 1`,
      }).click();
      await expectNextRelatedGetRequest("dependency inspection retry", {
        knowledgeObjectId: maliciousDependencyId,
        version: 1n,
      });
      await expect(dependencyInspector).toContainText("Loading related object…", { timeout });
      await expect(dependencyTrigger).toBeFocused();
    } finally {
      dependencyRetryGate.release?.();
      if (dependencyRetryGate.started) await dependencyRetryGate.settled;
    }
    await expect(dependencyInspector.getByText(
      maliciousDependencyObject.name,
      { exact: true },
    )).toBeVisible({ timeout });
    await expect(dependencyTrigger).toBeFocused();
    await expect(manager.locator("script, img")).toHaveCount(0);
    expect(await page.evaluate(() => Reflect.get(globalThis, "__knowledgeScriptExecuted")))
      .toBeUndefined();
  });

  await test.step("late dependency A completion cannot replace disclosed B", async () => {
    try {
      await dependenciesSection.getByRole("button", {
        name: `Inspect dependency ${dependencyBId}, version 3`,
      }).click();
      await expectNextRelatedGetRequest("held dependency A inspection", {
        knowledgeObjectId: dependencyBId,
        version: 3n,
      });
      await expect(dependencyInspector).toContainText("Loading related object…", { timeout });
      await dependenciesSection.getByRole("button", {
        name: `Inspect dependency ${maliciousDependencyId}, version 1`,
      }).click();
      heldDependencyBReplacementStarted = true;
      await expectNextRelatedGetRequest("dependency A-to-B replacement", {
        knowledgeObjectId: maliciousDependencyId,
        version: 1n,
      });
      await expect(dependencyInspector.getByText(
        maliciousDependencyObject.name,
        { exact: true },
      )).toBeVisible({ timeout });
    } finally {
      heldDependencyB.release?.();
      if (heldDependencyB.started) await heldDependencyB.settled;
    }
  });
  await waitForBrowserRender(page);
  await expect(dependencyInspector.getByText(
    maliciousDependencyObject.name,
    { exact: true },
  )).toBeVisible();
  await expect(dependencyInspector.getByText(dependencyBObject.name, { exact: true }))
    .toHaveCount(0);
  expect(relatedGetRequests).toHaveLength(expectedRelatedGetRequestCount);

  const dependentInspector = dependentsSection.getByRole("region", {
    name: "Dependent object inspector",
  });
  await test.step("each direction owns one responsive inspector without storage or URL state", async () => {
    await dependenciesSection.getByRole("button", {
      name: `Inspect dependency ${dependencyBId}, version 3`,
    }).click();
    await expectNextRelatedGetRequest("dependency replacement", {
      knowledgeObjectId: dependencyBId,
      version: 3n,
    });
    await expect(dependencyInspector.getByText(dependencyBObject.name, { exact: true }))
      .toBeVisible({ timeout });
    await expect(dependenciesSection.locator(".knowledge-manager__related-inspector"))
      .toHaveCount(1);

    await dependentsSection.getByRole("button", {
      name: `Inspect dependent ${maliciousDependentId}, version 3`,
    }).click();
    await expectNextRelatedGetRequest("simultaneous dependent inspection", {
      knowledgeObjectId: maliciousDependentId,
      version: 3n,
    });
    await expect(dependentInspector.getByText(
      maliciousDependentObject.name,
      { exact: true },
    )).toBeVisible({ timeout });
    await expect(manager.locator(".knowledge-manager__related-inspector")).toHaveCount(2);
    await expect(dependenciesSection.locator(".knowledge-manager__related-inspector"))
      .toHaveCount(1);
    await expect(dependentsSection.locator(".knowledge-manager__related-inspector"))
      .toHaveCount(1);
    expect(new Set(await manager.locator(".knowledge-manager__related-inspector").evaluateAll(
      (elements) => elements.map((element) => element.id),
    )).size).toBe(2);
    expect(page.url()).toBe(inspectorURL);
    expect(await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }))).toEqual(inspectorStorage);

    expect((await dependencyInspector.locator("dl").evaluate(
      (element) => getComputedStyle(element).gridTemplateColumns.split(" ").length,
    ))).toBe(2);
    await page.setViewportSize({ width: 375, height: 812 });
    expect((await dependencyInspector.locator("dl").evaluate(
      (element) => getComputedStyle(element).gridTemplateColumns.split(" ").length,
    ))).toBe(1);
    expect(await dependenciesSection.getByRole("button", {
      name: `Close dependency ${dependencyBId}, version 3`,
    }).evaluate((element) => getComputedStyle(element).minHeight)).toBe("42px");
    await page.setViewportSize({ width: 1280, height: 800 });
  });

  await dependenciesSection.getByRole("button", { name: "Load more dependencies" }).click();
  await expect.poll(() => dependencyRequests.length, {
    message: "dependency continuation issued exactly once",
    timeout,
  }).toBe(initialDependencyRequestCount + 1);
  expect(dependencyRequests.at(-1)).toEqual({
    ...initialRelationshipRequest,
    page: { ...initialRelationshipPage, pageToken: dependencyCursor },
  });
  await expect(dependenciesSection.getByRole("list", { name: "Visible direct dependencies" })
    .getByRole("listitem")).toHaveCount(3, { timeout });
  await expect(dependenciesSection.getByText("ko-dependency-z", { exact: true })).toBeVisible();
  await expect(dependenciesSection.getByRole("button", { name: "Load more dependencies" }))
    .toHaveCount(0);
  await expect(dependencyInspector.getByText(dependencyBObject.name, { exact: true }))
    .toBeVisible();
  expect(relatedGetRequests).toHaveLength(expectedRelatedGetRequestCount);

  await dependentsSection.getByRole("button", { name: "Load more dependents" }).click();
  await expect.poll(() => dependentRequests.length, {
    message: "dependent continuation issued exactly once",
    timeout,
  }).toBe(initialDependentRequestCount + 1);
  expect(dependentRequests.at(-1)).toEqual({
    ...initialRelationshipRequest,
    page: { ...initialRelationshipPage, pageToken: dependentCursor },
  });
  await expect(dependentsSection.getByRole("alert")).toContainText(
    "This relationship page cannot be safely continued.",
    { timeout },
  );
  await expect(dependentsSection).toContainText("2 visible · revision 12");
  await expect(dependentsSection.getByText("ko-dependent-z", { exact: true })).toHaveCount(0);
  expect(dependencyRequests).toHaveLength(initialDependencyRequestCount + 1);
  await expect(dependenciesSection).toContainText("3 visible · revision 11");
  await expect(dependenciesSection.getByRole("list", { name: "Visible direct dependencies" })
    .getByRole("listitem")).toHaveCount(3);
  await expect(dependentInspector.getByText(
    maliciousDependentObject.name,
    { exact: true },
  )).toBeVisible();
  expect(relatedGetRequests).toHaveLength(expectedRelatedGetRequestCount);
  await dependentsSection.getByRole("button", { name: "Reload dependents" }).click();
  await expect.poll(() => dependentRequests.length, {
    message: "dependent retry issued exactly one fresh first-page request",
    timeout,
  }).toBe(initialDependentRequestCount + 2);
  expect(dependentRequests.at(-1)).toEqual(initialRelationshipRequest);
  await expect(dependentsSection.getByRole("alert")).toHaveCount(0, { timeout });
  await expect(dependentsSection.getByRole("button", { name: "Load more dependents" }))
    .toBeVisible({ timeout });
  expect(dependencyRequests).toHaveLength(initialDependencyRequestCount + 1);
  await expect(dependenciesSection).toContainText("3 visible · revision 11");
  await expect(dependenciesSection.getByRole("list", { name: "Visible direct dependencies" })
    .getByRole("listitem")).toHaveCount(3);
  await expect(dependentsSection.locator(".knowledge-manager__related-inspector"))
    .toHaveCount(0);
  await expect(dependencyInspector.getByText(dependencyBObject.name, { exact: true }))
    .toBeVisible();
  expect(relatedGetRequests).toHaveLength(expectedRelatedGetRequestCount);

  const relatedGetsBeforeToggle = relatedGetRequests.length;
  await dependenciesSection.getByRole("button", {
    name: `Close dependency ${dependencyBId}, version 3`,
  }).click();
  await expect(dependenciesSection.locator(".knowledge-manager__related-inspector"))
    .toHaveCount(0);
  const collapsedDependencyB = dependenciesSection.getByRole("button", {
    name: `Inspect dependency ${dependencyBId}, version 3`,
  });
  await expect(collapsedDependencyB).toBeFocused();
  expect(relatedGetRequests).toHaveLength(relatedGetsBeforeToggle);
  await collapsedDependencyB.click();
  await expectNextRelatedGetRequest("dependency toggle reopen", {
    knowledgeObjectId: dependencyBId,
    version: 3n,
  });
  await expect(dependencyInspector.getByText(dependencyBObject.name, { exact: true }))
    .toBeVisible({ timeout });
  await dependentsSection.getByRole("button", {
    name: `Inspect dependent ${maliciousDependentId}, version 3`,
  }).click();
  await expectNextRelatedGetRequest("dependent reopen before parent reset", {
    knowledgeObjectId: maliciousDependentId,
    version: 3n,
  });
  await expect(dependentInspector.getByText(
    maliciousDependentObject.name,
    { exact: true },
  )).toBeVisible({ timeout });
  await expect(manager.locator(".knowledge-manager__related-inspector")).toHaveCount(2);

  await manager.getByRole("button", { name: "Close knowledge object details" }).click();
  await expect(manager.locator(".knowledge-manager__related-inspector")).toHaveCount(0);
  await expect(manager.locator(".knowledge-manager__detail")).toHaveCount(0);
  const relatedGetsBeforeParentReopen = relatedGetRequests.length;
  await maliciousRow.press("Enter");
  await expect.poll(() => rootGetRequests.length, {
    message: "parent-detail reopen issued one exact root Get",
    timeout,
  }).toBe(3);
  expect(rootGetRequests.at(-1)).toEqual({
    knowledgeObjectId: "ko-malicious",
    version: 2n,
  });
  await expect(manager.getByRole("heading", { name: maliciousName })).toBeVisible({ timeout });
  await expect(manager.getByText(maliciousDependencyId, { exact: true })).toBeVisible({ timeout });
  await waitForBrowserRender(page);
  expect(relatedGetRequests).toHaveLength(relatedGetsBeforeParentReopen);
  expect(page.url()).toBe(inspectorURL);

  await textFilter.evaluate((element) => {
    const input = element as HTMLInputElement;
    input.removeAttribute("maxlength");
  });
  await textFilter.fill("é".repeat(128));
  await manager.getByRole("button", { name: "Apply filters" }).click();
  await expect(manager.getByText("Knowledge Manager unavailable")).toBeVisible({ timeout });
  await expect(manager.locator(".knowledge-manager__workspace")).toHaveCount(0);
  await expect(manager.getByRole("button", { name: "Retry" })).toHaveCount(0);
  await expect(textFilter).toHaveAttribute("aria-invalid", "true");
  await expect(ownerFilter).not.toHaveAttribute("aria-invalid", "true");
  expect(listRequests).toHaveLength(expectedListRequestCount);

  await ownerFilter.fill("owner-8");
  await waitForBrowserRender(page);
  await expect(textFilter).toHaveAttribute("aria-invalid", "true");
  expect(listRequests).toHaveLength(expectedListRequestCount);
  await ownerFilter.fill("owner-7");
  await textFilter.fill("different unapplied draft");
  await waitForBrowserRender(page);
  await expect(manager.locator("#knowledge-advanced-filter-status"))
    .toContainText("Draft filters not applied.");
  await expect(manager.getByRole("button", { name: "Retry" })).toHaveCount(0);
  expect(listRequests).toHaveLength(expectedListRequestCount);
  await textFilter.fill("latency warning");
  await textFilter.press("Enter");
  await expectNextListRequest("same-tuple fail-closed recovery", committedListRequest);
  await expect(manager.getByText(maliciousName, { exact: true })).toBeVisible({ timeout });

  await manager.getByRole("button", { name: "Clear filters" }).click();
  await expectNextListRequest("Clear", initialListRequest);
  await expect(ownerFilter).toHaveValue("");
  await expect(textFilter).toHaveValue("");
  await expect(sharingFilter).toHaveValue("all");
  await expect(selectorFilter).toHaveValue("");

  await sharingFilter.evaluate((element) => {
    const select = element as HTMLSelectElement;
    select.add(new Option("Forged", "future-sharing"));
  });
  await sharingFilter.selectOption("future-sharing");
  await expect(manager.getByText("Knowledge Manager unavailable")).toBeVisible({ timeout });
  await expect(manager.getByRole("button", { name: "Retry" })).toHaveCount(0);
  await waitForBrowserRender(page);
  expect(listRequests).toHaveLength(expectedListRequestCount);
  await sharingFilter.selectOption("all");
  await manager.getByRole("button", { name: "Apply filters" }).click();
  await expectNextListRequest("forged-sharing recovery", initialListRequest);

  await page.setViewportSize({ width: 375, height: 812 });
  expect((await manager.locator(".knowledge-manager__advanced-filter-grid").evaluate(
    (element) => getComputedStyle(element).gridTemplateColumns.split(" ").length,
  ))).toBe(1);

  await test.step("Tier-1 create validates fail-closed and ignores an aborted stale result", async () => {
    await manager.getByRole("button", { name: "Create knowledge object" }).click();
    const form = manager.locator(".knowledge-manager__mutation-form");
    await expect(form.getByRole("heading", { name: "Create knowledge object" })).toBeVisible();
    await form.getByLabel("Definition type").selectOption("regex-extraction");
    await form.getByLabel("Name").fill("browser_regex_stale");
    await form.getByLabel("Source patterns").fill("source::browser");
    await form.getByLabel("Regex pattern").fill("status=(?<status>[0-9]+)");
    await form.getByLabel(/Output fields/).fill("status");

    await form.getByRole("button", { name: "Validate draft" }).click();
    await expect(form.getByRole("alert")).toContainText(
      "Validation is unavailable. No definition details were accepted.",
      { timeout },
    );
    expect(createRequests).toHaveLength(0);
    await expect(form.getByRole("button", { name: "Create draft" })).toBeDisabled();

    await form.getByRole("button", { name: "Validate draft" }).click();
    await expect.poll(() => staleValidationGate.started, {
      message: "the replacement Validate request reached the held route",
      timeout,
    }).toBe(true);
    await form.getByLabel("Name").fill("browser_regex_final");
    staleValidationGate.release?.();
    await staleValidationGate.settled;
    await waitForBrowserRender(page);
    await expect(form.getByText("Validation passed")).toHaveCount(0);
    await expect(form.getByRole("button", { name: "Create draft" })).toBeDisabled();

    await form.getByRole("button", { name: "Validate draft" }).click();
    await expect(form.getByText("Validation passed")).toBeVisible({ timeout });
    await form.getByRole("button", { name: "Create draft" }).click();
    await expectNextListRequest("Create reload", initialListRequest);
    await expect(manager.getByRole("button", { name: "Create knowledge object" })).toBeVisible();
  });

  await test.step("exact current authority drives disable, masked edit, activate, and delete", async () => {
    await maliciousRow.click();
    await expect(manager.getByRole("heading", { name: maliciousName })).toBeVisible({ timeout });
    await manager.getByRole("button", { name: "Disable" }).click();
    await expectNextListRequest("Disable reload", initialListRequest);

    await maliciousRow.click();
    await expect(manager.getByRole("button", { name: "Activate" })).toBeVisible({ timeout });
    await manager.getByRole("button", { name: "Edit" }).click();
    const editForm = manager.locator(".knowledge-manager__mutation-form");
    await expect(editForm.getByRole("heading", { name: "Edit knowledge object" })).toBeVisible();
    await expect(editForm.getByLabel("Definition type")).toBeDisabled();
    await editForm.getByLabel(/Description/).fill("Updated through the exact browser authority");
    await editForm.getByRole("button", { name: "Validate changes" }).click();
    await expect(editForm.getByText("Validation passed")).toBeVisible({ timeout });
    await editForm.getByRole("button", { name: "Save changes" }).click();
    await expectNextListRequest("Update reload", initialListRequest);

    await maliciousRow.click();
    await manager.getByRole("button", { name: "Activate" }).click();
    await expectNextListRequest("Activate reload", initialListRequest);

    await maliciousRow.click();
    await manager.getByRole("button", { name: "Disable" }).click();
    await expectNextListRequest("second Disable reload", initialListRequest);

    await maliciousRow.click();
    await manager.getByRole("button", { name: "Delete" }).click();
    const confirmation = manager.locator(".knowledge-manager__delete-confirmation");
    await expect(confirmation.getByRole("heading", { name: "Confirm delete" })).toBeVisible();
    await confirmation.getByLabel("Object name").fill("wrong-name");
    await expect(confirmation.getByRole("button", { name: "Delete knowledge object" }))
      .toBeDisabled();
    expect(deleteRequests).toHaveLength(0);
    await confirmation.getByLabel("Object name").fill(maliciousName);
    await confirmation.getByRole("button", { name: "Delete knowledge object" }).click();
    await expectNextListRequest("Delete reload", initialListRequest);
    await expect(manager.getByRole("heading", { name: "No knowledge objects" })).toBeVisible();
  });

  expect(validateRequests).toHaveLength(4);
  expect(validateRequests.slice(0, 3).map((request) => ({
    name: request.definition?.name,
    knowledgeObjectId: request.knowledgeObjectId,
    expectedVersion: request.expectedVersion,
    updateMask: request.updateMask,
    intent: request.intent,
  }))).toEqual([
    {
      name: "browser_regex_stale",
      knowledgeObjectId: undefined,
      expectedVersion: undefined,
      updateMask: undefined,
      intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
    },
    {
      name: "browser_regex_stale",
      knowledgeObjectId: undefined,
      expectedVersion: undefined,
      updateMask: undefined,
      intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
    },
    {
      name: "browser_regex_final",
      knowledgeObjectId: undefined,
      expectedVersion: undefined,
      updateMask: undefined,
      intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
    },
  ]);
  expect(validateRequests[3]).toMatchObject({
    knowledgeObjectId: "ko-malicious",
    expectedVersion: 3n,
    updateMask: ["description"],
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
  });
  expect(createRequests).toHaveLength(1);
  expect(createRequests[0]).toMatchObject({
    initialState: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DRAFT,
  });
  expect(createRequests[0]?.definition?.body?.$case).toBe("fieldExtraction");
  expect(createRequests[0]?.clientRequestId).toMatch(/^browser-[0-9a-f]{32}$/);
  expect(updateRequests).toHaveLength(1);
  expect(updateRequests[0]).toMatchObject({
    knowledgeObjectId: "ko-malicious",
    expectedVersion: 3n,
    updateMask: ["description"],
  });
  expect(updateRequests[0]?.clientRequestId).toMatch(/^browser-[0-9a-f]{32}$/);
  expect(stateRequests.map((request) => ({
    id: request.knowledgeObjectId,
    version: request.expectedVersion,
    state: request.state,
  }))).toEqual([
    {
      id: "ko-malicious",
      version: 2n,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED,
    },
    {
      id: "ko-malicious",
      version: 4n,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
    },
    {
      id: "ko-malicious",
      version: 5n,
      state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_DISABLED,
    },
  ]);
  expect(stateRequests.every((request) => /^browser-[0-9a-f]{32}$/.test(
    request.clientRequestId,
  ))).toBe(true);
  expect(deleteRequests).toHaveLength(1);
  expect(deleteRequests[0]).toMatchObject({
    knowledgeObjectId: "ko-malicious",
    expectedVersion: 6n,
  });
  expect(deleteRequests[0]?.clientRequestId).toMatch(/^browser-[0-9a-f]{32}$/);
  expect(mutationAuthorizations).toEqual(Array.from(
    { length: 11 },
    () => `Bearer ${administratorToken}`,
  ));
  await expect(manager.getByRole("button", { name: /Preview/i })).toHaveCount(0);
  expect(new URL(page.url()).search).toBe("");
  expect(requestedURLs.filter((value) => {
    const pathname = new URL(value).pathname;
    return pathname.startsWith("/api/knowledge/") && !new Set([
      "/api/knowledge/objects/list",
      "/api/knowledge/objects/get",
      "/api/knowledge/objects/dependencies",
      "/api/knowledge/objects/dependents",
    ]).has(pathname);
  }).map((value) => new URL(value).pathname)).toEqual([
    "/api/knowledge/objects/preview",
    "/api/knowledge/objects/validate",
    "/api/knowledge/objects/validate",
    "/api/knowledge/objects/validate",
    "/api/knowledge/objects/create",
    "/api/knowledge/objects/set-state",
    "/api/knowledge/objects/validate",
    "/api/knowledge/objects/update",
    "/api/knowledge/objects/set-state",
    "/api/knowledge/objects/set-state",
    "/api/knowledge/objects/delete",
  ]);
});

test("history Run again delegates persisted intent with source-only rerun provenance", async ({
  page,
}) => {
  const historySearchId = "history-rerun-source-job";
  const deletedHistorySearchId = "history-rerun-deleted-source-job";
  const selectedAppId = "history-rerun-current-app";
  const retainedAppId = "history-rerun-stale-app";
  const indexName = "history-rerun-index";
  const historySPL = `index=${JSON.stringify(indexName)} | eval adjusted=duration_ms+1 | where status IN (500, 503) | table _time message adjusted`;
  const deletedHistorySPL = `index=${JSON.stringify(indexName)} level=WARN | table _time message`;
  const historyTimeRange = {
    earliest: "server-owned-relative-expression",
    latest: "server-owned-latest-expression",
    timezone: "server-owned-timezone",
  };
  const deletedHistoryTimeRange = {
    earliest: "-24h",
    latest: "now",
    timezone: "UTC",
  };
  const protobufHeaders = { "content-type": "application/x-protobuf" };
  const ordinaryValidateRequests: ValidateSearchRequest[] = [];
  const ordinaryCreateRequests: CreateSearchJobRequest[] = [];
  const historyRerunCreateRequests: CreateSearchJobRequest[] = [];
  let historyRerunSourceMissing = false;

  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/system/bootstrap",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(
        GetSystemBootstrapResponse.fromPartial({
          searchWebsocketPath: "/api/search/ws",
          features: [
            ServerFeature.SERVER_FEATURE_SEARCH,
            ServerFeature.SERVER_FEATURE_SEARCH_HISTORY,
          ],
          limits: { maximumPageSize: 15 },
          apps: [{
            appId: selectedAppId,
            slug: "history-rerun-current",
            displayName: "History rerun current app",
            defaultIndexNames: [indexName],
            state: AppState.APP_STATE_ACTIVE,
          }],
          indexes: [{
            indexId: "history-rerun-index-id",
            name: indexName,
            displayName: "History rerun index",
            state: IndexState.INDEX_STATE_ACTIVE,
            ingestionAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
            searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
          }],
          selectedAppId,
          serverTime: new Date("2026-08-04T12:00:00.000Z"),
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/history/list",
    (route) => route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(ListSearchHistoryResponse.encode(
        ListSearchHistoryResponse.fromPartial({
          historyEntries: [{
            searchJobId: historySearchId,
            definition: {
              spl: historySPL,
              timeRange: historyTimeRange,
              appId: retainedAppId,
              indexScope: [indexName],
              preferredResultTab: 0,
              selectedFields: [],
            },
            source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
            effectiveIndexScope: [indexName],
            finalState: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
            producedRows: 1n,
            duration: { seconds: 1n, nanos: 0 },
            createdAt: new Date("2026-08-04T11:59:58.000Z"),
            startedAt: new Date("2026-08-04T11:59:59.000Z"),
            finishedAt: new Date("2026-08-04T12:00:00.000Z"),
          }, {
            searchJobId: deletedHistorySearchId,
            definition: {
              spl: deletedHistorySPL,
              timeRange: deletedHistoryTimeRange,
              appId: retainedAppId,
              indexScope: [indexName],
              preferredResultTab: 0,
              selectedFields: [],
            },
            source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
            effectiveIndexScope: [indexName],
            finalState: SearchJobState.SEARCH_JOB_STATE_COMPLETED,
            producedRows: 1n,
            duration: { seconds: 1n, nanos: 0 },
            createdAt: new Date("2026-08-04T11:59:55.000Z"),
            startedAt: new Date("2026-08-04T11:59:56.000Z"),
            finishedAt: new Date("2026-08-04T11:59:57.000Z"),
          }],
          page: { totalSize: 2n, totalSizeExact: true },
        }),
      ).finish()),
    }),
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/history/delete",
    async (route) => {
      const requestWire = route.request().postDataBuffer();
      if (requestWire === null) throw new Error("history delete request omitted its protobuf body");
      expect(DeleteSearchHistoryEntryRequest.decode(requestWire).searchJobId).toBe(
        deletedHistorySearchId,
      );
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(DeleteSearchHistoryEntryResponse.encode({
          searchJobId: deletedHistorySearchId,
        }).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/validate",
    async (route) => {
      const requestWire = route.request().postDataBuffer();
      if (requestWire === null) throw new Error("history ordinary Validate omitted its protobuf body");
      const request = ValidateSearchRequest.decode(requestWire);
      if (request.definition === undefined) {
        throw new Error("history ordinary Validate omitted its search definition");
      }
      ordinaryValidateRequests.push(request);
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(ValidateSearchResponse.encode(
          ValidateSearchResponse.fromPartial({
            valid: true,
            normalizedSpl: request.definition.spl,
            referencedIndexes: [indexName],
            referencedFields: ["message"],
            predictedResultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
          }),
        ).finish()),
      });
    },
  );
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/create",
    async (route) => {
      const requestWire = route.request().postDataBuffer();
      if (requestWire === null) throw new Error("history rerun create request omitted its protobuf body");
      const request = CreateSearchJobRequest.decode(requestWire);
      const historyRerun = request.source?.origin
        === SearchJobOrigin.SEARCH_JOB_ORIGIN_HISTORY_RERUN;
      if (historyRerun) historyRerunCreateRequests.push(request);
      else ordinaryCreateRequests.push(request);
      if (historyRerun && historyRerunSourceMissing) {
        await route.fulfill({
          status: 404,
          contentType: "application/json",
          body: JSON.stringify({ error: "history entry no longer exists" }),
        });
        return;
      }
      const requestOrdinal = ordinaryCreateRequests.length + historyRerunCreateRequests.length;
      const admittedAt = new Date(Date.parse("2026-08-04T12:00:01.000Z") + requestOrdinal);
      const admittedDefinition = historyRerun
        ? {
          spl: historySPL,
          timeRange: historyTimeRange,
          appId: retainedAppId,
          indexScope: [indexName],
        }
        : request.definition;
      if (admittedDefinition === undefined) {
        throw new Error("ordinary create request omitted its search definition");
      }
      await route.fulfill({
        status: 200,
        headers: protobufHeaders,
        body: Buffer.from(CreateSearchJobResponse.encode(
          CreateSearchJobResponse.fromPartial({
            searchJob: {
              searchJobId: historyRerun
                ? `history-rerun-admitted-job-${historyRerunCreateRequests.length}`
                : `history-rerun-ad-hoc-job-${ordinaryCreateRequests.length}`,
              stateVersion: 1n,
              definition: admittedDefinition,
              source: historyRerun
                ? request.source
                : { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
              effectiveIndexScope: admittedDefinition.indexScope,
              resolvedTimeRange: {
                earliest: new Date("2026-08-04T11:00:01.000Z"),
                latest: admittedAt,
                timezone: "UTC",
              },
              state: SearchJobState.SEARCH_JOB_STATE_CANCELED,
              progress: {
                phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE,
                percentComplete: 100,
                elapsed: { seconds: 0n, nanos: 1_000_000 },
                queueWait: { seconds: 0n, nanos: 0 },
                updatedAt: admittedAt,
                stateVersion: 1n,
              },
              createdAt: admittedAt,
              startedAt: admittedAt,
              finishedAt: admittedAt,
            },
          }),
        ).finish()),
      });
    },
  );

  const launchURL = new URL("/search/", origin);
  launchURL.search = new URLSearchParams({
    q: `index=${JSON.stringify(indexName)}`,
    earliest: "-24h",
    latest: "now",
    timezone: "UTC",
    run: "0",
  }).toString();
  await page.goto(launchURL.href, { waitUntil: "domcontentloaded", timeout });
  await expect(page.getByText("Backend data", { exact: true })).toBeVisible({ timeout });
  await page.getByRole("button", { name: "History", exact: true }).click();
  const deletedHistoryRow = page.getByTestId("history-list").getByRole("row").filter({
    hasText: deletedHistorySPL,
  });
  await expect(deletedHistoryRow).toHaveCount(1, { timeout });
  await deletedHistoryRow.getByRole("button", { name: "Open", exact: true }).click();
  await page.getByRole("button", { name: "History", exact: true }).click();
  const deleteResponse = page.waitForResponse((response) => {
    const responseURL = new URL(response.url());
    return responseURL.origin === origin
      && responseURL.pathname === "/api/search/history/delete"
      && response.status() === 200;
  });
  await page.getByTestId("history-list").getByRole("row").filter({
    hasText: deletedHistorySPL,
  }).getByRole("button", { name: "Delete history entry", exact: true }).click();
  await deleteResponse;
  await expect(page.getByTestId("history-list").getByRole("row").filter({
    hasText: deletedHistorySPL,
  })).toHaveCount(0, { timeout });
  const historyDialogAfterDelete = page.getByRole("dialog", {
    name: "Search history",
    exact: true,
  });
  await expect(historyDialogAfterDelete.getByRole("button", {
    name: "Clear history",
    exact: true,
  })).toBeEnabled({ timeout });
  await historyDialogAfterDelete.getByRole("button", {
    name: "Close dialog",
    exact: true,
  }).click();
  const postDeleteCreateRequest = page.waitForRequest((request) => {
    const requestURL = new URL(request.url());
    return request.method() === "POST"
      && requestURL.origin === origin
      && requestURL.pathname === "/api/search/jobs/create";
  });
  await page.getByTestId("run-search").click();
  await postDeleteCreateRequest;
  await expect(page.getByTestId("job-strip")).toContainText("Canceled", { timeout });
  expect(historyRerunCreateRequests).toHaveLength(0);
  expect(ordinaryValidateRequests).toHaveLength(1);
  expect(ordinaryCreateRequests).toHaveLength(1);
  expect(ordinaryValidateRequests[0]?.definition).toEqual(
    ordinaryCreateRequests[0]?.definition,
  );
  expect(ordinaryCreateRequests[0]?.definition?.spl).toBe(deletedHistorySPL);
  expect(ordinaryCreateRequests[0]?.source).toBeUndefined();

  await page.getByRole("button", { name: "History", exact: true }).click();
  const historyRow = page.getByTestId("history-list").getByRole("row").filter({
    hasText: historySPL,
  });
  await expect(historyRow).toHaveCount(1, { timeout });

  const rerunRequestPromise = page.waitForRequest((request) => {
    const requestURL = new URL(request.url());
    return request.method() === "POST"
      && requestURL.origin === origin
      && requestURL.pathname === "/api/search/jobs/create";
  });
  await historyRow.getByRole("button", { name: "Run again", exact: true }).click();
  await rerunRequestPromise;

  expect(historyRerunCreateRequests).toHaveLength(1);
  expect(ordinaryValidateRequests).toHaveLength(1);
  expect(historyRerunCreateRequests[0]).toEqual({
    definition: undefined,
    source: {
      origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_HISTORY_RERUN,
      savedSearchId: undefined,
      historySearchId,
      dashboardId: undefined,
    },
    options: undefined,
    clientRequestId: undefined,
  });

  await expect(page.getByTestId("job-strip")).toContainText("Canceled", { timeout });
  await page.getByTestId("run-search").click();
  await expect(page.getByText(/The connected server accepts RFC 3339 timestamps/)).toBeVisible({
    timeout,
  });
  expect(historyRerunCreateRequests).toHaveLength(1);
  expect(ordinaryCreateRequests).toHaveLength(1);

  await page.getByRole("button", { name: "History", exact: true }).click();
  const refreshedHistoryRow = page.getByTestId("history-list").getByRole("row").filter({
    hasText: historySPL,
  });
  await expect(refreshedHistoryRow).toHaveCount(1, { timeout });
  historyRerunSourceMissing = true;
  const missingRerunResponse = page.waitForResponse((response) => {
    const responseURL = new URL(response.url());
    return responseURL.origin === origin
      && responseURL.pathname === "/api/search/jobs/create"
      && response.status() === 404;
  });
  await refreshedHistoryRow.getByRole("button", { name: "Run again", exact: true }).click();
  await missingRerunResponse;
  await expect.poll(() => historyRerunCreateRequests.length, { timeout }).toBe(2);
  expect(historyRerunCreateRequests[1]).toEqual(historyRerunCreateRequests[0]);

  await page.getByRole("button", { name: "History", exact: true }).click();
  await expect(page.getByTestId("history-list").getByRole("row").filter({
    hasText: historySPL,
  })).toHaveCount(0, { timeout });
  await page.getByRole("dialog", { name: "Search history", exact: true })
    .getByRole("button", { name: "Close dialog", exact: true }).click();
  await page.getByTestId("run-search").click();
  await expect(page.getByText(/The connected server accepts RFC 3339 timestamps/)).toBeVisible({
    timeout,
  });
  expect(historyRerunCreateRequests).toHaveLength(2);
  expect(ordinaryValidateRequests).toHaveLength(1);
  expect(ordinaryCreateRequests).toHaveLength(1);
});

test("failed search terminal rejects without waiting for results", async () => {
  test.setTimeout(10_000);
  const searchJobID = "browser-controlled-failed-search";
  const subscriptionID = "browser-controlled-failed-subscription";
  const failureMessage = "controlled storage outage";
  const subscribeFrame = SearchWebSocketCommand.encode({
    requestId: "browser-controlled-failed-request",
    payload: {
      $case: "subscribe",
      value: {
        subscriptions: [{
          subscriptionId: subscriptionID,
          target: { target: { $case: "searchJobId", value: searchJobID } },
          afterSequence: 0n,
          includePreviews: true,
          previewRowLimit: undefined,
        }],
      },
    },
  }).finish();
  const terminalFrame = SearchWebSocketEvent.encode({
    sequence: 1n,
    occurredAt: new Date(0),
    subscriptionId: subscriptionID,
    target: { target: { $case: "searchJobId", value: searchJobID } },
    payload: {
      $case: "searchTerminal",
      value: {
        searchJobId: searchJobID,
        state: SearchJobState.SEARCH_JOB_STATE_FAILED,
        stateVersion: 2n,
        finalProgress: undefined,
        failure: {
          code: SearchFailureCode.SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE,
          message: failureMessage,
          retryable: true,
          diagnostics: [],
        },
        resultsExpireAt: undefined,
      },
    },
  }).finish();
  const pageEvents = new EventEmitter();
  const socketEvents = new EventEmitter();
  const controlledSocket = Object.assign(socketEvents, {
    url: () => `${origin.replace(/^http/, "ws")}/api/search/ws`,
  });
  const protocolObservation = observeSearchProtocol(
    pageEvents as unknown as Page,
    origin,
    5_000,
    true,
  );
  // Model the results response as deliberately unavailable. Promise.all must
  // still reject from the failed terminal instead of waiting for this gate.
  const unavailableResultsResponse = new Promise<never>(() => undefined);
  const failedSearch = Promise.all([
    unavailableResultsResponse,
    protocolObservation.waitForJob(searchJobID),
  ]);
  void failedSearch.catch(() => undefined);

  try {
    pageEvents.emit("websocket", controlledSocket as unknown as WebSocket);
    socketEvents.emit("framesent", { payload: Buffer.from(subscribeFrame) });
    socketEvents.emit("framereceived", { payload: Buffer.from(terminalFrame) });
    const failure = await failedSearch.then(
      () => new Error("failed search unexpectedly completed"),
      (error: unknown) => normalizeError(error),
    );
    expect(failure.message).toBe(
      `browser search ${searchJobID} terminated in state ${SearchJobState.SEARCH_JOB_STATE_FAILED}`
      + ` with failure code ${SearchFailureCode.SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE}`
      + `: ${failureMessage}`,
    );
  } finally {
    protocolObservation.dispose();
  }
});

test("renders a fixed 1,000-row statistics result with bounded browser work", async ({
  page,
}, testInfo) => {
  test.skip(
    !renderingTest || !renderingArtifactDirectory || !renderingMetricsPath,
    "the deterministic fixed-result rendering fixture is not enabled",
  );
  test.setTimeout(60_000);
  await mkdir(renderingArtifactDirectory!, { recursive: true });
  await page.setViewportSize({ width: 1_600, height: 900 });
  const renderingViewport = page.viewportSize();
  if (renderingViewport === null) {
    throw new Error("fixed-result rendering viewport is unavailable");
  }
  const safety = observeBrowserSafety(page);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Performance.enable");
  let baselineCDPMetrics: Map<string, number> | undefined;
  let responseBytes = 0;
  let responseSHA256 = "";
  let resultColumnCount = 0;
  let routeFailure: Error | undefined;
  let settleResultsRoute!: () => void;
  const resultsRouteSettled = new Promise<void>((resolve) => {
    settleResultsRoute = resolve;
  });
  const fixedResultsRouteMatcher = (url: URL): boolean =>
    url.origin === origin && url.pathname === "/api/search/jobs/results";
  await page.route(
    fixedResultsRouteMatcher,
    async (route) => {
      try {
        const requestPayload = route.request().postDataBuffer();
        if (requestPayload === null) {
          throw new Error("fixed-result request had no protobuf body");
        }
        const request = GetSearchResultsRequest.decode(requestPayload);
        expect(request.page?.pageSize, "fixed-result request page size").toBe(expectedRows);
        expect(request.page?.includeTotalSize, "fixed-result request exact total").toBe(true);
        expect(request.page?.pageToken ?? "", "fixed-result request page token").toBe("");
        expect(request.allowPartialResults, "fixed-result partial-result request").toBe(false);

        const upstream = await route.fetch();
        if (
          upstream.status() !== 200
          || upstream.headers()["content-type"] !== "application/x-protobuf"
        ) {
          throw new Error("fixed-result route was not a protobuf success");
        }
        const body = await upstream.body();
        const decoded = GetSearchResultsResponse.decode(body);
        const resultPage = decoded.resultPage;
        if (resultPage?.schema === undefined || resultPage.page === undefined) {
          throw new Error("fixed-result response omitted schema or page metadata");
        }
        expect(decoded.searchJobId).toBe(browserRenderingJobID);
        expect(resultPage.rows).toHaveLength(expectedRows);
        const resultColumnNames = resultPage.schema.columns.map(
          (column) => column.fieldName,
        );
        expect(resultColumnNames).toHaveLength(MAXIMUM_BROWSER_RESULT_COLUMNS);
        expect(resultColumnNames.slice(0, 2)).toEqual(["group", "count"]);
        expect(resultColumnNames.at(-1)).toBe("metric_63");
        expect(resultPage.page.totalSize).toBe(BigInt(expectedRows));
        expect(resultPage.page.totalSizeExact).toBe(true);
        expect(resultPage.page.nextPageToken ?? "").toBe("");
        expect(resultPage.snapshotComplete).toBe(true);
        expect(
          resultPage.rows.map((row) => row.ordinal),
          "fixed-result row ordinals",
        ).toEqual(Array.from({ length: expectedRows }, (_, index) => BigInt(index)));
        expect(new Set(resultPage.rows.map((row) => row.rowId)).size).toBe(expectedRows);
        expect(requiredStringCell(resultPage.rows[0], 0)).toBe("render-row-0000");
        expect(requiredStringCell(resultPage.rows.at(-1), 0)).toBe(expectedText);

        responseBytes = body.byteLength;
        responseSHA256 = createHash("sha256").update(body).digest("hex");
        resultColumnCount = resultPage.schema.columns.length;
        baselineCDPMetrics = await readCDPMetrics(cdp);
        await beginFixedResultRenderingObservation(page);
        await route.fulfill({ response: upstream, body });
      } catch (error) {
        routeFailure = normalizeError(error);
        await route.abort("failed").catch(() => undefined);
      } finally {
        settleResultsRoute();
      }
    },
  );

  try {
    await verifySameTaskMaterializedRowPeak(page);
    const runSearch = await openSearchWorkspace(page);
    const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);
    await runSearch.click();

    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    expect(decodeCreateSearchJobID(await createResponse.body())).toBe(browserRenderingJobID);
    await resultsRouteSettled;
    if (routeFailure !== undefined) throw routeFailure;
    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserRenderingJobID);

    const initialStability = await waitForStableDOM(
      page,
      '[data-testid="search-workspace"]',
      [
        '[data-testid="job-strip"][aria-busy="false"]',
        '[aria-label="Backend search statistics"][data-total-rows="1000"]',
        '[data-virtualized="true"]',
      ],
    );
    const renderingObservation = await finishFixedResultRenderingObservation(page);
    const finalCDPMetrics = await readCDPMetrics(cdp);
    if (baselineCDPMetrics === undefined) {
      throw new Error("fixed-result CDP baseline was not captured");
    }
    await beginFixedResultRenderingObservation(page, false);
    const jobStrip = page.getByTestId("job-strip");
    await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
    await expect(jobStrip).toContainText("Completed", { timeout });
    await expect(jobStrip).toContainText("1,000 rows", { timeout });
    const shell = page.getByRole("region", { name: "Scrollable statistics table" });
    await expect(shell).toHaveAttribute("data-virtualized", "true", { timeout });
    const table = page.getByRole("table", { name: "Backend search statistics" });
    await expect(table).toHaveAttribute("data-total-rows", "1000", { timeout });
    await expect(table).toHaveAttribute("aria-rowcount", "1001", { timeout });
    const materializedRows = table.locator("tbody tr:not(.virtual-table-spacer)");
    const spacerRows = table.locator("tbody tr.virtual-table-spacer");
    const tableBodyRows = table.locator("tbody tr");
    const materializedCells = table.locator(
      "thead th, tbody tr:not(.virtual-table-spacer) td",
    );
    await expect.poll(() => materializedRows.count(), { timeout }).toBeGreaterThan(0);
    await expect.poll(
      () => materializedRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumMaterializedRows);
    await expect.poll(
      () => spacerRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumSpacerRows);
    await expect.poll(
      () => tableBodyRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumTableBodyRows);
    await expect.poll(
      () => materializedCells.count(),
      { timeout },
    ).toBeLessThanOrEqual(
      MAXIMUM_BROWSER_RESULT_COLUMNS * (maximumMaterializedRows + 1),
    );
    await expect(
      materializedRows.filter({ hasText: "render-row-0000" }),
    ).toHaveCount(1);
    await expect(
      materializedRows.filter({ hasText: expectedText }),
    ).toHaveCount(0);
    await expect(
      materializedRows.filter({ hasText: "render-row-0000" }),
    ).toHaveAttribute("aria-rowindex", "2");
    await expect(page.getByText("Showing 1–1,000 · 1,000 rows", { exact: true })).toBeVisible();

    const initialMaterializedRows = await materializedRows.count();
    const initialSpacerRows = await spacerRows.count();
    const initialTableBodyRows = await tableBodyRows.count();
    const initialMaterializedCells = await materializedCells.count();
    expect(await table.evaluate((element) => getComputedStyle(element).tableLayout))
      .toBe("fixed");
    const tableScrollWidth = await table.evaluate((element) => element.scrollWidth);
    expect(tableScrollWidth).toBeLessThanOrEqual(MAXIMUM_BROWSER_RESULT_COLUMNS * 168);
    expect(renderingObservation.maximumMaterializedRows)
      .toBeLessThanOrEqual(maximumMaterializedRows);
    expect(renderingObservation.maximumTableBodyRows)
      .toBeLessThanOrEqual(maximumTableBodyRows);
    const topScreenshot = await page.screenshot({
      animations: "disabled",
      path: path.join(renderingArtifactDirectory!, "statistics-top.png"),
    });
    await testInfo.attach("fixed-result-top", {
      body: topScreenshot,
      contentType: "image/png",
    });

    await shell.focus();
    await beginScrollTiming(page);
    await shell.press("End");
    const bottomStability = await waitForStableDOM(
      page,
      '[aria-label="Backend search statistics"]',
      [`[aria-rowindex="1001"]`],
    );
    const bottomStableMilliseconds = await finishScrollTiming(
      page,
      bottomStability.stableAt,
    );
    await expect(
      materializedRows.filter({ hasText: expectedText }),
    ).toHaveCount(1, { timeout });
    await expect(
      materializedRows.filter({ hasText: expectedText }),
    ).toHaveAttribute("aria-rowindex", "1001");
    await expect(
      materializedRows.filter({ hasText: "render-row-0000" }),
    ).toHaveCount(0);
    await expect.poll(
      () => materializedRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumMaterializedRows);
    await expect.poll(
      () => spacerRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumSpacerRows);
    await expect.poll(
      () => tableBodyRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumTableBodyRows);
    const shellBoxAtBottom = await shell.boundingBox();
    const stickyHeaderBox = await table.locator("thead th").first().boundingBox();
    if (shellBoxAtBottom === null || stickyHeaderBox === null) {
      throw new Error("fixed-result table or sticky header has no bounding box");
    }
    expect(Math.abs(stickyHeaderBox.y - shellBoxAtBottom.y)).toBeLessThanOrEqual(2);
    const bottomScreenshot = await page.screenshot({
      animations: "disabled",
      path: path.join(renderingArtifactDirectory!, "statistics-bottom.png"),
    });
    await testInfo.attach("fixed-result-bottom", {
      body: bottomScreenshot,
      contentType: "image/png",
    });

    const ascendingCountSort = page.getByRole("button", {
      name: "Sort by count, ascending",
    });
    await ascendingCountSort.click();
    await expect(
      materializedRows.filter({ hasText: "render-row-0000" }),
    ).toHaveCount(1, { timeout });
    await expect(
      materializedRows.filter({ hasText: "render-row-0000" }),
    ).toHaveAttribute("aria-rowindex", "2");
    await waitForStableDOM(page, '[aria-label="Backend search statistics"]');
    await page.getByRole("button", { name: "Sort by count, descending" }).click();
    await expect(
      materializedRows.filter({ hasText: expectedText }),
    ).toHaveCount(1, { timeout });
    await expect(
      materializedRows.filter({ hasText: expectedText }),
    ).toHaveAttribute("aria-rowindex", "2");
    await waitForStableDOM(page, '[aria-label="Backend search statistics"]');

    await page.getByRole("button", { name: /^Format/ }).click();
    await page.getByRole("menuitemradio", { name: /Standard rows/ }).click();
    await expect(table).toHaveClass(/density-standard/);
    const standardRowHeight = await requiredBoundingHeight(
      materializedRows.filter({ hasText: expectedText }),
    );
    await page.getByRole("button", { name: /^Format/ }).click();
    await page.getByRole("menuitemradio", { name: /Compact rows/ }).click();
    await expect(table).toHaveClass(/density-compact/);
    const compactRowHeight = await requiredBoundingHeight(
      materializedRows.filter({ hasText: expectedText }),
    );
    expect(standardRowHeight).toBeGreaterThan(compactRowHeight);
    await page.setViewportSize({ width: 1_024, height: 768 });
    await waitForStableDOM(page, '[aria-label="Backend search statistics"]');
    const compactViewportShellBox = await shell.boundingBox();
    if (compactViewportShellBox === null) {
      throw new Error("fixed-result table shell has no compact-viewport bounding box");
    }
    expect(compactViewportShellBox.x).toBeGreaterThanOrEqual(0);
    expect(compactViewportShellBox.x + compactViewportShellBox.width).toBeLessThanOrEqual(1_024);
    const compactViewportScreenshot = await page.screenshot({
      animations: "disabled",
      path: path.join(renderingArtifactDirectory!, "statistics-compact-1024x768.png"),
    });
    await testInfo.attach("fixed-result-compact-1024x768", {
      body: compactViewportScreenshot,
      contentType: "image/png",
    });
    await expect.poll(
      () => spacerRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumSpacerRows);
    await expect.poll(
      () => tableBodyRows.count(),
      { timeout },
    ).toBeLessThanOrEqual(maximumTableBodyRows);
    const interactionDOMObservation = await finishFixedResultDOMObservation(page);
    expect(interactionDOMObservation.maximumMaterializedRows)
      .toBeLessThanOrEqual(maximumMaterializedRows);
    expect(interactionDOMObservation.maximumTableBodyRows)
      .toBeLessThanOrEqual(maximumTableBodyRows);
    expect(safety.createRequests(), "fixed-result browser create requests").toBe(1);
    expect(safety.resultsRequests(), "fixed-result browser results requests").toBe(1);
    assertBrowserSafety(safety);

    const metrics = {
      version: 1,
      rowCount: expectedRows,
      columnCount: resultColumnCount,
      responseBytes,
      responseSHA256,
      materializedRows: initialMaterializedRows,
      spacerRows: initialSpacerRows,
      tableBodyRows: initialTableBodyRows,
      materializedCells: initialMaterializedCells,
      tableScrollWidth,
      maximumMaterializedRows: Math.max(
        renderingObservation.maximumMaterializedRows,
        interactionDOMObservation.maximumMaterializedRows,
      ),
      maximumTableBodyRows: Math.max(
        renderingObservation.maximumTableBodyRows,
        interactionDOMObservation.maximumTableBodyRows,
      ),
      stableRenderMilliseconds: renderingObservation.stableRenderMilliseconds,
      firstMutationMilliseconds: renderingObservation.firstMutationMilliseconds,
      bottomStableMilliseconds,
      mutationCallbacks: renderingObservation.mutationCallbacks,
      mutationRecords: renderingObservation.mutationRecords,
      addedNodes: renderingObservation.addedNodes,
      stabilityRetries: initialStability.retries,
      longTasks: renderingObservation.longTasks,
      layoutShifts: renderingObservation.layoutShifts,
      cdp: renderingCDPMetrics(baselineCDPMetrics, finalCDPMetrics),
      browserVersion: page.context().browser()?.version() ?? "unknown",
      viewport: renderingViewport,
      devicePixelRatio: await page.evaluate(() => window.devicePixelRatio),
      hardwareConcurrency: await page.evaluate(() => navigator.hardwareConcurrency),
    };
    assertFiniteMetricNumbers(metrics);
    const encodedMetrics = `${JSON.stringify(metrics, null, 2)}\n`;
    expect(Buffer.byteLength(encodedMetrics), "fixed-result metrics bytes").toBeLessThanOrEqual(64 << 10);
    await writeFile(renderingMetricsPath!, encodedMetrics, { encoding: "utf8" });
    await testInfo.attach("fixed-result-rendering-metrics", {
      body: Buffer.from(encodedMetrics),
      contentType: "application/json",
    });
  } finally {
    await discardFixedResultRenderingObservation(page).catch(() => undefined);
    await page.unroute(fixedResultsRouteMatcher).catch(() => undefined);
    await cdp.detach().catch(() => undefined);
  }
});

test("browser cancellation is authoritative and does not reconnect", async ({ page }) => {
  test.skip(
    !cancellationTest || !recoveryInitialText,
    "the deterministic browser-cancellation fixture is not enabled",
  );
  test.setTimeout(60_000);
  await page.clock.install({ time: new Date(latest) });
  const safety = observeBrowserSafety(page);
  const sockets = observeCancellationSockets(page, origin, timeout);
  let releaseCancellationRequest!: () => void;
  const cancellationRequestGate = new Promise<void>((resolve) => {
    releaseCancellationRequest = resolve;
  });
  let releaseCancellationResponse!: () => void;
  const cancellationResponseGate = new Promise<void>((resolve) => {
    releaseCancellationResponse = resolve;
  });
  let cancellationRequests = 0;
  let fulfilledCancellationResponses = 0;
  const cancellationResponses: CancelSearchJobResponse[] = [];
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/cancel",
    async (route) => {
      cancellationRequests += 1;
      if (cancellationRequests > 1) {
        await route.abort("blockedbyclient");
        return;
      }
      await cancellationRequestGate;
      const upstream = await route.fetch();
      if (
        upstream.status() !== 200
        || upstream.headers()["content-type"] !== "application/x-protobuf"
      ) {
        throw new Error("browser cancellation was not a protobuf success");
      }
      const body = await upstream.body();
      cancellationResponses.push(CancelSearchJobResponse.decode(body));
      await cancellationResponseGate;
      await route.fulfill({ response: upstream, body });
      fulfilledCancellationResponses += 1;
    },
  );
  const runSearch = await openSearchWorkspace(page);
  const createResponsePromise = waitForCreateSearchResponse(page);
  const protocolObservation = observeSearchProtocol(page, origin, timeout);

  try {
    await runSearch.click();
    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const browserSearchJobID = decodeCreateSearchJobID(await createResponse.body());
    await protocolObservation.waitForJob(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toBeVisible({ timeout });
    expect(sockets.connectionCount(), "search WebSockets before cancellation").toBe(1);

    const cancelSearch = page.getByRole("button", { name: "Cancel search" });
    await cancelSearch.click();
    await expect.poll(() => cancellationRequests, { timeout }).toBe(1);
    await sockets.waitForInitialClose();

    // The request is still blocked before it reaches the server, so this close
    // can only be the browser's intentional disposal. A second discrete click
    // exercises the pending-request guard rather than the DOM double-click
    // filter.
    await cancelSearch.click();
    expect(cancellationRequests, "browser cancellation requests while held").toBe(1);
    const jobStrip = page.getByTestId("job-strip");
    const expectCancellationPending = async (): Promise<void> => {
      await expect(jobStrip).toHaveAttribute("aria-busy", "true", { timeout });
      await expect(jobStrip).not.toContainText("Canceled");
      await expect(page.getByText(recoveryInitialText!, { exact: true })).toBeVisible({ timeout });
    };
    await expectCancellationPending();
    expect(fulfilledCancellationResponses, "cancel responses before backend release").toBe(0);

    // The first automatic reconnect would be scheduled after 750 ms. Advancing
    // the browser clock beyond that boundary while the backend is still
    // untouched proves dispose() canceled the reconnect lifecycle.
    await page.clock.runFor(2_000);
    await page.waitForTimeout(100);
    sockets.assertHealthy();

    releaseCancellationRequest();
    await expect.poll(() => cancellationResponses.length, { timeout }).toBe(1);
    const canceledJob = cancellationResponses[0].searchJob;
    expect(canceledJob?.searchJobId, "canceled search job ID").toBe(browserSearchJobID);
    expect(canceledJob?.state, "authoritative cancellation state").toBe(
      SearchJobState.SEARCH_JOB_STATE_CANCELED,
    );
    expect(canceledJob?.stateVersion, "authoritative cancellation revision").toBeGreaterThan(0n);
    expect(canceledJob?.progress?.stateVersion, "authoritative cancellation progress revision").toBe(
      canceledJob?.stateVersion,
    );
    expect(canceledJob?.progress?.phase, "authoritative cancellation phase").toBe(
      SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE,
    );
    expect(canceledJob?.failure, "authoritative cancellation failure").toBeUndefined();
    await expectCancellationPending();
    expect(fulfilledCancellationResponses, "cancel responses before release").toBe(0);

    releaseCancellationResponse();
    await expect.poll(() => fulfilledCancellationResponses, { timeout }).toBe(1);
    await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
    await expect(jobStrip).toContainText("Canceled", { timeout });
    await expect(page.getByTestId("run-search")).toHaveAttribute("aria-label", "Run search", { timeout });
    await expect(page.getByTestId("run-search")).toBeEnabled({ timeout });
    await expect(page.getByTestId("backend-preview-status")).toHaveCount(0);
    await expect(page.locator(".event-row--preview")).toHaveCount(0);

    sockets.assertHealthy();
    expect(cancellationRequests, "browser cancellation requests").toBe(1);
    expect(safety.resultsRequests(), "browser search result requests").toBe(0);
    expect(safety.createRequests(), "browser search create requests").toBe(1);
    assertBrowserSafety(safety);
  } finally {
    releaseCancellationRequest();
    releaseCancellationResponse();
    protocolObservation.dispose();
    sockets.dispose();
  }
});

test("live preview resumes from the exact retained WebSocket sequence", async ({ page }) => {
  test.setTimeout(60_000);
  const safety = observeBrowserSafety(page);
  const replay = await interceptOneRetainedReplay(page, origin, timeout);
  let releaseAuthoritativeRecovery!: () => void;
  const authoritativeRecoveryGate = new Promise<void>((resolve) => {
    releaseAuthoritativeRecovery = resolve;
  });
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/get",
    async (route) => {
      await authoritativeRecoveryGate;
      await route.continue();
    },
  );
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);

  try {
    await runSearch.click();
    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const browserSearchJobID = decodeCreateSearchJobID(await createResponse.body());
    await replay.waitForCheckpoint(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute("data-status", "live", { timeout });
    await expect(page.locator(".event-row--preview").first()).toBeVisible({ timeout });
    await replay.disconnect();
    await replay.waitForTerminalReplay();
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "finalizing",
      { timeout },
    );
    releaseAuthoritativeRecovery();

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
    releaseAuthoritativeRecovery();
    replay.dispose();
  }

  const jobStrip = page.getByTestId("job-strip");
  await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(jobStrip).toContainText("Completed", { timeout });
  await expect(jobStrip).toContainText(`${expectedRows} events`, { timeout });

  const eventList = page.getByTestId("event-list");
  const finalRows = eventList.locator('[data-testid^="event-row-"]:not(.event-row--preview)');
  await expect(finalRows).toHaveCount(expectedRows, { timeout });
  await expect(eventList).toContainText(expectedText, { timeout });
  await expect(eventList.locator(".event-row--preview")).toHaveCount(0);

  const previewStatuses = await collectPreviewStatuses(page);
  expect(previewStatuses, "UI preview status transitions").toContain("live");
  expect(previewStatuses, "UI preview status transitions").toContain("paused");
  expect(
    previewStatuses.filter((status) => status === "resyncing" || status === "finalization-error"),
    `UI preview status transitions: ${JSON.stringify(previewStatuses)}`,
  ).toEqual([]);
  expect(safety.createRequests(), "browser search create requests").toBe(1);
  assertBrowserSafety(safety);
});

test("live preview recovers from real sequence expiration and a transient snapshot failure", async ({ page }) => {
  test.skip(
    !sequenceExpirationTest
      || recoveryControlURL === undefined
      || !recoveryControlToken
      || !recoveryInitialText,
    "the deterministic sequence-expiration fixture is not enabled",
  );
  test.setTimeout(60_000);
  const safety = observeBrowserSafety(page);
  const expiration = await interceptSequenceExpiration(page, origin, timeout);
  let releaseTransientRecoveryFailure!: () => void;
  const transientRecoveryFailureGate = new Promise<void>((resolve) => {
    releaseTransientRecoveryFailure = resolve;
  });
  let releaseStaleRecovery!: () => void;
  const staleRecoveryGate = new Promise<void>((resolve) => {
    releaseStaleRecovery = resolve;
  });
  let releaseFreshRecovery!: () => void;
  const freshRecoveryGate = new Promise<void>((resolve) => {
    releaseFreshRecovery = resolve;
  });
  let releaseDelayedWatchdog!: () => void;
  const delayedWatchdogGate = new Promise<void>((resolve) => {
    releaseDelayedWatchdog = resolve;
  });
  let releasePostWatchdogRecovery!: () => void;
  const postWatchdogRecoveryGate = new Promise<void>((resolve) => {
    releasePostWatchdogRecovery = resolve;
  });
  let authoritativeJobRequests = 0;
  let fulfilledAuthoritativeJobRequests = 0;
  let transientRecoveryFailures = 0;
  let initialJobStateVersion: bigint | undefined;
  const authoritativeSnapshots: GetSearchJobResponse[] = [];
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/get",
    async (route) => {
      authoritativeJobRequests += 1;
      const requestOrdinal = authoritativeJobRequests;
      if (requestOrdinal > 7) {
        await route.abort("blockedbyclient");
        return;
      }
      if (requestOrdinal === 1) {
        await transientRecoveryFailureGate;
        transientRecoveryFailures += 1;
        await route.fulfill({
          status: 503,
          contentType: "application/json",
          body: JSON.stringify({ error: "transient authoritative recovery failure" }),
        });
        return;
      }
      if (requestOrdinal === 2) await staleRecoveryGate;
      if (requestOrdinal === 5) await postWatchdogRecoveryGate;
      const upstream = await route.fetch();
      if (
        upstream.status() !== 200
        || upstream.headers()["content-type"] !== "application/x-protobuf"
      ) {
        throw new Error(`authoritative recovery GET ${requestOrdinal} was not protobuf success`);
      }
      const response = GetSearchJobResponse.decode(await upstream.body());
      if (response.searchJob === undefined) {
        throw new Error(`authoritative recovery GET ${requestOrdinal} returned no search job`);
      }
      if (requestOrdinal === 2) {
        if (initialJobStateVersion === undefined || initialJobStateVersion === 0n) {
          throw new Error("the created search job did not establish a positive state version");
        }
        response.searchJob.stateVersion = 0n;
        if (response.searchJob.progress !== undefined) {
          response.searchJob.progress.stateVersion = 0n;
        }
      }
      authoritativeSnapshots.push(response);
      if (requestOrdinal === 3) await freshRecoveryGate;
      if (requestOrdinal === 4) await delayedWatchdogGate;
      await route.fulfill({
        response: upstream,
        body: Buffer.from(GetSearchJobResponse.encode(response).finish()),
      });
      fulfilledAuthoritativeJobRequests += 1;
    },
  );
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);

  try {
    await runSearch.click();
    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const createdJob = CreateSearchJobResponse.decode(await createResponse.body()).searchJob;
    if (createdJob === undefined || !createdJob.searchJobId.trim()) {
      throw new Error("CreateSearchJobResponse.search_job is empty");
    }
    const browserSearchJobID = createdJob.searchJobId;
    initialJobStateVersion = createdJob.stateVersion;
    expect(initialJobStateVersion, "created search job state version").toBeGreaterThan(0n);
    await expiration.waitForCheckpoint(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toContainText(recoveryInitialText!, { timeout });

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.withheldFrameCount(), { timeout }).toBe(1);
    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.withheldFrameCount(), { timeout }).toBe(2);
    await expiration.disconnect();
    await expiration.waitForResynchronizations(1);

    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
    expect(authoritativeJobRequests, "authoritative GETs before recovery release").toBe(1);

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.heldRecoveryFrameCount(), { timeout }).toBe(1);
    expect(authoritativeJobRequests, "authoritative GET while target update is queued").toBe(1);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    const statusCountBeforeTransientFailure = (await snapshotPreviewStatuses(page)).length;

    releaseTransientRecoveryFailure();
    await expiration.waitForResynchronizations(2);
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(2);
    expect(transientRecoveryFailures, "transient authoritative GET failures").toBe(1);
    expect(authoritativeSnapshots, "snapshots after transient recovery failure").toEqual([]);
    expect(expiration.connectionCount(), "reconnect after transient authoritative failure").toBe(3);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeTransientFailure),
      "preview statuses while the transient failure is retried",
    ).not.toContain("waiting");

    const statusCountBeforeStaleRecovery = (await snapshotPreviewStatuses(page)).length;
    releaseStaleRecovery();
    await expect.poll(
      () => authoritativeSnapshots.length,
      { timeout },
    ).toBeGreaterThanOrEqual(1);
    const staleJob = authoritativeSnapshots[0].searchJob;
    expect(staleJob?.stateVersion, "deliberately stale authoritative state version").toBe(0n);
    expect(staleJob?.progress?.scannedRows, "stale recovery scanned rows").toBe(3n);
    await expiration.waitForResynchronizations(3);
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(3);
    expect(expiration.connectionCount(), "reconnect after stale authoritative snapshot").toBe(4);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeStaleRecovery),
      "preview statuses while the stale snapshot is rejected",
    ).not.toContain("waiting");

    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
    const recoveredJob = authoritativeSnapshots[1].searchJob;
    expect(recoveredJob?.searchJobId, "authoritative recovery job ID").toBe(browserSearchJobID);
    expect(recoveredJob?.stateVersion, "fresh authoritative state version")
      .toBeGreaterThan(createdJob.stateVersion);
    expect(
      recoveredJob?.progress?.scannedRows,
      "authoritative recovery rows captured before the queued live update",
    ).toBe(3n);

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.postRecoveryFrameCount(), { timeout }).toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    expect(
      (await snapshotPreviewStatuses(page)).slice(statusCountBeforeStaleRecovery),
      "preview statuses while fresh authoritative recovery is blocked",
    ).not.toContain("waiting");

    releaseFreshRecovery();
    await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBeGreaterThanOrEqual(2);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "waiting",
      { timeout },
    );
    await expect(page.getByLabel("Job metrics")).toContainText("4 rows", { timeout });

    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(3);
    const delayedWatchdogJob = authoritativeSnapshots[2].searchJob;
    expect(delayedWatchdogJob?.progress?.scannedRows, "delayed watchdog scanned rows").toBe(4n);
    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => expiration.postRecoveryFrameCount(), { timeout }).toBe(2);
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });
    releaseDelayedWatchdog();
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(5);
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });
    releasePostWatchdogRecovery();
    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(4);
    expect(
      authoritativeSnapshots[3].searchJob?.progress?.scannedRows,
      "post-watchdog recovery scanned rows",
    ).toBe(5n);
    await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBeGreaterThanOrEqual(4);

    await sendBrowserRecoveryControl("append");
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toContainText(expectedText, { timeout });
    await sendBrowserRecoveryControl("complete");

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
  } finally {
    releaseTransientRecoveryFailure();
    releaseStaleRecovery();
    releaseFreshRecovery();
    releaseDelayedWatchdog();
    releasePostWatchdogRecovery();
    expiration.dispose();
  }

  const jobStrip = page.getByTestId("job-strip");
  await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
  await expect(jobStrip).toContainText("Completed", { timeout });
  await expect(jobStrip).toContainText("2 rows", { timeout });
  const finalTable = page.getByRole("table", { name: "Backend search statistics" });
  await expect(finalTable.locator("tbody tr")).toHaveCount(expectedRows, { timeout });
  await expect(finalTable).toContainText(expectedText, { timeout });
  await expect(
    page.getByRole("table", { name: "Live preview search statistics" }),
  ).toHaveCount(0);

  const previewStatuses = await collectPreviewStatuses(page);
  expect(previewStatuses, "UI preview status transitions").toContain("live");
  expect(previewStatuses, "UI preview status transitions").toContain("resyncing");
  expect(previewStatuses, "UI preview status transitions").toContain("waiting");
  expect(expiration.connectionCount(), "search WebSocket connections").toBe(4);
  expect(expiration.heldRecoveryFrameCount(), "target frames ignored during held recovery").toBe(1);
  expect(expiration.postRecoveryFrameCount(), "post-recovery target frames").toBeGreaterThan(0);
  expiration.assertHealthy();
  expect(safety.createRequests(), "browser search create requests").toBe(1);
  expect(transientRecoveryFailures, "transient authoritative GET failures").toBe(1);
  expect(authoritativeJobRequests, "bounded authoritative job GETs").toBeLessThanOrEqual(6);
  assertBrowserSafety(safety);
});

for (const sequenceGapScenario of [
  {
    title: "live preview recovers from a real sequence gap",
    enabled: sequenceGapTest,
    mode: "gap",
  },
  {
    title: "live preview recovers through REST-only completion after a real sequence gap",
    enabled: sequenceGapRESTTerminalTest,
    mode: "rest-terminal",
  },
  {
    title: "live progress preserves a REST-first snapshot across retained replay",
    enabled: sequenceGapRESTFirstProgressTest,
    mode: "rest-first-progress",
  },
] as const) {
test(sequenceGapScenario.title, async ({ page }) => {
  test.skip(
    !sequenceGapScenario.enabled
      || recoveryControlURL === undefined
      || !recoveryControlToken
      || !recoveryInitialText,
    "the deterministic sequence-gap fixture is not enabled",
  );
  test.setTimeout(60_000);
  const restOnlyTerminal = sequenceGapScenario.mode === "rest-terminal";
  const restFirstProgress = sequenceGapScenario.mode === "rest-first-progress";
  const staleDuplicateProof = sequenceGapScenario.mode === "gap";
  const safety = observeBrowserSafety(page);
  const gap = await interceptSequenceGap(
    page,
    origin,
    timeout,
    {
      withholdTerminalProjection: restOnlyTerminal,
      injectStaleDuplicates: staleDuplicateProof,
    },
  );
  let releaseAuthoritativeResultsResponse!: () => void;
  const authoritativeResultsResponseGate = new Promise<void>((resolve) => {
    releaseAuthoritativeResultsResponse = resolve;
  });
  let authoritativeResultsRequests = 0;
  const authoritativeResultSnapshots: GetSearchResultsResponse[] = [];
  if (restOnlyTerminal) {
    await page.route(
      (url) => url.origin === origin && url.pathname === "/api/search/jobs/results",
      async (route) => {
        authoritativeResultsRequests += 1;
        if (authoritativeResultsRequests > 1) {
          await route.abort("blockedbyclient");
          return;
        }
        const upstream = await route.fetch();
        if (
          upstream.status() !== 200
          || upstream.headers()["content-type"] !== "application/x-protobuf"
        ) {
          throw new Error("sequence-gap authoritative results were not protobuf success");
        }
        const body = await upstream.body();
        const response = GetSearchResultsResponse.decode(body);
        authoritativeResultSnapshots.push(response);
        await authoritativeResultsResponseGate;
        await route.fulfill({
          response: upstream,
          body,
        });
      },
    );
  }
  let allowFirstAuthoritativeFetch!: () => void;
  const firstAuthoritativeFetchGate = new Promise<void>((resolve) => {
    allowFirstAuthoritativeFetch = resolve;
  });
  let allowSecondAuthoritativeFetch!: () => void;
  const secondAuthoritativeFetchGate = new Promise<void>((resolve) => {
    allowSecondAuthoritativeFetch = resolve;
  });
  let releaseFirstAuthoritativeResponse!: () => void;
  const firstAuthoritativeResponseGate = new Promise<void>((resolve) => {
    releaseFirstAuthoritativeResponse = resolve;
  });
  let releaseSecondAuthoritativeResponse!: () => void;
  const secondAuthoritativeResponseGate = new Promise<void>((resolve) => {
    releaseSecondAuthoritativeResponse = resolve;
  });
  let releaseThirdAuthoritativeResponse!: () => void;
  const thirdAuthoritativeResponseGate = new Promise<void>((resolve) => {
    releaseThirdAuthoritativeResponse = resolve;
  });
  let authoritativeJobRequests = 0;
  let fulfilledAuthoritativeJobRequests = 0;
  const authoritativeSnapshots: GetSearchJobResponse[] = [];
  await page.route(
    (url) => url.origin === origin && url.pathname === "/api/search/jobs/get",
    async (route) => {
      authoritativeJobRequests += 1;
      const requestOrdinal = authoritativeJobRequests;
      if (requestOrdinal > 3) {
        await route.abort("blockedbyclient");
        return;
      }
      if (requestOrdinal === 1) await firstAuthoritativeFetchGate;
      if (requestOrdinal === 2) await secondAuthoritativeFetchGate;
      const upstream = await route.fetch();
      if (
        upstream.status() !== 200
        || upstream.headers()["content-type"] !== "application/x-protobuf"
      ) {
        throw new Error(`sequence-gap authoritative GET ${requestOrdinal} was not protobuf success`);
      }
      const response = GetSearchJobResponse.decode(await upstream.body());
      if (response.searchJob === undefined) {
        throw new Error(`sequence-gap authoritative GET ${requestOrdinal} returned no search job`);
      }
      if (restFirstProgress && requestOrdinal === 1) {
        const replayProgress = gap.replayProgress(2);
        const authoritativeProgress = response.searchJob.progress;
        if (
          authoritativeProgress === undefined
          || authoritativeProgress.stateVersion !== response.searchJob.stateVersion
          || authoritativeProgress.stateVersion !== replayProgress.stateVersion
          || authoritativeProgress.elapsed === undefined
          || authoritativeProgress.updatedAt === undefined
          || replayProgress.elapsed === undefined
          || replayProgress.updatedAt === undefined
        ) {
          throw new Error("REST-first progress snapshot did not match retained replay revision K+2");
        }
        // Exercise equal-version reconciliation against legitimate projection-time
        // drift without changing any stable execution counters.
        authoritativeProgress.elapsed = {
          seconds: replayProgress.elapsed.seconds + 1n,
          nanos: replayProgress.elapsed.nanos,
        };
        authoritativeProgress.updatedAt = new Date(replayProgress.updatedAt.getTime() + 1_000);
      }
      authoritativeSnapshots.push(response);
      if (requestOrdinal === 1) await firstAuthoritativeResponseGate;
      if (requestOrdinal === 2) await secondAuthoritativeResponseGate;
      if (requestOrdinal === 3) await thirdAuthoritativeResponseGate;
      await route.fulfill({
        response: upstream,
        body: Buffer.from(GetSearchJobResponse.encode(response).finish()),
      });
      fulfilledAuthoritativeJobRequests += 1;
    },
  );
  const runSearch = await openSearchWorkspace(page);
  const { createResponsePromise, resultsResponsePromise } = waitForSearchResponses(page);

  try {
    await runSearch.click();
    const createResponse = await createResponsePromise;
    assertProtobufResponse(createResponse);
    const createdJob = CreateSearchJobResponse.decode(await createResponse.body()).searchJob;
    if (createdJob === undefined || !createdJob.searchJobId.trim()) {
      throw new Error("CreateSearchJobResponse.search_job is empty");
    }
    const browserSearchJobID = createdJob.searchJobId;
    await gap.waitForCheckpoint(browserSearchJobID);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "live",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toContainText(recoveryInitialText!, { timeout });

    await sendBrowserRecoveryControl("progress");
    await expect.poll(() => gap.droppedFrameCount(), { timeout }).toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });

    await sendBrowserRecoveryControl("progress");
    await gap.waitForReplayReady();
    allowFirstAuthoritativeFetch();
    await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(1);
    const preReplaySnapshot = authoritativeSnapshots[0].searchJob;
    expect(preReplaySnapshot?.searchJobId, "pre-replay authoritative job ID").toBe(browserSearchJobID);
    expect(preReplaySnapshot?.progress?.scannedRows, "pre-replay authoritative rows").toBe(2n);
    expect(preReplaySnapshot?.progress?.stateVersion, "pre-replay progress revision").toBe(
      preReplaySnapshot?.stateVersion,
    );
    expect(preReplaySnapshot?.progress?.stateVersion, "pre-replay K+2 revision").toBe(
      gap.replayProgress(2).stateVersion,
    );
    expect(fulfilledAuthoritativeJobRequests, "authoritative responses before replay").toBe(0);
    expect(gap.connectionCount(), "sequence-gap WebSocket connections").toBe(2);
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
    await expect(page.getByLabel("Job metrics")).toContainText("0 rows", { timeout });
    await expect(page.locator("body")).toContainText(
      /Live job updates skipped a sequence; resynchronizing from the server/i,
      { timeout },
    );

    if (restFirstProgress) {
      const replayProgressK1 = gap.replayProgress(1);
      const replayProgressK2 = gap.replayProgress(2);
      expect(replayProgressK1.stateVersion, "retained progress revisions").toBeLessThan(
        replayProgressK2.stateVersion,
      );
      expect(preReplaySnapshot?.progress?.updatedAt?.getTime(), "REST projection-time drift").toBe(
        (replayProgressK2.updatedAt?.getTime() ?? 0) + 1_000,
      );
      expect(preReplaySnapshot?.progress?.elapsed?.seconds, "REST elapsed-time drift").toBe(
        (replayProgressK2.elapsed?.seconds ?? 0n) + 1n,
      );

      releaseFirstAuthoritativeResponse();
      await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBe(1);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
      await expect(page.locator("body")).toContainText(
        /resynchronized from the server after a sequence gap/i,
        { timeout },
      );
      expect(authoritativeJobRequests, "REST-first authoritative job GETs").toBe(1);

      await beginJobMetricsObservation(page);
      const firstReplaySequence = gap.releaseNextReplayFrame();
      await gap.waitForReplayFrameReceived(firstReplaySequence);
      await waitForBrowserRender(page);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
      const secondReplaySequence = gap.releaseNextReplayFrame();
      expect(secondReplaySequence, "contiguous replay sequence").toBe(firstReplaySequence + 1n);
      await gap.waitForReplayFrameReceived(secondReplaySequence);
      await waitForBrowserRender(page);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
    } else {
      const firstReplaySequence = gap.releaseNextReplayFrame();
      await expect(page.getByLabel("Job metrics")).toContainText("1 rows", { timeout });
      expect(fulfilledAuthoritativeJobRequests, "responses after replay K+1").toBe(0);
      const secondReplaySequence = gap.releaseNextReplayFrame();
      expect(secondReplaySequence, "contiguous replay sequence").toBe(firstReplaySequence + 1n);
      await expect(page.getByLabel("Job metrics")).toContainText("2 rows", { timeout });
      expect(fulfilledAuthoritativeJobRequests, "responses after replay K+2").toBe(0);
    }

    let staleCheckpointSequence: bigint | undefined;
    let staleCheckpointStatusOffset: number | undefined;
    if (staleDuplicateProof) {
      staleCheckpointStatusOffset = (await snapshotPreviewStatuses(page)).length;
      await beginStaleDuplicateDOMObservation(
        page,
        recoveryInitialText!,
        "resyncing",
        false,
      );
      staleCheckpointSequence = await gap.injectStaleCheckpointDuplicate();
    }
    await sendBrowserRecoveryControl("progress");
    await expect(page.getByLabel("Job metrics")).toContainText("3 rows", { timeout });
    if (staleDuplicateProof) {
      expect(staleCheckpointSequence, "stale preview checkpoint sequence").toBeGreaterThan(0n);
    }
    if (restFirstProgress) {
      const observedMetrics = await finishJobMetricsObservation(page);
      expect(
        observedMetrics.staleOneRowSeen,
        `stale replay K+1 never rendered, even transiently: ${JSON.stringify(observedMetrics.diagnostics)}`,
      ).toBe(false);
      expect(observedMetrics.diagnosticOverflow, "job-metric diagnostic overflow").toBe(0);
      expect(authoritativeJobRequests, "authoritative GETs through live K+3").toBe(1);
      await expect(page.locator("body")).not.toContainText(
        /Live search progress was inconsistent|legacy live progress update could not be ordered/i,
      );
    }
    expect(gap.connectionCount(), "connections after live K+3").toBe(2);
    await sendBrowserRecoveryControl("progress");
    await expect(page.getByLabel("Job metrics")).toContainText("4 rows", { timeout });
    if (staleDuplicateProof) {
      const observation = await finishStaleDuplicateDOMObservation(page);
      const previewStatuses = (await snapshotPreviewStatuses(page))
        .slice(staleCheckpointStatusOffset);
      expect(observation.stalePreviewTextSeen, "stale preview text never resurrected").toBe(false);
      expect(observation.previewTableSeen, "stale preview table never resurrected").toBe(false);
      expect(observation.jobStripSnapshotOverflow, "stale job-strip diagnostic overflow").toBe(0);
      expect(
        observation.unexpectedPreviewStatusSeen,
        `preview status across stale duplicate: ${JSON.stringify(previewStatuses)}`,
      ).toBe(false);
      await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
        "data-status",
        "resyncing",
        { timeout },
      );
      expect(gap.connectionCount(), "connections after stale checkpoint duplicate").toBe(2);
      expect(authoritativeJobRequests, "GETs after stale checkpoint duplicate").toBe(1);
    }
    await sendBrowserRecoveryControl("progress");
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });

    // The original scenarios keep GET 1 at revision K+2 in flight until live
    // progress reaches K+5, proving the progress-revision fence rejects it.
    // The REST-first scenario applied GET 1 at K+2 and waits for the ordinary
    // active-job poll instead.
    releaseFirstAuthoritativeResponse();
    await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(2);
    expect(fulfilledAuthoritativeJobRequests, "stale authoritative responses").toBe(1);
    await expect(page.getByLabel("Job metrics")).toContainText("5 rows", { timeout });
    await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
      "data-status",
      "resyncing",
      { timeout },
    );
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);

    if (!restOnlyTerminal) {
      // Capture GET 2 before completion so the subsequent terminal revision
      // deterministically makes this response stale and requires GET 3.
      allowSecondAuthoritativeFetch();
      await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
      const preTerminalSnapshot = authoritativeSnapshots[1].searchJob;
      expect(preTerminalSnapshot?.searchJobId, "in-flight pre-terminal job ID").toBe(browserSearchJobID);
      expect(preTerminalSnapshot?.state, "in-flight pre-terminal job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_RUNNING,
      );
      expect(preTerminalSnapshot?.progress?.scannedRows, "in-flight pre-terminal rows").toBe(5n);
    }

    await sendBrowserRecoveryControl("complete");
    if (restOnlyTerminal) {
      await gap.waitForTerminalProjectionWithheld();
      expect(
        gap.withheldTerminalProjectionFrameCount(),
        "withheld terminal projection frames",
      ).toBe(3);
      await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
        "data-status",
        "resyncing",
        { timeout },
      );
      await expect(
        page.getByRole("table", { name: "Live preview search statistics" }),
      ).toHaveCount(0);
      await expect(
        page.getByRole("table", { name: "Backend search statistics" }),
      ).toHaveCount(0);
      await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
      await expect(page.getByTestId("job-strip")).toHaveAttribute(
        "aria-busy",
        "true",
        { timeout },
      );
      await expect(page.getByTestId("job-strip")).not.toContainText("Completed");
      expect(fulfilledAuthoritativeJobRequests, "responses before REST terminal").toBe(1);

      allowSecondAuthoritativeFetch();
      await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(2);
      const restTerminalSnapshot = authoritativeSnapshots[1].searchJob;
      expect(restTerminalSnapshot?.searchJobId, "REST terminal job ID").toBe(browserSearchJobID);
      expect(restTerminalSnapshot?.state, "REST terminal job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_COMPLETED,
      );
      expect(restTerminalSnapshot?.progress?.scannedRows, "REST terminal rows").toBe(5n);
      releaseSecondAuthoritativeResponse();

      await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBe(2);
      await expect.poll(() => authoritativeResultSnapshots.length, { timeout }).toBe(1);
      expect(
        authoritativeResultSnapshots[0].searchJobId,
        "gated authoritative results job ID",
      ).toBe(browserSearchJobID);
      expect(
        authoritativeResultSnapshots[0].resultPage?.rows.length,
        "gated authoritative result rows",
      ).toBe(expectedRows);

      await expectZeroRowBackendPreviewFinalizing(page);
      await expect(
        page.getByRole("table", { name: "Backend search statistics" }),
      ).toHaveCount(0);
      await expect(page.getByText(recoveryInitialText!, { exact: true })).toHaveCount(0);
      await expect(page.getByTestId("job-strip")).toHaveAttribute(
        "aria-busy",
        "true",
        { timeout },
      );
      await expect(page.getByTestId("job-strip")).toContainText("Finalizing", { timeout });
      releaseAuthoritativeResultsResponse();
    } else {
      await expectZeroRowBackendPreviewFinalizing(page);
      await expect(page.getByTestId("job-strip")).toContainText("Completed", { timeout });
      expect(fulfilledAuthoritativeJobRequests, "responses before terminal replay proof").toBe(1);
      if (staleDuplicateProof) {
        const staleRunningStatusOffset = (await snapshotPreviewStatuses(page)).length;
        await beginStaleDuplicateDOMObservation(
          page,
          recoveryInitialText!,
          "finalizing",
          true,
        );
        const staleRunningSequence = await gap.injectStaleRunningStateDuplicate();
        await waitForBrowserRender(page);
        const observation = await finishStaleDuplicateDOMObservation(page);
        const previewStatuses = (await snapshotPreviewStatuses(page))
          .slice(staleRunningStatusOffset);
        expect(staleRunningSequence, "stale running-state sequence").toBeGreaterThan(0n);
        expect(observation.stalePreviewTextSeen, "terminal duplicate did not restore preview text")
          .toBe(false);
        expect(observation.previewTableSeen, "terminal duplicate did not restore the preview table")
          .toBe(false);
        expect(observation.jobStripSnapshotOverflow, "terminal job-strip diagnostic overflow")
          .toBe(0);
        expect(
          observation.unexpectedPreviewStatusSeen,
          `preview status across terminal duplicate: ${JSON.stringify(previewStatuses)}`,
        ).toBe(false);
        await expect(page.getByTestId("backend-preview-status")).toHaveAttribute(
          "data-status",
          "finalizing",
          { timeout },
        );
        expect(observation.jobStripSnapshots.length, "terminal-state DOM observations")
          .toBeGreaterThan(0);
        expect(
          observation.terminalPhaseRegressionSeen,
          `terminal-state DOM observations: ${JSON.stringify(observation.jobStripSnapshots)}`,
        ).toBe(false);
        expect(gap.connectionCount(), "connections after stale running-state duplicate").toBe(2);
        expect(authoritativeJobRequests, "GETs after stale running-state duplicate").toBe(2);
      }

      const staleTerminalSnapshot = authoritativeSnapshots[1].searchJob;
      expect(staleTerminalSnapshot?.searchJobId, "in-flight terminal job ID").toBe(browserSearchJobID);
      expect(staleTerminalSnapshot?.state, "in-flight terminal job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_RUNNING,
      );
      expect(staleTerminalSnapshot?.progress?.scannedRows, "in-flight terminal rows").toBe(5n);
      releaseSecondAuthoritativeResponse();

      await expect.poll(() => authoritativeJobRequests, { timeout }).toBe(3);
      await expect.poll(() => authoritativeSnapshots.length, { timeout }).toBe(3);
      const recoveredSnapshot = authoritativeSnapshots[2].searchJob;
      expect(recoveredSnapshot?.searchJobId, "post-gap authoritative job ID").toBe(browserSearchJobID);
      expect(recoveredSnapshot?.state, "post-gap authoritative job state").toBe(
        SearchJobState.SEARCH_JOB_STATE_COMPLETED,
      );
      expect(recoveredSnapshot?.progress?.scannedRows, "post-gap authoritative rows").toBe(5n);
      expect(fulfilledAuthoritativeJobRequests, "responses before final recovery").toBe(2);
      releaseThirdAuthoritativeResponse();
    }

    const resultsResponse = await resultsResponsePromise;
    assertProtobufResponse(resultsResponse);
    expect(decodeSearchResultsJobID(await resultsResponse.body())).toBe(browserSearchJobID);
    await gap.waitForTerminalClose();

    await expect.poll(() => fulfilledAuthoritativeJobRequests, { timeout }).toBe(
      restOnlyTerminal ? 2 : 3,
    );
    const jobStrip = page.getByTestId("job-strip");
    await expect(jobStrip).toHaveAttribute("aria-busy", "false", { timeout });
    await expect(jobStrip).toContainText("Completed", { timeout });
    await expect(jobStrip).toContainText(`${expectedRows} rows`, { timeout });
    const finalTable = page.getByRole("table", { name: "Backend search statistics" });
    await expect(finalTable.locator("tbody tr")).toHaveCount(expectedRows, { timeout });
    await expect(finalTable).toContainText(expectedText, { timeout });
    await expect(
      page.getByRole("table", { name: "Live preview search statistics" }),
    ).toHaveCount(0);
    await expect(page.getByTestId("backend-preview-status")).toHaveCount(0);
    if (!restFirstProgress) {
      await expect(page.locator("body")).toContainText(
        /resynchronized from the server after a sequence gap/i,
        { timeout },
      );
    }

    const previewStatuses = await collectPreviewStatuses(page);
    expect(previewStatuses, "UI preview status transitions").toContain("live");
    expect(previewStatuses, "UI preview status transitions").toContain("resyncing");
    expect(previewStatuses, "UI preview status transitions").toContain("finalizing");
    expect(previewStatuses, "UI preview status transitions").not.toContain("finalization-error");
    expect(gap.connectionCount(), "search WebSocket connections").toBe(2);
    expect(gap.liveFrameCount(), "post-replay live target frames").toBe(
      restOnlyTerminal ? 3 : 6,
    );
    gap.assertHealthy();
    expect(safety.createRequests(), "browser search create requests").toBe(1);
    expect(authoritativeJobRequests, "authoritative job GETs").toBe(
      restOnlyTerminal ? 2 : 3,
    );
    expect(authoritativeResultsRequests, "authoritative results GETs").toBe(
      restOnlyTerminal ? 1 : 0,
    );
    assertBrowserSafety(safety);
  } finally {
    await discardJobMetricsObservation(page).catch(() => undefined);
    await discardStaleDuplicateDOMObservation(page).catch(() => undefined);
    allowFirstAuthoritativeFetch();
    allowSecondAuthoritativeFetch();
    releaseFirstAuthoritativeResponse();
    releaseSecondAuthoritativeResponse();
    releaseThirdAuthoritativeResponse();
    releaseAuthoritativeResultsResponse();
    gap.dispose();
  }
});
}

async function expectZeroRowBackendPreviewFinalizing(page: Page): Promise<void> {
  const status = page.getByTestId("backend-preview-status");
  await expect(status).toHaveAttribute("data-status", "finalizing", { timeout });
  await expect(status).toContainText("Loading the authoritative result snapshot.", { timeout });
  await expect(
    page.getByText("Search complete. Loading authoritative results.", { exact: true }),
  ).toHaveCount(1);
  await expect(
    page.getByRole("table", { name: "Live preview search statistics" }),
  ).toHaveCount(0);
}

async function beginJobMetricsObservation(page: Page): Promise<void> {
  await page.evaluate((maximumRecordedDiagnostics) => {
    const target = document.querySelector('[aria-label="Job metrics"]');
    if (target === null) throw new Error("job metrics are unavailable for replay observation");
    const runtime = (window as BrowserHarnessRuntimeWindow)
      .openSplunkBrowserHarnessRuntime;
    if (runtime === undefined) throw new Error("browser harness runtime is unavailable");
    const diagnostics: string[] = [];
    let diagnosticOverflow = 0;
    let staleOneRowSeen = false;
    const record = (): void => {
      const scannedRows = target.querySelector("strong")?.textContent;
      if (scannedRows === undefined) {
        throw new Error("scanned rows are unavailable for replay observation");
      }
      if (/^(?:≈ )?1 rows$/.test(scannedRows)) staleOneRowSeen = true;
      const diagnostic = runtime.boundedDiagnostic(scannedRows);
      if (diagnostics.at(-1) === diagnostic) return;
      if (diagnostics.length < maximumRecordedDiagnostics) diagnostics.push(diagnostic);
      else if (diagnosticOverflow < Number.MAX_SAFE_INTEGER) diagnosticOverflow += 1;
    };
    record();
    const observer = new MutationObserver(record);
    observer.observe(target, { childList: true, characterData: true, subtree: true });
    const observedWindow = window as JobMetricsObservationWindow;
    if (observedWindow.openSplunkJobMetricsObservation !== undefined) {
      observer.disconnect();
      throw new Error("job metrics replay observation is already active");
    }
    observedWindow.openSplunkJobMetricsObservation = {
      diagnosticOverflow: () => diagnosticOverflow,
      diagnostics,
      observer,
      staleOneRowSeen: () => staleOneRowSeen,
    };
  }, MAXIMUM_RECORDED_DIAGNOSTICS);
}

async function finishJobMetricsObservation(page: Page): Promise<JobMetricsSnapshot> {
  return stopJobMetricsObservation(page, true);
}

async function discardJobMetricsObservation(page: Page): Promise<void> {
  await stopJobMetricsObservation(page, false);
}

async function stopJobMetricsObservation(
  page: Page,
  required: boolean,
): Promise<JobMetricsSnapshot> {
  return page.evaluate((observationRequired) => {
    const observedWindow = window as JobMetricsObservationWindow;
    const observation = observedWindow.openSplunkJobMetricsObservation;
    if (observation === undefined) {
      if (observationRequired) {
        throw new Error("job metrics replay observation was not active");
      }
      return { diagnosticOverflow: 0, diagnostics: [], staleOneRowSeen: false };
    }
    observation.observer.disconnect();
    delete observedWindow.openSplunkJobMetricsObservation;
    return {
      diagnosticOverflow: observation.diagnosticOverflow(),
      diagnostics: [...observation.diagnostics],
      staleOneRowSeen: observation.staleOneRowSeen(),
    };
  }, required);
}

async function waitForBrowserRender(page: Page): Promise<void> {
  await page.evaluate(
    () => new Promise<void>((resolve) => {
      setTimeout(() => {
        requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
      }, 0);
    }),
  );
}

interface FixedResultRenderingObservation {
  collectPerformanceMetrics: boolean;
  startedAt: number;
  stableAt: number | null;
  firstMutationAt: number | null;
  mutationCallbacks: number;
  mutationRecords: number;
  addedNodes: number;
  maximumMaterializedRows: number;
  maximumTableBodyRows: number;
  materializedTables: Set<Element>;
  materializedTableBodies: Set<Element>;
  tableBodyRows: Set<Element>;
  materializedRows: Set<Element>;
  longTaskEntries: Array<{ startTime: number; duration: number }>;
  longTaskOverflow: number;
  layoutShiftEntries: Array<{ startTime: number; value: number }>;
  layoutShiftOverflow: number;
  layoutShiftSupported: boolean;
  mutationObserver: MutationObserver | null;
  longTaskObserver: PerformanceObserver | null;
  layoutShiftObserver: PerformanceObserver | null;
  processMutationRecords(records: MutationRecord[]): void;
  processLongTaskEntries(entries: PerformanceEntry[]): void;
  processLayoutShiftEntries(entries: PerformanceEntry[]): void;
}

interface FixedResultRenderingSnapshot {
  firstMutationMilliseconds: number | null;
  stableRenderMilliseconds: number;
  mutationCallbacks: number;
  mutationRecords: number;
  addedNodes: number;
  maximumMaterializedRows: number;
  maximumTableBodyRows: number;
  longTasks: {
    supported: boolean;
    count: number;
    totalMilliseconds: number;
    maximumMilliseconds: number;
    overflow: number;
  };
  layoutShifts: {
    supported: boolean;
    count: number;
    cumulativeValue: number;
    overflow: number;
  };
}

interface FixedResultDOMSnapshot {
  maximumMaterializedRows: number;
  maximumTableBodyRows: number;
}

type FixedResultRenderingWindow = Window & {
  openSplunkFixedResultRendering?: FixedResultRenderingObservation;
};

type ScrollTimingWindow = Window & {
  openSplunkScrollStartedAt?: number;
};

async function beginFixedResultRenderingObservation(
  page: Page,
  collectPerformanceMetrics = true,
): Promise<void> {
  await page.evaluate((collectMetrics) => {
    const observedWindow = window as FixedResultRenderingWindow;
    if (observedWindow.openSplunkFixedResultRendering !== undefined) {
      throw new Error("fixed-result rendering observation is already active");
    }
    const root = document.querySelector<HTMLElement>('[data-testid="search-workspace"]');
    if (root === null) throw new Error("search workspace is unavailable for rendering observation");
    const supportedEntryTypes = new Set(PerformanceObserver.supportedEntryTypes ?? []);
    if (collectMetrics) {
      performance.clearResourceTimings();
      performance.setResourceTimingBufferSize(1_024);
    }
    const statisticsTableSelector = 'table[aria-label="Backend search statistics"]';
    const tableBodyRowSelector = "tr";
    const materializedRowSelector = "tr:not(.virtual-table-spacer)";
    const observation: FixedResultRenderingObservation = {
      collectPerformanceMetrics: collectMetrics,
      startedAt: performance.now(),
      stableAt: null,
      firstMutationAt: null,
      mutationCallbacks: 0,
      mutationRecords: 0,
      addedNodes: 0,
      maximumMaterializedRows: 0,
      maximumTableBodyRows: 0,
      materializedTables: new Set(),
      materializedTableBodies: new Set(),
      tableBodyRows: new Set(),
      materializedRows: new Set(),
      longTaskEntries: [],
      longTaskOverflow: 0,
      layoutShiftEntries: [],
      layoutShiftOverflow: 0,
      layoutShiftSupported: collectMetrics && supportedEntryTypes.has("layout-shift"),
      mutationObserver: null,
      longTaskObserver: null,
      layoutShiftObserver: null,
      processMutationRecords: () => undefined,
      processLongTaskEntries: () => undefined,
      processLayoutShiftEntries: () => undefined,
    };
    // This helper must be defined inside the evaluated browser realm.
    // oxlint-disable-next-line unicorn/consistent-function-scoping
    const matchingElements = (node: Node, selector: string): Element[] => {
      if (!(node instanceof Element)) return [];
      return [
        ...(node.matches(selector) ? [node] : []),
        ...node.querySelectorAll(selector),
      ];
    };
    const addMaterializedRow = (row: Element): void => {
      observation.materializedRows.add(row);
      observation.maximumMaterializedRows = Math.max(
        observation.maximumMaterializedRows,
        observation.materializedRows.size,
      );
    };
    const addTableBodyRow = (row: Element): void => {
      observation.tableBodyRows.add(row);
      observation.maximumTableBodyRows = Math.max(
        observation.maximumTableBodyRows,
        observation.tableBodyRows.size,
      );
      if (row.matches(materializedRowSelector)) addMaterializedRow(row);
    };
    const addRows = (node: Node): void => {
      for (const row of matchingElements(node, tableBodyRowSelector)) {
        addTableBodyRow(row);
      }
    };
    const removeRows = (node: Node): void => {
      for (const row of matchingElements(node, tableBodyRowSelector)) {
        observation.tableBodyRows.delete(row);
        observation.materializedRows.delete(row);
      }
    };
    const targetBelongsToTrackedTable = (target: Node): boolean =>
      [...observation.materializedTables].some(
        (table) => table === target || table.contains(target),
      );
    const targetBelongsToTrackedTableBody = (target: Node): boolean =>
      [...observation.materializedTableBodies].some(
        (body) => body === target || body.contains(target),
      );
    const sampleMaterializedRows = (): void => {
      observation.materializedTables = new Set(
        root.querySelectorAll(statisticsTableSelector),
      );
      observation.materializedTableBodies = new Set(
        root.querySelectorAll(`${statisticsTableSelector} tbody`),
      );
      observation.tableBodyRows = new Set(
        root.querySelectorAll(`${statisticsTableSelector} tbody tr`),
      );
      observation.materializedRows = new Set(
        root.querySelectorAll(
          `${statisticsTableSelector} tbody ${materializedRowSelector}`,
        ),
      );
      observation.maximumMaterializedRows = Math.max(
        observation.maximumMaterializedRows,
        observation.materializedRows.size,
      );
      observation.maximumTableBodyRows = Math.max(
        observation.maximumTableBodyRows,
        observation.tableBodyRows.size,
      );
    };
    observation.processMutationRecords = (records) => {
      const now = performance.now();
      observation.firstMutationAt ??= now;
      observation.mutationCallbacks += 1;
      observation.mutationRecords += records.length;
      for (const record of records) {
        observation.addedNodes += record.addedNodes.length;
        const targetWasInTrackedTable = targetBelongsToTrackedTable(record.target);
        const targetWasInTrackedTableBody =
          targetBelongsToTrackedTableBody(record.target);
        for (const removedNode of record.removedNodes) {
          if (targetWasInTrackedTableBody) removeRows(removedNode);
          for (const body of observation.materializedTableBodies) {
            if (body !== removedNode && !removedNode.contains(body)) continue;
            removeRows(body);
            observation.materializedTableBodies.delete(body);
          }
          for (const table of observation.materializedTables) {
            if (table !== removedNode && !removedNode.contains(table)) continue;
            observation.materializedTables.delete(table);
          }
        }
        for (const addedNode of record.addedNodes) {
          for (const table of matchingElements(addedNode, statisticsTableSelector)) {
            observation.materializedTables.add(table);
            for (const body of table.querySelectorAll("tbody")) {
              observation.materializedTableBodies.add(body);
              addRows(body);
            }
          }
          if (targetWasInTrackedTable) {
            for (const body of matchingElements(addedNode, "tbody")) {
              observation.materializedTableBodies.add(body);
              addRows(body);
            }
          }
          if (targetWasInTrackedTableBody) addRows(addedNode);
        }
        if (record.type === "attributes" && record.target instanceof Element) {
          if (record.target.matches(tableBodyRowSelector)) {
            if (targetBelongsToTrackedTableBody(record.target)) {
              addTableBodyRow(record.target);
            } else {
              observation.tableBodyRows.delete(record.target);
              observation.materializedRows.delete(record.target);
            }
          }
        }
      }
      sampleMaterializedRows();
    };
    observation.mutationObserver = new MutationObserver(
      observation.processMutationRecords,
    );
    observation.mutationObserver.observe(root, {
      attributes: true,
      attributeFilter: ["aria-busy", "aria-rowcount", "class", "data-total-rows", "data-virtualized"],
      characterData: true,
      childList: true,
      subtree: true,
    });
    observation.processLongTaskEntries = (entries) => {
      for (const entry of entries) {
        if (entry.startTime < observation.startedAt) continue;
        if (observation.longTaskEntries.length < 256) {
          observation.longTaskEntries.push({
            startTime: entry.startTime,
            duration: entry.duration,
          });
        } else {
          observation.longTaskOverflow += 1;
        }
      }
    };
    if (collectMetrics && supportedEntryTypes.has("longtask")) {
      observation.longTaskObserver = new PerformanceObserver((entries) => {
        observation.processLongTaskEntries(entries.getEntries());
      });
      observation.longTaskObserver.observe({ type: "longtask", buffered: true });
    }
    observation.processLayoutShiftEntries = (entries) => {
      for (const entry of entries) {
        if (entry.startTime < observation.startedAt) continue;
        const layoutShift = entry as PerformanceEntry & {
          hadRecentInput?: boolean;
          value?: number;
        };
        if (layoutShift.hadRecentInput !== true && typeof layoutShift.value === "number") {
          if (observation.layoutShiftEntries.length < 256) {
            observation.layoutShiftEntries.push({
              startTime: entry.startTime,
              value: layoutShift.value,
            });
          } else {
            observation.layoutShiftOverflow += 1;
          }
        }
      }
    };
    if (observation.layoutShiftSupported) {
      observation.layoutShiftObserver = new PerformanceObserver((entries) => {
        observation.processLayoutShiftEntries(entries.getEntries());
      });
      observation.layoutShiftObserver.observe({ type: "layout-shift", buffered: true });
    }
    observedWindow.openSplunkFixedResultRendering = observation;
    sampleMaterializedRows();
  }, collectPerformanceMetrics);
}

async function verifySameTaskMaterializedRowPeak(page: Page): Promise<void> {
  const proofPage = await page.context().newPage();
  try {
    await proofPage.setContent(`
      <main data-testid="search-workspace">
        <table data-testid="materialized-row-peak-fixture"
               aria-label="Backend search statistics"><tbody></tbody></table>
      </main>
    `);
    await beginFixedResultRenderingObservation(proofPage, false);
    try {
      const peaks = await exerciseSameTaskRowPeak(proofPage, false);
      expect(peaks.materialized, "same-task materialized-row peak").toBe(1_000);
      expect(peaks.tableBody, "same-task table-body-row peak").toBe(1_000);
    } finally {
      await discardFixedResultRenderingObservation(proofPage);
    }
    await proofPage.locator("tbody").evaluate((body) => body.replaceChildren());
    await beginFixedResultRenderingObservation(proofPage, false);
    try {
      const peaks = await exerciseSameTaskRowPeak(proofPage, true);
      expect(peaks.materialized, "same-task spacer materialized-row peak").toBe(0);
      expect(peaks.tableBody, "same-task spacer table-body-row peak").toBe(1_000);
    } finally {
      await discardFixedResultRenderingObservation(proofPage);
    }
  } finally {
    await proofPage.close();
  }
}

async function exerciseSameTaskRowPeak(
  page: Page,
  spacerRows: boolean,
): Promise<{ materialized: number; tableBody: number }> {
  return page.evaluate((useSpacerRows) => {
    const table = document.querySelector<HTMLTableElement>(
      '[data-testid="materialized-row-peak-fixture"]',
    );
    const body = table?.tBodies.item(0);
    const observation =
      (window as FixedResultRenderingWindow).openSplunkFixedResultRendering;
    if (body === undefined || body === null || observation === undefined) {
      throw new Error("materialized-row peak fixture is unavailable");
    }
    const rows = Array.from({ length: 1_000 }, () => {
      const row = document.createElement("tr");
      if (useSpacerRows) row.className = "virtual-table-spacer";
      row.append(document.createElement("td"));
      return row;
    });
    body.append(...rows);
    for (let index = 0; index < 984; index += 1) rows[index]?.remove();
    const pendingRecords = observation.mutationObserver?.takeRecords() ?? [];
    observation.processMutationRecords(pendingRecords);
    return {
      materialized: observation.maximumMaterializedRows,
      tableBody: observation.maximumTableBodyRows,
    };
  }, spacerRows);
}

async function finishFixedResultDOMObservation(
  page: Page,
): Promise<FixedResultDOMSnapshot> {
  return page.evaluate(() => {
    const observedWindow = window as FixedResultRenderingWindow;
    const observation = observedWindow.openSplunkFixedResultRendering;
    if (observation === undefined || observation.collectPerformanceMetrics) {
      throw new Error("fixed-result interaction DOM observation is not active");
    }
    const pendingMutations = observation.mutationObserver?.takeRecords() ?? [];
    if (pendingMutations.length > 0) {
      observation.processMutationRecords(pendingMutations);
    }
    observation.mutationObserver?.disconnect();
    observation.longTaskObserver?.disconnect();
    observation.layoutShiftObserver?.disconnect();
    delete observedWindow.openSplunkFixedResultRendering;
    return {
      maximumMaterializedRows: observation.maximumMaterializedRows,
      maximumTableBodyRows: observation.maximumTableBodyRows,
    };
  });
}

async function finishFixedResultRenderingObservation(
  page: Page,
): Promise<FixedResultRenderingSnapshot> {
  const snapshot = await stopFixedResultRenderingObservation(page, true);
  if (snapshot === null) {
    throw new Error("fixed-result rendering observation is not active");
  }
  return snapshot;
}

async function discardFixedResultRenderingObservation(page: Page): Promise<void> {
  await stopFixedResultRenderingObservation(page, false);
}

async function stopFixedResultRenderingObservation(
  page: Page,
  required: boolean,
): Promise<FixedResultRenderingSnapshot | null> {
  return page.evaluate((observationRequired) => {
    const observedWindow = window as FixedResultRenderingWindow;
    const observation = observedWindow.openSplunkFixedResultRendering;
    if (observation === undefined) {
      if (observationRequired) {
        throw new Error("fixed-result rendering observation is not active");
      }
      return null;
    }
    const pendingMutations = observation.mutationObserver?.takeRecords() ?? [];
    if (pendingMutations.length > 0) {
      observation.processMutationRecords(pendingMutations);
    }
    observation.processLongTaskEntries(observation.longTaskObserver?.takeRecords() ?? []);
    observation.processLayoutShiftEntries(
      observation.layoutShiftObserver?.takeRecords() ?? [],
    );
    observation.mutationObserver?.disconnect();
    observation.longTaskObserver?.disconnect();
    observation.layoutShiftObserver?.disconnect();
    delete observedWindow.openSplunkFixedResultRendering;
    if (!observationRequired) return null;
    if (!observation.collectPerformanceMetrics) {
      throw new Error("fixed-result rendering metrics observation is not active");
    }
    if (observation.stableAt === null) {
      throw new Error("fixed-result rendering observation stopped before DOM stability");
    }
    const resultResource = performance.getEntriesByType("resource")
      .findLast((entry) => {
        try {
          return new URL(entry.name).pathname === "/api/search/jobs/results";
        } catch {
          return false;
        }
      }) as PerformanceResourceTiming | undefined;
    const responseEnd = resultResource?.responseEnd;
    if (
      responseEnd === undefined
      || !Number.isFinite(responseEnd)
      || responseEnd <= 0
      || responseEnd > observation.stableAt
    ) {
      throw new Error("fixed-result resource responseEnd is unavailable or invalid");
    }
    const longTasks = observation.longTaskEntries.filter(
      (entry) => entry.startTime >= responseEnd && entry.startTime <= observation.stableAt!,
    );
    const layoutShifts = observation.layoutShiftEntries.filter(
      (entry) => entry.startTime >= responseEnd && entry.startTime <= observation.stableAt!,
    );
    return {
      firstMutationMilliseconds: observation.firstMutationAt === null
        ? null
        : Math.max(0, observation.firstMutationAt - responseEnd),
      stableRenderMilliseconds: observation.stableAt - responseEnd,
      mutationCallbacks: observation.mutationCallbacks,
      mutationRecords: observation.mutationRecords,
      addedNodes: observation.addedNodes,
      maximumMaterializedRows: observation.maximumMaterializedRows,
      maximumTableBodyRows: observation.maximumTableBodyRows,
      longTasks: {
        supported: observation.longTaskObserver !== null,
        count: longTasks.length,
        totalMilliseconds: longTasks.reduce((total, entry) => total + entry.duration, 0),
        maximumMilliseconds: longTasks.reduce(
          (maximum, entry) => Math.max(maximum, entry.duration),
          0,
        ),
        overflow: observation.longTaskOverflow,
      },
      layoutShifts: {
        supported: observation.layoutShiftSupported,
        count: layoutShifts.length,
        cumulativeValue: layoutShifts.reduce((total, entry) => total + entry.value, 0),
        overflow: observation.layoutShiftOverflow,
      },
    };
  }, required);
}

interface StableDOMResult {
  retries: number;
  stableAt: number;
}

async function waitForStableDOM(
  page: Page,
  selector: string,
  readySelectors: string[] = [],
): Promise<StableDOMResult> {
  return page.evaluate(async ({
    requiredSelectors,
    targetSelector,
    timeoutMilliseconds,
  }) => {
    const target = document.querySelector(targetSelector);
    if (target === null) throw new Error(`stable DOM target is missing: ${targetSelector}`);
    let generation = 0;
    const observer = new MutationObserver(() => {
      generation += 1;
    });
    observer.observe(target, {
      attributes: true,
      characterData: true,
      childList: true,
      subtree: true,
    });
    try {
      const deadline = performance.now() + timeoutMilliseconds;
      for (let retry = 0; performance.now() <= deadline; retry += 1) {
        const readyBefore = requiredSelectors.every(
          (requiredSelector) => document.querySelector(requiredSelector) !== null,
        );
        const before = generation;
        // Stability retries intentionally observe consecutive event-loop turns.
        // oxlint-disable-next-line eslint/no-await-in-loop
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        // oxlint-disable-next-line eslint/no-await-in-loop
        await new Promise<void>((resolve) => {
          requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
        });
        const readyAfter = requiredSelectors.every(
          (requiredSelector) => document.querySelector(requiredSelector) !== null,
        );
        if (readyBefore && readyAfter && generation === before) {
          const stableAt = performance.now();
          const renderingObservation =
            (window as FixedResultRenderingWindow).openSplunkFixedResultRendering;
          if (renderingObservation !== undefined && renderingObservation.stableAt === null) {
            renderingObservation.stableAt = stableAt;
          }
          return { retries: retry, stableAt };
        }
      }
      throw new Error(`DOM did not become stable across two animation frames: ${targetSelector}`);
    } finally {
      observer.disconnect();
    }
  }, {
    requiredSelectors: readySelectors,
    targetSelector: selector,
    timeoutMilliseconds: timeout,
  });
}

async function beginScrollTiming(page: Page): Promise<void> {
  await page.evaluate(() => {
    const timingWindow = window as ScrollTimingWindow;
    delete timingWindow.openSplunkScrollStartedAt;
    const shell = document.querySelector<HTMLElement>(
      '[aria-label="Scrollable statistics table"]',
    );
    if (shell === null) throw new Error("statistics table shell is unavailable for scroll timing");
    shell.addEventListener("scroll", () => {
      timingWindow.openSplunkScrollStartedAt ??= performance.now();
    }, { once: true });
  });
}

async function finishScrollTiming(page: Page, stableAt: number): Promise<number> {
  return page.evaluate((renderingStableAt) => {
    const timingWindow = window as ScrollTimingWindow;
    const startedAt = timingWindow.openSplunkScrollStartedAt;
    delete timingWindow.openSplunkScrollStartedAt;
    if (
      startedAt === undefined
      || !Number.isFinite(startedAt)
      || !Number.isFinite(renderingStableAt)
      || renderingStableAt < startedAt
    ) {
      throw new Error("statistics bottom-scroll timing is unavailable or invalid");
    }
    return renderingStableAt - startedAt;
  }, stableAt);
}

function requiredStringCell(row: ResultRow | undefined, cellIndex: number): string {
  const cell = row?.cells[cellIndex]?.kind;
  if (cell?.$case !== "stringValue") {
    throw new Error(`fixed-result row cell ${cellIndex} is not a string`);
  }
  return cell.value;
}

async function requiredBoundingHeight(locator: Locator): Promise<number> {
  await expect(locator).toHaveCount(1, { timeout });
  const box = await locator.boundingBox();
  if (box === null || !Number.isFinite(box.height) || box.height <= 0) {
    throw new Error("fixed-result row has no positive finite bounding height");
  }
  return box.height;
}

async function readCDPMetrics(session: CDPSession): Promise<Map<string, number>> {
  const response = await session.send("Performance.getMetrics") as {
    metrics: Array<{ name: string; value: number }>;
  };
  return new Map(response.metrics.map((metric) => [metric.name, metric.value]));
}

function renderingCDPMetrics(
  baseline: Map<string, number>,
  final: Map<string, number>,
): Record<string, number> {
  const absolute = (name: string): number => final.get(name) ?? 0;
  const delta = (name: string): number =>
    Math.max(0, absolute(name) - (baseline.get(name) ?? 0));
  return {
    taskDuration: delta("TaskDuration"),
    scriptDuration: delta("ScriptDuration"),
    layoutDuration: delta("LayoutDuration"),
    recalcStyleDuration: delta("RecalcStyleDuration"),
    layoutCount: delta("LayoutCount"),
    recalcStyleCount: delta("RecalcStyleCount"),
    nodes: absolute("Nodes"),
    documents: absolute("Documents"),
    eventListeners: absolute("JSEventListeners"),
    jsHeapUsedBytes: absolute("JSHeapUsedSize"),
    jsHeapTotalBytes: absolute("JSHeapTotalSize"),
  };
}

function assertFiniteMetricNumbers(value: unknown, metricPath = "metrics"): void {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`${metricPath} is not finite`);
    return;
  }
  if (value === null || typeof value !== "object") return;
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      assertFiniteMetricNumbers(item, `${metricPath}[${index}]`),
    );
    return;
  }
  for (const [key, item] of Object.entries(value)) {
    assertFiniteMetricNumbers(item, `${metricPath}.${key}`);
  }
}

async function beginStaleDuplicateDOMObservation(
  page: Page,
  stalePreviewText: string,
  expectedPreviewStatus: string,
  requireTerminalJobPhase: boolean,
): Promise<void> {
  await page.evaluate(({
    expectedStalePreviewText,
    expectedStatus,
    maximumRecordedDiagnostics,
    terminalJobPhaseRequired,
  }) => {
    const observedWindow = window as StaleDuplicateDOMObservationWindow;
    if (observedWindow.openSplunkStaleDuplicateDOMObservation !== undefined) {
      throw new Error("stale-duplicate DOM observation is already active");
    }
    const root = document.querySelector<HTMLElement>('[data-testid="search-workspace"]');
    if (root === null) throw new Error("search workspace is unavailable for DOM observation");
    const runtime = (window as BrowserHarnessRuntimeWindow)
      .openSplunkBrowserHarnessRuntime;
    if (runtime === undefined) throw new Error("browser harness runtime is unavailable");
    let observation: StaleDuplicateDOMObservation;
    const record = (): void => {
      if (!observation.stalePreviewTextSeen) {
        for (const row of root.querySelectorAll(".event-row--preview")) {
          if (!row.textContent?.includes(expectedStalePreviewText)) continue;
          observation.stalePreviewTextSeen = true;
          break;
        }
      }
      if (root.querySelector('[aria-label="Live preview search statistics"]') !== null) {
        observation.previewTableSeen = true;
      }
      const previewStatus = root.querySelector<HTMLElement>(
        '[data-testid="backend-preview-status"]',
      )?.dataset.status;
      if (previewStatus !== expectedStatus) {
        observation.unexpectedPreviewStatusSeen = true;
      }
      const jobStrip = root.querySelector('[data-testid="job-strip"]');
      const jobPhase =
        jobStrip?.querySelector(".job-result-copy > strong")?.textContent?.trim()
        ?? "";
      if (
        terminalJobPhaseRequired
        && jobPhase !== "Completed"
      ) {
        observation.terminalPhaseRegressionSeen = true;
      }
      const jobCount =
        jobStrip?.querySelector(".job-result-copy > span")?.textContent?.trim()
        ?? "";
      const jobSnapshot = jobStrip === null
        ? "missing"
        : runtime.boundedDiagnostic(
          `${jobStrip.getAttribute("aria-busy") ?? "unset"}:`
          + `${runtime.boundedDiagnostic(jobPhase)}:`
          + runtime.boundedDiagnostic(jobCount),
        );
      if (
        observation.jobStripSnapshots.at(-1) !== jobSnapshot
      ) {
        if (observation.jobStripSnapshots.length < maximumRecordedDiagnostics) {
          observation.jobStripSnapshots.push(jobSnapshot);
        } else if (observation.jobStripSnapshotOverflow < Number.MAX_SAFE_INTEGER) {
          observation.jobStripSnapshotOverflow += 1;
        }
      }
    };
    const observer = new MutationObserver(record);
    observation = {
      stalePreviewTextSeen: false,
      previewTableSeen: false,
      unexpectedPreviewStatusSeen: false,
      terminalPhaseRegressionSeen: false,
      jobStripSnapshotOverflow: 0,
      jobStripSnapshots: [],
      observer,
    };
    observer.observe(root, {
      attributes: true,
      attributeFilter: ["aria-busy", "data-status"],
      characterData: true,
      childList: true,
      subtree: true,
    });
    observedWindow.openSplunkStaleDuplicateDOMObservation = observation;
    record();
  }, {
    expectedStalePreviewText: stalePreviewText,
    expectedStatus: expectedPreviewStatus,
    maximumRecordedDiagnostics: MAXIMUM_RECORDED_DIAGNOSTICS,
    terminalJobPhaseRequired: requireTerminalJobPhase,
  });
}

async function finishStaleDuplicateDOMObservation(
  page: Page,
): Promise<StaleDuplicateDOMSnapshot> {
  return stopStaleDuplicateDOMObservation(page, true);
}

async function discardStaleDuplicateDOMObservation(page: Page): Promise<void> {
  await stopStaleDuplicateDOMObservation(page, false);
}

async function stopStaleDuplicateDOMObservation(
  page: Page,
  required: boolean,
): Promise<StaleDuplicateDOMSnapshot> {
  return page.evaluate((observationRequired) => {
    const observedWindow = window as StaleDuplicateDOMObservationWindow;
    const observation = observedWindow.openSplunkStaleDuplicateDOMObservation;
    if (observation === undefined) {
      if (observationRequired) {
        throw new Error("stale-duplicate DOM observation was not active");
      }
      return {
        stalePreviewTextSeen: false,
        previewTableSeen: false,
        unexpectedPreviewStatusSeen: false,
        terminalPhaseRegressionSeen: false,
        jobStripSnapshotOverflow: 0,
        jobStripSnapshots: [],
      };
    }
    observation.observer.disconnect();
    delete observedWindow.openSplunkStaleDuplicateDOMObservation;
    return {
      stalePreviewTextSeen: observation.stalePreviewTextSeen,
      previewTableSeen: observation.previewTableSeen,
      unexpectedPreviewStatusSeen: observation.unexpectedPreviewStatusSeen,
      terminalPhaseRegressionSeen: observation.terminalPhaseRegressionSeen,
      jobStripSnapshotOverflow: observation.jobStripSnapshotOverflow,
      jobStripSnapshots: [...observation.jobStripSnapshots],
    };
  }, required);
}

async function installBrowserWebSocketFrameRecorder(
  page: Page,
  expectedOrigin: string,
): Promise<void> {
  await page.addInitScript(({
    expectedSocketOrigin,
    maximumFrameBytes,
    maximumFrames,
    maximumMatchingSockets,
  }) => {
    const recorderWindow = window as BrowserWebSocketFrameRecorderWindow;
    const frames: string[] = [];
    const activeSockets = new Set<EventTarget>();
    let frameOverflow = 0;
    let matchingSockets = 0;
    let pendingFrameConversions = 0;
    let socketOverflow = 0;
    const reserveFrame = (byteLength: number): boolean => {
      if (
        byteLength > maximumFrameBytes
        || frames.length + pendingFrameConversions >= maximumFrames
      ) {
        frameOverflow += 1;
        return false;
      }
      return true;
    };
    const recordReservedFrame = (bytes: Uint8Array): void => {
      let binary = "";
      for (const byte of bytes) binary += String.fromCharCode(byte);
      frames.push(btoa(binary));
    };
    const matchesSearchSocket = (url: string): boolean => {
      const socketURL = new URL(url);
      const socketHTTPURL = new URL(socketURL);
      socketHTTPURL.protocol = socketURL.protocol === "wss:" ? "https:" : "http:";
      return socketHTTPURL.origin === expectedSocketOrigin
        && socketURL.pathname === "/api/search/ws";
    };
    const NativeWebSocket = window.WebSocket;
    class RecordingWebSocket extends NativeWebSocket {
      public constructor(url: string | URL, protocols?: string | string[]) {
        super(url, protocols);
        if (!matchesSearchSocket(this.url)) return;
        matchingSockets += 1;
        if (matchingSockets > maximumMatchingSockets) {
          socketOverflow += 1;
          return;
        }
        activeSockets.add(this);
        const onMessage = (event: MessageEvent): void => {
          if (event.data instanceof ArrayBuffer) {
            if (!reserveFrame(event.data.byteLength)) return;
            recordReservedFrame(new Uint8Array(event.data));
          } else if (event.data instanceof Blob) {
            if (!reserveFrame(event.data.size)) return;
            pendingFrameConversions += 1;
            void (async () => {
              try {
                const buffer = await event.data.arrayBuffer();
                if (buffer.byteLength > maximumFrameBytes) {
                  frameOverflow += 1;
                  return;
                }
                recordReservedFrame(new Uint8Array(buffer));
              } catch {
                frameOverflow += 1;
              } finally {
                pendingFrameConversions -= 1;
              }
            })();
          }
        };
        const onClose = (): void => {
          this.removeEventListener("message", onMessage);
          activeSockets.delete(this);
        };
        this.addEventListener("message", onMessage);
        this.addEventListener("close", onClose, { once: true });
      }
    }
    Object.defineProperty(window, "WebSocket", {
      configurable: true,
      value: RecordingWebSocket,
      writable: true,
    });
    recorderWindow.openSplunkBrowserWebSocketFrameRecorder = {
      activeSockets: () => activeSockets.size,
      frames,
      frameOverflow: () => frameOverflow,
      socketOverflow: () => socketOverflow,
    };
  }, {
    expectedSocketOrigin: expectedOrigin,
    maximumFrameBytes: maximumRecordedBrowserFrameBytes,
    maximumFrames: maximumRecordedBrowserFrames,
    maximumMatchingSockets: 2,
  });
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function optionalLoopbackURL(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  if (!trimmed) return undefined;
  const parsed = new URL(trimmed);
  if (
    parsed.protocol !== "http:"
    || (parsed.hostname !== "127.0.0.1" && parsed.hostname !== "localhost")
    || parsed.username
    || parsed.password
    || parsed.pathname !== "/"
    || parsed.search
    || parsed.hash
  ) {
    throw new Error("the browser recovery control URL must contain only a loopback HTTP origin");
  }
  return parsed.origin;
}

function parsePositiveInteger(value: string): number {
  if (!/^[1-9][0-9]*$/.test(value)) throw new Error(`invalid positive integer ${JSON.stringify(value)}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`integer exceeds the safe range: ${value}`);
  return parsed;
}

function validatedOrigin(value: string): string {
  const parsed = new URL(value);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("the browser vertical test requires an HTTP(S) loopback URL");
  }
  if (ignoreHTTPSErrors && parsed.protocol !== "https:") {
    throw new Error("ignoring HTTPS errors requires an HTTPS browser vertical URL");
  }
  if (parsed.hostname !== "127.0.0.1" && parsed.hostname !== "localhost") {
    throw new Error("the browser vertical test only connects to a loopback server");
  }
  if (parsed.username || parsed.password || parsed.pathname !== "/" || parsed.search || parsed.hash) {
    throw new Error("the browser vertical base URL must contain only a loopback origin");
  }
  return parsed.origin;
}

function httpOriginForWebSocket(socketURL: URL): string {
  if (socketURL.protocol !== "ws:" && socketURL.protocol !== "wss:") {
    throw new Error(`unexpected WebSocket URL protocol: ${socketURL.protocol}`);
  }
  const httpURL = new URL(socketURL);
  httpURL.protocol = socketURL.protocol === "wss:" ? "https:" : "http:";
  return httpURL.origin;
}

function matchesSearchWebSocketURL(socketURL: URL, expectedOrigin: string): boolean {
  return httpOriginForWebSocket(socketURL) === expectedOrigin
    && socketURL.pathname === "/api/search/ws";
}

function rejectExcessWebSocketRoute(client: WebSocketRoute): void {
  void client.close({
    code: 1008,
    reason: "test harness connection limit",
  }).catch(() => undefined);
}

function matchesAPIResponse(response: Response, expectedOrigin: string, pathname: string): boolean {
  const responseURL = new URL(response.url());
  return responseURL.origin === expectedOrigin
    && responseURL.pathname === pathname
    && response.request().method() === "POST";
}

function assertProtobufResponse(response: Response): void {
  expect(response.status(), `${response.url()} status`).toBe(200);
  expect(response.headers()["content-type"], `${response.url()} Content-Type`).toBe("application/x-protobuf");
}

interface BrowserSafetyObservation {
  pageErrors: BoundedRecorder;
  failedAPIRequests: BoundedRecorder;
  externalRequests: BoundedRecorder;
  externalWebSockets: BoundedRecorder;
  createRequests(): number;
  resultsRequests(): number;
}

interface CancellationSocketObservation {
  waitForInitialClose(): Promise<void>;
  connectionCount(): number;
  assertHealthy(): void;
  dispose(): void;
}

interface CancellationSocketListeners {
  close(): void;
  error(error: string): void;
}

function observeCancellationSockets(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
): CancellationSocketObservation {
  let matchingConnections = 0;
  let closedConnections = 0;
  let initialCloseSettled = false;
  let failure: Error | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let resolveInitialClose!: () => void;
  let rejectInitialClose!: (reason: Error) => void;
  const listeners = new Map<WebSocket, CancellationSocketListeners>();
  const initialClose = new Promise<void>((resolve, reject) => {
    resolveInitialClose = resolve;
    rejectInitialClose = reject;
  });
  void initialClose.catch(() => undefined);

  const fail = (error: Error): void => {
    const normalized = normalizeError(error);
    failure ??= normalized;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    page.off("websocket", observeSocket);
    for (const [socket, socketListeners] of listeners) {
      socket.off("close", socketListeners.close);
      socket.off("socketerror", socketListeners.error);
    }
    listeners.clear();
    if (!initialCloseSettled) {
      initialCloseSettled = true;
      rejectInitialClose(normalized);
    }
  };
  const observeSocket = (socket: WebSocket): void => {
    const socketURL = new URL(socket.url());
    if (!matchesSearchWebSocketURL(socketURL, expectedOrigin)) return;
    matchingConnections += 1;
    if (matchingConnections > 1) {
      fail(new Error(`browser cancellation opened ${matchingConnections} search WebSockets`));
      return;
    }
    const socketListeners: CancellationSocketListeners = {
      close: () => {
        closedConnections += 1;
        socket.off("close", socketListeners.close);
        socket.off("socketerror", socketListeners.error);
        listeners.delete(socket);
        if (closedConnections > 1) {
          fail(new Error(`browser cancellation closed ${closedConnections} search WebSockets`));
          return;
        }
        if (!initialCloseSettled) {
          initialCloseSettled = true;
          if (timer !== undefined) {
            clearTimeout(timer);
            timer = undefined;
          }
          resolveInitialClose();
        }
      },
      error: (error) => fail(new Error(`browser cancellation WebSocket failed: ${error}`)),
    };
    listeners.set(socket, socketListeners);
    socket.on("close", socketListeners.close);
    socket.on("socketerror", socketListeners.error);
  };

  timer = setTimeout(
    () => fail(new Error("timed out waiting for browser cancellation to close its search WebSocket")),
    timeoutMilliseconds,
  );
  page.on("websocket", observeSocket);
  return {
    waitForInitialClose() {
      if (failure !== undefined) return Promise.reject(failure);
      return initialClose;
    },
    connectionCount() {
      return matchingConnections;
    },
    assertHealthy() {
      if (failure !== undefined) throw failure;
      if (matchingConnections !== 1 || closedConnections !== 1) {
        throw new Error(
          `browser cancellation WebSockets = ${matchingConnections} connections/${closedConnections} closes`,
        );
      }
    },
    dispose() {
      if (timer !== undefined) clearTimeout(timer);
      page.off("websocket", observeSocket);
      for (const [socket, socketListeners] of listeners) {
        socket.off("close", socketListeners.close);
        socket.off("socketerror", socketListeners.error);
      }
      listeners.clear();
      if (!initialCloseSettled) {
        initialCloseSettled = true;
        resolveInitialClose();
      }
    },
  };
}

function observeBrowserSafety(page: Page): BrowserSafetyObservation {
  const pageErrors = boundedRecorder();
  const failedAPIRequests = boundedRecorder();
  const externalRequests = boundedRecorder();
  const externalWebSockets = boundedRecorder();
  let createRequests = 0;
  let resultsRequests = 0;
  page.on("pageerror", (error) => pageErrors.add(error.message));
  page.on("requestfailed", (request) => {
    const requestURL = new URL(request.url());
    if (requestURL.origin === origin && requestURL.pathname.startsWith("/api/")) {
      failedAPIRequests.add(`${request.method()} ${requestURL.pathname}: ${request.failure()?.errorText ?? "failed"}`);
    }
  });
  page.on("request", (request) => {
    const requestURL = new URL(request.url());
    if (
      requestURL.origin === origin
      && requestURL.pathname === "/api/search/jobs/create"
      && request.method() === "POST"
    ) {
      createRequests += 1;
    }
    if (
      requestURL.origin === origin
      && requestURL.pathname === "/api/search/jobs/results"
      && request.method() === "POST"
    ) {
      resultsRequests += 1;
    }
    if ((requestURL.protocol === "http:" || requestURL.protocol === "https:") && requestURL.origin !== origin) {
      externalRequests.add(`${request.method()} ${requestURL.origin}${requestURL.pathname}`);
    }
  });
  page.on("websocket", (socket) => {
    const socketURL = new URL(socket.url());
    if (httpOriginForWebSocket(socketURL) !== origin) {
      externalWebSockets.add(socket.url());
    }
  });
  return {
    pageErrors,
    failedAPIRequests,
    externalRequests,
    externalWebSockets,
    createRequests: () => createRequests,
    resultsRequests: () => resultsRequests,
  };
}

function assertBrowserSafety(
  observation: BrowserSafetyObservation,
  expectedFailedAPIRequests: readonly RegExp[] = [],
): void {
  expect(observation.pageErrors.snapshot(), "uncaught browser errors").toEqual([]);
  const failedAPIRequests = observation.failedAPIRequests.snapshot();
  expect(failedAPIRequests, "failed same-origin API request count")
    .toHaveLength(expectedFailedAPIRequests.length);
  expectedFailedAPIRequests.forEach((expected, index) => {
    expect(failedAPIRequests[index], `failed same-origin API request ${index + 1}`)
      .toMatch(expected);
  });
  expect(observation.externalRequests.snapshot(), "external browser resources").toEqual([]);
  expect(observation.externalWebSockets.snapshot(), "external browser WebSockets").toEqual([]);
}

async function openSearchWorkspace(page: Page): Promise<Locator> {
  await installBrowserHarnessRuntime(page);
  const launchURL = new URL("/search/", origin);
  launchURL.search = new URLSearchParams({
    q: searchSPL,
    earliest,
    latest,
    timezone: "UTC",
    run: "0",
  }).toString();
  await page.goto(launchURL.href, { waitUntil: "domcontentloaded", timeout });
  await verifyBrowserHarnessRuntime(page);
  await expect(page.getByTestId("search-workspace")).toBeVisible({ timeout });
  await expect(page.getByText("Backend data", { exact: true })).toBeVisible({ timeout });
  await expect(page.getByTestId("search-input")).toHaveValue(searchSPL);
  const runSearch = page.getByTestId("run-search");
  await expect(runSearch).toBeEnabled({ timeout });
  await installPreviewStatusRecorder(page);
  if (!browserRecorderSelfTestCompleted) {
    try {
      await verifyBrowserRecorderBounds(page);
      browserRecorderSelfTestCompleted = true;
    } finally {
      await installPreviewStatusRecorder(page);
    }
  }
  return runSearch;
}

async function installBrowserHarnessRuntime(page: Page): Promise<void> {
  await page.addInitScript(({
    maximumDiagnosticBytes,
    truncationSuffix,
  }) => {
    if ((window as BrowserHarnessRuntimeWindow).openSplunkBrowserHarnessRuntime !== undefined) {
      return;
    }
    const boundedPageDiagnostic = (value: string): string => {
      const prefixByteLimit = maximumDiagnosticBytes - truncationSuffix.length;
      let byteLength = 0;
      let prefixEnd = 0;
      for (let index = 0; index < value.length;) {
        const codePoint = value.codePointAt(index)!;
        const codeUnits = codePoint > 0xffff ? 2 : 1;
        byteLength += codePoint <= 0x7f
          ? 1
          : codePoint <= 0x7ff
            ? 2
            : codePoint <= 0xffff
              ? 3
              : 4;
        index += codeUnits;
        if (byteLength <= prefixByteLimit) prefixEnd = index;
        if (byteLength > maximumDiagnosticBytes) {
          return value.slice(0, prefixEnd) + truncationSuffix;
        }
      }
      return value;
    };
    Object.defineProperty(window, "openSplunkBrowserHarnessRuntime", {
      configurable: false,
      enumerable: false,
      value: Object.freeze({ boundedDiagnostic: boundedPageDiagnostic }),
      writable: false,
    });
  }, {
    maximumDiagnosticBytes: MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
    truncationSuffix: BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX,
  });
}

async function verifyBrowserHarnessRuntime(page: Page): Promise<void> {
  const result = await page.evaluate(({
    maximumDiagnosticBytes,
    truncationSuffix,
  }) => {
    const runtime = (window as BrowserHarnessRuntimeWindow)
      .openSplunkBrowserHarnessRuntime;
    if (runtime === undefined) return undefined;
    const candidates = [
      "x".repeat(maximumDiagnosticBytes + 1),
      "🙂".repeat(maximumDiagnosticBytes),
      `${"x".repeat(maximumDiagnosticBytes - 1)}\ud800`,
    ];
    return candidates.map((candidate) => {
      const bounded = runtime.boundedDiagnostic(candidate);
      return {
        byteLength: new TextEncoder().encode(bounded).byteLength,
        hasSuffix: bounded.endsWith(truncationSuffix),
        hasReplacement: bounded.includes("\ufffd"),
      };
    });
  }, {
    maximumDiagnosticBytes: MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
    truncationSuffix: BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX,
  });
  if (
    result === undefined
    || result.some((candidate) =>
      candidate.byteLength > MAXIMUM_BROWSER_DIAGNOSTIC_BYTES
      || !candidate.hasSuffix
      || candidate.hasReplacement
    )
  ) {
    throw new Error("browser harness diagnostic byte-bound self-test failed");
  }
}

async function verifyBrowserRecorderBounds(page: Page): Promise<void> {
  const fixtureIDs = {
    metrics: "open-splunk-harness-metrics-fixture",
    stale: "open-splunk-harness-stale-fixture",
    status: "open-splunk-harness-status-fixture",
  };

  try {
    await page.evaluate(async ({
      maximumRecordedDiagnostics,
      statusFixtureID,
    }) => {
      const status = document.createElement("div");
      status.id = statusFixtureID;
      status.hidden = true;
      status.dataset.testid = "backend-preview-status";
      document.body.prepend(status);
      // This helper must be defined inside the evaluated browser realm.
      // oxlint-disable-next-line unicorn/consistent-function-scoping
      const settle = async (): Promise<void> => {
        await new Promise<void>((resolve) => queueMicrotask(resolve));
      };
      for (let index = 0; index <= maximumRecordedDiagnostics; index += 1) {
        status.dataset.status = `harness-overflow-${index}`;
        // Each distinct value must reach the production MutationObserver.
        // oxlint-disable-next-line eslint/no-await-in-loop
        await settle();
      }
      status.dataset.status = "finalization-error";
      await settle();
    }, {
      maximumRecordedDiagnostics: MAXIMUM_RECORDED_DIAGNOSTICS,
      statusFixtureID: fixtureIDs.status,
    });
    const previewResult = await page.evaluate(() => {
      const recorder = (window as PreviewRecorderWindow)
        .openSplunkE2EPreviewRecorder;
      return recorder === undefined
        ? undefined
        : {
          finalizationErrorSeen: recorder.finalizationErrorSeen(),
          overflow: recorder.overflow(),
        };
    });
    if (
      previewResult === undefined
      || previewResult.overflow <= 0
      || !previewResult.finalizationErrorSeen
    ) {
      throw new Error("preview-status overflow/latch self-test failed");
    }
    await page.evaluate((fixtureID) => document.getElementById(fixtureID)?.remove(), fixtureIDs.status);
    await installPreviewStatusRecorder(page);

    await page.evaluate((fixtureID) => {
      const metrics = document.createElement("div");
      metrics.id = fixtureID;
      metrics.hidden = true;
      metrics.setAttribute("aria-label", "Job metrics");
      metrics.append(document.createElement("strong"));
      metrics.querySelector("strong")!.textContent = "0 rows";
      document.body.prepend(metrics);
    }, fixtureIDs.metrics);
    await beginJobMetricsObservation(page);
    await page.evaluate(async ({
      fixtureID,
      maximumRecordedDiagnostics,
    }) => {
      const strong = document.querySelector<HTMLElement>(`#${fixtureID} strong`);
      if (strong === null) throw new Error("job-metric self-test fixture is missing");
      // This helper must be defined inside the evaluated browser realm.
      // oxlint-disable-next-line unicorn/consistent-function-scoping
      const settle = async (): Promise<void> => {
        await new Promise<void>((resolve) => queueMicrotask(resolve));
      };
      for (let index = 0; index <= maximumRecordedDiagnostics; index += 1) {
        strong.textContent = `${index + 2} rows`;
        // Each distinct value must reach the production MutationObserver.
        // oxlint-disable-next-line eslint/no-await-in-loop
        await settle();
      }
      strong.textContent = "1 rows";
      await settle();
    }, {
      fixtureID: fixtureIDs.metrics,
      maximumRecordedDiagnostics: MAXIMUM_RECORDED_DIAGNOSTICS,
    });
    const metricsResult = await finishJobMetricsObservation(page);
    if (metricsResult.diagnosticOverflow <= 0 || !metricsResult.staleOneRowSeen) {
      throw new Error("job-metric overflow/latch self-test failed");
    }
    await page.evaluate((fixtureID) => document.getElementById(fixtureID)?.remove(), fixtureIDs.metrics);

    const stalePreviewText = "harness-forbidden-stale-preview";
    const expectedStatus = "harness-expected";
    await page.evaluate(({
      expectedPreviewStatus,
      fixtureID,
    }) => {
      const root = document.createElement("div");
      root.id = fixtureID;
      root.hidden = true;
      root.dataset.testid = "search-workspace";

      const status = document.createElement("div");
      status.dataset.testid = "backend-preview-status";
      status.dataset.status = expectedPreviewStatus;
      root.append(status);

      const jobStrip = document.createElement("div");
      jobStrip.dataset.testid = "job-strip";
      jobStrip.setAttribute("aria-busy", "false");
      const resultCopy = document.createElement("div");
      resultCopy.className = "job-result-copy";
      const phase = document.createElement("strong");
      phase.textContent = "Completed";
      const count = document.createElement("span");
      count.textContent = "0 events";
      resultCopy.append(phase, count);
      jobStrip.append(resultCopy);
      root.append(jobStrip);
      document.body.prepend(root);
    }, {
      expectedPreviewStatus: expectedStatus,
      fixtureID: fixtureIDs.stale,
    });
    await beginStaleDuplicateDOMObservation(
      page,
      stalePreviewText,
      expectedStatus,
      true,
    );
    await page.evaluate(async ({
      fixtureID,
      maximumRecordedDiagnostics,
      staleText,
    }) => {
      const root = document.getElementById(fixtureID);
      const count = root?.querySelector<HTMLElement>(".job-result-copy > span");
      const phase = root?.querySelector<HTMLElement>(".job-result-copy > strong");
      const status = root?.querySelector<HTMLElement>(
        '[data-testid="backend-preview-status"]',
      );
      if (root === null || count == null || phase == null || status == null) {
        throw new Error("stale-DOM self-test fixture is incomplete");
      }
      // This helper must be defined inside the evaluated browser realm.
      // oxlint-disable-next-line unicorn/consistent-function-scoping
      const settle = async (): Promise<void> => {
        await new Promise<void>((resolve) => queueMicrotask(resolve));
      };
      for (let index = 0; index <= maximumRecordedDiagnostics; index += 1) {
        count.textContent = `${index + 1} events`;
        // Each distinct value must reach the production MutationObserver.
        // oxlint-disable-next-line eslint/no-await-in-loop
        await settle();
      }
      phase.textContent = "Queued";
      status.dataset.status = "harness-unexpected";
      const preview = document.createElement("article");
      preview.className = "event-row--preview";
      preview.textContent = staleText;
      root.append(preview);
      const previewTable = document.createElement("table");
      previewTable.setAttribute("aria-label", "Live preview search statistics");
      root.append(previewTable);
      await settle();
    }, {
      fixtureID: fixtureIDs.stale,
      maximumRecordedDiagnostics: MAXIMUM_RECORDED_DIAGNOSTICS,
      staleText: stalePreviewText,
    });
    const staleResult = await finishStaleDuplicateDOMObservation(page);
    if (
      staleResult.jobStripSnapshotOverflow <= 0
      || !staleResult.stalePreviewTextSeen
      || !staleResult.previewTableSeen
      || !staleResult.unexpectedPreviewStatusSeen
      || !staleResult.terminalPhaseRegressionSeen
    ) {
      throw new Error("stale-DOM overflow/latch self-test failed");
    }
  } finally {
    await discardJobMetricsObservation(page).catch(() => undefined);
    await discardStaleDuplicateDOMObservation(page).catch(() => undefined);
    await page.evaluate((ids) => {
      document.getElementById(ids.metrics)?.remove();
      document.getElementById(ids.stale)?.remove();
      document.getElementById(ids.status)?.remove();
    }, fixtureIDs).catch(() => undefined);
  }
}

interface SearchResponseWaiters {
  createResponsePromise: Promise<Response>;
  resultsResponsePromise: Promise<Response>;
}

function waitForCreateSearchResponse(page: Page): Promise<Response> {
  const createResponsePromise = page.waitForResponse(
    (response) => matchesAPIResponse(response, origin, "/api/search/jobs/create"),
    { timeout },
  );
  // Page teardown can reject the waiter before the normal await is reached.
  // Mark it handled immediately while retaining the original promise.
  void createResponsePromise.catch(() => undefined);
  return createResponsePromise;
}

function waitForSearchResponses(page: Page): SearchResponseWaiters {
  const createResponsePromise = waitForCreateSearchResponse(page);
  const resultsResponsePromise = page.waitForResponse(
    (response) => matchesAPIResponse(response, origin, "/api/search/jobs/results"),
    { timeout },
  );
  // Canceled and failed searches may tear the page down before results exist.
  void resultsResponsePromise.catch(() => undefined);
  return { createResponsePromise, resultsResponsePromise };
}

interface PreviewRecorderState {
  finalizationErrorSeen(): boolean;
  observer: MutationObserver;
  overflow(): number;
  statuses: string[];
}

type PreviewRecorderWindow = Window & {
  openSplunkE2EPreviewRecorder?: PreviewRecorderState;
};

interface BrowserHarnessRuntime {
  boundedDiagnostic(value: string): string;
}

type BrowserHarnessRuntimeWindow = Window & {
  openSplunkBrowserHarnessRuntime?: BrowserHarnessRuntime;
};

interface JobMetricsSnapshot {
  diagnosticOverflow: number;
  diagnostics: string[];
  staleOneRowSeen: boolean;
}

interface JobMetricsObservation {
  diagnosticOverflow(): number;
  diagnostics: string[];
  observer: MutationObserver;
  staleOneRowSeen(): boolean;
}

type JobMetricsObservationWindow = Window & {
  openSplunkJobMetricsObservation?: JobMetricsObservation;
};

interface StaleDuplicateDOMSnapshot {
  jobStripSnapshotOverflow: number;
  stalePreviewTextSeen: boolean;
  previewTableSeen: boolean;
  unexpectedPreviewStatusSeen: boolean;
  terminalPhaseRegressionSeen: boolean;
  jobStripSnapshots: string[];
}

interface StaleDuplicateDOMObservation extends StaleDuplicateDOMSnapshot {
  observer: MutationObserver;
}

type StaleDuplicateDOMObservationWindow = Window & {
  openSplunkStaleDuplicateDOMObservation?: StaleDuplicateDOMObservation;
};

interface BrowserWebSocketFrameRecorder {
  activeSockets(): number;
  frames: string[];
  frameOverflow(): number;
  socketOverflow(): number;
}

type BrowserWebSocketFrameRecorderWindow = Window & {
  openSplunkBrowserWebSocketFrameRecorder?: BrowserWebSocketFrameRecorder;
};

async function installPreviewStatusRecorder(page: Page): Promise<void> {
  await page.evaluate((maximumRecordedDiagnostics) => {
    const recorderWindow = window as PreviewRecorderWindow;
    recorderWindow.openSplunkE2EPreviewRecorder?.observer.disconnect();
    const runtime = (window as BrowserHarnessRuntimeWindow)
      .openSplunkBrowserHarnessRuntime;
    if (runtime === undefined) throw new Error("browser harness runtime is unavailable");
    const statuses: string[] = [];
    let finalizationErrorSeen = false;
    let overflow = 0;
    const record = (): void => {
      const status = document.querySelector<HTMLElement>('[data-testid="backend-preview-status"]')
        ?.dataset.status;
      if (!status) return;
      if (status === "finalization-error") finalizationErrorSeen = true;
      const diagnostic = runtime.boundedDiagnostic(status);
      if (statuses.at(-1) === diagnostic) return;
      if (statuses.length < maximumRecordedDiagnostics) statuses.push(diagnostic);
      else if (overflow < Number.MAX_SAFE_INTEGER) overflow += 1;
    };
    const observer = new MutationObserver(record);
    observer.observe(document.body, {
      attributes: true,
      attributeFilter: ["data-status"],
      childList: true,
      subtree: true,
    });
    recorderWindow.openSplunkE2EPreviewRecorder = {
      finalizationErrorSeen: () => finalizationErrorSeen,
      observer,
      overflow: () => overflow,
      statuses,
    };
    record();
  }, MAXIMUM_RECORDED_DIAGNOSTICS);
}

async function collectPreviewStatuses(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const recorderWindow = window as PreviewRecorderWindow;
    const recorder = recorderWindow.openSplunkE2EPreviewRecorder;
    recorder?.observer.disconnect();
    if (recorder === undefined) return [];
    if (recorder.overflow() !== 0) {
      throw new Error(`preview status diagnostics overflowed by ${recorder.overflow()} entries`);
    }
    const statuses = [...recorder.statuses];
    if (recorder.finalizationErrorSeen() && !statuses.includes("finalization-error")) {
      statuses.push("finalization-error");
    }
    return statuses;
  });
}

async function snapshotPreviewStatuses(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const recorder = (window as PreviewRecorderWindow).openSplunkE2EPreviewRecorder;
    if (recorder === undefined) return [];
    if (recorder.overflow() !== 0) {
      throw new Error(`preview status diagnostics overflowed by ${recorder.overflow()} entries`);
    }
    const statuses = [...recorder.statuses];
    if (recorder.finalizationErrorSeen() && !statuses.includes("finalization-error")) {
      statuses.push("finalization-error");
    }
    return statuses;
  });
}

interface ObservedSubscription {
  subscriptionID: string;
  searchJobID: string;
  afterSequence: bigint;
  includePreviews: boolean;
  previewRowLimit: number | undefined;
}

function assertRetainedSubscription(
  candidate: ObservedSubscription,
  initial: ObservedSubscription,
  expectedAfterSequence: bigint,
  context: string,
): void {
  if (
    candidate.subscriptionID === initial.subscriptionID
    && candidate.searchJobID === initial.searchJobID
    && candidate.afterSequence === expectedAfterSequence
    && candidate.includePreviews === initial.includePreviews
    && candidate.previewRowLimit === initial.previewRowLimit
  ) {
    return;
  }
  throw new Error(
    `${context} subscription = ${JSON.stringify({
      subscriptionID: candidate.subscriptionID,
      searchJobID: candidate.searchJobID,
      afterSequence: candidate.afterSequence.toString(),
      includePreviews: candidate.includePreviews,
      previewRowLimit: candidate.previewRowLimit,
    })}; expected the original subscription after ${expectedAfterSequence.toString()}`,
  );
}

interface ObservedProgress {
  sequence: bigint;
  subscriptionID: string;
  searchJobID: string;
}

interface ObservedSearchTerminal {
  sequence: bigint;
  subscriptionID: string;
  searchJobID: string;
  state: SearchJobState;
  failureCode: number | undefined;
  failureMessage: string | undefined;
}

type FrameEvent = { payload: string | Buffer };

interface SocketListeners {
  sent(event: FrameEvent): void;
  received(event: FrameEvent): void;
  error(error: string): void;
  close(): void;
}

interface SearchProtocolObservation {
  waitForJob(searchJobID: string): Promise<void>;
  dispose(): void;
}

interface RetainedReplayObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  disconnect(): Promise<void>;
  waitForTerminalReplay(): Promise<void>;
  dispose(): void;
}

interface SequenceExpirationObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  withheldFrameCount(): number;
  disconnect(): Promise<void>;
  waitForResynchronizations(count: number): Promise<void>;
  heldRecoveryFrameCount(): number;
  connectionCount(): number;
  postRecoveryFrameCount(): number;
  assertHealthy(): void;
  dispose(): void;
}

interface SequenceGapObservation {
  waitForCheckpoint(searchJobID: string): Promise<void>;
  droppedFrameCount(): number;
  waitForReplayReady(): Promise<void>;
  replayProgress(index: 1 | 2): SearchProgress;
  releaseNextReplayFrame(): bigint;
  waitForReplayFrameReceived(sequence: bigint): Promise<void>;
  injectStaleCheckpointDuplicate(): Promise<bigint>;
  injectStaleRunningStateDuplicate(): Promise<bigint>;
  waitForTerminalProjectionWithheld(): Promise<void>;
  withheldTerminalProjectionFrameCount(): number;
  waitForTerminalClose(): Promise<void>;
  connectionCount(): number;
  liveFrameCount(): number;
  assertHealthy(): void;
  dispose(): void;
}

interface ObservedSubscribeCommand {
  requestID: string;
  subscriptions: ObservedSubscription[];
}

async function sendBrowserRecoveryControl(
  action: "progress" | "append" | "complete",
): Promise<void> {
  if (recoveryControlURL === undefined || !recoveryControlToken) {
    throw new Error("the browser recovery control endpoint is unavailable");
  }
  const response = await fetch(new URL(`/${action}`, recoveryControlURL), {
    method: "POST",
    headers: {
      [browserRecoveryControlTokenHeader]: recoveryControlToken,
    },
  });
  const body = new Uint8Array(await response.arrayBuffer());
  if (body.byteLength > 1_024) {
    throw new Error(`browser recovery control ${action} response exceeded 1024 bytes`);
  }
  if (!response.ok) {
    throw new Error(
      `browser recovery control ${action} failed with status ${response.status}`,
    );
  }
  if (response.headers.get("content-type") !== "application/json") {
    throw new Error(`browser recovery control ${action} returned a non-JSON response`);
  }
}

const browserRecoveryControlTokenHeader = "X-Open-Splunk-Test-Token";

async function interceptSequenceGap(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
  options: {
    withholdTerminalProjection: boolean;
    injectStaleDuplicates: boolean;
  },
): Promise<SequenceGapObservation> {
  const { withholdTerminalProjection, injectStaleDuplicates } = options;
  let expectedJobID: string | undefined;
  let routedJobID: string | undefined;
  let subscriptionID: string | undefined;
  let checkpoint: bigint | undefined;
  let checkpointFrame: Buffer | undefined;
  let runningStateFrame: Buffer | undefined;
  let initialSubscription: ObservedSubscription | undefined;
  let reconnectRequestID: string | undefined;
  let reconnectAcknowledged = false;
  let secondClient: WebSocketRoute | undefined;
  let lastUpstreamSequence: bigint | undefined;
  let matchingConnectionCount = 0;
  let subscribeCommandCount = 0;
  let clientFrameCount = 0;
  let serverFrameCount = 0;
  let gapCloseCount = 0;
  let gapCloseEchoCount = 0;
  let terminalCloseCount = 0;
  let terminalCloseEchoCount = 0;
  let gapCloseForwardCompleted = false;
  let terminalCloseForwardCompleted = false;
  let replayReleaseCount = 0;
  let staleCheckpointInjectionCount = 0;
  let staleRunningStateInjectionCount = 0;
  let liveTargetFrameCount = 0;
  let completedStateVersion: bigint | undefined;
  let withholdingTerminalProjection = false;
  let withheldTerminalProjectionFrameCount = 0;
  let withheldTerminalStateVersion: bigint | undefined;
  let gapStimulusSent = false;
  let disposed = false;
  let failure: Error | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const forwardedCloseConnections = new Set<number>();
  const originalFrames = new Map<bigint, Buffer>();
  const replayFrames = new Map<bigint, Buffer>();
  const connectionOneDeliveredSequences: bigint[] = [];
  const browserFrameDiagnostics: string[] = [];
  const replayBrowserFrameReceipts = new Set<bigint>();
  const replayBrowserFrameWaiters = new Map<
    bigint,
    { resolve: () => void; reject: (reason: Error) => void }
  >();
  let matchingBrowserSocketCount = 0;
  let replayBrowserSocket: WebSocket | undefined;
  let replayBrowserFrameListener: ((event: FrameEvent) => void) | undefined;
  let checkpointSettled = false;
  let replayReadySettled = false;
  let gapCloseSettled = false;
  let terminalProjectionSettled = false;
  let terminalCloseSettled = false;
  let resolveCheckpoint!: () => void;
  let rejectCheckpoint!: (reason: Error) => void;
  let resolveReplayReady!: () => void;
  let rejectReplayReady!: (reason: Error) => void;
  let resolveGapClose!: () => void;
  let rejectGapClose!: (reason: Error) => void;
  let resolveTerminalProjection!: () => void;
  let rejectTerminalProjection!: (reason: Error) => void;
  let resolveTerminalClose!: () => void;
  let rejectTerminalClose!: (reason: Error) => void;
  const checkpointReady = new Promise<void>((resolve, reject) => {
    resolveCheckpoint = resolve;
    rejectCheckpoint = reject;
  });
  const replayReady = new Promise<void>((resolve, reject) => {
    resolveReplayReady = resolve;
    rejectReplayReady = reject;
  });
  const gapCloseReady = new Promise<void>((resolve, reject) => {
    resolveGapClose = resolve;
    rejectGapClose = reject;
  });
  const terminalProjectionReady = new Promise<void>((resolve, reject) => {
    resolveTerminalProjection = resolve;
    rejectTerminalProjection = reject;
  });
  const terminalCloseReady = new Promise<void>((resolve, reject) => {
    resolveTerminalClose = resolve;
    rejectTerminalClose = reject;
  });
  void checkpointReady.catch(() => undefined);
  void replayReady.catch(() => undefined);
  void gapCloseReady.catch(() => undefined);
  void terminalProjectionReady.catch(() => undefined);
  void terminalCloseReady.catch(() => undefined);

  const fail = (error: unknown): void => {
    const normalized = normalizeError(error);
    failure ??= normalized;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (!checkpointSettled) {
      checkpointSettled = true;
      rejectCheckpoint(normalized);
    }
    if (!replayReadySettled) {
      replayReadySettled = true;
      rejectReplayReady(normalized);
    }
    if (!gapCloseSettled) {
      gapCloseSettled = true;
      rejectGapClose(normalized);
    }
    if (!terminalProjectionSettled) {
      terminalProjectionSettled = true;
      rejectTerminalProjection(normalized);
    }
    if (!terminalCloseSettled) {
      terminalCloseSettled = true;
      rejectTerminalClose(normalized);
    }
    for (const waiter of replayBrowserFrameWaiters.values()) waiter.reject(normalized);
    replayBrowserFrameWaiters.clear();
  };
  const routeMatchesSearchSocket = (url: URL): boolean =>
    matchesSearchWebSocketURL(url, expectedOrigin);
  const observeBrowserSocket = (socket: WebSocket): void => {
    const socketURL = new URL(socket.url());
    if (!routeMatchesSearchSocket(socketURL)) return;
    matchingBrowserSocketCount += 1;
    if (matchingBrowserSocketCount > 2) {
      fail(
        new Error(
          `sequence-gap browser opened ${matchingBrowserSocketCount} matching WebSockets`,
        ),
      );
      return;
    }
    if (matchingBrowserSocketCount !== 2) return;
    replayBrowserSocket = socket;
    replayBrowserFrameListener = ({ payload }) => {
      try {
        if (typeof payload === "string") {
          throw new Error("sequence-gap browser received a text replay frame");
        }
        const event = SearchWebSocketEvent.decode(payload);
        if (browserFrameDiagnostics.length < 32) {
          browserFrameDiagnostics.push(
            `${event.sequence.toString()}:${String(event.payload?.$case)}:${Buffer.from(payload).byteLength}`,
          );
        }
        if (
          checkpoint === undefined
          || event.sequence < checkpoint + 1n
          || event.sequence > checkpoint + 2n
        ) {
          return;
        }
        replayBrowserFrameReceipts.add(event.sequence);
        replayBrowserFrameWaiters.get(event.sequence)?.resolve();
        replayBrowserFrameWaiters.delete(event.sequence);
      } catch (error) {
        fail(error);
      }
    };
    socket.on("framereceived", replayBrowserFrameListener);
  };
  page.on("websocket", observeBrowserSocket);

  await page.routeWebSocket(routeMatchesSearchSocket, (client) => {
    matchingConnectionCount += 1;
    const connectionOrdinal = matchingConnectionCount;
    if (connectionOrdinal > 2) {
      fail(new Error(`sequence-gap recovery opened ${connectionOrdinal} matching connections`));
      rejectExcessWebSocketRoute(client);
      return;
    }
    const server = client.connectToServer();
    if (connectionOrdinal === 2) secondClient = client;

    client.onMessage((message) => {
      try {
        clientFrameCount += 1;
        if (clientFrameCount > 32) {
          throw new Error("sequence-gap WebSocket sent more than 32 routed command frames");
        }
        if (typeof message === "string") throw new Error("sequence-gap WebSocket sent a text frame");
        const subscribe = decodeSearchSubscribeCommand(message);
        if (subscribe !== undefined && subscribe.subscriptions.length !== 1) {
          throw new Error("sequence-gap WebSocket sent a non-singleton subscribe command");
        }
        const subscription = subscribe?.subscriptions[0];
        if (subscription !== undefined) {
          subscribeCommandCount += 1;
          if (connectionOrdinal === 1) {
            if (initialSubscription !== undefined || subscription.afterSequence !== 0n) {
              throw new Error("sequence-gap initial subscription was duplicated or resumed");
            }
            initialSubscription = subscription;
            subscriptionID = subscription.subscriptionID;
            routedJobID = subscription.searchJobID;
          } else {
            if (
              checkpoint === undefined
              || subscriptionID === undefined
              || routedJobID === undefined
              || initialSubscription === undefined
              || reconnectRequestID !== undefined
              || subscribe === undefined
            ) {
              throw new Error("sequence-gap reconnect preceded its exact checkpoint");
            }
            assertRetainedSubscription(
              subscription,
              initialSubscription,
              checkpoint,
              "sequence-gap reconnect",
            );
            reconnectRequestID = subscribe.requestID;
          }
        }
        server.send(message);
      } catch (error) {
        fail(error);
      }
    });

    client.onClose((code, reason) => {
      if (disposed) {
        if (!forwardedCloseConnections.has(connectionOrdinal)) {
          forwardedCloseConnections.add(connectionOrdinal);
          void server.close({ code, reason }).catch(() => undefined);
        }
        return;
      }
      try {
        if (
          connectionOrdinal === 2
          && code === 1000
          && (reason === undefined || reason === "" || reason === "Client disposed")
        ) {
          if (terminalCloseCount === 0) {
            terminalCloseCount = 1;
            forwardedCloseConnections.add(connectionOrdinal);
            void server.close({ code, reason })
              .then(() => {
                terminalCloseForwardCompleted = true;
                if (timer !== undefined) {
                  clearTimeout(timer);
                  timer = undefined;
                }
                if (!terminalCloseSettled) {
                  terminalCloseSettled = true;
                  resolveTerminalClose();
                }
              })
              .catch(fail);
            return;
          }
          if (terminalCloseEchoCount !== 0) {
            throw new Error("sequence-gap terminal close produced more than one forwarded echo");
          }
          terminalCloseEchoCount = 1;
          return;
        }
        if (connectionOrdinal === 1 && gapCloseCount === 1 && code === 4000) {
          if (gapCloseEchoCount !== 0) {
            throw new Error("sequence-gap close produced more than one forwarded echo");
          }
          gapCloseEchoCount += 1;
          return;
        }
        if (
          connectionOrdinal !== 1
          || gapCloseCount !== 0
          || !gapStimulusSent
          || code !== 4000
          || (
            reason !== undefined
            && reason !== ""
            && reason !== "Sequence gap; replay required"
          )
        ) {
          throw new Error(
            `unexpected sequence-gap client close on connection ${connectionOrdinal}: ${String(code)} ${JSON.stringify(reason)}`,
          );
        }
        gapCloseCount += 1;
        forwardedCloseConnections.add(connectionOrdinal);
        void server.close({ code, reason })
          .then(() => {
            gapCloseForwardCompleted = true;
            if (!gapCloseSettled) {
              gapCloseSettled = true;
              resolveGapClose();
            }
          })
          .catch(fail);
      } catch (error) {
        fail(error);
      }
    });

    server.onMessage((message) => {
      try {
        serverFrameCount += 1;
        if (serverFrameCount > 128) {
          throw new Error("sequence-gap WebSocket received more than 128 routed event frames");
        }
        if (typeof message === "string") throw new Error("sequence-gap WebSocket received a text frame");
        const frame = Buffer.from(message);
        const event = SearchWebSocketEvent.decode(frame);
        const target = event.target?.target;
        const matchesSubscription = event.sequence > 0n
          && subscriptionID !== undefined
          && routedJobID !== undefined
          && event.subscriptionId === subscriptionID
          && target?.$case === "searchJobId"
          && target.value === routedJobID;

        if (connectionOrdinal === 1) {
          if (
            matchesSubscription
            && runningStateFrame === undefined
            && event.payload?.$case === "searchStateChanged"
            && event.payload.value.state === SearchJobState.SEARCH_JOB_STATE_RUNNING
          ) {
            runningStateFrame = frame;
          }
          if (matchesSubscription && checkpoint !== undefined) {
            if (originalFrames.size >= 2) {
              throw new Error("sequence-gap connection published more than K+1 and K+2 before reconnect");
            }
            const expectedSequence = checkpoint + BigInt(originalFrames.size + 1);
            const expectedProgressRows = BigInt(originalFrames.size + 1);
            if (
              event.sequence !== expectedSequence
              || event.payload?.$case !== "searchProgress"
              || event.payload.value.scannedRows !== expectedProgressRows
              || event.payload.value.stateVersion <= 0n
            ) {
              throw new Error(
                `sequence-gap stimulus ${event.sequence.toString()} was not progress ${expectedProgressRows.toString()} at ${expectedSequence.toString()}`,
              );
            }
            if (originalFrames.size === 1) {
              const firstFrame = originalFrames.get(checkpoint + 1n);
              const firstEvent = firstFrame === undefined
                ? undefined
                : SearchWebSocketEvent.decode(firstFrame);
              if (
                firstEvent?.payload?.$case !== "searchProgress"
                || event.payload.value.stateVersion !== firstEvent.payload.value.stateVersion + 1n
              ) {
                throw new Error("sequence-gap progress revisions K+1 and K+2 were not contiguous");
              }
            }
            originalFrames.set(event.sequence, frame);
            if (originalFrames.size === 1) return;
            gapStimulusSent = true;
            connectionOneDeliveredSequences.push(event.sequence);
            client.send(message);
            return;
          }
          client.send(message);
          if (
            matchesSubscription
            && event.payload?.$case === "resultPreview"
            && event.payload.value.rows.length > 0
          ) {
            checkpoint = event.sequence;
            checkpointFrame = frame;
            if (!checkpointSettled) {
              checkpointSettled = true;
              resolveCheckpoint();
            }
          }
          return;
        }

        if (event.payload?.$case === "resynchronizationRequired") {
          throw new Error(
            `sequence gap unexpectedly required authoritative resynchronization: ${event.payload.value.reason}`,
          );
        }
        if (event.payload?.$case === "protocolError") {
          throw new Error(`sequence-gap subscription received protocol error ${event.payload.value.code}`);
        }
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgment = event.payload.value;
          if (
            reconnectAcknowledged
            || reconnectRequestID === undefined
            || checkpoint === undefined
            || subscriptionID === undefined
            || routedJobID === undefined
            || event.sequence !== 0n
            || event.subscriptionId !== subscriptionID
            || acknowledgment.requestId !== reconnectRequestID
            || acknowledgment.subscriptionId !== subscriptionID
            || acknowledgment.target?.target?.$case !== "searchJobId"
            || acknowledgment.target.target.value !== routedJobID
            || !acknowledgment.replayWillFollow
            || acknowledgment.earliestAvailableSequence > checkpoint + 1n
            || acknowledgment.latestSequence !== checkpoint + 2n
          ) {
            throw new Error("sequence-gap reconnect acknowledgment was invalid");
          }
          reconnectAcknowledged = true;
          client.send(message);
          return;
        }
        if (matchesSubscription) {
          if (checkpoint === undefined || !reconnectAcknowledged) {
            throw new Error("sequence-gap reconnect delivered a target frame before acknowledgment");
          }
          if (replayFrames.size < 2) {
            const expectedSequence = checkpoint + BigInt(replayFrames.size + 1);
            const original = originalFrames.get(expectedSequence);
            if (
              event.sequence !== expectedSequence
              || original === undefined
              || !frame.equals(original)
            ) {
              throw new Error(
                `sequence-gap replay frame ${event.sequence.toString()} was not the byte-identical ${expectedSequence.toString()} frame`,
              );
            }
            replayFrames.set(event.sequence, frame);
            lastUpstreamSequence = event.sequence;
            if (replayFrames.size === 2 && !replayReadySettled) {
              replayReadySettled = true;
              resolveReplayReady();
            }
            return;
          }
          if (replayReleaseCount !== 2 || lastUpstreamSequence === undefined) {
            throw new Error("sequence-gap live target frame arrived before replay release completed");
          }
          if (event.sequence !== lastUpstreamSequence + 1n) {
            throw new Error(
              `sequence-gap live sequence = ${event.sequence.toString()}, want ${(lastUpstreamSequence + 1n).toString()}`,
            );
          }
          lastUpstreamSequence = event.sequence;
          if (
            event.payload?.$case === "searchStateChanged"
            && event.payload.value.state === SearchJobState.SEARCH_JOB_STATE_COMPLETED
          ) {
            if (
              completedStateVersion !== undefined
              || event.payload.value.stateVersion <= 0n
            ) {
              throw new Error("sequence-gap completed state revision was invalid");
            }
            completedStateVersion = event.payload.value.stateVersion;
          }
          if (
            withholdTerminalProjection
            && (
              withholdingTerminalProjection
              || (
                event.payload?.$case === "searchStateChanged"
                && event.payload.value.state === SearchJobState.SEARCH_JOB_STATE_COMPLETED
              )
            )
          ) {
            withholdingTerminalProjection = true;
            const expectedPayloads = [
              "searchStateChanged",
              "searchProgress",
              "searchTerminal",
            ] as const;
            const expectedPayload =
              expectedPayloads[withheldTerminalProjectionFrameCount];
            if (expectedPayload === undefined || event.payload?.$case !== expectedPayload) {
              throw new Error(
                `sequence-gap terminal projection frame ${withheldTerminalProjectionFrameCount + 1} was ${String(event.payload?.$case)}, want ${String(expectedPayload)}`,
              );
            }
            withheldTerminalProjectionFrameCount += 1;
            if (event.payload.$case === "searchStateChanged") {
              if (
                event.payload.value.searchJobId !== routedJobID
                || event.payload.value.state
                !== SearchJobState.SEARCH_JOB_STATE_COMPLETED
                || event.payload.value.stateVersion <= 0n
              ) {
                throw new Error("sequence-gap withheld completed state was invalid");
              }
              withheldTerminalStateVersion = event.payload.value.stateVersion;
            } else if (event.payload.$case === "searchProgress") {
              if (
                event.payload.value.phase
                !== SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE
                || event.payload.value.scannedRows !== 5n
                || event.payload.value.producedRows !== 1n
                || event.payload.value.stateVersion !== withheldTerminalStateVersion
              ) {
                throw new Error("sequence-gap withheld final progress was invalid");
              }
            } else if (event.payload.$case === "searchTerminal") {
              if (
                event.payload.value.searchJobId !== routedJobID
                || event.payload.value.state
                !== SearchJobState.SEARCH_JOB_STATE_COMPLETED
                || event.payload.value.stateVersion !== withheldTerminalStateVersion
                || event.payload.value.finalProgress?.phase
                !== SearchExecutionPhase.SEARCH_EXECUTION_PHASE_COMPLETE
                || event.payload.value.finalProgress.scannedRows !== 5n
                || event.payload.value.finalProgress.producedRows !== 1n
                || event.payload.value.finalProgress.stateVersion
                !== withheldTerminalStateVersion
              ) {
                throw new Error("sequence-gap withheld terminal event was invalid");
              }
              if (!terminalProjectionSettled) {
                terminalProjectionSettled = true;
                resolveTerminalProjection();
              }
            }
            return;
          }
          liveTargetFrameCount += 1;
        }
        client.send(message);
      } catch (error) {
        fail(error);
      }
    });
  });
  if (injectStaleDuplicates) {
    // Register after routeWebSocket so the recorder wraps Playwright's
    // page-side routed socket rather than its native upstream connection.
    await installBrowserWebSocketFrameRecorder(page, expectedOrigin);
  }

  timer = setTimeout(
    () => fail(new Error(
      "timed out waiting for deterministic sequence-gap replay"
      + ` (browser_sockets=${matchingBrowserSocketCount}`
      + ` stale_checkpoint_injected=${staleCheckpointInjectionCount}`
      + ` stale_running_injected=${staleRunningStateInjectionCount}`
      + ` browser_frames=${JSON.stringify(browserFrameDiagnostics)})`,
    )),
    timeoutMilliseconds,
  );

  async function injectStaleBrowserFrame(frame: Buffer, label: string): Promise<void> {
    if (frame.byteLength > maximumRecordedBrowserFrameBytes) {
      throw new Error(
        `browser ${label} exceeded ${maximumRecordedBrowserFrameBytes} evidence bytes`,
      );
    }
    const expectedFrame = frame.toString("base64");
    const baseline = await page.evaluate((expected) => {
      const recorder =
        (window as BrowserWebSocketFrameRecorderWindow)
          .openSplunkBrowserWebSocketFrameRecorder;
      if (recorder === undefined) {
        return { frameOverflow: -1, matches: -1, socketOverflow: -1 };
      }
      return {
        frameOverflow: recorder.frameOverflow(),
        matches: recorder.frames.filter((candidate) => candidate === expected).length,
        socketOverflow: recorder.socketOverflow(),
      };
    }, expectedFrame);
    if (
      baseline.matches < 0
      || baseline.frameOverflow !== 0
      || baseline.socketOverflow !== 0
      || secondClient === undefined
    ) {
      throw new Error(
        `browser ${label} injection was invalid: ${JSON.stringify(baseline)}`,
      );
    }
    secondClient.send(frame);
    await expect.poll(
      () => page.evaluate((expected) => {
        const recorder =
          (window as BrowserWebSocketFrameRecorderWindow)
            .openSplunkBrowserWebSocketFrameRecorder;
        if (recorder === undefined) {
          return { frameOverflow: -1, matches: 0, socketOverflow: -1 };
        }
        return {
          frameOverflow: recorder.frameOverflow(),
          matches: recorder.frames.filter((candidate) => candidate === expected).length,
          socketOverflow: recorder.socketOverflow(),
        };
      }, expectedFrame),
      {
        message: `browser receipt of ${label}`,
        timeout: Math.min(timeoutMilliseconds, 5_000),
      },
    ).toEqual({
      frameOverflow: 0,
      matches: baseline.matches + 1,
      socketOverflow: 0,
    });
  }

  return {
    waitForCheckpoint(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("sequence-gap browser job was already selected");
      expectedJobID = searchJobID;
      if (routedJobID !== undefined && routedJobID !== expectedJobID) {
        fail(
          new Error(
            `sequence-gap routed job ${routedJobID} did not match created job ${expectedJobID}`,
          ),
        );
      }
      return checkpointReady;
    },
    droppedFrameCount() {
      return originalFrames.size;
    },
    waitForReplayReady() {
      if (failure !== undefined) return Promise.reject(failure);
      return Promise.all([gapCloseReady, replayReady]).then(() => undefined);
    },
    replayProgress(index) {
      if (failure !== undefined) throw failure;
      if (checkpoint === undefined || replayFrames.size !== 2) {
        throw new Error("sequence-gap replay is not ready for progress inspection");
      }
      const sequence = checkpoint + BigInt(index);
      const frame = replayFrames.get(sequence);
      if (frame === undefined) {
        throw new Error(`sequence-gap replay ${sequence.toString()} is missing`);
      }
      const event = SearchWebSocketEvent.decode(frame);
      if (event.payload?.$case !== "searchProgress") {
        throw new Error(`sequence-gap replay ${sequence.toString()} is not progress`);
      }
      return event.payload.value;
    },
    releaseNextReplayFrame() {
      if (failure !== undefined) throw failure;
      if (
        checkpoint === undefined
        || secondClient === undefined
        || replayFrames.size !== 2
        || replayReleaseCount >= 2
      ) {
        throw new Error("sequence-gap replay is not ready for staged release");
      }
      const sequence = checkpoint + BigInt(replayReleaseCount + 1);
      const frame = replayFrames.get(sequence);
      if (frame === undefined) throw new Error(`sequence-gap replay ${sequence.toString()} is missing`);
      replayReleaseCount += 1;
      secondClient.send(frame);
      return sequence;
    },
    waitForReplayFrameReceived(sequence) {
      if (failure !== undefined) return Promise.reject(failure);
      if (
        checkpoint === undefined
        || sequence < checkpoint + 1n
        || sequence > checkpoint + 2n
      ) {
        return Promise.reject(
          new Error(`sequence-gap replay receipt ${sequence.toString()} is outside K+1..K+2`),
        );
      }
      if (replayBrowserFrameReceipts.has(sequence)) return Promise.resolve();
      if (replayBrowserFrameWaiters.has(sequence)) {
        return Promise.reject(
          new Error(`sequence-gap replay receipt ${sequence.toString()} already has a waiter`),
        );
      }
      return new Promise<void>((resolve, reject) => {
        replayBrowserFrameWaiters.set(sequence, { resolve, reject });
      });
    },
    async injectStaleCheckpointDuplicate() {
      if (
        failure !== undefined
        || !injectStaleDuplicates
        || checkpoint === undefined
        || checkpointFrame === undefined
        || replayReleaseCount !== 2
        || lastUpstreamSequence !== checkpoint + 2n
        || staleCheckpointInjectionCount !== 0
      ) {
        throw failure
          ?? new Error("sequence-gap stale checkpoint duplicate is not ready for injection");
      }
      await injectStaleBrowserFrame(
        checkpointFrame,
        "stale checkpoint frame",
      );
      staleCheckpointInjectionCount = 1;
      return checkpoint;
    },
    async injectStaleRunningStateDuplicate() {
      if (
        failure !== undefined
        || !injectStaleDuplicates
        || runningStateFrame === undefined
        || staleCheckpointInjectionCount !== 1
        || staleRunningStateInjectionCount !== 0
      ) {
        throw failure
          ?? new Error("sequence-gap stale running-state duplicate is not ready for injection");
      }
      const event = SearchWebSocketEvent.decode(runningStateFrame);
      if (
        event.sequence <= 0n
        || event.payload?.$case !== "searchStateChanged"
        || event.payload.value.state !== SearchJobState.SEARCH_JOB_STATE_RUNNING
        || lastUpstreamSequence === undefined
        || event.sequence >= lastUpstreamSequence
        || completedStateVersion === undefined
        || event.payload.value.stateVersion >= completedStateVersion
      ) {
        throw new Error("captured stale running-state frame is invalid");
      }
      await injectStaleBrowserFrame(
        runningStateFrame,
        "stale running-state frame",
      );
      staleRunningStateInjectionCount = 1;
      return event.sequence;
    },
    waitForTerminalProjectionWithheld() {
      if (!withholdTerminalProjection) {
        return Promise.reject(new Error("sequence-gap terminal projection is not being withheld"));
      }
      if (failure !== undefined) return Promise.reject(failure);
      return terminalProjectionReady;
    },
    withheldTerminalProjectionFrameCount() {
      return withheldTerminalProjectionFrameCount;
    },
    waitForTerminalClose() {
      if (failure !== undefined) return Promise.reject(failure);
      return terminalCloseReady;
    },
    connectionCount() {
      return matchingConnectionCount;
    },
    liveFrameCount() {
      return liveTargetFrameCount;
    },
    assertHealthy() {
      if (failure !== undefined) throw failure;
      if (
        expectedJobID === undefined
        || routedJobID !== expectedJobID
        || checkpoint === undefined
        || subscribeCommandCount !== 2
        || matchingConnectionCount !== 2
        || matchingBrowserSocketCount !== 2
        || originalFrames.size !== 2
        || connectionOneDeliveredSequences.length !== 1
        || connectionOneDeliveredSequences[0] !== checkpoint + 2n
        || !gapStimulusSent
        || gapCloseCount !== 1
        || gapCloseEchoCount > 1
        || !gapCloseForwardCompleted
        || terminalCloseCount !== 1
        || terminalCloseEchoCount > 1
        || !terminalCloseForwardCompleted
        || !reconnectAcknowledged
        || replayFrames.size !== 2
        || replayReleaseCount !== 2
        || (
          injectStaleDuplicates
          && (
            checkpointFrame === undefined
            || runningStateFrame === undefined
            || staleCheckpointInjectionCount !== 1
            || staleRunningStateInjectionCount !== 1
            || completedStateVersion === undefined
          )
        )
        || liveTargetFrameCount === 0
        || (
          withholdTerminalProjection
            ? withheldTerminalProjectionFrameCount !== 3
              || !terminalProjectionSettled
              || withheldTerminalStateVersion === undefined
            : withheldTerminalProjectionFrameCount !== 0
        )
      ) {
        throw new Error("sequence-gap observation did not complete its exact contract");
      }
    },
    dispose() {
      disposed = true;
      if (timer !== undefined) clearTimeout(timer);
      page.off("websocket", observeBrowserSocket);
      if (replayBrowserSocket !== undefined && replayBrowserFrameListener !== undefined) {
        replayBrowserSocket.off("framereceived", replayBrowserFrameListener);
      }
      for (const waiter of replayBrowserFrameWaiters.values()) waiter.resolve();
      replayBrowserFrameWaiters.clear();
      if (!checkpointSettled) {
        checkpointSettled = true;
        resolveCheckpoint();
      }
      if (!replayReadySettled) {
        replayReadySettled = true;
        resolveReplayReady();
      }
      if (!gapCloseSettled) {
        gapCloseSettled = true;
        resolveGapClose();
      }
      if (!terminalProjectionSettled) {
        terminalProjectionSettled = true;
        resolveTerminalProjection();
      }
      if (!terminalCloseSettled) {
        terminalCloseSettled = true;
        resolveTerminalClose();
      }
    },
  };
}

async function interceptSequenceExpiration(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
): Promise<SequenceExpirationObservation> {
  let expectedJobID: string | undefined;
  let routedJobID: string | undefined;
  let subscriptionID: string | undefined;
  let checkpoint: bigint | undefined;
  let initialSubscription: ObservedSubscription | undefined;
  let latestTargetSequence: bigint | undefined;
  let firstServer: WebSocketRoute | undefined;
  const reconnectRequestIDs = new Map<number, string>();
  const acknowledgedConnections = new Set<number>();
  const resynchronizedConnections = new Set<number>();
  const lastConnectionSequences = new Map<number, bigint>();
  let matchingConnectionCount = 0;
  let clientFrameCount = 0;
  let serverFrameCount = 0;
  let heldRecoveryTargetFrames = 0;
  let recoveredTargetFrames = 0;
  let failure: Error | undefined;
  let checkpointSettled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const withheldFrames = new Map<bigint, Buffer>();
  let resolveCheckpoint!: () => void;
  let rejectCheckpoint!: (reason: Error) => void;
  const checkpointReady = new Promise<void>((resolve, reject) => {
    resolveCheckpoint = resolve;
    rejectCheckpoint = reject;
  });
  void checkpointReady.catch(() => undefined);
  const resynchronizationWaiters = new Set<{
    count: number;
    resolve: () => void;
    reject: (reason: Error) => void;
  }>();

  const settleResynchronizationWaiters = (): void => {
    for (const waiter of resynchronizationWaiters) {
      if (resynchronizedConnections.size < waiter.count) continue;
      resynchronizationWaiters.delete(waiter);
      waiter.resolve();
    }
  };

  const fail = (error: unknown): void => {
    const normalized = normalizeError(error);
    failure ??= normalized;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (!checkpointSettled) {
      checkpointSettled = true;
      rejectCheckpoint(normalized);
    }
    for (const waiter of resynchronizationWaiters) {
      waiter.reject(normalized);
    }
    resynchronizationWaiters.clear();
  };
  const routeMatchesSearchSocket = (url: URL): boolean =>
    matchesSearchWebSocketURL(url, expectedOrigin);

  await page.routeWebSocket(routeMatchesSearchSocket, (client) => {
    matchingConnectionCount += 1;
    const connectionOrdinal = matchingConnectionCount;
    if (connectionOrdinal > 4) {
      fail(new Error(`search WebSocket opened ${connectionOrdinal} matching connections`));
      rejectExcessWebSocketRoute(client);
      return;
    }
    const server = client.connectToServer();
    if (connectionOrdinal === 1) firstServer = server;

    client.onMessage((message) => {
      try {
        clientFrameCount += 1;
        if (clientFrameCount > 32) {
          throw new Error("search WebSocket sent more than 32 routed command frames");
        }
        if (typeof message === "string") throw new Error("search WebSocket sent a text frame");
        const subscribe = decodeSearchSubscribeCommand(message);
        if (subscribe !== undefined && subscribe.subscriptions.length !== 1) {
          throw new Error("search WebSocket sent a non-singleton subscribe command");
        }
        const subscription = subscribe?.subscriptions[0];
        if (subscription !== undefined) {
          if (connectionOrdinal === 1) {
            if (initialSubscription !== undefined) {
              throw new Error("initial WebSocket sent more than one subscribe command");
            }
            if (subscription.afterSequence !== 0n) {
              throw new Error(
                `initial subscription started after sequence ${subscription.afterSequence.toString()}`,
              );
            }
            initialSubscription = subscription;
            subscriptionID = subscription.subscriptionID;
            routedJobID = subscription.searchJobID;
          } else {
            if (
              checkpoint === undefined
              || subscriptionID === undefined
              || routedJobID === undefined
              || initialSubscription === undefined
              || subscribe === undefined
            ) {
              throw new Error("expiration reconnect preceded its browser checkpoint");
            }
            if (reconnectRequestIDs.has(connectionOrdinal)) {
              throw new Error(
                `expiration connection ${connectionOrdinal} sent more than one subscribe command`,
              );
            }
            assertRetainedSubscription(
              subscription,
              initialSubscription,
              checkpoint,
              `expiration connection ${connectionOrdinal}`,
            );
            reconnectRequestIDs.set(connectionOrdinal, subscribe.requestID);
          }
        }
        server.send(message);
      } catch (error) {
        fail(error);
      }
    });

    server.onMessage((message) => {
      try {
        serverFrameCount += 1;
        if (serverFrameCount > 128) {
          throw new Error("search WebSocket received more than 128 routed event frames");
        }
        if (typeof message === "string") throw new Error("search WebSocket received a text frame");
        const frame = Buffer.from(message);
        const event = SearchWebSocketEvent.decode(frame);
        const target = event.target?.target;
        const matchesSubscription = event.sequence > 0n
          && subscriptionID !== undefined
          && routedJobID !== undefined
          && event.subscriptionId === subscriptionID
          && target?.$case === "searchJobId"
          && target.value === routedJobID;

        if (connectionOrdinal === 1) {
          if (matchesSubscription && checkpoint !== undefined) {
            const previous = Array.from(withheldFrames.keys()).at(-1) ?? checkpoint;
            if (event.sequence !== previous + 1n) {
              throw new Error(
                `withheld expiration sequence = ${event.sequence.toString()}, want ${(previous + 1n).toString()}`,
              );
            }
            const expectedScannedRows = BigInt(withheldFrames.size + 1);
            if (
              event.payload?.$case !== "searchProgress"
              || event.payload.value.scannedRows !== expectedScannedRows
            ) {
              throw new Error(
                `withheld expiration frame ${event.sequence.toString()} was not progress ${expectedScannedRows.toString()}`,
              );
            }
            withheldFrames.set(event.sequence, frame);
            latestTargetSequence = event.sequence;
            return;
          }
          client.send(message);
          if (
            matchesSubscription
            && event.payload?.$case === "resultPreview"
            && event.payload.value.rows.length > 0
          ) {
            checkpoint = event.sequence;
            latestTargetSequence = event.sequence;
            if (!checkpointSettled) {
              checkpointSettled = true;
              resolveCheckpoint();
            }
          }
          return;
        }

        const reconnectRequestID = reconnectRequestIDs.get(connectionOrdinal);
        const expectedLatestSequence = latestTargetSequence;
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgment = event.payload.value;
          if (
            acknowledgedConnections.has(connectionOrdinal)
            || reconnectRequestID === undefined
            || subscriptionID === undefined
            || routedJobID === undefined
            || expectedLatestSequence === undefined
            || event.sequence !== 0n
            || event.subscriptionId !== subscriptionID
            || acknowledgment.requestId !== reconnectRequestID
            || acknowledgment.subscriptionId !== subscriptionID
            || acknowledgment.target?.target?.$case !== "searchJobId"
            || acknowledgment.target.target.value !== routedJobID
            || acknowledgment.replayWillFollow
            || acknowledgment.earliestAvailableSequence !== expectedLatestSequence
            || acknowledgment.latestSequence !== expectedLatestSequence
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} acknowledgment was invalid`,
            );
          }
          acknowledgedConnections.add(connectionOrdinal);
        } else if (event.payload?.$case === "resynchronizationRequired") {
          const required = event.payload.value;
          if (
            !acknowledgedConnections.has(connectionOrdinal)
            || resynchronizedConnections.has(connectionOrdinal)
            || subscriptionID === undefined
            || routedJobID === undefined
            || expectedLatestSequence === undefined
            || event.sequence !== 0n
            || event.subscriptionId !== subscriptionID
            || event.target?.target?.$case !== "searchJobId"
            || event.target.target.value !== routedJobID
            || required.subscriptionId !== subscriptionID
            || required.target?.target?.$case !== "searchJobId"
            || required.target.target.value !== routedJobID
            || required.reason
              !== ResynchronizationReason.RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
            || required.earliestAvailableSequence !== expectedLatestSequence
            || required.latestSequence !== expectedLatestSequence
            || required.recoveryPath !== "/api/search/jobs/get"
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} resynchronization frame was invalid`,
            );
          }
          resynchronizedConnections.add(connectionOrdinal);
          lastConnectionSequences.set(connectionOrdinal, expectedLatestSequence);
        } else if (matchesSubscription) {
          const previousSequence = lastConnectionSequences.get(connectionOrdinal);
          if (
            !resynchronizedConnections.has(connectionOrdinal)
            || previousSequence === undefined
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} delivered a target frame before resynchronization`,
            );
          }
          if (
            event.sequence !== previousSequence + 1n
            || latestTargetSequence === undefined
            || event.sequence !== latestTargetSequence + 1n
          ) {
            throw new Error(
              `expiration connection ${connectionOrdinal} sequence = ${event.sequence.toString()}, want ${(previousSequence + 1n).toString()}`,
            );
          }
          lastConnectionSequences.set(connectionOrdinal, event.sequence);
          latestTargetSequence = event.sequence;
          if (connectionOrdinal === 2) {
            if (
              event.payload?.$case !== "searchProgress"
              || event.payload.value.scannedRows !== 3n
            ) {
              throw new Error("the held-recovery target frame was not progress for three rows");
            }
            heldRecoveryTargetFrames += 1;
          }
          if (connectionOrdinal === 4) recoveredTargetFrames += 1;
        }
        client.send(message);
        if (event.payload?.$case === "resynchronizationRequired") {
          if (
            expectedJobID !== undefined
            && routedJobID !== expectedJobID
          ) {
            throw new Error(
              `routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`,
            );
          }
          settleResynchronizationWaiters();
        }
      } catch (error) {
        fail(error);
      }
    });
  });

  timer = setTimeout(
    () => fail(new Error("timed out waiting for real WebSocket sequence expiration")),
    timeoutMilliseconds,
  );
  return {
    waitForCheckpoint(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("browser search job was already selected");
      expectedJobID = searchJobID;
      if (routedJobID !== undefined && routedJobID !== expectedJobID) {
        fail(
          new Error(
            `routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`,
          ),
        );
      }
      return checkpointReady;
    },
    withheldFrameCount() {
      return withheldFrames.size;
    },
    async disconnect() {
      if (
        firstServer === undefined
        || checkpoint === undefined
        || withheldFrames.size !== 2
        || Array.from(withheldFrames.keys())[0] !== checkpoint + 1n
        || Array.from(withheldFrames.keys())[1] !== checkpoint + 2n
      ) {
        throw new Error("sequence-expiration disruption did not capture exactly K+1 and K+2");
      }
      await firstServer.close({
        code: 4000,
        reason: "deterministic sequence-expiration checkpoint",
      });
    },
    waitForResynchronizations(count) {
      if (!Number.isSafeInteger(count) || count < 1 || count > 3) {
        return Promise.reject(new RangeError("resynchronization count must be between 1 and 3"));
      }
      if (failure !== undefined) return Promise.reject(failure);
      if (resynchronizedConnections.size >= count) return Promise.resolve();
      return new Promise<void>((resolve, reject) => {
        resynchronizationWaiters.add({ count, resolve, reject });
      });
    },
    heldRecoveryFrameCount() {
      return heldRecoveryTargetFrames;
    },
    connectionCount() {
      return matchingConnectionCount;
    },
    postRecoveryFrameCount() {
      return recoveredTargetFrames;
    },
    assertHealthy() {
      if (failure !== undefined) throw failure;
      if (
        reconnectRequestIDs.size !== 3
        || acknowledgedConnections.size !== 3
        || resynchronizedConnections.size !== 3
        || matchingConnectionCount !== 4
        || withheldFrames.size !== 2
        || heldRecoveryTargetFrames !== 1
        || recoveredTargetFrames < 1
      ) {
        throw new Error("sequence-expiration observation did not complete its exact contract");
      }
    },
    dispose() {
      if (timer !== undefined) clearTimeout(timer);
      if (!checkpointSettled) {
        checkpointSettled = true;
        resolveCheckpoint();
      }
      for (const waiter of resynchronizationWaiters) {
        waiter.resolve();
      }
      resynchronizationWaiters.clear();
    },
  };
}

async function interceptOneRetainedReplay(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
): Promise<RetainedReplayObservation> {
  let expectedJobID: string | undefined;
  let routedJobID: string | undefined;
  let subscriptionID: string | undefined;
  let checkpoint: bigint | undefined;
  let withheldTerminalSequence: bigint | undefined;
  let reconnectRequestID: string | undefined;
  let reconnectAcknowledged = false;
  let initialSubscription: ObservedSubscription | undefined;
  const withheldFrames = new Map<bigint, Buffer>();
  let replayVerified = false;
  let terminalReplayed = false;
  let lastReplaySequence: bigint | undefined;
  let firstServer: WebSocketRoute | undefined;
  let connectionCount = 0;
  let clientFrameCount = 0;
  let serverFrameCount = 0;
  let checkpointSettled = false;
  let completionSettled = false;
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let resolveCheckpoint!: () => void;
  let rejectCheckpoint!: (reason: Error) => void;
  let resolveCompletion!: () => void;
  let rejectCompletion!: (reason: Error) => void;
  const checkpointReady = new Promise<void>((resolve, reject) => {
    resolveCheckpoint = resolve;
    rejectCheckpoint = reject;
  });
  const completion = new Promise<void>((resolve, reject) => {
    resolveCompletion = resolve;
    rejectCompletion = reject;
  });
  void checkpointReady.catch(() => undefined);
  void completion.catch(() => undefined);

  const finish = (error?: Error): void => {
    if (settled) return;
    settled = true;
    if (timer !== undefined) clearTimeout(timer);
    if (!checkpointSettled) {
      checkpointSettled = true;
      if (error) rejectCheckpoint(error);
      else resolveCheckpoint();
    }
    if (!completionSettled) {
      completionSettled = true;
      if (error) rejectCompletion(error);
      else resolveCompletion();
    }
  };

  const checkCompletion = (): void => {
    if (!replayVerified || !terminalReplayed || !expectedJobID || !routedJobID) return;
    if (routedJobID !== expectedJobID) {
      finish(new Error(`routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`));
      return;
    }
    finish();
  };

  const fail = (error: unknown): void => finish(normalizeError(error));
  const routeMatchesSearchSocket = (url: URL): boolean =>
    matchesSearchWebSocketURL(url, expectedOrigin);

  await page.routeWebSocket(routeMatchesSearchSocket, (client) => {
    connectionCount += 1;
    const connectionOrdinal = connectionCount;
    if (connectionOrdinal > 2) {
      fail(new Error(`search WebSocket opened ${connectionOrdinal} connections before retained replay completed`));
      rejectExcessWebSocketRoute(client);
      return;
    }
    const server = client.connectToServer();
    if (connectionOrdinal === 1) firstServer = server;

    client.onMessage((message) => {
      try {
        clientFrameCount += 1;
        if (clientFrameCount > 64) throw new Error("search WebSocket sent more than 64 routed command frames");
        if (typeof message === "string") throw new Error("search WebSocket sent a text frame");
        const subscribe = decodeSearchSubscribeCommand(message);
        if (subscribe !== undefined && subscribe.subscriptions.length !== 1) {
          throw new Error("search WebSocket sent more than one subscription in a reconnect command");
        }
        const subscription = subscribe?.subscriptions[0];
        if (subscription !== undefined) {
          if (connectionOrdinal === 1) {
            if (initialSubscription !== undefined) {
              throw new Error("initial WebSocket sent more than one subscribe command");
            }
            if (subscription.afterSequence !== 0n) {
              throw new Error(`initial subscription started after sequence ${subscription.afterSequence.toString()}`);
            }
            initialSubscription = subscription;
            subscriptionID = subscription.subscriptionID;
            routedJobID = subscription.searchJobID;
          } else {
            if (
              checkpoint === undefined
              || subscriptionID === undefined
              || routedJobID === undefined
              || initialSubscription === undefined
              || subscribe === undefined
            ) {
              throw new Error("reconnect subscribed before the disruption checkpoint was established");
            }
            if (reconnectRequestID !== undefined) {
              throw new Error("reconnect WebSocket sent more than one subscribe command");
            }
            assertRetainedSubscription(
              subscription,
              initialSubscription,
              checkpoint,
              "retained replay reconnect",
            );
            reconnectRequestID = subscribe.requestID;
            lastReplaySequence = checkpoint;
          }
        }
        server.send(message);
      } catch (error) {
        fail(error);
      }
    });

    server.onMessage((message) => {
      try {
        serverFrameCount += 1;
        if (serverFrameCount > 256) throw new Error("search WebSocket received more than 256 routed event frames");
        if (typeof message === "string") throw new Error("search WebSocket received a text frame");
        const frame = Buffer.from(message);
        const event = SearchWebSocketEvent.decode(frame);
        const target = event.target?.target;
        const matchesSubscription = event.sequence > 0n
          && subscriptionID !== undefined
          && routedJobID !== undefined
          && event.subscriptionId === subscriptionID
          && target?.$case === "searchJobId"
          && target.value === routedJobID;

        if (connectionOrdinal === 1) {
          if (matchesSubscription && checkpoint !== undefined) {
            const prior = withheldTerminalSequence
              ?? Array.from(withheldFrames.keys()).at(-1)
              ?? checkpoint;
            if (event.sequence !== prior + 1n) {
              throw new Error(
                `withheld sequence = ${event.sequence.toString()}, want ${(prior + 1n).toString()}`,
              );
            }
            withheldFrames.set(event.sequence, frame);
            if (event.payload?.$case === "searchTerminal") {
              withheldTerminalSequence = event.sequence;
              if (!checkpointSettled) {
                checkpointSettled = true;
                resolveCheckpoint();
              }
            }
            return;
          }
          client.send(message);
          if (
            matchesSubscription
            && event.payload?.$case === "resultPreview"
            && event.payload.value.rows.length > 0
          ) {
            checkpoint = event.sequence;
          }
          return;
        }

        if (event.payload?.$case === "resynchronizationRequired") {
          throw new Error(
            `retained replay unexpectedly required resynchronization: ${event.payload.value.reason}`,
          );
        }
        if (event.payload?.$case === "subscriptionAcknowledged") {
          const acknowledgment = event.payload.value;
          if (
            reconnectAcknowledged
            || reconnectRequestID === undefined
            || checkpoint === undefined
            || withheldTerminalSequence === undefined
            || acknowledgment.requestId !== reconnectRequestID
            || acknowledgment.subscriptionId !== subscriptionID
            || acknowledgment.target?.target?.$case !== "searchJobId"
            || acknowledgment.target.target.value !== routedJobID
            || !acknowledgment.replayWillFollow
            || acknowledgment.earliestAvailableSequence > checkpoint + 1n
            || acknowledgment.latestSequence !== withheldTerminalSequence
          ) {
            throw new Error("reconnect acknowledgment did not describe the withheld retained suffix");
          }
          reconnectAcknowledged = true;
        }
        if (matchesSubscription) {
          if (
            !reconnectAcknowledged
            || checkpoint === undefined
            || withheldTerminalSequence === undefined
            || withheldFrames.size === 0
          ) {
            throw new Error("reconnect delivered a target frame before the withheld replay was captured");
          }
          const previous = lastReplaySequence ?? checkpoint;
          if (event.sequence !== previous + 1n) {
            throw new Error(
              `replay sequence = ${event.sequence.toString()}, want ${(previous + 1n).toString()}`,
            );
          }
          const withheld = withheldFrames.get(event.sequence);
          if (withheld === undefined || !frame.equals(withheld)) {
            throw new Error(`retained replay frame ${event.sequence.toString()} was not byte-identical`);
          }
          lastReplaySequence = event.sequence;
          replayVerified = event.sequence === withheldTerminalSequence;
        }
        client.send(message);
        if (matchesSubscription && event.payload?.$case === "searchTerminal") {
          if (!replayVerified || event.sequence !== withheldTerminalSequence) {
            throw new Error("terminal arrived before the complete retained suffix was replayed");
          }
          terminalReplayed = true;
          checkCompletion();
        }
      } catch (error) {
        fail(error);
      }
    });
  });

  timer = setTimeout(
    () => finish(new Error("timed out waiting for a byte-identical retained WebSocket replay")),
    timeoutMilliseconds,
  );
  return {
    waitForCheckpoint(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("browser search job was already selected");
      expectedJobID = searchJobID;
      if (routedJobID !== undefined && routedJobID !== expectedJobID) {
        fail(new Error(`routed WebSocket job ${routedJobID} did not match created job ${expectedJobID}`));
      }
      return checkpointReady;
    },
    async disconnect() {
      if (
        firstServer === undefined
        || checkpoint === undefined
        || withheldTerminalSequence === undefined
        || withheldFrames.size === 0
      ) {
        throw new Error("retained replay disruption was requested before its checkpoint was ready");
      }
      await firstServer.close({
        code: 4000,
        reason: "deterministic retained replay checkpoint",
      });
    },
    waitForTerminalReplay() {
      return completion;
    },
    dispose() {
      finish();
    },
  };
}

function observeSearchProtocol(
  page: Page,
  expectedOrigin: string,
  timeoutMilliseconds: number,
  requireCompletedTerminal = false,
): SearchProtocolObservation {
  const expectedEventDescription = requireCompletedTerminal
    ? "sequenced search-terminal event"
    : "sequenced search-progress event";
  let expectedJobID: string | undefined;
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let resolveCompletion!: () => void;
  let rejectCompletion!: (reason: Error) => void;
  const subscriptions: ObservedSubscription[] = [];
  const progressEvents: ObservedProgress[] = [];
  const terminalEvents: ObservedSearchTerminal[] = [];
  const socketObservers = new BoundedObservationRegistry<WebSocket>();
  const completion = new Promise<void>((resolve, reject) => {
    resolveCompletion = resolve;
    rejectCompletion = reject;
  });
  // A socket can fail before the create response selects its job. Mark the
  // rejection handled immediately; callers still observe it from completion.
  void completion.catch(() => undefined);

  const finish = (error?: Error): void => {
    if (settled) return;
    settled = true;
    if (timer !== undefined) clearTimeout(timer);
    page.off("websocket", observeSocket);
    socketObservers.clear();
    if (error) rejectCompletion(normalizeError(error));
    else resolveCompletion();
  };

  const checkCompletion = (): void => {
    if (!expectedJobID) return;
    const subscription = subscriptions.find((candidate) => candidate.searchJobID === expectedJobID);
    if (!subscription) return;
    if (requireCompletedTerminal) {
      const terminal = terminalEvents.find((event) =>
        event.searchJobID === expectedJobID
        && event.subscriptionID === subscription.subscriptionID
        && event.sequence > 0n);
      if (!terminal) return;
      if (terminal.state !== SearchJobState.SEARCH_JOB_STATE_COMPLETED) {
        finish(new Error(
          `browser search ${terminal.searchJobID} terminated in state ${terminal.state}`
          + ` with failure code ${terminal.failureCode ?? "missing"}`
          + `: ${terminal.failureMessage ?? "no failure message"}`,
        ));
        return;
      }
      finish();
      return;
    }
    const progress = progressEvents.find((event) =>
      event.searchJobID === expectedJobID
      && event.subscriptionID === subscription.subscriptionID
      && event.sequence > 0n);
    if (progress) finish();
  };

  const observeSocket = (socket: WebSocket): void => {
    const socketURL = new URL(socket.url());
    if (!matchesSearchWebSocketURL(socketURL, expectedOrigin)) return;
    if (socketObservers.has(socket)) return;
    const listeners: SocketListeners = {
      sent: ({ payload }) => {
        try {
          if (typeof payload === "string") throw new Error("search WebSocket sent a text frame");
          subscriptions.push(...(decodeSearchSubscribeCommand(payload)?.subscriptions ?? []));
          if (subscriptions.length > 32) throw new Error("search WebSocket sent too many subscriptions");
          checkCompletion();
        } catch (error) {
          finish(normalizeError(error));
        }
      },
      received: ({ payload }) => {
        try {
          if (typeof payload === "string") throw new Error("search WebSocket received a text frame");
          const progress = decodeSearchProgress(payload);
          if (progress !== undefined) progressEvents.push(progress);
          if (progressEvents.length > 256) throw new Error("search WebSocket received too many progress events");
          const terminal = decodeSearchTerminal(payload);
          if (terminal !== undefined) terminalEvents.push(terminal);
          if (terminalEvents.length > 32) throw new Error("search WebSocket received too many terminal events");
          checkCompletion();
        } catch (error) {
          finish(normalizeError(error));
        }
      },
      error: (error) => finish(new Error(`search WebSocket failed: ${error}`)),
      close: () => finish(new Error(`search WebSocket closed before a ${expectedEventDescription} arrived`)),
    };
    if (!socketObservers.tryObserve(socket, () => {
      socket.on("framesent", listeners.sent);
      socket.on("framereceived", listeners.received);
      socket.on("socketerror", listeners.error);
      socket.on("close", listeners.close);
      return () => {
        socket.off("framesent", listeners.sent);
        socket.off("framereceived", listeners.received);
        socket.off("socketerror", listeners.error);
        socket.off("close", listeners.close);
      };
    })) {
      finish(
        new Error(
          "browser opened more than "
          + `${MAXIMUM_OBSERVED_MATCHING_WEBSOCKETS} simultaneously observed search WebSockets`,
        ),
      );
    }
  };

  timer = setTimeout(
    () => finish(new Error(`timed out waiting for the browser's protobuf ${expectedEventDescription}`)),
    timeoutMilliseconds,
  );
  page.on("websocket", observeSocket);
  return {
    waitForJob(searchJobID) {
      if (expectedJobID !== undefined) throw new Error("browser search job was already selected");
      expectedJobID = searchJobID;
      checkCompletion();
      return completion;
    },
    dispose() {
      finish();
    },
  };
}

function decodeCreateSearchJobID(payload: Uint8Array): string {
  const searchJobID = CreateSearchJobResponse.decode(payload).searchJob?.searchJobId.trim() ?? "";
  if (!searchJobID) throw new Error("CreateSearchJobResponse.search_job.search_job_id is empty");
  return searchJobID;
}

function decodeSearchResultsJobID(payload: Uint8Array): string {
  const searchJobID = GetSearchResultsResponse.decode(payload).searchJobId.trim();
  if (!searchJobID) throw new Error("GetSearchResultsResponse.search_job_id is empty");
  return searchJobID;
}

function decodeSearchSubscribeCommand(payload: Uint8Array): ObservedSubscribeCommand | undefined {
  const command = SearchWebSocketCommand.decode(payload);
  if (command.payload?.$case !== "subscribe") return undefined;
  if (!command.requestId) throw new Error("SearchWebSocketCommand.request_id is empty");
  return {
    requestID: command.requestId,
    subscriptions: command.payload.value.subscriptions.map((subscription) => {
      const target = subscription.target?.target;
      if (!subscription.subscriptionId) throw new Error("SearchSubscription.subscription_id is empty");
      if (target?.$case !== "searchJobId" || !target.value) {
        throw new Error("SearchSubscription.search_job_id is empty");
      }
      return {
        subscriptionID: subscription.subscriptionId,
        searchJobID: target.value,
        afterSequence: subscription.afterSequence,
        includePreviews: subscription.includePreviews,
        previewRowLimit: subscription.previewRowLimit,
      };
    }),
  };
}

function decodeSearchProgress(payload: Uint8Array): ObservedProgress | undefined {
  const event = SearchWebSocketEvent.decode(payload);
  if (event.payload?.$case !== "searchProgress") return undefined;
  const target = event.target?.target;
  if (!event.subscriptionId) throw new Error("SearchWebSocketEvent.subscription_id is empty");
  if (target?.$case !== "searchJobId" || !target.value) {
    throw new Error("search-progress event search_job_id is empty");
  }
  return {
    sequence: event.sequence,
    subscriptionID: event.subscriptionId,
    searchJobID: target.value,
  };
}

function decodeSearchTerminal(payload: Uint8Array): ObservedSearchTerminal | undefined {
  const event = SearchWebSocketEvent.decode(payload);
  if (event.payload?.$case !== "searchTerminal") return undefined;
  const target = event.target?.target;
  const terminal = event.payload.value;
  if (!event.subscriptionId) throw new Error("SearchWebSocketEvent.subscription_id is empty");
  if (target?.$case !== "searchJobId" || !target.value) {
    throw new Error("search-terminal event search_job_id is empty");
  }
  if (!terminal.searchJobId || terminal.searchJobId !== target.value) {
    throw new Error("search-terminal payload does not match its target");
  }
  return {
    sequence: event.sequence,
    subscriptionID: event.subscriptionId,
    searchJobID: terminal.searchJobId,
    state: terminal.state,
    failureCode: terminal.failure?.code,
    failureMessage: terminal.failure?.message,
  };
}

function normalizeError(error: unknown): Error {
  const message = error instanceof Error ? error.message : String(error);
  return new Error(boundedDiagnostic(message));
}

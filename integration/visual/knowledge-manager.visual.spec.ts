import { createHash } from "node:crypto";

import { expect, test, type Page } from "@playwright/test";

import { AppState } from "../../gen/ts/open_splunk/app";
import { SharingScope } from "../../gen/ts/open_splunk/common";
import { IndexAccessState, IndexState } from "../../gen/ts/open_splunk/index";
import { ListIndexesResponse } from "../../gen/ts/open_splunk/index_api";
import { ListIngestionTokensResponse } from "../../gen/ts/open_splunk/collector_admin_api";
import {
  KnowledgeObject,
  KnowledgeObjectDefinition,
  KnowledgeObjectState,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
} from "../../gen/ts/open_splunk/knowledge";
import { ListKnowledgeObjectsResponse } from "../../gen/ts/open_splunk/knowledge_api";
import { GetSystemBootstrapResponse, ServerFeature } from "../../gen/ts/open_splunk/system_api";

import { expectPageScreenshot, gotoVisualRoute, settleVisualPage } from "./visual-harness";
import { BACKEND_EXPORT_URL } from "./visual-servers";

/**
 * Appearance of the bootstrap-advertised Knowledge Manager.
 *
 * That surface owns its own section of `app/admin/admin.css` and only renders when
 * the connected server advertises the knowledge capability, so it is
 * unreachable from the demo export. This spec renders the backend-mode export
 * instead and supplies the two protobuf responses the panel needs; every other
 * administration route answers with an empty page so its own section stays
 * honestly empty rather than silently borrowing this fixture.
 */

test.use({ baseURL: BACKEND_EXPORT_URL });

const PROTOBUF_HEADERS = { "content-type": "application/x-protobuf" };
const APP_ID = "app-observability";
const CREATED_AT = new Date("2026-03-01T09:00:00.000Z");
const UPDATED_AT = new Date("2026-03-04T16:20:00.000Z");

function fieldAliasObject(id: string, name: string, version: bigint): KnowledgeObject {
  const definition = KnowledgeObjectDefinition.fromPartial({
    appId: APP_ID,
    name,
    description: "Normalizes the transport status field for downstream searches.",
    sharingScope: SharingScope.SHARING_SCOPE_APP,
    selector: {
      indexPatterns: [{
        matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
        value: "gradethis",
      }],
      sourcetypePatterns: [{
        matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
        value: "go:zap:json",
      }],
    },
    body: {
      $case: "fieldAlias",
      value: {
        sourceField: "status",
        destinationField: "http_status",
        overwriteBehavior: KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
      },
    },
  });
  return KnowledgeObject.fromPartial({
    appId: APP_ID,
    createdAt: CREATED_AT,
    definition,
    definitionSha256: createHash("sha256")
      .update(KnowledgeObjectDefinition.encode(definition).finish())
      .digest(),
    knowledgeObjectId: id,
    name,
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
    ownerId: "administrator",
    sharingScope: SharingScope.SHARING_SCOPE_APP,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
    tenantId: "tenant-local",
    updatedAt: UPDATED_AT,
    version,
  });
}

function regexExtractionObject(id: string, name: string, version: bigint): KnowledgeObject {
  const definition = KnowledgeObjectDefinition.fromPartial({
    appId: APP_ID,
    name,
    description: "Extracts the route and latency budget from the request log line.",
    sharingScope: SharingScope.SHARING_SCOPE_APP,
    selector: {
      indexPatterns: [{
        matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
        value: "gradethis",
      }],
    },
    body: {
      $case: "fieldExtraction",
      value: {
        inputField: "_raw",
        overwriteBehavior: KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
        extraction: {
          $case: "regex",
          value: {
            pattern: "route=(?<route>[^ ]+) budget_ms=(?<budget_ms>[0-9]+)",
            outputFields: ["route", "budget_ms"],
          },
        },
      },
    },
  });
  return KnowledgeObject.fromPartial({
    appId: APP_ID,
    createdAt: CREATED_AT,
    definition,
    definitionSha256: createHash("sha256")
      .update(KnowledgeObjectDefinition.encode(definition).finish())
      .digest(),
    knowledgeObjectId: id,
    name,
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
    ownerId: "administrator",
    sharingScope: SharingScope.SHARING_SCOPE_APP,
    state: KnowledgeObjectState.KNOWLEDGE_OBJECT_STATE_ACTIVE,
    tenantId: "tenant-local",
    updatedAt: UPDATED_AT,
    version,
  });
}

const KNOWLEDGE_OBJECTS: readonly KnowledgeObject[] = [
  fieldAliasObject("ko-http-status-alias", "http_status_alias", 2n),
  regexExtractionObject("ko-request-route", "request_route_extraction", 4n),
];

async function installBackendRoutes(page: Page): Promise<void> {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    body: Buffer.from(GetSystemBootstrapResponse.encode(
      GetSystemBootstrapResponse.fromPartial({
        apps: [{
          appId: APP_ID,
          defaultIndexNames: ["gradethis"],
          displayName: "Observability",
          slug: "observability",
          state: AppState.APP_STATE_ACTIVE,
        }],
        features: [
          ServerFeature.SERVER_FEATURE_SEARCH,
          ServerFeature.SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
        ],
        indexes: [{
          indexId: "index-gradethis",
          ingestionAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
          name: "gradethis",
          searchAccess: IndexAccessState.INDEX_ACCESS_STATE_ENABLED,
          state: IndexState.INDEX_STATE_ACTIVE,
        }],
        limits: { maximumPageSize: 50 },
        searchWebsocketPath: "/api/search/ws",
        selectedAppId: APP_ID,
        serverTime: new Date("2026-03-17T15:30:00.000Z"),
      }),
    ).finish()),
    headers: PROTOBUF_HEADERS,
    status: 200,
  }));
  await page.route("**/api/indexes/list", (route) => route.fulfill({
    body: Buffer.from(ListIndexesResponse.encode(
      ListIndexesResponse.fromPartial({ page: { totalSize: 0n, totalSizeExact: true } }),
    ).finish()),
    headers: PROTOBUF_HEADERS,
    status: 200,
  }));
  await page.route("**/api/ingestion-tokens/list", (route) => route.fulfill({
    body: Buffer.from(ListIngestionTokensResponse.encode(
      ListIngestionTokensResponse.fromPartial({ page: { totalSize: 0n, totalSizeExact: true } }),
    ).finish()),
    headers: PROTOBUF_HEADERS,
    status: 200,
  }));
  await page.route("**/api/knowledge/objects/list", (route) => route.fulfill({
    body: Buffer.from(ListKnowledgeObjectsResponse.encode(
      ListKnowledgeObjectsResponse.fromPartial({
        knowledgeObjects: [...KNOWLEDGE_OBJECTS],
        page: { totalSize: BigInt(KNOWLEDGE_OBJECTS.length), totalSizeExact: true },
        tenantCatalogRevision: 11n,
      }),
    ).finish()),
    headers: PROTOBUF_HEADERS,
    status: 200,
  }));
}

test.describe("knowledge manager", () => {
  test("advertised object list", async ({ page }) => {
    await installBackendRoutes(page);
    await gotoVisualRoute(page, "/admin/?section=knowledge");
    await expect(page.getByRole("heading", { level: 2, name: "Knowledge Manager" })).toBeVisible();
    await expect(page.getByText("http_status_alias")).toBeVisible();
    await settleVisualPage(page);
    await expectPageScreenshot(page, "admin-knowledge-manager");
  });

  test("create object form", async ({ page }) => {
    await installBackendRoutes(page);
    await gotoVisualRoute(page, "/admin/?section=knowledge");
    await page.getByRole("button", { name: "Create knowledge object" }).click();
    await settleVisualPage(page);
    await expectPageScreenshot(page, "admin-knowledge-manager-create");
  });
});

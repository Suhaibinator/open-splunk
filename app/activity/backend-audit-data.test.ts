import assert from "node:assert/strict";
import test from "node:test";

import {
  AuditAction,
  AuditActorKind,
  AuditActorRole,
  AuditTargetKind,
  type AuditEvent,
} from "@/gen/ts/open_splunk/v1/audit";
import type { SearchAttemptAuditEvent } from "@/gen/ts/open_splunk/v1/search_attempt_audit";
import { HttpError } from "@/lib/api";

import {
  auditErrorPresentation,
  buildMutationAuditRequest,
  buildSearchAttemptAuditRequest,
  listMutationAuditEvents,
  listSearchAttemptAuditEvents,
  type AuditListClient,
} from "./backend-audit-data";

function mutation(sequence: bigint): AuditEvent {
  return {
    sequence,
    occurredAt: new Date(`2026-08-06T12:00:0${sequence.toString()}Z`),
    actorKind: AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
    actorId: "admin-1",
    actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
    action: AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE,
    targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH,
    targetId: "saved-1",
    targetVersion: 3n,
  };
}

function attempt(sequence: bigint): SearchAttemptAuditEvent {
  return {
    sequence,
    occurredAt: new Date(`2026-08-06T12:00:0${sequence.toString()}Z`),
    actorKind: AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
    actorId: "admin-1",
    actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
    ownerId: "owner-1",
    searchJobId: `job-${sequence.toString()}`,
  };
}

function httpError(status: number): HttpError {
  return new HttpError({
    status,
    message: `HTTP ${status}`,
    url: "/api/v1/audit/events/list",
  });
}

test("mutation audit requests preserve exact filters and opaque pagination", () => {
  assert.deepEqual(buildMutationAuditRequest({
    actions: [
      AuditAction.AUDIT_ACTION_INDEX_UPDATE,
      AuditAction.AUDIT_ACTION_INDEX_CREATE,
      AuditAction.AUDIT_ACTION_INDEX_UPDATE,
    ],
    actorId: "  admin-1  ",
    targetKind: AuditTargetKind.AUDIT_TARGET_KIND_INDEX,
  }, {
    pageSize: 100,
    pageToken: " opaque.signed.cursor ",
  }), {
    page: {
      pageSize: 100,
      pageToken: " opaque.signed.cursor ",
      includeTotalSize: true,
    },
    actionFilters: [
      AuditAction.AUDIT_ACTION_INDEX_CREATE,
      AuditAction.AUDIT_ACTION_INDEX_UPDATE,
    ],
    actorIdFilter: "admin-1",
    targetKindFilter: AuditTargetKind.AUDIT_TARGET_KIND_INDEX,
  });
});

test("search-attempt requests include only exact actor and owner filters", () => {
  assert.deepEqual(buildSearchAttemptAuditRequest({
    actorId: " browser-admin ",
    ownerId: " tenant-owner ",
  }, {
    pageSize: 50,
    pageToken: "next-opaque-token",
  }), {
    page: {
      pageSize: 50,
      pageToken: "next-opaque-token",
      includeTotalSize: true,
    },
    actorIdFilter: "browser-admin",
    ownerIdFilter: "tenant-owner",
  });
});

test("mutation adapter requires descending events and an exact total", async () => {
  const requests: unknown[] = [];
  const client = {
    auditEvents: {
      list: async (request: unknown) => {
        requests.push(request);
        return {
          auditEvents: [mutation(3n), mutation(2n)],
          page: { nextPageToken: "opaque-page-two", totalSize: 9n, totalSizeExact: true },
        };
      },
    },
    searchAttemptAudit: { list: async () => ({ events: [], page: undefined }) },
  } as AuditListClient;
  const result = await listMutationAuditEvents(client, { actions: [] }, { pageSize: 25 });
  assert.equal(result.totalSize, 9n);
  assert.equal(result.totalSizeExact, true);
  assert.equal(result.nextPageToken, "opaque-page-two");
  assert.deepEqual(result.items.map((event) => event.sequence), [3n, 2n]);
  assert.equal(requests.length, 1);

  const invalid = {
    ...client,
    auditEvents: {
      list: async () => ({
        auditEvents: [mutation(2n), mutation(3n)],
        page: { totalSize: 2n, totalSizeExact: true },
      }),
    },
  } as AuditListClient;
  await assert.rejects(() => listMutationAuditEvents(invalid, { actions: [] }, { pageSize: 25 }), /descending sequence/);
});

test("search-attempt adapter never joins query text and validates exact totals", async () => {
  let received: unknown;
  const client = {
    auditEvents: { list: async () => ({ auditEvents: [], page: undefined }) },
    searchAttemptAudit: {
      list: async (request: unknown) => {
        received = request;
        return {
          events: [attempt(5n), attempt(4n)],
          page: { totalSize: 2n, totalSizeExact: true },
        };
      },
    },
  } as AuditListClient;
  const result = await listSearchAttemptAuditEvents(client, { ownerId: "owner-1" }, { pageSize: 25 });
  assert.deepEqual(result.items.map((event) => Object.keys(event).toSorted()), [
    ["actorId", "actorKind", "actorRole", "occurredAt", "ownerId", "searchJobId", "sequence"],
    ["actorId", "actorKind", "actorRole", "occurredAt", "ownerId", "searchJobId", "sequence"],
  ]);
  assert.deepEqual(received, {
    page: { pageSize: 25, pageToken: undefined, includeTotalSize: true },
    actorIdFilter: undefined,
    ownerIdFilter: "owner-1",
  });
});

test("audit failures distinguish invalid traversal, auth, capacity, and unavailable storage", () => {
  assert.equal(auditErrorPresentation(httpError(400), "Mutation audit").invalidTraversal, true);
  assert.match(auditErrorPresentation(httpError(401), "Mutation audit").title, /Authentication/);
  assert.match(auditErrorPresentation(httpError(403), "Mutation audit").title, /Administrator/);
  assert.match(auditErrorPresentation(httpError(429), "Mutation audit").title, /capacity/);
  assert.match(auditErrorPresentation(httpError(503), "Mutation audit").title, /unavailable/);
});

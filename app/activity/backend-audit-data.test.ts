import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import {
  AuditAction,
  AuditActorKind,
  AuditActorRole,
  AuditTargetKind,
  type AuditEvent,
} from "@/gen/ts/open_splunk/audit";
import { SharingScope } from "@/gen/ts/open_splunk/common";
import { KnowledgeObjectType } from "@/gen/ts/open_splunk/knowledge";
import type { SearchAttemptAuditEvent } from "@/gen/ts/open_splunk/search_attempt_audit";
import { HttpError } from "@/lib/api";

import {
  auditIdentifierFromDraft,
  auditErrorPresentation,
  buildMutationAuditRequest,
  buildSearchAttemptAuditRequest,
  listMutationAuditEvents,
  listSearchAttemptAuditEvents,
  mutationAuditActionOptions,
  mutationAuditTargetOptions,
  type AuditListClient,
} from "./backend-audit-data";
import { MutationAuditTargetProjection } from "./backend-audit-views";

function mutation(sequence: bigint): AuditEvent {
  return {
    sequence,
    occurredAt: new Date(Date.parse("2026-08-06T12:00:00.000Z") + Number(sequence)),
    actorKind: AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
    actorId: "admin-1",
    actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
    action: AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE,
    targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH,
    targetId: "saved-1",
    targetVersion: 3n,
  };
}

function knowledgeMutation(
  sequence: bigint,
  overrides: Partial<AuditEvent> = {},
): AuditEvent {
  return {
    ...mutation(sequence),
    action: AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE,
    targetKind: AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT,
    targetId: "ko-a",
    targetVersion: 2n,
    appId: "app-observability",
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
    sharingScope: SharingScope.SHARING_SCOPE_APP,
    ...overrides,
  };
}

function mutationClient(
  auditEvents: AuditEvent[],
  page: { nextPageToken?: string; totalSize?: bigint; totalSizeExact?: boolean },
): AuditListClient {
  return {
    auditEvents: { list: async () => ({ auditEvents, page }) },
    searchAttemptAudit: { list: async () => ({ events: [], page: undefined }) },
  } as AuditListClient;
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
    url: "/api/audit/events/list",
  });
}

const mutationActionContracts: ReadonlyArray<readonly [
  AuditAction,
  AuditTargetKind,
  bigint,
]> = [
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_CREATE, AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, 1n],
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_UPDATE, AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, 2n],
  [AuditAction.AUDIT_ACTION_INGESTION_TOKEN_REVOKE, AuditTargetKind.AUDIT_TARGET_KIND_INGESTION_TOKEN, 2n],
  [AuditAction.AUDIT_ACTION_INDEX_CREATE, AuditTargetKind.AUDIT_TARGET_KIND_INDEX, 1n],
  [AuditAction.AUDIT_ACTION_INDEX_UPDATE, AuditTargetKind.AUDIT_TARGET_KIND_INDEX, 2n],
  [AuditAction.AUDIT_ACTION_INDEX_ACTIVATE, AuditTargetKind.AUDIT_TARGET_KIND_INDEX, 2n],
  [AuditAction.AUDIT_ACTION_INDEX_ARCHIVE, AuditTargetKind.AUDIT_TARGET_KIND_INDEX, 2n],
  [AuditAction.AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, AuditTargetKind.AUDIT_TARGET_KIND_INDEX, 2n],
  [AuditAction.AUDIT_ACTION_INDEX_DELETE_DATA, AuditTargetKind.AUDIT_TARGET_KIND_INDEX, 3n],
  [AuditAction.AUDIT_ACTION_APP_CREATE, AuditTargetKind.AUDIT_TARGET_KIND_APP, 1n],
  [AuditAction.AUDIT_ACTION_APP_UPDATE, AuditTargetKind.AUDIT_TARGET_KIND_APP, 2n],
  [AuditAction.AUDIT_ACTION_APP_ACTIVATE, AuditTargetKind.AUDIT_TARGET_KIND_APP, 2n],
  [AuditAction.AUDIT_ACTION_APP_ARCHIVE, AuditTargetKind.AUDIT_TARGET_KIND_APP, 2n],
  [AuditAction.AUDIT_ACTION_APP_DELETE, AuditTargetKind.AUDIT_TARGET_KIND_APP, 2n],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_CREATE, AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, 1n],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_UPDATE, AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, 2n],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_DUPLICATE, AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, 1n],
  [AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE, AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH, 1n],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE, AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, 1n],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE, AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, 2n],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE, AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, 2n],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE, AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, 2n],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE, AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, 2n],
  [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE, AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT, 2n],
  [AuditAction.AUDIT_ACTION_SERVER_SETTINGS_UPDATE, AuditTargetKind.AUDIT_TARGET_KIND_SERVER_SETTINGS, 1n],
  [AuditAction.AUDIT_ACTION_LOOKUP_CREATE, AuditTargetKind.AUDIT_TARGET_KIND_LOOKUP, 1n],
  [AuditAction.AUDIT_ACTION_LOOKUP_REPLACE, AuditTargetKind.AUDIT_TARGET_KIND_LOOKUP, 2n],
  [AuditAction.AUDIT_ACTION_LOOKUP_ENABLE, AuditTargetKind.AUDIT_TARGET_KIND_LOOKUP, 2n],
  [AuditAction.AUDIT_ACTION_LOOKUP_DISABLE, AuditTargetKind.AUDIT_TARGET_KIND_LOOKUP, 2n],
  [AuditAction.AUDIT_ACTION_LOOKUP_DELETE, AuditTargetKind.AUDIT_TARGET_KIND_LOOKUP, 2n],
];

function eventForContract(
  action: AuditAction,
  targetKind: AuditTargetKind,
  targetVersion: bigint,
  sequence: bigint,
): AuditEvent {
  if (targetKind === AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT) {
    return knowledgeMutation(sequence, { action, targetKind, targetVersion });
  }
  return { ...mutation(sequence), action, targetKind, targetVersion };
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
    pageToken: "opaque.signed.cursor",
  }), {
    page: {
      pageSize: 100,
      pageToken: "opaque.signed.cursor",
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

test("mutation audit exposes the complete ordered taxonomy without correlating independent filters", () => {
  assert.equal(mutationAuditActionOptions.length, 30);
  assert.deepEqual(
    mutationAuditActionOptions.map((option) => option.value),
    mutationActionContracts.map(([action]) => action),
  );
  assert.deepEqual(
    mutationAuditActionOptions.slice(-6).map((option) => option.label),
    [
      "Server settings · update",
      "Lookup · create",
      "Lookup · replace",
      "Lookup · enable",
      "Lookup · disable",
      "Lookup · delete",
    ],
  );
  assert.deepEqual(mutationAuditTargetOptions.at(-1), {
    value: AuditTargetKind.AUDIT_TARGET_KIND_LOOKUP,
    label: "Lookup",
  });

  assert.deepEqual(buildMutationAuditRequest({
    actions: [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE],
    targetKind: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH,
  }, { pageSize: 25 }), {
    page: { pageSize: 25, pageToken: undefined, includeTotalSize: true },
    actionFilters: [AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE],
    actorIdFilter: undefined,
    targetKindFilter: AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH,
  });
});

test("mutation adapter accepts every exact action-target contract and returns a closed detached page", async () => {
  const sourceEvents = mutationActionContracts.map(([action, targetKind, version], index) =>
    eventForContract(action, targetKind, version, BigInt(mutationActionContracts.length - index)));
  const firstSourceDate = sourceEvents[0]!.occurredAt!;
  Object.assign(sourceEvents[0]!, { arbitraryPayload: "must not cross the adapter" });
  const result = await listMutationAuditEvents(
    mutationClient(sourceEvents, { totalSize: 30n, totalSizeExact: true }),
    { actions: [] },
    { pageSize: 30 },
  );

  assert.equal(result.items.length, 30);
  assert.notEqual(result.items[0], sourceEvents[0]);
  assert.notEqual(result.items[0]!.occurredAt, firstSourceDate);
  assert.doesNotMatch(JSON.stringify(Object.keys(result.items[0]!).toSorted()), /arbitraryPayload/);
  assert.deepEqual(
    result.items.map((event) => [event.action, event.targetKind, event.targetVersion]),
    mutationActionContracts,
  );
  for (const event of result.items) {
    const knowledge = event.targetKind === AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT;
    assert.equal(event.appId !== undefined, knowledge);
    assert.equal(event.objectType !== undefined, knowledge);
    assert.equal(event.sharingScope !== undefined, knowledge);
  }

  sourceEvents[0]!.actorId = "mutated-after-adaptation";
  firstSourceDate.setUTCFullYear(2030);
  assert.equal(result.items[0]!.actorId, "admin-1");
  assert.equal(result.items[0]!.occurredAt!.getUTCFullYear(), 2026);
});

test("mutation adapter rejects every action paired with another target", async () => {
  await Promise.all(mutationActionContracts.map(async ([action, targetKind, version]) => {
    const forgedTarget = targetKind === AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT
      ? AuditTargetKind.AUDIT_TARGET_KIND_SAVED_SEARCH
      : AuditTargetKind.AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT;
    const event = eventForContract(action, forgedTarget, version, 1n);
    await assert.rejects(
      () => listMutationAuditEvents(
        mutationClient([event], { totalSize: 1n, totalSizeExact: true }),
        { actions: [] },
        { pageSize: 1 },
      ),
      /invalid action or target projection/,
      `action ${action.toString()} accepted target ${forgedTarget.toString()}`,
    );
  }));
});

test("mutation adapter enforces every action version boundary", async () => {
  const exactOne = new Set<AuditAction>([
    AuditAction.AUDIT_ACTION_INGESTION_TOKEN_CREATE,
    AuditAction.AUDIT_ACTION_INDEX_CREATE,
    AuditAction.AUDIT_ACTION_APP_CREATE,
    AuditAction.AUDIT_ACTION_SAVED_SEARCH_CREATE,
    AuditAction.AUDIT_ACTION_SAVED_SEARCH_DUPLICATE,
    AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE,
    AuditAction.AUDIT_ACTION_LOOKUP_CREATE,
  ]);
  await Promise.all(mutationActionContracts.map(async ([action, targetKind]) => {
    const invalidVersion = exactOne.has(action)
      ? 2n
      : action === AuditAction.AUDIT_ACTION_INDEX_DELETE_DATA
        ? 2n
        : action === AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE ||
            action === AuditAction.AUDIT_ACTION_SERVER_SETTINGS_UPDATE
          ? 0n
          : 1n;
    await assert.rejects(
      () => listMutationAuditEvents(
        mutationClient(
          [eventForContract(action, targetKind, invalidVersion, 1n)],
          { totalSize: 1n, totalSizeExact: true },
        ),
        { actions: [] },
        { pageSize: 1 },
      ),
      /invalid sequence or target version/,
      `action ${action.toString()} accepted version ${invalidVersion.toString()}`,
    );
  }));
});

test("mutation adapter requires the complete canonical Knowledge metadata triple and legacy absence", async () => {
  const invalidKnowledgeEvents: AuditEvent[] = [
    knowledgeMutation(1n, { appId: undefined }),
    knowledgeMutation(1n, { objectType: undefined }),
    knowledgeMutation(1n, { sharingScope: undefined }),
    knowledgeMutation(1n, { appId: "" }),
    knowledgeMutation(1n, { appId: " app-a" }),
    knowledgeMutation(1n, { appId: "app-a\u0085" }),
    knowledgeMutation(1n, { appId: "app\u0000a" }),
    knowledgeMutation(1n, { appId: "\ud800" }),
    knowledgeMutation(1n, { appId: "é".repeat(65) }),
    knowledgeMutation(1n, { appId: "a".repeat(129) }),
    knowledgeMutation(1n, { objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED }),
    knowledgeMutation(1n, { objectType: 99 as KnowledgeObjectType }),
    knowledgeMutation(1n, { sharingScope: SharingScope.SHARING_SCOPE_UNSPECIFIED }),
    knowledgeMutation(1n, { sharingScope: 99 as SharingScope }),
  ];
  await Promise.all(invalidKnowledgeEvents.map((event) =>
    assert.rejects(() => listMutationAuditEvents(
      mutationClient([event], { totalSize: 1n, totalSizeExact: true }),
      { actions: [] },
      { pageSize: 1 },
    ), /invalid action or target projection/)));

  await Promise.all([
    { ...mutation(1n), appId: "app-a" },
    {
      ...mutation(1n),
      appId: "app-a",
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
    },
  ].map((legacy) => assert.rejects(() => listMutationAuditEvents(
      mutationClient([legacy], { totalSize: 1n, totalSizeExact: true }),
      { actions: [] },
      { pageSize: 1 },
    ), /invalid action or target projection/)));
});

test("mutation adapter pins primitive types, identity bounds, actor authority, and page atomicity", async () => {
  const boundary = knowledgeMutation(100_000n, {
    actorId: "a".repeat(255),
    targetId: "k".repeat(128),
    targetVersion: 9_223_372_036_854_775_807n,
    appId: "p".repeat(128),
  });
  const accepted = await listMutationAuditEvents(
    mutationClient([boundary], { totalSize: 1n, totalSizeExact: true }),
    { actions: [] },
    { pageSize: 1 },
  );
  assert.equal(accepted.items.length, 1);

  const validAuthorityPairs = await listMutationAuditEvents(
    mutationClient([
      knowledgeMutation(2n, {
        actorKind: AuditActorKind.AUDIT_ACTOR_KIND_SYSTEM,
        actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_SYSTEM,
      }),
      {
        ...mutation(1n),
        actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_USER,
        action: AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE,
        targetVersion: 1n,
      },
    ], { totalSize: 2n, totalSizeExact: true }),
    { actions: [] },
    { pageSize: 2 },
  );
  assert.deepEqual(
    validAuthorityPairs.items.map((event) => [event.actorKind, event.actorRole, event.action]),
    [
      [
        AuditActorKind.AUDIT_ACTOR_KIND_SYSTEM,
        AuditActorRole.AUDIT_ACTOR_ROLE_SYSTEM,
        AuditAction.AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE,
      ],
      [
        AuditActorKind.AUDIT_ACTOR_KIND_BROWSER,
        AuditActorRole.AUDIT_ACTOR_ROLE_USER,
        AuditAction.AUDIT_ACTION_SAVED_SEARCH_DELETE,
      ],
    ],
  );

  const invalidEvents: AuditEvent[] = [
    knowledgeMutation(1n, { sequence: 100_001n }),
    knowledgeMutation(1n, { sequence: 1 as unknown as bigint }),
    knowledgeMutation(1n, { targetVersion: 9_223_372_036_854_775_808n }),
    knowledgeMutation(1n, { targetVersion: 2 as unknown as bigint }),
    knowledgeMutation(1n, { occurredAt: "2026-08-06" as unknown as Date }),
    knowledgeMutation(1n, { actorId: "a".repeat(256) }),
    knowledgeMutation(1n, { actorId: " actor" }),
    knowledgeMutation(1n, { actorId: "actor\u0007" }),
    knowledgeMutation(1n, { targetId: "k".repeat(129) }),
    knowledgeMutation(1n, { targetId: "target\u009f" }),
    knowledgeMutation(1n, {
      actorKind: AuditActorKind.AUDIT_ACTOR_KIND_SYSTEM,
      actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_ADMINISTRATOR,
    }),
    knowledgeMutation(1n, { actorRole: AuditActorRole.AUDIT_ACTOR_ROLE_USER }),
  ];
  await Promise.all(invalidEvents.map((event) => assert.rejects(() => listMutationAuditEvents(
      mutationClient([event], { totalSize: 1n, totalSizeExact: true }),
      { actions: [] },
      { pageSize: 1 },
    ))));

  const validThenInvalid = [
    knowledgeMutation(2n, { targetId: "FIRST_ROW_SECRET" }),
    knowledgeMutation(1n, {
      targetId: "SECOND_ROW_SECRET",
      appId: " SECOND_APP_SECRET",
    }),
  ];
  const atomicRejection: unknown = await listMutationAuditEvents(
    mutationClient(validThenInvalid, { totalSize: 2n, totalSizeExact: true }),
    { actions: [] },
    { pageSize: 2 },
  ).then(() => null, (reason: unknown) => reason);
  assert.ok(atomicRejection instanceof Error);
  assert.doesNotMatch(
    `${atomicRejection.message} ${auditErrorPresentation(atomicRejection, "Mutation audit").message}`,
    /FIRST_ROW_SECRET|SECOND_ROW_SECRET|SECOND_APP_SECRET/,
  );
});

test("audit page adapter enforces requested size, canonical tokens, and exact total relationships", async () => {
  const cases: Array<{
    page: { nextPageToken?: string; totalSize?: bigint; totalSizeExact?: boolean };
    events: AuditEvent[];
    options?: { pageSize: number; pageToken?: string };
  }> = [
    { events: [mutation(1n)], page: { totalSize: 2n, totalSizeExact: true } },
    { events: [mutation(1n)], page: { totalSize: 100_001n, totalSizeExact: true } },
    { events: [mutation(2n), mutation(1n)], page: { totalSize: 1n, totalSizeExact: true }, options: { pageSize: 2 } },
    { events: [mutation(1n)], page: { nextPageToken: "next", totalSize: 2n, totalSizeExact: true }, options: { pageSize: 2 } },
    { events: [mutation(1n)], page: { nextPageToken: "same", totalSize: 2n, totalSizeExact: true }, options: { pageSize: 1, pageToken: "same" } },
    { events: [mutation(1n)], page: { nextPageToken: " next ", totalSize: 2n, totalSizeExact: true }, options: { pageSize: 1 } },
    { events: [mutation(1n)], page: { nextPageToken: "x".repeat(2_049), totalSize: 2n, totalSizeExact: true }, options: { pageSize: 1 } },
  ];
  await Promise.all(cases.map((testCase) => assert.rejects(() => listMutationAuditEvents(
      mutationClient(testCase.events, testCase.page),
      { actions: [] },
      testCase.options ?? { pageSize: 1 },
    ))));

  await assert.rejects(() => listMutationAuditEvents(
    mutationClient(
      [mutation(3n), mutation(2n), knowledgeMutation(1n, { appId: undefined })],
      { totalSize: 3n, totalSizeExact: true },
    ),
    { actions: [] },
    { pageSize: 2 },
  ), /exceeded the requested page size/);

  const continuation = await listMutationAuditEvents(
    mutationClient([mutation(1n)], { totalSize: 3n, totalSizeExact: true }),
    { actions: [] },
    { pageSize: 2, pageToken: "page-two" },
  );
  assert.equal(continuation.totalSize, 3n);
  assert.equal(continuation.nextPageToken, null);
});

test("audit identifier normalization matches Go TrimSpace edges without stripping FEFF", () => {
  assert.equal(auditIdentifierFromDraft("\u0085 actor-a \u00a0"), "actor-a");
  assert.equal(auditIdentifierFromDraft("\ufeffactor-a\ufeff"), "\ufeffactor-a\ufeff");
  assert.throws(() => buildMutationAuditRequest({
    actions: [],
    actorId: "actor\u0007",
  }, { pageSize: 25 }), /invalid identifier/);
});

test("Knowledge audit target projection is escaped, read-only, and independent of the Knowledge feature", () => {
  const event = knowledgeMutation(1n, {
    targetId: "ko-<script>globalThis.__auditXss=true</script>",
    appId: "app-<img src=x onerror=globalThis.__auditXss=true>",
  });
  const markup = renderToStaticMarkup(createElement(MutationAuditTargetProjection, { event }));
  assert.match(markup, /ko-&lt;script&gt;globalThis\.__auditXss=true&lt;\/script&gt;/);
  assert.match(markup, /App: app-&lt;img src=x onerror=globalThis\.__auditXss=true&gt;/);
  assert.match(markup, /Type: Field alias/);
  assert.match(markup, /Sharing: App/);
  assert.doesNotMatch(markup, /<(?:script|img|a|button|input)\b/i);

  const legacyMarkup = renderToStaticMarkup(createElement(MutationAuditTargetProjection, {
    event: mutation(1n),
  }));
  assert.match(legacyMarkup, /Saved search/);
  assert.doesNotMatch(legacyMarkup, /App:|Type:|Sharing:/);

  const dataSource = readFileSync(path.join(process.cwd(), "app", "activity", "backend-audit-data.ts"), "utf8");
  const viewSource = readFileSync(path.join(process.cwd(), "app", "activity", "backend-audit-views.tsx"), "utf8");
  assert.doesNotMatch(`${dataSource}\n${viewSource}`, /SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS|dangerouslySetInnerHTML/);
});

test("audit continuation validates a repeated cursor before committing page state", () => {
  const source = readFileSync(
    path.join(process.cwd(), "app", "activity", "backend-audit-views.tsx"),
    "utf8",
  );
  const finalPageCheck = source.indexOf("continuation did not match its retained exact total");
  const tokenValidation = source.indexOf(
    "const validatedNextPageToken = recordNextPageToken(",
    finalPageCheck,
  );
  const sequenceCommit = source.indexOf(
    "for (const item of page.items) sequencesSeenRef.current.add(item.sequence)",
    tokenValidation,
  );
  const itemCommit = source.indexOf("setItems((current) =>", sequenceCommit);
  const tokenCommit = source.indexOf("setNextPageToken(validatedNextPageToken)", itemCommit);
  assert.ok(
    finalPageCheck >= 0
      && finalPageCheck < tokenValidation
      && tokenValidation < sequenceCommit
      && sequenceCommit < itemCommit
      && itemCommit < tokenCommit,
    "continuation cursor validation must precede every retained-page mutation",
  );
});

test("activity layouts preserve capability tabs and mobile job metadata", () => {
  const backendSource = readFileSync(
    path.join(process.cwd(), "app", "activity", "backend-activity-console.tsx"),
    "utf8",
  );
  const demoSource = readFileSync(
    path.join(process.cwd(), "app", "activity", "activity-console.tsx"),
    "utf8",
  );
  assert.match(backendSource, /data-tab-count=\{availableViews\.length\}/u);
  for (const label of ["Search", "Status", "Owner", "Runtime", "Events", "Started", "Actions"]) {
    assert.match(demoSource, new RegExp(`data-label="${label}"`, "u"));
  }
  // The demo console used to carry a card mode of its own in
  // `activity-console.module.css`; it now opts into the one `.table--cards`
  // implementation the backend console beside it already used. Opting in is a
  // fact about this component's markup, so it is asserted here.
  assert.match(demoSource, /className="table table--cards activity-table"/u);
  // What the card mode then *does* -- print each labelled cell's column name
  // and outrank the per-column desktop widths -- is a fact about the cascade,
  // not about any one file, so it is asserted against computed style in
  // integration/style-contracts/css-contracts.spec.ts ("activity job table contracts"),
  // beside the `.live-jobs-table` contracts. Matching the stylesheet's
  // characters here would pin the rule to a file, breaking this test if the
  // implementation moved without changing what the browser renders.
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
  const result = await listMutationAuditEvents(client, { actions: [] }, { pageSize: 2 });
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

  const impossibleTotal = {
    ...client,
    searchAttemptAudit: {
      list: async () => ({
        events: [attempt(1n)],
        page: { totalSize: 100_001n, totalSizeExact: true },
      }),
    },
  } as AuditListClient;
  await assert.rejects(() => listSearchAttemptAuditEvents(
    impossibleTotal,
    {},
    { pageSize: 25, pageToken: "page-two" },
  ), /invalid item count or exact total/);
});

test("audit failures distinguish invalid traversal, auth, capacity, and unavailable storage", () => {
  assert.equal(auditErrorPresentation(httpError(400), "Mutation audit").invalidTraversal, true);
  assert.match(auditErrorPresentation(httpError(401), "Mutation audit").title, /Authentication/);
  assert.match(auditErrorPresentation(httpError(403), "Mutation audit").title, /Administrator/);
  assert.match(auditErrorPresentation(httpError(429), "Mutation audit").title, /capacity/);
  assert.match(auditErrorPresentation(httpError(503), "Mutation audit").title, /unavailable/);
});

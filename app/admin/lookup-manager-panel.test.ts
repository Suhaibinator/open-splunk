import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SharingScope } from "@/gen/ts/open_splunk/v1/common";
import {
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/v1/knowledge";
import { Lookup, LookupState } from "@/gen/ts/open_splunk/v1/lookup";
import { PreviewLookupResponse } from "@/gen/ts/open_splunk/v1/lookup_api";

import {
  LookupManagerTable,
  LookupPreviewTable,
  createLookupDraft,
  lookupDefinitionFromDraft,
  lookupDraftFromLookup,
  lookupStateLabel,
  isExactEventField,
  isExactLookupColumn,
  isExactPublicField,
  normalizeLookupPreview,
  parseLookupMappings,
} from "./lookup-manager-panel";

function exactEventPathBytes(length: number): string {
  const maximumEncodedSegmentBytes = 2 * 256;
  const segments = Math.ceil((length + 1) / (maximumEncodedSegmentBytes + 1));
  let remaining = length - (segments - 1);
  const result: string[] = [];
  for (let index = 0; index < segments; index += 1) {
    const segmentsLeft = segments - index;
    const segmentBytes = Math.min(maximumEncodedSegmentBytes, remaining - (segmentsLeft - 1));
    result.push(`${segmentBytes % 2 === 0 ? "" : "a"}${"\\.".repeat(Math.floor(segmentBytes / 2))}`);
    remaining -= segmentBytes;
  }
  return result.join(".");
}

function lookupFixture() {
  return Lookup.fromPartial({
    lookupId: "lookup-service-catalog",
    tenantId: "tenant-local",
    ownerId: "administrator",
    version: 3n,
    state: LookupState.LOOKUP_STATE_ACTIVE,
    definition: lookupDefinitionFromDraft({
      ...createLookupDraft("app-observability"),
      name: "service_catalog",
      description: "Service ownership",
      automatic: true,
      indexPatterns: "main\nprod-*",
      keyMappings: "service_id AS service_key",
      outputMappings: "owner AS service_owner\ntier",
      overwrite: "preserve",
    }),
    columns: ["service_id", "owner", "tier"],
    rowCount: 2n,
    canonicalSizeBytes: 78n,
    sourceSha256: new Uint8Array(32).fill(1),
    contentSha256: new Uint8Array(32).fill(2),
    createdAt: new Date("2026-08-12T10:00:00.000Z"),
    updatedAt: new Date("2026-08-13T10:00:00.000Z"),
  });
}

test("mapping drafts use the explicit bounded v0.4 key and output grammar", () => {
  assert.deepEqual(parseLookupMappings("id AS event_id\nregion AS event_region", "key"), [
    { lookupField: "id", eventField: "event_id" },
    { lookupField: "region", eventField: "event_region" },
  ]);
  assert.deepEqual(parseLookupMappings("owner AS service_owner\ntier", "output"), [
    { lookupField: "owner", eventField: "service_owner" },
    { lookupField: "tier", eventField: "tier" },
  ]);
  assert.throws(() => parseLookupMappings("id", "key"), /must use/);
  assert.throws(() => parseLookupMappings("id AS fields", "key"), /public field/);
  assert.throws(() => parseLookupMappings("id AS __os_key", "key"), /public field/);
  assert.throws(() => parseLookupMappings("id AS key\nid AS other", "key"), /repeated/);
  assert.throws(
    () => parseLookupMappings(Array.from({ length: 17 }, (_, index) => `value_${index}`).join("\n"), "output"),
    /between 1 and 16/,
  );
});

test("exact public field validation mirrors SPL word-token boundaries", () => {
  for (const value of ["service/catalog", "service+catalog", "世界", "a?b", "a[b]", "a{b}", ".com"]) {
    assert.equal(isExactPublicField(value), true, `${value} should be an exact public token`);
  }
  for (const value of [".", "a!b", "a<b", "a>b", "a=b", "a|b", "a,b", "a b", "fields", "__os_private"]) {
    assert.equal(isExactPublicField(value), false, `${value} should not be an exact public token`);
  }
  assert.deepEqual(parseLookupMappings("a?b AS a[b]", "key"), [
    { lookupField: "a?b", eventField: "a[b]" },
  ]);
  assert.throws(() => parseLookupMappings("a!b AS target", "key"), /public field/);

  assert.equal(isExactLookupColumn("fields"), true);
  assert.equal(isExactPublicField("fields"), false);
  assert.equal(isExactEventField("a\\.b"), true);
  for (const value of ["a..b", "a\\q", "a\\", "event\u200bfield", `${"x".repeat(257)}.leaf`]) {
    assert.equal(isExactEventField(value), false, `${value} should not be an event path`);
  }
  assert.deepEqual(parseLookupMappings("fields AS event_fields", "key"), [
    { lookupField: "fields", eventField: "event_fields" },
  ]);
  assert.throws(() => parseLookupMappings("OUTPUT AS event_key", "key"), /public field/);
  assert.throws(() => parseLookupMappings("id AS outputnew", "key"), /public field/);
  assert.deepEqual(parseLookupMappings("OUTPUT AS OUTPUTNEW", "output"), [
    { lookupField: "OUTPUT", eventField: "OUTPUTNEW" },
  ]);
});

test("lookup editor draft produces exact mappings, selectors, and overwrite authority", () => {
  const definition = lookupDefinitionFromDraft({
    ...createLookupDraft("app-security"),
    name: "asset_inventory",
    description: "Asset ownership",
    sharingScope: "global",
    automatic: true,
    hostPatterns: "api-*\nworker-01",
    keyMappings: "asset_id AS host_id",
    outputMappings: "owner\ntier AS asset_tier",
    overwrite: "replace",
  });
  assert.equal(definition.appId, "app-security");
  assert.equal(definition.sharingScope, SharingScope.SHARING_SCOPE_GLOBAL);
  assert.equal(definition.overwriteBehavior, KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING);
  assert.equal(definition.selector?.hostPatterns[0]?.matchKind, KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD);
  assert.equal(definition.selector?.hostPatterns[1]?.matchKind, KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT);
  assert.deepEqual(definition.outputMappings, [
    { lookupField: "owner", eventField: "owner" },
    { lookupField: "tier", eventField: "asset_tier" },
  ]);
  assert.throws(() => lookupDefinitionFromDraft({
    ...createLookupDraft("app-security"),
    name: "asset_inventory",
    hostPatterns: "api-\\x",
    keyMappings: "asset_id AS host_id",
    outputMappings: "owner",
  }), /invalid escape/);
});

test("lookup editor rejects a mapping contract outside the authored SPL ceiling", () => {
  assert.throws(() => lookupDefinitionFromDraft({
    ...createLookupDraft("app-security"),
    name: "asset_inventory",
    keyMappings: `key AS ${exactEventPathBytes(8_000)}`,
    outputMappings: `value AS ${exactEventPathBytes(8_400)}`,
  }), /16 KiB authored SPL ceiling/);
});

test("published lookup projections round-trip into replace drafts without losing mappings", () => {
  const draft = lookupDraftFromLookup(lookupFixture());
  assert.equal(draft.appId, "app-observability");
  assert.equal(draft.name, "service_catalog");
  assert.equal(draft.keyMappings, "service_id AS service_key");
  assert.equal(draft.outputMappings, "owner AS service_owner\ntier");
  assert.equal(draft.overwrite, "preserve");
  assert.equal(lookupStateLabel(LookupState.LOOKUP_STATE_DISABLED), "Disabled");
});

test("preview normalization rejects forged shapes before rendering server-controlled cells", () => {
  const response = PreviewLookupResponse.fromPartial({
    columns: ["service_id", "owner"],
    rows: [{ values: ["billing", "<script>alert(1)</script>"] }, { values: ["search", ""] }],
    totalRows: 2n,
    sourceSha256: new Uint8Array(32).fill(3),
    contentSha256: new Uint8Array(32).fill(4),
  });
  const preview = normalizeLookupPreview(response);
  const markup = renderToStaticMarkup(createElement(LookupPreviewTable, { preview }));
  assert.match(markup, /Validated CSV preview/);
  assert.match(markup, /&lt;script&gt;alert\(1\)&lt;\/script&gt;/);
  assert.doesNotMatch(markup, /<script>/);
  assert.match(markup, /aria-label="empty string"/);

  response.rows[0]!.values.pop();
  assert.throws(() => normalizeLookupPreview(response), /row width/);

  const inconsistentTruncation = PreviewLookupResponse.fromPartial({
    columns: ["id"],
    rows: [{ values: ["one"] }],
    totalRows: 2n,
    truncated: false,
    sourceSha256: new Uint8Array(32),
    contentSha256: new Uint8Array(32),
  });
  assert.throws(() => normalizeLookupPreview(inconsistentTruncation), /success authority/);

  for (const violation of [
    { fieldPath: "é".repeat(128), code: "INVALID", message: "invalid path" },
    { fieldPath: "definition", code: "é".repeat(65), message: "invalid code" },
  ]) {
    assert.throws(() => normalizeLookupPreview(PreviewLookupResponse.fromPartial({
      sourceSha256: new Uint8Array(32),
      violations: [violation],
    })), /violations are outside/);
  }

  const invalid = normalizeLookupPreview(PreviewLookupResponse.fromPartial({
    sourceSha256: new Uint8Array(32).fill(5),
    violations: [{
      fieldPath: "csv_data",
      code: "LOOKUP_DUPLICATE_KEY",
      message: "duplicate exact composite key",
    }],
  }));
  const invalidMarkup = renderToStaticMarkup(createElement(LookupPreviewTable, {
    preview: invalid,
  }));
  assert.match(invalidMarkup, /CSV preview needs attention/);
  assert.match(invalidMarkup, /LOOKUP_DUPLICATE_KEY|duplicate exact composite key/);
});

test("lookup table exposes replace, lifecycle, and disabled-only deletion controls", () => {
  const active = lookupFixture();
  const disabled = Lookup.fromPartial({
    ...active,
    lookupId: "lookup-disabled",
    version: 4n,
    state: LookupState.LOOKUP_STATE_DISABLED,
    definition: { ...active.definition!, name: "disabled_catalog" },
    disabledAt: new Date("2026-08-13T12:00:00.000Z"),
  });
  const markup = renderToStaticMarkup(createElement(LookupManagerTable, {
    lookups: [active, disabled],
    busy: false,
    onReplace() {},
    onChangeState() {},
    onDelete() {},
  }));
  assert.match(markup, /service_catalog/);
  assert.match(markup, /Automatic \+ explicit/);
  assert.match(markup, />Disable</);
  assert.match(markup, />Enable</);
  assert.equal((markup.match(/>Delete</g) ?? []).length, 1);
});

test("the advertised backend navigation renders Lookup Manager through its gate", () => {
  const featureSource = readFileSync(path.join(process.cwd(), "app/admin/knowledge-manager-feature.ts"), "utf8");
  const consoleSource = readFileSync(path.join(process.cwd(), "app/admin/backend-admin-console.tsx"), "utf8");
  assert.match(featureSource, /key: "lookups"/);
  assert.match(consoleSource, /section === "lookups"/);
  assert.match(consoleSource, /<LookupManagerGate/);
});

import assert from "node:assert/strict";
import test from "node:test";

import { SharingScope } from "@/gen/ts/open_splunk/v1/common";
import {
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
} from "@/gen/ts/open_splunk/v1/knowledge";
import { Lookup, LookupState } from "@/gen/ts/open_splunk/v1/lookup";
import {
  DeleteLookupResponse,
  GetLookupResponse,
  ListLookupsRequest,
  ListLookupsResponse,
} from "@/gen/ts/open_splunk/v1/lookup_api";
import { PROTOBUF_CONTENT_TYPE } from "@/lib/api/protobuf-transport";

import { createLookupManagerClient, validateCSVBytes } from "./lookup-manager-data";
import {
  LOOKUP_MANAGER_CONTRACT,
  isBoundedCanonicalLookupDefinition,
} from "./lookup-manager-contract";

test("lookup manager uses one frozen backend-parity contract", () => {
  assert.equal(Object.isFrozen(LOOKUP_MANAGER_CONTRACT), true);
  assert.deepEqual(LOOKUP_MANAGER_CONTRACT, {
    maximumNameBytes: 255,
    maximumDescriptionBytes: 16 << 10,
    maximumAuthoredSourceBytes: 16 << 10,
    maximumAppIdBytes: 128,
    maximumLookupIdBytes: 128,
    maximumTenantIdBytes: 255,
    maximumOwnerIdBytes: 255,
    maximumEventFieldBytes: 8_720,
    maximumEventFieldSegments: 17,
    maximumEventFieldSegmentBytes: 256,
    maximumSelectorPatternBytes: 255,
    maximumSelectorPatternsPerDimension: 16,
    maximumSelectorPatterns: 64,
    maximumSelectorNormalizedBytes: 8 << 10,
    maximumSelectorWorkUnits: 1 << 10,
    maximumUploadBytes: 8 << 20,
    maximumAssetRows: 100_000,
    maximumColumns: 64,
    maximumCellBytes: 64 << 10,
    maximumRowBytes: 1 << 20,
    maximumHeaderBytes: 255,
    maximumKeyMappings: 4,
    maximumOutputMappings: 16,
    listPageSize: 100,
    maximumManagedLookups: 2_048,
    maximumListPages: 21,
    maximumPageTokenBytes: 4 << 10,
    maximumPreviewRows: 100,
    maximumPreviewViolations: 8,
    maximumViolationFieldPathBytes: 255,
    maximumViolationCodeBytes: 128,
    maximumViolationMessageBytes: 4 << 10,
    sha256Bytes: 32,
  });
});

test("validateCSVBytes accepts a bounded nonempty CSV", () => {
  assert.doesNotThrow(() => validateCSVBytes(new TextEncoder().encode("id,value\n1,one\n")));
});

test("validateCSVBytes rejects empty and oversized input", () => {
  assert.throws(() => validateCSVBytes(new Uint8Array()), /nonempty/);
  assert.throws(
    () => validateCSVBytes(new Uint8Array(LOOKUP_MANAGER_CONTRACT.maximumUploadBytes + 1)),
    /exceeds/,
  );
});

function managedLookup(id: string, name: string): Lookup {
  return Lookup.fromPartial({
    lookupId: id,
    tenantId: "tenant-lookups",
    ownerId: "owner-lookups",
    version: 1n,
    state: LookupState.LOOKUP_STATE_ACTIVE,
    definition: {
      appId: "app_AAAAAAAAAAAAAAAAAAAAAA",
      name,
      sharingScope: SharingScope.SHARING_SCOPE_APP,
      automatic: false,
      keyMappings: [{ lookupField: "key", eventField: "key" }],
      outputMappings: [{ lookupField: "value", eventField: "value" }],
      overwriteBehavior: KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
    },
    columns: ["key", "value"],
    rowCount: 1n,
    canonicalSizeBytes: 16n,
    sourceSha256: new Uint8Array(32).fill(1),
    contentSha256: new Uint8Array(32).fill(2),
    createdAt: new Date("2026-08-13T00:00:00.000Z"),
    updatedAt: new Date("2026-08-13T00:00:00.000Z"),
  });
}

test("lookup manager follows every bounded page cursor", async () => {
  const tokens: Array<string | undefined> = [];
  const firstPage = Array.from(
    { length: 100 },
    (_, index) => managedLookup(`lookup-${index.toString().padStart(3, "0")}`, `name-${index}`),
  );
  const pages = [
    ListLookupsResponse.fromPartial({
      lookups: firstPage,
      page: { nextPageToken: "cursor-b" },
    }),
    ListLookupsResponse.fromPartial({
      lookups: [managedLookup("lookup-b", "b")],
      page: {},
    }),
  ];
  const client = createLookupManagerClient({
    fetch: async (_input, init) => {
      const request = ListLookupsRequest.decode(init?.body as Uint8Array);
      tokens.push(request.page?.pageToken);
      const page = pages.shift();
      assert.notEqual(page, undefined);
      return new Response(ListLookupsResponse.encode(page!).finish(), {
        status: 200,
        headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
      });
    },
  });
  const lookups = await client.list("app_AAAAAAAAAAAAAAAAAAAAAA");
  assert.equal(lookups.length, 101);
  assert.equal(lookups[0]?.lookupId, "lookup-000");
  assert.equal(lookups.at(-1)?.lookupId, "lookup-b");
  assert.deepEqual(tokens, [undefined, "cursor-b"]);
});

test("lookup manager rejects non-progressing pagination authority", async () => {
  const client = createLookupManagerClient({
    fetch: async () => new Response(ListLookupsResponse.encode(ListLookupsResponse.fromPartial({
      page: { nextPageToken: "empty-next" },
    })).finish(), {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }),
  });
  await assert.rejects(() => client.list(), /invalid or repeated page cursor/);
});

test("lookup manager applies the cursor ceiling to UTF-8 bytes", async () => {
  const client = createLookupManagerClient({
    fetch: async () => new Response(ListLookupsResponse.encode(ListLookupsResponse.fromPartial({
      lookups: Array.from(
        { length: 100 },
        (_, index) => managedLookup(`lookup-${index.toString().padStart(3, "0")}`, `name-${index}`),
      ),
      page: { nextPageToken: "é".repeat(2_049) },
    })).finish(), {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }),
  });
  await assert.rejects(() => client.list(), /invalid or repeated page cursor/);
});

test("lookup manager binds get and delete responses to request authority", async () => {
  let calls = 0;
  const client = createLookupManagerClient({
    fetch: async () => {
      calls += 1;
      const body = calls === 1
        ? GetLookupResponse.encode(GetLookupResponse.fromPartial({
          lookup: managedLookup("different-id", "different"),
        })).finish()
        : DeleteLookupResponse.encode(DeleteLookupResponse.fromPartial({
          lookupId: "lookup-a",
          version: 4n,
        })).finish();
      return new Response(body, {
        status: 200,
        headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
      });
    },
  });
  await assert.rejects(() => client.get("lookup-a"), /get response authority/);
  await assert.rejects(() => client.delete("lookup-a", 1n, "a"), /delete response authority/);
});

test("lookup manager preflights repeated definition shape before detaching", async () => {
  const forged = managedLookup("lookup-a", "a");
  forged.definition!.outputMappings = Array.from({ length: 17 }, () => ({
    lookupField: "value",
    eventField: "value",
  }));
  const client = createLookupManagerClient({
    fetch: async () => new Response(GetLookupResponse.encode(GetLookupResponse.fromPartial({
      lookup: forged,
    })).finish(), {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }),
  });
  await assert.rejects(() => client.get("lookup-a"), /definition exceeds its entry limit/);
});

test("lookup manager validates every nested definition authority before UI use", async () => {
  const cases: Array<{
    readonly name: string;
    readonly mutate: (lookup: Lookup) => void;
  }> = [
    {
      name: "app",
      mutate: (lookup) => { lookup.definition!.appId = "../other-app"; },
    },
    {
      name: "name",
      mutate: (lookup) => { lookup.definition!.name = "fields"; },
    },
    {
      name: "description",
      mutate: (lookup) => { lookup.definition!.description = "unsafe\u200bdescription"; },
    },
    {
      name: "sharing",
      mutate: (lookup) => { lookup.definition!.sharingScope = SharingScope.SHARING_SCOPE_UNSPECIFIED; },
    },
    {
      name: "selector",
      mutate: (lookup) => {
        lookup.definition!.selector = {
          indexPatterns: [{
            value: "api-*",
            matchKind: KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
          }],
          hostPatterns: [],
          sourcePatterns: [],
          sourcetypePatterns: [],
        };
      },
    },
    {
      name: "mapping",
      mutate: (lookup) => { lookup.definition!.keyMappings[0]!.eventField = "a..b"; },
    },
    {
      name: "column binding",
      mutate: (lookup) => { lookup.definition!.outputMappings[0]!.lookupField = "missing"; },
    },
    {
      name: "overwrite",
      mutate: (lookup) => {
        lookup.definition!.overwriteBehavior = KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED;
      },
    },
  ];
  await Promise.all(cases.map(async (entry) => {
    const forged = managedLookup("lookup-a", "a");
    entry.mutate(forged);
    const client = createLookupManagerClient({
      fetch: async () => new Response(GetLookupResponse.encode(GetLookupResponse.fromPartial({
        lookup: forged,
      })).finish(), {
        status: 200,
        headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
      }),
    });
    await assert.rejects(() => client.get("lookup-a"), /definition authority/, entry.name);
  }));

  const definition = managedLookup("lookup-a", "a").definition!;
  (definition as { automatic: unknown }).automatic = "true";
  assert.equal(isBoundedCanonicalLookupDefinition(definition, ["key", "value"]), false);
});

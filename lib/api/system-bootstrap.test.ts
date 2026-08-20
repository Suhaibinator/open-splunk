import assert from "node:assert/strict";
import test from "node:test";

import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";

import {
  MAXIMUM_BROWSER_BOOTSTRAP_APPS,
  analyzeSPLIndexScope,
  adaptSystemBootstrap,
} from "./system-bootstrap";

test("SPL index scope analysis collects selectors after an exhaustive stage", () => {
  assert.deepEqual(
    analyzeSPLIndexScope("(index=main OR index=security) source=api | search index=audit"),
    {
      selectors: ["main", "security", "audit"],
      exhaustivelyConstrained: true,
    },
  );
});

test("system bootstrap preserves structured source identity", () => {
  const response = GetSystemBootstrapResponse.fromPartial({
    serverTime: new Date("2026-07-26T12:00:00Z"),
    build: {
      sourceRevision: "a".repeat(40),
      uiBuildId: `r${"g".repeat(40)}`,
      uiSha256: "1".repeat(64),
      protobufSchemaSha256: "2".repeat(64),
      sqliteMigrationsSha256: "3".repeat(64),
      clickhouseMigrationsSha256: "4".repeat(64),
    },
  });

  const adapted = adaptSystemBootstrap(response);

  assert.deepEqual(adapted.build, response.build);
  response.build!.sourceRevision = "mutated";
  assert.equal(adapted.build?.sourceRevision, "a".repeat(40));
});

test("system bootstrap keeps build metadata optional for development servers", () => {
  const response = GetSystemBootstrapResponse.fromPartial({
    serverTime: new Date("2026-07-26T12:00:00Z"),
  });
  assert.equal(adaptSystemBootstrap(response).build, null);
});

test("system bootstrap rejects an oversized spoofed app catalog before mapping entries", () => {
  const response = GetSystemBootstrapResponse.fromPartial({
    serverTime: new Date("2026-07-26T12:00:00Z"),
  });
  let entryReads = 0;
  response.apps = new Proxy(
    Array.from({ length: MAXIMUM_BROWSER_BOOTSTRAP_APPS + 1 }, () => ({
      appId: "must-not-be-read",
      slug: "must-not-be-read",
      displayName: "Must not be read",
      defaultIndexNames: [],
      state: 0,
    })),
    {
      get(target, property, receiver) {
        if (property !== "length") entryReads += 1;
        return Reflect.get(target, property, receiver);
      },
    },
  );

  assert.throws(() => adaptSystemBootstrap(response), /app catalog limit/);
  assert.equal(entryReads, 0);
});

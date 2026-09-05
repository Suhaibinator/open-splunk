import assert from "node:assert/strict";
import test from "node:test";

import { UiPalette } from "@/gen/ts/open_splunk/server_settings_api";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";
import { PALETTES } from "@/lib/palettes";

import {
  MAXIMUM_BROWSER_BOOTSTRAP_APPS,
  analyzeSPLIndexScope,
  adaptSystemBootstrap,
} from "./system-bootstrap";
import { paletteToProto } from "./ui-palette";

function paletteAt(uiPalette?: UiPalette) {
  return adaptSystemBootstrap(GetSystemBootstrapResponse.fromPartial({
    serverTime: new Date("2026-07-26T12:00:00Z"),
    ...(uiPalette === undefined ? {} : { uiPalette }),
  })).palette;
}

test("system bootstrap carries the instance palette and paints classic for anything else", () => {
  const at = paletteAt;
  // Absent: a server without a settings service leaves the field at zero.
  assert.equal(at(), "classic");
  assert.equal(at(UiPalette.UI_PALETTE_UNSPECIFIED), "classic");
  assert.equal(at(UiPalette.UI_PALETTE_TERMINAL), "terminal");
  // A newer server naming a palette this build does not ship.
  assert.equal(at(UiPalette.UNRECOGNIZED), "classic");
  assert.equal(at(99 as UiPalette), "classic");
  for (const palette of PALETTES) assert.equal(at(paletteToProto(palette)), palette);

  // The wire decoder hands an unknown number through untouched, so the
  // adapter has to be the place that maps it rather than the enum.
  const encoded = GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
    serverTime: new Date("2026-07-26T12:00:00Z"),
    uiPalette: 99 as UiPalette,
  })).finish();
  assert.equal(GetSystemBootstrapResponse.decode(encoded).uiPalette, 99);
  assert.equal(adaptSystemBootstrap(GetSystemBootstrapResponse.decode(encoded)).palette, "classic");
});

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

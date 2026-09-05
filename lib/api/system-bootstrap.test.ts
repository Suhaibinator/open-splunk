import assert from "node:assert/strict";
import test from "node:test";

import { UiPalette } from "@/gen/ts/open_splunk/server_settings_api";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";
import { PALETTES } from "@/lib/palettes";

import type { OpenSplunkApiClient } from "./open-splunk-client";
import {
  MAXIMUM_BROWSER_BOOTSTRAP_APPS,
  analyzeSPLIndexScope,
  adaptSystemBootstrap,
  getSystemBootstrap,
  subscribeToSystemBootstrap,
  type SystemBootstrapModel,
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

/** A client whose bootstrap answers from a queue, recording what it was asked. */
function bootstrapClient(answers: Array<() => Promise<GetSystemBootstrapResponse>>) {
  const asked: Array<string | undefined> = [];
  const client = {
    system: {
      bootstrap(request: { preferredAppId?: string }) {
        asked.push(request.preferredAppId);
        const next = answers.shift();
        assert.ok(next, "unexpected bootstrap request");
        return next();
      },
    },
  } as unknown as OpenSplunkApiClient;
  return { asked, client };
}

test("every resolved bootstrap is announced to subscribers, whoever asked and for whatever app", async () => {
  const heard: Array<[string, SystemBootstrapModel]> = [];
  const unsubscribeFirst = subscribeToSystemBootstrap((bootstrap) => heard.push(["first", bootstrap]));
  const unsubscribeSecond = subscribeToSystemBootstrap((bootstrap) => heard.push(["second", bootstrap]));
  try {
    const { asked, client } = bootstrapClient([
      () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({
        serverTime: new Date("2026-07-26T12:00:00Z"),
        selectedAppId: "ops",
        uiPalette: UiPalette.UI_PALETTE_OCEAN,
      })),
      () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({
        serverTime: new Date("2026-07-26T12:00:01Z"),
        uiPalette: UiPalette.UI_PALETTE_EMBER,
      })),
    ]);
    const withApp = await getSystemBootstrap(client, " ops ");
    assert.deepEqual(asked, ["ops"]);
    // Each caller keeps its own answer; subscribers see that same model.
    assert.equal(withApp.selectedAppId, "ops");
    assert.deepEqual(heard.map(([who, bootstrap]) => [who, bootstrap.palette, bootstrap === withApp]), [
      ["first", "ocean", true],
      ["second", "ocean", true],
    ]);

    unsubscribeFirst();
    heard.length = 0;
    const plain = await getSystemBootstrap(client);
    assert.deepEqual(asked, ["ops", undefined]);
    assert.equal(plain.palette, "ember");
    assert.deepEqual(heard.map(([who, bootstrap]) => [who, bootstrap === plain]), [["second", true]]);
  } finally {
    unsubscribeFirst();
    unsubscribeSecond();
  }
});

test("a subscriber never hears a failed bootstrap, and a throwing subscriber never fails the loader", async () => {
  const heard: SystemBootstrapModel[] = [];
  const unsubscribeThrowing = subscribeToSystemBootstrap(() => {
    throw new Error("the listener is broken");
  });
  const unsubscribeQuiet = subscribeToSystemBootstrap((bootstrap) => heard.push(bootstrap));
  try {
    const { client } = bootstrapClient([
      () => Promise.reject(new Error("HTTP 503: Service Unavailable")),
      // No clock: the adapter refuses the envelope before anyone hears of it.
      () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({})),
      () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({
        serverTime: new Date("2026-07-26T12:00:00Z"),
        uiPalette: UiPalette.UI_PALETTE_TERMINAL,
      })),
    ]);
    await assert.rejects(getSystemBootstrap(client), /503/);
    await assert.rejects(getSystemBootstrap(client), /server clock/);
    assert.deepEqual(heard, []);
    const bootstrap = await getSystemBootstrap(client);
    assert.equal(bootstrap.palette, "terminal");
    assert.deepEqual(heard, [bootstrap]);
  } finally {
    unsubscribeThrowing();
    unsubscribeQuiet();
  }
});

test("unsubscribing during an announcement neither skips a listener nor tells a removed one", async () => {
  const order: string[] = [];
  let unsubscribeLate: (() => void) | undefined;
  const unsubscribeEarly = subscribeToSystemBootstrap(() => {
    order.push("early");
    unsubscribeLate?.();
  });
  unsubscribeLate = subscribeToSystemBootstrap(() => {
    order.push("late");
  });
  const unsubscribeLast = subscribeToSystemBootstrap(() => {
    order.push("last");
  });
  try {
    const { client } = bootstrapClient([
      () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({ serverTime: new Date("2026-07-26T12:00:00Z") })),
      () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({ serverTime: new Date("2026-07-26T12:00:01Z") })),
    ]);
    await getSystemBootstrap(client);
    // The set is walked as it stood when the envelope resolved: the listener
    // removed mid-walk was still told once, the one after it was not skipped.
    assert.deepEqual(order, ["early", "late", "last"]);
    order.length = 0;
    await getSystemBootstrap(client);
    assert.deepEqual(order, ["early", "last"]);
  } finally {
    unsubscribeEarly();
    unsubscribeLate?.();
    unsubscribeLast();
  }
});

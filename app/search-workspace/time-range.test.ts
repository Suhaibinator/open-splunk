import assert from "node:assert/strict";
import test from "node:test";

import { TIME_PRESETS } from "./constants";
import {
  backendTimeRangeIntent,
  isServerExecutableTimeExpression,
  serverTimeRangeValidationError,
} from "./time-range";

const SERVER_NOW = new Date("2026-03-09T19:00:00.000Z");

test("connected time expressions include bounded day snaps and earliest data", () => {
  for (const expression of [
    "now",
    "-15m",
    "-024h",
    "-7d",
    "@d",
    "-1d@d",
    "-0001d@d",
    "-132218d@d",
    "0",
    "2026-07-22T13:00:00.123456789Z",
  ]) {
    assert.equal(
      isServerExecutableTimeExpression(expression),
      true,
      expression,
    );
  }
  for (const expression of [
    "@D",
    "@day",
    "-d@d",
    "-0d@d",
    "-132219d@d",
    "-1h@d",
    "-1d@h",
    "-1d@d+1h",
    "+1d@d",
    "00",
  ]) {
    assert.equal(
      isServerExecutableTimeExpression(expression),
      false,
      expression,
    );
  }
});

test("expression byte limits match server normalization order", () => {
  assert.equal(
    isServerExecutableTimeExpression(`${" ".repeat(1_021)}now`),
    true,
    "1,024 raw bytes",
  );
  assert.equal(
    isServerExecutableTimeExpression(`${" ".repeat(1_022)}now`),
    false,
    "1,025 raw bytes that trim to now",
  );
  assert.equal(
    isServerExecutableTimeExpression("\u0085now\u0085"),
    true,
    "Go trims Unicode next-line",
  );
  assert.equal(
    isServerExecutableTimeExpression("\uFEFFnow\uFEFF"),
    false,
    "Go does not trim a byte-order mark",
  );
});

test("every published time preset is dispatchable by the connected backend", () => {
  for (const preset of TIME_PRESETS) {
    assert.equal(
      serverTimeRangeValidationError(preset, SERVER_NOW),
      null,
      `${preset.label}: ${preset.earliest} to ${preset.latest}`,
    );
  }
});

test("connected ranges enforce endpoint and snapped-day ordering", () => {
  assert.equal(
    serverTimeRangeValidationError(
      { label: "Yesterday", earliest: "-1d@d", latest: "@d" },
      SERVER_NOW,
    ),
    null,
  );
  assert.match(
    serverTimeRangeValidationError(
      { label: "Inverted", earliest: "@d", latest: "-1d@d" },
      SERVER_NOW,
    ) ?? "",
    /Earliest/,
  );
  assert.match(
    serverTimeRangeValidationError(
      { label: "Inverted", earliest: "-1d@d", latest: "-2d@d" },
      SERVER_NOW,
    ) ?? "",
    /Earliest/,
  );
  assert.match(
    serverTimeRangeValidationError(
      { label: "Invalid latest", earliest: "-1h", latest: "0" },
      SERVER_NOW,
    ) ?? "",
    /earliest boundary/i,
  );
  assert.match(
    serverTimeRangeValidationError(
      { label: "Empty", earliest: "now", latest: "now" },
      SERVER_NOW,
    ) ?? "",
    /Earliest/,
  );
  assert.equal(
    serverTimeRangeValidationError(
      {
        label: "Nanosecond before request now",
        earliest: "2026-03-09T19:00:00.000000001Z",
        latest: "now",
      },
      SERVER_NOW,
    ),
    null,
    "a millisecond client clock must not reject a backend-valid nanosecond range",
  );
  assert.equal(
    serverTimeRangeValidationError(
      {
        label: "Nanosecond before elapsed boundary",
        earliest: "2026-03-09T18:59:59.000000001Z",
        latest: "-1s",
      },
      SERVER_NOW,
    ),
    null,
    "an elapsed boundary also uses the server's nanosecond request anchor",
  );
  assert.match(
    serverTimeRangeValidationError(
      { label: "Inverted elapsed", earliest: "-1s", latest: "-2s" },
      SERVER_NOW,
    ) ?? "",
    /Earliest/,
  );
  assert.equal(
    serverTimeRangeValidationError(
      {
        label: "Server-authoritative mixed range",
        earliest: "2027-01-01T00:00:00Z",
        latest: "@d",
        timezone: "UTC",
      },
      SERVER_NOW,
    ),
    null,
    "mixed exact/calendar ordering remains authoritative on the server",
  );
});

test("all time remains valid before 1970 and preserves the backend minimum", () => {
  assert.equal(
    serverTimeRangeValidationError(
      { label: "All time", earliest: "0", latest: "now" },
      new Date("1965-07-01T12:00:00.000Z"),
    ),
    null,
  );
  assert.match(
    serverTimeRangeValidationError(
      { label: "Before storage", earliest: "0", latest: "1899-12-31T23:59:59Z" },
      SERVER_NOW,
    ) ?? "",
    /1900–2262/,
  );
});

test("backend time intent attaches local timezone without changing authored expressions", () => {
  assert.deepEqual(
    backendTimeRangeIntent(
      { label: "Today", earliest: "@d", latest: "now" },
      false,
      "America/Los_Angeles",
    ),
    {
      earliest: "@d",
      latest: "now",
      timezone: "America/Los_Angeles",
    },
  );
  assert.deepEqual(
    backendTimeRangeIntent(
      {
        label: "Yesterday",
        earliest: "-1d@d",
        latest: "@d",
        timezone: " Pacific/Chatham ",
      },
      false,
      "America/Los_Angeles",
    ),
    {
      earliest: "-1d@d",
      latest: "@d",
      timezone: "Pacific/Chatham",
    },
  );
  assert.deepEqual(
    backendTimeRangeIntent(
      { label: "Saved all time", earliest: "0", latest: "now" },
      true,
      "America/Los_Angeles",
    ),
    {
      earliest: "0",
      latest: "now",
      timezone: undefined,
    },
  );
  assert.deepEqual(
    backendTimeRangeIntent(
      {
        label: "Go whitespace",
        earliest: "@d",
        latest: "now",
        timezone: "\u0085Pacific/Chatham\u0085",
      },
      false,
      "UTC",
    ),
    {
      earliest: "@d",
      latest: "now",
      timezone: "Pacific/Chatham",
    },
  );
});

test("timezone preflight avoids browser tzdb false negatives", () => {
  assert.equal(
    serverTimeRangeValidationError(
      {
        label: "Backend tzdb",
        earliest: "-1h",
        latest: "now",
        timezone: "Factory",
      },
      SERVER_NOW,
    ),
    null,
  );
  assert.match(
    serverTimeRangeValidationError(
      {
        label: "Host-dependent zone",
        earliest: "-1h",
        latest: "now",
        timezone: "right/UTC",
      },
      SERVER_NOW,
    ) ?? "",
    /IANA timezone/,
  );
});

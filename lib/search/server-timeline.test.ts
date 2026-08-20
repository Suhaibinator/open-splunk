import assert from "node:assert/strict";
import test from "node:test";

import type { GetSearchTimelineResponse } from "../../gen/ts/open_splunk/search_api";
import { adaptSearchTimeline } from "./server-timeline";

function trackDateTimeFormatConstructions<T>(
  action: () => T,
): { value: T; constructions: number } {
  const descriptor = Object.getOwnPropertyDescriptor(Intl, "DateTimeFormat");
  assert.ok(descriptor);
  let constructions = 0;
  const trackedDateTimeFormat = new Proxy(Intl.DateTimeFormat, {
    apply(target, thisArgument, argumentsList) {
      constructions += 1;
      return Reflect.apply(target, thisArgument, argumentsList);
    },
    construct(target, argumentsList) {
      constructions += 1;
      return Reflect.construct(target, argumentsList);
    },
  });
  Object.defineProperty(Intl, "DateTimeFormat", {
    ...descriptor,
    value: trackedDateTimeFormat,
  });
  try {
    return { value: action(), constructions };
  } finally {
    Object.defineProperty(Intl, "DateTimeFormat", descriptor);
  }
}

test("server timeline reuses one date-time formatter per response", () => {
  const firstTimestamp = Date.parse("2026-07-21T22:00:00.000Z");
  const response: GetSearchTimelineResponse = {
    buckets: Array.from({ length: 1_000 }, (_, index) => ({
      earliest: new Date(firstTimestamp + index * 60_000),
      latest: new Date(firstTimestamp + (index + 1) * 60_000),
      eventCount: BigInt(index + 1),
      partial: false,
    })),
    bucketWidth: { seconds: 60n, nanos: 0 },
    complete: true,
  };
  const expectedFirstLabel = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(firstTimestamp));

  const measured = trackDateTimeFormatConstructions(() => [
    adaptSearchTimeline(response),
    adaptSearchTimeline({
      ...response,
      buckets: response.buckets.slice(0, 1),
    }),
  ]);
  const adapted = measured.value[0];

  assert.equal(measured.constructions, 2);
  assert.equal(adapted.points.length, response.buckets.length);
  assert.equal(adapted.points[0]?.label, expectedFirstLabel);
  assert.equal(
    adapted.points.at(-1)?.latest,
    new Date(firstTimestamp + response.buckets.length * 60_000).toISOString(),
  );
  assert.equal(adapted.bucketWidthMs, 60_000);
  assert.equal(adapted.complete, true);
});

test("empty and invalid server timelines do not construct a formatter", () => {
  const empty = trackDateTimeFormatConstructions(
    () => adaptSearchTimeline({
      buckets: [],
      bucketWidth: undefined,
      complete: true,
    }),
  );
  const invalid = trackDateTimeFormatConstructions(
    () => adaptSearchTimeline({
      buckets: [{
        earliest: new Date(Number.NaN),
        latest: new Date("2026-07-21T22:01:00.000Z"),
        eventCount: 1n,
        partial: false,
      }],
      bucketWidth: undefined,
      complete: false,
    }),
  );

  assert.equal(empty.constructions, 0);
  assert.deepEqual(empty.value.points, []);
  assert.equal(invalid.constructions, 0);
  assert.deepEqual(invalid.value.points, []);
});

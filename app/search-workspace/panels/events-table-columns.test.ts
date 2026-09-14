import assert from "node:assert/strict";
import test from "node:test";

import type { DemoEvent, DemoField } from "@/lib/demo/search-data";

import { eventsTableColumns, eventsTableValue } from "./events-table-columns";

function field(name: string, selected: boolean): DemoField {
  return {
    name,
    displayName: name,
    distinctCount: 0,
    eventCount: 0,
    selected,
    interesting: !selected,
    type: "string",
    values: [],
  };
}

test("event table columns keep _time first and selected fields in rail order", () => {
  assert.deepEqual(
    eventsTableColumns([
      field("host", true),
      field("source", false),
      field("level", true),
    ]),
    [
      { id: "_time", label: "_time" },
      { id: "host", label: "host" },
      { id: "level", label: "level" },
    ],
  );
});

test("event table columns never duplicate _time", () => {
  assert.deepEqual(
    eventsTableColumns([field("host", true), field("_time", true)]),
    [
      { id: "_time", label: "_time" },
      { id: "host", label: "host" },
    ],
  );
});

test("event table values prefer typed fields and fall back to the event timestamp", () => {
  const event: DemoEvent = {
    id: "event-1",
    time: "2026-09-01T00:00:00Z",
    timeLabel: "Sep 1",
    raw: "raw event",
    fields: { host: "api-1", nullable: null },
  };

  assert.equal(eventsTableValue(event, "_time"), event.time);
  assert.equal(eventsTableValue(event, "host"), "api-1");
  assert.equal(eventsTableValue(event, "nullable"), null);
  assert.equal(eventsTableValue(event, "missing"), null);
});

import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import type { DemoEvent, DemoField } from "@/lib/demo/search-data";

import { EventsTable } from "./events-table";

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

const event: DemoEvent = {
  id: "event-1",
  time: "2026-09-01T00:00:00Z",
  timeLabel: "Sep 1, 2026",
  raw: "raw event",
  fields: { host: "api-1", count: 4 },
};

test("events table renders selected fields through SPL value formatting", () => {
  const markup = renderToStaticMarkup(
    <EventsTable
      events={[event]}
      expandedEvents={new Set()}
      fields={[field("host", true), field("source", false), field("count", true)]}
      isPreview={false}
      onToggleEvent={() => undefined}
      renderEventDetail={() => null}
    />,
  );

  assert.match(markup, /class="table table--fixed events-table"/u);
  assert.match(markup, /<th scope="col">_time<\/th><th scope="col">host<\/th><th scope="col">count<\/th>/u);
  assert.match(markup, /&quot;2026-09-01T00:00:00Z&quot;/u);
  assert.match(markup, /&quot;api-1&quot;/u);
  assert.match(markup, />4<\/code>/u);
});

test("events table shows the field-selection hint and expanded detail semantics", () => {
  const markup = renderToStaticMarkup(
    <EventsTable
      events={[event]}
      expandedEvents={new Set([event.id])}
      fields={[field("host", false)]}
      isPreview={false}
      onToggleEvent={() => undefined}
      renderEventDetail={() => <div>Shared event detail</div>}
    />,
  );

  assert.match(markup, /select fields in the rail to add columns/u);
  assert.match(markup, /aria-expanded="true"/u);
  assert.match(markup, /Shared event detail/u);
  assert.match(markup, /class="events-table__detail"/u);
});

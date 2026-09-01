import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import type { DemoEvent } from "@/lib/demo/search-data";

import type { MenuName } from "../model";
import { EventsPanel } from "./events-panel";

function pageEvents(startRow: number, count: number): DemoEvent[] {
  return Array.from({ length: count }, (_unused, index) => ({
    id: `event-${startRow + index}`,
    time: "2026-09-01T00:00:00Z",
    timeLabel: "9/1/26 12:00:00.000 AM",
    raw: `row ${startRow + index}`,
    fields: {},
  }));
}

function pageJumpInput(markup: string): string {
  const input = /<input id="event-page-number"[^>]*>/u.exec(markup);
  assert.notEqual(input, null, "the page jump input should be rendered");
  return input?.[0] ?? "";
}

function renderEventsFooter(overrides: {
  backendResultTotalRows: number | null;
  eventPage: number;
  eventPageSize: number;
  pageCount: number;
  menu?: MenuName | null;
  maximumEventPageSize?: number | null;
  backendHasNextPage?: boolean;
}): string {
  const events = pageEvents((overrides.eventPage - 1) * overrides.eventPageSize + 1, overrides.eventPageSize);
  return renderToStaticMarkup(
    <EventsPanel
      activeField={null}
      backendEnabled
      backendHasNextPage={overrides.backendHasNextPage ?? true}
      backendResultTotalExact
      backendResultTotalRows={overrides.backendResultTotalRows}
      defaultQuery="index=main"
      draggingTimeline={false}
      eventDisplay="List"
      eventPage={overrides.eventPage}
      eventPageLoading={false}
      eventPageStart={(overrides.eventPage - 1) * overrides.eventPageSize + 1}
      eventPageSize={overrides.eventPageSize}
      eventSortDirection="desc"
      expandedEvents={new Set()}
      fieldFilter=""
      fieldSummaryError={null}
      fieldSummaryLoading={false}
      fields={[]}
      fieldsCollapsed={false}
      fieldsHasMore={false}
      fieldsLoading={false}
      fieldsLoadingMore={false}
      isPreview={false}
      menu={overrides.menu ?? null}
      maximumEventPageSize={overrides.maximumEventPageSize ?? 1_000}
      pageCount={overrides.pageCount}
      pagedResultEvents={events}
      previewTruncated={false}
      resultEvents={events}
      showAllFields={false}
      submittedQuery="index=main"
      timelineDisplay="Columns"
      timelinePoints={[]}
      timelineSelection={null}
      timelineSelectionZoomable={false}
      wrapEvents={false}
      applyPivot={() => undefined}
      copyText={() => undefined}
      endTimelineDrag={() => undefined}
      moveTimelineDrag={() => undefined}
      onLoadMoreFields={() => undefined}
      onCollapsePage={() => undefined}
      onCopyPageRaw={() => undefined}
      onExpandPage={() => undefined}
      setActiveField={() => undefined}
      setEventDisplay={() => undefined}
      setEventPage={() => undefined}
      setEventPageSize={() => undefined}
      setEventSortDirection={() => undefined}
      setFieldFilter={() => undefined}
      setFieldsCollapsed={() => undefined}
      setMenu={() => undefined}
      setQuery={() => undefined}
      setShowAllFields={() => undefined}
      setTimelineDisplay={() => undefined}
      setTimelineEnd={() => undefined}
      setTimelineStart={() => undefined}
      setWrapEvents={() => undefined}
      showToast={() => undefined}
      startTimelineDrag={() => undefined}
      toggleEvent={() => undefined}
      toggleField={() => undefined}
      zoomTimeline={() => undefined}
      zoomOutTimeline={() => undefined}
      canZoomOut={false}
    />,
  );
}

test("the events footer counts every page of the reported total, not just the cursored ones", () => {
  const markup = renderEventsFooter({
    backendResultTotalRows: 95,
    eventPage: 1,
    eventPageSize: 10,
    pageCount: 10,
  });

  assert.match(markup, /Showing 1–10/u);
  assert.match(markup, /95 results/u);
  assert.match(markup, /<span>of 10<\/span>/u);
});

test("the events footer keeps the jump bound and the range aligned on a later page", () => {
  const markup = renderEventsFooter({
    backendResultTotalRows: 95,
    eventPage: 7,
    eventPageSize: 10,
    pageCount: 10,
  });

  assert.match(markup, /Showing 61–70/u);
  assert.match(markup, /<span>of 10<\/span>/u);
});

test("events per page offers 100 and 500 below the server maximum", () => {
  const markup = renderEventsFooter({
    backendResultTotalRows: 95,
    eventPage: 1,
    eventPageSize: 10,
    pageCount: 10,
    menu: "event-page-size",
  });

  const offered = [...markup.matchAll(/<strong>(\d+) events<\/strong>/gu)].map((match) => Number(match[1]));
  assert.deepEqual(offered, [10, 20, 50, 100, 500, 1_000]);
  assert.doesNotMatch(markup, /Above server limit/u);
});

test("events per page keeps sizes above a lower server maximum but disables them", () => {
  const markup = renderEventsFooter({
    backendResultTotalRows: 95,
    eventPage: 1,
    eventPageSize: 10,
    pageCount: 10,
    menu: "event-page-size",
    maximumEventPageSize: 100,
  });

  const offered = [...markup.matchAll(/<strong>(\d+) events<\/strong>/gu)].map((match) => Number(match[1]));
  assert.deepEqual(offered, [10, 20, 50, 100, 500]);
  assert.match(markup, /Above server limit/u);
});

test("the page jump is not capped by the count while the server offers a next cursor", () => {
  // A page can hold fewer rows than the page size, so the count derived from the reported total
  // is a lower bound. Capping the jump at it would refuse pages the cursor chain still reaches.
  const markup = renderEventsFooter({
    backendResultTotalRows: 5_000,
    eventPage: 1,
    eventPageSize: 500,
    pageCount: 10,
  });

  assert.doesNotMatch(pageJumpInput(markup), /max="/u);
});

test("the page jump is capped by the count once the cursor chain is exhausted", () => {
  const markup = renderEventsFooter({
    backendResultTotalRows: 95,
    eventPage: 10,
    eventPageSize: 10,
    pageCount: 10,
    backendHasNextPage: false,
  });

  assert.match(pageJumpInput(markup), /max="10"/u);
});

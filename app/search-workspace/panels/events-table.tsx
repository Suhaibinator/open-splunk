import type { KeyboardEvent, MouseEvent, ReactNode } from "react";

import type { DemoEvent, DemoField } from "@/lib/demo/search-data";
import { formatSplValue } from "@/lib/search/spl-syntax";

import { AppIcon } from "../../_components/app-icon";
import { eventsTableColumns, eventsTableValue } from "./events-table-columns";

interface EventsTableProps {
  events: readonly DemoEvent[];
  expandedEvents: ReadonlySet<string>;
  fields: readonly DemoField[];
  isPreview: boolean;
  onToggleEvent: (eventId: string) => void;
  renderEventDetail: (event: DemoEvent) => ReactNode;
}

function eventRowLabel(event: DemoEvent, expanded: boolean): string {
  return `${expanded ? "Collapse" : "Expand"} event at ${event.timeLabel}`;
}

export function EventsTable({
  events,
  expandedEvents,
  fields,
  isPreview,
  onToggleEvent,
  renderEventDetail,
}: EventsTableProps) {
  const columns = eventsTableColumns(fields);
  const hasSelectedFieldColumns = columns.length > 1;

  function toggleFromRow(event: MouseEvent<HTMLTableRowElement>, eventId: string): void {
    if (isPreview) return;
    const target = event.target;
    if (target instanceof Element && target.closest("button, a, input, select, textarea")) return;
    onToggleEvent(eventId);
  }

  function toggleFromKeyboard(event: KeyboardEvent<HTMLTableRowElement>, eventId: string): void {
    if (isPreview || (event.key !== "Enter" && event.key !== " ")) return;
    const target = event.target;
    if (target instanceof Element && target.closest("button, a, input, select, textarea")) return;
    event.preventDefault();
    onToggleEvent(eventId);
  }

  return (
    <div className="table-wrap events-table-wrap" data-testid="event-list">
      <table className="table table--fixed events-table" aria-label={isPreview ? "Live preview events table" : "Events table"}>
        <thead>
          <tr>
            {columns.map((column) => <th scope="col" key={column.id}>{column.label}</th>)}
          </tr>
        </thead>
        <tbody>
          {!hasSelectedFieldColumns ? (
            <tr className="events-table__hint">
              <td colSpan={columns.length}>select fields in the rail to add columns</td>
            </tr>
          ) : null}
          {events.map((event) => {
            const expanded = !isPreview && expandedEvents.has(event.id);
            return (
              <EventsTableRow
                columns={columns}
                event={event}
                expanded={expanded}
                isPreview={isPreview}
                key={event.id}
                onClick={toggleFromRow}
                onKeyDown={toggleFromKeyboard}
                onToggleEvent={onToggleEvent}
                renderEventDetail={renderEventDetail}
              />
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

interface EventsTableRowProps {
  columns: ReturnType<typeof eventsTableColumns>;
  event: DemoEvent;
  expanded: boolean;
  isPreview: boolean;
  onClick: (event: MouseEvent<HTMLTableRowElement>, eventId: string) => void;
  onKeyDown: (event: KeyboardEvent<HTMLTableRowElement>, eventId: string) => void;
  onToggleEvent: (eventId: string) => void;
  renderEventDetail: (event: DemoEvent) => ReactNode;
}

function EventsTableRow({
  columns,
  event,
  expanded,
  isPreview,
  onClick,
  onKeyDown,
  onToggleEvent,
  renderEventDetail,
}: EventsTableRowProps) {
  return (
    <>
      <tr
        className={`events-table__event${expanded ? " is-expanded" : ""}`}
        data-event-id={event.id}
        data-testid={`event-row-${event.id}`}
        tabIndex={isPreview ? undefined : 0}
        aria-label={isPreview ? `Provisional event at ${event.timeLabel}` : eventRowLabel(event, expanded)}
        onClick={(pointerEvent) => onClick(pointerEvent, event.id)}
        onKeyDown={(keyboardEvent) => onKeyDown(keyboardEvent, event.id)}
      >
        {columns.map((column, columnIndex) => (
          <td data-label={column.label} key={column.id}>
            {columnIndex === 0 ? (
              <button
                className="events-table__expander"
                type="button"
                aria-disabled={isPreview}
                aria-expanded={expanded}
                aria-label={isPreview ? "Event details unavailable during live preview" : eventRowLabel(event, expanded)}
                onClick={() => {
                  if (!isPreview) onToggleEvent(event.id);
                }}
              >
                <AppIcon name={expanded ? "chevron-down" : "chevron-right"} size="sm" />
              </button>
            ) : null}
            <code>{formatSplValue(eventsTableValue(event, column.id))}</code>
          </td>
        ))}
      </tr>
      {expanded ? (
        <tr className="events-table__detail">
          <td colSpan={columns.length}>{renderEventDetail(event)}</td>
        </tr>
      ) : null}
    </>
  );
}

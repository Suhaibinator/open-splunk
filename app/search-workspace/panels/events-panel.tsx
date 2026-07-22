import type { Dispatch, PointerEvent, SetStateAction } from "react";

import type { DemoEvent, DemoField, DemoScalar, TimelinePoint } from "@/lib/demo/search-data";
import type { PivotMode } from "@/lib/search/query-pivots";

import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import type { EventDisplay, MenuName, TimelineDisplay } from "../model";
import { formatFieldValue, highlightedRaw } from "../workspace-utils";

interface EventsPanelProps {
  activeField: string | null;
  backendEnabled: boolean;
  draggingTimeline: boolean;
  eventDisplay: EventDisplay;
  eventPage: number;
  eventPageCount: number;
  eventPageSize: 10 | 20 | 50;
  eventSortDirection: "asc" | "desc";
  expandedEvents: Set<string>;
  fieldFilter: string;
  fields: DemoField[];
  fieldsCollapsed: boolean;
  menu: MenuName | null;
  pagedResultEvents: DemoEvent[];
  resultEvents: DemoEvent[];
  showAllFields: boolean;
  submittedQuery: string;
  timelineDisplay: TimelineDisplay;
  timelinePoints: TimelinePoint[];
  timelineSelection: readonly [number, number] | null;
  wrapEvents: boolean;
  applyPivot: (field: string, value: DemoScalar, mode: PivotMode, runImmediately?: boolean) => void;
  copyText: (text: string, message: string) => Promise<void> | void;
  endTimelineDrag: (event: PointerEvent<HTMLDivElement>) => void;
  moveTimelineDrag: (event: PointerEvent<HTMLDivElement>) => void;
  setActiveField: Dispatch<SetStateAction<string | null>>;
  setEventDisplay: Dispatch<SetStateAction<EventDisplay>>;
  setEventPage: Dispatch<SetStateAction<number>>;
  setEventPageSize: Dispatch<SetStateAction<10 | 20 | 50>>;
  setEventSortDirection: Dispatch<SetStateAction<"asc" | "desc">>;
  setFieldFilter: Dispatch<SetStateAction<string>>;
  setFieldsCollapsed: Dispatch<SetStateAction<boolean>>;
  setMenu: Dispatch<SetStateAction<MenuName | null>>;
  setQuery: Dispatch<SetStateAction<string>>;
  setShowAllFields: Dispatch<SetStateAction<boolean>>;
  setTimelineDisplay: Dispatch<SetStateAction<TimelineDisplay>>;
  setTimelineEnd: Dispatch<SetStateAction<number | null>>;
  setTimelineStart: Dispatch<SetStateAction<number | null>>;
  setWrapEvents: Dispatch<SetStateAction<boolean>>;
  showToast: (message: string, tone?: "success" | "info" | "warning") => void;
  startTimelineDrag: (event: PointerEvent<HTMLDivElement>) => void;
  toggleEvent: (eventId: string) => void;
  toggleField: (fieldName: string) => void;
  zoomTimeline: () => void;
}

export function EventsPanel({
  activeField,
  backendEnabled,
  draggingTimeline,
  eventDisplay,
  eventPage,
  eventPageCount,
  eventPageSize,
  eventSortDirection,
  expandedEvents,
  fieldFilter,
  fields,
  fieldsCollapsed,
  menu,
  pagedResultEvents,
  resultEvents,
  showAllFields,
  submittedQuery,
  timelineDisplay,
  timelinePoints,
  timelineSelection,
  wrapEvents,
  applyPivot,
  copyText,
  endTimelineDrag,
  moveTimelineDrag,
  setActiveField,
  setEventDisplay,
  setEventPage,
  setEventPageSize,
  setEventSortDirection,
  setFieldFilter,
  setFieldsCollapsed,
  setMenu,
  setQuery,
  setShowAllFields,
  setTimelineDisplay,
  setTimelineEnd,
  setTimelineStart,
  setWrapEvents,
  showToast,
  startTimelineDrag,
  toggleEvent,
  toggleField,
  zoomTimeline,
}: EventsPanelProps) {
  const selectedFields = fields.filter((field) => field.selected);
  const interestingFields = fields.filter((field) => field.interesting && !field.selected);
  const visibleInterestingFields = (showAllFields ? interestingFields : interestingFields.slice(0, 8)).filter((field) =>
    field.name.toLowerCase().includes(fieldFilter.toLowerCase()),
  );
  const activeFieldData = fields.find((field) => field.name === activeField) ?? null;
  const maxTimelineCount = Math.max(1, ...timelinePoints.map((point) => point.count));

  return (
      <section id="panel-events" role="tabpanel" aria-labelledby="tab-events" className="events-panel">
        <section className="timeline-section" data-testid="timeline" aria-label="Event timeline">
          <div className="timeline-toolbar">
            <div>
              <div className="header-menu-wrap result-menu-wrap">
                <button type="button" aria-haspopup="menu" aria-expanded={menu === "timeline-format"} onClick={() => setMenu(menu === "timeline-format" ? null : "timeline-format")}>Format Timeline <span aria-hidden="true">▾</span></button>
                {menu === "timeline-format" ? (
                  <div className="floating-menu result-control-menu" role="menu" aria-label="Timeline format">
                    {(["Columns", "Compact"] as const).map((display) => (
                      <button role="menuitemradio" aria-checked={timelineDisplay === display} type="button" key={display} onClick={() => { setTimelineDisplay(display); setMenu(null); }}><span className="radio-mark">{timelineDisplay === display ? "●" : "○"}</span><span><strong>{display}</strong><small>{display === "Columns" ? "Full-height event volume" : "Condensed activity profile"}</small></span></button>
                    ))}
                  </div>
                ) : null}
              </div>
              <button type="button" onClick={() => showToast("Timeline is already at the full selected range.")}>− Zoom Out</button>
              <button type="button" disabled={timelineSelection === null} onClick={zoomTimeline}>＋ Zoom to Selection</button>
              <button type="button" disabled={timelineSelection === null} onClick={() => { setTimelineStart(null); setTimelineEnd(null); }}>× Deselect</button>
            </div>
            <span>{timelineSelection === null ? (backendEnabled ? "Automatic time span" : "20 minutes per column") : `${timelinePoints[timelineSelection[0]]?.label ?? ""} – ${timelinePoints[timelineSelection[1]]?.label ?? ""}`}</span>
          </div>
          <div
            className={`timeline-chart timeline-${timelineDisplay.toLowerCase()}${draggingTimeline ? " dragging" : ""}`}
            onPointerDown={startTimelineDrag}
            onPointerMove={moveTimelineDrag}
            onPointerUp={endTimelineDrag}
            onPointerCancel={endTimelineDrag}
          >
            <div className="timeline-grid-lines" aria-hidden="true"><span /><span /><span /></div>
            <div className="timeline-bars">
              {timelinePoints.map((point, index) => {
                const selected = timelineSelection !== null && index >= timelineSelection[0] && index <= timelineSelection[1];
                return (
                  <button
                    type="button"
                    className={selected ? "selected" : ""}
                    data-timeline-index={index}
                    aria-label={`${point.label}: ${NUMBER_FORMAT.format(point.count)} events`}
                    title={`${point.label}\n${NUMBER_FORMAT.format(point.count)} events`}
                    key={point.id}
                    style={{ height: `${Math.max(4, (point.count / maxTimelineCount) * 100)}%` }}
                  />
                );
              })}
            </div>
            {timelineSelection === null ? null : (
              <div
                className="timeline-selection"
                aria-hidden="true"
                style={{
                  left: `${(timelineSelection[0] / timelinePoints.length) * 100}%`,
                  width: `${((timelineSelection[1] - timelineSelection[0] + 1) / timelinePoints.length) * 100}%`,
                }}
              />
            )}
          </div>
          <div className="timeline-axis" aria-hidden="true"><span>Jul 20, 4 PM</span><span>Jul 20, 10 PM</span><span>Jul 21, 4 AM</span><span>Jul 21, 10 AM</span><span>Jul 21, 4 PM</span></div>
        </section>

        <div className={`events-layout${fieldsCollapsed ? " fields-collapsed" : ""}`}>
          <aside className="fields-rail" data-testid="fields-rail" aria-label="Search fields">
            <div className="fields-topbar">
              <button type="button" onClick={() => setFieldsCollapsed(!fieldsCollapsed)}><span aria-hidden="true">{fieldsCollapsed ? "»" : "‹"}</span>{fieldsCollapsed ? null : "Hide Fields"}</button>
              {fieldsCollapsed ? null : <button type="button" onClick={() => setShowAllFields(true)}>▦ All Fields</button>}
            </div>
            {fieldsCollapsed ? (
              <button className="vertical-fields-label" type="button" onClick={() => setFieldsCollapsed(false)}>Fields</button>
            ) : (
              <>
                <label className="field-filter">
                  <span aria-hidden="true">⌕</span>
                  <input aria-label="Filter fields" placeholder="Filter fields" value={fieldFilter} onChange={(event) => setFieldFilter(event.target.value)} />
                </label>
                <div className="field-group">
                  <h2>Selected Fields <span>{selectedFields.length}</span></h2>
                  <div className="field-list">
                    {selectedFields.filter((field) => field.name.toLowerCase().includes(fieldFilter.toLowerCase())).map((field) => (
                      <button type="button" key={field.name} className={activeField === field.name ? "active" : ""} onClick={() => setActiveField(activeField === field.name ? null : field.name)}>
                        <span className={`field-type type-${field.type}`}>{field.type === "number" ? "#" : field.type === "boolean" ? "✓" : "a"}</span>
                        <span>{field.displayName}</span><b>{COMPACT_NUMBER_FORMAT.format(field.distinctCount)}</b>
                      </button>
                    ))}
                  </div>
                </div>
                <div className="field-group interesting-group">
                  <h2>Interesting Fields <span>{interestingFields.length}</span></h2>
                  <div className="field-list">
                    {visibleInterestingFields.map((field) => (
                      <button type="button" key={field.name} className={activeField === field.name ? "active" : ""} onClick={() => setActiveField(activeField === field.name ? null : field.name)}>
                        <span className={`field-type type-${field.type}`}>{field.type === "number" ? "#" : field.type === "boolean" ? "✓" : "a"}</span>
                        <span>{field.displayName}</span><b>{COMPACT_NUMBER_FORMAT.format(field.distinctCount)}</b>
                      </button>
                    ))}
                  </div>
                  {!showAllFields && interestingFields.length > 8 ? <button className="more-fields" type="button" onClick={() => setShowAllFields(true)}>Show {interestingFields.length - 8} more fields</button> : null}
                </div>
              </>
            )}
            {activeFieldData === null || fieldsCollapsed ? null : (
              <section className="field-inspector" data-testid="field-inspector" aria-label={`${activeFieldData.displayName} field summary`}>
                <header>
                  <div><span className={`field-type type-${activeFieldData.type}`}>{activeFieldData.type === "number" ? "#" : "a"}</span><strong>{activeFieldData.displayName}</strong></div>
                  <button className="icon-button" type="button" aria-label="Close field summary" onClick={() => setActiveField(null)}>×</button>
                </header>
                <div className="field-summary-meta">
                  <span><strong>{NUMBER_FORMAT.format(activeFieldData.eventCount)}</strong> events</span>
                  <span><strong>{NUMBER_FORMAT.format(activeFieldData.distinctCount)}</strong> values</span>
                </div>
                <label className="select-field-checkbox"><input type="checkbox" checked={activeFieldData.selected} onChange={() => toggleField(activeFieldData.name)} /> Selected field</label>
                <h3>Top values</h3>
                <div className="top-values">
                  {activeFieldData.values.map((item) => {
                    const percent = Math.max(2, (item.count / activeFieldData.eventCount) * 100);
                    return (
                      <div className="top-value" key={formatFieldValue(item.value)}>
                        <button type="button" title="Include this value" onClick={() => applyPivot(activeFieldData.name, item.value, "include")}><span>{formatFieldValue(item.value)}</span><b>{NUMBER_FORMAT.format(item.count)}</b></button>
                        <span className="value-bar" style={{ width: `${percent}%` }} />
                        <div><button type="button" onClick={() => applyPivot(activeFieldData.name, item.value, "include")} aria-label={`Include ${formatFieldValue(item.value)}`}>＋</button><button type="button" onClick={() => applyPivot(activeFieldData.name, item.value, "exclude")} aria-label={`Exclude ${formatFieldValue(item.value)}`}>−</button></div>
                      </div>
                    );
                  })}
                </div>
                <footer><button type="button" onClick={() => setShowAllFields(true)}>View all values</button><button type="button" onClick={() => applyPivot(activeFieldData.name, activeFieldData.values[0]?.value ?? "*", "new", true)}>New search</button></footer>
              </section>
            )}
          </aside>

          {!fieldsCollapsed ? <button className="fields-mobile-dismiss" aria-label="Close fields panel" type="button" onClick={() => setFieldsCollapsed(true)} /> : null}
          <section className={`event-results display-${eventDisplay.toLowerCase()}${wrapEvents ? " wrap-events" : " nowrap-events"}`} aria-label="Events">
            <div className="event-toolbar">
              <div>
                <button className="mobile-fields-button" type="button" onClick={() => setFieldsCollapsed(false)}>☰ Fields</button>
                <div className="header-menu-wrap result-menu-wrap">
                  <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-display"} onClick={() => setMenu(menu === "event-display" ? null : "event-display")}>{eventDisplay} <span aria-hidden="true">▾</span></button>
                  {menu === "event-display" ? (
                    <div className="floating-menu result-control-menu" role="menu" aria-label="Event display">
                      {(["List", "Raw"] as const).map((display) => (
                        <button role="menuitemradio" aria-checked={eventDisplay === display} type="button" key={display} onClick={() => { setEventDisplay(display); setMenu(null); }}><span className="radio-mark">{eventDisplay === display ? "●" : "○"}</span><span><strong>{display}</strong><small>{display === "List" ? "Fields, metadata, and raw event" : "Raw event text with minimal chrome"}</small></span></button>
                      ))}
                    </div>
                  ) : null}
                </div>
                <button type="button" aria-pressed={wrapEvents} title={wrapEvents ? "Turn event wrapping off" : "Wrap long event text"} onClick={() => setWrapEvents((current) => !current)}><span aria-hidden="true">✎</span> {wrapEvents ? "Wrap on" : "Wrap off"}</button>
                <div className="header-menu-wrap result-menu-wrap">
                  <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-page-size"} onClick={() => setMenu(menu === "event-page-size" ? null : "event-page-size")}>{eventPageSize} Per Page <span aria-hidden="true">▾</span></button>
                  {menu === "event-page-size" ? (
                    <div className="floating-menu result-control-menu page-size-menu" role="menu" aria-label="Events per page">
                      {([10, 20, 50] as const).map((size) => <button role="menuitemradio" aria-checked={eventPageSize === size} type="button" key={size} onClick={() => { setEventPageSize(size); setEventPage(1); setMenu(null); }}>{eventPageSize === size ? "✓" : ""}<span><strong>{size} events</strong></span></button>)}
                    </div>
                  ) : null}
                </div>
              </div>
              <nav aria-label="Event pages">
                <button type="button" disabled={eventPage === 1} onClick={() => setEventPage((current) => Math.max(1, current - 1))}>‹ Prev</button>
                {[1, 2, 3].filter((page) => page <= eventPageCount).map((page) => <button className={eventPage === page ? "active" : ""} aria-current={eventPage === page ? "page" : undefined} type="button" key={page} onClick={() => setEventPage(page)}>{page}</button>)}
                {eventPage > 3 && eventPage < eventPageCount ? <><span>…</span><button className="active" aria-current="page" type="button">{NUMBER_FORMAT.format(eventPage)}</button></> : null}
                {eventPageCount > 4 ? <span>…</span> : null}
                {eventPageCount > 3 ? <button className={eventPage === eventPageCount ? "active" : ""} aria-current={eventPage === eventPageCount ? "page" : undefined} type="button" onClick={() => setEventPage(eventPageCount)}>{NUMBER_FORMAT.format(eventPageCount)}</button> : null}
                <button type="button" disabled={eventPage === eventPageCount} onClick={() => setEventPage((current) => Math.min(eventPageCount, current + 1))}>Next ›</button>
              </nav>
            </div>
            <div className="event-head"><span /><button type="button" aria-label={`Sort by time, ${eventSortDirection === "desc" ? "ascending" : "descending"}`} onClick={() => { setEventSortDirection((current) => current === "desc" ? "asc" : "desc"); setEventPage(1); }}>Time <span aria-hidden="true">{eventSortDirection === "desc" ? "↓" : "↑"}</span></button><span>Event</span></div>
            <div className="event-list" data-testid="event-list">
              {pagedResultEvents.map((event) => {
                const expanded = expandedEvents.has(event.id);
                const level = String(event.fields.level ?? "INFO").toLowerCase();
                return (
                  <article className={`event-row level-${level}${expanded ? " expanded" : ""}`} data-testid={`event-row-${event.id}`} key={event.id}>
                    <button className="event-expander" type="button" aria-label={`${expanded ? "Collapse" : "Expand"} event`} aria-expanded={expanded} onClick={() => toggleEvent(event.id)}>{expanded ? "⌄" : "›"}</button>
                    <button className="event-time" type="button" title="Find nearby events" onClick={() => showToast("Choose a nearby interval from the time range picker.")}><span>{event.timeLabel.split(", ")[0]}</span><strong>{event.timeLabel.split(", ").slice(1).join(", ")}</strong></button>
                    <div className="event-content">
                      <button className="event-raw" type="button" aria-label={`${expanded ? "Collapse" : "Expand"} event details`} onClick={() => toggleEvent(event.id)}>{highlightedRaw(event.raw, submittedQuery)}</button>
                      <div className="event-chips">
                        {["host", "source", "sourcetype"].map((fieldName) => (
                          <button type="button" key={fieldName} onClick={() => setActiveField(fieldName)}><span>{fieldName}</span> = {formatFieldValue(event.fields[fieldName] ?? "")}</button>
                        ))}
                      </div>
                      {expanded ? (
                        <div className="event-detail">
                          <header><strong>Event fields</strong><span>{Object.keys(event.fields).length} fields · typed JSON</span><button type="button" onClick={() => void copyText(event.raw, "Raw event copied.")}>Copy raw</button></header>
                          <div className="event-field-grid">
                            {Object.entries(event.fields).map(([fieldName, fieldValue]) => (
                              <div className="event-field" key={fieldName}>
                                <button className="event-field-name" type="button" onClick={() => setActiveField(fieldName)}>{fieldName}</button>
                                <span className={`value-type value-${fieldValue === null ? "null" : typeof fieldValue}`}>{fieldValue === null ? "null" : typeof fieldValue}</span>
                                <code>{formatFieldValue(fieldValue)}</code>
                                <div className="event-field-actions">
                                  <button type="button" title="Include in current search" aria-label={`Include ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "include")}>＋</button>
                                  <button type="button" title="Exclude from current search" aria-label={`Exclude ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "exclude")}>−</button>
                                  <button type="button" title="Open as new search" aria-label={`New search for ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "new", true)}>⌕</button>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </article>
                );
              })}
              {resultEvents.length === 0 ? <div className="empty-state event-empty"><strong>No events found</strong><span>Widen the time range or remove a field filter.</span><button className="button secondary compact" type="button" onClick={() => setQuery("index=gradethis")}>Reset search</button></div> : null}
            </div>
          </section>
        </div>
      </section>
  );
}

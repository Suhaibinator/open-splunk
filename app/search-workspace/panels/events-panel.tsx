import { type Dispatch, type FormEvent, type KeyboardEvent, type PointerEvent, type SetStateAction, useEffect, useMemo, useRef, useState } from "react";

import type { DemoEvent, DemoField, DemoScalar, TimelinePoint } from "@/lib/demo/search-data";
import type { PivotMode } from "@/lib/search/query-pivots";

import { installModalSurface } from "../../_components/modal-surface";
import { AppIcon } from "../../_components/app-icon";
import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "../constants";
import { formatExactInteger, formatExactNumericText } from "../formatters";
import type { EventDisplay, MenuName, TimelineDisplay } from "../model";
import {
  eventFieldValueWhiteSpace,
  formatFieldValue,
  highlightedRaw,
} from "../workspace-utils";

interface EventsPanelProps {
  activeField: string | null;
  backendEnabled: boolean;
  backendHasNextPage: boolean;
  backendResultTotalExact: boolean;
  backendResultTotalRows: number | null;
  defaultQuery: string;
  draggingTimeline: boolean;
  eventDisplay: EventDisplay;
  eventPage: number;
  eventPageLoading: boolean;
  eventPageStart: number | null;
  eventPageSize: number;
  eventSortDirection: "asc" | "desc";
  expandedEvents: Set<string>;
  fieldFilter: string;
  fieldSummaryError: string | null;
  fieldSummaryLoading: boolean;
  fields: DemoField[];
  fieldsCollapsed: boolean;
  fieldsHasMore: boolean;
  fieldsLoading: boolean;
  fieldsLoadingMore: boolean;
  isPreview: boolean;
  menu: MenuName | null;
  maximumEventPageSize: number | null;
  maximumReachablePage: number;
  pagedResultEvents: DemoEvent[];
  previewTruncated: boolean;
  resultEvents: DemoEvent[];
  showAllFields: boolean;
  submittedQuery: string;
  timelineDisplay: TimelineDisplay;
  timelinePoints: TimelinePoint[];
  timelineSelection: readonly [number, number] | null;
  timelineSelectionZoomable: boolean;
  wrapEvents: boolean;
  applyPivot: (field: string, value: DemoScalar, mode: PivotMode, runImmediately?: boolean) => void;
  copyText: (text: string, message: string) => Promise<void> | void;
  endTimelineDrag: (event: PointerEvent<HTMLInputElement>) => void;
  moveTimelineDrag: (event: PointerEvent<HTMLInputElement>) => void;
  onLoadMoreFields: () => void;
  onCollapsePage: () => void;
  onCopyPageRaw: () => void;
  onExpandPage: () => void;
  setActiveField: Dispatch<SetStateAction<string | null>>;
  setEventDisplay: Dispatch<SetStateAction<EventDisplay>>;
  setEventPage: Dispatch<SetStateAction<number>>;
  setEventPageSize: (size: number) => void;
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
  startTimelineDrag: (event: PointerEvent<HTMLInputElement>) => void;
  toggleEvent: (eventId: string) => void;
  toggleField: (fieldName: string) => void;
  zoomTimeline: () => void;
  zoomOutTimeline: () => void;
  canZoomOut: boolean;
}

function scalarIdentity(value: DemoScalar): string {
  if (value === null) return "null";
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "number:NaN";
    if (Object.is(value, -0)) return "number:-0";
  }
  return `${typeof value}:${JSON.stringify(value)}`;
}

function scalarTypeLabel(value: DemoScalar): string {
  return value === null ? "null" : typeof value;
}

function formatTimelineCount(point: TimelinePoint): string {
  return point.exactCount === undefined
    ? NUMBER_FORMAT.format(point.count)
    : formatExactNumericText(point.exactCount);
}

function formatProjectedCount(
  coordinate: number,
  exact: string | undefined,
  approximate = false,
  compact = false,
): string {
  const formatter = compact ? COMPACT_NUMBER_FORMAT : NUMBER_FORMAT;
  const formattedExact = exact === undefined ? null : formatExactInteger(exact, compact);
  const formatted = formattedExact ?? formatter.format(coordinate);
  return `${approximate ? "≈" : ""}${formatted}`;
}

function projectedCountTitle(
  label: string,
  coordinate: number,
  exact: string | undefined,
  approximate = false,
): string {
  const formatted = formatProjectedCount(coordinate, exact, approximate);
  const qualifiers = [
    approximate ? "Backend estimate" : null,
    exact === undefined ? null : "Full uint64 retained; visual scale uses an approximate browser coordinate",
  ].filter((qualifier): qualifier is string => qualifier !== null);
  return qualifiers.length === 0 ? `${label}: ${formatted}` : `${label}: ${formatted}\n${qualifiers.join(". ")}`;
}

export function EventsPanel({
  activeField,
  backendEnabled,
  backendHasNextPage,
  backendResultTotalExact,
  backendResultTotalRows,
  defaultQuery,
  draggingTimeline,
  eventDisplay,
  eventPage,
  eventPageLoading,
  eventPageStart,
  eventPageSize,
  eventSortDirection,
  expandedEvents,
  fieldFilter,
  fieldSummaryError,
  fieldSummaryLoading,
  fields,
  fieldsCollapsed,
  fieldsHasMore,
  fieldsLoading,
  fieldsLoadingMore,
  isPreview,
  menu,
  maximumEventPageSize,
  maximumReachablePage,
  pagedResultEvents,
  previewTruncated,
  resultEvents,
  showAllFields,
  submittedQuery,
  timelineDisplay,
  timelinePoints,
  timelineSelection,
  timelineSelectionZoomable,
  wrapEvents,
  applyPivot,
  copyText,
  endTimelineDrag,
  moveTimelineDrag,
  onLoadMoreFields,
  onCollapsePage,
  onCopyPageRaw,
  onExpandPage,
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
  zoomOutTimeline,
  canZoomOut,
}: EventsPanelProps) {
  const [timelineKeyboardIndex, setTimelineKeyboardIndex] = useState(0);
  const [mobileFieldsMode, setMobileFieldsMode] = useState(false);
  const [pageJumpDraft, setPageJumpDraft] = useState({ page: eventPage, value: String(eventPage) });
  const fieldsRailRef = useRef<HTMLElement>(null);
  const fieldsReturnFocusRef = useRef<HTMLElement | null>(null);
  const mobileFieldsButtonRef = useRef<HTMLButtonElement>(null);
  const selectedFields = fields.filter((field) => field.selected);
  const interestingFields = fields.filter((field) => field.interesting && !field.selected);
  const matchingInterestingFields = interestingFields.filter((field) => field.name.toLowerCase().includes(fieldFilter.toLowerCase()));
  const visibleInterestingFields = showAllFields || fieldFilter.trim().length > 0 ? matchingInterestingFields : matchingInterestingFields.slice(0, 8);
  const activeFieldData = fields.find((field) => field.name === activeField) ?? null;
  const firstPivotableFieldValue = activeFieldData?.values.find((value) => value.pivotable !== false);
  const eventPageSizeOptions = [...new Set([
    10,
    20,
    50,
    ...(maximumEventPageSize === null ? [] : [maximumEventPageSize]),
    eventPageSize,
  ])].filter((size) => size > 0).toSorted((left, right) => left - right);
  const pageEventIds = pagedResultEvents.map((event) => event.id);
  const expandedPageEventCount = pageEventIds.filter((eventId) => expandedEvents.has(eventId)).length;
  const pageStart = pagedResultEvents.length === 0 ? 0 : eventPageStart;
  const pageEnd = pageStart === null || pageStart === 0
    ? null
    : pageStart + pagedResultEvents.length - 1;
  const pageRangeLabel = pagedResultEvents.length === 0
    ? "Showing 0 events"
    : pageStart === null || pageEnd === null
      ? `${NUMBER_FORMAT.format(pagedResultEvents.length)} events on this page`
      : `Showing ${NUMBER_FORMAT.format(pageStart)}–${NUMBER_FORMAT.format(pageEnd)}`;
  const totalResultLabel = backendResultTotalRows === null
    ? "total unavailable"
    : `${backendResultTotalExact ? "" : "at least "}${NUMBER_FORMAT.format(backendResultTotalRows)} results`;
  const pageJumpValue = pageJumpDraft.page === eventPage
    ? pageJumpDraft.value
    : String(eventPage);

  function submitPageJump(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const requestedPage = /^\d+$/.test(pageJumpValue) ? Number(pageJumpValue) : Number.NaN;
    if (
      !Number.isSafeInteger(requestedPage)
      || requestedPage < 1
      || requestedPage > maximumReachablePage
    ) {
      showToast(`Choose a page from 1 to ${NUMBER_FORMAT.format(maximumReachablePage)}.`, "warning");
      setPageJumpDraft({ page: eventPage, value: String(eventPage) });
      return;
    }
    if (requestedPage !== eventPage) setEventPage(requestedPage);
    setPageJumpDraft({ page: requestedPage, value: String(requestedPage) });
  }
  const ambiguousTopValueLabels = useMemo(() => {
    const identitiesByLabel = new Map<string, Set<string>>();
    for (const item of activeFieldData?.values ?? []) {
      const label = formatFieldValue(item.value);
      const identities = identitiesByLabel.get(label) ?? new Set<string>();
      identities.add(item.typedIdentity ?? scalarIdentity(item.value));
      identitiesByLabel.set(label, identities);
    }
    return new Set(
      [...identitiesByLabel.entries()]
        .filter(([, identities]) => identities.size > 1)
        .map(([label]) => label),
    );
  }, [activeFieldData?.values]);
  const maxTimelineCount = Math.max(1, ...timelinePoints.map((point) => point.count));
  const hasApproximateTimelineCoordinates = timelinePoints.some((point) => point.coordinateApproximate === true);
  const timelineAxisLabels = useMemo(() => {
    if (timelinePoints.length === 0) return [];
    const indexes = [0, Math.round((timelinePoints.length - 1) * 0.25), Math.round((timelinePoints.length - 1) * 0.5), Math.round((timelinePoints.length - 1) * 0.75), timelinePoints.length - 1];
    return [...new Set(indexes)].map((index) => timelinePoints[index]?.label).filter((label): label is string => label !== undefined);
  }, [timelinePoints]);

  useEffect(() => {
    setTimelineKeyboardIndex((current) => Math.min(current, Math.max(0, timelinePoints.length - 1)));
  }, [timelinePoints.length]);

  useEffect(() => {
    const phoneViewport = window.matchMedia("(max-width: 760px)");
    const updateMode = () => setMobileFieldsMode(phoneViewport.matches);
    updateMode();
    phoneViewport.addEventListener("change", updateMode);
    return () => phoneViewport.removeEventListener("change", updateMode);
  }, []);

  useEffect(() => {
    if (fieldsCollapsed || !window.matchMedia("(max-width: 760px)").matches) return;
    const rail = fieldsRailRef.current;
    if (rail === null) return;
    return installModalSurface({
      container: rail,
      excludedSiblingClassNames: ["fields-mobile-dismiss"],
      onEscape: () => setFieldsCollapsed(true),
      returnFocus: fieldsReturnFocusRef.current,
    });
  }, [fieldsCollapsed, setFieldsCollapsed]);

  function handleTimelineKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (timelinePoints.length === 0) return;
    let nextIndex = timelineKeyboardIndex;
    if (event.key === "ArrowLeft") nextIndex = Math.max(0, timelineKeyboardIndex - 1);
    else if (event.key === "ArrowRight") nextIndex = Math.min(timelinePoints.length - 1, timelineKeyboardIndex + 1);
    else if (event.key === "Home") nextIndex = 0;
    else if (event.key === "End") nextIndex = timelinePoints.length - 1;
    else if (event.key === "Escape") {
      event.preventDefault();
      setTimelineStart(null);
      setTimelineEnd(null);
      return;
    } else if (event.key === "Enter" && timelineSelection !== null) {
      event.preventDefault();
      zoomTimeline();
      return;
    } else return;
    event.preventDefault();
    setTimelineKeyboardIndex(nextIndex);
    setTimelineStart(nextIndex);
    setTimelineEnd(nextIndex);
  }

  function openFieldInspector(fieldName: string) {
    if (mobileFieldsMode && document.activeElement instanceof HTMLElement) fieldsReturnFocusRef.current = document.activeElement;
    setActiveField(fieldName);
    setFieldsCollapsed(false);
  }

  function closeFieldInspector() {
    const fieldName = activeField;
    setActiveField(null);
    window.requestAnimationFrame(() => {
      const matchingRow = fieldName === null
        ? null
        : Array.from(fieldsRailRef.current?.querySelectorAll<HTMLButtonElement>("[data-field-name]") ?? [])
            .find((button) => button.dataset.fieldName === fieldName);
      (matchingRow ?? fieldsRailRef.current?.querySelector<HTMLInputElement>(".field-filter input"))?.focus();
    });
  }

  return (
      <section id="panel-events" role="tabpanel" aria-labelledby="tab-events" className="events-panel">
        <section className={`timeline-section${isPreview ? " timeline-section--preview" : ""}`} data-testid="timeline" aria-label={isPreview ? "Event timeline pending final results" : "Event timeline"}>
          {isPreview ? (
            <div className="preview-metadata-placeholder">
              <span className="preview-placeholder-icon" aria-hidden="true"><i /><i /><i /><i /></span>
              <span>
                <strong>Timeline available after completion</strong>
                <small>Live rows are provisional; authoritative time buckets and zoom controls appear with the completed result set.</small>
              </span>
              <span className="preview-context-badge"><i aria-hidden="true" /> Live preview</span>
            </div>
          ) : (
            <>
              <div className="timeline-toolbar">
            <div>
              <div className="header-menu-wrap result-menu-wrap">
                <button type="button" aria-haspopup="menu" aria-expanded={menu === "timeline-format"} onClick={() => setMenu(menu === "timeline-format" ? null : "timeline-format")}>Format Timeline <AppIcon name="chevron-down" size="xs" /></button>
                {menu === "timeline-format" ? (
                  <div className="floating-menu result-control-menu" role="menu" aria-label="Timeline format">
                    {(["Columns", "Compact"] as const).map((display) => (
                      <button role="menuitemradio" aria-checked={timelineDisplay === display} type="button" key={display} onClick={() => { setTimelineDisplay(display); setMenu(null); }}><span className="radio-mark">{timelineDisplay === display ? "●" : "○"}</span><span><strong>{display}</strong><small>{display === "Columns" ? "Full-height event volume" : "Condensed activity profile"}</small></span></button>
                    ))}
                  </div>
                ) : null}
              </div>
              <button type="button" disabled={!canZoomOut} onClick={zoomOutTimeline}><AppIcon name="minus" size="sm" /> Zoom Out</button>
              <button
                type="button"
                disabled={!timelineSelectionZoomable}
                title={timelineSelection !== null && !timelineSelectionZoomable ? "This bucket does not include an authoritative end boundary." : undefined}
                onClick={zoomTimeline}
              ><AppIcon name="plus" size="sm" /> Zoom to Selection</button>
              <button type="button" disabled={timelineSelection === null} onClick={() => { setTimelineStart(null); setTimelineEnd(null); }}><AppIcon name="close" size="sm" /> Deselect</button>
            </div>
            <span>
              {timelineSelection === null ? (backendEnabled ? "Automatic time span" : "20 minutes per column") : `${timelinePoints[timelineSelection[0]]?.label ?? ""} – ${timelinePoints[timelineSelection[1]]?.label ?? ""}`}
              {hasApproximateTimelineCoordinates ? " · ≈ scale; exact counts on hover" : ""}
            </span>
          </div>
          <div
            className={`timeline-chart timeline-${timelineDisplay.toLowerCase()}${draggingTimeline ? " dragging" : ""}`}
          >
            <input
              className="timeline-keyboard-control"
              type="range"
              min={0}
              max={Math.max(0, timelinePoints.length - 1)}
              value={Math.min(timelineKeyboardIndex, Math.max(0, timelinePoints.length - 1))}
              aria-label="Event timeline. Use Left and Right arrows to inspect a bucket, then Enter to zoom."
              aria-valuetext={timelinePoints[timelineKeyboardIndex] === undefined
                ? "No timeline data"
                : `${timelinePoints[timelineKeyboardIndex].label}, ${formatTimelineCount(timelinePoints[timelineKeyboardIndex])} events${timelinePoints[timelineKeyboardIndex].coordinateApproximate ? ", chart position approximate" : ""}`}
              onChange={(event) => {
                const nextIndex = Number(event.currentTarget.value);
                setTimelineKeyboardIndex(nextIndex);
                setTimelineStart(nextIndex);
                setTimelineEnd(nextIndex);
              }}
              onKeyDown={handleTimelineKeyDown}
              onPointerDown={startTimelineDrag}
              onPointerMove={moveTimelineDrag}
              onPointerUp={endTimelineDrag}
              onPointerCancel={endTimelineDrag}
            />
            <div className="timeline-grid-lines" aria-hidden="true"><span /><span /><span /></div>
            <div className="timeline-bars" aria-hidden="true">
              {timelinePoints.map((point, index) => {
                const selected = timelineSelection !== null && index >= timelineSelection[0] && index <= timelineSelection[1];
                return (
                  <span
                    className={selected ? "selected" : ""}
                    data-timeline-index={index}
                    title={`${point.label}\n${formatTimelineCount(point)} events${point.coordinateApproximate ? "\nChart position is approximate" : ""}`}
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
              <div className="timeline-axis" aria-hidden="true">{timelineAxisLabels.map((label) => <span key={label}>{label}</span>)}</div>
            </>
          )}
        </section>

        <div className={`events-layout${fieldsCollapsed ? " fields-collapsed" : ""}`}>
          <aside ref={fieldsRailRef} className="fields-rail" data-testid="fields-rail" role={mobileFieldsMode && !fieldsCollapsed ? "dialog" : undefined} aria-modal={mobileFieldsMode && !fieldsCollapsed ? "true" : undefined} aria-label="Search fields">
            <div className="fields-topbar">
              <button type="button" aria-label={fieldsCollapsed ? "Show fields" : undefined} onClick={() => setFieldsCollapsed(!fieldsCollapsed)}><AppIcon name={fieldsCollapsed ? "chevron-right" : "chevron-left"} size="sm" />{fieldsCollapsed ? null : "Hide Fields"}</button>
              {fieldsCollapsed || isPreview ? null : <button type="button" onClick={() => setShowAllFields(true)}><AppIcon name="list" size="sm" /> All Fields</button>}
            </div>
            {fieldsCollapsed ? (
              <button className="vertical-fields-label" type="button" onClick={() => setFieldsCollapsed(false)}>Fields</button>
            ) : isPreview ? (
              <div className="preview-fields-placeholder">
                <span className="preview-fields-placeholder__icon" aria-hidden="true">a</span>
                <strong>Fields load with final results</strong>
                <p>Field coverage, distinct counts, and top values require the authoritative result set.</p>
              </div>
            ) : (
              <>
                <label className="field-filter">
                  <span aria-hidden="true"><AppIcon name="search" size="sm" /></span>
                  <input aria-label="Filter fields" placeholder="Filter fields" value={fieldFilter} onChange={(event) => setFieldFilter(event.target.value)} />
                </label>
                {fieldsLoading ? <output className="field-loading">Loading server fields…</output> : null}
                {!fieldsLoading && fields.length === 0
                  ? <p className="field-loading">No authoritative fields were returned.</p>
                  : null}
                <div className="field-group">
                  <h2>Selected Fields <span>{selectedFields.length}</span></h2>
                  <div className="field-list">
                    {selectedFields.filter((field) => field.name.toLowerCase().includes(fieldFilter.toLowerCase())).map((field) => (
                      <button type="button" key={field.name} data-field-name={field.name} className={activeField === field.name ? "active" : ""} onClick={() => activeField === field.name ? closeFieldInspector() : setActiveField(field.name)}>
                        <span className={`field-type type-${field.type}`}>{field.type === "number" ? "#" : field.type === "boolean" ? "✓" : "a"}</span>
                        <span>{field.displayName}</span>
                        <b title={field.distinctCount === null ? "Distinct count unavailable" : projectedCountTitle("Distinct values", field.distinctCount, field.distinctCountExact, field.distinctCountIsApproximate)}>
                          {field.distinctCount === null ? "—" : formatProjectedCount(field.distinctCount, field.distinctCountExact, field.distinctCountIsApproximate, true)}
                        </b>
                      </button>
                    ))}
                  </div>
                </div>
                <div className="field-group interesting-group">
                  <h2>Interesting Fields <span>{interestingFields.length}</span></h2>
                  <div className="field-list">
                    {visibleInterestingFields.map((field) => (
                      <button type="button" key={field.name} data-field-name={field.name} className={activeField === field.name ? "active" : ""} onClick={() => activeField === field.name ? closeFieldInspector() : setActiveField(field.name)}>
                        <span className={`field-type type-${field.type}`}>{field.type === "number" ? "#" : field.type === "boolean" ? "✓" : "a"}</span>
                        <span>{field.displayName}</span>
                        <b title={field.distinctCount === null ? "Distinct count unavailable" : projectedCountTitle("Distinct values", field.distinctCount, field.distinctCountExact, field.distinctCountIsApproximate)}>
                          {field.distinctCount === null ? "—" : formatProjectedCount(field.distinctCount, field.distinctCountExact, field.distinctCountIsApproximate, true)}
                        </b>
                      </button>
                    ))}
                  </div>
                  {!showAllFields && fieldFilter.trim().length === 0 && interestingFields.length > 8 ? <button className="more-fields" type="button" onClick={() => setShowAllFields(true)}>Show {interestingFields.length - 8} more loaded fields</button> : null}
                  {fieldsHasMore ? (
                    <button
                      className="more-fields"
                      type="button"
                      aria-busy={fieldsLoadingMore}
                      disabled={fieldsLoadingMore}
                      onClick={() => {
                        setShowAllFields(true);
                        onLoadMoreFields();
                      }}
                    >
                      {fieldsLoadingMore ? "Loading more fields…" : "Load more server fields"}
                    </button>
                  ) : null}
                </div>
              </>
            )}
            {isPreview || activeFieldData === null || fieldsCollapsed ? null : (
              <section className="field-inspector" data-testid="field-inspector" aria-label={`${activeFieldData.displayName} field summary`}>
                <header>
                  <div><span className={`field-type type-${activeFieldData.type}`}>{activeFieldData.type === "number" ? "#" : "a"}</span><strong>{activeFieldData.displayName}</strong></div>
                  <button className="button button--ghost button--icon" type="button" aria-label="Close field summary" onClick={closeFieldInspector}><AppIcon name="close" size="md" /></button>
                </header>
                <div className="field-summary-meta">
                  <span title={projectedCountTitle("Events", activeFieldData.eventCount, activeFieldData.eventCountExact)}>
                    <strong>{formatProjectedCount(activeFieldData.eventCount, activeFieldData.eventCountExact)}</strong> events
                  </span>
                  <span title={activeFieldData.distinctCount === null ? "Distinct count unavailable" : projectedCountTitle("Distinct values", activeFieldData.distinctCount, activeFieldData.distinctCountExact, activeFieldData.distinctCountIsApproximate)}>
                    <strong>{activeFieldData.distinctCount === null ? "Unknown" : formatProjectedCount(activeFieldData.distinctCount, activeFieldData.distinctCountExact, activeFieldData.distinctCountIsApproximate)}</strong> values
                  </span>
                </div>
                <label className="select-field-checkbox"><input type="checkbox" checked={activeFieldData.selected} onChange={() => toggleField(activeFieldData.name)} /> Selected field</label>
                <h3>Top values</h3>
                <div className="top-values">
                  {fieldSummaryLoading ? <output className="field-loading">Loading top values…</output> : null}
                  {fieldSummaryError === null ? null : <p className="field-loading" role="alert">{fieldSummaryError}</p>}
                  {activeFieldData.values.map((item) => {
                    const percent = activeFieldData.eventCount <= 0
                      ? 0
                      : item.count <= 0
                        ? 0
                        : Math.min(100, Math.max(2, (item.count / activeFieldData.eventCount) * 100));
                    const pivotable = item.pivotable !== false;
                    const formattedValue = formatFieldValue(item.value);
                    const countTitle = projectedCountTitle("Frequency", item.count, item.exactCount, item.countIsApproximate);
                    return (
                      <div className="top-value" key={item.typedIdentity ?? scalarIdentity(item.value)}>
                        <button type="button" disabled={!pivotable} title={`${pivotable ? "Include this value" : "This typed value cannot be represented losslessly in SPL"}\n${countTitle}`} onClick={() => applyPivot(activeFieldData.name, item.value, "include")}><span>{formattedValue}{ambiguousTopValueLabels.has(formattedValue) ? <small className="top-value-type">{item.typeLabel ?? scalarTypeLabel(item.value)}</small> : null}</span><b>{formatProjectedCount(item.count, item.exactCount, item.countIsApproximate)}</b></button>
                        <span
                          className="value-bar"
                          title={item.countIsApproximate
                            ? "Bar width uses the backend’s estimated frequency."
                            : item.exactCount !== undefined || activeFieldData.eventCountExact !== undefined
                              ? "Bar width uses an approximate browser coordinate; displayed counts retain their full uint64 values."
                              : undefined}
                          style={{ width: `${percent}%` }}
                        />
                        <div><button type="button" disabled={!pivotable} onClick={() => applyPivot(activeFieldData.name, item.value, "include")} aria-label={`Include ${formattedValue} (${scalarTypeLabel(item.value)})`}><AppIcon name="plus" size="sm" /></button><button type="button" disabled={!pivotable} onClick={() => applyPivot(activeFieldData.name, item.value, "exclude")} aria-label={`Exclude ${formattedValue} (${scalarTypeLabel(item.value)})`}><AppIcon name="minus" size="sm" /></button></div>
                      </div>
                    );
                  })}
                  {!fieldSummaryLoading && fieldSummaryError === null && activeFieldData.values.length === 0
                    ? <p className="field-loading">No top values were returned for this field.</p>
                    : null}
                </div>
                <footer>
                  <span>Showing top {activeFieldData.values.length} values</span>
                  <button type="button" disabled={firstPivotableFieldValue === undefined} onClick={() => {
                    if (firstPivotableFieldValue !== undefined) {
                      applyPivot(activeFieldData.name, firstPivotableFieldValue.value, "new", true);
                    }
                  }}>New search</button>
                </footer>
              </section>
            )}
          </aside>

          {!fieldsCollapsed ? <button className="fields-mobile-dismiss" aria-label="Close fields panel" type="button" onClick={() => setFieldsCollapsed(true)} /> : null}
          <section className={`event-results display-${eventDisplay.toLowerCase()}${wrapEvents ? " wrap-events" : " nowrap-events"}`} aria-label="Events">
            <div className="event-toolbar">
              <div>
                <button ref={mobileFieldsButtonRef} className="mobile-fields-button" type="button" onClick={() => { fieldsReturnFocusRef.current = mobileFieldsButtonRef.current; setFieldsCollapsed(false); }}><AppIcon name="menu" size="md" /> Fields</button>
                <div className="header-menu-wrap result-menu-wrap">
                  <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-display"} onClick={() => setMenu(menu === "event-display" ? null : "event-display")}>{eventDisplay} <AppIcon name="chevron-down" size="xs" /></button>
                  {menu === "event-display" ? (
                    <div className="floating-menu result-control-menu" role="menu" aria-label="Event display">
                      {(["List", "Raw"] as const).map((display) => (
                        <button role="menuitemradio" aria-checked={eventDisplay === display} type="button" key={display} onClick={() => { setEventDisplay(display); setMenu(null); }}><span className="radio-mark">{eventDisplay === display ? "●" : "○"}</span><span><strong>{display}</strong><small>{display === "List" ? "Fields, metadata, and raw event" : "Raw event text with minimal chrome"}</small></span></button>
                      ))}
                    </div>
                  ) : null}
                </div>
                <button type="button" aria-pressed={wrapEvents} title={wrapEvents ? "Turn event wrapping off" : "Wrap long event text"} onClick={() => setWrapEvents((current) => !current)}><AppIcon name="wrap" size="sm" /> {wrapEvents ? "Wrap on" : "Wrap off"}</button>
                <div className="header-menu-wrap result-menu-wrap">
                  <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-row-actions"} disabled={eventPageLoading} onClick={() => setMenu(menu === "event-row-actions" ? null : "event-row-actions")}><AppIcon name="more" size="sm" /> Rows <AppIcon name="chevron-down" size="xs" /></button>
                  {menu === "event-row-actions" ? (
                    <div className="floating-menu result-control-menu event-row-actions-menu" role="menu" aria-label="Event row actions">
                      <button role="menuitem" type="button" disabled={eventPageLoading || isPreview || pageEventIds.length === 0 || expandedPageEventCount === pageEventIds.length} onClick={() => { onExpandPage(); setMenu(null); }}><AppIcon name="chevron-down" size="sm" /><span><strong>Expand page</strong><small>Show fields for every displayed event</small></span></button>
                      <button role="menuitem" type="button" disabled={eventPageLoading || isPreview || expandedPageEventCount === 0} onClick={() => { onCollapsePage(); setMenu(null); }}><AppIcon name="chevron-right" size="sm" /><span><strong>Collapse page</strong><small>Hide fields for displayed events</small></span></button>
                      <button role="menuitem" type="button" disabled={eventPageLoading || isPreview || pageEventIds.length === 0} onClick={() => { onCopyPageRaw(); setMenu(null); }}><AppIcon name="copy" size="sm" /><span><strong>Copy page raw</strong><small>Copy displayed raw events with newline separators</small></span></button>
                    </div>
                  ) : null}
                </div>
                {isPreview ? null : <div className="header-menu-wrap result-menu-wrap">
                  <button type="button" aria-haspopup="menu" aria-expanded={menu === "event-page-size"} disabled={eventPageLoading} onClick={() => setMenu(menu === "event-page-size" ? null : "event-page-size")}>{eventPageSize} Per Page <AppIcon name="chevron-down" size="xs" /></button>
                  {menu === "event-page-size" ? (
                    <div className="floating-menu result-control-menu page-size-menu" role="menu" aria-label="Events per page">
                      {eventPageSizeOptions.map((size) => {
                        const unavailable = maximumEventPageSize !== null && size > maximumEventPageSize;
                        return (
                          <button
                            role="menuitemradio"
                            aria-checked={eventPageSize === size}
                            type="button"
                            disabled={eventPageLoading || unavailable}
                            title={unavailable ? `The server allows at most ${maximumEventPageSize} results per page.` : undefined}
                            key={size}
                            onClick={() => {
                              setEventPageSize(size);
                              setMenu(null);
                            }}
                          >
                            {eventPageSize === size ? <AppIcon name="check" size="sm" /> : null}
                            <span>
                              <strong>{size} events</strong>
                              {unavailable ? <small>Above server limit</small> : null}
                            </span>
                          </button>
                        );
                      })}
                    </div>
                  ) : null}
                </div>}
              </div>
              {isPreview ? (
                <span className="event-preview-count" aria-label={`${resultEvents.length} provisional ${resultEvents.length === 1 ? "row" : "rows"} shown`}>
                  <i aria-hidden="true" />
                  {NUMBER_FORMAT.format(resultEvents.length)} {resultEvents.length === 1 ? "row" : "rows"} shown
                  {previewTruncated ? <b>Preview limit reached</b> : null}
                </span>
              ) : <nav aria-label="Event pages" aria-busy={eventPageLoading}>
                <span className="event-page-range">{pageRangeLabel} · {totalResultLabel}</span>
                <button type="button" disabled={eventPageLoading || eventPage === 1} onClick={() => setEventPage(1)}>First</button>
                <button type="button" disabled={eventPageLoading || eventPage === 1} onClick={() => setEventPage((current) => Math.max(1, current - 1))}><AppIcon name="chevron-left" size="xs" /> Prev</button>
                <form className="event-page-jump" aria-label="Jump to event page" noValidate onSubmit={submitPageJump}>
                  <label htmlFor="event-page-number">Page</label>
                  <input
                    id="event-page-number"
                    aria-label="Event page number"
                    type="number"
                    min={1}
                    max={maximumReachablePage}
                    step={1}
                    value={pageJumpValue}
                    disabled={eventPageLoading}
                    onChange={(event) => setPageJumpDraft({ page: eventPage, value: event.target.value })}
                  />
                  <span>of {NUMBER_FORMAT.format(maximumReachablePage)}</span>
                  <button type="submit" disabled={eventPageLoading}>Go</button>
                </form>
                <button
                  type="button"
                  disabled={eventPageLoading || (backendEnabled ? !backendHasNextPage : eventPage === maximumReachablePage)}
                  onClick={() => setEventPage((current) => backendEnabled ? current + 1 : Math.min(maximumReachablePage, current + 1))}
                >Next <AppIcon name="chevron-right" size="xs" /></button>
              </nav>}
            </div>
            <div className="event-head">
              <span />
              {isPreview
                ? <span title="Preview arrival order; final order is established when the search completes">Time · provisional order</span>
                : backendEnabled
                ? <span title="Server cursor order; add SPL sort for global ordering">Time · server order</span>
                : <button type="button" aria-label={`Sort by time, ${eventSortDirection === "desc" ? "ascending" : "descending"}`} onClick={() => { setEventSortDirection((current) => current === "desc" ? "asc" : "desc"); setEventPage(1); }}>Time <span aria-hidden="true">{eventSortDirection === "desc" ? "↓" : "↑"}</span></button>}
              <span>{isPreview ? "Event · live preview" : "Event"}</span>
              <span className="event-row-actions-heading">Actions</span>
            </div>
            <div className="event-list" data-testid="event-list">
              {pagedResultEvents.map((event) => {
                const expanded = !isPreview && expandedEvents.has(event.id);
                const level = String(event.fields.level ?? "INFO").toLowerCase();
                return (
                  <article className={`event-row level-${level}${expanded ? " expanded" : ""}${isPreview ? " event-row--preview" : ""}`} data-testid={`event-row-${event.id}`} key={event.id}>
                    <button className="event-expander" type="button" aria-disabled={isPreview} title={isPreview ? "Event details become available with final results." : undefined} aria-label={isPreview ? "Event details unavailable during live preview" : `${expanded ? "Collapse" : "Expand"} event`} aria-expanded={expanded} onClick={() => { if (!isPreview) toggleEvent(event.id); }}><AppIcon name={expanded ? "chevron-down" : "chevron-right"} size="sm" /></button>
                    <button className="event-time" type="button" aria-disabled={isPreview} title={isPreview ? "Nearby-event navigation becomes available with final results." : "Find nearby events"} aria-label={isPreview ? `${event.timeLabel}; nearby-event navigation unavailable during live preview` : undefined} onClick={() => { if (!isPreview) showToast("Choose a nearby interval from the time range picker."); }}><span>{event.timeLabel.split(", ")[0]}</span><strong>{event.timeLabel.split(", ").slice(1).join(", ")}</strong></button>
                    <div className="event-content">
                      <button className="event-raw" type="button" aria-disabled={isPreview} title={isPreview ? "This row may change until the search completes." : undefined} aria-label={isPreview ? "Provisional event row; details unavailable until completion" : `${expanded ? "Collapse" : "Expand"} event details`} onClick={() => { if (!isPreview) toggleEvent(event.id); }}>{highlightedRaw(event.raw, submittedQuery)}</button>
                      <div className="event-chips">
                        {["host", "source", "sourcetype"]
                          .filter((fieldName) => Object.hasOwn(event.fields, fieldName))
                          .map((fieldName) => (
                          <button type="button" aria-disabled={isPreview} title={isPreview ? "Authoritative field summaries load after completion." : undefined} key={fieldName} style={{ whiteSpace: eventFieldValueWhiteSpace(event.fields[fieldName] ?? null) }} onClick={() => { if (!isPreview) openFieldInspector(fieldName); }}><span>{fieldName}</span> = {formatFieldValue(event.fields[fieldName] ?? null)}</button>
                        ))}
                      </div>
                      {expanded ? (
                        <div className="event-detail">
                          <header><strong>Event fields</strong><span>{Object.keys(event.fields).length} fields · typed JSON</span><button className="event-detail-copy-raw" type="button" onClick={() => void copyText(event.raw, "Raw event copied.")}>Copy raw</button></header>
                          <div className="event-field-grid">
                            {Object.entries(event.fields).map(([fieldName, fieldValue]) => (
                              <div className="event-field" key={fieldName}>
                                <button className="event-field-name" type="button" onClick={() => openFieldInspector(fieldName)}>{fieldName}</button>
                                <span className={`value-type value-${fieldValue === null ? "null" : typeof fieldValue}`}>{fieldValue === null ? "null" : typeof fieldValue}</span>
                                <code style={{ whiteSpace: eventFieldValueWhiteSpace(fieldValue) }}>{formatFieldValue(fieldValue)}</code>
                                <div className="event-field-actions">
                                  <button type="button" disabled={event.pivotableFields?.[fieldName] === false} title={event.pivotableFields?.[fieldName] === false ? "This typed value cannot be represented losslessly in SPL" : "Include in current search"} aria-label={`Include ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "include")}><AppIcon name="plus" size="sm" /></button>
                                  <button type="button" disabled={event.pivotableFields?.[fieldName] === false} title={event.pivotableFields?.[fieldName] === false ? "This typed value cannot be represented losslessly in SPL" : "Exclude from current search"} aria-label={`Exclude ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "exclude")}><AppIcon name="minus" size="sm" /></button>
                                  <button type="button" disabled={event.pivotableFields?.[fieldName] === false} title={event.pivotableFields?.[fieldName] === false ? "This typed value cannot be represented losslessly in SPL" : "Open as new search"} aria-label={`New search for ${fieldName}`} onClick={() => applyPivot(fieldName, fieldValue, "new", true)}><AppIcon name="search" size="sm" /></button>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>
                    <button
                      className="event-copy-raw"
                      type="button"
                      aria-label="Copy raw event"
                      title={isPreview ? "Raw copy becomes available after authoritative results load." : "Copy raw event"}
                      disabled={isPreview}
                      onClick={() => void copyText(event.raw, "Raw event copied.")}
                    ><AppIcon name="copy" size="sm" /></button>
                  </article>
                );
              })}
              {resultEvents.length === 0 ? <div className="empty-state event-empty"><strong>No events found</strong><span>Widen the time range or remove a field filter.</span><button className="button button--secondary button--compact" type="button" onClick={() => setQuery(defaultQuery)}>Reset search</button></div> : null}
            </div>
          </section>
        </div>
      </section>
  );
}

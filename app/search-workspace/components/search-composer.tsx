import type {
  ChangeEvent,
  Dispatch,
  KeyboardEvent,
  RefObject,
  SetStateAction,
  UIEvent,
} from "react";
import { useEffect, useEffectEvent, useSyncExternalStore } from "react";

import { resolveAbsoluteTimeRange } from "@/lib/search/backend-data";
import type { EditorProblem } from "@/lib/search/spl-diagnostic-markers";
import type { SplDiagnostic } from "@/lib/search/spl-editor";

import { installModalSurface } from "../../_components/modal-surface";
import { TIME_PRESETS } from "../constants";
import type { ModalName, TimePickerSection, TimeRange } from "../model";
import { serverTimeRangeValidationError } from "../time-range";
import { AppIcon, StatusIcon } from "../../_components/app-icon";
import { Button } from "../../_components/button";
import type { KeyboardPlatform } from "../keyboard-shortcuts";
import { SearchEditor, type CompletionItem } from "./search-editor";

const PHONE_VIEWPORT = "(max-width: 760px)";

function subscribePhoneViewport(listener: () => void): () => void {
  const query = window.matchMedia(PHONE_VIEWPORT);
  query.addEventListener("change", listener);
  return () => query.removeEventListener("change", listener);
}

function phoneViewportSnapshot(): boolean {
  return window.matchMedia(PHONE_VIEWPORT).matches;
}

function localTimeZoneSnapshot(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "Local browser time";
}

function subscribeToStableBrowserValue(): () => void {
  return () => undefined;
}

interface SearchComposerProps {
  absoluteEnd: string;
  absoluteStart: string;
  absoluteTimeInvalid: boolean;
  backendTimeSyntax: boolean;
  completionIndex: number;
  completionOpen: boolean;
  draftTimeRange: TimeRange;
  editorFocused: boolean;
  editorLineCount: number;
  editorRef: RefObject<HTMLTextAreaElement | null>;
  filteredCompletions: CompletionItem[];
  gutterLinesRef: RefObject<HTMLDivElement | null>;
  highlightRef: RefObject<HTMLPreElement | null>;
  historyAnnouncement: string | null;
  historyRecallable: boolean;
  isRunning: boolean;
  launchPending: boolean;
  modal: ModalName | null;
  platform: KeyboardPlatform;
  /** The problems list under the editor; empty renders nothing. */
  problems: EditorProblem[];
  query: string;
  relativeAmount: number;
  relativeUnit: "m" | "h" | "d";
  runDisabledReason: string | null;
  timePickerRef: RefObject<HTMLDivElement | null>;
  timePickerSection: TimePickerSection;
  timeRange: TimeRange;
  timeRangeButtonRef: RefObject<HTMLButtonElement | null>;
  onAbsoluteRangeChange: (start: string, end: string) => void;
  onCancelSearch: () => void;
  onCloseTimePicker: () => void;
  onCompletionIndexChange: Dispatch<SetStateAction<number>>;
  onCompletionOpenChange: Dispatch<SetStateAction<boolean>>;
  onDiagnosticFix: (diagnostic: SplDiagnostic) => void;
  onDiagnosticFocus: (offset: number) => void;
  onDraftTimeRangeChange: Dispatch<SetStateAction<TimeRange>>;
  onEditorCaretChange: (position: number) => void;
  onEditorChange: (event: ChangeEvent<HTMLTextAreaElement>) => void;
  onEditorFocusedChange: (focused: boolean) => void;
  onEditorKeyDown: (event: KeyboardEvent<HTMLTextAreaElement>) => void;
  onEditorScroll: (event: UIEvent<HTMLTextAreaElement>) => void;
  onInsertCompletion: (completion: CompletionItem) => void;
  onModalChange: (modal: ModalName | null) => void;
  onRelativeRangeChange: (amount: number, unit: "m" | "h" | "d") => void;
  onRunSearch: () => void;
  onSeedAbsoluteRange: () => void;
  onTimePickerSectionChange: (section: TimePickerSection) => void;
  onTimeRangeChange: (range: TimeRange) => void;
}

export function SearchComposer({
  absoluteEnd,
  absoluteStart,
  absoluteTimeInvalid,
  backendTimeSyntax,
  completionIndex,
  completionOpen,
  draftTimeRange,
  editorFocused,
  editorLineCount,
  editorRef,
  filteredCompletions,
  gutterLinesRef,
  highlightRef,
  historyAnnouncement,
  historyRecallable,
  isRunning,
  launchPending,
  modal,
  platform,
  problems,
  query,
  relativeAmount,
  relativeUnit,
  runDisabledReason,
  timePickerRef,
  timePickerSection,
  timeRange,
  timeRangeButtonRef,
  onAbsoluteRangeChange,
  onCancelSearch,
  onCloseTimePicker,
  onCompletionIndexChange,
  onCompletionOpenChange,
  onDiagnosticFix,
  onDiagnosticFocus,
  onDraftTimeRangeChange,
  onEditorCaretChange,
  onEditorChange,
  onEditorFocusedChange,
  onEditorKeyDown,
  onEditorScroll,
  onInsertCompletion,
  onModalChange,
  onRelativeRangeChange,
  onRunSearch,
  onSeedAbsoluteRange,
  onTimePickerSectionChange,
  onTimeRangeChange,
}: SearchComposerProps) {
  const mobileTimePicker = useSyncExternalStore(subscribePhoneViewport, phoneViewportSnapshot, () => false);
  const localTimeZone = useSyncExternalStore(
    subscribeToStableBrowserValue,
    localTimeZoneSnapshot,
    () => "Local browser time",
  );
  const closeTimePicker = useEffectEvent(onCloseTimePicker);
  let draftTimeRangeInvalid = backendTimeSyntax
    ? serverTimeRangeValidationError(draftTimeRange) !== null
    : false;
  if (!backendTimeSyntax) {
    try {
      resolveAbsoluteTimeRange(draftTimeRange.earliest, draftTimeRange.latest);
    } catch {
      draftTimeRangeInvalid = true;
    }
  }
  const availablePresets = backendTimeSyntax
    ? TIME_PRESETS.filter((preset) =>
      serverTimeRangeValidationError(preset) === null
    )
    : TIME_PRESETS;

  useEffect(() => {
    if (modal !== "time" || !mobileTimePicker) return;
    const dialog = document.querySelector<HTMLElement>("[data-testid='time-picker-dialog']");
    if (dialog === null) return;
    const trigger = document.querySelector<HTMLButtonElement>("[data-testid='time-range-button']");
    return installModalSurface({
      container: dialog,
      excludedSiblingClassNames: ["drawer-backdrop"],
      onEscape: closeTimePicker,
      returnFocus: trigger,
    });
  }, [mobileTimePicker, modal]);

  return (
    <>
      <section className="search-composer" aria-label="SPL search" aria-busy={launchPending}>
        <SearchEditor
          completionIndex={completionIndex}
          completionOpen={completionOpen}
          editorFocused={editorFocused}
          editorLineCount={editorLineCount}
          editorRef={editorRef}
          filteredCompletions={filteredCompletions}
          gutterLinesRef={gutterLinesRef}
          highlightRef={highlightRef}
          historyAnnouncement={historyAnnouncement}
          historyRecallable={historyRecallable}
          launchPending={launchPending}
          modal={modal}
          platform={platform}
          problems={problems}
          query={query}
          onCompletionIndexChange={onCompletionIndexChange}
          onCompletionOpenChange={onCompletionOpenChange}
          onDiagnosticFocus={onDiagnosticFocus}
          onEditorCaretChange={onEditorCaretChange}
          onEditorChange={onEditorChange}
          onEditorFocusedChange={onEditorFocusedChange}
          onEditorKeyDown={onEditorKeyDown}
          onEditorScroll={onEditorScroll}
          onInsertCompletion={onInsertCompletion}
          onModalChange={onModalChange}
        />
        <div className="time-picker-wrap" ref={timePickerRef}>
          <button
            ref={timeRangeButtonRef}
            className="time-range-button"
            data-testid="time-range-button"
            type="button"
            aria-haspopup="dialog"
            aria-expanded={modal === "time"}
            aria-controls={modal === "time" ? "time-range-popover" : undefined}
            disabled={launchPending}
            onClick={() => {
              onCompletionOpenChange(false);
              if (modal === "time") {
                onCloseTimePicker();
                return;
              }
              onDraftTimeRangeChange(timeRange);
              onTimePickerSectionChange("presets");
              onModalChange("time");
            }}
          >
            <span aria-hidden="true"><AppIcon name="clock" size="lg" /></span>
            <span><small>Time range</small><strong>{timeRange.label}</strong></span>
            <span aria-hidden="true"><AppIcon name="chevron-down" size="xs" /></span>
          </button>
          {modal === "time" ? (
            <>
            <button className="drawer-backdrop" type="button" aria-label="Close time range" onClick={onCloseTimePicker} />
            <dialog open className="time-popover" id="time-range-popover" data-testid="time-picker-dialog" aria-modal={mobileTimePicker} aria-labelledby="time-popover-title">
              <header className="time-popover-header">
                <div><strong id="time-popover-title">Select time range</strong><small>{localTimeZone}</small></div>
                <button type="button" aria-label="Close time range" onClick={onCloseTimePicker}><AppIcon name="close" size="lg" /></button>
              </header>
              <div className="time-picker-layout">
                <aside className="time-picker-nav" aria-label="Time range categories">
                  {([[
                    "presets", "Presets"], ["relative", "Relative"], ["range", "Date & time range"], ["advanced", "Advanced"],
                  ] as const).map(([section, label]) => (
                    <button
                      className={timePickerSection === section ? "active" : ""}
                      type="button"
                      aria-pressed={timePickerSection === section}
                      key={section}
                      onClick={() => {
                        onTimePickerSectionChange(section);
                        if (section === "relative") onRelativeRangeChange(relativeAmount, relativeUnit);
                        if (section === "range") onSeedAbsoluteRange();
                      }}
                    >{label}</button>
                  ))}
                </aside>
                <div className="time-picker-content">
                  {timePickerSection === "presets" ? (
                    <><h3>Common time ranges</h3><div className="preset-grid">{availablePresets.map((preset) => (
                      <button className={draftTimeRange.label === preset.label ? "selected" : ""} type="button" key={preset.label} onClick={() => onDraftTimeRangeChange(preset)}><span>{preset.label}</span>{draftTimeRange.label === preset.label ? <AppIcon name="check" size="sm" /> : null}</button>
                    ))}</div></>
                  ) : null}
                  {timePickerSection === "relative" ? (
                    <div className="time-form-section">
                      <h3>Relative time</h3><p>Search backward from the current moment.</p>
                      <div className="relative-time-row">
                        <label><span>Last</span><input type="number" min="1" max="999" value={relativeAmount} onChange={(event) => onRelativeRangeChange(Number(event.target.value), relativeUnit)} /></label>
                        <label><span>Unit</span><select value={relativeUnit} onChange={(event) => onRelativeRangeChange(relativeAmount, event.target.value as "m" | "h" | "d")}><option value="m">Minutes</option><option value="h">Hours</option><option value="d">Days</option></select></label>
                        <label><span>Anchor</span><select value="now" disabled><option value="now">Now</option></select></label>
                      </div>
                    </div>
                  ) : null}
                  {timePickerSection === "range" ? (
                    <div className="time-form-section">
                      <h3>Date &amp; time range</h3><p>Use local time in {localTimeZone}.</p>
                      <div className="absolute-time-row">
                        <label><span>Start</span><input type="datetime-local" max={absoluteEnd} value={absoluteStart} onInput={(event) => onAbsoluteRangeChange(event.currentTarget.value, absoluteEnd)} /></label>
                        <label><span>End</span><input type="datetime-local" min={absoluteStart} value={absoluteEnd} onInput={(event) => onAbsoluteRangeChange(absoluteStart, event.currentTarget.value)} /></label>
                      </div>
                      {absoluteTimeInvalid ? <p className="time-validation" role="alert">End must be later than start.</p> : null}
                    </div>
                  ) : null}
                  {timePickerSection === "advanced" ? (
                    <div className="time-form-section">
                      <h3>Advanced time modifiers</h3><p>{backendTimeSyntax
                        ? "Enter RFC 3339 timestamps, now, -N[s|m|h|d], @d, or -Nd@d. Earliest also accepts 0 for all data."
                        : "Enter SPL relative modifiers or ISO timestamps."}</p>
                      <div className="absolute-time-row">
                        <label><span>Earliest</span><input value={draftTimeRange.earliest} onChange={(event) => onDraftTimeRangeChange({ ...draftTimeRange, label: "Custom time range", earliest: event.target.value })} /></label>
                        <label><span>Latest</span><input value={draftTimeRange.latest} onChange={(event) => onDraftTimeRangeChange({ ...draftTimeRange, label: "Custom time range", latest: event.target.value })} /></label>
                      </div>
                      {draftTimeRangeInvalid ? <p className="time-validation" role="alert">{backendTimeSyntax
                        ? serverTimeRangeValidationError(draftTimeRange)
                        : "Enter valid time modifiers and make earliest precede latest."}</p> : null}
                    </div>
                  ) : null}
                </div>
              </div>
              <div className="range-preview time-popover-preview"><span>Earliest <code>{draftTimeRange.earliest}</code></span><span>Latest <code>{draftTimeRange.latest}</code></span></div>
              <footer className="time-popover-footer">
                <button className="button button--secondary button--compact" type="button" onClick={onCloseTimePicker}>Cancel</button>
                <button
                  className="button button--primary button--compact"
                  type="button"
                  disabled={draftTimeRange.earliest.trim().length === 0 || draftTimeRange.latest.trim().length === 0 || draftTimeRangeInvalid || (timePickerSection === "range" && absoluteTimeInvalid)}
                  onClick={() => { onTimeRangeChange(draftTimeRange); onCloseTimePicker(); }}
                >Apply</button>
              </footer>
            </dialog>
            </>
          ) : null}
        </div>
        <Button
          className="run-button"
          variant={isRunning && !launchPending ? "danger" : "primary"}
          data-testid="run-search"
          aria-label={launchPending ? "Opening persisted search" : isRunning ? "Cancel search" : "Run search"}
          aria-keyshortcuts={isRunning && !launchPending ? "Escape" : undefined}
          disabled={launchPending || (!isRunning && runDisabledReason !== null)}
          title={!isRunning && runDisabledReason !== null ? runDisabledReason : undefined}
          onClick={(event) => {
            if (isRunning) {
              if (event.detail > 1) return;
              onCancelSearch();
            } else {
              onRunSearch();
            }
          }}
        >
          <span aria-hidden="true"><AppIcon name={launchPending ? "loading" : isRunning ? "stop" : "search"} size="lg" spin={launchPending} /></span>
          <strong>{launchPending ? "Opening" : isRunning ? "Cancel" : "Search"}</strong>
        </Button>
      </section>

      {problems.length === 0 ? null : (
        <section className="diagnostic-problems" aria-labelledby="editor-problems-title" data-testid="search-diagnostics">
          <h2 className="sr-only" id="editor-problems-title">Problems in the search</h2>
          <ul>
            {problems.map(({ diagnostic, fix, stale }) => (
              <li
                data-severity={diagnostic.severity}
                data-stale={stale ? "true" : undefined}
                key={`${diagnostic.code}:${diagnostic.range?.start ?? "none"}:${diagnostic.message}`}
              >
                <StatusIcon tone={diagnostic.severity} icon="warning" />
                <button
                  className="diagnostic-problem"
                  type="button"
                  disabled={stale || diagnostic.range === null}
                  onClick={() => {
                    if (diagnostic.range !== null) onDiagnosticFocus(diagnostic.range.start);
                  }}
                >
                  <strong>{diagnostic.message}</strong>
                  <small>
                    {[
                      diagnostic.range === null ? undefined : `Line ${diagnostic.range.line}, column ${diagnostic.range.column}`,
                      ...diagnostic.suggestions,
                      stale ? "From the previous run; the search has changed since." : undefined,
                    ].filter((part) => part !== undefined).join(" · ")}
                  </small>
                </button>
                {fix?.actionLabel === undefined ? null : <button className="diagnostic-fix" type="button" onClick={() => onDiagnosticFix(fix)}>{fix.actionLabel}</button>}
              </li>
            ))}
          </ul>
        </section>
      )}
    </>
  );
}

/* oxlint-disable jsx-a11y/prefer-tag-over-role */

import type {
  ChangeEvent,
  Dispatch,
  KeyboardEvent,
  RefObject,
  SetStateAction,
  UIEvent,
} from "react";

import type { SplDiagnostic } from "@/lib/search/spl-editor";

import { TIME_PRESETS } from "../constants";
import type { ModalName, TimePickerSection, TimeRange } from "../model";
import { syntaxTokens } from "../workspace-utils";

interface CompletionItem {
  label: string;
  insertion: string;
  detail: string;
}

interface SearchComposerProps {
  absoluteEnd: string;
  absoluteStart: string;
  absoluteTimeInvalid: boolean;
  completionIndex: number;
  completionOpen: boolean;
  diagnostic: SplDiagnostic | null;
  draftTimeRange: TimeRange;
  editorFocused: boolean;
  editorLineCount: number;
  editorRef: RefObject<HTMLTextAreaElement | null>;
  filteredCompletions: CompletionItem[];
  gutterLinesRef: RefObject<HTMLDivElement | null>;
  highlightRef: RefObject<HTMLPreElement | null>;
  isRunning: boolean;
  modal: ModalName | null;
  query: string;
  relativeAmount: number;
  relativeUnit: "m" | "h" | "d";
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
  onDraftTimeRangeChange: Dispatch<SetStateAction<TimeRange>>;
  onEditorCaretChange: (position: number) => void;
  onEditorChange: (event: ChangeEvent<HTMLTextAreaElement>) => void;
  onEditorFocusedChange: (focused: boolean) => void;
  onEditorKeyDown: (event: KeyboardEvent<HTMLTextAreaElement>) => void;
  onEditorScroll: (event: UIEvent<HTMLTextAreaElement>) => void;
  onInsertCompletion: (insertion: string) => void;
  onModalChange: (modal: ModalName | null) => void;
  onRelativeRangeChange: (amount: number, unit: "m" | "h" | "d") => void;
  onRunSearch: () => void;
  onTimePickerSectionChange: (section: TimePickerSection) => void;
  onTimeRangeChange: (range: TimeRange) => void;
}

export function SearchComposer({
  absoluteEnd,
  absoluteStart,
  absoluteTimeInvalid,
  completionIndex,
  completionOpen,
  diagnostic,
  draftTimeRange,
  editorFocused,
  editorLineCount,
  editorRef,
  filteredCompletions,
  gutterLinesRef,
  highlightRef,
  isRunning,
  modal,
  query,
  relativeAmount,
  relativeUnit,
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
  onTimePickerSectionChange,
  onTimeRangeChange,
}: SearchComposerProps) {
  return (
    <>
      <section className="search-composer" aria-label="SPL search">
        <div
          className={`spl-editor${editorFocused ? " focused" : ""}${diagnostic === null ? "" : " has-error"}`}
          role="combobox"
          aria-label="SPL command editor"
          aria-expanded={completionOpen}
          aria-haspopup="listbox"
          aria-controls={completionOpen ? "spl-completion-list" : undefined}
        >
          <div className="editor-gutter" aria-hidden="true">
            <div className="editor-gutter-lines" ref={gutterLinesRef}>
              {Array.from({ length: editorLineCount }, (_, index) => <span key={index + 1}>{index + 1}</span>)}
            </div>
          </div>
          <pre className="editor-highlight" ref={highlightRef} aria-hidden="true">{syntaxTokens(query)}{query.endsWith("\n") ? "\n " : null}</pre>
          <textarea
            ref={editorRef}
            data-testid="search-input"
            aria-label="Search with SPL"
            aria-describedby={`${diagnostic === null ? "editor-help" : "editor-diagnostic"} spl-completion-status`}
            aria-autocomplete="list"
            aria-controls={completionOpen ? "spl-completion-list" : undefined}
            aria-activedescendant={completionOpen && filteredCompletions.length > 0 ? `spl-completion-${completionIndex}` : undefined}
            value={query}
            rows={2}
            spellCheck={false}
            autoCapitalize="off"
            autoComplete="off"
            onChange={onEditorChange}
            onFocus={() => {
              onEditorFocusedChange(true);
              if (modal === "time") onModalChange(null);
            }}
            onBlur={() => window.setTimeout(() => {
              onEditorFocusedChange(false);
              onCompletionOpenChange(false);
            }, 120)}
            onKeyDown={onEditorKeyDown}
            onScroll={onEditorScroll}
            onSelect={(event) => onEditorCaretChange(event.currentTarget.selectionStart)}
          />
          <div className="editor-meta" id="editor-help"><span>SPL</span><span>Ctrl+Space for commands</span><span>⌘↵ to run</span></div>
          <span className="sr-only" id="spl-completion-status" aria-live="polite">
            {completionOpen
              ? filteredCompletions.length === 0
                ? "No matching SPL commands."
                : `${filteredCompletions.length} suggestions available. Use Up and Down arrows, then Enter or Tab to insert.`
              : "Suggestions closed."}
          </span>
          {completionOpen ? (
            <div className="completion-menu" id="spl-completion-list" data-testid="completion-menu" role="listbox" aria-label="SPL suggestions">
              <div className="completion-title"><span>Commands</span><small>Enter a pipeline stage</small></div>
              {filteredCompletions.map((completion, index) => (
                <button
                  id={`spl-completion-${index}`}
                  role="option"
                  aria-selected={index === completionIndex}
                  data-highlighted={index === completionIndex}
                  type="button"
                  key={completion.label}
                  onMouseEnter={() => onCompletionIndexChange(index)}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => onInsertCompletion(completion.insertion)}
                >
                  <code>{completion.label}</code><span>{completion.detail}</span><kbd>{index === completionIndex ? "↵" : ""}</kbd>
                </button>
              ))}
              {filteredCompletions.length === 0 ? <p className="completion-empty">No matching SPL commands</p> : null}
            </div>
          ) : null}
        </div>
        <div className="time-picker-wrap" ref={timePickerRef}>
          <button
            ref={timeRangeButtonRef}
            className="time-range-button"
            data-testid="time-range-button"
            type="button"
            aria-haspopup="dialog"
            aria-expanded={modal === "time"}
            aria-controls={modal === "time" ? "time-range-popover" : undefined}
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
            <span aria-hidden="true">◷</span>
            <span><small>Time range</small><strong>{timeRange.label}</strong></span>
            <span aria-hidden="true">▾</span>
          </button>
          {modal === "time" ? (
            <section className="time-popover" id="time-range-popover" data-testid="time-picker-dialog" role="dialog" aria-modal="false" aria-labelledby="time-popover-title">
              <header className="time-popover-header">
                <div><strong id="time-popover-title">Select time range</strong><small>America/Los_Angeles</small></div>
                <button type="button" aria-label="Close time range" onClick={onCloseTimePicker}>×</button>
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
                        if (section === "range") onAbsoluteRangeChange(absoluteStart, absoluteEnd);
                      }}
                    >{label}</button>
                  ))}
                </aside>
                <div className="time-picker-content">
                  {timePickerSection === "presets" ? (
                    <><h3>Common time ranges</h3><div className="preset-grid">{TIME_PRESETS.map((preset) => (
                      <button className={draftTimeRange.label === preset.label ? "selected" : ""} type="button" key={preset.label} onClick={() => onDraftTimeRangeChange(preset)}><span>{preset.label}</span>{draftTimeRange.label === preset.label ? <span aria-hidden="true">✓</span> : null}</button>
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
                      <h3>Date &amp; time range</h3><p>Use local time in America/Los_Angeles.</p>
                      <div className="absolute-time-row">
                        <label><span>Start</span><input type="datetime-local" max={absoluteEnd} value={absoluteStart} onInput={(event) => onAbsoluteRangeChange(event.currentTarget.value, absoluteEnd)} /></label>
                        <label><span>End</span><input type="datetime-local" min={absoluteStart} value={absoluteEnd} onInput={(event) => onAbsoluteRangeChange(absoluteStart, event.currentTarget.value)} /></label>
                      </div>
                      {absoluteTimeInvalid ? <p className="time-validation" role="alert">End must be later than start.</p> : null}
                    </div>
                  ) : null}
                  {timePickerSection === "advanced" ? (
                    <div className="time-form-section">
                      <h3>Advanced time modifiers</h3><p>Enter SPL relative modifiers or ISO timestamps.</p>
                      <div className="absolute-time-row">
                        <label><span>Earliest</span><input value={draftTimeRange.earliest} onChange={(event) => onDraftTimeRangeChange({ ...draftTimeRange, label: "Custom time range", earliest: event.target.value })} /></label>
                        <label><span>Latest</span><input value={draftTimeRange.latest} onChange={(event) => onDraftTimeRangeChange({ ...draftTimeRange, label: "Custom time range", latest: event.target.value })} /></label>
                      </div>
                    </div>
                  ) : null}
                </div>
              </div>
              <div className="range-preview time-popover-preview"><span>Earliest <code>{draftTimeRange.earliest}</code></span><span>Latest <code>{draftTimeRange.latest}</code></span></div>
              <footer className="time-popover-footer">
                <button className="button secondary compact" type="button" onClick={onCloseTimePicker}>Cancel</button>
                <button
                  className="button primary compact"
                  type="button"
                  disabled={draftTimeRange.earliest.trim().length === 0 || draftTimeRange.latest.trim().length === 0 || (timePickerSection === "range" && absoluteTimeInvalid)}
                  onClick={() => { onTimeRangeChange(draftTimeRange); onCloseTimePicker(); }}
                >Apply</button>
              </footer>
            </section>
          ) : null}
        </div>
        <button
          className={`run-button${isRunning ? " cancel" : ""}`}
          data-testid="run-search"
          type="button"
          aria-label={isRunning ? "Cancel search" : "Run search"}
          onClick={(event) => {
            if (isRunning) {
              if (event.detail > 1) return;
              onCancelSearch();
            } else {
              onRunSearch();
            }
          }}
        ><span aria-hidden="true">{isRunning ? "■" : "⌕"}</span><strong>{isRunning ? "Cancel" : "Search"}</strong></button>
      </section>

      {diagnostic === null ? null : (
        <div className="diagnostic-strip" id="editor-diagnostic" role="alert" data-testid="search-diagnostic">
          <span className="diagnostic-icon">!</span>
          <span><strong>{diagnostic.message}</strong><small>Line {diagnostic.line}, column {diagnostic.column} · {diagnostic.suggestion}</small></span>
          {diagnostic.actionLabel === undefined ? null : <button type="button" onClick={() => onDiagnosticFix(diagnostic)}>{diagnostic.actionLabel}</button>}
        </div>
      )}
    </>
  );
}

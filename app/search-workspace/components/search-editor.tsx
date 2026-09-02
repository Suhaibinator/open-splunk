import {
  type ChangeEvent,
  type Dispatch,
  type KeyboardEvent,
  type RefObject,
  type SetStateAction,
  type UIEvent,
  useEffect,
} from "react";

import {
  diagnosticMarkers,
  diagnosticSummary,
  type EditorProblem,
  markedLines,
} from "@/lib/search/spl-diagnostic-markers";

import { AppIcon } from "../../_components/app-icon";
import {
  COMPLETION_KIND_PRESENTATION,
  type CompletionKind,
  groupCompletions,
} from "../completion-groups";
import type { ModalName } from "../model";
import { syntaxTokens } from "../workspace-utils";

export interface CompletionItem {
  kind: CompletionKind;
  label: string;
  insertion: string;
  detail: string;
  /** Server relevance (0.5 any, 0.75 prefix, 1 exact); local items reuse the ladder. */
  relevance: number;
  replaceStart?: number;
  replaceEnd?: number;
}

export interface SearchEditorProps {
  completionIndex: number;
  completionOpen: boolean;
  editorFocused: boolean;
  editorLineCount: number;
  editorRef: RefObject<HTMLTextAreaElement | null>;
  /** Already ordered by kind and relevance; `completionIndex` addresses this list. */
  filteredCompletions: CompletionItem[];
  gutterLinesRef: RefObject<HTMLDivElement | null>;
  highlightRef: RefObject<HTMLPreElement | null>;
  /** Live-region text after an arrow-key history recall; null until one happens. */
  historyAnnouncement: string | null;
  /** Whether ↑/↓ on the boundary lines has anything to recall. */
  historyRecallable: boolean;
  launchPending: boolean;
  modal: ModalName | null;
  /** Every listed diagnostic; only current ones with a range are marked in the text. */
  problems: EditorProblem[];
  query: string;
  onCompletionIndexChange: Dispatch<SetStateAction<number>>;
  onCompletionOpenChange: Dispatch<SetStateAction<boolean>>;
  /** Move the caret to a marked span, as a gutter dot or problems row asks. */
  onDiagnosticFocus: (offset: number) => void;
  onEditorCaretChange: (position: number) => void;
  onEditorChange: (event: ChangeEvent<HTMLTextAreaElement>) => void;
  onEditorFocusedChange: (focused: boolean) => void;
  onEditorKeyDown: (event: KeyboardEvent<HTMLTextAreaElement>) => void;
  onEditorScroll: (event: UIEvent<HTMLTextAreaElement>) => void;
  onInsertCompletion: (completion: CompletionItem) => void;
  onModalChange: (modal: ModalName | null) => void;
}

/** What the live region says about the highlighted suggestion. */
function completionAnnouncement(completion: CompletionItem | undefined): string {
  if (completion === undefined) return "First suggestion selected.";
  const noun = COMPLETION_KIND_PRESENTATION[completion.kind].heading.toLowerCase().replace(/s$/u, "");
  return `${completion.label}, ${noun}, selected.`;
}

/**
 * The SPL editor: a transparent textarea over a syntax-highlighted mirror,
 * with the line gutter, the hint strip, the completion menu and the live
 * region that narrates them. The workspace owns every piece of state; this
 * component only lays the pieces out.
 */
export function SearchEditor({
  completionIndex,
  completionOpen,
  editorFocused,
  editorLineCount,
  editorRef,
  filteredCompletions,
  gutterLinesRef,
  highlightRef,
  historyAnnouncement,
  historyRecallable,
  launchPending,
  modal,
  problems,
  query,
  onCompletionIndexChange,
  onCompletionOpenChange,
  onDiagnosticFocus,
  onEditorCaretChange,
  onEditorChange,
  onEditorFocusedChange,
  onEditorKeyDown,
  onEditorScroll,
  onInsertCompletion,
  onModalChange,
}: SearchEditorProps) {
  const groups = groupCompletions(filteredCompletions);
  const markers = diagnosticMarkers(problems);
  const gutterMarks = markedLines(query, markers);
  const hasError = markers.some((marker) => marker.severity === "error");
  const activeCompletionId = completionOpen && filteredCompletions.length > 0
    ? `spl-completion-${completionIndex}`
    : undefined;
  // The menu scrolls once the groups outgrow it, and the keyboard walks
  // options the pointer never hovered, so the highlighted one follows the
  // arrow keys into view.
  useEffect(() => {
    if (activeCompletionId === undefined) return;
    document.getElementById(activeCompletionId)?.scrollIntoView({ block: "nearest" });
  }, [activeCompletionId]);
  return (
    <div
      className={`spl-editor${editorFocused ? " focused" : ""}${hasError ? " has-error" : ""}`}
    >
      <div className="editor-gutter" aria-hidden="true">
        <div className="editor-gutter-lines" ref={gutterLinesRef}>
          {Array.from({ length: editorLineCount }, (_, index) => {
            const line = index + 1;
            const mark = gutterMarks.get(line);
            // The gutter is pointer-only (keyboard users have the problems
            // list), so a marked line is a button the tab order skips.
            return mark === undefined
              ? <span key={line}>{line}</span>
              : (
                <button
                  className="editor-gutter-marker"
                  data-severity={mark.severity}
                  data-testid={`editor-gutter-marker-${line}`}
                  key={line}
                  tabIndex={-1}
                  type="button"
                  onClick={() => onDiagnosticFocus(mark.start)}
                >
                  {line}
                </button>
              );
          })}
        </div>
      </div>
      <pre className="editor-highlight" ref={highlightRef} aria-hidden="true">{syntaxTokens(query, markers)}{query.endsWith("\n") ? "\n " : null}</pre>
      <textarea
        ref={editorRef}
        data-testid="search-input"
        aria-label="Search with SPL"
        aria-describedby={`${problems.length === 0 ? "editor-help" : "editor-diagnostic"} spl-completion-status`}
        aria-invalid={hasError ? true : undefined}
        aria-autocomplete="list"
        aria-haspopup="listbox"
        aria-controls={completionOpen ? "spl-completion-list" : undefined}
        aria-activedescendant={activeCompletionId}
        value={query}
        disabled={launchPending}
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
      <div className="editor-meta" id="editor-help">
        <span>SPL</span>
        <span>Ctrl+Space for suggestions</span>
        {historyRecallable ? <span>↑↓ history</span> : null}
        <span>⌘↵ to run</span>
      </div>
      <span className="sr-only" id="editor-diagnostic" aria-live="polite">{diagnosticSummary(problems)}</span>
      <span className="sr-only" id="spl-completion-status" aria-live="polite">
        {completionOpen
          ? filteredCompletions.length === 0
            ? "No matching SPL suggestions."
            : `${filteredCompletions.length} suggestions available. ${completionAnnouncement(filteredCompletions[completionIndex])} Use Up and Down arrows, then Enter or Tab to insert.`
          : historyAnnouncement ?? "Suggestions closed."}
      </span>
      {completionOpen ? (
        <div
          className="completion-menu"
          id="spl-completion-list"
          role="listbox"
          aria-label="SPL suggestions"
          data-testid="completion-menu"
        >
          {groups.map((group) => {
            const presentation = COMPLETION_KIND_PRESENTATION[group.kind];
            const headingId = `spl-completion-group-${group.kind}`;
            return (
              <div className="completion-group" role="group" aria-labelledby={headingId} key={group.kind}>
                <div className="completion-title" id={headingId}>
                  <span><AppIcon name={presentation.icon} size="xs" />{presentation.heading}</span>
                  <small>{presentation.hint}</small>
                </div>
                {group.items.map(({ index, item }) => (
                  <button
                    className="completion-option"
                    id={`spl-completion-${index}`}
                    role="option"
                    aria-selected={index === completionIndex}
                    data-kind={item.kind}
                    type="button"
                    key={`${item.kind}:${item.label}`}
                    onMouseEnter={() => onCompletionIndexChange(index)}
                    onMouseDown={(event) => event.preventDefault()}
                    onClick={() => onInsertCompletion(item)}
                  >
                    <code>{item.label}</code><span>{item.detail}</span><kbd>{index === completionIndex ? "↵" : ""}</kbd>
                  </button>
                ))}
              </div>
            );
          })}
          {filteredCompletions.length === 0 ? <p className="completion-empty">No matching SPL suggestions</p> : null}
        </div>
      ) : null}
    </div>
  );
}

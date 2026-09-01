import type {
  ChangeEvent,
  Dispatch,
  KeyboardEvent,
  RefObject,
  SetStateAction,
  UIEvent,
} from "react";

import type { SplDiagnostic } from "@/lib/search/spl-editor";

import type { ModalName } from "../model";
import { syntaxTokens } from "../workspace-utils";

export interface CompletionItem {
  label: string;
  insertion: string;
  detail: string;
  replaceStart?: number;
  replaceEnd?: number;
}

export interface SearchEditorProps {
  completionIndex: number;
  completionOpen: boolean;
  diagnostic: SplDiagnostic | null;
  editorFocused: boolean;
  editorLineCount: number;
  editorRef: RefObject<HTMLTextAreaElement | null>;
  filteredCompletions: CompletionItem[];
  gutterLinesRef: RefObject<HTMLDivElement | null>;
  highlightRef: RefObject<HTMLPreElement | null>;
  launchPending: boolean;
  modal: ModalName | null;
  query: string;
  onCompletionIndexChange: Dispatch<SetStateAction<number>>;
  onCompletionOpenChange: Dispatch<SetStateAction<boolean>>;
  onEditorCaretChange: (position: number) => void;
  onEditorChange: (event: ChangeEvent<HTMLTextAreaElement>) => void;
  onEditorFocusedChange: (focused: boolean) => void;
  onEditorKeyDown: (event: KeyboardEvent<HTMLTextAreaElement>) => void;
  onEditorScroll: (event: UIEvent<HTMLTextAreaElement>) => void;
  onInsertCompletion: (completion: CompletionItem) => void;
  onModalChange: (modal: ModalName | null) => void;
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
  diagnostic,
  editorFocused,
  editorLineCount,
  editorRef,
  filteredCompletions,
  gutterLinesRef,
  highlightRef,
  launchPending,
  modal,
  query,
  onCompletionIndexChange,
  onCompletionOpenChange,
  onEditorCaretChange,
  onEditorChange,
  onEditorFocusedChange,
  onEditorKeyDown,
  onEditorScroll,
  onInsertCompletion,
  onModalChange,
}: SearchEditorProps) {
  return (
    <div
      className={`spl-editor${editorFocused ? " focused" : ""}${diagnostic === null ? "" : " has-error"}`}
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
        value={query}
        disabled={launchPending}
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
            : `${filteredCompletions.length} suggestions available. ${filteredCompletions[completionIndex]?.label ?? "First suggestion"} selected. Use Up and Down arrows, then Enter or Tab to insert.`
          : "Suggestions closed."}
      </span>
      {completionOpen ? (
        <div className="completion-menu" id="spl-completion-list" data-testid="completion-menu">
          <div className="completion-title"><span>Commands</span><small>Enter a pipeline stage</small></div>
          {filteredCompletions.map((completion, index) => (
            <button
              id={`spl-completion-${index}`}
              data-highlighted={index === completionIndex}
              type="button"
              key={completion.label}
              onMouseEnter={() => onCompletionIndexChange(index)}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => onInsertCompletion(completion)}
            >
              <code>{completion.label}</code><span>{completion.detail}</span><kbd>{index === completionIndex ? "↵" : ""}</kbd>
            </button>
          ))}
          {filteredCompletions.length === 0 ? <p className="completion-empty">No matching SPL commands</p> : null}
        </div>
      ) : null}
    </div>
  );
}

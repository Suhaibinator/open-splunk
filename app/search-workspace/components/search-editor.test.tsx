import assert from "node:assert/strict";
import test from "node:test";

import { createRef } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SearchEditor, type SearchEditorProps } from "./search-editor";

const noop = () => undefined;

function render(overrides: Partial<SearchEditorProps> = {}): string {
  return renderToStaticMarkup(
    <SearchEditor
      completionIndex={0}
      completionOpen={false}
      diagnostic={null}
      editorFocused={false}
      editorLineCount={2}
      editorRef={createRef<HTMLTextAreaElement>()}
      filteredCompletions={[]}
      gutterLinesRef={createRef<HTMLDivElement>()}
      highlightRef={createRef<HTMLPreElement>()}
      launchPending={false}
      modal={null}
      query="index=main"
      onCompletionIndexChange={noop}
      onCompletionOpenChange={noop}
      onEditorCaretChange={noop}
      onEditorChange={noop}
      onEditorFocusedChange={noop}
      onEditorKeyDown={noop}
      onEditorScroll={noop}
      onInsertCompletion={noop}
      onModalChange={noop}
      {...overrides}
    />,
  );
}

test("the editor describes itself through the help strip and the completion live region", () => {
  const markup = render();
  assert.match(markup, /<textarea[^>]*aria-describedby="editor-help spl-completion-status"/u);
  assert.match(markup, /id="editor-help"/u);
  assert.match(markup, /id="spl-completion-status"[^>]*aria-live="polite"/u);
  assert.match(markup, /Suggestions closed\./u);
});

test("a diagnostic redirects the description to the diagnostic strip", () => {
  const markup = render({
    diagnostic: { kind: "unclosed-quote", token: "\"", message: "Bad", line: 1, column: 1, suggestion: "Fix it" },
  });
  assert.match(markup, /class="spl-editor has-error"/u);
  assert.match(markup, /aria-describedby="editor-diagnostic spl-completion-status"/u);
});

test("the highlight mirror keeps a trailing newline visible with a sentinel line", () => {
  assert.match(render({ query: "index=main\n" }), /<\/span>\n <\/pre>/u);
  assert.doesNotMatch(render({ query: "index=main" }), /\n <\/pre>/u);
});

test("the gutter numbers every line the workspace counts", () => {
  const markup = render({ editorLineCount: 4 });
  assert.equal((markup.match(/<span>\d<\/span>/gu) ?? []).length, 4);
});

test("an open completion menu lists the highlighted command and announces the count", () => {
  const markup = render({
    completionIndex: 1,
    completionOpen: true,
    filteredCompletions: [
      { label: "stats", insertion: "stats count", detail: "Aggregate" },
      { label: "table", insertion: "table", detail: "Project" },
    ],
  });
  assert.match(markup, /data-testid="completion-menu"/u);
  assert.match(markup, /id="spl-completion-1"[^>]*data-highlighted="true"/u);
  assert.match(markup, /2 suggestions available\. table selected\./u);
});

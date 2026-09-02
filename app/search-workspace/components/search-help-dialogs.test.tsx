import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { KEYBOARD_SHORTCUTS } from "../keyboard-shortcuts";
import { SPL_REFERENCE_SECTIONS } from "../spl-reference-data";
import { KeyboardShortcutsDialog, SplReferenceDialog } from "./search-help-dialogs";

const noop = () => undefined;

test("the SPL reference dialog names itself and its filter controls the section list", () => {
  const markup = renderToStaticMarkup(<SplReferenceDialog onClose={noop} onInsert={noop} />);
  assert.match(markup, /<h2 id="[^"]+">SPL reference<\/h2>/u);
  assert.match(markup, /<input id="spl-reference-filter" type="search" aria-controls="spl-reference-sections" aria-label="Filter the SPL reference"/u);
  assert.match(markup, /<div class="workspace-dialog-reference-sections" id="spl-reference-sections">/u);
  assert.match(markup, /<nav class="workspace-dialog-reference-nav" aria-label="Reference sections">/u);
  for (const section of SPL_REFERENCE_SECTIONS) {
    assert.match(markup, new RegExp(`<button type="button" aria-controls="spl-reference-${section.id}"`, "u"));
    assert.match(markup, new RegExp(`<section class="workspace-dialog-reference-section" id="spl-reference-${section.id}"`, "u"));
  }
});

test("every supported entry offers Insert and unsupported ones are flagged instead", () => {
  const markup = renderToStaticMarkup(<SplReferenceDialog onClose={noop} onInsert={noop} />);
  const entries = SPL_REFERENCE_SECTIONS.flatMap((section) => section.entries);
  const insertButtons = markup.match(/aria-label="Insert [^"]+"/gu) ?? [];
  assert.equal(insertButtons.length, entries.filter((entry) => entry.supported).length);
  const flagged = markup.match(/data-supported="false"/gu) ?? [];
  assert.equal(flagged.length, entries.filter((entry) => !entry.supported).length);
  assert.match(markup, /<code class="workspace-dialog-reference-name">transaction<\/code><span class="badge badge--warning">Not supported<\/span>/u);
  assert.match(markup, /<pre class="workspace-dialog-reference-syntax"><code>stats \[partitions=&lt;n&gt;\]/u);
});

test("the shortcut sheet lists every shortcut as a definition with keycaps for the platform", () => {
  const mac = renderToStaticMarkup(<KeyboardShortcutsDialog platform="mac" onClose={noop} />);
  assert.match(mac, /<h2 id="[^"]+">Keyboard shortcuts<\/h2>/u);
  assert.equal((mac.match(/<dd>/gu) ?? []).length, KEYBOARD_SHORTCUTS.length);
  assert.match(mac, /<dt><span class="workspace-dialog-shortcut-chord"><kbd>⌘<\/kbd><kbd>Enter<\/kbd><\/span><\/dt><dd>Run the search<\/dd>/u);
  assert.match(mac, /<kbd>Enter<\/kbd><\/span><span class="workspace-dialog-shortcut-chord"><span class="workspace-dialog-shortcut-or">or<\/span><kbd>Tab<\/kbd>/u);
  assert.match(mac, /<kbd>\?<\/kbd>/u);

  const other = renderToStaticMarkup(<KeyboardShortcutsDialog platform="other" onClose={noop} />);
  assert.match(other, /<kbd>Ctrl<\/kbd><kbd>Enter<\/kbd><\/span><\/dt><dd>Run the search<\/dd>/u);
  assert.doesNotMatch(other, /⌘/u);
});

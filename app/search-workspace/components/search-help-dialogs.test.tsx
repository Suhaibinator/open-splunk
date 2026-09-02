import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { EXAMPLE_DRAFTS, exampleDraft } from "@/lib/search/example-drafts";

import { KEYBOARD_SHORTCUTS } from "../keyboard-shortcuts";
import { SPL_REFERENCE_SECTIONS } from "../spl-reference-data";
import { ExamplesDialog, KeyboardShortcutsDialog, SplReferenceDialog } from "./search-help-dialogs";

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

test("the examples gallery offers Use for every example and shows the preview SPL as written", () => {
  const markup = renderToStaticMarkup(<ExamplesDialog connected={false} onClose={noop} onUse={noop} />);
  assert.match(markup, /<h2 id="[^"]+">Example searches<\/h2>/u);
  assert.match(markup, /<ul class="workspace-dialog-examples" data-testid="example-searches">/u);
  const useButtons = markup.match(/aria-label="Use [^"]+"/gu) ?? [];
  assert.equal(useButtons.length, EXAMPLE_DRAFTS.length);
  for (const example of EXAMPLE_DRAFTS) {
    assert.match(markup, new RegExp(`<h3 id="[^"]+">${example.title}</h3>`, "u"));
  }
  assert.match(markup, /<pre class="workspace-dialog-example-spl"><code>index=gradethis level=ERROR \| stats count by service<\/code><\/pre>/u);
  assert.doesNotMatch(markup, /workspace-dialog-example-note/u);
});

test("connected to a server the gallery drops the preview index selectors and says so", () => {
  const markup = renderToStaticMarkup(<ExamplesDialog connected onClose={noop} onUse={noop} />);
  assert.doesNotMatch(markup, /<code>index=gradethis/u);
  assert.match(markup, /<pre class="workspace-dialog-example-spl"><code>level=ERROR \| stats count by service<\/code><\/pre>/u);
  assert.match(markup, /<pre class="workspace-dialog-example-spl"><code>\* \| timechart span=5m count<\/code><\/pre>/u);
  const notes = markup.match(/class="workspace-dialog-example-note"/gu) ?? [];
  assert.equal(notes.length, EXAMPLE_DRAFTS.filter((example) => example.needsIndex).length);
});

test("the gallery lists exactly the examples it is given, each labelled by its heading", () => {
  const example = exampleDraft("Slowest API routes");
  const markup = renderToStaticMarkup(
    <ExamplesDialog connected={false} examples={[example]} onClose={noop} onUse={noop} />,
  );
  const useButtons = markup.match(/aria-label="Use [^"]+"/gu) ?? [];
  assert.deepEqual(useButtons, ['aria-label="Use Slowest API routes"']);
  const labelled = /<li class="workspace-dialog-example" aria-labelledby="([^"]+)"><div class="workspace-dialog-example-head"><h3 id="([^"]+)">Slowest API routes<\/h3>/u.exec(markup);
  assert.ok(labelled !== null);
  assert.equal(labelled[1], labelled[2]);
  assert.match(markup, /<p class="workspace-dialog-example-prose">Ranks request paths by their 95th-percentile latency, slowest at the top\.<\/p>/u);
});

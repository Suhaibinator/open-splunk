import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { SPL_REFERENCE_SECTIONS } from "../spl-reference-data";
import { SplReferenceDialog } from "./search-help-dialogs";

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

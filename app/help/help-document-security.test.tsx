import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";

import type { HelpDocument } from "@/lib/help/help-content";

import { HelpDocumentContent } from "./help-document";

test("Help renders HTML-shaped documentation and example source as escaped text", () => {
  const document: HelpDocument = {
    headings: [],
    kind: "markdown",
    nodes: [
      { key: "html", type: "paragraph", children: [{ key: "raw", type: "text", text: '<script>alert("documentation")</script><img src=x onerror=alert(1)>' }] },
      { key: "code", type: "code-block", language: 'html" onmouseover="alert(1)', text: '</pre><iframe src="https://example.org/"></iframe>' },
      { key: "image", type: "paragraph", children: [{ key: "reference", type: "image-reference", alt: '<img src=x onerror=alert(1)>', external: true, href: "https://example.org/image.svg", title: '" onmouseover="alert(1)' }] },
      { key: "table", type: "table", align: [null], header: [{ key: "head-cell", children: [{ key: "head", type: "text", text: "Example" }] }], rows: [{ key: "row", cells: [{ key: "table-cell", children: [{ key: "cell", type: "code", text: '<svg onload="alert(1)">' }] }] }] },
    ],
    route: "/help/fixture/",
    searchText: "fixture",
    section: "Examples",
    slug: "fixture",
    sourcePath: "docs/fixture.md",
    title: "Fixture",
  };
  const markup = renderToStaticMarkup(<HelpDocumentContent document={document} />);
  assert.doesNotMatch(markup, /<(?:script|img|iframe)(?:\s|>)/u);
  assert.doesNotMatch(markup, /\s(?:onerror|onload|onmouseover)="/u);
  assert.match(markup, /&lt;script&gt;alert\(/u);
  assert.match(markup, /&lt;\/pre&gt;&lt;iframe/u);
  assert.match(markup, /&lt;svg onload=/u);
  assert.match(markup, /href="https:\/\/example\.org\/image\.svg"/u);
});

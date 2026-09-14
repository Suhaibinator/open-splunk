import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";

import type { HelpBlockNode, HelpInlineNode } from "@/lib/help/help-content";
import { backendAppHref } from "@/lib/search/app-navigation";

import { HELP_CONTENT, HELP_DOCUMENTS, helpDocumentForSlug } from "./help-data";
import { HelpDocumentContent } from "./help-document";

function inlineLinks(nodes: readonly HelpInlineNode[]): string[] {
  return nodes.flatMap((node) => {
    if (node.type === "link" || node.type === "image-reference") return node.external ? [] : [node.href];
    if (node.type === "strong" || node.type === "emphasis" || node.type === "delete") return inlineLinks(node.children);
    return [];
  });
}

function blockLinks(nodes: readonly HelpBlockNode[]): string[] {
  return nodes.flatMap((node) => {
    if (node.type === "heading" || node.type === "paragraph") return inlineLinks(node.children);
    if (node.type === "blockquote") return blockLinks(node.children);
    if (node.type === "list") return node.items.flatMap((item) => blockLinks(item.children));
    if (node.type === "table") return [
      ...node.header.flatMap((cell) => inlineLinks(cell.children)),
      ...node.rows.flatMap((row) => row.cells.flatMap((cell) => inlineLinks(cell.children))),
    ];
    return [];
  });
}

test("the generated Help bundle has unique routes and closed local links", () => {
  assert.match(HELP_CONTENT.contentRevision, /^[a-f0-9]{64}$/u);
  assert.equal(new Set(HELP_DOCUMENTS.map((document) => document.route)).size, HELP_DOCUMENTS.length);
  assert.equal(HELP_DOCUMENTS.filter((document) => document.route === "/help/").length, 1);
  const routes = new Set(HELP_DOCUMENTS.map((document) => document.route));
  for (const document of HELP_DOCUMENTS) {
    for (const href of blockLinks(document.nodes)) {
      const route = href.split(/[?#]/u, 1)[0];
      assert.ok(routes.has(route), `${document.sourcePath} references missing bundled route ${route}`);
    }
  }
});

test("rendered documents preserve exact source text and map local links", () => {
  const spl = helpDocumentForSlug("spl");
  const collectorExample = helpDocumentForSlug("examples/collector-container");
  assert.ok(spl);
  assert.ok(collectorExample);
  const splMarkup = renderToStaticMarkup(<HelpDocumentContent document={spl} localHref={(href) => backendAppHref(href, "selected")} />);
  const exampleMarkup = renderToStaticMarkup(<HelpDocumentContent document={collectorExample} />);
  assert.equal((splMarkup.match(/<h1(?:\s|>)/gu) ?? []).length, 1);
  assert.match(splMarkup, /\?appId=selected/u);
  assert.match(exampleMarkup, /inputs:/u);
  assert.match(exampleMarkup, /Bundled from/u);
});

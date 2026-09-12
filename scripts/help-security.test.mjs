import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { buildHelpDocumentation, compileHelpDocument, helpHeadingSlug } from "./build-help.mjs";

const entry = (sourcePath, slug, kind = "markdown") => ({
  kind, published: true, section: "Documentation", slug, sourcePath, title: slug || "Documentation",
});

async function fixture(t, content, extra = {}) {
  const root = await mkdtemp(path.join(tmpdir(), "open-splunk-help-security-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const sources = { "docs/README.md": content, ...extra };
  await Promise.all(Object.entries(sources).map(async ([filename, source]) => {
    await mkdir(path.dirname(path.join(root, filename)), { recursive: true });
    await writeFile(path.join(root, filename), source);
  }));
  return root;
}

test("Help rejects unsafe, encoded, missing and escaping link destinations at build time", async (t) => {
  await Promise.all([
    "javascript:alert%281%29", "JaVaScRiPt:alert%281%29", "data:text/html,hello",
    "vbscript:msgbox%281%29", "//remote.invalid/file", "jav&#x61;script:alert%281%29",
    "../../private.txt", "%2e%2e/%2e%2e/private.txt", "missing.md", "#missing-heading",
    "../examples/config.yaml#not-a-markdown-heading", "%GG.md",
  ].map((destination) => t.test(destination, async (subtest) => {
      const root = await fixture(subtest, `# Documentation\n\n[link](${destination})\n`, { "examples/config.yaml": "inputs: []\n" });
      await assert.rejects(buildHelpDocumentation({ root, registry: [entry("docs/README.md", ""), entry("examples/config.yaml", "examples/config", "source")] }));
    })));
});

test("Help rejects published source symlinks that escape its repository", async (t) => {
  const outside = await mkdtemp(path.join(tmpdir(), "open-splunk-help-outside-"));
  t.after(() => rm(outside, { recursive: true, force: true }));
  await writeFile(path.join(outside, "private.md"), "# Private\n");
  const root = await fixture(t, "# Documentation\n\n[linked](linked.md)\n");
  await symlink(path.join(outside, "private.md"), path.join(root, "docs/linked.md"));
  await assert.rejects(buildHelpDocumentation({ root, registry: [entry("docs/README.md", ""), entry("docs/linked.md", "linked")] }));
});

test("Help rejects colliding or escaping static routes instead of overwriting a document", async (t) => {
  const root = await fixture(t, "# Documentation\n", { "docs/topic.md": "# Topic\n" });
  await assert.rejects(buildHelpDocumentation({ root, registry: [entry("docs/README.md", "same"), entry("docs/topic.md", "same")] }));
  await assert.rejects(buildHelpDocumentation({ root, registry: [entry("docs/README.md", "../outside")] }));
});

test("Help rewrites local fragments and examples, and content revision follows exact source bytes", async (t) => {
  const root = await fixture(t, "# Documentation\n\n[Topic](topic.md#über-café)\n\n[Example](../examples/config.yaml)\n", {
    "docs/topic.md": "# Topic\n\n## Über Café\n\nNative ingestion.\n",
    "examples/config.yaml": "inputs:\n  - type: file\n",
  });
  const registry = [entry("docs/README.md", ""), entry("docs/topic.md", "topic"), entry("examples/config.yaml", "examples/config", "source")];
  const bundle = await buildHelpDocumentation({ root, registry });
  const document = bundle.documents.find((candidate) => candidate.slug === "");
  assert.ok(document);
  assert.match(JSON.stringify(document.nodes), /\/help\/topic\/#(?:über-café|%C3%BCber-caf%C3%A9)/u);
  assert.match(JSON.stringify(document.nodes), /\/help\/examples\/config\//u);
  assert.equal((await buildHelpDocumentation({ root, registry })).contentRevision, bundle.contentRevision);
  await writeFile(path.join(root, "docs/topic.md"), "# Topic\n\n## Über Café\n\nChanged text.\n");
  assert.notEqual((await buildHelpDocumentation({ root, registry })).contentRevision, bundle.contentRevision);
});

test("Help heading identifiers stay unique across duplicate and suffixed headings", () => {
  const used = new Set();
  const identifiers = ["Same", "Same", "Same-1", "Same", "Über Café", "Über Café"]
    .map((heading) => helpHeadingSlug(heading, used));
  assert.equal(new Set(identifiers).size, identifiers.length);
  assert.equal(identifiers[0], "same");
  assert.equal(identifiers[1], "same-1");
  assert.match(identifiers[4], /über-café/u);
  assert.notEqual(helpHeadingSlug("!!!", used), "");
  assert.notEqual(helpHeadingSlug("!!!", used), "");
});

test("nested headings share document-wide identifiers and are addressable by local links", async (t) => {
  const source = "# Same\n\n> ## Same\n\n- ### Same\n\n## Same-1\n\n[Quoted heading](#same-1)\n\n[List heading](#same-2)\n";
  const root = await fixture(t, source);
  const bundle = await buildHelpDocumentation({ root, registry: [entry("docs/README.md", "")] });
  const headings = bundle.documents[0].headings;
  assert.equal(headings.length, 4);
  assert.equal(new Set(headings.map((heading) => heading.id)).size, headings.length);
  assert.deepEqual(headings.map((heading) => heading.id), ["same", "same-1", "same-2", "same-1-1"]);
});

test("Help preserves explicitly allowed external links without treating them as bundled sources", async (t) => {
  const root = await fixture(t, "# Documentation\n\n[Web](https://example.org/reference?q=x#section)\n\n[HTTP](http://example.org/)\n\n[Mail](mailto:help@example.org)\n");
  const bundle = await buildHelpDocumentation({ root, registry: [entry("docs/README.md", "")] });
  const serialized = JSON.stringify(bundle.documents[0].nodes);
  assert.match(serialized, /https:\/\/example\.org\/reference\?q=x#section/u);
  assert.match(serialized, /http:\/\/example\.org\//u);
  assert.match(serialized, /mailto:help@example\.org/u);
});

test("Help compilation delegates every nested Markdown URL to the checked resolver", () => {
  const destinations = [];
  compileHelpDocument([
    "# Links",
    "",
    "> [Quote](quote.md)",
    "",
    "- **[List](list.md)**",
    "",
    "| Column |",
    "| --- |",
    "| [Cell](cell.md) |",
    "",
    "![Image](image.svg)",
    "",
    "[Reference][named]",
    "",
    "[named]: reference.md",
  ].join("\n"), (destination) => {
    destinations.push(destination);
    return { external: false, href: "/help/checked/" };
  });
  assert.deepEqual(destinations.toSorted(), ["cell.md", "image.svg", "list.md", "quote.md", "reference.md"]);
});

test("a rejected nested destination aborts Help compilation instead of omitting a broken link", () => {
  assert.throws(() => compileHelpDocument("> - [unsafe](javascript:alert%281%29)", () => {
    throw new Error("unsafe destination");
  }), /unsafe destination/u);
});

test("code examples remain literal and never resolve apparent Markdown links", () => {
  const resolved = [];
  const source = "```html\n<script>alert(1)</script>\n[link](javascript:alert%281%29)\n```\n\n`<img src=x onerror=alert(1)>`";
  const document = compileHelpDocument(source, (destination) => {
    resolved.push(destination);
    return { external: false, href: "/help/checked/" };
  });
  assert.deepEqual(resolved, []);
  assert.match(JSON.stringify(document), /<script>alert\(1\)<\/script>/u);
  assert.match(JSON.stringify(document), /<img src=x onerror=alert\(1\)>/u);
});

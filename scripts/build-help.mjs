#!/usr/bin/env node

import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import { marked } from "marked";

import { DOCUMENTATION_REGISTRY } from "../lib/help/documentation-registry.mjs";

const GENERATED_CONTENT_PATH = "app/help/help-content.generated.json";
const ALLOWED_EXTERNAL_SCHEMES = new Set(["http", "https", "mailto"]);

function decodeEntities(value) {
  const named = { amp: "&", apos: "'", colon: ":", gt: ">", lt: "<", newline: "\n", quot: '"', tab: "\t" };
  return value.replace(/&(?:#(\d+)|#x([0-9a-f]+)|([a-z]+));/giu, (entity, decimal, hexadecimal, name) => {
    if (decimal !== undefined) {
      const point = Number(decimal);
      return Number.isSafeInteger(point) && point <= 0x10_FFFF ? String.fromCodePoint(point) : entity;
    }
    if (hexadecimal !== undefined) {
      const point = Number.parseInt(hexadecimal, 16);
      return Number.isSafeInteger(point) && point <= 0x10_FFFF ? String.fromCodePoint(point) : entity;
    }
    return named[name.toLocaleLowerCase("en-US")] ?? entity;
  });
}

function plainInlineText(tokens) {
  return tokens.map((token) => {
    switch (token.type) {
      case "br": return "\n";
      case "codespan": return token.text;
      case "image": return token.text;
      case "html": return token.raw;
      case "text":
      case "escape": return token.tokens ? plainInlineText(token.tokens) : decodeEntities(token.text);
      case "strong":
      case "em":
      case "del":
      case "link": return plainInlineText(token.tokens ?? []);
      default: throw new Error(`Unsupported inline Markdown token: ${token.type}`);
    }
  }).join("");
}

function slugBase(text) {
  return text
    .trim()
    .toLocaleLowerCase("en-US")
    .replace(/[^\p{L}\p{M}\p{N}\s_-]/gu, "")
    .replace(/\s+/gu, "-") || "section";
}

export function helpHeadingSlug(text, used = new Set()) {
  const base = slugBase(text);
  let candidate = base;
  let suffix = 1;
  while (used.has(candidate)) {
    candidate = `${base}-${suffix}`;
    suffix += 1;
  }
  used.add(candidate);
  return candidate;
}

function headingMetadata(tokens) {
  const used = new Set();
  const headings = [];
  const visit = (nestedTokens) => {
    for (const token of nestedTokens) {
      if (token.type === "heading") {
        const text = plainInlineText(token.tokens ?? []);
        headings.push({ depth: token.depth, id: helpHeadingSlug(text, used), text });
      } else if (token.type === "blockquote") {
        visit(token.tokens ?? []);
      } else if (token.type === "list") {
        for (const item of token.items) visit(item.tokens ?? []);
      }
    }
  };
  visit(tokens);
  return headings;
}

export function helpMarkdownHeadings(source) {
  if (typeof source !== "string") throw new TypeError("Help Markdown source must be a string.");
  return headingMetadata(marked.lexer(source, { gfm: true }));
}

function containsControlCharacter(value) {
  return [...value].some((character) => {
    const point = character.codePointAt(0);
    return point !== undefined && (point <= 0x1F || point === 0x7F);
  });
}

function inlineNodes(tokens, resolver) {
  return tokens.flatMap((token) => {
    switch (token.type) {
      case "text":
      case "escape": return token.tokens
        ? inlineNodes(token.tokens, resolver)
        : [{ type: "text", text: decodeEntities(token.text) }];
      case "codespan": return [{ type: "code", text: token.text }];
      case "strong": return [{ type: "strong", children: inlineNodes(token.tokens ?? [], resolver) }];
      case "em": return [{ type: "emphasis", children: inlineNodes(token.tokens ?? [], resolver) }];
      case "del": return [{ type: "delete", children: inlineNodes(token.tokens ?? [], resolver) }];
      case "br": return [{ type: "break" }];
      case "link": {
        const target = resolver(token.href);
        return [{
          type: "link",
          children: inlineNodes(token.tokens ?? [], resolver),
          external: target.external,
          href: target.href,
          title: token.title || undefined,
        }];
      }
      case "image": {
        const target = resolver(token.href);
        return [{
          type: "image-reference",
          alt: decodeEntities(token.text),
          external: target.external,
          href: target.href,
          title: token.title || undefined,
        }];
      }
      case "html": return [{ type: "text", text: token.raw }];
      default: throw new Error(`Unsupported inline Markdown token: ${token.type}`);
    }
  });
}

function blockNodes(tokens, resolver, headingState) {
  return tokens.flatMap((token) => {
    switch (token.type) {
      case "space": return [];
      case "def": return [];
      case "heading": {
        const heading = headingState.headings[headingState.index];
        headingState.index += 1;
        if (heading === undefined) throw new Error("Markdown heading metadata is incomplete.");
        return [{ type: "heading", depth: token.depth, id: heading.id, children: inlineNodes(token.tokens ?? [], resolver) }];
      }
      case "paragraph": return [{ type: "paragraph", children: inlineNodes(token.tokens ?? [], resolver) }];
      case "text": return [{ type: "paragraph", children: inlineNodes(token.tokens ?? [token], resolver) }];
      case "code": return [{ type: "code-block", language: token.lang || undefined, text: token.text }];
      case "blockquote": return [{ type: "blockquote", children: blockNodes(token.tokens ?? [], resolver, headingState) }];
      case "list": return [{
        type: "list",
        ordered: token.ordered,
        start: typeof token.start === "number" ? token.start : undefined,
        items: token.items.map((item) => blockNodes(item.tokens ?? [], resolver, headingState)),
      }];
      case "table": return [{
        type: "table",
        align: token.align.map((alignment) => alignment || null),
        header: token.header.map((cell) => inlineNodes(cell.tokens ?? [], resolver)),
        rows: token.rows.map((row) => row.map((cell) => inlineNodes(cell.tokens ?? [], resolver))),
      }];
      case "hr": return [{ type: "thematic-break" }];
      case "html": return [{ type: "paragraph", children: [{ type: "text", text: token.raw }] }];
      default: throw new Error(`Unsupported block Markdown token: ${token.type}`);
    }
  });
}

function compileTokens(tokens, resolver) {
  const headings = headingMetadata(tokens);
  return {
    headings,
    nodes: assignNodeKeys(blockNodes(tokens, resolver, { headings, index: 0 }), "block"),
  };
}

function assignInlineKeys(nodes, prefix) {
  return nodes.map((node, index) => {
    const key = `${prefix}-${index}`;
    switch (node.type) {
      case "strong":
      case "emphasis":
      case "delete": return { ...node, children: assignInlineKeys(node.children, `${key}-inline`), key };
      case "link": return { ...node, children: assignInlineKeys(node.children, `${key}-inline`), key };
      default: return { ...node, key };
    }
  });
}

function assignNodeKeys(nodes, prefix) {
  return nodes.map((node, index) => {
    const key = `${prefix}-${index}`;
    switch (node.type) {
      case "heading":
      case "paragraph": return { ...node, children: assignInlineKeys(node.children, `${key}-inline`), key };
      case "blockquote": return { ...node, children: assignNodeKeys(node.children, `${key}-quote`), key };
      case "list": return {
        ...node,
        items: node.items.map((item, itemIndex) => ({
          children: assignNodeKeys(item, `${key}-item-${itemIndex}`),
          key: `${key}-item-${itemIndex}`,
        })),
        key,
      };
      case "table": return {
        ...node,
        header: node.header.map((cell, cellIndex) => ({
          children: assignInlineKeys(cell, `${key}-header-${cellIndex}`),
          key: `${key}-header-${cellIndex}`,
        })),
        key,
        rows: node.rows.map((row, rowIndex) => ({
          cells: row.map((cell, cellIndex) => ({
            children: assignInlineKeys(cell, `${key}-row-${rowIndex}-cell-${cellIndex}`),
            key: `${key}-row-${rowIndex}-cell-${cellIndex}`,
          })),
          key: `${key}-row-${rowIndex}`,
        })),
      };
      default: return { ...node, key };
    }
  });
}

export function compileHelpDocument(source, resolver, sourcePath = "fixture.md") {
  if (typeof source !== "string") throw new TypeError("Help Markdown source must be a string.");
  if (typeof resolver !== "function") throw new TypeError("Help link resolver must be a function.");
  try {
    return compileTokens(marked.lexer(source, { gfm: true }), resolver);
  } catch (error) {
    throw new Error(`Could not compile Help document ${sourcePath}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
}

function normalizedRelativePath(root, filename) {
  const relative = path.relative(root, filename);
  if (relative === "" || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new Error(`Help source escapes the repository: ${filename}`);
  }
  return relative.split(path.sep).join("/");
}

function containedRealPath(root, filename) {
  const realRoot = realpathSync(root);
  const realFile = realpathSync(filename);
  normalizedRelativePath(realRoot, realFile);
  return realFile;
}

function routeForSlug(slug) {
  return slug === "" ? "/help/" : `/help/${slug}/`;
}

function entryMap(entries) {
  return new Map(entries.map((entry) => [entry.sourcePath, entry]));
}

export function rewriteHelpDestination({ destination, root, sourcePath, documents, resources, anchors }) {
  if (typeof destination !== "string" || destination.length === 0) throw new Error("Help link has an empty destination.");
  const decodedDestination = decodeEntities(destination).trim();
  if (decodedDestination.startsWith("//")) throw new Error(`Protocol-relative Help link is not allowed: ${destination}`);
  if (containsControlCharacter(decodedDestination)) throw new Error(`Help link contains control characters: ${destination}`);
  const firstDelimiter = decodedDestination.search(/[/?#]/u);
  const colon = decodedDestination.indexOf(":");
  if (colon >= 0 && (firstDelimiter < 0 || colon < firstDelimiter)) {
    const scheme = decodedDestination.slice(0, colon).replace(/\s+/gu, "").toLocaleLowerCase("en-US");
    if (!ALLOWED_EXTERNAL_SCHEMES.has(scheme)) throw new Error(`Unsafe Help link scheme: ${destination}`);
    return { external: true, href: decodedDestination };
  }
  if (decodedDestination.includes("\\")) throw new Error(`Help link contains a backslash: ${destination}`);

  const hashIndex = decodedDestination.indexOf("#");
  const beforeHash = hashIndex < 0 ? decodedDestination : decodedDestination.slice(0, hashIndex);
  const encodedFragment = hashIndex < 0 ? "" : decodedDestination.slice(hashIndex + 1);
  const queryIndex = beforeHash.indexOf("?");
  const encodedPath = queryIndex < 0 ? beforeHash : beforeHash.slice(0, queryIndex);
  const query = queryIndex < 0 ? "" : beforeHash.slice(queryIndex);
  let decodedPath;
  let fragment;
  try {
    decodedPath = decodeURIComponent(encodedPath);
    fragment = decodeURIComponent(encodedFragment);
  } catch {
    throw new Error(`Help link has invalid percent encoding: ${destination}`);
  }
  if (decodedPath.includes("\0") || fragment.includes("\0")) throw new Error(`Help link contains a null byte: ${destination}`);
  const sourceFilename = path.resolve(root, sourcePath);
  const targetFilename = decodedPath === "" ? sourceFilename : path.resolve(path.dirname(sourceFilename), decodedPath);
  normalizedRelativePath(root, targetFilename);
  containedRealPath(root, targetFilename);
  const targetPath = path.relative(root, targetFilename).split(path.sep).join("/");
  const target = documents.get(targetPath) ?? resources.get(targetPath);
  if (target === undefined || target.published !== true) throw new Error(`Help link target is not bundled: ${destination}`);
  if (fragment !== "") {
    if (target.kind !== "markdown") throw new Error(`Help fragment targets a non-Markdown source: ${destination}`);
    if (!anchors.get(targetPath)?.has(fragment)) throw new Error(`Help link targets a missing heading: ${destination}`);
  }
  return {
    external: false,
    href: `${routeForSlug(target.slug)}${query}${fragment === "" ? "" : `#${encodeURIComponent(fragment)}`}`,
  };
}

function validateRegistry(registry) {
  const sources = new Set();
  const slugs = new Set();
  let roots = 0;
  for (const entry of registry) {
    if (!entry || typeof entry.sourcePath !== "string" || typeof entry.slug !== "string" || typeof entry.title !== "string") {
      throw new Error("Help registry contains an invalid entry.");
    }
    if (entry.kind !== "markdown" && entry.kind !== "source") throw new Error(`Help registry kind is unsupported: ${entry.kind}`);
    if (sources.has(entry.sourcePath)) throw new Error(`Help registry repeats source path: ${entry.sourcePath}`);
    sources.add(entry.sourcePath);
    if (!entry.published) continue;
    if (!/^(?:[a-z0-9]+(?:-[a-z0-9]+)*)(?:\/[a-z0-9]+(?:-[a-z0-9]+)*)*$|^$/u.test(entry.slug)) {
      throw new Error(`Help registry slug is invalid: ${entry.slug}`);
    }
    if (slugs.has(entry.slug)) throw new Error(`Help registry repeats slug: ${entry.slug}`);
    slugs.add(entry.slug);
    if (entry.slug === "") roots += 1;
  }
  if (roots !== 1) throw new Error("Help registry must publish exactly one root document.");
}

export async function buildHelpDocumentation({ root, registry = DOCUMENTATION_REGISTRY, revision } = {}) {
  if (typeof root !== "string" || root.length === 0) throw new TypeError("Help build requires a repository root.");
  validateRegistry(registry);
  const sources = new Map();
  const tokens = new Map();
  const anchors = new Map();
  const documents = entryMap(registry.filter((entry) => entry.kind === "markdown"));
  const resources = entryMap(registry.filter((entry) => entry.kind === "source"));
  const digest = createHash("sha256");
  digest.update("open-splunk-help-content\0");
  const loadedSources = await Promise.all(registry.map(async (entry) => {
    const filename = path.resolve(root, entry.sourcePath);
    containedRealPath(root, filename);
    const source = await readFile(filename, "utf8");
    return { entry, source };
  }));
  for (const { entry, source } of loadedSources) {
    sources.set(entry.sourcePath, source);
    if (entry.published) {
      digest.update(JSON.stringify(entry));
      digest.update("\0");
      digest.update(source);
      digest.update("\0");
    }
    if (entry.kind === "markdown") {
      const parsed = marked.lexer(source, { gfm: true });
      tokens.set(entry.sourcePath, parsed);
      anchors.set(entry.sourcePath, new Set(headingMetadata(parsed).map((heading) => heading.id)));
    }
  }

  const compiled = [];
  for (const entry of registry) {
    if (!entry.published) continue;
    const source = sources.get(entry.sourcePath);
    if (source === undefined) throw new Error(`Help source was not loaded: ${entry.sourcePath}`);
    const route = routeForSlug(entry.slug);
    if (entry.kind === "source") {
      compiled.push({
        headings: [],
        kind: entry.kind,
        nodes: [{ key: "block-0", type: "code-block", language: path.extname(entry.sourcePath).slice(1) || undefined, text: source }],
        route,
        searchText: `${entry.title}\n${entry.sourcePath}\n${source}`.toLocaleLowerCase("en-US"),
        section: entry.section,
        slug: entry.slug,
        sourcePath: entry.sourcePath,
        title: entry.title,
      });
      continue;
    }
    const parsed = tokens.get(entry.sourcePath);
    if (parsed === undefined) throw new Error(`Help Markdown tokens were not loaded: ${entry.sourcePath}`);
    const resolve = (destination) => rewriteHelpDestination({
      anchors,
      destination,
      documents,
      resources,
      root,
      sourcePath: entry.sourcePath,
    });
    const result = compileTokens(parsed, resolve);
    compiled.push({
      ...result,
      kind: entry.kind,
      route,
      searchText: `${entry.title}\n${entry.sourcePath}\n${source}`.toLocaleLowerCase("en-US"),
      section: entry.section,
      slug: entry.slug,
      sourcePath: entry.sourcePath,
      title: entry.title,
    });
  }
  return { contentRevision: revision ?? digest.digest("hex"), documents: compiled };
}

export async function writeHelpDocumentation({ root, registry = DOCUMENTATION_REGISTRY, revision } = {}) {
  const bundle = await buildHelpDocumentation({ registry, revision, root });
  const filename = path.join(root, GENERATED_CONTENT_PATH);
  await mkdir(path.dirname(filename), { recursive: true });
  await writeFile(filename, `${JSON.stringify(bundle)}\n`);
  return bundle;
}

if (import.meta.main) {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const bundle = await writeHelpDocumentation({ root });
  process.stdout.write(`Help content generated (${bundle.documents.length} documents, ${bundle.contentRevision})\n`);
}

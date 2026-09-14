"use client";

import { useMemo, useState, useSyncExternalStore } from "react";

import { OPEN_SPLUNK_BUILD_LABEL } from "@/lib/build-identity";
import type { SearchDataMode } from "@/lib/search/backend-data";
import {
  backendAppHref,
  currentBackendAppId,
  subscribeToBackendAppId,
} from "@/lib/search/app-navigation";

import { ProductShell } from "../_components/product-shell";
import { HELP_CONTENT, HELP_DOCUMENTS, helpDocumentForSlug } from "./help-data";
import { HelpDocumentContent } from "./help-document";

function subscribeToHydrationReadiness(): () => void {
  return () => undefined;
}

function browserHydrationReady(): boolean {
  return true;
}

function serverHydrationReady(): boolean {
  return false;
}

function searchWords(query: string): string[] {
  return query
    .trim()
    .toLocaleLowerCase("en-US")
    .split(/\s+/u)
    .filter(Boolean);
}

export function matchingHelpDocuments(query: string) {
  const words = searchWords(query);
  if (words.length === 0) return [];
  return HELP_DOCUMENTS.filter((document) => words.every((word) => document.searchText.includes(word)));
}

export function HelpBrowser({ apiBaseUrl, dataMode, initialSlug }: { apiBaseUrl: string; dataMode: SearchDataMode; initialSlug: string }) {
  const [query, setQuery] = useState("");
  const searchReady = useSyncExternalStore(
    subscribeToHydrationReadiness,
    browserHydrationReady,
    serverHydrationReady,
  );
  const backendAppId = useSyncExternalStore(subscribeToBackendAppId, currentBackendAppId, () => undefined);
  const document = helpDocumentForSlug(initialSlug);
  const matches = useMemo(() => matchingHelpDocuments(query), [query]);
  if (document === undefined) throw new Error(`Bundled Help document is missing: ${initialSlug}`);
  const sections = [...new Set(HELP_DOCUMENTS.map((candidate) => candidate.section))];
  const searching = query.trim().length > 0;
  const helpHref = (route: string) => dataMode === "backend" && backendAppId !== undefined
    ? backendAppHref(route, backendAppId)
    : route;

  return <ProductShell activeSection="help" apiBaseUrl={apiBaseUrl} appName="Documentation" dataMode={dataMode} disclosure={false}>
    <div className="help-page">
      <header className="help-header">
        <div><span className="suite-eyebrow">Offline reference</span><p>Browse the documentation and linked examples bundled with this build.</p></div>
        <div className="form-stack help-search"><label><span>Search documentation</span><input aria-label="Search documentation" disabled={!searchReady} type="search" value={query} onChange={(event) => setQuery(event.target.value)} /></label></div>
      </header>
      {searching ? <section aria-label="Documentation search results" className="help-search-results">
        <h2>Search results</h2>
        <p aria-live="polite" role="status">{matches.length === 0 ? "No documentation results match this search." : `${matches.length} documentation ${matches.length === 1 ? "result" : "results"}.`}</p>
        {matches.length === 0 ? null : <ul>{matches.map((match) => <li key={match.slug || "root"}><a href={helpHref(match.route)}><strong>{match.title}</strong><span>{match.sourcePath}</span></a></li>)}</ul>}
      </section> : null}
      <div className="help-layout">
        <nav aria-label="Documentation" className="help-navigation">
          {sections.map((section) => <section key={section}><h2>{section}</h2><ul>{HELP_DOCUMENTS.filter((candidate) => candidate.section === section).map((candidate) => <li key={candidate.slug || "root"}><a aria-current={candidate.slug === document.slug ? "page" : undefined} href={helpHref(candidate.route)}>{candidate.title}</a></li>)}</ul></section>)}
        </nav>
        <div className="help-document-pane">
          <HelpDocumentContent document={document} localHref={helpHref} />
          <footer className="help-revision">Docs revision {OPEN_SPLUNK_BUILD_LABEL} · content {HELP_CONTENT.contentRevision.slice(0, 12)}</footer>
        </div>
      </div>
    </div>
  </ProductShell>;
}

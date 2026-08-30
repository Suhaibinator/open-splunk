import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { SearchSharingDialog } from "./search-sharing-dialog";

test("expired exact results disable job sharing and explain how to recover", () => {
  const markup = renderToStaticMarkup(
    <SearchSharingDialog
      canCopyJob
      canCopySavedSearch
      dialog="share"
      loadState={{ status: "idle" }}
      manualCopyValue={null}
      mutationState={{ status: "idle" }}
      onClose={() => {}}
      onCopyJobLink={() => {}}
      onCopyQueryLink={() => {}}
      onCopySavedSearchLink={() => {}}
      onMakePrivate={() => {}}
      onMakeShared={() => {}}
      settings={{
        expiresAt: new Date(0),
        id: "job-1",
        lastAccessedAt: null,
        lifetimeMs: 600_000,
        provenance: "Ad hoc search",
        retainedResultState: "expired",
        retentionClass: "manual",
        stateVersion: 1n,
        visibility: "private",
      }}
    />,
  );
  assert.match(markup, /Copy job link/);
  assert.match(markup, /aria-describedby="search-sharing-job-unavailable"/);
  assert.match(markup, /disabled=""/);
  assert.match(markup, /Run the search again/);
});

test("clipboard failure exposes the complete link in a selectable field", () => {
  const manualURL = "https://open-splunk.example.test/search/?q=index%3Dmain&run=0";
  const markup = renderToStaticMarkup(
    <SearchSharingDialog
      canCopyJob={false}
      canCopySavedSearch={false}
      dialog="share"
      loadState={{ status: "idle" }}
      manualCopyValue={manualURL}
      mutationState={{ status: "idle" }}
      onClose={() => {}}
      onCopyJobLink={() => {}}
      onCopyQueryLink={() => {}}
      onCopySavedSearchLink={() => {}}
      onMakePrivate={() => {}}
      onMakeShared={() => {}}
      settings={null}
    />,
  );
  assert.match(markup, /Copy this link manually/);
  assert.match(markup, /readOnly=""/);
  assert.match(markup, /open-splunk\.example\.test/);
  assert.match(markup, /id="search-sharing-copy-query"/);
});

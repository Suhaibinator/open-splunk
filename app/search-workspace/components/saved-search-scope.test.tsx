import assert from "node:assert/strict";
import test from "node:test";
import { createElement, type ComponentProps } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SharingScope } from "@/gen/ts/open_splunk/common";

import { WorkspaceDialogs } from "./workspace-dialogs";

function saveDialogMarkup(): string {
  const props = {
    activeSavedSearchId: null,
    activeTab: "events",
    appName: "Search & Reporting",
    modal: "save",
    saveAppAvailable: true,
    saveDescription: "",
    saveDialogReturnFocus: null,
    saveName: "Production errors",
    savePurpose: "search",
    saveSharingAvailable: true,
    saveSharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
    saveState: { status: "idle" },
    timeRange: { earliest: "-15m", label: "Last 15 minutes", latest: "now" },
    onModalChange() {},
    onSaveDescriptionChange() {},
    onSaveNameChange() {},
    onSaveSharingScopeChange() {},
    onSaveSearch() {},
  } as unknown as ComponentProps<typeof WorkspaceDialogs>;
  return renderToStaticMarkup(createElement(WorkspaceDialogs, props));
}

test("new saved searches expose Private, App, and Global sharing choices", () => {
  const markup = saveDialogMarkup();
  assert.match(markup, />Sharing</u);
  assert.match(markup, />Private</u);
  assert.match(markup, />App</u);
  assert.match(markup, />Global</u);
});

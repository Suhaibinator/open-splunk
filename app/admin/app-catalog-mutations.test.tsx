import assert from "node:assert/strict";
import test from "node:test";

// Install a host before react-dom/client decides whether a document exists.
import { browser } from "@/lib/testing/fake-browser";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { AppState, AppWorkspace } from "@/gen/ts/open_splunk/app";
import type { CreateAppRequest, DeleteAppRequest, GetAppRequest, UpdateAppRequest, SetAppStateRequest } from "@/gen/ts/open_splunk/app_api";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { appCatalogStore, type OpenSplunkApiClient } from "@/lib/api";
import * as clientModule from "@/lib/api/open-splunk-client";
import { adaptSystemBootstrap } from "@/lib/api/system-bootstrap";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import * as modalModule from "../_components/modal";
import * as selectModule from "../_components/select";
import { ProductShell } from "../_components/product-shell";
import { AppsAdminPanel } from "./admin-resource-panels";

const baseUrl = "https://catalog-mutations.test";
const serverTime = new Date("2026-09-12T00:00:00Z");

function app(id: string, name: string, state = AppState.APP_STATE_ACTIVE): AppWorkspace {
  return AppWorkspace.fromPartial({
    appId: id,
    definition: { slug: id, displayName: name },
    version: 1n,
    state,
  });
}

function envelope(apps: AppWorkspace[]) {
  return GetSystemBootstrapResponse.fromPartial({
    apps: apps.filter((entry) => entry.state === AppState.APP_STATE_ACTIVE).map((entry) => ({
      appId: entry.appId,
      displayName: entry.definition?.displayName,
      slug: entry.definition?.slug,
      state: entry.state,
    })),
    features: [ServerFeature.SERVER_FEATURE_APP_ADMIN],
    selectedAppId: apps.find((entry) => entry.state === AppState.APP_STATE_ACTIVE)?.appId,
    serverTime,
  });
}

async function settle() {
  await act(async () => new Promise((resolve) => setImmediate(resolve)));
}

async function click(container: FakeElement, label: string) {
  const button = container.querySelectorAll("button").find((candidate) => candidate.getAttribute("aria-label") === label || candidate.textContent.trim() === label);
  assert.ok(button, `missing button: ${label}`);
  await act(async () => { button.dispatchEvent(fakeEvent("click")); });
  await settle();
}

async function type(container: FakeElement, id: string, value: string) {
  const input = container.querySelector(`#${id}`);
  assert.ok(input, `missing input: ${id}`);
  await act(async () => { input.value = value; input.dispatchEvent(fakeEvent("input")); });
}

async function submit(container: FakeElement, id: string) {
  const form = container.querySelector(`#${id}`);
  assert.ok(form, `missing form: ${id}`);
  await act(async () => { form.dispatchEvent(fakeEvent("submit")); });
  await settle();
}

test("admin create, rename, archive, activate and delete refresh the mounted shell from bootstrap", async () => {
  const realFactory = clientModule.createOpenSplunkApiClient;
  const realModal = modalModule.Modal;
  const realSelect = selectModule.Select;
  const mutableClientModule = clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory };
  const mutableModalModule = modalModule as unknown as { Modal: typeof realModal };
  const mutableSelectModule = selectModule as unknown as { Select: typeof realSelect };
  const mutations: string[] = [];
  let bootstrapCalls = 0;
  let apps = [app("app-a", "Original app")];
  const lookup = () => apps.find((entry) => entry.appId === "created-app")!;
  const client = {
    system: { bootstrap() { bootstrapCalls += 1; return Promise.resolve(envelope(apps)); } },
    apps: {
      // The admin list deliberately contains an archived record absent from bootstrap.
      list: () => Promise.resolve({ apps: [...apps, app("admin-only", "Archived admin-only app", AppState.APP_STATE_ARCHIVED)] }),
      get(request: GetAppRequest) {
        assert.deepEqual(request.selector, { selector: { $case: "appId", value: "created-app" } });
        return Promise.resolve({ app: lookup() });
      },
      create(request: CreateAppRequest) {
        mutations.push("create");
        const created = AppWorkspace.fromPartial({ appId: request.definition?.slug, definition: request.definition, version: 1n, state: AppState.APP_STATE_ACTIVE });
        apps = [...apps, created];
        return Promise.resolve({ app: created });
      },
      update(request: UpdateAppRequest) {
        mutations.push("update");
        assert.deepEqual(request.selector, { selector: { $case: "appId", value: "created-app" } });
        assert.equal(request.expectedVersion, lookup().version);
        assert.deepEqual(request.updateMask, ["display_name"]);
        const updated = { ...lookup(), definition: request.definition, version: 2n };
        apps = apps.map((entry) => entry.appId === updated.appId ? updated : entry);
        return Promise.resolve({ app: updated });
      },
      setState(request: SetAppStateRequest) {
        assert.deepEqual(request.selector, { selector: { $case: "appId", value: "created-app" } });
        assert.equal(request.expectedVersion, lookup().version);
        mutations.push(request.state === AppState.APP_STATE_ACTIVE ? "activate" : "archive");
        const updated = { ...lookup(), state: request.state, version: lookup().version + 1n };
        apps = apps.map((entry) => entry.appId === updated.appId ? updated : entry);
        return Promise.resolve({ app: updated });
      },
      delete(request: DeleteAppRequest) {
        assert.deepEqual(request.selector, { selector: { $case: "appId", value: "created-app" } });
        assert.equal(request.expectedVersion, lookup().version);
        assert.equal(request.confirmationSlug, "created-app");
        mutations.push("delete");
        apps = apps.filter((entry) => entry.appId !== "created-app");
        return Promise.resolve({});
      },
    },
  } as unknown as OpenSplunkApiClient;
  mutableClientModule.createOpenSplunkApiClient = () => client;
  // Modal focus/scroll behavior has its own contracts; keep this test on CRUD handlers.
  mutableModalModule.Modal = ({ children, footer }: { children: ReactNode; footer?: ReactNode }) => <div>{children}{footer}</div>;
  mutableSelectModule.Select = () => <span />;
  appCatalogStore.clear();
  const shellContainer = browser.document.body.appendChild(browser.document.createElement("div"));
  const panelContainer = browser.document.body.appendChild(browser.document.createElement("div"));
  const shellRoot = createRoot(shellContainer as unknown as Element);
  const panelRoot = createRoot(panelContainer as unknown as Element);
  try {
    await act(async () => {
      shellRoot.render(<ProductShell activeSection="admin" apiBaseUrl={baseUrl} appName="Administration" dataMode="backend"><p>Shell content</p></ProductShell>);
      panelRoot.render(<AppsAdminPanel apiBaseUrl={baseUrl} bootstrap={adaptSystemBootstrap(envelope(apps))} />);
    });
    await settle();
    await click(shellContainer, "App: Original app");
    assert.equal(bootstrapCalls, 1);
    assert.match(shellContainer.textContent ?? "", /Original app/u);
    assert.doesNotMatch(shellContainer.textContent ?? "", /Archived admin-only app/u);
    assert.match(panelContainer.textContent ?? "", /Archived admin-only app/u);

    await click(panelContainer, "Create app");
    await type(panelContainer, "app-slug", "created-app");
    await type(panelContainer, "app-display-name", "New app");
    await submit(panelContainer, "create-app-form");
    assert.equal(bootstrapCalls, 2);
    assert.match(shellContainer.textContent ?? "", /New app/u);

    await click(panelContainer, "Edit app New app");
    await type(panelContainer, "app-display-name", "Renamed app");
    await submit(panelContainer, "edit-app-form");
    assert.equal(bootstrapCalls, 3);
    assert.match(shellContainer.textContent ?? "", /Renamed app/u);
    assert.doesNotMatch(shellContainer.textContent ?? "", /New app/u);

    await click(panelContainer, "Archive app Renamed app");
    assert.equal(bootstrapCalls, 4);
    assert.doesNotMatch(shellContainer.textContent ?? "", /Renamed app/u);
    await click(panelContainer, "Activate app Renamed app");
    assert.equal(bootstrapCalls, 5);
    assert.match(shellContainer.textContent ?? "", /Renamed app/u);
    await click(panelContainer, "Archive app Renamed app");
    await click(panelContainer, "Delete app Renamed app");
    await type(panelContainer, "delete-app-confirmation", "created-app");
    await submit(panelContainer, "delete-app-form");
    assert.equal(bootstrapCalls, 7);
    assert.doesNotMatch(shellContainer.textContent ?? "", /Renamed app/u);
    assert.deepEqual(mutations, ["create", "update", "archive", "activate", "archive", "delete"]);
    assert.doesNotMatch(shellContainer.textContent ?? "", /Archived admin-only app/u);
  } finally {
    await act(async () => { shellRoot.unmount(); panelRoot.unmount(); });
    shellContainer.parentNode?.removeChild(shellContainer);
    panelContainer.parentNode?.removeChild(panelContainer);
    appCatalogStore.clear();
    mutableClientModule.createOpenSplunkApiClient = realFactory;
    mutableModalModule.Modal = realModal;
    mutableSelectModule.Select = realSelect;
  }
});

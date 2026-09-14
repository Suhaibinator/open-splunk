import assert from "node:assert/strict";
import test from "node:test";

// Load the DOM before react-dom selects its host implementation.
import { browser } from "@/lib/testing/fake-browser";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { AppWorkspace } from "@/gen/ts/open_splunk/app";
import { Index } from "@/gen/ts/open_splunk/index";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import * as clientModule from "@/lib/api/open-splunk-client";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import { AppCreateDialog } from "../_components/app-create-dialog";
import * as modalModule from "../_components/modal";
import * as selectModule from "../_components/select";
import { BackendAdminConsole } from "./backend-admin-console";

async function settle() {
  await act(async () => new Promise((resolve) => setImmediate(resolve)));
}
async function type(container: FakeElement, id: string, value: string) {
  const input = container.querySelector(`#${id}`);
  assert.ok(input, `missing ${id}`);
  await act(async () => { input.value = value; input.dispatchEvent(fakeEvent("input")); });
}

for (const kind of ["app", "index"] as const) {
  test(`${kind} creation retries use normalized wire intent and accept current replay metadata`, async () => {
    const realFactory = clientModule.createOpenSplunkApiClient;
    const realModal = modalModule.Modal;
    const realSelect = selectModule.Select;
    const mutableFactory = clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory };
    const mutableModal = modalModule as unknown as { Modal: typeof realModal };
    const mutableSelect = selectModule as unknown as { Select: typeof realSelect };
    browser.storage.clear();
    Object.assign(window.location, { origin: "https://request-keys.test", href: "https://request-keys.test/admin/?section=indexes" });
    const requests: Array<{ clientRequestId?: string; definition?: { displayName?: string } }> = [];
    let accepted = 0;
    const create = async (request: { clientRequestId?: string; definition?: { displayName?: string } }) => {
      requests.push(request);
      if (requests.length <= 2) throw new Error("Connection lost after acceptance");
      return { app: AppWorkspace.fromPartial({ appId: "app-1", version: 7n }), index: Index.fromPartial({ indexId: "index-1", version: 7n }), replayed: true };
    };
    mutableFactory.createOpenSplunkApiClient = () => ({
      system: { bootstrap: async () => GetSystemBootstrapResponse.fromPartial({ serverTime: new Date(), features: [ServerFeature.SERVER_FEATURE_INDEX_ADMIN] }) },
      indexes: { list: async () => ({ indexes: [] }), create },
      ingestionTokens: { list: async () => ({ ingestionTokens: [] }) },
      apps: { create },
    } as unknown as OpenSplunkApiClient);
    // Focus/scroll/select interactions have separate primitive contracts.
    mutableModal.Modal = ({ children, footer }: { children: ReactNode; footer?: ReactNode }) => <div>{children}{footer}</div>;
    mutableSelect.Select = () => <span />;
    const container = browser.document.body.appendChild(browser.document.createElement("div"));
    const root = createRoot(container as unknown as Element);
    try {
      await act(async () => { root.render(kind === "app"
        ? <AppCreateDialog apiBaseUrl="https://request-keys.test" onClose={() => {}} onCreated={(app) => { assert.equal(app.version, 7n); accepted += 1; }} />
        : <BackendAdminConsole apiBaseUrl="https://request-keys.test" />); });
      await settle();
      if (kind === "index") {
        const open = container.querySelectorAll("button").find((button) => button.textContent.trim() === "Create index");
        assert.ok(open, container.textContent);
        await act(async () => { open.dispatchEvent(fakeEvent("click")); });
      }
      const nameID = kind === "app" ? "app-slug" : "new-index-name";
      const displayID = kind === "app" ? "app-display-name" : "new-index-display-name";
      await type(container, nameID, "request-keys");
      await type(container, displayID, "Request keys");
      const submit = async () => {
        const form = container.querySelector(`#create-${kind}-form`);
        assert.ok(form);
        await act(async () => { form.dispatchEvent(fakeEvent("submit")); });
        await settle();
      };
      await submit();
      await type(container, displayID, "  Request keys  ");
      await submit();
      assert.equal(requests.length, 2);
      assert.equal(requests[0]?.clientRequestId, requests[1]?.clientRequestId, "normalization-preserving edit rotated the logical key");
      assert.deepEqual(requests[0]?.definition, requests[1]?.definition);
      assert.match(requests[0]?.clientRequestId ?? "", /^[0-9a-f]{8}-[0-9a-f-]{27}$/u);
      await type(container, displayID, "Changed intent");
      await submit();
      assert.equal(requests.length, 3);
      assert.notEqual(requests[1]?.clientRequestId, requests[2]?.clientRequestId);
      if (kind === "app") assert.equal(accepted, 1);
      else assert.equal(container.querySelector("#create-index-form"), null, "successful current-metadata replay did not close create");
    } finally {
      await act(async () => { root.unmount(); });
      container.parentNode?.removeChild(container);
      mutableFactory.createOpenSplunkApiClient = realFactory;
      mutableModal.Modal = realModal;
      mutableSelect.Select = realSelect;
    }
  });
}

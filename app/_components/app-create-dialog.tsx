"use client";

import { type FormEvent, useMemo, useState } from "react";

import type { AppWorkspace } from "@/gen/ts/open_splunk/app";
import { createOpenSplunkApiClient } from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import { AppFields } from "./app-fields";
import { blankAppForm, definitionFromForm } from "../admin/admin-resource-data";
import { Modal } from "./modal";

interface AppCreateDialogProps {
  apiBaseUrl: string;
  onClose: () => void;
  onCreated: (app: AppWorkspace) => void;
}

const appCreateError = createErrorMessage("The app could not be created.");

export function AppCreateDialog({ apiBaseUrl, onClose, onCreated }: AppCreateDialogProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [form, setForm] = useState(blankAppForm);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const definition = definitionFromForm(form);
    setBusy(true);
    setError(null);
    try {
      const response = await client.apps.create({ definition, clientRequestId: undefined });
      if (response.app === undefined) throw new Error("The server returned an empty app workspace.");
      onCreated(response.app);
    } catch (requestError) {
      setError(appCreateError(requestError));
    } finally {
      setBusy(false);
    }
  }

  return <Modal title="Create app" subtitle="Create an app workspace and its default search context." dismissible={!busy} onClose={onClose} footer={<><button className="button button--secondary" type="button" disabled={busy} onClick={onClose}>Cancel</button><button className="button button--primary" type="submit" form="create-app-form" disabled={busy || !form.slug.trim() || !form.displayName.trim()}>{busy ? "Creating…" : "Create app"}</button></>}><form id="create-app-form" onSubmit={(event) => void submit(event)}>{error === null ? null : <div className="access-mode-notice" role="alert"><span>!</span><div><strong>App could not be created</strong><p>{error}</p></div></div>}<AppFields form={form} onChange={setForm} editing={false} /></form></Modal>;
}

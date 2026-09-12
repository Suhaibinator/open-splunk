"use client";

import { useEffect, useRef, useState, type FormEvent } from "react";

import { SharingScope } from "@/gen/ts/open_splunk/common";
import {
  isHttpError,
  isHttpStatus,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import {
  getServerSavedSearch,
  type ServerSavedSearch,
} from "@/lib/search/server-objects";
import {
  isEditableSavedSearchScope,
  SAVED_SEARCH_SCOPE_OPTIONS,
  savedSearchScopeLabel,
  updateSavedSearchScope,
  type EditableSavedSearchScope,
} from "@/lib/search/saved-search-scope";

import { Modal } from "../_components/modal";
import { Select, SelectOption } from "../_components/select";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The saved-search sharing update failed.";
}

function mayHaveReachedServer(error: unknown): boolean {
  return !isHttpError(error) || error.status === 408 || error.status >= 500;
}

interface SavedSearchScopeEditorProps {
  bootstrap: SystemBootstrapModel;
  client: OpenSplunkApiClient;
  onClose: () => void;
  onNotice: (message: string) => void;
  onUpdated: (savedSearch: ServerSavedSearch) => void;
  savedSearch: ServerSavedSearch;
}

const clientIdentities = new WeakMap<OpenSplunkApiClient, number>();
let nextClientIdentity = 1;

function clientIdentity(client: OpenSplunkApiClient): number {
  const known = clientIdentities.get(client);
  if (known !== undefined) return known;
  const identity = nextClientIdentity;
  nextClientIdentity += 1;
  clientIdentities.set(client, identity);
  return identity;
}

export function SavedSearchScopeEditor(props: SavedSearchScopeEditorProps) {
  const { client, savedSearch } = props;
  return (
    <SavedSearchScopeEditorSession
      key={`${clientIdentity(client)}:${savedSearch.id}:${savedSearch.version}`}
      {...props}
    />
  );
}

function SavedSearchScopeEditorSession({
  bootstrap,
  client,
  onClose,
  onNotice,
  onUpdated,
  savedSearch,
}: SavedSearchScopeEditorProps) {
  const [baseline, setBaseline] = useState(savedSearch);
  const [selected, setSelected] = useState<SharingScope>(savedSearch.sharingScope);
  const [pending, setPending] = useState(false);
  const [confirmationRequired, setConfirmationRequired] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const activeRequestRef = useRef<AbortController | null>(null);
  const requestEpochRef = useRef(0);
  const submitLatchRef = useRef(false);

  useEffect(() => {
    requestEpochRef.current += 1;
    submitLatchRef.current = false;
    return () => {
      requestEpochRef.current += 1;
      activeRequestRef.current?.abort();
      activeRequestRef.current = null;
      submitLatchRef.current = false;
    };
  }, []);

  const baselineIsEditable = isEditableSavedSearchScope(baseline.sharingScope);
  const selectedIsEditable = isEditableSavedSearchScope(selected);
  const dirty = selected !== baseline.sharingScope;

  const finish = (updated: ServerSavedSearch, message: string) => {
    onUpdated(updated);
    onNotice(message);
    onClose();
  };

  const loadCurrent = async (
    submitted: ServerSavedSearch,
    proposed: EditableSavedSearchScope,
    conflict: boolean,
    originalError: unknown,
    controller: AbortController,
    epoch: number,
  ) => {
    try {
      const currentResult = await getServerSavedSearch(
        client,
        bootstrap,
        submitted.id,
        { signal: controller.signal },
      );
      if (controller.signal.aborted || requestEpochRef.current !== epoch) return;
      if (currentResult.status === "unavailable") {
        throw new Error("The saved-search read route is no longer available.");
      }
      const current = currentResult.value;
      if (
        current.id !== submitted.id
        || current.version <= 0n
        || current.version < submitted.version
        || current.ownerId !== submitted.ownerId
        || (conflict && current.version <= submitted.version)
      ) {
        throw new TypeError("The server returned an invalid saved-search conflict baseline.");
      }

      setBaseline(current);
      if (!conflict && current.version > submitted.version && current.sharingScope === proposed) {
        finish(
          current,
          `Sharing for “${current.name}” was confirmed as ${savedSearchScopeLabel(current.sharingScope)} after reconnecting.`,
        );
        return;
      }

      setConfirmationRequired(true);
      const currentLabel = savedSearchScopeLabel(current.sharingScope);
      const proposedLabel = savedSearchScopeLabel(proposed);
      setError(conflict
        ? current.sharingScope === proposed
          ? `This saved search changed on the server and now also says ${currentLabel}. Review the refreshed version and submit once more to confirm this edit.`
          : `This saved search changed on the server and now says ${currentLabel}. Your proposed ${proposedLabel} value is preserved; review it and submit again.`
        : `The update response was not confirmed. The latest server value is ${currentLabel}; your proposed ${proposedLabel} value is preserved. Review it and submit again.`);
    } catch (reconcileError) {
      if (controller.signal.aborted || requestEpochRef.current !== epoch) return;
      setError(
        `${errorMessage(originalError)} The latest saved search could not be loaded: ${errorMessage(reconcileError)}`,
      );
    }
  };

  const save = async (event: FormEvent) => {
    event.preventDefault();
    if (
      submitLatchRef.current
      || !baselineIsEditable
      || !selectedIsEditable
      || (!dirty && !confirmationRequired)
    ) return;

    submitLatchRef.current = true;
    const submitted = baseline;
    const proposed = selected;
    const controller = new AbortController();
    const epoch = ++requestEpochRef.current;
    activeRequestRef.current?.abort();
    activeRequestRef.current = controller;
    setPending(true);
    setConfirmationRequired(false);
    setError(null);
    try {
      const updated = await updateSavedSearchScope(
        client,
        bootstrap,
        submitted,
        proposed,
        { signal: controller.signal },
      );
      if (controller.signal.aborted || requestEpochRef.current !== epoch) return;
      finish(
        updated,
        `Sharing for “${updated.name}” changed to ${savedSearchScopeLabel(updated.sharingScope)}.`,
      );
    } catch (cause) {
      if (controller.signal.aborted || requestEpochRef.current !== epoch) return;
      if (isHttpStatus(cause, 409) || mayHaveReachedServer(cause)) {
        await loadCurrent(
          submitted,
          proposed,
          isHttpStatus(cause, 409),
          cause,
          controller,
          epoch,
        );
      } else {
        setError(errorMessage(cause));
      }
    } finally {
      if (requestEpochRef.current === epoch) {
        activeRequestRef.current = null;
        submitLatchRef.current = false;
        setPending(false);
      }
    }
  };

  return (
    <Modal
      title="Edit saved-search sharing"
      subtitle={`Change the organizational sharing label for “${baseline.name}”.`}
      initialFocus="#reports-sharing-scope"
      dismissible={!pending}
      onClose={onClose}
      footer={(
        <>
          <button className="button button--secondary" type="button" disabled={pending} onClick={onClose}>Cancel</button>
          <button
            className="button button--primary"
            type="submit"
            form="reports-edit-sharing-scope"
            disabled={pending || !baselineIsEditable || !selectedIsEditable || (!dirty && !confirmationRequired)}
            aria-busy={pending}
          >
            {pending ? "Saving…" : confirmationRequired ? "Submit again" : "Save sharing"}
          </button>
        </>
      )}
    >
      <form className="form-stack" id="reports-edit-sharing-scope" onSubmit={(event) => void save(event)}>
        {error === null ? null : <p className="reports-action-error" role="alert">{error}</p>}
        {!baselineIsEditable ? (
          <p className="reports-action-error" role="alert">
            The server returned an unknown sharing value. This newer value cannot be edited by this client.
          </p>
        ) : null}
        <label htmlFor="reports-sharing-scope">
          <span>Sharing</span>
          <Select
            aria-label="Sharing"
            id="reports-sharing-scope"
            value={String(selected)}
            disabled={pending || !baselineIsEditable}
            placeholder="Choose a supported scope"
            onValueChange={(value) => {
              const scope = Number(value) as SharingScope;
              if (!isEditableSavedSearchScope(scope)) return;
              setSelected(scope);
              setError(null);
              setConfirmationRequired(false);
            }}
          >
            {SAVED_SEARCH_SCOPE_OPTIONS.map((option) => (
              <SelectOption
                disabled={option.value === SharingScope.SHARING_SCOPE_APP && !baseline.search.appId?.trim()}
                key={option.value}
                value={String(option.value)}
              >
                {option.label}
              </SelectOption>
            ))}
          </Select>
        </label>
        <p className="reports-action-hint">
          This field organizes saved searches in the current single-user model. It does not grant access or change search, time range, schedule, app, or owner.
        </p>
      </form>
    </Modal>
  );
}

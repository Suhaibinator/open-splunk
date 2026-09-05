"use client";

import { useState, type FormEvent } from "react";

import { IngestionTokenPurpose, type IngestionToken } from "@/gen/ts/open_splunk/collector_admin";

import { FieldNote, fieldControlProps } from "../_components/field-validation";
import { Modal } from "../_components/modal";
import { HECTokenProfileFields, TokenPolicyFields, TokenScopePicker, tokenCanBeRevoked, tokenStateLabel, type TokenIndexScopeOption, type TokenScopeSource } from "./backend-admin-panels";
import { tokenPolicyFormFromToken, tokenPolicyIsValid, type TokenPolicyForm } from "./ingestion-policy-form";
import type { TokenRecoveryOwnership } from "./token-create-recovery-policy";
import { COLLECTOR_ID_ERROR, tokenUsesHEC, validCollectorId, validHECMetadataDefault } from "./token-creation";
import { Select, SelectOption } from "../_components/select";

export interface TokenCreateFormValue {
  collectorId: string;
  description: string;
  expiration: string;
  hecDefaultHost: string;
  hecDefaultIndex: string;
  hecDefaultSource: string;
  hecDefaultSourcetype: string;
  hecIndexerAcknowledgment: boolean;
  indexes: Set<string>;
  name: string;
  policy: TokenPolicyForm;
  purpose: IngestionTokenPurpose;
}

interface TokenCreationDialogProps {
  blockReason: string | null;
  busy: boolean;
  hecEnabled: boolean;
  onClose: () => void;
  onSubmit: (value: TokenCreateFormValue) => void;
  scopeOptions: TokenIndexScopeOption[];
  scopeSource: TokenScopeSource;
}

export function TokenCreationDialog({
  blockReason,
  busy,
  hecEnabled,
  onClose,
  onSubmit,
  scopeOptions,
  scopeSource,
}: TokenCreationDialogProps) {
  const [collectorId, setCollectorId] = useState("");
  const [description, setDescription] = useState("");
  const [expiration, setExpiration] = useState("");
  const [hecDefaultHost, setHECDefaultHost] = useState("");
  const [hecDefaultIndex, setHECDefaultIndex] = useState("");
  const [hecDefaultSource, setHECDefaultSource] = useState("");
  const [hecDefaultSourcetype, setHECDefaultSourcetype] = useState("");
  const [hecIndexerAcknowledgment, setHECIndexerAcknowledgment] = useState(false);
  const [indexes, setIndexes] = useState<Set<string>>(
    () => new Set(scopeOptions.filter((option) => option.ingestible).slice(0, 1).map((option) => option.name)),
  );
  const [name, setName] = useState("");
  const [policy, setPolicy] = useState<TokenPolicyForm>(() => tokenPolicyFormFromToken());
  const [purpose, setPurpose] = useState(
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
  );
  const creatingHECToken = tokenUsesHEC(purpose);
  const ingestibleIndexNames = new Set(
    scopeOptions.filter((option) => option.ingestible).map((option) => option.name),
  );
  const hasUnavailableScope = [...indexes].some((indexName) => !ingestibleIndexNames.has(indexName));
  const collectorIdInvalid = !validCollectorId(collectorId);
  const hecDefaultIndexInvalid = hecDefaultIndex.length > 0 && !indexes.has(hecDefaultIndex);
  const hecMetadataInvalid = !validHECMetadataDefault(hecDefaultHost)
    || !validHECMetadataDefault(hecDefaultSource)
    || !validHECMetadataDefault(hecDefaultSourcetype);
  const hecProfileInvalid = hecDefaultIndexInvalid || hecMetadataInvalid;
  const scopeInvalid = scopeSource === "unavailable"
    || indexes.size === 0
    || hasUnavailableScope
    || (!creatingHECToken && collectorIdInvalid)
    || (creatingHECToken && !hecEnabled)
    || (creatingHECToken && hecProfileInvalid)
    || blockReason !== null;

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onSubmit({
      collectorId,
      description,
      expiration,
      hecDefaultHost,
      hecDefaultIndex,
      hecDefaultSource,
      hecDefaultSourcetype,
      hecIndexerAcknowledgment,
      indexes,
      name,
      policy,
      purpose,
    });
  }

  return (
    <Modal
      title="Generate ingestion token"
      subtitle="Scope a new credential to one or more ingestible indexes."
      dismissible={!busy}
      initialFocus="#new-token-name"
      onClose={onClose}
      footer={<><button className="button button--secondary" type="button" onClick={onClose} disabled={busy}>Cancel</button><button className="button button--primary" type="submit" form="create-token-form" disabled={busy || name.trim().length === 0 || scopeInvalid || !tokenPolicyIsValid(policy)}>{busy ? "Generating…" : "Generate token"}</button></>}
    >
      <form className="admin-form" id="create-token-form" onSubmit={submit}>
        <label htmlFor="new-token-name"><span>Token name</span><input id="new-token-name" value={name} onChange={(event) => setName(event.target.value)} placeholder="prod-api-collector" autoComplete="off" /></label>
        <label htmlFor="new-token-description"><span>Description <small>(optional)</small></span><input id="new-token-description" value={description} onChange={(event) => setDescription(event.target.value)} placeholder="Production collector credential" /></label>
        <label htmlFor="new-token-purpose"><span>Purpose</span><Select id="new-token-purpose" value={String(purpose)} onValueChange={(selectedValue) => { const next = Number(selectedValue) as IngestionTokenPurpose; setPurpose(next); if (tokenUsesHEC(next)) setCollectorId(""); }}><SelectOption value={String(IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR)}>Native collector</SelectOption><SelectOption value={String(IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC)} disabled={!hecEnabled}>HTTP Event Collector (HEC){hecEnabled ? "" : " — disabled on server"}</SelectOption></Select><small>Purpose is an immutable transport boundary. HEC credentials can only be created while the server advertises HEC ingestion.</small></label>
        {creatingHECToken ? null : (
          <label htmlFor="new-token-collector-id">
            <span>Collector ID</span>
            <input autoComplete="off" id="new-token-collector-id" onChange={(event) => setCollectorId(event.target.value)} placeholder="Paste the collector’s stable ID" spellCheck={false} value={collectorId} {...fieldControlProps("new-token-collector-id", collectorIdInvalid ? COLLECTOR_ID_ERROR : null)} />
            <FieldNote error={collectorIdInvalid ? COLLECTOR_ID_ERROR : null} fieldId="new-token-collector-id">Run <code>open-splunk-collector identity -config PATH</code> against the collector’s final state directory, then paste the printed ID. The binding cannot be changed after creation.</FieldNote>
          </label>
        )}
        <TokenScopePicker idPrefix="new-token" options={scopeOptions} selected={indexes} onChange={setIndexes} disabled={scopeSource === "unavailable"} />
        <TokenPolicyFields
          idPrefix="new-token"
          value={policy}
          onChange={(next) => setPolicy((current) => ({ ...current, ...next }))}
        />
        {creatingHECToken ? (
          <HECTokenProfileFields
            idPrefix="new-token"
            selectedIndexes={indexes}
            defaultIndex={hecDefaultIndex}
            onDefaultIndexChange={setHECDefaultIndex}
            defaultHost={hecDefaultHost}
            onDefaultHostChange={setHECDefaultHost}
            defaultSource={hecDefaultSource}
            onDefaultSourceChange={setHECDefaultSource}
            defaultSourcetype={hecDefaultSourcetype}
            onDefaultSourcetypeChange={setHECDefaultSourcetype}
            indexerAcknowledgment={hecIndexerAcknowledgment}
            onIndexerAcknowledgmentChange={setHECIndexerAcknowledgment}
          />
        ) : null}
        {creatingHECToken && hecProfileInvalid ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>HEC defaults are invalid</strong><p>The default index must remain in the allowed scope. Metadata defaults must contain 1–255 UTF-8 bytes without control characters or surrounding ASCII whitespace.</p></div></div> : null}
        {hasUnavailableScope ? <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Choose an available scope</strong><p>Tokens can only be generated for active, ingestion-enabled indexes. Remove the unavailable scope before continuing.</p></div></div> : null}
        {scopeSource === "unavailable" ? <div className="access-mode-notice" role="note"><span>i</span><div><strong>Index scopes are unavailable</strong><p>Token generation is disabled until the server returns an authoritative index summary.</p></div></div> : null}
        {blockReason === null ? null : <div className="access-mode-notice" role="alert"><span>!</span><div><strong>Token generation is locked</strong><p>{blockReason}</p></div></div>}
        <label htmlFor="new-token-expiration"><span>Expiration <small>(optional)</small></span><input id="new-token-expiration" type="datetime-local" value={expiration} onChange={(event) => setExpiration(event.target.value)} /><small>Leave blank for a token that does not expire. Any expiration must be in the future.</small></label>
      </form>
    </Modal>
  );
}

interface IssuedTokenDialogProps {
  busy: string | null;
  curlExample: string | null;
  dismissible: boolean;
  issuedToken: IngestionToken;
  onAcknowledge: (secretAcknowledged: boolean) => void;
  onClose: () => void;
  onCopyResult: (message: string, success: boolean) => void;
  onRevoke: () => void;
  ownership: TokenRecoveryOwnership;
  secret: string | null;
}

export function issuedTokenCompletionDisabled(options: {
  busy: boolean;
  canRevoke: boolean;
  hasSecret: boolean;
  ownership: TokenRecoveryOwnership;
  secretAcknowledged: boolean;
}): boolean {
  return options.busy
    || options.ownership !== "owned"
    || (options.hasSecret ? !options.secretAcknowledged : options.canRevoke);
}

export function IssuedTokenDialog({
  busy,
  curlExample,
  dismissible,
  issuedToken,
  onAcknowledge,
  onClose,
  onCopyResult,
  onRevoke,
  ownership,
  secret,
}: IssuedTokenDialogProps) {
  const [secretAcknowledged, setSecretAcknowledged] = useState(false);
  const hasSecret = secret !== null && secret.length > 0;
  const canRevoke = tokenCanBeRevoked(issuedToken);

  function copy(value: string, successMessage: string, failureMessage: string) {
    void navigator.clipboard.writeText(value).then(
      () => onCopyResult(successMessage, true),
      () => onCopyResult(failureMessage, false),
    );
  }

  return (
    <Modal
      title="Save this token now"
      subtitle="The server reveals this plaintext credential only once."
      dismissible={dismissible}
      initialFocus={!hasSecret ? busy === null ? "#revoke-issued-token" : undefined : "#copy-issued-token"}
      onClose={onClose}
      footer={(
        <>
          <button id="revoke-issued-token" className="button button--danger" type="button" disabled={busy !== null || ownership !== "owned" || !canRevoke} onClick={onRevoke}>
            {busy === `issued-token-${issuedToken.ingestionTokenId}` ? "Revoking…" : "Revoke unused token"}
          </button>
          <button
            className="button button--primary"
            type="button"
            disabled={issuedTokenCompletionDisabled({
              busy: busy !== null,
              canRevoke,
              hasSecret,
              ownership,
              secretAcknowledged,
            })}
            onClick={() => onAcknowledge(secretAcknowledged)}
          >
            Done
          </button>
        </>
      )}
    >
      <div className="token-reveal">
        <span className="token-warning-icon">!</span>
        {!hasSecret ? (
          <p>{canRevoke
            ? "The server created this token, but its plaintext secret is no longer available. Revoke the unusable token before generating another; you may close this dialog and keep using the app."
            : `This token was identified without its plaintext secret and is confirmed ${tokenStateLabel(issuedToken.state).toLowerCase()}. It is no longer usable.`}</p>
        ) : (
          <>
            <p>Copy this credential now. Closing, reloading, or navigating away cannot reveal it again.</p>
            <div><code>{secret}</code><button id="copy-issued-token" type="button" onClick={() => copy(secret, "Token copied to the clipboard.", "Copy failed. Select the token text and copy it manually.")}>Copy token</button></div>
            {curlExample === null ? null : (
              <section className="token-recovery-summary token-recovery-summary--wide" aria-label="HEC curl example">
                <strong>Send a test HEC event</strong>
                <p>This command contains the one-time credential and disappears permanently when this dialog is dismissed.</p>
                <pre className="token-recovery-command"><code>{curlExample}</code></pre>
                <button type="button" onClick={() => copy(curlExample, "HEC curl example copied to the clipboard.", "Copy failed. Select the curl command and copy it manually.")}>Copy curl example</button>
              </section>
            )}
            <label className="admin-checkbox" htmlFor="token-secret-acknowledgement" aria-label="I stored this ingestion token securely">
              <input
                id="token-secret-acknowledgement"
                type="checkbox"
                checked={secretAcknowledged}
                onChange={(event) => setSecretAcknowledged(event.target.checked)}
              />
              <span><strong>I stored this token securely</strong><small>Required before this one-time secret can be dismissed.</small></span>
            </label>
          </>
        )}
      </div>
    </Modal>
  );
}

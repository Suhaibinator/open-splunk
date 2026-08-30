import { useState } from "react";

import { Modal } from "../_components/modal";

interface AlertSecretRecoveryProps {
  alertName: string;
  secret: string;
  onClose: () => void;
  returnFocus?: HTMLElement | null;
}

type SecretAcknowledgement = "discarded" | "pending" | "saved";

export function AlertSecretRecovery({ alertName, secret, onClose, returnFocus = null }: AlertSecretRecoveryProps) {
  const [acknowledgement, setAcknowledgement] = useState<SecretAcknowledgement>("pending");
  const [copySucceeded, setCopySucceeded] = useState(false);
  const [copyError, setCopyError] = useState(false);
  const mayClose = acknowledgement !== "pending";

  async function copySecret() {
    try {
      await navigator.clipboard.writeText(secret);
      setAcknowledgement("saved");
      setCopySucceeded(true);
      setCopyError(false);
    } catch {
      setCopyError(true);
    }
  }

  return (
    <Modal
      title="Save the webhook signing secret"
      subtitle={`This is the only time the secret for “${alertName}” will be shown.`}
      dismissible={false}
      onClose={() => {}}
      returnFocus={returnFocus}
      footer={<button className="button button--primary" type="button" disabled={!mayClose} onClick={onClose}>I’m done</button>}
    >
      <div className="alerts-secret-recovery">
        <p>Configure the receiver to verify the Open-Splunk-Signature header. The secret cannot be recovered later; rotate it to issue a replacement.</p>
        <div className="alerts-secret-value"><code>{secret}</code><button className="button button--secondary" type="button" onClick={() => void copySecret()}>Copy secret</button></div>
        {copyError ? <p className="alerts-inline-error" role="alert">Copy failed. Select the secret and copy it manually.</p> : null}
        {copySucceeded ? <output className="alerts-inline-success" aria-live="polite">Copied to the clipboard.</output> : null}
        <label className="alerts-discard-confirmation"><input type="checkbox" checked={acknowledgement === "saved"} onChange={(event) => { setAcknowledgement(event.target.checked ? "saved" : "pending"); if (event.target.checked) setCopyError(false); }} />I confirm that I saved this secret, either automatically or manually.</label>
        <label className="alerts-discard-confirmation"><input type="checkbox" checked={acknowledgement === "discarded"} onChange={(event) => { setAcknowledgement(event.target.checked ? "discarded" : "pending"); if (event.target.checked) setCopySucceeded(false); }} />I understand that closing without copying permanently discards this secret.</label>
      </div>
    </Modal>
  );
}

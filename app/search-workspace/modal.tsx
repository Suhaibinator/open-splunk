import { type ReactNode, useEffect, useRef } from "react";

interface ModalProps {
  title: string;
  subtitle?: string;
  wide?: boolean;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  returnFocus?: HTMLElement | null;
}

export function Modal({ title, subtitle, wide = false, onClose, children, footer, returnFocus = null }: ModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const returnFocusRef = useRef<HTMLElement | null>(null);
  const onCloseRef = useRef(onClose);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (dialog === null) return;
    const mountedDialog: HTMLDialogElement = dialog;
    const previousBodyOverflow = document.body.style.overflow;
    returnFocusRef.current = returnFocus ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    document.body.style.overflow = "hidden";

    const focusableControls = () => Array.from(mountedDialog.querySelectorAll<HTMLElement>(
      'button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), a[href], [tabindex]:not([tabindex="-1"])',
    )).filter((element) => !element.hasAttribute("hidden") && element.getClientRects().length > 0);

    const focusFrame = window.requestAnimationFrame(() => {
      (focusableControls()[0] ?? mountedDialog).focus();
    });

    function handleDialogKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseRef.current();
        return;
      }
      if (event.key !== "Tab") return;
      const controls = focusableControls();
      if (controls.length === 0) {
        event.preventDefault();
        mountedDialog.focus();
        return;
      }
      const first = controls[0];
      const last = controls.at(-1) ?? first;
      if (event.shiftKey && (document.activeElement === first || !mountedDialog.contains(document.activeElement))) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && (document.activeElement === last || !mountedDialog.contains(document.activeElement))) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", handleDialogKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleDialogKeyDown);
      document.body.style.overflow = previousBodyOverflow;
      if (returnFocusRef.current?.isConnected) returnFocusRef.current.focus();
    };
  }, [returnFocus]);

  return (
    <div className="modal-layer" data-testid="modal-layer">
      <button className="modal-backdrop" aria-label="Close dialog" type="button" onClick={onClose} />
      <dialog
        ref={dialogRef}
        open
        className={`modal-card${wide ? " modal-card-wide" : ""}`}
        aria-labelledby="modal-title"
        aria-modal="true"
        tabIndex={-1}
      >
        <header className="modal-header">
          <div>
            <h2 id="modal-title">{title}</h2>
            {subtitle === undefined ? null : <p>{subtitle}</p>}
          </div>
          <button className="icon-button close-button" aria-label="Close dialog" type="button" onClick={onClose}>×</button>
        </header>
        <div className="modal-body">{children}</div>
        {footer === undefined ? null : <footer className="modal-footer">{footer}</footer>}
      </dialog>
    </div>
  );
}

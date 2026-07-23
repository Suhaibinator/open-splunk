import { type ReactNode, useEffect, useRef } from "react";

import { installModalSurface } from "../_components/modal-surface";

interface ModalProps {
  title: string;
  subtitle?: string;
  wide?: boolean;
  dismissible?: boolean;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  returnFocus?: HTMLElement | null;
  initialFocus?: string;
}

export function Modal({
  title,
  subtitle,
  wide = false,
  dismissible = true,
  onClose,
  children,
  footer,
  returnFocus = null,
  initialFocus,
}: ModalProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const onCloseRef = useRef(onClose);
  const dismissibleRef = useRef(dismissible);
  onCloseRef.current = onClose;
  dismissibleRef.current = dismissible;

  useEffect(() => {
    const dialog = dialogRef.current;
    if (dialog === null) return;
    const capturedReturnFocus = returnFocus
      ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    return installModalSurface({
      container: dialog,
      excludedSiblingClassNames: ["modal-backdrop"],
      onEscape: () => {
        if (dismissibleRef.current) onCloseRef.current();
      },
      returnFocus: capturedReturnFocus,
    });
  }, [returnFocus]);

  useEffect(() => {
    if (initialFocus === undefined) return;
    const focusFrame = window.requestAnimationFrame(() => {
      dialogRef.current?.querySelector<HTMLElement>(initialFocus)?.focus();
    });
    return () => window.cancelAnimationFrame(focusFrame);
  }, [initialFocus]);

  return (
    <div className="modal-layer" data-testid="modal-layer">
      {dismissible
        ? <button className="modal-backdrop" aria-label="Close dialog" type="button" onClick={onClose} />
        : <div className="modal-backdrop" aria-hidden="true" />}
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
          {dismissible
            ? <button className="icon-button close-button" aria-label="Close dialog" type="button" onClick={onClose}>×</button>
            : null}
        </header>
        <div className="modal-body">{children}</div>
        {footer === undefined ? null : <footer className="modal-footer">{footer}</footer>}
      </dialog>
    </div>
  );
}

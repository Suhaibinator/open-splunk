import type { ReactNode } from "react";

export interface BackendResourceStateProps {
  kind: "loading" | "unavailable" | "error" | "empty";
  title: string;
  message: string;
  action?: ReactNode;
}

export function BackendResourceState({ kind, title, message, action }: BackendResourceStateProps) {
  return (
    <div className={`backend-resource-state backend-resource-state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <span aria-hidden="true">{kind === "loading" ? "↻" : kind === "error" ? "!" : kind === "empty" ? "∅" : "i"}</span>
      <div><strong>{title}</strong><p>{message}</p></div>
      {action}
    </div>
  );
}

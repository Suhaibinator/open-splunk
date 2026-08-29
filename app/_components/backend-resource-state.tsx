import type { ReactNode } from "react";

import { StatusIcon } from "./app-icon";

export interface BackendResourceStateProps {
  kind: "loading" | "unavailable" | "error" | "empty";
  title: string;
  message: string;
  action?: ReactNode;
}

export function BackendResourceState({ kind, title, message, action }: BackendResourceStateProps) {
  return (
    <div className={`backend-resource-state backend-resource-state--${kind}`} role={kind === "error" ? "alert" : "status"}>
      <StatusIcon
        tone={kind === "error" ? "error" : kind === "loading" ? "info" : "neutral"}
        icon={kind === "loading" ? "loading" : kind === "error" ? "warning" : kind === "empty" ? "circle-x" : "info"}
        spin={kind === "loading"}
      />
      <div><strong>{title}</strong><p>{message}</p></div>
      {action}
    </div>
  );
}

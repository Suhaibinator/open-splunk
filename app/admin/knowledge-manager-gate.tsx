"use client";

import type { ComponentType } from "react";
import { useEffect, useState } from "react";

import type {
  KnowledgeManagerPanelModule,
  KnowledgeManagerPanelProps,
} from "./knowledge-manager-feature";

const importKnowledgeManagerPanel = (): Promise<KnowledgeManagerPanelModule> =>
  import("./knowledge-manager-panel" as string);

export function KnowledgeManagerGate(panelProps: KnowledgeManagerPanelProps) {
  const [Panel, setPanel] = useState<ComponentType<KnowledgeManagerPanelProps> | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let current = true;
    void importKnowledgeManagerPanel().then(
      (module) => {
        if (current) setPanel(() => module.KnowledgeManagerPanel);
      },
      () => {
        if (current) setFailed(true);
      },
    );
    return () => {
      current = false;
    };
  }, []);

  if (failed) {
    return (
      <output className="knowledge-manager__status knowledge-manager__status--unavailable">
        <span aria-hidden="true">!</span>
        <span className="knowledge-manager__status-copy">
          <strong>Knowledge Manager unavailable</strong>
          <span>The read-only knowledge surface could not be loaded.</span>
        </span>
      </output>
    );
  }
  if (Panel === null) {
    return (
      <output className="knowledge-manager__status knowledge-manager__status--loading">
        <span aria-hidden="true">…</span>
        <span className="knowledge-manager__status-copy">
          <strong>Loading Knowledge Manager</strong>
          <span>Preparing the advertised read-only surface…</span>
        </span>
      </output>
    );
  }
  return <Panel {...panelProps} />;
}

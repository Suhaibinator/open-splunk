"use client";

import type { ComponentType } from "react";
import { useEffect, useState } from "react";

import { StatusIcon } from "../_components/app-icon";

import type { KnowledgeManagerAppOption } from "./knowledge-manager-feature";

export interface LookupManagerPanelProps {
  apiBaseUrl: string;
  apps: readonly KnowledgeManagerAppOption[];
  initialAppId: string | null;
}

interface LookupManagerModule {
  LookupManagerPanel: ComponentType<LookupManagerPanelProps>;
}

const importLookupManagerPanel = (): Promise<LookupManagerModule> =>
  import("./lookup-manager-panel" as string);

export function LookupManagerGate(props: LookupManagerPanelProps) {
  const [Panel, setPanel] = useState<ComponentType<LookupManagerPanelProps> | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let current = true;
    void importLookupManagerPanel().then((module) => {
      if (current) setPanel(() => module.LookupManagerPanel);
    }, () => {
      if (current) setFailed(true);
    });
    return () => {
      current = false;
    };
  }, []);

  if (failed) {
    return (
      <output className="backend-resource-state backend-resource-state--unavailable">
        <StatusIcon tone="error" icon="warning" />
        <div><strong>Lookup Manager unavailable</strong><p>The advertised lookup management surface could not be loaded.</p></div>
      </output>
    );
  }
  if (Panel === null) {
    return (
      <output className="backend-resource-state backend-resource-state--loading">
        <StatusIcon tone="info" icon="loading" spin />
        <div><strong>Loading Lookup Manager</strong><p>Preparing the bounded CSV management surface…</p></div>
      </output>
    );
  }
  return <Panel {...props} />;
}

export interface HelpHeading {
  depth: number;
  id: string;
  text: string;
}

interface HelpNodeIdentity {
  key: string;
}

export type HelpInlineNode =
  | (HelpNodeIdentity & { type: "text"; text: string })
  | (HelpNodeIdentity & { type: "code"; text: string })
  | (HelpNodeIdentity & { type: "strong" | "emphasis" | "delete"; children: HelpInlineNode[] })
  | (HelpNodeIdentity & { type: "break" })
  | (HelpNodeIdentity & { type: "link"; children: HelpInlineNode[]; external: boolean; href: string; title?: string })
  | (HelpNodeIdentity & { type: "image-reference"; alt: string; external: boolean; href: string; title?: string });

export type HelpBlockNode =
  | (HelpNodeIdentity & { type: "heading"; children: HelpInlineNode[]; depth: number; id: string })
  | (HelpNodeIdentity & { type: "paragraph"; children: HelpInlineNode[] })
  | (HelpNodeIdentity & { type: "code-block"; language?: string; text: string })
  | (HelpNodeIdentity & { type: "blockquote"; children: HelpBlockNode[] })
  | (HelpNodeIdentity & { type: "list"; items: Array<HelpNodeIdentity & { children: HelpBlockNode[] }>; ordered: boolean; start?: number })
  | (HelpNodeIdentity & {
    type: "table";
    align: Array<"center" | "left" | "right" | null>;
    header: Array<HelpNodeIdentity & { children: HelpInlineNode[] }>;
    rows: Array<HelpNodeIdentity & { cells: Array<HelpNodeIdentity & { children: HelpInlineNode[] }> }>;
  })
  | (HelpNodeIdentity & { type: "thematic-break" });

export interface HelpDocument {
  headings: HelpHeading[];
  kind: "markdown" | "source";
  nodes: HelpBlockNode[];
  route: string;
  searchText: string;
  section: string;
  slug: string;
  sourcePath: string;
  title: string;
}

export interface HelpContentBundle {
  contentRevision: string;
  documents: HelpDocument[];
}

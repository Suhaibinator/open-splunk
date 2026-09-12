import type { ReactNode } from "react";

import type {
  HelpBlockNode,
  HelpDocument,
  HelpInlineNode,
} from "@/lib/help/help-content";

const identityHref = (href: string) => href;

function externalNote(external: boolean): ReactNode {
  return external ? <span className="sr-only"> (external)</span> : null;
}

function inlineContent(nodes: readonly HelpInlineNode[], localHref: (href: string) => string): ReactNode {
  return nodes.map((node) => {
    switch (node.type) {
      case "text": return <span key={node.key}>{node.text}</span>;
      case "code": return <code key={node.key}>{node.text}</code>;
      case "strong": return <strong key={node.key}>{inlineContent(node.children, localHref)}</strong>;
      case "emphasis": return <em key={node.key}>{inlineContent(node.children, localHref)}</em>;
      case "delete": return <del key={node.key}>{inlineContent(node.children, localHref)}</del>;
      case "break": return <br key={node.key} />;
      case "link": return <a href={node.external ? node.href : localHref(node.href)} key={node.key} rel={node.external ? "noreferrer" : undefined}>{inlineContent(node.children, localHref)}{externalNote(node.external)}</a>;
      case "image-reference": return <a href={node.external ? node.href : localHref(node.href)} key={node.key} rel={node.external ? "noreferrer" : undefined}>Image reference: {node.alt || node.title || node.href}{externalNote(node.external)}</a>;
    }
  });
}

function heading(node: Extract<HelpBlockNode, { type: "heading" }>, localHref: (href: string) => string): ReactNode {
  const content = inlineContent(node.children, localHref);
  switch (node.depth) {
    case 1: return <h2 id={node.id} key={node.key}>{content}</h2>;
    case 2: return <h2 id={node.id} key={node.key}>{content}</h2>;
    case 3: return <h3 id={node.id} key={node.key}>{content}</h3>;
    case 4: return <h4 id={node.id} key={node.key}>{content}</h4>;
    case 5: return <h5 id={node.id} key={node.key}>{content}</h5>;
    case 6: return <h6 id={node.id} key={node.key}>{content}</h6>;
    default: throw new Error(`Unsupported Help heading depth: ${node.depth}`);
  }
}

function blockContent(nodes: readonly HelpBlockNode[], replacedHeadingId: string | undefined, localHref: (href: string) => string): ReactNode {
  return nodes.map((node) => {
    switch (node.type) {
      case "heading": return node.id === replacedHeadingId ? null : heading(node, localHref);
      case "paragraph": return <p key={node.key}>{inlineContent(node.children, localHref)}</p>;
      case "code-block": return <div aria-label={node.language ? `${node.language} code example` : "Code example"} className="help-code-wrap" key={node.key} role="region"><pre><code>{node.text}</code></pre></div>;
      case "blockquote": return <blockquote key={node.key}>{blockContent(node.children, replacedHeadingId, localHref)}</blockquote>;
      case "list": {
        const items = node.items.map((item) => <li key={item.key}>{blockContent(item.children, replacedHeadingId, localHref)}</li>);
        return node.ordered
          ? <ol key={node.key} start={node.start}>{items}</ol>
          : <ul key={node.key}>{items}</ul>;
      }
      case "table": return <div className="table-wrap help-table-wrap" key={node.key}><table className="table"><thead><tr>{node.header.map((cell, columnIndex) => <th data-align={node.align[columnIndex]} key={cell.key} scope="col">{inlineContent(cell.children, localHref)}</th>)}</tr></thead><tbody>{node.rows.map((row) => <tr key={row.key}>{row.cells.map((cell, columnIndex) => <td data-align={node.align[columnIndex]} key={cell.key}>{inlineContent(cell.children, localHref)}</td>)}</tr>)}</tbody></table></div>;
      case "thematic-break": return <hr key={node.key} />;
    }
  });
}

export function HelpDocumentContent({ document, localHref = identityHref }: { document: HelpDocument; localHref?: (href: string) => string }) {
  const replacedHeadingId = document.headings.find((item) => item.depth === 1)?.id;
  return <article className="help-content">
    <h1 id={replacedHeadingId ?? "document-title"}>{document.title}</h1>
    <p className="help-source-path">Bundled from <code>{document.sourcePath}</code></p>
    {blockContent(document.nodes, replacedHeadingId, localHref)}
  </article>;
}

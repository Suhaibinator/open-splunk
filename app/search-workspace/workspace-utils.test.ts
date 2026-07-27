import assert from "node:assert/strict";
import test from "node:test";
import { isValidElement, type ReactNode } from "react";

import { syntaxTokens } from "./workspace-utils";

interface SyntaxTokenProps {
  children?: ReactNode;
  className?: string;
}

function classifiedTokens(query: string): Array<{ className: string; text: string }> {
  return syntaxTokens(query).map((node) => {
    assert.ok(isValidElement<SyntaxTokenProps>(node));
    return {
      className: node.props.className ?? "",
      text: String(node.props.children ?? ""),
    };
  });
}

test("count abbreviation highlights only when used as a parenthesized function", () => {
  const tokens = classifiedTokens(`index=main c=1 | stats c(user) BY c`);
  assert.deepEqual(
    tokens.filter((token) => token.text.toLowerCase() === "c" && token.className === "spl-function"),
    [{ className: "spl-function", text: "c" }],
  );
  assert.equal(tokens.map((token) => token.text).join(""), `index=main c=1 | stats c(user) BY c`);
});

import assert from "node:assert/strict";
import test from "node:test";
import { isValidElement, type ReactNode } from "react";

import { SPL_PIPELINE_COMMANDS } from "@/lib/search/spl-syntax";

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

test("null predicates highlight only when used as parenthesized functions", () => {
  const query = `index=main isnull=1 | where isnull(optional) OR ISNOTNULL(required) | table isnotnull`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["isnull", "isnotnull"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("if highlights only when used as a parenthesized function", () => {
  const query = `index=main if=1 | eval state=IF(isnull(optional), "missing", "present") | table if`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["if", "isnull"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("coalesce highlights only when used as a parenthesized function", () => {
  const query = `index=main coalesce=1 | eval selected=COALESCE(null, source, "fallback") | table coalesce`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["coalesce"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("eval completion advertises the exact if and coalesce signatures", () => {
  const evalCompletion = SPL_PIPELINE_COMMANDS.find((command) => command.name === "eval");
  assert.ok(evalCompletion);
  assert.equal(evalCompletion.insertion, 'eval availability=if(isnull(status), "missing", "present")');
  assert.match(evalCompletion.detail, /if\(predicate, true_value, false_value\)/);
  assert.match(evalCompletion.detail, /coalesce\(value, fallback, \.\.\.\)/);
  assert.match(evalCompletion.detail, /first non-null fixed value/i);
});

test("stats completion advertises true-only conditional count with an explicit alias", () => {
  const statsCompletion = SPL_PIPELINE_COMMANDS.find((command) => command.name === "stats");
  assert.ok(statsCompletion);
  assert.match(statsCompletion.insertion, /count\(eval\(status>=500\)\) AS errors/);
  assert.match(statsCompletion.detail, /true-only count\(eval\(predicate\)\) AS output/);
});

test("nested stats eval highlights as a function without relabeling the eval command", () => {
  const query = `index=main | stats count(EVAL(status=500)) AS errors | eval label="ok" | table label`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.text.toLowerCase() === "eval")
      .map((token) => token.className),
    ["spl-function", "spl-command"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

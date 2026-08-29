import assert from "node:assert/strict";
import test from "node:test";

import { serializeRowsForClipboard } from "./clipboard-export";

test("serializes the selected page as a spreadsheet-friendly clipboard table", () => {
  assert.equal(
    serializeRowsForClipboard(
      ["host", "message", "context", "missing"],
      { host: "Host", message: "Message", context: "Context" },
      [
        { host: "api-01", message: "ready", context: { attempt: 2 } },
        { host: "api-02", message: "line one\nline \"two\"", missing: null },
      ],
    ),
    [
      "Host\tMessage\tContext\tmissing",
      'api-01\tready\t"{""attempt"":2}"\t',
      'api-02\t"line one\nline ""two"""\t\t',
    ].join("\r\n"),
  );
});

test("keeps a header row when a page has no results", () => {
  assert.equal(serializeRowsForClipboard(["message"], {}, []), "message");
});

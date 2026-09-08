import assert from "node:assert/strict";
import test from "node:test";

import { serializeRowsAsCsv, serializeRowsAsJsonLinesForClipboard, serializeRowsForClipboard } from "./clipboard-export";

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
  assert.equal(serializeRowsAsCsv(["message"], {}, []), '"message"');
});

for (const prefix of ["=", "+", "-", "@", "\t", "\r", "\n", "'", "＝", "＋", "－", "＠"]) {
  test(`protects spreadsheet prefix ${JSON.stringify(prefix)} in labels, field names, and values`, () => {
    const value = `${prefix}1+1`;
    const csvCell = `"'${value}"`;
    const tsvCell = ["\t", "\r", "\n"].includes(prefix) ? csvCell : `'${value}`;
    const labelVariants: Record<string, string>[] = [{ field: value }, {}];
    for (const labels of labelVariants) {
      const field = "field" in labels ? "field" : value;
      const rows = [{ [field]: value }];
      assert.equal(serializeRowsAsCsv([field], labels, rows), `${csvCell}\n${csvCell}`);
      assert.equal(serializeRowsForClipboard([field], labels, rows), `${tsvCell}\r\n${tsvCell}`);
      assert.equal(rows[0][field], value, "export must not mutate source values");
    }
  });
}

test("keeps formula-leading and apostrophe-leading column identities distinct", () => {
  const fields = ["-a", "'-a", "''-a"];
  assert.equal(serializeRowsForClipboard(fields, {}, []), "'-a\t''-a\t'''-a");
  assert.equal(serializeRowsAsCsv(fields, {}, []), `"'-a","''-a","'''-a"`);
});

test("protects text without changing numeric, empty, boolean, or structured cells", () => {
  const fields = ["negative", "fraction", "bigint", "zero", "positive", "numericText", "exactText", "boolean", "null", "missing", "object", "array", "unicode"];
  const rows = [{
    negative: -7,
    fraction: -0.25,
    bigint: -9007199254740993n,
    zero: 0,
    positive: 42,
    numericText: "-7",
    exactText: "-9007199254740993",
    boolean: false,
    null: null,
    object: { expression: "=1+1" },
    array: ["=1+1", "@value"],
    unicode: "日本語 café",
  }];
  assert.equal(
    serializeRowsForClipboard(fields, {}, rows),
    `${fields.join("\t")}\r\n-7\t-0.25\t-9007199254740993\t0\t42\t'-7\t'-9007199254740993\tfalse\t\t\t"{""expression"":""=1+1""}"\t"[""=1+1"",""@value""]"\t日本語 café`,
  );
  assert.equal(
    serializeRowsAsCsv(fields, {}, rows),
    `${fields.map((field) => `"${field}"`).join(",")}\n"-7","-0.25","-9007199254740993","0","42","'-7","'-9007199254740993","false","","","{""expression"":""=1+1""}","[""=1+1"",""@value""]","日本語 café"`,
  );
});

test("keeps delimiters and quotes inside protected cells in both formats", () => {
  const rows = [{ value: '=1+1,"quoted";\r\n@next\tcell' }, { value: 'plain,\t=1+1;"quoted"' }];
  assert.equal(
    serializeRowsAsCsv(["value"], { value: 'Label,"quoted"' }, rows),
    `"Label,""quoted"""\n"'=1+1,""quoted"";\r\n@next\tcell"\n"plain,\t=1+1;""quoted"""`,
  );
  assert.equal(
    serializeRowsForClipboard(["value"], { value: 'Label,"quoted"' }, rows),
    `"Label,""quoted"""\r\n"'=1+1,""quoted"";\r\n@next\tcell"\r\n"plain,\t=1+1;""quoted"""`,
  );
});

test("local CSV keeps selected field order and omits unselected data", () => {
  assert.equal(
    serializeRowsAsCsv(["second", "first", "missing"], { first: "First" }, [{ first: "ready", second: "a,b", ignored: "not exported" }]),
    '"second","First","missing"\n"a,b","ready",""',
  );
});

test("spreadsheet protection leaves formula-looking JSON Lines keys and values unchanged", () => {
  const fields = ["=field", "array", "negative"];
  const row = { "=field": "=1+1", array: ["'text", "＠value", "\tvalue"], negative: -7 };
  assert.equal(serializeRowsAsJsonLinesForClipboard(fields, [row]), JSON.stringify(row));
});

test("serializes the selected page as JSON Lines restricted to the selected fields", () => {
  assert.equal(
    serializeRowsAsJsonLinesForClipboard(
      ["host", "status", "missing"],
      [
        { host: "api-01", status: 200, ignored: "dropped" },
        { host: "api-02", status: undefined, missing: null },
      ],
    ),
    [
      '{"host":"api-01","status":200,"missing":null}',
      '{"host":"api-02","status":null,"missing":null}',
    ].join("\n"),
  );
});

test("keeps multi-value JSON Lines cells as native arrays", () => {
  assert.equal(
    serializeRowsAsJsonLinesForClipboard(["path"], [{ path: ["/a", "/b"] }]),
    '{"path":["/a","/b"]}',
  );
});

test("produces no JSON Lines output when a page has no results", () => {
  assert.equal(serializeRowsAsJsonLinesForClipboard(["message"], []), "");
});

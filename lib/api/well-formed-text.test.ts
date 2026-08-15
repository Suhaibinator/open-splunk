import assert from "node:assert/strict";
import test from "node:test";

import { KnowledgeSelectorMatchKind } from "@/gen/ts/open_splunk/v1/knowledge";
import { ValueType } from "@/gen/ts/open_splunk/v1/value";

import {
  hasUnpairedSurrogate,
  isExactEventField,
  isExactLookupColumn,
  selectorPatternKind,
} from "../../app/admin/lookup-manager-contract";
import { isSplFieldRepresentable } from "../search/query-pivots";
import { canonicalBoundedServerText } from "../search/server-text";
import {
  MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES,
  validFlatMultivalueColumnPresentation,
} from "./result-column-presentation";

/** Every consumer below funnels an untrusted server string through isWellFormed(). */
const MALFORMED: readonly (readonly [string, string])[] = [
  ["lone high surrogate", "\ud800"],
  ["lone low surrogate", "\udc00"],
  ["reversed surrogate pair", "\udc00\ud800"],
  ["two consecutive high surrogates", "\ud83d\ud83d"],
  ["pair split by a BMP character", "\ud83dA\ude00"],
  ["highest high surrogate at the end", "ok\udbff"],
  ["highest low surrogate at the start", "\udfffok"],
];

const WELL_FORMED: readonly (readonly [string, string])[] = [
  ["astral emoji", "😀"],
  ["maximum code point", "\u{10ffff}"],
  ["byte order mark", "﻿"],
  ["replacement character", "�"],
  ["noncharacter U+FFFE", "￾"],
  ["plane 14 tag character", "\u{e0001}"],
];

function delimiterAccepted(delimiter: string): boolean {
  return validFlatMultivalueColumnPresentation({
    flatMultivalueDelimiter: delimiter,
    multivalue: true,
    statsSparkline: false,
    valueType: ValueType.VALUE_TYPE_LIST,
  });
}

test("every well-formedness consumer agrees with String.prototype.isWellFormed", () => {
  for (const [label, probe] of MALFORMED) {
    assert.equal(probe.isWellFormed(), false, label);
    assert.equal(hasUnpairedSurrogate(probe), true, label);
    assert.equal(delimiterAccepted(probe), false, label);
    assert.equal(isExactLookupColumn(probe), false, label);
    assert.equal(isExactEventField(`a.${probe}`), false, label);
    assert.equal(
      selectorPatternKind(probe),
      KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED,
      label,
    );
    assert.equal(canonicalBoundedServerText(probe, 64), null, label);
  }
  for (const [label, probe] of WELL_FORMED) {
    assert.equal(probe.isWellFormed(), true, label);
    assert.equal(hasUnpairedSurrogate(probe), false, label);
    assert.equal(delimiterAccepted(probe), true, label);
  }
});

test("a malformed SPL field is rejected in every position and before byte accounting", () => {
  for (const [label, probe] of MALFORMED) {
    const fields = [
      probe,
      `${probe}field`,
      `fie${probe}ld`,
      `field${probe}`,
      `a.${probe}.b`,
      `${probe}${"x".repeat(9_000)}`,
      `${probe}\\.escaped`,
    ];
    for (const field of fields) {
      assert.equal(isSplFieldRepresentable(field), false, `${label}: ${JSON.stringify(field)}`);
    }
  }
  // TextEncoder silently substitutes U+FFFD for a lone surrogate, so the
  // well-formedness gate has to run before any UTF-8 length is measured.
  assert.equal(isSplFieldRepresentable("😀.field"), true);
});

test("well-formed but hostile characters follow each module's own grammar", () => {
  // The SPL v0.1 field lexer has no format-character rule; the lookup schema
  // token grammar mirrors spl.IsExactUnquotedFieldName and rejects \p{Cf}.
  assert.equal(isSplFieldRepresentable("﻿field"), true);
  assert.equal(isExactLookupColumn("﻿field"), false);
  assert.equal(isSplFieldRepresentable("fi�eld"), true);
  assert.equal(isExactLookupColumn("fi�eld"), true);
  assert.equal(isSplFieldRepresentable("\u{1f600}field"), true);
  assert.equal(isExactLookupColumn("\u{1f600}field"), true);
  assert.equal(isExactLookupColumn("\u{e0001}field"), false);
  // A paired surrogate escape and a code point escape are the same string.
  assert.equal("😀", "\u{1f600}");
});

test("the flat delimiter ceiling counts UTF-8 bytes rather than UTF-16 code units", () => {
  const astral = "\u{1f600}".repeat(MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES / 4);
  assert.equal(astral.length, MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES / 2);
  assert.equal(delimiterAccepted(astral), true);
  assert.equal(delimiterAccepted(`${astral}a`), false);
  // A lone surrogate is one code unit but three replacement bytes; the
  // well-formedness gate must reject it rather than the byte bound.
  assert.equal(delimiterAccepted(`${astral.slice(0, -1)}`), false);
});

test("a sparkline column forbids any delimiter, well formed or not", () => {
  for (const delimiter of ["", ",", "\ud800", "\u{1f600}"]) {
    assert.equal(validFlatMultivalueColumnPresentation({
      flatMultivalueDelimiter: delimiter,
      multivalue: true,
      statsSparkline: true,
      valueType: ValueType.VALUE_TYPE_LIST,
    }), false, JSON.stringify(delimiter));
  }
});

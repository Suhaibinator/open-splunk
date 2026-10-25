import assert from "node:assert/strict";
import test from "node:test";

import type { IdentityOrganization, IdentityUser, Principal } from "@/lib/api/identity";
import {
  CJK_ORGANIZATION,
  EMOJI_FIRST_NAME,
  FLAG_LAST_NAME,
  HEBREW_FIRST_NAME,
  HEBREW_LAST_NAME,
  INVISIBLE_LEAD_NAME,
  LONG_EMAIL,
  LONG_WORD_ORGANIZATION,
  MARKUP_ORGANIZATION,
  RTL_ORGANIZATION,
  isWellFormed,
} from "@/lib/testing/hostile-text";

import {
  initialOf,
  leadingCharacter,
  organizationChoices,
  organizationInitial,
  userDisplayName,
  userInitials,
} from "./product-access";

const USER: IdentityUser = {
  id: "5",
  username: "mark",
  email: "mark@open-splunk.test",
  firstName: "Mark",
  lastName: "Mendez",
  userType: "regular",
  banned: false,
  isBot: false,
};

function organization(id: string, name: string, slug = `org-${id}`): IdentityOrganization {
  return { id, name, slug, createdAt: "2026-01-01T00:00:00.000Z", updatedAt: "2026-01-01T00:00:00.000Z", lineage: [] };
}

test("isWellFormed tells a lone surrogate from a pair, which is the defect charAt(0) made", () => {
  assert.equal(isWellFormed("😀"), true);
  assert.equal(isWellFormed("😀".charAt(0)), false, "charAt(0) of an emoji is half a surrogate pair");
  assert.equal(isWellFormed("\uDE00x"), false);
  assert.equal(isWellFormed(""), true);
});

test("the leading character is the whole first grapheme, however many code points it takes", () => {
  const cases: ReadonlyArray<readonly [string, string, string]> = [
    ["an emoji with a skin tone and a joiner", EMOJI_FIRST_NAME, "👩🏽‍💻"],
    ["a flag", FLAG_LAST_NAME, "🇳🇿"],
    ["a letter with a decomposed accent", "Émilie", "É"],
    ["a letter with two stacked marks", "Ẹ́lodie", "Ẹ́"],
    ["a mathematical letter outside the BMP", "𝓐lex", "𝓐"],
    ["a Hangul syllable written as jamo", "각bc", "각"],
    ["a keycap sequence", "1️⃣ team", "1️⃣"],
    ["Arabic", RTL_ORGANIZATION, "ش"],
    ["CJK", CJK_ORGANIZATION, "株"],
    ["markup", MARKUP_ORGANIZATION, "<"],
  ];
  for (const [label, text, expected] of cases) {
    const character = leadingCharacter(text);
    assert.equal(character, expected, label);
    assert.equal(isWellFormed(character), true, `${label}: well formed`);
  }
});

test("the leading character skips what draws nothing: space, bidi marks, zero-width characters and orphaned marks", () => {
  assert.equal(leadingCharacter(INVISIBLE_LEAD_NAME), "م");
  assert.equal(leadingCharacter(" \t\n  Ada"), "A");
  assert.equal(leadingCharacter("́̂abc"), "a", "a combining mark with nothing before it is not an initial");
  assert.equal(leadingCharacter("‍👍"), "👍", "a leading joiner joins nothing");
  assert.equal(leadingCharacter("⁦⁧isolated⁩"), "i", "bidi isolate controls are format characters");
  for (const blank of ["", " ", "​", "‏‎", "﻿", "́", "­"]) {
    assert.equal(leadingCharacter(blank), "", `${JSON.stringify(blank)} has no visible first character`);
  }
});

test("an initial is the capital of the first character when that capital is one character, else the character", () => {
  assert.equal(initialOf("éclair"), "É");
  assert.equal(initialOf("éclair"), "É");
  assert.equal(initialOf("ωmega"), "Ω");
  assert.equal(initialOf("ǆemal"), "Ǆ", "a digraph letter capitalises to one character");
  assert.equal(initialOf("straße"), "S");
  assert.equal(initialOf("ßtraße"), "ß", "ß capitalises to SS, which is two initials");
  assert.equal(initialOf("ﬁnance"), "ﬁ", "the ligature capitalises to FI");
  assert.equal(initialOf("ŉ"), "ŉ", "ŉ capitalises to ʼN");
  assert.equal(initialOf("istanbul"), "I");
  assert.equal(initialOf("42 crew"), "4");
  assert.equal(initialOf(HEBREW_FIRST_NAME), "ד", "a script without case keeps its letter");
  assert.equal(initialOf(""), "");
  assert.equal(initialOf("​"), "");
});

test("user initials take a whole character from each name and never half of one", () => {
  assert.equal(userInitials({ ...USER, firstName: EMOJI_FIRST_NAME, lastName: FLAG_LAST_NAME }), "👩🏽‍💻🇳🇿");
  assert.equal(userInitials({ ...USER, firstName: HEBREW_FIRST_NAME, lastName: HEBREW_LAST_NAME }), "דל");
  assert.equal(userInitials({ ...USER, firstName: INVISIBLE_LEAD_NAME, lastName: "" }), "م");
  assert.equal(userInitials({ ...USER, firstName: "", lastName: "​o'Brien" }), "O");
  assert.equal(userInitials({ ...USER, firstName: "​", lastName: "‏" }), "M", "invisible names fall back to the username");
  assert.equal(userInitials({ ...USER, firstName: "", lastName: "", username: "", email: "😀@open-splunk.test" }), "😀");
  assert.equal(userInitials({ ...USER, firstName: "", lastName: "", username: "", email: "" }), "?");
  const corpus = [
    EMOJI_FIRST_NAME, FLAG_LAST_NAME, HEBREW_FIRST_NAME, INVISIBLE_LEAD_NAME, RTL_ORGANIZATION, CJK_ORGANIZATION,
    "𝓐lex", "Émilie", "\uD83D", "\uDE00", "😀😀", "́", "ß", LONG_WORD_ORGANIZATION,
  ];
  for (const first of corpus) {
    for (const last of corpus) {
      const initials = userInitials({ ...USER, firstName: first, lastName: last });
      assert.equal(isWellFormed(initials) || !isWellFormed(first + last), true, `initials of ${JSON.stringify([first, last])} are well formed`);
      assert.equal(initials.length > 0, true);
    }
  }
});

test("a display name made only of invisible characters is no name", () => {
  assert.equal(userDisplayName({ ...USER, firstName: "‏", lastName: "​" }), "mark");
  assert.equal(userDisplayName({ ...USER, firstName: "‏", lastName: "", username: "﻿ " }), "mark@open-splunk.test");
  assert.equal(userDisplayName({ ...USER, firstName: INVISIBLE_LEAD_NAME, lastName: "" }), INVISIBLE_LEAD_NAME, "a visible name keeps its own characters");
  assert.equal(userDisplayName({ ...USER, firstName: EMOJI_FIRST_NAME, lastName: FLAG_LAST_NAME }), `${EMOJI_FIRST_NAME} ${FLAG_LAST_NAME}`);
  assert.equal(userDisplayName({ ...USER, firstName: "", lastName: "", username: "", email: LONG_EMAIL }), LONG_EMAIL, "an address is never shortened");
});

test("an organization's initial is its name's first character, then its slug's, then a question mark", () => {
  assert.equal(organizationInitial(organization("1", "🦊 Fox Labs")), "🦊");
  assert.equal(organizationInitial(organization("1", RTL_ORGANIZATION)), "ش");
  assert.equal(organizationInitial(organization("1", "​", "acme")), "A");
  assert.equal(organizationInitial(organization("1", "", "​")), "?");
  assert.equal(organizationInitial(organization("1", MARKUP_ORGANIZATION)), "<");
});

test("the switcher's rows keep every name whole and give every row a well-formed initial", () => {
  const names = [RTL_ORGANIZATION, LONG_WORD_ORGANIZATION, MARKUP_ORGANIZATION, CJK_ORGANIZATION, "🇳🇿 Aotearoa Ops"];
  const organizations = names.map((name, index) => organization(String(index + 1), name));
  const principal: Principal = {
    user: USER,
    siteAdmin: false,
    activeOrganization: organizations[0] ?? null,
    role: "owner",
    capabilities: ["search"],
    organizations: organizations.map((entry) => ({ organization: entry, explicitRole: "owner", effectiveRole: "owner" })),
  };
  const choices = organizationChoices(principal);
  assert.deepEqual(choices.map((choice) => choice.name), names);
  assert.deepEqual(choices.map((choice) => choice.initial), ["ش", "I", "<", "株", "🇳🇿"]);
  assert.equal(choices.every((choice) => isWellFormed(choice.initial)), true);
});

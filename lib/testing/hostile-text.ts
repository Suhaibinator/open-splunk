/**
 * Text the product never writes but has to print: names, addresses and
 * descriptions as people actually type them, in any script and at any
 * length. Shared by the unit and mount tests that hold the identity surfaces
 * to it, so every surface is broken the same way.
 *
 * Test-only: no page or library module imports this file.
 */

/** Arabic, with Arabic-Indic digits and trailing punctuation a bare LTR context moves to the wrong end. */
export const RTL_ORGANIZATION = "شركة الأمن السيبراني ٢٠٢٦!";

/** One word of 225 characters: nothing in it is a soft wrap opportunity. */
export const LONG_WORD_ORGANIZATION = "Internationalisierungsverwaltungsgesellschaft".repeat(5);

/** Markup an unescaped renderer would turn into an element. */
export const MARKUP_ORGANIZATION = "<img src=x onerror=alert(1)> & Söhne";

/** CJK, where every character is a break opportunity and none is a space. */
export const CJK_ORGANIZATION = "株式会社データ分析基盤チーム";

/** A woman technologist with a skin tone: four code points, one character. */
export const EMOJI_FIRST_NAME = "👩🏽‍💻Zoë";

/** A flag: two regional indicators, one character. */
export const FLAG_LAST_NAME = "🇳🇿Ngata";

/** Hebrew given and family names. */
export const HEBREW_FIRST_NAME = "דוד";
export const HEBREW_LAST_NAME = "לוי";

/** An address at the length the mail RFCs allow, with no break opportunity but its dots. */
export const LONG_EMAIL = `${"firstname.lastname.".repeat(3)}x@${"subdomain.".repeat(12)}example.test`;

/** A description mixing both directions, long enough to need a cap on a desktop table. */
export const MIXED_DESCRIPTION = `Regional SOC for ${RTL_ORGANIZATION} and partners — `.repeat(6).trim();

/** A name that opens with a right-to-left mark, a zero-width space and a byte-order mark. */
export const INVISIBLE_LEAD_NAME = "‏​﻿محمد";

/** Whether `text` holds no unpaired surrogate: the shape `charAt(0)` leaves behind on an emoji. */
export function isWellFormed(text: string): boolean {
  for (let index = 0; index < text.length; index += 1) {
    const unit = text.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = text.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) return false;
  }
  return true;
}

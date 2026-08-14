import { SharingScope } from "@/gen/ts/open_splunk/v1/common";
import {
  KnowledgeOverwriteBehavior,
  KnowledgeSelectorMatchKind,
  type KnowledgeSelectorPattern,
} from "@/gen/ts/open_splunk/v1/knowledge";
import type { LookupDefinition, LookupFieldMapping } from "@/gen/ts/open_splunk/v1/lookup";

export const LOOKUP_MAXIMUM_NAME_BYTES = 255;
export const LOOKUP_MAXIMUM_DESCRIPTION_BYTES = 16 << 10;
export const LOOKUP_MAXIMUM_AUTHORED_SOURCE_BYTES = 16 << 10;
const LOOKUP_MAXIMUM_EVENT_FIELD_BYTES = 8_720;
const LOOKUP_MAXIMUM_EVENT_FIELD_SEGMENTS = 17;
const LOOKUP_MAXIMUM_EVENT_FIELD_SEGMENT_BYTES = 256;
const LOOKUP_MAXIMUM_SELECTOR_PATTERN_BYTES = 255;
const LOOKUP_MAXIMUM_SELECTOR_NORMALIZED_BYTES = 8 << 10;
const LOOKUP_MAXIMUM_SELECTOR_WORK_UNITS = 1 << 10;
const CANONICAL_SELECTOR_DOMAIN = "open-splunk/knowledge-selector/v1\0";

export function textBytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

export function hasUnpairedSurrogate(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return true;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function isExactLookupToken(value: string): boolean {
  if (
    value.length === 0
    // These are precisely the word boundaries and forbidden quoting forms in
    // spl.IsExactUnquotedFieldName. Question marks, square brackets, and
    // braces are ordinary exact-token bytes rather than wildcards here.
    || value === "."
    || /\p{White_Space}/u.test(value)
    || /['"`*|(),=!<>]/u.test(value)
    || hasUnpairedSurrogate(value)
  ) return false;
  return ![...value].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0;
    return codePoint <= 0x1f
      || (codePoint >= 0x7f && codePoint <= 0x9f)
      || /\p{Cf}/u.test(character);
  });
}

/** Exact CSV schema token; unlike an event field, the column `fields` is valid. */
export function isExactLookupColumn(value: string): boolean {
  return isExactLookupToken(value) && !value.toLowerCase().startsWith("__os_");
}

export function isExactPublicField(value: string): boolean {
  return isExactLookupColumn(value) && value.toLowerCase() !== "fields";
}

/** Mirrors the canonical event path accepted by plan.ResolveField. */
export function isExactEventField(value: string): boolean {
  if (!isExactPublicField(value) || textBytes(value) > LOOKUP_MAXIMUM_EVENT_FIELD_BYTES) return false;
  let segment = "";
  let segmentCount = 0;
  let escaped = false;
  const finishSegment = (): boolean => {
    if (segment.length === 0 || textBytes(segment) > LOOKUP_MAXIMUM_EVENT_FIELD_SEGMENT_BYTES) return false;
    segmentCount += 1;
    segment = "";
    return segmentCount <= LOOKUP_MAXIMUM_EVENT_FIELD_SEGMENTS;
  };
  for (const character of value) {
    if (escaped) {
      if (character !== "." && character !== "\\") return false;
      segment += character;
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === ".") {
      if (!finishSegment()) return false;
    } else {
      segment += character;
    }
  }
  return !escaped && finishSegment();
}

export function isLookupOutputMarker(value: string): boolean {
  const folded = value.toUpperCase();
  return folded === "OUTPUT" || folded === "OUTPUTNEW";
}

interface CanonicalSelectorPattern {
  readonly value: string;
  readonly kind: KnowledgeSelectorMatchKind;
  readonly workUnits: number;
}

function normalizeSelectorPattern(value: string): CanonicalSelectorPattern | undefined {
  if (
    value.length === 0
    || value.replace(/^[\t-\r ]+|[\t-\r ]+$/gu, "") !== value
    || textBytes(value) > LOOKUP_MAXIMUM_SELECTOR_PATTERN_BYTES
    || hasUnpairedSurrogate(value)
  ) return undefined;
  let canonical = "";
  let escaped = false;
  let wildcard = false;
  let previousMany = false;
  let workUnits = 0;
  for (const character of value) {
    if (escaped) {
      if (character !== "*" && character !== "?" && character !== "\\") return undefined;
      canonical += `\\${character}`;
      escaped = false;
      previousMany = false;
      workUnits += 1;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    const codePoint = character.codePointAt(0) ?? 0;
    if (codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)) return undefined;
    if (character === "*") {
      wildcard = true;
      if (!previousMany) {
        canonical += character;
        workUnits += 4;
      }
      previousMany = true;
    } else if (character === "?") {
      wildcard = true;
      canonical += character;
      previousMany = false;
      workUnits += 2;
    } else {
      canonical += character;
      previousMany = false;
      workUnits += 1;
    }
  }
  if (escaped || canonical !== value || textBytes(canonical) > LOOKUP_MAXIMUM_SELECTOR_PATTERN_BYTES) {
    return undefined;
  }
  return {
    value: canonical,
    kind: wildcard
      ? KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
      : KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
    workUnits,
  };
}

export function selectorPatternKind(value: string): KnowledgeSelectorMatchKind {
  return normalizeSelectorPattern(value)?.kind
    ?? KnowledgeSelectorMatchKind.KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED;
}

function compareUTF8(left: string, right: string): number {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    const comparison = leftBytes[index]! - rightBytes[index]!;
    if (comparison !== 0) return comparison;
  }
  return leftBytes.length - rightBytes.length;
}

function validCanonicalSelector(definition: LookupDefinition): boolean {
  const selector = definition.selector;
  if (selector === undefined) return true;
  const dimensions = [
    selector.indexPatterns,
    selector.hostPatterns,
    selector.sourcePatterns,
    selector.sourcetypePatterns,
  ];
  let patternCount = 0;
  let normalizedBytes = textBytes(CANONICAL_SELECTOR_DOMAIN) + dimensions.length * 3;
  let workUnits = 0;
  for (const patterns of dimensions) {
    if (patterns.length > 16) return false;
    let previous: string | undefined;
    for (const pattern of patterns) {
      const normalized = validSelectorPattern(pattern);
      if (normalized === undefined || (previous !== undefined && compareUTF8(previous, normalized.value) >= 0)) {
        return false;
      }
      previous = normalized.value;
      patternCount += 1;
      normalizedBytes += 4 + textBytes(normalized.value);
      workUnits += normalized.workUnits;
    }
  }
  return patternCount >= 1
    && patternCount <= 64
    && normalizedBytes <= LOOKUP_MAXIMUM_SELECTOR_NORMALIZED_BYTES
    && workUnits <= LOOKUP_MAXIMUM_SELECTOR_WORK_UNITS;
}

function validSelectorPattern(pattern: KnowledgeSelectorPattern): CanonicalSelectorPattern | undefined {
  const normalized = normalizeSelectorPattern(pattern.value);
  return normalized !== undefined && pattern.matchKind === normalized.kind ? normalized : undefined;
}

function validMappings(
  mappings: readonly LookupFieldMapping[],
  columns: ReadonlySet<string>,
  keyMappings: boolean,
): boolean {
  const lookupFields = new Set<string>();
  const eventFields = new Set<string>();
  for (const mapping of mappings) {
    if (
      !isExactLookupColumn(mapping.lookupField)
      || !columns.has(mapping.lookupField)
      || !isExactEventField(mapping.eventField)
      || (keyMappings && (isLookupOutputMarker(mapping.lookupField) || isLookupOutputMarker(mapping.eventField)))
      || lookupFields.has(mapping.lookupField)
      || eventFields.has(mapping.eventField)
    ) return false;
    lookupFields.add(mapping.lookupField);
    eventFields.add(mapping.eventField);
  }
  return true;
}

function isManagementIdentity(value: string, maximumBytes: number): boolean {
  return value.length > 0
    && textBytes(value) <= maximumBytes
    && /^[A-Za-z0-9](?:[A-Za-z0-9._:-]*)$/u.test(value);
}

function validDescription(value: string | undefined): boolean {
  return value === undefined
    || (textBytes(value) <= LOOKUP_MAXIMUM_DESCRIPTION_BYTES
      && !hasUnpairedSurrogate(value)
      && !/[\p{Cc}\p{Cf}]/u.test(value));
}

/**
 * Returns the byte length of the stable explicit spelling used to prove that
 * a persisted definition can be authored through the public SPL grammar.
 */
export function canonicalLookupSourceBytes(definition: LookupDefinition): number {
  let bytes = textBytes("* | lookup ") + textBytes(definition.name);
  for (const mapping of definition.keyMappings) {
    bytes += 1 + textBytes(mapping.lookupField) + textBytes(" AS ") + textBytes(mapping.eventField);
  }
  bytes += definition.overwriteBehavior
    === KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING
    ? textBytes(" OUTPUTNEW")
    : textBytes(" OUTPUT");
  // Always spelling AS avoids optional-output-alias ambiguity when a lookup
  // column itself is named AS, and remains accepted when both names match.
  for (const mapping of definition.outputMappings) {
    bytes += 1 + textBytes(mapping.lookupField) + textBytes(" AS ") + textBytes(mapping.eventField);
  }
  return bytes;
}

export function isCanonicallyAuthorableLookupDefinition(definition: LookupDefinition): boolean {
  return canonicalLookupSourceBytes(definition) <= LOOKUP_MAXIMUM_AUTHORED_SOURCE_BYTES;
}

/** Validates a detached server projection before any nested value reaches UI state. */
export function isBoundedCanonicalLookupDefinition(
  definition: LookupDefinition,
  responseColumns: readonly string[],
): boolean {
  const columns = new Set(responseColumns);
  return isManagementIdentity(definition.appId, 128)
    && isExactPublicField(definition.name)
    && textBytes(definition.name) <= LOOKUP_MAXIMUM_NAME_BYTES
    && validDescription(definition.description)
    && (definition.sharingScope === SharingScope.SHARING_SCOPE_PRIVATE
      || definition.sharingScope === SharingScope.SHARING_SCOPE_APP
      || definition.sharingScope === SharingScope.SHARING_SCOPE_GLOBAL)
    && typeof definition.automatic === "boolean"
    && definition.keyMappings.length >= 1
    && definition.keyMappings.length <= 4
    && definition.outputMappings.length >= 1
    && definition.outputMappings.length <= 16
    && validMappings(definition.keyMappings, columns, true)
    && validMappings(definition.outputMappings, columns, false)
    && (definition.overwriteBehavior
      === KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING
      || definition.overwriteBehavior
        === KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING)
    && validCanonicalSelector(definition)
    && isCanonicallyAuthorableLookupDefinition(definition);
}

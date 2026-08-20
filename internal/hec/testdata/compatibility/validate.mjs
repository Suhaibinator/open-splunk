#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import {fileURLToPath} from "node:url";

const directory = path.dirname(fileURLToPath(import.meta.url));
const schema = readJSON(path.join(directory, "fixture.schema.json"));
const corpusFiles = fs.readdirSync(directory)
  .filter((name) => name.endsWith(".json") && name !== "fixture.schema.json")
  .toSorted();

const errors = [];
const ids = new Set();
const responseCodes = new Set();

for (const name of corpusFiles) {
  const document = readJSON(path.join(directory, name));
  validate(schema, document, `${name}#`);
  for (const [index, fixture] of document.cases.entries()) {
    const location = `${name}#/cases/${index}`;
    if (ids.has(fixture.id)) {
      errors.push(`${location}/id: duplicate fixture ID ${JSON.stringify(fixture.id)}`);
    }
    ids.add(fixture.id);

    let response;
    try {
      response = JSON.parse(fixture.expect.http.body_utf8);
    } catch (error) {
      errors.push(`${location}/expect/http/body_utf8: invalid response JSON: ${error.message}`);
      continue;
    }
    if (Number.isInteger(response.code)) {
      responseCodes.add(response.code);
    }

    for (const header of fixture.request.headers) {
      if (header.name.toLowerCase() !== "authorization") continue;
      if (!/^Splunk \{\{token:[a-z][a-z0-9_-]{0,31}\}\}$/.test(header.value) &&
          !/^splunk \{\{token:[a-z][a-z0-9_-]{0,31}\}\}$/.test(header.value) &&
          !/^Splunk  \{\{token:[a-z][a-z0-9_-]{0,31}\}\}$/.test(header.value)) {
        errors.push(`${location}/request/headers: authorization fixture must use a symbolic token placeholder`);
      }
    }

    const durable = fixture.expect.durable;
    if (fixture.expect.events.length > 0 && durable.request !== "staged") {
      errors.push(`${location}/expect: event projections require a staged request`);
    }
    if (durable.request === "staged" && fixture.expect.events.length === 0) {
      errors.push(`${location}/expect: a staged ingestion fixture must project at least one event`);
    }
    if (durable.request === "staged" && !fixture.expect.spl) {
      errors.push(`${location}/expect: a staged ingestion fixture must declare an SPL projection`);
    }

    const maximumAckID = 9007199254740991n;
    for (const allocation of fixture.setup.ack_allocations ?? []) {
      if (BigInt(allocation.id) > maximumAckID) {
        errors.push(`${location}/setup/ack_allocations: ID exceeds exact JSON integer maximum`);
      }
    }
    for (const row of fixture.setup.ack_rows ?? []) {
      if (BigInt(row.id) > maximumAckID) {
        errors.push(`${location}/setup/ack_rows: acknowledgment ID exceeds exact JSON integer maximum`);
      }
    }
    for (const [eventIndex, event] of fixture.expect.events.entries()) {
      for (const [fieldIndex, field] of event.fields.entries()) {
        validateTypedValue(field.value, `${location}/expect/events/${eventIndex}/fields/${fieldIndex}/value`);
      }
    }
  }
}

for (const code of [...Array(24).keys(), 26, 27]) {
  if (!responseCodes.has(code)) {
    errors.push(`corpus: emitted response code ${code} has no fixture`);
  }
}
for (const reserved of [24, 25]) {
  if (responseCodes.has(reserved)) {
    errors.push(`corpus: reserved response code ${reserved} must not be emitted`);
  }
}

if (errors.length > 0) {
  for (const error of errors) console.error(error);
  process.exitCode = 1;
} else {
  console.log(`validated ${ids.size} HEC compatibility fixtures in ${corpusFiles.length} corpora`);
}

function readJSON(filename) {
  try {
    return JSON.parse(fs.readFileSync(filename, "utf8"));
  } catch (error) {
    console.error(`${path.basename(filename)}: ${error.message}`);
    process.exit(1);
  }
}

function validate(rule, value, location) {
  if (rule.$ref) {
    validate(resolveReference(rule.$ref), value, location);
    return;
  }
  if (rule.oneOf) {
    const branchResults = rule.oneOf.map((branch) => capture(() => validate(branch, value, location)));
    const matches = branchResults.filter((result) => result.length === 0);
    if (matches.length !== 1) {
      errors.push(`${location}: expected exactly one schema branch, matched ${matches.length}`);
    }
    return;
  }
  if (Object.hasOwn(rule, "const") && !deepEqual(value, rule.const)) {
    errors.push(`${location}: value does not equal const ${JSON.stringify(rule.const)}`);
    return;
  }
  if (rule.enum && !rule.enum.some((candidate) => deepEqual(candidate, value))) {
    errors.push(`${location}: value is outside enum ${JSON.stringify(rule.enum)}`);
    return;
  }
  if (rule.type && !hasType(value, rule.type)) {
    errors.push(`${location}: expected ${rule.type}, got ${jsonType(value)}`);
    return;
  }

  if (typeof value === "string") {
    if (rule.minLength !== undefined && [...value].length < rule.minLength) {
      errors.push(`${location}: string is shorter than ${rule.minLength}`);
    }
    if (rule.maxLength !== undefined && [...value].length > rule.maxLength) {
      errors.push(`${location}: string is longer than ${rule.maxLength}`);
    }
    if (rule.pattern && !(new RegExp(rule.pattern, "u")).test(value)) {
      errors.push(`${location}: string does not match ${rule.pattern}`);
    }
    if (rule.format === "date-time" && !validDateTime(value)) {
      errors.push(`${location}: invalid RFC 3339 date-time`);
    }
    if (rule.contentEncoding === "base64" && !validBase64(value)) {
      errors.push(`${location}: invalid canonical base64`);
    }
  }

  if (typeof value === "number") {
    if (rule.minimum !== undefined && value < rule.minimum) {
      errors.push(`${location}: number is below ${rule.minimum}`);
    }
    if (rule.maximum !== undefined && value > rule.maximum) {
      errors.push(`${location}: number is above ${rule.maximum}`);
    }
  }

  if (Array.isArray(value)) {
    if (rule.minItems !== undefined && value.length < rule.minItems) {
      errors.push(`${location}: array has fewer than ${rule.minItems} items`);
    }
    if (rule.maxItems !== undefined && value.length > rule.maxItems) {
      errors.push(`${location}: array has more than ${rule.maxItems} items`);
    }
    if (rule.uniqueItems) {
      const seen = new Set();
      for (const item of value) {
        const key = JSON.stringify(item);
        if (seen.has(key)) errors.push(`${location}: array items are not unique`);
        seen.add(key);
      }
    }
    if (rule.items) value.forEach((item, index) => validate(rule.items, item, `${location}/${index}`));
  }

  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    for (const required of rule.required ?? []) {
      if (!Object.hasOwn(value, required)) errors.push(`${location}: missing required property ${required}`);
    }
    for (const [key, item] of Object.entries(value)) {
      if (rule.propertyNames) validate(rule.propertyNames, key, `${location}/<property-name>`);
      if (rule.properties && Object.hasOwn(rule.properties, key)) {
        validate(rule.properties[key], item, `${location}/${escapePointer(key)}`);
      } else if (rule.additionalProperties === false) {
        errors.push(`${location}: unexpected property ${key}`);
      } else if (rule.additionalProperties && typeof rule.additionalProperties === "object") {
        validate(rule.additionalProperties, item, `${location}/${escapePointer(key)}`);
      }
    }
  }
}

function resolveReference(reference) {
  if (!reference.startsWith("#/")) throw new Error(`unsupported nonlocal schema reference ${reference}`);
  return reference.slice(2).split("/").reduce((value, part) => value[part.replaceAll("~1", "/").replaceAll("~0", "~")], schema);
}

function capture(action) {
  const offset = errors.length;
  action();
  return errors.splice(offset);
}

function hasType(value, type) {
  switch (type) {
    case "object": return value !== null && typeof value === "object" && !Array.isArray(value);
    case "array": return Array.isArray(value);
    case "string": return typeof value === "string";
    case "integer": return Number.isInteger(value);
    case "number": return typeof value === "number" && Number.isFinite(value);
    case "boolean": return typeof value === "boolean";
    case "null": return value === null;
    default: throw new Error(`unsupported schema type ${type}`);
  }
}

function jsonType(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (Number.isInteger(value)) return "integer";
  return typeof value;
}

function validDateTime(value) {
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value));
}

function validBase64(value) {
  if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) return false;
  return Buffer.from(value, "base64").toString("base64") === value;
}

function validateTypedValue(value, location) {
  const signedMinimum = -9223372036854775808n;
  const signedMaximum = 9223372036854775807n;
  const unsignedMaximum = 18446744073709551615n;
  if (value.kind === "sint64") {
    const integer = BigInt(value.value);
    if (integer < signedMinimum || integer > signedMaximum) {
      errors.push(`${location}: signed integer exceeds 64-bit range`);
    }
  } else if (value.kind === "uint64") {
    if (BigInt(value.value) > unsignedMaximum) {
      errors.push(`${location}: unsigned integer exceeds 64-bit range`);
    }
  } else if (value.kind === "decimal") {
    const match = /[eE]([+-]?[0-9]+)$/.exec(value.value);
    if (match && (BigInt(match[1]) < -1024n || BigInt(match[1]) > 1024n)) {
      errors.push(`${location}: decimal exponent exceeds the supported range`);
    }
  } else if (value.kind === "list") {
    value.items.forEach((item, index) => validateTypedValue(item, `${location}/items/${index}`));
  }
}

function deepEqual(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function escapePointer(value) {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

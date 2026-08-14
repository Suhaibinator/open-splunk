/* eslint-disable unicorn/consistent-function-scoping */
// Parser-local predicates keep the closed JSON grammar next to its single state machine.
// JSON.parse deliberately accepts duplicate object keys. Evidence manifests
// need a stricter contract because a reviewer and a runtime must never bind
// different values to the same apparent field.
export function parseStrictJSON(text, label = "JSON") {
  let offset = 0;

  function isWhitespace(character) {
    return character === "\t" || character === "\n" ||
      character === "\r" || character === " ";
  }

  function isPrimitiveDelimiter(character) {
    return isWhitespace(character) || character === "," ||
      character === "]" || character === "}";
  }

  function syntax(message) {
    throw new SyntaxError(`${label}: ${message} at byte ${offset}`);
  }

  function skipWhitespace() {
    while (offset < text.length && isWhitespace(text[offset])) {
      offset++;
    }
  }

  function parseStringToken() {
    if (text[offset] !== '"') syntax("expected a string");
    const start = offset++;
    while (offset < text.length) {
      const character = text[offset++];
      if (character === '"') {
        return JSON.parse(text.slice(start, offset));
      }
      if (character === "\\") {
        if (offset >= text.length) syntax("unterminated string escape");
        offset++;
      } else if (character.charCodeAt(0) < 0x20) {
        syntax("unescaped control character in string");
      }
    }
    syntax("unterminated string");
  }

  function parsePrimitive() {
    const start = offset;
    while (offset < text.length && !isPrimitiveDelimiter(text[offset])) {
      offset++;
    }
    if (offset === start) syntax("expected a JSON value");
  }

  function parseArray() {
    offset++;
    skipWhitespace();
    if (text[offset] === "]") {
      offset++;
      return;
    }
    for (;;) {
      parseValue();
      skipWhitespace();
      if (text[offset] === "]") {
        offset++;
        return;
      }
      if (text[offset] !== ",") syntax("expected ',' or ']' in array");
      offset++;
      skipWhitespace();
    }
  }

  function parseObject() {
    offset++;
    const keys = new Set();
    skipWhitespace();
    if (text[offset] === "}") {
      offset++;
      return;
    }
    for (;;) {
      const key = parseStringToken();
      if (keys.has(key)) {
        throw new SyntaxError(`${label}: duplicate JSON object key ${JSON.stringify(key)}`);
      }
      keys.add(key);
      skipWhitespace();
      if (text[offset] !== ":") syntax("expected ':' after object key");
      offset++;
      parseValue();
      skipWhitespace();
      if (text[offset] === "}") {
        offset++;
        return;
      }
      if (text[offset] !== ",") syntax("expected ',' or '}' in object");
      offset++;
      skipWhitespace();
    }
  }

  function parseValue() {
    skipWhitespace();
    if (text[offset] === "{") parseObject();
    else if (text[offset] === "[") parseArray();
    else if (text[offset] === '"') parseStringToken();
    else parsePrimitive();
  }

  parseValue();
  skipWhitespace();
  if (offset !== text.length) syntax("trailing content");
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new SyntaxError(`${label}: ${error.message}`);
  }
}

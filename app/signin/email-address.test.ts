import assert from "node:assert/strict";
import test from "node:test";

import { emailFieldError, isEmailShaped, prefillEmail } from "./email-address";

test("an address needs a local part, an @ and a dotted domain", () => {
  assert.equal(isEmailShaped("ada@open-splunk.test"), true);
  for (const value of ["", "ada", "ada@", "@open-splunk.test", "ada@localhost", "a da@x.test", "a@b@c.test"]) {
    assert.equal(isEmailShaped(value), false, JSON.stringify(value));
  }
  assert.equal(isEmailShaped(`${"a".repeat(250)}@x.test`), false, "longer than a forward path may be");
});

test("the field error says empty and malformed apart", () => {
  assert.equal(emailFieldError(""), "Enter your email address.");
  assert.equal(emailFieldError("ada"), "Enter a valid email address, like name@example.com.");
  assert.equal(emailFieldError("ada@open-splunk.test"), null);
});

test("a query string prefills only an email address", () => {
  assert.equal(prefillEmail(" nora@open-splunk.test "), "nora@open-splunk.test");
  assert.equal(prefillEmail(null), "");
  assert.equal(prefillEmail(undefined), "");
  assert.equal(prefillEmail("Your account is locked, call 555-0100"), "");
});

import assert from "node:assert/strict";
import test from "node:test";
import { createElement, Fragment, isValidElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { SPL_PIPELINE_COMMANDS } from "@/lib/search/spl-syntax";
import { SearchJobState } from "@/gen/ts/open_splunk/search";

import {
  backendJobPhase,
  demoTimechartSplitField,
  eventCountForQuery,
  eventFieldValueWhiteSpace,
  filteredDemoEvents,
  historyPhase,
  stateTone,
  syntaxTokens,
} from "./workspace-utils";

test("demo split-series detection is bounded to unquoted timechart by clauses", () => {
  assert.equal(
    demoTimechartSplitField("index=main | timechart span=5m count by service"),
    "service",
  );
  assert.equal(
    demoTimechartSplitField('index=main | timechart count eval(note="by decoy") BY host'),
    "host",
  );
  assert.equal(demoTimechartSplitField('index=main | timechart count eval(note="by service")'), null);
  assert.equal(demoTimechartSplitField("index=main | stats count by service"), null);
});

interface SyntaxTokenProps {
  children?: ReactNode;
  className?: string;
}

test("event field presentation preserves adapted nomv newlines", () => {
  assert.equal(eventFieldValueWhiteSpace("alpha"), "nowrap");
  assert.equal(eventFieldValueWhiteSpace(7), "nowrap");
  assert.equal(eventFieldValueWhiteSpace("alpha\nbeta"), "pre-wrap");
  assert.equal(eventFieldValueWhiteSpace("alpha\rbeta"), "pre-wrap");
});

test("numeric head limits clamp demo rows and result counts", () => {
  assert.equal(eventCountForQuery("index=gradethis | head 3"), 3);
  assert.equal(eventCountForQuery("index=gradethis | HEAD 0"), 0);
  assert.equal(eventCountForQuery("index=gradethis | head 100 | HEAD 3"), 3);
  assert.equal(filteredDemoEvents("index=gradethis | head 3").length, 3);
  assert.equal(eventCountForQuery("index=gradethis | head invalid"), 12_846);
});

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

test("case highlights only when used as a parenthesized function", () => {
  const query = `index=main case=1 | eval selected=CASE(status=200, "ok", 1=1, "other") | table case`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["case"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("lower and upper highlight only when used as parenthesized functions", () => {
  const query = `index=main lower=1 upper=2 | eval normalized=LOWER(source), shouted=upper(normalized) | table lower,upper`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["lower", "upper"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("len and length highlight only when used as parenthesized functions", () => {
  const query = `index=main len=1 length=2 | eval short=LEN(source), long=length(message) | table len,length`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["len", "length"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("substr highlights only when used as a parenthesized function", () => {
  const query = `index=main substr=1 | eval part=SuBsTr(source, -3, 2) | table substr`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["substr"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("tostring highlights only when used as a parenthesized function", () => {
  const query = `index=main tostring=1 | eval rendered=ToStRiNg(status) | table tostring`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["tostring"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("round highlights only when used as a parenthesized function", () => {
  const query = `index=main round=1 | eval rendered=RoUnD(duration_ms, 2) | table round`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["round"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("ceil, ceiling, and floor highlight only as parenthesized functions", () => {
  const query = `index=main ceil=1 ceiling=2 floor=3 | eval up=CeIl(ratio), alias=CEILING(ratio), down=FlOoR(ratio) | table ceil,ceiling,floor`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["ceil", "ceiling", "floor"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("mvcount highlights only when used as a parenthesized function", () => {
  const query = `index=main mvcount=1 | eval tally=MvCoUnT(recipients) | table mvcount`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["mvcount"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("mvsort highlights only when used as a parenthesized function", () => {
  const query = `index=main mvsort=1 | eval sorted=MvSoRt(recipients) | table mvsort`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["mvsort"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("match highlights only when used as a parenthesized function", () => {
  const query = `index=main match=1 | where MaTcH(message, "(?i)error") | table match`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["match"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("like highlights only when used as a parenthesized function", () => {
  const query = `index=main like=1 | where LiKe(message, "%error%") | table like`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["like"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("now highlights only when used as a parenthesized function", () => {
  const query = `index=main now=1 | eval started=NoW() | table now,started`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["now"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("relative_time highlights only when used as a parenthesized function", () => {
  const query = `index=main relative_time=1 | eval shifted=ReLaTiVe_TiMe(_time, "-1d@d+2h") | table relative_time`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["relative_time"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("strftime highlights only when used as a parenthesized function", () => {
  const query = `index=main strftime=1 | eval rendered=StRfTiMe(_time, "%F %T.%Q") | table strftime`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["strftime"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("strptime highlights only when used as a parenthesized function", () => {
  const query = `index=main strptime=1 | eval epoch=StRpTiMe(timestamp, "%F %T.%6N") | table strptime`;
  const tokens = classifiedTokens(query);
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-function")
      .map((token) => token.text.toLowerCase()),
    ["strptime"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("eval completion advertises the exact supported scalar signatures", () => {
  const evalCompletion = SPL_PIPELINE_COMMANDS.find((command) => command.name === "eval");
  assert.ok(evalCompletion);
  assert.equal(evalCompletion.insertion, 'eval availability=if(isnull(status), "missing", "present")');
  assert.match(evalCompletion.detail, /if\(predicate, true_value, false_value\)/);
  assert.match(evalCompletion.detail, /coalesce\(value, fallback, \.\.\.\)/);
  assert.match(evalCompletion.detail, /case\(predicate, value, \.\.\.\)/);
  assert.match(evalCompletion.detail, /lower\(value\)/);
  assert.match(evalCompletion.detail, /upper\(value\)/);
  assert.match(evalCompletion.detail, /len\(value\)\/length\(value\)/);
  assert.match(evalCompletion.detail, /substr\(value, start\[, length\]\)/);
  assert.match(evalCompletion.detail, /tostring\(value\)/);
  assert.match(evalCompletion.detail, /round\(value\[, precision\]\)/);
  assert.match(evalCompletion.detail, /ceil\(value\)\/ceiling\(value\)/);
  assert.match(evalCompletion.detail, /floor\(value\)/);
  assert.match(evalCompletion.detail, /mvcount\(value\)/);
  assert.match(evalCompletion.detail, /single value as 1/i);
  assert.match(evalCompletion.detail, /no values as null/i);
  assert.match(evalCompletion.detail, /mvsort\(multivalue_field\)/);
  assert.match(evalCompletion.detail, /ascending encoded order/i);
  assert.match(evalCompletion.detail, /match\(value, "regex"\)/);
  assert.match(evalCompletion.detail, /4 KiB literal RE2 pattern/i);
  assert.match(evalCompletion.detail, /like\(value, "pattern"\)/);
  assert.match(evalCompletion.detail, /4 KiB literal wildcard pattern/i);
  assert.match(evalCompletion.detail, /%.*zero or more.*_.*one Unicode code point/i);
  assert.match(evalCompletion.detail, /now\(\)/);
  assert.match(evalCompletion.detail, /fixed search-start Unix second/i);
  assert.match(evalCompletion.detail, /relative_time\(time, "-1d@d\+2h"\)/);
  assert.match(evalCompletion.detail, /optional signed offset.*optional snap.*optional signed post-snap offset/i);
  assert.match(evalCompletion.detail, /nullable Unix seconds.*1 KiB literal specifier/i);
  assert.match(evalCompletion.detail, /strftime\(time, "%Y-%m-%dT%H:%M:%S\.%Q"\)/);
  assert.match(evalCompletion.detail, /effective IANA search timezone/i);
  assert.match(evalCompletion.detail, /strptime\(text, "%Y-%m-%dT%H:%M:%S\.%6N"\)/);
  assert.match(evalCompletion.detail, /full date.*microsecond Unix seconds/i);
  assert.match(evalCompletion.detail, /4 KiB literal format.*4 KiB String input/i);
  assert.match(evalCompletion.detail, /literal precision from 0 through 18/i);
  assert.match(evalCompletion.detail, /first non-null fixed value/i);
  assert.match(evalCompletion.detail, /first true predicate/i);
  assert.match(evalCompletion.detail, /Unicode string or multivalue/i);
  assert.match(evalCompletion.detail, /UTF-8 code points/i);
  assert.match(evalCompletion.detail, /SQLite indexing/i);
  assert.match(evalCompletion.detail, /capitalized Boolean/i);
  assert.match(evalCompletion.detail, /period operator concatenates 2-32 String or number operands/i);
  assert.match(evalCompletion.detail, /full_name=first\." "\.last/);
  assert.match(evalCompletion.detail, /null-propagating.*use tostring\(value\) for Boolean/i);
});

test("stats completion advertises the expanded bounded aggregate surface", () => {
  const statsCompletion = SPL_PIPELINE_COMMANDS.find((command) => command.name === "stats");
  assert.ok(statsCompletion);
  assert.match(statsCompletion.insertion, /sparkline\(avg\(latency\),5m\) AS latency_trend/);
  assert.match(statsCompletion.insertion, /values\(user\) AS users/);
  assert.match(statsCompletion.insertion, /rate\(bytes\) AS byte_rate/);
  assert.match(statsCompletion.detail, /row, predicate, field, distinct-count, percentile, distribution/);
  assert.match(statsCompletion.detail, /values above 100 clamp to 100/);
});

test("eventstats completion advertises bounded values and percentile aggregates", () => {
  const eventstatsCompletion = SPL_PIPELINE_COMMANDS.find((command) => command.name === "eventstats");
  assert.ok(eventstatsCompletion);
  assert.equal(
    eventstatsCompletion.insertion,
    "eventstats values(user) AS users BY service",
  );
  assert.match(eventstatsCompletion.detail, /true-only count\(eval\(predicate\)\)/i);
  assert.match(eventstatsCompletion.detail, /exact distinct count/i);
  assert.match(eventstatsCompletion.detail, /bounded canonical distinct-values list/i);
  assert.match(eventstatsCompletion.detail, /pN\/percN percentile.*1-99/i);
  assert.match(eventstatsCompletion.detail, /numeric sum/i);
  assert.match(eventstatsCompletion.detail, /average/i);
  assert.match(eventstatsCompletion.detail, /exact mixed-type minimum/i);
  assert.match(eventstatsCompletion.detail, /exact mixed-type maximum/i);
  assert.match(eventstatsCompletion.detail, /every input row/i);

  const commandToken = classifiedTokens("index=main | eventstats count")
    .find((token) => token.text.toLowerCase() === "eventstats");
  assert.deepEqual(commandToken, { className: "spl-command", text: "eventstats" });

  const sumToken = classifiedTokens("index=main | eventstats sum(bytes) AS total")
    .find((token) => token.text.toLowerCase() === "sum");
  assert.deepEqual(sumToken, { className: "spl-function", text: "sum" });

  const averageToken = classifiedTokens("index=main | eventstats avg(duration_ms) AS mean_ms")
    .find((token) => token.text.toLowerCase() === "avg");
  assert.deepEqual(averageToken, { className: "spl-function", text: "avg" });

  const minimumToken = classifiedTokens("index=main | eventstats min(duration_ms) AS min_ms")
    .find((token) => token.text.toLowerCase() === "min");
  assert.deepEqual(minimumToken, { className: "spl-function", text: "min" });

  const maximumToken = classifiedTokens("index=main | eventstats max(duration_ms) AS max_ms")
    .find((token) => token.text.toLowerCase() === "max");
  assert.deepEqual(maximumToken, { className: "spl-function", text: "max" });

  const distinctToken = classifiedTokens("index=main | eventstats dc(user) AS unique_users")
    .find((token) => token.text.toLowerCase() === "dc");
  assert.deepEqual(distinctToken, { className: "spl-function", text: "dc" });

  const percentileToken = classifiedTokens("index=main | eventstats p95(duration_ms) AS p95_ms")
    .find((token) => token.text.toLowerCase() === "p95");
  assert.deepEqual(percentileToken, { className: "spl-function", text: "p95" });
});

test("where completion advertises direct bounded match and like predicates", () => {
  const whereCompletion = SPL_PIPELINE_COMMANDS.find((command) => command.name === "where");
  assert.ok(whereCompletion);
  assert.match(whereCompletion.detail, /match\(value, "regex"\)/);
  assert.match(whereCompletion.detail, /substring/i);
  assert.match(whereCompletion.detail, /like\(value, "pattern"\)/);
  assert.match(whereCompletion.detail, /whole-string/i);
  assert.match(whereCompletion.detail, /now\(\)/);
  assert.match(whereCompletion.detail, /search-start/i);
  assert.match(whereCompletion.detail, /relative_time\(time, "specifier"\)/);
  assert.match(whereCompletion.detail, /bounded literal offset-and-snap program/i);
  assert.match(whereCompletion.detail, /nullable Unix seconds/i);
  assert.match(whereCompletion.detail, /strftime\(time, "format"\)/);
  assert.match(whereCompletion.detail, /effective IANA search timezone/i);
  assert.match(whereCompletion.detail, /strptime\(text, "format"\)/);
  assert.match(whereCompletion.detail, /nullable Unix seconds/i);
  assert.match(whereCompletion.detail, /period concatenation.*first\." "\.last = full_name/i);
  assert.match(whereCompletion.detail, /String and number operands.*tostring\(value\) for Boolean/i);
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

test("field quotes and expression operators highlight only in scalar stages", () => {
  const query = `index=main source=/var/log/app-1.log O'Reilly | eval 'request-bytes'=duration_ms+1 | where 'HTTP Status' IN (200, 204) | search literal=1+2`;
  const tokens = classifiedTokens(query);

  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-field" && token.text.startsWith("'"))
      .map((token) => token.text),
    ["'request-bytes'", "'HTTP Status'"],
  );
  assert.deepEqual(
    tokens.filter((token) => token.className === "spl-operator").map((token) => token.text.toUpperCase()),
    ["=", "+", "IN"],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
  assert.equal(
    tokens.some((token) => token.className === "spl-operator" && token.text === "-"),
    false,
  );
});

test("count eval predicates highlight nested fields and operators", () => {
  const query = `index=main | stats count(eval('HTTP Status' IN (500, 503))) AS errors | eventstats count(eval('request-bytes'/2>100)) AS large | streamstats count(eval(status==503)) AS unavailable`;
  const tokens = classifiedTokens(query);

  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-field" && token.text.startsWith("'"))
      .map((token) => token.text),
    ["'HTTP Status'", "'request-bytes'"],
  );
  assert.deepEqual(
    tokens
      .filter((token) => token.className === "spl-operator")
      .map((token) => token.text.toUpperCase()),
    ["IN", "/", ">", "=="],
  );
  assert.equal(tokens.map((token) => token.text).join(""), query);
});

test("a recorded history outcome reads its tone from the one job vocabulary", () => {
  // The Jobs dialog paints the running job and the four cards beside it from
  // the same table. When they were two tables, one of them was migrated to
  // `StatusLabel` and the other kept asking for a class whose rules had been
  // deleted -- rendering four cards with no layout and an invisible swatch,
  // which the CSS-to-markup check cannot see.
  assert.equal(historyPhase("Completed"), "completed");
  assert.equal(historyPhase("Failed"), "failed");
  assert.equal(historyPhase("Canceled"), "canceled");
  assert.equal(historyPhase("Expired"), "expired");
  assert.equal(historyPhase("Interrupted"), "interrupted");

  assert.equal(stateTone(historyPhase("Completed")), "success");
  assert.equal(stateTone(historyPhase("Failed")), "error");
  assert.equal(stateTone(historyPhase("Canceled")), "neutral");
  assert.equal(stateTone(historyPhase("Expired")), "neutral");
  assert.equal(stateTone(historyPhase("Interrupted")), "neutral");
});

test("backend lifecycle mapping treats restart interruption as terminal and rejects unknown states", () => {
  assert.equal(
    backendJobPhase(SearchJobState.SEARCH_JOB_STATE_INTERRUPTED),
    "interrupted",
  );
  assert.throws(
    () => backendJobPhase(SearchJobState.UNRECOGNIZED),
    /unsupported lifecycle state/,
  );
});

test("diagnostic markers underline slices inside tokens without changing the text", () => {
  const query = "index=main | stats count BY host";
  const marked = renderToStaticMarkup(createElement(Fragment, null, ...syntaxTokens(query, [
    { start: 13, end: 18, severity: "error" },
    { start: 15, end: 24, severity: "warning" },
  ])));
  // The error covers all of `stats`; the warning starts inside it and runs
  // on through the space and `count`, so the token splits at every edge.
  assert.match(marked, /<mark class="spl-diagnostic" data-severity="error">st<\/mark><mark class="spl-diagnostic" data-severity="error">ats<\/mark>/u);
  assert.match(marked, /<mark class="spl-diagnostic" data-severity="warning"> <\/mark>/u);
  assert.match(marked, /<mark class="spl-diagnostic" data-severity="warning">count<\/mark>/u);
  assert.doesNotMatch(marked, /<mark[^>]*>BY/u);
  assert.equal(marked.replaceAll(/<[^>]+>/gu, ""), query);
  assert.equal(
    renderToStaticMarkup(createElement(Fragment, null, ...syntaxTokens(query, []))),
    renderToStaticMarkup(createElement(Fragment, null, ...syntaxTokens(query))),
  );
});

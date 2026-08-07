import assert from "node:assert/strict";
import test from "node:test";

import { getQueryDiagnostic } from "./spl-editor";
import {
  isSupportedSplPipelineCommand,
  SPL_PIPELINE_COMMANDS,
  UNSUPPORTED_SPL_PIPELINE_COMMANDS,
} from "./spl-syntax";

test("streamstats is advertised once with the bounded supported syntax", () => {
  const definitions = SPL_PIPELINE_COMMANDS.filter((command) => command.name === "streamstats");
  assert.equal(definitions.length, 1);
  assert.equal(definitions[0]?.insertion, "streamstats count AS running_count");
  assert.match(definitions[0]?.detail ?? "", /bounded running row count/i);
  assert.match(definitions[0]?.detail ?? "", /field occurrence count/i);
  assert.match(definitions[0]?.detail ?? "", /true-only count\(eval\(predicate\)\)/i);
  assert.match(definitions[0]?.detail ?? "", /numeric sum/i);
  assert.match(definitions[0]?.detail ?? "", /numeric average/i);
  assert.match(definitions[0]?.detail ?? "", /exact mixed-type minimum/i);
  assert.match(definitions[0]?.detail ?? "", /exact mixed-type maximum/i);
  assert.match(definitions[0]?.detail ?? "", /chronological earliest\/latest/i);
  assert.match(definitions[0]?.detail ?? "", /deterministic pipeline frames/i);
  assert.match(definitions[0]?.detail ?? "", /immutable event order/i);
  assert.match(definitions[0]?.detail ?? "", /excluding the current row/i);
  assert.match(definitions[0]?.detail ?? "", /exact fields/i);
});

test("frontend support classification accepts streamstats without weakening rejection", () => {
  assert.equal(isSupportedSplPipelineCommand("streamstats"), true);
  assert.equal(isSupportedSplPipelineCommand("StReAmStAtS"), true);
  assert.equal(new Set<string>(UNSUPPORTED_SPL_PIPELINE_COMMANDS).has("streamstats"), false);

  const supported = getQueryDiagnostic(
    "index=main | STREAMSTATS current=f window=3 global=f count AS prior BY service",
  );
  assert.equal(supported, null);
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f count(eval(status>=500)) AS prior_errors BY service",
    ),
    null,
  );
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f sum(bytes) AS prior_bytes BY service",
    ),
    null,
  );
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f avg(bytes) AS prior_mean BY service",
    ),
    null,
  );
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f min(bytes) AS prior_min BY service",
    ),
    null,
  );
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f MAX(bytes) AS prior_max BY service",
    ),
    null,
  );
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f earliest(status) AS prior_first BY service",
    ),
    null,
  );
  assert.equal(
    getQueryDiagnostic(
      "index=main | STREAMSTATS current=f window=3 global=f LATEST(status) AS prior_last BY service",
    ),
    null,
  );

  const unsupported = getQueryDiagnostic("index=main | transaction trace_id");
  assert.equal(unsupported?.kind, "unsupported");
  assert.equal(unsupported?.token, "transaction");
});

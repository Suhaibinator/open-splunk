import assert from "node:assert/strict";
import test from "node:test";
import { ListSearchPatternsResponse, ListSearchPatternMembersResponse } from "@/gen/ts/open_splunk/patterns_api";
import { BackendPatterns } from "./backend-patterns";

test("the selected retained member relation can resize after another summary page is displayed", async () => {
  const requests: Array<{ patternId: string; pageSize?: number }> = [];
  const controller = new BackendPatterns({ searchJobId: "job", snapshotRef: "snapshot", sensitivity: "Balanced" }, {
    patterns: async (request) => {
      const second = request.page?.pageToken === "second-group";
      return ListSearchPatternsResponse.fromPartial({
        patterns: [{ patternId: second ? "group-b" : "group-a", signature: second ? "b" : "a", eventCount: 1n }],
        eligibleEventCount: 2n, excludedEventCount: 0n, retainedEventCount: 2n,
        snapshotRef: "snapshot", algorithmVersion: "1", snapshotComplete: true,
        page: { totalSize: 2n, totalSizeExact: true, nextPageToken: second ? undefined : "second-group" },
      });
    },
    patternMembers: async (request) => {
      requests.push({ patternId: request.patternId, pageSize: request.page?.pageSize });
      return ListSearchPatternMembersResponse.fromPartial({
        patternId: request.patternId,
        resultPage: {
          snapshotRef: "snapshot", snapshotComplete: true,
          schema: { schemaId: "schema", revision: 1n, columns: [{ fieldName: "_raw" }] },
          rows: [{ rowId: "job:0", ordinal: 0n, cells: [{ kind: { $case: "stringValue", value: "a" } }] }],
          page: { totalSize: 1n, totalSizeExact: true },
        },
      });
    },
  }, 1);
  try {
    await controller.loadGroups(1);
    const selected = controller.getSnapshot().rows[0];
    await controller.selectPattern(selected);
    await controller.loadGroups(2);
    assert.equal(controller.getSnapshot().rows[0].patternId, "group-b");
    assert.equal(controller.getSnapshot().members?.pattern.patternId, "group-a");
    // This is the workspace Events page-size callback after returning from Patterns.
    await controller.selectPattern(selected, 2);
    assert.equal(controller.getSnapshot().members?.pageSize, 2);
    assert.equal(controller.getSnapshot().members?.pattern.patternId, "group-a");
    assert.deepEqual(requests, [{ patternId: "group-a", pageSize: 1 }, { patternId: "group-a", pageSize: 2 }]);
  } finally { controller.dispose(); }
});

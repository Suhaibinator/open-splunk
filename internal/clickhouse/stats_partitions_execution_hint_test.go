package clickhouse

import "testing"

func TestCompileStatsPartitionsMaxThreadsHintUsesWholeQueryMinimum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		want    uint8
		present bool
	}{
		{
			name:   "no stats",
			source: `index=gradethis | where status=200`,
		},
		{
			name:   "top internal aggregate is not stats",
			source: `index=gradethis | top host`,
		},
		{
			name:   "rare internal aggregate is not stats",
			source: `index=gradethis | rare host`,
		},
		{
			name:    "default partitions",
			source:  `index=gradethis | stats count`,
			want:    1,
			present: true,
		},
		{
			name:    "two partitions",
			source:  `index=gradethis | stats partitions=2 count`,
			want:    2,
			present: true,
		},
		{
			name:    "four partitions",
			source:  `index=gradethis | stats partitions=4 count`,
			want:    4,
			present: true,
		},
		{
			name:    "documented limit is capped to executor ceiling",
			source:  `index=gradethis | stats partitions=100 count`,
			want:    4,
			present: true,
		},
		{
			name:    "authored value above documented limit is first clamped by plan",
			source:  `index=gradethis | stats partitions=1000 count`,
			want:    4,
			present: true,
		},
		{
			name: "multiple stages take minimum in authored order",
			source: `index=gradethis | stats partitions=4 count AS events ` +
				`| stats partitions=2 sum(events) AS total`,
			want:    2,
			present: true,
		},
		{
			name: "multiple stages take minimum in reverse order",
			source: `index=gradethis | stats partitions=2 count AS events ` +
				`| stats partitions=4 sum(events) AS total`,
			want:    2,
			present: true,
		},
		{
			name: "default stage caps explicit stage",
			source: `index=gradethis | stats partitions=4 count AS events ` +
				`| stats sum(events) AS total`,
			want:    1,
			present: true,
		},
		{
			name: "top before stats does not contribute",
			source: `index=gradethis | top host | ` +
				`stats partitions=4 sum(count) AS total`,
			want:    4,
			present: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if !compiled.HasValidExecutionSeal() {
				t.Fatal("compiler output has no valid execution seal")
			}
			got, present := compiled.StatsPartitionsMaxThreadsHint()
			if got != test.want || present != test.present {
				t.Fatalf("StatsPartitionsMaxThreadsHint() = (%d, %t), want (%d, %t)", got, present, test.want, test.present)
			}
		})
	}
}

func TestCompiledStatsPartitionsMaxThreadsHintIsSealedClonedAndRetained(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats partitions=2 count`)
	digest, digestOK := compiled.ExecutionAuthorityDigest()
	retained, retainedOK := compiled.RetainedBytes()
	cloned, cloneOK := compiled.CloneForExecution()
	clonedDigest, clonedDigestOK := cloned.ExecutionAuthorityDigest()
	clonedRetained, clonedRetainedOK := cloned.RetainedBytes()
	hint, hintOK := cloned.StatsPartitionsMaxThreadsHint()
	if !digestOK || !retainedOK || retained == 0 || !cloneOK ||
		!clonedDigestOK || clonedDigest != digest ||
		!clonedRetainedOK || clonedRetained != retained ||
		!compiled.EqualForExecution(cloned) || !hintOK || hint != 2 {
		t.Fatalf(
			"clone contract = digest %t/%t retained %d/%t %d/%t clone %t equal %t hint %d/%t",
			digestOK,
			clonedDigestOK,
			retained,
			retainedOK,
			clonedRetained,
			clonedRetainedOK,
			cloneOK,
			compiled.EqualForExecution(cloned),
			hint,
			hintOK,
		)
	}

	for _, test := range []struct {
		name string
		hint uint8
	}{
		{name: "different valid value", hint: 3},
		{name: "removed hint", hint: 0},
		{name: "out of range value", hint: maximumStatsPartitionsMaxThreadsHint + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tampered := compiled
			tampered.statsPartitionsMaxThreadsHint = test.hint
			if tampered.HasValidExecutionSeal() {
				t.Fatal("tampered stats partitions hint retained execution authority")
			}
			if _, ok := tampered.StatsPartitionsMaxThreadsHint(); ok {
				t.Fatal("tampered stats partitions hint opened")
			}
			if _, ok := tampered.ExecutionAuthorityDigest(); ok {
				t.Fatal("tampered stats partitions hint exposed an authority digest")
			}
			if _, ok := tampered.CloneForExecution(); ok {
				t.Fatal("tampered stats partitions hint cloned for execution")
			}
			if _, ok := tampered.RetainedBytes(); ok {
				t.Fatal("tampered stats partitions hint reported retained bytes")
			}
		})
	}
}

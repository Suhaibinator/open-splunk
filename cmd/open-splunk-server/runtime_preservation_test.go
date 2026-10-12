package main

import "testing"

func TestRuntimePreservationGuardRejectsRecreatedAuthority(t *testing.T) {
	known := map[string]bool{"events": true}
	for _, tc := range []struct {
		name, tenant        string
		assigned, committed int64
		known               map[string]bool
		physical            []physicalIndexAuthority
		valid               bool
	}{
		{"new empty installation", "physical", 0, 0, nil, nil, true},
		{"preserved events", "physical", 42, 42, known, []physicalIndexAuthority{{"physical", "events", 42}}, true},
		{"retention removed newest rows", "physical", 99, 99, known, []physicalIndexAuthority{{"physical", "events", 42}}, true},
		{"fresh PostgreSQL attached old ClickHouse", "physical", 0, 0, known, []physicalIndexAuthority{{"physical", "events", 42}}, false},
		{"uncommitted authority after drain", "physical", 42, 41, known, []physicalIndexAuthority{{"physical", "events", 42}}, false},
		{"recreated index name missing ownership", "physical", 42, 42, nil, []physicalIndexAuthority{{"physical", "events", 42}}, false},
		{"changed physical tenant", "newtenant", 42, 42, known, []physicalIndexAuthority{{"physical", "events", 42}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := compareRuntimePreservedAuthority(tc.tenant, tc.assigned, tc.committed, tc.known, tc.physical)
			if (err == nil) != tc.valid {
				t.Fatalf("guard valid=%t err=%v", tc.valid, err)
			}
		})
	}
}

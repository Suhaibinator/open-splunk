package queryexec

import (
	"errors"
	"testing"
	"unsafe"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestTimechartGridReservationHonorsMemoryAndResultBudgets(t *testing.T) {
	query := clickhouse.CompiledQuery{Timechart: &clickhouse.TimechartOutput{
		Mode: clickhouse.TimechartModeRuntimeWide, ExactGrid: true, BucketCount: 100,
	}}
	reservation := uint64(100) + uint64(unsafe.Sizeof(timechartGridRows{}))
	for _, memoryBudget := range []uint64{reservation, reservation + 8, 4096} {
		policy := searchlimits.Default()
		policy.MaxResultBytes = 2048
		policy.MaxMemoryBytes = memoryBudget
		limits, err := deriveTimechartResourceLimits(clickhousedriver.Settings{}, query, policy, true)
		if memoryBudget == reservation {
			if !errors.Is(err, searchjobs.ErrExecutionLimit) {
				t.Fatalf("exhausted grid budget error = %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if limits.retainedBytes+reservation > memoryBudget || limits.retainedBytes+reservation > policy.MaxResultBytes {
			t.Fatalf("decoder can overcommit grid memory: limits=%+v reservation=%d", limits, reservation)
		}
		if memoryBudget == reservation+8 && limits.cells != 1 {
			t.Fatalf("one-cell budget admits %d cells", limits.cells)
		}
	}
}

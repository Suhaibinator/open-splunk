package queryexec

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestExecutorAttachesSealedLookupExternalTable(t *testing.T) {
	t.Parallel()

	logical := buildReadAdmissionPlan(t,
		`index=target | lookup service_catalog service_id AS service OUTPUT owner | stats count`,
	)
	resolution, err := clickhouse.NewLookupResolution(
		"tenant-read",
		"service_catalog",
		"asset-7",
		7,
		uint64(len("service_id,owner\napi,platform\n")),
		sha256.Sum256([]byte("asset-7")),
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(): %v", err)
	}
	lookup, ok := logical.Operators[2].(*plan.Lookup)
	if !ok {
		t.Fatalf("operator 2 = %T, want *plan.Lookup", logical.Operators[2])
	}
	resolution, err = resolution.WithLogicalContract(
		*lookup,
		"lookup-service-catalog",
		1,
	)
	if err != nil {
		t.Fatalf("WithLogicalContract(): %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).CompileWithLookupResolutions(
		logical,
		[]clickhouse.LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(): %v", err)
	}

	stop := errors.New("lookup external table context captured")
	connection := &lookupExternalTableConnection{err: stop}
	executor := mustExecutor(t, connection)
	executor.readAdmission = indexread.UnfencedAdmission{}
	probe := &lookupExternalTableContextProbe{Context: context.Background()}
	err = executor.Execute(probe, compiled, &fakeSink{})
	if !errors.Is(err, stop) {
		t.Fatalf("Execute() error = %v, want %v", err, stop)
	}
	if connection.ctx == nil || probe.key == nil {
		t.Fatal("executor did not create a ClickHouse query context")
	}
	raw := connection.ctx.Value(probe.key)
	options := reflect.ValueOf(raw)
	for options.Kind() == reflect.Pointer {
		options = options.Elem()
	}
	external := options.FieldByName("external")
	if !external.IsValid() || external.Kind() != reflect.Slice || external.Len() != 1 {
		t.Fatalf("ClickHouse external table options = %#v", raw)
	}
}

type lookupExternalTableConnection struct {
	ctx context.Context
	err error
}

func (connection *lookupExternalTableConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	connection.ctx = ctx
	return nil, connection.err
}

type lookupExternalTableContextProbe struct {
	context.Context
	key any
}

func (probe *lookupExternalTableContextProbe) Value(key any) any {
	if reflect.TypeOf(key) == reflect.TypeFor[*clickhousedriver.QueryOptions]() {
		probe.key = key
	}
	return probe.Context.Value(key)
}

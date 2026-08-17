package queryexec

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	// The fixed structured PLAN form is exercised against the
	// production-pinned ClickHouse image. It exposes only plan structure,
	// physical read headers, and index selection; actions remain disabled.
	// Descriptions remain disabled because they add rendered expressions and
	// lookup-join internals that are neither parsed nor published.
	// The compiler SQL is sealed inside this fixed outer SELECT so its
	// server-side timeout clause takes precedence over clickhouse-go's
	// deadline-derived protocol setting. Callers cannot select an EXPLAIN
	// mode, alias, or setting.
	explainQueryPrefix = "EXPLAIN PLAN json = 1, description = 0, indexes = 1, " +
		"actions = 0, header = 1 SELECT * FROM ("
	explainQuerySettingsPrefix = ") AS __os_explain_input SETTINGS max_execution_time = "
	explainQuerySettingsSuffix = ", use_query_condition_cache = 0, " +
		"use_skip_indexes_on_data_read = 0, enable_full_text_index = 1"

	maximumConcurrentExplains   = 2
	maximumExplainExecutionTime = 10 * time.Second
	maximumExplainMemoryBytes   = uint64(128 << 20)
	maximumExplainRowsToRead    = uint64(5_000_000)
	maximumExplainBytesToRead   = uint64(1 << 30)
	maximumExplainResultRows    = uint64(4_096)
	maximumExplainLineBytes     = uint64(32 << 10)
	maximumExplainResultBytes   = uint64(1 << 20)
	maximumExplainArrayElements = 4_096
	maximumExplainGroups        = uint64(4_096)
	maximumExplainThreads       = uint64(1)
	// This independently enforces the compiler's 256 KiB generated-query
	// ceiling at the execution boundary. The ClickHouse max_query_size setting
	// includes the fixed wrapper and rendered bind arguments.
	maximumExplainQueryBytes   = uint64(256 << 10)
	maximumExplainQueryIDBytes = 128
	explainQueryIDPrefix       = "open-splunk-explain-"
)

var (
	errExplainQueryFailed = errors.New("execute ClickHouse EXPLAIN: query failed")
	errExplainerClosed    = errors.New("execute ClickHouse EXPLAIN: explainer is closed")
)

// ExplainResult is one completely buffered and validated ClickHouse query
// plan. Text can contain ClickHouse-rendered bound values and must therefore
// remain restricted to an administrator-only diagnostic boundary. QueryID
// identifies this EXPLAIN request, not the original search execution.
type ExplainResult struct {
	Text    string
	QueryID string
}

type explainLane struct {
	connection      queryConnection
	activateContext func(context.Context) (func() error, error)
	discard         func() error
	close           func() error
}

// Explainer owns two isolated native ClickHouse lanes used only for bounded
// administrative plans. Each lane has one physical connection and its own
// transport-deadline overlay, so a short or canceled request cannot poison an
// unrelated plan while the initial native query write remains cancellable.
//
// Construct an Explainer with NewExplainer and close it after every inspection
// service has stopped. It is intentionally separate from Executor: ordinary
// search and export queries do not reserve these diagnostic connections.
type Explainer struct {
	settings             clickhousedriver.Settings
	executionTimeout     time.Duration
	requireExecutionSeal bool
	lanes                chan *explainLane
	allLanes             []*explainLane
	newQueryID           func() (string, error)

	mu         sync.Mutex
	closed     bool
	nextCallID uint64
	active     map[uint64]context.CancelFunc
	calls      sync.WaitGroup
	closeOnce  sync.Once
	closeErr   error
}

// Explain obtains ClickHouse's fixed structured logical/physical PLAN for a
// compiler-produced query. It validates and detaches the ordered bind
// arguments but never returns or logs them. The complete bounded one-row JSON
// result is structurally validated before publication, so cancellation,
// malformed plans, and driver failures are atomic. EXPLAIN does not execute
// the event read and uses dedicated diagnostic lanes, so it intentionally does
// not acquire Executor's runtime index-read lease.
func (explainer *Explainer) Explain(
	ctx context.Context,
	query clickhouse.CompiledQuery,
) (result ExplainResult, resultErr error) {
	if ctx == nil {
		return ExplainResult{}, errors.New("execute ClickHouse EXPLAIN: context is nil")
	}
	if explainer == nil {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: explainer is required",
		)
	}
	if explainer.newQueryID == nil {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: query ID generator is required",
		)
	}
	if explainer.lanes == nil ||
		cap(explainer.lanes) != maximumConcurrentExplains ||
		len(explainer.allLanes) != maximumConcurrentExplains {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: transport lanes are invalid",
		)
	}
	timeoutSeconds, timeoutOK := explainer.settings["max_execution_time"].(uint64)
	maximumBoundQueryBytes, querySizeOK :=
		explainer.settings["max_query_size"].(uint64)
	if explainer.settings == nil ||
		!timeoutOK ||
		timeoutSeconds == 0 ||
		timeoutSeconds > uint64(maximumExplainExecutionTime/time.Second) ||
		!querySizeOK ||
		maximumBoundQueryBytes == 0 ||
		maximumBoundQueryBytes > defaultMaxQueryBytes ||
		explainer.executionTimeout <= 0 ||
		explainer.executionTimeout > maximumExplainExecutionTime {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: runtime limits are invalid",
		)
	}
	if err := ctx.Err(); err != nil {
		return ExplainResult{}, err
	}
	if err := explainer.checkOpen(); err != nil {
		return ExplainResult{}, err
	}
	if explainer.requireExecutionSeal {
		detached, ok, cloneErr := query.CloneForExecutionContext(ctx)
		if cloneErr != nil {
			return ExplainResult{}, cloneErr
		}
		if !ok {
			return ExplainResult{}, invalidExplainResult(
				"compiled execution authority is not an unchanged Compiler result",
			)
		}
		query = detached
	}

	// Admission is deliberately fail-fast and precedes SQL hashing, argument
	// inspection, timer creation, and lifecycle registration. At most the two
	// lane owners can consume any of those resources.
	var lane *explainLane
	select {
	case lane = <-explainer.lanes:
	default:
		if err := explainer.checkOpen(); err != nil {
			return ExplainResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return ExplainResult{}, err
		}
		return ExplainResult{}, searchjobs.ErrCapacity
	}
	if lane == nil ||
		isNilDriverValue(lane.connection) ||
		lane.activateContext == nil ||
		lane.discard == nil {
		if lane != nil {
			explainer.lanes <- lane
		}
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: transport lane is invalid",
		)
	}
	defer func() { explainer.lanes <- lane }()

	explainContext, cancel := context.WithTimeout(
		ctx,
		explainer.executionTimeout,
	)
	defer cancel()

	callID, err := explainer.beginCall(cancel)
	if err != nil {
		return ExplainResult{}, err
	}
	defer explainer.endCall(callID)

	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}
	if err := validateExplainQuery(query); err != nil {
		return ExplainResult{}, err
	}
	externalTables, err := query.ExternalTablesForExecution(explainContext)
	if err != nil {
		if contextErr := explainContext.Err(); contextErr != nil {
			return ExplainResult{}, contextErr
		}
		return ExplainResult{}, invalidExplainResult(
			"compiled lookup transport is invalid",
		)
	}

	releaseContext, err := lane.activateContext(explainContext)
	if err != nil {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: activate transport deadline failed",
		)
	}
	if releaseContext == nil {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: transport deadline release is required",
		)
	}
	defer func() {
		if err := releaseContext(); resultErr == nil && err != nil {
			result = ExplainResult{}
			resultErr = errors.New(
				"execute ClickHouse EXPLAIN: release transport deadline failed",
			)
		}
	}()

	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}

	querySQL := explainQueryPrefix + query.SQL +
		explainQuerySettingsPrefix + strconv.FormatUint(timeoutSeconds, 10) +
		explainQuerySettingsSuffix
	detachedArgs, err := detachExplainArguments(
		query.Args,
		compilerPlaceholderCount(query.SQL),
		uint64(len(querySQL)),
		maximumBoundQueryBytes,
	)
	if err != nil {
		return ExplainResult{}, err
	}

	queryID, err := explainer.newQueryID()
	if err != nil {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: create query ID failed",
		)
	}
	if !validExplainQueryID(queryID) {
		return ExplainResult{}, errors.New(
			"execute ClickHouse EXPLAIN: query ID is invalid",
		)
	}
	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}

	// Preserve the real deadline so clickhouse-go can install native socket
	// deadlines. The pinned driver also derives a looser max_execution_time
	// protocol setting from this deadline; the fixed SQL SETTINGS clause above
	// has server-side precedence and pins the effective value back to our cap.
	queryOptions := []clickhousedriver.QueryOption{
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(explainer.settings),
	}
	queryOptions = appendExternalTableOption(queryOptions, externalTables)
	queryContext := clickhousedriver.Context(explainContext, queryOptions...)
	rows, err := lane.connection.Query(
		queryContext,
		querySQL,
		detachedArgs...,
	)
	if err != nil {
		queryErr := sanitizeExplainQueryError(explainContext, err)
		if discardErr := lane.discard(); discardErr != nil {
			return ExplainResult{}, errors.Join(
				queryErr,
				errors.New(
					"execute ClickHouse EXPLAIN: discard failed transport",
				),
			)
		}
		return ExplainResult{}, queryErr
	}
	if isNilDriverValue(rows) {
		return ExplainResult{}, invalidExplainResult("returned no result stream")
	}

	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			result = ExplainResult{}
			resultErr = sanitizeExplainQueryError(explainContext, closeErr)
		}
	}()

	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}
	if err := validateExplainColumns(rows.Columns(), rows.ColumnTypes()); err != nil {
		return ExplainResult{}, err
	}

	var text strings.Builder
	var serverRowCount, lineCount, resultBytes uint64
	for {
		if err := explainContext.Err(); err != nil {
			return ExplainResult{}, err
		}
		if !rows.Next() {
			break
		}
		if err := explainContext.Err(); err != nil {
			return ExplainResult{}, err
		}
		if serverRowCount != 0 {
			return ExplainResult{}, invalidExplainResult(
				"returned multiple structured plan rows",
			)
		}
		var row string
		if err := rows.Scan(&row); err != nil {
			return ExplainResult{}, sanitizeExplainQueryError(explainContext, err)
		}
		if err := explainContext.Err(); err != nil {
			return ExplainResult{}, err
		}
		if err := appendExplainRow(
			&text,
			row,
			&lineCount,
			&resultBytes,
		); err != nil {
			return ExplainResult{}, err
		}
		serverRowCount++
	}
	if err := rows.Err(); err != nil {
		return ExplainResult{}, sanitizeExplainQueryError(explainContext, err)
	}
	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}
	if serverRowCount != 1 || lineCount == 0 {
		return ExplainResult{}, invalidExplainResult("returned an empty plan")
	}

	rowsClosed = true
	if err := rows.Close(); err != nil {
		return ExplainResult{}, sanitizeExplainQueryError(explainContext, err)
	}
	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}
	result = ExplainResult{Text: text.String(), QueryID: queryID}
	if _, err := parseExplainPlanText(explainContext, result.Text); err != nil {
		return ExplainResult{}, err
	}
	if err := explainContext.Err(); err != nil {
		return ExplainResult{}, err
	}
	return result, nil
}

// ClickHouse's json=1 PLAN is one String row containing pretty-printed JSON,
// so normalize its embedded lines into the bounded newline-delimited result
// contract. Literal newlines are the only admitted controls and are retained
// exactly between independently validated nonempty lines.
func appendExplainRow(
	text *strings.Builder,
	row string,
	lineCount *uint64,
	resultBytes *uint64,
) error {
	remaining := row
	for {
		if *lineCount >= maximumExplainResultRows {
			return explainLimit("returned too many rows")
		}
		newline := strings.IndexByte(remaining, '\n')
		line := remaining
		if newline >= 0 {
			line = remaining[:newline]
		}
		if err := validateExplainLine(line); err != nil {
			return err
		}

		additionalBytes := uint64(len(line))
		if *lineCount > 0 {
			additionalBytes++
		}
		if additionalBytes > maximumExplainResultBytes-*resultBytes {
			return explainLimit("exceeded the result byte limit")
		}
		if *lineCount > 0 {
			text.WriteByte('\n')
		}
		text.WriteString(line)
		*resultBytes += additionalBytes
		*lineCount++

		if newline < 0 {
			return nil
		}
		remaining = remaining[newline+1:]
	}
}

func (explainer *Explainer) beginCall(
	cancel context.CancelFunc,
) (uint64, error) {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	if explainer.closed {
		return 0, errExplainerClosed
	}
	if explainer.active == nil {
		explainer.active = make(map[uint64]context.CancelFunc)
	}
	explainer.nextCallID++
	if explainer.nextCallID == 0 {
		return 0, errors.New(
			"execute ClickHouse EXPLAIN: active call identity is exhausted",
		)
	}
	callID := explainer.nextCallID
	explainer.active[callID] = cancel
	// Add is serialized with Close setting closed under the same mutex. Once
	// closed is visible, no later Add can race with Close's Wait.
	explainer.calls.Add(1)
	return callID, nil
}

func (explainer *Explainer) checkOpen() error {
	explainer.mu.Lock()
	defer explainer.mu.Unlock()
	if explainer.closed {
		return errExplainerClosed
	}
	return nil
}

func (explainer *Explainer) endCall(callID uint64) {
	explainer.mu.Lock()
	delete(explainer.active, callID)
	explainer.mu.Unlock()
	explainer.calls.Done()
}

// Close cancels active plans, waits for their native calls to unwind, and
// closes both dedicated lanes. It is safe to call concurrently and more than
// once.
func (explainer *Explainer) Close() error {
	if explainer == nil {
		return errors.New("close ClickHouse EXPLAIN: explainer is required")
	}
	explainer.closeOnce.Do(func() {
		explainer.mu.Lock()
		explainer.closed = true
		cancels := make([]context.CancelFunc, 0, len(explainer.active))
		for _, cancel := range explainer.active {
			cancels = append(cancels, cancel)
		}
		explainer.mu.Unlock()

		for _, cancel := range cancels {
			cancel()
		}
		explainer.calls.Wait()

		var closeErrors []error
		for _, lane := range explainer.allLanes {
			if lane == nil || lane.close == nil {
				continue
			}
			if err := lane.close(); err != nil {
				closeErrors = append(
					closeErrors,
					errors.New("close ClickHouse EXPLAIN transport failed"),
				)
			}
		}
		explainer.closeErr = errors.Join(closeErrors...)
	})
	return explainer.closeErr
}

func validateExplainQuery(query clickhouse.CompiledQuery) error {
	sqlBytes := uint64(len(query.SQL))
	if sqlBytes > maximumExplainQueryBytes {
		return invalidExplainResult("compiled SQL exceeds the raw byte limit")
	}
	if strings.TrimSpace(query.SQL) == "" {
		return invalidExplainResult("compiled SQL is empty")
	}
	if !utf8.ValidString(query.SQL) {
		return invalidExplainResult("compiled SQL is not valid UTF-8")
	}
	if strings.IndexByte(query.SQL, 0) >= 0 {
		return invalidExplainResult("compiled SQL contains NUL")
	}
	for _, character := range query.SQL {
		if unicode.IsControl(character) &&
			character != '\t' &&
			character != '\n' &&
			character != '\r' {
			return invalidExplainResult("compiled SQL contains unsupported controls")
		}
	}
	if !query.HasValidSQLSeal() {
		return invalidExplainResult("compiled SQL is not an unchanged Compiler result")
	}
	return nil
}

// detachExplainArguments admits exactly the concrete argument inventory
// emitted by Compiler.Compile: string, bool, int64, uint64, float64, uint8,
// []string, and []uint8. In particular, it rejects the formatter, Valuer,
// pointer, other-collection, and named-scalar fallbacks that clickhouse-go
// would otherwise evaluate during unsafe client-side query binding.
//
// Compiler's two slice forms are independently bounded and deeply copied.
// CompiledQuery.Args is public, so the executor must retain its detached
// snapshot even if a caller replaces an interface value after admission.
func detachExplainArguments(
	arguments []any,
	compilerPlaceholders int,
	rawQueryBytes uint64,
	maximumBoundQueryBytes uint64,
) ([]any, error) {
	if len(arguments) != compilerPlaceholders {
		return nil, invalidExplainResult(
			"argument count does not match compiler placeholders",
		)
	}
	if rawQueryBytes > maximumBoundQueryBytes {
		return nil, explainLimit("query exceeds the bound byte limit")
	}

	// Clone once, under the EXPLAIN concurrency gate, before inspecting any
	// element. Type and byte validation below apply to this exact snapshot,
	// which is the only slice later passed to the driver.
	detached := slices.Clone(arguments)
	estimatedBytes := rawQueryBytes
	for index, argument := range detached {
		switch value := argument.(type) {
		case []string:
			if len(value) > maximumExplainArrayElements {
				return nil, explainLimit(
					"argument collection exceeds the element limit",
				)
			}
			detached[index] = slices.Clone(value)
			argument = detached[index]
		case []uint8:
			if len(value) > maximumExplainArrayElements {
				return nil, explainLimit(
					"argument collection exceeds the element limit",
				)
			}
			detached[index] = slices.Clone(value)
			argument = detached[index]
		}
		renderedBytes, supported := explainArgumentRenderedBytes(argument)
		if !supported {
			return nil, invalidExplainResult(
				"argument " + strconv.Itoa(index) + " has an unsupported type",
			)
		}
		if renderedBytes > maximumBoundQueryBytes-estimatedBytes {
			return nil, explainLimit(
				"bound arguments exceed the query byte limit",
			)
		}
		estimatedBytes += renderedBytes
	}
	return detached, nil
}

// Every question mark in sealed Compiler SQL is a positional bind
// placeholder. The compiler never emits question marks inside literals or
// escaped identifiers, so a raw byte count is its exact cardinality contract.
func compilerPlaceholderCount(sql string) int {
	return strings.Count(sql, "?")
}

func explainArgumentRenderedBytes(argument any) (uint64, bool) {
	if argument == nil {
		return 0, false
	}
	switch argument.(type) {
	case sqldriver.Valuer, fmt.Formatter, fmt.Stringer:
		return 0, false
	}
	switch value := argument.(type) {
	case string:
		// clickhouse-go v2.46 surrounds Strings with quotes and doubles every
		// quote or backslash. Two bytes per input byte plus the quotes is a
		// conservative bound, independent of the actual contents.
		valueBytes := uint64(len(value))
		if valueBytes > (^uint64(0)-2)/2 {
			return ^uint64(0), true
		}
		return valueBytes*2 + 2, true
	case []string:
		total := uint64(2) // array brackets
		for _, element := range value {
			elementBytes, _ := explainArgumentRenderedBytes(element)
			if elementBytes >= ^uint64(0)-total {
				return ^uint64(0), true
			}
			total += elementBytes + 1 // one separator byte of headroom
		}
		return total, true
	case []uint8:
		length := uint64(len(value))
		if length > (^uint64(0)-2)/4 {
			return ^uint64(0), true
		}
		// Three decimal digits and one separator per element, plus brackets.
		return length*4 + 2, true
	case bool:
		return 1, true
	case int64, uint64:
		return 20, true
	case float64:
		// fmt's shortest Float64 representation is at most 24 bytes for a
		// finite value. Leave additional headroom for special values and any
		// compatible formatter detail in the pinned driver.
		return 32, true
	case uint8:
		return 3, true
	default:
		if isNilDriverValue(argument) {
			return 0, false
		}
		return 0, false
	}
}

func settingsForExplain(
	base *validatedExecutorSettings,
) (clickhousedriver.Settings, error) {
	if base == nil {
		return nil, errors.New(
			"execute ClickHouse EXPLAIN: explainer does not have read-only settings",
		)
	}
	settings := boundedExecutorSettings(
		base,
		settingLimit{name: "max_execution_time", maximum: uint64(maximumExplainExecutionTime / time.Second)},
		settingLimit{name: "max_memory_usage", maximum: maximumExplainMemoryBytes},
		settingLimit{name: "max_rows_to_read", maximum: maximumExplainRowsToRead},
		settingLimit{name: "max_bytes_to_read", maximum: maximumExplainBytesToRead},
		settingLimit{name: "max_result_rows", maximum: maximumExplainResultRows},
		settingLimit{name: "max_result_bytes", maximum: maximumExplainResultBytes},
		settingLimit{name: "max_rows_to_group_by", maximum: maximumExplainGroups},
		settingLimit{name: "max_threads", maximum: maximumExplainThreads},
		settingLimit{name: "max_query_size", maximum: defaultMaxQueryBytes},
		settingLimit{name: "max_subquery_depth", maximum: defaultMaxSubqueryDepth},
	)
	settings["use_query_cache"] = uint8(0)
	return settings, nil
}

func validateExplainColumns(
	columns []string,
	columnTypes []driver.ColumnType,
) error {
	const explainColumn = "explain"
	contracts := []resultColumnContract{{
		name:         explainColumn,
		databaseType: "String",
		scanType:     reflect.TypeFor[string](),
	}}
	violation, _ := validateResultColumnContracts(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType,
	)
	if violation == resultColumnContractShapeMismatch {
		return invalidExplainResult("columns do not match the expected output")
	}
	if violation == resultColumnContractTypeMismatch {
		return invalidExplainResult("column has an invalid type")
	}
	return nil
}

func validateExplainLine(line string) error {
	lineBytes := uint64(len(line))
	if line == "" || lineBytes > maximumExplainLineBytes {
		if lineBytes > maximumExplainLineBytes {
			return explainLimit("returned an oversized line")
		}
		return invalidExplainResult("returned an empty line")
	}
	if !utf8.ValidString(line) {
		return invalidExplainResult("returned invalid UTF-8")
	}
	for _, character := range line {
		if unicode.IsControl(character) {
			return invalidExplainResult("returned control characters")
		}
	}
	return nil
}

// ValidateExplainResult verifies the complete public contract of a buffered
// EXPLAIN result. Returned errors expose only stable search-job categories and
// fixed diagnostics; neither plan text nor query IDs are included because both
// may contain administrator-sensitive data.
func ValidateExplainResult(result ExplainResult) error {
	if !validExplainQueryID(result.QueryID) {
		return invalidExplainResult("query ID is invalid")
	}
	if result.Text == "" {
		return invalidExplainResult("returned an empty plan")
	}
	if uint64(len(result.Text)) > maximumExplainResultBytes {
		return explainLimit("exceeded the result byte limit")
	}

	remaining := result.Text
	var lineCount uint64
	for {
		if lineCount >= maximumExplainResultRows {
			return explainLimit("returned too many rows")
		}
		newline := strings.IndexByte(remaining, '\n')
		line := remaining
		if newline >= 0 {
			line = remaining[:newline]
		}
		if err := validateExplainLine(line); err != nil {
			return err
		}
		lineCount++
		if newline < 0 {
			return nil
		}
		remaining = remaining[newline+1:]
	}
}

func validExplainQueryID(queryID string) bool {
	if !strings.HasPrefix(queryID, explainQueryIDPrefix) ||
		len(queryID) == len(explainQueryIDPrefix) ||
		len(queryID) > maximumExplainQueryIDBytes {
		return false
	}
	for _, character := range queryID[len(explainQueryIDPrefix):] {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case character == '-', character == '_', character == '.', character == ':':
		default:
			return false
		}
	}
	return true
}

func randomExplainQueryID() (string, error) {
	return randomPrefixedQueryID(explainQueryIDPrefix)
}

func sanitizeExplainQueryError(ctx context.Context, err error) error {
	if explainContextDeadlineTimeout(ctx, err) {
		return context.DeadlineExceeded
	}
	classified := classifyQueryError(ctx, err)
	switch {
	case classified == nil:
		return nil
	case errors.Is(classified, context.Canceled):
		return context.Canceled
	case errors.Is(classified, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(classified, searchjobs.ErrExecutionLimit):
		return searchjobs.ErrExecutionLimit
	case errors.Is(classified, searchjobs.ErrStorageUnavailable):
		return searchjobs.ErrStorageUnavailable
	case errors.Is(classified, searchjobs.ErrUnsupportedValue):
		return searchjobs.ErrUnsupportedValue
	default:
		return errExplainQueryFailed
	}
}

// A socket deadline and a context deadline share the same monotonic instant,
// but the network poller can publish its timeout just before the context timer
// goroutine records ctx.Err(). Treat only timeout-shaped errors at or after the
// exact Explain deadline as context expiration. Earlier driver timeouts remain
// storage failures.
func explainContextDeadlineTimeout(ctx context.Context, err error) bool {
	if ctx == nil || err == nil {
		return false
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.IsZero() || time.Now().Before(deadline) {
		return false
	}
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func invalidExplainResult(message string) error {
	return fmt.Errorf(
		"%w: ClickHouse EXPLAIN %s",
		searchjobs.ErrInvalidResult,
		message,
	)
}

func explainLimit(message string) error {
	return fmt.Errorf(
		"%w: ClickHouse EXPLAIN %s",
		searchjobs.ErrExecutionLimit,
		message,
	)
}

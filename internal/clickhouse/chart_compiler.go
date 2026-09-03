package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func compileTimechart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Timechart,
	outputFields []string,
	dynamic *plan.DynamicSeriesOutput,
	scan *plan.Scan,
	alias string,
) (CompiledQuery, error) {
	if err := validateTimechartMeasure(operator, state); err != nil {
		return CompiledQuery{}, err
	}
	if operator.FirstBucket.Nanosecond() != 0 || operator.FirstBucket.IsZero() ||
		operator.BucketCount == 0 || operator.BucketCount > 10_000 || !operator.FixedRange ||
		!operator.Continuous || !operator.IncludePartial {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: bounded defaults are invalid")
	}
	if scan == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: Scan snapshot is required")
	}
	var gridSpec timechartGridSpec
	var err error
	switch operator.Calendar {
	case plan.CalendarNone:
		if operator.Span < time.Second || operator.Span > 24*time.Hour ||
			operator.Span%time.Second != 0 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse timechart: fixed span is invalid",
			)
		}
		gridSpec, err = fixedTimechartGridSpec(operator, scan)
	case plan.CalendarDay, plan.CalendarWeek:
		if operator.Span != 0 || state.context == nil {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse timechart: calendar span is invalid",
			)
		}
		if err = validateCompileContextSearchTimezone(state.context); err == nil {
			gridSpec, err = calendarTimechartGridSpec(
				operator,
				scan,
				state.context.searchTimezone,
			)
		}
	default:
		return CompiledQuery{}, errors.New(
			"compile ClickHouse timechart: calendar unit is invalid",
		)
	}
	if err != nil {
		return CompiledQuery{}, err
	}
	if !state.eventRows {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_INPUT",
			Message: "timechart requires event rows with the canonical _time field",
			Range:   operator.Range,
		}
	}
	if err := validateCanonicalFieldRef("timechart", "time", operator.Time); err != nil {
		return CompiledQuery{}, err
	}
	timeField, ok, err := resolveCompiledField(operator.Time, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !ok || operator.Time.Name != "_time" || timeField.kind != fieldKindTime || !timeField.canonicalTime {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
			Message: "timechart requires the unmodified canonical _time field",
			Range:   operator.Range,
		}
	}

	if valueKind, fixedValue := fixedTimechartValueKind(operator.Measure.Function); fixedValue && operator.Split == nil {
		if len(outputFields) != 2 || outputFields[0] != "_time" ||
			outputFields[1] != operator.Measure.Output || outputFields[1] == "_time" ||
			dynamic != nil {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse timechart: fixed value output contract is invalid",
			)
		}
		measureField, measureExists, resolveErr := resolveCompiledField(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
		measureInputSQL := "CAST([], 'Array(Float64)')"
		var measureArgs []any
		if measureExists {
			measureInputSQL, measureArgs = numericArrayInputSQL(measureField)
		}
		return compileFixedValueTimechart(
			relation,
			args,
			operator,
			valueKind,
			timeField,
			measureInputSQL,
			measureArgs,
			outputFields,
			gridSpec,
			alias,
		)
	}

	if operator.Split == nil {
		if operator.Measure.Function == plan.AggregateFunctionCountValues {
			if len(outputFields) != 2 || outputFields[0] != "_time" ||
				outputFields[1] != operator.Measure.Output ||
				outputFields[1] == "_time" || dynamic != nil {
				return CompiledQuery{}, errors.New(
					"compile ClickHouse timechart: fixed count(field) output contract is invalid",
				)
			}
			measureInputSQL, measureArgs, resolveErr := resolveCountValueInput(
				operator.Measure.Input,
				state,
			)
			if resolveErr != nil {
				return CompiledQuery{}, resolveErr
			}
			return compileFixedCountValueTimechart(
				relation,
				args,
				operator,
				timeField,
				measureInputSQL,
				measureArgs,
				outputFields,
				gridSpec,
				alias,
			)
		}
		if !slices.Equal(outputFields, []string{"_time", "count"}) || dynamic != nil {
			return CompiledQuery{}, errors.New("compile ClickHouse timechart: fixed output contract is invalid")
		}
		return compileFixedCountTimechart(
			relation,
			args,
			operator,
			timeField,
			outputFields,
			gridSpec,
			alias,
		)
	}
	if len(outputFields) != 0 || dynamic == nil ||
		!slices.Equal(dynamic.FixedFields, []string{"_time"}) ||
		operator.Split.SeriesLimit < 1 ||
		operator.Split.SeriesLimit > spl.MaximumTimechartSeriesLimit ||
		dynamic.MaxSeries != timechartSplitMaxSeries(operator.Split) ||
		operator.Split.NullLabel != "NULL" || operator.Split.OtherLabel != "OTHER" {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: dynamic output contract is invalid")
	}
	if err := validateCanonicalFieldRef(
		"timechart",
		"split",
		operator.Split.Field,
	); err != nil {
		return CompiledQuery{}, err
	}

	splitField, splitExists, err := resolveCompiledField(operator.Split.Field, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !splitExists {
		// A projected-away split field is missing for every retained event. SPL's
		// default usenull=true therefore produces a NULL series rather than
		// resurrecting the private source document.
		splitField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if splitField.kind == fieldKindInvalid {
		// A statically null field is in the supported missing/null split domain.
		// Preserve its exact-presence expression while assigning the String type
		// used by the runtime label classifier.
		splitField = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  splitField.existsSQL,
			existsArgs: splitField.existsArgs,
			kind:       fieldKindString,
		}
	}
	if splitField.kind != fieldKindString && splitField.kind != fieldKindDynamic {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE",
			Message:     "timechart split fields currently support strings and missing values",
			Range:       operator.Range,
			Suggestions: []string{"convert the split field to a string before timechart"},
		}
	}

	existsSQL := splitField.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	valueTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitField.kind == fieldKindDynamic {
		valueTypeSQL = dynamicTypeExpression(splitField)
	}
	if valueKind, splitValue := fixedTimechartValueKind(operator.Measure.Function); splitValue {
		measureField, measureExists, resolveErr := resolveCompiledField(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
		measureInputSQL := "CAST([], 'Array(Float64)')"
		var measureArgs []any
		if measureExists {
			measureInputSQL, measureArgs = numericArrayInputSQL(measureField)
		}
		return compileSplitValueTimechart(
			relation,
			state,
			args,
			operator,
			valueKind,
			timeField,
			splitField,
			existsSQL,
			valueTypeSQL,
			measureInputSQL,
			measureArgs,
			dynamic,
			gridSpec,
			alias,
		)
	}
	fieldOccurrenceCount := operator.Measure.Function == plan.AggregateFunctionCountValues
	measureInputSQL := ""
	var measureArgs []any
	if fieldOccurrenceCount {
		var resolveErr error
		measureInputSQL, measureArgs, resolveErr = resolveCountValueInput(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
	}
	// Source-select bind markers precede the nested scoped relation. Split
	// descendant detection lives in the next CTE, after every source marker.
	prefixArgs := make([]any, 0, len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	hasDescendant := splitField.kind == fieldKindDynamic && splitField.descendantSQL != ""
	if hasDescendant {
		args = append(args, splitField.descendantArgs...)
	}

	q := quoteIdentifier
	source := q("__os_timechart_source")
	prepared := q("__os_timechart_prepared")
	classified := q("__os_timechart_classified")
	canonicalized := q("__os_timechart_canonicalized")
	counts := q("__os_timechart_group_counts")
	scored := q("__os_timechart_scored")
	ranked := q("__os_timechart_ranked")
	collapsed := q("__os_timechart_collapsed")
	domainRows := q("__os_timechart_domain_rows")
	domain := q("__os_timechart_domain")
	bucketMaps := q("__os_timechart_bucket_maps")
	grid := q("__os_timechart_grid")

	eventTime := q("__os_tc_event_time")
	value := q("__os_tc_value")
	present := q("__os_tc_present")
	descendant := q("__os_tc_descendant")
	valueType := q("__os_tc_value_type")
	ticks := q("__os_tc_ticks")
	label := q("__os_tc_label")
	measureCount := q("__os_tc_measure_count")
	bucketNumber := q("__os_tc_bucket_number")
	kind := q("__os_tc_kind")
	frequency := q("__os_tc_count")
	rowCount := q("__os_tc_row_count")
	occurrenceCount := q("__os_tc_occurrence_count")
	collapsedRowCount := q("__os_tc_collapsed_row_count")
	collapsedCount := q("__os_tc_collapsed_count")
	seriesScore := q("__os_tc_series_score")
	seriesRank := q("__os_tc_series_rank")
	encoded := q("__os_tc_encoded")
	collisionCardinality := q("__os_tc_collision_cardinality")
	collision := q("__os_tc_collision")
	sortLabel := q("__os_tc_sort_label")
	countMap := q("__os_tc_count_map")
	invalid := q("__os_tc_invalid")
	ordinal := q(TimechartOrdinalColumn)

	bucketNumberExpression := gridSpec.bucketKeySQL(eventTime, ticks)
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(eventTime)
	sql.WriteString(", ")
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(existsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(valueTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureInputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(measureCount)
	}
	for _, column := range pivotDescendantSourceColumns(state, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	if hasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("reinterpretAsInt64(")
	sql.WriteString(eventTime)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', ")
	sql.WriteString("assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumberExpression)
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString("multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	// Keep the raw bucket/label aggregate as the first bounded operation. The
	// executor's max_rows_to_group_by seal therefore continues to cap exactly
	// the same 130k raw groups before any series selection or publication.
	sql.WriteString(counts)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", ")
	if fieldOccurrenceCount {
		// Row frequency and occurrence cardinality are intentionally
		// independent. The former keeps zero-contribution labels in the domain
		// and validates bad split rows; only the latter selects series and
		// populates their cells.
		sql.WriteString("count() AS ")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(")
		sql.WriteString(measureCount)
		sql.WriteString("))) AS ")
		sql.WriteString(occurrenceCount)
	} else {
		sql.WriteString("count() AS ")
		sql.WriteString(frequency)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(canonicalized)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString("), ")

	// Score every raw label once across buckets. The collision cardinality is a
	// window over the public label normalization domain, so it travels with the
	// same single-consumer chain instead of requiring another counts branch.
	scoreInput := frequency
	if fieldOccurrenceCount {
		scoreInput = occurrenceCount
	}
	sql.WriteString(scored)
	sql.WriteString(" AS (SELECT *, sum(toUInt128(")
	sql.WriteString(scoreInput)
	sql.WriteString(")) OVER (PARTITION BY ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(seriesScore)
	sql.WriteString(", ")
	sql.WriteString("uniqExact(")
	sql.WriteString(label)
	sql.WriteString(") OVER (PARTITION BY ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(") AS ")
	sql.WriteString(collisionCardinality)
	sql.WriteString(" FROM ")
	sql.WriteString(counts)
	sql.WriteString("), ")

	// The label tie-breaker makes every ordinary label's dense rank unique,
	// while repeated bucket rows for that label retain one shared rank.
	sql.WriteString(ranked)
	sql.WriteString(" AS (SELECT *, dense_rank() OVER (PARTITION BY ")
	sql.WriteString(kind)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(seriesScore)
	sql.WriteString(" DESC, ")
	sql.WriteString(label)
	sql.WriteString(" ASC) AS ")
	sql.WriteString(seriesRank)
	sql.WriteString(" FROM ")
	sql.WriteString(scored)
	sql.WriteString("), ")

	// usenull=false and useother=false drop their sentinel branch, so the
	// affected rows collapse into the private empty encoding that never becomes
	// a map key or a public series name.
	seriesLimit := strconv.FormatUint(uint64(operator.Split.SeriesLimit), 10)
	sql.WriteString(collapsed)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", multiIf(")
	if operator.Split.IncludeNull {
		sql.WriteString(kind)
		sql.WriteString(" = 1, '1:', ")
	}
	sql.WriteString(kind)
	sql.WriteString(" = 0 AND ")
	sql.WriteString(seriesRank)
	sql.WriteString(" <= ")
	sql.WriteString(seriesLimit)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString("), ")
	if operator.Split.IncludeOther {
		sql.WriteString(kind)
		sql.WriteString(" = 0, '2:', ")
	}
	sql.WriteString("CAST('' AS String)) AS ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	if fieldOccurrenceCount {
		sql.WriteString("sum(")
		sql.WriteString(rowCount)
		sql.WriteString(") AS ")
		sql.WriteString(collapsedRowCount)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(")
		sql.WriteString(occurrenceCount)
		sql.WriteString("))) AS ")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
	} else {
		sql.WriteString("sum(")
		sql.WriteString(frequency)
		sql.WriteString(") AS ")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
	}
	validationCount := frequency
	if fieldOccurrenceCount {
		validationCount = rowCount
	}
	sql.WriteString("toUInt8(sumIf(")
	sql.WriteString(validationCount)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(" = 3) > 0) AS ")
	sql.WriteString(invalid)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(maxIf(")
	sql.WriteString(collisionCardinality)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(" = 0) > 1) AS ")
	sql.WriteString(collision)
	sql.WriteString(" FROM ")
	sql.WriteString(ranked)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	// Every domain member now comes from the sealed, already-collapsed relation.
	// Empty encodings are private validation rows and never become map keys or
	// public names.
	domainFrequency := collapsedCount
	if fieldOccurrenceCount {
		domainFrequency = collapsedRowCount
	}
	rawEncodedLabel := "substring(" + encoded + ", 3)"
	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT multiIf(")
	sql.WriteString(encoded)
	sql.WriteString(" = '1:', toUInt8(1), ")
	sql.WriteString(encoded)
	sql.WriteString(" = '2:', toUInt8(2), toUInt8(0)) AS sort_kind, ")
	sql.WriteString("if(startsWith(")
	sql.WriteString(encoded)
	sql.WriteString(", '0:'), ")
	sql.WriteString(splunkSeriesLabelSQL(rawEncodedLabel))
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString(" WHERE ")
	sql.WriteString(encoded)
	sql.WriteString(" != '' AND ")
	sql.WriteString(domainFrequency)
	sql.WriteString(" > 0 GROUP BY ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(")))) AS names FROM ")
	sql.WriteString(domainRows)
	sql.WriteString("), ")

	sql.WriteString(bucketMaps)
	mapValue := collapsedCount
	// The empty String is outside the public encoded domain (0:/1:/2:) and is
	// therefore a private per-bucket validation key. Carrying the combined flag
	// in the existing map removes a third collapsed consumer without adding a
	// second public projection or losing invalid-only buckets. The executor
	// buffers the complete fixed grid before publishing, so any nonzero bucket
	// still rejects the result atomically.
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", mapFromArrays(")
	sql.WriteString("arrayPushBack(groupArrayIf(")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(" != ''), CAST('' AS String)), ")
	sql.WriteString("arrayPushBack(groupArrayIf(")
	sql.WriteString(mapValue)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(" != ''), ")
	sql.WriteString("toUInt64(max(")
	sql.WriteString(invalid)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(collision)
	sql.WriteString(" != 0)))) AS ")
	sql.WriteString(countMap)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(gridSpec.gridSQL(ordinal, bucketNumber))
	sql.WriteString(") ")

	sql.WriteString("SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	gridSpec.writeBucketProjection(&sql, grid, bucketNumber)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" = 0, ")
	sql.WriteString(domain)
	sql.WriteString(".names, CAST([], 'Array(String)')) AS ")
	sql.WriteString(q(TimechartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(countMap)
	sql.WriteString("[name], toUInt64(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(TimechartCountsColumn))
	sql.WriteString(", ")
	sql.WriteString("toUInt8(ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(countMap)
	sql.WriteString("[''], toUInt64(0)) != 0) AS ")
	sql.WriteString(q(TimechartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(domain)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(bucketMaps)
	sql.WriteString(" ON ")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = gridSpec.appendArgs(args)
	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	classifiedDepth := relationalNodeDepth(preparedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	countsDepth := relationalNodeDepth(canonicalizedDepth)
	scoredDepth := relationalNodeDepth(countsDepth)
	rankedDepth := relationalNodeDepth(scoredDepth)
	collapsedDepth := relationalNodeDepth(rankedDepth)
	domainRowsDepth := relationalNodeDepth(collapsedDepth)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	bucketMapsDepth := relationalNodeDepth(collapsedDepth)
	gridDepth := gridSpec.relationalDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		domainDepth,
		bucketMapsDepth,
	)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeRuntimeWide,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			Calendar:      gridSpec.isCalendar(),
			BucketCount:   operator.BucketCount,
			MaxSeries:     dynamic.MaxSeries,
			MaxLabelBytes: maxTimechartLabelBytes,
			ValueKind:     TimechartValueKindInvalid,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileSplitValueTimechart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Timechart,
	valueKind TimechartValueKind,
	timeField fieldState,
	splitField fieldState,
	splitExistsSQL string,
	splitValueTypeSQL string,
	measureInputSQL string,
	measureArgs []any,
	dynamic *plan.DynamicSeriesOutput,
	gridSpec timechartGridSpec,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Split == nil || dynamic == nil {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse split value timechart: contract is required",
		)
	}
	if !valueKind.Valid() {
		return CompiledQuery{}, errors.New(
			"compile ClickHouse split value timechart: value kind is invalid",
		)
	}

	q := quoteIdentifier
	source := q("__os_timechart_source")
	prepared := q("__os_timechart_prepared")
	classified := q("__os_timechart_classified")
	canonicalized := q("__os_timechart_canonicalized")
	numericGroups := q("__os_timechart_numeric_groups")
	numericScores := q("__os_timechart_numeric_scores")
	collapsed := q("__os_timechart_collapsed")
	finalized := q("__os_timechart_finalized")
	domainRows := q("__os_timechart_domain_rows")
	domain := q("__os_timechart_domain")
	collisions := q("__os_timechart_normalization_collisions")
	bucketMaps := q("__os_timechart_bucket_maps")
	validation := q("__os_timechart_validation")
	grid := q("__os_timechart_grid")

	eventTime := q("__os_tc_event_time")
	value := q("__os_tc_value")
	present := q("__os_tc_present")
	descendant := q("__os_tc_descendant")
	valueType := q("__os_tc_value_type")
	ticks := q("__os_tc_ticks")
	label := q("__os_tc_label")
	measureValues := q("__os_tc_measure_values")
	bucketNumber := q("__os_tc_bucket_number")
	kind := q("__os_tc_kind")
	numerator := q("__os_tc_numerator")
	denominator := q("__os_tc_denominator")
	frequency := q("__os_tc_count")
	numericState := q("__os_tc_numeric_state")
	percentileState := q("__os_tc_percentile_state")
	percentileValues := q("__os_tc_percentile_values")
	score := q("__os_tc_score")
	encoded := q("__os_tc_encoded")
	measureValue := q("__os_tc_measure_value")
	normalized := q("__os_tc_normalized")
	collision := q("__os_tc_collision")
	sortLabel := q("__os_tc_sort_label")
	valueMap := q("__os_tc_value_map")
	presentMap := q("__os_tc_present_map")
	invalid := q("__os_tc_invalid")
	ordinal := q(TimechartOrdinalColumn)

	bucketNumberExpression := gridSpec.bucketKeySQL(eventTime, ticks)
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"

	var scoreSQL string
	var publishSQL string
	switch valueKind {
	case TimechartValueKindPercentile:
		scoreSQL = "sum(ifNull(arrayElementOrNull(finalizeAggregation(" +
			percentileState + "), 1), toFloat64(0)))"
		publishSQL = "arrayElementOrNull(" + percentileValues + ", 1)"
	case TimechartValueKindSum:
		scoreSQL = "sum(if(" + denominator + " = 0, toFloat64(0), " + numerator + "))"
		publishSQL = "if(" + denominator + " = 0, CAST(NULL AS Nullable(Float64)), " + numerator + ")"
	case TimechartValueKindAverage:
		bucketAverage := numerator + " / toFloat64(" + denominator + ")"
		scoreSQL = "sum(if(" + denominator + " = 0, toFloat64(0), " + bucketAverage + "))"
		publishSQL = "if(" + denominator + " = 0, CAST(NULL AS Nullable(Float64)), " + bucketAverage + ")"
	}

	// Source-select bind markers precede the nested relation text. Descendant
	// detection lives in the next CTE and therefore follows every source marker.
	prefixArgs := make([]any, 0, len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	hasDescendant := splitField.kind == fieldKindDynamic && splitField.descendantSQL != ""
	if hasDescendant {
		args = append(args, splitField.descendantArgs...)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 12_288)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(timeField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(eventTime)
	sql.WriteString(", ")
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(splitExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(splitValueTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValues)
	for _, column := range pivotDescendantSourceColumns(state, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	if hasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("reinterpretAsInt64(")
	sql.WriteString(eventTime)
	sql.WriteString(") AS ")
	sql.WriteString(ticks)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', ")
	sql.WriteString("assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumberExpression)
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString("multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	sql.WriteString(numericGroups)
	if valueKind == TimechartValueKindPercentile {
		// Retain the GK aggregate state for every raw bucket/split group. Ordinary
		// series scoring finalizes each state independently, while OTHER merges
		// the omitted states before finalization so it remains a true percentile
		// of the combined member population.
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		eligibleValues := "if(" + kind + " IN (0, 1), " + measureValues + ", CAST([], 'Array(Float64)'))"
		sql.WriteString("quantilesGKOrNullArrayState(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(eligibleValues)
		sql.WriteString(") AS ")
		sql.WriteString(percentileState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")
	} else {
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 1) AS ")
		sql.WriteString(numerator)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 2)) AS ")
		sql.WriteString(denominator)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		// One mergeable aggregate consumes each normalized immediate-member array
		// exactly once and retains both the Float64 numerator and member count. This
		// avoids ARRAY JOIN and prevents repeated Dynamic-array normalization.
		sql.WriteString("sumCountArray(")
		sql.WriteString(measureValues)
		sql.WriteString(") AS ")
		sql.WriteString(numericState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(bucketNumber)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(") AS ")
		sql.WriteString(q("__os_timechart_numeric_state_source"))
		sql.WriteString("), ")
	}

	sql.WriteString(numericScores)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(scoreSQL)
	sql.WriteString(" AS ")
	sql.WriteString(score)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(label)
	sql.WriteString(" ORDER BY ")
	// Splunk does not specify computed non-finite score ordering. Pin a stable
	// boundary: +Inf, finite descending, -Inf, NaN, then raw label lexical order.
	sql.WriteString("multiIf(isNaN(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(0), isInfinite(")
	sql.WriteString(score)
	sql.WriteString(") AND ")
	sql.WriteString(score)
	sql.WriteString(" < 0, toUInt8(1), isInfinite(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(3), toUInt8(2)) DESC, ")
	sql.WriteString("if(isFinite(")
	sql.WriteString(score)
	sql.WriteString("), ")
	sql.WriteString(score)
	sql.WriteString(", toFloat64(0)) DESC, ")
	sql.WriteString(label)
	sql.WriteString(" ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.Split.SeriesLimit), 10))
	sql.WriteString("), ")

	// usenull=false excludes the missing/null rows before they are collapsed;
	// useother=false keeps only the selected ordinary labels, so neither
	// sentinel encoding can appear.
	selectedLabel := label + " IN (SELECT " + label + " FROM " + numericScores + ")"
	collapsedKinds := kind + " IN (0, 1)"
	if !operator.Split.IncludeNull {
		collapsedKinds = kind + " = 0"
	}
	if !operator.Split.IncludeOther {
		collapsedKinds += " AND (" + kind + " != 0 OR " + selectedLabel + ")"
	}
	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", multiIf(")
	sql.WriteString(kind)
	sql.WriteString(" = 1, '1:', ")
	sql.WriteString(selectedLabel)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString("), '2:') AS ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	if valueKind == TimechartValueKindPercentile {
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString("quantilesGKOrNullArrayMerge(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(percentileState)
		sql.WriteString(") AS ")
		sql.WriteString(percentileValues)
	} else {
		sql.WriteString("sum(")
		sql.WriteString(numerator)
		sql.WriteString(") AS ")
		sql.WriteString(numerator)
		sql.WriteString(", sum(")
		sql.WriteString(denominator)
		sql.WriteString(") AS ")
		sql.WriteString(denominator)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(collapsedKinds)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	sql.WriteString(finalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	sql.WriteString(publishSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValue)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString("), ")

	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	if operator.Split.IncludeNull {
		sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM ")
		sql.WriteString(numericGroups)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 1 LIMIT 1)")
	}
	if operator.Split.IncludeOther {
		sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM ")
		sql.WriteString(numericGroups)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND ")
		sql.WriteString(label)
		sql.WriteString(" NOT IN (SELECT ")
		sql.WriteString(label)
		sql.WriteString(" FROM ")
		sql.WriteString(numericScores)
		sql.WriteString(") LIMIT 1)")
	}
	sql.WriteString("), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(")))) AS names FROM ")
	sql.WriteString(domainRows)
	sql.WriteString("), ")

	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(collision)
	sql.WriteString(" FROM (")
	sql.WriteString("SELECT ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(normalized)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(normalized)
	sql.WriteString(" HAVING uniqExact(")
	sql.WriteString(label)
	sql.WriteString(") > 1 LIMIT 1)), ")

	sql.WriteString(bucketMaps)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(ifNull(")
	sql.WriteString(measureValue)
	sql.WriteString(", toFloat64(0)))) AS ")
	sql.WriteString(valueMap)
	sql.WriteString(", ")
	sql.WriteString("mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(toUInt8(isNotNull(")
	sql.WriteString(measureValue)
	sql.WriteString(")))) AS ")
	sql.WriteString(presentMap)
	sql.WriteString(" FROM ")
	sql.WriteString(finalized)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(sumIf(")
	sql.WriteString(frequency)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(" = 3) > 0) AS ")
	sql.WriteString(invalid)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(gridSpec.gridSQL(ordinal, bucketNumber))
	sql.WriteString(") ")

	sql.WriteString("SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	gridSpec.writeBucketProjection(&sql, grid, bucketNumber)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" = 0, ")
	sql.WriteString(domain)
	sql.WriteString(".names, CAST([], 'Array(String)')) AS ")
	sql.WriteString(q(TimechartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(valueMap)
	sql.WriteString("[name], toFloat64(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(TimechartValuesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(presentMap)
	sql.WriteString("[name], toUInt8(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(TimechartValuePresentColumn))
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(validation)
	sql.WriteString(".")
	sql.WriteString(invalid)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(collisions)
	sql.WriteString(".")
	sql.WriteString(collision)
	sql.WriteString(" != 0) AS ")
	sql.WriteString(q(TimechartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(domain)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(validation)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(collisions)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(bucketMaps)
	sql.WriteString(" ON ")
	sql.WriteString(bucketMaps)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = gridSpec.appendArgs(args)
	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	classifiedDepth := relationalNodeDepth(preparedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	var numericGroupsDepth int
	if valueKind == TimechartValueKindPercentile {
		numericGroupsDepth = relationalNodeDepth(canonicalizedDepth)
	} else {
		numericStateDepth := relationalNodeDepth(canonicalizedDepth)
		numericGroupsDepth = relationalNodeDepth(numericStateDepth)
	}
	numericScoresDepth := relationalNodeDepth(numericGroupsDepth)
	scoreMembershipDepth := relationalNodeDepth(numericScoresDepth)
	collapsedDepth := relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth)
	finalizedDepth := relationalNodeDepth(collapsedDepth)

	domainScoreBranchDepth := relationalNodeDepth(numericScoresDepth)
	domainNullInputDepth := relationalNodeDepth(numericGroupsDepth)
	domainNullBranchDepth := relationalNodeDepth(domainNullInputDepth)
	domainOtherInputDepth := relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth)
	domainOtherBranchDepth := relationalNodeDepth(domainOtherInputDepth)
	domainRowsDepth := relationalNodeDepth(
		domainScoreBranchDepth,
		domainNullBranchDepth,
		domainOtherBranchDepth,
	)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	collisionInputDepth := relationalNodeDepth(numericGroupsDepth)
	collisionsDepth := relationalNodeDepth(collisionInputDepth)
	bucketMapsDepth := relationalNodeDepth(finalizedDepth)
	validationDepth := relationalNodeDepth(numericGroupsDepth)
	gridDepth := gridSpec.relationalDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		domainDepth,
		validationDepth,
		collisionsDepth,
		bucketMapsDepth,
	)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeRuntimeWideValue,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			Calendar:      gridSpec.isCalendar(),
			BucketCount:   operator.BucketCount,
			MaxSeries:     dynamic.MaxSeries,
			MaxLabelBytes: maxTimechartLabelBytes,
			ValueKind:     valueKind,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func validateTimechartMeasure(operator *plan.Timechart, state compileState) error {
	if operator == nil {
		return errors.New("compile ClickHouse timechart: operator is required")
	}
	measure := operator.Measure
	if err := validateNonStatsAggregateMeasureMetadata("timechart", measure); err != nil {
		return err
	}
	if measure.Predicate != nil {
		return errors.New(
			"compile ClickHouse timechart: aggregate measure contains predicate metadata",
		)
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if measure.Input.Name != "" || measure.Input.Canonical ||
			measure.Input.Path != nil || measure.Input.Range != (spl.Range{}) ||
			measure.Percentile != 0 || measure.Output != "count" {
			return errors.New(
				"compile ClickHouse timechart: count measure contract is invalid",
			)
		}
	case plan.AggregateFunctionCountValues:
		if operator.Split != nil &&
			operator.Split.Field.Name == measure.Input.Name {
			return errors.New(
				"compile ClickHouse timechart: aggregate input and split field must differ",
			)
		}
		if measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse timechart: count(field) contains percentile metadata",
			)
		}
		return validateTimechartFieldMeasure(measure, state, operator.Range)
	case plan.AggregateFunctionPercentile:
		if operator.Split != nil &&
			operator.Split.Field.Name == measure.Input.Name {
			return errors.New(
				"compile ClickHouse timechart: aggregate input and split field must differ",
			)
		}
		if measure.Percentile < 1 || measure.Percentile > 99 {
			return errors.New(
				"compile ClickHouse timechart: percentile must be from 1 through 99",
			)
		}
		return validateTimechartFieldMeasure(measure, state, operator.Range)
	case plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage:
		if operator.Split != nil &&
			operator.Split.Field.Name == measure.Input.Name {
			return errors.New(
				"compile ClickHouse timechart: aggregate input and split field must differ",
			)
		}
		if measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse timechart: numeric aggregate contains percentile metadata",
			)
		}
		return validateTimechartFieldMeasure(measure, state, operator.Range)
	default:
		return errors.New(
			"compile ClickHouse timechart: aggregate function is unsupported",
		)
	}
	return nil
}

func validateTimechartFieldMeasure(
	measure plan.AggregateMeasure,
	state compileState,
	sourceRange spl.Range,
) error {
	if err := validateCanonicalFieldRef("timechart", "input", measure.Input); err != nil {
		return err
	}
	if _, err := plan.ResolveField(measure.Output, sourceRange); err != nil {
		return fmt.Errorf(
			"compile ClickHouse timechart: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	if measure.Output == "_time" {
		return errors.New(
			"compile ClickHouse timechart: field aggregate output contract is invalid",
		)
	}
	if state.eventRows && state.allowDynamic && measure.Input.Name == "fields" {
		return &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_TIMECHART_FIELD",
			Message: "timechart cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   measure.Input.Range,
		}
	}
	return nil
}

func fixedTimechartValueKind(function plan.AggregateFunction) (TimechartValueKind, bool) {
	switch function {
	case plan.AggregateFunctionPercentile:
		return TimechartValueKindPercentile, true
	case plan.AggregateFunctionSum:
		return TimechartValueKindSum, true
	case plan.AggregateFunctionAverage:
		return TimechartValueKindAverage, true
	default:
		return TimechartValueKindInvalid, false
	}
}

func compileFixedCountTimechart(
	relation compiledRelation,
	args []any,
	operator *plan.Timechart,
	timeField fieldState,
	outputFields []string,
	gridSpec timechartGridSpec,
	alias string,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timechart_source")
	counts := q("__os_timechart_group_counts")
	grid := q("__os_timechart_grid")
	eventTime := q("__os_tc_event_time")
	ticks := q("__os_tc_ticks")
	bucketNumber := q("__os_tc_bucket_number")
	ordinal := q(TimechartOrdinalColumn)
	count := q(TimechartCountColumn)

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 1_536)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	if gridSpec.isCalendar() {
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(timeField.valueSQL)
		sql.WriteString(" AS ")
		sql.WriteString(eventTime)
	} else {
		sql.WriteString(" AS (SELECT reinterpretAsInt64(")
		sql.WriteString(timeField.valueSQL)
		sql.WriteString(") AS ")
		sql.WriteString(ticks)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	gridEmitter := bucketCountGrid{
		counts:       counts,
		countsSource: source,
		ticks:        ticks,
		bucketNumber: bucketNumber,
		grid:         grid,
		ordinal:      ordinal,
		count:        count,
	}
	if gridSpec.isCalendar() {
		gridSpec.writeCalendarBucketCountGridSQL(&sql, gridEmitter, eventTime)
	} else {
		writeBucketCountGridSQL(&sql, gridEmitter)
	}

	args = gridSpec.appendArgs(args)
	sourceDepth := relationalNodeDepth(relation.depth)
	countsDepth := relationalNodeDepth(sourceDepth)
	gridDepth := gridSpec.relationalDepth()
	resultDepth := relationalNodeDepth(gridDepth, countsDepth)
	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(outputFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeFixedCount,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			Calendar:      gridSpec.isCalendar(),
			BucketCount:   operator.BucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileFixedCountValueTimechart(
	relation compiledRelation,
	args []any,
	operator *plan.Timechart,
	timeField fieldState,
	measureInputSQL string,
	measureArgs []any,
	outputFields []string,
	gridSpec timechartGridSpec,
	alias string,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timechart_source")
	counts := q("__os_timechart_group_counts")
	inputPresence := q("__os_timechart_input_presence")
	grid := q("__os_timechart_grid")
	eventTime := q("__os_tc_event_time")
	ticks := q("__os_tc_ticks")
	measureCount := q("__os_tc_measure_count")
	bucketNumber := q("__os_tc_bucket_number")
	upstreamPresent := q(TimechartInputPresentColumn)
	ordinal := q(TimechartOrdinalColumn)
	count := q(TimechartCountColumn)

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 2_048)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	if gridSpec.isCalendar() {
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(timeField.valueSQL)
		sql.WriteString(" AS ")
		sql.WriteString(eventTime)
	} else {
		sql.WriteString(" AS (SELECT reinterpretAsInt64(")
		sql.WriteString(timeField.valueSQL)
		sql.WriteString(") AS ")
		sql.WriteString(ticks)
	}
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureCount)
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	// The materialized bucket aggregate is the source's only consumer. Both the
	// fixed grid and the independent upstream-presence proof read this bounded
	// relation, so an all-zero count(field) result never re-runs the scoped scan.
	sql.WriteString(counts)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(gridSpec.bucketKeySQL(eventTime, ticks))
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", toUInt64(sum(toUInt128(")
	sql.WriteString(measureCount)
	sql.WriteString("))) AS ")
	sql.WriteString(count)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(inputPresence)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" FROM ")
	sql.WriteString(counts)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(gridSpec.gridSQL(ordinal, bucketNumber))
	sql.WriteString(") SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	gridSpec.writeBucketProjection(&sql, grid, bucketNumber)
	sql.WriteString(", ifNull(")
	sql.WriteString(counts)
	sql.WriteString(".")
	sql.WriteString(count)
	sql.WriteString(", toUInt64(0)) AS ")
	sql.WriteString(count)
	sql.WriteString(", ")
	sql.WriteString(inputPresence)
	sql.WriteString(".")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" AS ")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(inputPresence)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(counts)
	sql.WriteString(" ON ")
	sql.WriteString(counts)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = prependArguments(measureArgs, args)
	args = gridSpec.appendArgs(args)
	sourceDepth := relationalNodeDepth(relation.depth)
	countsDepth := relationalNodeDepth(sourceDepth)
	inputPresenceDepth := relationalNodeDepth(countsDepth)
	gridDepth := gridSpec.relationalDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		countsDepth,
		inputPresenceDepth,
	)
	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(outputFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeFixedFieldCount,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			Calendar:      gridSpec.isCalendar(),
			BucketCount:   operator.BucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
			ValueField:    operator.Measure.Output,
			ValueKind:     TimechartValueKindInvalid,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileFixedValueTimechart(
	relation compiledRelation,
	args []any,
	operator *plan.Timechart,
	valueKind TimechartValueKind,
	timeField fieldState,
	measureInputSQL string,
	measureArgs []any,
	outputFields []string,
	gridSpec timechartGridSpec,
	alias string,
) (CompiledQuery, error) {
	q := quoteIdentifier
	source := q("__os_timechart_source")
	aggregates := q("__os_timechart_value_groups")
	inputPresence := q("__os_timechart_input_presence")
	grid := q("__os_timechart_grid")
	eventTime := q("__os_tc_event_time")
	ticks := q("__os_tc_ticks")
	measureValues := q("__os_tc_measure_values")
	bucketNumber := q("__os_tc_bucket_number")
	measureValue := q("__os_tc_measure_value")
	upstreamPresent := q("__os_tc_input_present")
	ordinal := q(TimechartOrdinalColumn)

	var aggregateValueSQL string
	switch valueKind {
	case TimechartValueKindPercentile:
		aggregateValueSQL = singlePercentileArrayAggregateSQL(
			operator.Measure.Percentile,
			measureValues,
		)
	case TimechartValueKindSum, TimechartValueKindAverage:
		var supported bool
		aggregateValueSQL, supported = numericArrayAggregateSQL(
			operator.Measure.Function,
			measureValues,
		)
		if !supported {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse timechart: fixed value function is invalid",
			)
		}
	default:
		return CompiledQuery{}, errors.New(
			"compile ClickHouse timechart: fixed value kind is invalid",
		)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 2_048)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	if gridSpec.isCalendar() {
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(timeField.valueSQL)
		sql.WriteString(" AS ")
		sql.WriteString(eventTime)
	} else {
		sql.WriteString(" AS (SELECT reinterpretAsInt64(")
		sql.WriteString(timeField.valueSQL)
		sql.WriteString(") AS ")
		sql.WriteString(ticks)
	}
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(aggregates)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(gridSpec.bucketKeySQL(eventTime, ticks))
	sql.WriteString(" AS ")
	sql.WriteString(bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(aggregateValueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValue)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(inputPresence)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" FROM ")
	sql.WriteString(aggregates)
	sql.WriteString("), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (")
	sql.WriteString(gridSpec.gridSQL(ordinal, bucketNumber))
	sql.WriteString(") SELECT ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	gridSpec.writeBucketProjection(&sql, grid, bucketNumber)
	sql.WriteString(", ")
	sql.WriteString(aggregates)
	sql.WriteString(".")
	sql.WriteString(measureValue)
	sql.WriteString(" AS ")
	sql.WriteString(q(TimechartValueColumn))
	sql.WriteString(", ")
	sql.WriteString(inputPresence)
	sql.WriteString(".")
	sql.WriteString(upstreamPresent)
	sql.WriteString(" AS ")
	sql.WriteString(q(TimechartInputPresentColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(grid)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(inputPresence)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(aggregates)
	sql.WriteString(" ON ")
	sql.WriteString(aggregates)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	args = prependArguments(measureArgs, args)
	args = gridSpec.appendArgs(args)
	sourceDepth := relationalNodeDepth(relation.depth)
	aggregatesDepth := relationalNodeDepth(sourceDepth)
	inputPresenceDepth := relationalNodeDepth(aggregatesDepth)
	gridDepth := gridSpec.relationalDepth()
	resultDepth := relationalNodeDepth(
		gridDepth,
		aggregatesDepth,
		inputPresenceDepth,
	)
	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(outputFields),
		Timechart: &TimechartOutput{
			Mode:          TimechartModeFixedValue,
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			Calendar:      gridSpec.isCalendar(),
			BucketCount:   operator.BucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
			ValueField:    operator.Measure.Output,
			ValueKind:     valueKind,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

// splunkSeriesLabelSQL applies Splunk's leading-underscore VALUE prefix. Series
// labels become public column names, so the prefix decides both their sort
// position and their collision domain. Row values are data and are never
// normalized this way.
func splunkSeriesLabelSQL(label string) string {
	return "if(startsWith(" + label + ", '_'), concat('VALUE', " + label + "), " + label + ")"
}

// chartRowColumnType maps a resolved row field to the exact physical type and
// public value kind of the pivot's first column. It mirrors the group column
// that stats BY publishes for the same field: runtime-typed values converge on
// their lexical scalar text, while fixed columns keep their own scalar type.
// The public name participates because the ordinary result path derives _raw's
// kind from the name as well.
func chartRowColumnType(_ string, field fieldState) (databaseType string, kind ChartRowKind, err error) {
	switch field.kind {
	case fieldKindInvalid, fieldKindString, fieldKindDynamic:
		if field.stringOrBytes {
			// stats count BY _raw publishes a Mixed, nullable column because
			// _raw may hold non-UTF-8 bytes. The pivot's first column is that
			// same group column, so it declares the same kind.
			return "String", ChartRowKindMixed, nil
		}
		// A statically null column (eval x=null) is the String group column
		// stats BY publishes: it never produces a present, non-null value, so
		// it names no rows rather than failing the search.
		return "String", ChartRowKindString, nil
	case fieldKindBool:
		return "Bool", ChartRowKindBool, nil
	case fieldKindTime:
		return "DateTime64(9, 'UTC')", ChartRowKindTime, nil
	case fieldKindNumber:
		switch field.numberType {
		case "Int64":
			return "Int64", ChartRowKindSigned, nil
		case "UInt8", "UInt64":
			return field.numberType, ChartRowKindUnsigned, nil
		case "Float64":
			return "Float64", ChartRowKindDouble, nil
		}
	}
	return "", ChartRowKindInvalid, fmt.Errorf("compile ClickHouse chart: row field has an unsupported fixed type %d/%q", field.kind, field.numberType)
}

// chartValidationRowSQL produces one non-null value of the compiler-validated
// row transport type. It is used only by the private invalid-result sentinel:
// when the row axis is empty, a bad split label still has to cross the storage
// boundary so the executor can reject the whole command before publication.
func chartValidationRowSQL(databaseType string) string {
	if databaseType == "String" {
		return "CAST('' AS String)"
	}
	return "CAST(0 AS " + databaseType + ")"
}

// materializedCTESettingsSQL declares the requirement a lowering takes on when
// it reads an aggregate through a CTE declared `AS MATERIALIZED`. ClickHouse
// honors that declaration only while enable_materialized_cte is on; with it off
// the server silently inlines every reference, which re-runs the whole scoped
// scan once per reference and additionally exposes the analyzer to a
// cross-subquery expression-alias defect that drops a column outright ("Not
// found column multiIf(...)") whenever a search predicate shares expressions
// with the pivot's own projections — for example a comparison on the very field
// that names the column axis. Carrying the setting in the query text keeps the
// compiled SQL correct on any connection rather than only under one caller's
// per-query settings, and it stays correct when the query is wrapped in an
// outer SELECT.
const materializedCTESettingsSQL = " SETTINGS enable_materialized_cte = 1"

// compileChart lowers the bounded runtime-wide pivot. Both axes are runtime
// data, so the scoped scan feeds exactly two aggregations: a one-dimensional
// label aggregate that chooses the published column domain, and a row-keyed
// aggregate whose column axis is already collapsed to that domain. Every later
// stage reads one of those materialized aggregates as an ordinary relation —
// never through a scalar subquery, which ClickHouse evaluates during analysis
// and would therefore re-run the whole scoped scan.
func compileChart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: operator is required")
	}
	if err := validateNonStatsAggregateMeasureMetadata("chart", operator.Measure); err != nil {
		return CompiledQuery{}, err
	}
	switch operator.Measure.Function {
	case plan.AggregateFunctionCountRows, plan.AggregateFunctionCountValues:
		return compileCountChart(relation, state, args, operator, dynamic, alias)
	case plan.AggregateFunctionPercentile,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage:
		return compileNumericChart(relation, state, args, operator, dynamic, alias)
	default:
		return CompiledQuery{}, errors.New("compile ClickHouse chart: aggregate function is unsupported")
	}
}

// resolveChartAxes revalidates the chart bounding contract and resolves the row
// and column axis fields shared by the count and numeric chart compilers.
func resolveChartAxes(
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	state compileState,
) (fieldState, string, ChartRowKind, fieldState, error) {
	rowName := operator.Over.Name
	if dynamic == nil || !slices.Equal(dynamic.FixedFields, []string{rowName}) || dynamic.MaxSeries == 0 {
		return fieldState{}, "", 0, fieldState{}, errors.New("compile ClickHouse chart: dynamic output contract is invalid")
	}
	// The plan carries the complete bounding contract as data precisely so the
	// backend can revalidate it before emitting SQL.
	if rowName == "" || operator.SplitBy.Name == "" || rowName == operator.SplitBy.Name ||
		operator.RowLimit != maxChartRowValues || operator.SeriesLimit != 10 ||
		dynamic.MaxSeries != 12 || uint32(operator.SeriesLimit)+2 != uint32(dynamic.MaxSeries) ||
		!operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" {
		return fieldState{}, "", 0, fieldState{}, errors.New("compile ClickHouse chart: bounded defaults are invalid")
	}
	for _, axis := range []plan.FieldRef{operator.Over, operator.SplitBy} {
		if err := validateCanonicalFieldRef("chart", "axis", axis); err != nil {
			return fieldState{}, "", 0, fieldState{}, err
		}
		if axis.Name == operator.NullLabel || axis.Name == operator.OtherLabel {
			return fieldState{}, "", 0, fieldState{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "NULL and OTHER are reserved chart series names",
				Range:   axis.Range,
			}
		}
		if state.eventRows && state.allowDynamic && axis.Name == "fields" {
			return fieldState{}, "", 0, fieldState{}, &plan.Diagnostic{
				Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
				Message: "chart cannot use the event result's reserved fields payload without an exact upstream schema",
				Range:   axis.Range,
			}
		}
	}

	rowField, rowResolved, err := resolveCompiledField(operator.Over, state)
	if err != nil {
		return fieldState{}, "", 0, fieldState{}, err
	}
	if !rowResolved {
		// An upstream projection removed the row field, so no row value is
		// present. stats BY emits no groups in that case; keep the declared
		// one-column schema instead of resurrecting the private document.
		rowField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if isNativeMultivalueKind(rowField.kind) {
		return fieldState{}, "", 0, fieldState{}, unsupportedMultivalueUsage("chart row field", operator.Over.Range)
	}
	rowDatabaseType, rowKind, err := chartRowColumnType(rowName, rowField)
	if err != nil {
		return fieldState{}, "", 0, fieldState{}, err
	}
	if rowKind == ChartRowKindMixed && rowField.semanticBytesSQL == "" {
		return fieldState{}, "", 0, fieldState{}, errors.New(
			"compile ClickHouse chart: Mixed row lacks semantic Bytes provenance",
		)
	}

	splitField, splitResolved, err := resolveCompiledField(operator.SplitBy, state)
	if err != nil {
		return fieldState{}, "", 0, fieldState{}, err
	}
	if !splitResolved {
		// A projected-away column field is missing for every retained row, so
		// the documented usenull=true default produces one NULL column.
		splitField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if isNativeMultivalueKind(splitField.kind) {
		return fieldState{}, "", 0, fieldState{}, unsupportedMultivalueUsage("chart column field", operator.SplitBy.Range)
	}
	if splitField.kind == fieldKindInvalid {
		// A statically null column field (eval x=null) is inside the documented
		// column domain "string column values plus missing/explicit-null": it
		// carries no present, non-null value on any row, exactly like the
		// projected-away field above. fieldKindInvalid is unconditionally null
		// everywhere else in the compiler too — its stored semantic type is the
		// constant Null — so the pivot reads it as the same typed NULL and
		// publishes one usenull=true NULL series instead of failing the search.
		splitField = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  splitField.existsSQL,
			existsArgs: splitField.existsArgs,
			kind:       fieldKindString,
		}
	}
	if splitField.kind != fieldKindString && splitField.kind != fieldKindDynamic {
		return fieldState{}, "", 0, fieldState{}, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
			Message:     "chart column fields currently support strings plus missing and null values",
			Range:       operator.SplitBy.Range,
			Suggestions: []string{"convert the column field to a string before chart"},
		}
	}
	return rowField, rowDatabaseType, rowKind, splitField, nil
}

func compileCountChart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Measure.Predicate != nil {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: count operator is required")
	}
	fieldOccurrenceCount := operator.Measure.Function == plan.AggregateFunctionCountValues
	switch operator.Measure.Function {
	case plan.AggregateFunctionCountRows:
		if operator.Measure.Input.Name != "" || operator.Measure.Input.Canonical ||
			operator.Measure.Input.Path != nil || operator.Measure.Input.Range != (spl.Range{}) ||
			operator.Measure.Percentile != 0 || operator.Measure.Output != "count" {
			return CompiledQuery{}, errors.New("compile ClickHouse chart: row count contract is invalid")
		}
	case plan.AggregateFunctionCountValues:
		if operator.Measure.Percentile != 0 ||
			operator.Measure.Output != "count("+operator.Measure.Input.Name+")" ||
			operator.Measure.Input.Name == operator.Over.Name ||
			!spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) {
			return CompiledQuery{}, errors.New("compile ClickHouse chart: field count contract is invalid")
		}
		if err := validateCanonicalFieldRef("chart", "input", operator.Measure.Input); err != nil {
			return CompiledQuery{}, err
		}
		if state.eventRows && state.allowDynamic && operator.Measure.Input.Name == "fields" {
			return CompiledQuery{}, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_CHART_FIELD",
				Message: "chart cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   operator.Measure.Input.Range,
			}
		}
	default:
		return CompiledQuery{}, errors.New("compile ClickHouse chart: count operator is required")
	}
	rowName := operator.Over.Name
	rowField, rowDatabaseType, rowKind, splitField, err := resolveChartAxes(operator, dynamic, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	measureInputSQL := ""
	var measureArgs []any
	if fieldOccurrenceCount {
		var resolveErr error
		measureInputSQL, measureArgs, resolveErr = resolveCountValueInput(
			operator.Measure.Input,
			state,
		)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
	}

	rowExistsSQL := rowField.existsSQL
	if rowExistsSQL == "" {
		rowExistsSQL = "1"
	}
	splitExistsSQL := splitField.existsSQL
	if splitExistsSQL == "" {
		splitExistsSQL = "1"
	}
	rowDynamic := rowField.kind == fieldKindDynamic
	splitDynamic := splitField.kind == fieldKindDynamic
	rowHasDescendant := rowDynamic && rowField.descendantSQL != ""
	splitHasDescendant := splitDynamic && splitField.descendantSQL != ""

	q := quoteIdentifier
	source := q("__os_chart_source")
	prepared := q("__os_chart_prepared")
	kinded := q("__os_chart_kinded")
	classified := q("__os_chart_classified")
	canonicalized := q("__os_chart_canonicalized")
	labelTotals := q("__os_chart_label_totals")
	counts := q("__os_chart_group_counts")
	top := q("__os_chart_top")
	collapsed := q("__os_chart_collapsed")
	labelGroups := q("__os_chart_label_groups")
	normalizedGroups := q("__os_chart_normalized_groups")
	authority := q("__os_chart_authority")
	labelExpanded := q("__os_chart_label_expanded")
	expanded := q("__os_chart_expanded")
	domainRows := q("__os_chart_domain_rows")
	domain := q("__os_chart_domain")
	collisions := q("__os_chart_normalization_collisions")
	columnCheck := q("__os_chart_column_check")
	rowMaps := q("__os_chart_row_maps")
	validation := q("__os_chart_validation")
	rowDomain := q("__os_chart_row_domain")

	rowValue := q("__os_ch_row_value")
	rowSemanticBytes := q("__os_ch_row_semantic_bytes")
	rowExact := q("__os_ch_row_exact")
	rowType := q("__os_ch_row_type")
	rowPresent := q("__os_ch_row_present")
	rowEligible := q("__os_ch_row_eligible")
	rowSupported := q("__os_ch_row_supported")
	rowInvalid := q("__os_ch_row_invalid")
	row := q("__os_ch_row")
	value := q("__os_ch_value")
	present := q("__os_ch_present")
	descendant := q("__os_ch_descendant")
	valueType := q("__os_ch_value_type")
	label := q("__os_ch_label")
	kind := q("__os_ch_kind")
	measureCount := q("__os_ch_measure_count")
	rowCount := q("__os_ch_row_count")
	occurrenceCount := q("__os_ch_occurrence_count")
	occurrenceScore := q("__os_ch_occurrence_score")
	frequency := q("__os_ch_count")
	collapsedCount := q("__os_ch_collapsed_count")
	encoded := q("__os_ch_encoded")
	normalized := q("__os_ch_normalized")
	groupRow := q("__os_ch_group_row")
	seriesScore := q("__os_ch_series_score")
	rowEntries := q("__os_ch_row_entries")
	labelRecords := q("__os_ch_label_records")
	authorityValue := q("__os_ch_authority")
	globalCollision := q("__os_ch_global_collision")
	rowInvalidEvidence := q("__os_ch_row_invalid_evidence")
	columnInvalidEvidence := q("__os_ch_column_invalid_evidence")
	collisionEvidence := q("__os_ch_collision_evidence")
	sortLabel := q("__os_ch_sort_label")
	countMap := q("__os_ch_count_map")
	domainNames := q("__os_ch_domain_names")
	invalid := q("__os_ch_invalid")
	collision := q("__os_ch_collision")
	columnInvalid := q("__os_ch_column_invalid")
	transportInvalid := q("__os_ch_transport_invalid")
	ordinal := q(ChartOrdinalColumn)

	// Placeholder order follows CTE nesting, not declaration order. Exact
	// presence probes sit in the outer CTE that wraps the scoped fragment and
	// therefore precede every nested argument; descendant detection and the
	// reserved-column-name probe are emitted afterwards and append in the order
	// they appear.
	prefixArgs := make([]any, 0, len(rowField.existsArgs)+len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, rowField.existsArgs...)
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	if rowHasDescendant {
		args = append(args, rowField.descendantArgs...)
	}
	if splitHasDescendant {
		args = append(args, splitField.descendantArgs...)
	}
	args = append(args, rowName)

	splitTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitDynamic {
		splitTypeSQL = dynamicTypeExpression(splitField)
	}
	// The label is invalid when it cannot name a public column. The row
	// column's own name seeds that collision domain exactly as _time does for
	// timechart, because a runtime value equal to it would duplicate column 0.
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER') AND " +
		splunkSeriesLabelSQL(label) + " != ?"

	// Exact presence is materialized once in the source CTE. Re-reading the
	// column keeps each bind marker to exactly one occurrence.
	rowPresenceSQL := "(" + rowExact + " != 0 AND isNotNull(" + rowValue + "))"
	if rowHasDescendant {
		// Non-empty objects are stored as flattened leaf paths, so the parent
		// itself is absent. Retain those rows until the container check rejects
		// them explicitly rather than silently dropping a whole group.
		rowPresenceSQL = "(" + rowPresenceSQL + " OR " + rowField.descendantSQL + ")"
	}
	rowKeySQL := "CAST(assumeNotNull(" + rowValue + ") AS " + rowDatabaseType + ")"
	rowSupportedSQL := "1"
	if rowDynamic {
		// SPL groups by lexical value, so runtime scalar storage types converge
		// on the same row exactly as stats BY converges them. Unsupported
		// containers collapse into one placeholder key and raise the atomic
		// invalid flag instead of naming a row.
		runtime := fieldState{valueSQL: rowValue, dynamicTypeSQL: rowType, kind: fieldKindDynamic}
		supported, lexical := statsByScalarExpressions(runtime)
		rowSupportedSQL = supported
		rowKeySQL = "CAST(if(" + rowSupported + " != 0, " + lexical + ", '') AS String)"
	}
	if rowKind == ChartRowKindMixed {
		rowKeySQL = "tuple(" + rowKeySQL + ", " + rowSemanticBytes + ")"
	}
	rowSortSQL := row
	if rowDynamic || rowField.numericSort {
		// Automatic numeric-aware ordering: the exact order sort 0 +<field>
		// produces on the published column.
		rowSortSQL = dynamicSortValue(row, false)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + 8_192)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(rowValue)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowExact)
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString("toUInt8(ifNull(")
		sql.WriteString(rowField.semanticBytesSQL)
		sql.WriteString(", 0)) AS ")
		sql.WriteString(rowSemanticBytes)
		sql.WriteString(", ")
	}
	if rowDynamic {
		sql.WriteString(dynamicTypeExpression(rowField))
		sql.WriteString(" AS ")
		sql.WriteString(rowType)
		sql.WriteString(", ")
	}
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(splitExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(splitTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureInputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(measureCount)
	}
	for _, column := range pivotDescendantSourceColumns(state, rowField, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowPresenceSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowPresent)
	sql.WriteString(", ")
	if splitHasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("toUInt8(")
	sql.WriteString(rowSupportedSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowSupported)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', ")
	sql.WriteString("assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	// The column value is classified before row eligibility is considered. A
	// container, a non-string scalar, or an unusable label fails the whole
	// command on its own presence, exactly as compileAggregate validates each
	// BY key independently: an unsupported column value must not become
	// invisible because some other event happened to omit the row field.
	sql.WriteString(kinded)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	// Row eligibility matches stats BY exactly: only present, non-null row
	// values name a row, which is what makes the per-row totals equal
	// stats count BY <row field>. Ineligible rows are retained here so the
	// column-axis rejection above still sees them, and are dropped by the
	// row-keyed aggregation below.
	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowKeySQL)
	sql.WriteString(" AS ")
	sql.WriteString(row)
	sql.WriteString(", toUInt8(")
	sql.WriteString(rowSupported)
	sql.WriteString(" = 0) AS ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowPresent)
	sql.WriteString(" AS ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(kinded)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	if fieldOccurrenceCount {
		sql.WriteString(", ")
		sql.WriteString(measureCount)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	if fieldOccurrenceCount {
		// Field occurrence count cannot select its label domain before it knows
		// the measure totals. Materialize one bounded raw (row, label) aggregate
		// carrying both source-row frequency and a wide occurrence total; every
		// later domain, score, validation, and cell operation reads this relation.
		// This is the same one-scan topology used by numeric chart and avoids
		// materializing the unbounded per-event canonicalized relation.
		sql.WriteString(counts)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", count() AS ")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString("sum(toUInt128(")
		sql.WriteString(measureCount)
		sql.WriteString(")) AS ")
		sql.WriteString(occurrenceCount)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		sql.WriteString(labelTotals)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("sumIf(")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(rowCount)
		sql.WriteString(", ")
		sql.WriteString("sumIf(")
		sql.WriteString(occurrenceCount)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(occurrenceScore)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		sql.WriteString(top)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString(occurrenceScore)
		sql.WriteString(" FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND ")
		sql.WriteString(rowCount)
		sql.WriteString(" > 0 ORDER BY ")
		sql.WriteString(occurrenceScore)
		sql.WriteString(" DESC, ")
		sql.WriteString(label)
		sql.WriteString(" ASC LIMIT ")
		sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
		sql.WriteString("), ")

		sql.WriteString(collapsed)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", multiIf(")
		sql.WriteString(kind)
		sql.WriteString(" = 1, '1:', ")
		sql.WriteString(label)
		sql.WriteString(" IN (SELECT ")
		sql.WriteString(label)
		sql.WriteString(" FROM ")
		sql.WriteString(top)
		sql.WriteString("), concat('0:', ")
		sql.WriteString(label)
		sql.WriteString("), '2:') AS ")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(")
		sql.WriteString(occurrenceCount)
		sql.WriteString("))) AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		sql.WriteString(" WHERE ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0 AND ")
		sql.WriteString(kind)
		sql.WriteString(" IN (0, 1) GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString("), ")
	} else {
		// Bound the exact raw (row, label) work before any array retains label
		// state. max_rows_to_group_by therefore fails the whole query at the
		// executor's 130k chart allowance instead of letting attacker-controlled
		// label cardinality hide inside an unbounded aggregate array.
		sql.WriteString(counts)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString("max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		// Every array below is downstream of the materialized raw-pair bound. Each
		// raw group enters exactly one rowEntries array; later arrays only nest
		// those disjoint arrays and therefore retain at most the raw group count.
		// Zero-score labels remain in the normalization domain, while only positive
		// row-eligible UInt128 scores choose the public top ten.
		sql.WriteString(labelGroups)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", sumIf(toUInt128(")
		sql.WriteString(frequency)
		sql.WriteString("), ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(seriesScore)
		sql.WriteString(", ")
		sql.WriteString("groupArray(tuple(")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(")) AS ")
		sql.WriteString(rowEntries)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")

		sql.WriteString(normalizedGroups)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(splunkSeriesLabelSQL(label))
		sql.WriteString(" AS ")
		sql.WriteString(normalized)
		sql.WriteString(", ")
		sql.WriteString("groupArray(tuple(")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString(seriesScore)
		sql.WriteString(", ")
		sql.WriteString(rowEntries)
		sql.WriteString(")) AS ")
		sql.WriteString(labelRecords)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND count() > 1) AS ")
		sql.WriteString(collisionEvidence)
		sql.WriteString(" FROM ")
		sql.WriteString(labelGroups)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(normalized)
		sql.WriteString("), ")

		recordsName := "__os_ch_records"
		recordName := "__os_ch_record"
		topName := "__os_ch_top_label_values"
		collisionName := "__os_ch_collision_flag"
		recordsAggregate := "arrayFlatten(groupArray(" + labelRecords + "))"
		positiveRecords := "arrayFilter(" + recordName + " -> " + recordName + ".1 = toUInt8(0) AND " + recordName + ".3 > toUInt128(0), " + recordsName + ")"
		topRecords := "arraySlice(arraySort(" + recordName + " -> tuple(-toInt256(" + recordName + ".3), " + recordName + ".2), " + positiveRecords + "), 1, " + strconv.FormatUint(uint64(operator.SeriesLimit), 10) + ")"
		topLabelValues := "arrayMap(" + recordName + " -> " + recordName + ".2, " + topRecords + ")"
		authorizedRecords := "arrayMap(" + recordName + " -> tuple(" + recordName + ".1, " + recordName + ".4, multiIf(" +
			recordName + ".1 = toUInt8(1), CAST('1:' AS String), " +
			recordName + ".1 = toUInt8(0) AND has(" + topName + ", " + recordName + ".2), concat('0:', " + recordName + ".2), " +
			recordName + ".1 = toUInt8(0), CAST('2:' AS String), CAST('' AS String))), " + recordsName + ")"
		authorityExpression := "arrayElement(arrayMap((" + recordsName + ", " + collisionName + ") -> arrayElement(arrayMap(" + topName + " -> tuple(" +
			authorizedRecords + ", " + collisionName + "), [" + topLabelValues + "]), 1), [" + recordsAggregate + "], [toUInt8(maxOrDefault(" + collisionEvidence + ") != 0)]), 1)"
		sql.WriteString(authority)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(authorityExpression)
		sql.WriteString(" AS ")
		sql.WriteString(authorityValue)
		sql.WriteString(" FROM ")
		sql.WriteString(normalizedGroups)
		sql.WriteString("), ")

		labelRecord := q("__os_ch_label_record")
		sql.WriteString(labelExpanded)
		sql.WriteString(" AS (SELECT tupleElement(")
		sql.WriteString(labelRecord)
		sql.WriteString(", 1) AS ")
		sql.WriteString(kind)
		sql.WriteString(", tupleElement(")
		sql.WriteString(labelRecord)
		sql.WriteString(", 2) AS ")
		sql.WriteString(rowEntries)
		sql.WriteString(", ")
		sql.WriteString("tupleElement(")
		sql.WriteString(labelRecord)
		sql.WriteString(", 3) AS ")
		sql.WriteString(encoded)
		sql.WriteString(", tupleElement(")
		sql.WriteString(authorityValue)
		sql.WriteString(", 2) AS ")
		sql.WriteString(globalCollision)
		sql.WriteString(" FROM ")
		sql.WriteString(authority)
		sql.WriteString(" ARRAY JOIN tupleElement(")
		sql.WriteString(authorityValue)
		sql.WriteString(", 1) AS ")
		sql.WriteString(labelRecord)
		sql.WriteString("), ")

		rowEntry := q("__os_ch_row_entry")
		sql.WriteString(expanded)
		sql.WriteString(" AS (SELECT tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 1) AS ")
		sql.WriteString(row)
		sql.WriteString(", tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 2) AS ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString("tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 3) AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", tupleElement(")
		sql.WriteString(rowEntry)
		sql.WriteString(", 4) AS ")
		sql.WriteString(frequency)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString(globalCollision)
		sql.WriteString(" FROM ")
		sql.WriteString(labelExpanded)
		sql.WriteString(" ARRAY JOIN ")
		sql.WriteString(rowEntries)
		sql.WriteString(" AS ")
		sql.WriteString(rowEntry)
		sql.WriteString("), ")

		public := rowEligible + " != 0 AND " + kind + " IN (0, 1)"
		typedValidationRow := chartValidationRowSQL(rowDatabaseType)
		if rowKind == ChartRowKindMixed {
			typedValidationRow = "tuple(" + typedValidationRow + ", toUInt8(0))"
		}
		sql.WriteString(collapsed)
		sql.WriteString(" AS (SELECT if(")
		sql.WriteString(public)
		sql.WriteString(", ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(typedValidationRow)
		sql.WriteString(") AS ")
		sql.WriteString(groupRow)
		sql.WriteString(", ")
		sql.WriteString("if(")
		sql.WriteString(public)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString(", CAST('' AS String)) AS ")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(sum(toUInt128(if(")
		sql.WriteString(public)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(", 0)))) AS ")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(maxIf(")
		sql.WriteString(rowInvalid)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(" != 0) > 0) AS ")
		sql.WriteString(rowInvalidEvidence)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(sumIf(toUInt128(")
		sql.WriteString(frequency)
		sql.WriteString("), ")
		sql.WriteString(kind)
		sql.WriteString(" = 3) > 0) AS ")
		sql.WriteString(columnInvalidEvidence)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(")
		sql.WriteString(globalCollision)
		sql.WriteString(") > 0) AS ")
		sql.WriteString(collisionEvidence)
		sql.WriteString(" FROM ")
		sql.WriteString(expanded)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(groupRow)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString("), ")
	}

	if fieldOccurrenceCount {
		// Both sentinels probe the materialized label aggregate as ordinary
		// relations. A scalar subquery would be evaluated during analysis, before
		// the materialized temporary table exists, and would re-run the whole
		// scoped scan once per occurrence.
		domainFrequency := frequency
		if fieldOccurrenceCount {
			domainFrequency = rowCount
		}
		sql.WriteString(domainRows)
		sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, ")
		sql.WriteString(splunkSeriesLabelSQL(label))
		sql.WriteString(" AS ")
		sql.WriteString(sortLabel)
		sql.WriteString(", concat('0:', ")
		sql.WriteString(label)
		sql.WriteString(") AS ")
		sql.WriteString(encoded)
		sql.WriteString(" FROM ")
		sql.WriteString(top)
		sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 1 AND ")
		sql.WriteString(domainFrequency)
		sql.WriteString(" > 0 LIMIT 1)")
		sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 AND ")
		sql.WriteString(domainFrequency)
		sql.WriteString(" > 0 AND ")
		sql.WriteString(label)
		sql.WriteString(" NOT IN (SELECT ")
		sql.WriteString(label)
		sql.WriteString(" FROM ")
		sql.WriteString(top)
		sql.WriteString(") LIMIT 1)), ")

		sql.WriteString(domain)
		sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
		sql.WriteString(sortLabel)
		sql.WriteString(", ")
		sql.WriteString(encoded)
		sql.WriteString(")))) AS names FROM ")
		sql.WriteString(domainRows)
		sql.WriteString("), ")

		// Convergence after VALUE normalization is one member of the same label
		// rule as the empty, invalid-UTF-8, over-long, reserved, and row-name
		// labels, and every other member is evaluated on the column value's own
		// presence. The label aggregate carries a kind = 0 group for every ordinary
		// label any classified input row held, so reading it without the
		// row-eligible frequency filter keeps the rule presence-independent: two
		// labels that converge fail the whole command even when only row-ineligible
		// events carried them, exactly as a reserved label on such an event does.
		sql.WriteString(collisions)
		sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
		sql.WriteString(collision)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(splunkSeriesLabelSQL(label))
		sql.WriteString(" AS ")
		sql.WriteString(normalized)
		sql.WriteString(" FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString(" WHERE ")
		sql.WriteString(kind)
		sql.WriteString(" = 0 GROUP BY ")
		sql.WriteString(normalized)
		sql.WriteString(" HAVING uniqExact(")
		sql.WriteString(label)
		sql.WriteString(") > 1 LIMIT 1)), ")

		// The atomic column-value rejection is row-independent by construction:
		// the label aggregate carries a kind = 3 group whenever any classified
		// input row held an unsupported column value, whether or not that row also
		// carried an eligible row value.
		sql.WriteString(columnCheck)
		sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
		sql.WriteString(kind)
		sql.WriteString(" = 3)) AS ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" FROM ")
		sql.WriteString(labelTotals)
		sql.WriteString("), ")

		sql.WriteString(rowMaps)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", mapFromArrays(groupArray(")
		sql.WriteString(encoded)
		sql.WriteString("), groupArray(")
		sql.WriteString(frequency)
		sql.WriteString(")) AS ")
		sql.WriteString(countMap)
		sql.WriteString(" FROM ")
		sql.WriteString(collapsed)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString("), ")

		sql.WriteString(validation)
		sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") > 0) AS ")
		sql.WriteString(invalid)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		if fieldOccurrenceCount {
			// Missing and explicit-null dynamic row values are not chart rows and
			// therefore cannot invalidate the row domain. Unsupported descendants
			// remain eligible by construction and still fail atomically.
			sql.WriteString(" WHERE ")
			sql.WriteString(rowEligible)
			sql.WriteString(" != 0")
		}
		sql.WriteString("), ")

		// The row axis is data, so its ordinal is assigned server-side from the
		// declared order. Only the dense ordinal proves that order to the executor;
		// the row value itself crosses the boundary as an ordinary typed column.
		sql.WriteString(rowDomain)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", toUInt64(row_number() OVER (ORDER BY ")
		sql.WriteString(rowSortSQL)
		sql.WriteString(" ASC) - 1) AS ")
		sql.WriteString(ordinal)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(row)
		sql.WriteString(" FROM ")
		sql.WriteString(counts)
		if fieldOccurrenceCount {
			sql.WriteString(" WHERE ")
			sql.WriteString(rowEligible)
			sql.WriteString(" != 0")
		}
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(")) ")

		// A private sentinel carries row-independent validation across an empty row
		// axis. It is ordered first and rejected by the buffering executor, so the
		// synthetic row and empty arrays can never become public output.
		sql.WriteString("SELECT ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		sql.WriteString(q(ChartRowColumn))
		sql.WriteString(", ")
		if rowKind == ChartRowKindMixed {
			sql.WriteString(q(ChartRowSemanticBytesColumn))
			sql.WriteString(", ")
		}
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM (")
		sql.WriteString("SELECT ")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(ordinal)
		sql.WriteString(" AS ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		rowOutputSQL := rowDomain + "." + row
		if rowKind == ChartRowKindMixed {
			rowOutputSQL = "tupleElement(" + rowOutputSQL + ", 1)"
		}
		sql.WriteString(rowOutputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartRowColumn))
		sql.WriteString(", ")
		if rowKind == ChartRowKindMixed {
			sql.WriteString("tupleElement(")
			sql.WriteString(rowDomain)
			sql.WriteString(".")
			sql.WriteString(row)
			sql.WriteString(", 2) AS ")
			sql.WriteString(q(ChartRowSemanticBytesColumn))
			sql.WriteString(", ")
		}
		sql.WriteString(domain)
		sql.WriteString(".names AS ")
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", ")
		sql.WriteString("arrayMap(name -> ifNull(")
		sql.WriteString(rowMaps)
		sql.WriteString(".")
		sql.WriteString(countMap)
		sql.WriteString("[name], toUInt64(0)), ")
		sql.WriteString(domain)
		sql.WriteString(".names) AS ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", ")
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM ")
		sql.WriteString(rowDomain)
		sql.WriteString(" CROSS JOIN ")
		sql.WriteString(domain)
		sql.WriteString(" LEFT JOIN ")
		sql.WriteString(rowMaps)
		sql.WriteString(" ON ")
		sql.WriteString(rowMaps)
		sql.WriteString(".")
		sql.WriteString(row)
		sql.WriteString(" = ")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(row)
		// Deterministic, non-truncating overflow: the guard runs during filtering,
		// before the ordered result is produced, so no partial pivot is published.
		sql.WriteString(" WHERE throwIf(")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(ordinal)
		sql.WriteString(" >= ")
		sql.WriteString(strconv.FormatUint(uint64(operator.RowLimit), 10))
		sql.WriteString(", '")
		sql.WriteString(ChartRowLimitMarker)
		sql.WriteString("') = 0")
		sql.WriteString(" UNION ALL SELECT toUInt64(0) AS ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		sql.WriteString(chartValidationRowSQL(rowDatabaseType))
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartRowColumn))
		if rowKind == ChartRowKindMixed {
			sql.WriteString(", toUInt8(0) AS ")
			sql.WriteString(q(ChartRowSemanticBytesColumn))
		}
		sql.WriteString(", CAST([], 'Array(String)') AS ")
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", CAST([], 'Array(UInt64)') AS ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", toUInt8(1) AS ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM ")
		sql.WriteString(validation)
		sql.WriteString(" CROSS JOIN ")
		sql.WriteString(collisions)
		sql.WriteString(" CROSS JOIN ")
		sql.WriteString(columnCheck)
		sql.WriteString(" WHERE ")
		sql.WriteString(validation)
		sql.WriteString(".")
		sql.WriteString(invalid)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(collisions)
		sql.WriteString(".")
		sql.WriteString(collision)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(columnCheck)
		sql.WriteString(".")
		sql.WriteString(columnInvalid)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(q("__os_chart_transport"))
		sql.WriteString(" ORDER BY ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" DESC, ")
		sql.WriteString(ordinal)
		sql.WriteString(" ASC")
	} else {
		rawEncodedLabel := "substring(" + encoded + ", 3)"
		published := encoded + " != '' AND " + collapsedCount + " > 0"
		domainItem := "tuple(multiIf(" + encoded + " = '1:', toUInt8(1), " + encoded + " = '2:', toUInt8(2), toUInt8(0)), " +
			"if(startsWith(" + encoded + ", '0:'), " + splunkSeriesLabelSQL(rawEncodedLabel) + ", CAST('' AS String)), " + encoded + ")"

		// Consume the bounded collapsed relation exactly once. Per-row maps are
		// grouped first; only then do fixed-cardinality windows attach the global
		// domain and validation evidence to at most one row per public row key.
		//
		// The column domain must deduplicate inside the window's aggregate state
		// rather than in an expression over its result. Collapsing already maps
		// every label outside the published top to '2:', so the distinct domain
		// holds at most the series limit plus the NULL and OTHER sentinels, which
		// is the transport's whole MaxSeries allowance, no matter how many labels
		// the input carried; but a window aggregate delivers its result to every row,
		// so gathering one entry per collapsed group and deduplicating afterwards
		// would materialize a row-count-sized array once per row. That is
		// quadratic in the row axis and exhausted the executor's memory budget
		// well below the advertised row ceiling. groupUniqArrayArray unions the
		// per-group arrays into that bounded distinct set as it aggregates, so
		// each row receives only the small domain.
		sql.WriteString(rowMaps)
		sql.WriteString(" AS (SELECT ")
		sql.WriteString(groupRow)
		sql.WriteString(", mapFromArrays(groupArrayIf(")
		sql.WriteString(encoded)
		sql.WriteString(", ")
		sql.WriteString(published)
		sql.WriteString("), groupArrayIf(")
		sql.WriteString(collapsedCount)
		sql.WriteString(", ")
		sql.WriteString(published)
		sql.WriteString(")) AS ")
		sql.WriteString(countMap)
		sql.WriteString(", ")
		sql.WriteString("arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupUniqArrayArray(groupArrayIf(")
		sql.WriteString(domainItem)
		sql.WriteString(", ")
		sql.WriteString(published)
		sql.WriteString(")) OVER ())) AS ")
		sql.WriteString(domainNames)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(max(")
		sql.WriteString(rowInvalidEvidence)
		sql.WriteString(")) OVER () != 0) AS ")
		sql.WriteString(invalid)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(max(")
		sql.WriteString(collisionEvidence)
		sql.WriteString(")) OVER () != 0) AS ")
		sql.WriteString(collision)
		sql.WriteString(", ")
		sql.WriteString("toUInt8(max(max(")
		sql.WriteString(columnInvalidEvidence)
		sql.WriteString(")) OVER () != 0) AS ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" FROM ")
		sql.WriteString(collapsed)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(groupRow)
		sql.WriteString("), ")

		bareRowSortSQL := groupRow
		if rowDynamic || rowField.numericSort {
			bareRowSortSQL = dynamicSortValue(groupRow, false)
		}
		sql.WriteString(rowDomain)
		sql.WriteString(" AS (SELECT *, toUInt8(")
		sql.WriteString(invalid)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(collision)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" != 0) AS ")
		sql.WriteString(transportInvalid)
		sql.WriteString(", ")
		sql.WriteString("toUInt64(row_number() OVER (ORDER BY ")
		sql.WriteString(bareRowSortSQL)
		sql.WriteString(" ASC) - 1) AS ")
		sql.WriteString(ordinal)
		sql.WriteString(" FROM ")
		sql.WriteString(rowMaps)
		sql.WriteString(" WHERE length(mapKeys(")
		sql.WriteString(countMap)
		sql.WriteString(")) > 0 OR ")
		sql.WriteString(invalid)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(collision)
		sql.WriteString(" != 0 OR ")
		sql.WriteString(columnInvalid)
		sql.WriteString(" != 0) ")

		groupRowOutputSQL := groupRow
		if rowKind == ChartRowKindMixed {
			groupRowOutputSQL = "tupleElement(" + groupRow + ", 1)"
		}
		sql.WriteString("SELECT ")
		sql.WriteString(ordinal)
		sql.WriteString(", ")
		sql.WriteString(groupRowOutputSQL)
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartRowColumn))
		sql.WriteString(", ")
		if rowKind == ChartRowKindMixed {
			sql.WriteString("tupleElement(")
			sql.WriteString(groupRow)
			sql.WriteString(", 2) AS ")
			sql.WriteString(q(ChartRowSemanticBytesColumn))
			sql.WriteString(", ")
		}
		sql.WriteString("if(")
		sql.WriteString(transportInvalid)
		sql.WriteString(" != 0, CAST([], 'Array(String)'), ")
		sql.WriteString(domainNames)
		sql.WriteString(") AS ")
		sql.WriteString(q(ChartNamesColumn))
		sql.WriteString(", ")
		sql.WriteString("if(")
		sql.WriteString(transportInvalid)
		sql.WriteString(" != 0, CAST([], 'Array(UInt64)'), arrayMap(name -> ifNull(")
		sql.WriteString(countMap)
		sql.WriteString("[name], toUInt64(0)), ")
		sql.WriteString(domainNames)
		sql.WriteString(")) AS ")
		sql.WriteString(q(ChartCountsColumn))
		sql.WriteString(", ")
		sql.WriteString(transportInvalid)
		sql.WriteString(" AS ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" FROM ")
		sql.WriteString(rowDomain)
		sql.WriteString(" WHERE throwIf(if(")
		sql.WriteString(transportInvalid)
		sql.WriteString(" = 0, ")
		sql.WriteString(ordinal)
		sql.WriteString(" >= ")
		sql.WriteString(strconv.FormatUint(uint64(operator.RowLimit), 10))
		sql.WriteString(", 0), '")
		sql.WriteString(ChartRowLimitMarker)
		sql.WriteString("') = 0")
		sql.WriteString(" AND (")
		sql.WriteString(transportInvalid)
		sql.WriteString(" = 0 OR ")
		sql.WriteString(ordinal)
		sql.WriteString(" = 0) ORDER BY ")
		sql.WriteString(q(ChartInvalidColumn))
		sql.WriteString(" DESC, ")
		sql.WriteString(ordinal)
		sql.WriteString(" ASC")
	}
	sql.WriteString(materializedCTESettingsSQL)

	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	kindedDepth := relationalNodeDepth(preparedDepth)
	classifiedDepth := relationalNodeDepth(kindedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	var resultDepth int
	if fieldOccurrenceCount {
		countsDepth := relationalNodeDepth(canonicalizedDepth)
		labelTotalsDepth := relationalNodeDepth(countsDepth)
		topDepth := relationalNodeDepth(labelTotalsDepth)
		topMembershipDepth := relationalNodeDepth(topDepth)
		collapsedDepth := relationalNodeDepth(countsDepth, topMembershipDepth)

		domainTopBranchDepth := relationalNodeDepth(topDepth)
		domainNullInputDepth := relationalNodeDepth(labelTotalsDepth)
		domainNullBranchDepth := relationalNodeDepth(domainNullInputDepth)
		domainOtherInputDepth := relationalNodeDepth(labelTotalsDepth, topMembershipDepth)
		domainOtherBranchDepth := relationalNodeDepth(domainOtherInputDepth)
		domainRowsDepth := relationalNodeDepth(
			domainTopBranchDepth,
			domainNullBranchDepth,
			domainOtherBranchDepth,
		)
		domainDepth := relationalNodeDepth(domainRowsDepth)
		collisionInputDepth := relationalNodeDepth(labelTotalsDepth)
		collisionsDepth := relationalNodeDepth(collisionInputDepth)
		columnCheckDepth := relationalNodeDepth(labelTotalsDepth)
		rowMapsDepth := relationalNodeDepth(collapsedDepth)
		validationDepth := relationalNodeDepth(countsDepth)
		rowDomainInputDepth := relationalNodeDepth(countsDepth)
		rowDomainDepth := relationalNodeDepth(rowDomainInputDepth)
		regularResultDepth := relationalNodeDepth(
			rowDomainDepth,
			domainDepth,
			rowMapsDepth,
		)
		validationSentinelDepth := relationalNodeDepth(
			validationDepth,
			collisionsDepth,
			columnCheckDepth,
		)
		unionDepth := relationalNodeDepth(regularResultDepth, validationSentinelDepth)
		resultDepth = relationalNodeDepth(unionDepth)
	} else {
		countsDepth := relationalNodeDepth(canonicalizedDepth)
		labelGroupsDepth := relationalNodeDepth(countsDepth)
		normalizedGroupsDepth := relationalNodeDepth(labelGroupsDepth)
		authorityDepth := relationalNodeDepth(normalizedGroupsDepth)
		labelExpandedDepth := relationalNodeDepth(authorityDepth)
		expandedDepth := relationalNodeDepth(labelExpandedDepth)
		collapsedDepth := relationalNodeDepth(expandedDepth)
		rowMapsDepth := relationalNodeDepth(collapsedDepth)
		rowDomainDepth := relationalNodeDepth(rowMapsDepth)
		resultDepth = relationalNodeDepth(rowDomainDepth)
	}

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		sourceFanout: eventStatsOrdinarySourceFanout,
		Chart: &ChartOutput{
			RowField:         rowName,
			RowKind:          rowKind,
			RowDatabaseType:  rowDatabaseType,
			RowLimit:         uint64(operator.RowLimit),
			MaxSeries:        dynamic.MaxSeries,
			MaxLabelBytes:    maxTimechartLabelBytes,
			ValueKind:        ChartValueKindCount,
			RowSemanticBytes: rowKind == ChartRowKindMixed,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

func compileNumericChart(
	relation compiledRelation,
	state compileState,
	args []any,
	operator *plan.Chart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Measure.Predicate != nil {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: numeric measure contract is invalid")
	}
	var valueKind ChartValueKind
	canonicalOutput := ""
	switch operator.Measure.Function {
	case plan.AggregateFunctionPercentile:
		if operator.Measure.Percentile < 1 || operator.Measure.Percentile > 99 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse chart: percentile must be from 1 through 99",
			)
		}
		valueKind = ChartValueKindPercentile
		canonicalOutput = "perc" +
			strconv.Itoa(int(operator.Measure.Percentile)) + "(" +
			operator.Measure.Input.Name + ")"
	case plan.AggregateFunctionSum:
		if operator.Measure.Percentile != 0 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse chart: numeric aggregate contains percentile metadata",
			)
		}
		valueKind = ChartValueKindSum
		canonicalOutput = "sum(" + operator.Measure.Input.Name + ")"
	case plan.AggregateFunctionAverage:
		if operator.Measure.Percentile != 0 {
			return CompiledQuery{}, errors.New(
				"compile ClickHouse chart: numeric aggregate contains percentile metadata",
			)
		}
		valueKind = ChartValueKindAverage
		canonicalOutput = "avg(" + operator.Measure.Input.Name + ")"
	default:
		return CompiledQuery{}, errors.New("compile ClickHouse chart: numeric function is unsupported")
	}
	if err := validateCanonicalFieldRef("chart", "input", operator.Measure.Input); err != nil {
		return CompiledQuery{}, err
	}
	if !spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) ||
		operator.Measure.Input.Name == operator.Over.Name ||
		operator.Measure.Output != canonicalOutput {
		return CompiledQuery{}, errors.New("compile ClickHouse chart: numeric measure contract is invalid")
	}
	if state.eventRows && state.allowDynamic && operator.Measure.Input.Name == "fields" {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_CHART_FIELD",
			Message: "chart cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   operator.Measure.Input.Range,
		}
	}

	rowName := operator.Over.Name
	rowField, rowDatabaseType, rowKind, splitField, err := resolveChartAxes(operator, dynamic, state)
	if err != nil {
		return CompiledQuery{}, err
	}

	measureField, measureResolved, err := resolveCompiledField(
		operator.Measure.Input,
		state,
	)
	if err != nil {
		return CompiledQuery{}, err
	}
	measureInputSQL := "CAST([], 'Array(Float64)')"
	var measureArgs []any
	if measureResolved {
		measureInputSQL, measureArgs = numericArrayInputSQL(measureField)
	}

	rowExistsSQL := rowField.existsSQL
	if rowExistsSQL == "" {
		rowExistsSQL = "1"
	}
	splitExistsSQL := splitField.existsSQL
	if splitExistsSQL == "" {
		splitExistsSQL = "1"
	}
	rowDynamic := rowField.kind == fieldKindDynamic
	splitDynamic := splitField.kind == fieldKindDynamic
	rowHasDescendant := rowDynamic && rowField.descendantSQL != ""
	splitHasDescendant := splitDynamic && splitField.descendantSQL != ""

	q := quoteIdentifier
	source := q("__os_chart_source")
	prepared := q("__os_chart_prepared")
	kinded := q("__os_chart_kinded")
	classified := q("__os_chart_classified")
	canonicalized := q("__os_chart_canonicalized")
	labelTotals := q("__os_chart_label_totals")
	numericGroups := q("__os_chart_numeric_groups")
	numericScores := q("__os_chart_numeric_scores")
	collapsed := q("__os_chart_collapsed")
	finalized := q("__os_chart_finalized")
	domainRows := q("__os_chart_domain_rows")
	domain := q("__os_chart_domain")
	collisions := q("__os_chart_normalization_collisions")
	columnCheck := q("__os_chart_column_check")
	rowMaps := q("__os_chart_row_maps")
	validation := q("__os_chart_validation")
	rowDomain := q("__os_chart_row_domain")

	rowValue := q("__os_ch_row_value")
	rowSemanticBytes := q("__os_ch_row_semantic_bytes")
	rowExact := q("__os_ch_row_exact")
	rowType := q("__os_ch_row_type")
	rowPresent := q("__os_ch_row_present")
	rowEligible := q("__os_ch_row_eligible")
	rowSupported := q("__os_ch_row_supported")
	rowInvalid := q("__os_ch_row_invalid")
	row := q("__os_ch_row")
	value := q("__os_ch_value")
	present := q("__os_ch_present")
	descendant := q("__os_ch_descendant")
	valueType := q("__os_ch_value_type")
	label := q("__os_ch_label")
	kind := q("__os_ch_kind")
	measureValues := q("__os_ch_measure_values")
	numerator := q("__os_ch_numerator")
	denominator := q("__os_ch_denominator")
	numericState := q("__os_ch_numeric_state")
	percentileState := q("__os_ch_percentile_state")
	percentileValues := q("__os_ch_percentile_values")
	frequency := q("__os_ch_count")
	score := q("__os_ch_score")
	encoded := q("__os_ch_encoded")
	measureValue := q("__os_ch_measure_value")
	normalized := q("__os_ch_normalized")
	sortLabel := q("__os_ch_sort_label")
	valueMap := q("__os_ch_value_map")
	presentMap := q("__os_ch_present_map")
	invalid := q("__os_ch_invalid")
	collision := q("__os_ch_collision")
	columnInvalid := q("__os_ch_column_invalid")
	ordinal := q(ChartOrdinalColumn)

	prefixArgs := make([]any, 0,
		len(rowField.existsArgs)+len(splitField.existsArgs)+len(measureArgs))
	prefixArgs = append(prefixArgs, rowField.existsArgs...)
	prefixArgs = append(prefixArgs, splitField.existsArgs...)
	prefixArgs = append(prefixArgs, measureArgs...)
	args = prependArguments(prefixArgs, args)
	if rowHasDescendant {
		args = append(args, rowField.descendantArgs...)
	}
	if splitHasDescendant {
		args = append(args, splitField.descendantArgs...)
	}
	args = append(args, rowName)

	splitTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitDynamic {
		splitTypeSQL = dynamicTypeExpression(splitField)
	}
	validLabel := "isValidUTF8(" + label + ") AND length(" + label +
		") BETWEEN 1 AND " + strconv.Itoa(maxTimechartLabelBytes) + " AND " +
		label + " NOT IN ('NULL', 'OTHER') AND " +
		splunkSeriesLabelSQL(label) + " != ?"

	rowPresenceSQL := "(" + rowExact + " != 0 AND isNotNull(" + rowValue + "))"
	if rowHasDescendant {
		rowPresenceSQL = "(" + rowPresenceSQL + " OR " +
			rowField.descendantSQL + ")"
	}
	rowKeySQL := "CAST(assumeNotNull(" + rowValue + ") AS " +
		rowDatabaseType + ")"
	rowSupportedSQL := "1"
	if rowDynamic {
		runtime := fieldState{
			valueSQL:       rowValue,
			dynamicTypeSQL: rowType,
			kind:           fieldKindDynamic,
		}
		supported, lexical := statsByScalarExpressions(runtime)
		rowSupportedSQL = supported
		rowKeySQL = "CAST(if(" + rowSupported + " != 0, " + lexical +
			", '') AS String)"
	}
	if rowKind == ChartRowKindMixed {
		rowKeySQL = "tuple(" + rowKeySQL + ", " + rowSemanticBytes + ")"
	}
	rowSortSQL := row
	if rowDynamic || rowField.numericSort {
		rowSortSQL = dynamicSortValue(row, false)
	}

	var scoreSQL string
	var publishSQL string
	switch valueKind {
	case ChartValueKindPercentile:
		scoreSQL = "sum(ifNull(arrayElementOrNull(finalizeAggregation(" +
			percentileState + "), 1), toFloat64(0)))"
		publishSQL = "arrayElementOrNull(" + percentileValues + ", 1)"
	case ChartValueKindSum:
		scoreSQL = "sum(if(" + denominator +
			" = 0, toFloat64(0), " + numerator + "))"
		publishSQL = "if(" + denominator +
			" = 0, CAST(NULL AS Nullable(Float64)), " + numerator + ")"
	case ChartValueKindAverage:
		cellAverage := numerator + " / toFloat64(" + denominator + ")"
		scoreSQL = "sum(if(" + denominator +
			" = 0, toFloat64(0), " + cellAverage + "))"
		publishSQL = "if(" + denominator +
			" = 0, CAST(NULL AS Nullable(Float64)), " + cellAverage + ")"
	default:
		return CompiledQuery{}, errors.New(
			"compile ClickHouse chart: numeric value kind is invalid",
		)
	}

	var sql strings.Builder
	sql.Grow(len(relation.sql) + len(measureInputSQL) + 12_288)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(rowValue)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowExact)
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString("toUInt8(ifNull(")
		sql.WriteString(rowField.semanticBytesSQL)
		sql.WriteString(", 0)) AS ")
		sql.WriteString(rowSemanticBytes)
		sql.WriteString(", ")
	}
	if rowDynamic {
		sql.WriteString(dynamicTypeExpression(rowField))
		sql.WriteString(" AS ")
		sql.WriteString(rowType)
		sql.WriteString(", ")
	}
	sql.WriteString(splitField.valueSQL)
	sql.WriteString(" AS ")
	sql.WriteString(value)
	sql.WriteString(", ")
	sql.WriteString("toUInt8(")
	sql.WriteString(splitExistsSQL)
	sql.WriteString(") AS ")
	sql.WriteString(present)
	sql.WriteString(", ")
	sql.WriteString(splitTypeSQL)
	sql.WriteString(" AS ")
	sql.WriteString(valueType)
	sql.WriteString(", ")
	sql.WriteString(measureInputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValues)
	for _, column := range pivotDescendantSourceColumns(state, rowField, splitField) {
		sql.WriteString(", ")
		sql.WriteString(column)
	}
	sql.WriteString(" FROM (")
	sql.WriteString(relation.sql)
	sql.WriteString(") AS ")
	sql.WriteString(alias)
	sql.WriteString("), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	sql.WriteString("toUInt8(")
	sql.WriteString(rowPresenceSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowPresent)
	sql.WriteString(", ")
	if splitHasDescendant {
		sql.WriteString("toUInt8(if(")
		sql.WriteString(present)
		sql.WriteString(" != 0, 0, ")
		sql.WriteString(splitField.descendantSQL)
		sql.WriteString(")) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	} else {
		sql.WriteString("toUInt8(0) AS ")
		sql.WriteString(descendant)
		sql.WriteString(", ")
	}
	sql.WriteString("toUInt8(")
	sql.WriteString(rowSupportedSQL)
	sql.WriteString(") AS ")
	sql.WriteString(rowSupported)
	sql.WriteString(", ")
	sql.WriteString("if(")
	sql.WriteString(present)
	sql.WriteString(" != 0 AND isNotNull(")
	sql.WriteString(value)
	sql.WriteString(") AND ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'String', assumeNotNull(toString(")
	sql.WriteString(value)
	sql.WriteString(")), CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(source)
	sql.WriteString("), ")

	sql.WriteString(kinded)
	sql.WriteString(" AS (SELECT *, multiIf(")
	sql.WriteString(descendant)
	sql.WriteString(" != 0, toUInt8(3), ")
	sql.WriteString(present)
	sql.WriteString(" = 0 OR isNull(")
	sql.WriteString(value)
	sql.WriteString(") OR ")
	sql.WriteString(valueType)
	sql.WriteString(" = 'None', toUInt8(1), ")
	sql.WriteString(valueType)
	sql.WriteString(" != 'String', toUInt8(3), NOT (")
	sql.WriteString(validLabel)
	sql.WriteString("), toUInt8(3), toUInt8(0)) AS ")
	sql.WriteString(kind)
	sql.WriteString(" FROM ")
	sql.WriteString(prepared)
	sql.WriteString("), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(rowKeySQL)
	sql.WriteString(" AS ")
	sql.WriteString(row)
	sql.WriteString(", toUInt8(")
	sql.WriteString(rowSupported)
	sql.WriteString(" = 0) AS ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowPresent)
	sql.WriteString(" AS ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(kinded)
	sql.WriteString("), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(rowInvalid)
	sql.WriteString(", ")
	sql.WriteString(rowEligible)
	sql.WriteString(", ")
	sql.WriteString(kind)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" = 0, ")
	sql.WriteString(label)
	sql.WriteString(", CAST('' AS String)) AS ")
	sql.WriteString(label)
	sql.WriteString(", if(")
	sql.WriteString(kind)
	sql.WriteString(" IN (0, 1) AND ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0, ")
	sql.WriteString(measureValues)
	sql.WriteString(", CAST([], 'Array(Float64)')) AS ")
	sql.WriteString(measureValues)
	sql.WriteString(" FROM ")
	sql.WriteString(classified)
	sql.WriteString("), ")

	if valueKind == ChartValueKindPercentile {
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString(numericGroups)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", quantilesGKOrNullArrayState(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(measureValues)
		sql.WriteString(") AS ")
		sql.WriteString(percentileState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString("), ")
	} else {
		sql.WriteString(numericGroups)
		sql.WriteString(" AS MATERIALIZED (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 1) AS ")
		sql.WriteString(numerator)
		sql.WriteString(", toUInt64(tupleElement(")
		sql.WriteString(numericState)
		sql.WriteString(", 2)) AS ")
		sql.WriteString(denominator)
		sql.WriteString(", ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM (SELECT ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(", max(")
		sql.WriteString(rowInvalid)
		sql.WriteString(") AS ")
		sql.WriteString(rowInvalid)
		sql.WriteString(", sumCountArray(")
		sql.WriteString(measureValues)
		sql.WriteString(") AS ")
		sql.WriteString(numericState)
		sql.WriteString(", count() AS ")
		sql.WriteString(frequency)
		sql.WriteString(" FROM ")
		sql.WriteString(canonicalized)
		sql.WriteString(" GROUP BY ")
		sql.WriteString(row)
		sql.WriteString(", ")
		sql.WriteString(rowEligible)
		sql.WriteString(", ")
		sql.WriteString(kind)
		sql.WriteString(", ")
		sql.WriteString(label)
		sql.WriteString(") AS ")
		sql.WriteString(q("__os_chart_numeric_state_source"))
		sql.WriteString("), ")
	}

	// Label selection and row-independent validation derive from the same
	// materialized row/label aggregate. Unlike count chart, numeric chart has
	// exactly one consumer of the scoped event relation; no raw-event CTE is
	// expanded independently for label totals.
	sql.WriteString(labelTotals)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString(", sumIf(")
	sql.WriteString(frequency)
	sql.WriteString(", ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0) AS ")
	sql.WriteString(frequency)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(kind)
	sql.WriteString(", ")
	sql.WriteString(label)
	sql.WriteString("), ")

	// The label domain is read from two IN sets below. ClickHouse 26.7 drops
	// the DelayedPortsProcessor gate that holds a materialized CTE's readers
	// behind its writer whenever an IN set reads a materialized CTE that is
	// itself defined over another materialized CTE, and aborts the query with
	// LOGICAL_ERROR "Reading from materialized CTE ... before its
	// materialization completed" (the fail-fast check from ClickHouse PR
	// 108924; the surviving gate losses are tracked in ClickHouse issues
	// 113184, 113489, and 114810).
	//
	// This selection therefore stays an ordinary CTE. That is behavior
	// preserving because every reference selects the identical labels: the
	// GROUP BY makes each label appear once, and the ORDER BY below sorts on
	// the finite/infinite class, then the score, then the label itself, so the
	// final key is unique and the ordering is total. No two distinct rows can
	// compare equal, which leaves the LIMIT no freedom to choose between them.
	// Re-reading also never re-runs the scoped scan, because the group
	// aggregate this reads is itself materialized.
	//
	// Restore MATERIALIZED once the upstream gate is fixed on the pinned
	// release.
	sql.WriteString(numericScores)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(label)
	sql.WriteString(", ")
	sql.WriteString(scoreSQL)
	sql.WriteString(" AS ")
	sql.WriteString(score)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(label)
	sql.WriteString(" ORDER BY ")
	sql.WriteString("multiIf(isNaN(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(0), isInfinite(")
	sql.WriteString(score)
	sql.WriteString(") AND ")
	sql.WriteString(score)
	sql.WriteString(" < 0, toUInt8(1), isInfinite(")
	sql.WriteString(score)
	sql.WriteString("), toUInt8(3), toUInt8(2)) DESC, if(isFinite(")
	sql.WriteString(score)
	sql.WriteString("), ")
	sql.WriteString(score)
	sql.WriteString(", toFloat64(0)) DESC, ")
	sql.WriteString(label)
	sql.WriteString(" ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
	sql.WriteString("), ")

	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", multiIf(")
	sql.WriteString(kind)
	sql.WriteString(" = 1, '1:', ")
	sql.WriteString(label)
	sql.WriteString(" IN (SELECT ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString("), concat('0:', ")
	sql.WriteString(label)
	sql.WriteString("), '2:') AS ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	if valueKind == ChartValueKindPercentile {
		level := statsPercentileLevelSQL(operator.Measure.Percentile)
		sql.WriteString("quantilesGKOrNullArrayMerge(100, ")
		sql.WriteString(level)
		sql.WriteString(")(")
		sql.WriteString(percentileState)
		sql.WriteString(") AS ")
		sql.WriteString(percentileValues)
	} else {
		sql.WriteString("sum(")
		sql.WriteString(numerator)
		sql.WriteString(") AS ")
		sql.WriteString(numerator)
		sql.WriteString(", sum(")
		sql.WriteString(denominator)
		sql.WriteString(") AS ")
		sql.WriteString(denominator)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" IN (0, 1) GROUP BY ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString("), ")

	sql.WriteString(finalized)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(", ")
	sql.WriteString(publishSQL)
	sql.WriteString(" AS ")
	sql.WriteString(measureValue)
	sql.WriteString(" FROM ")
	sql.WriteString(collapsed)
	sql.WriteString("), ")

	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(sortLabel)
	sql.WriteString(", concat('0:', ")
	sql.WriteString(label)
	sql.WriteString(") AS ")
	sql.WriteString(encoded)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) FROM (SELECT 1 FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" = 1 LIMIT 1)")
	sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) FROM (SELECT 1 FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 AND ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 AND ")
	sql.WriteString(label)
	sql.WriteString(" NOT IN (SELECT ")
	sql.WriteString(label)
	sql.WriteString(" FROM ")
	sql.WriteString(numericScores)
	sql.WriteString(") LIMIT 1)), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, ")
	sql.WriteString(sortLabel)
	sql.WriteString(", ")
	sql.WriteString(encoded)
	sql.WriteString(")))) AS names FROM ")
	sql.WriteString(domainRows)
	sql.WriteString("), ")

	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT toUInt8(count() > 0) AS ")
	sql.WriteString(collision)
	sql.WriteString(" FROM (SELECT ")
	sql.WriteString(splunkSeriesLabelSQL(label))
	sql.WriteString(" AS ")
	sql.WriteString(normalized)
	sql.WriteString(" FROM ")
	sql.WriteString(labelTotals)
	sql.WriteString(" WHERE ")
	sql.WriteString(kind)
	sql.WriteString(" = 0 GROUP BY ")
	sql.WriteString(normalized)
	sql.WriteString(" HAVING uniqExact(")
	sql.WriteString(label)
	sql.WriteString(") > 1 LIMIT 1)), ")

	sql.WriteString(columnCheck)
	sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
	sql.WriteString(kind)
	sql.WriteString(" = 3)) AS ")
	sql.WriteString(columnInvalid)
	sql.WriteString(" FROM ")
	sql.WriteString(labelTotals)
	sql.WriteString("), ")

	sql.WriteString(rowMaps)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(ifNull(")
	sql.WriteString(measureValue)
	sql.WriteString(", toFloat64(0)))) AS ")
	sql.WriteString(valueMap)
	sql.WriteString(", mapFromArrays(groupArray(")
	sql.WriteString(encoded)
	sql.WriteString("), groupArray(toUInt8(isNotNull(")
	sql.WriteString(measureValue)
	sql.WriteString(")))) AS ")
	sql.WriteString(presentMap)
	sql.WriteString(" FROM ")
	sql.WriteString(finalized)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(row)
	sql.WriteString("), ")

	// Missing and explicit-null dynamic row values are outside the row domain.
	// Unsupported descendants are deliberately eligible and remain visible to
	// this atomic validation guard.
	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(maxOrDefault(")
	sql.WriteString(rowInvalid)
	sql.WriteString(") > 0) AS ")
	sql.WriteString(invalid)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0), ")

	sql.WriteString(rowDomain)
	sql.WriteString(" AS MATERIALIZED (SELECT ")
	sql.WriteString(row)
	sql.WriteString(", toUInt64(row_number() OVER (ORDER BY ")
	sql.WriteString(rowSortSQL)
	sql.WriteString(" ASC) - 1) AS ")
	sql.WriteString(ordinal)
	sql.WriteString(" FROM (SELECT ")
	sql.WriteString(row)
	sql.WriteString(" FROM ")
	sql.WriteString(numericGroups)
	sql.WriteString(" WHERE ")
	sql.WriteString(rowEligible)
	sql.WriteString(" != 0 GROUP BY ")
	sql.WriteString(row)
	sql.WriteString(")) ")

	// The invalid sentinel is a private transport row, ordered before every
	// real row. It makes split-type/label/collision validation observable even
	// when the row axis has no eligible values. The executor buffers the whole
	// result and rejects any nonzero invalid marker before publishing a schema.
	sql.WriteString("SELECT ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(q(ChartRowColumn))
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString(q(ChartRowSemanticBytesColumn))
		sql.WriteString(", ")
	}
	sql.WriteString(q(ChartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString(q(ChartValuesColumn))
	sql.WriteString(", ")
	sql.WriteString(q(ChartValuePresentColumn))
	sql.WriteString(", ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" FROM (")
	rowOutputSQL := rowDomain + "." + row
	if rowKind == ChartRowKindMixed {
		rowOutputSQL = "tupleElement(" + rowOutputSQL + ", 1)"
	}
	sql.WriteString("SELECT ")
	sql.WriteString(rowDomain)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(rowOutputSQL)
	sql.WriteString(" AS ")
	sql.WriteString(q(ChartRowColumn))
	sql.WriteString(", ")
	if rowKind == ChartRowKindMixed {
		sql.WriteString("tupleElement(")
		sql.WriteString(rowDomain)
		sql.WriteString(".")
		sql.WriteString(row)
		sql.WriteString(", 2) AS ")
		sql.WriteString(q(ChartRowSemanticBytesColumn))
		sql.WriteString(", ")
	}
	sql.WriteString(domain)
	sql.WriteString(".names AS ")
	sql.WriteString(q(ChartNamesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(rowMaps)
	sql.WriteString(".")
	sql.WriteString(valueMap)
	sql.WriteString("[name], toFloat64(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(ChartValuesColumn))
	sql.WriteString(", ")
	sql.WriteString("arrayMap(name -> ifNull(")
	sql.WriteString(rowMaps)
	sql.WriteString(".")
	sql.WriteString(presentMap)
	sql.WriteString("[name], toUInt8(0)), ")
	sql.WriteString(domain)
	sql.WriteString(".names) AS ")
	sql.WriteString(q(ChartValuePresentColumn))
	sql.WriteString(", ")
	sql.WriteString("toUInt8(0) AS ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(rowDomain)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(domain)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(rowMaps)
	sql.WriteString(" ON ")
	sql.WriteString(rowMaps)
	sql.WriteString(".")
	sql.WriteString(row)
	sql.WriteString(" = ")
	sql.WriteString(rowDomain)
	sql.WriteString(".")
	sql.WriteString(row)
	sql.WriteString(" WHERE throwIf(")
	sql.WriteString(rowDomain)
	sql.WriteString(".")
	sql.WriteString(ordinal)
	sql.WriteString(" >= ")
	sql.WriteString(strconv.FormatUint(uint64(operator.RowLimit), 10))
	sql.WriteString(", '")
	sql.WriteString(ChartRowLimitMarker)
	sql.WriteString("') = 0")
	sql.WriteString(" UNION ALL SELECT toUInt64(0) AS ")
	sql.WriteString(ordinal)
	sql.WriteString(", ")
	sql.WriteString(chartValidationRowSQL(rowDatabaseType))
	sql.WriteString(" AS ")
	sql.WriteString(q(ChartRowColumn))
	if rowKind == ChartRowKindMixed {
		sql.WriteString(", toUInt8(0) AS ")
		sql.WriteString(q(ChartRowSemanticBytesColumn))
	}
	sql.WriteString(", CAST([], 'Array(String)') AS ")
	sql.WriteString(q(ChartNamesColumn))
	sql.WriteString(", CAST([], 'Array(Float64)') AS ")
	sql.WriteString(q(ChartValuesColumn))
	sql.WriteString(", CAST([], 'Array(UInt8)') AS ")
	sql.WriteString(q(ChartValuePresentColumn))
	sql.WriteString(", toUInt8(1) AS ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" FROM ")
	sql.WriteString(validation)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(collisions)
	sql.WriteString(" CROSS JOIN ")
	sql.WriteString(columnCheck)
	sql.WriteString(" WHERE ")
	sql.WriteString(validation)
	sql.WriteString(".")
	sql.WriteString(invalid)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(collisions)
	sql.WriteString(".")
	sql.WriteString(collision)
	sql.WriteString(" != 0 OR ")
	sql.WriteString(columnCheck)
	sql.WriteString(".")
	sql.WriteString(columnInvalid)
	sql.WriteString(" != 0) AS ")
	sql.WriteString(q("__os_chart_transport"))
	sql.WriteString(" ORDER BY ")
	sql.WriteString(q(ChartInvalidColumn))
	sql.WriteString(" DESC, ")
	sql.WriteString(ordinal)
	sql.WriteString(" ASC")
	sql.WriteString(materializedCTESettingsSQL)

	sourceDepth := relationalNodeDepth(relation.depth)
	preparedDepth := relationalNodeDepth(sourceDepth)
	kindedDepth := relationalNodeDepth(preparedDepth)
	classifiedDepth := relationalNodeDepth(kindedDepth)
	canonicalizedDepth := relationalNodeDepth(classifiedDepth)
	numericStateDepth := relationalNodeDepth(canonicalizedDepth)
	numericGroupsDepth := numericStateDepth
	if valueKind != ChartValueKindPercentile {
		numericGroupsDepth = relationalNodeDepth(numericStateDepth)
	}
	labelTotalsDepth := relationalNodeDepth(numericGroupsDepth)
	numericScoresDepth := relationalNodeDepth(numericGroupsDepth)
	scoreMembershipDepth := relationalNodeDepth(numericScoresDepth)
	collapsedDepth := relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth)
	finalizedDepth := relationalNodeDepth(collapsedDepth)
	domainRowsDepth := relationalNodeDepth(
		relationalNodeDepth(numericScoresDepth),
		relationalNodeDepth(numericGroupsDepth),
		relationalNodeDepth(numericGroupsDepth, scoreMembershipDepth),
	)
	domainDepth := relationalNodeDepth(domainRowsDepth)
	collisionsDepth := relationalNodeDepth(relationalNodeDepth(labelTotalsDepth))
	columnCheckDepth := relationalNodeDepth(labelTotalsDepth)
	rowMapsDepth := relationalNodeDepth(finalizedDepth)
	validationDepth := relationalNodeDepth(numericGroupsDepth)
	rowDomainDepth := relationalNodeDepth(relationalNodeDepth(numericGroupsDepth))
	regularResultDepth := relationalNodeDepth(
		rowDomainDepth,
		domainDepth,
		rowMapsDepth,
	)
	validationSentinelDepth := relationalNodeDepth(
		validationDepth,
		collisionsDepth,
		columnCheckDepth,
	)
	unionDepth := relationalNodeDepth(regularResultDepth, validationSentinelDepth)
	resultDepth := relationalNodeDepth(unionDepth)

	compiled := CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Chart: &ChartOutput{
			RowField:         rowName,
			RowKind:          rowKind,
			RowDatabaseType:  rowDatabaseType,
			RowLimit:         uint64(operator.RowLimit),
			MaxSeries:        dynamic.MaxSeries,
			MaxLabelBytes:    maxTimechartLabelBytes,
			ValueKind:        valueKind,
			RowSemanticBytes: rowKind == ChartRowKindMixed,
		},
	}
	return withCompiledRelationalDepth(compiled, resultDepth, operator.Range), nil
}

type compileState struct {
	visible                          map[string]fieldState
	context                          *compileContext
	publicOrder                      []string
	privateColumns                   []string
	rexCapturedBytesSQL              string
	allowDynamic                     bool
	sparseFieldsSubset               bool
	eventRows                        bool
	blocked                          map[string]struct{}
	blockedPrefixes                  map[string]struct{}
	dynamicFieldFilters              []compiledDynamicFieldFilter
	order                            []compiledSortKey
	tieBreakers                      []compiledSortKey
	preAggregateValidationColumns    []string
	preAggregateValidationArgs       []any
	preAggregateColumns              []string
	preAggregateArgs                 []any
	preAggregateGroupExpansions      []compiledStatsGroupExpansion
	preAggregateSparklineWindows     []string
	preAggregateListWindowColumns    []string
	preAggregateListCandidateColumns []string
	postAggregateSparklines          []compiledStatsSparklineMeasure
	postAggregateChronological       []compiledChronologicalMeasure
	postAggregateScalarExtrema       []compiledScalarExtremaMeasure
	postAggregateExactStrings        []compiledExactStringMeasure
	postAggregateDistinctCounts      []compiledDistinctCount
	postAggregateOrderedStrings      []compiledOrderedStringMeasure
	deferredChronologicalValidation  []string
	chronologicalBarriers            []compiledChronologicalBarrier
	mvExpandQueryRowsSQL             string
}

type compiledDynamicFieldFilter struct {
	include  bool
	fields   []string
	patterns []string
}

type compiledStatsSparklineMeasure struct {
	recordsColumn string
	outputColumn  string
	spec          statsSparklineBucketSpec
	missing       statsSparklineMissingValue
}

// compileContext contains immutable query-wide values and shared resource
// accounting. Relation-shaping stages carry one pointer instead of manually
// copying each search-scoped constant into newly constructed compileState
// values.
type compileContext struct {
	operationContext                       context.Context
	patternBudgets                         compiledPatternBudgets
	strftimeBudget                         compiledStrftimeBudget
	strptimeBudget                         compiledStrptimeBudget
	relativeTimeBudget                     compiledRelativeTimeBudget
	unixTimestampBudget                    compiledUnixTimestampBudget
	concatenationBudget                    compiledConcatenationBudget
	stringConversionBudget                 compiledStringConversionBudget
	arithmeticOperators                    int
	membershipCandidates                   int
	mvExpandStages                         uint8
	atomicResult                           bool
	requiresMaterializedValidationSettings bool
	searchStartUnix                        int64
	searchEarliest                         time.Time
	searchLatest                           time.Time
	searchTimezone                         string
	searchLocalMinimumUnixNanoseconds      int64
	searchTimezoneChecked                  bool
	searchTimezoneInvalid                  bool
	lookupTables                           []compiledLookupExternalTable
}

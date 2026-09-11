package plan

// AutomaticTimeSpanUnit identifies one unit in Splunk's automatic time-bucket
// ladder. It is shared by timechart planning and stats sparkline compilation.
type AutomaticTimeSpanUnit uint8

const (
	AutomaticTimeSpanUnitInvalid AutomaticTimeSpanUnit = iota
	AutomaticTimeSpanUnitSecond
	AutomaticTimeSpanUnitMinute
	AutomaticTimeSpanUnitHour
	AutomaticTimeSpanUnitDay
	AutomaticTimeSpanUnitMonth
)

// AutomaticTimeSpan is one candidate on Splunk's ordered automatic ladder.
type AutomaticTimeSpan struct {
	Magnitude uint64
	Unit      AutomaticTimeSpanUnit
}

var automaticTimeSpanSteps = [...]AutomaticTimeSpan{
	{Magnitude: 1, Unit: AutomaticTimeSpanUnitSecond},
	{Magnitude: 5, Unit: AutomaticTimeSpanUnitSecond},
	{Magnitude: 10, Unit: AutomaticTimeSpanUnitSecond},
	{Magnitude: 30, Unit: AutomaticTimeSpanUnitSecond},
	{Magnitude: 1, Unit: AutomaticTimeSpanUnitMinute},
	{Magnitude: 5, Unit: AutomaticTimeSpanUnitMinute},
	{Magnitude: 10, Unit: AutomaticTimeSpanUnitMinute},
	{Magnitude: 30, Unit: AutomaticTimeSpanUnitMinute},
	{Magnitude: 1, Unit: AutomaticTimeSpanUnitHour},
	{Magnitude: 1, Unit: AutomaticTimeSpanUnitDay},
	{Magnitude: 1, Unit: AutomaticTimeSpanUnitMonth},
}

// AutomaticTimeSpanSteps returns the ordered Splunk threshold list. A copy is
// returned so callers cannot mutate the shared selection contract.
func AutomaticTimeSpanSteps() []AutomaticTimeSpan {
	return append([]AutomaticTimeSpan(nil), automaticTimeSpanSteps[:]...)
}

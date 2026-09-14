package clickhouse

import "errors"

// ErrTimechartResourceLimit identifies a local timechart continuation
// allocation rejected before constructing its detached or native copy.
var ErrTimechartResourceLimit = errors.New("timechart resource limit exceeded")

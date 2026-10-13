// Package metricstest gives tests a recording metrics handler and reads its samples back.
package metricstest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/config"
)

// New returns an enabled metrics handler with the default configuration, closed when t ends.
func New(t testing.TB) *common_metrics.CommonMetricsHandler {
	t.Helper()
	cfg := config.GetDefaults()
	cfg.MetricsEnabled = new(true)
	metrics := common_metrics.NewCommonMetricsHandler(config.Static(cfg).Metrics(), zap.NewNop())
	t.Cleanup(func() { _ = metrics.Close() })
	return metrics
}

// Sample is one series' recorded values: Value for a counter or gauge, Count and Sum for a
// histogram.
type Sample struct {
	Value float64
	Count uint64
	Sum   float64
}

// Read returns the series of the metric named name (without its namespace) that carries every
// label in labels. It is the zero Sample when nothing recorded that series yet.
func Read(t testing.TB, metrics *common_metrics.CommonMetricsHandler, name string, labels map[string]string) Sample {
	t.Helper()
	families, err := metrics.GetRegistry().Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name && !strings.HasSuffix(family.GetName(), "_"+name) {
			continue
		}
	series:
		for _, metric := range family.GetMetric() {
			matched := 0
			for _, label := range metric.GetLabel() {
				if want, ok := labels[label.GetName()]; ok {
					if label.GetValue() != want {
						continue series
					}
					matched++
				}
			}
			if matched != len(labels) {
				continue
			}
			if histogram := metric.GetHistogram(); histogram != nil {
				return Sample{Count: histogram.GetSampleCount(), Sum: histogram.GetSampleSum()}
			}
			if counter := metric.GetCounter(); counter != nil {
				return Sample{Value: counter.GetValue()}
			}
			return Sample{Value: metric.GetGauge().GetValue()}
		}
	}
	return Sample{}
}

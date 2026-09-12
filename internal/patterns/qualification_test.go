package patterns

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const qualificationSampleInterval = time.Millisecond

// This is an explicit controlled-host qualification, not a CI wall-clock test.
// The caller chooses an idle host and an output path; failed targets still
// persist all raw measurements before the test reports the failure.
func TestPatternQualification(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_PATTERNS_QUALIFICATION") != "1" {
		t.Skip("requires an explicitly reserved idle-host qualification slot")
	}
	outputPath := os.Getenv("OPEN_SPLUNK_PATTERNS_QUALIFICATION_OUTPUT")
	if outputPath == "" {
		t.Fatal("OPEN_SPLUNK_PATTERNS_QUALIFICATION_OUTPUT is required")
	}
	pairs := 7
	if configured := os.Getenv("OPEN_SPLUNK_PATTERNS_QUALIFICATION_PAIRS"); configured != "" {
		parsed, err := strconv.Atoi(configured)
		if err != nil || parsed < 7 || parsed > 31 {
			t.Fatal("qualification pairs must be between 7 and 31")
		}
		pairs = parsed
	}
	report := qualificationReport{
		FormatVersion: 1, StartedAt: time.Now().UTC(), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), SampleIntervalNS: int64(qualificationSampleInterval),
		AlgorithmVersion: AlgorithmVersion, Sensitivity: "Balanced", PageSize: DefaultMaximumPageSize,
		ColdDefinition:     "fresh Patterns service and analysis cache; shared durable Store and filesystem cache across pairs",
		ChargedMeasurement: "1ms samples plus held-response/end-of-operation observations; sampling may miss a transient peak",
		RSSMeasurement:     "Linux process VmRSS and VmHWM, separately from charged service accounting; includes fixture and Go runtime",
		ChargedGlobalLimit: DefaultMaximumPinnedBytes, ChargedWorkingLimit: DefaultMaximumWorkingBytes,
		TargetColdP95NS: int64(2 * time.Second), TargetCachedP95NS: int64(100 * time.Millisecond), TargetCancelMaximumNS: int64(250 * time.Millisecond),
	}
	if revision, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		report.SourceRevision = strings.TrimSpace(string(revision))
	}
	if status, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil {
		report.TrackedWorktreeDirty = len(status) != 0
	}
	fixture := newQualificationFixture(t)
	report.Fixture = qualificationFixtureMetadata{Rows: fixture.Rows, Groups: fixture.Groups, SHA256: fixture.SHA256, ArtifactBytes: fixture.ArtifactBytes, Generation: fixture.Generation}
	rss, err := qualificationRSS()
	if err != nil {
		t.Fatal(err)
	}
	report.RSSBefore = rss
	var failures []string
	defer func() {
		report.FinishedAt = time.Now().UTC()
		finalRSS, rssErr := qualificationRSS()
		if rssErr == nil {
			report.RSSAfter = finalRSS
		} else {
			failures = append(failures, rssErr.Error())
		}
		report.Failures = failures
		payload, marshalErr := json.MarshalIndent(report, "", "  ")
		if marshalErr != nil {
			t.Errorf("serialize qualification report: %v", marshalErr)
			return
		}
		if writeErr := os.WriteFile(outputPath, append(payload, '\n'), 0600); writeErr != nil {
			t.Errorf("write qualification report: %v", writeErr)
		}
		for _, failure := range failures {
			t.Error(failure)
		}
	}()
	key := []byte("patterns-fixed-qualification-cursor-key-v1")
	query := ListRequest{SearchJobID: fixture.JobID, Generation: fixture.Generation, Sensitivity: Balanced, PageSize: DefaultMaximumPageSize, IncludeTotal: true}
	var expectedDigest, expectedAllGroupsDigest string
	for pair := 0; pair < pairs; pair++ {
		service, err := New(Config{Source: fixture.Store, CursorKey: key})
		if err != nil {
			failures = append(failures, err.Error())
			return
		}
		report.Pairs = append(report.Pairs, qualificationPair{Index: pair + 1})
		sample := &report.Pairs[len(report.Pairs)-1]
		observation := newQualificationBudgetObservation(service)
		runtime.GC() // Deliberately outside both measured calls, identical per pair.
		started := time.Now()
		cold, coldErr := service.List(t.Context(), fixture.Access, query)
		coldDuration := time.Since(started)
		sample.ColdNS = int64(coldDuration)
		observation.sample()
		coldDigest, digestErr := qualificationPageDigest(cold, fixture.Rows, fixture.Groups, DefaultMaximumPageSize)
		cold.Close()
		sample.OutputSHA256 = coldDigest
		if coldErr != nil || digestErr != nil {
			sample.Error = "cold analysis failed: " + errors.Join(coldErr, digestErr).Error()
			failures = append(failures, sample.Error)
			_ = service.Close(context.Background())
			sample.SampledChargedPeak, sample.ChargedAfterClose = observation.stop()
			return
		}
		started = time.Now()
		cached, cachedErr := service.List(t.Context(), fixture.Access, query)
		cachedDuration := time.Since(started)
		sample.CachedNS = int64(cachedDuration)
		observation.sample()
		cachedDigest, cachedDigestErr := qualificationPageDigest(cached, fixture.Rows, fixture.Groups, DefaultMaximumPageSize)
		cached.Close()
		sample.CachedOutputSHA256 = cachedDigest
		if cachedErr != nil || cachedDigestErr != nil {
			sample.Error = "cached analysis failed: " + errors.Join(cachedErr, cachedDigestErr).Error()
			failures = append(failures, sample.Error)
			_ = service.Close(context.Background())
			sample.SampledChargedPeak, sample.ChargedAfterClose = observation.stop()
			return
		}
		if expectedDigest == "" {
			expectedDigest = coldDigest
		}
		if coldDigest != expectedDigest || cachedDigest != expectedDigest {
			failures = append(failures, "cold/cached immutable result digests differ")
		}

		allQuery := query
		allQuery.PageSize = MaximumPageSize
		allGroups, allErr := service.List(t.Context(), fixture.Access, allQuery)
		allDigest, allDigestErr := qualificationPageDigest(allGroups, fixture.Rows, fixture.Groups, MaximumPageSize)
		allGroups.Close()
		sample.AllGroupsSHA256 = allDigest
		if allErr != nil || allDigestErr != nil {
			sample.Error = "full grouping validation failed: " + errors.Join(allErr, allDigestErr).Error()
			failures = append(failures, sample.Error)
			_ = service.Close(context.Background())
			sample.SampledChargedPeak, sample.ChargedAfterClose = observation.stop()
			return
		}
		if expectedAllGroupsDigest == "" {
			expectedAllGroupsDigest = allDigest
		}
		if allDigest != expectedAllGroupsDigest {
			failures = append(failures, "full immutable group relation digest changed")
		}
		if err := service.Close(context.Background()); err != nil {
			failures = append(failures, err.Error())
		}
		observation.sample()
		peak, afterClose := observation.stop()
		if peak > report.ChargedGlobalLimit || afterClose != 0 {
			failures = append(failures, "charged global limit or post-close release invariant failed")
		}
		sample.SampledChargedPeak, sample.ChargedAfterClose = peak, afterClose
		sample.Complete = true

		blocked, entered := newQualificationBlockedSource(fixture.Store)
		canceledService, err := New(Config{Source: blocked, CursorKey: key})
		if err != nil {
			failures = append(failures, err.Error())
			return
		}
		cancelContext, cancel := context.WithCancel(t.Context())
		returned := make(chan error, 1)
		go func() {
			result, err := canceledService.List(cancelContext, fixture.Access, query)
			result.Close()
			returned <- err
		}()
		select {
		case <-entered:
		case err := <-returned:
			cancel()
			_ = canceledService.Close(context.Background())
			failures = append(failures, "cancellation fixture returned before read barrier: "+qualificationErrorText(err))
			return
		case <-time.After(20 * time.Second):
			cancel()
			_ = canceledService.Close(context.Background())
			failures = append(failures, "cancellation fixture did not reach the read barrier")
			return
		}
		started = time.Now()
		cancel()
		select {
		case err := <-returned:
			report.CancellationNS = append(report.CancellationNS, int64(time.Since(started)))
			if !errors.Is(err, context.Canceled) {
				failures = append(failures, "blocked read did not return context cancellation: "+qualificationErrorText(err))
			}
		case <-time.After(2 * time.Second):
			failures = append(failures, "blocked read did not cancel within the qualification watchdog")
		}
		closeContext, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := canceledService.Close(closeContext); err != nil {
			failures = append(failures, err.Error())
		}
		closeCancel()
	}
	var cold, cached []int64
	for _, pair := range report.Pairs {
		cold = append(cold, pair.ColdNS)
		cached = append(cached, pair.CachedNS)
	}
	report.ColdP95NS, report.CachedP95NS = qualificationP95(cold), qualificationP95(cached)
	for _, duration := range report.CancellationNS {
		report.CancelMaximumNS = max(report.CancelMaximumNS, duration)
	}
	if len(report.Pairs) != pairs || len(report.CancellationNS) != pairs {
		failures = append(failures, "qualification did not produce every requested measurement")
	}
	if report.ColdP95NS > report.TargetColdP95NS {
		failures = append(failures, "cold analysis p95 exceeds 2 seconds")
	}
	if report.CachedP95NS > report.TargetCachedP95NS {
		failures = append(failures, "cached first-page p95 exceeds 100 milliseconds")
	}
	if report.CancelMaximumNS > report.TargetCancelMaximumNS {
		failures = append(failures, "blocked-read cancellation exceeds 250 milliseconds")
	}
}

type qualificationReport struct {
	FormatVersion         int                          `json:"format_version"`
	StartedAt             time.Time                    `json:"started_at"`
	FinishedAt            time.Time                    `json:"finished_at"`
	SourceRevision        string                       `json:"source_revision"`
	TrackedWorktreeDirty  bool                         `json:"tracked_worktree_dirty"`
	GoVersion             string                       `json:"go_version"`
	GOOS                  string                       `json:"goos"`
	GOARCH                string                       `json:"goarch"`
	CPUs                  int                          `json:"cpus"`
	GOMAXPROCS            int                          `json:"gomaxprocs"`
	AlgorithmVersion      string                       `json:"algorithm_version"`
	Sensitivity           string                       `json:"sensitivity"`
	PageSize              int                          `json:"page_size"`
	Fixture               qualificationFixtureMetadata `json:"fixture"`
	ColdDefinition        string                       `json:"cold_definition"`
	ChargedMeasurement    string                       `json:"charged_measurement"`
	RSSMeasurement        string                       `json:"rss_measurement"`
	SampleIntervalNS      int64                        `json:"charged_sample_interval_ns"`
	ChargedGlobalLimit    uint64                       `json:"charged_global_limit_bytes"`
	ChargedWorkingLimit   uint64                       `json:"charged_working_limit_bytes"`
	RSSBefore             qualificationRSSValue        `json:"rss_before"`
	RSSAfter              qualificationRSSValue        `json:"rss_after"`
	Pairs                 []qualificationPair          `json:"pairs"`
	CancellationNS        []int64                      `json:"cancellation_ns"`
	ColdP95NS             int64                        `json:"cold_p95_ns"`
	CachedP95NS           int64                        `json:"cached_p95_ns"`
	CancelMaximumNS       int64                        `json:"cancel_maximum_ns"`
	TargetColdP95NS       int64                        `json:"target_cold_p95_ns"`
	TargetCachedP95NS     int64                        `json:"target_cached_p95_ns"`
	TargetCancelMaximumNS int64                        `json:"target_cancel_maximum_ns"`
	Failures              []string                     `json:"failures"`
}
type qualificationFixtureMetadata struct {
	Rows          uint64 `json:"rows"`
	Groups        uint64 `json:"groups"`
	SHA256        string `json:"sha256"`
	ArtifactBytes uint64 `json:"artifact_bytes"`
	Generation    uint64 `json:"generation"`
}
type qualificationPair struct {
	Complete           bool   `json:"complete"`
	Error              string `json:"error,omitempty"`
	Index              int    `json:"index"`
	ColdNS             int64  `json:"cold_ns"`
	CachedNS           int64  `json:"cached_ns"`
	OutputSHA256       string `json:"output_sha256"`
	CachedOutputSHA256 string `json:"cached_output_sha256"`
	AllGroupsSHA256    string `json:"all_groups_sha256"`
	SampledChargedPeak uint64 `json:"sampled_charged_peak_bytes"`
	ChargedAfterClose  uint64 `json:"charged_after_close_bytes"`
}
type qualificationRSSValue struct {
	Current   uint64 `json:"current_bytes"`
	HighWater uint64 `json:"high_water_bytes"`
}

func qualificationRSS() (qualificationRSSValue, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return qualificationRSSValue{}, err
	}
	result := qualificationRSSValue{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || (fields[0] != "VmRSS:" && fields[0] != "VmHWM:") {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || fields[2] != "kB" || value > math.MaxUint64/1024 {
			return qualificationRSSValue{}, errors.New("invalid Linux RSS measurement")
		}
		if fields[0] == "VmRSS:" {
			result.Current = value * 1024
		} else {
			result.HighWater = value * 1024
		}
	}
	if result.Current == 0 || result.HighWater == 0 {
		return qualificationRSSValue{}, errors.New("Linux RSS measurements are unavailable")
	}
	return result, nil
}

func qualificationPageDigest(result ListResult, rows, groups uint64, pageSize int) (string, error) {
	if result.RetainedEventCount != rows || result.EligibleEventCount != rows || result.ExcludedEventCount != 0 ||
		!result.RetainedTruncated || result.SnapshotComplete || result.TotalSize == nil || *result.TotalSize != groups ||
		!result.TotalSizeExact || len(result.Patterns) != pageSize || (result.NextPageToken != "") != (groups > uint64(pageSize)) {
		return "", errors.New("qualification result coverage or page shape changed")
	}
	var sum uint64
	for _, group := range result.Patterns {
		sum += group.EventCount
	}
	if groups == uint64(pageSize) && sum != rows {
		return "", errors.New("complete group counts do not conserve the fixture rows")
	}
	payload, err := json.Marshal(struct {
		Patterns   []Pattern
		Generation uint64
		Next       string
		Rows       uint64
		Groups     uint64
	}{result.Patterns, result.Generation, result.NextPageToken, rows, groups})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}
func qualificationP95(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	return ordered[(95*len(ordered)+99)/100-1]
}
func qualificationErrorText(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

type qualificationBudgetObservation struct {
	service *Service
	done    chan struct{}
	stopped chan struct{}
	mu      sync.Mutex
	peak    uint64
	last    uint64
}

func newQualificationBudgetObservation(service *Service) *qualificationBudgetObservation {
	observer := &qualificationBudgetObservation{service: service, done: make(chan struct{}), stopped: make(chan struct{})}
	go func() {
		defer close(observer.stopped)
		ticker := time.NewTicker(qualificationSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				observer.sample()
			case <-observer.done:
				return
			}
		}
	}()
	return observer
}
func (observer *qualificationBudgetObservation) sample() {
	observer.service.mu.Lock()
	bytes := observer.service.globalBytes
	observer.service.mu.Unlock()
	observer.mu.Lock()
	observer.peak = max(observer.peak, bytes)
	observer.last = bytes
	observer.mu.Unlock()
}
func (observer *qualificationBudgetObservation) stop() (uint64, uint64) {
	close(observer.done)
	<-observer.stopped
	observer.sample()
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.peak, observer.last
}

package metriccollector

import (
	"reflect"
	"testing"
	"time"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

func TestCountRollingWindowPushCountWithinCurrentWindow(t *testing.T) {
	start := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	window := newTestCountRollingWindow()

	firstDatapoints, firstSlid := window.pushCount(
		newTestCountObservation(start.Add(2*time.Second+100*time.Millisecond), 2),
	)
	secondDatapoints, secondSlid := window.pushCount(
		newTestCountObservation(start.Add(2*time.Second+900*time.Millisecond), 4),
	)

	if firstSlid || secondSlid {
		t.Fatal("pushCount() slid a window for observations in the same window")
	}
	if firstDatapoints != nil || secondDatapoints != nil {
		t.Fatal("pushCount() returned datapoints without sliding the window")
	}
	if len(window.observations) != 2 {
		t.Fatalf("len(observations) = %d, want 2", len(window.observations))
	}
	if window.totalValue != 6 {
		t.Fatalf("totalValue = %d, want 6", window.totalValue)
	}
}

func TestCountRollingWindowTightenWindowReturnsEveryStep(t *testing.T) {
	start := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	window := newTestCountRollingWindow()
	window.windowStartTime = start
	window.addCount(
		newTestCountObservation(start.Add(500*time.Millisecond), 2),
	)
	window.addCount(
		newTestCountObservation(start.Add(1500*time.Millisecond), 4),
	)

	got := window.tightenWindow(start.Add(3 * time.Second))
	want := []countDataPoints{
		{
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_SUM:  6,
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_RATE: 3,
		},
		{
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_SUM:  4,
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_RATE: 4,
		},
		{
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_SUM:  0,
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_RATE: 0,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tightenWindow() = %+v, want %+v", got, want)
	}
	if window.windowStartTime != start.Add(3*time.Second) {
		t.Fatalf(
			"windowStartTime = %v, want %v",
			window.windowStartTime,
			start.Add(3*time.Second),
		)
	}
	if len(window.observations) != 0 {
		t.Fatalf("len(observations) = %d, want 0", len(window.observations))
	}
	if window.totalValue != 0 {
		t.Fatalf("totalValue = %d, want 0", window.totalValue)
	}
}

func TestCountRollingWindowPushCountSlidesMultipleSteps(t *testing.T) {
	start := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	window := newTestCountRollingWindow()

	window.pushCount(
		newTestCountObservation(start.Add(2*time.Second+100*time.Millisecond), 6),
	)
	got, slid := window.pushCount(
		newTestCountObservation(start.Add(5*time.Second+100*time.Millisecond), 3),
	)

	if !slid {
		t.Fatal("pushCount() did not report sliding the window")
	}
	if len(got) != 3 {
		t.Fatalf("len(pushCount()) = %d, want 3", len(got))
	}
	if window.windowStartTime != start.Add(3*time.Second) {
		t.Fatalf(
			"windowStartTime = %v, want %v",
			window.windowStartTime,
			start.Add(3*time.Second),
		)
	}
	if len(window.observations) != 1 {
		t.Fatalf("len(observations) = %d, want 1", len(window.observations))
	}
	if window.totalValue != 3 {
		t.Fatalf("totalValue = %d, want 3", window.totalValue)
	}
}

func newTestCountRollingWindow() *countRollingWindow {
	return &countRollingWindow{
		observations: make([]countObservation, 0),
		metadata: windowMetadata{
			duration: 3 * time.Second,
			step:     time.Second,
		},
		aggregations: []collectorpb.CounterAggregation{
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_SUM,
			collectorpb.CounterAggregation_COUNTER_AGGREGATION_RATE,
		},
	}
}

func newTestCountObservation(observedAt time.Time, value int64) countObservation {
	return countObservation{
		metadata: observationMetadata{observedAt: observedAt},
		value:    value,
	}
}

package metriccollector

import (
	"testing"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

func TestConvertMetricRefToSeriesKey(t *testing.T) {
	ref := &collectorpb.MetricRef{
		Entity: "api",
		Series: "requests",
	}

	got := convertMetricRefToSeriesKey(ref)
	want := SeriesKey{
		Entity: "api",
		Series: "requests",
	}

	if got != want {
		t.Fatalf("convertMetricRefToSeriesKey() = %+v, want %+v", got, want)
	}
}

func TestConvertMetricRefToSeriesKeyAcceptsNilRef(t *testing.T) {
	got := convertMetricRefToSeriesKey(nil)
	want := SeriesKey{}

	if got != want {
		t.Fatalf("convertMetricRefToSeriesKey(nil) = %+v, want %+v", got, want)
	}
}

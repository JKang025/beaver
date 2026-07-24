package metriccollector

import (
	"context"
	"testing"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegisterMetrics(t *testing.T) {
	server := metricsServer{
		metrics: make(map[string]map[string]*collectorpb.Series),
	}

	request := &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			newCounterSeries("requests"),
			newGaugeSeries("connections"),
		},
	}

	_, err := server.RegisterMetrics(context.Background(), request)
	if err != nil {
		t.Fatalf("RegisterMetrics() error = %v", err)
	}

	if len(server.metrics["api"]) != 2 {
		t.Fatalf("registered series count = %d, want 2", len(server.metrics["api"]))
	}
}

func TestRegisterMetricsAddsSeriesToExistingEntity(t *testing.T) {
	server := metricsServer{
		metrics: map[string]map[string]*collectorpb.Series{
			"api": {
				"requests": newCounterSeries("requests"),
			},
		},
	}
	request := &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			newGaugeSeries("connections"),
		},
	}

	_, err := server.RegisterMetrics(context.Background(), request)
	if err != nil {
		t.Fatalf("RegisterMetrics() error = %v", err)
	}

	if len(server.metrics["api"]) != 2 {
		t.Fatalf("registered series count = %d, want 2", len(server.metrics["api"]))
	}
}

func TestRegisterMetricsIgnoresExistingSeries(t *testing.T) {
	existingSeries := newCounterSeries("requests")
	server := metricsServer{
		metrics: map[string]map[string]*collectorpb.Series{
			"api": {
				"requests": existingSeries,
			},
		},
	}
	request := &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			newGaugeSeries("connections"),
			newCounterSeries("requests"),
		},
	}

	_, err := server.RegisterMetrics(context.Background(), request)
	if err != nil {
		t.Fatalf("RegisterMetrics() error = %v", err)
	}
	if len(server.metrics["api"]) != 2 {
		t.Fatalf("registered series count = %d, want 2", len(server.metrics["api"]))
	}
	if server.metrics["api"]["requests"] != existingSeries {
		t.Fatal("existing requests series was replaced")
	}
}

func TestRegisterMetricsIgnoresDuplicateSeriesInRequest(t *testing.T) {
	firstSeries := newCounterSeries("requests")
	server := metricsServer{
		metrics: make(map[string]map[string]*collectorpb.Series),
	}
	request := &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			firstSeries,
			newGaugeSeries("requests"),
		},
	}

	_, err := server.RegisterMetrics(context.Background(), request)
	if err != nil {
		t.Fatalf("RegisterMetrics() error = %v", err)
	}
	if len(server.metrics["api"]) != 1 {
		t.Fatalf("registered series count = %d, want 1", len(server.metrics["api"]))
	}
	if server.metrics["api"]["requests"] != firstSeries {
		t.Fatal("first requests series was replaced by its duplicate")
	}
}

func TestRegisterMetricsRejectsInvalidRequestsAtomically(t *testing.T) {
	tests := []struct {
		name    string
		request *collectorpb.RegisterMetricsRequest
	}{
		{
			name: "missing entity",
			request: &collectorpb.RegisterMetricsRequest{
				Series: []*collectorpb.Series{newCounterSeries("requests")},
			},
		},
		{
			name: "empty series list",
			request: &collectorpb.RegisterMetricsRequest{
				Entity: "api",
			},
		},
		{
			name: "missing series name",
			request: &collectorpb.RegisterMetricsRequest{
				Entity: "api",
				Series: []*collectorpb.Series{
					newCounterSeries(""),
				},
			},
		},
		{
			name: "missing config",
			request: &collectorpb.RegisterMetricsRequest{
				Entity: "api",
				Series: []*collectorpb.Series{
					{Name: "requests"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := metricsServer{
				metrics: make(map[string]map[string]*collectorpb.Series),
			}

			_, err := server.RegisterMetrics(context.Background(), test.request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("RegisterMetrics() code = %v, want %v", status.Code(err), codes.InvalidArgument)
			}
			if len(server.metrics) != 0 {
				t.Fatalf("registered entity count = %d, want 0", len(server.metrics))
			}
		})
	}
}

func newCounterSeries(name string) *collectorpb.Series {
	return &collectorpb.Series{
		Name: name,
		Config: &collectorpb.Series_Counter{
			Counter: &collectorpb.CounterMetricConfig{},
		},
	}
}

func newGaugeSeries(name string) *collectorpb.Series {
	return &collectorpb.Series{
		Name: name,
		Config: &collectorpb.Series_Gauge{
			Gauge: &collectorpb.GaugeMetricConfig{},
		},
	}
}

package metriccollector

import (
	"context"
	"testing"

	collectorpb "github.com/JKang025/beaver/proto/collector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewMetricsServerInitializesRegistryAndWorker(t *testing.T) {
	server := newTestMetricsServer(t)

	if server.registry == nil {
		t.Fatal("registry is nil")
	}
	if server.statsWorker == nil {
		t.Fatal("stats worker is nil")
	}
	if server.statsWorker.observations == nil {
		t.Fatal("stats worker observations channel is nil")
	}
	if cap(server.statsWorker.observations) != 0 {
		t.Fatalf(
			"stats worker observations channel capacity = %d, want 0",
			cap(server.statsWorker.observations),
		)
	}
}

func TestRegisterMetricsAssignsSeriesToWorker(t *testing.T) {
	server := newTestMetricsServer(t)
	requests := newCounterSeries("requests")
	connections := newGaugeSeries("connections")

	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			requests,
			connections,
		},
	})

	assertRegisteredSeries(t, server, SeriesKey{Entity: "api", Series: "requests"}, requests)
	assertRegisteredSeries(t, server, SeriesKey{Entity: "api", Series: "connections"}, connections)
}

func TestRegisterMetricsSupportsMultipleEntities(t *testing.T) {
	server := newTestMetricsServer(t)
	apiRequests := newCounterSeries("requests")
	databaseRequests := newGaugeSeries("requests")

	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{apiRequests},
	})
	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "database",
		Series: []*collectorpb.Series{databaseRequests},
	})

	assertRegisteredSeries(t, server, SeriesKey{Entity: "api", Series: "requests"}, apiRequests)
	assertRegisteredSeries(
		t,
		server,
		SeriesKey{Entity: "database", Series: "requests"},
		databaseRequests,
	)
}

func TestRegisterMetricsIgnoresExistingSeries(t *testing.T) {
	server := newTestMetricsServer(t)
	existingSeries := newCounterSeries("requests")

	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{existingSeries},
	})
	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			newGaugeSeries("connections"),
			newGaugeSeries("requests"),
		},
	})

	assertRegisteredSeries(
		t,
		server,
		SeriesKey{Entity: "api", Series: "requests"},
		existingSeries,
	)
}

func TestRegisterMetricsIgnoresDuplicateSeriesInRequest(t *testing.T) {
	server := newTestMetricsServer(t)
	firstSeries := newCounterSeries("requests")

	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			firstSeries,
			newGaugeSeries("requests"),
		},
	})

	assertRegisteredSeries(
		t,
		server,
		SeriesKey{Entity: "api", Series: "requests"},
		firstSeries,
	)
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
					newCounterSeries("valid"),
					{Name: "invalid"},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newTestMetricsServer(t)

			_, err := server.RegisterMetrics(context.Background(), test.request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf(
					"RegisterMetrics() code = %v, want %v",
					status.Code(err),
					codes.InvalidArgument,
				)
			}
			if count := registrySize(server.registry); count != 0 {
				t.Fatalf("registered series count = %d, want 0", count)
			}
		})
	}
}

func TestMetricEndpointsRejectMissingSeries(t *testing.T) {
	server := newTestMetricsServer(t)
	metric := &collectorpb.MetricRef{Entity: "api", Series: "missing"}

	_, err := server.CountMetric(context.Background(), &collectorpb.CountMetricRequest{
		Metric: metric,
		Value:  1,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("CountMetric() code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestMetricEndpointsValidateSeriesType(t *testing.T) {
	server := newTestMetricsServer(t)
	registerMetrics(t, server, &collectorpb.RegisterMetricsRequest{
		Entity: "api",
		Series: []*collectorpb.Series{
			newCounterSeries("requests"),
			newGaugeSeries("connections"),
		},
	})

	tests := []struct {
		name     string
		call     func() error
		wantCode codes.Code
	}{
		{
			name: "count accepts counter",
			call: func() error {
				_, err := server.CountMetric(
					context.Background(),
					&collectorpb.CountMetricRequest{
						Metric: &collectorpb.MetricRef{Entity: "api", Series: "requests"},
						Value:  1,
					},
				)
				return err
			},
			wantCode: codes.OK,
		},
		{
			name: "count rejects gauge",
			call: func() error {
				_, err := server.CountMetric(
					context.Background(),
					&collectorpb.CountMetricRequest{
						Metric: &collectorpb.MetricRef{Entity: "api", Series: "connections"},
						Value:  1,
					},
				)
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "record accepts gauge",
			call: func() error {
				_, err := server.RecordMetric(
					context.Background(),
					&collectorpb.RecordMetricRequest{
						Metric: &collectorpb.MetricRef{Entity: "api", Series: "connections"},
						Value:  1.5,
					},
				)
				return err
			},
			wantCode: codes.OK,
		},
		{
			name: "record rejects counter",
			call: func() error {
				_, err := server.RecordMetric(
					context.Background(),
					&collectorpb.RecordMetricRequest{
						Metric: &collectorpb.MetricRef{Entity: "api", Series: "requests"},
						Value:  1.5,
					},
				)
				return err
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if code := status.Code(test.call()); code != test.wantCode {
				t.Fatalf("endpoint code = %v, want %v", code, test.wantCode)
			}
		})
	}
}

func newTestMetricsServer(t *testing.T) *metricsServer {
	t.Helper()

	server, ok := NewMetricsServer().(*metricsServer)
	if !ok {
		t.Fatal("NewMetricsServer() did not return *metricsServer")
	}
	return server
}

func registerMetrics(
	t *testing.T,
	server *metricsServer,
	request *collectorpb.RegisterMetricsRequest,
) {
	t.Helper()

	if _, err := server.RegisterMetrics(context.Background(), request); err != nil {
		t.Fatalf("RegisterMetrics() error = %v", err)
	}
}

func assertRegisteredSeries(
	t *testing.T,
	server *metricsServer,
	seriesKey SeriesKey,
	wantDefinition *collectorpb.Series,
) {
	t.Helper()

	worker, definition, exists := server.registry.lookup(seriesKey)
	if !exists {
		t.Fatalf("series %+v is not registered", seriesKey)
	}
	if worker != server.statsWorker {
		t.Fatalf("series %+v assigned to unexpected worker", seriesKey)
	}
	if definition != wantDefinition {
		t.Fatalf("series %+v definition was replaced", seriesKey)
	}
}

func registrySize(registry *seriesRegistry) int {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	return len(registry.workersBySeries)
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

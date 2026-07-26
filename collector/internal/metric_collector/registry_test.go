package metriccollector

import (
	"fmt"
	"sync"
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

func TestSeriesRegistryRegisterAndLookup(t *testing.T) {
	registry := newSeriesRegistry()
	worker := newStatisticsWorker()
	seriesKey := SeriesKey{Entity: "api", Series: "requests"}
	definition := newCounterSeries("requests")

	if registered := registry.register(seriesKey, definition, worker); !registered {
		t.Fatal("register() = false, want true")
	}

	gotWorker, gotDefinition, exists := registry.lookup(seriesKey)
	if !exists {
		t.Fatal("lookup() did not find registered series")
	}
	if gotWorker != worker {
		t.Fatal("lookup() returned unexpected worker")
	}
	if gotDefinition != definition {
		t.Fatal("lookup() returned unexpected definition")
	}
}

func TestSeriesRegistryRegisterPreservesFirstAssignment(t *testing.T) {
	registry := newSeriesRegistry()
	firstWorker := newStatisticsWorker()
	secondWorker := newStatisticsWorker()
	seriesKey := SeriesKey{Entity: "api", Series: "requests"}
	firstDefinition := newCounterSeries("requests")

	if registered := registry.register(seriesKey, firstDefinition, firstWorker); !registered {
		t.Fatal("first register() = false, want true")
	}
	if registered := registry.register(
		seriesKey,
		newGaugeSeries("requests"),
		secondWorker,
	); registered {
		t.Fatal("duplicate register() = true, want false")
	}

	gotWorker, gotDefinition, exists := registry.lookup(seriesKey)
	if !exists {
		t.Fatal("lookup() did not find registered series")
	}
	if gotWorker != firstWorker {
		t.Fatal("duplicate registration replaced worker")
	}
	if gotDefinition != firstDefinition {
		t.Fatal("duplicate registration replaced definition")
	}
}

func TestSeriesRegistryLookupMissing(t *testing.T) {
	registry := newSeriesRegistry()

	worker, definition, exists := registry.lookup(
		SeriesKey{Entity: "api", Series: "missing"},
	)
	if exists {
		t.Fatal("lookup() found unregistered series")
	}
	if worker != nil {
		t.Fatal("lookup() returned worker for unregistered series")
	}
	if definition != nil {
		t.Fatal("lookup() returned definition for unregistered series")
	}
}

func TestSeriesRegistrySupportsConcurrentRegistrationAndLookup(t *testing.T) {
	const seriesCount = 100

	registry := newSeriesRegistry()
	worker := newStatisticsWorker()
	var waitGroup sync.WaitGroup

	for index := 0; index < seriesCount; index++ {
		seriesKey := SeriesKey{
			Entity: "api",
			Series: fmt.Sprintf("series-%d", index),
		}
		definition := newCounterSeries(seriesKey.Series)

		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			registry.register(seriesKey, definition, worker)
		}()
		go func() {
			defer waitGroup.Done()
			registry.lookup(seriesKey)
		}()
	}

	waitGroup.Wait()

	for index := 0; index < seriesCount; index++ {
		seriesKey := SeriesKey{
			Entity: "api",
			Series: fmt.Sprintf("series-%d", index),
		}
		_, definition, exists := registry.lookup(seriesKey)
		if !exists {
			t.Fatalf("lookup() did not find series %+v", seriesKey)
		}
		if definition.GetName() != seriesKey.Series {
			t.Fatalf(
				"lookup() definition name = %q, want %q",
				definition.GetName(),
				seriesKey.Series,
			)
		}
	}
}

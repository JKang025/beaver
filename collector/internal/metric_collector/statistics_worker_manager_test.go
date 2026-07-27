package metriccollector

import (
	"context"
	"testing"
	"time"
)

// workerManagerTestObservation is used to verify that a managed worker is running.
type workerManagerTestObservation struct{}

func (workerManagerTestObservation) observationMetadata() observationMetadata {
	return observationMetadata{}
}

func TestStatisticsWorkerManagerOwnsConfiguredWorker(t *testing.T) {
	const observationBufferCapacity = 25

	manager := newStatisticsWorkerManager(observationBufferCapacity)

	if manager.worker == nil {
		t.Fatal("worker is nil")
	}
	if got := cap(manager.worker.observations); got != observationBufferCapacity {
		t.Fatalf(
			"worker observation capacity = %d, want %d",
			got,
			observationBufferCapacity,
		)
	}
}

func TestStatisticsWorkerManagerAssignsSeriesToOwnedWorker(t *testing.T) {
	manager := newStatisticsWorkerManager(0)
	key := seriesKey{entity: "api", series: "requests"}
	definition := newCounterSeries("requests")

	worker, err := manager.assignSeries(key, definition)
	if err != nil {
		t.Fatalf("assignSeries() error = %v", err)
	}
	if worker != manager.worker {
		t.Fatal("assignSeries() returned an unmanaged worker")
	}

	worker.mutex.RLock()
	state, exists := worker.series[key]
	worker.mutex.RUnlock()
	if !exists {
		t.Fatal("assigned series is missing from worker state")
	}
	if state.definition != definition {
		t.Fatal("assigned series definition was replaced")
	}
}

func TestStatisticsWorkerManagerStartsOwnedWorker(t *testing.T) {
	manager := newStatisticsWorkerManager(0)
	ctx, cancel := context.WithCancel(t.Context())
	manager.start(ctx)

	sent := make(chan struct{})
	go func() {
		select {
		case manager.worker.observations <- workerManagerTestObservation{}:
			close(sent)
		case <-ctx.Done():
		}
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("managed worker did not receive an observation")
	}

	cancel()
}

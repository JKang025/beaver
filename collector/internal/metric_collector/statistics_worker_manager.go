package metriccollector

import (
	"context"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

// statisticsWorkerManager owns statistics worker assignment and lifecycle.
type statisticsWorkerManager struct {
	worker *statisticsWorker
}

func newStatisticsWorkerManager(
	observationBufferCapacity int,
	store *memoryStore,
) *statisticsWorkerManager {
	return &statisticsWorkerManager{
		worker: newStatisticsWorker(observationBufferCapacity, store),
	}
}

// assignSeries initializes the series on the managed worker and returns it.
func (m *statisticsWorkerManager) assignSeries(
	key seriesKey,
	definition *collectorpb.Series,
) (*statisticsWorker, error) {
	if err := m.worker.registerSeries(key, definition); err != nil {
		return nil, err
	}

	return m.worker, nil
}

// start runs the managed worker until the context is canceled.
func (m *statisticsWorkerManager) start(ctx context.Context) {
	go m.worker.run(ctx)
}

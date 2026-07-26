package metriccollector

import (
	"context"
	"fmt"
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

type statisticsWorker struct {
	mutex        sync.RWMutex
	observations chan observation
	series       map[seriesKey]*seriesState
}

type seriesState struct {
	definition *collectorpb.Series
	window     rollingWindow
}

// stats worker initialization
func newStatisticsWorker(bufferCapacity int) *statisticsWorker {
	return &statisticsWorker{
		observations: make(chan observation, bufferCapacity),
		series:       make(map[seriesKey]*seriesState),
	}
}

// creates a seriesState object and associate a seriesKey with it in the registry map
func (w *statisticsWorker) registerSeries(
	key seriesKey,
	definition *collectorpb.Series,
) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.series[key] = &seriesState{
		definition: definition,
	}
}

// check if series exists in this stats worker
func (w *statisticsWorker) lookupSeries(
	key seriesKey,
) (*collectorpb.Series, bool) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	state, exists := w.series[key]
	if !exists {
		return nil, false
	}

	return state.definition, true
}

// continous thread that processes observations
func (w *statisticsWorker) run(ctx context.Context) {
	for {
		select {
		case observation, open := <-w.observations:
			if !open {
				return
			}

			w.process(observation)

		case <-ctx.Done():
			return
		}

	}
}

func (w *statisticsWorker) process(observation observation) {
	switch typedObs := observation.(type) {
	case countObservation:
		// obs is countObservation
	case recordObservation:
		fmt.Printf("recordObservation type is currently not supported.")
		return
	}
}

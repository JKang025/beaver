package metriccollector

import (
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
	window     *rollingWindow
}

func newStatisticsWorker() *statisticsWorker {
	return &statisticsWorker{
		observations: make(chan observation, 1000),
		series:       make(map[seriesKey]*seriesState),
	}
}

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

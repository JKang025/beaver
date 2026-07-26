package metriccollector

import (
	"sync"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

type statisticsWorker struct {
	mutex        sync.RWMutex
	observations chan observation
	series       map[SeriesKey]*seriesState
}

type seriesState struct {
	definition *collectorpb.Series
	window     *rollingWindow
}

func newStatisticsWorker() *statisticsWorker {
	return &statisticsWorker{
		observations: make(chan observation, 1000),
		series:       make(map[SeriesKey]*seriesState),
	}
}

func (w *statisticsWorker) registerSeries(
	seriesKey SeriesKey,
	definition *collectorpb.Series,
) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.series[seriesKey] = &seriesState{
		definition: definition,
	}
}

func (w *statisticsWorker) lookupSeries(
	seriesKey SeriesKey,
) (*collectorpb.Series, bool) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	state, exists := w.series[seriesKey]
	if !exists {
		return nil, false
	}

	return state.definition, true
}

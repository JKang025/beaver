package metriccollector

import (
	"context"
	"fmt"
	"sync"
	"time"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

// statisticsWorker aggregates observations for its assigned series.
type statisticsWorker struct {
	mutex        sync.RWMutex
	observations chan observation
	series       map[seriesKey]*seriesState
	store        *memoryStore
}

// seriesState contains worker-owned aggregation state for one series.
type seriesState struct {
	definition *collectorpb.Series
	window     rollingWindow
}

const (
	defaultWindowDuration = 60 * time.Second
	defaultWindowStep     = time.Second
)

// newStatisticsWorker creates a worker with a bounded observation queue.
func newStatisticsWorker(
	bufferCapacity int,
	store *memoryStore,
) *statisticsWorker {
	return &statisticsWorker{
		observations: make(chan observation, bufferCapacity),
		series:       make(map[seriesKey]*seriesState),
		store:        store,
	}
}

// registerSeries initializes the processing state for a series.
func (w *statisticsWorker) registerSeries(
	key seriesKey,
	definition *collectorpb.Series,
) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// create shared objects across all rollingWindow types
	metadata := windowMetadata{
		duration: defaultWindowDuration,
		step:     defaultWindowStep,
	}
	var window rollingWindow

	switch definition.GetConfig().(type) {
	case *collectorpb.Series_Counter:
		window = &countRollingWindow{
			observations:    make([]countObservation, 0),
			totalValue:      0,
			metadata:        metadata,
			windowStartTime: time.Time{},
			aggregations:    definition.GetCounter().GetAggregations(),
		}

	case *collectorpb.Series_Gauge, *collectorpb.Series_Sample:
		// Their processing state will be added with RecordMetric support.

	default:
		return fmt.Errorf("unsupported series config %T", definition.GetConfig())
	}

	w.series[key] = &seriesState{
		definition: definition,
		window:     window,
	}

	return nil
}

// run processes observations until its queue closes or the context is canceled.
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
		key := typedObs.observationMetadata().key

		w.mutex.RLock()
		seriesState, exists := w.series[key]
		w.mutex.RUnlock()

		if !exists {
			fmt.Printf("series is not registered")
			return
		}

		counterConfig := seriesState.definition.GetCounter()
		if counterConfig == nil {
			fmt.Printf("series is not configured as a counter")
			return
		}

		counterWindow, ok := seriesState.window.(*countRollingWindow)
		if !ok {
			fmt.Printf("counter series has non-counter window type")
			return
		}

		datapoints, _ := counterWindow.pushCount(typedObs)
		for _, datapoint := range datapoints {
			w.store.push(key, datapoint)
		}

	case recordObservation:
		fmt.Printf("recordObservation type is currently not supported.")
		return
	}
}

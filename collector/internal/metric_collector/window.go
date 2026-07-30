package metriccollector

import (
	"time"

	collectorpb "github.com/JKang025/beaver/proto/collector"
)

// windowMetadata configures a rolling window's duration and slide interval.
type windowMetadata struct {
	duration time.Duration
	step     time.Duration
}

// rollingWindow exposes the configuration shared by each window type.
type rollingWindow interface {
	getMetadata() windowMetadata
}

// dataPoint exposes the time interval shared by each metric datapoint type.
type dataPoint interface {
	timestamp() time.Time
	aggregationDuration() time.Duration
}

// countDataPoint contains one counter aggregation value for a time interval.
type countDataPoint struct {
	at          time.Time
	duration    time.Duration
	aggregation collectorpb.CounterAggregation
	value       int64
}

// countRollingWindow tracks counter observations and their aggregate sum.
type countRollingWindow struct {
	observations    []countObservation
	totalValue      int64
	metadata        windowMetadata
	windowStartTime time.Time
	aggregations    []collectorpb.CounterAggregation
}

// gaugeRollingWindow tracks observations for a gauge series.
type gaugeRollingWindow struct {
	observations []observation
}

func (c *countRollingWindow) getMetadata() windowMetadata {
	return c.metadata
}

func (c *countRollingWindow) pushCount(
	obs countObservation,
) ([]countDataPoint, bool) {
	currStartWindow := belongToNewWindow(obs, c.metadata)

	if c.windowStartTime.IsZero() {
		c.windowStartTime = currStartWindow
		c.addCount(obs)
		return nil, false
	}

	if !c.windowStartTime.Before(currStartWindow) {
		c.addCount(obs)
		return nil, false
	}

	datapoints := c.tightenWindow(currStartWindow)
	c.addCount(obs)
	return datapoints, true
}

func (c *countRollingWindow) addCount(obs countObservation) {
	c.observations = append(c.observations, obs)
	c.totalValue += obs.value
}

func (c *countRollingWindow) tightenWindow(
	nextWindowStartTime time.Time,
) []countDataPoint {
	var datapoints []countDataPoint
	for c.windowStartTime.Before(nextWindowStartTime) {
		datapoints = append(datapoints, c.getCurrWindowDataPoints()...)
		c.windowStartTime = c.windowStartTime.Add(c.metadata.step)
		c.removeExpiredObservations()
	}
	return datapoints
}

func (c *countRollingWindow) removeExpiredObservations() {
	retained := c.observations[:0]
	for _, obs := range c.observations {
		if obs.observationMetadata().observedAt.Before(c.windowStartTime) {
			c.totalValue -= obs.value
			continue
		}
		retained = append(retained, obs)
	}
	c.observations = retained
}

func (c *countRollingWindow) getCurrWindowDataPoints() []countDataPoint {
	datapoints := make([]countDataPoint, 0, len(c.aggregations))
	for _, agg := range c.aggregations {
		var value int64
		switch agg {
		case collectorpb.CounterAggregation_COUNTER_AGGREGATION_SUM:
			value = c.totalValue
		case collectorpb.CounterAggregation_COUNTER_AGGREGATION_RATE:
			if len(c.observations) > 0 {
				value = c.totalValue / int64(len(c.observations))
			}
		}
		datapoints = append(datapoints, countDataPoint{
			at:          c.windowStartTime,
			duration:    c.metadata.duration,
			aggregation: agg,
			value:       value,
		})
	}
	return datapoints
}

func (p countDataPoint) timestamp() time.Time {
	return p.at
}

func (p countDataPoint) aggregationDuration() time.Duration {
	return p.duration
}

var _ dataPoint = countDataPoint{}

// belongToNewWindow returns the start of the step-aligned window for observation.
func belongToNewWindow(
	observation observation,
	metadata windowMetadata,
) time.Time {
	currBucketTime := observation.observationMetadata().observedAt.Truncate(
		metadata.step,
	)

	// At t=60.1, a 60s window with a 1s step covers [1, 61).
	windowStartTime := currBucketTime.Add(-metadata.duration + metadata.step)
	return windowStartTime.Truncate(metadata.step)
}

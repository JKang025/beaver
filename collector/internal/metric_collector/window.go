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

// countDataPoints maps each configured counter aggregation to its window value.
type countDataPoints map[collectorpb.CounterAggregation]int64

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
) ([]countDataPoints, bool) {
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
	return datapoints, len(datapoints) > 0
}

func (c *countRollingWindow) addCount(obs countObservation) {
	c.observations = append(c.observations, obs)
	c.totalValue += obs.value
}

func (c *countRollingWindow) tightenWindow(
	nextWindowStartTime time.Time,
) []countDataPoints {
	var datapoints []countDataPoints
	for c.windowStartTime.Before(nextWindowStartTime) {
		datapoints = append(datapoints, c.getCurrWindowDataPoints())
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

func (c *countRollingWindow) getCurrWindowDataPoints() countDataPoints {
	datapoints := make(countDataPoints, len(c.aggregations))
	for _, agg := range c.aggregations {
		switch agg {
		case collectorpb.CounterAggregation_COUNTER_AGGREGATION_SUM:
			datapoints[agg] = c.totalValue
		case collectorpb.CounterAggregation_COUNTER_AGGREGATION_RATE:
			var rate int64
			if len(c.observations) > 0 {
				rate = c.totalValue / int64(len(c.observations))
			}
			datapoints[agg] = rate
		}
	}
	return datapoints
}

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

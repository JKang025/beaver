package metriccollector

import "time"

type windowMetadata struct {
	duration     time.Duration
	step         time.Duration
	currentStart time.Time
}

type rollingWindow struct {
	metadata     windowMetadata
	observations []observation
}

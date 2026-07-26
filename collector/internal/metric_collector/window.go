package metriccollector

import "time"

type WindowMetadata struct {
	Duration     time.Duration
	Step         time.Duration
	CurrentStart time.Time
}

type rollingWindow struct {
	metadata     WindowMetadata
	observations []observation
}

package security

import "time"

type Observation struct {
	Operation  Operation
	Effect     Effect
	Mode       Mode
	ReasonCode string
	Duration   time.Duration
}

type Observer interface {
	Decision(Observation)
}

package httpapi

import (
	"context"
	"net/netip"
	"time"

	"example.com/ozon/internal/auth"
	"example.com/ozon/internal/telemetry"
)

type Options struct {
	Auth             *auth.Authenticator
	Metrics          *telemetry.Metrics
	Ready            func(context.Context) error
	Draining         func() bool
	MaxOperations    int
	MaxSubscriptions int
	RequestTimeout   time.Duration
	StreamLifetime   time.Duration
	Heartbeat        time.Duration
	WriteTimeout     time.Duration
	MaxBodyBytes     int64
	Rate             float64
	Burst            int
	TrustedProxies   []netip.Prefix
	Depth            int
	Fields           int
	Cost             int
}

func (o Options) defaults() Options {
	if o.Metrics == nil {
		o.Metrics = &telemetry.Metrics{}
	}
	if o.MaxOperations == 0 {
		o.MaxOperations = 64
	}
	if o.MaxSubscriptions == 0 {
		o.MaxSubscriptions = 1024
	}
	if o.RequestTimeout == 0 {
		o.RequestTimeout = 10 * time.Second
	}
	if o.StreamLifetime == 0 {
		o.StreamLifetime = 10 * time.Minute
	}
	if o.Heartbeat == 0 {
		o.Heartbeat = 15 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.MaxBodyBytes == 0 {
		o.MaxBodyBytes = 1 << 20
	}
	if o.Rate == 0 {
		o.Rate = 120
	}
	if o.Burst == 0 {
		o.Burst = 240
	}
	if o.Depth == 0 {
		o.Depth = MaxDepth
	}
	if o.Fields == 0 {
		o.Fields = MaxFields
	}
	if o.Cost == 0 {
		o.Cost = MaxCost
	}
	return o
}

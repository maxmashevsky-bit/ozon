package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Metrics struct {
	Requests      atomic.Int64
	GraphQLErrors atomic.Int64
	RateRejected  atomic.Int64
	BusyRejected  atomic.Int64
	SQLQueries    atomic.Int64
	SQLErrors     atomic.Int64
	SQLNanos      atomic.Int64
	Inflight      atomic.Int64
	Pool          *pgxpool.Pool
	Events        func() map[string]float64
}
type queryStart struct{}

func (m *Metrics) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	m.SQLQueries.Add(1)
	return context.WithValue(ctx, queryStart{}, time.Now())
}
func (m *Metrics) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if d.Err != nil {
		m.SQLErrors.Add(1)
	}
	if start, ok := ctx.Value(queryStart{}).(time.Time); ok {
		m.SQLNanos.Add(time.Since(start).Nanoseconds())
	}
}
func (m *Metrics) Snapshot() map[string]float64 {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	values := map[string]float64{"requests_total": float64(m.Requests.Load()), "graphql_errors_total": float64(m.GraphQLErrors.Load()), "rate_rejected_total": float64(m.RateRejected.Load()), "busy_rejected_total": float64(m.BusyRejected.Load()), "sql_queries_total": float64(m.SQLQueries.Load()), "sql_errors_total": float64(m.SQLErrors.Load()), "sql_seconds_total": float64(m.SQLNanos.Load()) / 1e9, "inflight": float64(m.Inflight.Load()), "go_goroutines": float64(runtime.NumGoroutine()), "go_heap_bytes": float64(memory.HeapAlloc)}
	if m.Pool != nil {
		s := m.Pool.Stat()
		values["pool_acquired"] = float64(s.AcquiredConns())
		values["pool_total"] = float64(s.TotalConns())
		values["pool_acquires_total"] = float64(s.AcquireCount())
		values["pool_waits_total"] = float64(s.EmptyAcquireCount())
		values["pool_acquire_seconds_total"] = s.AcquireDuration().Seconds()
		values["pool_canceled_total"] = float64(s.CanceledAcquireCount())
	}
	if m.Events != nil {
		for k, v := range m.Events() {
			values[k] = v
		}
	}
	return values
}
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	values := m.Snapshot()
	if r.Header.Get("Accept") == "application/json" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(values)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for name, value := range values {
		if _, err := fmt.Fprintf(w, "ozon_%s %g\n", name, value); err != nil {
			return
		}
	}
}

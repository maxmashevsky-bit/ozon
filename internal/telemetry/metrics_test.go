package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestMetricsConcurrentCountersAndSQLTrace(t *testing.T) {
	m := &Metrics{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Requests.Add(1)
			m.GraphQLErrors.Add(1)
			m.RateRejected.Add(1)
			m.BusyRejected.Add(1)
			m.Inflight.Add(1)
			ctx := m.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{})
			m.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: errors.New("query failed")})
		}()
	}
	wg.Wait()
	values := m.Snapshot()
	for _, key := range []string{"requests_total", "graphql_errors_total", "rate_rejected_total", "busy_rejected_total", "sql_queries_total", "sql_errors_total", "inflight"} {
		if values[key] != 100 {
			t.Errorf("%s=%v", key, values[key])
		}
	}
	if values["sql_seconds_total"] < 0 {
		t.Fatal("negative SQL duration")
	}
	if values["go_goroutines"] < 1 || values["go_heap_bytes"] < 1 {
		t.Fatal("runtime snapshot missing")
	}
	m.Events = func() map[string]float64 { return map[string]float64{"listener_ready": 1, "subscribers": 3} }
	if m.Snapshot()["listener_ready"] != 1 || m.Snapshot()["subscribers"] != 3 {
		t.Fatal("event metrics missing")
	}
}
func TestMetricsHTTPFormatsAndNoSecrets(t *testing.T) {
	m := &Metrics{}
	m.Requests.Add(2)
	for _, accept := range []string{"application/json", "text/plain"} {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("Accept", accept)
		req.Header.Set("Authorization", "Bearer secret-token")
		req.Header.Set("X-User-ID", "user-123")
		recorder := httptest.NewRecorder()
		m.ServeHTTP(recorder, req)
		if recorder.Code != 200 || strings.Contains(recorder.Body.String(), "secret-token") || strings.Contains(recorder.Body.String(), "user-123") {
			t.Fatalf("invalid output: %s", recorder.Body.String())
		}
		if accept == "application/json" {
			var values map[string]float64
			if err := json.Unmarshal(recorder.Body.Bytes(), &values); err != nil || values["requests_total"] != 2 {
				t.Fatalf("JSON: %v %v", values, err)
			}
		} else if !strings.Contains(recorder.Body.String(), "ozon_requests_total 2") || !strings.Contains(recorder.Header().Get("Content-Type"), "text/plain") {
			t.Fatalf("Prometheus: %s", recorder.Body.String())
		}
	}
}

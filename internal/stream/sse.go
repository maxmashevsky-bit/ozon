package stream

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
)

type readyKey struct{}

func Ready(ctx context.Context) {
	if ch, ok := ctx.Value(readyKey{}).(chan struct{}); ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

type SSE struct{ Heartbeat, WriteTimeout time.Duration }

func (SSE) Supports(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}
func (t SSE) Do(w http.ResponseWriter, r *http.Request, exec graphql.GraphExecutor) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	ready := make(chan struct{}, 1)
	ctx = context.WithValue(ctx, readyKey{}, ready)
	var params graphql.RawParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	params.Headers = r.Header
	op, errs := exec.CreateOperationContext(ctx, &params)
	ctx = graphql.WithOperationContext(ctx, op)
	if len(errs) > 0 {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(exec.DispatchError(ctx, errs))
		return
	}
	if op.Operation.Operation != ast.Subscription {
		http.Error(w, "SSE requires a subscription", http.StatusBadRequest)
		return
	}
	responses, ctx := exec.DispatchOperation(ctx, op)
	next := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(next)
		for {
			response := responses(ctx)
			if response == nil {
				return
			}
			// gqlgen may reuse Response.Data on the next call to responses.
			// Encode while this goroutine still owns the current response.
			data, err := json.Marshal(response)
			if err != nil {
				return
			}
			select {
			case next <- data:
			case <-ctx.Done():
				return
			}
		}
	}()
	// All writes, including heartbeat, happen on this goroutine. A deadline
	// prevents a slow socket from retaining the operation and its subscriber.
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	write := func(data string) bool {
		if controller.SetWriteDeadline(time.Now().Add(t.WriteTimeout)) != nil {
			return false
		}
		if _, err := w.Write([]byte(data)); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	if !write(": connected\n\n") {
		return
	}
	ticker := time.NewTicker(t.Heartbeat)
	defer ticker.Stop()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(t.WriteTimeout):
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ready:
			if !write(": ready\n\n") {
				return
			}
		case <-ticker.C:
			if !write(": heartbeat\n\n") {
				return
			}
		case data, ok := <-next:
			if !ok {
				write("event: complete\n\n")
				return
			}
			if !write("event: next\ndata: " + string(data) + "\n\n") {
				return
			}
		}
	}
}

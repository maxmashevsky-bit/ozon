package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/ozon/internal/core"
)

type waitingStore struct {
	core.Store
	entered chan struct{}
}

func (s waitingStore) GetPost(ctx context.Context, _ int64) (core.Post, error) {
	s.entered <- struct{}{}
	<-ctx.Done()
	return core.Post{}, ctx.Err()
}
func TestOperationCapacityAndDeadline(t *testing.T) {
	entered := make(chan struct{}, 1)
	h := New(core.NewService(waitingStore{entered: entered}), slog.New(slog.NewTextHandler(io.Discard, nil)), Options{MaxOperations: 1, RequestTimeout: 100 * time.Millisecond})
	call := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/query", strings.NewReader(`{"query":"{post(id:\"1\"){id}}"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- call() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first operation did not start")
	}
	if w := call(); w.Code != 503 || !strings.Contains(w.Body.String(), "BUSY") {
		t.Fatal("parallel operation not rejected", w.Body)
	}
	select {
	case w := <-done:
		if !strings.Contains(w.Body.String(), "UNAVAILABLE") {
			t.Fatal(w.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("operation deadline ignored")
	}
}

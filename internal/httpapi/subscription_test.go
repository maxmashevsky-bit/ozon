package httpapi

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/ozon/internal/auth"
	"example.com/ozon/internal/core"
	"example.com/ozon/internal/store/memory"
)

type observedStore struct {
	core.Store
	ready chan struct{}
}

func (s observedStore) Subscribe(ctx context.Context, id int64) (<-chan core.Comment, error) {
	ch, err := s.Store.Subscribe(ctx, id)
	close(s.ready)
	return ch, err
}
func TestSubscriptionSSE(t *testing.T) {
	ready := make(chan struct{})
	s := core.NewService(observedStore{memory.New(), ready})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(s, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Auth: auth.DevHeader()}))
	defer server.Close()
	body := fmt.Sprintf(`{"query":"subscription {commentAdded(postId:\"%d\"){id text authorId}}"}`, p.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/query", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("not SSE: %s", resp.Header)
	}
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("subscription not ready")
	}
	if _, err = s.AddComment(ctx, "bob", p.ID, nil, "live event"); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			if !strings.Contains(line, `"text":"live event"`) || !strings.Contains(line, `"authorId":"bob"`) {
				t.Fatalf("wrong event: %s", line)
			}
			return
		}
	}
	t.Fatalf("event missing: %v", scanner.Err())
}

func TestHeartbeatCapacityAndCancellation(t *testing.T) {
	store := memory.New()
	s := core.NewService(store)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(s, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Heartbeat: 20 * time.Millisecond, MaxSubscriptions: 1}))
	defer server.Close()
	open := func() *http.Response {
		body := fmt.Sprintf(`{"query":"subscription {commentAdded(postId:\"%d\"){id}}"}`, p.ID)
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/query", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream")
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	response := open()
	scanner := bufio.NewScanner(response.Body)
	found := false
	for scanner.Scan() {
		if scanner.Text() == ": heartbeat" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("heartbeat missing")
	}
	second := open()
	if second.StatusCode != 503 {
		t.Fatalf("subscription cap: %d", second.StatusCode)
	}
	second.Body.Close()
	response.Body.Close()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for store.EventMetrics()["subscribers"] != 0 {
		select {
		case <-deadline.C:
			t.Fatal("subscriber leaked after cancellation")
		case <-ticker.C:
		}
	}
}

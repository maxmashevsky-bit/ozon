package stream_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/graph"
	"example.com/ozon/internal/store/memory"
	"example.com/ozon/internal/stream"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/executor"
)

type observingExecutor struct {
	graphql.GraphExecutor
	produced chan struct{}
}

func (e observingExecutor) DispatchOperation(ctx context.Context, op *graphql.OperationContext) (graphql.ResponseHandler, context.Context) {
	next, ctx := e.GraphExecutor.DispatchOperation(ctx, op)
	return func(ctx context.Context) *graphql.Response {
		response := next(ctx)
		if response != nil {
			e.produced <- struct{}{}
		}
		return response
	}, ctx
}

type blockedReadyWriter struct {
	http.ResponseWriter
	blocked chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *blockedReadyWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(": ready")) {
		w.once.Do(func() { close(w.blocked) })
		<-w.release
	}
	return w.ResponseWriter.Write(p)
}
func (w *blockedReadyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestSSEOwnsResponseDataBeforeNextExecution(t *testing.T) {
	service := core.NewService(memory.New())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	post, err := service.CreatePost(ctx, "alice", "title", "text")
	if err != nil {
		t.Fatal(err)
	}
	exec := observingExecutor{GraphExecutor: executor.New(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{Service: service}})), produced: make(chan struct{}, 2)}
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	transport := stream.SSE{Heartbeat: 20 * time.Millisecond, WriteTimeout: time.Second}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transport.Do(&blockedReadyWriter{ResponseWriter: w, blocked: blocked, release: release}, r.WithContext(graphql.StartOperationTrace(r.Context())), exec)
	}))
	defer server.Close()
	query := fmt.Sprintf(`subscription{commentAdded(postId:"%d"){id text}}`, post.ID)
	body, _ := json.Marshal(map[string]string{"query": query})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("ready write was not blocked")
	}
	first, err := service.AddComment(ctx, "bob", post.ID, nil, strings.Repeat("first-long-", 80))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.AddComment(ctx, "bob", post.ID, nil, "two")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-exec.produced:
		case <-ctx.Done():
			t.Fatal("executor did not produce both responses")
		}
	}
	unblock()
	events := make(chan struct{ ID, Text string }, 3)
	readErrors := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var payload struct {
				Data struct {
					Comment struct{ ID, Text string } `json:"commentAdded"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &payload); err != nil {
				readErrors <- err
				return
			}
			events <- payload.Data.Comment
		}
		if err := scanner.Err(); err != nil && err != io.EOF {
			readErrors <- err
		}
	}()
	want := []struct{ ID, Text string }{{fmt.Sprint(first.ID), first.Text}, {fmt.Sprint(second.ID), second.Text}}
	for _, expected := range want {
		select {
		case got := <-events:
			if got != expected {
				t.Fatalf("SSE event = %+v, want %+v", got, expected)
			}
		case err := <-readErrors:
			t.Fatalf("SSE read: %v", err)
		case <-ctx.Done():
			t.Fatal("missing SSE event")
		}
	}
	select {
	case extra := <-events:
		t.Fatalf("duplicate SSE event: %+v", extra)
	case err := <-readErrors:
		t.Fatalf("SSE read: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

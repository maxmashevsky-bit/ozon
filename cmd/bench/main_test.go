package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestSubscriptionExitCodes(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bench")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build bench: %v: %s", err, output)
	}
	cases := []struct {
		name                                                                string
		dropAll, partial, drop, duplicate, disconnect, setup, mutationError bool
		wantExit                                                            int
	}{
		{name: "delivered"},
		{name: "no_events", dropAll: true, wantExit: 1},
		{name: "partial_delivery", partial: true, wantExit: 1},
		{name: "duplicate_cannot_hide_loss", drop: true, duplicate: true, wantExit: 1},
		{name: "unexpected_disconnect", disconnect: true, wantExit: 1},
		{name: "setup_failure", setup: true, wantExit: 1},
		{name: "mutation_error", mutationError: true, wantExit: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			type event struct{ id, stamp string }
			type stream struct {
				ch   chan event
				done chan struct{}
			}
			var mu sync.Mutex
			streams := map[int]stream{}
			next := 0
			nextID := 0
			pattern := regexp.MustCompile(`bench-sse:(\d+)`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Query string `json:"query"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "bad JSON", 400)
					return
				}
				if strings.HasPrefix(body.Query, "subscription") {
					mu.Lock()
					index := next
					next++
					mu.Unlock()
					if tc.setup && index == 1 {
						http.Error(w, "unavailable", 503)
						return
					}
					s := stream{ch: make(chan event, 2048), done: make(chan struct{})}
					mu.Lock()
					streams[index] = s
					mu.Unlock()
					defer func() { mu.Lock(); delete(streams, index); mu.Unlock() }()
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, ": ready\n\n")
					w.(http.Flusher).Flush()
					for {
						select {
						case item := <-s.ch:
							fmt.Fprintf(w, "data: {\"data\":{\"commentAdded\":{\"id\":\"%s\",\"text\":\"bench-sse:%s\"}}}\n\n", item.id, item.stamp)
							w.(http.Flusher).Flush()
							if tc.disconnect && index == 1 {
								return
							}
						case <-r.Context().Done():
							return
						case <-s.done:
							return
						}
					}
				}
				if tc.mutationError {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"errors":[{"extensions":{"code":"INVALID"}}]}`)
					return
				}
				id := pattern.FindStringSubmatch(body.Query)
				if len(id) != 2 {
					http.Error(w, "bad query", 400)
					return
				}
				mu.Lock()
				nextID++
				item := event{id: fmt.Sprint(nextID), stamp: id[1]}
				for index, s := range streams {
					if tc.dropAll || tc.drop && index == 1 || tc.partial && index == 1 && nextID%2 == 0 {
						continue
					}
					s.ch <- item
					if tc.duplicate && index == 0 {
						s.ch <- item
					}
				}
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"data":{"addComment":{"id":"%s"}}}`, item.id)
			}))
			defer server.Close()
			out := filepath.Join(t.TempDir(), "result.json")
			cmd := exec.Command(binary, "-url", server.URL, "-scenario", "subscriptions", "-concurrency", "2", "-warmup", "0s", "-duration", "100ms", "-out", out)
			output, err := cmd.CombinedOutput()
			exit := 0
			if err != nil {
				if e, ok := err.(*exec.ExitError); ok {
					exit = e.ExitCode()
				} else {
					t.Fatalf("run: %v", err)
				}
			}
			if exit != tc.wantExit {
				t.Fatalf("exit=%d, want %d: %s", exit, tc.wantExit, output)
			}
			var result Result
			b, readErr := os.ReadFile(out)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if err := json.Unmarshal(b, &result); err != nil {
				t.Fatal(err)
			}
			if (tc.dropAll || tc.partial || tc.drop) && result.MissingEvents == 0 {
				t.Fatalf("missing events unreported: %s", b)
			}
			if tc.duplicate && result.DuplicateEvents == 0 {
				t.Fatalf("duplicates unreported: %s", b)
			}
			if tc.disconnect && result.DisconnectReasons["stream_eof"] == 0 {
				t.Fatalf("disconnect unreported: %s", b)
			}
			if tc.setup && result.DisconnectReasons["setup_http_503"] == 0 {
				t.Fatalf("setup unreported: %s", b)
			}
			if tc.mutationError && result.Errors["graphql"] == 0 {
				t.Fatalf("mutation error unreported: %s", b)
			}
			if !tc.dropAll && !tc.partial && !tc.drop && !tc.disconnect && !tc.setup && !tc.mutationError && (result.MissingEvents != 0 || result.ExpectedEvents == 0) {
				t.Fatalf("expected full delivery: %s", b)
			}
		})
	}
}

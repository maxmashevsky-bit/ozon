package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/graph/model"
	"example.com/ozon/internal/httpapi"
	"example.com/ozon/internal/store/memory"
)

func TestWatchStopsOnConsumerFailureOrCancellation(t *testing.T) {
	service := core.NewService(memory.New())
	p, err := service.CreatePost(context.Background(), "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = service.AddComment(context.Background(), "a", p.ID, nil, "x"); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(httpapi.New(service, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	for _, cancelConsumer := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		count := 0
		failure := errors.New("consumer failed")
		got := (Client{URL: server.URL}).Watch(ctx, "1", func(*model.Comment) error {
			count++
			if cancelConsumer {
				cancel()
				return nil
			}
			return failure
		})
		cancel()
		if count != 1 {
			t.Fatalf("consumer called %d times after stop", count)
		}
		if cancelConsumer {
			if !errors.Is(got, context.Canceled) {
				t.Fatal(got)
			}
		} else if !errors.Is(got, failure) {
			t.Fatal(got)
		}
	}
}

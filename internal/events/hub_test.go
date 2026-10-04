package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"example.com/ozon/internal/core"
)

func TestSlowSubscriberAndPause(t *testing.T) {
	h := New(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow, err := h.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= BufferSize; i++ {
		h.Publish(core.Comment{ID: int64(i + 1), PostID: 1})
	}
	count := 0
	for range slow {
		count++
	}
	if count != BufferSize {
		t.Fatalf("buffer size %d", count)
	}
	ch, err := h.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	h.Pause()
	if _, ok := <-ch; ok {
		t.Fatal("pause did not close subscriber")
	}
	if _, err = h.Subscribe(ctx, 1); err == nil {
		t.Fatal("paused hub accepted subscriber")
	}
	h.Resume()
	if _, err = h.Subscribe(ctx, 1); err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentCancelAndPublish(t *testing.T) {
	h := New(true)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			ch, err := h.Subscribe(ctx, 1)
			if err != nil {
				t.Error(err)
				cancel()
				return
			}
			h.Publish(core.Comment{PostID: 1})
			cancel()
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						return
					}
				case <-time.After(time.Second):
					t.Error("canceled subscription remained open")
					return
				}
			}
		}()
	}
	wg.Wait()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.count != 0 {
		t.Fatalf("leaked %d subscribers", h.count)
	}
}

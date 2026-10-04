package events

import (
	"context"
	"sync"
	"sync/atomic"

	"example.com/ozon/internal/core"
)

const BufferSize = 64
const MaxSubscribers = 1024

type subscriber struct {
	channel chan core.Comment
	stop    func() bool
}
type Config struct {
	BufferSize     int
	MaxSubscribers int
}
type Hub struct {
	config    Config
	slow      atomic.Int64
	published atomic.Int64
	mu        sync.Mutex
	active    bool
	count     int
	posts     map[int64]map[*subscriber]struct{}
}

func New(active bool, options ...Config) *Hub {
	cfg := Config{BufferSize: BufferSize, MaxSubscribers: MaxSubscribers}
	if len(options) > 0 {
		cfg = options[0]
	}
	if cfg.BufferSize < 1 || cfg.MaxSubscribers < 1 {
		panic("invalid subscription limits")
	}
	return &Hub{active: active, config: cfg, posts: make(map[int64]map[*subscriber]struct{})}
}
func (h *Hub) Subscribe(ctx context.Context, postID int64) (<-chan core.Comment, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !h.active || h.count >= h.config.MaxSubscribers {
		return nil, core.Fail(core.Unavailable, "subscriptions temporarily unavailable")
	}
	sub := &subscriber{channel: make(chan core.Comment, h.config.BufferSize)}
	if h.posts[postID] == nil {
		h.posts[postID] = make(map[*subscriber]struct{})
	}
	h.posts[postID][sub] = struct{}{}
	h.count++
	sub.stop = context.AfterFunc(ctx, func() { h.mu.Lock(); defer h.mu.Unlock(); h.remove(postID, sub) })
	return sub.channel, nil
}
func (h *Hub) remove(postID int64, sub *subscriber) {
	if _, ok := h.posts[postID][sub]; !ok {
		return
	}
	delete(h.posts[postID], sub)
	h.count--
	close(sub.channel)
	sub.stop()
	if len(h.posts[postID]) == 0 {
		delete(h.posts, postID)
	}
}
func (h *Hub) Publish(c core.Comment) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.published.Add(1)
	for sub := range h.posts[c.PostID] {
		copy := c
		if c.ParentID != nil {
			value := *c.ParentID
			copy.ParentID = &value
		}
		select {
		case sub.channel <- copy:
		default:
			// Disconnect slow consumers instead of blocking writes or silently dropping
			// an arbitrary event. Reconnection requires a fresh read of stored comments.
			h.slow.Add(1)
			h.remove(c.PostID, sub)
		}
	}
}
func (h *Hub) Pause() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active = false
	for postID, subscribers := range h.posts {
		for sub := range subscribers {
			h.remove(postID, sub)
		}
	}
}
func (h *Hub) Resume() { h.mu.Lock(); h.active = true; h.mu.Unlock() }

func (h *Hub) Metrics() map[string]float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	active := 0.0
	if h.active {
		active = 1
	}
	return map[string]float64{"subscribers": float64(h.count), "subscription_slow_disconnects_total": float64(h.slow.Load()), "events_published_total": float64(h.published.Load()), "listener_ready": active}
}

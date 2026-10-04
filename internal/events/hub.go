package events

import (
	"context"
	"sync"

	"example.com/ozon/internal/core"
)

const BufferSize = 64
const MaxSubscribers = 1024

type subscriber struct {
	channel chan core.Comment
	stop    func() bool
}
type Hub struct {
	mu     sync.Mutex
	active bool
	count  int
	posts  map[int64]map[*subscriber]struct{}
}

func New(active bool) *Hub {
	return &Hub{active: active, posts: make(map[int64]map[*subscriber]struct{})}
}
func (h *Hub) Subscribe(ctx context.Context, postID int64) (<-chan core.Comment, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !h.active || h.count >= MaxSubscribers {
		return nil, core.Fail(core.Unavailable, "subscriptions temporarily unavailable")
	}
	sub := &subscriber{channel: make(chan core.Comment, BufferSize)}
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

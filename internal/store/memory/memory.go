package memory

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/events"
)

type postState struct {
	mu       sync.RWMutex
	post     core.Post
	children map[int64][]core.Comment
	feed     []core.Comment
}
type Store struct {
	hub         *events.Hub
	mu          sync.RWMutex
	posts       map[int64]*postState
	postIDs     []int64
	comments    sync.Map
	nextPost    int64
	nextComment atomic.Int64
}

func New(config ...events.Config) *Store {
	return &Store{hub: events.New(true, config...), posts: make(map[int64]*postState)}
}
func (s *Store) CreatePost(ctx context.Context, in core.NewPost) (core.Post, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return core.Post{}, err
	}
	s.nextPost++
	p := core.Post{ID: s.nextPost, AuthorID: in.AuthorID, Title: in.Title, Text: in.Text, CommentsAllowed: true, CreatedAt: time.Now().UTC()}
	s.posts[p.ID] = &postState{post: p, children: make(map[int64][]core.Comment)}
	s.postIDs = append(s.postIDs, p.ID)
	return p, nil
}
func (s *Store) state(id int64) (*postState, error) {
	s.mu.RLock()
	p := s.posts[id]
	s.mu.RUnlock()
	if p == nil {
		return nil, core.Fail(core.NotFound, "post not found")
	}
	return p, nil
}
func (s *Store) GetPost(ctx context.Context, id int64) (core.Post, error) {
	p, err := s.state(id)
	if err != nil {
		return core.Post{}, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err = ctx.Err(); err != nil {
		return core.Post{}, err
	}
	return p.post, nil
}
func (s *Store) ListPosts(ctx context.Context, after int64, limit int) ([]core.Post, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	start := sort.Search(len(s.postIDs), func(i int) bool { return s.postIDs[i] > after })
	end := min(start+limit, len(s.postIDs))
	states := make([]*postState, 0, end-start)
	for _, id := range s.postIDs[start:end] {
		states = append(states, s.posts[id])
	}
	s.mu.RUnlock()
	result := make([]core.Post, 0, len(states))
	for _, p := range states {
		p.mu.RLock()
		result = append(result, p.post)
		p.mu.RUnlock()
	}
	return result, nil
}
func clone(c core.Comment) core.Comment {
	if c.ParentID != nil {
		v := *c.ParentID
		c.ParentID = &v
	}
	return c
}
func (s *Store) GetComment(ctx context.Context, id int64) (core.Comment, error) {
	if err := ctx.Err(); err != nil {
		return core.Comment{}, err
	}
	c, ok := s.comments.Load(id)
	if !ok {
		return core.Comment{}, core.Fail(core.NotFound, "comment not found")
	}
	return clone(c.(core.Comment)), nil
}
func (s *Store) ListComments(ctx context.Context, postID int64, parentID *int64, after int64, limit int) ([]core.Comment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := s.state(postID)
	if err != nil {
		return nil, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	var parent int64
	if parentID != nil {
		parent = *parentID
	}
	list := p.children[parent]
	start := sort.Search(len(list), func(i int) bool { return list[i].ID > after })
	end := min(start+limit, len(list))
	result := make([]core.Comment, 0, end-start)
	for _, c := range list[start:end] {
		result = append(result, clone(c))
	}
	return result, nil
}
func (s *Store) WithinPost(ctx context.Context, id int64, fn func(core.PostTx) error) error {
	p, err := s.state(id)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	tx := &postTx{store: s, post: p.post}
	if err = fn(tx); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	p.post = tx.post
	for _, c := range tx.added {
		var parent int64
		if c.ParentID != nil {
			parent = *c.ParentID
		}
		p.children[parent] = append(p.children[parent], c)
		p.feed = append(p.feed, c)
		s.comments.Store(c.ID, c)
		s.hub.Publish(c)
	}
	return nil
}

type postTx struct {
	store *Store
	post  core.Post
	added []core.Comment
}

func (t *postTx) Post() core.Post { return t.post }
func (t *postTx) GetComment(ctx context.Context, id int64) (core.Comment, error) {
	for _, c := range t.added {
		if c.ID == id {
			return clone(c), nil
		}
	}
	return t.store.GetComment(ctx, id)
}
func (t *postTx) SetCommentsAllowed(ctx context.Context, allowed bool) (core.Post, error) {
	if err := ctx.Err(); err != nil {
		return core.Post{}, err
	}
	t.post.CommentsAllowed = allowed
	return t.post, nil
}
func (t *postTx) AddComment(ctx context.Context, in core.NewComment) (core.Comment, error) {
	if err := ctx.Err(); err != nil {
		return core.Comment{}, err
	}
	if in.PostID != t.post.ID {
		return core.Comment{}, core.Fail(core.Invalid, "comment belongs to another post")
	}
	if in.ParentID != nil {
		p, err := t.GetComment(ctx, *in.ParentID)
		if err != nil {
			return core.Comment{}, err
		}
		if p.PostID != in.PostID {
			return core.Comment{}, core.Fail(core.Invalid, "parent comment belongs to another post")
		}
	}
	c := clone(core.Comment{ID: t.store.nextComment.Add(1), PostID: in.PostID, ParentID: in.ParentID, AuthorID: in.AuthorID, Text: in.Text, CreatedAt: time.Now().UTC()})
	t.added = append(t.added, c)
	return clone(c), nil
}

func (s *Store) Subscribe(ctx context.Context, postID int64) (<-chan core.Comment, error) {
	return s.hub.Subscribe(ctx, postID)
}

func (s *Store) EventMetrics() map[string]float64 { return s.hub.Metrics() }
func (s *Store) CloseSubscriptions()              { s.hub.Pause() }

func (s *Store) GetComments(ctx context.Context, ids []int64) ([]core.Comment, error) {
	result := make([]core.Comment, 0, len(ids))
	seen := map[int64]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		c, err := s.GetComment(ctx, id)
		if err != nil {
			if core.CodeOf(err) == core.NotFound {
				continue
			}
			return nil, err
		}
		result = append(result, c)
	}
	return result, nil
}
func (s *Store) ListCommentBranches(ctx context.Context, postID int64, branches []core.BranchQuery) ([][]core.Comment, error) {
	result := make([][]core.Comment, len(branches))
	for i, b := range branches {
		rows, err := s.ListComments(ctx, postID, &b.ParentID, b.After, b.Limit)
		if err != nil {
			return nil, err
		}
		result[i] = rows
	}
	return result, nil
}
func (s *Store) ListCommentFeed(ctx context.Context, postID int64, after int64, limit int) ([]core.Comment, error) {
	state, err := s.state(postID)
	if err != nil {
		return nil, err
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	start := sort.Search(len(state.feed), func(i int) bool { return state.feed[i].ID > after })
	result := make([]core.Comment, 0, limit)
	for _, c := range state.feed[start:min(start+limit, len(state.feed))] {
		result = append(result, clone(c))
	}
	return result, nil
}

package contract

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"example.com/ozon/internal/core"
)

func Run(t *testing.T, factory func(*testing.T) core.Store) {
	t.Helper()
	ctx := context.Background()
	t.Run("posts_and_authorization", func(t *testing.T) {
		s := core.NewService(factory(t))
		p, err := s.CreatePost(ctx, "alice", "Title", "Text")
		must(t, err)
		if p.ID < 1 || p.AuthorID != "alice" || !p.CommentsAllowed || p.CreatedAt.IsZero() {
			t.Fatalf("bad post: %+v", p)
		}
		got, err := s.Post(ctx, p.ID)
		must(t, err)
		if got != p {
			t.Fatalf("post mismatch: %+v %+v", p, got)
		}
		_, err = s.SetCommentsAllowed(ctx, "bob", p.ID, false)
		code(t, err, core.Forbidden)
		_, err = s.SetCommentsAllowed(ctx, "alice", p.ID, false)
		must(t, err)
		_, err = s.AddComment(ctx, "bob", p.ID, nil, "no")
		code(t, err, core.Forbidden)
		_, err = s.SetCommentsAllowed(ctx, "alice", p.ID, true)
		must(t, err)
		_, err = s.AddComment(ctx, "bob", p.ID, nil, "yes")
		must(t, err)
		_, err = s.Post(ctx, 99999999)
		code(t, err, core.NotFound)
		_, err = s.Comment(ctx, 99999999)
		code(t, err, core.NotFound)
		_, err = s.AddComment(ctx, "bob", 99999999, nil, "missing")
		code(t, err, core.NotFound)
		_, err = s.CreatePost(ctx, "", "t", "x")
		code(t, err, core.Unauthenticated)
	})
	t.Run("unicode_and_validation", func(t *testing.T) {
		s := core.NewService(factory(t))
		p, err := s.CreatePost(ctx, "a", "t", "x")
		must(t, err)
		c, err := s.AddComment(ctx, "b", p.ID, nil, strings.Repeat("🙂", 2000))
		must(t, err)
		if len([]rune(c.Text)) != 2000 {
			t.Fatal("Unicode text changed")
		}
		for _, v := range []string{strings.Repeat("я", 2001), " \n\t", "", "a\x00b", string([]byte{0xff})} {
			_, err = s.AddComment(ctx, "b", p.ID, nil, v)
			code(t, err, core.Invalid)
		}
		for _, actor := range []string{"a b", "русский", strings.Repeat("a", 65)} {
			_, err = s.AddComment(ctx, actor, p.ID, nil, "x")
			code(t, err, core.Invalid)
		}
		_, err = s.CreatePost(ctx, "a", strings.Repeat("t", 201), "x")
		code(t, err, core.Invalid)
	})
	t.Run("hierarchy_and_cursor_scope", func(t *testing.T) {
		s := core.NewService(factory(t))
		p, err := s.CreatePost(ctx, "a", "t", "x")
		must(t, err)
		p2, err := s.CreatePost(ctx, "a", "t2", "x")
		must(t, err)
		roots := make([]core.Comment, 3)
		for i := range roots {
			roots[i], err = s.AddComment(ctx, "b", p.ID, nil, fmt.Sprint(i))
			must(t, err)
		}
		reply, err := s.AddComment(ctx, "b", p.ID, &roots[0].ID, "reply")
		must(t, err)
		_, err = s.AddComment(ctx, "b", p2.ID, &roots[0].ID, "wrong")
		code(t, err, core.Invalid)
		missing := int64(99999999)
		_, err = s.AddComment(ctx, "b", p.ID, &missing, "missing")
		code(t, err, core.NotFound)
		first, err := s.Comments(ctx, p.ID, nil, core.PageInput{First: 2})
		must(t, err)
		if len(first.Edges) != 2 || !first.PageInfo.HasNextPage || first.PageInfo.EndCursor == nil {
			t.Fatalf("bad first page: %+v", first)
		}
		next, err := s.Comments(ctx, p.ID, nil, core.PageInput{First: 2, After: *first.PageInfo.EndCursor})
		must(t, err)
		if len(next.Edges) != 1 || next.Edges[0].Node.ID != roots[2].ID || next.PageInfo.HasNextPage {
			t.Fatalf("bad next page: %+v", next)
		}
		empty, err := s.Comments(ctx, p.ID, nil, core.PageInput{First: 2, After: *next.PageInfo.EndCursor})
		must(t, err)
		if len(empty.Edges) != 0 || empty.PageInfo.HasNextPage || empty.PageInfo.EndCursor != nil {
			t.Fatalf("bad empty page: %+v", empty)
		}
		replies, err := s.Comments(ctx, p.ID, &roots[0].ID, core.PageInput{First: 10})
		must(t, err)
		if len(replies.Edges) != 1 || replies.Edges[0].Node.ID != reply.ID {
			t.Fatalf("bad replies: %+v", replies)
		}
		for _, page := range []core.PageInput{{First: 0}, {First: 101}, {First: -1}, {First: 1, After: "garbage"}} {
			_, err = s.Comments(ctx, p.ID, nil, page)
			code(t, err, core.Invalid)
		}
		_, err = s.Comments(ctx, p2.ID, nil, core.PageInput{First: 1, After: *first.PageInfo.EndCursor})
		code(t, err, core.Invalid)
		_, err = s.Comments(ctx, p.ID, &roots[0].ID, core.PageInput{First: 1, After: *first.PageInfo.EndCursor})
		code(t, err, core.Invalid)
		_, err = s.Comments(ctx, p2.ID, &roots[0].ID, core.PageInput{First: 1})
		code(t, err, core.Invalid)
		_, err = s.Comments(ctx, p.ID, &missing, core.PageInput{First: 1})
		code(t, err, core.NotFound)
		_, err = s.Comments(ctx, missing, nil, core.PageInput{First: 1})
		code(t, err, core.NotFound)
		posts, err := s.Posts(ctx, core.PageInput{First: 1})
		must(t, err)
		if len(posts.Edges) != 1 || posts.Edges[0].Node.ID != p.ID || !posts.PageInfo.HasNextPage {
			t.Fatalf("bad posts: %+v", posts)
		}
		more, err := s.Posts(ctx, core.PageInput{First: 1, After: *posts.PageInfo.EndCursor})
		must(t, err)
		if len(more.Edges) != 1 || more.Edges[0].Node.ID != p2.ID || more.PageInfo.HasNextPage {
			t.Fatalf("bad posts: %+v", more)
		}
		_, err = s.Posts(ctx, core.PageInput{First: 1, After: *first.PageInfo.EndCursor})
		code(t, err, core.Invalid)
	})
	t.Run("deep_tree_and_copy_isolation", func(t *testing.T) {
		s := core.NewService(factory(t))
		p, err := s.CreatePost(ctx, "a", "t", "x")
		must(t, err)
		root, err := s.AddComment(ctx, "a", p.ID, nil, "root")
		must(t, err)
		previous := root.ID
		for i := 0; i < 200; i++ {
			parent := previous
			c, err := s.AddComment(ctx, "a", p.ID, &parent, "child")
			must(t, err)
			parent = -1
			got, err := s.Comment(ctx, c.ID)
			must(t, err)
			if got.ParentID == nil || *got.ParentID != previous {
				t.Fatal("input pointer changed stored parent")
			}
			*got.ParentID = -2
			again, err := s.Comment(ctx, c.ID)
			must(t, err)
			if *again.ParentID != previous {
				t.Fatal("returned pointer changed stored parent")
			}
			previous = c.ID
		}
		page, err := s.Comments(ctx, p.ID, &root.ID, core.PageInput{First: 10})
		must(t, err)
		if len(page.Edges) != 1 {
			t.Fatalf("expected direct child only, got %d", len(page.Edges))
		}
	})
	t.Run("rollback_and_cancellation", func(t *testing.T) {
		store := factory(t)
		s := core.NewService(store)
		p, err := s.CreatePost(ctx, "a", "t", "x")
		must(t, err)
		abort := errors.New("abort")
		var c core.Comment
		err = store.WithinPost(ctx, p.ID, func(tx core.PostTx) error {
			c, err = tx.AddComment(ctx, core.NewComment{PostID: p.ID, AuthorID: "a", Text: "rolled back"})
			if err != nil {
				return err
			}
			if _, err = tx.SetCommentsAllowed(ctx, false); err != nil {
				return err
			}
			return abort
		})
		if !errors.Is(err, abort) {
			t.Fatalf("expected abort: %v", err)
		}
		_, err = s.Comment(ctx, c.ID)
		code(t, err, core.NotFound)
		got, err := s.Post(ctx, p.ID)
		must(t, err)
		if !got.CommentsAllowed {
			t.Fatal("rollback did not restore post")
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = s.AddComment(canceled, "a", p.ID, nil, "canceled")
		if err == nil {
			t.Fatal("canceled write succeeded")
		}
	})
	t.Run("concurrent_close_and_reads", func(t *testing.T) {
		s := core.NewService(factory(t))
		p, err := s.CreatePost(ctx, "a", "t", "x")
		must(t, err)
		var successes atomic.Int64
		start := make(chan struct{})
		errs := make(chan error, 100)
		var wg sync.WaitGroup
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := s.AddComment(ctx, "b", p.ID, nil, "racing"); err == nil {
					successes.Add(1)
				} else if core.CodeOf(err) != core.Forbidden {
					errs <- err
				}
				if _, err := s.Comments(ctx, p.ID, nil, core.PageInput{First: 100}); err != nil {
					errs <- err
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.SetCommentsAllowed(ctx, "a", p.ID, false)
			if err != nil {
				errs <- err
			}
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		for i := 0; i < 10; i++ {
			_, err = s.AddComment(ctx, "b", p.ID, nil, "after close")
			code(t, err, core.Forbidden)
		}
		page, err := s.Comments(ctx, p.ID, nil, core.PageInput{First: 100})
		must(t, err)
		if int64(len(page.Edges)) != successes.Load() {
			t.Fatalf("lost writes: %d vs %d", len(page.Edges), successes.Load())
		}
		for i := 1; i < len(page.Edges); i++ {
			if page.Edges[i-1].Node.ID >= page.Edges[i].Node.ID {
				t.Fatal("unordered IDs")
			}
		}
	})
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func code(t *testing.T, err error, want core.Code) {
	t.Helper()
	if err == nil || core.CodeOf(err) != want {
		t.Fatalf("wanted %s, got %v", want, err)
	}
}

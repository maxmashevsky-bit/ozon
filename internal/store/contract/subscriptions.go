package contract

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/ozon/internal/core"
)

func Subscriptions(t *testing.T, store core.Store, writer core.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := core.NewService(store)
	w := core.NewService(writer)
	post, err := w.CreatePost(ctx, "a", "t", "x")
	must(t, err)
	other, err := w.CreatePost(ctx, "a", "other", "x")
	must(t, err)
	_, err = s.Subscribe(ctx, 99999999)
	code(t, err, core.NotFound)
	ch, err := s.Subscribe(ctx, post.ID)
	must(t, err)
	_, err = w.AddComment(ctx, "a", other.ID, nil, "different post")
	must(t, err)
	abort := errors.New("abort")
	err = writer.WithinPost(ctx, post.ID, func(tx core.PostTx) error {
		if _, err := tx.AddComment(ctx, core.NewComment{PostID: post.ID, AuthorID: "a", Text: "rolled back"}); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	c, err := w.AddComment(ctx, "a", post.ID, nil, "committed")
	must(t, err)
	select {
	case got, ok := <-ch:
		if !ok || got.ID != c.ID || got.Text != c.Text {
			t.Fatalf("wrong event: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("notification not delivered")
	}
	reply, err := w.AddComment(ctx, "a", post.ID, &c.ID, "reply")
	must(t, err)
	select {
	case got := <-ch:
		if got.ID != reply.ID || got.ParentID == nil || *got.ParentID != c.ID {
			t.Fatalf("wrong reply event: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("reply notification not delivered")
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected buffered event")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription not cleaned up")
	}
}

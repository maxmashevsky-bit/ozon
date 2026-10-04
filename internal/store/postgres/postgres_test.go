//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/store/contract"
	"example.com/ozon/internal/store/postgres"
	"example.com/ozon/internal/testdb"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) core.Store { pool, _ := testdb.New(t); return postgres.New(pool) })
}
func TestDatabaseTreeConstraints(t *testing.T) {
	pool, _ := testdb.New(t)
	s := core.NewService(postgres.New(pool))
	ctx := context.Background()
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreatePost(ctx, "a", "other", "x")
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.AddComment(ctx, "a", p.ID, nil, "root")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		sql  string
		args []any
		code string
	}{
		{"INSERT INTO comments(post_id,parent_id,author_id,text) VALUES($1,$2,'a','x')", []any{other.ID, c.ID}, "23503"},
		{"INSERT INTO comments(post_id,author_id,text) VALUES(99999999,'a','x')", nil, "23503"},
		{"UPDATE comments SET parent_id=id WHERE id=$1", []any{c.ID}, "23514"},
		{"UPDATE comments SET post_id=$1 WHERE id=$2", []any{other.ID, c.ID}, "23514"},
	}
	for _, tc := range cases {
		_, err := pool.Exec(ctx, tc.sql, tc.args...)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != tc.code {
			t.Fatalf("wanted %s, got %v", tc.code, err)
		}
	}
}
func TestClosingHoldsRowLock(t *testing.T) {
	pool, _ := testdb.New(t)
	s := core.NewService(postgres.New(pool))
	ctx := context.Background()
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "UPDATE posts SET comments_allowed=false WHERE id=$1", p.ID); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err = s.AddComment(deadline, "b", p.ID, nil, "must wait for lock")
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("insert did not wait on row lock: %v", err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = s.AddComment(ctx, "b", p.ID, nil, "after commit")
	if err == nil || core.CodeOf(err) != core.Forbidden {
		t.Fatalf("wanted closed: %v", err)
	}
	page, err := s.Comments(ctx, p.ID, nil, core.PageInput{First: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Edges) != 0 {
		t.Fatal("comment bypassed row lock")
	}
}

func TestSubscriptionsAcrossInstances(t *testing.T) {
	pool, _ := testdb.New(t)
	listener := postgres.New(pool)
	stop, err := listener.StartNotifications(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// A separate store publishes only via the database trigger, not a shared hub.
	contract.Subscriptions(t, listener, postgres.New(pool))
}

func TestListenerReconnectsAndClosesExistingStreams(t *testing.T) {
	pool, _ := testdb.New(t)
	store := postgres.New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stop, err := store.StartNotifications(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	s := core.NewService(store)
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.Subscribe(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	var killed bool
	err = pool.QueryRow(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND query='LISTEN ozon_comments'`).Scan(&killed)
	if err != nil || !killed {
		t.Fatalf("terminate own listener: %v", err)
	}
	select {
	case _, ok := <-old:
		if ok {
			t.Fatal("unexpected event")
		}
	case <-ctx.Done():
		t.Fatal("old subscription not closed")
	}
	var ch <-chan core.Comment
	for {
		ch, err = s.Subscribe(ctx, p.ID)
		if err == nil {
			break
		}
		if core.CodeOf(err) != core.Unavailable {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("listener not reconnected")
		case <-time.After(20 * time.Millisecond):
		}
	}
	c, err := s.AddComment(ctx, "a", p.ID, nil, "after reconnect")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got.ID != c.ID {
			t.Fatalf("wrong comment: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("new subscription did not receive event")
	}
}

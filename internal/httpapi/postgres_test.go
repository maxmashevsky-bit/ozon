//go:build integration

package httpapi

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/store/postgres"
	"example.com/ozon/internal/testdb"
)

func TestSQLCountDoesNotGrowWithPageSize(t *testing.T) {
	pool, counter := testdb.New(t)
	s := core.NewService(postgres.New(pool))
	ctx := context.Background()
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.AddComment(ctx, "a", p.ID, nil, "root")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err = s.CreatePost(ctx, "a", "t", "x"); err != nil {
			t.Fatal(err)
		}
		if _, err = s.AddComment(ctx, "a", p.ID, nil, "root"); err != nil {
			t.Fatal(err)
		}
		if _, err = s.AddComment(ctx, "a", p.ID, &root.ID, "reply"); err != nil {
			t.Fatal(err)
		}
	}
	h := New(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, n := range []int{1, 50, 100} {
		for _, tc := range []struct {
			query string
			want  int64
		}{
			{fmt.Sprintf(`{posts(first:%d){edges{node{id title text authorId}}}}`, n), 1},
			{fmt.Sprintf(`{comments(postId:"%d",first:%d){edges{node{id text authorId}}}}`, p.ID, n), 2},
			{fmt.Sprintf(`{comments(postId:"%d",parentId:"%d",first:%d){edges{node{id text authorId}}}}`, p.ID, root.ID, n), 3},
		} {
			counter.Selects.Store(0)
			noErrors(t, request(t, h, "", tc.query, nil))
			if got := counter.Selects.Load(); got != tc.want {
				t.Fatalf("page size %d: got %d SQL SELECTs, wanted %d", n, got, tc.want)
			}
		}
	}
}

func TestBatchBranchesSQLCount(t *testing.T) {
	pool, counter := testdb.New(t)
	s := core.NewService(postgres.New(pool))
	ctx := context.Background()
	p, err := s.CreatePost(ctx, "a", "t", "x")
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 20)
	for i := range ids {
		root, err := s.AddComment(ctx, "a", p.ID, nil, "root")
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = root.ID
		for j := 0; j < 3; j++ {
			if _, err = s.AddComment(ctx, "a", p.ID, &root.ID, "child"); err != nil {
				t.Fatal(err)
			}
		}
	}
	h := New(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, n := range []int{1, 10, 20} {
		branches := make([]map[string]any, n)
		for i := range branches {
			branches[i] = map[string]any{"parentId": fmt.Sprint(ids[i]), "first": 2}
		}
		counter.Selects.Store(0)
		result := request(t, h, "", `query($p:ID!,$b:[BranchPageInput!]!){commentBranches(postId:$p,branches:$b){edges{node{id}}pageInfo{hasNextPage endCursor}}}`, map[string]any{"p": fmt.Sprint(p.ID), "b": branches})
		noErrors(t, result)
		if got := counter.Selects.Load(); got != 3 {
			t.Fatalf("%d branches used %d SELECTs", n, got)
		}
	}
}

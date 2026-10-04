package contract

import (
	"context"
	"sync"
	"testing"

	"example.com/ozon/internal/core"
)

func PagesDuringWrites(t *testing.T, store core.Store) {
	t.Helper()
	ctx := context.Background()
	s := core.NewService(store)
	p, err := s.CreatePost(ctx, "a", "t", "x")
	must(t, err)
	roots := make([]core.Comment, 3)
	for i := range roots {
		roots[i], err = s.AddComment(ctx, "a", p.ID, nil, "root")
		must(t, err)
		for j := 0; j < 3; j++ {
			_, err = s.AddComment(ctx, "a", p.ID, &roots[i].ID, "reply")
			must(t, err)
		}
	}
	branches := []core.BranchPageInput{{ParentID: roots[0].ID, Page: core.PageInput{First: 2}}, {ParentID: roots[1].ID, Page: core.PageInput{First: 1}}}
	pages, err := s.CommentBranches(ctx, p.ID, branches)
	must(t, err)
	if len(pages) != 2 || len(pages[0].Edges) != 2 || len(pages[1].Edges) != 1 || !pages[0].PageInfo.HasNextPage {
		t.Fatalf("bad branches: %+v", pages)
	}
	branches[0].Page.After = *pages[0].PageInfo.EndCursor
	branches[1].Page.After = *pages[1].PageInfo.EndCursor
	next, err := s.CommentBranches(ctx, p.ID, branches)
	must(t, err)
	if len(next[0].Edges) != 1 || next[0].PageInfo.HasNextPage || len(next[1].Edges) != 1 {
		t.Fatal("branch pages are not independent")
	}
	branches[1].Page.After = *pages[0].PageInfo.EndCursor
	_, err = s.CommentBranches(ctx, p.ID, branches)
	code(t, err, core.Invalid)
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.AddComment(ctx, "a", p.ID, &roots[2].ID, "concurrent")
			if err != nil {
				errs <- err
			}
		}()
	}
	seen := map[int64]bool{}
	cursor := ""
	collect := func() {
		for {
			page, err := s.CommentFeed(ctx, p.ID, core.PageInput{First: 3, After: cursor})
			must(t, err)
			for _, edge := range page.Edges {
				if seen[edge.Node.ID] {
					t.Fatal("duplicate in forward traversal")
				}
				seen[edge.Node.ID] = true
				cursor = edge.Cursor
			}
			if !page.PageInfo.HasNextPage {
				return
			}
		}
	}
	collect()
	wg.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	collect()
	if len(seen) != 52 {
		t.Fatalf("pagination skipped a committed comment: got %d want 52", len(seen))
	}
}

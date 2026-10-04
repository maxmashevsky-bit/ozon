package graph

import (
	"context"
	"strconv"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/graph/model"
)

type Resolver struct{ Service *core.Service }
type actorKey struct{}

func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}
func actor(ctx context.Context) string { value, _ := ctx.Value(actorKey{}).(string); return value }
func parseID(value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id < 1 {
		return 0, core.Fail(core.Invalid, "ID must be a positive 64-bit integer")
	}
	return id, nil
}
func parseParent(value *string) (*int64, error) {
	if value == nil {
		return nil, nil
	}
	id, err := parseID(*value)
	return &id, err
}
func postModel(p core.Post) *model.Post {
	return &model.Post{ID: strconv.FormatInt(p.ID, 10), AuthorID: p.AuthorID, Title: p.Title, Text: p.Text, CommentsAllowed: p.CommentsAllowed, CreatedAt: p.CreatedAt}
}
func commentModel(c core.Comment) *model.Comment {
	var parent *string
	if c.ParentID != nil {
		p := strconv.FormatInt(*c.ParentID, 10)
		parent = &p
	}
	return &model.Comment{ID: strconv.FormatInt(c.ID, 10), PostID: strconv.FormatInt(c.PostID, 10), ParentID: parent, AuthorID: c.AuthorID, Text: c.Text, CreatedAt: c.CreatedAt}
}
func pageInput(first int, after *string) core.PageInput {
	p := core.PageInput{First: first}
	if after != nil {
		p.After = *after
	}
	return p
}

func commentConnection(page core.CommentPage) *model.CommentConnection {
	result := &model.CommentConnection{Edges: make([]*model.CommentEdge, 0, len(page.Edges)), PageInfo: &model.PageInfo{HasNextPage: page.PageInfo.HasNextPage, EndCursor: page.PageInfo.EndCursor}}
	for _, edge := range page.Edges {
		result.Edges = append(result.Edges, &model.CommentEdge{Cursor: edge.Cursor, Node: commentModel(edge.Node)})
	}
	return result
}

package core

import (
	"encoding/base64"
	"encoding/json"
)

const DefaultPageSize = 20
const MaxPageSize = 100

type PageInput struct {
	First int
	After string
}
type PageInfo struct {
	HasNextPage bool
	EndCursor   *string
}
type PostEdge struct {
	Cursor string
	Node   Post
}
type PostPage struct {
	Edges    []PostEdge
	PageInfo PageInfo
}
type CommentEdge struct {
	Cursor string
	Node   Comment
}
type CommentPage struct {
	Edges    []CommentEdge
	PageInfo PageInfo
}

type cursor struct {
	Version  int    `json:"v"`
	Kind     string `json:"k"`
	PostID   int64  `json:"p"`
	ParentID int64  `json:"r"`
	ID       int64  `json:"i"`
}

func parentValue(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
func encodeCursor(kind string, postID int64, parentID *int64, id int64) string {
	b, _ := json.Marshal(cursor{1, kind, postID, parentValue(parentID), id})
	return base64.RawURLEncoding.EncodeToString(b)
}
func pageAfter(page PageInput, kind string, postID int64, parentID *int64) (int64, error) {
	if page.First < 1 || page.First > MaxPageSize {
		return 0, Fail(Invalid, "first must be between 1 and 100")
	}
	if page.After == "" {
		return 0, nil
	}
	if len(page.After) > 512 {
		return 0, Fail(Invalid, "invalid cursor")
	}
	b, err := base64.RawURLEncoding.DecodeString(page.After)
	var c cursor
	if err != nil || json.Unmarshal(b, &c) != nil || c.Version != 1 || c.Kind != kind || c.PostID != postID || c.ParentID != parentValue(parentID) || c.ID < 1 {
		return 0, Fail(Invalid, "cursor does not belong to this page")
	}
	return c.ID, nil
}

package core

import (
	"context"
	"time"
)

type Post struct {
	ID              int64
	AuthorID        string
	Title           string
	Text            string
	CommentsAllowed bool
	CreatedAt       time.Time
}

type Comment struct {
	ID        int64
	PostID    int64
	ParentID  *int64
	AuthorID  string
	Text      string
	CreatedAt time.Time
}

type NewPost struct{ AuthorID, Title, Text string }
type NewComment struct {
	PostID         int64
	ParentID       *int64
	AuthorID, Text string
}

// WithinPost serializes changes to a post and its comments. An error rolls back
// all writes. Callbacks must use the transaction, not reenter the store.
type Store interface {
	Subscribe(context.Context, int64) (<-chan Comment, error)
	CreatePost(context.Context, NewPost) (Post, error)
	GetPost(context.Context, int64) (Post, error)
	ListPosts(context.Context, int64, int) ([]Post, error)
	GetComment(context.Context, int64) (Comment, error)
	ListComments(context.Context, int64, *int64, int64, int) ([]Comment, error)
	WithinPost(context.Context, int64, func(PostTx) error) error
}

type PostTx interface {
	Post() Post
	GetComment(context.Context, int64) (Comment, error)
	AddComment(context.Context, NewComment) (Comment, error)
	SetCommentsAllowed(context.Context, bool) (Post, error)
}

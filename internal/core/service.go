package core

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxCommentRunes = 2000

type Service struct{ store Store }

func NewService(store Store) *Service { return &Service{store: store} }

func ValidateActor(actor string) error {
	if actor == "" {
		return Fail(Unauthenticated, "set X-User-ID to use mutations")
	}
	if len(actor) > 64 {
		return Fail(Invalid, "X-User-ID must contain 1 to 64 ASCII letters, digits, '-' or '_'")
	}
	for _, r := range actor {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return Fail(Invalid, "invalid X-User-ID")
		}
	}
	return nil
}
func validateText(value, name string, max int) error {
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return Fail(Invalid, name+" must be valid Unicode without NUL")
	}
	if strings.TrimSpace(value) == "" {
		return Fail(Invalid, name+" must not be blank")
	}
	if utf8.RuneCountInString(value) > max {
		return Fail(Invalid, name+" is too long")
	}
	return nil
}
func validID(id int64) error {
	if id < 1 {
		return Fail(Invalid, "ID must be a positive integer")
	}
	return nil
}
func (s *Service) CreatePost(ctx context.Context, actor, title, text string) (Post, error) {
	if err := ValidateActor(actor); err != nil {
		return Post{}, err
	}
	if err := validateText(title, "title", 200); err != nil {
		return Post{}, err
	}
	if err := validateText(text, "post text", 100000); err != nil {
		return Post{}, err
	}
	p, err := s.store.CreatePost(ctx, NewPost{actor, title, text})
	return p, StorageError(err)
}
func (s *Service) Post(ctx context.Context, id int64) (Post, error) {
	if err := validID(id); err != nil {
		return Post{}, err
	}
	p, err := s.store.GetPost(ctx, id)
	return p, StorageError(err)
}
func (s *Service) Comment(ctx context.Context, id int64) (Comment, error) {
	if err := validID(id); err != nil {
		return Comment{}, err
	}
	c, err := s.store.GetComment(ctx, id)
	return c, StorageError(err)
}
func (s *Service) SetCommentsAllowed(ctx context.Context, actor string, id int64, allowed bool) (Post, error) {
	if err := ValidateActor(actor); err != nil {
		return Post{}, err
	}
	if err := validID(id); err != nil {
		return Post{}, err
	}
	var result Post
	err := s.store.WithinPost(ctx, id, func(tx PostTx) error {
		if tx.Post().AuthorID != actor {
			return Fail(Forbidden, "only the post author can change commentsAllowed")
		}
		var err error
		result, err = tx.SetCommentsAllowed(ctx, allowed)
		return err
	})
	return result, StorageError(err)
}
func (s *Service) AddComment(ctx context.Context, actor string, postID int64, parentID *int64, text string) (Comment, error) {
	if err := ValidateActor(actor); err != nil {
		return Comment{}, err
	}
	if err := validID(postID); err != nil {
		return Comment{}, err
	}
	if parentID != nil {
		if err := validID(*parentID); err != nil {
			return Comment{}, err
		}
	}
	if err := validateText(text, "comment text", MaxCommentRunes); err != nil {
		return Comment{}, err
	}
	var result Comment
	err := s.store.WithinPost(ctx, postID, func(tx PostTx) error {
		if !tx.Post().CommentsAllowed {
			return Fail(Forbidden, "comments are closed")
		}
		if parentID != nil {
			parent, err := tx.GetComment(ctx, *parentID)
			if err != nil {
				return err
			}
			if parent.PostID != postID {
				return Fail(Invalid, "parent comment belongs to another post")
			}
		}
		var err error
		result, err = tx.AddComment(ctx, NewComment{postID, parentID, actor, text})
		return err
	})
	return result, StorageError(err)
}
func (s *Service) Posts(ctx context.Context, page PageInput) (PostPage, error) {
	after, err := pageAfter(page, "posts", 0, nil)
	if err != nil {
		return PostPage{}, err
	}
	posts, err := s.store.ListPosts(ctx, after, page.First+1)
	if err != nil {
		return PostPage{}, StorageError(err)
	}
	result := PostPage{Edges: make([]PostEdge, 0, len(posts))}
	if len(posts) > page.First {
		result.PageInfo.HasNextPage = true
		posts = posts[:page.First]
	}
	for _, p := range posts {
		result.Edges = append(result.Edges, PostEdge{encodeCursor("posts", 0, nil, p.ID), p})
	}
	if len(result.Edges) > 0 {
		result.PageInfo.EndCursor = &result.Edges[len(result.Edges)-1].Cursor
	}
	return result, nil
}
func (s *Service) Comments(ctx context.Context, postID int64, parentID *int64, page PageInput) (CommentPage, error) {
	if err := validID(postID); err != nil {
		return CommentPage{}, err
	}
	if parentID != nil {
		if err := validID(*parentID); err != nil {
			return CommentPage{}, err
		}
	}
	after, err := pageAfter(page, "comments", postID, parentID)
	if err != nil {
		return CommentPage{}, err
	}
	if _, err = s.Post(ctx, postID); err != nil {
		return CommentPage{}, err
	}
	if parentID != nil {
		parent, err := s.Comment(ctx, *parentID)
		if err != nil {
			return CommentPage{}, err
		}
		if parent.PostID != postID {
			return CommentPage{}, Fail(Invalid, "parent comment belongs to another post")
		}
	}
	comments, err := s.store.ListComments(ctx, postID, parentID, after, page.First+1)
	if err != nil {
		return CommentPage{}, StorageError(err)
	}
	result := CommentPage{Edges: make([]CommentEdge, 0, len(comments))}
	if len(comments) > page.First {
		result.PageInfo.HasNextPage = true
		comments = comments[:page.First]
	}
	for _, c := range comments {
		result.Edges = append(result.Edges, CommentEdge{encodeCursor("comments", postID, parentID, c.ID), c})
	}
	if len(result.Edges) > 0 {
		result.PageInfo.EndCursor = &result.Edges[len(result.Edges)-1].Cursor
	}
	return result, nil
}

func (s *Service) Subscribe(ctx context.Context, postID int64) (<-chan Comment, error) {
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := s.Post(setup, postID); err != nil {
		return nil, err
	}
	ch, err := s.store.Subscribe(ctx, postID)
	return ch, StorageError(err)
}

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
		return Fail(Unauthenticated, "verified bearer identity is required for mutations")
	}
	if len(actor) > 64 {
		return Fail(Invalid, "user ID must contain 1 to 64 ASCII letters, digits, '-' or '_'")
	}
	for _, r := range actor {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return Fail(Invalid, "invalid user ID")
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
	return commentPage(comments, page.First, "comments", postID, parentID), nil
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

const MaxBranches = 20
const MaxBranchItems = 500

func (s *Service) CommentBranches(ctx context.Context, postID int64, branches []BranchPageInput) ([]CommentPage, error) {
	if err := validID(postID); err != nil {
		return nil, err
	}
	if len(branches) < 1 || len(branches) > MaxBranches {
		return nil, Fail(Invalid, "branches must contain 1 to 20 parents")
	}
	ids := make([]int64, len(branches))
	queries := make([]BranchQuery, len(branches))
	total := 0
	for i, b := range branches {
		if err := validID(b.ParentID); err != nil {
			return nil, err
		}
		after, err := pageAfter(b.Page, "comments", postID, &b.ParentID)
		if err != nil {
			return nil, err
		}
		ids[i] = b.ParentID
		queries[i] = BranchQuery{ParentID: b.ParentID, After: after, Limit: b.Page.First + 1}
		total += b.Page.First
	}
	if total > MaxBranchItems {
		return nil, Fail(Invalid, "combined branch page size exceeds 500")
	}
	if _, err := s.Post(ctx, postID); err != nil {
		return nil, err
	}
	parents, err := s.store.GetComments(ctx, ids)
	if err != nil {
		return nil, StorageError(err)
	}
	found := make(map[int64]bool, len(parents))
	for _, p := range parents {
		if p.PostID != postID {
			return nil, Fail(Invalid, "parent comment belongs to another post")
		}
		found[p.ID] = true
	}
	for _, id := range ids {
		if !found[id] {
			return nil, Fail(NotFound, "parent comment not found")
		}
	}
	pages, err := s.store.ListCommentBranches(ctx, postID, queries)
	if err != nil {
		return nil, StorageError(err)
	}
	result := make([]CommentPage, len(branches))
	for i, rows := range pages {
		result[i] = commentPage(rows, branches[i].Page.First, "comments", postID, &branches[i].ParentID)
	}
	return result, nil
}
func (s *Service) CommentFeed(ctx context.Context, postID int64, page PageInput) (CommentPage, error) {
	after, err := pageAfter(page, "feed", postID, nil)
	if err != nil {
		return CommentPage{}, err
	}
	if _, err = s.Post(ctx, postID); err != nil {
		return CommentPage{}, err
	}
	rows, err := s.store.ListCommentFeed(ctx, postID, after, page.First+1)
	if err != nil {
		return CommentPage{}, StorageError(err)
	}
	return commentPage(rows, page.First, "feed", postID, nil), nil
}
func commentPage(rows []Comment, first int, kind string, postID int64, parentID *int64) CommentPage {
	result := CommentPage{Edges: make([]CommentEdge, 0, min(len(rows), first))}
	if len(rows) > first {
		result.PageInfo.HasNextPage = true
		rows = rows[:first]
	}
	for _, c := range rows {
		result.Edges = append(result.Edges, CommentEdge{encodeCursor(kind, postID, parentID, c.ID), c})
	}
	if len(result.Edges) > 0 {
		result.PageInfo.EndCursor = &result.Edges[len(result.Edges)-1].Cursor
	}
	return result
}

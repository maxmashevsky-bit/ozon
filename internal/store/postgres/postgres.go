package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/events"
	"example.com/ozon/internal/store/postgres/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	hub  *events.Hub
	pool *pgxpool.Pool
	q    *db.Queries
}

func New(pool *pgxpool.Pool, config ...events.Config) *Store {
	return &Store{pool: pool, q: db.New(pool), hub: events.New(false, config...)}
}
func post(p db.Post) core.Post {
	return core.Post{ID: p.ID, AuthorID: p.AuthorID, Title: p.Title, Text: p.Text, CommentsAllowed: p.CommentsAllowed, CreatedAt: p.CreatedAt.Time.UTC()}
}
func comment(c db.Comment) core.Comment {
	var parent *int64
	if c.ParentID.Valid {
		v := c.ParentID.Int64
		parent = &v
	}
	return core.Comment{ID: c.ID, PostID: c.PostID, ParentID: parent, AuthorID: c.AuthorID, Text: c.Text, CreatedAt: c.CreatedAt.Time.UTC()}
}
func nullable(id *int64) pgtype.Int8 {
	if id == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *id, Valid: true}
}
func queryError(err error, entity string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return core.Fail(core.NotFound, entity+" not found")
	}
	return fmt.Errorf("%s query: %w", entity, err)
}
func (s *Store) CreatePost(ctx context.Context, in core.NewPost) (core.Post, error) {
	var p db.Post
	err := s.transact(ctx, func(q *db.Queries) error {
		if err := q.LockPostCreation(ctx); err != nil {
			return err
		}
		var err error
		p, err = q.CreatePost(ctx, db.CreatePostParams{AuthorID: in.AuthorID, Title: in.Title, Text: in.Text})
		return err
	})
	return post(p), queryError(err, "post")
}
func (s *Store) GetPost(ctx context.Context, id int64) (core.Post, error) {
	p, err := s.q.GetPost(ctx, id)
	return post(p), queryError(err, "post")
}
func (s *Store) ListPosts(ctx context.Context, after int64, limit int) ([]core.Post, error) {
	rows, err := s.q.ListPosts(ctx, db.ListPostsParams{ID: after, Limit: int32(limit)})
	if err != nil {
		return nil, queryError(err, "posts")
	}
	result := make([]core.Post, 0, len(rows))
	for _, p := range rows {
		result = append(result, post(p))
	}
	return result, nil
}
func (s *Store) GetComment(ctx context.Context, id int64) (core.Comment, error) {
	c, err := s.q.GetComment(ctx, id)
	return comment(c), queryError(err, "comment")
}
func (s *Store) ListComments(ctx context.Context, postID int64, parentID *int64, after int64, limit int) ([]core.Comment, error) {
	var rows []db.Comment
	var err error
	if parentID == nil {
		rows, err = s.q.ListRoots(ctx, db.ListRootsParams{PostID: postID, ID: after, Limit: int32(limit)})
	} else {
		rows, err = s.q.ListReplies(ctx, db.ListRepliesParams{PostID: postID, ParentID: nullable(parentID), ID: after, Limit: int32(limit)})
	}
	if err != nil {
		return nil, queryError(err, "comments")
	}
	result := make([]core.Comment, 0, len(rows))
	for _, c := range rows {
		result = append(result, comment(c))
	}
	return result, nil
}
func (s *Store) WithinPost(ctx context.Context, id int64, fn func(core.PostTx) error) error {
	return s.transact(ctx, func(q *db.Queries) error {
		p, err := q.LockPost(ctx, id)
		if err != nil {
			return queryError(err, "post")
		}
		return fn(&postTx{q: q, post: post(p)})
	})
}
func (s *Store) transact(ctx context.Context, fn func(*db.Queries) error) (err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin post transaction: %w", err)
	}
	defer func() {
		// A canceled request must still release its connection and row lock.
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if rollbackErr := tx.Rollback(cleanup); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback post transaction: %w", rollbackErr))
		}
	}()
	if err = fn(s.q.WithTx(tx)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit post transaction: %w", err)
	}
	return nil
}

type postTx struct {
	q    *db.Queries
	post core.Post
}

func (t *postTx) Post() core.Post { return t.post }
func (t *postTx) GetComment(ctx context.Context, id int64) (core.Comment, error) {
	c, err := t.q.GetComment(ctx, id)
	return comment(c), queryError(err, "comment")
}
func (t *postTx) AddComment(ctx context.Context, in core.NewComment) (core.Comment, error) {
	if in.PostID != t.post.ID {
		return core.Comment{}, core.Fail(core.Invalid, "comment belongs to another post")
	}
	c, err := t.q.AddComment(ctx, db.AddCommentParams{PostID: in.PostID, ParentID: nullable(in.ParentID), AuthorID: in.AuthorID, Text: in.Text})
	return comment(c), queryError(err, "comment")
}
func (t *postTx) SetCommentsAllowed(ctx context.Context, allowed bool) (core.Post, error) {
	p, err := t.q.SetCommentsAllowed(ctx, db.SetCommentsAllowedParams{ID: t.post.ID, CommentsAllowed: allowed})
	if err != nil {
		return core.Post{}, queryError(err, "post")
	}
	t.post = post(p)
	return t.post, nil
}

func (s *Store) Subscribe(ctx context.Context, postID int64) (<-chan core.Comment, error) {
	return s.hub.Subscribe(ctx, postID)
}

func (s *Store) EventMetrics() map[string]float64 { return s.hub.Metrics() }
func (s *Store) CloseSubscriptions()              { s.hub.Pause() }

func (s *Store) Ready(ctx context.Context) error { return s.q.Ready(ctx) }
func (s *Store) GetComments(ctx context.Context, ids []int64) ([]core.Comment, error) {
	rows, err := s.q.GetComments(ctx, ids)
	if err != nil {
		return nil, queryError(err, "parents")
	}
	result := make([]core.Comment, 0, len(rows))
	for _, c := range rows {
		result = append(result, comment(c))
	}
	return result, nil
}
func (s *Store) ListCommentFeed(ctx context.Context, postID, after int64, limit int) ([]core.Comment, error) {
	rows, err := s.q.ListCommentFeed(ctx, db.ListCommentFeedParams{PostID: postID, ID: after, Limit: int32(limit)})
	if err != nil {
		return nil, queryError(err, "comment feed")
	}
	result := make([]core.Comment, 0, len(rows))
	for _, c := range rows {
		result = append(result, comment(c))
	}
	return result, nil
}
func (s *Store) ListCommentBranches(ctx context.Context, postID int64, branches []core.BranchQuery) ([][]core.Comment, error) {
	params := db.ListCommentBranchesParams{PostID: postID}
	for _, b := range branches {
		params.ParentIds = append(params.ParentIds, b.ParentID)
		params.AfterIds = append(params.AfterIds, b.After)
		params.PageLimits = append(params.PageLimits, int32(b.Limit))
	}
	rows, err := s.q.ListCommentBranches(ctx, params)
	if err != nil {
		return nil, queryError(err, "comment branches")
	}
	result := make([][]core.Comment, len(branches))
	for i := range result {
		result[i] = []core.Comment{}
	}
	for _, c := range rows {
		index := int(c.BranchIndex) - 1
		result[index] = append(result[index], comment(db.Comment{ID: c.ID, PostID: c.PostID, ParentID: c.ParentID, AuthorID: c.AuthorID, Text: c.Text, CreatedAt: c.CreatedAt}))
	}
	return result, nil
}

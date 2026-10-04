-- name: CreatePost :one
INSERT INTO posts (author_id, title, text) VALUES ($1, $2, $3) RETURNING *;

-- name: GetPost :one
SELECT * FROM posts WHERE id = $1;

-- name: LockPost :one
SELECT * FROM posts WHERE id = $1 FOR UPDATE;

-- name: ListPosts :many
SELECT * FROM posts WHERE id > $1 ORDER BY id LIMIT $2;

-- name: SetCommentsAllowed :one
UPDATE posts SET comments_allowed = $2 WHERE id = $1 RETURNING *;

-- name: GetComment :one
SELECT * FROM comments WHERE id = $1;

-- name: AddComment :one
INSERT INTO comments (post_id, parent_id, author_id, text) VALUES ($1, $2, $3, $4) RETURNING *;

-- name: ListRoots :many
SELECT * FROM comments WHERE post_id = $1 AND parent_id IS NULL AND id > $2 ORDER BY id LIMIT $3;

-- name: ListReplies :many
SELECT * FROM comments WHERE post_id = $1 AND parent_id = $2 AND id > $3 ORDER BY id LIMIT $4;

-- name: CurrentSchema :one
SELECT current_schema()::text;

-- name: GetComments :many
SELECT * FROM comments WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: ListCommentBranches :many
SELECT b.ordinality::integer AS branch_index, c.*
FROM unnest(sqlc.arg(parent_ids)::bigint[]) WITH ORDINALITY AS b(parent_id, ordinality)
CROSS JOIN LATERAL (
 SELECT * FROM comments
 WHERE post_id = sqlc.arg(post_id) AND parent_id = b.parent_id AND id > (sqlc.arg(after_ids)::bigint[])[b.ordinality]
 ORDER BY id LIMIT (sqlc.arg(page_limits)::integer[])[b.ordinality]
) c
ORDER BY b.ordinality, c.id;

-- name: ListCommentFeed :many
SELECT * FROM comments WHERE post_id = $1 AND id > $2 ORDER BY id LIMIT $3;

-- name: LockPostCreation :exec
SELECT pg_advisory_xact_lock(19483726);

-- name: Ready :exec
SELECT id FROM posts LIMIT 0;

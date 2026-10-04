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

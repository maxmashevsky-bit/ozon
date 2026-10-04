-- +goose Up
CREATE TABLE posts (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 author_id text NOT NULL,
 title text NOT NULL CHECK (char_length(title) BETWEEN 1 AND 200),
 text text NOT NULL CHECK (char_length(text) BETWEEN 1 AND 100000),
 comments_allowed boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE comments (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 post_id bigint NOT NULL REFERENCES posts(id),
 parent_id bigint,
 author_id text NOT NULL,
 text text NOT NULL CHECK (char_length(text) BETWEEN 1 AND 2000),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (post_id, id),
 FOREIGN KEY (post_id, parent_id) REFERENCES comments(post_id, id),
 CHECK (parent_id IS NULL OR parent_id < id)
);
CREATE INDEX comments_page_idx ON comments(post_id, parent_id, id);

-- Parent links and post membership are immutable; changing either could corrupt the tree.
-- +goose StatementBegin
CREATE FUNCTION protect_comment_tree() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.parent_id IS DISTINCT FROM OLD.parent_id OR NEW.post_id IS DISTINCT FROM OLD.post_id OR NEW.id IS DISTINCT FROM OLD.id THEN
  RAISE EXCEPTION 'comment tree links are immutable' USING ERRCODE = '23514';
 END IF;
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER comments_tree_immutable BEFORE UPDATE ON comments
FOR EACH ROW EXECUTE FUNCTION protect_comment_tree();

-- +goose Down
DROP TABLE comments;
DROP FUNCTION protect_comment_tree();
DROP TABLE posts;

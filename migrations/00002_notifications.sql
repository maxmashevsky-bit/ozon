-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION notify_new_comment() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_notify('ozon_comments', json_build_object('schema', TG_TABLE_SCHEMA, 'id', NEW.id)::text);
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER comments_notify AFTER INSERT ON comments
FOR EACH ROW EXECUTE FUNCTION notify_new_comment();

-- +goose Down
DROP TRIGGER comments_notify ON comments;
DROP FUNCTION notify_new_comment();

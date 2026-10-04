package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed *.sql
var files embed.FS

func Up(ctx context.Context, dsn string) (err error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	db.SetMaxOpenConns(2)
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	// Goose Up checks pending versions before its built-in migration lock.
	// Hold a separate session lock around the entire call, including first-time
	// creation of goose_db_version. The reserved connection cannot reenter the pool.
	guard, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migration guard connection: %w", err)
	}
	defer guard.Close()
	if err = locker.SessionLock(ctx, guard); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if unlockErr := locker.SessionUnlock(cleanup, guard); err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	if _, err = provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

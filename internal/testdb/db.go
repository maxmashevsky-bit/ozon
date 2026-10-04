//go:build integration

package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.com/ozon/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Counter struct{ Selects atomic.Int64 }

func (c *Counter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "SELECT ") {
		c.Selects.Add(1)
	}
	return ctx
}
func (*Counter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func New(t *testing.T) (*pgxpool.Pool, *Counter)      { return newDatabase(t, true) }
func NewEmpty(t *testing.T) (*pgxpool.Pool, *Counter) { return newDatabase(t, false) }
func newDatabase(t *testing.T, applyMigrations bool) (*pgxpool.Pool, *Counter) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("integration tests require TEST_DATABASE_URL (use a disposable database)")
	}
	base, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	if !strings.HasSuffix(base.ConnConfig.Database, "_test") {
		t.Fatal("integration database name must end in _test; use make verify-full")
	}
	ctx := context.Background()
	admin, err := pgxpool.NewWithConfig(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("ozon_test_%d", time.Now().UnixNano())
	name := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	params := u.Query()
	params.Set("search_path", schema)
	u.RawQuery = params.Encode()
	config, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["application_name"] = schema
	counter := &Counter{}
	config.ConnConfig.Tracer = counter
	if applyMigrations {
		if err = migrations.Up(ctx, config.ConnString()); err != nil {
			t.Fatal(err)
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, counter
}

//go:build integration

package migrations_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"example.com/ozon/internal/testdb"
	"example.com/ozon/migrations"
)

func TestConcurrentMigrationOnEmptySchema(t *testing.T) {
	pool, _ := testdb.NewEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- migrations.Up(ctx, pool.Config().ConnString()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var tables int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name IN ('posts','comments')`).Scan(&tables); err != nil || tables != 2 {
		t.Fatalf("missing schema: %d %v", tables, err)
	}
}

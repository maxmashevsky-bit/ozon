package main

import (
	"context"
	"example.com/ozon/internal/config"
	"example.com/ozon/migrations"
	"log/slog"
	"os"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn, err := config.DatabaseURL()
	if err != nil || migrations.Up(ctx, dsn) != nil {
		slog.Error("migration failed; check database availability and permissions")
		os.Exit(1)
	}
	slog.Info("migrations applied")
}

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Each application instance uses one dedicated LISTEN connection, separate from
// the query pool. PostgreSQL only delivers NOTIFY after a transaction commits.
func (s *Store) StartNotifications(ctx context.Context, logger *slog.Logger) (func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	setup, stop := context.WithTimeout(ctx, 5*time.Second)
	schema, err := s.q.CurrentSchema(setup)
	stop()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("notification schema: %w", err)
	}
	connect := func() (*pgx.Conn, error) {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		conn, err := pgx.ConnectConfig(attempt, s.pool.Config().ConnConfig.Copy())
		if err != nil {
			return nil, err
		}
		if _, err = conn.Exec(attempt, "LISTEN ozon_comments"); err != nil {
			closeListener(conn, logger)
			return nil, err
		}
		s.hub.Resume()
		return conn, nil
	}
	conn, err := connect()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("listen for comments: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer s.hub.Pause()
		for {
			notification, err := conn.WaitForNotification(ctx)
			if err != nil {
				s.hub.Pause()
				closeListener(conn, logger)
				if ctx.Err() != nil {
					return
				}
				logger.ErrorContext(ctx, "comment listener disconnected; subscribers must reconnect", "error", err)
				for {
					timer := time.NewTimer(time.Second)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					conn, err = connect()
					if err == nil {
						break
					}
					logger.ErrorContext(ctx, "reconnect comment listener", "error", err)
				}
				continue
			}
			var payload struct {
				Schema string `json:"schema"`
				ID     int64  `json:"id"`
			}
			if err = json.Unmarshal([]byte(notification.Payload), &payload); err != nil || payload.Schema != schema || payload.ID < 1 {
				continue
			}
			read, stop := context.WithTimeout(ctx, 5*time.Second)
			c, err := s.GetComment(read, payload.ID)
			stop()
			if err != nil {
				logger.ErrorContext(ctx, "read comment notification", "error", err)
				s.hub.Pause()
				s.hub.Resume()
				continue
			}
			s.hub.Publish(c)
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); <-done }) }, nil
}
func closeListener(conn *pgx.Conn, logger *slog.Logger) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Close(cleanup); err != nil {
		logger.Error("close comment listener", "error", err)
	}
}

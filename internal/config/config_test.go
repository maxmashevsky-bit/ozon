package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var keys = []string{"JWT_SECRET_FILE", "DB_POOL_SIZE", "MAX_OPERATIONS", "MAX_SUBSCRIPTIONS", "SUBSCRIPTION_BUFFER", "RATE_RPS", "RATE_BURST", "REQUEST_TIMEOUT", "STREAM_LIFETIME", "SSE_HEARTBEAT", "WRITE_TIMEOUT", "SHUTDOWN_TIMEOUT", "GRAPHQL_DEPTH", "GRAPHQL_FIELDS", "GRAPHQL_COST", "MAX_BODY_BYTES", "AUTH_MODE", "TRUSTED_PROXIES", "DATABASE_URL", "DATABASE_URL_FILE"}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range keys {
		t.Setenv(k, "")
	}
}
func TestDefaults(t *testing.T) {
	clearEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.AuthMode != "jwt" || c.SecretFile != "" || c.PoolSize != 20 || c.Operations != 64 || c.Subscribers != 1024 || c.BufferSize != 64 || c.Rate != 120 || c.Burst != 240 || c.Timeout != 10*time.Second || c.StreamLifetime != 10*time.Minute || c.Heartbeat != 15*time.Second || c.WriteTimeout != 5*time.Second || c.Shutdown != 10*time.Second || c.Depth != 16 || c.Fields != 300 || c.Cost != 5000 || c.BodyBytes != 1<<20 || len(c.Trusted) != 0 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}
func TestOverrides(t *testing.T) {
	clearEnv(t)
	overrides := map[string]string{"JWT_SECRET_FILE": "/tmp/key", "DB_POOL_SIZE": "7", "MAX_OPERATIONS": "21", "MAX_SUBSCRIPTIONS": "256", "SUBSCRIPTION_BUFFER": "8", "RATE_RPS": "40", "RATE_BURST": "80", "REQUEST_TIMEOUT": "2s", "STREAM_LIFETIME": "2m", "SSE_HEARTBEAT": "500ms", "WRITE_TIMEOUT": "3s", "SHUTDOWN_TIMEOUT": "4s", "GRAPHQL_DEPTH": "8", "GRAPHQL_FIELDS": "100", "GRAPHQL_COST": "1000", "MAX_BODY_BYTES": "2048", "AUTH_MODE": "dev", "TRUSTED_PROXIES": "10.0.0.0/8, 2001:db8::/32"}
	for k, v := range overrides {
		t.Setenv(k, v)
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.AuthMode != "dev" || c.SecretFile != "/tmp/key" || c.PoolSize != 7 || c.Operations != 21 || c.Subscribers != 256 || c.BufferSize != 8 || c.Rate != 40 || c.Burst != 80 || c.Timeout != 2*time.Second || c.StreamLifetime != 2*time.Minute || c.Heartbeat != 500*time.Millisecond || c.WriteTimeout != 3*time.Second || c.Shutdown != 4*time.Second || c.Depth != 8 || c.Fields != 100 || c.Cost != 1000 || c.BodyBytes != 2048 || len(c.Trusted) != 2 || !c.Trusted[0].Contains(c.Trusted[0].Addr()) {
		t.Fatalf("unexpected overrides: %+v", c)
	}
}
func TestInvalidConfiguration(t *testing.T) {
	cases := []struct{ key, value string }{
		{"DB_POOL_SIZE", "0"}, {"DB_POOL_SIZE", "101"}, {"MAX_OPERATIONS", "0"}, {"MAX_OPERATIONS", "1025"}, {"MAX_SUBSCRIPTIONS", "0"}, {"MAX_SUBSCRIPTIONS", "4097"}, {"SUBSCRIPTION_BUFFER", "0"}, {"SUBSCRIPTION_BUFFER", "257"}, {"RATE_RPS", "0"}, {"RATE_RPS", "100001"}, {"RATE_BURST", "0"}, {"RATE_BURST", "100001"}, {"GRAPHQL_DEPTH", "3"}, {"GRAPHQL_DEPTH", "33"}, {"GRAPHQL_FIELDS", "0"}, {"GRAPHQL_FIELDS", "1001"}, {"GRAPHQL_COST", "0"}, {"GRAPHQL_COST", "100001"}, {"MAX_BODY_BYTES", "127"}, {"MAX_BODY_BYTES", "4194305"},
		{"REQUEST_TIMEOUT", "garbage"}, {"REQUEST_TIMEOUT", "1ms"}, {"REQUEST_TIMEOUT", "25h"}, {"STREAM_LIFETIME", "1ms"}, {"SSE_HEARTBEAT", "1ms"}, {"WRITE_TIMEOUT", "1ms"}, {"SHUTDOWN_TIMEOUT", "1ms"}, {"AUTH_MODE", "none"}, {"TRUSTED_PROXIES", "garbage"},
		{"REQUEST_TIMEOUT", "-1s"}, {"STREAM_LIFETIME", "-1s"}, {"SSE_HEARTBEAT", "-1s"}, {"WRITE_TIMEOUT", "-1s"}, {"SHUTDOWN_TIMEOUT", "-1s"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tc.key, tc.value)
			_, err := Load()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if tc.key != "STREAM_LIFETIME" && !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("wrong error: %v", err)
			}
		})
	}
	t.Run("heartbeat_relation", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("STREAM_LIFETIME", "1s")
		t.Setenv("SSE_HEARTBEAT", "1s")
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "SSE_HEARTBEAT") {
			t.Fatalf("expected relation error: %v", err)
		}
	})
}
func TestDatabaseURL(t *testing.T) {
	clearEnv(t)
	if _, err := DatabaseURL(); err == nil {
		t.Fatal("missing database URL accepted")
	}
	t.Setenv("DATABASE_URL", "postgres://fallback")
	value, err := DatabaseURL()
	if err != nil || value != "postgres://fallback" {
		t.Fatalf("fallback: %q %v", value, err)
	}
	path := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(path, []byte("postgres://from-file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL_FILE", path)
	value, err = DatabaseURL()
	if err != nil || value != "postgres://from-file" {
		t.Fatalf("file: %q %v", value, err)
	}
	t.Setenv("DATABASE_URL_FILE", path+"-missing")
	value, err = DatabaseURL()
	if err == nil || value != "" || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("missing file: %q %v", value, err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL_FILE", path)
	value, err = DatabaseURL()
	if err == nil || value != "" || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("unreadable file: %q %v", value, err)
	}
}

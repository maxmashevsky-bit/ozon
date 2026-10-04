package config

import (
	"errors"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AuthMode, SecretFile                                       string
	PoolSize                                                   int32
	Operations, Subscribers, BufferSize                        int
	Rate                                                       float64
	Burst                                                      int
	Timeout, StreamLifetime, Heartbeat, WriteTimeout, Shutdown time.Duration
	Depth, Fields, Cost                                        int
	BodyBytes                                                  int64
	Trusted                                                    []netip.Prefix
}

func Load() (Config, error) {
	c := Config{AuthMode: "jwt", SecretFile: os.Getenv("JWT_SECRET_FILE"), PoolSize: 20, Operations: 64, Subscribers: 1024, BufferSize: 64, Rate: 120, Burst: 240, Timeout: 10 * time.Second, StreamLifetime: 10 * time.Minute, Heartbeat: 15 * time.Second, WriteTimeout: 5 * time.Second, Shutdown: 10 * time.Second, Depth: 16, Fields: 300, Cost: 5000, BodyBytes: 1 << 20}
	var err error
	integer := func(name string, defaultValue, min, max int) int {
		if value := os.Getenv(name); value != "" {
			n, e := strconv.Atoi(value)
			if e != nil || n < min || n > max {
				err = errors.New("invalid " + name)
				return defaultValue
			}
			return n
		}
		return defaultValue
	}
	duration := func(name string, defaultValue time.Duration) time.Duration {
		if value := os.Getenv(name); value != "" {
			n, e := time.ParseDuration(value)
			if e != nil || n < 10*time.Millisecond || n > 24*time.Hour {
				err = errors.New("invalid " + name)
				return defaultValue
			}
			return n
		}
		return defaultValue
	}
	c.PoolSize = int32(integer("DB_POOL_SIZE", 20, 1, 100))
	c.Operations = integer("MAX_OPERATIONS", 64, 1, 1024)
	c.Subscribers = integer("MAX_SUBSCRIPTIONS", 1024, 1, 4096)
	c.BufferSize = integer("SUBSCRIPTION_BUFFER", 64, 1, 256)
	c.Rate = float64(integer("RATE_RPS", 120, 1, 100000))
	c.Burst = integer("RATE_BURST", 240, 1, 100000)
	c.Timeout = duration("REQUEST_TIMEOUT", c.Timeout)
	c.StreamLifetime = duration("STREAM_LIFETIME", c.StreamLifetime)
	c.Heartbeat = duration("SSE_HEARTBEAT", c.Heartbeat)
	c.WriteTimeout = duration("WRITE_TIMEOUT", c.WriteTimeout)
	c.Shutdown = duration("SHUTDOWN_TIMEOUT", c.Shutdown)
	c.Depth = integer("GRAPHQL_DEPTH", 16, 4, 32)
	c.Fields = integer("GRAPHQL_FIELDS", 300, 1, 1000)
	c.Cost = integer("GRAPHQL_COST", 5000, 1, 100000)
	c.BodyBytes = int64(integer("MAX_BODY_BYTES", 1<<20, 128, 4<<20))
	if mode := os.Getenv("AUTH_MODE"); mode != "" {
		c.AuthMode = mode
	}
	if c.AuthMode != "jwt" && c.AuthMode != "dev" {
		err = errors.New("AUTH_MODE must be jwt or dev")
	}
	for _, value := range strings.Split(os.Getenv("TRUSTED_PROXIES"), ",") {
		if strings.TrimSpace(value) == "" {
			continue
		}
		prefix, e := netip.ParsePrefix(strings.TrimSpace(value))
		if e != nil {
			err = errors.New("invalid TRUSTED_PROXIES")
			break
		}
		c.Trusted = append(c.Trusted, prefix)
	}
	if c.Heartbeat >= c.StreamLifetime {
		err = errors.New("SSE_HEARTBEAT must be shorter than STREAM_LIFETIME")
	}
	return c, err
}

func DatabaseURL() (string, error) {
	if file := os.Getenv("DATABASE_URL_FILE"); file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", errors.New("cannot read database configuration file")
		}
		return strings.TrimSpace(string(b)), nil
	}
	if value := os.Getenv("DATABASE_URL"); value != "" {
		return value, nil
	}
	return "", errors.New("configure DATABASE_URL_FILE or DATABASE_URL")
}

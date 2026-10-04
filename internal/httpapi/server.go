package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/graph"
	"example.com/ozon/internal/stream"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func New(service *core.Service, logger *slog.Logger, options ...Options) http.Handler {
	var cfg Options
	if len(options) > 0 {
		cfg = options[0]
	}
	cfg = cfg.defaults()
	metrics := cfg.Metrics
	server := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{Service: service}}))
	server.AddTransport(stream.SSE{Heartbeat: cfg.Heartbeat, WriteTimeout: cfg.WriteTimeout})
	server.AddTransport(transport.POST{})
	server.AroundOperations(func(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
		if graphql.GetOperationContext(ctx).Operation.Operation == ast.Subscription {
			return next(ctx)
		}
		limited, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		response := next(limited)
		return func(context.Context) *graphql.Response { defer cancel(); return response(limited) }
	})
	server.AroundResponses(func(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
		response := next(ctx)
		if response != nil && len(response.Errors) > 0 {
			metrics.GraphQLErrors.Add(1)
		}
		return response
	})
	server.SetParserTokenLimit(5000)
	server.SetQueryCache(lru.New[*ast.QueryDocument](100))
	server.Use(documentLimits{Depth: cfg.Depth, Fields: cfg.Fields, Cost: cfg.Cost})
	server.Use(extension.Introspection{})
	server.SetErrorPresenter(func(ctx context.Context, err error) *gqlerror.Error {
		var appErr *core.Error
		if errors.As(err, &appErr) {
			if appErr.Code == core.Unavailable {
				logger.ErrorContext(ctx, "storage operation failed", "code", core.Unavailable)
			}
			return &gqlerror.Error{Message: appErr.Message, Path: graphql.GetPath(ctx), Extensions: map[string]any{"code": string(appErr.Code)}}
		}
		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			code := "BAD_USER_INPUT"
			if c, ok := gqlErr.Extensions["code"].(string); ok {
				code = c
			}
			return &gqlerror.Error{Message: "invalid GraphQL request", Path: graphql.GetPath(ctx), Extensions: map[string]any{"code": code}}
		}
		logger.ErrorContext(ctx, "request failed", "code", "INTERNAL")
		return &gqlerror.Error{Message: "internal server error", Path: graphql.GetPath(ctx), Extensions: map[string]any{"code": "INTERNAL"}}
	})
	server.SetRecoverFunc(func(ctx context.Context, _ any) error {
		logger.ErrorContext(ctx, "resolver panic")
		return errors.New("internal server error")
	})
	ops := make(chan struct{}, cfg.MaxOperations)
	streams := make(chan struct{}, cfg.MaxSubscriptions)
	gate := newRateGate(cfg.Rate, cfg.Burst)
	mux := http.NewServeMux()
	mux.Handle("/", playground.Handler("Posts and comments", "/query"))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if cfg.Draining != nil && cfg.Draining() {
			http.Error(w, "draining", 503)
			return
		}
		if cfg.Ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if cfg.Ready(ctx) != nil {
				http.Error(w, "storage unavailable", 503)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/metrics", metrics)
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		metrics.Requests.Add(1)
		if cfg.Draining != nil && cfg.Draining() {
			writeError(w, 503, "UNAVAILABLE", "server is draining")
			return
		}
		actor, err := cfg.Auth.Actor(r)
		if err != nil {
			writeError(w, 401, "UNAUTHENTICATED", "invalid or expired credentials")
			return
		}
		key := "ip:" + clientIP(r, cfg.TrustedProxies)
		if actor != "" {
			key = "user:" + actor
		}
		if !gate.allow(key, time.Now()) {
			metrics.RateRejected.Add(1)
			w.Header().Set("Retry-After", "1")
			writeError(w, 429, "RATE_LIMITED", "request rate exceeded")
			return
		}
		capacity := ops
		timeout := cfg.RequestTimeout
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			capacity = streams
			timeout = cfg.StreamLifetime
		}
		select {
		case capacity <- struct{}{}:
			defer func() { <-capacity }()
		default:
			metrics.BusyRejected.Add(1)
			writeError(w, 503, "BUSY", "too many concurrent operations")
			return
		}
		metrics.Inflight.Add(1)
		defer metrics.Inflight.Add(-1)
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, 413, "BAD_USER_INPUT", "request body too large or unreadable")
			return
		}
		// Reject malformed input before gqlgen transports can include the body in logs.
		var params graphql.RawParams
		if json.Unmarshal(data, &params) != nil || params.Query == "" {
			writeError(w, 400, "BAD_USER_INPUT", "invalid GraphQL JSON body")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		server.ServeHTTP(w, r.WithContext(graph.WithActor(ctx, actor)))
	})
	return mux
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{map[string]any{"message": message, "extensions": map[string]any{"code": code}}}})
}

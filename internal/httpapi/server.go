package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/graph"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func New(service *core.Service, logger *slog.Logger) http.Handler {
	server := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{Service: service}}))
	server.AddTransport(transport.SSE{})
	server.AddTransport(transport.POST{})
	server.AroundOperations(func(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
		if graphql.GetOperationContext(ctx).Operation.Operation == ast.Subscription {
			return next(ctx)
		}
		limited, cancel := context.WithTimeout(ctx, 10*time.Second)
		response := next(limited)
		return func(context.Context) *graphql.Response { defer cancel(); return response(limited) }
	})
	server.SetParserTokenLimit(5000)
	server.SetQueryCache(lru.New[*ast.QueryDocument](100))
	server.Use(documentLimits{})
	server.Use(extension.Introspection{})
	server.SetErrorPresenter(func(ctx context.Context, err error) *gqlerror.Error {
		var appErr *core.Error
		if errors.As(err, &appErr) {
			if appErr.Code == core.Unavailable {
				logger.ErrorContext(ctx, "storage operation failed", "error", err)
			}
			return &gqlerror.Error{Message: appErr.Message, Path: graphql.GetPath(ctx), Extensions: map[string]any{"code": string(appErr.Code)}}
		}
		var gqlErr *gqlerror.Error
		if errors.As(err, &gqlErr) {
			result := graphql.DefaultErrorPresenter(ctx, err)
			if result.Extensions == nil {
				result.Extensions = map[string]any{}
			}
			if _, ok := result.Extensions["code"]; !ok {
				result.Extensions["code"] = "BAD_USER_INPUT"
			}
			return result
		}
		logger.ErrorContext(ctx, "request failed", "error", err)
		return &gqlerror.Error{Message: "internal server error", Path: graphql.GetPath(ctx), Extensions: map[string]any{"code": "INTERNAL"}}
	})
	server.SetRecoverFunc(func(ctx context.Context, recovered any) error {
		logger.ErrorContext(ctx, "resolver panic", "panic", recovered)
		return errors.New("internal server error")
	})
	mux := http.NewServeMux()
	mux.Handle("/", playground.Handler("Posts and comments", "/query"))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("/query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		timeout := 10 * time.Second
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			timeout = 10 * time.Minute
			if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(timeout)); err != nil {
				logger.ErrorContext(r.Context(), "set subscription deadline", "error", err)
				http.Error(w, "streaming unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		server.ServeHTTP(w, r.WithContext(graph.WithActor(ctx, r.Header.Get("X-User-ID"))))
	}))
	return mux
}

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/ozon/internal/auth"
	"example.com/ozon/internal/core"
	"example.com/ozon/internal/store/memory"
	"github.com/99designs/gqlgen/graphql/introspection"
)

type response struct {
	Data   map[string]json.RawMessage `json:"data"`
	Errors []struct {
		Message    string         `json:"message"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

func request(t *testing.T, h http.Handler, actor, query string, variables map[string]any) response {
	t.Helper()
	return requestOperation(t, h, actor, query, "", variables)
}
func requestOperation(t *testing.T, h http.Handler, actor, query, operation string, variables map[string]any) response {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables, "operationName": operation})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-User-ID", actor)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var got response
	if err = json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("status %d: %s: %v", w.Code, w.Body, err)
	}
	return got
}
func noErrors(t *testing.T, r response) {
	t.Helper()
	if len(r.Errors) > 0 {
		t.Fatalf("GraphQL errors: %+v", r.Errors)
	}
}
func errorCode(t *testing.T, r response, want string) {
	t.Helper()
	if len(r.Errors) == 0 || r.Errors[0].Extensions["code"] != want {
		t.Fatalf("wanted %s: %+v", want, r)
	}
}
func newHandler() http.Handler {
	return New(core.NewService(memory.New()), slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Auth: auth.DevHeader()})
}

func TestGraphQLFlow(t *testing.T) {
	h := newHandler()
	created := request(t, h, "alice", `mutation { createPost(title:"Title",text:"Body") { id authorId commentsAllowed } }`, nil)
	noErrors(t, created)
	var p struct {
		ID, AuthorID    string
		CommentsAllowed bool
	}
	if err := json.Unmarshal(created.Data["createPost"], &p); err != nil {
		t.Fatal(err)
	}
	if p.ID == "" || p.AuthorID != "alice" || !p.CommentsAllowed {
		t.Fatalf("bad post: %+v", p)
	}
	add := `mutation($post: ID!, $text: String!) { addComment(postId:$post,text:$text) { id authorId parentId } }`
	var firstID string
	for i := 0; i < 3; i++ {
		r := request(t, h, "bob", add, map[string]any{"post": p.ID, "text": fmt.Sprint(i)})
		noErrors(t, r)
		var c struct{ ID, AuthorID string }
		if err := json.Unmarshal(r.Data["addComment"], &c); err != nil {
			t.Fatal(err)
		}
		if c.AuthorID != "bob" {
			t.Fatal("author is not server identity")
		}
		if i == 0 {
			firstID = c.ID
		}
	}
	reply := request(t, h, "carol", `mutation($p: ID!, $r: ID!) { addComment(postId:$p,parentId:$r,text:"reply") { id parentId } }`, map[string]any{"p": p.ID, "r": firstID})
	noErrors(t, reply)
	query := `query($p:ID!, $n:Int!, $after:String){ post(id:$p){id} comments(postId:$p,first:$n,after:$after){edges{cursor node{id text parentId}} pageInfo{hasNextPage endCursor}} }`
	page := request(t, h, "", query, map[string]any{"p": p.ID, "n": 2})
	noErrors(t, page)
	var result struct {
		Edges []struct {
			Node struct {
				ID       string
				ParentID *string
			}
		}
		PageInfo core.PageInfo
	}
	if err := json.Unmarshal(page.Data["comments"], &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Edges) != 2 || !result.PageInfo.HasNextPage || result.PageInfo.EndCursor == nil {
		t.Fatalf("bad page: %+v", result)
	}
	next := request(t, h, "", query, map[string]any{"p": p.ID, "n": 2, "after": *result.PageInfo.EndCursor})
	noErrors(t, next)
	if err := json.Unmarshal(next.Data["comments"], &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Edges) != 1 || result.PageInfo.HasNextPage {
		t.Fatalf("bad next page: %+v", result)
	}
	replies := request(t, h, "", `query($p:ID!,$r:ID!){comments(postId:$p,parentId:$r){edges{node{text parentId}}}}`, map[string]any{"p": p.ID, "r": firstID})
	noErrors(t, replies)
	if !strings.Contains(string(replies.Data["comments"]), "reply") {
		t.Fatal("reply missing")
	}
	closeQuery := `mutation($p:ID!){setCommentsAllowed(postId:$p,allowed:false){commentsAllowed}}`
	errorCode(t, request(t, h, "bob", closeQuery, map[string]any{"p": p.ID}), "FORBIDDEN")
	noErrors(t, request(t, h, "alice", closeQuery, map[string]any{"p": p.ID}))
	errorCode(t, request(t, h, "bob", add, map[string]any{"post": p.ID, "text": "late"}), "FORBIDDEN")
	errorCode(t, request(t, h, "", add, map[string]any{"post": p.ID, "text": "anonymous"}), "UNAUTHENTICATED")
	errorCode(t, request(t, h, "", `{post(id:"9999999"){id}}`, nil), "NOT_FOUND")
	errorCode(t, request(t, h, "", `{post(id:"bad"){id}}`, nil), "BAD_USER_INPUT")
	errorCode(t, request(t, h, "alice", `mutation{createPost(title:"t",text:"x",authorId:"someone"){id}}`, nil), "GRAPHQL_VALIDATION_FAILED")
}
func TestDocumentLimits(t *testing.T) {
	h := newHandler()
	noErrors(t, request(t, h, "", `query($first:Int!){posts(first:$first){edges{node{id title}}}}`, map[string]any{"first": 10}))
	for _, query := range []string{
		`{posts(first:101){edges{node{id}}}}`,
		`{posts(first:0){edges{node{id}}}}`,
		`{` + strings.Repeat("...F ", MaxFields) + `} fragment F on Query{posts{pageInfo{hasNextPage}}}`,
		`{__type(name:"Post"){` + strings.Repeat("ofType{", MaxDepth) + "name" + strings.Repeat("}", MaxDepth) + `}}`,
	} {
		errorCode(t, request(t, h, "", query, nil), "BAD_USER_INPUT")
	}
	errorCode(t, requestOperation(t, h, "", `query One{posts{pageInfo{hasNextPage}}} query Two{posts{pageInfo{hasNextPage}}}`, "One", nil), "BAD_USER_INPUT")
	var wide strings.Builder
	wide.WriteString("{")
	for i := 0; i < MaxFields+1; i++ {
		fmt.Fprintf(&wide, "a%d:__typename ", i)
	}
	wide.WriteString("}")
	errorCode(t, request(t, h, "", wide.String(), nil), "BAD_USER_INPUT")
	var costly strings.Builder
	costly.WriteString("{")
	// Fewer than 100 selections, but >5000 weighted fields after pagination.
	for i := 0; i < 8; i++ {
		fmt.Fprintf(&costly, "p%d:posts(first:100){edges{node{a:id b:id c:id d:id e:id f:id g:id h:id i:id}}} ", i)
	}
	costly.WriteString("}")
	errorCode(t, request(t, h, "", costly.String(), nil), "BAD_USER_INPUT")
	errorCode(t, request(t, h, "", `query($n:Int!){posts(first:$n){edges{node{id}}}}`, map[string]any{"n": 101}), "BAD_USER_INPUT")
	errorCode(t, request(t, h, "", `{ ...Cycle } fragment Cycle on Query { ...Cycle }`, nil), "GRAPHQL_VALIDATION_FAILED")
}

type brokenStore struct{ core.Store }

func (brokenStore) GetPost(context.Context, int64) (core.Post, error) {
	return core.Post{}, errors.New("SELECT secret FROM posts; postgres://admin:password@db")
}
func TestStorageErrorsAreSanitized(t *testing.T) {
	h := New(core.NewService(brokenStore{}), slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Auth: auth.DevHeader()})
	r := request(t, h, "", `{post(id:"1"){id}}`, nil)
	errorCode(t, r, "UNAVAILABLE")
	if r.Errors[0].Message != "storage temporarily unavailable" {
		t.Fatalf("leaked error: %+v", r.Errors)
	}
}

func TestIntrospectionAndDefaultPageSize(t *testing.T) {
	h := newHandler()
	noErrors(t, request(t, h, "", introspection.Query, nil))
	noErrors(t, request(t, h, "", `query($n:Int! = 20){posts(first:$n){edges{node{id}}}}`, nil))
}

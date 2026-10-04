package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/ozon/internal/auth"
	"example.com/ozon/internal/core"
	"example.com/ozon/internal/store/memory"
	"github.com/golang-jwt/jwt/v5"
)

func TestVerifiedIdentity(t *testing.T) {
	key := []byte(strings.Repeat("a", 32))
	identity, err := auth.JWT(key)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	h := New(core.NewService(memory.New()), slog.New(slog.NewTextHandler(&logs, nil)), Options{Auth: identity})
	call := func(token, header, query string) response {
		body, _ := json.Marshal(map[string]string{"query": query})
		r := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-User-ID", header)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var out response
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	alice, err := auth.Issue(key, "alice", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bob, err := auth.Issue(key, "bob", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := auth.Issue([]byte(strings.Repeat("b", 32)), "alice", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	claims := jwt.RegisteredClaims{Issuer: auth.Issuer, Audience: jwt.ClaimStrings{auth.Audience}, Subject: "alice", ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour))}
	expired, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	claims.ExpiresAt = nil
	missingExpiry, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"", "broken", wrong, expired, missingExpiry} {
		errorCode(t, call(token, "alice", `mutation{createPost(title:"t",text:"x"){id}}`), "UNAUTHENTICATED")
	}
	created := call(alice, "bob", `mutation{createPost(title:"t",text:"x"){id authorId}}`)
	noErrors(t, created)
	if !strings.Contains(string(created.Data["createPost"]), `"authorId":"alice"`) {
		t.Fatal("header overrode token identity")
	}
	errorCode(t, call(bob, "alice", `mutation{setCommentsAllowed(postId:"1",allowed:false){id}}`), "FORBIDDEN")
	noErrors(t, call(alice, "bob", `mutation{setCommentsAllowed(postId:"1",allowed:false){id}}`))
	if strings.Contains(logs.String(), alice) || strings.Contains(logs.String(), string(key)) {
		t.Fatal("credentials leaked into logs")
	}
}
func TestBodyRateAndForwardedAddress(t *testing.T) {
	h := New(core.NewService(memory.New()), slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Rate: 1, Burst: 1, MaxBodyBytes: 128})
	run := func(forwarded, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
		r.RemoteAddr = "192.0.2.1:1234"
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Forwarded-For", forwarded)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := run("1.2.3.4", `{"query":"{__typename}"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	if w := run("2.3.4.5", `{"query":"{__typename}"}`); w.Code != 429 {
		t.Fatal("forged proxy header bypassed rate gate", w.Code)
	}
	h = New(core.NewService(memory.New()), slog.New(slog.NewTextHandler(io.Discard, nil)), Options{MaxBodyBytes: 128})
	if w := run("", strings.Repeat("x", 129)); w.Code != 413 {
		t.Fatal("body limit", w.Code)
	}
}

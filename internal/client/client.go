package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"example.com/ozon/internal/graph/model"
)

type Client struct {
	URL, Token string
	HTTP       *http.Client
}

func (c Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}
func (c Client) request(ctx context.Context, query string, variables map[string]any, stream bool) (*http.Response, error) {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(c.URL, "/")+"/query", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		r.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if stream {
		r.Header.Set("Accept", "text/event-stream")
	}
	return c.httpClient().Do(r)
}
func (c Client) Query(ctx context.Context, query string, variables map[string]any, out any) error {
	limited, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := c.request(limited, query, variables, false)
	if err != nil {
		return errors.New("HTTP request failed")
	}
	defer response.Body.Close()
	var payload struct {
		Data   json.RawMessage   `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("HTTP status %d", response.StatusCode)
	}
	if json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&payload) != nil {
		return errors.New("invalid GraphQL response")
	}
	if len(payload.Errors) > 0 {
		return errors.New("GraphQL operation failed")
	}
	return json.Unmarshal(payload.Data, out)
}

// Watch uses notifications as hints and reads the committed feed. The feed
// cursor advances only after the caller accepts a comment, so reconnects can
// recover missed events without keeping an unbounded set of seen IDs.
func (c Client) Watch(ctx context.Context, postID string, accept func(*model.Comment) error) error {
	var cursor *string
	var consumerErr error
	syncFeed := func() error {
		for {
			var result struct {
				Feed model.CommentConnection `json:"commentFeed"`
			}
			err := c.Query(ctx, `query($p:ID!,$after:String){commentFeed(postId:$p,first:100,after:$after){edges{cursor node{id postId parentId authorId text createdAt}}pageInfo{hasNextPage}}}`, map[string]any{"p": postID, "after": cursor}, &result)
			if err != nil {
				return err
			}
			if result.Feed.PageInfo == nil {
				return errors.New("invalid feed page")
			}
			for _, edge := range result.Feed.Edges {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if edge == nil || edge.Node == nil {
					return errors.New("invalid feed item")
				}
				if err = accept(edge.Node); err != nil {
					consumerErr = err
					return err
				}
				value := edge.Cursor
				cursor = &value
			}
			if !result.Feed.PageInfo.HasNextPage {
				return nil
			}
		}
	}
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		connection, cancel := context.WithCancel(ctx)
		response, err := c.request(connection, `subscription($p:ID!){commentAdded(postId:$p){id}}`, map[string]any{"p": postID}, true)
		if err == nil && response.StatusCode == 200 && strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
			hints := make(chan struct{}, 1)
			closed := make(chan struct{})
			ready := false
			go func() {
				defer close(closed)
				scanner := bufio.NewScanner(response.Body)
				scanner.Buffer(make([]byte, 4096), 64<<10)
				for scanner.Scan() {
					line := scanner.Text()
					if line == ": ready" || line == ": heartbeat" || strings.HasPrefix(line, "data:") {
						select {
						case hints <- struct{}{}:
						default:
						}
					}
				}
			}()
			alive := true
			for alive && ctx.Err() == nil {
				select {
				case <-ctx.Done():
					alive = false
				case <-closed:
					alive = false
				case <-hints:
					if err = syncFeed(); err != nil {
						alive = false
					} else {
						ready = true
						backoff = 100 * time.Millisecond
					}
				}
			}
			cancel()
			response.Body.Close()
			<-closed
			if consumerErr != nil {
				return consumerErr
			}
			if ready && ctx.Err() == nil {
				if err = syncFeed(); err != nil && consumerErr != nil {
					return consumerErr
				}
			}
		} else {
			cancel()
			if response != nil {
				response.Body.Close()
			}
		}
		if ctx.Err() != nil {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		backoff = min(2*time.Second, backoff*2)
	}
	return ctx.Err()
}

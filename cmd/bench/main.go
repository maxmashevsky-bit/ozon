package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Result struct {
	Scenario          string           `json:"scenario"`
	Concurrency       int              `json:"concurrency"`
	Duration          float64          `json:"seconds"`
	Warmup            string           `json:"warmup"`
	PublishInterval   string           `json:"publish_interval,omitempty"`
	Attempts          int64            `json:"attempts"`
	Success           int64            `json:"success"`
	RPS               float64          `json:"successful_rps"`
	P50               float64          `json:"p50_ms"`
	P95               float64          `json:"p95_ms"`
	P99               float64          `json:"p99_ms"`
	Errors            map[string]int64 `json:"errors"`
	ExpectedEvents    int64            `json:"expected_events"`
	MissingEvents     int64            `json:"missing_events"`
	Events            int64            `json:"received_events"`
	MatchedEvents     int64            `json:"matched_events"`
	DuplicateEvents   int64            `json:"duplicate_events"`
	UnexpectedEvents  int64            `json:"unexpected_events"`
	InvalidEvents     int64            `json:"invalid_events"`
	Subscribers       int              `json:"subscribers,omitempty"`
	Disconnects       int64            `json:"disconnects"`
	DisconnectReasons map[string]int64 `json:"disconnect_reasons"`
	MissingSamples    []string         `json:"missing_samples,omitempty"`
	DuplicateSamples  []string         `json:"duplicate_samples,omitempty"`
	DeliveryP50       float64          `json:"delivery_p50_ms,omitempty"`
	DeliveryP95       float64          `json:"delivery_p95_ms,omitempty"`
	DeliveryP99       float64          `json:"delivery_p99_ms,omitempty"`
	ExpectedErrors    map[string]int64 `json:"expected_errors"`
	UnexpectedErrors  map[string]int64 `json:"unexpected_errors"`
}

func main() {
	target := flag.String("url", "http://localhost:8080/query", "comma-separated HTTP GraphQL URLs")
	scenario := flag.String("scenario", "read", "read, mixed, uniform, hot, branches, batch, large, subscriptions")
	workers := flag.Int("concurrency", 8, "parallel operations")
	duration := flag.Duration("duration", 5*time.Second, "measurement time")
	warmup := flag.Duration("warmup", time.Second, "warmup time")
	publishInterval := flag.Duration("publish-interval", 0, "minimum gap between SSE mutations (zero means unthrottled)")
	tokenFile := flag.String("token-file", "", "bearer token file")
	output := flag.String("out", "", "JSON result path")
	flag.Parse()
	token := ""
	if *tokenFile != "" {
		b, e := os.ReadFile(*tokenFile)
		if e != nil {
			panic(e)
		}
		token = strings.TrimSpace(string(b))
	}
	targets := strings.Split(*target, ",")
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 256}, Timeout: 12 * time.Second}
	run := func(span time.Duration) Result {
		stop := time.Now().Add(span)
		var sequence atomic.Int64
		var mu sync.Mutex
		var latencies []float64
		result := Result{Scenario: *scenario, Concurrency: *workers, Warmup: warmup.String(), Errors: map[string]int64{}}
		start := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(stop) {
					n := sequence.Add(1)
					post := n%100 + 1
					q := `query{posts(first:20){edges{node{id title}}}}`
					if *scenario == "hot" || *scenario == "uniform" || (*scenario == "mixed" && n%5 == 0) {
						if *scenario == "hot" {
							post = 1
						}
						q = fmt.Sprintf(`mutation{addComment(postId:"%d",text:"load"){id}}`, post)
					} else if *scenario == "mixed" || (*scenario == "read" && n%2 == 0) {
						q = fmt.Sprintf(`{comments(postId:"%d",first:20){edges{node{id text}}}}`, post)
					} else if *scenario == "branches" {
						var b strings.Builder
						b.WriteByte('{')
						for i := 1; i <= 20; i++ {
							fmt.Fprintf(&b, `r%d:comments(postId:"1",parentId:"%d",first:20){edges{node{id}}} `, i, i)
						}
						b.WriteByte('}')
						q = b.String()
					} else if *scenario == "large" {
						q = fmt.Sprintf(`mutation{addComment(postId:"%d",text:"%s"){id}}`, post, strings.Repeat("界", 2000))
					} else if *scenario == "batch" {
						var branches []string
						for i := 1; i <= 20; i++ {
							branches = append(branches, fmt.Sprintf(`{parentId:"%d",first:20}`, i))
						}
						q = `{commentBranches(postId:"1",branches:[` + strings.Join(branches, ",") + `]){edges{node{id}}}}`
					}
					body, _ := json.Marshal(map[string]string{"query": q})
					req, e := http.NewRequestWithContext(context.Background(), "POST", targets[int(n)%len(targets)], bytes.NewReader(body))
					if e != nil {
						panic(e)
					}
					req.Header.Set("Content-Type", "application/json")
					if token != "" {
						req.Header.Set("Authorization", "Bearer "+token)
					} else {
						req.Header.Set("X-User-ID", "bench")
					}
					begin := time.Now()
					response, e := client.Do(req)
					reason := ""
					if e != nil {
						reason = "transport"
					} else {
						var payload struct {
							Errors []struct {
								Extensions struct {
									Code string `json:"code"`
								} `json:"extensions"`
							} `json:"errors"`
							Data json.RawMessage `json:"data"`
						}
						data, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
						response.Body.Close()
						if response.StatusCode != 200 {
							reason = fmt.Sprintf("http_%d", response.StatusCode)
							if json.Unmarshal(data, &payload) == nil && len(payload.Errors) > 0 && payload.Errors[0].Extensions.Code != "" {
								reason = payload.Errors[0].Extensions.Code
							}
						} else if readErr != nil || json.Unmarshal(data, &payload) != nil {
							reason = "decode"
						} else if len(payload.Errors) > 0 {
							reason = payload.Errors[0].Extensions.Code
							if reason == "" {
								reason = "graphql"
							}
						} else if len(payload.Data) == 0 {
							reason = "empty_data"
						}
					}
					elapsed := float64(time.Since(begin).Microseconds()) / 1000
					mu.Lock()
					result.Attempts++
					if reason == "" {
						result.Success++
						latencies = append(latencies, elapsed)
					} else {
						result.Errors[reason]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		result.Duration = time.Since(start).Seconds()
		result.RPS = float64(result.Success) / result.Duration
		sort.Float64s(latencies)
		percentile := func(p float64) float64 {
			if len(latencies) == 0 {
				return 0
			}
			return latencies[int(float64(len(latencies)-1)*p)]
		}
		result.P50 = percentile(.5)
		result.P95 = percentile(.95)
		result.P99 = percentile(.99)
		return result
	}
	if *warmup > 0 && *scenario != "subscriptions" {
		run(*warmup)
	}
	var result Result
	if *scenario == "subscriptions" {
		result = runSubscriptions(client, targets, token, *workers, *warmup, *duration, *publishInterval)
	} else {
		result = run(*duration)
	}
	result.ExpectedErrors = map[string]int64{}
	result.UnexpectedErrors = map[string]int64{}
	for reason, n := range result.Errors {
		if reason == "http_429" || reason == "RATE_LIMITED" || reason == "BUSY" {
			result.ExpectedErrors[reason] = n
		} else {
			result.UnexpectedErrors[reason] = n
		}
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
	if *output != "" {
		if err = os.WriteFile(*output, append(data, '\n'), 0644); err != nil {
			panic(err)
		}
	}
	if result.Success == 0 || len(result.UnexpectedErrors) > 0 ||
		(*scenario == "subscriptions" && (result.Subscribers != *workers || result.MissingEvents > 0 || result.DuplicateEvents > 0 || result.UnexpectedEvents > 0 || result.InvalidEvents > 0 || result.Disconnects > 0)) {
		os.Exit(1)
	}
}

func runSubscriptions(client *http.Client, targets []string, token string, count int, warmup, duration, publishInterval time.Duration) Result {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamClient := &http.Client{Transport: client.Transport}
	var mu sync.Mutex
	var wg sync.WaitGroup
	result := Result{Scenario: "subscriptions", Concurrency: count, Warmup: warmup.String(), PublishInterval: publishInterval.String(), Errors: map[string]int64{}, DisconnectReasons: map[string]int64{}}
	var deliveries, operations []float64
	var measureAfter int64
	seen := make([]map[int64]int, count)
	for i := range seen {
		seen[i] = make(map[int64]int)
	}
	successful := make(map[int64]bool)
	ready := make(chan bool, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			body := []byte(`{"query":"subscription{commentAdded(postId:\"1\"){id text}}"}`)
			req, _ := http.NewRequestWithContext(ctx, "POST", targets[index%len(targets)], bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "text/event-stream")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			response, err := streamClient.Do(req)
			if err != nil {
				mu.Lock()
				result.DisconnectReasons["setup_transport"]++
				mu.Unlock()
				ready <- false
				return
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				mu.Lock()
				result.DisconnectReasons[fmt.Sprintf("setup_http_%d", response.StatusCode)]++
				mu.Unlock()
				ready <- false
				return
			}
			scanner := bufio.NewScanner(response.Body)
			scanner.Buffer(make([]byte, 4096), 128<<10)
			announced := false
			for scanner.Scan() {
				line := scanner.Text()
				if !announced && (line == ": ready" || line == ":") {
					announced = true
					ready <- true
				}
				if strings.HasPrefix(line, "data: ") {
					var event struct {
						Data struct {
							Comment struct {
								ID   string `json:"id"`
								Text string `json:"text"`
							} `json:"commentAdded"`
						} `json:"data"`
					}
					sent, eventID, parseErr := int64(0), int64(0), error(nil)
					if json.Unmarshal([]byte(line[6:]), &event) == nil && strings.HasPrefix(event.Data.Comment.Text, "bench-sse:") {
						sent, parseErr = strconv.ParseInt(strings.TrimPrefix(event.Data.Comment.Text, "bench-sse:"), 10, 64)
						if parseErr == nil {
							eventID, parseErr = strconv.ParseInt(event.Data.Comment.ID, 10, 64)
						}
					}
					if parseErr == nil && sent > 0 && eventID > 0 {
						mu.Lock()
						if sent >= measureAfter {
							result.Events++
							seen[index][eventID]++
							deliveries = append(deliveries, float64(time.Since(time.Unix(0, sent)).Microseconds())/1000)
						}
						mu.Unlock()
					} else {
						mu.Lock()
						result.InvalidEvents++
						mu.Unlock()
					}
				}
			}
			if !announced {
				mu.Lock()
				result.DisconnectReasons["setup_no_ready"]++
				mu.Unlock()
				ready <- false
			}
			if ctx.Err() == nil {
				mu.Lock()
				result.Disconnects++
				if scanner.Err() != nil {
					result.DisconnectReasons["stream_read"]++
				} else {
					result.DisconnectReasons["stream_eof"]++
				}
				mu.Unlock()
			}
		}(i)
	}
	timer := time.NewTimer(15 * time.Second)
	for i := 0; i < count; i++ {
		select {
		case ok := <-ready:
			if ok {
				result.Subscribers++
			}
		case <-timer.C:
			cancel()
			wg.Wait()
			result.Errors["subscription_setup"] = int64(count)
			return result
		}
	}
	timer.Stop()
	started := time.Now()
	measurementStart := started.Add(warmup)
	mu.Lock()
	measureAfter = measurementStart.UnixNano()
	mu.Unlock()
	end := measurementStart.Add(duration)
	for time.Now().Before(end) {
		now := time.Now()
		measuring := !now.Before(measurementStart)
		body, _ := json.Marshal(map[string]string{"query": fmt.Sprintf(`mutation{addComment(postId:"1",text:"bench-sse:%d"){id}}`, now.UnixNano())})
		req, _ := http.NewRequest("POST", targets[0], bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		} else {
			req.Header.Set("X-User-ID", "bench")
		}
		response, err := client.Do(req)
		reason := ""
		var commentID int64
		if err != nil {
			reason = "transport"
		} else {
			var payload struct {
				Errors []json.RawMessage `json:"errors"`
				Data   struct {
					AddComment struct {
						ID string `json:"id"`
					} `json:"addComment"`
				} `json:"data"`
			}
			data, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				reason = fmt.Sprintf("http_%d", response.StatusCode)
			} else if readErr != nil || json.Unmarshal(data, &payload) != nil {
				reason = "decode"
			} else if len(payload.Errors) > 0 {
				reason = "graphql"
			} else if commentID, err = strconv.ParseInt(payload.Data.AddComment.ID, 10, 64); err != nil || commentID <= 0 {
				reason = "decode_id"
			}
		}
		mu.Lock()
		if measuring {
			result.Attempts++
			if reason == "" {
				result.Success++
				successful[commentID] = true
				operations = append(operations, float64(time.Since(now).Microseconds())/1000)
			} else {
				result.Errors[reason]++
			}
		}
		mu.Unlock()
		if publishInterval > 0 {
			remaining := time.Until(end)
			if remaining > 0 {
				time.Sleep(min(publishInterval, remaining))
			}
		}
	}
	result.Duration = time.Since(measurementStart).Seconds()
	result.ExpectedEvents = result.Success * int64(result.Subscribers)
	// A duplicate cannot replace a missing event on another subscriber.
	drain := time.NewTimer(2 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	waiting := true
	for waiting {
		mu.Lock()
		complete := true
		for _, subscriber := range seen {
			for id := range successful {
				if subscriber[id] == 0 {
					complete = false
					break
				}
			}
		}
		mu.Unlock()
		if complete {
			break
		}
		select {
		case <-drain.C:
			waiting = false
		case <-tick.C:
		}
	}
	drain.Stop()
	tick.Stop()
	cancel()
	wg.Wait()
	for index, subscriber := range seen {
		for id := range successful {
			if subscriber[id] == 0 {
				result.MissingEvents++
				if len(result.MissingSamples) < 10 {
					result.MissingSamples = append(result.MissingSamples, fmt.Sprintf("subscriber=%d comment=%d", index, id))
				}
			} else {
				result.MatchedEvents++
				result.DuplicateEvents += int64(subscriber[id] - 1)
				if subscriber[id] > 1 && len(result.DuplicateSamples) < 10 {
					result.DuplicateSamples = append(result.DuplicateSamples, fmt.Sprintf("subscriber=%d comment=%d count=%d", index, id, subscriber[id]))
				}
			}
		}
		for id, n := range subscriber {
			if !successful[id] {
				result.UnexpectedEvents += int64(n)
			}
		}
	}
	result.RPS = float64(result.Success) / result.Duration
	quantiles := func(values []float64) (float64, float64, float64) {
		if len(values) == 0 {
			return 0, 0, 0
		}
		sort.Float64s(values)
		at := func(p float64) float64 { return values[int(float64(len(values)-1)*p)] }
		return at(.5), at(.95), at(.99)
	}
	result.P50, result.P95, result.P99 = quantiles(operations)
	result.DeliveryP50, result.DeliveryP95, result.DeliveryP99 = quantiles(deliveries)
	if result.Subscribers != count {
		result.Errors["subscription_setup"] = int64(count - result.Subscribers)
	}
	return result
}

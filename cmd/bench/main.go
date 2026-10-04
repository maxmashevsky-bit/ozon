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
	Scenario         string           `json:"scenario"`
	Concurrency      int              `json:"concurrency"`
	Duration         float64          `json:"seconds"`
	Warmup           string           `json:"warmup"`
	Attempts         int64            `json:"attempts"`
	Success          int64            `json:"success"`
	RPS              float64          `json:"successful_rps"`
	P50              float64          `json:"p50_ms"`
	P95              float64          `json:"p95_ms"`
	P99              float64          `json:"p99_ms"`
	Errors           map[string]int64 `json:"errors"`
	ExpectedEvents   int64            `json:"expected_events,omitempty"`
	MissingEvents    int64            `json:"missing_events,omitempty"`
	Events           int64            `json:"events,omitempty"`
	Subscribers      int              `json:"subscribers,omitempty"`
	Disconnects      int64            `json:"disconnects,omitempty"`
	DeliveryP50      float64          `json:"delivery_p50_ms,omitempty"`
	DeliveryP95      float64          `json:"delivery_p95_ms,omitempty"`
	DeliveryP99      float64          `json:"delivery_p99_ms,omitempty"`
	ExpectedErrors   map[string]int64 `json:"expected_errors"`
	UnexpectedErrors map[string]int64 `json:"unexpected_errors"`
}

func main() {
	target := flag.String("url", "http://localhost:8080/query", "comma-separated HTTP GraphQL URLs")
	scenario := flag.String("scenario", "read", "read, mixed, uniform, hot, branches, batch, subscriptions")
	workers := flag.Int("concurrency", 8, "parallel operations")
	duration := flag.Duration("duration", 5*time.Second, "measurement time")
	warmup := flag.Duration("warmup", time.Second, "warmup time")
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
		result = runSubscriptions(client, targets, token, *workers, *warmup, *duration)
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
	if result.Success == 0 || len(result.UnexpectedErrors) > 0 {
		os.Exit(1)
	}
}

func runSubscriptions(client *http.Client, targets []string, token string, count int, warmup, duration time.Duration) Result {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamClient := &http.Client{Transport: client.Transport}
	var mu sync.Mutex
	var wg sync.WaitGroup
	result := Result{Scenario: "subscriptions", Concurrency: count, Warmup: warmup.String(), Errors: map[string]int64{}}
	var deliveries, operations []float64
	measuring := false
	var measureAfter int64
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
				ready <- false
				return
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
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
								Text string `json:"text"`
							} `json:"commentAdded"`
						} `json:"data"`
					}
					sent, parseErr := int64(0), error(nil)
					if json.Unmarshal([]byte(line[6:]), &event) == nil && strings.HasPrefix(event.Data.Comment.Text, "bench-sse:") {
						sent, parseErr = strconv.ParseInt(strings.TrimPrefix(event.Data.Comment.Text, "bench-sse:"), 10, 64)
					}
					if parseErr == nil && sent > 0 {
						mu.Lock()
						if measuring && sent >= measureAfter {
							result.Events++
							deliveries = append(deliveries, float64(time.Since(time.Unix(0, sent)).Microseconds())/1000)
						}
						mu.Unlock()
					}
				}
			}
			if !announced {
				ready <- false
			}
			if ctx.Err() == nil {
				mu.Lock()
				result.Disconnects++
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
		mu.Lock()
		measuring = !now.Before(measurementStart)
		mu.Unlock()
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
		if err != nil {
			reason = "transport"
		} else {
			var payload struct {
				Errors []json.RawMessage `json:"errors"`
			}
			data, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				reason = fmt.Sprintf("http_%d", response.StatusCode)
			} else if readErr != nil || json.Unmarshal(data, &payload) != nil {
				reason = "decode"
			} else if len(payload.Errors) > 0 {
				reason = "graphql"
			}
		}
		mu.Lock()
		if measuring {
			result.Attempts++
			if reason == "" {
				result.Success++
				operations = append(operations, float64(time.Since(now).Microseconds())/1000)
			} else {
				result.Errors[reason]++
			}
		}
		mu.Unlock()
	}
	result.Duration = time.Since(measurementStart).Seconds()
	result.ExpectedEvents = result.Success * int64(result.Subscribers)
	// A bounded drain measures late delivery rather than discarding in-flight events.
	drain := time.NewTimer(2 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	waiting := true
	for waiting {
		mu.Lock()
		complete := result.Events >= result.ExpectedEvents
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
	result.MissingEvents = max(0, result.ExpectedEvents-result.Events)
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

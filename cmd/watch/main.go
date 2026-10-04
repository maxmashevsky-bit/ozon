package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"example.com/ozon/internal/client"
	"example.com/ozon/internal/graph/model"
)

func main() {
	url := flag.String("url", "http://localhost:8080", "service URL")
	post := flag.String("post", "", "post ID")
	tokenFile := flag.String("token-file", ".local/alice.token", "token file")
	duration := flag.Duration("duration", time.Hour, "maximum watch duration")
	count := flag.Int("count", 0, "stop after this many unique stored comments (0 means unlimited)")
	flag.Parse()
	token, err := os.ReadFile(*tokenFile)
	if err != nil || *post == "" {
		fmt.Fprintln(os.Stderr, "configure post and token file")
		os.Exit(1)
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, *duration)
	defer cancel()
	c := client.Client{URL: *url, Token: strings.TrimSpace(string(token))}
	delivered := 0
	err = c.Watch(ctx, *post, func(comment *model.Comment) error {
		if err := json.NewEncoder(os.Stdout).Encode(comment); err != nil {
			return err
		}
		delivered++
		if *count > 0 && delivered >= *count {
			cancel()
		}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "watch failed")
		os.Exit(1)
	}
	if *count > 0 && delivered < *count {
		fmt.Fprintln(os.Stderr, "watch ended before expected count")
		os.Exit(1)
	}
}

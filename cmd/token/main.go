package main

import (
	"example.com/ozon/internal/auth"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	user := flag.String("user", "alice", "token subject")
	ttl := flag.Duration("ttl", time.Hour, "token lifetime, at most 24h")
	file := flag.String("key-file", os.Getenv("JWT_SECRET_FILE"), "JWT key file")
	flag.Parse()
	key, err := auth.ReadKey(*file)
	if err == nil {
		var token string
		token, err = auth.Issue(key, *user, *ttl)
		if err == nil {
			fmt.Println(token)
			return
		}
	}
	fmt.Fprintln(os.Stderr, "cannot issue token: check key, subject and lifetime")
	os.Exit(1)
}

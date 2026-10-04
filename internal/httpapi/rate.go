package httpapi

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type clientBucket struct {
	limiter *rate.Limiter
	seen    time.Time
}
type rateGate struct {
	mu      sync.Mutex
	clients map[string]clientBucket
	limit   rate.Limit
	burst   int
	sweep   time.Time
}

func newRateGate(rps float64, burst int) *rateGate {
	return &rateGate{clients: map[string]clientBucket{}, limit: rate.Limit(rps), burst: burst}
}
func (g *rateGate) allow(key string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now.Sub(g.sweep) > time.Minute {
		for k, b := range g.clients {
			if now.Sub(b.seen) > 5*time.Minute {
				delete(g.clients, k)
			}
		}
		g.sweep = now
	}
	bucket, ok := g.clients[key]
	if !ok {
		if len(g.clients) >= 10000 {
			return false
		}
		bucket.limiter = rate.NewLimiter(g.limit, g.burst)
	}
	bucket.seen = now
	g.clients[key] = bucket
	return bucket.limiter.AllowN(now, 1)
}
func clientIP(r *http.Request, trusted []netip.Prefix) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "unknown"
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return "unknown"
	}
	ip = ip.Unmap()
	isTrusted := func(ip netip.Addr) bool {
		for _, prefix := range trusted {
			if prefix.Contains(ip) {
				return true
			}
		}
		return false
	}
	if !isTrusted(ip) {
		return ip.String()
	}
	chain := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(chain) - 1; i >= 0; i-- {
		candidate, err := netip.ParseAddr(strings.TrimSpace(chain[i]))
		if err != nil {
			return ip.String()
		}
		ip = candidate.Unmap()
		if !isTrusted(ip) {
			return ip.String()
		}
	}
	return ip.String()
}

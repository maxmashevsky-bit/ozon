package auth

import (
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"example.com/ozon/internal/core"
	"github.com/golang-jwt/jwt/v5"
)

const Issuer = "ozon-local"
const Audience = "ozon-api"

var ErrCredentials = errors.New("invalid or expired credentials")

type Authenticator struct {
	key []byte
	dev bool
}

func JWT(key []byte) (*Authenticator, error) {
	if len(key) < 32 {
		return nil, errors.New("JWT key must contain at least 32 bytes")
	}
	return &Authenticator{key: append([]byte(nil), key...)}, nil
}
func DevHeader() *Authenticator { return &Authenticator{dev: true} }
func ReadKey(file string) ([]byte, error) {
	var key []byte
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, errors.New("cannot read JWT key file")
		}
		key = []byte(strings.TrimSpace(string(b)))
	} else {
		key = []byte(os.Getenv("JWT_SECRET"))
	}
	if len(key) < 32 {
		return nil, errors.New("configure JWT_SECRET_FILE or JWT_SECRET with at least 32 bytes")
	}
	return key, nil
}
func (a *Authenticator) Actor(r *http.Request) (string, error) {
	if a == nil {
		return "", nil
	}
	if a.dev {
		actor := r.Header.Get("X-User-ID")
		if actor == "" {
			return "", nil
		}
		if core.ValidateActor(actor) != nil {
			return "", ErrCredentials
		}
		return actor, nil
	}
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", nil
	}
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) > 4096 {
		return "", ErrCredentials
	}
	claims := &jwt.RegisteredClaims{}
	token, err := jwt.ParseWithClaims(parts[1], claims, func(*jwt.Token) (any, error) { return a.key, nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer(Issuer), jwt.WithAudience(Audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt())
	if err != nil || !token.Valid || core.ValidateActor(claims.Subject) != nil {
		return "", ErrCredentials
	}
	return claims.Subject, nil
}
func Issue(key []byte, subject string, ttl time.Duration) (string, error) {
	if _, err := JWT(key); err != nil {
		return "", err
	}
	if err := core.ValidateActor(subject); err != nil {
		return "", err
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		return "", errors.New("token lifetime must be between 0 and 24h")
	}
	now := time.Now()
	claims := jwt.RegisteredClaims{Issuer: Issuer, Audience: jwt.ClaimStrings{Audience}, Subject: subject, IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(ttl))}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
}

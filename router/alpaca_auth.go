package router

import (
	"context"
	"crypto/subtle"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	headerAPIKeyID     = "X-Alpaca-Api-Key-Id"
	headerAPISecretKey = "X-Alpaca-Api-Secret-Key"
)

type alpacaCredentials struct {
	mu     sync.Mutex
	secret string
	loaded bool
}

func (c *alpacaCredentials) resolve() (secret string, loaded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.secret, c.loaded
}

func (c *alpacaCredentials) load(secret string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secret, c.loaded = secret, true
}

func (c *alpacaCredentials) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secret, c.loaded = "", false
}

type alpacaSecretContextKey struct{}

func alpacaSecretFromContext(ctx context.Context) string {
	secret, _ := ctx.Value(alpacaSecretContextKey{}).(string)
	return secret
}

func alpacaAPIKeyID() string {
	if path := strings.TrimSpace(os.Getenv("ALPACA_API_KEY_ID_FILE")); path != "" {
		if value, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(value))
		}
	}
	return strings.TrimSpace(os.Getenv("ALPACA_API_KEY_ID"))
}

func requireAlpacaAuth(creds *alpacaCredentials) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerKey := r.Header.Get(headerAPIKeyID)
			configuredKey := alpacaAPIKeyID()
			if headerKey == "" || configuredKey == "" ||
				subtle.ConstantTimeCompare([]byte(headerKey), []byte(configuredKey)) != 1 {
				http.Error(w, "invalid or missing "+headerAPIKeyID, http.StatusUnauthorized)
				return
			}

			secret, loaded := creds.resolve()
			if !loaded {
				secret = r.Header.Get(headerAPISecretKey)
				if secret == "" {
					http.Error(w, "missing "+headerAPISecretKey, http.StatusUnauthorized)
					return
				}
				creds.load(secret)
			}

			ctx := context.WithValue(r.Context(), alpacaSecretContextKey{}, secret)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

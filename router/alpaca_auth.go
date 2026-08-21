package router

import (
	"context"
	"crypto/subtle"
	"net/http"
	"os"
	"sync"
)

// Header names a caller uses to authenticate to stonks and supply
// Alpaca credentials. Chosen to match this repo's own ALPACA_API_KEY_ID/
// ALPACA_API_SECRET_KEY naming rather than Alpaca's own APCA_* SDK
// convention, since these are headers to stonks, not to Alpaca itself.
const (
	headerAPIKeyID     = "X-Alpaca-Api-Key-Id"
	headerAPISecretKey = "X-Alpaca-Api-Secret-Key"
)

// alpacaCredentials caches the Alpaca API secret this container is
// currently authenticated with. The secret is never a GitHub secret or
// Cloud Function env var -- it only ever arrives via headerAPISecretKey,
// and is cached here, in memory, for this container's lifetime only (or
// until clear() runs), so only the first request since a cold start (or
// since the last clear) needs to supply it; every later request in the
// same container only repeats the (env-var-backed) API key header. One
// alpacaCredentials is constructed per container in NewRouter and shared
// by every request it handles, the same lifetime as the pubsub.Holder
// it's constructed alongside.
type alpacaCredentials struct {
	mu     sync.Mutex
	secret string
	loaded bool
}

// resolve returns the cached secret and whether one is cached.
func (c *alpacaCredentials) resolve() (secret string, loaded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.secret, c.loaded
}

// load caches secret as this container's current Alpaca secret.
func (c *alpacaCredentials) load(secret string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secret, c.loaded = secret, true
}

// clear discards the cached secret, so the next request falls back to
// requiring headerAPISecretKey again. Called when Alpaca itself rejects
// the cached secret, so a corrected one can be supplied.
func (c *alpacaCredentials) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secret, c.loaded = "", false
}

type alpacaSecretContextKey struct{}

// alpacaSecretFromContext returns the Alpaca secret requireAlpacaAuth
// resolved for this request (freshly supplied or from cache).
func alpacaSecretFromContext(ctx context.Context) string {
	secret, _ := ctx.Value(alpacaSecretContextKey{}).(string)
	return secret
}

// requireAlpacaAuth authenticates a request against creds: headerAPIKeyID
// must equal the ALPACA_API_KEY_ID env var, always. If creds has no
// secret cached yet, headerAPISecretKey must also be present -- it's
// cached in creds and used for this request; every later request on the
// same container skips this and reuses the cached secret. The resolved
// secret is attached to the request context for Configure to read.
func requireAlpacaAuth(creds *alpacaCredentials) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerKey := r.Header.Get(headerAPIKeyID)
			envKey := os.Getenv("ALPACA_API_KEY_ID")
			if headerKey == "" || envKey == "" ||
				subtle.ConstantTimeCompare([]byte(headerKey), []byte(envKey)) != 1 {
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

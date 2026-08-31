package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/xd-dash/logma/serverless/pubsub"
	baserouter "github.com/xd-dash/logma/serverless/router"
)

// NewRouter builds stonks's router via Logma serverless's shared HTTP shell.
// Each completed request gets a fresh StonksRuntime; the active request owns
// both its Alpaca publisher and the request-scoped SSE relay.
func NewRouter() http.Handler {
	creds := &alpacaCredentials{}
	holder := pubsub.NewHolder(func() *StonksRuntime { return NewStonksRuntime(creds) })

	return baserouter.Build(func(r chi.Router) {
		r.With(requireAlpacaAuth(creds)).Post("/stream", streamHandler(holder))
	})
}

package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	baserouter "github.com/xd-dash/logma/serverless/router"
)

// NewRouter builds stonks's router via Logma serverless's shared HTTP shell.
// The process owns one retained/shared Alpaca publisher; every /stream request
// creates only request-scoped Logma Redis subscribers and an SSE response.
func NewRouter() http.Handler {
	creds := &alpacaCredentials{}
	publisher := newPublisherManager(creds)

	return baserouter.Build(func(r chi.Router) {
		r.With(requireAlpacaAuth(creds)).Post("/stream", streamHandler(publisher))
	})
}

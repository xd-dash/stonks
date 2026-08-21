package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/xd-dash/logma-serverless/pubsub"
	baserouter "github.com/xd-dash/logma-serverless/router"
)

// NewRouter builds stonks's router via logma-serverless's router.Build
// shell -- its standard middleware stack -- dropping in this package's
// own /stream route and handler as the register closure. A container
// instance lives across many sequential requests, but each request's
// runtime is single-use: once a session's StonksRuntime finishes, the
// next request gets a fresh one.
func NewRouter() http.Handler {
	holder := pubsub.NewHolder(NewStonksRuntime)

	return baserouter.Build(func(r chi.Router) {
		r.Post("/stream", streamHandler(holder))
	})
}

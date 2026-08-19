package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// NewRouter builds the router for this deployment. It's called once per
// container by gospace-minimal's generated routersource.Serve(), so the
// pubsub.Holder constructed here lives for the container's entire
// lifetime and is shared by every request it handles -- mirroring
// logma-serverless's own NewRouter. A container instance lives across
// many sequential requests, but each request's runtime is single-use:
// once a session's Runtime finishes, the next request gets a fresh one.
//
// There is deliberately no "/" route: if concurrency ends up pinned to
// 1 per container (as logma-serverless's is), a health-check path would
// itself consume the container's one request slot.
func NewRouter() http.Handler {
	holder := pubsub.NewHolder(NewRuntime)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/stream", streamHandler(holder))

	return r
}

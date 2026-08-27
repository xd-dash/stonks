package router

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// streamHandler backs POST /stream: it validates the request, claims a
// runtime, configures it, and blocks until the stream stops (via
// stonks:control:shutdown, client disconnect, or a terminal Alpaca error),
// reporting the outcome as JSON. Consumers never connect to stonks itself for
// the data -- they subscribe to the derived Redis channels through
// logma-serverless's SSE layer instead.
func streamHandler(holder *pubsub.Holder[*StonksRuntime]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req StreamRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		cfg, err := req.validate()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		rt, ok := holder.Claim()
		if !ok {
			http.Error(w, "stream already running", http.StatusConflict)
			return
		}
		// The runtime owns its Redis client for exactly this stream session.
		// Deferring Close immediately after Claim keeps every later exit path
		// deterministic, including future validation/configuration changes.
		defer rt.Close()

		rt.RecordInvocation(r, middleware.GetReqID(r.Context()))
		rt.Configure(cfg, alpacaSecretFromContext(r.Context()))
		rt.Start(r.Context())

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
	}
}

package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/xd-dash/logma/serverless/pubsub"
)

const (
	stonksSSEKeepAlive = 15 * time.Second
	stonksSSEBuffer    = 256
)

type streamEvent struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// streamHandler backs POST /stream. One request owns one Stonks publisher
// session and one embedded Logma serverless Redis->SSE relay. The relay is
// subscribed and Redis-acknowledged before Alpaca is allowed to start so the
// first market event cannot race the consumer into existence.
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

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		rt, ok := holder.Claim()
		if !ok {
			http.Error(w, "stream already running", http.StatusConflict)
			return
		}
		rt.RecordInvocation(r, middleware.GetReqID(r.Context()))
		rt.Configure(cfg, alpacaSecretFromContext(r.Context()))

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		events := make(chan streamEvent, stonksSSEBuffer)
		channels := rt.streamChannels()
		subscribers := make([]*pubsub.Subscriber, 0, len(channels))
		for _, channel := range channels {
			channel := channel
			subscriber := pubsub.Subscribe(ctx, rt.Client, channel, func(payload string) {
				event := streamEvent{Channel: channel, Data: json.RawMessage(payload)}
				select {
				case events <- event:
				case <-ctx.Done():
				default:
					// A live screener must not silently convert transport overload
					// into incomplete market state. Fail the request instead.
					log.Printf("stonks: SSE consumer fell behind on %s; cancelling request", channel)
					cancel()
				}
			})
			subscribers = append(subscribers, subscriber)
		}

		// Redis Pub/Sub does not replay. Wait until every subscription has
		// received its Redis acknowledgement before connecting Alpaca.
		for _, subscriber := range subscribers {
			select {
			case <-subscriber.Ready():
			case <-ctx.Done():
				return
			}
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": connected\n\n"))
		flusher.Flush()

		go rt.Start(ctx)

		keepAlive := time.NewTicker(stonksSSEKeepAlive)
		defer keepAlive.Stop()
		for {
			select {
			case <-ctx.Done():
				rt.Cancel()
				return
			case <-rt.Done():
				return
			case event := <-events:
				encoded, err := json.Marshal(event)
				if err != nil {
					continue
				}
				if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", encoded); err != nil {
					cancel()
					return
				}
				flusher.Flush()
			case <-keepAlive.C:
				if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
					cancel()
					return
				}
				flusher.Flush()
			}
		}
	}
}

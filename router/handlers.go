package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

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

// streamHandler backs POST /stream. Every request owns only its Logma
// Redis->SSE subscriptions. The Alpaca publisher is retained/shared by the
// process-level publisherManager and survives requester disconnects.
func streamHandler(publisher *publisherManager) http.HandlerFunc {
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

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// prepare may create the shared publisher, but intentionally does not
		// start Alpaca yet. This lets the first requester make every Redis
		// subscription live before the producer emits its first event.
		rt := publisher.prepare(r, cfg, alpacaSecretFromContext(r.Context()))
		channels := rt.streamChannels(cfg)
		events := make(chan streamEvent, stonksSSEBuffer)
		subscribers := make([]*pubsub.Subscriber, 0, len(channels))

		for _, channel := range channels {
			channel := channel
			subscriber := pubsub.Subscribe(ctx, rt.Client, channel, func(payload string) {
				event := streamEvent{Channel: channel, Data: json.RawMessage(payload)}
				select {
				case events <- event:
				case <-ctx.Done():
				default:
					// Do not silently turn an overloaded transport into incomplete
					// market state for a screener. End only this requester.
					log.Printf("stonks: SSE requester fell behind on %s; cancelling requester", channel)
					cancel()
				}
			})
			subscribers = append(subscribers, subscriber)
		}

		for _, subscriber := range subscribers {
			select {
			case <-subscriber.Ready():
			case <-ctx.Done():
				return
			}
		}

		// The first request starts the process-owned publisher only after its
		// subscribers are ready. Later requests merely ensure any newly asked
		// symbol/type pairs are subscribed on the existing Alpaca connection.
		if err := publisher.activate(ctx, rt, cfg); err != nil {
			http.Error(w, "publisher unavailable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": connected\n\n"))
		flusher.Flush()

		keepAlive := time.NewTicker(stonksSSEKeepAlive)
		defer keepAlive.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-rt.Done():
				return
			case event := <-events:
				encoded, err := json.Marshal(event)
				if err != nil {
					continue
				}
				if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", encoded); err != nil {
					return
				}
				flusher.Flush()
			case <-keepAlive.C:
				if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

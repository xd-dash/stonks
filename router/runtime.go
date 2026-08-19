// Package router implements the stonks HTTP entry point: POST /stream
// takes a list of tickers, connects to the Alpaca market-data stream on
// their behalf, and publishes every trade/quote/bar it receives onto a
// deterministic Redis channel (stonks:<type>:<SYMBOL>) instead of
// returning it over the same HTTP connection. Anyone who wants to watch
// the stream connects to logma-serverless, which subscribes to those
// channels and fans them out over SSE.
//
// The single *redis.Client a Runtime owns is used for exactly two
// things: Publish for outbound market data, and a Subscribe (via the
// shared pubsub package) on stonks:control:shutdown for its own
// lifecycle. It never issues any other Redis command.
package router

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/redis/go-redis/v9"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// shutdownControlChannel is a fixed, global channel name -- publishing to
// it ends every stonks Runtime currently listening, the same broadcast
// semantics logma-serverless's own control:shutdown channel has.
const shutdownControlChannel = "stonks:control:shutdown"

const (
	runtimeStateIdle int32 = iota
	runtimeStateRunning
	runtimeStateDone
)

// ShutdownRequest is the payload published to stonks:control:shutdown to
// end a running stream.
type ShutdownRequest struct {
	Reason string `json:"reason"`
}

// Runtime is a container-global, single-owner actor: it owns one Alpaca
// stream connection and the Redis client used for both its outbound
// publishing and its inbound control-plane subscription. Claim() is a
// defensive guard against a second request driving the same runtime
// concurrently; the actual guarantee comes from the Cloud Function's
// maxInstanceRequestConcurrency=1 configuration.
type Runtime struct {
	client       *redis.Client
	streamClient *stream.StocksClient
	cfg          streamConfig

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	state     atomic.Int32
	startOnce sync.Once
}

// NewRuntime builds a Runtime wired to REDIS_URI/REDISCLI_AUTH. It does
// not connect to Redis or Alpaca, and has no stream configuration, until
// Configure and Start are called.
func NewRuntime() *Runtime {
	ctx, cancel := context.WithCancel(context.Background())

	return &Runtime{
		client: pubsub.NewClientFromEnv(),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

// Configure wires this Runtime's Alpaca stream client to cfg. It must be
// called exactly once, after a successful Claim and before Start.
func (rt *Runtime) Configure(cfg streamConfig) {
	rt.cfg = cfg
	rt.streamClient = stream.NewStocksClient(cfg.feed, rt.streamOptions()...)
}

func (rt *Runtime) streamOptions() []stream.StockOption {
	opts := make([]stream.StockOption, 0, len(rt.cfg.subscriptions))
	for _, sub := range rt.cfg.subscriptions {
		switch sub {
		case subTrades:
			opts = append(opts, stream.WithTrades(rt.onTrade, rt.cfg.tickers...))
		case subQuotes:
			opts = append(opts, stream.WithQuotes(rt.onQuote, rt.cfg.tickers...))
		case subBars:
			opts = append(opts, stream.WithBars(rt.onBar, rt.cfg.tickers...))
		case subDailyBars:
			opts = append(opts, stream.WithDailyBars(rt.onDailyBar, rt.cfg.tickers...))
		}
	}
	return opts
}

func (rt *Runtime) onTrade(t stream.Trade)  { rt.publish(subTrades, t.Symbol, t) }
func (rt *Runtime) onQuote(q stream.Quote)  { rt.publish(subQuotes, q.Symbol, q) }
func (rt *Runtime) onBar(b stream.Bar)      { rt.publish(subBars, b.Symbol, b) }
func (rt *Runtime) onDailyBar(b stream.Bar) { rt.publish(subDailyBars, b.Symbol, b) }

func (rt *Runtime) publish(sub subscriptionType, symbol string, event any) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("stonks: failed to marshal %s event for %s: %v", sub, symbol, err)
		return
	}

	channel := channelFor(sub, symbol)
	if err := rt.client.Publish(rt.ctx, channel, data).Err(); err != nil {
		log.Printf("stonks: failed to publish to %s: %v", channel, err)
	}
}

// Claim attempts to take exclusive ownership of the runtime for the
// calling request. It returns false if the runtime has already been
// claimed or has already run to completion.
func (rt *Runtime) Claim() bool {
	return rt.state.CompareAndSwap(runtimeStateIdle, runtimeStateRunning)
}

// Done returns a channel that's closed once Start has returned.
func (rt *Runtime) Done() <-chan struct{} {
	return rt.done
}

// Cancel ends the runtime's lifetime, causing Start to return. It's safe
// to call multiple times and from any goroutine.
func (rt *Runtime) Cancel() {
	rt.cancel()
}

// Start connects to the Alpaca stream and blocks until ctx is cancelled,
// a stonks:control:shutdown message is received, or the Alpaca
// connection terminates on its own. It must only be called once
// (guarded by startOnce -- a second call is a no-op that returns
// immediately), and Configure must have been called first.
func (rt *Runtime) Start(ctx context.Context) {
	rt.startOnce.Do(func() {
		defer rt.state.Store(runtimeStateDone)
		defer close(rt.done)

		go func() {
			select {
			case <-ctx.Done():
				rt.cancel()
			case <-rt.ctx.Done():
			}
		}()

		shutdown := pubsub.Subscribe(rt.ctx, rt.client, shutdownControlChannel, rt.handleShutdown)
		defer func() {
			rt.cancel()
			<-shutdown.Stopped()
		}()

		if err := rt.streamClient.Connect(rt.ctx); err != nil {
			log.Printf("stonks: failed to connect to Alpaca stream: %v", err)
			return
		}
		log.Printf("stonks: connected to Alpaca stream (feed=%s tickers=%v)", rt.cfg.feed, rt.cfg.tickers)

		select {
		case <-rt.ctx.Done():
		case err := <-rt.streamClient.Terminated():
			if err != nil {
				log.Printf("stonks: Alpaca stream terminated: %v", err)
			}
		}
	})
}

func (rt *Runtime) handleShutdown(payload string) {
	var request ShutdownRequest
	if payload != "" {
		if err := json.Unmarshal([]byte(payload), &request); err != nil {
			log.Printf("stonks: invalid shutdown message: %v", err)
		}
	}
	log.Printf("stonks: shutting down runtime: reason=%q", request.Reason)
	rt.cancel()
}

// Package router implements the stonks HTTP entry point: POST /stream
// takes a list of tickers, connects to the Alpaca market-data stream on
// their behalf, and publishes every trade/quote/bar it receives onto a
// deterministic Redis channel (stonks:<type>:<SYMBOL>) instead of
// returning it over the same HTTP connection. Anyone who wants to watch
// the stream connects to logma-serverless, which subscribes to those
// channels and fans them out over SSE.
//
// The single *redis.Client a Runtime owns (via the embedded
// pubsub.ControlPlane) is used for exactly two things: Publish for
// outbound market data, and Subscribe (via the shared pubsub package)
// on stonks:control:shutdown/stonks:control:add for its own lifecycle
// and ticker management. It never issues any other Redis command.
package router

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// Base control channel names. Each is scoped in two ways by the
// embedded pubsub.ControlPlane: publish to its instance channel to
// target only this container, or to its global channel to reach every
// stonks Runtime currently listening -- the same instance/global split
// logma-serverless's own control:add/control:shutdown channels use.
const (
	shutdownControlChannel = "stonks:control:shutdown"
	addControlChannel      = "stonks:control:add"
)

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
	pubsub.ControlPlane

	streamClient *stream.StocksClient
	cfg          streamConfig

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	invocation pubsub.InvocationInfo

	state     atomic.Int32
	startOnce sync.Once
}

// NewRuntime builds a Runtime wired to REDIS_URI/REDISCLI_AUTH. It does
// not connect to Redis or Alpaca, and has no stream configuration, until
// Configure and Start are called.
func NewRuntime() *Runtime {
	ctx, cancel := context.WithCancel(context.Background())

	return &Runtime{
		ControlPlane: pubsub.NewControlPlane(pubsub.NewClientFromEnv()),
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
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
	if err := rt.Client.Publish(rt.ctx, channel, data).Err(); err != nil {
		log.Printf("stonks: failed to publish to %s: %v", channel, err)
	}
}

// RecordInvocation captures which Cloud Function instance and HTTP
// request are driving this Runtime. It must be called after a
// successful Claim and before Start -- Start records it in Redis as the
// first thing it does, strictly before the client's first Subscribe.
func (rt *Runtime) RecordInvocation(r *http.Request, requestID string) {
	rt.invocation = pubsub.InvocationInfoFromRequest(r, requestID)
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

		// Recorded via plain Redis commands before the client issues its
		// first Subscribe below -- once it does, this client is never
		// used for anything but Publish/Subscribe again.
		if err := pubsub.RegisterInvocation(rt.ctx, rt.Client, rt.invocation); err != nil {
			log.Printf("stonks: failed to record invocation info: %v", err)
		}

		shutdownInstance, shutdownRelay := rt.ControlPlane.Subscribe(rt.ctx, shutdownControlChannel, rt.handleShutdown)
		addInstance, addRelay := rt.ControlPlane.Subscribe(rt.ctx, addControlChannel, rt.handleAdd)
		defer func() {
			rt.cancel()
			<-shutdownInstance.Stopped()
			<-shutdownRelay.Stopped()
			<-addInstance.Stopped()
			<-addRelay.Stopped()
		}()

		if err := rt.streamClient.Connect(rt.ctx); err != nil {
			log.Printf("stonks: failed to connect to Alpaca stream: %v", err)
			return
		}
		log.Printf("stonks: connected to Alpaca stream (instance=%s feed=%s tickers=%v)", rt.InstanceID, rt.cfg.feed, rt.cfg.tickers)

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

// handleAdd hot-adds tickers to the already-connected Alpaca stream, in
// response to a stonks:control:add publish -- the only way to add
// tickers once POST /stream has claimed the runtime and is blocked in
// Start. It's only ever called sequentially from the single
// control:add subscriber goroutine, so it never calls the SDK's
// subscription-change methods concurrently with itself.
func (rt *Runtime) handleAdd(payload string) {
	var request AddTickersRequest
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		log.Printf("stonks: invalid add-tickers message: %v", err)
		return
	}

	cfg, err := request.validate()
	if err != nil {
		log.Printf("stonks: invalid add-tickers request: %v", err)
		return
	}

	for _, sub := range cfg.subscriptions {
		var subErr error
		switch sub {
		case subTrades:
			subErr = rt.streamClient.SubscribeToTrades(rt.onTrade, cfg.tickers...)
		case subQuotes:
			subErr = rt.streamClient.SubscribeToQuotes(rt.onQuote, cfg.tickers...)
		case subBars:
			subErr = rt.streamClient.SubscribeToBars(rt.onBar, cfg.tickers...)
		case subDailyBars:
			subErr = rt.streamClient.SubscribeToDailyBars(rt.onDailyBar, cfg.tickers...)
		}
		if subErr != nil {
			log.Printf("stonks: failed to add %s subscription for %v: %v", sub, cfg.tickers, subErr)
		}
	}
	log.Printf("stonks: added tickers %v (%v)", cfg.tickers, cfg.subscriptions)
}

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
	"fmt"
	"log"
	"net/http"

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

// Runtime is a container-global, single-owner actor: it owns one Alpaca
// stream connection and the Redis client used for both its outbound
// publishing and its inbound control-plane subscription. Claim() is a
// defensive guard against a second request driving the same runtime
// concurrently; the actual guarantee comes from the Cloud Function's
// maxInstanceRequestConcurrency=1 configuration.
type Runtime struct {
	pubsub.ControlPlane
	pubsub.Session

	streamClient *stream.StocksClient
	cfg          streamConfig

	invocation pubsub.InvocationInfo
}

// NewRuntime builds a Runtime wired to REDIS_URI/REDISCLI_AUTH. It does
// not connect to Redis or Alpaca, and has no stream configuration, until
// Configure and Start are called.
func NewRuntime() *Runtime {
	return &Runtime{
		ControlPlane: pubsub.NewControlPlane(pubsub.NewClientFromEnv()),
		Session:      pubsub.NewSession(),
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
	if err := rt.Client.Publish(rt.Context(), channel, data).Err(); err != nil {
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

// Start connects to the Alpaca stream and blocks until ctx is cancelled,
// a stonks:control:shutdown message is received, or the Alpaca
// connection terminates on its own. It must only be called once -- a
// second call is a no-op that returns immediately (guarded by the
// embedded Session.Begin, which also provides Claim/Done/Cancel) -- and
// Configure must have been called first.
func (rt *Runtime) Start(ctx context.Context) {
	rt.Begin(ctx, rt.run)
}

// run declares this Runtime's ServiceSpec -- which control channels it
// listens on and what actually does the work -- and hands it to the
// embedded ControlPlane.Run, which owns recording invocation info,
// wiring every channel's instance+global subscriptions, and tearing
// them down once streamAlpaca returns. Nothing here is orchestration:
// it's config plus the two handlers/work function that are genuinely
// stonks-specific.
func (rt *Runtime) run() {
	if err := rt.Run(rt.Context(), pubsub.ServiceSpec{
		Invocation: rt.invocation,
		Channels: pubsub.ChannelHandlers{
			shutdownControlChannel: rt.handleShutdown,
			addControlChannel:      rt.handleAdd,
		},
		Work: rt.streamAlpaca,
	}); err != nil {
		log.Printf("stonks: %v", err)
	}
}

// streamAlpaca is the Work half of run's ServiceSpec: connect to the
// Alpaca stream and block until ctx is done (a control:shutdown handler
// called Cancel, or the claiming request's own context ended) or the
// connection terminates on its own.
func (rt *Runtime) streamAlpaca(ctx context.Context) error {
	if err := rt.streamClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to Alpaca stream: %w", err)
	}
	log.Printf("stonks: connected to Alpaca stream (instance=%s feed=%s tickers=%v)", rt.InstanceID, rt.cfg.feed, rt.cfg.tickers)

	select {
	case <-ctx.Done():
		return nil
	case err := <-rt.streamClient.Terminated():
		if err != nil {
			return fmt.Errorf("Alpaca stream terminated: %w", err)
		}
		return nil
	}
}

func (rt *Runtime) handleShutdown(payload string) {
	request := pubsub.ParseShutdownRequest(payload)
	log.Printf("stonks: shutting down runtime: reason=%q", request.Reason)
	rt.Cancel()
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

// Package router implements the stonks HTTP entry point: POST /stream
// takes a list of tickers, connects to the Alpaca market-data stream on
// their behalf, and publishes every trade/quote/bar it receives onto a
// deterministic Redis channel (stonks:<type>:<SYMBOL>) instead of
// returning it over the same HTTP connection. Anyone who wants to watch
// the stream connects to logma-serverless, which subscribes to those
// channels and fans them out over SSE.
//
// StonksRuntime embeds pubsub.Runtime, which owns the single
// *redis.Client (used for exactly two things: Publish for outbound
// market data, and Subscribe for its control:shutdown/control:add
// channels, auto-namespaced by pubsub/channels -- "stonks:..." under
// K_SERVICE, or in local dev/tests) and the whole
// claim/register-invocation/subscribe/teardown orchestration.
// StonksRuntime itself only supplies its own state (the Alpaca stream
// client) and the handlers/work function that are genuinely
// stonks-specific.
package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"

	"github.com/xd-dash/logma-serverless/pubsub"
)

// StonksRuntime is a container-global, single-owner actor: it owns one
// Alpaca stream connection, plus (via the embedded pubsub.Runtime) the
// Redis client and claim/lifecycle/control-plane machinery shared with
// every other fixed-channel-set service. Claim() is a defensive guard
// against a second request driving the same runtime concurrently; the
// actual guarantee comes from the Cloud Function's
// maxInstanceRequestConcurrency=1 configuration.
type StonksRuntime struct {
	pubsub.Runtime

	streamClient *stream.StocksClient
	cfg          streamConfig
}

// NewStonksRuntime builds a StonksRuntime wired to REDIS_URI/REDISCLI_AUTH.
// It does not connect to Redis or Alpaca, and has no stream
// configuration, until Configure and Start are called.
func NewStonksRuntime() *StonksRuntime {
	return &StonksRuntime{Runtime: pubsub.NewRuntime(pubsub.NewClientFromEnv())}
}

// Configure wires this StonksRuntime's Alpaca stream client to cfg and
// declares the ServiceSpec the embedded pubsub.Runtime's Start will
// run: which control channels it listens on (control:shutdown gets the
// default parse-log-Cancel handler every such channel wants;
// control:add is genuinely stonks-specific) and streamAlpaca as the
// actual work. It must be called exactly once, after a successful
// Claim and before Start.
func (rt *StonksRuntime) Configure(cfg streamConfig) {
	rt.cfg = cfg
	rt.streamClient = stream.NewStocksClient(cfg.feed, rt.streamOptions()...)

	rt.Runtime.Configure(pubsub.ServiceSpec{
		Channels: pubsub.ChannelHandlers{
			rt.ShutdownChannel(): rt.DefaultShutdownHandler(),
			rt.AddChannel():      rt.handleAdd,
		},
		Work: rt.streamAlpaca,
	})
}

func (rt *StonksRuntime) streamOptions() []stream.StockOption {
	opts := make([]stream.StockOption, 0, len(rt.cfg.subscriptions)+1)
	if key, secret := os.Getenv("ALPACA_API_KEY_ID"), os.Getenv("ALPACA_API_SECRET_KEY"); key != "" && secret != "" {
		opts = append(opts, stream.WithCredentials(key, secret))
	}
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

func (rt *StonksRuntime) onTrade(t stream.Trade)  { rt.publish(subTrades, t.Symbol, t) }
func (rt *StonksRuntime) onQuote(q stream.Quote)  { rt.publish(subQuotes, q.Symbol, q) }
func (rt *StonksRuntime) onBar(b stream.Bar)      { rt.publish(subBars, b.Symbol, b) }
func (rt *StonksRuntime) onDailyBar(b stream.Bar) { rt.publish(subDailyBars, b.Symbol, b) }

func (rt *StonksRuntime) publish(sub subscriptionType, symbol string, event any) {
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

// streamAlpaca is the Work half of Configure's ServiceSpec: connect to
// the Alpaca stream and block until ctx is done (the default
// control:shutdown handler called Cancel, or the claiming request's own
// context ended) or the connection terminates on its own.
func (rt *StonksRuntime) streamAlpaca(ctx context.Context) error {
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

// handleAdd hot-adds tickers to the already-connected Alpaca stream, in
// response to a stonks:control:add publish -- the only way to add
// tickers once POST /stream has claimed the runtime and is blocked in
// Start. It's only ever called sequentially from the single
// control:add subscriber goroutine, so it never calls the SDK's
// subscription-change methods concurrently with itself.
func (rt *StonksRuntime) handleAdd(payload string) {
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

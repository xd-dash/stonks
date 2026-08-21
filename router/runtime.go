// Package router implements the stonks HTTP entry point: POST /stream
// takes a list of tickers, connects to the Alpaca market-data stream on
// their behalf, and publishes every trade/quote/bar it receives onto a
// Redis channel instead of returning it over the same HTTP connection --
// either one channel per (type, symbol) pair (stonks:<type>:<SYMBOL>) or,
// for a type listed in the request's combined_channels, one channel
// shared by every requested ticker (stonks:<type>:combined:<SYMBOL1>:
// <SYMBOL2>:...). Either way the channel is scoped to the specific
// container instance producing it (a ":<instanceID>" suffix, via the
// same InstanceChannel every control channel already uses), since more
// than one container can be streaming the same ticker/type at once.
// Anyone who wants to watch the stream connects to logma-serverless,
// which subscribes to those channels and fans them out over SSE.
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
	credentials  *alpacaCredentials
	alpacaSecret string
}

// NewStonksRuntime builds a StonksRuntime wired to REDIS_URI/REDISCLI_AUTH
// and creds -- the container's lazily-loaded Alpaca secret cache. It
// does not connect to Redis or Alpaca, and has no stream configuration,
// until Configure and Start are called.
func NewStonksRuntime(creds *alpacaCredentials) *StonksRuntime {
	return &StonksRuntime{
		Runtime:     pubsub.NewRuntimeFromEnv(),
		credentials: creds,
	}
}

// Configure wires this StonksRuntime's Alpaca stream client to cfg and
// alpacaSecret (resolved by requireAlpacaAuth from the request's headers
// or this container's cache), and declares, via ConfigureDefault, the
// ServiceSpec the embedded pubsub.Runtime's Start will run: control:add
// (the only channel genuinely stonks-specific -- control:shutdown's
// handling is the embedded Runtime's default) and streamAlpaca as the
// actual work. It must be called exactly once, after a successful Claim
// and before Start.
func (rt *StonksRuntime) Configure(cfg streamConfig, alpacaSecret string) {
	rt.cfg = cfg
	rt.alpacaSecret = alpacaSecret
	rt.streamClient = stream.NewStocksClient(cfg.feed, rt.streamOptions()...)

	rt.Runtime.ConfigureDefault(rt.streamAlpaca, pubsub.ChannelHandlers{
		rt.AddChannel(): rt.handleAdd,
	})
}

func (rt *StonksRuntime) streamOptions() []stream.StockOption {
	opts := make([]stream.StockOption, 0, len(rt.cfg.subscriptions)+1)
	if key := os.Getenv("ALPACA_API_KEY_ID"); key != "" && rt.alpacaSecret != "" {
		opts = append(opts, stream.WithCredentials(key, rt.alpacaSecret))
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
	channel := rt.InstanceChannel(rt.baseChannel(sub, symbol))
	if err := rt.Runtime.Publish(channel, event); err != nil {
		log.Printf("stonks: %v", err)
	}
}

// baseChannel picks channelFor (per-symbol) or combinedChannelFor
// (shared across every ticker in rt.cfg.tickers), per
// rt.cfg.combined[sub]. rt.cfg.tickers is fixed at Configure time and
// never mutated by handleAdd, so a combined channel's name stays fixed
// to the original request's ticker set even after tickers are hot-added
// via control:add -- the hot-added tickers' events still land on that
// same channel, they just aren't reflected in its name.
func (rt *StonksRuntime) baseChannel(sub subscriptionType, symbol string) string {
	if rt.cfg.combined[sub] {
		return combinedChannelFor(sub, rt.cfg.tickers)
	}
	return channelFor(sub, symbol)
}

// streamAlpaca is the Work half of Configure's ServiceSpec: connect to
// the Alpaca stream and block until ctx is done (the default
// control:shutdown handler called Cancel, or the claiming request's own
// context ended) or the connection terminates on its own.
func (rt *StonksRuntime) streamAlpaca(ctx context.Context) error {
	if err := rt.streamClient.Connect(ctx); err != nil {
		rt.credentials.clear()
		return fmt.Errorf("failed to connect to Alpaca stream: %w", err)
	}
	log.Printf("stonks: connected to Alpaca stream (instance=%s feed=%s tickers=%v)", rt.InstanceID, rt.cfg.feed, rt.cfg.tickers)

	select {
	case <-ctx.Done():
		return nil
	case err := <-rt.streamClient.Terminated():
		if err != nil {
			rt.credentials.clear()
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

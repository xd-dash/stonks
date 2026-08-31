// Package router implements the stonks HTTP/SSE entry point.
package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/xd-dash/logma/serverless/pubsub"
)

// StonksRuntime owns one Alpaca stream plus Logma serverless's Redis,
// lifecycle, control-plane, and publication runtime for one HTTP request.
type StonksRuntime struct {
	pubsub.Runtime

	streamClient   *stream.StocksClient
	cfg            streamConfig
	credentials    *alpacaCredentials
	alpacaSecret   string
	globalChannels bool
}

func NewStonksRuntime(creds *alpacaCredentials) *StonksRuntime {
	return &StonksRuntime{
		Runtime:        pubsub.NewRuntimeFromEnv(),
		credentials:    creds,
		globalChannels: os.Getenv("STONKS_GLOBAL_CHANNELS") == "true",
	}
}

// Configure fixes the request's publication channels before either the SSE
// relay or Alpaca publisher starts. The handler subscribes to streamChannels
// first; Start then activates this publisher and its control subscriptions.
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
	if err := rt.Runtime.Publish(rt.publishChannel(sub, symbol), event); err != nil {
		log.Printf("stonks: %v", err)
	}
}

func (rt *StonksRuntime) publishChannel(sub subscriptionType, symbol string) string {
	base := rt.baseChannel(sub, symbol)
	if rt.globalChannels {
		return rt.GlobalChannel(base)
	}
	return rt.InstanceChannel(base)
}

func (rt *StonksRuntime) baseChannel(sub subscriptionType, symbol string) string {
	if rt.cfg.combined[sub] {
		return combinedChannelFor(sub, rt.cfg.tickers)
	}
	return channelFor(sub, symbol)
}

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

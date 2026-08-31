// Package router implements the stonks HTTP/SSE entry point.
package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
	"github.com/xd-dash/logma/serverless/pubsub"
)

// StonksRuntime owns the retained/shared Alpaca publisher for one stonks
// service instance. HTTP /stream requests never own this runtime: they only
// attach request-scoped Logma subscribers to the Redis channels it publishes.
type StonksRuntime struct {
	pubsub.Runtime

	streamClient   *stream.StocksClient
	credentials    *alpacaCredentials
	alpacaSecret   string
	globalChannels bool

	mu           sync.Mutex
	tickers      map[string]struct{}
	subscriptions map[subscriptionType]map[string]struct{}
	connected    chan struct{}
	connectedOnce sync.Once
}

func NewStonksRuntime(creds *alpacaCredentials) *StonksRuntime {
	return &StonksRuntime{
		Runtime:        pubsub.NewRuntimeFromEnv(),
		credentials:    creds,
		globalChannels: os.Getenv("STONKS_GLOBAL_CHANNELS") == "true",
		tickers:        make(map[string]struct{}),
		subscriptions:  make(map[subscriptionType]map[string]struct{}),
		connected:      make(chan struct{}),
	}
}

// Configure initializes the shared publisher from the first request that needs
// market data. Shared publishers always use canonical per-symbol Redis channels;
// request-specific fan-in/selection belongs at the SSE subscriber boundary.
func (rt *StonksRuntime) Configure(cfg streamConfig, alpacaSecret string) {
	rt.alpacaSecret = alpacaSecret
	cfg.combined = nil

	rt.mu.Lock()
	for _, ticker := range cfg.tickers {
		rt.tickers[ticker] = struct{}{}
	}
	for _, sub := range cfg.subscriptions {
		if rt.subscriptions[sub] == nil {
			rt.subscriptions[sub] = make(map[string]struct{})
		}
		for _, ticker := range cfg.tickers {
			rt.subscriptions[sub][ticker] = struct{}{}
		}
	}
	rt.mu.Unlock()

	rt.streamClient = stream.NewStocksClient(cfg.feed, rt.streamOptions(cfg)...)
	rt.Runtime.ConfigureDefault(rt.streamAlpaca, pubsub.ChannelHandlers{
		rt.AddChannel(): rt.handleAdd,
	})
}

func (rt *StonksRuntime) streamOptions(cfg streamConfig) []stream.StockOption {
	opts := make([]stream.StockOption, 0, len(cfg.subscriptions)+1)
	if key := os.Getenv("ALPACA_API_KEY_ID"); key != "" && rt.alpacaSecret != "" {
		opts = append(opts, stream.WithCredentials(key, rt.alpacaSecret))
	}
	for _, sub := range cfg.subscriptions {
		switch sub {
		case subTrades:
			opts = append(opts, stream.WithTrades(rt.onTrade, cfg.tickers...))
		case subQuotes:
			opts = append(opts, stream.WithQuotes(rt.onQuote, cfg.tickers...))
		case subBars:
			opts = append(opts, stream.WithBars(rt.onBar, cfg.tickers...))
		case subDailyBars:
			opts = append(opts, stream.WithDailyBars(rt.onDailyBar, cfg.tickers...))
		}
	}
	return opts
}

func (rt *StonksRuntime) Ready() <-chan struct{} { return rt.connected }

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
	base := channelFor(sub, symbol)
	if rt.globalChannels {
		return rt.GlobalChannel(base)
	}
	return rt.InstanceChannel(base)
}

// streamChannels returns the canonical Redis channels a request should attach
// to. CombinedChannels is intentionally not a publisher concern in the shared
// runtime: different requesters can select overlapping symbol/type sets without
// changing the one retained publisher's channel topology.
func (rt *StonksRuntime) streamChannels(cfg streamConfig) []string {
	seen := make(map[string]struct{})
	channels := make([]string, 0, len(cfg.tickers)*len(cfg.subscriptions))
	for _, sub := range cfg.subscriptions {
		for _, ticker := range cfg.tickers {
			channel := rt.publishChannel(sub, ticker)
			if _, ok := seen[channel]; ok {
				continue
			}
			seen[channel] = struct{}{}
			channels = append(channels, channel)
		}
	}
	return channels
}

func (rt *StonksRuntime) streamAlpaca(ctx context.Context) error {
	if err := rt.streamClient.Connect(ctx); err != nil {
		rt.credentials.clear()
		return fmt.Errorf("failed to connect to Alpaca stream: %w", err)
	}
	rt.connectedOnce.Do(func() { close(rt.connected) })
	log.Printf("stonks: shared Alpaca publisher connected (instance=%s)", rt.InstanceID)

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

// EnsureSubscriptions expands the retained Alpaca stream for a requester.
// Calls are serialized so the SDK's subscription mutation methods are never
// invoked concurrently. Existing subscriptions are left untouched.
func (rt *StonksRuntime) EnsureSubscriptions(cfg streamConfig) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	for _, sub := range cfg.subscriptions {
		if rt.subscriptions[sub] == nil {
			rt.subscriptions[sub] = make(map[string]struct{})
		}
		missing := make([]string, 0, len(cfg.tickers))
		for _, ticker := range cfg.tickers {
			if _, exists := rt.subscriptions[sub][ticker]; exists {
				continue
			}
			missing = append(missing, ticker)
		}
		if len(missing) == 0 {
			continue
		}

		var err error
		switch sub {
		case subTrades:
			err = rt.streamClient.SubscribeToTrades(rt.onTrade, missing...)
		case subQuotes:
			err = rt.streamClient.SubscribeToQuotes(rt.onQuote, missing...)
		case subBars:
			err = rt.streamClient.SubscribeToBars(rt.onBar, missing...)
		case subDailyBars:
			err = rt.streamClient.SubscribeToDailyBars(rt.onDailyBar, missing...)
		}
		if err != nil {
			return fmt.Errorf("subscribe %s for %v: %w", sub, missing, err)
		}
		for _, ticker := range missing {
			rt.tickers[ticker] = struct{}{}
			rt.subscriptions[sub][ticker] = struct{}{}
		}
	}
	return nil
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
	if err := rt.EnsureSubscriptions(streamConfig{tickers: cfg.tickers, subscriptions: cfg.subscriptions}); err != nil {
		log.Printf("stonks: failed to add tickers: %v", err)
		return
	}
	log.Printf("stonks: added tickers %v (%v)", cfg.tickers, cfg.subscriptions)
}

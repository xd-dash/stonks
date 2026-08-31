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
// service instance. Stock and option feeds are separate Alpaca websocket
// clients but one process/runtime owns both and publishes them into the same
// scoped Redis/Logma fabric.
type StonksRuntime struct {
	pubsub.Runtime

	streamClient   *stream.StocksClient
	optionClient   *stream.OptionClient
	credentials    *alpacaCredentials
	alpacaSecret   string
	globalChannels bool
	replayFixture  string

	mu                  sync.Mutex
	tickers             map[string]struct{}
	subscriptions       map[subscriptionType]map[string]struct{}
	optionContracts     map[string]struct{}
	optionSubscriptions map[subscriptionType]map[string]struct{}
	connected           chan struct{}
	connectedOnce       sync.Once
}

func NewStonksRuntime(creds *alpacaCredentials) *StonksRuntime {
	return &StonksRuntime{
		Runtime:              pubsub.NewRuntimeFromEnv(),
		credentials:          creds,
		globalChannels:       os.Getenv("STONKS_GLOBAL_CHANNELS") == "true",
		replayFixture:        replayFixturePath(),
		tickers:              make(map[string]struct{}),
		subscriptions:        make(map[subscriptionType]map[string]struct{}),
		optionContracts:      make(map[string]struct{}),
		optionSubscriptions: make(map[subscriptionType]map[string]struct{}),
		connected:            make(chan struct{}),
	}
}

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
	for _, contract := range cfg.optionContracts {
		rt.optionContracts[contract] = struct{}{}
	}
	for _, sub := range cfg.optionSubscriptions {
		if rt.optionSubscriptions[sub] == nil {
			rt.optionSubscriptions[sub] = make(map[string]struct{})
		}
		for _, contract := range cfg.optionContracts {
			rt.optionSubscriptions[sub][contract] = struct{}{}
		}
	}
	rt.mu.Unlock()

	streamFn := rt.streamAlpaca
	if rt.replayFixture != "" {
		streamFn = rt.streamReplay
	} else {
		rt.streamClient = stream.NewStocksClient(cfg.feed, rt.streamOptions(cfg)...)
		if len(cfg.optionContracts) > 0 {
			rt.optionClient = stream.NewOptionClient(cfg.optionFeed, rt.optionOptions(cfg)...)
		}
	}
	rt.Runtime.ConfigureDefault(streamFn, pubsub.ChannelHandlers{rt.AddChannel(): rt.handleAdd})
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

func (rt *StonksRuntime) optionOptions(cfg streamConfig) []stream.OptionOption {
	opts := make([]stream.OptionOption, 0, len(cfg.optionSubscriptions)+1)
	if key := os.Getenv("ALPACA_API_KEY_ID"); key != "" && rt.alpacaSecret != "" {
		opts = append(opts, stream.WithCredentials(key, rt.alpacaSecret))
	}
	for _, sub := range cfg.optionSubscriptions {
		switch sub {
		case subQuotes:
			opts = append(opts, stream.WithOptionQuotes(rt.onOptionQuote, cfg.optionContracts...))
		case subTrades:
			opts = append(opts, stream.WithOptionTrades(rt.onOptionTrade, cfg.optionContracts...))
		}
	}
	return opts
}

func (rt *StonksRuntime) Ready() <-chan struct{} { return rt.connected }

func (rt *StonksRuntime) onTrade(t stream.Trade)  { rt.publish(subTrades, t.Symbol, t) }
func (rt *StonksRuntime) onQuote(q stream.Quote)  { rt.publish(subQuotes, q.Symbol, q) }
func (rt *StonksRuntime) onBar(b stream.Bar)      { rt.publish(subBars, b.Symbol, b) }
func (rt *StonksRuntime) onDailyBar(b stream.Bar) { rt.publish(subDailyBars, b.Symbol, b) }
func (rt *StonksRuntime) onOptionQuote(q stream.OptionQuote) {
	rt.publishOption(subQuotes, q.Symbol, q)
}
func (rt *StonksRuntime) onOptionTrade(t stream.OptionTrade) {
	rt.publishOption(subTrades, t.Symbol, t)
}

func (rt *StonksRuntime) publish(sub subscriptionType, symbol string, event any) {
	if err := rt.Runtime.Publish(rt.publishChannel(sub, symbol), event); err != nil {
		log.Printf("stonks: %v", err)
	}
}

func (rt *StonksRuntime) publishOption(sub subscriptionType, contract string, event any) {
	if err := rt.Runtime.Publish(rt.optionPublishChannel(sub, contract), event); err != nil {
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

func (rt *StonksRuntime) optionPublishChannel(sub subscriptionType, contract string) string {
	base := optionChannelFor(sub, contract)
	if rt.globalChannels {
		return rt.GlobalChannel(base)
	}
	return rt.InstanceChannel(base)
}

func (rt *StonksRuntime) streamChannels(cfg streamConfig) []string {
	seen := make(map[string]struct{})
	channels := make([]string, 0, len(cfg.tickers)*len(cfg.subscriptions)+len(cfg.optionContracts)*len(cfg.optionSubscriptions))
	appendUnique := func(channel string) {
		if _, ok := seen[channel]; ok {
			return
		}
		seen[channel] = struct{}{}
		channels = append(channels, channel)
	}
	for _, sub := range cfg.subscriptions {
		for _, ticker := range cfg.tickers {
			appendUnique(rt.publishChannel(sub, ticker))
		}
	}
	for _, sub := range cfg.optionSubscriptions {
		for _, contract := range cfg.optionContracts {
			appendUnique(rt.optionPublishChannel(sub, contract))
		}
	}
	return channels
}

func (rt *StonksRuntime) streamAlpaca(ctx context.Context) error {
	if err := rt.streamClient.Connect(ctx); err != nil {
		rt.credentials.clear()
		return fmt.Errorf("failed to connect to Alpaca stock stream: %w", err)
	}
	if rt.optionClient != nil {
		if err := rt.optionClient.Connect(ctx); err != nil {
			rt.credentials.clear()
			return fmt.Errorf("failed to connect to Alpaca option stream: %w", err)
		}
	}
	rt.connectedOnce.Do(func() { close(rt.connected) })
	log.Printf("stonks: shared Alpaca publisher connected (instance=%s options=%t)", rt.InstanceID, rt.optionClient != nil)

	var optionTerminated <-chan error
	if rt.optionClient != nil {
		optionTerminated = rt.optionClient.Terminated()
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-rt.streamClient.Terminated():
		if err != nil {
			rt.credentials.clear()
			return fmt.Errorf("Alpaca stock stream terminated: %w", err)
		}
		return nil
	case err := <-optionTerminated:
		if err != nil {
			rt.credentials.clear()
			return fmt.Errorf("Alpaca option stream terminated: %w", err)
		}
		return nil
	}
}

func (rt *StonksRuntime) EnsureSubscriptions(cfg streamConfig) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	for _, sub := range cfg.subscriptions {
		if rt.subscriptions[sub] == nil {
			rt.subscriptions[sub] = make(map[string]struct{})
		}
		missing := make([]string, 0, len(cfg.tickers))
		for _, ticker := range cfg.tickers {
			if _, exists := rt.subscriptions[sub][ticker]; !exists {
				missing = append(missing, ticker)
			}
		}
		if len(missing) == 0 {
			continue
		}
		if rt.replayFixture == "" {
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
		}
		for _, ticker := range missing {
			rt.tickers[ticker] = struct{}{}
			rt.subscriptions[sub][ticker] = struct{}{}
		}
	}

	if len(cfg.optionContracts) > 0 && rt.optionClient == nil && rt.replayFixture == "" {
		return fmt.Errorf("shared publisher was created without option streaming; restart with option_contracts in the first request")
	}
	for _, sub := range cfg.optionSubscriptions {
		if rt.optionSubscriptions[sub] == nil {
			rt.optionSubscriptions[sub] = make(map[string]struct{})
		}
		missing := make([]string, 0, len(cfg.optionContracts))
		for _, contract := range cfg.optionContracts {
			if _, exists := rt.optionSubscriptions[sub][contract]; !exists {
				missing = append(missing, contract)
			}
		}
		if len(missing) == 0 {
			continue
		}
		if rt.replayFixture == "" {
			var err error
			switch sub {
			case subQuotes:
				err = rt.optionClient.SubscribeToQuotes(rt.onOptionQuote, missing...)
			case subTrades:
				err = rt.optionClient.SubscribeToTrades(rt.onOptionTrade, missing...)
			}
			if err != nil {
				return fmt.Errorf("subscribe option %s for %v: %w", sub, missing, err)
			}
		}
		for _, contract := range missing {
			rt.optionContracts[contract] = struct{}{}
			rt.optionSubscriptions[sub][contract] = struct{}{}
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

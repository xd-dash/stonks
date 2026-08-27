// Package router implements the stonks HTTP entry point. A stream session may
// own both Alpaca's stock websocket and its separate options websocket; all
// outbound events are published to Redis and consumed through logma-serverless.
package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"

	"github.com/xd-dash/logma-serverless/pubsub"
)

type StonksRuntime struct {
	pubsub.Runtime

	streamClient       *stream.StocksClient
	optionStreamClient *stream.OptionClient
	cfg                streamConfig
	credentials        *alpacaCredentials
	alpacaSecret       string
	globalChannels     bool
	closeOnce          sync.Once
}

func NewStonksRuntime(creds *alpacaCredentials) *StonksRuntime {
	return &StonksRuntime{
		Runtime:        pubsub.NewRuntimeFromEnv(),
		credentials:    creds,
		globalChannels: os.Getenv("STONKS_GLOBAL_CHANNELS") == "true",
	}
}

// Close releases resources owned by this stream session. It is idempotent.
// streamAlpaca does not return until every connected Alpaca client's
// Terminated channel has closed, so callback processors are joined before the
// Redis client is closed here.
func (rt *StonksRuntime) Close() {
	rt.closeOnce.Do(func() {
		if rt.Client == nil {
			return
		}
		if err := rt.Client.Close(); err != nil {
			log.Printf("stonks: close redis client: %v", err)
		}
	})
}

func (rt *StonksRuntime) Configure(cfg streamConfig, alpacaSecret string) {
	rt.cfg = cfg
	rt.alpacaSecret = alpacaSecret
	rt.streamClient = stream.NewStocksClient(cfg.feed, rt.streamOptions()...)
	if len(cfg.optionContracts) != 0 {
		rt.optionStreamClient = stream.NewOptionClient(cfg.optionFeed, rt.optionStreamOptions()...)
	}

	rt.Runtime.ConfigureDefault(rt.streamAlpaca, pubsub.ChannelHandlers{
		rt.AddChannel(): rt.handleAdd,
	})
}

func (rt *StonksRuntime) credentialsOption() stream.Option {
	return stream.WithCredentials(os.Getenv("ALPACA_API_KEY_ID"), rt.alpacaSecret)
}

func (rt *StonksRuntime) streamOptions() []stream.StockOption {
	opts := make([]stream.StockOption, 0, len(rt.cfg.subscriptions)+1)
	if key := os.Getenv("ALPACA_API_KEY_ID"); key != "" && rt.alpacaSecret != "" {
		opts = append(opts, rt.credentialsOption())
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

func (rt *StonksRuntime) optionStreamOptions() []stream.OptionOption {
	opts := make([]stream.OptionOption, 0, len(rt.cfg.optionSubscriptions)+1)
	if key := os.Getenv("ALPACA_API_KEY_ID"); key != "" && rt.alpacaSecret != "" {
		opts = append(opts, rt.credentialsOption())
	}
	for _, sub := range rt.cfg.optionSubscriptions {
		switch sub {
		case optionSubTrades:
			opts = append(opts, stream.WithOptionTrades(rt.onOptionTrade, rt.cfg.optionContracts...))
		case optionSubQuotes:
			opts = append(opts, stream.WithOptionQuotes(rt.onOptionQuote, rt.cfg.optionContracts...))
		}
	}
	return opts
}

func (rt *StonksRuntime) onTrade(t stream.Trade)  { rt.publish(subTrades, t.Symbol, t) }
func (rt *StonksRuntime) onQuote(q stream.Quote)  { rt.publish(subQuotes, q.Symbol, q) }
func (rt *StonksRuntime) onBar(b stream.Bar)      { rt.publish(subBars, b.Symbol, b) }
func (rt *StonksRuntime) onDailyBar(b stream.Bar) { rt.publish(subDailyBars, b.Symbol, b) }

func (rt *StonksRuntime) onOptionTrade(t stream.OptionTrade) {
	rt.publishOption(optionSubTrades, t.Symbol, t)
}

func (rt *StonksRuntime) onOptionQuote(q stream.OptionQuote) {
	rt.publishOption(optionSubQuotes, q.Symbol, q)
}

func (rt *StonksRuntime) publish(sub subscriptionType, symbol string, event any) {
	channel := rt.publishChannel(sub, symbol)
	if err := rt.Runtime.Publish(channel, event); err != nil {
		log.Printf("stonks: %v", err)
	}
}

func (rt *StonksRuntime) publishOption(sub optionSubscriptionType, contract string, event any) {
	channel := optionChannelFor(sub, contract)
	if rt.globalChannels {
		channel = rt.GlobalChannel(channel)
	} else {
		channel = rt.InstanceChannel(channel)
	}
	if err := rt.Runtime.Publish(channel, event); err != nil {
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

// waitForAlpacaTermination drains terminated until the SDK closes it and
// returns the first non-nil terminal error. Channel closure is the teardown
// barrier after Alpaca's message processors have joined.
func waitForAlpacaTermination(terminated <-chan error) error {
	var terminalErr error
	for err := range terminated {
		if terminalErr == nil && err != nil {
			terminalErr = err
		}
	}
	return terminalErr
}

type alpacaTermination struct {
	name string
	err  error
}

// waitForConnectedStreams cancels all sibling streams when either the request
// ends or one Alpaca stream terminates, then waits for every Terminated channel
// to close before returning.
func waitForConnectedStreams(ctx context.Context, cancel context.CancelFunc, streams map[string]<-chan error) error {
	results := make(chan alpacaTermination, len(streams))
	for name, terminated := range streams {
		go func(name string, terminated <-chan error) {
			results <- alpacaTermination{name: name, err: waitForAlpacaTermination(terminated)}
		}(name, terminated)
	}

	var first alpacaTermination
	requestCancelled := false
	select {
	case <-ctx.Done():
		requestCancelled = true
		cancel()
	case first = <-results:
		cancel()
	}

	remaining := len(streams)
	if !requestCancelled {
		remaining--
	}
	for i := 0; i < remaining; i++ {
		result := <-results
		if first.err == nil && result.err != nil {
			first = result
		}
	}

	if requestCancelled {
		return nil
	}
	if first.err != nil {
		return fmt.Errorf("Alpaca %s stream terminated: %w", first.name, first.err)
	}
	return nil
}

func (rt *StonksRuntime) streamAlpaca(ctx context.Context) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := rt.streamClient.Connect(streamCtx); err != nil {
		if ctx.Err() == nil {
			rt.credentials.clear()
		}
		return fmt.Errorf("failed to connect to Alpaca stock stream: %w", err)
	}
	log.Printf("stonks: connected to Alpaca stock stream (instance=%s feed=%s tickers=%v)", rt.InstanceID, rt.cfg.feed, rt.cfg.tickers)

	streams := map[string]<-chan error{
		"stock": rt.streamClient.Terminated(),
	}

	if rt.optionStreamClient != nil {
		if err := rt.optionStreamClient.Connect(streamCtx); err != nil {
			// The stock stream authenticated successfully. An option connection
			// failure can instead mean feed entitlement or contract validation;
			// do not invalidate the credential cache.
			cancel()
			_ = waitForAlpacaTermination(rt.streamClient.Terminated())
			return fmt.Errorf("failed to connect to Alpaca option stream: %w", err)
		}
		log.Printf("stonks: connected to Alpaca option stream (instance=%s feed=%s contracts=%v)", rt.InstanceID, rt.cfg.optionFeed, rt.cfg.optionContracts)
		streams["option"] = rt.optionStreamClient.Terminated()
	}

	return waitForConnectedStreams(ctx, cancel, streams)
}

// handleAdd continues to hot-add stock tickers. Option contracts are fixed for
// the session for now, which keeps the two Alpaca clients' subscription-change
// lifecycles independent and avoids concurrent SDK mutation.
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

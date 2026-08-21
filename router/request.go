package router

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

// subscriptionType is one of the market-data event types stonks can
// stream from Alpaca and publish to Redis.
type subscriptionType string

const (
	subTrades    subscriptionType = "trades"
	subQuotes    subscriptionType = "quotes"
	subBars      subscriptionType = "bars"
	subDailyBars subscriptionType = "dailybars"
)

// defaultSubscriptions is used when a request omits Subscriptions
// entirely. bars is the only one of the four types that's actually
// interval-based (a one-minute aggregated OHLCV candle per symbol) --
// trades/quotes/dailybars are event-driven (every print, every NBBO
// change, once a day) or a different interval, so they're opt-in.
var defaultSubscriptions = []subscriptionType{subBars}

// channelType is the singular token used in the Redis channel name for a
// subscriptionType, e.g. "trades" (request) -> "trade" (channel).
func (t subscriptionType) channelType() string {
	switch t {
	case subTrades:
		return "trade"
	case subQuotes:
		return "quote"
	case subBars:
		return "bar"
	case subDailyBars:
		return "dailybar"
	default:
		return string(t)
	}
}

// channelFor derives the base (pre-instance-scoping) Redis channel a
// per-symbol (subscriptionType, symbol) pair publishes to. It's derived
// purely from its inputs so a consumer can compute it ahead of time
// without waiting on a response from stonks; StonksRuntime.baseChannel
// scopes the result to the producing container via InstanceChannel.
func channelFor(t subscriptionType, symbol string) string {
	return fmt.Sprintf("stonks:%s:%s", t.channelType(), symbol)
}

// combinedChannelFor derives the base (pre-instance-scoping) channel a
// combined subscription type publishes to: every ticker in tickers,
// sorted for a deterministic name regardless of the request's own
// ticker order, e.g. combinedChannelFor(subQuotes, []string{"MSFT",
// "AAPL"}) == "stonks:quote:combined:AAPL:MSFT".
func combinedChannelFor(t subscriptionType, tickers []string) string {
	sorted := append([]string(nil), tickers...)
	sort.Strings(sorted)
	return fmt.Sprintf("stonks:%s:combined:%s", t.channelType(), strings.Join(sorted, ":"))
}

// StreamRequest is the POST /stream body: which tickers to stream, from
// which feed, which event types to subscribe to, and which of those
// types (if any) should feed one shared channel across every ticker
// instead of each ticker getting its own -- see CombinedChannels. Alpaca
// credentials are never part of this body -- they come from the
// X-Alpaca-Api-Key-Id/X-Alpaca-Api-Secret-Key request headers, checked
// and resolved by requireAlpacaAuth. The API key is also the
// ALPACA_API_KEY_ID env var (the header must match it); the secret key
// is never an env var at all -- see requireAlpacaAuth and
// alpacaCredentials in alpaca_auth.go.
type StreamRequest struct {
	Tickers          []string `json:"tickers"`
	Feed             string   `json:"feed"`
	Subscriptions    []string `json:"subscriptions"`
	CombinedChannels []string `json:"combined_channels"`
}

// streamConfig is the validated, normalized form of a StreamRequest.
type streamConfig struct {
	tickers       []string
	feed          marketdata.Feed
	subscriptions []subscriptionType
	combined      map[subscriptionType]bool
}

// validate normalizes and checks the request, returning the config a
// StonksRuntime needs to build its Alpaca subscriptions.
func (req StreamRequest) validate() (streamConfig, error) {
	tickers, err := validateTickers(req.Tickers)
	if err != nil {
		return streamConfig{}, err
	}

	feed, err := validateFeed(req.Feed)
	if err != nil {
		return streamConfig{}, err
	}

	subs, err := validateSubscriptions(req.Subscriptions)
	if err != nil {
		return streamConfig{}, err
	}

	combined, err := validateCombinedChannels(req.CombinedChannels, subs)
	if err != nil {
		return streamConfig{}, err
	}

	return streamConfig{tickers: tickers, feed: feed, subscriptions: subs, combined: combined}, nil
}

// AddTickersRequest is the payload published to stonks:control:add to
// hot-add tickers to an already-running stream. There's no feed field --
// Alpaca's feed is fixed for the lifetime of the stream's connection.
type AddTickersRequest struct {
	Tickers       []string `json:"tickers"`
	Subscriptions []string `json:"subscriptions"`
}

// addConfig is the validated, normalized form of an AddTickersRequest.
type addConfig struct {
	tickers       []string
	subscriptions []subscriptionType
}

func (req AddTickersRequest) validate() (addConfig, error) {
	tickers, err := validateTickers(req.Tickers)
	if err != nil {
		return addConfig{}, err
	}

	subs, err := validateSubscriptions(req.Subscriptions)
	if err != nil {
		return addConfig{}, err
	}

	return addConfig{tickers: tickers, subscriptions: subs}, nil
}

func validateTickers(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("tickers must be non-empty")
	}
	tickers := make([]string, 0, len(raw))
	for _, t := range raw {
		t = strings.ToUpper(strings.TrimSpace(t))
		if t == "" {
			return nil, errors.New("tickers must not contain empty values")
		}
		tickers = append(tickers, t)
	}
	return tickers, nil
}

func validateFeed(raw string) (marketdata.Feed, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "iex":
		return marketdata.IEX, nil
	case "sip":
		return marketdata.SIP, nil
	default:
		return "", fmt.Errorf("unsupported feed %q: must be \"iex\" or \"sip\"", raw)
	}
}

func validateSubscriptions(raw []string) ([]subscriptionType, error) {
	if len(raw) == 0 {
		return defaultSubscriptions, nil
	}
	subs := make([]subscriptionType, 0, len(raw))
	for _, s := range raw {
		st := subscriptionType(strings.ToLower(strings.TrimSpace(s)))
		switch st {
		case subTrades, subQuotes, subBars, subDailyBars:
			subs = append(subs, st)
		default:
			return nil, fmt.Errorf("unsupported subscription %q", s)
		}
	}
	return subs, nil
}

// validateCombinedChannels checks raw (StreamRequest.CombinedChannels)
// against subs (the request's own already-resolved subscription types):
// each entry must be a recognized subscription type and must also be
// one of subs -- combining a type the request never subscribed to is a
// validation error, not a silent no-op. An empty/omitted raw returns a
// nil map, meaning every type stays per-symbol.
func validateCombinedChannels(raw []string, subs []subscriptionType) (map[subscriptionType]bool, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	allowed := make(map[subscriptionType]bool, len(subs))
	for _, s := range subs {
		allowed[s] = true
	}
	combined := make(map[subscriptionType]bool, len(raw))
	for _, r := range raw {
		st := subscriptionType(strings.ToLower(strings.TrimSpace(r)))
		switch st {
		case subTrades, subQuotes, subBars, subDailyBars:
		default:
			return nil, fmt.Errorf("unsupported combined_channels entry %q", r)
		}
		if !allowed[st] {
			return nil, fmt.Errorf("combined_channels entry %q is not in subscriptions", r)
		}
		combined[st] = true
	}
	return combined, nil
}

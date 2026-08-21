package router

import (
	"errors"
	"fmt"
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

var defaultSubscriptions = []subscriptionType{subTrades, subQuotes, subBars}

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

// channelFor derives the deterministic Redis channel a (subscriptionType,
// symbol) pair publishes to. It's derived purely from its inputs so a
// consumer can compute it ahead of time without waiting on a response
// from stonks.
func channelFor(t subscriptionType, symbol string) string {
	return fmt.Sprintf("stonks:%s:%s", t.channelType(), symbol)
}

// StreamRequest is the POST /stream body: which tickers to stream, from
// which feed, and which event types to subscribe to. Alpaca credentials
// are never part of this body -- they come from the server's
// ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY environment, passed explicitly
// to the SDK via stream.WithCredentials.
type StreamRequest struct {
	Tickers       []string `json:"tickers"`
	Feed          string   `json:"feed"`
	Subscriptions []string `json:"subscriptions"`
}

// streamConfig is the validated, normalized form of a StreamRequest.
type streamConfig struct {
	tickers       []string
	feed          marketdata.Feed
	subscriptions []subscriptionType
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

	return streamConfig{tickers: tickers, feed: feed, subscriptions: subs}, nil
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

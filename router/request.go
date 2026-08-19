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
// APCA_API_KEY_ID/APCA_API_SECRET_KEY environment, read automatically by
// the SDK.
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
// Runtime needs to build its Alpaca subscriptions.
func (req StreamRequest) validate() (streamConfig, error) {
	if len(req.Tickers) == 0 {
		return streamConfig{}, errors.New("tickers must be non-empty")
	}
	tickers := make([]string, 0, len(req.Tickers))
	for _, t := range req.Tickers {
		t = strings.ToUpper(strings.TrimSpace(t))
		if t == "" {
			return streamConfig{}, errors.New("tickers must not contain empty values")
		}
		tickers = append(tickers, t)
	}

	var feed marketdata.Feed
	switch strings.ToLower(strings.TrimSpace(req.Feed)) {
	case "", "iex":
		feed = marketdata.IEX
	case "sip":
		feed = marketdata.SIP
	default:
		return streamConfig{}, fmt.Errorf("unsupported feed %q: must be \"iex\" or \"sip\"", req.Feed)
	}

	subs := defaultSubscriptions
	if len(req.Subscriptions) > 0 {
		subs = make([]subscriptionType, 0, len(req.Subscriptions))
		for _, s := range req.Subscriptions {
			st := subscriptionType(strings.ToLower(strings.TrimSpace(s)))
			switch st {
			case subTrades, subQuotes, subBars, subDailyBars:
				subs = append(subs, st)
			default:
				return streamConfig{}, fmt.Errorf("unsupported subscription %q", s)
			}
		}
	}

	return streamConfig{tickers: tickers, feed: feed, subscriptions: subs}, nil
}

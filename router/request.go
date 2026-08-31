package router

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

type subscriptionType string

const (
	subTrades    subscriptionType = "trades"
	subQuotes    subscriptionType = "quotes"
	subBars      subscriptionType = "bars"
	subDailyBars subscriptionType = "dailybars"
)

var defaultSubscriptions = []subscriptionType{subBars}
var defaultOptionSubscriptions = []subscriptionType{subQuotes, subTrades}

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

func channelFor(t subscriptionType, symbol string) string {
	return fmt.Sprintf("stonks:%s:%s", t.channelType(), symbol)
}

func optionChannelFor(t subscriptionType, contract string) string {
	return fmt.Sprintf("stonks:option:%s:%s", t.channelType(), contract)
}

func combinedChannelFor(t subscriptionType, tickers []string) string {
	sorted := append([]string(nil), tickers...)
	sort.Strings(sorted)
	return fmt.Sprintf("stonks:%s:combined:%s", t.channelType(), strings.Join(sorted, ":"))
}

// StreamRequest describes one request-scoped SSE view over the shared
// process-owned Alpaca publisher. Option contracts are explicit and bounded;
// Stonks does not perform unbounded option-chain discovery in this endpoint.
type StreamRequest struct {
	Tickers             []string `json:"tickers"`
	Feed                string   `json:"feed"`
	Subscriptions       []string `json:"subscriptions"`
	CombinedChannels    []string `json:"combined_channels"`
	OptionContracts     []string `json:"option_contracts"`
	OptionFeed          string   `json:"option_feed"`
	OptionSubscriptions []string `json:"option_subscriptions"`
}

type streamConfig struct {
	tickers             []string
	feed                marketdata.Feed
	subscriptions       []subscriptionType
	combined            map[subscriptionType]bool
	optionContracts     []string
	optionFeed          marketdata.OptionFeed
	optionSubscriptions []subscriptionType
}

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
	contracts, err := validateOptionContracts(req.OptionContracts)
	if err != nil {
		return streamConfig{}, err
	}
	optionFeed, err := validateOptionFeed(req.OptionFeed)
	if err != nil {
		return streamConfig{}, err
	}
	optionSubs, err := validateOptionSubscriptions(req.OptionSubscriptions, len(contracts) > 0)
	if err != nil {
		return streamConfig{}, err
	}
	return streamConfig{
		tickers: tickers, feed: feed, subscriptions: subs, combined: combined,
		optionContracts: contracts, optionFeed: optionFeed, optionSubscriptions: optionSubs,
	}, nil
}

type AddTickersRequest struct {
	Tickers       []string `json:"tickers"`
	Subscriptions []string `json:"subscriptions"`
}

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
	return normalizeSymbols(raw, "tickers")
}

func validateOptionContracts(raw []string) ([]string, error) {
	if len(raw) > 200 {
		return nil, errors.New("option_contracts exceeds bounded limit of 200")
	}
	if len(raw) == 0 {
		return nil, nil
	}
	return normalizeSymbols(raw, "option_contracts")
}

func normalizeSymbols(raw []string, field string) ([]string, error) {
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value == "" {
			return nil, fmt.Errorf("%s must not contain empty values", field)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
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

func validateOptionFeed(raw string) (marketdata.OptionFeed, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "indicative":
		return marketdata.Indicative, nil
	case "opra":
		return marketdata.OPRA, nil
	default:
		return "", fmt.Errorf("unsupported option_feed %q: must be \"indicative\" or \"opra\"", raw)
	}
}

func validateSubscriptions(raw []string) ([]subscriptionType, error) {
	if len(raw) == 0 {
		return defaultSubscriptions, nil
	}
	return validateSubscriptionSet(raw, false)
}

func validateOptionSubscriptions(raw []string, enabled bool) ([]subscriptionType, error) {
	if !enabled {
		if len(raw) != 0 {
			return nil, errors.New("option_subscriptions requires option_contracts")
		}
		return nil, nil
	}
	if len(raw) == 0 {
		return append([]subscriptionType(nil), defaultOptionSubscriptions...), nil
	}
	return validateSubscriptionSet(raw, true)
}

func validateSubscriptionSet(raw []string, option bool) ([]subscriptionType, error) {
	subs := make([]subscriptionType, 0, len(raw))
	seen := make(map[subscriptionType]struct{}, len(raw))
	for _, s := range raw {
		st := subscriptionType(strings.ToLower(strings.TrimSpace(s)))
		allowed := st == subTrades || st == subQuotes
		if !option {
			allowed = allowed || st == subBars || st == subDailyBars
		}
		if !allowed {
			if option {
				return nil, fmt.Errorf("unsupported option subscription %q", s)
			}
			return nil, fmt.Errorf("unsupported subscription %q", s)
		}
		if _, ok := seen[st]; ok {
			continue
		}
		seen[st] = struct{}{}
		subs = append(subs, st)
	}
	return subs, nil
}

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

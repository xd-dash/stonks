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

func combinedChannelFor(t subscriptionType, tickers []string) string {
	sorted := append([]string(nil), tickers...)
	sort.Strings(sorted)
	return fmt.Sprintf("stonks:%s:combined:%s", t.channelType(), strings.Join(sorted, ":"))
}

type optionSubscriptionType string

const (
	optionSubTrades optionSubscriptionType = "trades"
	optionSubQuotes optionSubscriptionType = "quotes"
)

var defaultOptionSubscriptions = []optionSubscriptionType{optionSubQuotes}

func (t optionSubscriptionType) channelType() string {
	switch t {
	case optionSubTrades:
		return "trade"
	case optionSubQuotes:
		return "quote"
	default:
		return string(t)
	}
}

func optionChannelFor(t optionSubscriptionType, contract string) string {
	return fmt.Sprintf("stonks:option:%s:%s", t.channelType(), contract)
}

// StreamRequest configures stock and, optionally, option streams on the same
// request/session. Stock fields retain their existing semantics. OptionContracts
// are OCC contract symbols and use Alpaca's separate options websocket.
type StreamRequest struct {
	Tickers          []string `json:"tickers"`
	Feed             string   `json:"feed"`
	Subscriptions    []string `json:"subscriptions"`
	CombinedChannels []string `json:"combined_channels"`

	OptionContracts     []string `json:"option_contracts,omitempty"`
	OptionFeed          string   `json:"option_feed,omitempty"`
	OptionSubscriptions []string `json:"option_subscriptions,omitempty"`
}

type streamConfig struct {
	tickers       []string
	feed          marketdata.Feed
	subscriptions []subscriptionType
	combined      map[subscriptionType]bool

	optionContracts     []string
	optionFeed          marketdata.OptionFeed
	optionSubscriptions []optionSubscriptionType
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

	cfg := streamConfig{
		tickers:       tickers,
		feed:          feed,
		subscriptions: subs,
		combined:      combined,
	}

	if len(req.OptionContracts) == 0 {
		if strings.TrimSpace(req.OptionFeed) != "" || len(req.OptionSubscriptions) != 0 {
			return streamConfig{}, errors.New("option_contracts must be non-empty when option_feed or option_subscriptions is set")
		}
		return cfg, nil
	}

	cfg.optionContracts, err = validateOptionContracts(req.OptionContracts)
	if err != nil {
		return streamConfig{}, err
	}
	cfg.optionFeed, err = validateOptionFeed(req.OptionFeed)
	if err != nil {
		return streamConfig{}, err
	}
	cfg.optionSubscriptions, err = validateOptionSubscriptions(req.OptionSubscriptions)
	if err != nil {
		return streamConfig{}, err
	}

	return cfg, nil
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
	return normalizeNonEmpty(raw, "tickers")
}

func validateOptionContracts(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("option_contracts must be non-empty")
	}
	return normalizeNonEmpty(raw, "option_contracts")
}

func normalizeNonEmpty(raw []string, field string) ([]string, error) {
	values := make([]string, 0, len(raw))
	for _, value := range raw {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value == "" {
			return nil, fmt.Errorf("%s must not contain empty values", field)
		}
		values = append(values, value)
	}
	return values, nil
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

func validateOptionSubscriptions(raw []string) ([]optionSubscriptionType, error) {
	if len(raw) == 0 {
		return defaultOptionSubscriptions, nil
	}
	subs := make([]optionSubscriptionType, 0, len(raw))
	for _, s := range raw {
		st := optionSubscriptionType(strings.ToLower(strings.TrimSpace(s)))
		switch st {
		case optionSubTrades, optionSubQuotes:
			subs = append(subs, st)
		default:
			return nil, fmt.Errorf("unsupported option subscription %q", s)
		}
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

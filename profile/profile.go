package profile

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

type Discovery struct {
	MinDTE           int     `json:"min_dte"`
	MaxDTE           int     `json:"max_dte"`
	MinMoneyness     float64 `json:"min_moneyness"`
	MaxMoneyness     float64 `json:"max_moneyness"`
	MinOpenInterest  int     `json:"min_open_interest"`
	MaxContracts     int     `json:"max_contracts"`
	MaxPerUnderlying int     `json:"max_per_underlying"`
}

type Alpaca struct {
	Groups              map[string][]string `json:"groups"`
	StockFeed           string              `json:"stock_feed"`
	Subscriptions       []string            `json:"subscriptions"`
	OptionFeed          string              `json:"option_feed"`
	OptionSubscriptions []string            `json:"option_subscriptions"`
	OptionContracts     []string            `json:"option_contracts,omitempty"`
	DiscoverOptions     bool                `json:"discover_options"`
	GlobalChannels      bool                `json:"global_channels"`
	Discovery           Discovery           `json:"discovery"`
}

type Logmash struct {
	Country      string   `json:"country"`
	Region       string   `json:"region"`
	Channels     []string `json:"channels,omitempty"`
	Patterns     []string `json:"patterns,omitempty"`
	CallbackArgs []string `json:"callback_args,omitempty"`
}

type Profile struct {
	Version int     `json:"version"`
	Name    string  `json:"name"`
	Alpaca  Alpaca  `json:"alpaca"`
	Logmash Logmash `json:"logmash"`
}

func Load(r io.Reader) (Profile, error) {
	var p Profile
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return Profile{}, err
	}
	if err := p.Validate(); err != nil {
		return Profile{}, err
	}
	return p, nil
}

func (p Profile) Validate() error {
	if p.Version != 1 {
		return fmt.Errorf("unsupported profile version %d", p.Version)
	}
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("profile name is required")
	}
	if len(p.Tickers()) == 0 {
		return fmt.Errorf("alpaca groups must contain at least one ticker")
	}
	if p.Alpaca.DiscoverOptions {
		d := p.Alpaca.Discovery
		if d.MinDTE < 0 || d.MaxDTE < d.MinDTE {
			return fmt.Errorf("invalid option DTE range")
		}
		if d.MinMoneyness <= 0 || d.MaxMoneyness < d.MinMoneyness {
			return fmt.Errorf("invalid option moneyness range")
		}
		if d.MaxContracts <= 0 || d.MaxContracts > 200 {
			return fmt.Errorf("max_contracts must be in 1..200")
		}
		if d.MaxPerUnderlying <= 0 {
			return fmt.Errorf("max_per_underlying must be positive")
		}
	}
	if strings.TrimSpace(p.Logmash.Country) == "" || strings.TrimSpace(p.Logmash.Region) == "" {
		return fmt.Errorf("logmash country and region are required")
	}
	return nil
}

func (p Profile) Tickers() []string {
	seen := map[string]struct{}{}
	for _, symbols := range p.Alpaca.Groups {
		for _, symbol := range symbols {
			symbol = strings.ToUpper(strings.TrimSpace(symbol))
			if symbol != "" {
				seen[symbol] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for symbol := range seen {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// LogmashArgs projects the Stonks-owned receive policy into Smoke's existing
// source-qualified Logmash grammar. Stonks owns what it publishes and the
// recommended receive profile; Smoke remains the subscription/callback runtime.
func (p Profile) LogmashArgs() []string {
	prefix := strings.ToLower(strings.TrimSpace(p.Logmash.Country)) + ":" + strings.ToLower(strings.TrimSpace(p.Logmash.Region)) + ":"
	args := make([]string, 0, len(p.Logmash.Channels)+2*len(p.Logmash.Patterns)+len(p.Logmash.CallbackArgs))
	for _, channel := range p.Logmash.Channels {
		args = append(args, prefix+strings.TrimSpace(channel))
	}
	for _, pattern := range p.Logmash.Patterns {
		args = append(args, "--pattern", prefix+strings.TrimSpace(pattern))
	}
	args = append(args, p.Logmash.CallbackArgs...)
	return args
}

package router

import (
	"testing"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

func TestOptionRequestDefaultsIndicativeQuotesAndTrades(t *testing.T) {
	req := StreamRequest{
		Tickers:         []string{"SPY"},
		OptionContracts: []string{" spy261218c00700000 ", "SPY261218C00700000"},
	}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.optionFeed != marketdata.Indicative {
		t.Fatalf("option feed = %q, want indicative", cfg.optionFeed)
	}
	if len(cfg.optionContracts) != 1 || cfg.optionContracts[0] != "SPY261218C00700000" {
		t.Fatalf("contracts = %v", cfg.optionContracts)
	}
	if len(cfg.optionSubscriptions) != 2 || cfg.optionSubscriptions[0] != subQuotes || cfg.optionSubscriptions[1] != subTrades {
		t.Fatalf("option subscriptions = %v", cfg.optionSubscriptions)
	}
}

func TestOptionRequestAcceptsOPRA(t *testing.T) {
	cfg, err := (StreamRequest{
		Tickers:         []string{"SPY"},
		OptionContracts: []string{"SPY261218C00700000"},
		OptionFeed:      "opra",
	}).validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.optionFeed != marketdata.OPRA {
		t.Fatalf("option feed = %q, want opra", cfg.optionFeed)
	}
}

func TestOptionRequestRejectsUnboundedContractSet(t *testing.T) {
	contracts := make([]string, 201)
	for i := range contracts {
		contracts[i] = "SPY261218C00700000"
	}
	if _, err := (StreamRequest{Tickers: []string{"SPY"}, OptionContracts: contracts}).validate(); err == nil {
		t.Fatal("expected bounded option_contracts rejection")
	}
}

func TestOptionSubscriptionsRequireContracts(t *testing.T) {
	if _, err := (StreamRequest{
		Tickers:             []string{"SPY"},
		OptionSubscriptions: []string{"quotes"},
	}).validate(); err == nil {
		t.Fatal("expected option_subscriptions without option_contracts to fail")
	}
}

func TestOptionSubscriptionsRejectBars(t *testing.T) {
	if _, err := (StreamRequest{
		Tickers:             []string{"SPY"},
		OptionContracts:     []string{"SPY261218C00700000"},
		OptionSubscriptions: []string{"bars"},
	}).validate(); err == nil {
		t.Fatal("expected unsupported option bars subscription to fail")
	}
}

func TestOptionChannelFor(t *testing.T) {
	if got, want := optionChannelFor(subQuotes, "SPY261218C00700000"), "stonks:option:quote:SPY261218C00700000"; got != want {
		t.Fatalf("option channel = %q, want %q", got, want)
	}
}

func TestStreamChannelsIncludesBoundedOptionChannels(t *testing.T) {
	rt := NewStonksRuntime(&alpacaCredentials{})
	cfg, err := (StreamRequest{
		Tickers:             []string{"SPY"},
		Subscriptions:       []string{"quotes"},
		OptionContracts:     []string{"SPY261218C00700000"},
		OptionSubscriptions: []string{"quotes", "trades"},
	}).validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	channels := rt.streamChannels(cfg)
	if len(channels) != 3 {
		t.Fatalf("channels = %v", channels)
	}
}

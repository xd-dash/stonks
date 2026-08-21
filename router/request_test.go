package router

import (
	"testing"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

func TestValidateRejectsEmptyTickers(t *testing.T) {
	req := StreamRequest{}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected an error for empty tickers")
	}
}

func TestValidateRejectsBlankTicker(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL", "  "}}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected an error for a blank ticker")
	}
}

func TestValidateNormalizesTickers(t *testing.T) {
	req := StreamRequest{Tickers: []string{" aapl ", "spy"}}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(cfg.tickers) != 2 || cfg.tickers[0] != "AAPL" || cfg.tickers[1] != "SPY" {
		t.Fatalf("unexpected normalized tickers: %v", cfg.tickers)
	}
}

func TestValidateDefaultsFeedToIEX(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.feed != marketdata.IEX {
		t.Fatalf("expected default feed IEX, got %q", cfg.feed)
	}
}

func TestValidateAcceptsSIPFeed(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}, Feed: "SIP"}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.feed != marketdata.SIP {
		t.Fatalf("expected feed SIP, got %q", cfg.feed)
	}
}

func TestValidateRejectsUnknownFeed(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}, Feed: "nasdaq"}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected an error for an unsupported feed")
	}
}

func TestValidateDefaultsSubscriptions(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(cfg.subscriptions) != 1 || cfg.subscriptions[0] != subBars {
		t.Fatalf("expected default subscriptions to be [bars], got %v", cfg.subscriptions)
	}
}

func TestValidateRejectsUnknownSubscription(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}, Subscriptions: []string{"ticks"}}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected an error for an unsupported subscription type")
	}
}

func TestAddTickersValidateRejectsEmptyTickers(t *testing.T) {
	req := AddTickersRequest{}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected an error for empty tickers")
	}
}

func TestAddTickersValidateNormalizesTickers(t *testing.T) {
	req := AddTickersRequest{Tickers: []string{" tsla "}}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(cfg.tickers) != 1 || cfg.tickers[0] != "TSLA" {
		t.Fatalf("unexpected normalized tickers: %v", cfg.tickers)
	}
}

func TestAddTickersValidateDefaultsSubscriptions(t *testing.T) {
	req := AddTickersRequest{Tickers: []string{"TSLA"}}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(cfg.subscriptions) != 1 || cfg.subscriptions[0] != subBars {
		t.Fatalf("expected default subscriptions to be [bars], got %v", cfg.subscriptions)
	}
}

func TestAddTickersValidateRejectsUnknownSubscription(t *testing.T) {
	req := AddTickersRequest{Tickers: []string{"TSLA"}, Subscriptions: []string{"ticks"}}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected an error for an unsupported subscription type")
	}
}

func TestChannelForDerivesDeterministicNames(t *testing.T) {
	cases := []struct {
		sub  subscriptionType
		want string
	}{
		{subTrades, "stonks:trade:AAPL"},
		{subQuotes, "stonks:quote:AAPL"},
		{subBars, "stonks:bar:AAPL"},
		{subDailyBars, "stonks:dailybar:AAPL"},
	}
	for _, c := range cases {
		if got := channelFor(c.sub, "AAPL"); got != c.want {
			t.Errorf("channelFor(%q, AAPL) = %q, want %q", c.sub, got, c.want)
		}
	}
}

func TestCombinedChannelForSortsTickersRegardlessOfInputOrder(t *testing.T) {
	want := "stonks:quote:combined:AAPL:MSFT"
	if got := combinedChannelFor(subQuotes, []string{"MSFT", "AAPL"}); got != want {
		t.Errorf("combinedChannelFor(subQuotes, [MSFT AAPL]) = %q, want %q", got, want)
	}
	if got := combinedChannelFor(subQuotes, []string{"AAPL", "MSFT"}); got != want {
		t.Errorf("combinedChannelFor(subQuotes, [AAPL MSFT]) = %q, want %q", got, want)
	}
}

func TestValidateCombinedChannelsEmptyIsNilMap(t *testing.T) {
	combined, err := validateCombinedChannels(nil, []subscriptionType{subQuotes})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if combined != nil {
		t.Fatalf("expected a nil map for an empty list, got %v", combined)
	}
}

func TestValidateCombinedChannelsAcceptsTypeInSubscriptions(t *testing.T) {
	combined, err := validateCombinedChannels([]string{"quotes"}, []subscriptionType{subQuotes, subBars})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !combined[subQuotes] {
		t.Fatalf("expected quotes to be combined, got %v", combined)
	}
}

func TestValidateCombinedChannelsRejectsUnknownType(t *testing.T) {
	if _, err := validateCombinedChannels([]string{"ticks"}, []subscriptionType{subQuotes}); err == nil {
		t.Fatal("expected an error for an unrecognized combined_channels entry")
	}
}

func TestValidateCombinedChannelsRejectsTypeNotInSubscriptions(t *testing.T) {
	if _, err := validateCombinedChannels([]string{"trades"}, []subscriptionType{subQuotes}); err == nil {
		t.Fatal("expected an error for a combined_channels entry absent from subscriptions")
	}
}

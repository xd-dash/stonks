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
	if len(cfg.subscriptions) != 3 {
		t.Fatalf("expected 3 default subscriptions, got %v", cfg.subscriptions)
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
	if len(cfg.subscriptions) != 3 {
		t.Fatalf("expected 3 default subscriptions, got %v", cfg.subscriptions)
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

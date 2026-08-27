package router

import (
	"context"
	"errors"
	"testing"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

func TestValidateOptionStreamDefaultsToIndicativeQuotes(t *testing.T) {
	req := StreamRequest{
		Tickers:         []string{"AAPL"},
		OptionContracts: []string{" aapl260828c00315000 "},
	}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.optionFeed != marketdata.Indicative {
		t.Fatalf("expected indicative feed, got %q", cfg.optionFeed)
	}
	if len(cfg.optionContracts) != 1 || cfg.optionContracts[0] != "AAPL260828C00315000" {
		t.Fatalf("unexpected option contracts: %v", cfg.optionContracts)
	}
	if len(cfg.optionSubscriptions) != 1 || cfg.optionSubscriptions[0] != optionSubQuotes {
		t.Fatalf("expected default option quotes, got %v", cfg.optionSubscriptions)
	}
}

func TestValidateOptionStreamAcceptsQuotesAndTrades(t *testing.T) {
	req := StreamRequest{
		Tickers:             []string{"AAPL"},
		OptionContracts:     []string{"AAPL260828C00315000"},
		OptionFeed:          "indicative",
		OptionSubscriptions: []string{"quotes", "trades"},
	}
	cfg, err := req.validate()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(cfg.optionSubscriptions) != 2 || cfg.optionSubscriptions[0] != optionSubQuotes || cfg.optionSubscriptions[1] != optionSubTrades {
		t.Fatalf("unexpected option subscriptions: %v", cfg.optionSubscriptions)
	}
}

func TestValidateOptionStreamRejectsOptionConfigWithoutContracts(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}, OptionFeed: "indicative"}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected option configuration without contracts to fail")
	}
}

func TestValidateOptionStreamRejectsUnknownFeed(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}, OptionContracts: []string{"AAPL260828C00315000"}, OptionFeed: "other"}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected unsupported option feed to fail")
	}
}

func TestValidateOptionStreamRejectsUnsupportedSubscription(t *testing.T) {
	req := StreamRequest{Tickers: []string{"AAPL"}, OptionContracts: []string{"AAPL260828C00315000"}, OptionSubscriptions: []string{"bars"}}
	if _, err := req.validate(); err == nil {
		t.Fatal("expected unsupported option subscription to fail")
	}
}

func TestOptionChannelFor(t *testing.T) {
	if got, want := optionChannelFor(optionSubQuotes, "AAPL260828C00315000"), "stonks:option:quote:AAPL260828C00315000"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := optionChannelFor(optionSubTrades, "AAPL260828C00315000"), "stonks:option:trade:AAPL260828C00315000"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWaitForConnectedStreamsPreservesTerminalError(t *testing.T) {
	ctx := context.Background()
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	stock := make(chan error, 1)
	option := make(chan error, 1)
	want := errors.New("option upstream failed")
	option <- want
	close(option)

	go func() {
		<-ctx.Done()
		stock <- nil
		close(stock)
	}()

	err := waitForConnectedStreams(ctx, cancelCtx, map[string]<-chan error{
		"stock":  stock,
		"option": option,
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected terminal error %v, got %v", want, err)
	}
}

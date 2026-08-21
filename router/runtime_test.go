package router

import "testing"

func TestBaseChannelUsesPerSymbolWhenNotCombined(t *testing.T) {
	rt := &StonksRuntime{cfg: streamConfig{tickers: []string{"AAPL", "MSFT"}}}

	want := "stonks:quote:AAPL"
	if got := rt.baseChannel(subQuotes, "AAPL"); got != want {
		t.Fatalf("baseChannel(subQuotes, AAPL) = %q, want %q", got, want)
	}
}

func TestBaseChannelUsesCombinedWhenConfigured(t *testing.T) {
	rt := &StonksRuntime{cfg: streamConfig{
		tickers:  []string{"MSFT", "AAPL"},
		combined: map[subscriptionType]bool{subQuotes: true},
	}}

	want := "stonks:quote:combined:AAPL:MSFT"
	if got := rt.baseChannel(subQuotes, "AAPL"); got != want {
		t.Fatalf("baseChannel(subQuotes, AAPL) = %q, want %q", got, want)
	}
	if got := rt.baseChannel(subQuotes, "MSFT"); got != want {
		t.Fatalf("baseChannel(subQuotes, MSFT) = %q, want %q", got, want)
	}
}

func TestBaseChannelOnlyCombinesConfiguredTypes(t *testing.T) {
	rt := &StonksRuntime{cfg: streamConfig{
		tickers:  []string{"AAPL", "MSFT"},
		combined: map[subscriptionType]bool{subQuotes: true},
	}}

	want := "stonks:bar:AAPL"
	if got := rt.baseChannel(subBars, "AAPL"); got != want {
		t.Fatalf("baseChannel(subBars, AAPL) = %q, want %q (bars weren't combined)", got, want)
	}
}

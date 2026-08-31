package router

import (
	"testing"

	"github.com/xd-dash/logma/serverless/pubsub"
)

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

func TestPublishChannelUsesInstanceScopeByDefault(t *testing.T) {
	rt := &StonksRuntime{
		Runtime: pubsub.NewRuntime(nil),
		cfg:     streamConfig{tickers: []string{"AAPL"}},
	}

	want := "stonks:trade:AAPL:" + rt.InstanceID
	if got := rt.publishChannel(subTrades, "AAPL"); got != want {
		t.Fatalf("publishChannel(subTrades, AAPL) = %q, want %q", got, want)
	}
}

func TestPublishChannelUsesGlobalScopeWhenEnabled(t *testing.T) {
	rt := &StonksRuntime{
		Runtime:        pubsub.NewRuntime(nil),
		cfg:            streamConfig{tickers: []string{"AAPL"}},
		globalChannels: true,
	}

	want := "stonks:trade:AAPL:global"
	if got := rt.publishChannel(subTrades, "AAPL"); got != want {
		t.Fatalf("publishChannel(subTrades, AAPL) = %q, want %q", got, want)
	}
}

func TestStreamChannelsDeduplicatesCombinedChannels(t *testing.T) {
	rt := &StonksRuntime{
		Runtime: pubsub.NewRuntime(nil),
		cfg: streamConfig{
			tickers:       []string{"AAPL", "MSFT"},
			subscriptions: []subscriptionType{subQuotes},
			combined:      map[subscriptionType]bool{subQuotes: true},
		},
	}
	channels := rt.streamChannels()
	if len(channels) != 1 {
		t.Fatalf("streamChannels() returned %d channels, want 1: %v", len(channels), channels)
	}
	want := "stonks:quote:combined:AAPL:MSFT:" + rt.InstanceID
	if channels[0] != want {
		t.Fatalf("streamChannels()[0] = %q, want %q", channels[0], want)
	}
}

func TestNewStonksRuntimePicksUpGlobalChannelsFromEnv(t *testing.T) {
	t.Setenv("STONKS_GLOBAL_CHANNELS", "true")

	rt := NewStonksRuntime(&alpacaCredentials{})
	if !rt.globalChannels {
		t.Fatal("expected NewStonksRuntime to read STONKS_GLOBAL_CHANNELS=true from the environment")
	}
}

func TestNewStonksRuntimeDefaultsGlobalChannelsToFalse(t *testing.T) {
	t.Setenv("STONKS_GLOBAL_CHANNELS", "")

	rt := NewStonksRuntime(&alpacaCredentials{})
	if rt.globalChannels {
		t.Fatal("expected NewStonksRuntime to default globalChannels to false")
	}
}

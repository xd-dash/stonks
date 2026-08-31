package router

import (
	"testing"

	"github.com/xd-dash/logma/serverless/pubsub"
)

func TestPublishChannelUsesInstanceScopeByDefault(t *testing.T) {
	rt := &StonksRuntime{Runtime: pubsub.NewRuntime(nil)}

	want := rt.InstanceID + ":stonks:trade:AAPL"
	if got := rt.publishChannel(subTrades, "AAPL"); got != want {
		t.Fatalf("publishChannel(subTrades, AAPL) = %q, want %q", got, want)
	}
}

func TestPublishChannelUsesGlobalScopeWhenEnabled(t *testing.T) {
	rt := &StonksRuntime{
		Runtime:        pubsub.NewRuntime(nil),
		globalChannels: true,
	}

	want := "global:stonks:trade:AAPL"
	if got := rt.publishChannel(subTrades, "AAPL"); got != want {
		t.Fatalf("publishChannel(subTrades, AAPL) = %q, want %q", got, want)
	}
}

func TestStreamChannelsUsesCanonicalPerSymbolChannels(t *testing.T) {
	rt := &StonksRuntime{Runtime: pubsub.NewRuntime(nil)}
	cfg := streamConfig{
		tickers:       []string{"AAPL", "MSFT"},
		subscriptions: []subscriptionType{subQuotes},
		combined:      map[subscriptionType]bool{subQuotes: true},
	}

	channels := rt.streamChannels(cfg)
	if len(channels) != 2 {
		t.Fatalf("streamChannels returned %d channels, want 2: %v", len(channels), channels)
	}
	want := map[string]bool{
		rt.InstanceID + ":stonks:quote:AAPL": true,
		rt.InstanceID + ":stonks:quote:MSFT": true,
	}
	for _, channel := range channels {
		if !want[channel] {
			t.Fatalf("unexpected channel %q", channel)
		}
	}
}

func TestStreamChannelsDeduplicatesRepeatedTickers(t *testing.T) {
	rt := &StonksRuntime{Runtime: pubsub.NewRuntime(nil)}
	cfg := streamConfig{
		tickers:       []string{"AAPL", "AAPL"},
		subscriptions: []subscriptionType{subQuotes},
	}
	channels := rt.streamChannels(cfg)
	if len(channels) != 1 {
		t.Fatalf("streamChannels returned %d channels, want 1: %v", len(channels), channels)
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

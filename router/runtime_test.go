package router

import (
	"errors"
	"testing"

	"github.com/xd-dash/logma-serverless/pubsub"
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

func TestWaitForAlpacaTerminationDrainsAndReturnsFirstError(t *testing.T) {
	terminated := make(chan error, 2)
	want := errors.New("first terminal error")
	terminated <- want
	terminated <- errors.New("later terminal error")
	close(terminated)

	if got := waitForAlpacaTermination(terminated); !errors.Is(got, want) {
		t.Fatalf("waitForAlpacaTermination() = %v, want %v", got, want)
	}
}

func TestWaitForAlpacaTerminationReturnsNilForCleanShutdown(t *testing.T) {
	terminated := make(chan error, 1)
	terminated <- nil
	close(terminated)

	if err := waitForAlpacaTermination(terminated); err != nil {
		t.Fatalf("waitForAlpacaTermination() = %v, want nil", err)
	}
}

func TestCloseIsIdempotentWithoutClient(t *testing.T) {
	rt := &StonksRuntime{}
	rt.Close()
	rt.Close()
}

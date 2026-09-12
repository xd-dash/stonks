package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xd-dash/stonks/discovery"
	"github.com/xd-dash/stonks/profile"
	"github.com/xd-dash/stonks/router"
)

func readSecret(name string) string {
	if path := strings.TrimSpace(os.Getenv(name + "_FILE")); path != "" {
		if b, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return strings.TrimSpace(os.Getenv(name))
}

func main() {
	profilePath := strings.TrimSpace(os.Getenv("STONKS_PROFILE"))
	if profilePath == "" {
		profilePath = "profiles/energy-calibration.json"
	}
	f, err := os.Open(profilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	p, err := profile.Load(f)
	if err != nil {
		log.Fatal(err)
	}

	tickers := p.Tickers()
	contracts := append([]string(nil), p.Alpaca.OptionContracts...)
	if p.Alpaca.DiscoverOptions && strings.TrimSpace(os.Getenv("STONKS_REPLAY_FIXTURE")) == "" {
		client := discovery.New(readSecret("ALPACA_API_KEY_ID"), readSecret("ALPACA_API_SECRET_KEY"))
		d := p.Alpaca.Discovery
		result, err := client.Prepare(context.Background(), tickers, discovery.Config{
			MinDTE: d.MinDTE,
			MaxDTE: d.MaxDTE,
			MinMoneyness: d.MinMoneyness,
			MaxMoneyness: d.MaxMoneyness,
			MinOpenInterest: d.MinOpenInterest,
			MaxContracts: d.MaxContracts,
			MaxPerUnderlying: d.MaxPerUnderlying,
			StockFeed: p.Alpaca.StockFeed,
		})
		if err != nil {
			log.Fatal(err)
		}
		contracts = contracts[:0]
		for _, contract := range result.Contracts {
			contracts = append(contracts, contract.Symbol)
		}
		log.Printf("stonks: profile=%s underlyings=%d discovered-options=%d", p.Name, len(tickers), len(contracts))
	}

	req := router.StreamRequest{
		Tickers: tickers,
		Feed: p.Alpaca.StockFeed,
		Subscriptions: p.Alpaca.Subscriptions,
		OptionContracts: contracts,
		OptionFeed: p.Alpaca.OptionFeed,
		OptionSubscriptions: p.Alpaca.OptionSubscriptions,
	}
	if encoded, err := json.Marshal(req); err == nil {
		log.Printf("stonks: publisher request=%s", encoded)
	}
	logmash, _ := json.Marshal(p.LogmashArgs())
	fmt.Fprintf(os.Stderr, "stonks: smoke logmash args=%s\n", logmash)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := router.RunPublisher(ctx, req, router.PublisherOptions{GlobalChannels: p.Alpaca.GlobalChannels}); err != nil {
		log.Fatal(err)
	}
}

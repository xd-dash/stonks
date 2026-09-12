package router

import (
	"context"
	"fmt"
	"os"
	"strings"
)

type PublisherOptions struct {
	GlobalChannels bool
}

func alpacaAPISecret() string {
	if path := strings.TrimSpace(os.Getenv("ALPACA_API_SECRET_KEY_FILE")); path != "" {
		if value, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(value))
		}
	}
	return strings.TrimSpace(os.Getenv("ALPACA_API_SECRET_KEY"))
}

// RunPublisher starts the process-owned Alpaca->Redis publisher without
// creating any Redis subscriber or SSE response. Subscription/callback
// ownership belongs to Smoke/Logmash or another downstream consumer.
func RunPublisher(ctx context.Context, req StreamRequest, opts PublisherOptions) error {
	cfg, err := req.validate()
	if err != nil {
		return err
	}
	secret := alpacaAPISecret()
	if replayFixturePath() == "" {
		if alpacaAPIKeyID() == "" {
			return fmt.Errorf("ALPACA_API_KEY_ID or ALPACA_API_KEY_ID_FILE is required")
		}
		if secret == "" {
			return fmt.Errorf("ALPACA_API_SECRET_KEY or ALPACA_API_SECRET_KEY_FILE is required")
		}
	}
	creds := &alpacaCredentials{}
	if secret != "" {
		creds.load(secret)
	}
	rt := NewStonksRuntime(creds)
	rt.globalChannels = opts.GlobalChannels
	rt.Configure(cfg, secret)
	go rt.Start(ctx)

	select {
	case <-rt.Ready():
	case <-rt.Done():
		return fmt.Errorf("stonks publisher stopped before becoming ready")
	case <-ctx.Done():
		return nil
	}

	select {
	case <-ctx.Done():
		return nil
	case <-rt.Done():
		return nil
	}
}

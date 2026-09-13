package router

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata/stream"
)

// replayEnvelope stores an Alpaca SDK callback object immediately before the
// normal Stonks callback boundary. It deliberately does not store Redis/Logma
// payloads: replay must still traverse the normal callback -> publish path.
type replayEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func replayFixturePath() string {
	return strings.TrimSpace(os.Getenv("STONKS_REPLAY_FIXTURE"))
}

func replayDelay() time.Duration {
	raw := strings.TrimSpace(os.Getenv("STONKS_REPLAY_DELAY"))
	if raw == "" {
		return 25 * time.Millisecond
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return 25 * time.Millisecond
	}
	return d
}

// streamReplay replaces only the external Alpaca WebSockets. Every decoded
// object enters through the same stock/option callbacks used by the live SDK,
// so qualification still exercises Stonks -> Redis publication. A replay
// fixture is finite by definition, so successful EOF completes the replay
// publisher instead of manufacturing a long-running live lifecycle.
func (rt *StonksRuntime) streamReplay(ctx context.Context) error {
	path := rt.replayFixture
	if path == "" {
		return fmt.Errorf("replay fixture path is empty")
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open replay fixture: %w", err)
	}
	defer f.Close()

	rt.connectedOnce.Do(func() { close(rt.connected) })

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	delay := replayDelay()
	line := 0
	for scanner.Scan() {
		line++
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		var envelope replayEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			return fmt.Errorf("decode replay fixture line %d: %w", line, err)
		}
		if err := rt.publishReplayEnvelope(envelope); err != nil {
			return fmt.Errorf("replay fixture line %d: %w", line, err)
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read replay fixture: %w", err)
	}
	return nil
}

func (rt *StonksRuntime) publishReplayEnvelope(envelope replayEnvelope) error {
	switch strings.ToLower(strings.TrimSpace(envelope.Type)) {
	case "trade":
		var event stream.Trade
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("decode trade callback: %w", err)
		}
		if event.Symbol == "" {
			return fmt.Errorf("trade callback has empty symbol")
		}
		if rt.replaySubscribed(subTrades, event.Symbol) {
			rt.onTrade(event)
		}
	case "quote":
		var event stream.Quote
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("decode quote callback: %w", err)
		}
		if event.Symbol == "" {
			return fmt.Errorf("quote callback has empty symbol")
		}
		if rt.replaySubscribed(subQuotes, event.Symbol) {
			rt.onQuote(event)
		}
	case "bar":
		var event stream.Bar
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("decode bar callback: %w", err)
		}
		if event.Symbol == "" {
			return fmt.Errorf("bar callback has empty symbol")
		}
		if rt.replaySubscribed(subBars, event.Symbol) {
			rt.onBar(event)
		}
	case "dailybar":
		var event stream.Bar
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("decode dailybar callback: %w", err)
		}
		if event.Symbol == "" {
			return fmt.Errorf("dailybar callback has empty symbol")
		}
		if rt.replaySubscribed(subDailyBars, event.Symbol) {
			rt.onDailyBar(event)
		}
	case "option_quote":
		var event stream.OptionQuote
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("decode option quote callback: %w", err)
		}
		if event.Symbol == "" {
			return fmt.Errorf("option quote callback has empty symbol")
		}
		if rt.replayOptionSubscribed(subQuotes, event.Symbol) {
			rt.onOptionQuote(event)
		}
	case "option_trade":
		var event stream.OptionTrade
		if err := json.Unmarshal(envelope.Data, &event); err != nil {
			return fmt.Errorf("decode option trade callback: %w", err)
		}
		if event.Symbol == "" {
			return fmt.Errorf("option trade callback has empty symbol")
		}
		if rt.replayOptionSubscribed(subTrades, event.Symbol) {
			rt.onOptionTrade(event)
		}
	default:
		return fmt.Errorf("unsupported callback type %q", envelope.Type)
	}
	return nil
}

func (rt *StonksRuntime) replaySubscribed(sub subscriptionType, symbol string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	bySymbol := rt.subscriptions[sub]
	if bySymbol == nil {
		return false
	}
	_, ok := bySymbol[strings.ToUpper(symbol)]
	return ok
}

func (rt *StonksRuntime) replayOptionSubscribed(sub subscriptionType, contract string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	byContract := rt.optionSubscriptions[sub]
	if byContract == nil {
		return false
	}
	_, ok := byContract[strings.ToUpper(contract)]
	return ok
}

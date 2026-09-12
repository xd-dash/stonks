package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	MinDTE           int
	MaxDTE           int
	MinMoneyness     float64
	MaxMoneyness     float64
	MinOpenInterest  int
	MaxContracts     int
	MaxPerUnderlying int
	StockFeed        string
}

type Contract struct {
	Symbol       string
	Type         string
	StrikePrice  float64
	OpenInterest int
	Score        float64
}

type Result struct {
	StockMidpoints map[string]float64
	Contracts      []Contract
}

type Client struct {
	APIKey          string
	APISecret       string
	HTTPClient      *http.Client
	DataBaseURL     string
	TradingBaseURL  string
}

func New(apiKey, apiSecret string) *Client {
	return &Client{
		APIKey: apiKey,
		APISecret: apiSecret,
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
		DataBaseURL: "https://data.alpaca.markets",
		TradingBaseURL: "https://paper-api.alpaca.markets",
	}
}

func (c *Client) Prepare(ctx context.Context, underlyings []string, cfg Config) (Result, error) {
	underlyings = normalize(underlyings)
	midpoints, err := c.latestStockMidpoints(ctx, underlyings, cfg.StockFeed)
	if err != nil {
		return Result{}, err
	}
	all := make([]Contract, 0)
	for _, underlying := range underlyings {
		spot, ok := midpoints[underlying]
		if !ok {
			continue
		}
		rows, err := c.contractsForUnderlying(ctx, underlying, spot, cfg)
		if err != nil {
			return Result{}, fmt.Errorf("discover %s options: %w", underlying, err)
		}
		all = append(all, selectContracts(rows, spot, cfg)...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	if cfg.MaxContracts > 0 && len(all) > cfg.MaxContracts {
		all = all[:cfg.MaxContracts]
	}
	return Result{StockMidpoints: midpoints, Contracts: all}, nil
}

func (c *Client) latestStockMidpoints(ctx context.Context, symbols []string, feed string) (map[string]float64, error) {
	if len(symbols) == 0 {
		return map[string]float64{}, nil
	}
	q := url.Values{}
	q.Set("symbols", strings.Join(symbols, ","))
	if strings.TrimSpace(feed) != "" {
		q.Set("feed", feed)
	}
	var payload struct {
		Quotes map[string]struct {
			Bid float64 `json:"bp"`
			Ask float64 `json:"ap"`
		} `json:"quotes"`
	}
	if err := c.getJSON(ctx, c.DataBaseURL+"/v2/stocks/quotes/latest?"+q.Encode(), &payload); err != nil {
		return nil, fmt.Errorf("latest stock quotes: %w", err)
	}
	out := make(map[string]float64, len(payload.Quotes))
	for symbol, quote := range payload.Quotes {
		if quote.Bid > 0 && quote.Ask >= quote.Bid {
			out[strings.ToUpper(symbol)] = (quote.Bid + quote.Ask) / 2
		}
	}
	return out, nil
}

type contractRow struct {
	Symbol       string `json:"symbol"`
	Type         string `json:"type"`
	StrikePrice  string `json:"strike_price"`
	OpenInterest string `json:"open_interest"`
}

func (c *Client) contractsForUnderlying(ctx context.Context, underlying string, spot float64, cfg Config) ([]contractRow, error) {
	today := time.Now().UTC()
	q := url.Values{}
	q.Set("underlying_symbols", underlying)
	q.Set("status", "active")
	q.Set("expiration_date_gte", today.AddDate(0, 0, cfg.MinDTE).Format("2006-01-02"))
	q.Set("expiration_date_lte", today.AddDate(0, 0, cfg.MaxDTE).Format("2006-01-02"))
	q.Set("strike_price_gte", strconv.FormatFloat(spot*cfg.MinMoneyness, 'f', 4, 64))
	q.Set("strike_price_lte", strconv.FormatFloat(spot*cfg.MaxMoneyness, 'f', 4, 64))
	q.Set("limit", "10000")

	rows := make([]contractRow, 0)
	pageToken := ""
	for {
		if pageToken != "" {
			q.Set("page_token", pageToken)
		} else {
			q.Del("page_token")
		}
		var payload struct {
			Contracts []contractRow `json:"option_contracts"`
			NextPageToken string `json:"next_page_token"`
		}
		if err := c.getJSON(ctx, c.TradingBaseURL+"/v2/options/contracts?"+q.Encode(), &payload); err != nil {
			return nil, err
		}
		rows = append(rows, payload.Contracts...)
		pageToken = strings.TrimSpace(payload.NextPageToken)
		if pageToken == "" {
			break
		}
	}
	return rows, nil
}

func selectContracts(rows []contractRow, spot float64, cfg Config) []Contract {
	calls := make([]Contract, 0)
	puts := make([]Contract, 0)
	for _, row := range rows {
		strike, err := strconv.ParseFloat(row.StrikePrice, 64)
		if err != nil || strike <= 0 {
			continue
		}
		oi, err := strconv.Atoi(row.OpenInterest)
		if err != nil || oi < cfg.MinOpenInterest {
			continue
		}
		right := strings.ToLower(strings.TrimSpace(row.Type))
		if right != "call" && right != "put" {
			continue
		}
		distance := math.Abs(math.Log(strike / spot))
		item := Contract{
			Symbol: strings.ToUpper(strings.TrimSpace(row.Symbol)),
			Type: right,
			StrikePrice: strike,
			OpenInterest: oi,
			Score: math.Log1p(float64(oi)) / (0.01 + distance),
		}
		if item.Symbol == "" {
			continue
		}
		if right == "call" {
			calls = append(calls, item)
		} else {
			puts = append(puts, item)
		}
	}
	sort.Slice(calls, func(i, j int) bool { return calls[i].Score > calls[j].Score })
	sort.Slice(puts, func(i, j int) bool { return puts[i].Score > puts[j].Score })
	half := cfg.MaxPerUnderlying / 2
	if half < 1 {
		half = 1
	}
	if len(calls) > half {
		calls = calls[:half]
	}
	if len(puts) > half {
		puts = puts[:half]
	}
	return append(calls, puts...)
}

func (c *Client) getJSON(ctx context.Context, endpoint string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("APCA-API-KEY-ID", c.APIKey)
	req.Header.Set("APCA-API-SECRET-KEY", c.APISecret)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alpaca returned HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func normalize(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

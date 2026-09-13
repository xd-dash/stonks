package profile

import (
	"reflect"
	"strings"
	"testing"
)

func TestProfileFlattensTickerGroupsAndBuildsLogmashArgs(t *testing.T) {
	p, err := Load(strings.NewReader(`{
		"version":1,
		"name":"test",
		"alpaca":{
			"groups":{"a":["spy","xom"],"b":["SPY","fro"]},
			"stock_feed":"iex",
			"subscriptions":["bars"],
			"option_feed":"indicative",
			"option_subscriptions":["quotes"],
			"discover_options":true,
			"global_channels":true,
			"discovery":{"min_dte":7,"max_dte":60,"min_moneyness":0.85,"max_moneyness":1.15,"min_open_interest":500,"max_contracts":180,"max_per_underlying":24}
		},
		"logmash":{"country":"us","region":"west","channels":["stonks:bar:SPY:global"],"patterns":["stonks:option:quote:*:global"]}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := p.Tickers(), []string{"FRO", "SPY", "XOM"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tickers=%v want=%v", got, want)
	}
	wantArgs := []string{
		"us:west:stonks:bar:SPY:global",
		"--pattern", "us:west:stonks:option:quote:*:global",
	}
	if got := p.LogmashArgs(); !reflect.DeepEqual(got, wantArgs) {
		t.Fatalf("logmash args=%v want=%v", got, wantArgs)
	}
}

func TestProfileRejectsUnboundedOptionCount(t *testing.T) {
	_, err := Load(strings.NewReader(`{
		"version":1,
		"name":"bad",
		"alpaca":{"groups":{"a":["SPY"]},"discover_options":true,"discovery":{"min_dte":0,"max_dte":1,"min_moneyness":1,"max_moneyness":1,"max_contracts":201,"max_per_underlying":1}},
		"logmash":{"country":"us","region":"west"}
	}`))
	if err == nil {
		t.Fatal("expected validation error")
	}
}

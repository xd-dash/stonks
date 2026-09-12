package discovery

import "testing"

func TestSelectContractsBalancesCallsAndPuts(t *testing.T) {
	cfg := Config{MinOpenInterest: 500, MaxPerUnderlying: 4}
	rows := []contractRow{
		{Symbol: "XYZ260918C00100000", Type: "call", StrikePrice: "100", OpenInterest: "5000"},
		{Symbol: "XYZ260918C00110000", Type: "call", StrikePrice: "110", OpenInterest: "3000"},
		{Symbol: "XYZ260918C00120000", Type: "call", StrikePrice: "120", OpenInterest: "100"},
		{Symbol: "XYZ260918P00100000", Type: "put", StrikePrice: "100", OpenInterest: "4500"},
		{Symbol: "XYZ260918P00090000", Type: "put", StrikePrice: "90", OpenInterest: "2500"},
	}
	got := selectContracts(rows, 100, cfg)
	if len(got) != 4 {
		t.Fatalf("len=%d want=4", len(got))
	}
	calls, puts := 0, 0
	for _, contract := range got {
		if contract.OpenInterest < 500 {
			t.Fatalf("selected low-OI contract: %+v", contract)
		}
		switch contract.Type {
		case "call":
			calls++
		case "put":
			puts++
		}
	}
	if calls != 2 || puts != 2 {
		t.Fatalf("calls=%d puts=%d", calls, puts)
	}
}

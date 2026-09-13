# stonks

`stonks` owns market-data acquisition, bounded Alpaca option discovery, and publication into Redis/Logma. It does not require an attached SSE requester to keep its Alpaca publisher alive.

The canonical path is:

```text
Alpaca stock + option data
          |
          v
        Stonks
 acquisition / discovery
      publication
          |
          v
      Redis / Logma
          |
          v
    Smoke / Logmash
 subscription / callbacks
          |
          v
 rh-agent / observers / sinks
```

Redis is the runtime boundary between Stonks and downstream consumers. Smoke/Logmash owns subscription execution and callback policy; analytical consumers such as rh-agent own state and interpretation.

## Standalone publisher

`cmd/stonks-publisher` is the canonical process-owned publisher entry point. It loads a Stonks market-data profile, performs bounded option discovery when enabled, configures the stock and option streams, and publishes events without creating a Redis subscriber or SSE response.

Typical configuration:

```text
REDIS_URI=127.0.0.1:6379
STONKS_PROFILE=profiles/energy-calibration.json
ALPACA_API_KEY_ID=...
ALPACA_API_SECRET_KEY=...
```

For deterministic qualification, `STONKS_REPLAY_FIXTURE` replaces only the external Alpaca websocket source. Fixture objects still traverse the normal Stonks callback-to-Redis publication path.

## Profiles

A Stonks profile owns market-data intent:

- underlying groups;
- stock feed and event classes;
- option feed and event classes;
- bounded option-discovery policy;
- optional exact seed contracts;
- global versus instance channel scope;
- recommended Logmash source/channel/pattern selectors.

Callback destinations, retries, failure policy, and sink-specific arguments are not Stonks profile concerns; those belong to Smoke/Logmash invocation or downstream consumers.

`profiles/energy-calibration.json` is the current energy-market profile used by qualification.

## Channel shape

Canonical channels are:

```text
stonks:trade:AAPL:<scope>
stonks:quote:AAPL:<scope>
stonks:bar:SPY:<scope>
stonks:dailybar:SPY:<scope>
stonks:option:quote:SPY261218C00700000:<scope>
stonks:option:trade:SPY261218C00700000:<scope>
```

The scope is normally the Stonks instance ID. Profiles that require stable cross-process consumption may set global channels, producing a final `:global` scope.

Redis Pub/Sub fans one Stonks publication to every active subscriber; downstream analytical, callback, diagnostic, or compatibility consumers do not require separate Alpaca connections.

## Option discovery

Bounded option discovery is Stonks infrastructure. Discovery is constrained by profile parameters such as DTE, moneyness, minimum open interest, maximum total contracts, and maximum contracts per underlying.

The `/stream` compatibility API still accepts an explicit bounded `option_contracts` set and does not perform unbounded discovery inside a request. That request-level restriction does not change ownership of the canonical standalone discovery path.

## HTTP/SSE compatibility adapter

`router.NewRouter()` remains a plain `http.Handler` for existing serverless/container shells. `POST /stream` can attach a request-scoped Logma subscriber and return `text/event-stream` for compatibility with older consumers.

Example request:

```json
{
  "tickers": ["AAPL", "SPY"],
  "feed": "iex",
  "subscriptions": ["trades", "quotes", "bars"],
  "option_contracts": ["SPY261218C00700000"],
  "option_feed": "indicative",
  "option_subscriptions": ["quotes", "trades"]
}
```

A `/stream` requester owns only its subscriber/SSE lifetime. Requester disconnect must not be treated as authority to retire a process-owned publisher. New qualification should use the standalone publisher + Redis + Smoke/Logmash boundary unless the SSE adapter itself is what is under test.

## Farcaster ownership

In a retained Farcaster deployment:

- Redis/Logma are retained shared infrastructure;
- Stonks owns its publisher for the Stonks service/session lifetime;
- Smoke/Logmash or other consumers own their subscriptions/callbacks;
- request/observer disconnect does not retire the publisher;
- run-scoped qualification activity does not own or replace the retained Farcaster or canonical Redis/Logma runtime.

Service retirement follows explicit Huram lifecycle ownership rather than consumer presence.

## Configuration and credentials

Redis uses the shared Logma conventions:

- `REDIS_URI`;
- `REDIS_SOCKET` for Unix sockets;
- `REDIS_USERNAME` when required;
- `REDISCLI_AUTH` or supported file-backed credential form.

Alpaca authentication uses `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` for the standalone publisher. The HTTP compatibility adapter also supports its qualified request/file-backed admission forms.

Credentials are not added to Redis market events, callback payloads, or SSE output.

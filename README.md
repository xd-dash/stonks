# stonks

`stonks` owns one retained/shared Alpaca publisher per running service instance and composes Logma's `serverless` package for request-scoped Redis-to-SSE fanout.

The live path is:

```text
Alpaca stock + option streams
          |
          v
 shared Stonks publisher
          |
          v
 Farcaster Redis / Logma
     /       |       \
    v        v        v
 /stream  /stream  /stream
```

The publisher is process/service-owned. An HTTP requester never owns the Alpaca connection, so disconnecting one SSE client removes only that client's Redis subscriptions.

`POST /stream` accepts stock symbols plus an optional bounded set of exact option contracts:

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

Stock `feed` defaults to `iex`; stock subscriptions default to `bars`. Option `option_feed` defaults to `indicative`; option subscriptions default to `quotes` + `trades` when option contracts are supplied. `option_contracts` is explicit, deduplicated, and bounded to 200 contracts per request. Stonks deliberately does not perform unbounded option-chain discovery in `/stream`.

`combined_channels` remains accepted for stock request compatibility but does not alter the shared publisher's canonical per-symbol Redis topology.

The response is `text/event-stream` and includes the exact Redis/Logma channel identity:

```text
event: message
data: {"channel":"stonks:quote:AAPL:<instance>","data":{...}}
```

## Shared publisher lifecycle

The first accepted requester prepares the process-owned publisher but does not immediately start Alpaca. Its Logma Redis subscriptions are created first and must receive Redis subscription acknowledgements. Only then is the shared Alpaca publisher started. This avoids losing the first live publication because Redis Pub/Sub has no replay.

Stocks and options use Alpaca's separate websocket clients but one Stonks runtime owns both. When the first request includes options, readiness means both required upstream clients connected before the request proceeds.

After the publisher is active, later requesters can add stock symbol/type subscriptions and can add option contracts only when the shared publisher was initially created with option streaming. Feed identity is process-wide: a conflicting stock or option feed is rejected rather than silently substituted.

If one SSE requester cannot keep up with its bounded request buffer, only that request is cancelled. The retained publisher and other requesters remain unaffected.

## Channel shape

Canonical channels are:

```text
stonks:trade:AAPL:<instanceID>
stonks:quote:AAPL:<instanceID>
stonks:bar:SPY:<instanceID>
stonks:option:quote:SPY261218C00700000:<instanceID>
stonks:option:trade:SPY261218C00700000:<instanceID>
```

`STONKS_GLOBAL_CHANNELS=true` changes the final scope to `:global`. Keep the instance-scoped default for reusable or potentially multi-instance deployments.

Redis Pub/Sub naturally fans one publication out to every active subscriber, so one Stonks publisher can feed an analytical sandbox, qualification Action, diagnostics, and other SSE consumers simultaneously without opening one upstream connection per requester.

## Farcaster ownership

In the retained Farcaster sandbox:

- Redis/Logma remain retained shared infrastructure;
- the transient or retained Stonks process owns its Alpaca publisher for its own process lifetime;
- `/stream` requesters own only request-scoped Logma subscribers;
- requester disconnect does not retire the Stonks publisher;
- child qualification activity does not own or replace the retained Farcaster host or canonical Redis/Logma runtime.

Service retirement follows Huram Abi exact deployment/state ownership rather than being inferred from SSE client presence.

## Configuration and credentials

Redis uses the shared Logma convention:

- `REDIS_URI`
- `REDIS_SOCKET` when using a Unix socket
- `REDISCLI_AUTH` / supported file-backed credential form

Alpaca authentication uses:

- `ALPACA_API_KEY_ID` or the qualified file-backed admission-key form in the service;
- `X-Alpaca-Api-Key-Id` on each request;
- `X-Alpaca-Api-Secret-Key` when the service does not already hold the secret in its in-memory credential cache.

The Alpaca secret is never added to Redis market events or SSE payloads.

`router.NewRouter()` remains a plain `http.Handler` suitable for the existing Go serverless/container shells.

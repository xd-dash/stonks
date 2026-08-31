# stonks

`stonks` streams Alpaca market data into Redis and, for each accepted `POST /stream` request, composes Logma's `serverless` package in-process so the same request receives the selected Redis channels back as Server-Sent Events.

The live path is:

```text
requester / GitHub Action
        |
        | POST /stream
        v
stonks
  |- Logma serverless subscriber(s) -- subscribe first
  |- Alpaca publisher --------------- start second
  |
  v
shared Farcaster Redis / Logma runtime
  |
  +---------------- Redis Pub/Sub ----------------+
                                                  |
                                                  v
                                      request SSE response
```

`POST /stream` accepts:

```json
{
  "tickers": ["AAPL", "SPY"],
  "feed": "iex",
  "subscriptions": ["trades", "quotes", "bars"],
  "combined_channels": ["quotes"]
}
```

`feed`, `subscriptions`, and `combined_channels` are optional. The default subscription is `bars`. Supported subscriptions are `trades`, `quotes`, `bars`, and `dailybars`.

The response is `text/event-stream`. Every Redis market event is emitted using Logma's normal envelope shape:

```text
event: message
data: {"channel":"stonks:quote:AAPL:<instance>","data":{...}}
```

The handler creates the Redis subscriptions and waits for Redis to acknowledge all of them before starting the Alpaca connection. Redis Pub/Sub has no replay, so this subscriber-first ordering prevents the first Alpaca event from racing the SSE consumer into existence.

If the SSE consumer cannot keep up with the bounded request buffer, the request is cancelled rather than silently dropping market data. Disconnecting the requester also cancels both the embedded Logma subscriber side and the Alpaca publisher side.

## Channel shape

By default each event type/symbol publishes to an instance-scoped channel such as:

```text
stonks:trade:AAPL:<instanceID>
stonks:quote:AAPL:<instanceID>
stonks:bar:SPY:<instanceID>
```

A type named in `combined_channels` uses one deterministic channel for the requested ticker set, for example:

```text
stonks:quote:combined:AAPL:MSFT:<instanceID>
```

`STONKS_GLOBAL_CHANNELS=true` changes the final scope to `:global`. Keep the instance-scoped default for reusable or potentially multi-instance deployments.

## Runtime ownership

One `stonks` service instance owns one active `/stream` request at a time. Once that request ends, the holder creates a fresh `StonksRuntime` for the next request. This preserves the existing Logma lifecycle/control-plane semantics and avoids conflating concurrent request streams under the process-level instance ID.

For a retained Farcaster sandbox, Redis/Logma remain shared infrastructure. A transient `stonks` deployment owns only its own process/container and request runtime; it does not own or replace the retained Farcaster host or canonical Redis/Logma services.

## Configuration

Redis uses the shared Logma convention:

- `REDIS_URI`
- `REDIS_SOCKET` when using a Unix socket
- `REDISCLI_AUTH`

When Redis connection values are deliberately not supplied by the environment, Logma serverless can resolve them from its existing request headers.

Alpaca authentication uses:

- `ALPACA_API_KEY_ID` in the service environment
- `X-Alpaca-Api-Key-Id` on each request
- `X-Alpaca-Api-Secret-Key` when the container does not already hold the secret in its in-memory credential cache

The Alpaca secret is not added to the SSE payload or Redis market messages.

`router.NewRouter()` remains a plain `http.Handler` suitable for the existing Go serverless/container shells.

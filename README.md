# stonks

`stonks` owns one retained/shared Alpaca publisher per running service instance and composes Logma's `serverless` package for request-scoped Redis-to-SSE fanout.

The live path is:

```text
                         Alpaca
                           |
                           v
                 shared stonks publisher
                           |
                           v
                 Farcaster Redis / Logma
                    /          |          \
                   /           |           \
                  v            v            v
          /stream requester  /stream     /stream
              SSE A           SSE B       SSE C
```

The publisher is process/service-owned. An HTTP requester never owns the Alpaca connection, so disconnecting one SSE client removes only that client's Redis subscriptions. Other requesters and the publisher remain active.

`POST /stream` accepts:

```json
{
  "tickers": ["AAPL", "SPY"],
  "feed": "iex",
  "subscriptions": ["trades", "quotes", "bars"]
}
```

`feed` and `subscriptions` are optional. The default subscription is `bars`. Supported subscriptions are `trades`, `quotes`, `bars`, and `dailybars`.

`combined_channels` remains accepted for request compatibility but does not alter the shared publisher's Redis topology. A shared publisher must expose canonical per-symbol channels so independent requesters can ask for overlapping but different symbol sets without changing one another's channel identity.

The response is `text/event-stream`:

```text
event: message
data: {"channel":"stonks:quote:AAPL:<instance>","data":{...}}
```

## Shared publisher lifecycle

The first accepted requester prepares the process-owned publisher but does not immediately start Alpaca. Its Logma Redis subscriptions are created first and must receive Redis subscription acknowledgements. Only then is the shared Alpaca publisher started. This avoids losing the first live publication because Redis Pub/Sub has no replay.

After the publisher is active, later requesters:

1. select ticker/event-type channels;
2. create their own Logma Redis subscriptions;
3. wait until those subscriptions are ready;
4. ask the existing Alpaca connection to add any missing symbol/type subscriptions;
5. receive SSE until they disconnect.

Requested ticker/type additions are serialized and deduplicated. Asking for a ticker already present does not create another Alpaca publisher or duplicate the retained subscription.

The Alpaca feed is connection-wide. The first live publisher fixes the service instance to `iex` or `sip`; a later requester asking for the other feed receives a conflict rather than silently receiving data from the wrong feed.

If one SSE requester cannot keep up with its bounded request buffer, only that request is cancelled. The retained publisher and other requesters remain unaffected.

## Channel shape

The shared publisher always publishes canonical per-symbol channels:

```text
stonks:trade:AAPL:<instanceID>
stonks:quote:AAPL:<instanceID>
stonks:bar:SPY:<instanceID>
```

`STONKS_GLOBAL_CHANNELS=true` changes the final scope to `:global`. Keep the instance-scoped default for reusable or potentially multi-instance deployments.

Redis Pub/Sub naturally fans one publication out to every active subscriber, so one Stonks publisher can feed the Python screener, a GitHub qualification Action, diagnostics, and other SSE consumers simultaneously without opening separate Alpaca connections.

## Farcaster ownership

In the retained Farcaster sandbox:

- Redis/Logma remain retained shared infrastructure;
- the Stonks service/container owns the retained publisher for its own service lifetime;
- `/stream` requesters own only their request-scoped Logma subscribers;
- requester disconnect does not retire the Stonks publisher;
- child qualification activity does not own or replace the retained Farcaster host or canonical Redis/Logma runtime.

Service/container retirement remains a deployment/lifecycle concern and should follow the existing Huram Abi exact deployment/state ownership rules rather than being inferred from whether SSE clients are currently connected.

## Configuration

Redis uses the shared Logma convention:

- `REDIS_URI`
- `REDIS_SOCKET` when using a Unix socket
- `REDISCLI_AUTH`

When Redis connection values are deliberately not supplied by the environment, Logma serverless can resolve them from its existing request headers.

Alpaca authentication uses:

- `ALPACA_API_KEY_ID` in the service environment
- `X-Alpaca-Api-Key-Id` on each request
- `X-Alpaca-Api-Secret-Key` when the service does not already hold the secret in its in-memory credential cache

The Alpaca secret is never added to Redis market events or SSE payloads.

`router.NewRouter()` remains a plain `http.Handler` suitable for the existing Go serverless/container shells.

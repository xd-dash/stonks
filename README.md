stonks!

Streams Alpaca market data (trades/quotes/bars) for tickers given in a
`POST /stream` request body, and publishes each event onto a Redis
channel instead of returning it over the same HTTP connection. See
`router/` for the implementation:

- `POST /stream` — body: `{"tickers": ["AAPL","SPY"], "feed": "iex",
  "subscriptions": ["trades","quotes","bars"]}` (`feed` and
  `subscriptions` are optional). The request blocks for the life of the
  stream, ending on client disconnect, a `stonks:control:shutdown`
  publish, or a terminal Alpaca error.
- Events are published to `stonks:<type>:<SYMBOL>`, e.g.
  `stonks:trade:AAPL`, `stonks:quote:AAPL`, `stonks:bar:SPY`,
  `stonks:dailybar:SPY` — deterministic from the request, so a consumer
  doesn't need a response from stonks before subscribing.
- Consumers watch that data by connecting to
  [logma-serverless](https://github.com/xd-dash/logma-serverless), which
  subscribes to Redis channels and fans them out over SSE. stonks is
  never itself an SSE endpoint.

Configuration is entirely environment-based, never part of the request
body:

- `REDIS_URI` / `REDISCLI_AUTH` — Redis connection (shared convention
  with logma-serverless, via `github.com/xd-dash/logma-serverless/pubsub`).
- `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` — Alpaca credentials,
  passed explicitly to the SDK via `stream.WithCredentials`.

`router.NewRouter()` returns a plain `http.Handler` — the `func
NewRouter() http.Handler` contract [gospace-minimal](https://github.com/dash-xd/gospace-minimal)
expects for its Cloud Functions Gen 2 shell.

stonks!

Streams Alpaca market data (trades/quotes/bars) for tickers given in a
`POST /stream` request body, and publishes each event onto a Redis
channel instead of returning it over the same HTTP connection. See
`router/` for the implementation:

- `POST /stream` — body: `{"tickers": ["AAPL","SPY"], "feed": "iex",
  "subscriptions": ["trades","quotes","bars"], "combined_channels":
  ["quotes"]}` (`feed`, `subscriptions`, and `combined_channels` are all
  optional). The request blocks for the life of the stream, ending on
  client disconnect, a `stonks:control:shutdown` publish, or a terminal
  Alpaca error.
- `subscriptions` picks which of four event types to stream, each with
  different timing: `trades` (every executed trade print), `quotes`
  (every NBBO best-bid/ask change — can fire far more often than once a
  second for a liquid ticker), `bars` (one aggregated OHLCV candle per
  ticker per minute — the only one that's actually interval-based), and
  `dailybars` (one candle per ticker per day). Omitting `subscriptions`
  defaults to `["bars"]`.
- By default, each (type, ticker) pair gets its own channel:
  `stonks:<type>:<SYMBOL>`, e.g. `stonks:trade:AAPL`, `stonks:quote:AAPL`,
  `stonks:bar:SPY`, `stonks:dailybar:SPY`. Listing a type in
  `combined_channels` makes every requested ticker's events for that type
  land on one shared channel instead: `stonks:<type>:combined:<SYMBOL1>:
  <SYMBOL2>:...` (tickers sorted alphabetically), e.g.
  `{"tickers":["AAPL","MSFT"],"subscriptions":["quotes"],
  "combined_channels":["quotes"]}` publishes every quote for both tickers
  to `stonks:quote:combined:AAPL:MSFT`. Either way, the channel name is
  further suffixed with `:<instanceID>` — see below.
- Every channel is scoped to the specific container instance producing
  it (`:<instanceID>` appended, e.g. `stonks:quote:AAPL:<instanceID>`),
  since more than one container can be streaming the same ticker/type at
  once and a subscriber needs to tell the resulting streams apart. A
  consumer discovers which instance IDs are currently live via the
  `instance:stonks:<instanceID>:<requestID>` Redis hash stonks writes at
  stream start (see `pubsub.RegisterInvocation` in
  `github.com/xd-dash/logma-serverless/pubsub`).
- Consumers watch that data by connecting to
  [logma-serverless](https://github.com/xd-dash/logma-serverless), which
  subscribes to Redis channels and fans them out over SSE. stonks is
  never itself an SSE endpoint.

Configuration is entirely environment-based, never part of the request
body:

- `REDIS_URI` / `REDISCLI_AUTH` — Redis connection (shared convention
  with logma-serverless, via `github.com/xd-dash/logma-serverless/pubsub`).
- `ALPACA_API_KEY_ID` — the non-secret Alpaca API key, still a
  GitHub-secret-backed env var. The Alpaca *secret* key is never an env
  var or GitHub secret at all — see below.

Every `POST /stream` must also authenticate itself with two headers,
checked by router-level middleware before the request body is ever read:

- `X-Alpaca-Api-Key-Id` — must equal the `ALPACA_API_KEY_ID` env var, on
  every request.
- `X-Alpaca-Api-Secret-Key` — the Alpaca secret key. Only required on the
  first request since this container instance started (or since Alpaca
  last rejected the cached secret) — stonks caches it in memory for the
  rest of that container's lifetime, so later requests only need to
  repeat `X-Alpaca-Api-Key-Id`. A request missing a required header, or
  sending the wrong API key, gets `401 Unauthorized`.

`router.NewRouter()` returns a plain `http.Handler` — the `func
NewRouter() http.Handler` contract [gospace-minimal](https://github.com/dash-xd/gospace-minimal)
expects for its Cloud Functions Gen 2 shell.

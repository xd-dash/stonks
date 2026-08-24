#!/usr/bin/env bash
#
# End-to-end smoke test for a deployed stonks router *plus* the
# xd-dash/logma-serverless instance that watches it. stonks never serves
# SSE itself (see its README) -- it only publishes market data onto Redis
# channels named stonks:<type>:<SYMBOL>:<instanceID> (or, for a combined
# subscription, stonks:<type>:combined:<SYMBOL1>:<SYMBOL2>:...:<instanceID>).
# A consumer has to go through a paired logma-serverless deployment's GET
# /events to actually see that data as SSE.
#
# The instanceID segment is chosen by stonks itself at stream start (its
# Cloud Run container hostname -- see gospace-minimal's InstanceID) and
# can't be predicted ahead of time, so by default this script discovers it
# the same way any real consumer has to: read the
# instance:<K_SERVICE>:<instanceID>:<requestID> Redis hash stonks writes
# via pubsub.RegisterInvocation as soon as its stream starts (see
# router/runtime.go's Configure -> Start -> pubsub.Runtime.Run), then
# passes the resulting fully-qualified channel names to logma-serverless's
# GET /events?channel=... . With --global-channels (stonks deployed with
# STONKS_GLOBAL_CHANNELS=true -- see router/runtime.go's publishChannel),
# the channel suffix is the literal "global" instead of a discovered
# instance ID, so this whole discovery step -- and Redis access generally
# -- is skipped entirely.
#
# Requires this deployment pairing to be run through huram-abi's
# deploy-stonks-router workflow with deploy_logma_serverless left on (the
# default) -- see xd-dash/huram-abi/.github/workflows/deploy-stonks-router.yml.
#
# Usage:
#   ALPACA_API_KEY_ID=... ALPACA_API_SECRET_KEY=... \
#   REDIS_URI=host:port REDISCLI_AUTH=... \
#   ./scripts/test-stream-with-logma.sh
#
# Env vars:
#   PROJECT_ID                GCP project (default: gcloud config get-value project)
#   REGION                    GCP region (default: us-central1)
#   STONKS_SERVICE_NAME       stonks Cloud Run service name (default: stonks-router)
#   LOGMA_SERVICE_NAME        paired logma-serverless Cloud Run service name
#                             (default: stonks-logma-serverless -- matches
#                             deploy-stonks-router's logma_function_name default)
#   ALPACA_API_KEY_ID         Must match the deployed stonks function's ALPACA_API_KEY_ID.
#   ALPACA_API_SECRET_KEY     Real Alpaca secret key, for the actual stream request.
#   REDIS_URI                 host:port of the same Redis instance both functions share.
#                             Required to discover stonks's live instance ID.
#   REDISCLI_AUTH             Redis auth password/token, read natively by redis-cli
#                             (same env var name the deployed functions themselves use).
#   STREAM_TICKERS            Comma-separated tickers to stream (default: AAPL).
#   STREAM_SUBSCRIPTIONS      Comma-separated subscription types (default: trades,quotes).
#   DISCOVERY_TIMEOUT_SECONDS How long to wait for stonks's instance-registration
#                             key to show up in Redis (default: 20).
#   SSE_TIMEOUT_SECONDS       How long to hold the logma-serverless GET /events
#                             connection open collecting events (default: 20).
#
# Flags:
#   --stonks-public     Skip the Authorization: Bearer header when calling
#                       stonks (it was deployed with allow_unauthenticated=true).
#                       logma-serverless never needs this: deploy-logma-serverless
#                       always deploys it with --allow-unauthenticated (auth is
#                       handled at the application layer there, not GCP IAM).
#   --global-channels   stonks was deployed with STONKS_GLOBAL_CHANNELS=true,
#                       so its channels end in :global instead of a live,
#                       only-discoverable-after-the-fact instance ID -- skips
#                       the Redis instance-discovery step entirely and
#                       computes channel names directly from
#                       STREAM_TICKERS/STREAM_SUBSCRIPTIONS. REDIS_URI/
#                       REDISCLI_AUTH and redis-cli aren't needed in this mode.
#   -h, --help
#
# Caveats:
#   - trades/quotes only fire when the market is actually producing them.
#     Outside market hours (or for an illiquid ticker), the connection can
#     legitimately stay open with zero events -- this script reports that
#     as INFO, not FAIL. Only a failed/rejected connection, or the instance
#     discovery step timing out, is treated as a hard failure.
#   - A container already warmed by an earlier run reuses the same
#     instance ID; the discovery step only looks for a *new* invocation key
#     (diffed against a baseline snapshot), so it still finds the right one.

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
STONKS_SERVICE_NAME="${STONKS_SERVICE_NAME:-stonks-router}"
LOGMA_SERVICE_NAME="${LOGMA_SERVICE_NAME:-stonks-logma-serverless}"
STREAM_TICKERS="${STREAM_TICKERS:-AAPL}"
STREAM_SUBSCRIPTIONS="${STREAM_SUBSCRIPTIONS:-trades,quotes}"
DISCOVERY_TIMEOUT_SECONDS="${DISCOVERY_TIMEOUT_SECONDS:-20}"
SSE_TIMEOUT_SECONDS="${SSE_TIMEOUT_SECONDS:-20}"

STONKS_PUBLIC=false
GLOBAL_CHANNELS=false

usage() {
    echo "Usage: $0 [--stonks-public] [--global-channels]"
    echo
    echo "  --stonks-public    Skip stonks's Authorization: Bearer header"
    echo "                     (it was deployed with allow_unauthenticated=true)."
    echo "  --global-channels  stonks was deployed with STONKS_GLOBAL_CHANNELS=true;"
    echo "                     skip Redis instance discovery and compute channel"
    echo "                     names directly (no REDIS_URI/redis-cli needed)."
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stonks-public)
            STONKS_PUBLIC=true
            shift
            ;;
        --global-channels)
            GLOBAL_CHANNELS=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

required_vars=(ALPACA_API_KEY_ID ALPACA_API_SECRET_KEY)
if [[ "$GLOBAL_CHANNELS" != true ]]; then
    required_vars+=(REDIS_URI)
fi
for required in "${required_vars[@]}"; do
    if [[ -z "${!required:-}" ]]; then
        echo "ERROR: $required must be set." >&2
        exit 1
    fi
done

tools=(jq curl gcloud)
if [[ "$GLOBAL_CHANNELS" != true ]]; then
    tools+=(redis-cli)
fi
for tool in "${tools[@]}"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "ERROR: $tool is required on PATH." >&2
        exit 1
    fi
done

REDIS_HOST="${REDIS_URI%%:*}"
REDIS_PORT="${REDIS_URI##*:}"

redis_cli() {
    # REDISCLI_AUTH, if set, is read natively by redis-cli itself -- no
    # need to pass it as a flag (and doing so would leak it into `ps`).
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

STONKS_URL="$(
    gcloud run services describe "$STONKS_SERVICE_NAME" \
        --project "$PROJECT_ID" --region "$REGION" \
        --format='value(status.url)'
)"
LOGMA_URL="$(
    gcloud run services describe "$LOGMA_SERVICE_NAME" \
        --project "$PROJECT_ID" --region "$REGION" \
        --format='value(status.url)'
)"

if [[ -z "$STONKS_URL" || -z "$LOGMA_URL" ]]; then
    echo "Could not determine Cloud Run service URL(s) for $STONKS_SERVICE_NAME / $LOGMA_SERVICE_NAME." >&2
    exit 1
fi

echo "Project:       $PROJECT_ID"
echo "Region:        $REGION"
echo "stonks:        $STONKS_SERVICE_NAME ($STONKS_URL)"
echo "logma-serverless: $LOGMA_SERVICE_NAME ($LOGMA_URL)"
echo

STONKS_AUTH_ARGS=()
if [[ "$STONKS_PUBLIC" != true ]]; then
    stonks_identity_token="$(gcloud auth print-identity-token --audiences="$STONKS_URL")"
    STONKS_AUTH_ARGS=(--header "Authorization: Bearer ${stonks_identity_token}")
fi

WORKDIR="$(mktemp -d)"
STONKS_PID=""
cleanup() {
    if [[ -n "$STONKS_PID" ]] && kill -0 "$STONKS_PID" 2>/dev/null; then
        kill "$STONKS_PID" 2>/dev/null || true
        wait "$STONKS_PID" 2>/dev/null || true
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# --- snapshot instance-registration keys before starting a new stream ---
if [[ "$GLOBAL_CHANNELS" != true ]]; then
    BASELINE_KEYS="$(redis_cli --scan --pattern "instance:${STONKS_SERVICE_NAME}:*" | sort)"
fi

# --- build the /stream request body ---
tickers_json="$(printf '%s\n' "${STREAM_TICKERS//,/$'\n'}" | jq -R . | jq -sc .)"
subscriptions_json="$(printf '%s\n' "${STREAM_SUBSCRIPTIONS//,/$'\n'}" | jq -R . | jq -sc .)"
stream_body="$(jq -nc --argjson tickers "$tickers_json" --argjson subscriptions "$subscriptions_json" \
    '{tickers: $tickers, subscriptions: $subscriptions}')"

echo "--- starting stonks stream (tickers=${STREAM_TICKERS} subscriptions=${STREAM_SUBSCRIPTIONS}) ---"

# POST /stream only responds once the stream itself stops (see
# router/handlers.go), so it's started in the background and left running
# for the life of this test; cleanup() above kills it on exit.
curl --silent --show-error --output "${WORKDIR}/stonks-response.log" \
    "${STONKS_AUTH_ARGS[@]}" \
    --request POST "${STONKS_URL}/stream" \
    --header "Content-Type: application/json" \
    --header "X-Alpaca-Api-Key-Id: ${ALPACA_API_KEY_ID}" \
    --header "X-Alpaca-Api-Secret-Key: ${ALPACA_API_SECRET_KEY}" \
    --data "$stream_body" &
STONKS_PID=$!

# --- discover the instance ID this stream just registered ---
CHANNEL_SUFFIX=""
if [[ "$GLOBAL_CHANNELS" == true ]]; then
    echo "--- STONKS_GLOBAL_CHANNELS mode: skipping instance discovery ---"
    CHANNEL_SUFFIX="global"
else
    echo "--- discovering stonks instance ID (up to ${DISCOVERY_TIMEOUT_SECONDS}s) ---"

    deadline=$((SECONDS + DISCOVERY_TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
        if ! kill -0 "$STONKS_PID" 2>/dev/null; then
            echo "FAIL: stonks stream exited before an instance ID could be discovered:" >&2
            cat "${WORKDIR}/stonks-response.log" >&2
            exit 1
        fi

        current_keys="$(redis_cli --scan --pattern "instance:${STONKS_SERVICE_NAME}:*" | sort)"
        new_key="$(comm -13 <(printf '%s\n' "$BASELINE_KEYS") <(printf '%s\n' "$current_keys") | head -n1)"
        if [[ -n "$new_key" ]]; then
            CHANNEL_SUFFIX="$(redis_cli HGET "$new_key" instance_id)"
            break
        fi
        sleep 1
    done

    if [[ -z "$CHANNEL_SUFFIX" ]]; then
        echo "FAIL: no new instance:${STONKS_SERVICE_NAME}:* key appeared within ${DISCOVERY_TIMEOUT_SECONDS}s." >&2
        exit 1
    fi
    echo "PASS: discovered stonks instance ID: ${CHANNEL_SUFFIX}"
fi

# --- derive the exact channel names stonks publishes to (router/subscriptions.go) ---
# Built into a query string directly (percent-encoded via jq's @uri, no
# network call involved) rather than handed to curl as repeated
# --data-urlencode args: logma-serverless's GET /events is the real SSE
# endpoint itself (single-concurrency per container), so anything that
# would touch it here would race with the actual request below.
query=""
IFS=',' read -ra sub_types <<<"$STREAM_SUBSCRIPTIONS"
IFS=',' read -ra tickers <<<"$STREAM_TICKERS"
for sub in "${sub_types[@]}"; do
    case "$sub" in
        trades) channel_type="trade" ;;
        quotes) channel_type="quote" ;;
        bars) channel_type="bar" ;;
        dailybars) channel_type="dailybar" ;;
        *) echo "WARN: unrecognized subscription type '$sub', skipping" >&2; continue ;;
    esac
    for ticker in "${tickers[@]}"; do
        channel="stonks:${channel_type}:${ticker}:${CHANNEL_SUFFIX}"
        encoded_channel="$(jq -rn --arg c "$channel" '$c|@uri')"
        query="${query}&channel=${encoded_channel}"
        echo "watching channel: ${channel}"
    done
done
query="${query#&}"
events_url="${LOGMA_URL}/events?${query}"

# --- subscribe via logma-serverless's SSE endpoint and collect events ---
echo
echo "--- collecting SSE events from logma-serverless for ${SSE_TIMEOUT_SECONDS}s ---"

set +e
timeout "${SSE_TIMEOUT_SECONDS}" curl --silent --show-error --no-buffer \
    "$events_url" >"${WORKDIR}/sse-events.log" 2>"${WORKDIR}/sse-curl.err"
curl_exit=$?
set -e

# 124 == GNU coreutils `timeout` killed curl after SSE_TIMEOUT_SECONDS,
# which is the expected outcome for a connection meant to stay open --
# curl itself never returns a non-timeout error in that case.
if [[ "$curl_exit" -ne 0 && "$curl_exit" -ne 124 ]]; then
    echo "FAIL: connecting to logma-serverless's GET /events failed (curl exit $curl_exit):" >&2
    cat "${WORKDIR}/sse-curl.err" >&2
    exit 1
fi

if ! grep -q ": connected" "${WORKDIR}/sse-events.log"; then
    echo "FAIL: never received the SSE connection prologue from logma-serverless." >&2
    cat "${WORKDIR}/sse-events.log" >&2
    exit 1
fi
echo "PASS: SSE connection to logma-serverless established"

event_count="$(grep -c '^event: message$' "${WORKDIR}/sse-events.log" || true)"
if [[ "$event_count" -gt 0 ]]; then
    echo "PASS: received ${event_count} market-data event(s) over SSE"
    echo
    echo "Sample event:"
    grep -A1 '^event: message$' "${WORKDIR}/sse-events.log" | head -n2
else
    echo "INFO: connection stayed open but no market-data events arrived in ${SSE_TIMEOUT_SECONDS}s."
    echo "      This can be expected outside market hours or for an illiquid ticker -- it is not"
    echo "      treated as a failure on its own."
fi

echo
echo "All hard checks passed."

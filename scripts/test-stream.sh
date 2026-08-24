#!/usr/bin/env bash
#
# Smoke-tests a deployed stonks router (see
# xd-dash/huram-abi/.github/workflows/deploy-stonks-router.yml) by hitting
# its POST /stream endpoint with a series of requests that should be
# rejected, plus one that should be accepted and start streaming.
#
# stonks never responds to POST /stream until the stream itself stops
# (client disconnect, stonks:control:shutdown, or a terminal Alpaca
# error - see router/handlers.go), so the one "should succeed" case can't
# be checked by waiting for a response: instead, it's sent with a short
# client-side timeout and treated as a pass if curl times out (exit 28)
# rather than getting an immediate error response, which would mean
# stonks accepted the request and is now blocked streaming, exactly as
# expected.
#
# Usage:
#   ALPACA_API_KEY_ID=... ALPACA_API_SECRET_KEY=... ./scripts/test-stream.sh
#
# Env vars:
#   PROJECT_ID              GCP project (default: gcloud config get-value project)
#   SERVICE_NAME             Cloud Run service name (default: stonks-router)
#   REGION                   GCP region (default: us-central1)
#   ALPACA_API_KEY_ID        Must match the deployed function's ALPACA_API_KEY_ID.
#                            Required for anything beyond the two
#                            always-reject tests (missing/wrong header).
#   ALPACA_API_SECRET_KEY    Real Alpaca secret key. Required for the
#                            request-body-validation tests and the final
#                            live-stream test; those are skipped without it.
#   STREAM_TICKERS           Comma-separated tickers for the live-stream
#                            test (default: AAPL).
#   STREAM_TIMEOUT_SECONDS   How long to hold the live-stream request open
#                            before giving up and treating a still-blocked
#                            connection as a pass (default: 5).
#
# Flags:
#   --public      Skip the Authorization: Bearer header (target was
#                 deployed with allow_unauthenticated=true).
#   -h, --help    Show this usage and exit.

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
SERVICE_NAME="${SERVICE_NAME:-stonks-router}"
REGION="${REGION:-us-central1}"
STREAM_TICKERS="${STREAM_TICKERS:-AAPL}"
STREAM_TIMEOUT_SECONDS="${STREAM_TIMEOUT_SECONDS:-5}"

PUBLIC=false

usage() {
    echo "Usage: $0 [--public]"
    echo
    echo "  --public    Skip the Authorization: Bearer header (target was"
    echo "              deployed with allow_unauthenticated=true)."
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --public)
            PUBLIC=true
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

SERVICE_URL="$(
    gcloud run services describe "$SERVICE_NAME" \
        --project "$PROJECT_ID" \
        --region "$REGION" \
        --format='value(status.url)'
)"

if [[ -z "$SERVICE_URL" ]]; then
    echo "Could not determine Cloud Run service URL." >&2
    exit 1
fi

echo "Project:      $PROJECT_ID"
echo "Service:      $SERVICE_NAME"
echo "Region:       $REGION"
echo "Service URL:  $SERVICE_URL"
echo

AUTH_HEADER_ARGS=()
if [[ "$PUBLIC" != true ]]; then
    identity_token="$(gcloud auth print-identity-token --audiences="$SERVICE_URL")"
    AUTH_HEADER_ARGS=(--header "Authorization: Bearer ${identity_token}")
fi

FAILURES=0

# expect_status DESCRIPTION EXPECTED_HTTP_STATUS -- curl-args...
expect_status() {
    local desc="$1" expected="$2"
    shift 2

    local status
    status="$(
        curl --silent --output /dev/null --write-out '%{http_code}' \
            "${AUTH_HEADER_ARGS[@]}" "$@"
    )"

    if [[ "$status" == "$expected" ]]; then
        echo "PASS: $desc (HTTP $status)"
    else
        echo "FAIL: $desc (expected HTTP $expected, got HTTP $status)" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "--- auth checks (no real Alpaca credentials required) ---"

expect_status "missing X-Alpaca-Api-Key-Id is rejected" 401 \
    --request POST "${SERVICE_URL}/stream" \
    --header "Content-Type: application/json" \
    --data '{"tickers":["AAPL"]}'

expect_status "wrong X-Alpaca-Api-Key-Id is rejected" 401 \
    --request POST "${SERVICE_URL}/stream" \
    --header "Content-Type: application/json" \
    --header "X-Alpaca-Api-Key-Id: definitely-not-the-configured-key" \
    --data '{"tickers":["AAPL"]}'

if [[ -z "${ALPACA_API_KEY_ID:-}" ]]; then
    echo
    echo "ALPACA_API_KEY_ID not set - skipping request-validation and live-stream tests."
    echo "(they need the real key configured on the deployed function to get past auth)"
else
    echo
    echo "--- request validation (needs ALPACA_API_KEY_ID) ---"

    # First request since this container last started (or last had its
    # cached secret cleared) must also supply X-Alpaca-Api-Secret-Key.
    # This is only a reliable test against a cold container - a container
    # already warmed by an earlier successful request would have this
    # secret cached and wouldn't require the header again. Treat a
    # different outcome here as informational, not a hard failure.
    status="$(
        curl --silent --output /dev/null --write-out '%{http_code}' \
            "${AUTH_HEADER_ARGS[@]}" \
            --request POST "${SERVICE_URL}/stream" \
            --header "Content-Type: application/json" \
            --header "X-Alpaca-Api-Key-Id: ${ALPACA_API_KEY_ID}" \
            --data '{"tickers":["AAPL"]}'
    )"
    echo "INFO: correct key id, no secret header -> HTTP $status (401 expected only on a cold container; a warm one may already have a cached secret)"

    if [[ -z "${ALPACA_API_SECRET_KEY:-}" ]]; then
        echo
        echo "ALPACA_API_SECRET_KEY not set - skipping body-validation and live-stream tests."
    else
        expect_status "malformed JSON body is rejected" 400 \
            --request POST "${SERVICE_URL}/stream" \
            --header "Content-Type: application/json" \
            --header "X-Alpaca-Api-Key-Id: ${ALPACA_API_KEY_ID}" \
            --header "X-Alpaca-Api-Secret-Key: ${ALPACA_API_SECRET_KEY}" \
            --data '{not valid json'

        expect_status "empty tickers is rejected" 400 \
            --request POST "${SERVICE_URL}/stream" \
            --header "Content-Type: application/json" \
            --header "X-Alpaca-Api-Key-Id: ${ALPACA_API_KEY_ID}" \
            --header "X-Alpaca-Api-Secret-Key: ${ALPACA_API_SECRET_KEY}" \
            --data '{"tickers":[]}'

        expect_status "unsupported subscription is rejected" 400 \
            --request POST "${SERVICE_URL}/stream" \
            --header "Content-Type: application/json" \
            --header "X-Alpaca-Api-Key-Id: ${ALPACA_API_KEY_ID}" \
            --header "X-Alpaca-Api-Secret-Key: ${ALPACA_API_SECRET_KEY}" \
            --data '{"tickers":["AAPL"],"subscriptions":["not-a-real-type"]}'

        echo
        echo "--- live stream accept (needs ALPACA_API_SECRET_KEY, holds for ${STREAM_TIMEOUT_SECONDS}s) ---"

        tickers_json="$(
            IFS=',' read -ra syms <<<"$STREAM_TICKERS"
            printf '%s\n' "${syms[@]}" | jq -R . | jq -sc .
        )"

        set +e
        curl --silent --show-error --output /dev/null \
            --max-time "$STREAM_TIMEOUT_SECONDS" \
            "${AUTH_HEADER_ARGS[@]}" \
            --request POST "${SERVICE_URL}/stream" \
            --header "Content-Type: application/json" \
            --header "X-Alpaca-Api-Key-Id: ${ALPACA_API_KEY_ID}" \
            --header "X-Alpaca-Api-Secret-Key: ${ALPACA_API_SECRET_KEY}" \
            --data "$(jq -nc --argjson tickers "$tickers_json" '{tickers: $tickers, subscriptions: ["trades","quotes"]}')"
        curl_exit=$?
        set -e

        # 28 == CURLE_OPERATION_TIMEDOUT: the connection was accepted and
        # is still blocked streaming when curl gave up waiting, which is
        # the success case for a request that only responds once the
        # stream itself stops. Any other outcome means stonks rejected or
        # errored out on the request well before that.
        if [[ "$curl_exit" -eq 28 ]]; then
            echo "PASS: stream request was accepted and is still streaming after ${STREAM_TIMEOUT_SECONDS}s"
        else
            echo "FAIL: stream request did not stay open (curl exit code $curl_exit, expected 28/timeout)" >&2
            FAILURES=$((FAILURES + 1))
        fi
    fi
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All checks passed."
    exit 0
else
    echo "$FAILURES check(s) failed." >&2
    exit 1
fi

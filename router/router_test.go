package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewRouterHasNoRootRoute(t *testing.T) {
	r := NewRouter()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /, got %d", rec.Code)
	}
}

func newAuthenticatedStreamRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	t.Setenv("ALPACA_API_KEY_ID", "test-key")

	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(body))
	req.Header.Set(headerAPIKeyID, "test-key")
	req.Header.Set(headerAPISecretKey, "test-secret")
	return req
}

func TestStreamHandlerRejectsInvalidJSON(t *testing.T) {
	r := NewRouter()

	req := newAuthenticatedStreamRequest(t, "not json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON body, got %d", rec.Code)
	}
}

func TestStreamHandlerRejectsEmptyTickers(t *testing.T) {
	r := NewRouter()

	req := newAuthenticatedStreamRequest(t, `{"tickers":[]}`)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty tickers, got %d", rec.Code)
	}
}

func TestStreamHandlerRejectsUnsupportedFeed(t *testing.T) {
	r := NewRouter()

	req := newAuthenticatedStreamRequest(t, `{"tickers":["AAPL"],"feed":"nasdaq"}`)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an unsupported feed, got %d", rec.Code)
	}
}

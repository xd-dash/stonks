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

func TestStreamHandlerRejectsInvalidJSON(t *testing.T) {
	r := NewRouter()

	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader("not json"))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON body, got %d", rec.Code)
	}
}

func TestStreamHandlerRejectsEmptyTickers(t *testing.T) {
	r := NewRouter()

	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`{"tickers":[]}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty tickers, got %d", rec.Code)
	}
}

func TestStreamHandlerRejectsUnsupportedFeed(t *testing.T) {
	r := NewRouter()

	req := httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(`{"tickers":["AAPL"],"feed":"nasdaq"}`))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an unsupported feed, got %d", rec.Code)
	}
}

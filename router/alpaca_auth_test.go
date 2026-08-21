package router

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func passThroughHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestRequireAlpacaAuthRejectsMissingAPIKeyHeader(t *testing.T) {
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	creds := &alpacaCredentials{}
	handler := requireAlpacaAuth(creds)(passThroughHandler())

	req := httptest.NewRequest(http.MethodPost, "/stream", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing %s, got %d", headerAPIKeyID, rec.Code)
	}
}

func TestRequireAlpacaAuthRejectsWrongAPIKeyHeader(t *testing.T) {
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	creds := &alpacaCredentials{}
	handler := requireAlpacaAuth(creds)(passThroughHandler())

	req := httptest.NewRequest(http.MethodPost, "/stream", nil)
	req.Header.Set(headerAPIKeyID, "wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong %s, got %d", headerAPIKeyID, rec.Code)
	}
}

func TestRequireAlpacaAuthRejectsMissingSecretOnFirstRequest(t *testing.T) {
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	creds := &alpacaCredentials{}
	handler := requireAlpacaAuth(creds)(passThroughHandler())

	req := httptest.NewRequest(http.MethodPost, "/stream", nil)
	req.Header.Set(headerAPIKeyID, "test-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing %s on first request, got %d", headerAPISecretKey, rec.Code)
	}
}

func TestRequireAlpacaAuthAcceptsFirstRequestAndAttachesSecretToContext(t *testing.T) {
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	creds := &alpacaCredentials{}

	var gotSecret string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSecret = alpacaSecretFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := requireAlpacaAuth(creds)(next)

	req := httptest.NewRequest(http.MethodPost, "/stream", nil)
	req.Header.Set(headerAPIKeyID, "test-key")
	req.Header.Set(headerAPISecretKey, "test-secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotSecret != "test-secret" {
		t.Fatalf("expected secret %q in context, got %q", "test-secret", gotSecret)
	}
}

func TestRequireAlpacaAuthReusesCachedSecretOnSubsequentRequest(t *testing.T) {
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	creds := &alpacaCredentials{}

	var gotSecret string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSecret = alpacaSecretFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := requireAlpacaAuth(creds)(next)

	first := httptest.NewRequest(http.MethodPost, "/stream", nil)
	first.Header.Set(headerAPIKeyID, "test-key")
	first.Header.Set(headerAPISecretKey, "test-secret")
	handler.ServeHTTP(httptest.NewRecorder(), first)

	second := httptest.NewRequest(http.MethodPost, "/stream", nil)
	second.Header.Set(headerAPIKeyID, "test-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, second)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 reusing cached secret, got %d", rec.Code)
	}
	if gotSecret != "test-secret" {
		t.Fatalf("expected cached secret %q, got %q", "test-secret", gotSecret)
	}
}

func TestRequireAlpacaAuthRequiresSecretAgainAfterClear(t *testing.T) {
	t.Setenv("ALPACA_API_KEY_ID", "test-key")
	creds := &alpacaCredentials{}
	handler := requireAlpacaAuth(creds)(passThroughHandler())

	first := httptest.NewRequest(http.MethodPost, "/stream", nil)
	first.Header.Set(headerAPIKeyID, "test-key")
	first.Header.Set(headerAPISecretKey, "test-secret")
	handler.ServeHTTP(httptest.NewRecorder(), first)

	creds.clear()

	second := httptest.NewRequest(http.MethodPost, "/stream", nil)
	second.Header.Set(headerAPIKeyID, "test-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, second)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 requiring %s again after clear, got %d", headerAPISecretKey, rec.Code)
	}
}

package router

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5/middleware"
)

// publisherManager owns the one retained/shared publisher runtime for this
// stonks process. Request handlers may attach/detach freely; cancelling a
// request never cancels this manager's background context.
type publisherManager struct {
	mu      sync.Mutex
	creds   *alpacaCredentials
	runtime *StonksRuntime
	started bool
	cancel  context.CancelFunc
}

func newPublisherManager(creds *alpacaCredentials) *publisherManager {
	return &publisherManager{creds: creds}
}

// prepare returns the process-owned publisher, creating and configuring it
// from the first request if necessary. It deliberately does not start Alpaca;
// the caller first makes its Redis subscriptions ready, then calls activate.
func (m *publisherManager) prepare(r *http.Request, cfg streamConfig, secret string) *StonksRuntime {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runtime != nil {
		select {
		case <-m.runtime.Done():
			m.runtime = nil
			m.started = false
			if m.cancel != nil {
				m.cancel()
				m.cancel = nil
			}
		default:
		}
	}
	if m.runtime != nil {
		return m.runtime
	}

	rt := NewStonksRuntime(m.creds)
	rt.RecordInvocation(r, "publisher-"+middleware.GetReqID(r.Context()))
	rt.Configure(cfg, secret)
	m.runtime = rt
	return rt
}

// activate starts the retained publisher exactly once and then ensures the
// request's desired symbol/type subscriptions exist. Waiting on Ready makes a
// first request subscriber-first without coupling publisher lifetime to it.
func (m *publisherManager) activate(ctx context.Context, rt *StonksRuntime, cfg streamConfig) error {
	m.mu.Lock()
	if rt != m.runtime {
		m.mu.Unlock()
		return errors.New("publisher runtime was replaced")
	}
	if !m.started {
		publisherCtx, cancel := context.WithCancel(context.Background())
		m.cancel = cancel
		m.started = true
		go rt.Start(publisherCtx)
	}
	m.mu.Unlock()

	select {
	case <-rt.Ready():
	case <-rt.Done():
		return errors.New("publisher stopped before becoming ready")
	case <-ctx.Done():
		return ctx.Err()
	}

	return rt.EnsureSubscriptions(cfg)
}

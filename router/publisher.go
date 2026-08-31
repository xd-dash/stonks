package router

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
	"github.com/go-chi/chi/v5/middleware"
)

// publisherManager owns the one retained/shared publisher runtime for this
// stonks process. Request handlers may attach/detach freely; cancelling a
// request never cancels this manager's background context.
type publisherManager struct {
	mu         sync.Mutex
	creds      *alpacaCredentials
	runtime    *StonksRuntime
	feed       marketdata.Feed
	optionFeed marketdata.OptionFeed
	started    bool
	cancel     context.CancelFunc
}

func newPublisherManager(creds *alpacaCredentials) *publisherManager {
	return &publisherManager{creds: creds}
}

func (m *publisherManager) prepare(r *http.Request, cfg streamConfig, secret string) (*StonksRuntime, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runtime != nil {
		select {
		case <-m.runtime.Done():
			m.runtime = nil
			m.started = false
			m.feed = ""
			m.optionFeed = ""
			if m.cancel != nil {
				m.cancel()
				m.cancel = nil
			}
		default:
		}
	}
	if m.runtime != nil {
		if cfg.feed != m.feed {
			return nil, fmt.Errorf("shared publisher already uses stock feed %q; requested %q", m.feed, cfg.feed)
		}
		if len(cfg.optionContracts) > 0 && cfg.optionFeed != m.optionFeed {
			return nil, fmt.Errorf("shared publisher already uses option feed %q; requested %q", m.optionFeed, cfg.optionFeed)
		}
		return m.runtime, nil
	}

	rt := NewStonksRuntime(m.creds)
	rt.RecordInvocation(r, "publisher-"+middleware.GetReqID(r.Context()))
	rt.Configure(cfg, secret)
	m.runtime = rt
	m.feed = cfg.feed
	m.optionFeed = cfg.optionFeed
	return rt, nil
}

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

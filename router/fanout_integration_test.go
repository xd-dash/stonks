package router

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/logma/serverless/pubsub"
)

// TestLogmaFanoutSubscriberIsolation is an opt-in disposable-Redis integration
// test. Huram Abi's GitHub-hosted local qualification sets REDIS_URI and runs it
// before any Farcaster mutation is authorized.
func TestLogmaFanoutSubscriberIsolation(t *testing.T) {
	addr := os.Getenv("REDIS_URI")
	if addr == "" {
		t.Skip("REDIS_URI is not set; disposable Redis integration not requested")
	}

	client := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDISCLI_AUTH")})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping disposable Redis: %v", err)
	}

	channel := fmt.Sprintf("stonks:test:fanout:%d", time.Now().UnixNano())
	aCtx, cancelA := context.WithCancel(ctx)
	defer cancelA()
	bCtx, cancelB := context.WithCancel(ctx)
	defer cancelB()

	aEvents := make(chan string, 4)
	bEvents := make(chan string, 4)
	a := pubsub.Subscribe(aCtx, client, channel, func(payload string) { aEvents <- payload })
	b := pubsub.Subscribe(bCtx, client, channel, func(payload string) { bEvents <- payload })

	waitReady := func(name string, sub *pubsub.Subscriber) {
		t.Helper()
		select {
		case <-sub.Ready():
		case <-ctx.Done():
			t.Fatalf("%s subscriber never became ready: %v", name, ctx.Err())
		}
	}
	waitReady("A", a)
	waitReady("B", b)

	publish := func(payload string) {
		t.Helper()
		if err := client.Publish(ctx, channel, payload).Err(); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}
	wantEvent := func(name string, events <-chan string, want string) {
		t.Helper()
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("%s got %q, want %q", name, got, want)
			}
		case <-ctx.Done():
			t.Fatalf("%s did not receive %q: %v", name, want, ctx.Err())
		}
	}

	publish(`{"sequence":1}`)
	wantEvent("A", aEvents, `{"sequence":1}`)
	wantEvent("B", bEvents, `{"sequence":1}`)

	cancelA()
	select {
	case <-a.Stopped():
	case <-ctx.Done():
		t.Fatalf("A did not stop after cancellation: %v", ctx.Err())
	}

	publish(`{"sequence":2}`)
	wantEvent("B", bEvents, `{"sequence":2}`)

	select {
	case got := <-aEvents:
		t.Fatalf("cancelled A unexpectedly received another event: %q", got)
	case <-time.After(200 * time.Millisecond):
	}
}

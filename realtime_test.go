package supabase

import (
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// TestStaleReaderDoesNotKillNewConnection is a regression test for the
// reconnect-loop bug where an old reader goroutine closed c.ws after open()
// had already replaced it with a fresh connection, killing the new socket
// and starting a feedback loop that reconnected ~50 times per second.
//
// The fix: open() swaps c.ws before closing the old connection, and
// handleCallbacks only fires OnDisconnect / reconnect if c.ws is still the
// ws the goroutine was spawned for.
func TestStaleReaderDoesNotKillNewConnection(t *testing.T) {
	// Long-lived server: reads until the client closes.
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		buf := make([]byte, 4096)
		for {
			_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := ws.Read(buf); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)

	var connects, disconnects atomic.Int64
	ch.OnConnect = func(*Channel) { connects.Add(1) }
	ch.OnDisconnect = func(*Channel) { disconnects.Add(1) }

	if err := ch.open(); err != nil {
		t.Fatalf("first open: %v", err)
	}
	firstWs := ch.ws

	// Give the first reader goroutine time to start and block in Read.
	time.Sleep(100 * time.Millisecond)

	if err := ch.open(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	secondWs := ch.ws

	if firstWs == secondWs {
		t.Fatal("second open() did not create a new ws")
	}

	// Let the stale reader observe the old ws closing and exit.
	time.Sleep(300 * time.Millisecond)

	if got := connects.Load(); got != 2 {
		t.Errorf("OnConnect count = %d; want 2", got)
	}
	if got := disconnects.Load(); got != 0 {
		t.Errorf("stale reader fired OnDisconnect %d times; want 0 "+
			"(the old goroutine must not treat the already-replaced ws as its own)", got)
	}
	if !ch.Connected {
		t.Error("channel marked disconnected; stale reader clobbered c.Connected")
	}
	if ch.ws != secondWs {
		t.Error("c.ws changed after second open(); stale reader triggered a spurious reconnect")
	}

	// The new connection should still be usable. If the stale reader had
	// called c.ws.Close() (the pre-fix behavior), this Write would fail.
	if _, err := ch.ws.Write([]byte(`{"event":"ping","topic":"topic","payload":{}}`)); err != nil {
		t.Errorf("write on supposedly-healthy new connection failed: %v", err)
	}

	ch.ws.Close()
}

// TestChannelReconnectOneForOne verifies that repeated server-side
// disconnects produce exactly one OnConnect / OnDisconnect per cycle rather
// than the runaway storm of events the bug produced.
func TestChannelReconnectOneForOne(t *testing.T) {
	var accepts atomic.Int64

	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		accepts.Add(1)
		buf := make([]byte, 4096)
		_ = ws.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = ws.Read(buf) // read phx_join, then hang up
		ws.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)

	var connects, disconnects atomic.Int64
	ch.OnConnect = func(*Channel) { connects.Add(1) }
	ch.OnDisconnect = func(*Channel) { disconnects.Add(1) }

	if err := ch.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && accepts.Load() < 10 {
		time.Sleep(20 * time.Millisecond)
	}

	srv.Close() // stop accepting; existing cycle drains
	time.Sleep(300 * time.Millisecond)

	ac := accepts.Load()
	cc := connects.Load()
	dc := disconnects.Load()
	t.Logf("accepts=%d connects=%d disconnects=%d", ac, cc, dc)

	if ac < 5 {
		t.Fatalf("want >= 5 accepts, got %d", ac)
	}
	// Slack of +1 for the in-flight cycle when srv.Close() happens.
	if cc > ac {
		t.Errorf("connects=%d > accepts=%d (spurious connects)", cc, ac)
	}
	if dc > ac+1 {
		t.Errorf("disconnects=%d > accepts+1=%d (spurious disconnects)", dc, ac+1)
	}
	if cc < ac-2 {
		t.Errorf("connects=%d much less than accepts=%d", cc, ac)
	}
}

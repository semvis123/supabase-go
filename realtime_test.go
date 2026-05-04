package supabase

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// replyJoinOK consumes the next frame from ws, expects it to be a phx_join,
// and writes back a phx_reply with status:"ok" matching the join's ref.
// All test fake servers must call this before any other application traffic
// because the client now blocks open() until it sees this reply.
func replyJoinOK(ws *websocket.Conn) error {
	var raw []byte
	if err := ws.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if err := websocket.Message.Receive(ws, &raw); err != nil {
		return err
	}
	var join Message
	if err := json.Unmarshal(raw, &join); err != nil {
		return err
	}
	reply := Message{
		Topic:   join.Topic,
		Event:   phxReply,
		Ref:     join.Ref,
		Payload: map[string]interface{}{"status": "ok"},
	}
	b, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	if err := ws.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err = ws.Write(b)
	return err
}

// TestOpenWhileConnectedIsNoop verifies that calling open() while the channel
// is already connected does not create a second socket and does not fire
// OnConnect again. The pre-fix behavior re-dialed unconditionally, leaving a
// stale reader to clobber the new connection and producing a runaway
// connect/disconnect storm.
func TestOpenWhileConnectedIsNoop(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
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
	ch.mu.Lock()
	firstWs := ch.ws
	ch.mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	if err := ch.open(); err != nil {
		t.Fatalf("second open: %v", err)
	}
	ch.mu.Lock()
	secondWs := ch.ws
	ch.mu.Unlock()

	if firstWs != secondWs {
		t.Fatal("second open() replaced ws while still connected; should have been a no-op")
	}

	time.Sleep(200 * time.Millisecond)

	if got := connects.Load(); got != 1 {
		t.Errorf("OnConnect count = %d; want 1 (second open is a no-op)", got)
	}
	if got := disconnects.Load(); got != 0 {
		t.Errorf("OnDisconnect count = %d; want 0", got)
	}
	if !ch.IsConnected() {
		t.Error("channel marked disconnected after redundant open()")
	}

	if _, err := firstWs.Write([]byte(`{"event":"ping","topic":"topic","payload":{}}`)); err != nil {
		t.Errorf("write on healthy connection failed: %v", err)
	}

	ch.Close()
}

// TestChannelReconnectOneForOne verifies that repeated server-side
// disconnects produce exactly one OnConnect / OnDisconnect per cycle rather
// than the runaway storm of events the bug produced.
func TestChannelReconnectOneForOne(t *testing.T) {
	var accepts atomic.Int64

	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		accepts.Add(1)
		if err := replyJoinOK(ws); err != nil {
			return
		}
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

// TestCloseStaysClosed verifies that Close() terminates keepAlive and prevents
// further reconnect attempts. Pre-fix, the reader's error handler fired
// OnDisconnect + reconnectChan after Close, causing the channel to reconnect
// after the user had asked it to stop.
func TestCloseStaysClosed(t *testing.T) {
	var accepts atomic.Int64

	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		accepts.Add(1)
		if err := replyJoinOK(ws); err != nil {
			return
		}
		ws.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)

	var disconnects atomic.Int64
	ch.OnDisconnect = func(*Channel) { disconnects.Add(1) }

	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}

	// Wait for a couple of connect/disconnect cycles so we're exercising the
	// reconnect loop at the time Close is called.
	for i := 0; i < 200 && accepts.Load() < 3; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if accepts.Load() < 2 {
		t.Fatalf("need >= 2 accepts before Close, got %d", accepts.Load())
	}

	acceptsAtClose := accepts.Load()
	disconnectsAtClose := disconnects.Load()

	ch.Close()

	// Idempotency: second Close must not panic / block.
	done := make(chan struct{})
	go func() { ch.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second Close() blocked; not idempotent")
	}

	// After Close, Send must reject.
	if err := ch.Send("x", map[string]interface{}{"k": "v"}); !errors.Is(err, errChannelClosed) {
		t.Errorf("Send after Close: got err=%v; want errChannelClosed", err)
	}

	// Give the system time to (incorrectly) reconnect. Without the fix, the
	// reader's error handler would have signaled reconnectChan and keepAlive
	// would have opened a fresh connection.
	time.Sleep(800 * time.Millisecond)

	extraAccepts := accepts.Load() - acceptsAtClose
	if extraAccepts > 1 {
		t.Errorf("reconnected after Close: %d extra server accepts", extraAccepts)
	}

	// At most one final OnDisconnect (fired by keepAlive's defer) after Close.
	extraDisconnects := disconnects.Load() - disconnectsAtClose
	if extraDisconnects > 1 {
		t.Errorf("got %d OnDisconnect calls after Close; want at most 1", extraDisconnects)
	}

	if ch.IsConnected() {
		t.Error("Connected is still true after Close")
	}
}

// TestCloseFromOnDisconnect verifies that calling Close() from inside the
// OnDisconnect callback does not deadlock. Pre-fix, OnDisconnect ran on the
// keepAlive goroutine and Close()'s keepAliveWG.Wait() blocked on that same
// goroutine forever.
func TestCloseFromOnDisconnect(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
		ws.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)

	closed := make(chan struct{})
	ch.OnDisconnect = func(c *Channel) {
		c.Close()
		close(closed)
	}

	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() from inside OnDisconnect deadlocked")
	}
}

// TestConcurrentSendNoFrameCorruption fires many Send() calls concurrently.
// Without write serialization, the SetWriteDeadline+Write pairs in Send (and
// the heartbeat path in keepAlive) interleave, corrupting WebSocket frames on
// the wire. The server validates each frame as JSON; corruption shows up as
// parse failures or as the server's read loop erroring out before all
// messages arrive.
func TestConcurrentSendNoFrameCorruption(t *testing.T) {
	var received atomic.Int64
	var parseFailures atomic.Int64

	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
		received.Add(1) // count the join so the existing expected+1 assertion still holds
		for {
			_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			var raw []byte
			if err := websocket.Message.Receive(ws, &raw); err != nil {
				return
			}
			var msg map[string]interface{}
			if err := json.Unmarshal(raw, &msg); err != nil {
				parseFailures.Add(1)
				continue
			}
			received.Add(1)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)
	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	const senders = 20
	const perSender = 50
	var wg sync.WaitGroup
	var sendErrs atomic.Int64
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perSender; j++ {
				if err := ch.Send("test", map[string]interface{}{"k": "v"}); err != nil {
					sendErrs.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()

	expected := int64(senders * perSender)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && received.Load() < expected+1 {
		time.Sleep(20 * time.Millisecond)
	}

	if pf := parseFailures.Load(); pf > 0 {
		t.Errorf("server saw %d parse failures (frame corruption)", pf)
	}
	if se := sendErrs.Load(); se > 0 {
		t.Errorf("Send returned %d errors (connection broke during concurrent writes)", se)
	}
	if got := received.Load(); got < expected+1 {
		t.Errorf("server received %d messages; want >= %d (join + %d sends)",
			got, expected+1, expected)
	}
}

// TestConcurrentListenerMutationRace exercises the data race between
// On()/RemoveCallbacksForEvent (which mutate c.listeners) and the read
// goroutine in handleCallbacks (which iterates c.listeners). Run under
// -race; without locking on listeners, the detector fires.
func TestConcurrentListenerMutationRace(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		// Read the join, reply OK, then push events back to the client at high
		// frequency so the read goroutine is constantly iterating listeners.
		if err := replyJoinOK(ws); err != nil {
			return
		}
		for i := 0; i < 500; i++ {
			_ = ws.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := ws.Write([]byte(`{"event":"e","topic":"topic","payload":{}}`)); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)
	ch.On("e", func(*Channel, *Message) {})

	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	// Hammer the listener slice while the read goroutine iterates it.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		ch.On("x", func(*Channel, *Message) {})
		ch.RemoveCallbacksForEvent("x")
	}
}

// TestPanickingListenerDoesNotLeakConnection verifies that a panic in a
// user-supplied listener doesn't crash the read goroutine, doesn't kill
// the process, and doesn't leak the websocket. Pre-fix, the panic
// propagated out of handleCallbacks, the cleanup code never ran, and
// IsConnected stayed true forever.
func TestPanickingListenerDoesNotLeakConnection(t *testing.T) {
	var messages atomic.Int64
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
		for range 5 {
			_ = ws.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := ws.Write([]byte(`{"event":"e","topic":"topic","payload":{}}`)); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		// Hold the connection so we can verify per-message survival before
		// the test tears the channel down.
		buf := make([]byte, 4096)
		_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = ws.Read(buf)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)
	ch.On("e", func(*Channel, *Message) {
		messages.Add(1)
		panic("boom")
	})

	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	// All five server-pushed messages should be delivered despite each
	// listener invocation panicking.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && messages.Load() < 5 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := messages.Load(); got < 5 {
		t.Errorf("listener invoked %d times; want 5 (panic killed the read goroutine)", got)
	}
	if !ch.IsConnected() {
		t.Error("IsConnected=false after panicking listeners; read goroutine died without staying alive")
	}
}

// TestPanickingLifecycleCallbacksDoNotCrash verifies that a panic inside
// a user-supplied OnConnect/OnDisconnect doesn't crash the process. Both
// callbacks run in their own goroutines, so without a recover guard a
// faulty callback would propagate up and terminate the whole program.
// We exercise the full connect/disconnect cycle and confirm the channel
// still tears down cleanly afterward.
func TestPanickingLifecycleCallbacksDoNotCrash(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
		ws.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)

	var connects, disconnects atomic.Int64
	ch.OnConnect = func(*Channel) {
		connects.Add(1)
		panic("boom-connect")
	}
	ch.OnDisconnect = func(*Channel) {
		disconnects.Add(1)
		panic("boom-disconnect")
	}

	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (connects.Load() < 1 || disconnects.Load() < 1) {
		time.Sleep(20 * time.Millisecond)
	}
	if connects.Load() < 1 {
		t.Errorf("OnConnect never fired; got %d", connects.Load())
	}
	if disconnects.Load() < 1 {
		t.Errorf("OnDisconnect never fired; got %d", disconnects.Load())
	}

	// Close must complete; if a panic killed an internal goroutine without
	// signalling the WG, this would block.
	done := make(chan struct{})
	go func() { ch.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() blocked after panicking lifecycle callbacks")
	}
}

// TestListenIsOnce verifies that calling Listen() a second time returns
// errChannelAlreadyStarted. Pre-fix, a second Listen call spawned a second
// keepAlive goroutine — both would heartbeat on the same socket and both
// would race their cleanup defers on Close().
func TestListenIsOnce(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
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
	defer ch.Close()

	if err := ch.Listen(); err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	if err := ch.Listen(); !errors.Is(err, errChannelAlreadyStarted) {
		t.Errorf("second Listen: got err=%v; want errChannelAlreadyStarted", err)
	}
}

// TestListenAfterFailureRetries verifies that if Listen()'s underlying
// open() fails, started is reset and the channel can be retried.
func TestListenAfterFailureRetries(t *testing.T) {
	ch := newChannel("topic", "ws://127.0.0.1:1") // unroutable
	if err := ch.Listen(); err == nil {
		t.Fatal("expected first Listen to fail against unroutable URL")
	}
	// started must be reset on failure so the second call can proceed past
	// the CompareAndSwap guard. We expect this second call to also fail
	// (still unroutable), but it must *not* return errChannelAlreadyStarted.
	if err := ch.Listen(); errors.Is(err, errChannelAlreadyStarted) {
		t.Errorf("second Listen returned errChannelAlreadyStarted; started not reset")
	}
	ch.Close()
}

// TestConcurrentListenAndClose stresses the Listen/Close ordering. Pre-fix,
// Close()'s keepAliveWG.Wait() could land on a zero counter just before
// Listen()'s Add(1), a sync.WaitGroup misuse. Run under -race.
func TestConcurrentListenAndClose(t *testing.T) {
	for range 200 {
		ch := newChannel("topic", "ws://127.0.0.1:1") // unroutable; open() fails fast
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = ch.Listen()
		}()
		go func() {
			defer wg.Done()
			ch.Close()
		}()
		wg.Wait()
		ch.Close() // ensure full teardown
	}
}

// TestCloseFromOnConnect verifies that calling Close() from inside the
// OnConnect callback does not deadlock. On the reconnect path, OnConnect
// runs on the keepAlive goroutine, so a synchronous Close()+Wait() there
// would block on itself.
func TestCloseFromOnConnect(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
		buf := make([]byte, 4096)
		_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = ws.Read(buf)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)

	closed := make(chan struct{})
	var once sync.Once
	ch.OnConnect = func(c *Channel) {
		once.Do(func() {
			c.Close()
			close(closed)
		})
	}

	if err := ch.Listen(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() from inside OnConnect deadlocked")
	}
}

// TestCloseAfterSendOnly verifies that Close() tears down the websocket and
// fires OnDisconnect even when Listen() was never called. Pre-fix, the ws
// cleanup lived only in keepAlive's defer, so a Send-initiated connection
// leaked the socket (and stayed IsConnected==true) until the read deadline
// expired ~10s later.
func TestCloseAfterSendOnly(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		if err := replyJoinOK(ws); err != nil {
			return
		}
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

	var disconnects atomic.Int64
	ch.OnDisconnect = func(*Channel) { disconnects.Add(1) }

	if err := ch.Send("e", map[string]interface{}{"k": "v"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !ch.IsConnected() {
		t.Fatal("Send did not establish a connection")
	}

	ch.Close()

	if ch.IsConnected() {
		t.Error("IsConnected still true after Close in Send-only mode")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && disconnects.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if disconnects.Load() != 1 {
		t.Errorf("OnDisconnect count = %d; want 1", disconnects.Load())
	}
}

// TestJoinRejectionFailsListen verifies that when the Phoenix server replies
// to phx_join with status:"error", the client tears down the socket and surfaces
// the rejection rather than reporting a healthy connection. Pre-fix, the client
// dropped phx_reply silently, so an auth/RLS-rejected join still set
// IsConnected=true and fired OnConnect — the symptom the user observed:
// "agent shows connected but website cannot reach it".
func TestJoinRejectionFailsListen(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		var raw []byte
		_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err := websocket.Message.Receive(ws, &raw); err != nil {
			return
		}
		var join Message
		if err := json.Unmarshal(raw, &join); err != nil {
			return
		}
		reply := Message{
			Topic:   join.Topic,
			Event:   phxReply,
			Ref:     join.Ref,
			Payload: map[string]interface{}{"status": "error", "response": map[string]interface{}{"reason": "unauthorized"}},
		}
		b, _ := json.Marshal(reply)
		_ = ws.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, _ = ws.Write(b)
		// Hold so the client sees the reply before the socket dies.
		_ = ws.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = ws.Read(make([]byte, 1024))
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)
	defer ch.Close()

	var connects atomic.Int64
	ch.OnConnect = func(*Channel) { connects.Add(1) }

	err := ch.open()
	if err == nil {
		t.Fatal("open() succeeded against a server that rejected the join")
	}
	if !strings.Contains(err.Error(), "rejected") {
		t.Errorf("open() error = %v; want one mentioning 'rejected'", err)
	}
	if ch.IsConnected() {
		t.Error("IsConnected=true after rejected join")
	}
	if connects.Load() != 0 {
		t.Errorf("OnConnect fired %d times for rejected join; want 0", connects.Load())
	}
}

// TestServerInitiatedCloseTriggersRejoin verifies that when the server sends
// phx_close on the channel's topic mid-session, the client tears the socket
// down and re-joins instead of silently staying "connected" while no
// broadcasts arrive. This is the long-uptime silent-failure mode: Phoenix's
// inactivity reaper / RLS revocation / server drain sends phx_close on the
// topic but keeps the websocket open, so the 10s read deadline never fires
// (heartbeats on the "phoenix" topic keep the socket warm).
func TestServerInitiatedCloseTriggersRejoin(t *testing.T) {
	var joins atomic.Int64
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		// First join: ack, then send phx_close on the topic and wait for the
		// client to disconnect. The client should treat this as "channel gone"
		// and rejoin.
		// Second join (and beyond): ack and hold the connection open, proving
		// the rejoin happened.
		for {
			var raw []byte
			_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			if err := websocket.Message.Receive(ws, &raw); err != nil {
				return
			}
			var join Message
			if err := json.Unmarshal(raw, &join); err != nil {
				return
			}
			if join.Event != phxJoin {
				continue
			}
			n := joins.Add(1)
			reply := Message{Topic: join.Topic, Event: phxReply, Ref: join.Ref, Payload: map[string]interface{}{"status": "ok"}}
			b, _ := json.Marshal(reply)
			_ = ws.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, err := ws.Write(b); err != nil {
				return
			}
			if n == 1 {
				closeMsg := Message{Topic: join.Topic, Event: phxClose, Payload: map[string]interface{}{}}
				b, _ := json.Marshal(closeMsg)
				_ = ws.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := ws.Write(b); err != nil {
					return
				}
				// Drain whatever the client sends until it disconnects on its end.
				buf := make([]byte, 4096)
				for {
					_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
					if _, err := ws.Read(buf); err != nil {
						return
					}
				}
			}
			// Second+ join: keep the connection alive briefly so the test
			// can assert the rejoin happened.
			buf := make([]byte, 4096)
			for {
				_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
				if _, err := ws.Read(buf); err != nil {
					return
				}
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)
	defer ch.Close()

	var connects, disconnects atomic.Int64
	ch.OnConnect = func(*Channel) { connects.Add(1) }
	ch.OnDisconnect = func(*Channel) { disconnects.Add(1) }

	if err := ch.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && joins.Load() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	if joins.Load() < 2 {
		t.Fatalf("server saw %d joins; want >= 2 (initial + rejoin after phx_close)", joins.Load())
	}
	if disconnects.Load() < 1 {
		t.Errorf("OnDisconnect fired %d times after phx_close; want >= 1", disconnects.Load())
	}
	if connects.Load() < 2 {
		t.Errorf("OnConnect fired %d times; want >= 2 (initial + reconnect)", connects.Load())
	}
}

// TestJoinErrorEventFailsListen verifies that a phx_error frame on the
// channel's topic — Phoenix's other rejection signal — is also surfaced as
// a join failure rather than ignored.
func TestJoinErrorEventFailsListen(t *testing.T) {
	srv := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		var raw []byte
		_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err := websocket.Message.Receive(ws, &raw); err != nil {
			return
		}
		var join Message
		if err := json.Unmarshal(raw, &join); err != nil {
			return
		}
		errMsg := Message{
			Topic:   join.Topic,
			Event:   phxError,
			Payload: map[string]interface{}{"reason": "boom"},
		}
		b, _ := json.Marshal(errMsg)
		_ = ws.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, _ = ws.Write(b)
		_ = ws.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = ws.Read(make([]byte, 1024))
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ch := newChannel("topic", wsURL)
	defer ch.Close()

	if err := ch.open(); err == nil {
		t.Fatal("open() succeeded despite phx_error on join")
	}
	if ch.IsConnected() {
		t.Error("IsConnected=true after phx_error on join")
	}
}

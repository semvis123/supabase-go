package supabase

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/slices"
	"golang.org/x/net/websocket"
)

const (
	phxClose        = "phx_close"
	phxError        = "phx_error"
	phxJoin         = "phx_join"
	phxReply        = "phx_reply"
	phxLeave        = "phx_leave"
	phxHeartbeat    = "heartbeat"
	phxTopic        = "phoenix"
	maxMessageBytes = 3_072_000

	dialTimeout         = 10 * time.Second
	writeTimeout        = 10 * time.Second
	joinReplyTimeout    = 10 * time.Second
	initialReconnectGap = 500 * time.Millisecond
	maxReconnectGap     = 30 * time.Second
)

var (
	errChannelClosed         = errors.New("channel is closed")
	errChannelAlreadyStarted = errors.New("Listen already called on this channel")
)

var (
	phxHeartbeatPayload = map[string]interface{}{"msg": "heartbeat"}
)

type Message struct {
	Event   string                 `json:"event"`
	Payload map[string]interface{} `json:"payload"`
	Ref     *string                `json:"ref"`
	Topic   string                 `json:"topic"`
}

type Realtime struct {
	client *Client
}

// Channel represents a realtime channel.
//
// All exported fields (Topic, Url, Origin, OnConnect, OnDisconnect) must
// be set before Listen() or the first Send() and treated as read-only
// after; they are read from internal goroutines without synchronization.
//
// OnConnect and OnDisconnect are invoked from internal goroutines, so they
// may call Close() without deadlocking. They are not serialized with each
// other — a slow OnConnect can overlap with the next OnDisconnect, and in
// rare races (Close() arriving just as a connect goroutine is being
// launched) you may observe an OnDisconnect with no preceding OnConnect.
// Listener callbacks registered with On(...) run synchronously on the read
// goroutine and MUST NOT call Close().
type Channel struct {
	Topic     string
	Url       string
	Origin    string
	listeners []Listener

	mu        sync.Mutex // protects ws, connected, listeners
	ws        *websocket.Conn
	connected bool

	// openMu serializes open() so concurrent callers don't each create a
	// fresh dial+OnConnect cycle for the same logical connect.
	openMu sync.Mutex
	// writeMu serializes the SetWriteDeadline+Write pair on c.ws so concurrent
	// writers (Send and the heartbeat) can't clobber each other's deadline,
	// and is held by anyone closing c.ws so a Send/heartbeat write cannot
	// land on a connection that is being torn down. Frame integrity itself
	// is provided by the underlying websocket library.
	writeMu sync.Mutex

	started       atomic.Bool // guards repeated Listen() calls
	closed        atomic.Bool
	closeOnce     sync.Once
	closeChan     chan struct{}
	reconnectChan chan struct{}
	keepAliveWG   sync.WaitGroup
	nextRef       atomic.Uint64
	OnDisconnect  func(*Channel)
	OnConnect     func(*Channel)
}

func newChannel(topic string, url string) *Channel {
	return &Channel{
		Topic:         topic,
		Url:           url,
		Origin:        "http://localhost/",
		closeChan:     make(chan struct{}),
		reconnectChan: make(chan struct{}, 1),
		OnDisconnect:  func(*Channel) {},
		OnConnect:     func(*Channel) {},
	}
}

type Listener struct {
	EventName string
	callback  func(*Channel, *Message)
}

func (r *Realtime) Channel(topic string) *Channel {
	websocketUrl := r.client.BaseURL
	websocketUrl = strings.Replace(websocketUrl, "https://", "wss://", 1)
	websocketUrl = strings.Replace(websocketUrl, "http://", "ws://", 1)
	websocketUrl = fmt.Sprintf("%s/realtime/v1/websocket?apikey=%s&vsn=1.0.0", websocketUrl, r.client.apiKey)
	return newChannel(topic, websocketUrl)
}

func (r *Realtime) ChannelWithUrl(topic string, websocketUrl string) *Channel {
	return newChannel(topic, websocketUrl)
}

// Listen connects the channel and starts the keepAlive loop. It must be
// called at most once per channel; calling it twice would spawn two
// keepAlive goroutines, doubling heartbeats and racing on shutdown. If
// Listen returns an error, the channel can be retried (started is reset).
func (c *Channel) Listen() error {
	// Hold c.mu across the closed-check and WG.Add so a concurrent Close()
	// either observes !closed here (and the Add) before its own closed.Store
	// + Wait, or observes closed first and we bail early. Without this
	// ordering, Close()'s Wait could land on a zero counter just before
	// Listen's Add(1), which is a sync.WaitGroup misuse.
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return errChannelClosed
	}
	if !c.started.CompareAndSwap(false, true) {
		c.mu.Unlock()
		return errChannelAlreadyStarted
	}
	// Add to the WaitGroup before open() so that any goroutine open()
	// spawns (handleCallbacks, OnConnect) that ends up calling Close()
	// observes a non-zero counter and waits for keepAlive to finish.
	c.keepAliveWG.Add(1)
	c.mu.Unlock()
	if err := c.open(); err != nil {
		// Reset started before Done so a concurrent Listen() doesn't observe
		// started=true on a channel we've effectively given up on.
		c.started.Store(false)
		c.keepAliveWG.Done()
		return err
	}
	go func() {
		defer c.keepAliveWG.Done()
		c.keepAlive()
	}()
	return nil
}

// Send transmits an event, opening the connection on first use. Without a
// prior Listen() the channel does not auto-reconnect: a dropped connection
// is reopened lazily on the next Send().
func (c *Channel) Send(event string, payload map[string]interface{}) error {
	if c.closed.Load() {
		return errChannelClosed
	}
	msg := &Message{Topic: c.Topic, Event: event, Payload: payload}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	c.mu.Lock()
	connected := c.connected
	c.mu.Unlock()
	if !connected {
		if err := c.open(); err != nil {
			return err
		}
	}

	// Re-snapshot ws under writeMu so we cannot pick up a connection that
	// open() is about to swap out (open() also takes writeMu around the
	// swap+close-oldWs).
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return errChannelClosed
	}
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()
	if ws == nil {
		return errChannelClosed
	}
	if err := ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		c.markBrokenLocked(ws)
		return err
	}
	if _, err := ws.Write(msgBytes); err != nil {
		c.markBrokenLocked(ws)
		return err
	}
	return nil
}

// markBrokenLocked marks the current connection as dead and closes ws so
// IsConnected/the next Send don't keep lying until handleCallbacks's read
// deadline catches up. Caller must hold writeMu; ws.Close is idempotent and
// will also wake the reader, which runs the rest of the disconnect path.
func (c *Channel) markBrokenLocked(ws *websocket.Conn) {
	c.mu.Lock()
	if c.ws == ws {
		c.connected = false
	}
	c.mu.Unlock()
	ws.Close()
}

// IsConnected reports whether the channel currently believes it has an open
// websocket. It is a snapshot — by the time the caller acts on it, the channel
// may have disconnected.
func (c *Channel) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// Close terminates the channel. Subsequent reconnect signals are ignored,
// keepAlive (if running) exits, the websocket is closed, and further Send
// calls return errChannelClosed. Safe to call more than once. On return
// c.connected is false and c.ws is closed regardless of whether Listen()
// was called.
//
// Close does not wait for the read goroutine or for OnConnect/OnDisconnect
// callback goroutines, which is what makes it safe to call from those
// callbacks.
func (c *Channel) Close() {
	c.closeOnce.Do(func() {
		// Set closed under c.mu to pair with Listen()'s WG.Add: if Listen
		// has already taken the lock and added, we observe the Add before
		// Wait; otherwise Listen sees closed and bails without Add.
		c.mu.Lock()
		c.closed.Store(true)
		c.mu.Unlock()
		close(c.closeChan)
		c.shutdownConn()
	})
	c.keepAliveWG.Wait()
}

// shutdownConn closes c.ws and fires OnDisconnect if the channel was
// connected. Invoked from Close so cleanup runs even without Listen().
func (c *Channel) shutdownConn() {
	c.writeMu.Lock()
	c.mu.Lock()
	wasConnected := c.connected
	c.connected = false
	ws := c.ws
	c.mu.Unlock()
	if ws != nil {
		ws.Close()
	}
	c.writeMu.Unlock()
	if wasConnected {
		// Async to keep Close() callable from inside OnDisconnect.
		go c.fireOnDisconnect()
	}
}

func (c *Channel) open() error {
	if c.closed.Load() {
		return errChannelClosed
	}
	c.openMu.Lock()
	defer c.openMu.Unlock()

	if c.closed.Load() {
		return errChannelClosed
	}
	// Short-circuit if another goroutine just connected. Without this, two
	// concurrent open() callers (Send vs reconnect) each dial, each fire
	// OnConnect, but only one OnDisconnect ever fires for the loser — leaving
	// connect/disconnect counts permanently out of balance.
	c.mu.Lock()
	if c.connected {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	cfg, err := websocket.NewConfig(c.Url, c.Origin)
	if err != nil {
		return err
	}
	cfg.Dialer = &net.Dialer{Timeout: dialTimeout}

	newWs, err := c.dial(cfg)
	if err != nil {
		return err
	}

	joinRef := strconv.FormatUint(c.nextRef.Add(1), 10)
	msg := &Message{Topic: c.Topic, Event: phxJoin, Ref: &joinRef, Payload: map[string]interface{}{
		"config": map[string]interface{}{
			"broadcast": map[string]interface{}{
				"self": true,
			},
		}}}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		newWs.Close()
		panic("incorrect join message configured")
	}
	if err := newWs.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		newWs.Close()
		return err
	}
	if _, err := newWs.Write(msgBytes); err != nil {
		newWs.Close()
		return err
	}
	if err := c.awaitJoinReply(newWs, joinRef); err != nil {
		newWs.Close()
		return err
	}

	// Install the new connection under mu. Concurrent open() calls are
	// already serialized by openMu, but writeMu is also taken here so that
	// any in-flight Send/heartbeat completes before we close oldWs — this
	// prevents a write from landing on a connection that is being torn down
	// out from under it.
	c.writeMu.Lock()
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		c.writeMu.Unlock()
		newWs.Close()
		return errChannelClosed
	}
	oldWs := c.ws
	c.ws = newWs
	c.connected = true
	c.mu.Unlock()

	if oldWs != nil {
		oldWs.Close()
	}
	c.writeMu.Unlock()

	go c.handleCallbacks(newWs)
	// Fire OnConnect from a separate goroutine so user code may safely call
	// Close() from inside it without deadlocking on keepAliveWG. Skip it if
	// Close() has already raced in — firing OnConnect for a channel the
	// user has just closed is misleading. A late race (Close arrives after
	// this check) is still possible; see Channel doc.
	if !c.closed.Load() {
		go c.fireOnConnect()
	}
	return nil
}

// awaitJoinReply blocks on ws until the server's phx_reply for our join (matched
// by ref) arrives, or it sees a phx_error / the deadline / a read error. A
// successful join is signaled by payload.status == "ok"; anything else is
// surfaced as an error so the caller can tear down newWs and (if appropriate)
// back off and retry. Without this step the client cannot distinguish an
// accepted join from one the server rejected (e.g. auth/RLS), which manifests
// as "connected" channels that silently never receive broadcasts.
func (c *Channel) awaitJoinReply(ws *websocket.Conn, joinRef string) error {
	if err := ws.SetReadDeadline(time.Now().Add(joinReplyTimeout)); err != nil {
		return err
	}
	buf := make([]byte, maxMessageBytes)
	for {
		n, err := ws.Read(buf)
		if err != nil {
			return fmt.Errorf("waiting for join reply: %w", err)
		}
		var m Message
		if err := json.Unmarshal(buf[:n], &m); err != nil {
			continue
		}
		if m.Topic != c.Topic {
			continue
		}
		if m.Event == phxError {
			return fmt.Errorf("server rejected channel join (phx_error): %v", m.Payload)
		}
		if m.Event == phxReply && m.Ref != nil && *m.Ref == joinRef {
			status, _ := m.Payload["status"].(string)
			if status == "ok" {
				return nil
			}
			return fmt.Errorf("channel join rejected: status=%q payload=%v", status, m.Payload)
		}
	}
}

// dial wraps websocket.DialConfig with cancellation on closeChan, so Close()
// doesn't sit through a 10s dialTimeout. The dial goroutine is drained in the
// background to avoid leaking the resulting socket.
func (c *Channel) dial(cfg *websocket.Config) (*websocket.Conn, error) {
	type result struct {
		ws  *websocket.Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ws, err := websocket.DialConfig(cfg)
		ch <- result{ws, err}
	}()
	select {
	case <-c.closeChan:
		go func() {
			r := <-ch
			if r.ws != nil {
				r.ws.Close()
			}
		}()
		return nil, errChannelClosed
	case r := <-ch:
		return r.ws, r.err
	}
}

func (c *Channel) handleCallbacks(ws *websocket.Conn) {
	// Tear down under writeMu so an in-flight Send/heartbeat completes
	// before the socket closes. Silent exit if c.ws was already replaced
	// or the channel was closed by the user. Runs in defer so a panicking
	// listener doesn't leak the socket and stall reconnect.
	defer func() {
		c.writeMu.Lock()
		c.mu.Lock()
		isCurrent := c.ws == ws && !c.closed.Load()
		if isCurrent {
			c.connected = false
		}
		c.mu.Unlock()
		ws.Close()
		c.writeMu.Unlock()

		if !isCurrent {
			return
		}
		go c.fireOnDisconnect()
		select {
		case c.reconnectChan <- struct{}{}:
		default:
		}
	}()

	msg := make([]byte, maxMessageBytes)
	for {
		if err := ws.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return
		}
		n, err := ws.Read(msg)
		if err != nil {
			return
		}
		message := &Message{}
		if err := json.Unmarshal(msg[:n], message); err != nil {
			continue // ignore errors
		}
		if message.Event == phxReply {
			continue
		}
		// A phx_close or phx_error on our topic means the server has dropped
		// our channel subscription while keeping the websocket open (Phoenix
		// inactivity reaper, RLS revocation, server-side drain, etc.). Without
		// this, the TCP-level read deadline never fires (heartbeats on the
		// "phoenix" topic keep the socket warm) and we silently stay
		// "connected" while broadcasts go nowhere. Returning here lets the
		// defer mark us disconnected and signal a fresh join via reconnectChan.
		if message.Topic == c.Topic && (message.Event == phxClose || message.Event == phxError) {
			return
		}
		c.mu.Lock()
		listeners := append([]Listener(nil), c.listeners...)
		c.mu.Unlock()
		for _, l := range listeners {
			if l.EventName == message.Event || l.EventName == "*" {
				c.invokeListener(l, message)
			}
		}
	}
}

// invokeListener calls a user listener with a panic guard so a faulty
// callback doesn't tear down the read goroutine (and the whole program
// via unrecovered-panic) and doesn't skip subsequent listeners on the
// same message.
func (c *Channel) invokeListener(l Listener, message *Message) {
	defer func() { _ = recover() }()
	l.callback(c, message)
}

// fireOnConnect/fireOnDisconnect invoke the lifecycle callbacks with a
// panic guard. They run in their own goroutines so user code is free to
// call Close() from inside, and a panic there must not take down the
// process.
func (c *Channel) fireOnConnect() {
	defer func() { _ = recover() }()
	c.OnConnect(c)
}

func (c *Channel) fireOnDisconnect() {
	defer func() { _ = recover() }()
	c.OnDisconnect(c)
}

func (c *Channel) keepAlive() {
	msg := Message{
		Event:   phxHeartbeat,
		Payload: phxHeartbeatPayload,
		Ref:     nil,
		Topic:   phxTopic,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		panic("incorrect heartbeat msg configured")
	}

	gap := initialReconnectGap
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-c.closeChan:
			return
		case <-c.reconnectChan:
			if !c.reconnectWithBackoff(&gap) {
				return
			}
		case <-heartbeat.C:
			writeErr := func() error {
				c.writeMu.Lock()
				defer c.writeMu.Unlock()
				c.mu.Lock()
				connected := c.connected
				ws := c.ws
				c.mu.Unlock()
				if !connected || ws == nil {
					return nil
				}
				if err := ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
					c.markBrokenLocked(ws)
					return err
				}
				if _, err := ws.Write(msgBytes); err != nil {
					c.markBrokenLocked(ws)
					return err
				}
				return nil
			}()
			if writeErr != nil {
				if !c.reconnectWithBackoff(&gap) {
					return
				}
			}
		}
	}
}

// reconnectWithBackoff retries open() with exponential backoff until it
// succeeds or the channel is closed. Returns false if closed.
func (c *Channel) reconnectWithBackoff(gap *time.Duration) bool {
	for {
		select {
		case <-c.closeChan:
			return false
		default:
		}
		if err := c.open(); err == nil {
			*gap = initialReconnectGap
			return true
		}
		select {
		case <-c.closeChan:
			return false
		case <-time.After(*gap):
		}
		*gap = min(*gap*2, maxReconnectGap)
	}
}

func (c *Channel) On(event string, callback func(*Channel, *Message)) {
	c.mu.Lock()
	c.listeners = append(c.listeners, Listener{event, callback})
	c.mu.Unlock()
}

func (c *Channel) RemoveCallbacksForEvent(event string) {
	c.mu.Lock()
	c.listeners = slices.DeleteFunc(c.listeners, func(l Listener) bool {
		return l.EventName == event
	})
	c.mu.Unlock()
}

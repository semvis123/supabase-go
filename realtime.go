package supabase

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
	initialReconnectGap = 500 * time.Millisecond
	maxReconnectGap     = 30 * time.Second
)

var errChannelClosed = errors.New("channel is closed")

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

type Channel struct {
	Topic         string
	Url           string
	Origin        string
	listeners     []Listener
	ws            *websocket.Conn
	Connected     bool
	closed        atomic.Bool
	closeOnce     sync.Once
	closeChan     chan struct{}
	reconnectChan chan struct{}
	keepAliveWG   sync.WaitGroup
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

func (c *Channel) Listen() error {
	err := c.open()
	if err != nil {
		return err
	}
	c.keepAliveWG.Add(1)
	go func() {
		defer c.keepAliveWG.Done()
		c.keepAlive()
	}()
	return nil
}

func (c *Channel) Send(event string, payload map[string]interface{}) error {
	if c.closed.Load() {
		return errChannelClosed
	}
	msg := &Message{Topic: c.Topic, Event: event, Payload: payload}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if !c.Connected {
		// attempt to reconnect
		err = c.open()
		if err != nil {
			return err
		}
	}
	_, err = c.ws.Write(msgBytes)
	if err != nil {
		return err
	}
	return nil
}

// Close terminates the channel. Subsequent reconnect signals are ignored,
// keepAlive exits, and further Send calls return errChannelClosed. Safe to
// call more than once. Blocks until keepAlive has finished cleanup, so on
// return c.Connected is guaranteed to be false and c.ws is closed.
func (c *Channel) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeChan)
	})
	c.keepAliveWG.Wait()
}

func (c *Channel) open() error {
	if c.closed.Load() {
		return errChannelClosed
	}
	cfg, err := websocket.NewConfig(c.Url, c.Origin)
	if err != nil {
		return err
	}
	cfg.Dialer = &net.Dialer{Timeout: dialTimeout}
	newWs, err := websocket.DialConfig(cfg)
	if err != nil {
		return err
	}
	msg := &Message{Topic: c.Topic, Event: phxJoin, Payload: map[string]interface{}{
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
	if _, err := newWs.Write(msgBytes); err != nil {
		newWs.Close()
		return err
	}

	// Close may have been called while we were dialing. Don't install the
	// new connection in that case — leave Connected false and let Close's
	// cleanup proceed.
	if c.closed.Load() {
		newWs.Close()
		return errChannelClosed
	}

	// Swap the new connection into place before closing the old one. The old
	// reader goroutine checks c.ws == its local ws and will exit silently
	// instead of treating the imminent close as a disconnect.
	oldWs := c.ws
	c.ws = newWs
	c.Connected = true
	if oldWs != nil {
		oldWs.Close()
	}

	go c.handleCallbacks(newWs)
	c.OnConnect(c)
	return nil
}

func (c *Channel) handleCallbacks(ws *websocket.Conn) {
	var msg = make([]byte, maxMessageBytes)
	var n int

	for {
		if err := ws.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			break
		}
		var err error
		if n, err = ws.Read(msg); err != nil {
			break
		}
		message := &Message{}
		if err := json.Unmarshal(msg[:n], message); err != nil {
			continue // ignore errors
		}
		if message.Event == phxReply {
			continue
		}
		for _, l := range c.listeners {
			if l.EventName == message.Event || l.EventName == "*" {
				l.callback(c, message)
			}
		}
	}

	ws.Close()

	// Silent exit if:
	//  - open() has already replaced c.ws (new reader owns this connection), or
	//  - the channel was closed by the user (keepAlive will fire OnDisconnect).
	if c.ws != ws || c.closed.Load() {
		return
	}
	c.Connected = false
	c.OnDisconnect(c)
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
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

	// On shutdown, close the current ws and fire a final OnDisconnect — but
	// only if the channel was believed to be connected at close time.
	defer func() {
		c.closed.Store(true)
		wasConnected := c.Connected
		c.Connected = false
		if c.ws != nil {
			c.ws.Close()
		}
		if wasConnected {
			c.OnDisconnect(c)
		}
	}()

	gap := initialReconnectGap
	for {
		select {
		case <-c.closeChan:
			return
		case <-c.reconnectChan:
			if !c.reconnectWithBackoff(&gap) {
				return
			}
		case <-time.After(time.Second * 5):
			if !c.Connected || c.ws == nil {
				continue
			}
			if _, err := c.ws.Write(msgBytes); err != nil {
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
	c.listeners = append(c.listeners, Listener{event, callback})
}

func (c *Channel) RemoveCallbacksForEvent(event string) {
	c.listeners = slices.DeleteFunc(c.listeners, func(l Listener) bool {
		return l.EventName == event
	})
}

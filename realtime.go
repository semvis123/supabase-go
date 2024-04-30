package supabase

import (
	"encoding/json"
	"fmt"
	"strings"
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

type Channel struct {
	Topic     string
	Url       string
	Origin    string
	listeners []Listener
	ws        *websocket.Conn
	Connected bool
	closeChan chan struct{}
}

type Listener struct {
	EventName string
	callback  func(*Channel, *Message)
}

func (r *Realtime) Channel(topic string) *Channel {
	websocketUrl := r.client.BaseURL
	websocketUrl = strings.Replace(websocketUrl, "https://", "wss://", 1)
	websocketUrl = strings.Replace(websocketUrl, "http://", "ws://", 1)
	websocketUrl = fmt.Sprintf("%s/ws", websocketUrl)
	return &Channel{topic, websocketUrl, "http://localhost/", nil, nil, false, make(chan struct{})}
}

func (r *Realtime) ChannelWithUrl(topic string, websocketUrl string) *Channel {
	return &Channel{topic, websocketUrl, "http://localhost/", nil, nil, false, make(chan struct{})}
}

func (c *Channel) Listen() error {
	err := c.Open()
	if err != nil {
		return err
	}
	go c.keepAlive()
	return nil
}

func (c *Channel) Close() {
	c.closeChan <- struct{}{}
}

func (c *Channel) Open() error {
	if c.ws != nil {
		c.ws.Close()
	}
	ws, err := websocket.Dial(c.Url, "", c.Origin)
	if err != nil {
		return err
	}
	c.ws = ws
	c.Connected = true
	msg := &Message{Topic: c.Topic, Event: phxJoin}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		panic("incorrect join message configured")
	}
	_, err = c.ws.Write(msgBytes)
	if err != nil {
		return err
	}

	go c.handleCallbacks()
	return nil
}

func (c *Channel) handleCallbacks() {
	var msg = make([]byte, maxMessageBytes)
	var n int

	for {
		if !c.Connected {
			break
		}
		err := c.ws.SetReadDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			continue
		}
		if n, err = c.ws.Read(msg); err != nil {
			continue // ignore errors
		}
		message := &Message{}
		if err := json.Unmarshal(msg[n:], message); err != nil {
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
	for {
		select {
		case <-c.closeChan:
			c.Connected = false
			c.ws.Close()
		case <-time.After(time.Second * 5):
			if _, err := c.ws.Write(msgBytes); err != nil {
				// try reconnecting
				err = c.Open()
				if err != nil {
					// ignore connection errors, and just try again in the next heartbeat
					c.Connected = false
				}
			}
		}

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

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
	Topic         string
	Url           string
	Origin        string
	listeners     []Listener
	ws            *websocket.Conn
	Connected     bool
	closeChan     chan struct{}
	reconnectChan chan struct{}
	OnDisconnect  func(*Channel)
	OnConnect     func(*Channel)
}

func newChannel(topic string, url string) *Channel {
	return &Channel{
		topic,
		url,
		"http://localhost/",
		nil,
		nil,
		false,
		make(chan struct{}),
		make(chan struct{}),
		func(*Channel) {},
		func(*Channel) {},
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
	go c.keepAlive()
	return nil
}

func (c *Channel) Send(event string, payload map[string]interface{}) error {
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

func (c *Channel) Close() {
	c.closeChan <- struct{}{}
}

func (c *Channel) open() error {
	if c.ws != nil {
		c.ws.Close()
	}
	ws, err := websocket.Dial(c.Url, "", c.Origin)
	if err != nil {
		return err
	}
	c.ws = ws
	c.Connected = true
	msg := &Message{Topic: c.Topic, Event: phxJoin, Payload: map[string]interface{}{
		"config": map[string]interface{}{
			"broadcast": map[string]interface{}{
				"self": true,
			},
		}}}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		panic("incorrect join message configured")
	}
	_, err = c.ws.Write(msgBytes)
	if err != nil {
		return err
	}

	go c.handleCallbacks()
	c.OnConnect(c)
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
			c.Connected = false
			c.ws.Close()
			c.OnDisconnect(c)
			c.reconnectChan <- struct{}{}

			continue // ignore errors
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
			c.OnDisconnect(c)
		case <-c.reconnectChan:
			_ = c.open()
		case <-time.After(time.Second * 5):
			if _, err := c.ws.Write(msgBytes); err != nil {
				// try reconnecting
				err = c.open()
				if err != nil && c.Connected {
					// ignore connection errors, and just try again in the next heartbeat
					c.Connected = false
					c.OnDisconnect(c)
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

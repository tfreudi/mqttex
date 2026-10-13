// Package broker owns the single MQTT connection and feeds the store.
package broker

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/silasmartin/mqttex/internal/profiles"
	"github.com/silasmartin/mqttex/internal/store"
)

const (
	StateDisconnected = "disconnected"
	StateConnecting   = "connecting"
	StateConnected    = "connected"
)

// ackTimeout bounds the wait for SUBACK and UNSUBACK. The broker queues them
// behind the publishes it already has on the way to us; with "#" on a busy
// broker that backlog is regularly longer than paho's default of 10 s.
const ackTimeout = 2 * time.Minute

// ErrNotConnected is returned by operations that need a live broker connection.
var ErrNotConnected = errors.New("not connected to a broker")

// Status is what the UI shows about the connection.
type Status struct {
	State         string   `json:"state"`
	ProfileID     string   `json:"profileId,omitempty"`
	ProfileName   string   `json:"profileName,omitempty"`
	ClientID      string   `json:"clientId,omitempty"`
	Subscriptions []string `json:"subscriptions,omitempty"` // filters currently subscribed
	Focus         bool     `json:"focus,omitempty"`         // Subscriptions were narrowed by Focus
	Error         string   `json:"error,omitempty"`         // why the connection is not up
	SubErrors     []string `json:"subErrors,omitempty"`     // filters the broker rejected
}

type Manager struct {
	store *store.Store

	mu     sync.Mutex
	gen    int // bumped on every Connect/Disconnect so stale callbacks are ignored
	cm     *autopaho.ConnectionManager
	cancel context.CancelFunc
	status Status
	// profileSubs are the filters of the connected profile, active the ones
	// subscribed right now; a reconnect restores active, not profileSubs.
	profileSubs []string
	active      []string

	focusMu sync.Mutex // serializes Focus so a later call cannot be overtaken
	// accept is the focus while one is set. Publishes already queued in paho
	// when the old filters were dropped still arrive after the store reset;
	// they are discarded instead of reappearing in the tree. nil accepts all.
	accept atomic.Pointer[[]string]
}

func New(s *store.Store) *Manager {
	return &Manager{store: s, status: Status{State: StateDisconnected}}
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// Connect drops the current connection and topic tree, then connects in the
// background. Progress and failures are reported through Status.
func (m *Manager) Connect(p profiles.Profile) error {
	serverURL, err := url.Parse(p.URL())
	if err != nil {
		return fmt.Errorf("broker address %q is invalid: %w", p.URL(), err)
	}
	m.Disconnect()
	m.store.Reset()

	clientID := p.ClientID
	if clientID == "" {
		var b [4]byte
		rand.Read(b[:])
		clientID = "mqttex-" + hex.EncodeToString(b[:])
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.gen++
	gen := m.gen
	m.status = Status{State: StateConnecting, ProfileID: p.ID, ProfileName: p.Name, ClientID: clientID, Subscriptions: p.Subscriptions}
	m.profileSubs, m.active = p.Subscriptions, p.Subscriptions

	update := func(f func(*Status)) {
		m.mu.Lock()
		if m.gen == gen {
			f(&m.status)
		}
		m.mu.Unlock()
	}

	cfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		ConnectTimeout:                10 * time.Second,
		ReconnectBackoff:              autopaho.NewConstantBackoff(3 * time.Second),
		ConnectUsername:               p.Username,
		ConnectPassword:               []byte(p.Password),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			update(func(s *Status) { s.State, s.Error, s.SubErrors = StateConnected, "", nil })
			go func() {
				m.mu.Lock()
				filters, current := m.active, m.gen == gen
				m.mu.Unlock()
				if !current {
					return
				}
				failed := subscribe(cm, filters)
				update(func(s *Status) { s.SubErrors = failed })
			}()
		},
		OnConnectionDown: func() bool {
			update(func(s *Status) { s.State, s.Error = StateConnecting, "connection lost, reconnecting" })
			return true
		},
		OnConnectError: func(err error) {
			update(func(s *Status) { s.State, s.Error = StateConnecting, err.Error() })
		},
		ClientConfig: paho.ClientConfig{
			ClientID:      clientID,
			PacketTimeout: ackTimeout, // caps every context passed to paho
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					m.ingest(pr.Packet)
					return true, nil
				},
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				msg := fmt.Sprintf("broker closed the connection (reason code 0x%02x)", d.ReasonCode)
				if d.Properties != nil && d.Properties.ReasonString != "" {
					msg += ": " + d.Properties.ReasonString
				}
				update(func(s *Status) { s.Error = msg })
			},
		},
	}
	if p.Protocol == "mqtts" || p.Protocol == "wss" {
		cfg.TlsCfg = &tls.Config{InsecureSkipVerify: p.TLSInsecure}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		cancel()
		m.status = Status{State: StateDisconnected, Error: err.Error()}
		return err
	}
	m.cm, m.cancel = cm, cancel
	return nil
}

func (m *Manager) ingest(p *paho.Publish) {
	if accept := m.accept.Load(); accept != nil && !matchesAny(*accept, p.Topic) {
		return
	}
	msg := store.Message{Time: time.Now(), Payload: p.Payload, QoS: p.QoS, Retain: p.Retain}
	if pp := p.Properties; pp != nil {
		msg.ContentType = pp.ContentType
		if len(pp.User) > 0 {
			msg.UserProps = make(map[string]string, len(pp.User))
			for _, u := range pp.User {
				msg.UserProps[u.Key] = u.Value
			}
		}
	}
	m.store.Ingest(p.Topic, msg)
}

// Focus replaces the active subscriptions with filters and rebuilds the topic
// tree from what they deliver. No filters restores the profile subscriptions.
// Unsubscribe comes before the reset so the fresh tree only receives the new
// filters, and subscribe after it so their retained messages land in it.
// The UNSUBACK also marks the end of the old backlog: a broker still busy
// flushing it may drop the retained messages of a new subscription, so the
// subscribe has to wait for it.
func (m *Manager) Focus(filters []string) error {
	m.focusMu.Lock()
	defer m.focusMu.Unlock()

	focus := len(filters) > 0
	m.mu.Lock()
	cm, gen, old := m.cm, m.gen, m.active
	if cm == nil || m.status.State != StateConnected {
		m.mu.Unlock()
		return ErrNotConnected
	}
	if focus {
		filters = slices.Clone(filters)
	} else {
		filters = m.profileSubs
	}
	// Set before the network calls so a reconnect meanwhile subscribes the new filters.
	m.active = filters
	m.mu.Unlock()

	var errs []string
	if len(old) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
		_, err := cm.Unsubscribe(ctx, &paho.Unsubscribe{Topics: old})
		cancel()
		if err != nil {
			errs = append(errs, fmt.Sprintf("unsubscribe failed: %v", err))
		}
	}

	// Under m.mu so a concurrent Connect cannot have its fresh tree wiped.
	m.mu.Lock()
	if m.gen != gen {
		m.mu.Unlock()
		return fmt.Errorf("%w: the connection changed while switching the focus", ErrNotConnected)
	}
	if focus {
		m.accept.Store(&filters)
	} else {
		m.accept.Store(nil)
	}
	m.store.Reset()
	m.mu.Unlock()

	errs = append(errs, subscribe(cm, filters)...)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gen != gen {
		return fmt.Errorf("%w: the connection changed while switching the focus", ErrNotConnected)
	}
	m.status.Subscriptions, m.status.Focus, m.status.SubErrors = filters, focus, errs
	return nil
}

// matchesAny reports whether topic matches one of the MQTT topic filters.
func matchesAny(filters []string, topic string) bool {
	for _, f := range filters {
		if matches(f, topic) {
			return true
		}
	}
	return false
}

func matches(filter, topic string) bool {
	// Wildcards in the first level do not match the broker's $-topics.
	if strings.HasPrefix(topic, "$") && (strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#")) {
		return false
	}
	for {
		fl, frest, fmore := strings.Cut(filter, "/")
		if fl == "#" {
			return true
		}
		tl, trest, tmore := strings.Cut(topic, "/")
		if fl != "+" && fl != tl {
			return false
		}
		if !fmore || !tmore {
			// "a/#" also matches "a" itself.
			return fmore == tmore || (!tmore && frest == "#")
		}
		filter, topic = frest, trest
	}
}

// subscribe returns a description of every filter the broker rejected.
// The Suback is inspected even when paho reports an error, since a rejected
// filter is exactly the case the user needs to see.
func subscribe(cm *autopaho.ConnectionManager, filters []string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()

	sub := &paho.Subscribe{}
	for _, f := range filters {
		sub.Subscriptions = append(sub.Subscriptions, paho.SubscribeOptions{Topic: f, QoS: 0})
	}
	ack, err := cm.Subscribe(ctx, sub)
	if ack == nil {
		if err == nil {
			err = errors.New("no SUBACK received")
		}
		return []string{fmt.Sprintf("subscribe failed: %v", err)}
	}
	var failed []string
	for i, code := range ack.Reasons {
		if code >= 0x80 && i < len(filters) {
			failed = append(failed, fmt.Sprintf("%s rejected by the broker (%s)", filters[i], subackReason(code)))
		}
	}
	return failed
}

func subackReason(code byte) string {
	switch code {
	case 0x83:
		return "implementation specific error"
	case 0x87:
		return "not authorized, check the ACL of this user"
	case 0x8F:
		return "topic filter invalid"
	case 0x97:
		return "quota exceeded"
	case 0xA1:
		return "subscription identifiers not supported"
	case 0xA2:
		return "wildcard subscriptions not supported"
	case 0x9E:
		return "shared subscriptions not supported"
	}
	return fmt.Sprintf("reason code 0x%02x", code)
}

// Disconnect closes the connection. The topic tree is kept for inspection.
func (m *Manager) Disconnect() {
	m.mu.Lock()
	cm, cancel := m.cm, m.cancel
	m.cm, m.cancel = nil, nil
	m.gen++
	m.status = Status{State: StateDisconnected}
	m.accept.Store(nil)
	m.mu.Unlock()

	if cm == nil {
		return
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	cm.Disconnect(ctx)
	stop()
	cancel()
}

// PublishRequest is a message to send to the broker.
type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain"`
}

func (m *Manager) Publish(req PublishRequest) error {
	if err := profiles.ValidateTopic(req.Topic); err != nil {
		return err
	}
	if req.QoS > 2 {
		return fmt.Errorf("qos %d is not 0, 1 or 2", req.QoS)
	}
	m.mu.Lock()
	cm, state := m.cm, m.status.State
	m.mu.Unlock()
	if cm == nil || state != StateConnected {
		return ErrNotConnected
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cm.Publish(ctx, &paho.Publish{Topic: req.Topic, Payload: []byte(req.Payload), QoS: req.QoS, Retain: req.Retain})
	if err != nil {
		return err
	}
	if resp != nil && resp.ReasonCode >= 0x80 {
		return fmt.Errorf("broker rejected the publish (reason code 0x%02x)", resp.ReasonCode)
	}
	return nil
}

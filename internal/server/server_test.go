package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/coder/websocket"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/silasmartin/mqttex/internal/broker"
	"github.com/silasmartin/mqttex/internal/profiles"
	"github.com/silasmartin/mqttex/internal/store"
)

// aclHook lets everybody in but refuses any topic below denied/.
type aclHook struct{ mqtt.HookBase }

func (h *aclHook) ID() string { return "test-acl" }
func (h *aclHook) Provides(b byte) bool {
	return b == mqtt.OnConnectAuthenticate || b == mqtt.OnACLCheck
}
func (h *aclHook) OnConnectAuthenticate(*mqtt.Client, packets.Packet) bool { return true }
func (h *aclHook) OnACLCheck(_ *mqtt.Client, topic string, _ bool) bool {
	return !strings.HasPrefix(topic, "denied/")
}

func startBroker(t *testing.T) (*mqtt.Server, int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	srv := mqtt.New(&mqtt.Options{InlineClient: true})
	if err := srv.AddHook(new(aclHook), nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: fmt.Sprintf("127.0.0.1:%d", port)})); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, port
}

type env struct {
	t      *testing.T
	st     *store.Store
	br     *broker.Manager
	book   *profiles.Book
	http   *httptest.Server
	client *http.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()
	book, err := profiles.Open(filepath.Join(t.TempDir(), "profiles.json"))
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(0)
	br := broker.New(st)
	ui := fstest.MapFS{"index.html": {Data: []byte("<!doctype html>ui")}}
	ts := httptest.NewServer(New(st, br, book, ui, true))
	t.Cleanup(func() {
		br.Disconnect()
		ts.Close()
	})
	return &env{t: t, st: st, br: br, book: book, http: ts, client: ts.Client()}
}

func (e *env) do(method, path string, body any, mutate func(*http.Request)) (int, map[string]any) {
	e.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req, err := http.NewRequest(method, e.http.URL+path, &buf)
	if err != nil {
		e.t.Fatal(err)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if mutate != nil {
		mutate(req)
	}
	res, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func (e *env) waitFor(what string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// wsSession collects what a browser would know after reading the socket.
type wsSession struct {
	t        *testing.T
	conn     *websocket.Conn
	names    map[uint32]string
	counts   map[uint32]uint32
	previews map[uint32]wirePreview
	compare  map[uint32]wirePreview
	history  []wireMessage
	last     wireTick
}

func (e *env) dialWS() *wsSession {
	e.t.Helper()
	conn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(e.http.URL, "http")+"/ws", nil)
	if err != nil {
		e.t.Fatal(err)
	}
	conn.SetReadLimit(64 << 20)
	e.t.Cleanup(func() { conn.CloseNow() })
	return &wsSession{t: e.t, conn: conn, names: map[uint32]string{}, counts: map[uint32]uint32{}, previews: map[uint32]wirePreview{}, compare: map[uint32]wirePreview{}}
}

func (s *wsSession) send(m clientMsg) {
	s.t.Helper()
	data, _ := json.Marshal(m)
	if err := s.conn.Write(context.Background(), websocket.MessageText, data); err != nil {
		s.t.Fatal(err)
	}
}

func (s *wsSession) readUntil(what string, cond func() bool) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for !cond() {
		typ, data, err := s.conn.Read(ctx)
		if err != nil {
			s.t.Fatalf("waiting for %s: %v", what, err)
		}
		if typ == websocket.MessageBinary {
			if binary.LittleEndian.Uint32(data) != frameCounts {
				s.t.Fatalf("unknown binary frame type %d", binary.LittleEndian.Uint32(data))
			}
			n := int(binary.LittleEndian.Uint32(data[4:]))
			if len(data) != 8+8*n {
				s.t.Fatalf("counts frame has %d bytes for %d pairs", len(data), n)
			}
			for i := 0; i < n; i++ {
				id := binary.LittleEndian.Uint32(data[8+8*i:])
				if _, known := s.names[id]; !known {
					s.t.Fatalf("count for id %d arrived before its name", id)
				}
				s.counts[id] = binary.LittleEndian.Uint32(data[12+8*i:])
			}
			continue
		}
		var tick wireTick
		if err := json.Unmarshal(data, &tick); err != nil {
			s.t.Fatal(err)
		}
		if tick.Reset {
			s.names, s.counts = map[uint32]string{}, map[uint32]uint32{}
		}
		for i, name := range tick.Names {
			s.names[tick.First+uint32(i)] = name
		}
		for _, p := range tick.Previews {
			s.previews[p.ID] = p
		}
		for _, p := range tick.Compare {
			s.compare[p.ID] = p
		}
		s.history = append(s.history, tick.History...)
		s.last = tick
	}
}

func TestEndToEndBrokerToBrowser(t *testing.T) {
	mq, port := startBroker(t)
	e := newEnv(t)
	p, err := e.book.Save(profiles.Profile{Name: "local", Protocol: "mqtt", Host: "127.0.0.1", Port: port,
		Subscriptions: []string{"/topic/#", "denied/#"}}, false)
	if err != nil {
		t.Fatal(err)
	}

	if code, body := e.do("POST", "/api/connect", map[string]string{"id": p.ID}, nil); code != 200 {
		t.Fatalf("connect = %d %v", code, body)
	}
	e.waitFor("the broker connection", func() bool {
		s := e.br.Status()
		return s.State == broker.StateConnected && len(s.SubErrors) > 0
	})
	if errs := e.br.Status().SubErrors; len(errs) != 1 || !strings.Contains(errs[0], "denied/#") || !strings.Contains(errs[0], "not authorized") {
		t.Fatalf("rejected subscription was not reported: %v", errs)
	}

	const topics = 25_000 // more than one names frame
	for round := 1; round <= 2; round++ {
		for i := 0; i < topics; i++ {
			payload := fmt.Sprintf(`{"round":%d}`, round)
			if err := mq.Publish(fmt.Sprintf("/topic/SN%06d", i), []byte(payload), false, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	mq.Publish("elsewhere/x", []byte("not subscribed"), false, 0)

	ws := e.dialWS()
	ws.readUntil("all topics and counts", func() bool {
		if len(ws.names) != topics || len(ws.counts) != topics {
			return false
		}
		for _, c := range ws.counts {
			if c != 2 {
				return false
			}
		}
		return true
	})
	if ws.last.Stats.Messages != 2*topics || ws.last.Status.State != broker.StateConnected {
		t.Fatalf("stats = %+v status = %+v", ws.last.Stats, ws.last.Status)
	}
	var id uint32
	for i, name := range ws.names {
		if strings.HasPrefix(name, "elsewhere") {
			t.Fatalf("received unsubscribed topic %s", name)
		}
		if name == "/topic/SN000042" {
			id = i
		}
	}

	// Values are only sent for rows the browser says it shows.
	if len(ws.previews) != 0 {
		t.Fatalf("got %d previews without watching anything", len(ws.previews))
	}
	ws.send(clientMsg{Type: "watch", Epoch: ws.last.Epoch, IDs: []uint32{id}})
	ws.readUntil("the watched preview", func() bool { return len(ws.previews) == 1 })
	if pv := ws.previews[id]; pv.Text == nil || *pv.Text != `{"round":2}` {
		t.Fatalf("preview = %+v", pv)
	}

	// The compare channel carries whole payloads, unlike the 160-byte previews.
	long := `{"blob":"` + strings.Repeat("x", 300) + `"}`
	if err := mq.Publish("/topic/LONG", []byte(long), false, 0); err != nil {
		t.Fatal(err)
	}
	ws.readUntil("the long topic", func() bool { return len(ws.names) == topics+1 })
	var longID uint32
	for i, name := range ws.names {
		if name == "/topic/LONG" {
			longID = i
		}
	}
	ws.send(clientMsg{Type: "compare", Epoch: ws.last.Epoch, IDs: []uint32{longID, id}})
	ws.readUntil("the compared messages", func() bool { return len(ws.compare) == 2 })
	if cv := ws.compare[longID]; cv.Text == nil || *cv.Text != long || cv.Trunc || cv.Size != len(long) {
		t.Fatalf("compared = %+v", cv)
	}
	if cv := ws.compare[id]; cv.Text == nil || *cv.Text != `{"round":2}` {
		t.Fatalf("compared = %+v", cv)
	}
	if len(ws.previews) != 1 {
		t.Fatalf("compare leaked into previews: %d", len(ws.previews))
	}

	// Selecting a topic starts its history with the latest message; publishing
	// through the API comes back as the next entry.
	ws.send(clientMsg{Type: "select", Epoch: ws.last.Epoch, ID: int64(id)})
	ws.readUntil("the history seed", func() bool { return len(ws.history) == 1 })
	if ws.last.Selected != int64(id) || ws.history[0].N != 2 {
		t.Fatalf("selected = %d history = %+v", ws.last.Selected, ws.history)
	}
	req := broker.PublishRequest{Topic: "/topic/SN000042", Payload: "from the ui", QoS: 1}
	if code, body := e.do("POST", "/api/publish", req, nil); code != 200 {
		t.Fatalf("publish = %d %v", code, body)
	}
	// The counts frame of an update follows its JSON frame.
	ws.readUntil("the published message", func() bool { return len(ws.history) == 2 && ws.counts[id] == 3 })
	if m := ws.history[1]; m.N != 3 || m.Text == nil || *m.Text != "from the ui" {
		t.Fatalf("history entry = %+v", m)
	}
	if ws.counts[id] != 3 {
		t.Fatalf("count = %d, want 3", ws.counts[id])
	}

	// Clearing resets every browser.
	if code, _ := e.do("POST", "/api/clear", nil, nil); code != 200 {
		t.Fatalf("clear = %d", code)
	}
	ws.readUntil("the reset", func() bool { return ws.last.Reset })
	if len(ws.names) != 0 || ws.last.Selected != -1 {
		t.Fatalf("after reset: %d names, selected %d", len(ws.names), ws.last.Selected)
	}

	if code, body := e.do("POST", "/api/publish", broker.PublishRequest{Topic: "a/#"}, nil); code != 400 {
		t.Fatalf("publish to wildcard topic = %d %v", code, body)
	}
	e.do("POST", "/api/disconnect", nil, nil)
	if code, body := e.do("POST", "/api/publish", req, nil); code != 400 || !strings.Contains(fmt.Sprint(body["error"]), "not connected") {
		t.Fatalf("publish while disconnected = %d %v", code, body)
	}
}

func (s *wsSession) nameList() string {
	list := make([]string, 0, len(s.names))
	for _, n := range s.names {
		list = append(list, n)
	}
	sort.Strings(list)
	return strings.Join(list, " ")
}

func (s *wsSession) has(name string) bool {
	for _, n := range s.names {
		if n == name {
			return true
		}
	}
	return false
}

func TestFocusNarrowsTheSubscription(t *testing.T) {
	mq, port := startBroker(t)
	e := newEnv(t)
	p, err := e.book.Save(profiles.Profile{Name: "local", Protocol: "mqtt", Host: "127.0.0.1", Port: port,
		Subscriptions: []string{"#"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	device := map[string][]string{"filters": {"/topic/SN000001/#"}}
	if code, body := e.do("POST", "/api/focus", device, nil); code != 409 || !strings.Contains(fmt.Sprint(body["error"]), "not connected") {
		t.Fatalf("focus while disconnected = %d %v", code, body)
	}

	publish := func(topic string, retain bool) {
		t.Helper()
		if err := mq.Publish(topic, []byte("v"), retain, 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, topic := range []string{"/topic/SN000001/a", "/topic/SN000001/b", "/topic/SN000002/a", "other/x"} {
		publish(topic, true)
	}
	if code, body := e.do("POST", "/api/connect", map[string]string{"id": p.ID}, nil); code != 200 {
		t.Fatalf("connect = %d %v", code, body)
	}
	ws := e.dialWS()
	ws.readUntil("the retained topics", func() bool { return len(ws.names) == 4 })

	epoch := ws.last.Epoch
	code, body := e.do("POST", "/api/focus", device, nil)
	if code != 200 || body["focus"] != true || fmt.Sprint(body["subscriptions"]) != "[/topic/SN000001/#]" {
		t.Fatalf("focus = %d %v", code, body)
	}
	ws.readUntil("the focused tree", func() bool { return ws.last.Epoch != epoch && len(ws.names) == 2 })
	if got := ws.nameList(); got != "/topic/SN000001/a /topic/SN000001/b" {
		t.Fatalf("focused tree = %s", got)
	}
	// The last publish is a barrier: the broker delivers in order, so the
	// others would have arrived before it if they were still subscribed.
	publish("/topic/SN000002/live", false)
	publish("other/live", false)
	publish("/topic/SN000001/live", false)
	ws.readUntil("the focused live topic", func() bool { return ws.has("/topic/SN000001/live") })
	if got := ws.nameList(); got != "/topic/SN000001/a /topic/SN000001/b /topic/SN000001/live" {
		t.Fatalf("focused tree after live publishes = %s", got)
	}

	// A reconnect keeps the focus instead of falling back to the profile.
	cl, ok := mq.Clients.Get(e.br.Status().ClientID)
	if !ok {
		t.Fatal("client not found in the broker")
	}
	cl.Stop(errors.New("kicked by the test"))
	// autopaho reconnects at once, too fast to watch its state; the broker
	// sees the new session and its subscriptions.
	e.waitFor("the resubscription", func() bool {
		next, ok := mq.Clients.Get(e.br.Status().ClientID)
		return ok && next != cl && len(next.State.Subscriptions.GetAll()) > 0
	})
	next, _ := mq.Clients.Get(e.br.Status().ClientID)
	if subs := next.State.Subscriptions.GetAll(); len(subs) != 1 || subs["/topic/SN000001/#"].Filter == "" {
		t.Fatalf("subscriptions after the reconnect = %v", subs)
	}
	e.waitFor("the reconnect", func() bool { return e.br.Status().State == broker.StateConnected })
	publish("/topic/SN000002/b", true)
	publish("/topic/SN000001/c", true)
	ws.readUntil("the retained topic after the reconnect", func() bool { return ws.has("/topic/SN000001/c") })
	publish("/topic/SN000002/live2", false)
	publish("/topic/SN000001/live2", false)
	ws.readUntil("the live topic after the reconnect", func() bool { return ws.has("/topic/SN000001/live2") })
	if got := ws.nameList(); got != "/topic/SN000001/a /topic/SN000001/b /topic/SN000001/c /topic/SN000001/live /topic/SN000001/live2" {
		t.Fatalf("focused tree after the reconnect = %s", got)
	}
	if s := e.br.Status(); !s.Focus || len(s.Subscriptions) != 1 || s.Subscriptions[0] != "/topic/SN000001/#" {
		t.Fatalf("status after the reconnect = %+v", s)
	}

	tooMany := make([]string, 33)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("/topic/SN%06d/#", i)
	}
	for name, filters := range map[string][]string{
		"too many filters": tooMany,
		"empty filter":     {""},
		"long filter":      {strings.Repeat("a", 1025)},
		"invalid filter":   {"a/#/b"},
	} {
		if code, body := e.do("POST", "/api/focus", map[string][]string{"filters": filters}, nil); code != 400 {
			t.Errorf("%s: focus = %d %v, want 400", name, code, body)
		}
	}

	// No filters restore the profile subscriptions.
	epoch = ws.last.Epoch
	code, body = e.do("POST", "/api/focus", map[string][]string{"filters": {}}, nil)
	if code != 200 || body["focus"] != nil || fmt.Sprint(body["subscriptions"]) != "[#]" {
		t.Fatalf("unfocus = %d %v", code, body)
	}
	ws.readUntil("the whole tree", func() bool { return ws.last.Epoch != epoch && len(ws.names) == 6 })
	if got := ws.nameList(); got != "/topic/SN000001/a /topic/SN000001/b /topic/SN000001/c /topic/SN000002/a /topic/SN000002/b other/x" {
		t.Fatalf("tree after unfocus = %s", got)
	}

	// A new connect drops the focus.
	if code, _ := e.do("POST", "/api/focus", device, nil); code != 200 {
		t.Fatalf("second focus = %d", code)
	}
	code, body = e.do("POST", "/api/connect", map[string]string{"id": p.ID}, nil)
	if code != 200 || body["focus"] != nil || fmt.Sprint(body["subscriptions"]) != "[#]" {
		t.Fatalf("connect after focus = %d %v", code, body)
	}
	ws.readUntil("the profile subscription", func() bool { return ws.has("other/x") })

	e.do("POST", "/api/disconnect", nil, nil)
	if code, _ := e.do("POST", "/api/focus", device, nil); code != 409 {
		t.Fatalf("focus after disconnect = %d", code)
	}
}

// gatedProxy forwards a TCP connection to the broker and can hold back
// everything the broker sends, like a client that is seconds behind a busy
// subscription: acks queue behind the publishes already on their way.
type gatedProxy struct {
	mu   sync.Mutex
	open chan struct{} // closed while traffic flows
}

func startProxy(t *testing.T, target int) (*gatedProxy, int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	g := &gatedProxy{open: make(chan struct{})}
	close(g.open)
	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", target))
			if err != nil {
				client.Close()
				continue
			}
			t.Cleanup(func() { client.Close(); upstream.Close() })
			go io.Copy(upstream, client)
			go func() {
				buf := make([]byte, 32<<10)
				for {
					n, err := upstream.Read(buf)
					if err != nil {
						client.Close()
						return
					}
					g.mu.Lock()
					open := g.open
					g.mu.Unlock()
					<-open
					if _, err := client.Write(buf[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	return g, l.Addr().(*net.TCPAddr).Port
}

func (g *gatedProxy) hold() {
	g.mu.Lock()
	g.open = make(chan struct{})
	g.mu.Unlock()
}

func (g *gatedProxy) release() {
	g.mu.Lock()
	close(g.open)
	g.mu.Unlock()
}

// On a busy broker the UNSUBACK arrives only after the backlog of old
// publishes, often later than paho's default 10 s packet timeout. Focus must
// wait for it: subscribing while the backlog is still queued loses the
// retained messages on the real broker.
func TestFocusWaitsForSlowAcks(t *testing.T) {
	mq, brokerPort := startBroker(t)
	proxy, port := startProxy(t, brokerPort)
	e := newEnv(t)
	p, err := e.book.Save(profiles.Profile{Name: "slow", Protocol: "mqtt", Host: "127.0.0.1", Port: port,
		Subscriptions: []string{"#"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	mq.Publish("/topic/SN000001/a", []byte("v"), true, 0)
	mq.Publish("/topic/SN000002/a", []byte("v"), true, 0)
	if code, body := e.do("POST", "/api/connect", map[string]string{"id": p.ID}, nil); code != 200 {
		t.Fatalf("connect = %d %v", code, body)
	}
	ws := e.dialWS()
	ws.readUntil("the retained topics", func() bool { return len(ws.names) == 2 })

	proxy.hold()
	for i := 0; i < 200; i++ {
		mq.Publish(fmt.Sprintf("backlog/%d", i), []byte("old"), false, 0)
	}
	type result struct {
		code   int
		status broker.Status
		err    error
	}
	done := make(chan result, 1)
	go func() {
		req, _ := http.NewRequest("POST", e.http.URL+"/api/focus", strings.NewReader(`{"filters":["/topic/SN000001/#"]}`))
		req.Header.Set("Content-Type", "application/json")
		res, err := e.client.Do(req)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer res.Body.Close()
		var st broker.Status
		json.NewDecoder(res.Body).Decode(&st)
		done <- result{code: res.StatusCode, status: st}
	}()
	select {
	case r := <-done:
		t.Fatalf("focus returned while the broker's acks were held back: %+v", r)
	case <-time.After(11 * time.Second):
	}
	epoch := ws.last.Epoch
	proxy.release()
	r := <-done
	if r.err != nil || r.code != 200 || !r.status.Focus || len(r.status.SubErrors) != 0 {
		t.Fatalf("focus = %+v", r)
	}
	ws.readUntil("the focused retained topic", func() bool { return ws.last.Epoch != epoch && ws.has("/topic/SN000001/a") })
	mq.Publish("/topic/SN000001/live", []byte("v"), false, 0)
	ws.readUntil("the focused live topic", func() bool { return ws.has("/topic/SN000001/live") })
	if got := ws.nameList(); got != "/topic/SN000001/a /topic/SN000001/live" {
		t.Fatalf("focused tree = %s", got)
	}
}

func TestConnectErrorIsReported(t *testing.T) {
	e := newEnv(t)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens here any more

	p, _ := e.book.Save(profiles.Profile{Protocol: "mqtt", Host: "127.0.0.1", Port: port}, false)
	if code, _ := e.do("POST", "/api/connect", map[string]string{"id": p.ID}, nil); code != 200 {
		t.Fatalf("connect = %d", code)
	}
	e.waitFor("the connect error", func() bool { return e.br.Status().Error != "" })
	if s := e.br.Status(); s.State != broker.StateConnecting {
		t.Fatalf("status = %+v", s)
	}
	if code, _ := e.do("POST", "/api/connect", map[string]string{"id": "nope"}, nil); code != 404 {
		t.Fatalf("connect with unknown profile = %d", code)
	}
}

func TestProfilesAPINeverReturnsPasswords(t *testing.T) {
	e := newEnv(t)
	code, saved := e.do("POST", "/api/profiles", map[string]any{"protocol": "mqtts", "host": "broker.example", "port": 8883,
		"username": "u", "password": "hunter2", "subscriptions": []string{"/topic/#"}}, nil)
	if code != 200 || saved["hasPassword"] != true || saved["password"] != nil {
		t.Fatalf("save = %d %v", code, saved)
	}

	res, err := e.client.Get(e.http.URL + "/api/profiles")
	if err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	raw.ReadFrom(res.Body)
	res.Body.Close()
	if strings.Contains(raw.String(), "hunter2") || !strings.Contains(raw.String(), "broker.example") {
		t.Fatalf("profiles = %s", raw.String())
	}
	if stored, _ := e.book.Get(saved["id"].(string)); stored.Password != "hunter2" {
		t.Fatalf("stored password = %q", stored.Password)
	}

	if code, body := e.do("POST", "/api/profiles", map[string]any{"protocol": "mqtt", "host": "h", "port": 1883,
		"subscriptions": []string{"a/#/b"}}, nil); code != 400 || !strings.Contains(fmt.Sprint(body["error"]), "last level") {
		t.Fatalf("bad filter = %d %v", code, body)
	}
	if code, _ := e.do("DELETE", "/api/profiles/"+saved["id"].(string), nil, nil); code != 200 {
		t.Fatalf("delete = %d", code)
	}
	if code, _ := e.do("DELETE", "/api/profiles/"+saved["id"].(string), nil, nil); code != 404 {
		t.Fatalf("second delete = %d", code)
	}
}

func TestCrossSiteRequestsAreRefused(t *testing.T) {
	e := newEnv(t)
	cases := map[string]func(*http.Request){
		"foreign origin":   func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") },
		"form content":     func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") },
		"rebound hostname": func(r *http.Request) { r.Host = "evil.example" },
	}
	for name, mutate := range cases {
		if code, body := e.do("POST", "/api/clear", nil, mutate); code != http.StatusForbidden {
			t.Errorf("%s: status = %d %v, want 403", name, code, body)
		}
	}
	sameOrigin := func(r *http.Request) { r.Header.Set("Origin", e.http.URL) }
	if code, body := e.do("POST", "/api/clear", nil, sameOrigin); code != 200 {
		t.Errorf("same origin: status = %d %v", code, body)
	}

	_, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(e.http.URL, "http")+"/ws",
		&websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://evil.example"}}})
	if err == nil {
		t.Error("websocket from a foreign origin was accepted")
	}

	res, err := e.client.Get(e.http.URL + "/")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("ui: %v %v", res, err)
	}
	res.Body.Close()
}

func TestEncodePayload(t *testing.T) {
	if w := encodePayload([]byte("hello"), 10); w.Text == nil || *w.Text != "hello" || w.Trunc || w.Size != 5 {
		t.Errorf("text = %+v", w)
	}
	// The cut must not split the two-byte "ä".
	if w := encodePayload([]byte("aä"), 2); w.Text == nil || *w.Text != "a" || !w.Trunc || w.Size != 3 {
		t.Errorf("truncated = %+v", w)
	}
	if w := encodePayload([]byte{0xff, 0xfe}, 10); w.Text != nil || w.Base64 != "//4=" {
		t.Errorf("binary = %+v", w)
	}
	if w := encodePayload(nil, 10); w.Text == nil || *w.Text != "" || w.Size != 0 {
		t.Errorf("empty = %+v", w)
	}
}

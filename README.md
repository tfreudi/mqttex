# mqttex

An MQTT 5 explorer for brokers with a lot of topics. It stays responsive with tens of thousands of topics and tens of thousands of messages per second, where GUI explorers that store or render every message fall over.

One Go binary, no database, UI in your browser.

![mqttex showing a filtered topic tree and the history of one topic](docs/screenshot.png)

## Features

- Live topic tree with message counts, subtree totals and an activity indicator per row
- Instant filter over all topics (several terms are combined with AND, a comma lists alternatives: `SN1, SN2, SN3 post/json`; `/` focuses the input)
- Per-topic view: latest value (JSON is pretty-printed), message history with millisecond timestamps and the interval between messages, so you can see at a glance whether a device is still pushing
- Compare several topics side by side: one column per device, one row per JSON field, live values, differences highlighted
- Publish with QoS and retain, and clear retained messages
- Saved connection profiles: mqtt, mqtts, ws, wss, username/password, several subscriptions
- Connection problems are shown with their cause: refused connections, failed authentication and subscriptions the broker rejected (for example because of an ACL)
- Keyboard navigation in the tree, light and dark mode

## Install

Requires Go 1.27 or newer.

```bash
go install github.com/silasmartin/mqttex@latest
mqttex
```

Or build it from source:

```bash
git clone https://github.com/silasmartin/mqttex.git
cd mqttex
go build -o mqttex .
./mqttex
```

The UI opens at `http://127.0.0.1:18830`. Create a connection with **New**, then press **Connect**.

Subscriptions are MQTT topic filters: `#` matches all remaining levels, `+` matches exactly one level. `*` is not a wildcard. If your topics start with a slash, the filter has to as well: `/devices/#`, not `devices/#`.

### Flags

| Flag | Default | |
|---|---|---|
| `-addr` | `127.0.0.1:18830` | address the UI is served on |
| `-profiles` | `<user config dir>/mqttex/profiles.json` | connection profiles file |
| `-max-topics` | `2000000` | stop adding topics beyond this number |
| `-no-open` | | do not open the browser on start |
| `-web` | | serve the UI from a directory instead of the embedded copy (development) |

## Compare devices

To see several devices next to each other, add their topics to the comparison: select a topic and press **+ Compare**, or press `c` on a topic in the tree. Topics in the comparison carry a `cmp` badge in the tree. The **Compare** tab shows them as a table:

- one column per topic, titled by the part of the topic that differs (for `/topic/SN1/V0/post/json` and `/topic/SN2/V0/post/json` that is `SN1` and `SN2`), with the age of its last message
- one row per JSON field (`a.b.c`, arrays as `a[0]`, keys with a dot or bracket quoted as `a["b.c"]`); payloads that are not JSON form a single `(payload)` row
- strings keep their quotes, so `"21"` next to `21` counts as a difference; numbers keep the digits that were sent (`1.0`, integers beyond 2^53) in browsers that give `JSON.parse` the source text, such as Chromium-based ones, and are rounded as usual elsewhere
- rows whose values differ are highlighted, changed values flash, missing fields show `-`
- **Only differences** hides the rows that are equal everywhere, × removes a column, **Clear** empties the list
- the table holds about 10 000 values (312 fields with 32 topics); fields beyond that are counted below the table instead of shown, so large arrays cannot freeze the page

With a filter active, **Compare matches** next to the filter adds all matching topics at once. Several serial numbers work as comma-separated alternatives (`SN1, SN2, SN3 post/json`), and a pasted list with one serial number per line (or cells copied from a spreadsheet) is turned into such a filter. Topics beyond the limit of 32 are skipped with a notice.

**Add device** takes a serial number and builds the topic from the compared ones by swapping the device part, so you do not have to find each device in the tree. The device part is the segment in which the compared topics differ, or, with a single topic, the segment that looks most like the serial number; of these guesses the first topic the broker has is taken, if need be one that only differs in upper and lower case. A full topic works as well. Up to 32 topics can be compared; the list is kept in the browser across reloads.

While the Compare tab is open, it gets the full payload of each topic (up to 64 KB), independent of the shortened previews in the tree. Values longer than 200 characters are cut in their cell and end with a hash, so values that only differ further in still count as different; binary payloads show their size and a hash.

**Only compared devices** takes the load off when the profile subscribes to a busy broker (say `#` with 100k topics) but you only look at a few devices. Instead of the profile subscriptions, mqttex then subscribes to the whole device of each compared topic, `/topic/{SN}/#` for `/topic/{SN}/V0/post/json`, and nothing else. Switching it on, and every change to the comparison while it is on, rebuilds the topic tree: retained values show up at once, the rest with the next message. **Add device** then no longer needs the topic to be in the tree, since it only arrives once it is subscribed. The top bar shows `focus: N devices` while it is active. Switching it off, or emptying the comparison, restores the profile subscriptions. The setting is kept in the browser; a new connection starts on the profile subscriptions and the browser narrows them again once it is connected.

## How it handles the load

- **Ingest is one map lookup.** Every message updates an in-memory topic table (about 20 ns, no allocation). Nothing is written to disk and no per-message event is emitted.
- **The browser gets state, not events.** Five times per second each browser receives what changed since its last update: new topic names (once), message counts as a binary frame, payload previews only for the rows currently on screen, and full messages only for the topic that is open. A slow browser simply gets fewer, larger updates and never causes message loss.
- **The tree is virtualized.** 40 000 visible rows are rendered with a few dozen DOM nodes.

Measured on an Apple M5 Pro against a local broker with 40 000 topics:

| Load | Server | Browser |
|---|---|---|
| 20 000 msg/s | 22 % of one core, 83 MB | 60 fps, no long tasks |
| 100 000 msg/s | 76 % of one core, 85 MB | 60 fps, 18 MB JS heap |

History is recorded for a topic while it is open (last 500 messages). All other topics keep only their latest message. Nothing survives a restart.

## Security

- mqttex has no login. It binds to loopback by default; if you pass a non-loopback `-addr`, everyone who can reach the port can use your saved broker credentials.
- Cross-origin requests, WebSocket connections from other origins and requests with a foreign `Host` header are refused, so another website open in your browser cannot drive the API.
- Profiles, including passwords, are stored as plain text in the profiles file with mode `0600`. The API never returns a stored password.

## Development

```bash
go test -race ./...            # unit tests and an end-to-end test against an embedded broker
(cd web && node --test)        # topic tree and compare table models
go run . -web web              # serve the UI from disk while editing it
```

`cmd/loadgen` starts a local broker and floods it, so you can try mqttex under load without touching a real broker:

```bash
go run ./cmd/loadgen -topics 40000 -rate 20000   # broker on mqtt://127.0.0.1:18831
```

The frontend is plain JavaScript and CSS without a build step. `web/tree.js` holds the tree model and has no DOM dependency.

## License

[MIT](LICENSE)

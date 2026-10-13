import { TopicTree, filterPaste } from './tree.js';
import { flatten, digest, columnLabels, buildRows, deviceTopics, pickTopic, rowLimit, deviceFilters } from './compare.js';
import { formatBytes, formatInterval } from './format.js';

const ROW_H = 24;
const OVERSCAN = 6;
const ACTIVE_MS = 1500; // how long a row counts as "just received a message"
const HISTORY_MAX = 500;
const MAX_COMPARED = 32; // the server ignores ids beyond this

const $ = (id) => document.getElementById(id);

// All times are shown in German local time, whatever the machine is set to.
const TZ = 'Europe/Berlin';
const timeFmt = new Intl.DateTimeFormat('de-DE', {
  timeZone: TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3, hour12: false,
});
const dateFmt = new Intl.DateTimeFormat('de-DE', { timeZone: TZ, dateStyle: 'medium' });
const compactFmt = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });
const fullFmt = new Intl.NumberFormat('en');

const compact = (n) => (n < 10_000 ? fullFmt.format(n) : compactFmt.format(n));

// ---------------------------------------------------------------- state

const tree = new TopicTree();
const previews = new Map(); // topic id -> latest preview, only for rows that were on screen
let epoch = 0;
let ws = null;
let wsUp = false;
let status = { state: 'disconnected' };
let stats = { topics: 0, messages: 0, bytes: 0, dropped: 0 };
let rateSamples = [];
let profiles = [];

let cursor = null; // keyboard position in the tree
let selectedId = -1;
let history = []; // oldest first
let pinnedN = null; // history entry shown instead of the latest one
let shownN = null; // entry currently rendered in the value box

// Compared topics are kept by name: ids are only valid within one epoch.
const COMPARE_KEY = 'mqttex.compare';
let compareNames = loadCompare();
const compareValues = new Map(); // topic id -> latest message
let compareIds = new Set(); // resolved ids of compareNames
let resolvedKey = null; // compareIds as last resolved, to notice changes
let sentCompareKey = ''; // ids the server streams, none while the table is hidden
let compareDirty = true; // the table has to be updated
let activeTab = 'topic';

// "Only compared devices": the broker subscription is narrowed to the devices
// in the comparison. The server owns the state (status.focus); this is the wish.
const FOCUS_KEY = 'mqttex.focus';
let focusOn = loadFocus();
let focusTimer = null;
// The broker answers (un)subscribes only after its queue, so a switch can take
// minutes: one request at a time, and changes meanwhile are sent once after it.
let focusBusy = false;
let focusAgain = false;

// ---------------------------------------------------------------- toasts

function toast(kind, text) {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = text;
  $('toasts').append(el);
  setTimeout(() => el.remove(), kind === 'error' ? 8000 : 3500);
}

// A destructive button asks for a second click instead of opening a dialog.
function confirmClick(button, action) {
  let armed = null;
  const label = button.textContent;
  const disarm = () => {
    clearTimeout(armed);
    armed = null;
    button.textContent = label;
    button.classList.remove('armed');
  };
  button.addEventListener('click', () => {
    if (armed) {
      disarm();
      action();
      return;
    }
    button.textContent = 'Click again to confirm';
    button.classList.add('armed');
    armed = setTimeout(disarm, 3000);
  });
  return disarm;
}

// ---------------------------------------------------------------- api

async function api(method, path, body, timeoutMs) {
  let res;
  try {
    res = await fetch(path, {
      method,
      headers: method === 'POST' ? { 'Content-Type': 'application/json' } : {},
      body: method === 'POST' ? JSON.stringify(body ?? {}) : undefined,
      signal: timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined,
    });
  } catch (err) {
    if (err.name === 'TimeoutError') throw new Error(`no answer within ${Math.round(timeoutMs / 1000)} s`);
    throw new Error('the mqttex server is not reachable, is it still running?');
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

// ---------------------------------------------------------------- websocket

function connectWS() {
  ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
  ws.binaryType = 'arraybuffer';
  ws.onopen = () => {
    wsUp = true;
    resetClient(); // the server starts every socket from zero
    renderTop();
  };
  ws.onmessage = (ev) => {
    if (typeof ev.data === 'string') onTick(JSON.parse(ev.data));
    else onCounts(new Uint32Array(ev.data));
  };
  ws.onclose = () => {
    wsUp = false;
    renderTop();
    setTimeout(connectWS, 1000);
  };
}

function send(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function resetClient() {
  tree.reset();
  if ($('filter').value) tree.setFilter($('filter').value);
  previews.clear();
  rateSamples = [];
  cursor = null;
  lastWatchKey = '';
  compareValues.clear();
  compareIds = new Set();
  resolvedKey = null;
  sentCompareKey = ''; // the server dropped the list as well
  headKey = null; // new rows, so values of the old epoch do not flash
  compareDirty = true;
  deselect();
  scheduleRender();
}

function onTick(t) {
  if (t.reset || t.epoch !== epoch) {
    if (epoch !== 0) resetClient();
    epoch = t.epoch;
  }
  if (t.names) tree.addTopics(t.first, t.names);
  if (t.compare) {
    for (const m of t.compare) {
      if (!compareIds.has(m.id)) continue;
      // Devices often send the same payload again. That only moves the age in
      // the column header, which is read from compareValues every second.
      const old = compareValues.get(m.id);
      if (old && old.s === m.s && old.b === m.b && old.size === m.size && old.trunc === m.trunc) {
        old.ts = m.ts;
        continue;
      }
      compareValues.set(m.id, m);
      compareDirty = true;
    }
  }
  syncCompare();
  if (t.previews) for (const p of t.previews) previews.set(p.id, p);
  if (previews.size > 20_000) {
    previews.clear();
    lastWatchKey = ''; // forces a new watch message, which refills the visible rows
  }
  if (t.history && t.selected === selectedId) appendHistory(t.history);

  stats = t.stats;
  setStatus(t.status);
  const now = performance.now();
  if (rateSamples.length && stats.messages < rateSamples.at(-1).m) rateSamples = [];
  rateSamples.push({ t: now, m: stats.messages });
  while (rateSamples.length > 2 && now - rateSamples[0].t > 2000) rateSamples.shift();

  renderTop();
  scheduleRender();
}

// A new broker connection starts on the profile subscriptions again, so the
// focus has to be sent anew once it is up.
function setStatus(next) {
  const was = status.state;
  status = next;
  if (status.state === 'connected' && was !== 'connected') syncFocus();
}

// Binary frame: uint32 [type, n, id, count, id, count, ...]
function onCounts(frame) {
  if (frame[0] !== 1) return;
  tree.applyCounts(frame.subarray(2, 2 + 2 * frame[1]), performance.now());
  scheduleRender();
}

// ---------------------------------------------------------------- top bar

function renderTop() {
  $('stat-topics').textContent = compact(stats.topics);
  $('stat-topics').title = fullFmt.format(stats.topics);
  $('stat-messages').textContent = compact(stats.messages);
  $('stat-messages').title = fullFmt.format(stats.messages);
  $('stat-bytes').textContent = formatBytes(stats.bytes);
  let rate = 0;
  if (rateSamples.length > 1) {
    const a = rateSamples[0];
    const b = rateSamples.at(-1);
    rate = ((b.m - a.m) * 1000) / (b.t - a.t);
  }
  $('stat-rate').textContent = compact(Math.round(rate));

  const state = $('state');
  state.dataset.state = status.state;
  state.textContent = status.profileName ? `${status.state} · ${status.profileName}` : status.state;
  state.title = status.clientId ? `client id ${status.clientId}` : '';

  const subs = status.subscriptions ?? [];
  $('focus').hidden = !status.focus && !focusBusy;
  $('focus').classList.toggle('busy', focusBusy);
  setText($('focus'), focusBusy ? 'focus: switching…' : `focus: ${subs.length} ${subs.length === 1 ? 'device' : 'devices'}`);
  $('focus').title = focusBusy ? 'Waiting for the broker to switch the subscriptions' : status.focus ? `Subscribed only to ${subs.join(', ')}` : '';

  const connected = status.state !== 'disconnected';
  $('connect').textContent = connected ? 'Disconnect' : 'Connect';
  $('connect').disabled = !connected && profiles.length === 0;
  $('edit').disabled = profiles.length === 0;

  const problems = [];
  if (!wsUp) problems.push('Lost the connection to the mqttex server, retrying every second.');
  if (status.error) problems.push(`Broker: ${status.error}`);
  for (const e of status.subErrors ?? []) problems.push(`Subscription ${e}`);
  if (stats.dropped > 0) {
    problems.push(`${fullFmt.format(stats.dropped)} messages on new topics were ignored because the -max-topics limit is reached.`);
  }
  $('banner').hidden = problems.length === 0;
  $('banner').replaceChildren(...problems.map((p) => Object.assign(document.createElement('p'), { textContent: p })));
}

// ---------------------------------------------------------------- tree (virtual list)

const treeEl = $('tree');
const spacer = $('tree-spacer');
const pool = [];
let renderQueued = false;
let lastWatchKey = '';

function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => {
    renderQueued = false;
    renderTree();
    renderDetail();
    renderCompare();
  });
}

function makeRow() {
  const el = document.createElement('div');
  el.className = 'row';
  el.setAttribute('role', 'treeitem');
  const part = (cls) => {
    const s = document.createElement('span');
    s.className = cls;
    el.append(s);
    return s;
  };
  el._indent = part('indent');
  el._chev = part('chev');
  el._dot = part('dot');
  el._name = part('name');
  el._cmp = part('cmp-badge');
  el._cmp.textContent = 'cmp';
  el._cmp.title = 'In the comparison';
  el._val = part('val');
  el._meta = part('meta');
  spacer.append(el);
  return el;
}

function setText(span, text) {
  if (span._t !== text) {
    span._t = text;
    span.textContent = text;
  }
}

function fillRow(el, node, index, now) {
  el._node = node;
  el.hidden = false;
  el.style.transform = `translateY(${index * ROW_H}px)`;
  el._indent.style.width = `${node.depth * 14}px`;

  const folder = node.kids.length > 0;
  const open = folder && tree.isOpen(node);
  setText(el._chev, folder ? (open ? '▾' : '▸') : '');
  setText(el._name, node.name === '' ? '(empty)' : node.name);
  el._name.classList.toggle('muted', node.name === '');
  el._dot.classList.toggle('on', now - node.active < ACTIVE_MS && node.active > 0);
  el._cmp.hidden = !compareIds.has(node.id);

  let value = '';
  if (node.id >= 0) {
    const p = previews.get(node.id);
    if (p) value = p.s !== undefined ? p.s : `binary, ${formatBytes(p.size)}`;
  }
  setText(el._val, value);

  let meta;
  if (folder) {
    meta = `${compact(node.leaves)} ${node.leaves === 1 ? 'topic' : 'topics'} · ${compact(node.total)} msgs`;
  } else {
    meta = `× ${compact(node.count)}`;
  }
  setText(el._meta, meta);

  el.classList.toggle('selected', node.id >= 0 && node.id === selectedId);
  el.classList.toggle('cursor', node === cursor);
  if (folder) el.setAttribute('aria-expanded', String(open));
  else el.removeAttribute('aria-expanded');
}

function renderTree() {
  const rows = tree.rows();
  spacer.style.height = `${rows.length * ROW_H}px`;

  const top = treeEl.scrollTop;
  const first = Math.max(0, Math.floor(top / ROW_H) - OVERSCAN);
  const last = Math.min(rows.length, Math.ceil((top + treeEl.clientHeight) / ROW_H) + OVERSCAN);
  const need = Math.max(0, last - first);
  while (pool.length < need) pool.push(makeRow());

  const now = performance.now();
  const watch = [];
  for (let i = 0; i < pool.length; i++) {
    if (i >= need) {
      pool[i].hidden = true;
      pool[i]._node = null;
      continue;
    }
    const node = rows[first + i];
    fillRow(pool[i], node, first + i, now);
    if (node.id >= 0) watch.push(node.id);
  }

  // Tell the server which topics are on screen; it only sends values for those.
  const key = watch.join();
  if (key !== lastWatchKey && epoch !== 0) {
    lastWatchKey = key;
    send({ t: 'watch', epoch, ids: watch });
  }

  $('filter-count').textContent = tree.filtering ? `${fullFmt.format(tree.matches)} of ${fullFmt.format(tree.size)}` : '';
  $('filter-compare').hidden = !(tree.filtering && tree.matches > 0);
  const empty = $('tree-empty');
  empty.hidden = rows.length > 0;
  if (rows.length === 0) empty.textContent = emptyTreeText();
}

function emptyTreeText() {
  if (tree.size > 0) return 'No topic matches this filter.';
  if (status.state === 'connected') {
    return `Connected. Waiting for messages on ${(status.subscriptions ?? []).join(', ')}`;
  }
  if (status.state === 'connecting') return `Connecting to ${status.profileName} ...`;
  return profiles.length ? 'Pick a connection and press Connect.' : 'Create a connection with "New" to get started.';
}

treeEl.addEventListener('scroll', scheduleRender, { passive: true });
new ResizeObserver(scheduleRender).observe(treeEl);
setInterval(scheduleRender, 1000); // lets activity dots fade when traffic stops

treeEl.addEventListener('click', (ev) => {
  const el = ev.target.closest('.row');
  if (!el || !el._node) return;
  const node = el._node;
  cursor = node;
  if (node.id >= 0 && !ev.target.classList.contains('chev')) select(node.id);
  else tree.toggle(node);
  scheduleRender();
});

treeEl.addEventListener('keydown', (ev) => {
  if (ev.key === 'c' && !ev.metaKey && !ev.ctrlKey && !ev.altKey && !ev.target.matches('input, textarea, select')) {
    if (cursor && cursor.id >= 0 && !ev.repeat) {
      ev.preventDefault();
      toggleCompare(tree.names[cursor.id]);
    }
    return;
  }
  const rows = tree.rows();
  if (rows.length === 0) return;
  let i = cursor ? rows.indexOf(cursor) : -1;
  const open = cursor && cursor.kids.length > 0 && tree.isOpen(cursor);
  switch (ev.key) {
    case 'ArrowDown': i = Math.min(rows.length - 1, i + 1); break;
    case 'ArrowUp': i = Math.max(0, i - 1); break;
    case 'Home': i = 0; break;
    case 'End': i = rows.length - 1; break;
    case 'PageDown': i = Math.min(rows.length - 1, i + Math.floor(treeEl.clientHeight / ROW_H)); break;
    case 'PageUp': i = Math.max(0, i - Math.floor(treeEl.clientHeight / ROW_H)); break;
    case 'ArrowRight':
      if (!cursor) return;
      if (cursor.kids.length && !open) tree.toggle(cursor, true);
      else if (open) i++;
      break;
    case 'ArrowLeft':
      if (!cursor) return;
      if (open) tree.toggle(cursor, false);
      else if (cursor.parent !== tree.root) i = rows.indexOf(cursor.parent);
      break;
    case 'Enter':
    case ' ':
      if (!cursor) return;
      if (cursor.id >= 0) select(cursor.id);
      else tree.toggle(cursor);
      break;
    default:
      return;
  }
  ev.preventDefault();
  if (i >= 0 && i < rows.length) {
    cursor = rows[i];
    if (cursor.id >= 0) select(cursor.id);
    const y = i * ROW_H;
    if (y < treeEl.scrollTop) treeEl.scrollTop = y;
    else if (y + ROW_H > treeEl.scrollTop + treeEl.clientHeight) treeEl.scrollTop = y + ROW_H - treeEl.clientHeight;
  }
  scheduleRender();
});

// ---------------------------------------------------------------- filter

let filterTimer = null;
function applyFilter() {
  filterTimer = null;
  tree.setFilter($('filter').value);
  treeEl.scrollTop = 0;
  scheduleRender();
}
$('filter').addEventListener('input', () => {
  clearTimeout(filterTimer);
  filterTimer = setTimeout(applyFilter, 100);
});
// A pasted column of serial numbers becomes a list of alternatives, see filterPaste.
$('filter').addEventListener('paste', (ev) => {
  const input = ev.target;
  const list = filterPaste(ev.clipboardData?.getData('text') ?? '', input.value.slice(0, input.selectionStart), input.value.slice(input.selectionEnd));
  if (list === null) return;
  ev.preventDefault();
  input.setRangeText(list, input.selectionStart, input.selectionEnd, 'end');
  input.dispatchEvent(new Event('input', { bubbles: true }));
});
$('filter-compare').addEventListener('click', () => {
  // The filter is applied after a short delay; make sure the matches are current.
  if (filterTimer !== null) {
    clearTimeout(filterTimer);
    applyFilter();
  }
  const missing = tree.matchingIds().map((id) => tree.names[id]).filter((name) => !compareNames.includes(name));
  const added = missing.slice(0, Math.max(0, MAX_COMPARED - compareNames.length));
  if (added.length > 0) setCompare([...compareNames, ...added]);
  if (added.length < missing.length) {
    toast('error', `Added ${added.length} of ${missing.length} matches; the comparison holds at most ${MAX_COMPARED} topics.`);
  } else if (added.length === 0) {
    toast('ok', 'All matches are already in the comparison.');
  } else {
    toast('ok', `Added ${added.length} ${added.length === 1 ? 'topic' : 'topics'} to the comparison.`);
  }
  showTab('compare');
});
$('filter').addEventListener('keydown', (ev) => {
  if (ev.key === 'ArrowDown') {
    ev.preventDefault();
    treeEl.focus();
  }
});
document.addEventListener('keydown', (ev) => {
  const typing = ev.target.matches('input, textarea, select');
  if (ev.key === '/' && !typing && !$('profile-dialog').open) {
    ev.preventDefault();
    $('filter').focus();
    $('filter').select();
  }
});

// ---------------------------------------------------------------- selected topic

function select(id) {
  if (id === selectedId) return;
  selectedId = id;
  history = [];
  pinnedN = null;
  shownN = null;
  $('d-history').replaceChildren();
  $('d-value').textContent = '';
  $('detail').hidden = false;
  $('detail-empty').hidden = true;
  $('d-topic').textContent = tree.names[id];
  $('p-topic').value = tree.names[id];
  updateCompareButton();
  send({ t: 'select', epoch, id });
}

function deselect() {
  if (selectedId >= 0) send({ t: 'select', epoch, id: -1 });
  selectedId = -1;
  history = [];
  pinnedN = null;
  shownN = null;
  $('detail').hidden = true;
  $('detail-empty').hidden = false;
}

function payloadText(m) {
  if (m.s !== undefined) return m.s;
  return `binary payload, base64:\n${m.b ?? ''}`;
}

function appendHistory(msgs) {
  const list = $('d-history');
  for (const m of msgs) {
    if (history.length && m.n <= history.at(-1).n) continue;
    const prev = history.at(-1);
    history.push(m);

    const li = document.createElement('li');
    li.dataset.n = m.n;
    li.tabIndex = 0;
    const cell = (cls, text) => {
      const s = document.createElement('span');
      s.className = cls;
      s.textContent = text;
      li.append(s);
    };
    cell('h-time', timeFmt.format(m.ts));
    cell('h-delta', prev ? `+${formatInterval(m.ts - prev.ts)}` : '');
    cell('h-size', formatBytes(m.size));
    cell('h-text', m.size === 0 ? '(empty payload)' : payloadText(m).slice(0, 200).replace(/\s+/g, ' '));
    list.prepend(li);
  }
  while (history.length > HISTORY_MAX) {
    history.shift();
    list.lastElementChild?.remove();
  }
  if (pinnedN !== null && !history.some((m) => m.n === pinnedN)) pinnedN = null;
}

function renderDetail() {
  if (selectedId < 0) return;
  const node = tree.byId[selectedId];
  if (!node) return;
  $('d-count').textContent = fullFmt.format(node.count);

  const latest = history.at(-1);
  if (!latest) return;
  const before = history.at(-2);
  $('d-last').textContent = timeFmt.format(latest.ts);
  $('d-last').title = dateFmt.format(latest.ts);
  $('d-interval').textContent = before ? formatInterval(latest.ts - before.ts) : 'waiting for the next message';

  const shown = pinnedN === null ? latest : history.find((m) => m.n === pinnedN) ?? latest;
  $('d-value-title').textContent = pinnedN === null ? 'Latest value' : `Message #${fullFmt.format(shown.n)} from ${timeFmt.format(shown.ts)}`;
  $('d-live').hidden = pinnedN === null;
  if (shown.n === shownN) return;

  const flash = shownN !== null && pinnedN === null;
  shownN = shown.n;
  $('d-size').textContent = formatBytes(shown.size);
  $('d-qos').textContent = String(shown.qos);
  $('d-retain').textContent = shown.retain ? 'yes' : 'no';

  let text = payloadText(shown);
  if (shown.s !== undefined && !shown.trunc && /^\s*[[{]/.test(text)) {
    try {
      text = JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      // not JSON after all, show it as received
    }
  }
  if (shown.size === 0) text = '(empty payload)';
  if (shown.trunc) text += `\n\n(cut off, the full payload is ${formatBytes(shown.size)})`;
  const value = $('d-value');
  value.textContent = text;
  if (flash) {
    value.classList.remove('flash');
    void value.offsetWidth; // restart the animation
    value.classList.add('flash');
  }

  const props = [];
  if (shown.contentType) props.push(`content-type: ${shown.contentType}`);
  for (const [k, v] of Object.entries(shown.userProps ?? {})) props.push(`${k}: ${v}`);
  $('d-props').hidden = props.length === 0;
  $('d-props').textContent = props.join('   ');

  for (const li of $('d-history').children) li.classList.toggle('pinned', Number(li.dataset.n) === pinnedN);
}

function pinFromEvent(ev) {
  const li = ev.target.closest('li');
  if (!li) return;
  pinnedN = Number(li.dataset.n);
  shownN = null;
  scheduleRender();
}
$('d-history').addEventListener('click', pinFromEvent);
$('d-history').addEventListener('keydown', (ev) => {
  if (ev.key === 'Enter' || ev.key === ' ') {
    ev.preventDefault();
    pinFromEvent(ev);
  }
});
$('d-live').addEventListener('click', () => {
  pinnedN = null;
  shownN = null;
  scheduleRender();
});

async function copy(text, what) {
  try {
    await navigator.clipboard.writeText(text);
    toast('ok', `Copied the ${what}.`);
  } catch {
    toast('error', `Could not copy the ${what}: the browser denied clipboard access.`);
  }
}
$('d-copy').addEventListener('click', () => copy($('d-topic').textContent, 'topic'));
$('d-compare').addEventListener('click', () => {
  if (selectedId >= 0) toggleCompare(tree.names[selectedId]);
});
$('d-copy-value').addEventListener('click', () => copy($('d-value').textContent, 'value'));

// ---------------------------------------------------------------- compare

function loadCompare() {
  try {
    const list = JSON.parse(localStorage.getItem(COMPARE_KEY) ?? '[]');
    if (Array.isArray(list)) return list.filter((n) => typeof n === 'string').slice(0, MAX_COMPARED);
  } catch {
    // private window, blocked storage or a broken entry: start empty
  }
  return [];
}

function saveCompare() {
  try {
    localStorage.setItem(COMPARE_KEY, JSON.stringify(compareNames));
  } catch {
    // only a convenience, like the last profile
  }
}

// Resolves the names to ids whenever that can change: after edits, after a
// reset and when the names of stored topics arrive. Names the server has not
// delivered yet stay out until they show up in tick.names. Payloads are only
// streamed while the table is on screen; the tree badge and the tab title
// need the names alone.
function syncCompare() {
  if (epoch === 0) return;
  const ids = [];
  for (const name of compareNames) {
    const id = tree.idOf(name);
    if (id >= 0 && ids.length < MAX_COMPARED) ids.push(id);
  }
  const key = ids.join();
  if (key !== resolvedKey) {
    resolvedKey = key;
    compareIds = new Set(ids);
    for (const id of compareValues.keys()) if (!compareIds.has(id)) compareValues.delete(id);
    compareDirty = true;
  }
  const streamed = activeTab === 'compare' ? ids : [];
  if (streamed.join() === sentCompareKey) return;
  sentCompareKey = streamed.join();
  send({ t: 'compare', epoch, ids: streamed });
}

function setCompare(names) {
  compareNames = names;
  saveCompare();
  compareDirty = true;
  syncCompare();
  syncFocus();
  updateCompareButton();
  scheduleRender();
}

function toggleCompare(name) {
  if (compareNames.includes(name)) {
    setCompare(compareNames.filter((n) => n !== name));
    return;
  }
  if (compareNames.length >= MAX_COMPARED) {
    toast('error', `Could not add ${name}: at most ${MAX_COMPARED} topics can be compared.`);
    return;
  }
  setCompare([...compareNames, name]);
}

function updateCompareButton() {
  if (selectedId < 0) return;
  const inList = compareNames.includes(tree.names[selectedId]);
  $('d-compare').textContent = inList ? '− Compare' : '+ Compare';
  $('d-compare').title = inList ? 'Remove this topic from the comparison ( c )' : 'Add this topic to the comparison ( c )';
}

function showTab(tab) {
  if (tab === activeTab) return;
  activeTab = tab;
  $('tab-topic').setAttribute('aria-selected', String(tab === 'topic'));
  $('tab-compare').setAttribute('aria-selected', String(tab === 'compare'));
  $('panel-topic').hidden = tab !== 'topic';
  $('panel-compare').hidden = tab !== 'compare';
  // Values that changed while the table was hidden arrive right after the
  // switch; they should not all flash at once.
  quietUntil = performance.now() + 1000;
  compareDirty = true;
  syncCompare();
  scheduleRender();
}
$('tab-topic').addEventListener('click', () => showTab('topic'));
$('tab-compare').addEventListener('click', () => showTab('compare'));

const flatCache = new WeakMap(); // message -> fields, so a payload is parsed once
let headKey = null; // compareNames the header row was built for
let rowEls = new Map(); // field path -> { tr, cells }, kept while the columns stay
let ageCells = [];
let quietUntil = 0; // changed cells do not flash before this time, see showTab
const emptyRow = document.createElement('tr');
const emptyCell = emptyRow.appendChild(document.createElement('td'));
emptyCell.className = 'missing';
const cutRow = document.createElement('tr'); // says how many rows rowLimit left out
const cutCell = cutRow.appendChild(document.createElement('td'));
cutCell.className = 'missing';

function fieldsOf(m) {
  let f = flatCache.get(m);
  if (!f) {
    let text = m.s;
    if (m.size === 0) text = '(empty payload)';
    // Binary payloads are only told apart, like the tree only names their size.
    else if (text === undefined) text = `binary, ${formatBytes(m.size)}, #${digest(m.b ?? '')}`;
    f = flatten(text, m.trunc);
    flatCache.set(m, f);
  }
  return f;
}

function ageText(name, now) {
  const id = tree.idOf(name);
  if (id < 0) return 'not seen yet';
  const m = compareValues.get(id);
  if (!m) return 'waiting for data';
  const ms = Math.max(0, now - m.ts);
  if (ms < 1000) return 'just now';
  if (ms < 60_000) return `${Math.floor(ms / 1000)} s ago`;
  return `${formatInterval(ms)} ago`;
}

function renderCompare() {
  setText($('tab-compare'), `Compare (${compareNames.length})`);
  if (activeTab !== 'compare') return;
  const empty = compareNames.length === 0;
  $('c-empty').hidden = !empty;
  $('c-wrap').hidden = empty;
  // Only new payloads, a changed column list or the filter mark the table
  // dirty, and then only the cells that changed are touched. The ages
  // refresh every second.
  if (compareDirty) {
    compareDirty = false;
    updateCompareTable();
  }
  const now = Date.now();
  for (const { el, name } of ageCells) setText(el, ageText(name, now));
}

function element(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

// The header changes only with the column list, so its buttons stay put
// while values stream in.
function buildCompareHead() {
  const labels = columnLabels(compareNames);
  ageCells = [];
  const head = element('tr');
  head.append(element('th', '', 'Field'));
  compareNames.forEach((name, i) => {
    const th = element('th');
    th.title = name;
    const top = element('div', 'c-col');
    const remove = element('button', 'c-remove', '×');
    remove.type = 'button';
    remove.dataset.name = name;
    remove.title = 'Remove from the comparison';
    remove.setAttribute('aria-label', `Remove ${name} from the comparison`);
    top.append(element('span', 'c-label', labels[i]), remove);
    const age = element('span', 'c-age');
    ageCells.push({ el: age, name });
    th.append(top, age);
    head.append(th);
  });
  return head;
}

function newCompareRow(path) {
  const tr = element('tr');
  const th = element('th', '', path);
  th.scope = 'row';
  tr.append(th);
  const cells = compareNames.map(() => {
    const td = element('td');
    td._v = null; // nothing shown yet
    tr.append(td);
    return td;
  });
  return { tr, cells };
}

// Returns true when the cell showed another value before, which makes it flash.
function fillCell(td, v) {
  if (td._v === v) return false;
  const changed = typeof td._v === 'string' && v !== undefined;
  td._v = v;
  td.textContent = v ?? '-';
  td.classList.toggle('missing', v === undefined);
  return changed;
}

function updateCompareTable() {
  const table = $('c-table');
  const body = table.tBodies[0];
  // Topic names may contain any character, a newline included.
  const key = JSON.stringify(compareNames);
  if (key !== headKey) {
    headKey = key;
    rowEls = new Map();
    table.tHead.replaceChildren(buildCompareHead());
    body.replaceChildren();
    emptyCell.colSpan = compareNames.length + 1;
    cutCell.colSpan = compareNames.length + 1;
  }

  const columns = compareNames.map((name) => {
    const m = compareValues.get(tree.idOf(name));
    return m ? fieldsOf(m) : null;
  });
  const rows = buildRows(columns);
  // A field that comes back later gets a new row, which does not flash.
  const live = new Set(rows.map((r) => r.path));
  for (const path of rowEls.keys()) if (!live.has(path)) rowEls.delete(path);
  const onlyDiffs = $('c-diff').checked;

  // Rows that are not shown (by "Only differences" or beyond the row limit)
  // but exist from before are filled as well, so they do not flash with old
  // changes when they are shown again. Others are created once they are.
  const flash = performance.now() >= quietUntil;
  const limit = rowLimit(compareNames.length);
  const changed = [];
  const trs = [];
  let cut = 0;
  for (const row of rows) {
    const wanted = row.differs || !onlyDiffs;
    const shown = wanted && trs.length < limit;
    if (wanted && !shown) cut++;
    let r = rowEls.get(row.path);
    if (!r) {
      if (!shown) continue;
      r = newCompareRow(row.path);
      rowEls.set(row.path, r);
    }
    r.tr.classList.toggle('differs', row.differs);
    row.values.forEach((v, i) => {
      if (fillCell(r.cells[i], v) && flash && shown) changed.push(r.cells[i]);
    });
    if (shown) trs.push(r.tr);
  }
  if (trs.length === 0) {
    emptyCell.textContent = columns.some(Boolean) && onlyDiffs ? 'No differences.' : 'Waiting for messages on these topics.';
    trs.push(emptyRow);
  }
  if (cut > 0) {
    const fields = `${fullFmt.format(cut)} more ${cut === 1 ? 'field is' : 'fields are'} not shown`;
    cutCell.textContent = onlyDiffs ? `${fields}. Compare fewer topics to see them.` : `${fields}. Turn on Only differences or compare fewer topics to see them.`;
    trs.push(cutRow);
  }

  // Rows that stay are not moved, so a text selection in them survives.
  const keep = new Set(trs);
  for (const tr of [...body.rows]) {
    if (keep.has(tr)) continue;
    tr.remove();
    // A removed row no longer reports the end of its animations to the
    // table, so a flash still running would start over once it is back.
    for (const td of tr.cells) td.classList.remove('flash');
  }
  let at = body.firstChild;
  for (const tr of trs) {
    if (tr === at) at = at.nextSibling;
    else body.insertBefore(tr, at);
  }

  if (changed.length > 0) {
    for (const td of changed) td.classList.remove('flash');
    void body.offsetWidth; // restarts the animation in cells that are still flashing
    for (const td of changed) td.classList.add('flash');
  }
}

$('c-table').addEventListener('click', (ev) => {
  const btn = ev.target.closest('.c-remove');
  if (btn) setCompare(compareNames.filter((n) => n !== btn.dataset.name));
});
// A flashed cell would flash again whenever its row is put back into the table.
$('c-table').addEventListener('animationend', (ev) => ev.target.classList.remove('flash'));

$('c-diff').addEventListener('change', () => {
  compareDirty = true;
  scheduleRender();
});

confirmClick($('c-clear'), () => setCompare([]));

$('c-add').addEventListener('submit', (ev) => {
  ev.preventDefault();
  const value = $('c-device').value.trim();
  if (!value) return;
  const full = value.includes('/');
  if (!full && compareNames.length === 0) {
    toast('error', 'Could not add the device: add a first topic with + Compare or c in the tree, or enter a full topic.');
    return;
  }
  const guesses = deviceTopics(compareNames, value);
  if (guesses.length === 0) {
    toast('error', `Could not tell which part of the compared topics is the device ${value}. Enter the full topic instead.`);
    return;
  }
  // Several segments can pass for the device; the topic the broker has
  // decides. Topic names are case-sensitive and typed serial numbers often
  // are not, so a topic that only differs in case is the second choice.
  const exact = (n) => (tree.idOf(n) >= 0 ? n : undefined);
  const loose = (n) => {
    const id = tree.idOfIgnoringCase(n);
    return id >= 0 ? tree.names[id] : undefined;
  };
  const name = pickTopic(guesses, [exact, loose], compareNames);
  if (compareNames.includes(name)) {
    toast('error', `${name} is already in the comparison.`);
    return;
  }
  // With the focus on, the topic of a new device has never been received: it
  // only arrives once its filter is subscribed, so the best guess is taken.
  if (!status.focus && tree.idOf(name) < 0) {
    if (full) toast('error', `Could not add ${name}: no message was received on this topic so far.`);
    else toast('error', `Could not add ${value}: no message was received on ${name} or another guessed topic so far. Enter the full topic instead.`);
    return;
  }
  if (compareNames.length >= MAX_COMPARED) {
    toast('error', `Could not add ${name}: at most ${MAX_COMPARED} topics can be compared.`);
    return;
  }
  $('c-device').value = '';
  setCompare([...compareNames, name]);
});

function loadFocus() {
  try {
    return localStorage.getItem(FOCUS_KEY) === '1';
  } catch {
    return false;
  }
}

function saveFocus() {
  try {
    localStorage.setItem(FOCUS_KEY, focusOn ? '1' : '0');
  } catch {
    // only a convenience, like the comparison
  }
}

// Debounced, because every change makes the server drop its topic table and
// subscribe again: several quick edits should cost one rebuild.
function syncFocus() {
  clearTimeout(focusTimer);
  focusTimer = setTimeout(applyFocus, 300);
}

const FOCUS_TIMEOUT_MS = 180_000; // the server itself gives up after about 2 min

async function applyFocus() {
  if (focusBusy) {
    focusAgain = true; // latest wins: the state is read again once the request is done
    return;
  }
  if (status.state !== 'connected') return; // the server refuses; sent again once connected
  const subs = status.subscriptions ?? [];
  let filters;
  if (focusOn && compareNames.length > 0) {
    filters = deviceFilters(compareNames);
    // Already active, e.g. after a reload or a reconnect of the broker client.
    if (status.focus && filters.length === subs.length && filters.every((f) => subs.includes(f))) return;
  } else if (status.focus) {
    filters = [];
  } else {
    return;
  }
  setFocusBusy(true);
  try {
    setStatus(await api('POST', '/api/focus', { filters }, FOCUS_TIMEOUT_MS));
  } catch (err) {
    toast('error', filters.length > 0
      ? `Could not subscribe to the compared devices only: ${err.message}`
      : `Could not restore the profile subscriptions: ${err.message}`);
  }
  setFocusBusy(false);
  if (focusAgain) {
    focusAgain = false;
    applyFocus();
  }
}

function setFocusBusy(busy) {
  focusBusy = busy;
  $('c-focus-wait').hidden = !busy;
  renderTop();
}

$('c-focus').checked = focusOn;
$('c-focus').addEventListener('change', () => {
  focusOn = $('c-focus').checked;
  saveFocus();
  syncFocus();
});

// ---------------------------------------------------------------- publish

async function publish(body, done) {
  try {
    await api('POST', '/api/publish', body);
    toast('ok', done);
  } catch (err) {
    toast('error', `Could not publish to ${body.topic || 'the topic'}: ${err.message}`);
  }
}

$('publish').addEventListener('submit', (ev) => {
  ev.preventDefault();
  const topic = $('p-topic').value.trim();
  publish(
    { topic, payload: $('p-payload').value, qos: Number($('p-qos').value), retain: $('p-retain').checked },
    `Published to ${topic}.`,
  );
});

confirmClick($('p-clear'), () => {
  const topic = $('p-topic').value.trim();
  if (!topic) {
    toast('error', 'Could not clear the retained message: enter a topic first.');
    return;
  }
  publish({ topic, payload: '', qos: 0, retain: true }, `Cleared the retained message on ${topic}.`);
});

// ---------------------------------------------------------------- connection + profiles

const LAST_PROFILE = 'mqttex.lastProfile';

function remember(id) {
  try {
    localStorage.setItem(LAST_PROFILE, id);
  } catch {
    // private window or blocked storage: only a convenience
  }
}

async function loadProfiles(selectId) {
  try {
    profiles = await api('GET', '/api/profiles');
  } catch (err) {
    toast('error', `Could not load the saved connections: ${err.message}`);
    return;
  }
  let wanted = selectId ?? $('profile').value;
  if (!wanted) {
    try {
      wanted = localStorage.getItem(LAST_PROFILE);
    } catch {
      wanted = null;
    }
  }
  $('profile').replaceChildren(
    ...profiles.map((p) => Object.assign(document.createElement('option'), { value: p.id, textContent: `${p.name}  (${p.protocol}://${p.host}:${p.port})` })),
  );
  if (profiles.some((p) => p.id === wanted)) $('profile').value = wanted;
  renderTop();
  scheduleRender();
}

$('connect').addEventListener('click', async () => {
  if (status.state !== 'disconnected') {
    try {
      setStatus(await api('POST', '/api/disconnect'));
    } catch (err) {
      toast('error', `Could not disconnect: ${err.message}`);
    }
    renderTop();
    return;
  }
  const id = $('profile').value;
  const name = profiles.find((p) => p.id === id)?.name ?? 'the broker';
  try {
    setStatus(await api('POST', '/api/connect', { id }));
    remember(id);
  } catch (err) {
    toast('error', `Could not start connecting to ${name}: ${err.message}`);
  }
  renderTop();
});

$('clear').addEventListener('click', async () => {
  try {
    await api('POST', '/api/clear');
  } catch (err) {
    toast('error', `Could not clear the topic tree: ${err.message}`);
  }
});

const dialog = $('profile-dialog');
let editingId = '';

function syncProtocolFields() {
  const proto = $('pf-protocol').value;
  const isWS = proto === 'ws' || proto === 'wss';
  const isTLS = proto === 'mqtts' || proto === 'wss';
  for (const el of dialog.querySelectorAll('[data-ws]')) el.hidden = !isWS;
  for (const el of dialog.querySelectorAll('[data-tls]')) el.hidden = !isTLS;
  const port = $('pf-port').value;
  if (proto === 'mqtts' && port === '1883') $('pf-port').value = '8883';
  if (proto === 'mqtt' && port === '8883') $('pf-port').value = '1883';
}

function checkSubscriptions() {
  const star = $('pf-subs').value.split('\n').some((l) => l.includes('*'));
  $('pf-subs-warning').hidden = !star;
  $('pf-subs-warning').textContent = star
    ? '"*" is not an MQTT wildcard and only matches a topic literally named "*". Use # or + instead.'
    : '';
}

let disarmDelete = () => {};

function openDialog(p) {
  editingId = p?.id ?? '';
  $('pf-title').textContent = p ? `Edit ${p.name}` : 'New connection';
  $('pf-name').value = p?.name ?? '';
  $('pf-protocol').value = p?.protocol ?? 'mqtt';
  $('pf-host').value = p?.host ?? '';
  $('pf-port').value = p?.port ?? 1883;
  $('pf-path').value = p?.path ?? '';
  $('pf-username').value = p?.username ?? '';
  $('pf-password').value = '';
  $('pf-password').placeholder = p?.hasPassword ? 'unchanged' : '';
  $('pf-clientid').value = p?.clientId ?? '';
  $('pf-insecure').checked = p?.tlsInsecure ?? false;
  $('pf-subs').value = (p?.subscriptions ?? ['#']).join('\n');
  $('pf-delete').hidden = !p;
  $('pf-error').hidden = true;
  disarmDelete();
  syncProtocolFields();
  checkSubscriptions();
  dialog.showModal();
}

$('pf-protocol').addEventListener('change', syncProtocolFields);
$('pf-subs').addEventListener('input', checkSubscriptions);
$('new').addEventListener('click', () => openDialog(null));
$('edit').addEventListener('click', () => openDialog(profiles.find((p) => p.id === $('profile').value) ?? null));
$('pf-cancel').addEventListener('click', () => dialog.close());

$('profile-form').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const username = $('pf-username').value.trim();
  const body = {
    id: editingId,
    name: $('pf-name').value,
    protocol: $('pf-protocol').value,
    host: $('pf-host').value,
    port: Number($('pf-port').value),
    path: $('pf-path').value.trim(),
    username,
    password: $('pf-password').value,
    clearPassword: username === '',
    clientId: $('pf-clientid').value.trim(),
    tlsInsecure: $('pf-insecure').checked,
    subscriptions: $('pf-subs').value.split('\n'),
  };
  try {
    const saved = await api('POST', '/api/profiles', body);
    dialog.close();
    await loadProfiles(saved.id);
    const active = status.profileId === saved.id && status.state !== 'disconnected';
    toast('ok', active ? `Saved ${saved.name}. Reconnect to apply the changes.` : `Saved ${saved.name}.`);
  } catch (err) {
    $('pf-error').hidden = false;
    $('pf-error').textContent = `Could not save the connection: ${err.message}`;
  }
});

disarmDelete = confirmClick($('pf-delete'), async () => {
  try {
    await api('DELETE', `/api/profiles/${encodeURIComponent(editingId)}`);
    dialog.close();
    await loadProfiles();
    toast('ok', 'Deleted the connection.');
  } catch (err) {
    $('pf-error').hidden = false;
    $('pf-error').textContent = `Could not delete the connection: ${err.message}`;
  }
});

// ---------------------------------------------------------------- start

loadProfiles();
connectWS();
renderTop();
scheduleRender();

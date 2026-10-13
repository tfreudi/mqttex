// Field table for comparing several topics side by side. No DOM in here, so it
// runs under `node --test` as well.

const PAYLOAD = '(payload)';
const TRUNCATED = '(truncated)';
// Longest value shown in a cell. A longer one is cut and ends with a hash of
// the whole text, so values that only differ further in still mark their row.
const CELL_MAX = 200;
// Value cells the table holds at most. Every array element is a row, so a
// few large arrays across many columns would otherwise freeze the page.
const MAX_CELLS = 10_000;

// Turns a payload into field path -> text. Objects become a.b.c, arrays a[0];
// a key that would be ambiguous in a path is quoted instead: a["b.c"], [""].
// Strings keep their JSON quotes so that "1" and 1 or "null" and null differ,
// and numbers keep the digits that were sent where the engine allows it (see
// parse). Anything that is not a JSON object or array is one field,
// "(payload)".
export function flatten(text, trunc = false) {
  const out = new Map();
  let root;
  // A cut-off payload never parses, and a half document would be misleading.
  if (!trunc && /^\s*[[{]/.test(text)) {
    try {
      root = parse(text);
    } catch {
      root = undefined;
    }
  }
  if (root === undefined || root === null || typeof root !== 'object') {
    out.set(PAYLOAD, cell(text));
  } else {
    walk(root, '', out);
  }
  if (trunc) out.set(TRUNCATED, 'yes');
  return out;
}

// A number as written in the payload.
class Num {
  constructor(source) {
    this.source = source;
  }
}

// JSON.parse alone rounds integers beyond 2^53 and turns 1.0 into 1, so
// numbers are taken from the source text where the engine passes it to the
// reviver. Older engines do not; there numbers are shown as JSON.parse reads
// them.
function parse(text) {
  return JSON.parse(text, (_key, value, ctx) => (typeof value === 'number' && ctx?.source !== undefined ? new Num(ctx.source) : value));
}

function walk(v, path, out) {
  if (v instanceof Num) {
    out.set(path, cell(v.source));
    return;
  }
  if (v !== null && typeof v === 'object') {
    const entries = Array.isArray(v) ? v.map((x, i) => [`${path}[${i}]`, x]) : Object.keys(v).map((k) => [path + step(k, path), v[k]]);
    if (entries.length === 0) {
      out.set(path || PAYLOAD, Array.isArray(v) ? '[]' : '{}');
      return;
    }
    for (const [p, x] of entries) walk(x, p, out);
    return;
  }
  out.set(path, cell(typeof v === 'string' ? JSON.stringify(v) : String(v)));
}

// The path step for an object key: .key, or ["key"] when the key is empty,
// contains a character of the path syntax or, at the top, starts like the
// (payload) and (truncated) rows, so no two fields share a path.
function step(key, path) {
  if (key !== '' && !/[.[\]"]/.test(key) && (path !== '' || key[0] !== '(')) return path ? `.${key}` : key;
  return `[${JSON.stringify(key)}]`;
}

// Rows the table shows for the given number of columns.
export function rowLimit(columns) {
  return Math.floor(MAX_CELLS / Math.max(1, columns));
}

function cell(text) {
  if (text.length <= CELL_MAX) return text;
  let end = CELL_MAX;
  if (/[\uD800-\uDBFF]/.test(text[end - 1])) end--; // keep surrogate pairs whole
  return `${text.slice(0, end)} ... (${text.length} characters, #${digest(text)})`;
}

// Short hash of a text (32-bit FNV-1a), to tell long values apart without
// showing them.
export function digest(text) {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, '0');
}

// Short column titles: the topic segments all names share at the start and at
// the end are dropped, so /topic/SN1/V0/post/json becomes SN1.
export function columnLabels(names) {
  if (names.length < 2) return [...names];
  const parts = names.map((n) => n.split('/'));
  const min = Math.min(...parts.map((p) => p.length));
  let pre = 0;
  while (pre < min && parts.every((p) => p[pre] === parts[0][pre])) pre++;
  let suf = 0;
  while (pre + suf < min && parts.every((p) => p[p.length - 1 - suf] === parts[0][parts[0].length - 1 - suf])) suf++;
  return parts.map((p, i) => {
    const label = p.slice(pre, p.length - suf).join('/');
    return label === '' ? names[i] : label;
  });
}

// columns: one Map per topic (from flatten), or null while a topic has no
// message yet. Rows keep the order in which their path first shows up.
export function buildRows(columns) {
  const order = [];
  const seen = new Set();
  for (const col of columns) {
    if (!col) continue;
    for (const path of col.keys()) {
      if (!seen.has(path)) {
        seen.add(path);
        order.push(path);
      }
    }
  }
  const present = columns.filter(Boolean).length;
  return order.map((path) => {
    const values = columns.map((col) => (col ? col.get(path) : undefined));
    // Topics without any message are left out: they would mark every row.
    let differs = false;
    if (present > 1) {
      let first;
      let have = false;
      for (let i = 0; i < columns.length; i++) {
        if (!columns[i]) continue;
        if (!have) {
          first = values[i];
          have = true;
        } else if (values[i] !== first) {
          differs = true;
          break;
        }
      }
    }
    return { path, values, differs };
  });
}

// The topics another device most likely uses, best guess first: one segment
// of a compared topic is replaced by value. A value containing "/" is taken
// as a complete topic. Every guess is a whole topic, so the caller can take
// the first one that exists on the broker.
//
// Segments in which compared topics of the same depth differ come first,
// since that is where the device sits (/topic/SN1/V0 next to /topic/SN2/V0).
// After them come segments that look like value: ones with a digit if value
// has one, otherwise ones of the same length. Within each group a longer
// shared start with value ranks higher (SN10 replaces SN1, not V0), then the
// same length, then the longer segment.
export function deviceTopics(names, value) {
  const v = value.trim();
  if (v === '') return [];
  if (v.includes('/')) return [v];
  const parts = names.map((n) => n.split('/'));
  const out = new Set();
  const add = (p, at) => {
    for (const i of rank(p, at, v)) out.add([...p.slice(0, i), v, ...p.slice(i + 1)].join('/'));
  };
  for (const p of parts) add(p, differingSegments(parts, p));
  const digit = /\d/.test(v);
  for (const p of parts) add(p, segments(p, (s) => s !== '' && (digit ? /\d/.test(s) : s.length === v.length)));
  return [...out];
}

// Indexes of the segments of p in which compared topics of the same depth
// differ, see deviceTopics.
function differingSegments(parts, p) {
  const peers = parts.filter((q) => q !== p && q.length === p.length);
  return segments(p, (s, i) => peers.some((q) => q[i] !== s));
}

function segments(p, keep) {
  const at = [];
  p.forEach((s, i) => {
    if (keep(s, i)) at.push(i);
  });
  return at;
}

// Broker filters that cover the whole device of each compared topic, e.g.
// /topic/SN1/V0/post/json -> /topic/SN1/#, so its other topics (response, ...)
// arrive as well. The device segment is found as in deviceTopics, without a
// typed value: where compared topics of the same depth differ, then segments
// with a digit, ranked the same way. A segment with a digit is preferred, so
// post next to resp of one device does not split it in two. A topic whose
// device cannot be told is subscribed to as it is.
export function deviceFilters(names) {
  const unique = [...new Set(names)];
  const parts = unique.map((n) => n.split('/'));
  const filters = parts.map((p, i) => {
    const at = [...rank(p, differingSegments(parts, p), ''), ...rank(p, segments(p, (s) => /\d/.test(s)), '')].filter((j) => p[j] !== '');
    const device = at.find((j) => /\d/.test(p[j])) ?? at[0];
    return device === undefined ? unique[i] : [...p.slice(0, device + 1), '#'].join('/');
  });
  return withoutCovered([...new Set(filters)]);
}

// Overlapping filters would deliver a message twice and count one device twice:
// a/# already covers a, a/x/# and a/x/y.
function withoutCovered(filters) {
  return filters.filter((f) => !filters.some((g) => {
    if (g === f || !g.endsWith('/#')) return false;
    const base = g.slice(0, -2);
    return f === base || f.startsWith(`${base}/`);
  }));
}

// The topic Add device takes from the guesses of deviceTopics. lookups map a
// guess to the name of an existing topic or undefined, best first (exact
// name, then ignoring case). Of the guesses the first lookup finds, one that
// is not compared yet wins; without any, the first guess is returned and the
// caller reports it as unknown.
export function pickTopic(guesses, lookups, compared) {
  for (const lookup of lookups) {
    const known = guesses.map(lookup).filter((name) => name !== undefined);
    if (known.length > 0) return known.find((name) => !compared.includes(name)) ?? known[0];
  }
  return guesses[0];
}

function rank(parts, at, value) {
  const v = value.toLowerCase();
  const score = (i) => {
    const s = parts[i].toLowerCase();
    let shared = 0;
    while (shared < s.length && s[shared] === v[shared]) shared++;
    return [shared, s.length === v.length ? 1 : 0, s.length];
  };
  const scored = at.map((i) => ({ i, s: score(i) }));
  scored.sort((a, b) => b.s[0] - a.s[0] || b.s[1] - a.s[1] || b.s[2] - a.s[2]);
  return scored.map((x) => x.i);
}

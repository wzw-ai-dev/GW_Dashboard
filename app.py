import hmac, json, os, re, threading, time
from calendar import timegm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

# Login: set GW_PASSWORD (and optionally GW_USER) on the host. If unset, no login.
GW_USER = os.environ.get("GW_USER", "gw")
GW_PASSWORD = os.environ.get("GW_PASSWORD")


@app.before_request
def require_login():
    if not GW_PASSWORD:
        return None
    a = request.authorization
    if (a and hmac.compare_digest(a.username or "", GW_USER)
            and hmac.compare_digest(a.password or "", GW_PASSWORD)):
        return None
    return Response("Login required", 401, {"WWW-Authenticate": 'Basic realm="GW dashboard"'})


# ---- Settings you can edit ---------------------------------------------------
NEWSDATA_KEY = os.environ.get("NEWSDATA_API_KEY")  # set on Render, never in this file
CACHE_SECONDS = 3600               # reuse saved news for up to 1 hour
MIN_REFRESH_GAP = 60               # a new fetch can't start more often than this
MAX_NEWSDATA_CALLS_PER_DAY = 150   # free plan allows about 200; stay under it
TOP_N = 10
TIMEOUT = 12
UA = "Mozilla/5.0 (compatible; GWDashboard/1.0)"

# Each search is one NewsData.io request (keep each under ~100 characters).
QUERIES = [
    '"guided missile" OR "missile seeker" OR "precision strike missile"',
    '"hypersonic missile" OR "cruise missile" OR "low-cost missile"',
    '"missile interceptor" OR "air defense missile" OR "solid rocket motor"',
]
# Fallback source (no key): one combined search.
GDELT_QUERY = ('("guided missile" OR "missile seeker" OR "hypersonic missile" '
               'OR "missile interceptor" OR "cruise missile") sourcelang:english')
# A story must mention at least one of these, to cut off-topic results.
RELEVANT = ["missile", "munition", "interceptor", "hypersonic", "rocket motor",
            "seeker", "air defense", "air defence"]
THEMES = {
    "Seeker": ["seeker", "infrared", "radar", "guidance", "targeting", "sensor"],
    "Propulsion": ["rocket motor", "propulsion", "scramjet", "solid rocket", "engine"],
    "Production": ["production", "contract", "multiyear", "stockpile", "munitions",
                   "award", "billion", "order", "deal", "factory", "supply"],
    "Low-cost": ["low-cost", "low cost", "affordable", "cheap", "drone"],
    "Interceptors": ["interceptor", "air defense", "air defence", "missile defense",
                     "patriot", "iron dome", "shield"],
    "Hypersonic": ["hypersonic", "glide vehicle", "mach"],
    "Datalink": ["data link", "datalink", "networked", "communications"],
    "Testing": ["test", "fires", "fired", "launch", "trial", "demonstrat", "flight"],
}
# ------------------------------------------------------------------------------

cache = {"items": [], "themes": {}, "updated": 0, "tried": 0, "error": None,
         "source": "none yet", "new": 0, "debug": []}
calls = {"day": "", "n": 0}
lock = threading.Lock()


def clean_img(u):
    """Keep only http(s) picture links; upgrade http to https so phones don't block them."""
    u = (u or "").strip()
    if u.startswith("http://"):
        u = "https://" + u[7:]
    return u if u.startswith("https://") else ""


def http_json(url):
    with urlopen(Request(url, headers={"User-Agent": UA}), timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def fetch_newsdata(q):
    data = http_json("https://newsdata.io/api/1/latest?" +
                     urlencode({"apikey": NEWSDATA_KEY, "q": q, "language": "en"}))
    if data.get("status") != "success":
        raise RuntimeError("NewsData.io returned an error")
    out = []
    for r in data.get("results") or []:
        try:
            ts = timegm(time.strptime(r.get("pubDate") or "", "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            ts = time.time()
        out.append({"title": r.get("title") or "", "link": r.get("link") or "",
                    "source": r.get("source_name") or r.get("source_id") or "",
                    "desc": r.get("description") or "", "ts": ts,
                    "image": clean_img(r.get("image_url"))})
    return out


def fetch_gdelt():
    data = http_json("https://api.gdeltproject.org/api/v2/doc/doc?" + urlencode({
        "query": GDELT_QUERY, "mode": "artlist", "maxrecords": "50",
        "format": "json", "sort": "datedesc", "timespan": "7d"}))
    out = []
    for a in data.get("articles") or []:
        try:
            ts = timegm(time.strptime(a.get("seendate") or "", "%Y%m%dT%H%M%SZ"))
        except ValueError:
            ts = time.time()
        out.append({"title": a.get("title") or "", "link": a.get("url") or "",
                    "source": a.get("domain") or "", "desc": "", "ts": ts,
                    "image": clean_img(a.get("socialimage"))})
    return out


def attempt(label, fn, *args):
    try:
        res = fn(*args)
        return res, {"source": label, "stories": len(res), "status": "ok"}
    except Exception as ex:  # never include the URL/key in what we record
        return [], {"source": label, "stories": 0, "status": str(ex)[:80]}


def fetch_news():
    now, steps, articles, used = time.time(), [], [], None

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if calls["day"] != today:
        calls.update(day=today, n=0)
    if not NEWSDATA_KEY:
        steps.append({"source": "NewsData.io", "stories": 0, "status": "skipped: no API key set"})
    elif calls["n"] + len(QUERIES) > MAX_NEWSDATA_CALLS_PER_DAY:
        steps.append({"source": "NewsData.io", "stories": 0, "status": "skipped: daily limit reached"})
    else:
        calls["n"] += len(QUERIES)
        with ThreadPoolExecutor(max_workers=len(QUERIES)) as pool:
            for res, step in pool.map(lambda q: attempt("NewsData.io", fetch_newsdata, q), QUERIES):
                articles += res
                steps.append(step)
        if articles:
            used = "NewsData.io"
    if not articles:
        articles, step = attempt("GDELT", fetch_gdelt)
        steps.append(step)
        if articles:
            used = "GDELT"

    seen = {}
    for a in articles:
        text = (a["title"] + " " + a["desc"]).lower()
        if not a["title"] or not a["link"] or not any(k in text for k in RELEVANT):
            continue
        key = re.sub(r"[^a-z0-9]", "", a["title"].lower())[:60]
        if key in seen:
            seen[key]["hits"] += 1
            if not seen[key].get("image") and a.get("image"):
                seen[key]["image"] = a["image"]
            continue
        desc = a["desc"].strip()
        seen[key] = {**a, "desc": desc[:220] + ("…" if len(desc) > 220 else ""), "hits": 1,
                     "tags": [t for t, kw in THEMES.items() if any(k in text for k in kw)] or ["General"]}
    ranked = sorted(seen.values(),
                    key=lambda i: i["hits"] * 24 - (now - i["ts"]) / 3600, reverse=True)[:TOP_N]

    cache["tried"], cache["debug"] = now, steps
    if ranked:  # only replace saved news if we actually got some
        old = {i["link"] for i in cache["items"]}
        cache.update(items=ranked, updated=now, error=None, source=used,
                     themes={t: sum(t in i["tags"] for i in ranked) for t in [*THEMES, "General"]},
                     new=sum(i["link"] not in old for i in ranked) if old else 0)
    else:
        cache["error"] = "No news could be fetched right now. Try again in a few minutes."


@app.route("/api/news")
def news():
    now, did = time.time(), False
    due = (now - cache["tried"] > MIN_REFRESH_GAP and
           (now - cache["updated"] > CACHE_SECONDS or request.args.get("refresh")))
    if due and lock.acquire(blocking=False):
        try:
            fetch_news()
            did = True
        finally:
            lock.release()
    return jsonify({**{k: v for k, v in cache.items() if k != "debug"}, "refreshed": did})


@app.route("/api/debug")
def debug():
    return jsonify({"last_tried": cache["tried"], "error": cache["error"], "source": cache["source"],
                    "newsdata_key_set": bool(NEWSDATA_KEY), "newsdata_calls_today": calls["n"],
                    "steps": cache["debug"]})


PAGE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Top 10 News</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400;1,600&display=swap">
<style>
/* Layout: header and topic chips on the page gutter, then one full-width swipe row of black-and-red picture cards; rank numeral top-left and topic top-right, both inside the picture. */
:root{
  color-scheme:light;
  --bg:#dcebfa;
  --ink:#26324f;
  --muted:#52638a;
  --sky:#a9d1f5;
  --peri:#b3bff2;
  --aqua:#bfe8e3;
  --butter:#f7e7b0;
  --peach:#f4b8a4;
  --blue:#6f8be0;
  --link:#2f4fa2;
  --ink-rgb:38 50 79;
  --blue-rgb:111 139 224;
  --peri-rgb:150 160 240;
  --aqua-rgb:176 230 226;
  --nf-black:#141414;
  --nf-card:#1b1b1b;
  --nf-red:#e50914;
  --nf-white:#ffffff;
  --nf-grey:#b3b3b3;
  --c1:color-mix(in srgb,var(--nf-red) 70%,#000);
  --c2:color-mix(in srgb,var(--nf-red) 60%,#000);
  --c3:color-mix(in srgb,var(--nf-red) 50%,#000);
  --c4:color-mix(in srgb,var(--nf-red) 40%,#000);
  --c5:color-mix(in srgb,var(--nf-red) 30%,#000);
  --c6:color-mix(in srgb,var(--nf-red) 22%,#000);
  --x3:color-mix(in srgb,var(--blue) 64%,var(--ink));
  --x4:color-mix(in srgb,var(--blue) 56%,var(--ink));
  --x5:color-mix(in srgb,var(--blue) 48%,var(--ink));
  --x6:color-mix(in srgb,var(--blue) 40%,var(--ink));
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
}
[hidden]{display:none!important}
html,body{height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);overflow-x:hidden}
button{font:inherit;color:inherit}

.stage{position:relative;min-height:100%;box-sizing:border-box;overflow:hidden;padding-inline:16px;padding-top:clamp(20px,4vh,40px);padding-bottom:48px}
.stage::before{content:"";position:absolute;inset:0;pointer-events:none;background:
  radial-gradient(60% 50% at 12% 6%,rgb(var(--peri-rgb) / .45),transparent 70%),
  radial-gradient(55% 45% at 94% 96%,rgb(var(--aqua-rgb) / .80),transparent 70%)}
.floor{position:absolute;left:-50%;right:-50%;top:55%;height:100%;pointer-events:none;
  background-image:
    linear-gradient(rgb(var(--blue-rgb) / .30) 1.5px,transparent 1.5px),
    linear-gradient(90deg,rgb(var(--blue-rgb) / .30) 1.5px,transparent 1.5px);
  background-size:72px 72px;
  transform:perspective(420px) rotateX(62deg);transform-origin:50% 0;
  -webkit-mask-image:linear-gradient(to bottom,transparent 0%,#000 40%);
  mask-image:linear-gradient(to bottom,transparent 0%,#000 40%);
  animation:drift 9s linear infinite}
@keyframes drift{to{background-position:0 72px}}

.page{position:relative;z-index:1;max-width:76rem;margin:0 auto;min-width:0}
.top{display:flex;flex-wrap:wrap;align-items:flex-end;justify-content:space-between;gap:14px}
.back{display:inline-block;color:var(--link);font-family:var(--display);font-weight:500;font-size:.95rem;letter-spacing:.06em;text-decoration:none;margin-bottom:6px}
.back:hover{text-decoration:underline}
h1{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.9rem,8vw,3.4rem);letter-spacing:.1em;text-transform:uppercase;line-height:1.1;color:var(--ink);text-shadow:3px 3px 0 var(--blue)}
.btn{--st:0 1px 0 var(--x3),0 2px 0 var(--x4),0 3px 0 var(--x5),0 4px 0 var(--x6);
  cursor:pointer;padding:9px 20px;border-radius:999px;border:2px solid var(--ink);background:var(--aqua);
  font-family:var(--display);font-weight:700;font-size:.85rem;letter-spacing:.16em;text-transform:uppercase;
  box-shadow:inset 0 2px 0 rgb(255 255 255 / .6),var(--st);transition:transform .12s,box-shadow .12s}
.btn:active{transform:translateY(3px);box-shadow:inset 0 2px 0 rgb(255 255 255 / .6),0 1px 0 var(--x3)}
.btn:focus-visible,.chip:focus-visible,.arrow:focus-visible,.bubble:focus-visible{outline:3px solid var(--link);outline-offset:3px}
.st{margin:12px 0 0;color:var(--muted);font-size:1rem;line-height:1.45}

.chips{display:flex;flex-wrap:wrap;gap:8px;margin:18px 0 0}
.chip{cursor:pointer;display:inline-flex;align-items:baseline;gap:6px;padding:6px 14px;border-radius:999px;border:2px solid rgb(var(--ink-rgb) / .45);background:rgb(255 255 255 / .55);
  font-family:var(--display);font-weight:500;font-size:.88rem;letter-spacing:.05em}
.chip b{font-weight:700}
.chip .n{color:var(--muted);font-size:.8rem}
.chip[aria-pressed="true"]{background:var(--aqua);border-color:var(--ink)}

/* the swipe row */
.rail-wrap{position:relative;margin:6px -16px 0;--cw:clamp(200px,56vw,280px)}
.rail{list-style:none;margin:0;padding:22px 16px 34px;display:flex;gap:clamp(14px,3vw,26px);overflow-x:auto;scroll-snap-type:x proximity;scroll-padding-inline:16px;scrollbar-width:none;-webkit-overflow-scrolling:touch}
.rail::-webkit-scrollbar{display:none}
.item{flex:none;display:flex;scroll-snap-align:start}

.bubble{--stack:0 1px 0 var(--c1),0 2px 0 var(--c2),0 3px 0 var(--c3),0 4px 0 var(--c4),0 5px 0 var(--c5),0 6px 0 var(--c6);
  --glow:0 18px 22px -10px rgb(0 0 0 / .55);
  position:relative;width:var(--cw);display:flex;flex-direction:column;box-sizing:border-box;overflow:hidden;
  border:2px solid var(--nf-black);border-radius:22px;background:var(--nf-card);
  color:var(--nf-white);text-decoration:none;box-shadow:var(--stack),var(--glow);transition:transform .16s ease-out,box-shadow .16s ease-out;-webkit-tap-highlight-color:transparent}
@media (hover:hover){.bubble:hover{transform:translateY(-5px);--glow:0 26px 30px -10px rgb(0 0 0 / .65)}}
.bubble:active{transform:translateY(4px);--stack:0 1px 0 var(--c1),0 2px 0 var(--c2);--glow:0 6px 10px -6px rgb(0 0 0 / .5)}
.pic{position:relative;aspect-ratio:1 / 1.12;background:linear-gradient(160deg,#2b2b2b,var(--nf-black))}
.pic img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}
.pic::after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(to bottom,rgb(0 0 0 / .55),transparent 38%,transparent 72%,rgb(0 0 0 / .45))}
.pic .ph{position:absolute;inset:0;display:grid;place-items:center}
.pic .ph svg{width:38%;height:auto}
.rank{position:absolute;left:12px;top:2px;z-index:2;user-select:none;
  font-family:var(--display);font-weight:700;font-size:calc(var(--cw) * .34);line-height:1.05;letter-spacing:-.05em;
  color:var(--nf-red);-webkit-text-stroke:2px var(--nf-white);paint-order:stroke fill;text-shadow:0 3px 0 rgb(0 0 0 / .65)}
.tagpill{position:absolute;right:10px;top:12px;z-index:2;max-width:55%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:4px 9px;border-radius:999px;background:var(--nf-red);color:var(--nf-white);
  font-family:var(--display);font-weight:700;font-size:.6rem;letter-spacing:.1em;text-transform:uppercase;box-shadow:0 2px 0 rgb(0 0 0 / .45)}
.txt{display:grid;gap:6px;padding:12px 14px 15px}
.ttl{margin:0;font-weight:600;font-size:1.05rem;line-height:1.25;min-height:3.75em;color:var(--nf-white);display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.meta{margin:0;color:var(--nf-grey);font-size:.85rem;line-height:1.3}

.arrow{display:none}
@media (hover:hover){
  .arrow{display:grid;place-items:center;position:absolute;top:46%;z-index:3;width:46px;height:46px;border-radius:50%;cursor:pointer;
    border:2px solid var(--ink);background:var(--aqua);box-shadow:0 1px 0 var(--x3),0 2px 0 var(--x4),0 3px 0 var(--x5),0 4px 0 var(--x6)}
  .arrow.prev{left:18px}.arrow.next{right:18px}
  .arrow svg{width:18px;height:18px;fill:none;stroke:var(--ink);stroke-width:2.8;stroke-linecap:round;stroke-linejoin:round}
}
.empty{margin:30px 0;color:var(--muted);font-size:1.05rem}
.note{margin:0;color:var(--muted);font-size:.9rem}

@media (prefers-reduced-motion:reduce){
  .floor{animation:none}
  .bubble,.btn{transition:none}
  .rail{scroll-behavior:auto}
}
</style>
</head><body><div class="stage">
  <div class="floor" aria-hidden="true"></div>
  <main class="page">
    <header class="top">
      <div>
        <a class="back" href="/">&larr; Home</a>
        <h1>Top 10 News</h1>
      </div>
      <button class="btn" id="rf" type="button">Refresh</button>
    </header>
    <p class="st" id="st" aria-live="polite"></p>
    <div class="chips" id="chips" role="group" aria-label="Filter by topic"></div>

    <div class="rail-wrap">
      <button class="arrow prev" id="prev" type="button" aria-label="Scroll left"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10.5 2.5 5 8l5.5 5.5"/></svg></button>
      <ol class="rail" id="rail"></ol>
      <button class="arrow next" id="next" type="button" aria-label="Scroll right"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M5.5 2.5 11 8l-5.5 5.5"/></svg></button>
    </div>
    <p class="empty" id="empty" hidden>No stories match this topic right now.</p>
  </main>
</div>

<script>
var $ = function (s) { return document.querySelector(s); };
var items = [], filter = 'All';

var PH = '<svg viewBox="0 0 64 64" aria-hidden="true"><rect x="7" y="40" width="10" height="18" rx="2" fill="#b3bff2" stroke="#141414" stroke-width="2.2"/><rect x="22" y="30" width="10" height="28" rx="2" fill="#bfe8e3" stroke="#141414" stroke-width="2.2"/><rect x="37" y="20" width="10" height="38" rx="2" fill="#f7e7b0" stroke="#141414" stroke-width="2.2"/><polyline points="8,30 23,21 38,25 55,8" fill="none" stroke="#ffffff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/><circle cx="55" cy="8" r="5" fill="#e50914" stroke="#141414" stroke-width="2.2"/></svg>';

function el(tag, text, cls) {
  var e = document.createElement(tag);
  if (text != null) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}
function safeUrl(u) { return /^https?:\/\//i.test(u || '') ? u : ''; }
function ago(ts) {
  var h = (Date.now() / 1000 - ts) / 3600;
  return h < 1 ? 'just now' : h < 24 ? Math.round(h) + 'h ago' : Math.round(h / 24) + 'd ago';
}
function topics() {
  var c = {};
  items.forEach(function (i) { i.tags.forEach(function (t) { c[t] = (c[t] || 0) + 1; }); });
  return Object.keys(c).sort(function (a, b) { return c[b] - c[a]; }).map(function (t) { return [t, c[t]]; });
}

function renderChips() {
  var box = $('#chips');
  box.replaceChildren();
  [['All', items.length]].concat(topics()).forEach(function (p) {
    var b = el('button', null, 'chip');
    b.type = 'button';
    b.setAttribute('aria-pressed', String(p[0] === filter));
    b.append(el('b', p[0]), el('span', String(p[1]), 'n'));
    b.onclick = function () { filter = p[0]; render(); };
    box.append(b);
  });
}

function card(d, rank) {
  var li = el('li', null, 'item');
  var a = el('a', null, 'bubble');
  var link = safeUrl(d.link);
  if (link) { a.href = link; a.target = '_blank'; a.rel = 'noopener'; }
  a.setAttribute('aria-label', 'Number ' + rank + ': ' + d.title);

  var pic = el('div', null, 'pic');
  function overlays() {
    var n = el('span', String(rank), 'rank');
    n.setAttribute('aria-hidden', 'true');
    pic.append(n, el('span', d.tags[0], 'tagpill'));
  }
  function fallback() { pic.replaceChildren(); var ph = el('div', null, 'ph'); ph.innerHTML = PH; pic.append(ph); overlays(); }
  var img = safeUrl(d.image);
  if (img) {
    var im = document.createElement('img');
    im.alt = ''; im.loading = 'lazy'; im.referrerPolicy = 'no-referrer';
    im.onerror = fallback;
    im.src = img;
    pic.append(im);
    overlays();
  } else { fallback(); }

  var txt = el('div', null, 'txt');
  txt.append(el('h2', d.title, 'ttl'), el('p', d.source + ' · ' + ago(d.ts), 'meta'));
  a.append(pic, txt);
  li.append(a);
  return li;
}

function render() {
  renderChips();
  var rail = $('#rail');
  rail.replaceChildren();
  var shown = 0;
  items.forEach(function (d, i) {
    if (filter !== 'All' && d.tags.indexOf(filter) < 0) return;
    rail.append(card(d, i + 1));
    shown++;
  });
  $('#empty').hidden = shown > 0;
  rail.scrollLeft = 0;
}

function scrollRail(dir) {
  var r = $('#rail');
  r.scrollBy({ left: dir * r.clientWidth * 0.8, behavior: 'smooth' });
}
$('#prev').onclick = function () { scrollRail(-1); };
$('#next').onclick = function () { scrollRail(1); };

function load(force) {
  $('#st').textContent = 'Loading…';
  fetch('/api/news' + (force ? '?refresh=1' : ''))
    .then(function (r) { return r.json(); })
    .then(function (j) {
      items = j.items || [];
      if (!items.length) {
        $('#st').textContent = (j.error || 'No stories yet. Try Refresh in a minute.') + ' (details: /api/debug)';
        render();
        $('#empty').hidden = true;
        return;
      }
      var m = force ? (j.refreshed ? 'Refreshed: ' + j.new + ' new in top 10 · ' : 'Checked moments ago, wait a minute · ') : '';
      if (j.error) m = 'Latest fetch failed, showing saved news · ' + m;
      $('#st').textContent = m + 'Last fetched ' + new Date(j.updated * 1000).toLocaleString() + ' · source: ' + j.source;
      render();
    })
    .catch(function () { $('#st').textContent = 'Could not load news.'; });
}
$('#rf').onclick = function () { load(true); };
load();
</script>
</body></html>"""


HOME = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>GW Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400;1,600&display=swap">
<style>
/* Layout: one column as wide as the title; header at the top, dictionary entry and a 2 x 3 button grid share its left and right edges. */
:root{
  color-scheme:light;
  --bg:#dcebfa;
  --ink:#26324f;
  --muted:#52638a;
  --sky:#a9d1f5;
  --peri:#b3bff2;
  --aqua:#bfe8e3;
  --butter:#f7e7b0;
  --peach:#f4b8a4;
  --blue:#6f8be0;
  --link:#2f4fa2;
  --ink-rgb:38 50 79;
  --blue-rgb:111 139 224;
  --peri-rgb:150 160 240;
  --aqua-rgb:176 230 226;
  --x1:color-mix(in srgb,var(--blue) 80%,var(--ink));
  --x2:color-mix(in srgb,var(--blue) 72%,var(--ink));
  --x3:color-mix(in srgb,var(--blue) 64%,var(--ink));
  --x4:color-mix(in srgb,var(--blue) 56%,var(--ink));
  --x5:color-mix(in srgb,var(--blue) 48%,var(--ink));
  --x6:color-mix(in srgb,var(--blue) 40%,var(--ink));
  --x7:color-mix(in srgb,var(--blue) 32%,var(--ink));
  --x8:color-mix(in srgb,var(--blue) 24%,var(--ink));
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
}
[hidden]{display:none!important}
html,body{height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);overflow-x:hidden}

.stage{position:relative;min-height:100%;box-sizing:border-box;overflow:hidden;display:grid;place-items:start center;padding-inline:16px;padding-top:clamp(20px,4vh,40px);padding-bottom:56px}
.stage::before{content:"";position:absolute;inset:0;pointer-events:none;background:
  radial-gradient(60% 50% at 12% 6%,rgb(var(--peri-rgb) / .45),transparent 70%),
  radial-gradient(55% 45% at 94% 96%,rgb(var(--aqua-rgb) / .80),transparent 70%)}

.floor{position:absolute;left:-50%;right:-50%;top:55%;height:100%;pointer-events:none;
  background-image:
    linear-gradient(rgb(var(--blue-rgb) / .30) 1.5px,transparent 1.5px),
    linear-gradient(90deg,rgb(var(--blue-rgb) / .30) 1.5px,transparent 1.5px);
  background-size:72px 72px;
  transform:perspective(420px) rotateX(62deg);transform-origin:50% 0;
  -webkit-mask-image:linear-gradient(to bottom,transparent 0%,#000 40%);
  mask-image:linear-gradient(to bottom,transparent 0%,#000 40%);
  animation:drift 9s linear infinite}
@keyframes drift{to{background-position:0 72px}}

/* the title sets the column width; the entry and the grid take 0 width of their own and then stretch to match it */
.col{position:relative;z-index:1;width:fit-content;max-width:100%;min-width:0;display:grid;gap:clamp(22px,4vh,34px)}
.head{display:grid;gap:16px;min-width:0}
h1{margin:0 -.12em 0 0;text-align:center;font-family:var(--display);font-weight:700;font-size:clamp(1.9rem,9.2vw,4.8rem);letter-spacing:.12em;text-transform:uppercase;line-height:1.1;text-wrap:balance;
  color:var(--ink);text-shadow:4px 4px 0 var(--blue)}

/* dictionary entry: three short lines, smaller than the header */
.entry{width:0;min-width:100%;display:grid;gap:6px;text-align:left}
.entry p{margin:0}
.line1{display:flex;flex-wrap:wrap;align-items:baseline;gap:2px 12px;padding-bottom:8px;border-bottom:1.5px solid rgb(var(--ink-rgb) / .28)}
.hw{font-weight:600;font-size:1.3rem;line-height:1.2}
.pron{color:var(--muted);font-size:.95rem}
.pos{font-style:italic;color:var(--link);font-size:.95rem}
.def{font-size:1rem;line-height:1.5}
.abbr{font-style:italic;color:var(--muted)}
.lab{font-weight:600}
.entry i{font-style:italic;color:var(--link)}

/* 2 columns x 3 rows of mode buttons; the first slot is the live one */
.grid{width:0;min-width:100%;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));grid-auto-rows:1fr;gap:22px 16px;padding-bottom:8px}

.mode{display:block;height:100%;color:inherit;text-decoration:none;outline:none;-webkit-tap-highlight-color:transparent}
.face{--stack:0 1px 0 var(--x1),0 2px 0 var(--x2),0 3px 0 var(--x3),0 4px 0 var(--x4),0 5px 0 var(--x5),0 6px 0 var(--x6),0 7px 0 var(--x7),0 8px 0 var(--x8);
  --glow:0 20px 26px -12px rgb(var(--ink-rgb) / .36);
  position:relative;display:flex;flex-direction:column;align-items:flex-start;gap:8px;box-sizing:border-box;height:100%;padding:clamp(14px,3.6vw,24px);border-radius:22px;
  background:linear-gradient(150deg,#f5faff,var(--sky) 60%,#9db5ee);
  border:2px solid var(--ink);
  box-shadow:inset 0 2px 0 rgb(255 255 255 / .65),var(--stack),var(--glow);
  transition:transform .16s ease-out,box-shadow .16s ease-out}
.sheen{position:absolute;inset:0;border-radius:inherit;pointer-events:none;
  background:radial-gradient(200px circle at 26% 0%,rgb(255 255 255 / .55),transparent 62%)}
.glyph{width:clamp(38px,10vw,54px);height:clamp(38px,10vw,54px);flex:none}
.g-bar{stroke:var(--ink);stroke-width:2.2;stroke-linejoin:round}
.g-bar.b1{fill:var(--peri)}
.g-bar.b2{fill:var(--aqua)}
.g-bar.b3{fill:var(--butter)}
.g-line{fill:none;stroke:var(--ink);stroke-width:3;stroke-linecap:round;stroke-linejoin:round}
.g-dot{fill:var(--peach);stroke:var(--ink);stroke-width:2.2}
.title{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1rem,3.8vw,1.4rem);letter-spacing:.08em;text-transform:uppercase;line-height:1.2;color:var(--ink)}
.sub{margin:0 0 8px;color:var(--muted);font-size:clamp(.82rem,2.6vw,1rem);line-height:1.4}
.go{display:inline-flex;align-items:center;gap:8px;margin-top:auto;padding:6px 14px;border-radius:999px;border:2px solid var(--ink);
  background:var(--aqua);color:var(--ink);font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.16em;text-transform:uppercase}
.go svg{width:12px;height:12px;fill:none;stroke:currentColor;stroke-width:2.6;stroke-linecap:round;stroke-linejoin:round}

@media (hover:hover){
  .mode:hover .face{transform:translateY(-5px);--glow:0 30px 36px -12px rgb(var(--ink-rgb) / .44)}
}
.mode:active .face{transform:translateY(6px);--stack:0 1px 0 var(--x1),0 2px 0 var(--x2);--glow:0 8px 16px -8px rgb(var(--ink-rgb) / .40)}
.mode:focus-visible .face{outline:3px solid var(--link);outline-offset:6px}

.note{margin:0;color:var(--muted);font-size:.95rem;text-align:center}

@media (prefers-reduced-motion:reduce){
  .floor{animation:none}
  .face{transition:none}
}
</style>
</head><body><div class="stage">
  <div class="floor" aria-hidden="true"></div>
  <main class="col">
    <header class="head">
      <h1>GW Dashboard</h1>
      <div class="entry">
        <p class="line1"><span class="hw">guid·ed weap·on</span><span class="pron">/ˈɡaɪ·dɪd ˈwɛp·ən/</span><span class="pos">n.</span></p>
        <p class="def">A munition that steers itself toward a target after launch, using onboard guidance. <span class="abbr">Abbr. GW.</span></p>
        <p class="def"><span class="lab">Parts:</span> <i>seeker</i>, <i>guidance</i>, <i>propulsion</i>, <i>datalink</i>.</p>
      </div>
    </header>

    <div class="grid">
      <a class="mode" href="/trends">
        <span class="face">
          <span class="sheen" aria-hidden="true"></span>
          <svg class="glyph" viewBox="0 0 64 64" aria-hidden="true">
            <rect class="g-bar b1" x="7" y="40" width="10" height="18" rx="2"/>
            <rect class="g-bar b2" x="22" y="30" width="10" height="28" rx="2"/>
            <rect class="g-bar b3" x="37" y="20" width="10" height="38" rx="2"/>
            <polyline class="g-line" points="8,30 23,21 38,25 55,8"/>
            <circle class="g-dot" cx="55" cy="8" r="5"/>
          </svg>
          <h2 class="title">Top 10 News</h2>
          <p class="sub">Live updates on the latest news and trends on GW.</p>
          <span class="go">Open
            <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M5 2.5 10.5 8 5 13.5"/></svg>
          </span>
        </span>
      </a>
    </div>
  </main>
</div>
</body></html>"""


@app.route("/")
def home():
    return HOME


@app.route("/trends")
def trends():
    return PAGE


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

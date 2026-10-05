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
                    "desc": r.get("description") or "", "ts": ts})
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
                    "source": a.get("domain") or "", "desc": "", "ts": ts})
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


PAGE = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Top 10 Latest Trends | GW Dashboard</title>
<style>
:root{--bg:#f4f5f7;--card:#fff;--tx:#14181f;--mut:#5d6675;--bd:#dde1e8;--ac:#1f5fbf;--chip:#e8edf5}
@media(prefers-color-scheme:dark){:root{--bg:#0f1319;--card:#181e27;--tx:#e8ecf2;--mut:#99a3b3;--bd:#2a3340;--ac:#6ea8ff;--chip:#232c39}}
body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.5 system-ui,sans-serif}
.w{max-width:960px;margin:0 auto;padding:20px 16px 40px}h1{font-size:24px;margin:0}
.top{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}
#st{color:var(--mut);font-size:13px;margin:4px 0 16px}
.box{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px;margin-bottom:12px}
.row{display:flex;align-items:center;gap:10px;margin:6px 0;font-size:13px}.n{width:110px}
.bar{flex:1;height:10px;background:var(--chip);border-radius:5px;overflow:hidden}.bar i{display:block;height:100%;background:var(--ac)}
button{font:inherit;font-size:13px;border:1px solid var(--bd);background:var(--chip);color:var(--tx);border-radius:999px;padding:6px 12px;cursor:pointer}
button.on{background:var(--ac);color:#fff;border-color:var(--ac)}.chips{display:flex;flex-wrap:wrap;gap:8px;margin:14px 0}
.it{display:grid;grid-template-columns:30px 1fr;gap:10px}.rk{font-size:20px;font-weight:700;color:var(--ac)}
.it a.t{color:var(--tx);font-weight:600;text-decoration:none}.meta{font-size:12px;color:var(--mut);margin-top:4px}
.tag{background:var(--chip);border-radius:6px;padding:2px 7px;margin-left:6px}
</style></head><body><div class="w">
<div class="top"><div><a href="/" style="color:var(--ac);font-size:13px;text-decoration:none">&larr; Home</a><h1>Top 10 Latest Trends</h1></div><button id="rf">Refresh</button></div>
<div id="st"></div><div class="box"><div id="tr"></div></div>
<div class="chips" id="ch"></div><div id="ls"></div></div>
<script>
const $=s=>document.querySelector(s);let data=[],themes={},f="All";
const el=(t,x,c)=>{const e=document.createElement(t);if(x)e.textContent=x;if(c)e.className=c;return e};
function ago(ts){const h=(Date.now()/1000-ts)/3600;return h<1?"just now":h<24?Math.round(h)+"h ago":Math.round(h/24)+"d ago"}
function render(){
 const m=Math.max(1,...Object.values(themes));$("#tr").replaceChildren(...Object.entries(themes).map(([k,v])=>{
  const r=el("div","","row"),b=el("span","","bar"),i=el("i");i.style.width=v/m*100+"%";b.append(i);r.append(el("span",k,"n"),b,el("span",v));return r}));
 $("#ch").replaceChildren(...["All",...Object.keys(themes)].map(t=>{const b=el("button",t,t===f?"on":"");b.onclick=()=>{f=t;render()};return b}));
 $("#ls").replaceChildren(...data.map((d,n)=>({d,n})).filter(o=>f==="All"||o.d.tags.includes(f)).map(({d,n})=>{
  const c=el("div","","box it"),a=el("a",d.title,"t");a.href=d.link;a.target="_blank";a.rel="noopener";
  const mt=el("div",d.source+" · "+ago(d.ts),"meta");d.tags.forEach(t=>mt.append(el("span",t,"tag")));
  const body=el("div");body.append(a);if(d.desc)body.append(el("div",d.desc,"meta"));body.append(mt);c.append(el("div",n+1,"rk"),body);return c}))}
async function load(force){$("#st").textContent="Loading…";
 try{const j=await(await fetch("/api/news"+(force?"?refresh=1":""))).json();data=j.items;themes=j.themes;
  if(!j.items.length){$("#st").textContent=(j.error||"No stories yet. Try Refresh in a minute.")+" (details: /api/debug)";render();return}
  let m=force?(j.refreshed?"Refreshed: "+j.new+" new in top 10 · ":"Checked moments ago, wait a minute · "):"";
  if(j.error)m="Latest fetch failed, showing saved news · "+m;
  $("#st").textContent=m+"Last fetched "+new Date(j.updated*1000).toLocaleString()+" · source: "+j.source;render()}
 catch(e){$("#st").textContent="Could not load news."}}
$("#rf").onclick=()=>load(true);load();
</script></body></html>"""


HOME = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>GW Dashboard</title>
<style>
:root{--bg:#f4f5f7;--card:#fff;--tx:#14181f;--mut:#5d6675;--bd:#dde1e8;--ac:#1f5fbf}
@media(prefers-color-scheme:dark){:root{--bg:#0f1319;--card:#181e27;--tx:#e8ecf2;--mut:#99a3b3;--bd:#2a3340;--ac:#6ea8ff}}
body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.5 system-ui,sans-serif}
.w{max-width:900px;margin:0 auto;padding:48px 16px}h1{font-size:34px;margin:0 0 6px}
p{color:var(--mut);margin:0 0 28px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px}
a.card{display:block;background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:20px;
text-decoration:none;color:var(--tx)}a.card:hover{border-color:var(--ac)}
a.card b{display:block;font-size:18px;color:var(--ac);margin-bottom:4px}a.card span{color:var(--mut);font-size:14px}
</style></head><body><div class="w"><h1>GW dashboard</h1>
<p>Guided weapons and subsystem technology monitoring.</p>
<div class="grid">
<a class="card" href="/trends"><b>Top 10 Latest Trends</b><span>Live news and subsystem trends, refreshed on demand.</span></a>
</div></div></body></html>"""


@app.route("/")
def home():
    return HOME


@app.route("/trends")
def trends():
    return PAGE


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

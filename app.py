import calendar, hmac, os, re, time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import feedparser
from flask import Flask, Response, jsonify, request

app = Flask(__name__)

# Login: set GW_PASSWORD (and optionally GW_USER) on the host. If unset, no login (local use).
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

CACHE_SECONDS = 1800          # news is re-fetched at most every 30 min
MIN_REFRESH_GAP = 10          # Refresh button can't hammer the feeds
TOP_N = 10

# Edit these to change what the dashboard tracks.
# search phrase -> theme used if the headline itself matches no keyword
QUERIES = {
    "guided missile": "General",
    "missile seeker": "Seeker",
    "precision strike missile contract": "Production",
    "hypersonic missile test": "Hypersonic",
    "low-cost cruise missile": "Low-cost",
    "missile interceptor": "Interceptors",
    "solid rocket motor missile": "Propulsion",
    "air-to-air missile production": "Production",
}
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

cache = {"items": [], "themes": {}, "updated": 0}


def fetch_one(item):
    q, default = item
    url = ("https://news.google.com/rss/search?q=" + quote(q + " when:7d")
           + "&hl=en-US&gl=US&ceid=US:en")
    try:
        return default, feedparser.parse(url).entries
    except Exception:
        return default, []


def fetch_news():
    seen, now = {}, time.time()
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(fetch_one, QUERIES.items()))
    for default, entries in results:
        for e in entries:
            title, _, src = e.title.rpartition(" - ")
            title = title or e.title
            src = e.get("source", {}).get("title") or src
            key = re.sub(r"[^a-z0-9]", "", title.lower())[:60]
            if key not in seen:
                ts = calendar.timegm(e.published_parsed) if e.get("published_parsed") else now
                text = title.lower()
                seen[key] = {"title": title, "link": e.link, "source": src, "ts": ts,
                             "hits": 0, "fb": set(),
                             "tags": [t for t, kw in THEMES.items() if any(k in text for k in kw)]}
            seen[key]["hits"] += 1
            seen[key]["fb"].add(default)
    ranked = sorted(seen.values(),
                    key=lambda i: i["hits"] * 24 - (now - i["ts"]) / 3600, reverse=True)[:TOP_N]
    for i in ranked:
        if not i["tags"]:  # fall back to the search that found it
            i["tags"] = sorted(t for t in i["fb"] if t != "General") or ["General"]
        del i["fb"]
    counts = {t: sum(t in i["tags"] for i in ranked) for t in [*THEMES, "General"]}
    cache.update(items=ranked, themes=counts, updated=now)


@app.route("/api/news")
def news():
    age = time.time() - cache["updated"]
    did = False
    if age > CACHE_SECONDS or (request.args.get("refresh") and age > MIN_REFRESH_GAP):
        old = {i["link"] for i in cache["items"]}
        fetch_news()
        cache["new"] = sum(i["link"] not in old for i in cache["items"]) if old else 0
        did = True
    return jsonify({**cache, "refreshed": did})


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
  const body=el("div");body.append(a,mt);c.append(el("div",n+1,"rk"),body);return c}))}
async function load(force){$("#st").textContent="Loading…";
 try{const j=await(await fetch("/api/news"+(force?"?refresh=1":""))).json();data=j.items;themes=j.themes;
  let m=force?(j.refreshed?"Refreshed: "+j.new+" new in top 10 · ":"Checked moments ago, wait a few seconds · "):"";
  $("#st").textContent=m+"Last fetched "+new Date(j.updated*1000).toLocaleString()+" · source: Google News RSS";render()}
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

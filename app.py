import base64, csv, hmac, html, io, ipaddress, json, os, re, socket, threading, time
from calendar import timegm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen
from zoneinfo import ZoneInfo

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
MAX_NEWSDATA_CALLS_PER_DAY = 150   # free plan allows about 200; stay under it
TOP_N = 10
NEWS_MAX_AGE_DAYS = 20   # if today has fewer than TOP_N stories, top up with ones up to this old
TIMEOUT = 12
UA = "Mozilla/5.0 (compatible; GWDashboard/1.0)"

# News topics: each becomes a button on the Top 10 News page. Each NewsData.io search is one
# request (free plan: about 200 a day; keep each under ~100 characters). A story must mention at
# least one "must" word to count, which cuts off-topic results.
# "must": the story mentions at least one; "context": ...and at least one of these (checked against the
# article text too), so e.g. "hypersonic" UFO stories don't count; "exclude": never these.
_MIL = ["missile", "weapon", "military", "defense", "defence", "army", "navy", "air force", "pentagon",
        "warhead", "strike", "interceptor", "rocket", "launch", "test"]
NEWS_TOPICS = {
    "Hypersonic": {
        "queries": ['hypersonic missile OR "hypersonic weapon" OR "hypersonic glide"',
                    'hypersonic test OR scramjet OR "glide vehicle"'],
        "gdelt": '(hypersonic OR scramjet OR "glide vehicle") sourcelang:english',
        "must": ["hypersonic", "scramjet", "glide vehicle", "glide body"],
        "context": _MIL,
        "exclude": ["ufo", "uap", "alien", "orbs"],
    },
    "Air & Missile Defence": {
        "queries": ['"air defense" OR "air defence" OR "missile defense" OR "missile defence"',
                    'interceptor OR THAAD OR "Iron Dome" OR "Golden Dome" OR "Patriot missile"'],
        "gdelt": '("air defense" OR "missile defense" OR interceptor OR THAAD OR "Iron Dome") sourcelang:english',
        "must": ["air defense", "air defence", "missile defense", "missile defence", "interceptor", "thaad",
                 "iron dome", "golden dome", "patriot missile", "patriot system", "patriot batter", "s-400",
                 "arrow 3", "david's sling", "nasams", "iris-t", "sm-6", "sm-3", "samp/t", "aster 30"],
        "context": _MIL,
        "exclude": ["nfl", "touchdown", "quarterback", "football", "police interceptor", "crown victoria"],
    },
    "Ballistic Missiles": {
        "queries": ['"ballistic missile" OR ICBM OR IRBM OR "missile launch"',
                    '"missile test" OR Hwasong OR Iskander OR "Fateh-110" OR Oreshnik'],
        "gdelt": '("ballistic missile" OR ICBM OR IRBM OR "missile launch" OR "missile test") sourcelang:english',
        "must": ["ballistic missile", "icbm", "irbm", "srbm", "mrbm", "missile launch", "missile test",
                 "hwasong", "iskander", "oreshnik", "df-21", "df-26", "df-41", "fateh", "kheibar", "shaheen", "agni"],
        "context": _MIL,
        "exclude": ["iskander mirza", "daily quiz"],
    },
    "Drones & Loitering Munitions": {
        "queries": ['"loitering munition" OR "kamikaze drone" OR "one-way attack drone" OR "attack drone"',
                    'Shahed OR Switchblade OR Lancet drone OR "counter-drone" OR "drone swarm"'],
        "gdelt": '("loitering munition" OR "kamikaze drone" OR "attack drone" OR Shahed OR "counter-drone") sourcelang:english',
        "must": ["loitering munition", "kamikaze drone", "attack drone", "one-way attack", "shahed", "switchblade",
                 "lancet", "counter-drone", "counter drone", "counter-uas", "drone strike", "drone swarm", "drone attack"],
        "context": _MIL + ["war", "attack"],
        "exclude": ["delivery drone", "drone show", "wedding"],
    },
}
# ------------------------------------------------------------------------------

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


_gdelt_gate, _gdelt_last = threading.Lock(), [0.0]


def fetch_gdelt(query):
    url = "https://api.gdeltproject.org/api/v2/doc/doc?" + urlencode({
        "query": query, "mode": "artlist", "maxrecords": "75",
        "format": "json", "sort": "datedesc", "timespan": "%dd" % NEWS_MAX_AGE_DAYS})
    for n in range(3):   # GDELT is free but often busy (HTTP 429) or slow: try up to three times
        with _gdelt_gate:   # GDELT blocks bursts: keep searches at least 6 seconds apart
            gap = 6 - (time.time() - _gdelt_last[0])
            if gap > 0:
                time.sleep(gap)
            _gdelt_last[0] = time.time()
        try:
            data = http_json(url)
            break
        except Exception:
            if n == 2:
                raise
            time.sleep(15 * (n + 1))
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


# ---- Daily snapshot, storage and summaries -----------------------------------
# The news is fetched once a day at UPDATE_HOUR (local time in GW_TZ) and saved as a
# "snapshot". Visitors only ever read the saved snapshot; nothing they do triggers a fetch.
GW_TZ = os.environ.get("GW_TZ", "Asia/Singapore")
UPDATE_HOUR = int(os.environ.get("GW_UPDATE_HOUR", "6"))
CRON_KEY = os.environ.get("CRON_KEY")                # secret for /api/daily-update
GH_TOKEN = os.environ.get("GH_TOKEN")                # optional: keeps history across restarts
GH_REPO = os.environ.get("GH_REPO")                  # e.g. "yourname/gw-dashboard"
GH_BRANCH = os.environ.get("GH_BRANCH", "data")      # NOT your deploy branch, or every save redeploys
GH_PATH = os.environ.get("GH_PATH", "gw_news_history.json")
DATA_FILE = os.environ.get("GW_DATA_FILE", "gw_news_history.json")
RETRY_GAP = 900          # after a failed update, wait this long before trying again
KEEP_DAYS = 21           # days of snapshots kept (older days top up a topic that has few new stories)
# ------------------------------------------------------------------------------

lock = threading.Lock()          # held while an update runs
store_lock = threading.Lock()
calls = {"day": "", "n": 0}
state = {"running": False, "last_try": 0.0, "error": None, "steps": [], "store_note": ""}
store = {"loaded": False, "days": {}, "sha": None, "branch_ok": False,
         "standards": [], "std_notes": {}, "std_digests": {}}


def tz():
    try:
        return ZoneInfo(GW_TZ)
    except Exception:
        return timezone.utc


def local_now():
    return datetime.now(tz())


def slot_of(dt):
    """The snapshot 'slot' a moment belongs to: the date of the most recent update time."""
    return (dt - timedelta(hours=UPDATE_HOUR)).strftime("%Y-%m-%d")


def utc_label(dt):
    off = dt.utcoffset() or timedelta(0)
    h = off.total_seconds() / 3600
    return "UTC" + ("%+d" % h if h == int(h) else "%+.1f" % h)


def nice(dt):
    return "%s %d %s, %d:%02d %s %s" % (dt.strftime("%a"), dt.day, dt.strftime("%b"),
                                         dt.hour % 12 or 12, dt.minute, "AM" if dt.hour < 12 else "PM", utc_label(dt))


def next_update_time():
    now = local_now()
    t = now.replace(hour=UPDATE_HOUR, minute=0, second=0, microsecond=0)
    return t if t > now else t + timedelta(days=1)


# ---- storage: GitHub data branch if configured, else a local file ----
def gh(method, path, body=None):
    req = Request("https://api.github.com" + path, method=method,
                  data=json.dumps(body).encode() if body is not None else None,
                  headers={"Authorization": "Bearer " + GH_TOKEN, "User-Agent": UA,
                           "Accept": "application/vnd.github+json",
                           "X-GitHub-Api-Version": "2022-11-28",
                           "Content-Type": "application/json"})
    with urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8") or "{}")


def read_local():
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def ensure_loaded():
    with store_lock:
        if store["loaded"]:
            return
        days, note, obj = None, "", {}
        if GH_TOKEN and GH_REPO:
            try:
                j = gh("GET", "/repos/%s/contents/%s?ref=%s" % (GH_REPO, GH_PATH, GH_BRANCH))
                raw = j.get("content") or ""
                if not raw and j.get("sha"):   # files over 1 MB: the contents API leaves content empty
                    raw = gh("GET", "/repos/%s/git/blobs/%s" % (GH_REPO, j["sha"]))["content"]
                obj = json.loads(base64.b64decode(raw).decode("utf-8"))
                days = obj.get("days", {})
                store["sha"] = j.get("sha")
                store["branch_ok"] = True
            except HTTPError as ex:
                if ex.code == 404:
                    obj, days = {}, {}   # branch or file not created yet: fine, the first save creates it
                else:
                    note = "GitHub storage unreachable (HTTP %s); using local file" % ex.code
            except Exception:
                note = "GitHub storage unreachable; using local file"
        if days is None:
            obj = read_local()
            days = obj.get("days", {})
        store.update(days=days, loaded=True, standards=obj.get("standards", []), std_notes=obj.get("std_notes", {}),
                     std_digests=obj.get("std_digests", {}))
        state["store_note"] = note


def ensure_branch():
    if store["branch_ok"]:
        return
    try:
        gh("GET", "/repos/%s/git/ref/heads/%s" % (GH_REPO, GH_BRANCH))
    except HTTPError as ex:
        if ex.code != 404:
            raise
        info = gh("GET", "/repos/%s" % GH_REPO)
        ref = gh("GET", "/repos/%s/git/ref/heads/%s" % (GH_REPO, info["default_branch"]))
        gh("POST", "/repos/%s/git/refs" % GH_REPO,
           {"ref": "refs/heads/" + GH_BRANCH, "sha": ref["object"]["sha"]})
    store["branch_ok"] = True


def store_save(label, message=None):
    payload = json.dumps({"days": store["days"],
                          "standards": store["standards"], "std_notes": store["std_notes"],
                          "std_digests": store["std_digests"]},
                         ensure_ascii=False, separators=(",", ":"))
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            f.write(payload)
    except OSError:
        pass
    if not (GH_TOKEN and GH_REPO):
        state["store_note"] = "no GitHub storage set: history may reset when the free host restarts"
        return
    try:
        ensure_branch()
        for attempt_no in range(2):
            body = {"message": message or ("Daily news snapshot " + label), "branch": GH_BRANCH,
                    "content": base64.b64encode(payload.encode("utf-8")).decode()}
            if store["sha"]:
                body["sha"] = store["sha"]
            try:
                j = gh("PUT", "/repos/%s/contents/%s" % (GH_REPO, GH_PATH), body)
                store["sha"] = j["content"]["sha"]
                state["store_note"] = "saved to GitHub branch " + GH_BRANCH
                return
            except HTTPError as ex:
                if ex.code in (409, 422) and attempt_no == 0:   # stale sha: fetch the current one
                    j = gh("GET", "/repos/%s/contents/%s?ref=%s" % (GH_REPO, GH_PATH, GH_BRANCH))
                    store["sha"] = j.get("sha")
                    continue
                raise
    except Exception as ex:
        code = getattr(ex, "code", "")
        state["store_note"] = "GitHub save failed %s; history kept in memory/local file only" % code


# ---- fetching each topic's top stories ----
def tidy_title(t):
    """GDELT titles come with spaces around punctuation ("world , AI - powered"); put them back together."""
    t = re.sub(r"\s+", " ", t or "").strip()
    t = re.sub(r"\s+([,:;.!?%)])", r"\1", t)
    t = re.sub(r"(\w) - (\w)", r"\1-\2", t)
    return re.sub(r"\(\s+", "(", t)


def collect_top(topic):
    """Search the topic, keep on-topic stories, merge duplicates and rank by coverage and freshness."""
    cfg, now, steps, articles, used = NEWS_TOPICS[topic], time.time(), [], [], None
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if calls["day"] != today:
        calls.update(day=today, n=0)
    if not NEWSDATA_KEY:
        steps.append({"source": "NewsData.io", "topic": topic, "stories": 0, "status": "skipped: no API key set"})
    elif calls["n"] + len(cfg["queries"]) > MAX_NEWSDATA_CALLS_PER_DAY:
        steps.append({"source": "NewsData.io", "topic": topic, "stories": 0, "status": "skipped: daily limit reached"})
    else:
        calls["n"] += len(cfg["queries"])
        with ThreadPoolExecutor(max_workers=len(cfg["queries"])) as pool:
            for res, step in pool.map(lambda q: attempt("NewsData.io", fetch_newsdata, q), cfg["queries"]):
                articles += res
                steps.append(dict(step, topic=topic))
        if articles:
            used = "NewsData.io"
    # NewsData's free plan only covers about two days; GDELT adds the last few weeks, so there are always
    # older stories to fill the top 10 when the last day has few
    more, step = attempt("GDELT", fetch_gdelt, cfg["gdelt"])
    steps.append(dict(step, topic=topic))
    if more:
        articles += more
        used = used + " + GDELT" if used else "GDELT"

    seen = {}
    for a in articles:
        a["title"] = tidy_title(a["title"])
        text = (a["title"] + " " + a["desc"]).lower()
        if not a["title"] or not a["link"] or not any(k in text for k in cfg["must"]) or off_topic(text, cfg):
            continue
        key = re.sub(r"[^a-z0-9]", "", a["title"].lower())[:60]
        if key in seen:
            seen[key]["hits"] += 1
            if not seen[key].get("image") and a.get("image"):
                seen[key]["image"] = a["image"]
            continue
        seen[key] = {**a, "desc": a["desc"].strip(), "hits": 1}
    # Many more candidates than needed: the weapons/military check needs the article text (GDELT gives
    # headlines only), and summarise_top keeps reading down this list until it has TOP_N stories.
    cutoff = now - NEWS_MAX_AGE_DAYS * 86400
    ranked = sorted((i for i in seen.values() if i["ts"] >= cutoff),
                    key=lambda i: i["hits"] * 24 - (now - i["ts"]) / 3600, reverse=True)[:TOP_N * 6]
    return ranked, steps, used


def off_topic(text, cfg):
    return any(re.search(r"\b%s\b" % k, text) for k in cfg["exclude"])


def on_topic(text, cfg):
    return any(k in text for k in cfg["context"]) and not off_topic(text, cfg)


# ---- reading the article (best effort): its text and preview picture ----
def public_url(u):
    """Only fetch normal public web addresses (never internal/private ones)."""
    try:
        p = urlsplit(u)
        if p.scheme not in ("http", "https") or not p.hostname:
            return False
        for info in socket.getaddrinfo(p.hostname, None):
            ip = ipaddress.ip_address(info[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return False
        return True
    except Exception:
        return False


class SafeRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not public_url(newurl):
            raise URLError("blocked redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _meta(raw, *names):
    for n in names:
        m = (re.search(r'(?is)<meta[^>]+(?:property|name)=["\']%s["\'][^>]+content=["\']([^"\']+)' % re.escape(n), raw) or
             re.search(r'(?is)<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']%s["\']' % re.escape(n), raw))
        if m:
            return html.unescape(m.group(1)).strip()
    return ""


def _ld_objects(raw):
    """Structured data (JSON-LD) that news sites embed for search engines; often holds the clean article text."""
    out = []
    for block in re.findall(r'(?is)<script[^>]+application/ld\+json[^>]*>(.*?)</script>', raw):
        try:
            data = json.loads(block.strip())
        except ValueError:
            continue
        stack = data if isinstance(data, list) else [data]
        while stack:
            o = stack.pop()
            if isinstance(o, dict):
                out.append(o)
                stack.extend(o.get("@graph") or [])
            elif isinstance(o, list):
                stack.extend(o)
    return out


def _text(s):
    t = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()
    return re.sub(r"\s+([,.;:!?)])", r"\1", t)   # "system , will" -> "system, will"


def _paragraphs(blocks):
    """Join text blocks into body text, dropping heading-like lines (short, no full stop) and making sure
    every block ends a sentence, so a heading or the next paragraph never runs into a sentence."""
    out = []
    for b in blocks:
        t = _text(b)
        if len(t.split()) < 8 and not re.search(r"[.!?][\"”’')]?$", t):
            continue
        out.append(t if re.search(r"[.!?][\"”’')]?$", t) else t + ".")
    return " ".join(out)


def fetch_article(url):
    """The article page's publisher summary ("lead"), its body text and its share picture ({} if unreadable).
    Body text comes from the page's structured data when present, else only from the main <article> area."""
    try:
        if not public_url(url):
            return {}
        op = build_opener(SafeRedirect)
        with op.open(Request(url, headers={"User-Agent": UA, "Accept": "text/html"}), timeout=8) as r:
            if "html" not in (r.headers.get("Content-Type") or "html"):
                return {}
            data = r.read(800000)
            # use the page's own character set (header, then <meta charset>), so quotes and dashes don't garble
            cs = r.headers.get_content_charset()
            if not cs:
                m = re.search(rb'(?i)<meta[^>]+charset=["\']?([\w-]+)', data[:5000])
                cs = m.group(1).decode() if m else "utf-8"
            try:
                raw = data.decode(cs, "replace")
            except LookupError:
                raw = data.decode("utf-8", "replace")
        image = clean_img(_meta(raw, "og:image", "twitter:image", "og:image:url"))
        lead = _meta(raw, "og:description", "description", "twitter:description")
        # 1. paragraphs, from the main article area if the page marks one (else the whole page)
        page = re.sub(r"(?is)<(script|style|noscript|figcaption|figure|aside|nav|footer|header|form|blockquote|h[1-6])[^>]*>.*?</\1>", " ", raw)
        areas = [a for a in re.findall(r"(?is)<article[^>]*>(.*?)</article>", page) if len(a) > 2000]
        paras = _paragraphs(re.findall(r"(?is)<p[^>]*>(.*?)</p>", max(areas, key=len) if areas else page))
        # 2. else the structured article text, split at its line breaks so headings don't run into sentences
        if len(paras) < 300:
            for o in _ld_objects(raw):
                if isinstance(o.get("articleBody"), str):
                    alt = _paragraphs(re.split(r"\s*\n\s*", html.unescape(o["articleBody"])))
                    if len(alt) > len(paras):
                        paras = alt
                if not lead and isinstance(o.get("description"), str):
                    lead = o["description"]
        return {"lead": _text(lead), "body": paras[:12000], "image": image}
    except Exception:
        return {}


# ---- free 3-line summary, built only from sentences that pass a strict quality check ----
_BAD = re.compile(r"https?://|www\.|pic\.twitter|twitter\.com|\.com/|\||»|«|•|©|�|\.\.\.$|…|\s[—–]\s\w+$|\b(cookies?|subscribe|subscription|subscriber|newsletter|"
                  r"advertis\w*|sign up|sign in|log ?in|read more|also read|read also|related|recommended|click|tap here|"
                  r"javascript|browser|all rights|terms of|privacy|updated|published|min read|minute read|share this|"
                  r"follow us|photo|image|credit|getty|file photo|watch|listen|podcast|video|copyright|disclaimer)\b", re.I)
_WIRE = re.compile(r"^(?:[A-Z][A-Za-z.' -]{1,30}(?:,\s*[A-Z][a-z]{2,8}\.? \d{1,2})?\s*)?\((?:Reuters|AP|AFP|Bloomberg|"
                   r"Yonhap|IANS|PTI|ANI|Xinhua|TASS|UPI|dpa|Kyodo)\)\s*[-–—:]?\s*")
_BYLINE = re.compile(r"^By [A-Z][\w.'-]+(?: [A-Z][\w.'-]+){0,3}\s*[,|:-]?\s*")
_ABBR = ("U.S.", "U.K.", "U.N.", "E.U.", "Mr.", "Mrs.", "Ms.", "Dr.", "Gen.", "Lt.", "Col.", "Maj.", "Capt.", "Adm.",
         "Sgt.", "Rep.", "Sen.", "Gov.", "No.", "St.", "Jan.", "Feb.", "Aug.", "Sept.", "Sep.", "Oct.", "Nov.", "Dec.",
         "Inc.", "Co.", "Corp.", "Ltd.", "vs.", "e.g.", "i.e.", "approx.", "est.")
_COMMON = set("""about after again also amid among been before being between both could during each from have
 into just more most news over says said than that their them then there these they this those through under
 until were what when where which while will with would year years world first make makes making""".split())


def _words(s):
    return {w for w in re.findall(r"[a-z][a-z\-]{3,}", s.lower()) if w not in _COMMON}


def _sentences(text):
    """Split into sentences without breaking on abbreviations like U.S. or Gen."""
    t = re.sub(r"([a-z0-9%)][.!?])([A-Z][a-z])", r"\1 \2", text)   # "late 2025.Dark Eagle" -> two sentences
    for a in _ABBR:
        t = t.replace(a, a.replace(".", "․"))
    parts = re.split(r"(?<=[.!?])[\"”’']?\s+(?=[A-Z0-9\"“‘(])", t)
    return [_BYLINE.sub("", _WIRE.sub("", p.replace("․", ".").strip())).strip() for p in parts]


def good_sentence(s):
    """A proper, complete sentence of prose: not a menu, caption, byline, link list or boilerplate."""
    w = s.split()
    if not (50 <= len(s) <= 260) or len(w) < 8 or _BAD.search(s):   # 260: always shown whole, never cut off
        return False
    if not re.match(r"[A-Z0-9\"“‘(]", s) or not re.search(r"[.!][\"”’')]?$", s):
        return False
    chars = [c for c in s if not c.isspace()]
    if sum(c.isalpha() for c in chars) < 0.75 * len(chars):
        return False
    return sum(x[:1].isupper() for x in w[1:]) <= 0.5 * (len(w) - 1)   # mostly capitalised = a menu or link list


def _similar(a, b):
    wa, wb = _words(a), _words(b)
    return bool(wa and wb) and len(wa & wb) / min(len(wa), len(wb)) > 0.6


def _short(s):
    return s if len(s) <= 260 else s[:257].rsplit(" ", 1)[0] + "…"


def summary_lines(page, desc, words, title="", n=3):
    """Up to n accurate lines, never filler: the publisher's own summary first, then the body sentences most
    about the headline (named systems, figures, early position), in article order. All are the article's own
    words. Fewer lines are shown rather than any that fail the quality check."""
    head = _words(title)
    picked = []
    feed = re.sub(r"\s*(\[\s*(\.\.\.|…)\s*\]|\.\.\.|…)\s*$", "", _text(desc))
    if feed != _text(desc) and re.search(r"[.!?]", feed):  # news-feed descriptions are often cut off: keep whole sentences
        feed = feed[:max(feed.rfind(". "), feed.rfind("! "), feed.rfind(".") if feed.endswith(".") else -1) + 1]
    for src in (page.get("lead"), feed):                   # 1. the publisher's summary
        for s in _sentences(src or ""):
            if good_sentence(s) and not any(_similar(s, p) for p in picked):
                picked.append(s)
                break
        if picked:
            break
    scored = []                                            # 2. body sentences about this story
    for pos, s in enumerate(_sentences(page.get("body") or "")):
        if not good_sentence(s) or s.endswith("?"):
            continue
        low = s.lower()
        shared, topical = len(head & _words(s)), sum(w in low for w in words)
        if shared < 2 and not topical:
            continue
        score = 2 * min(3, shared) + 3 * topical + min(3, len(re.findall(r"\d", s)) // 2) + max(0, 4 - pos // 3)
        scored.append((score, pos, s))
    body = []
    for score, pos, s in sorted(scored, key=lambda t: -t[0]):
        if len(picked) + len(body) >= n:
            break
        if not any(_similar(s, p) for p in picked + [b for _, b in body]):
            body.append((pos, s))
    return [_short(s) for s in picked + [s for _, s in sorted(body)]]


_pages = {}   # article pages read during one update, so a story under two topics is fetched once


def read_page(url):
    if url not in _pages:
        _pages[url] = fetch_article(url)
    return _pages[url]


FRESH_HOURS = 24   # stories newer than this lead the top 10; older ones fill the rest, newest first


def summarise_top(candidates, cfg):
    """Read the candidates' articles in rank order, a batch at a time, until TOP_N are really about the topic.
    The last day's stories come first (by coverage), then older ones newest first."""
    kept = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        for b in range(0, len(candidates), 12):
            batch = candidates[b:b + 12]
            for i, page in zip(batch, pool.map(lambda x: read_page(x["link"]), batch)):
                text = " ".join((i["title"], i["desc"], page.get("lead", ""), page.get("body", ""))).lower()
                if not on_topic(text, cfg):
                    continue
                i["points"] = summary_lines(page, i["desc"], cfg["must"], i["title"])
                if not i.get("image") and page.get("image"):
                    i["image"] = page["image"]
                kept.append(i)
            if len(kept) >= TOP_N:
                break
    fresh = time.time() - FRESH_HOURS * 3600
    new = [i for i in kept if i["ts"] >= fresh]                       # already in coverage order
    old = sorted((i for i in kept if i["ts"] < fresh), key=lambda i: i["ts"], reverse=True)
    return (new + old)[:TOP_N]


def run_update():
    """Fetch and summarise every topic and save today's snapshot. Returns True if one was saved."""
    if not lock.acquire(blocking=False):
        return False
    state["running"] = True
    _pages.clear()
    try:
        state["steps"], topics, sources = [], {}, set()
        for topic, cfg in NEWS_TOPICS.items():
            ranked, steps, used = collect_top(topic)
            state["steps"] += steps
            ranked = summarise_top(ranked, cfg) if ranked else []
            if ranked:
                topics[topic] = [{"rank": n + 1, "title": i["title"], "link": i["link"], "source": i["source"],
                                  "ts": i["ts"], "image": i.get("image", ""), "points": i["points"]}
                                 for n, i in enumerate(ranked)]
                sources.add(used)
        state["last_try"] = time.time()
        if not topics:
            state["error"] = "No news could be fetched; will retry automatically."
            return False
        ensure_loaded()
        slot = slot_of(local_now())
        with store_lock:
            store["days"][slot] = {"fetched": time.time(), "source": " + ".join(sorted(sources)), "topics": topics}
            for k in sorted(store["days"])[:-KEEP_DAYS]:
                del store["days"][k]
        store_save(slot)
        state["error"] = None
        return True
    except Exception as ex:
        state["error"] = "Update failed: " + str(ex)[:80]
        state["last_try"] = time.time()
        return False
    finally:
        _pages.clear()
        state["running"] = False
        lock.release()
        research_daily()   # also refresh Market Research data older than a week, in the background


def latest_day():
    ensure_loaded()
    return max(store["days"]) if store["days"] else None


def update_due():
    latest = latest_day()
    # also due if the saved snapshot is from before topics existed
    return not latest or slot_of(local_now()) > latest or "topics" not in store["days"][latest]


def kick_update():
    """Start an update in the background if one is due (never blocks a visitor)."""
    if (update_due() and not state["running"] and time.time() - state["last_try"] > RETRY_GAP):
        threading.Thread(target=run_update, daemon=True).start()
        return True
    return False


# Names spotted in a story's headline and summary, for the "Copy to share" text. Each entry: the name shown,
# then the words that count as a mention. Short all-capital names (US, UK, RTX...) must match exactly in capitals.
_COUNTRY_NAMES = [
    ("United States", ["United States", "US", "U.S.", "USA", "American", "Pentagon"]), ("China", ["China", "Chinese", "PLA"]),
    ("Russia", ["Russia", "Russian", "Kremlin"]), ("Ukraine", ["Ukraine", "Ukrainian", "Kyiv"]), ("Iran", ["Iran", "Iranian", "Tehran"]),
    ("Israel", ["Israel", "Israeli", "IDF"]), ("North Korea", ["North Korea", "North Korean", "Pyongyang", "DPRK"]),
    ("South Korea", ["South Korea", "South Korean", "Seoul"]), ("Japan", ["Japan", "Japanese"]), ("Taiwan", ["Taiwan", "Taiwanese"]),
    ("India", ["India", "Indian Army", "Indian Navy", "Indian Air Force", "New Delhi"]), ("Pakistan", ["Pakistan", "Pakistani"]),
    ("Turkey", ["Turkey", "Türkiye", "Turkish"]), ("Saudi Arabia", ["Saudi"]), ("UAE", ["UAE", "Emirati", "United Arab Emirates"]),
    ("Qatar", ["Qatar"]), ("Yemen", ["Yemen", "Houthi", "Houthis"]), ("Syria", ["Syria", "Syrian"]), ("Iraq", ["Iraq", "Iraqi"]),
    ("Lebanon", ["Lebanon", "Lebanese", "Hezbollah"]), ("Germany", ["Germany", "German", "Bundeswehr"]), ("France", ["France", "French"]),
    ("United Kingdom", ["United Kingdom", "UK", "U.K.", "Britain", "British"]), ("Italy", ["Italy", "Italian"]),
    ("Poland", ["Poland", "Polish"]), ("Norway", ["Norway", "Norwegian"]), ("Sweden", ["Sweden", "Swedish"]),
    ("Finland", ["Finland", "Finnish"]), ("Netherlands", ["Netherlands", "Dutch"]), ("Spain", ["Spain", "Spanish"]),
    ("Greece", ["Greece", "Greek"]), ("Romania", ["Romania", "Romanian"]), ("Belarus", ["Belarus"]), ("Canada", ["Canada", "Canadian"]),
    ("Australia", ["Australia", "Australian"]), ("Singapore", ["Singapore"]), ("Malaysia", ["Malaysia", "Malaysian"]),
    ("Indonesia", ["Indonesia", "Indonesian"]), ("Philippines", ["Philippines", "Philippine", "Filipino"]),
    ("Vietnam", ["Vietnam", "Vietnamese"]), ("Thailand", ["Thailand"]), ("Brazil", ["Brazil", "Brazilian"]),
    ("Egypt", ["Egypt", "Egyptian"]), ("NATO", ["NATO"]),
]
_COMPANY_NAMES = [
    ("Lockheed Martin", ["Lockheed"]), ("RTX (Raytheon)", ["Raytheon", "RTX"]), ("Boeing", ["Boeing"]),
    ("Northrop Grumman", ["Northrop"]), ("General Dynamics", ["General Dynamics"]), ("L3Harris", ["L3Harris"]),
    ("BAE Systems", ["BAE"]), ("Anduril", ["Anduril"]), ("General Atomics", ["General Atomics"]), ("Kratos", ["Kratos"]),
    ("AeroVironment", ["AeroVironment"]), ("Textron", ["Textron"]), ("Leidos", ["Leidos"]), ("Palantir", ["Palantir"]),
    ("Shield AI", ["Shield AI"]), ("Epirus", ["Epirus"]), ("MBDA", ["MBDA"]), ("Thales", ["Thales"]), ("Airbus", ["Airbus"]),
    ("Safran", ["Safran"]), ("Dassault", ["Dassault"]), ("Naval Group", ["Naval Group"]), ("KNDS", ["KNDS", "Nexter", "Krauss-Maffei"]),
    ("Rheinmetall", ["Rheinmetall"]), ("Diehl Defence", ["Diehl"]), ("Hensoldt", ["Hensoldt"]), ("Helsing", ["Helsing"]),
    ("Leonardo", ["Leonardo"]), ("Saab", ["Saab"]), ("Kongsberg", ["Kongsberg"]), ("Nammo", ["Nammo"]), ("Patria", ["Patria"]),
    ("Rafael", ["Rafael Advanced", "Israel's Rafael"]),   # not plain "Rafael": IAEA chief Rafael Grossi is often in the news ("Israel Aerospace Industries", ["Israel Aerospace", "IAI"]), ("Elbit Systems", ["Elbit"]),
    ("Hanwha", ["Hanwha"]), ("LIG Nex1", ["LIG Nex1", "LIG Nex"]), ("Korea Aerospace Industries", ["Korea Aerospace", "KAI"]),
    ("Hyundai Rotem", ["Hyundai Rotem"]), ("Mitsubishi Heavy Industries", ["Mitsubishi Heavy"]), ("Kawasaki Heavy Industries", ["Kawasaki Heavy"]),
    ("ST Engineering", ["ST Engineering"]), ("Baykar", ["Baykar"]), ("Roketsan", ["Roketsan"]), ("Aselsan", ["Aselsan"]),
    ("Turkish Aerospace", ["Turkish Aerospace", "TUSAŞ"]), ("Norinco", ["Norinco"]), ("CASC", ["CASC"]), ("CASIC", ["CASIC"]),
    ("AVIC", ["AVIC"]), ("Almaz-Antey", ["Almaz-Antey"]), ("Rostec", ["Rostec"]), ("Kalashnikov", ["Kalashnikov"]),
    ("Tactical Missiles Corporation", ["Tactical Missiles Corporation"]), ("BrahMos Aerospace", ["BrahMos Aerospace"]),
    ("Bharat Dynamics", ["Bharat Dynamics", "BDL"]), ("Bharat Electronics", ["Bharat Electronics", "BEL"]),
    ("Hindustan Aeronautics", ["Hindustan Aeronautics", "HAL"]), ("Tata Advanced Systems", ["Tata Advanced"]),
    ("Larsen & Toubro", ["Larsen & Toubro", "L&T"]), ("Solar Industries", ["Solar Industries"]), ("Embraer", ["Embraer"]),
    ("Avibras", ["Avibras"]), ("Denel", ["Denel"]), ("EDGE Group", ["EDGE Group"]), ("Fire Point", ["Fire Point"]),
]


def _name_rx(words):
    exact = [w for w in words if w.isupper() or "." in w or "&" in w]
    loose = [w for w in words if w not in exact]
    parts = []
    if exact:
        parts.append(re.compile(r"(?<![\w.])(?:%s)(?![\w])" % "|".join(map(re.escape, exact))))
    if loose:
        parts.append(re.compile(r"\b(?:%s)\b" % "|".join(map(re.escape, loose)), re.I))
    return parts


_COUNTRY_RX = [(name, _name_rx(w)) for name, w in _COUNTRY_NAMES]
_COMPANY_RX = [(name, _name_rx(w)) for name, w in _COMPANY_NAMES]


def mentions(text, table, limit=4):
    """Names from the table that appear in the text, in table order (most relevant first), at most `limit`."""
    return [name for name, rxs in table if any(rx.search(text) for rx in rxs)][:limit]


def topic_items(topic):
    """The latest snapshot's stories for a topic, topped up to TOP_N with the newest stories from earlier
    snapshots (no repeats, nothing older than NEWS_MAX_AGE_DAYS)."""
    out, seen = [], set()
    cutoff = time.time() - NEWS_MAX_AGE_DAYS * 86400
    for n, day in enumerate(sorted(store["days"], reverse=True)):
        items = store["days"][day].get("topics", {}).get(topic, [])
        if n:   # earlier days: newest first
            items = sorted(items, key=lambda i: i.get("ts", 0), reverse=True)
        for i in items:
            key = re.sub(r"[^a-z0-9]", "", (i.get("title") or "").lower())[:60]
            if key in seen or i.get("link") in seen or (n and i.get("ts", 0) < cutoff):
                continue
            seen.update((key, i.get("link")))
            text = " ".join([i.get("title") or ""] + list(i.get("points") or []))
            out.append(dict(i, rank=len(out) + 1, earlier=bool(n),
                            countries=mentions(text, _COUNTRY_RX), companies=mentions(text, _COMPANY_RX)))
            if len(out) >= TOP_N:
                return out
    return out


@app.route("/api/news")
def news():
    updating = kick_update() or state["running"]
    latest = latest_day()
    d = store["days"].get(latest) if latest else None
    saved = (d or {}).get("topics", {})
    topic = request.args.get("topic")
    return jsonify({
        "topics": list(NEWS_TOPICS), "topic": topic,
        "items": topic_items(topic) if topic in NEWS_TOPICS else [],
        "slot": latest, "source": (d or {}).get("source"),
        "updated_label": nice(datetime.fromtimestamp(d["fetched"], tz())) if d and saved else None,
        "next_label": nice(next_update_time()), "tz": GW_TZ,
        "updating": updating, "error": state["error"]})


@app.route("/api/daily-update")
def daily_update():
    """Called by an outside scheduler a little after 6am. Needs ?key=CRON_KEY."""
    if not CRON_KEY or not hmac.compare_digest(request.args.get("key", ""), CRON_KEY):
        return jsonify({"error": "forbidden"}), 403
    if request.args.get("force"):
        started = not state["running"]
        if started:
            threading.Thread(target=run_update, daemon=True).start()
    else:
        started = kick_update()
    return jsonify({"started": started, "running": state["running"], "due": update_due(),
                    "error": state["error"]}), 202


@app.route("/api/debug")
def debug():
    ensure_loaded()
    return jsonify({"days_saved": len(store["days"]), "latest": max(store["days"]) if store["days"] else None,
                    "update_due": update_due(), "running": state["running"], "last_try": state["last_try"],
                    "error": state["error"], "storage": state["store_note"],
                    "github_storage": bool(GH_TOKEN and GH_REPO),
                    "newsdata_key_set": bool(NEWSDATA_KEY), "newsdata_calls_today": calls["n"],
                    "cron_key_set": bool(CRON_KEY), "timezone": GW_TZ, "update_hour": UPDATE_HOUR,
                    "next_update": nice(next_update_time()), "steps": state["steps"]})


# =============================================================================
# Market research: closest published systems, from Wikipedia infobox data.
# Only publicly documented specifications of existing systems are returned.
# Nothing here calls Claude or any paid service.
# =============================================================================
import html as _html
import math
import tempfile
from urllib.parse import quote

WIKI_API = "https://en.wikipedia.org/w/api.php"
# Wikimedia asks API users to identify themselves. Optionally set WIKI_CONTACT on Render
# to an email address or your site address.
WIKI_UA = "GWDashboard/1.0 (internal research tool; " + os.environ.get("WIKI_CONTACT", "contact not set") + ")"
RS_CACHE_FILE = os.path.join(os.environ.get("GW_CACHE_DIR", tempfile.gettempdir()), "gw_research_cache.json")
RS_MAX_AGE = 7 * 86400          # reuse downloaded data for a week
RS_MAX_CATEGORIES = 120         # per system type
RS_MAX_PAGES = 1500             # per system type

# Drop-down label -> Wikipedia category that lists those systems.
RESEARCH_TYPES = {
    "Air-to-air missile": "Air-to-air missiles",
    "Surface-to-air missile": "Surface-to-air missiles",
    "Anti-ship missile": "Anti-ship missiles",
    "Anti-tank missile": "Anti-tank guided missiles",
    "Air-to-surface missile": "Air-to-surface missiles",
    "Cruise missile": "Cruise missiles",
    "Anti-ballistic missile": "Anti-ballistic missiles",
}

SEEKER = {
    "Active radar": [r"(?<!semi-)(?<!semi )active radar", r"\bARH\b", r"(?<!semi-)(?<!semi )active homing"],
    "Semi-active radar": [r"semi-?active radar", r"\bSARH\b"],
    "Passive RF / anti-radiation": [r"anti-?radiation", r"passive radar", r"passive homing", r"home-on-jam", r"passive RF"],
    "Infrared": [r"infra-?red", r"\bIR\b", r"heat-seek", r"\bIIR\b"],
    "Imaging infrared": [r"imaging infra-?red", r"\bIIR\b"],
    "Laser": [r"laser"],
    "Electro-optical / TV": [r"electro-?optical", r"\bTV\b", r"optical", r"imaging(?! infra)", r"camera"],
}
GUIDANCE = {
    "GPS / satellite": [r"\bGPS\b", r"\bGNSS\b", r"satellite"],
    "Inertial": [r"inertial", r"\bINS\b"],
    "Command / wire / beam-riding": [r"command", r"\bwire\b", r"beam[- ]?rid", r"\bSACLOS\b", r"\bMCLOS\b"],
    "Terrain / map matching": [r"terrain", r"TERCOM", r"DSMAC", r"map-?match"],
}
PLATFORMS = {
    "Aircraft": [r"aircraft", r"helicopter", r"fighter", r"air-?launched", r"\bF-\d", r"\bUAV\b", r"drone"],
    "Ship": [r"\bship", r"vessel", r"frigate", r"destroyer", r"cruiser", r"warship", r"\bVLS\b", r"vertical launch", r"corvette"],
    "Submarine": [r"submarine", r"torpedo tube"],
    "Ground vehicle / launcher": [r"truck", r"vehicle", r"\bTEL\b", r"launcher", r"land-based", r"ground-launched", r"battery", r"armou?red"],
    "Man-portable": [r"man-?portable", r"shoulder", r"tripod", r"infantry", r"hand-?held", r"MANPAD"],
}
GUIDANCE_RX = {k: [re.compile(p, re.I) for p in v] for k, v in GUIDANCE.items()}
SEEKER_RX = {k: [re.compile(p, re.I) for p in v] for k, v in SEEKER.items()}
PLATFORM_RX = {k: [re.compile(p, re.I) for p in v] for k, v in PLATFORMS.items()}

MACH_MS = 340.3  # sea-level speed of sound, used to convert Mach (approximate)
LEN = {"km": 1000, "kilometre": 1000, "kilometres": 1000, "kilometer": 1000, "kilometers": 1000,
       "m": 1, "metre": 1, "metres": 1, "meter": 1, "meters": 1, "cm": .01, "mm": .001,
       "mi": 1609.344, "mile": 1609.344, "miles": 1609.344, "nmi": 1852, "nm": 1852,
       "ft": .3048, "foot": .3048, "feet": .3048, "inch": .0254, "inches": .0254, "yd": .9144, "yards": .9144}
MASS = {"kg": 1, "kilogram": 1, "kilograms": 1, "lb": .45359237, "lbs": .45359237, "pound": .45359237,
        "pounds": .45359237, "t": 1000, "tonne": 1000, "tonnes": 1000, "ton": 1000, "tons": 1000}
SPEED = {"km/h": 1 / 3.6, "kph": 1 / 3.6, "km/hr": 1 / 3.6, "m/s": 1, "mph": .44704,
         "kn": .514444, "knot": .514444, "knots": .514444, "kt": .514444, "kts": .514444}
_NUM = r"(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"


def _unit_rx(table):
    alt = "|".join(sorted((re.escape(u) for u in table), key=len, reverse=True))
    return re.compile(_NUM + r"(?:\s*[–—-]\s*" + _NUM + r")?\s*(" + alt + r")(?![/A-Za-z])", re.I)


UNIT_RX = {"len": (_unit_rx(LEN), LEN), "mass": (_unit_rx(MASS), MASS), "speed": (_unit_rx(SPEED), SPEED)}
MACH_RX = re.compile(r"\bMach\s*" + _NUM + r"(?:\s*[–—-]\s*" + _NUM + r")?", re.I)
BOUNDS = {"len": (0.005, 2e7), "mass": (0.01, 1e6), "speed": (0.5, 1e4)}

# Spec rows shown on a result card: label -> infobox parameter names to look in.
SPEC_ROWS = [
    ("Range", ["range", "max_range", "maximum_range", "effective_range", "operational_range"], "len"),
    ("Speed", ["speed", "max_speed", "maximum_speed", "velocity"], "speed"),
    ("Warhead", ["warhead_weight", "warhead_mass", "filling_weight", "warhead", "filling"], "mass"),
    ("Launch weight", ["weight", "mass", "launch_weight", "launch_mass"], "mass"),
    ("Length", ["length"], "len"),
    ("Diameter", ["diameter"], "len"),
    ("Guidance", ["guidance", "guidance_system"], None),
    ("Propulsion", ["engine", "propellant", "propulsion", "motor"], None),
    ("Launch platform", ["launch_platform", "platform", "launch_platforms"], None),
]
# Extra infobox fields kept only for the Excel export (not shown on the cards).
EXTRA_FIELDS = [
    ("Cruise speed", ["cruise_speed", "cruising_speed"]),
    ("Flight altitude", ["flight_altitude", "cruise_altitude", "operational_altitude", "altitude", "flight_ceiling"]),
    ("Ceiling", ["ceiling", "max_altitude", "maximum_altitude", "altitude_max", "engagement_altitude"]),
    ("Warhead type", ["warhead", "warhead_type"]),
    ("Warhead weight", ["warhead_weight", "warhead_mass", "filling_weight"]),
    ("Filling", ["filling", "filling_type"]),   # many missile infoboxes put the warhead here
    ("Detonation", ["detonation", "detonation_mechanism", "fuze", "fuse"]),
    ("Datalink", ["datalink", "data_link"]),
]
RS_SCHEMA = 2   # bump when entries gain new fields, so saved data is refreshed once in the background
# What the user can enter: key, label, unit, factor to SI, spec row label, weight in the score.
NUMERIC = [
    ("range", "Range", "km", 1000.0, "Range", 1.5),
    ("speed", "Speed", "Mach", MACH_MS, "Speed", 1.0),
    ("warhead", "Warhead mass", "kg", 1.0, "Warhead", 1.0),
]

RS_LOCK = threading.Lock()
RS = {"data": {}, "queue": [], "building": None, "thread": None, "log": {}, "failed_at": {}}


# ---- wikitext parsing -------------------------------------------------------
def match_braces(s, start):
    depth, i = 0, start
    while i < len(s) - 1:
        two = s[i:i + 2]
        if two == "{{":
            depth += 1
            i += 2
        elif two == "}}":
            depth -= 1
            i += 2
            if depth == 0:
                return s[start:i]
        else:
            i += 1
    return None


def split_top(s, sep="|"):
    parts, cur, dc, db, i = [], [], 0, 0, 0
    while i < len(s):
        two = s[i:i + 2]
        if two in ("{{", "}}", "[[", "]]"):
            dc += (two == "{{") - (two == "}}")
            db += (two == "[[") - (two == "]]")
            cur.append(two)
            i += 2
            continue
        ch = s[i]
        if ch == sep and dc <= 0 and db <= 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        i += 1
    parts.append("".join(cur))
    return parts


def infobox_params(text):
    m = re.search(r"\{\{\s*infobox", text or "", re.I)
    if not m:
        return None
    box = match_braces(text, m.start())
    if not box:
        return None
    out = {}
    for part in split_top(box[2:-2])[1:]:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = re.sub(r"\s+", "_", k.strip().lower())
        if k and k not in out:
            out[k] = v.strip()
    return out


_NAMED = re.compile(r"^\s*[A-Za-z_]+\s*=")
_JOINERS = {"to", "-", "–", "—", "and", "or", "+", "&"}


def _tpl(inner):
    args = split_top(inner)
    name = args[0].strip().lower()
    pos = [a.strip() for a in args[1:] if not _NAMED.match(a)]
    if name in ("convert", "cvt", "conv"):
        if len(pos) >= 4 and pos[1].lower() in _JOINERS:
            return pos[0] + "–" + pos[2] + " " + pos[3]
        if len(pos) >= 2:
            return pos[0] + " " + pos[1]
        return pos[0] if pos else ""
    if name == "val":
        unit = next((a.split("=", 1)[1].strip() for a in args[1:] if re.match(r"^\s*u\s*=", a)), "")
        return (pos[0] + " " + unit).strip() if pos else ""
    if name in ("plainlist", "flatlist", "hlist", "ubl", "unbulleted list", "bulleted list", "unbulleted_list"):
        return "; ".join(re.sub(r"^\s*\*\s*", "", p).strip() for p in re.split(r"\n|\|", " | ".join(pos)) if p.strip())
    if name.startswith(("cite", "sfn", "efn", "refn", "citation", "cn", "clarify", "failed", "better source",
                        "flagicon", "flag icon", "ill", "main", "see also", "dubious", "when", "who", "page needed")):
        return ""
    if name in ("flag", "flagcountry", "flagu", "flagdeco") and pos:
        return pos[0]
    if name in ("nowrap", "nobr", "small", "nbsp", "abbr", "lang", "smaller", "small caps", "sic", "mono") and pos:
        return pos[0]
    return pos[0] if len(pos) == 1 else ""


def clean_wikitext(v):
    v = re.sub(r"<!--.*?-->", "", v or "", flags=re.S)
    v = re.sub(r"<ref[^>/]*/>", "", v)
    v = re.sub(r"<ref[^>]*>.*?</ref>", "", v, flags=re.S | re.I)
    v = re.sub(r"<br\s*/?>|</?li>", "; ", v, flags=re.I)
    for _ in range(12):
        nv = re.sub(r"\{\{([^{}]*)\}\}", lambda m: _tpl(m.group(1)), v)
        if nv == v:
            break
        v = nv
    v = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", v)
    v = re.sub(r"<[^>]+>", "", v)
    v = _html.unescape(v).replace("\xa0", " ")
    v = re.sub(r"'{2,}", "", v)
    v = re.sub(r"[{}]|\[\[|\]\]", "", v)
    v = re.sub(r"\s*;\s*(;\s*)+", "; ", v)
    v = re.sub(r"\s+", " ", v).strip(" ;,*")
    return v


def to_float(s):
    return float(s.replace(",", ""))


def parse_values(text, dim):
    """All numbers with a recognised unit in text, converted to SI (metres, kilograms, m/s)."""
    out = []
    if not text:
        return out
    if dim == "speed":
        for m in MACH_RX.finditer(text):
            out += [to_float(g) * MACH_MS for g in m.groups() if g]
    rx, table = UNIT_RX[dim]
    for m in rx.finditer(text):
        f = table[m.group(3).lower()]
        out += [to_float(g) * f for g in (m.group(1), m.group(2)) if g]
    lo, hi = BOUNDS[dim]
    dedup = []  # keep the first-stated value when the same figure appears in two units
    for v in out:
        if lo <= v <= hi and not any(abs(v - d) <= 0.015 * d for d in dedup):
            dedup.append(round(v, 4))
    return sorted(dedup)


def entry_from_page(p):
    title = p.get("title") or ""
    if not title or title.startswith("List of"):
        return None, False
    rev = (p.get("revisions") or [{}])[0]
    text = (rev.get("slots") or {}).get("main", {}).get("content") or rev.get("content") or ""
    params = infobox_params(text[:20000])
    if not params:
        return None, False
    cleaned = {k: clean_wikitext(v) for k, v in params.items()}
    raw, num = {}, {}
    for label, keys, dim in SPEC_ROWS:
        texts = [cleaned[k] for k in keys if cleaned.get(k)]
        if not texts:
            continue
        raw[label] = texts[0][:170]
        if dim:
            num[label] = parse_values(" ; ".join(texts), dim)
    pick = lambda *ks: next((cleaned[k][:80] for k in ks if cleaned.get(k)), "")
    extra = {}
    for label, keys in EXTRA_FIELDS:
        t = next((cleaned[k] for k in keys if cleaned.get(k)), "")
        if t:
            extra[label] = t[:200]
    entry = {"title": title, "image": clean_img(((p.get("thumbnail") or {}).get("source"))),
             "raw": raw, "num": {k: v for k, v in num.items() if v}, "extra": extra,
             "origin": pick("origin", "country"), "manufacturer": pick("manufacturer", "designer"),
             "kind": pick("type"), "platform_text": " ".join(
                 filter(None, [cleaned.get("launch_platform"), cleaned.get("platform"), cleaned.get("type")]))[:300]}
    return entry, bool(entry["num"] or "Guidance" in raw)


# ---- Wikipedia calls ---------------------------------------------------------
_wiki_gate = threading.Lock()
_wiki_last = [0.0]


def wiki_api(params, post=False):
    """One request at a time, spaced out, and patient with Wikipedia's rate limit (HTTP 429)."""
    q = {"format": "json", "formatversion": "2", "maxlag": "5", **params}
    body = urlencode(q)
    last = None
    for attempt in range(6):
        wait = 0.0
        try:
            with _wiki_gate:   # paces every call from every thread
                gap = 0.4 - (time.time() - _wiki_last[0])
                if gap > 0:
                    time.sleep(gap)
                _wiki_last[0] = time.time()
            if post:
                req = Request(WIKI_API, data=body.encode(), headers={"User-Agent": WIKI_UA})
            else:
                req = Request(WIKI_API + "?" + body, headers={"User-Agent": WIKI_UA})
            with urlopen(req, timeout=25) as r:
                d = json.loads(r.read().decode("utf-8", "replace"))
            if (d.get("error") or {}).get("code") == "maxlag":
                wait = 5
                raise RuntimeError("Wikipedia is busy")
            return d
        except HTTPError as ex:
            last = ex
            if ex.code in (429, 503):
                try:
                    wait = min(float(ex.headers.get("Retry-After", "")), 60)
                except ValueError:
                    wait = 0
                wait = wait or min(4 * 2 ** attempt, 60)
            else:
                wait = 1.5 * (attempt + 1)
        except Exception as ex:
            last = ex
            wait = wait or 1.5 * (attempt + 1)
        time.sleep(wait)
    raise last


_SKIP_CAT = re.compile(r"list|stub|fiction|video game|wikipedia|template|navigation|museum|redirect|portal", re.I)


def category_members(name):
    pages, subs, cont = [], [], {}
    while True:
        d = wiki_api({"action": "query", "list": "categorymembers", "cmtitle": "Category:" + name,
                      "cmlimit": "500", "cmtype": "page|subcat", **cont})
        for m in (d.get("query") or {}).get("categorymembers", []):
            if m.get("ns") == 14:
                if not _SKIP_CAT.search(m["title"]):
                    subs.append(m["title"].split(":", 1)[1])
            elif m.get("ns") == 0 and not m["title"].startswith("List of") and "(disambiguation)" not in m["title"]:
                pages.append(m["title"])
        cont = d.get("continue")
        if not cont:
            return pages, subs


def crawl_category(top, log):
    titles, seen, level = [], {top}, [top]
    for depth in range(3):
        if not level or len(seen) > RS_MAX_CATEGORIES:
            break
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(category_members, level))
        nxt = []
        for pages, subs in results:
            titles += pages
            for s in subs:
                if s not in seen and len(seen) < RS_MAX_CATEGORIES:
                    seen.add(s)
                    nxt.append(s)
        log["cats"] = len(seen)
        log["pages"] = len(set(titles))
        level = nxt
    return list(dict.fromkeys(titles))[:RS_MAX_PAGES]


def fetch_batch(batch):
    base = {"action": "query", "titles": "|".join(batch), "redirects": "1", "prop": "revisions|pageimages",
            "rvprop": "content", "rvslots": "main", "piprop": "thumbnail", "pithumbsize": "480"}
    d = wiki_api({**base, "rvsection": "0"}, post=True)
    if "error" in d:  # fall back to whole pages; the infobox is at the top either way
        d = wiki_api(base, post=True)
    got, boxes = [], 0
    for p in (d.get("query") or {}).get("pages", []):
        e, has_box = entry_from_page(p)
        boxes += 1 if e is not None else 0
        if e is not None and has_box:
            got.append(e)
    return boxes, got


def build_type(tname):
    t0, cat = time.time(), RESEARCH_TYPES[tname]
    log = {"category": cat, "cats": 0, "pages": 0, "with_infobox": 0, "kept": 0, "error": None, "seconds": 0}
    RS["log"][tname] = log
    try:
        titles = crawl_category(cat, log)
        batches = [titles[i:i + 50] for i in range(0, len(titles), 50)]
        entries = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            for boxes, got in pool.map(fetch_batch, batches):
                log["with_infobox"] += boxes
                entries += got
                log["kept"] = len(entries)
        if not entries:
            raise RuntimeError("No systems with published specifications were found in category '%s'." % cat)
        RS["data"][tname] = {"at": time.time(), "v": RS_SCHEMA, "entries": entries}
        save_cache()
    except Exception as ex:
        log["error"] = str(ex)[:160]
        RS["failed_at"][tname] = time.time()
    log["seconds"] = round(time.time() - t0, 1)


# The downloaded specifications are kept in a local file and, when GitHub storage is set up, in the data
# branch too, so a restart of the free host doesn't mean another minute-long download for the next visitor.
RS_GH_PATH = os.environ.get("GH_RESEARCH_PATH", "gw_research_cache.json")
_rs_gh = {"sha": None, "saving": False}


def save_cache():
    try:
        with open(RS_CACHE_FILE, "w") as f:
            json.dump(RS["data"], f)
    except Exception:
        pass
    if GH_TOKEN and GH_REPO and not RS["queue"]:   # once the whole batch is built, not after every type
        threading.Thread(target=_save_cache_gh, daemon=True).start()


def _save_cache_gh():
    if _rs_gh["saving"]:
        return
    _rs_gh["saving"] = True
    try:
        ensure_branch()
        content = base64.b64encode(json.dumps(RS["data"], separators=(",", ":")).encode()).decode()
        for attempt_no in range(2):
            if not _rs_gh["sha"]:
                try:
                    _rs_gh["sha"] = gh("GET", "/repos/%s/contents/%s?ref=%s" % (GH_REPO, RS_GH_PATH, GH_BRANCH)).get("sha")
                except HTTPError as ex:
                    if ex.code != 404:
                        raise
            body = {"message": "Market Research data cache", "branch": GH_BRANCH, "content": content}
            if _rs_gh["sha"]:
                body["sha"] = _rs_gh["sha"]
            try:
                _rs_gh["sha"] = gh("PUT", "/repos/%s/contents/%s" % (GH_REPO, RS_GH_PATH), body)["content"]["sha"]
                return
            except HTTPError as ex:
                if ex.code in (409, 422) and attempt_no == 0:   # stale sha: look it up again
                    _rs_gh["sha"] = None
                    continue
                raise
    except Exception:
        pass   # best effort: the local file still holds the data
    finally:
        _rs_gh["saving"] = False


def _apply_cache(data):
    """Use cached data even when older than a week; the daily refresh rebuilds old types in the background."""
    for t, v in (data or {}).items():
        if t in RESEARCH_TYPES and v.get("entries") and t not in RS["data"]:
            RS["data"][t] = v
            RS["log"][t] = {"category": RESEARCH_TYPES[t], "kept": len(v["entries"]), "error": None, "from_cache": True}


def load_cache():
    try:
        with open(RS_CACHE_FILE) as f:
            _apply_cache(json.load(f))
    except Exception:
        pass


def _load_cache_gh():
    try:
        j = gh("GET", "/repos/%s/contents/%s?ref=%s" % (GH_REPO, RS_GH_PATH, GH_BRANCH))
        raw = j.get("content") or ""
        if not raw and j.get("sha"):   # files over 1 MB: the contents API leaves content empty
            raw = gh("GET", "/repos/%s/git/blobs/%s" % (GH_REPO, j["sha"]))["content"]
        _rs_gh["sha"] = j.get("sha")
        _apply_cache(json.loads(base64.b64decode(raw).decode("utf-8")))
    except Exception:
        pass


def research_warmup():
    """At start-up: reload saved data (GitHub if the local file is gone), then fetch whatever is missing."""
    if GH_TOKEN and GH_REPO and len(RS["data"]) < len(RESEARCH_TYPES):
        _load_cache_gh()
    request_types(list(RESEARCH_TYPES), front=False)
    outdated = [t for t in RESEARCH_TYPES if t in RS["data"] and RS["data"][t].get("v", 1) < RS_SCHEMA]
    if outdated:   # saved before the export fields existed: refresh once, serving the old data meanwhile
        request_types(outdated, front=False, force=True)


def research_daily():
    """Called from the 6am update: rebuild types older than a week (old data is served until the new is ready)."""
    old = [t for t in RESEARCH_TYPES if time.time() - RS["data"].get(t, {}).get("at", 0) > RS_MAX_AGE
           or RS["data"].get(t, {}).get("v", 1) < RS_SCHEMA]
    if old:
        request_types(old, front=False, force=True)


def rs_worker():
    while True:
        with RS_LOCK:
            if not RS["queue"]:
                RS["thread"], RS["building"] = None, None
                return
            t = RS["queue"].pop(0)
            RS["building"] = t
        build_type(t)


def request_types(types, front=True, force=False):
    """Make sure data for these types is loaded or queued (force: rebuild even if loaded).
    Returns the ones not ready yet."""
    missing = [t for t in types if t not in RS["data"]]
    todo = list(types) if force else missing
    with RS_LOCK:
        for t in (reversed(todo) if front else todo):
            recently_failed = time.time() - RS["failed_at"].get(t, 0) < 300
            if t in RS["queue"] or t == RS["building"] or recently_failed:
                continue
            RS["queue"].insert(0, t) if front else RS["queue"].append(t)
        if RS["queue"] and RS["thread"] is None:
            RS["thread"] = threading.Thread(target=rs_worker, daemon=True)
            RS["thread"].start()
    return missing


def rs_progress():
    return {"ready": sum(t in RS["data"] for t in RESEARCH_TYPES), "total": len(RESEARCH_TYPES),
            "building": RS["building"]}


# ---- matching ----------------------------------------------------------------
def classify(text, table):
    return {k for k, rxs in table.items() if any(rx.search(text or "") for rx in rxs)}


def fmt_num(v):
    return ("%g" % v) if abs(v) < 1e6 else "%.3g" % v


def parse_query(args):
    q = {"num": {}, "types": [], "seeker": None, "guidance": None, "platform": None}
    for key, label, unit, fac, row, w in NUMERIC:
        raw = (args.get(key) or "").strip().replace(",", "")
        if raw:
            try:
                v = float(raw)
            except ValueError:
                raise ValueError("%s must be a number." % label)
            if not (0 < v < 1e7):
                raise ValueError("%s must be a positive number." % label)
            q["num"][key] = v
    t = (args.get("type") or "").strip()
    if t:
        if t not in RESEARCH_TYPES:
            raise ValueError("Unknown system type.")
        q["types"] = [t]
    k = (args.get("seeker") or "").strip()
    if k:
        if k not in SEEKER:
            raise ValueError("Unknown seeker capability.")
        q["seeker"] = k
    g = (args.get("guidance") or "").strip()
    if g:
        if g not in GUIDANCE:
            raise ValueError("Unknown guidance type.")
        q["guidance"] = g
    p = (args.get("platform") or "").strip()
    if p:
        if p not in PLATFORMS:
            raise ValueError("Unknown launch platform.")
        q["platform"] = p
    if not (q["num"] or q["seeker"] or q["guidance"] or q["platform"]):
        raise ValueError("Enter at least one parameter to compare.")
    return q


def wiki_url(title):
    return "https://en.wikipedia.org/wiki/" + quote(title.replace(" ", "_"), safe="()_,.'-")


def rank_matches(q, top=5, offset=0):
    pool = {}
    for t in (q["types"] or list(RESEARCH_TYPES)):
        for e in RS["data"].get(t, {}).get("entries", []):
            pool.setdefault(e["title"], e)
    ranked = []
    for e in pool.values():
        num = den = 0.0
        known = 0
        marks = {}
        for key, label, unit, fac, row, w in NUMERIC:
            if key not in q["num"]:
                continue
            den += w
            vals = e["num"].get(row)
            yours = "%s %s" % (fmt_num(q["num"][key]), unit)
            if not vals:
                marks[row] = {"state": "unknown", "yours": yours}
                continue
            best = min(abs(math.log(v / (q["num"][key] * fac))) for v in vals)
            num += w * max(0.0, 1 - best / math.log(3))
            known += 1
            ratio = math.exp(best)
            marks[row] = {"state": "close" if ratio <= 1.15 else "near" if ratio <= 1.6 else "far", "yours": yours}
        gtext = e["raw"].get("Guidance", "")
        for key, table, row, text, w in (
                ("seeker", SEEKER_RX, "Seeker", gtext, 1.2),
                ("guidance", GUIDANCE_RX, "Guidance", gtext, 0.8),
                ("platform", PLATFORM_RX, "Launch platform", e.get("platform_text", ""), 0.8)):
            if not q[key]:
                continue
            den += w
            have = classify(text, table)
            if not have:
                marks[row] = {"state": "unknown", "yours": q[key]}
                continue
            known += 1
            hit = q[key] in have
            num += w * (1.0 if hit else 0.0)
            marks[row] = {"state": "close" if hit else "far", "yours": q[key]}
        if known == 0 or den == 0:
            continue
        ranked.append((100 * num / den, known, e, marks))
    ranked.sort(key=lambda r: (-r[0], -r[1], r[2]["title"]))
    out = []
    for score, known, e, marks in ranked[offset:offset + top]:
        rows = []
        for label, keys, dim in SPEC_ROWS:
            if label in e["raw"] or label in marks:
                text = e["raw"].get(label) or (e.get("platform_text", "")[:120] if label == "Launch platform" else "") or "Not published"
                rows.append({"label": label, "text": text, **({"match": marks[label]} if label in marks else {})})
        seekers = classify(e["raw"].get("Guidance", ""), SEEKER_RX)
        if "Imaging infrared" in seekers:
            seekers.discard("Infrared")
        if seekers or "Seeker" in marks:
            srow = {"label": "Seeker", "text": ", ".join(sorted(seekers)) or "Not published",
                    **({"match": marks["Seeker"]} if "Seeker" in marks else {})}
            at = next((i for i, r in enumerate(rows) if r["label"] == "Guidance"), len(rows))
            rows.insert(at, srow)
        sub = " · ".join(x for x in (e.get("origin"), e.get("manufacturer")) if x)
        out.append({"title": e["title"], "url": wiki_url(e["title"]), "image": e.get("image", ""),
                    "score": int(round(score)), "sub": sub, "specs": rows,
                    "known": known, "asked": len(q["num"]) + bool(q["seeker"]) + bool(q["guidance"]) + bool(q["platform"])})
    return out, len(pool), len(ranked)


load_cache()
threading.Thread(target=research_warmup, daemon=True).start()


@app.route("/api/research/landscape")
def research_landscape():
    """Every published system of a type (or of all types) with a range and/or a speed, for the two charts."""
    t = (request.args.get("type") or "").strip()
    types = [t] if t in RESEARCH_TYPES else list(RESEARCH_TYPES)
    if request_types(types):
        return jsonify({"state": "loading", "progress": rs_progress()})
    seen, points, total = set(), [], 0
    for tt in types:
        for e in RS["data"][tt]["entries"]:
            if e["title"] in seen:
                continue
            seen.add(e["title"])
            total += 1
            r, s = e["num"].get("Range"), e["num"].get("Speed")
            km = max(r) / 1000 if r else 0
            mach = max(s) / MACH_MS if s else 0
            km = round(km, 2) if 0.1 <= km <= 20000 else None      # plausible values only
            mach = round(mach, 2) if 0.05 <= mach <= 40 else None
            if km is None and mach is None:
                continue
            points.append({"title": e["title"], "url": wiki_url(e["title"]), "km": km, "mach": mach})
    return jsonify({"state": "ready", "type": t if t in RESEARCH_TYPES else None, "points": points, "total": total})


@app.route("/api/research/options")
def research_options():
    return jsonify({"types": list(RESEARCH_TYPES), "seekers": list(SEEKER), "guidance": list(GUIDANCE), "platforms": list(PLATFORMS)})


@app.route("/api/research")
def research_api():
    try:
        q = parse_query(request.args)
    except ValueError as ex:
        return jsonify({"error": str(ex)}), 400
    types = q["types"] or list(RESEARCH_TYPES)
    missing = request_types(types)
    if missing:
        failed = [t for t in missing if RS["log"].get(t, {}).get("error")]
        if failed and len(failed) == len(missing):
            return jsonify({"state": "error", "error": "Could not load public specifications right now (" +
                            RS["log"][failed[0]]["error"] + "). Try again in a few minutes.",
                            "progress": rs_progress()})
        return jsonify({"state": "loading", "progress": rs_progress()})
    try:
        offset = max(0, min(int(request.args.get("offset", 0)), 5000))
    except ValueError:
        offset = 0
    results, considered, total = rank_matches(q, 5, offset)
    return jsonify({"state": "ready", "results": results, "considered": considered,
                    "total": total, "offset": offset})


# ---- Excel export: the matches on screen, one column per system --------------------------------------
# Every cell is published text from the system's Wikipedia infobox (with unit conversions added).
# Nothing is estimated: if the infobox doesn't state it, the cell says so.
try:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
except ImportError:
    Workbook = None
try:
    from openpyxl.drawing.image import Image as XLImage   # needs Pillow
    import PIL  # noqa: F401
except ImportError:
    XLImage = None
PHOTO_W, PHOTO_MAX_H = 260, 180   # pixels in the sheet


def _photo_url(e):
    """The article's photo, only if it is a Wikimedia Commons file (Commons holds freely licensed files only)."""
    u = clean_img(e.get("image"))
    p = urlsplit(u)
    ok = p.hostname in ("upload.wikimedia.org", "thumb.wikimedia.org") and p.path.startswith("/wikipedia/commons/")
    return u if ok else ""


def _sheet_image(data):
    """A photo sized to fit its cell, as PNG or JPEG (the formats Excel shows), or None."""
    if not data or XLImage is None:
        return None
    try:
        from PIL import Image as PILImage
        im = PILImage.open(io.BytesIO(data))
        im.load()
        if im.format not in ("JPEG", "PNG"):
            out = io.BytesIO()
            im.convert("RGBA").save(out, "PNG")
            data = out.getvalue()
        img = XLImage(io.BytesIO(data))
        s = min(PHOTO_W / img.width, PHOTO_MAX_H / img.height, 1.0)
        img.width, img.height = int(img.width * s), int(img.height * s)
        return img
    except Exception:
        return None


def _fetch_photo(url):
    try:
        with urlopen(Request(url, headers={"User-Agent": WIKI_UA}), timeout=8) as r:
            data = r.read(3_000_001)
        return data if 0 < len(data) <= 3_000_000 else None
    except Exception:
        return None

NOT_FOUND = "Information Not Found"
AIR_DEFENCE_TYPES = ("Surface-to-air missile", "Anti-ballistic missile")
EXPORT_ROWS = ["Cruise Speed (m/s and Mach)", "Max Speed (m/s and Mach)", "Flight Altitude (m)",
               "Interception Altitude (for Air Defence systems)", "Propulsion Type", "Warhead Type and Weight",
               "Fuze Type", "Navigation Capabilities onboard the weapon", "GNSS Resiliency Capabilities",
               "GNSS Denied Capabilities", "Seeker Type", "Datalink Type"]
NAV_RX = {k: [re.compile(p, re.I) for p in v] for k, v in {
    "Inertial (INS)": [r"inertial", r"\bINS\b", r"\bIMU\b"],
    "Satellite navigation (GNSS)": [r"\bGPS\b", r"\bGNSS\b", r"GLONASS", r"BeiDou", r"Galileo", r"satellite"],
    "Terrain contour matching": [r"terrain", r"TERCOM"],
    "Scene / map matching": [r"DSMAC", r"scene[- ]match", r"map[- ]?match"],
}.items()}


def _speed_cell(text):
    vals = parse_values(text, "speed")
    if not vals:
        return "Published: " + text
    ms = max(vals)
    return "about %s m/s · Mach %.2f\nPublished: %s" % ("{:,.0f}".format(ms), ms / MACH_MS, text)


def _alt_cell(text):
    vals = parse_values(text, "len")
    if not vals:
        return "Published: " + text
    lo, hi = min(vals), max(vals)
    fig = "{:,.0f} m".format(hi) if hi - lo < 0.5 else "{:,.0f} – {:,.0f} m".format(lo, hi)
    return "%s\nPublished: %s" % (fig, text)


def export_column(e, air_defence):
    raw, extra = e.get("raw", {}), e.get("extra", {})
    gtext = raw.get("Guidance", "")
    speed = raw.get("Speed", "")
    cruise = extra.get("Cruise speed") or "; ".join(
        s.strip() for s in re.split(r"[;,]", speed) if re.search(r"cruis", s, re.I))
    if air_defence:
        flight, intercept = "", extra.get("Ceiling") or extra.get("Flight altitude", "")
    else:
        flight, intercept = extra.get("Flight altitude") or extra.get("Ceiling", ""), None
    wtype, wweight, filling = extra.get("Warhead type", ""), extra.get("Warhead weight", ""), extra.get("Filling", "")
    warhead = "\n".join(x for x in (
        wtype and "Type: " + wtype,
        wweight and wweight != wtype and "Weight: " + wweight,
        filling and filling not in (wtype, wweight) and "Warhead / filling: " + filling) if x)
    nav = [k for k, rxs in NAV_RX.items() if any(rx.search(gtext) for rx in rxs)]
    seekers = classify(gtext, SEEKER_RX)
    if "Imaging infrared" in seekers:
        seekers.discard("Infrared")
    published = "\nPublished guidance: " + gtext if gtext else ""
    datalink = extra.get("Datalink") or (
        "Datalink mentioned (type not stated)" + published
        if re.search(r"data[- ]?link|up-?link|two-way link|mid-?course update", gtext, re.I) else "")
    cells = [
        _speed_cell(cruise) if cruise else "",
        _speed_cell(speed) if speed else "",
        _alt_cell(flight) if flight else "",
        "Not applicable (not an air defence system)" if intercept is None else _alt_cell(intercept) if intercept else "",
        raw.get("Propulsion", ""),
        warhead,
        extra.get("Detonation", ""),
        ", ".join(nav) + published if nav else "",
        "",   # GNSS resiliency: not an infobox field, and never inferred
        "",   # GNSS-denied capability: same
        ", ".join(sorted(seekers)) + published if seekers else "",
        datalink,
    ]
    return [c or NOT_FOUND for c in cells]


def _put(ws, row, col, value, **style):
    c = ws.cell(row=row, column=col, value=value)
    if isinstance(value, str):
        c.data_type = "s"   # published text is never treated as an Excel formula
    for k, v in style.items():
        setattr(c, k, v)
    return c


@app.route("/api/research/export")
def research_export():
    fmt = request.args.get("format", "xlsx")
    if fmt not in ("xlsx", "csv"):
        return jsonify({"error": "Unknown export format."}), 400
    if fmt == "xlsx" and Workbook is None:
        return jsonify({"error": "Excel export needs openpyxl in requirements.txt."}), 503
    try:
        q = parse_query(request.args)
        count = max(1, min(int(request.args.get("count", 5)), 100))
    except ValueError as ex:
        return jsonify({"error": str(ex)}), 400
    types = q["types"] or list(RESEARCH_TYPES)
    if request_types(types):
        return jsonify({"error": "The public data is still loading. Try again in a minute."}), 503
    results, _, _ = rank_matches(q, count, 0)
    if not results:
        return jsonify({"error": "There are no matches to export."}), 404
    entries = {}
    for t in types:
        for e in RS["data"][t]["entries"]:
            entries.setdefault(e["title"], e)
    # Air defence = listed under a surface-to-air or anti-ballistic missile type, within the types searched.
    ad_titles = {e["title"] for t in types if t in AIR_DEFENCE_TYPES for e in RS["data"][t]["entries"]}
    columns = [export_column(entries.get(r["title"], {}), r["title"] in ad_titles) for r in results]
    now = local_now()
    notes = [
        "Exported from GW University · Market Research on %s." % now.strftime("%d %b %Y %H:%M"),
        "Values are published figures from each system's Wikipedia infobox (refreshed daily at 6am) and may be incomplete, rounded or disputed.",
        "m/s and Mach are converted at 340.3 m/s = Mach 1 (sea level). '%s' means the source does not state it." % NOT_FOUND,
        "Wikipedia text is available under CC BY-SA; see each source link for credits.",
        "Photos are freely licensed files from Wikimedia Commons; the author and licence are on each file's page (open the photo on the source article)."]
    name = "market-research-%s.%s" % (now.strftime("%Y%m%d-%H%M"), fmt)
    headers = {"Content-Disposition": 'attachment; filename="%s"' % name, "Cache-Control": "no-store"}

    photos = [_photo_url(r) for r in results]

    if fmt == "csv":   # opens in Google Sheets (File > Import), Excel or Numbers
        safe = lambda v: "'" + v if v[:1] in ("=", "+", "-", "@") else v   # never read as a formula
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Specification"] + ["#%d %s" % (i, r["title"]) for i, r in enumerate(results, start=1)])
        # The only formula we write ourselves: Google Sheets shows the photo from its Wikimedia link.
        w.writerow(["Photo"] + ['=IMAGE("%s")' % u if u and '"' not in u else NOT_FOUND for u in photos])
        for i, label in enumerate(EXPORT_ROWS):
            w.writerow([label] + [safe(c[i]) for c in columns])
        w.writerow(["Source"] + [r["url"] for r in results])
        w.writerow([])
        for line in notes:
            w.writerow([line])
        return Response("\ufeff" + buf.getvalue(), mimetype="text/csv; charset=utf-8", headers=headers)

    wb = Workbook()
    ws = wb.active
    ws.title = "Market research"
    head = Font(bold=True, color="1C1505")
    fill = PatternFill("solid", fgColor="E3B34A")
    wrap = Alignment(wrap_text=True, vertical="top")
    _put(ws, 1, 1, "Specification", font=head, fill=fill, alignment=wrap)
    for i, label in enumerate(["Photo"] + EXPORT_ROWS + ["Source"], start=2):
        _put(ws, i, 1, label, font=Font(bold=True), alignment=wrap)
    with ThreadPoolExecutor(max_workers=4) as pool:   # a few small thumbnails, fetched together
        pics = list(pool.map(lambda u: _fetch_photo(u) if u and XLImage else None, photos))
    photo_h = 0
    for col, (r, cells, pic) in enumerate(zip(results, columns, pics), start=2):
        _put(ws, 1, col, "#%d %s" % (col - 1, r["title"]), font=head, fill=fill, alignment=wrap)
        img = _sheet_image(pic)
        if img:
            img.anchor = ws.cell(row=2, column=col).coordinate
            ws.add_image(img)
            photo_h = max(photo_h, img.height)
        else:
            _put(ws, 2, col, NOT_FOUND, alignment=wrap)
        for i, text in enumerate(cells, start=3):
            _put(ws, i, col, text, alignment=wrap)
        src = _put(ws, len(EXPORT_ROWS) + 3, col, r["url"], alignment=wrap)
        src.hyperlink = r["url"]
        src.font = Font(color="0563C1", underline="single")
        ws.column_dimensions[ws.cell(row=1, column=col).column_letter].width = 42
    ws.column_dimensions["A"].width = 34
    if photo_h:
        ws.row_dimensions[2].height = photo_h * 0.75 + 6   # row height is in points
    note = len(EXPORT_ROWS) + 5
    for k, line in enumerate(notes):
        _put(ws, note + k, 1, line, font=Font(italic=True, color="666666"))
    ws.freeze_panes = "B2"

    buf = io.BytesIO()
    wb.save(buf)
    return Response(buf.getvalue(), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers=headers)


@app.route("/api/research/status")
def research_status():
    return jsonify({"user_agent": WIKI_UA, "progress": rs_progress(), "queue": RS["queue"],
                    "types": {t: {"loaded": t in RS["data"], "entries": len(RS["data"].get(t, {}).get("entries", [])),
                                  "log": RS["log"].get(t)} for t in RESEARCH_TYPES}})


# =============================================================================
# Standards library: free, publicly released standards only.
# Paid standards (ISO, IEEE, SAE, IPC...) are not hosted here. Summaries are
# written for this app; the documents themselves are fetched from the official,
# free sources linked on each card. Revision letters are left out on purpose:
# the official source always shows the current revision.
# =============================================================================
STD_SOURCES = {
    "DoD": {"name": "US DoD · ASSIST", "url": "https://quicksearch.dla.mil/qsSearch.aspx",
            "how": "Search the number in ASSIST Quick Search. Documents cleared for public release download free."},
    "NASA": {"name": "NASA Technical Standards", "url": "https://standards.nasa.gov/",
             "how": "Search the number on the NASA Technical Standards site. Free download."},
}
STD_AREAS = ["Acquisition & systems engineering", "Environment & test", "EMC / E3", "Interfaces & power",
             "Reliability & safety", "Parts & workmanship", "Structures", "Logistics & support", "Marking & data",
             "Modelling & simulation", "Munition & fuze safety"]

# number, source, kind, area, title, what it is for, status note ("" if none)
_STD = [
    ("MIL-STD-810", "DoD", "Standard", "Environment & test",
     "Environmental Engineering Considerations and Laboratory Tests",
     "The reference for tailoring environmental life-cycle profiles and lab tests: vibration, shock, temperature, humidity, altitude, sand and dust, salt fog, gunfire shock and more.", ""),
    ("MIL-HDBK-310", "DoD", "Handbook", "Environment & test",
     "Global Climatic Data for Developing Military Products",
     "Climatic extremes (temperature, humidity, solar, wind, rain) to use when setting design and test requirements.", ""),
    ("MIL-STD-331", "DoD", "Test method", "Munition & fuze safety",
     "Fuze and Fuze Components, Environmental and Performance Tests for",
     "Test methods used to show a fuze survives its environments and still performs, including jolt, jumble and drop tests.", ""),
    ("MIL-STD-1316", "DoD", "Standard", "Munition & fuze safety",
     "Safety Criteria for Fuze Design",
     "Design safety rules for fuzes: independent safety features, arming environments, arming distance and failure-rate goals.", ""),
    ("MIL-STD-1901", "DoD", "Standard", "Munition & fuze safety",
     "Munition Rocket and Missile Motor Ignition System Design, Safety Criteria for",
     "Safety design criteria for rocket and missile motor ignition systems, including in-line and out-of-line ignition.", ""),
    ("MIL-STD-2105", "DoD", "Test method", "Munition & fuze safety",
     "Hazard Assessment Tests for Non-Nuclear Munitions",
     "Insensitive-munition and hazard tests: fast and slow cook-off, bullet and fragment impact, sympathetic reaction, drop.", ""),
    ("MIL-STD-1751", "DoD", "Test method", "Munition & fuze safety",
     "Safety and Performance Tests for the Qualification of Explosives",
     "Tests used to qualify a new explosive (sensitivity, thermal stability, compatibility) before it goes into a munition.", "Cancelled; check which document replaced it on ASSIST."),
    ("MIL-HDBK-1512", "DoD", "Handbook", "Munition & fuze safety",
     "Electroexplosive Subsystems, Electrically Initiated, Design Requirements and Test Methods",
     "Design and test guidance for electrically initiated explosive subsystems such as squibs, initiators and firing circuits.", ""),
    ("MIL-STD-1576", "DoD", "Standard", "Munition & fuze safety",
     "Electroexplosive Subsystem Safety Requirements and Test Methods for Space Systems",
     "Safety requirements and tests for electroexplosive subsystems on launch vehicles and spacecraft.", "Cancelled; check which document replaced it on ASSIST."),
    ("MIL-STD-461", "DoD", "Standard", "EMC / E3",
     "Requirements for the Control of Electromagnetic Interference Characteristics of Subsystems and Equipment",
     "Conducted and radiated emission and susceptibility limits and test methods (CE, CS, RE, RS) for equipment.", ""),
    ("MIL-STD-464", "DoD", "Standard", "EMC / E3",
     "Electromagnetic Environmental Effects Requirements for Systems",
     "System-level E3: lightning, EMP, HERO (ordnance in RF fields), ESD, intra-system compatibility and external RF environments.", ""),
    ("MIL-STD-1553", "DoD", "Standard", "Interfaces & power",
     "Digital Time Division Command/Response Multiplex Data Bus",
     "The classic dual-redundant 1 Mbit/s avionics data bus used between aircraft, stores and subsystems.", ""),
    ("MIL-STD-1760", "DoD", "Standard", "Interfaces & power",
     "Aircraft/Store Electrical Interconnection System",
     "The electrical interface between aircraft and smart stores: connectors, power, discretes, data bus and release consent.", ""),
    ("MIL-STD-8591", "DoD", "Standard", "Interfaces & power",
     "Airborne Stores, Suspension Equipment and Aircraft-Store Interface (Carriage Phase)",
     "Mechanical and environmental design requirements for stores and racks during captive carriage.", ""),
    ("MIL-STD-704", "DoD", "Standard", "Interfaces & power",
     "Aircraft Electric Power Characteristics",
     "Normal, abnormal and emergency limits of aircraft AC and DC power that equipment must tolerate.", ""),
    ("MIL-STD-1275", "DoD", "Standard", "Interfaces & power",
     "Characteristics of 28 Volt DC Input Power to Utilization Equipment in Military Vehicles",
     "Steady-state, surge and spike limits for 28 V DC ground-vehicle power, relevant to vehicle-mounted launchers.", ""),
    ("MIL-STD-1399", "DoD", "Standard", "Interfaces & power",
     "Interface Standard for Shipboard Systems",
     "A multi-section standard for ship interfaces; Section 300 covers shipboard electric power for equipment.", ""),
    ("MIL-STD-882", "DoD", "Standard", "Reliability & safety",
     "System Safety",
     "The DoD process for identifying hazards, assessing risk (severity x probability) and tracking mitigations over the life cycle.", ""),
    ("MIL-HDBK-217", "DoD", "Handbook", "Reliability & safety",
     "Reliability Prediction of Electronic Equipment",
     "Part-count and part-stress failure-rate models for electronics.", "Old and not updated for modern parts, but still widely cited."),
    ("MIL-HDBK-338", "DoD", "Handbook", "Reliability & safety",
     "Electronic Reliability Design Handbook",
     "Broad guidance on reliability engineering: allocation, derating, FMECA, testing and growth.", ""),
    ("MIL-STD-1629", "DoD", "Standard", "Reliability & safety",
     "Procedures for Performing a Failure Mode, Effects and Criticality Analysis",
     "The classic FMECA method and worksheets.", "Cancelled, but still widely referenced in contracts and practice."),
    ("MIL-STD-1472", "DoD", "Standard", "Reliability & safety",
     "Human Engineering",
     "Human-factors design criteria for controls, displays, labelling, maintainability and workspace.", ""),
    ("MIL-STD-1916", "DoD", "Standard", "Reliability & safety",
     "DoD Preferred Methods for Acceptance of Product",
     "Sampling and process-control based methods for accepting delivered product.", ""),
    ("MIL-STD-202", "DoD", "Test method", "Parts & workmanship",
     "Electronic and Electrical Component Parts",
     "Environmental, physical and electrical test methods for components such as resistors, capacitors and relays.", ""),
    ("MIL-STD-883", "DoD", "Test method", "Parts & workmanship",
     "Test Method Standard: Microcircuits",
     "Screening and qualification tests for microcircuits: burn-in, temperature cycling, hermeticity, die shear and more.", ""),
    ("MIL-HDBK-263", "DoD", "Handbook", "Parts & workmanship",
     "Electrostatic Discharge Control Handbook",
     "Guidance on ESD-sensitive items and how to protect them in design, handling and packaging.", "Cancelled, but still widely used as a reference."),
    ("MIL-HDBK-1823", "DoD", "Handbook", "Parts & workmanship",
     "Nondestructive Evaluation System Reliability Assessment",
     "How to measure probability of detection (POD) for NDE methods used on structures and parts.", ""),
    ("MIL-STD-130", "DoD", "Standard", "Marking & data",
     "Identification Marking of U.S. Military Property",
     "Item marking and unique identification (UID / 2D data matrix) rules.", ""),
    ("MIL-STD-129", "DoD", "Standard", "Marking & data",
     "Military Marking for Shipment and Storage",
     "Labels and markings for packages and unit loads, including bar codes and hazardous-material markings.", ""),
    ("MIL-STD-1168", "DoD", "Standard", "Marking & data",
     "Department of Defense Ammunition Lot Numbering and Ammunition Data Card",
     "How ammunition and explosive lots are numbered and documented.", ""),
    ("MIL-STD-31000", "DoD", "Standard", "Marking & data",
     "Technical Data Packages",
     "What goes into a technical data package: drawings, 3D models, specifications and associated lists.", ""),
    ("MIL-STD-3022", "DoD", "Standard", "Modelling & simulation",
     "Documentation of Verification, Validation, and Accreditation (VV&A) for Models and Simulations",
     "Templates for documenting VV&A of models and simulations, useful for flight and seeker simulations.", ""),
    ("MIL-STD-881", "DoD", "Standard", "Acquisition & systems engineering",
     "Work Breakdown Structures for Defense Materiel Items",
     "How to build a programme's work breakdown structure, with standard templates by system type; the basis for cost, schedule and contract reporting.", ""),
    ("MIL-STD-961", "DoD", "Standard", "Acquisition & systems engineering",
     "Defense and Program-Unique Specifications Format and Content",
     "How to write and lay out a specification: section structure, requirement wording and the verification section.", ""),
    ("MIL-STD-962", "DoD", "Standard", "Acquisition & systems engineering",
     "Defense Standards Format and Content",
     "Format and content rules for preparing defense standards and handbooks.", ""),
    ("MIL-HDBK-61", "DoD", "Handbook", "Acquisition & systems engineering",
     "Configuration Management Guidance",
     "Configuration identification, change control, status accounting and audits across the life cycle.", ""),
    ("MIL-HDBK-516", "DoD", "Handbook", "Acquisition & systems engineering",
     "Airworthiness Certification Criteria",
     "The criteria used to show an air system, including stores it carries or releases, is airworthy.", ""),
    ("MIL-HDBK-520", "DoD", "Handbook", "Acquisition & systems engineering",
     "Systems Requirements Document Guidance",
     "How to turn user needs into a well-formed systems requirements document.", ""),
    ("MIL-STD-46855", "DoD", "Standard", "Reliability & safety",
     "Human Engineering Requirements for Military Systems, Equipment, and Facilities",
     "The human-engineering programme a contractor runs during development: analysis, design support and test.", ""),
    ("MIL-STD-1474", "DoD", "Standard", "Environment & test",
     "Noise Limits",
     "Noise limits for military materiel, to protect hearing and keep speech communication possible.", ""),
    ("MIL-STD-889", "DoD", "Standard", "Parts & workmanship",
     "Galvanic Compatibility of Electrically Conductive Materials",
     "Which metal pairs corrode in contact, and how to protect dissimilar-metal joints.", ""),
    ("MIL-STD-11991", "DoD", "Standard", "Parts & workmanship",
     "General Standard for Parts, Materials, and Processes",
     "Requirements for a parts, materials and processes programme: selection, approval, obsolescence and counterfeit avoidance.", ""),
    ("MIL-HDBK-454", "DoD", "Handbook", "Parts & workmanship",
     "General Guidelines for Electronic Equipment",
     "Common design guidelines for electronic equipment: materials, parts, wiring, marking and safety.", ""),
    ("MIL-STD-2073-1", "DoD", "Standard", "Logistics & support",
     "Standard Practice for Military Packaging",
     "How to specify military packaging levels and methods for storage and shipment.", ""),
    ("MIL-STD-1366", "DoD", "Standard", "Logistics & support",
     "Transportability Criteria",
     "Size and weight limits for moving equipment by road, rail, sea and air.", ""),
    ("MIL-STD-1791", "DoD", "Standard", "Logistics & support",
     "Designing for Internal Aerial Delivery in Fixed Wing Aircraft",
     "Design requirements for cargo carried inside military transport aircraft: restraint, loading and dimensions.", ""),
    ("MIL-STD-3034", "DoD", "Standard", "Logistics & support",
     "Reliability-Centered Maintenance (RCM) Process",
     "The RCM method for choosing maintenance tasks from failure modes and their consequences.", ""),
    ("MIL-HDBK-502", "DoD", "Handbook", "Logistics & support",
     "Product Support Analysis",
     "The analyses that shape how a system is supported: maintenance planning, supply support and support equipment.", ""),
    ("NASA-STD-5001", "NASA", "Standard", "Structures",
     "Structural Design and Test Factors of Safety for Spaceflight Hardware",
     "Minimum design and test factors of safety for metallic, composite and other structures.", ""),
    ("NASA-STD-5019", "NASA", "Standard", "Structures",
     "Fracture Control Requirements for Spaceflight Hardware",
     "Fracture control: classifying parts, damage tolerance and NDE requirements.", ""),
    ("NASA-STD-7001", "NASA", "Standard", "Environment & test",
     "Payload Vibroacoustic Test Criteria",
     "Minimum random vibration and acoustic test levels and durations for flight hardware.", ""),
    ("NASA-STD-6016", "NASA", "Standard", "Parts & workmanship",
     "Standard Materials and Processes Requirements for Spacecraft",
     "Materials and processes requirements: flammability, outgassing, stress-corrosion and fluid compatibility.", ""),
    ("NASA-STD-8739.4", "NASA", "Standard", "Parts & workmanship",
     "Workmanship Standard for Crimping, Interconnecting Cables, Harnesses, and Wiring",
     "Workmanship rules for crimps, cables and harness builds, with acceptance criteria.", ""),
]
STANDARDS = [{"num": n, "source": src, "kind": k, "area": a, "title": t, "summary": su, "status": st}
             for n, src, k, a, t, su, st in _STD]
STD_KINDS = ["Standard", "Handbook", "Test method", "Specification", "Guide"]
# Listed with their official source only: no overview and no "Ask this standard" (public-information rule:
# no design, test or qualification guides for weapon subsystems).
STD_REFERENCE_ONLY = {"Munition & fuze safety"}
# First cut: "Ask this standard" only for EMI/EMC and HERO. A standard needs both a place here and its
# search file in standards_index/ (built on a PC from the official PDF).
STD_ASK = {"MIL-STD-461", "MIL-STD-464"}   # EMI/EMC, and E3 including HERO
# Direct links to the free official PDF, where one is stable (otherwise the card links to the source's search).
STD_LINKS = {
    "NASA-STD-5001": "https://standards.nasa.gov/sites/default/files/standards/NASA/B-w/CHANGE-3/3/2022-10-24-NASA-STD-5001B-w-Change-3-Approved.pdf",
    "NASA-STD-5019": "https://standards.nasa.gov/sites/default/files/standards/NASA/w/CHANGE-4/4/NASA-STD-5019A-w-Change-4-Revalidated-2025-09-05.pdf",
    "NASA-STD-6016": "https://standards.nasa.gov/sites/default/files/standards/NASA/C-w/Change-1/1/2023-11-15-NASA-STD-6016C-w-Chg-1-Final_0.pdf",
    "NASA-STD-7001": "https://standards.nasa.gov/system/files/tmp/NASA-STD-7001C%20%20-%20Final%207-14-2026.pdf",
    "NASA-STD-8739.4": "https://standards.nasa.gov/sites/default/files/standards/NASA/A/4/nasa-std-87394a_w_change_4_0.pdf",
    "MIL-HDBK-1512": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=208348",
    "MIL-HDBK-1823": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=206122",
    "MIL-HDBK-217": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=53939",
    "MIL-HDBK-263": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=53974",
    "MIL-HDBK-310": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=202658",
    "MIL-HDBK-338": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=54022",
    "MIL-HDBK-454": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=115984",
    "MIL-HDBK-502": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=201462",
    "MIL-HDBK-516": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=212162",
    "MIL-HDBK-520": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=277134",
    "MIL-HDBK-61": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=202239",
    "MIL-STD-1168": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36105",
    "MIL-STD-11991": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=71353",
    "MIL-STD-1275": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36186",
    "MIL-STD-129": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35520",
    "MIL-STD-130": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35521",
    "MIL-STD-1316": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36251",
    "MIL-STD-1366": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36714",
    "MIL-STD-1399": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36807",
    "MIL-STD-1472": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36903",
    "MIL-STD-1474": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36905",
    "MIL-STD-1553": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36973",
    "MIL-STD-1576": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36991",
    "MIL-STD-1629": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=37027",
    "MIL-STD-1751": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=37111",
    "MIL-STD-1760": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=37120",
    "MIL-STD-1791": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=37147",
    "MIL-STD-1901": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=106144",
    "MIL-STD-1916": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=120287",
    "MIL-STD-202": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35612",
    "MIL-STD-2073-1": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=37232",
    "MIL-STD-2105": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=72079",
    "MIL-STD-3022": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=275961",
    "MIL-STD-3034": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=277649",
    "MIL-STD-31000": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=276980",
    "MIL-STD-331": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35714",
    "MIL-STD-461": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35789",
    "MIL-STD-464": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35794",
    "MIL-STD-46855": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=113938",
    "MIL-STD-704": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35901",
    "MIL-STD-810": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=35978",
    "MIL-STD-8591": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=275030",
    "MIL-STD-881": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36026",
    "MIL-STD-882": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36027",
    "MIL-STD-883": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36028",
    "MIL-STD-889": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36032",
    "MIL-STD-961": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36063",
    "MIL-STD-962": "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=36064",
}

# ---- Built-in overviews -------------------------------------------------------
# Written for this tool as a quick orientation. They describe structure and the
# main obligations, not every requirement. Figures marked "check your revision"
# vary between revisions. For revision-specific, section-referenced checklists,
# upload the official PDF on the standard's page (see "digest" below).
CHK = " (check your revision)"
STD_DETAIL = {
"MIL-STD-810": {
  "purpose": "Make sure equipment is designed and tested for the environments it will really see over its whole life, instead of applying fixed test levels blindly.",
  "applies": "Any military materiel. Part One is the tailoring process for programme managers and engineers, Part Two holds the laboratory test methods, Part Three gives world climatic data.",
  "reqs": [
    "Build a Life Cycle Environmental Profile (LCEP) covering every phase: manufacture, transport, storage, handling, carriage, launch and use.",
    "Tailor: pick only the test methods, procedures and levels the LCEP justifies, and record the rationale for each choice.",
    "Base test levels on measured field data where you have it; use the method's default levels only when nothing better exists.",
    "Produce the planning and reporting documents Part One describes: Environmental Engineering Management Plan, Environmental Test and Evaluation Master Plan, detailed test plans and test reports.",
    "Plan the test sequence deliberately; the standard gives guidance on order and on reusing one test item across tests.",
    "Define pass/fail criteria before testing, with functional checks before, during and after each test.",
    "Meet the general laboratory requirements in Part Two: test tolerances, instrumentation accuracy and test-condition reporting.",
    "Note: the standard imposes nothing on its own; it becomes binding only through the tailored requirements in your contract or specification."],
  "parts_label": "Test methods (Part Two)",
  "parts": [["500", "Low pressure (altitude)"], ["501", "High temperature"], ["502", "Low temperature"], ["503", "Temperature shock"],
            ["504", "Contamination by fluids"], ["505", "Solar radiation"], ["506", "Rain"], ["507", "Humidity"], ["508", "Fungus"],
            ["509", "Salt fog"], ["510", "Sand and dust"], ["511", "Explosive atmosphere"], ["512", "Immersion"], ["513", "Acceleration"],
            ["514", "Vibration"], ["515", "Acoustic noise"], ["516", "Shock"], ["517", "Pyroshock"], ["518", "Acidic atmosphere"],
            ["519", "Gunfire shock"], ["520", "Combined environments"], ["521", "Icing / freezing rain"], ["522", "Ballistic shock"],
            ["523", "Vibro-acoustic / temperature"], ["524", "Freeze / thaw"], ["525", "Time waveform replication"], ["526", "Rail impact"],
            ["527", "Multi-exciter"], ["528", "Mechanical vibration of shipboard equipment"]],
  "parts_note": "Method list as of revision H" + CHK + ".",
  "tip": "For missiles the usual drivers are captive-carry vibration (514), acceleration at launch (513), pyroshock from stage or cover separation (517), gunfire shock near aircraft guns (519) and temperature extremes on the wing (501/502).",
  "related": ["MIL-HDBK-310", "MIL-STD-331", "NASA-HDBK-7005", "MIL-STD-8591"]},

"MIL-HDBK-310": {
  "purpose": "Give the climatic extremes to design and test against, so different programmes use consistent numbers.",
  "applies": "Guidance only (a handbook). Used when writing environmental requirements and choosing MIL-STD-810 test levels.",
  "reqs": [
    "Choose the climatic design types for where the item will be used: hot, basic, cold and severe cold.",
    "Pick a risk level: values are given at frequencies of occurrence (for example the value exceeded only 1% of the time in the worst month).",
    "Use operational (ambient air) values for equipment in use and induced values for storage and transit, where enclosures can get much hotter than the air.",
    "Use the upper-air data for items that fly, since temperature, pressure and density change with altitude.",
    "Record which climatic types and risk levels you chose in the system specification."],
  "parts_label": "Climatic elements covered",
  "parts": [["", "High and low temperature"], ["", "Humidity"], ["", "Solar radiation"], ["", "Rain, snow, hail and ice"],
            ["", "Wind"], ["", "Atmospheric pressure and density"], ["", "Sand and dust"]],
  "tip": "Pair it with MIL-STD-810 Part Three, which summarises the same climatic regions.",
  "related": ["MIL-STD-810"]},

"MIL-STD-461": {
  "purpose": "Limits and test methods for the electromagnetic emissions and susceptibility of individual equipment and subsystems.",
  "applies": "Electronic and electrical equipment. Which requirements apply, and their limits, depend on the platform (aircraft, ship, submarine, ground, space).",
  "reqs": [
    "Use the applicability table to find which requirements apply to your installation and platform.",
    "Meet the emission and susceptibility limits for that platform. Limits and test levels differ by platform and service.",
    "Write an EMI Control Procedure (design approach), an EMI Test Procedure (setup and steps) and an EMI Test Report.",
    "Follow the general test-setup rules: ground plane and bonding, LISNs on power leads, defined cable lengths and layout, ambient checks and calibration.",
    "Use the specified measurement bandwidths, dwell times and scan rates so results are comparable.",
    "Define and monitor susceptibility criteria (what counts as degraded performance) before testing.",
    "Tailor requirements only with justification agreed with the procuring activity."],
  "parts_label": "Requirements",
  "parts": [["CE101", "Conducted emissions, audio frequency, power leads"], ["CE102", "Conducted emissions, RF, power leads"],
            ["CE106", "Conducted emissions, antenna port"], ["CS101", "Conducted susceptibility, power leads"],
            ["CS103-105", "Antenna port susceptibility (intermodulation, rejection, cross-modulation)"],
            ["CS106", "Transients, power leads"], ["CS109", "Structure current"], ["CS114", "Bulk cable injection"],
            ["CS115", "Bulk cable injection, impulse"], ["CS116", "Damped sinusoidal transients"],
            ["CS117", "Lightning induced transients"], ["CS118", "Personnel-borne ESD"],
            ["RE101", "Radiated emissions, magnetic field"], ["RE102", "Radiated emissions, electric field"],
            ["RE103", "Antenna spurious and harmonic outputs"], ["RS101", "Radiated susceptibility, magnetic field"],
            ["RS103", "Radiated susceptibility, electric field"], ["RS105", "Transient electromagnetic field"]],
  "parts_note": "List as of revision G" + CHK + ".",
  "tip": "RE102 and CS114 are the usual troublemakers. Plan cable shielding, connector backshells and power-line filtering from the first board layout.",
  "related": ["MIL-STD-464", "MIL-STD-704", "MIL-STD-1275"]},

"MIL-STD-464": {
  "purpose": "Electromagnetic environmental effects (E3) requirements for the complete system, from intra-system compatibility to lightning, EMP and ordnance safety in RF fields.",
  "applies": "Complete systems (aircraft, ships, ground systems, missiles, space), including the ordnance they carry.",
  "reqs": [
    "Show safety margins for electrically initiated devices: 16.5 dB below the maximum no-fire stimulus for safety-critical functions, 6 dB for others" + CHK + ".",
    "Achieve intra-system compatibility: all subsystems work together, including antenna-to-antenna effects.",
    "Operate in the defined external RF environment for your platform.",
    "Survive direct and indirect lightning effects, and EMP where required.",
    "Make subsystems and equipment meet MIL-STD-461.",
    "Control ESD, and address RF hazards to ordnance (HERO), personnel (HERP) and fuel (HERF).",
    "Provide electrical bonding and grounding, and keep E3 hardness over the life cycle (maintenance and surveillance).",
    "Verify each requirement by test, analysis, inspection or a combination, as the standard's verification sections describe."],
  "parts_label": "Requirement areas",
  "parts": [["", "Margins"], ["", "Intra-system EMC"], ["", "External RF environment"], ["", "Lightning"], ["", "EMP"],
            ["", "Subsystem and equipment EMI"], ["", "Electrostatic charge control"], ["", "RF hazards (HERO, HERP, HERF)"],
            ["", "Life-cycle E3 hardness"], ["", "Electrical bonding"], ["", "External grounds"], ["", "TEMPEST"],
            ["", "Emission control"], ["", "Spectrum supportability"]],
  "tip": "For a missile, HERO is often the critical path. Get the platform's RF environment and the initiator no-fire data early.",
  "related": ["MIL-STD-461", "MIL-HDBK-1512", "MIL-STD-1901"]},

"MIL-STD-1553": {
  "purpose": "Defines the command/response serial data bus used between aircraft, stores and avionics.",
  "applies": "Bus controllers, remote terminals and bus monitors, plus the cabling and coupling between them.",
  "reqs": [
    "1 Mbit/s Manchester II bi-phase signalling on a shielded twisted pair; most systems use a dual-redundant bus (A and B).",
    "One bus controller manages all traffic; up to 31 remote terminal addresses respond to commands.",
    "20-bit words (3-bit sync, 16 data bits, 1 parity bit) of three types: command, status and data. Up to 32 data words per message.",
    "Remote terminals must answer within the specified response time, about 4 to 12 microseconds" + CHK + ".",
    "Support the defined message formats: BC-to-RT, RT-to-BC, RT-to-RT, mode codes and broadcast.",
    "Meet the electrical rules: cable impedance about 70 to 85 ohms, termination at each end of the main bus, transformer- or direct-coupled stubs within their maximum lengths" + CHK + ".",
    "Validate terminals against an RT validation test plan (published by SAE, which is a paid document)."],
  "parts_label": "Key elements",
  "parts": [["BC", "Bus controller"], ["RT", "Remote terminal"], ["BM", "Bus monitor"], ["", "Command, status and data words"],
            ["", "Mode codes"], ["", "Coupling: transformer and direct"]],
  "tip": "For stores, 1553 rides inside the MIL-STD-1760 interface, which adds safety-critical message rules on top.",
  "related": ["MIL-STD-1760", "MIL-STD-704"]},

"MIL-STD-1760": {
  "purpose": "Defines the electrical interface between an aircraft and a smart store such as a missile, so stores and aircraft can be integrated without custom wiring.",
  "applies": "Aircraft stations (Aircraft Station Interface) and mission stores (Mission Store Interface), plus carriage stores in between.",
  "reqs": [
    "Provide the interface signal set: dual 1553 data bus, high-bandwidth (video/RF) and low-bandwidth (audio) lines, and the defined discretes.",
    "Implement the safety discretes: Release Consent must be present before the store acts on safety-critical commands such as arming or release, and the Interlock tells the aircraft the store is connected.",
    "Set the store's bus address from the address discretes.",
    "Provide and accept the defined power: 28 V DC and 115/200 V AC 400 Hz, plus 270 V DC on later revisions" + CHK + ".",
    "Use the specified primary (and where needed auxiliary) connector and pin-out.",
    "Follow the data-bus protocol rules for stores: store control, monitor and description messages, and checksums on safety-critical messages.",
    "Meet the store's defined power-up and power-down behaviour and its electrical characteristics at the interface."],
  "parts_label": "Interface elements",
  "parts": [["", "1553 data bus A and B"], ["", "High-bandwidth signals"], ["", "Low-bandwidth signal"], ["", "Release Consent"],
            ["", "Interlock"], ["", "Address discretes"], ["", "Structure ground"], ["", "28 V DC power"],
            ["", "115/200 V AC power"], ["", "270 V DC power"]],
  "tip": "Safety-critical message handling and Release Consent logic are reviewed closely in store certification. Design and document them early.",
  "related": ["MIL-STD-1553", "MIL-STD-8591", "MIL-STD-704", "MIL-STD-464"]},

"MIL-STD-8591": {
  "purpose": "Design requirements for stores and suspension equipment during captive carriage on aircraft.",
  "applies": "Airborne stores (including missiles), racks and launchers, and the mechanical aircraft-store interface.",
  "reqs": [
    "Design the store to withstand carriage loads: flight manoeuvres, gusts, landing, and catapult launch and arrested landing on carrier aircraft.",
    "Use the standard suspension interface: lug spacing (14 inch and 30 inch) and sway-brace contact areas.",
    "Meet strength, stiffness and fatigue requirements for the carriage life, with the required factors of safety.",
    "Address the captive-carriage environment (vibration, acoustics, temperature), usually through MIL-STD-810 tailoring.",
    "Verify by analysis and ground tests (for example static load and vibration tests) and support flight clearance."],
  "parts_label": "Main topics",
  "parts": [["", "Carriage loads"], ["", "Suspension lugs and sway braces"], ["", "Strength and fatigue"],
            ["", "Carriage environments"], ["", "Verification"]],
  "tip": "Separation (release and launch) analysis is a separate effort from carriage. Plan both in the aircraft integration schedule.",
  "related": ["MIL-STD-1760", "MIL-STD-810"]},

"MIL-STD-704": {
  "purpose": "Defines the quality of electric power an aircraft supplies, so equipment can be designed to work with it.",
  "applies": "Aircraft electrical power systems, and all equipment that uses aircraft power.",
  "reqs": [
    "Operate normally across the steady-state limits: about 22 to 29 V for 28 V DC, about 250 to 280 V for 270 V DC, and about 108 to 118 V for 115 V AC" + CHK + ".",
    "Tolerate the defined normal transients, and the abnormal and emergency conditions, as the standard specifies for each.",
    "Ride through power interruptions during bus transfer (up to about 50 ms)" + CHK + ".",
    "Handle starting conditions and power failure in a defined, safe way.",
    "Verify with the test methods in the MIL-HDBK-704 series (parts 1 to 8, one per power type)."],
  "parts_label": "Power types and conditions",
  "parts": [["", "115/200 V AC, 400 Hz"], ["", "115 V AC variable frequency"], ["", "28 V DC"], ["", "270 V DC"],
            ["", "Normal, abnormal, emergency, transfer, starting, power failure"]],
  "tip": "Check which revision the aircraft was built to; equipment is normally qualified to that revision, not the latest.",
  "related": ["MIL-STD-1760", "MIL-STD-461", "MIL-STD-1275"]},

"MIL-STD-1275": {
  "purpose": "Defines the 28 V DC power in military ground vehicles, so vehicle-mounted equipment survives it.",
  "applies": "Equipment powered from 28 V DC military vehicle systems, for example vehicle-mounted launchers and fire-control equipment.",
  "reqs": [
    "Operate across the steady-state voltage band (roughly 20 to 33 V)" + CHK + ".",
    "Survive the defined surges and spikes, including high-voltage spikes and surges up to around 100 V" + CHK + ".",
    "Handle the different operating modes: normal (generator and battery), generator only, battery only, and engine starting with its voltage dip.",
    "Tolerate ripple and reverse polarity as specified.",
    "Verify with the test methods and waveforms the standard defines."],
  "parts_label": "Operating modes",
  "parts": [["", "Normal operating"], ["", "Generator only"], ["", "Battery only"], ["", "Starting"]],
  "tip": "Generator-only mode, with the battery disconnected, gives the worst surges. Size input protection for it.",
  "related": ["MIL-STD-704", "MIL-STD-461"]},

"MIL-STD-1399": {
  "purpose": "Defines the interfaces ship systems must work with. Section 300 covers shipboard AC electric power.",
  "applies": "Equipment installed on US Navy ships. Each section covers one interface.",
  "reqs": [
    "Identify which power type your equipment uses: Type I (60 Hz), Type II (400 Hz) or Type III (400 Hz precision).",
    "Operate within the voltage and frequency tolerances and transients defined for that type.",
    "Ride through or recover safely from power interruptions.",
    "Keep the equipment's own effect on the ship's power (harmonic current, power factor, inrush) within the limits.",
    "Follow the grounding and insulation requirements, and verify by test."],
  "parts_label": "Section 300 topics",
  "parts": [["", "Power types I, II and III"], ["", "Voltage and frequency tolerances"], ["", "Transients and interruptions"],
            ["", "User equipment limits (harmonics, inrush)"], ["", "Grounding"]],
  "tip": "Other sections cover interfaces such as cooling water and compressed air. Check which sections your ship specification calls up.",
  "related": ["MIL-STD-461", "MIL-STD-810"]},

"MIL-STD-882": {
  "purpose": "The DoD's standard process for finding hazards, judging risk and reducing it to an accepted level across the life cycle.",
  "applies": "All DoD systems, hardware and software. Tasks are selected and tailored in the contract.",
  "reqs": [
    "Follow the eight elements: document the approach; identify hazards; assess risk; identify mitigations; reduce risk; verify and validate the reduction; accept risk; manage risk over the life cycle.",
    "Assess risk using severity (1 Catastrophic, 2 Critical, 3 Marginal, 4 Negligible) and probability (A Frequent to E Improbable, plus F Eliminated).",
    "Map each hazard to a risk level (High, Serious, Medium or Low) and get acceptance from the authority required for that level.",
    "Apply mitigations in order of precedence: eliminate by design, reduce by design changes, add engineered safety features, add warning devices, then signs, procedures, training and PPE.",
    "For software, assign a software criticality index and apply the matching level of rigour in analysis and testing.",
    "Track every hazard in a hazard tracking system until it is closed and accepted.",
    "Perform the analysis tasks the contract calls for (see the task list)."],
  "parts_label": "Common tasks",
  "parts": [["201", "Preliminary hazard list"], ["202", "Preliminary hazard analysis"], ["203", "System requirements hazard analysis"],
            ["204", "Subsystem hazard analysis"], ["205", "System hazard analysis"], ["206", "Operating and support hazard analysis"],
            ["207", "Health hazard analysis"], ["208", "Functional hazard analysis"], ["209", "System-of-systems hazard analysis"],
            ["100-series", "Management tasks"], ["300-series", "Evaluation tasks"], ["400-series", "Verification tasks"]],
  "tip": "Fuze, ignition and software safety reviews all expect hazards traced through this process, so set up the hazard log on day one.",
  "related": ["MIL-STD-1316", "MIL-STD-1629", "MIL-STD-1901"]},

"MIL-HDBK-217": {
  "purpose": "Models for predicting failure rates of electronic parts, used to estimate MTBF and compare design options.",
  "applies": "Guidance only. Used in reliability predictions when a contract or customer calls for it.",
  "reqs": [
    "Use the parts count method early in design (generic failure rates by part type and quality).",
    "Use the parts stress method once the design is detailed: base failure rate multiplied by factors for temperature, electrical stress, quality and environment.",
    "Pick the environment code that matches use; missile-relevant codes include missile launch (ML) and missile free flight (MF).",
    "Sum part failure rates (failures per million hours) to get assembly and system failure rates and MTBF.",
    "State clearly which method, environment and assumptions you used."],
  "parts_label": "Methods and factors",
  "parts": [["", "Parts count method"], ["", "Parts stress method"], ["πT", "Temperature factor"], ["πQ", "Quality factor"],
            ["πE", "Environment factor"], ["πS", "Electrical stress factor"]],
  "tip": "Predictions for modern parts are usually pessimistic. Use them for comparisons, and agree with the customer if another method is acceptable.",
  "related": ["MIL-HDBK-338", "MIL-STD-1629"]},

"MIL-HDBK-338": {
  "purpose": "A broad reference on how to design and manage for reliability.",
  "applies": "Guidance only. Useful when setting up a reliability programme.",
  "reqs": [
    "Set up a reliability programme: requirements, allocation to subsystems, and tracking.",
    "Apply derating to parts so they operate well below their ratings.",
    "Analyse failures with FMECA and fault tree analysis.",
    "Manage thermal design, since temperature drives many failure rates.",
    "Plan reliability testing: growth testing (for example Duane or Crow-AMSAA models), demonstration tests and environmental stress screening.",
    "Run a failure reporting, analysis and corrective action system (FRACAS)."],
  "parts_label": "Main topics",
  "parts": [["", "Programme management"], ["", "Allocation and prediction"], ["", "Derating"], ["", "FMECA and FTA"],
            ["", "Reliability growth and testing"], ["", "Maintainability"], ["", "Software reliability"]],
  "tip": "",
  "related": ["MIL-HDBK-217", "MIL-STD-1629"]},

"MIL-STD-1629": {
  "purpose": "The classic method for failure mode, effects and criticality analysis (FMECA).",
  "applies": "Systems and equipment where a FMECA is called for. The standard is cancelled but still widely cited.",
  "reqs": [
    "Choose the approach: functional (early design) or hardware (part-level, once the design exists).",
    "For each item, list its failure modes, causes, and local, next-higher and end effects, plus how each would be detected.",
    "Classify severity: I Catastrophic, II Critical, III Marginal, IV Minor.",
    "Rank criticality qualitatively (probability levels A to E) or quantitatively (mode criticality from failure rate, mode ratio, effect probability and time).",
    "Summarise in a criticality matrix and list critical items and design actions.",
    "Prepare an FMECA plan and keep the analysis updated as the design changes."],
  "parts_label": "Tasks",
  "parts": [["101", "Failure mode and effects analysis"], ["102", "Criticality analysis"], ["103", "FMECA maintainability information"],
            ["104", "Damage mode and effects analysis"], ["105", "FMECA plan"]],
  "tip": "Many contracts now cite SAE J1739 or other methods; agree the format with the customer before starting.",
  "related": ["MIL-STD-882", "MIL-HDBK-338"]},

"MIL-STD-1472": {
  "purpose": "Human-factors design criteria so equipment can be operated and maintained safely and effectively.",
  "applies": "Military systems with human operators or maintainers, including launchers, ground equipment and software interfaces.",
  "reqs": [
    "Design controls and displays to be easy to read, reach and operate, and arranged in a logical order.",
    "Label controls, connectors and hazards clearly and consistently.",
    "Fit the range of users, using the anthropometric data provided, including users wearing protective clothing and gloves.",
    "Design for maintenance: access, handles, lifting weight limits, connector keying and error-proofing.",
    "Control workplace environment factors: noise, lighting and temperature.",
    "Design software user interfaces for clarity, feedback and error prevention."],
  "parts_label": "Main topics",
  "parts": [["", "Controls and displays"], ["", "Labelling"], ["", "Anthropometry"], ["", "Workspace"], ["", "Environment"],
            ["", "Maintainability"], ["", "User-computer interface"], ["", "Hazards and safety"]],
  "tip": "",
  "related": ["MIL-STD-882"]},

"MIL-STD-1916": {
  "purpose": "The DoD's preferred methods for accepting delivered product, favouring process control over inspecting quality in.",
  "applies": "Product acceptance when a contract invokes it. It replaced older sampling standards such as MIL-STD-105.",
  "reqs": [
    "Prefer a prevention-based quality system with process control. The contractor may propose this instead of sampling.",
    "When sampling, use the verification level (VL) and code letter set by the contract.",
    "Use the zero-acceptance-number plans: a lot is accepted only if no nonconformances are found in the sample.",
    "Choose attribute, variables or continuous sampling plans as appropriate.",
    "Apply the switching rules between normal, tightened and reduced inspection."],
  "parts_label": "Plan types",
  "parts": [["", "Attribute sampling"], ["", "Variables sampling"], ["", "Continuous sampling"], ["", "Process-control alternative"]],
  "tip": "",
  "related": []},

"MIL-STD-202": {
  "purpose": "Standard test methods for electronic and electrical component parts, used by part specifications.",
  "applies": "Components such as resistors, capacitors, relays, switches and connectors. A part specification states which method and test condition apply.",
  "reqs": [
    "Run the methods that the part specification (for example a MIL-PRF document) calls up, at the stated test condition letters.",
    "Follow each method's setup, measurement and failure criteria.",
    "Record and report results as the method requires."],
  "parts_label": "Method groups (examples)",
  "parts": [["100s", "Environmental: salt atmosphere, humidity, moisture resistance, thermal shock, life, seal"],
            ["200s", "Physical: vibration, shock, solderability, resistance to soldering heat, terminal strength, resistance to solvents"],
            ["300s", "Electrical: dielectric withstanding voltage, insulation resistance, DC resistance, capacitance, contact resistance"]],
  "tip": "",
  "related": ["MIL-STD-883", "MIL-STD-810"]},

"MIL-STD-883": {
  "purpose": "Test methods and procedures for microcircuits, including screening and qualification flows.",
  "applies": "Microcircuits (integrated circuits and hybrids) for military and space use.",
  "reqs": [
    "Screen parts per Method 5004 (for example burn-in, temperature cycling, hermeticity, visual inspection), to the class required.",
    "Qualify and run quality conformance inspection per Method 5005.",
    "Mark a device as \"compliant to MIL-STD-883\" only if all the applicable provisions are met; the standard defines what that claim requires.",
    "Apply the individual test methods as called up by the device specification."],
  "parts_label": "Method groups (examples)",
  "parts": [["1000s", "Environmental: temperature cycling (1010), thermal shock (1011), seal (1014), burn-in (1015), total dose (1019)"],
            ["2000s", "Mechanical: constant acceleration (2001), shock (2002), vibration (2007), internal visual (2010), bond strength (2011), radiography (2012), die shear (2019), PIND (2020)"],
            ["3000s", "Electrical (digital), including ESD classification (3015)"], ["4000s", "Electrical (linear)"],
            ["5000s", "Test procedures: screening (5004), qualification and QCI (5005)"]],
  "tip": "",
  "related": ["MIL-STD-202", "MIL-HDBK-263"]},

"MIL-HDBK-263": {
  "purpose": "Guidance on protecting electrostatic-discharge-sensitive parts and assemblies.",
  "applies": "Guidance only. Used with an ESD control programme for design, manufacture, handling and packaging.",
  "reqs": [
    "Classify each part's ESD sensitivity (human body model classes).",
    "Add protection in the circuit design where practical.",
    "Handle sensitive items only in ESD-protected areas, with personnel grounding (wrist straps, footwear) and grounded work surfaces.",
    "Package and mark ESD-sensitive items with static-shielding materials and labels.",
    "Train staff and audit the ESD programme regularly."],
  "parts_label": "Main topics",
  "parts": [["", "Sensitivity classification"], ["", "Design protection"], ["", "Protected areas and grounding"],
            ["", "Packaging and marking"], ["", "Training and audits"]],
  "tip": "Most contracts now point to ANSI/ESD S20.20 for the control programme (a paid document); this handbook is a free background reference.",
  "related": ["MIL-STD-883", "NASA-STD-8739.4"]},

"MIL-HDBK-1823": {
  "purpose": "How to measure how reliably an inspection method finds flaws (probability of detection, POD).",
  "applies": "Guidance for qualifying non-destructive evaluation (NDE) methods such as eddy current, ultrasonic and penetrant inspection.",
  "reqs": [
    "Design the POD experiment: enough specimens with real or realistic flaws across a range of sizes, and representative inspectors and conditions.",
    "Choose the analysis: hit/miss data, or signal response (â versus a) data.",
    "Report a90/95: the flaw size found 90% of the time with 95% confidence.",
    "Check that the model fits the data and document limits of validity."],
  "parts_label": "Main topics",
  "parts": [["", "Experiment design"], ["", "Hit/miss analysis"], ["", "Signal response (â vs a) analysis"], ["", "a90/95 reporting"]],
  "tip": "The a90/95 flaw size feeds damage-tolerance analysis such as NASA-STD-5019.",
  "related": ["NASA-STD-5019"]},

"MIL-STD-130": {
  "purpose": "Rules for marking military items so they can be identified and tracked, including unique item identification (IUID).",
  "applies": "Items delivered to the DoD that require identification marking.",
  "reqs": [
    "Mark items with their identification: part number, enterprise identifier (for example CAGE code) and serial number where needed.",
    "For items needing IUID, encode the unique item identifier (UII) in a 2D Data Matrix (ECC 200) using the specified syntax and semantics.",
    "Verify the mark's print quality meets the minimum grade.",
    "Make the mark permanent for the item's life and place it where it can be read in service.",
    "Register UIIs in the DoD IUID Registry when the contract requires (DFARS 252.211-7003)."],
  "parts_label": "Main topics",
  "parts": [["", "Identification data"], ["", "IUID and the UII"], ["", "2D Data Matrix marking"], ["", "Mark quality"],
            ["", "Placement and permanence"]],
  "tip": "",
  "related": ["MIL-STD-129", "MIL-STD-31000"]},

"MIL-STD-129": {
  "purpose": "How to mark packages and loads for shipment and storage in the DoD supply system.",
  "applies": "Shipping containers, unit packs and unit loads delivered to the DoD.",
  "reqs": [
    "Apply identification marking: NSN, CAGE, part number, quantity and contract number.",
    "Apply the Military Shipping Label with its linear and 2D bar codes.",
    "Add passive RFID tags where the contract requires.",
    "Apply hazardous material and ammunition markings where they apply.",
    "Follow the placement, size and durability rules for markings."],
  "parts_label": "Main topics",
  "parts": [["", "Identification marking"], ["", "Military Shipping Label"], ["", "Bar codes"], ["", "RFID"],
            ["", "Hazardous material marking"], ["", "Ammunition marking"]],
  "tip": "",
  "related": ["MIL-STD-130", "MIL-STD-1168"]},

"MIL-STD-1168": {
  "purpose": "How ammunition and explosive lots are numbered and documented, so every lot can be traced.",
  "applies": "Ammunition, missiles and explosive components and their lots.",
  "reqs": [
    "Assign lot numbers in the standard format (manufacturer code, date of manufacture and sequence elements).",
    "Keep components in a lot homogeneous: made under the same conditions from the same material lots.",
    "Prepare an Ammunition Data Card for each lot, listing component lots and key data.",
    "Keep traceability through rework and renovation, following the numbering rules for those lots."],
  "parts_label": "Main topics",
  "parts": [["", "Lot number format"], ["", "Lot homogeneity"], ["", "Ammunition Data Card"], ["", "Rework lots"]],
  "tip": "",
  "related": ["MIL-STD-129"]},

"MIL-STD-31000": {
  "purpose": "Defines what goes into a technical data package (TDP) delivered to the DoD.",
  "applies": "TDPs for items the DoD buys, from concept to production.",
  "reqs": [
    "Use the TDP option selection worksheet to agree with the customer exactly what the TDP will contain.",
    "Deliver the agreed level of drawing or model data: conceptual, developmental or production.",
    "Provide 2D drawings, 3D models (model-based definition) or both, as agreed.",
    "Include associated lists, specifications, quality assurance provisions and packaging data as required.",
    "Control revisions and keep the data consistent."],
  "parts_label": "TDP elements",
  "parts": [["", "Drawings and 3D models"], ["", "Associated lists"], ["", "Specifications"], ["", "Quality assurance provisions"],
            ["", "Packaging data"], ["", "Option selection worksheet"]],
  "tip": "3D model annotation practice usually points to ASME Y14.41 (a paid standard).",
  "related": ["MIL-STD-130"]},

"MIL-STD-3022": {
  "purpose": "Templates for documenting verification, validation and accreditation (VV&A) of models and simulations.",
  "applies": "Models and simulations used to support decisions, such as 6-DOF flight simulations, seeker models and lethality models.",
  "reqs": [
    "Produce the four core documents: Accreditation Plan, V&V Plan, V&V Report and Accreditation Report.",
    "State the intended use of the model and the questions it must answer.",
    "Define acceptability criteria that show the model is good enough for that use.",
    "Describe the model's capabilities, limitations and input data, and how they were checked.",
    "Record the evidence from verification and validation activities and the accreditation decision."],
  "parts_label": "Documents",
  "parts": [["", "Accreditation Plan"], ["", "V&V Plan"], ["", "V&V Report"], ["", "Accreditation Report"]],
  "tip": "Write the intended use and acceptability criteria first; everything else hangs off them.",
  "related": []},

"NASA-STD-5001": {
  "purpose": "Minimum factors of safety for designing and testing spaceflight structures.",
  "applies": "Spaceflight hardware structures; also a common reference for missile structures.",
  "reqs": [
    "For metallic structures verified by test, design to at least 1.25 on yield and 1.4 on ultimate" + CHK + ".",
    "Use higher factors when structure is not tested (\"no-test\" approach), for example about 2.0 on ultimate for metals" + CHK + ".",
    "Apply the specific factors for composites, bonded joints, glass and other brittle materials, and fasteners.",
    "Match the test approach (prototype or protoflight) with the test factors the standard gives.",
    "Treat pressure vessels and pressurised lines under their own standards."],
  "parts_label": "Main topics",
  "parts": [["", "Design factors of safety"], ["", "Test factors"], ["", "Metallic structures"], ["", "Composite and bonded structures"],
            ["", "Glass and brittle materials"], ["", "Fasteners"]],
  "tip": "",
  "related": ["NASA-STD-5019", "ECSS-E-ST-32"]},

"NASA-STD-5019": {
  "purpose": "Fracture control: making sure cracks and flaws cannot cause a catastrophic failure.",
  "applies": "Spaceflight hardware structures, pressure vessels and rotating machinery.",
  "reqs": [
    "Classify every part as fracture critical or not.",
    "For parts that are not fracture critical, justify the class: low released mass, contained, fail-safe or non-hazardous leak.",
    "For fracture-critical parts, show safe life by analysis or test, with a factor on service life (commonly 4)" + CHK + ".",
    "Inspect fracture-critical parts with NDE that can find the assumed initial flaw size.",
    "Keep traceability and records for fracture-critical parts, and write a fracture control plan."],
  "parts_label": "Main topics",
  "parts": [["", "Part classification"], ["", "Damage-tolerant (safe-life) analysis"], ["", "NDE requirements"],
            ["", "Traceability"], ["", "Fracture control plan"]],
  "tip": "",
  "related": ["NASA-STD-5001", "MIL-HDBK-1823"]},

"NASA-STD-7001": {
  "purpose": "Minimum random vibration and acoustic test levels for flight hardware.",
  "applies": "Flight hardware vibration and acoustic testing.",
  "reqs": [
    "Derive the maximum expected flight environment, then set acceptance levels at or above it.",
    "Set qualification levels at acceptance plus 3 dB, with longer test durations" + CHK + ".",
    "Use protoflight testing (qualification levels with acceptance durations) where flight hardware is also the qualification unit.",
    "Never test below the minimum workmanship random-vibration level the standard defines.",
    "Apply the specified test tolerances and control methods."],
  "parts_label": "Main topics",
  "parts": [["", "Random vibration"], ["", "Acoustics"], ["", "Acceptance, qualification and protoflight"],
            ["", "Workmanship minimum levels"]],
  "tip": "",
  "related": ["NASA-HDBK-7005", "NASA-STD-7003", "MIL-STD-810"]},

"NASA-STD-6016": {
  "purpose": "Materials and processes requirements for spacecraft hardware.",
  "applies": "Materials and processes used in spacecraft and flight hardware.",
  "reqs": [
    "Meet flammability requirements, tested per NASA-STD-6001.",
    "Meet vacuum outgassing limits: total mass loss 1.0% or less and collected volatile condensable material 0.10% or less, per ASTM E595" + CHK + ".",
    "Use alloys with high resistance to stress corrosion cracking (rated per MSFC-STD-3029), or justify others.",
    "Check fluid compatibility and hydrogen embrittlement where relevant.",
    "Keep a materials identification and usage list, and get approval for any non-compliant use."],
  "parts_label": "Main topics",
  "parts": [["", "Flammability"], ["", "Toxic offgassing"], ["", "Vacuum outgassing"], ["", "Stress corrosion cracking"],
            ["", "Fluid compatibility"], ["", "Materials usage lists and approvals"]],
  "tip": "",
  "related": ["NASA-STD-5019", "NASA-STD-8739.4"]},

"NASA-STD-8739.4": {
  "purpose": "Workmanship requirements for crimped connections, cables, harnesses and wiring.",
  "applies": "Building and inspecting cable and harness assemblies for flight and critical hardware.",
  "reqs": [
    "Use calibrated crimp tools and check them with pull (tensile) tests as the standard requires.",
    "Prepare conductors without nicked or cut strands, and with the right insulation gap at the crimp.",
    "Meet the rules for harness layout, ties and lacing, bend radius, and shield terminations.",
    "Inspect to the stated acceptance criteria, with magnification where specified.",
    "Use trained and certified operators and inspectors, and control ESD during work."],
  "parts_label": "Main topics",
  "parts": [["", "Crimping"], ["", "Cable and harness assembly"], ["", "Shield terminations"], ["", "Inspection criteria"],
            ["", "Training and certification"]],
  "tip": "",
  "related": ["MIL-HDBK-263", "NASA-STD-6016"]},
}


def std_key(num):
    return re.sub(r"[^A-Z0-9.]", "", (num or "").upper())


def std_all():
    ensure_loaded()
    rows = [dict(s, custom=False) for s in STANDARDS] + [dict(s, custom=True) for s in store["standards"]]
    for s in rows:
        k = std_key(s["num"])
        s["note"] = store["std_notes"].get(k, "")
        s["reference_only"] = s["area"] in STD_REFERENCE_ONLY
        s["has_overview"] = s["num"] in STD_DETAIL and not s["reference_only"]
        s["has_search"] = s["num"] in STD_ASK and not s["reference_only"] and std_has_search(s["num"])
        if not s["custom"] and s["num"] in STD_LINKS:
            s["link"] = STD_LINKS[s["num"]]
    return rows


def std_text(d, k, limit, required=False):
    v = re.sub(r"\s+", " ", str(d.get(k) or "")).strip()
    if required and not v:
        raise ValueError("Please fill in " + k + ".")
    if len(v) > limit:
        raise ValueError("%s is too long (max %d characters)." % (k.capitalize(), limit))
    return v


@app.route("/api/standards", methods=["GET"])
def standards_api():
    return jsonify({"standards": std_all(), "areas": STD_AREAS, "kinds": STD_KINDS, "sources": STD_SOURCES,
                    "persistent": bool(GH_TOKEN and GH_REPO)})


@app.route("/api/standards", methods=["POST"])
def standards_edit():
    # JSON only: a plain HTML form on another site cannot send this content type without a CORS check.
    if not request.is_json:
        return jsonify({"error": "Send JSON."}), 415
    d = request.get_json(silent=True) or {}
    ensure_loaded()
    try:
        act = d.get("action")
        if act == "note":
            key = std_key(d.get("num"))
            if not key or key not in {std_key(s["num"]) for s in std_all()}:
                raise ValueError("Unknown standard.")
            note = str(d.get("note") or "").strip()
            if len(note) > 1500:
                raise ValueError("Notes are limited to 1500 characters.")
            with store_lock:
                if note:
                    store["std_notes"][key] = note
                else:
                    store["std_notes"].pop(key, None)
            msg = "Standards note " + key
        elif act == "add":
            num = std_text(d, "num", 40, True)
            if std_key(num) in {std_key(s["num"]) for s in std_all()}:
                raise ValueError(num + " is already in the library.")
            area = std_text(d, "area", 40, True)
            if area not in STD_AREAS:
                raise ValueError("Pick an area from the list.")
            kind = std_text(d, "kind", 20) or "Standard"
            if kind not in STD_KINDS:
                raise ValueError("Pick a type from the list.")
            link = std_text(d, "link", 300)
            if link and not re.match(r"^https://[^\s/]+\.[^\s]+$", link):
                raise ValueError("The link must be a full https:// address.")
            entry = {"num": num, "source": std_text(d, "source", 40) or "Other", "kind": kind, "area": area,
                     "title": std_text(d, "title", 200, True), "summary": std_text(d, "summary", 400),
                     "status": "", "link": link, "added": time.time()}
            with store_lock:
                if len(store["standards"]) >= 500:
                    raise ValueError("The library is full (500 added standards).")
                store["standards"].append(entry)
            msg = "Standards: add " + num
        elif act == "delete":
            key = std_key(d.get("num"))
            with store_lock:
                before = len(store["standards"])
                store["standards"] = [s for s in store["standards"] if std_key(s["num"]) != key]
                if len(store["standards"]) == before:
                    raise ValueError("Only standards added by your team can be removed.")
                store["std_notes"].pop(key, None)
            msg = "Standards: remove " + key
        else:
            raise ValueError("Unknown action.")
    except ValueError as ex:
        return jsonify({"error": str(ex)}), 400
    store_save("standards", msg)
    return jsonify({"ok": True, "storage": state["store_note"], "standards": std_all()})


# ---- "Ask this standard": keyword search inside the official document ---------
# Each searchable standard has a prebuilt file in standards_index/ (made from the official PDF on a PC:
# the text of every page, split into passages with their section and page). A question is matched to
# passages by keywords (BM25 ranking), and the best passages are shown word for word with page numbers.
# Nothing is generated or paraphrased, and nothing here calls a paid service.
import gzip
from collections import Counter, OrderedDict

STD_INDEX_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "standards_index")
_STOP = set("""a an and are as at be been but by can could do does for from had has have how i if in into is it its
me my no not of on or our should so than that the their them then there these they this to us was we were what when
where which who why will with would you your about any all also more most must need needs shall such tell
standard document please give show explain say says find requirement requirements required comply purpose""".split())
_TOKEN = re.compile(r"[a-z0-9]+(?:[.\-][a-z0-9]+)*")
_idx_cache = OrderedDict()
_idx_lock = threading.Lock()


def _stem(w):
    if w[0].isdigit():
        return w
    for suf, rep, n in (("ies", "y", 5), ("ing", "", 6), ("ed", "", 5), ("es", "", 5), ("s", "", 4)):
        if len(w) >= n and w.endswith(suf) and not w.endswith("ss"):
            return w[: -len(suf)] + rep
    return w


def _terms(text):
    return [_stem(w) for w in _TOKEN.findall(text.lower()) if w not in _STOP and len(w) > 1]


def std_index_path(num):
    return os.path.join(STD_INDEX_DIR, std_key(num) + ".json.gz")


def std_has_search(num):
    return os.path.exists(std_index_path(num))


def std_index(num):
    """Load (and keep a few of) the passage indexes; each is a few MB of text at most."""
    key = std_key(num)
    with _idx_lock:
        if key in _idx_cache:
            _idx_cache.move_to_end(key)
            return _idx_cache[key]
    with gzip.open(std_index_path(num), "rt", encoding="utf-8") as f:
        meta = json.load(f)
    docs, df = [], Counter()
    for c in meta["chunks"]:   # [page, section, text] or [page, section, text, "Method 509.7 Salt Fog"]
        page, sec, text = c[:3]
        ctx = c[3] if len(c) > 3 else ""
        tf = Counter(_terms(text + " " + ctx))
        docs.append((page, sec, text, tf, sum(tf.values()) or 1, ctx))
        df.update(tf.keys())
    avgdl = sum(d[4] for d in docs) / max(1, len(docs))
    idx = {"meta": {k: v for k, v in meta.items() if k != "chunks"}, "docs": docs, "df": df, "avgdl": avgdl}
    with _idx_lock:
        _idx_cache[key] = idx
        while len(_idx_cache) > 4:
            _idx_cache.popitem(last=False)
    return idx


def _snippet(text, words, size=900):
    if len(text) <= size:
        return text
    low = text.lower()
    hits = [low.find(w) for w in words if low.find(w) >= 0]
    start = max(0, min(hits) - 200) if hits else 0
    start = text.rfind(" ", 0, start) + 1 if start else 0
    out = text[start:start + size]
    out = out[: out.rfind(" ")] if len(out) == size and " " in out else out
    return ("… " if start else "") + out + (" …" if start + len(out) < len(text) else "")


def std_search(num, question, top=5):
    idx = std_index(num)
    q = list(dict.fromkeys(_terms(question)))
    if not q:
        return idx, [], [], []
    n, k1, b = len(idx["docs"]), 1.2, 0.75
    qlow = question.lower()
    words = [w for w in dict.fromkeys(_TOKEN.findall(qlow)) if w not in _STOP and len(w) > 2]   # for highlighting
    bigrams = [a + " " + c for a, c in zip(_TOKEN.findall(qlow), _TOKEN.findall(qlow)[1:])
               if a not in _STOP and c not in _STOP]
    scored = []
    for i, (page, sec, text, tf, dl, ctx) in enumerate(idx["docs"]):
        s = 0.0
        for t in q:
            f = tf.get(t)
            if f:
                idf = math.log(1 + (n - idx["df"][t] + 0.5) / (idx["df"][t] + 0.5))
                s += idf * f * (k1 + 1) / (f + k1 * (1 - b + b * dl / idx["avgdl"]))
        if not s:
            continue
        low = text.lower()
        s *= 1 + 0.25 * sum(1 for bg in bigrams if bg in low)        # the words appear together
        s *= 1.3 if re.search(r"\bshall\b", low) else 1.0             # states a requirement
        if re.match(r"3(\.|$)", sec or "") and "defin" not in question.lower():
            s *= 0.6   # section 3 of a MIL-STD is its definitions: short, word-dense, rarely the answer
        if re.search(r"\.{6,}|(?:\s\d+\s*){12,}", text):              # contents pages and number tables
            s *= 0.35
        scored.append((s, i))
    scored.sort(reverse=True)
    out, used, ids = [], set(), []
    for s, i in scored:
        page, sec, text, tf, dl, ctx = idx["docs"][i]
        if (page, sec) in used:
            continue
        used.add((page, sec))
        ids.append(i)
        out.append({"page": page, "section": sec, "context": ctx, "text": _snippet(text, words), "score": round(s, 2)})
        if len(out) >= top:
            break
    return idx, out, words, direct_answer(idx, [i for _, i in scored[:12]], q, question)


# ---- a direct answer: the 1-3 sentences of the top passages that best answer the question ----
# Free and exact: every sentence is the standard's own wording, cited with its section and page.
_SENT_SPLIT = re.compile(r"(?<=[.;:])\s+(?=[A-Z(\"“]|(?:[A-Z]\.)?\d{1,3}(?:\.\d{1,3})+\.?\s+[A-Z])")
_HEADING = re.compile(r"^(?:[A-Z]\.)?\d{1,3}(?:\.\d{1,3}){0,5}\.?\s+[^.]{2,90}\.\s+(?=[A-Z])")
_ANSWER_SYNONYMS = {
    "apply": ("applicable", "applicability"), "frequency": ("hz", "khz", "mhz", "ghz"),
    "far": ("distance", "cm", "meter", "metre"), "distance": ("cm", "meter", "metre"),
    "long": ("duration", "minute", "hour", "second"), "limit": ("exceed", "maximum"),
}
_QUANT = re.compile(r"\b(how (long|many|much|far|often|high|low)|limit|level|minimum|maximum|frequency|duration|value|range|"
                    r"temperature|voltage|current|distance|time|rate)\b", re.I)


def direct_answer(idx, ids, q, question):
    """Score every sentence of the best passages against the question and keep the 1-3 that answer it.
    A sentence counts for how many of the question's (rarer) words it contains, more if it states a
    requirement ("shall"), holds a figure when the question asks for one, or is a definition when
    the question asks what something is."""
    n, df = len(idx["docs"]), idx["df"]
    idf = {t: math.log(1 + (n - df[t] + 0.5) / (df[t] + 0.5)) for t in q}
    total = sum(idf.values()) or 1
    want_num = bool(_QUANT.search(question))
    want_def = (bool(re.match(r"\s*(what\s+(is|are|does)|define|meaning\s+of)\b", question, re.I))
                and not re.search(r"\b(requirements?|required|need|must|shall|how|limits?|test(ing)?)\b", question, re.I))
    qwords = [w for w in _TOKEN.findall(question.lower()) if w not in _STOP]
    phrases = [a + " " + b for a, b in zip(qwords, qwords[1:])]   # e.g. "hero safe": the heading should say exactly that
    # words a standard uses for the same idea ("apply" / "applicable", a frequency written as kHz or MHz)
    extra = {}
    for t in q:
        for alt in _ANSWER_SYNONYMS.get(t, ()):
            extra[alt] = idf.get(t, 1.0) * 0.8
    cands = []
    for rank, i in enumerate(ids):
        page, sec, text, tf, dl, ctx = idx["docs"][i]
        heading, first = "", False   # the latest heading in this passage: its words count for the sentences under it
        for sent in _SENT_SPLIT.split(text):
            sent = sent.strip()
            m = _HEADING.match(sent)
            if m:                                            # "5.9.3 Title. First sentence..." -> keep the title as context
                heading, sent, first = m.group(0), sent[m.end():], True
            elif re.match(r"^(?:[A-Z]\.)?\d{1,3}(?:\.\d{1,3}){0,5}\.?\s", sent) and len(sent.split()) <= 12:
                heading, first = sent, True                  # a heading on its own: context only, never an answer
                continue
            lead, first = first, False                       # the first sentence under a heading (a definition's body)
            letters = sum(c.isalpha() for c in sent)
            if not (40 <= len(sent) <= 480) or letters < 0.6 * len(sent) or re.match(r"(FIGURE|TABLE)\b", sent):
                continue
            terms = set(_terms(sent + " " + heading))
            hit = sum(idf[t] for t in q if t in terms) + sum(w for t, w in extra.items() if t in terms and t not in q)
            if not hit:
                continue
            s = min(1.0, hit / total)                        # share of the question it covers
            hl = re.sub(r"\s+", " ", heading.lower())
            if any(re.search(r"(?<![a-z])" + re.escape(ph) + r"(?![a-z])", hl) for ph in phrases):
                s *= 1.3                                     # the heading names exactly what was asked
            s *= 0.8 if ctx.lower().startswith("appendix") else 1.0   # appendices explain; the body states the requirement
            s *= 1.35 if re.search(r"\b(shall|must)\b", sent, re.I) else 1.0
            if want_num and re.search(r"\d", sent):
                s *= 1.3
            if want_def and re.match(r"3(\.|$)", sec or ""):
                s *= 1.5 * (1.4 if lead else 1.0)
            if not want_def and re.match(r"3(\.|$)", sec or ""):
                s *= 0.7
            s *= 1 / (1 + 0.08 * rank)                       # passages ranked higher count a little more
            s *= 1 / (1 + max(0, len(sent) - 260) / 400)     # prefer sentences short enough to read at a glance
            cands.append((s, sent, page, sec, ctx))
    cands.sort(key=lambda c: -c[0])
    out, seen = [], set()
    for s, sent, page, sec, ctx in cands:
        key = re.sub(r"[^a-z]", "", sent.lower())[:90]
        if key in seen or (out and s < 0.6 * out[0]["score"]):
            continue
        seen.add(key)
        out.append({"text": sent, "page": page, "section": sec, "context": ctx, "score": round(s, 3)})
        if len(out) >= 3:
            break
    return out


def std_find(num):
    key = std_key(num)
    return key, next((s for s in std_all() if std_key(s["num"]) == key), None)


@app.route("/api/standards/detail")
def standards_detail():
    key, s = std_find(request.args.get("num"))
    if not s:
        return jsonify({"error": "Unknown standard."}), 404
    ov = STD_DETAIL.get(s["num"])
    if ov:   # only link to standards that are in the library
        known = {std_key(x["num"]) for x in std_all()}
        ov = dict(ov, related=[r for r in ov.get("related", []) if std_key(r) in known])
    meta = None
    if s.get("has_search"):
        try:
            meta = std_index(s["num"])["meta"]
        except Exception:
            meta = None
    return jsonify({"standard": s, "overview": ov, "search": meta})


@app.route("/api/standards/ask")
def standards_ask():
    key, s = std_find(request.args.get("num"))
    if not s:
        return jsonify({"error": "Unknown standard."}), 404
    if not s.get("has_search"):
        return jsonify({"error": "Search is not available for this standard yet."}), 404
    question = re.sub(r"\s+", " ", request.args.get("q") or "").strip()
    if len(question) < 2:
        return jsonify({"error": "Type a question or a few keywords."}), 400
    if len(question) > 300:
        return jsonify({"error": "Keep the question under 300 characters."}), 400
    try:
        idx, answers, words, answer = std_search(s["num"], question)
    except Exception:
        return jsonify({"error": "Could not read the search file for this standard."}), 500
    return jsonify({"answers": answers, "answer": answer, "words": words, "file": idx["meta"].get("file"),
                    "revision": idx["meta"].get("revision"), "pages": idx["meta"].get("pages")})



STANDARDS_PAGE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Standards Library</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400;1,600&display=swap">
<style>
/* Layout: the dark command-map look shared with the other apps. Search and filter chips, a grid of standard
   cards, and a pop-up per standard with its official link, a quick overview, "Ask this standard" and team notes. */
:root{
  color-scheme:dark;
  --bg:#0d1216;
  --ink:#d9e2d6;
  --muted:#8e9c93;
  --line:#34434b;
  --panel:#151e25;
  --field:#0f161b;
  --accent:#e3b34a;
  --accent-ink:#1c1505;
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
}
*{box-sizing:border-box}
[hidden]{display:none!important}
html,body{min-height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);font-size:16px;line-height:1.45;overflow-x:hidden}
button,input,select,textarea{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}

.bg{position:fixed;inset:0;z-index:0;pointer-events:none;background:
  radial-gradient(ellipse at 50% 45%,transparent 45%,rgb(0 0 0 / .6)),
  radial-gradient(50% 40% at 85% 0%,rgb(227 179 74 / .09),transparent 70%),
  linear-gradient(rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  linear-gradient(90deg,rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  var(--bg)}
.stage{position:relative;z-index:1;padding:clamp(16px,2.4vw,32px) clamp(16px,3vw,48px) 48px}
.page{max-width:96rem;margin:0 auto;min-width:0}
.back{display:inline-block;margin-bottom:6px;color:var(--muted);font-family:var(--display);font-weight:500;font-size:.92rem;letter-spacing:.06em;text-decoration:none}
.back:hover{color:var(--ink);text-decoration:underline}
h1{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.8rem,5vw,3rem);letter-spacing:.12em;text-transform:uppercase;line-height:1.1;
  text-shadow:3px 3px 0 rgb(227 179 74 / .4)}
.lede{margin:10px 0 0;max-width:52rem;color:var(--muted);font-size:1.02rem;line-height:1.5}

.search{margin:20px 0 0;display:flex;gap:10px;flex-wrap:wrap}
.search input{flex:1 1 16rem;min-width:0;padding:11px 14px;border:1px solid var(--line);border-radius:12px;background:var(--field);font-size:1.02rem}
.search input::placeholder,.fld input::placeholder,textarea::placeholder,.ask input::placeholder{color:rgb(217 226 214 / .35)}
.btn{cursor:pointer;display:inline-block;padding:9px 20px;border-radius:999px;border:1.5px solid var(--accent);background:var(--accent);color:var(--accent-ink);text-decoration:none;
  font-family:var(--display);font-weight:700;font-size:.78rem;letter-spacing:.16em;text-transform:uppercase;box-shadow:0 3px 0 #000;transition:transform .12s,box-shadow .12s;white-space:normal;max-width:100%}
.btn.alt{background:transparent;color:var(--accent)}
.btn.sm{padding:6px 14px;font-size:.68rem}
.btn:hover{filter:brightness(1.08)}
.btn:active{transform:translateY(2px);box-shadow:none}
.btn[disabled]{opacity:.6;cursor:progress}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0 0}
.chips .lbl{align-self:center;margin-right:2px;font-family:var(--display);font-weight:700;font-size:.68rem;letter-spacing:.16em;text-transform:uppercase;color:var(--accent)}
.chip{cursor:pointer;padding:5px 13px;border-radius:999px;border:1px solid var(--line);background:rgb(21 30 37 / .8);font-family:var(--display);font-weight:500;font-size:.85rem;letter-spacing:.04em}
.chip:hover{border-color:var(--muted)}
.chip[aria-pressed="true"]{background:var(--accent);border-color:var(--accent);color:var(--accent-ink);font-weight:700}
.chip .n{margin-left:6px;opacity:.7;font-size:.78rem}
.count{margin:16px 0 0;color:var(--muted);font-size:.95rem}

.grid{list-style:none;margin:12px 0 0;padding:0;display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,19rem),1fr));gap:18px}
.card{cursor:pointer;display:flex;flex-direction:column;gap:8px;height:100%;padding:16px 16px 14px;min-width:0;
  border:1px solid var(--line);border-radius:16px;background:linear-gradient(160deg,#1b2630,#11181e);box-shadow:0 14px 30px rgb(0 0 0 / .45);transition:border-color .18s,box-shadow .18s}
.card:hover{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent),0 20px 36px rgb(0 0 0 / .5)}
.open{all:unset;cursor:pointer}
.open:focus-visible{outline:2px solid var(--accent);outline-offset:3px}
.num{margin:0;font-family:var(--display);font-weight:700;font-size:1.2rem;letter-spacing:.05em;overflow-wrap:anywhere}
.ttl{margin:0;font-weight:600;font-size:1rem;line-height:1.3;overflow-wrap:anywhere}
.tags{display:flex;flex-wrap:wrap;gap:6px}
.tag{padding:2px 9px;border-radius:999px;border:1px solid var(--line);color:var(--muted);font-family:var(--display);font-weight:700;font-size:.6rem;letter-spacing:.12em;text-transform:uppercase}
.tag.ask{border-color:var(--accent);color:var(--accent)}
.tag.ref{border-style:dashed}
.tag.own{color:#9fc3ff;border-color:#4a6488}
.sum{margin:0;color:var(--muted);font-size:.95rem;overflow-wrap:anywhere}
.status{margin:0;padding:6px 9px;border-radius:8px;border:1px solid rgb(244 182 162 / .35);background:rgb(244 182 162 / .08);color:#f4b6a2;font-size:.88rem}
.acts{display:flex;flex-wrap:wrap;gap:8px 12px;align-items:center;margin-top:auto;padding-top:4px}
.acts a,.lnk{color:var(--accent);font-family:var(--display);font-weight:500;font-size:.86rem;letter-spacing:.04em}
.how{margin:0;color:var(--muted);font-size:.86rem}
.err{color:#f4b6a2}
.empty{margin:0;padding:10px 12px;border:1px dashed var(--line);border-radius:10px;color:var(--muted);font-size:.95rem}

.panel{margin:26px 0 0;padding:16px clamp(14px,2vw,22px) 18px;border:1px solid var(--line);border-radius:14px;background:var(--panel)}
.panel summary{cursor:pointer;font-family:var(--display);font-weight:700;font-size:.8rem;letter-spacing:.18em;text-transform:uppercase;color:var(--accent)}
.fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,13rem),1fr));gap:12px 14px;margin:14px 0 10px}
.fld{display:grid;gap:5px;min-width:0}
.fld.wide{grid-column:1 / -1}
.fld>span{font-family:var(--display);font-weight:700;font-size:.68rem;letter-spacing:.16em;text-transform:uppercase;color:var(--accent)}
.fld input,.fld select,.fld textarea{width:100%;min-width:0;padding:9px 12px;border:1px solid var(--line);border-radius:10px;background:var(--field);font-size:1rem}
.fld select option{background:var(--panel)}
.fld textarea{min-height:4.5rem;resize:vertical}
.foot{margin:24px 0 0;max-width:52rem;color:var(--muted);font-size:.86rem;line-height:1.5}

/* pop-up */
dialog{width:min(58rem,calc(100% - 16px));max-height:calc(100dvh - 24px);padding:0;border:1px solid var(--line);border-radius:16px;background:var(--panel);color:var(--ink);box-shadow:0 30px 70px rgb(0 0 0 / .7)}
dialog::backdrop{background:rgb(5 8 10 / .72)}
.dh{position:sticky;top:0;z-index:2;display:flex;gap:12px;align-items:flex-start;justify-content:space-between;padding:16px clamp(14px,3vw,24px) 12px;
  background:linear-gradient(160deg,#1b2630,#151e25);border-bottom:1px solid var(--line)}
.dh>div{min-width:0}
.dh h2{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.3rem,4.5vw,1.8rem);letter-spacing:.06em;overflow-wrap:anywhere}
.dh p{margin:2px 0 8px;font-weight:600;overflow-wrap:anywhere}
.x{flex:none;cursor:pointer;width:40px;height:40px;border-radius:50%;border:1px solid var(--line);background:var(--field);font-size:1.3rem;line-height:1}
.x:hover{border-color:var(--accent);color:var(--accent)}
.db{padding:16px clamp(14px,3vw,24px) 22px;display:grid;gap:18px;min-width:0}
.db>*{min-width:0}
.get{display:flex;flex-wrap:wrap;gap:10px;align-items:center;padding:12px 14px;border:1px solid var(--line);border-radius:12px;background:var(--field)}
.get .how{flex:1 1 14rem}
.prov{margin:0;padding:8px 11px;border:1px dashed var(--line);border-radius:10px;color:var(--muted);font-size:.9rem}
.sec h3{margin:0 0 8px;font-family:var(--display);font-weight:700;font-size:.78rem;letter-spacing:.18em;text-transform:uppercase;color:var(--accent)}
.sec>p{margin:0}
.chk{margin:0;padding:0;list-style:none;display:grid;gap:8px;counter-reset:c}
.chk li{counter-increment:c;display:grid;grid-template-columns:1.8rem minmax(0,1fr);gap:4px 10px;align-items:start;padding:9px 11px;border:1px solid var(--line);border-radius:10px;background:var(--field)}
.chk li::before{content:counter(c);display:grid;place-items:center;width:1.6rem;height:1.6rem;border-radius:50%;background:var(--accent);color:var(--accent-ink);font-family:var(--display);font-weight:700;font-size:.78rem}
.parts{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,15rem),1fr));gap:4px 16px;margin:0;padding:0;list-style:none}
.parts li{display:flex;gap:8px;padding:5px 0;border-bottom:1px solid rgb(52 67 75 / .6);font-size:.95rem;min-width:0}
.parts b{min-width:3rem;font-family:var(--display);font-weight:700;color:var(--accent)}
.tip{margin:0;padding:10px 12px;border:1px solid rgb(227 179 74 / .35);border-radius:10px;background:rgb(227 179 74 / .07)}
.rel{display:flex;flex-wrap:wrap;gap:8px}

/* ask this standard */
.ask{padding:14px clamp(12px,2vw,16px);border:1px solid rgb(227 179 74 / .45);border-radius:14px;background:linear-gradient(160deg,#1b2630,#11181e)}
.ask form{display:flex;flex-wrap:wrap;gap:10px;margin-top:4px}
.ask input{flex:1 1 15rem;min-width:0;padding:10px 13px;border:1px solid var(--line);border-radius:10px;background:var(--field);font-size:1rem}
.ask .ex{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
.ask .ex button{cursor:pointer;padding:3px 10px;border-radius:999px;border:1px dashed var(--line);background:transparent;color:var(--muted);font-size:.84rem}
.ask .ex button:hover{color:var(--ink);border-color:var(--muted)}
.ans{list-style:none;margin:12px 0 0;padding:0;display:grid;gap:10px}
.ans li{padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--field);min-width:0}
.ans .where{display:flex;flex-wrap:wrap;gap:6px 10px;margin-bottom:5px;font-family:var(--display);font-weight:700;font-size:.7rem;letter-spacing:.12em;text-transform:uppercase;color:var(--accent)}
.ans .where span:last-child{color:var(--muted);letter-spacing:.06em;text-transform:none;font-weight:500}
.ans p{margin:0;font-size:.95rem;line-height:1.5;overflow-wrap:anywhere;white-space:pre-line}
.ans mark,.direct mark{background:rgb(227 179 74 / .28);color:var(--ink);border-radius:3px;padding:0 1px}
/* the direct answer card */
.direct{margin-top:12px;padding:14px 16px;border:1px solid var(--accent);border-radius:12px;background:rgb(227 179 74 / .07);display:grid;gap:12px}
.direct .dlab{margin:0;font-family:var(--display);font-weight:700;font-size:.74rem;letter-spacing:.18em;text-transform:uppercase;color:var(--accent)}
.direct .ditem{display:grid;gap:4px;padding-left:12px;border-left:2px solid rgb(227 179 74 / .55)}
.direct .ditem p{margin:0;font-size:1.02rem;line-height:1.5;color:var(--ink);overflow-wrap:anywhere}
.direct .dcite{font-family:var(--display);font-weight:500;font-size:.78rem;letter-spacing:.06em;color:var(--muted)}
.direct .dnote{margin:0;color:var(--muted);font-size:.82rem;font-style:italic}
.moreans{margin-top:10px}
.moreans summary{cursor:pointer;color:var(--accent);font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.14em;text-transform:uppercase}
.nt textarea{width:100%;min-height:5.5rem;padding:9px 12px;border:1px solid var(--line);border-radius:10px;background:var(--field);font-size:.95rem;resize:vertical}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:8px}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
</style>
</head><body>
<div class="bg" aria-hidden="true"></div>
<div class="stage">
<main class="page">
  <a class="back" href="/">&larr; Home</a>
  <h1>Standards Library</h1>
  <p class="lede">Free, publicly released standards that engineers use in acquisition and design. Open a standard to get the official document, read a quick overview, or ask it a question: answers are passages quoted from the document itself, with page numbers.</p>

  <div class="search">
    <input id="q" type="search" placeholder="Search number, title or topic, e.g. 810, vibration, 1553" aria-label="Search standards" autocomplete="off">
  </div>
  <div class="chips" id="areas" role="group" aria-label="Filter by area"></div>
  <div class="chips" id="srcs" role="group" aria-label="Filter by source"></div>
  <p class="count" id="count" aria-live="polite"></p>
  <ul class="grid" id="grid"></ul>

  <details class="panel" id="addp">
    <summary>Add a standard</summary>
    <form id="addf" novalidate>
      <div class="fields">
        <label class="fld"><span>Number *</span><input id="a-num" maxlength="40" placeholder="e.g. MIL-STD-1474"></label>
        <label class="fld"><span>Area *</span><select id="a-area"></select></label>
        <label class="fld"><span>Type</span><select id="a-kind"></select></label>
        <label class="fld"><span>Issued by</span><input id="a-src" maxlength="40" placeholder="e.g. DoD, NASA"></label>
        <label class="fld wide"><span>Title *</span><input id="a-title" maxlength="200"></label>
        <label class="fld wide"><span>What it is for</span><textarea id="a-sum" maxlength="400" placeholder="One or two sentences in your own words"></textarea></label>
        <label class="fld wide"><span>Link to the free official copy (https)</span><input id="a-link" maxlength="300" placeholder="https://"></label>
      </div>
      <p class="how">Only add standards that are free and publicly released.</p>
      <div class="row"><button class="btn" type="submit" id="a-go">Add to library</button><span id="a-msg" class="how" aria-live="polite"></span></div>
    </form>
  </details>

  <p class="foot" id="foot"></p>
</main>
</div>

<dialog id="dlg" aria-labelledby="d-num">
  <div class="dh">
    <div><h2 id="d-num"></h2><p id="d-title"></p><div class="tags" id="d-tags"></div></div>
    <button class="x" id="d-x" type="button" aria-label="Close">&times;</button>
  </div>
  <div class="db" id="d-body"></div>
</dialog>

<script>
var $ = function (s) { return document.querySelector(s); };
function el(t, text, cls) { var e = document.createElement(t); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
function safeUrl(u) { return /^https:\/\//i.test(u || '') ? u : ''; }
var D = { standards: [], areas: [], kinds: [], sources: {} }, area = 'All', src = 'All';
var cur = null, askToken = 0;

function post(body) {
  return fetch('/api/standards', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
    .then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.error || 'Something went wrong.'); return j; }); });
}
function srcOf(s) { return s.custom ? 'Added by team' : s.source; }
function matches(s, words) {
  var hay = [s.num, s.title, s.summary, s.area, s.kind, s.source, s.note, s.num.replace(/[^0-9.]/g, '')].join(' ').toLowerCase();
  return words.every(function (w) { return hay.indexOf(w) >= 0; });
}
function tagsFor(s) {
  var t = el('div', null, 'tags');
  t.append(el('span', s.area, 'tag'), el('span', s.kind, 'tag'), el('span', srcOf(s), 'tag' + (s.custom ? ' own' : '')));
  if (s.has_search) t.append(el('span', 'Ask this standard', 'tag ask'));
  if (s.reference_only) t.append(el('span', 'Reference only', 'tag ref'));
  return t;
}
function officialLink(s, label) {
  var info = D.sources[s.source];
  var u = safeUrl(s.link) || (info && !s.custom ? info.url : '');
  if (!u) return null;
  var a = el('a', label || (safeUrl(s.link) ? 'Official PDF' : 'Find on ' + (info ? info.name : 'the official site'))); a.href = u; a.target = '_blank'; a.rel = 'noopener';
  return a;
}
function copyBtn(label, text) {
  var b = el('button', label, 'btn alt sm'); b.type = 'button';
  b.onclick = function (e) {
    e.stopPropagation();
    (navigator.clipboard ? navigator.clipboard.writeText(text()) : Promise.reject()).then(function () { b.textContent = 'Copied'; }, function () { b.textContent = 'Copy failed'; });
    setTimeout(function () { b.textContent = label; }, 1600);
  };
  return b;
}
function chipRow(box, label, list, current, set) {
  box.replaceChildren(el('span', label, 'lbl'));
  list.forEach(function (p) {
    var b = el('button', null, 'chip'); b.type = 'button';
    b.setAttribute('aria-pressed', String(p[0] === current));
    b.append(document.createTextNode(p[0]), el('span', String(p[1]), 'n'));
    b.onclick = function () { set(p[0]); draw(); };
    box.append(b);
  });
}

function card(s) {
  var li = el('li'), c = el('article', null, 'card');
  var open = el('button', null, 'open'); open.type = 'button';
  open.append(el('h2', s.num, 'num'));
  c.append(open, el('p', s.title, 'ttl'), tagsFor(s));
  if (s.summary) c.append(el('p', s.summary, 'sum'));
  if (s.status) c.append(el('p', s.status, 'status'));
  var acts = el('div', null, 'acts');
  var v = el('button', s.has_search ? 'Open · ask it' : 'Open', 'btn sm'); v.type = 'button';
  acts.append(v);
  var a = officialLink(s); if (a) { a.onclick = function (e) { e.stopPropagation(); }; acts.append(a); }
  if (s.note) acts.append(el('span', '● Team notes', 'how'));
  c.append(acts);
  c.onclick = function () { go(s.num); };
  li.append(c);
  return li;
}
function draw() {
  var words = $('#q').value.toLowerCase().split(/\s+/).filter(Boolean);
  var bySearch = D.standards.filter(function (s) { return matches(s, words); });
  function counts(key, list) { var c = {}; list.forEach(function (s) { var k = key(s); c[k] = (c[k] || 0) + 1; }); return c; }
  var inSrc = bySearch.filter(function (s) { return src === 'All' || srcOf(s) === src; });
  var ac = counts(function (s) { return s.area; }, inSrc);
  chipRow($('#areas'), 'Area', [['All', inSrc.length]].concat(D.areas.filter(function (a) { return ac[a]; }).map(function (a) { return [a, ac[a]]; })), area, function (v) { area = v; });
  var inArea = bySearch.filter(function (s) { return area === 'All' || s.area === area; });
  var sc = counts(srcOf, inArea);
  chipRow($('#srcs'), 'Source', [['All', inArea.length]].concat(Object.keys(sc).sort().map(function (k) { return [k, sc[k]]; })), src, function (v) { src = v; });
  var rows = inArea.filter(function (s) { return src === 'All' || srcOf(s) === src; });
  rows.sort(function (a, b) { return D.areas.indexOf(a.area) - D.areas.indexOf(b.area) || a.num.localeCompare(b.num, undefined, { numeric: true }); });
  var g = $('#grid'); g.replaceChildren();
  rows.forEach(function (s) { g.append(card(s)); });
  $('#count').textContent = rows.length ? 'Showing ' + rows.length + ' of ' + D.standards.length + ' standards' : '';
  if (!rows.length) g.append(el('li', 'No standards match. Try a different word, clear the filters, or add it below.', 'empty'));
}

/* ---- pop-up ---- */
function go(num) { location.hash = encodeURIComponent(num); }
function fromHash() { try { return decodeURIComponent(location.hash.slice(1)); } catch (e) { return ''; } }
function closeDetail() {
  cur = null; askToken++;
  if ($('#dlg').open) $('#dlg').close();
  if (location.hash) history.replaceState(null, '', location.pathname + location.search);
}
$('#d-x').onclick = closeDetail;
$('#dlg').addEventListener('close', function () { if (location.hash) closeDetail(); });
$('#dlg').addEventListener('click', function (e) { if (e.target === $('#dlg')) closeDetail(); });
window.addEventListener('hashchange', route);

function route() {
  var num = fromHash();
  if (!num) { closeDetail(); return; }
  if (!$('#dlg').open) $('#dlg').showModal();
  $('#d-num').textContent = num; $('#d-title').textContent = ''; $('#d-tags').replaceChildren();
  $('#d-body').replaceChildren(el('p', 'Loading…', 'how'));
  fetch('/api/standards/detail?num=' + encodeURIComponent(num)).then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
    .then(function (x) {
      if (fromHash() !== num) return;
      if (!x.ok) { $('#d-body').replaceChildren(el('p', x.j.error || 'Could not load this standard.', 'empty')); return; }
      cur = x.j; renderDetail();
    })
    .catch(function () { $('#d-body').replaceChildren(el('p', 'Could not reach the server.', 'empty')); });
}
function section(title, node, cls) { var s = el('section', null, 'sec' + (cls ? ' ' + cls : '')); s.append(el('h3', title)); if (typeof node === 'string') s.append(el('p', node)); else if (node) s.append(node); return s; }

function renderDetail() {
  var s = cur.standard, ov = cur.overview, body = $('#d-body');
  $('#d-num').textContent = s.num; $('#d-title').textContent = s.title;
  $('#d-tags').replaceChildren.apply($('#d-tags'), Array.prototype.slice.call(tagsFor(s).children));
  body.replaceChildren();

  var get = el('div', null, 'get'), info = D.sources[s.source];
  get.append(el('p', s.custom ? 'Official document' : safeUrl(s.link) && s.source === 'DoD'
    ? 'Free official copy, approved for public release. Opens the document on ASSIST; the newest revision is listed first.'
    : safeUrl(s.link) && s.source === 'NASA' ? 'Free official PDF from the NASA Technical Standards System.'
    : (info ? info.how : 'Official document'), 'how'));
  var a = officialLink(s, safeUrl(s.link) ? 'Get the official PDF' : null); if (a) { a.className = 'btn sm'; get.append(a); }
  get.append(copyBtn('Copy number', function () { return s.num; }));
  body.append(get);

  if (s.reference_only) {
    body.append(el('p', 'Reference only. This library lists munition and fuze safety standards with their official source but does not summarise or search them. Use the official document and your programme\'s safety authority.', 'prov'));
  }
  if (s.summary) body.append(section('What it is for', s.summary));
  if (s.has_search) body.append(askBox(s, cur.search || {}));
  else if (!s.reference_only) body.append(el('p', 'Ask this standard is not available for this one yet. Download the PDF from the official source above.', 'prov'));

  if (ov) {
    body.append(el('p', 'Quick overview written for this tool. It covers the main obligations, not every requirement; items marked "check your revision" vary between revisions. The official document is what you must comply with.', 'prov'));
    if (ov.purpose) body.append(section('Purpose', ov.purpose));
    if (ov.applies) body.append(section('Applies to', ov.applies));
    if (ov.reqs && ov.reqs.length) {
      var ol = el('ol', null, 'chk');
      ov.reqs.forEach(function (r) { var li = el('li'); li.append(el('span', r)); ol.append(li); });
      var sec = section('Main obligations', ol), row = el('div', null, 'row');
      row.append(copyBtn('Copy list', function () { return s.num + ' – ' + s.title + '\n' + ov.reqs.map(function (r, i) { return (i + 1) + '. ' + r; }).join('\n'); }));
      sec.append(row); body.append(sec);
    }
    if (ov.parts && ov.parts.length) {
      var ul = el('ul', null, 'parts');
      ov.parts.forEach(function (p) { var li = el('li'); if (p[0]) li.append(el('b', p[0])); li.append(el('span', p[1])); ul.append(li); });
      var ps = section(ov.parts_label || 'Contents', ul);
      if (ov.parts_note) ps.append(el('p', ov.parts_note, 'how'));
      body.append(ps);
    }
    if (ov.tip) body.append(section('Watch out for', el('p', ov.tip, 'tip')));
    if (ov.related && ov.related.length) {
      var rel = el('div', null, 'rel');
      ov.related.forEach(function (n) { var b = el('button', n, 'chip'); b.type = 'button'; b.onclick = function () { go(n); }; rel.append(b); });
      body.append(section('Related standards', rel));
    }
  }
  body.append(notesBox(s));
}

/* Highlight the question's words inside a quoted passage, built with text nodes only. */
function highlighted(text, words) {
  var p = el('p');
  if (!words.length) { p.textContent = text; return p; }
  var rx = new RegExp('(' + words.map(function (w) { return w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }).join('|') + ')', 'gi');
  text.split(rx).forEach(function (part, i) { p.append(i % 2 ? el('mark', part) : document.createTextNode(part)); });
  return p;
}
function askBox(s, meta) {
  var box = section('Ask this standard', null, 'ask');
  box.append(el('p', 'Ask in plain words, e.g. "what are the HERO requirements for ordnance". The answer gives the sentences of ' +
    (meta.revision || 'the official document') + ' that answer your question, quoted word for word with their section and page.', 'how'));
  var norm = function (v) { return (v || '').toLowerCase().replace(/\(.*?\)/g, '').replace(/[^a-z0-9]/g, ''); };
  if (meta.current && norm(meta.current) !== norm(meta.revision)) {
    box.append(el('p', 'Note: this search uses ' + meta.revision + '. The current version on ASSIST is ' + meta.current +
      ', which may change some requirements. Check the official copy before relying on an answer.', 'how err'));
  }
  var f = el('form'), inp = el('input'), b = el('button', 'Ask', 'btn sm');
  inp.type = 'search'; inp.maxLength = 300; inp.placeholder = 'Type your question'; inp.setAttribute('aria-label', 'Question about ' + s.num);
  b.type = 'submit';
  f.append(inp, b);
  var msg = el('p', '', 'how'); msg.setAttribute('aria-live', 'polite'); msg.style.marginTop = '10px';
  var list = el('div', null, 'anslist');
  box.append(f);
  if (meta.examples && meta.examples.length) {
    var ex = el('div', null, 'ex');
    meta.examples.forEach(function (q) { var e = el('button', q); e.type = 'button'; e.onclick = function () { inp.value = q; ask(); }; ex.append(e); });
    box.append(ex);
  }
  box.append(msg, list);
  function ask() {
    var q = inp.value.trim();
    if (q.length < 2) { msg.textContent = 'Type a question or a few keywords.'; msg.className = 'how err'; return; }
    var my = ++askToken;
    b.disabled = true; msg.textContent = 'Searching the document…'; msg.className = 'how'; list.replaceChildren();
    fetch('/api/standards/ask?num=' + encodeURIComponent(s.num) + '&q=' + encodeURIComponent(q))
      .then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.error || 'Search failed.'); return j; }); })
      .then(function (j) {
        if (my !== askToken) return;
        b.disabled = false;
        if (!j.answers.length) { msg.textContent = 'Nothing in the document matches those words. Try other words, a method number or a shorter question.'; return; }
        var where = function (a) { return [a.context, a.section ? 'Section ' + a.section : '', 'PDF page ' + a.page].filter(Boolean).join(' · '); };
        var passages = el('ol', null, 'ans');
        j.answers.forEach(function (a) {
          var li = el('li'), w = el('div', null, 'where');
          w.append(el('span', 'PDF page ' + a.page), el('span', [a.context, a.section ? 'Section ' + a.section : ''].filter(Boolean).join(' · ')));
          li.append(w, highlighted(a.text, j.words));
          passages.append(li);
        });
        if (j.answer && j.answer.length) {
          // the direct answer: the standard's own sentences that answer the question, each with where it is
          msg.textContent = '';
          var card = el('div', null, 'direct');
          card.append(el('p', 'Answer', 'dlab'));
          j.answer.forEach(function (a) {
            var item = el('div', null, 'ditem');
            item.append(highlighted(a.text, j.words), el('span', where(a), 'dcite'));
            card.append(item);
          });
          card.append(el('p', 'Quoted word for word from ' + (s.num) + '. Check the full clause before relying on it.', 'dnote'));
          var more = el('details', null, 'moreans');
          more.append(el('summary', 'Show where this comes from (' + j.answers.length + ' passages)'), passages);
          list.append(card, more);
        } else {
          msg.textContent = 'No single sentence answers that directly; these are the closest passages. Check the full text on the page shown.';
          list.append(passages);
        }
      }, function (e) { if (my !== askToken) return; b.disabled = false; msg.textContent = e.message; msg.className = 'how err'; });
  }
  f.addEventListener('submit', function (e) { e.preventDefault(); ask(); });
  return box;
}

function notesBox(s) {
  var box = section('Team notes', null, 'nt');
  var ta = el('textarea'); ta.value = s.note || ''; ta.maxLength = 1500;
  ta.setAttribute('aria-label', 'Team notes for ' + s.num);
  ta.placeholder = 'Which revision your programme uses, tailoring decisions, useful sections…';
  var row = el('div', null, 'row'), save = el('button', 'Save note', 'btn sm'), msg = el('span', '', 'how');
  save.type = 'button';
  save.onclick = function () {
    save.disabled = true; msg.textContent = 'Saving…'; msg.className = 'how';
    post({ action: 'note', num: s.num, note: ta.value }).then(function (j) {
      save.disabled = false; msg.textContent = 'Saved'; s.note = ta.value.trim(); D.standards = j.standards; draw();
    }, function (e) { save.disabled = false; msg.textContent = e.message; msg.className = 'how err'; });
  };
  row.append(save, msg);
  if (s.custom) {
    var del = el('button', 'Remove standard', 'btn alt sm'), armT = null; del.type = 'button';
    del.onclick = function () {
      // two taps instead of a pop-up confirm, which some browsers block (silently cancelling)
      if (!armT) {
        del.textContent = 'Tap again to remove (notes too)';
        armT = setTimeout(function () { armT = null; del.textContent = 'Remove standard'; }, 3000);
        return;
      }
      clearTimeout(armT); armT = null;
      del.disabled = true;
      post({ action: 'delete', num: s.num }).then(function (j) { D.standards = j.standards; closeDetail(); draw(); }, function (e) { del.disabled = false; msg.textContent = e.message; msg.className = 'how err'; });
    };
    row.append(del);
  }
  box.append(ta, row);
  return box;
}

/* ---- list ---- */
function fill(id, list) { var s = $(id); s.replaceChildren(); list.forEach(function (v) { var o = el('option', v); o.value = v; s.append(o); }); }
function load() {
  fetch('/api/standards').then(function (r) { return r.json(); }).then(function (j) {
    D = j; fill('#a-area', j.areas); fill('#a-kind', j.kinds); draw();
    $('#foot').textContent = 'Only free, publicly released standards are listed. Paid standards (ISO, IEEE, SAE and similar) and documents that need an account are not included. Overviews and search help you find your way; the official document and the revision on your contract are what you must comply with.' +
      (j.persistent ? '' : ' Note: GitHub storage is not set up, so added standards and notes may be lost when the host restarts.');
    if (fromHash()) route();
  }).catch(function () { $('#count').textContent = 'Could not load the library.'; });
}
var t = null;
$('#q').addEventListener('input', function () { clearTimeout(t); t = setTimeout(draw, 120); });
$('#addf').addEventListener('submit', function (e) {
  e.preventDefault();
  var b = $('#a-go'), m = $('#a-msg');
  b.disabled = true; m.textContent = 'Adding…'; m.className = 'how';
  var num = $('#a-num').value;
  post({ action: 'add', num: num, area: $('#a-area').value, kind: $('#a-kind').value, source: $('#a-src').value,
         title: $('#a-title').value, summary: $('#a-sum').value, link: $('#a-link').value })
    .then(function (j) {
      b.disabled = false; m.textContent = 'Added.'; D.standards = j.standards;
      ['#a-num', '#a-src', '#a-title', '#a-sum', '#a-link'].forEach(function (id) { $(id).value = ''; });
      area = 'All'; src = 'All'; draw(); go(num.trim());
    }, function (err) { b.disabled = false; m.textContent = err.message; m.className = 'how err'; });
});
load();
</script>
</body></html>"""


PAGE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Top 10 News</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400&display=swap">
<style>
/* Layout: the dark command-map look shared with the home page and Battle Lab. Header, then a row of topic
   buttons; choosing one opens that topic's top 10 as a swipe row of Netflix-style picture cards. */
:root{
  color-scheme:dark;
  --bg:#0d1216;
  --ink:#d9e2d6;
  --muted:#8e9c93;
  --line:#34434b;
  --panel:#151e25;
  --accent:#e3b34a;
  --accent-ink:#1c1505;
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
  --gut:clamp(16px,3vw,48px);
}
[hidden]{display:none!important}
html,body{min-height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);overflow-x:hidden}
button{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--accent);outline-offset:3px}

.bg{position:fixed;inset:0;z-index:0;pointer-events:none;background:
  radial-gradient(ellipse at 50% 45%,transparent 45%,rgb(0 0 0 / .6)),
  radial-gradient(50% 40% at 85% 0%,rgb(227 179 74 / .09),transparent 70%),
  linear-gradient(rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  linear-gradient(90deg,rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  var(--bg)}
.stage{position:relative;z-index:1;box-sizing:border-box;padding:clamp(16px,2.4vw,32px) var(--gut) 48px}
.page{max-width:96rem;margin:0 auto;min-width:0}
.back{display:inline-block;margin-bottom:6px;color:var(--muted);font-family:var(--display);font-weight:500;font-size:.92rem;letter-spacing:.06em;text-decoration:none}
.back:hover{color:var(--ink);text-decoration:underline}
h1{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.8rem,5vw,3rem);letter-spacing:.12em;text-transform:uppercase;line-height:1.1;
  text-shadow:3px 3px 0 rgb(227 179 74 / .4)}
.st{margin:10px 0 0;color:var(--muted);font-size:.98rem;line-height:1.45}

/* topic buttons */
.topics{display:flex;flex-wrap:wrap;gap:10px;margin:20px 0 0}
.topic{display:inline-flex;align-items:center;gap:10px;cursor:pointer;padding:9px 16px 9px 11px;border-radius:12px;border:1px solid var(--line);
  background:linear-gradient(160deg,#1b2630,#11181e);box-shadow:0 8px 20px rgb(0 0 0 / .45);color:var(--ink);
  font-family:var(--display);font-weight:700;font-size:.82rem;letter-spacing:.12em;text-transform:uppercase;text-align:left;transition:transform .15s,border-color .15s,box-shadow .15s}
.topic svg{width:28px;height:28px;flex:none;color:var(--accent)}
.topic[aria-pressed="true"] svg{color:var(--accent-ink)}
@media (max-width:720px){   /* phone: two buttons per row so the news stays in view */
  .topics{display:grid;grid-template-columns:1fr 1fr;gap:8px}
  .topic{padding:8px 9px;gap:7px;font-size:.66rem;letter-spacing:.07em;line-height:1.2}
  .topic svg{width:22px;height:22px}
}
.topic:hover{transform:translateY(-3px);border-color:var(--accent);box-shadow:0 0 0 1px var(--accent),0 16px 30px rgb(0 0 0 / .5)}
.topic[aria-pressed="true"]{border-color:var(--accent);background:var(--accent);color:var(--accent-ink)}
.hint{margin:14px 0 0;color:var(--muted);font-size:.95rem}

/* the top 10 for the chosen topic */
.board{margin:26px 0 0}
.board h2{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.2rem,3.2vw,1.7rem);letter-spacing:.16em;text-transform:uppercase;
  text-shadow:2px 2px 0 rgb(227 179 74 / .35)}
.board h2 span{color:var(--accent)}
.rail-wrap{position:relative;margin:4px calc(-1 * var(--gut)) 0;--cw:clamp(260px,78vw,350px)}
.rail{list-style:none;margin:0;padding:20px var(--gut) 30px;display:flex;gap:clamp(14px,2vw,22px);overflow-x:auto;scroll-snap-type:x proximity;
  scroll-padding-inline:var(--gut);scrollbar-width:none;-webkit-overflow-scrolling:touch}
.rail::-webkit-scrollbar{display:none}
.item{flex:none;display:flex;scroll-snap-align:start;position:relative}
/* "Copy to share" sits at the foot of the card, below the summary. It is outside the card link (so tapping it
   never opens the article) and placed over the space .txt keeps free at the bottom. */
.copy{position:absolute;left:15px;bottom:14px;z-index:3;display:inline-flex;align-items:center;gap:6px;cursor:pointer;padding:5px 11px 5px 9px;border-radius:999px;
  border:1px solid rgb(227 179 74 / .7);background:rgb(13 18 22 / .82);color:var(--accent);font-family:var(--display);font-weight:700;font-size:.66rem;
  letter-spacing:.12em;text-transform:uppercase;transition:transform .18s ease-out,background .15s}
.copy svg{width:13px;height:13px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.copy:hover{background:var(--accent);color:var(--accent-ink)}
.copy.done{background:#2f7d4f;border-color:#6fd39b;color:#fff}
@media (hover:hover){.item:hover .copy{transform:translateY(-4px)}}
.bubble{flex:1;position:relative;width:var(--cw);display:flex;flex-direction:column;box-sizing:border-box;overflow:hidden;
  border:1px solid var(--line);border-radius:16px;background:linear-gradient(160deg,#1b2630,#11181e);
  color:var(--ink);text-decoration:none;box-shadow:0 14px 30px rgb(0 0 0 / .45);transition:transform .18s ease-out,border-color .18s,box-shadow .18s;-webkit-tap-highlight-color:transparent}
@media (hover:hover){.item:hover .bubble{transform:translateY(-4px);border-color:var(--accent);box-shadow:0 0 0 1px var(--accent),0 22px 40px rgb(0 0 0 / .55),0 0 30px rgb(227 179 74 / .14)}}
.bubble:active{transform:translateY(-1px)}
.pic{position:relative;flex:none;aspect-ratio:16 / 10;background:linear-gradient(160deg,#1f2a32,#0f161b)}
.pic img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}
.pic::after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(to bottom,rgb(13 18 22 / .55),transparent 40%,transparent 62%,rgb(13 18 22 / .85))}
.pic .ph{position:absolute;inset:0;display:grid;place-items:center}
.pic .ph svg{width:30%;height:auto}
.rank{position:absolute;left:12px;bottom:-4px;z-index:2;user-select:none;
  font-family:var(--display);font-weight:700;font-size:calc(var(--cw) * .3);line-height:1;letter-spacing:-.04em;
  color:var(--accent);-webkit-text-stroke:2px #0d1216;paint-order:stroke fill;text-shadow:0 3px 0 rgb(0 0 0 / .7)}
.src{position:absolute;right:10px;top:10px;z-index:2;max-width:60%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:3px 9px;border-radius:999px;
  border:1px solid var(--accent);background:rgb(13 18 22 / .8);color:var(--accent);font-family:var(--display);font-weight:700;font-size:.62rem;letter-spacing:.1em}
.txt{display:grid;align-content:start;gap:8px;padding:12px 15px 58px;flex:1}
.ttl{margin:0;font-weight:600;font-size:1.04rem;line-height:1.28;color:var(--ink);display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.meta{margin:0;color:var(--muted);font-size:.84rem}
.sumh{margin:4px 0 0;display:flex;align-items:center;gap:8px;font-family:var(--display);font-weight:700;font-size:.62rem;letter-spacing:.16em;text-transform:uppercase;color:var(--muted)}
.sumh::before{content:"";width:18px;height:2px;border-radius:2px;background:var(--accent)}
.sum{margin:0;padding:0;list-style:none;display:grid;gap:8px}
.sum li{position:relative;padding-left:15px;font-size:.92rem;line-height:1.4;color:#c4cfc6}
.sum li::before{content:"";position:absolute;left:0;top:.55em;width:6px;height:6px;border-radius:50%;background:var(--accent)}
.sum.none{color:var(--muted);font-size:.9rem}
.arrow{display:none}
@media (hover:hover){
  .arrow{display:grid;place-items:center;position:absolute;top:44%;z-index:3;width:42px;height:42px;border-radius:50%;cursor:pointer;
    border:1px solid var(--line);background:rgb(16 23 28 / .92);box-shadow:0 4px 14px rgb(0 0 0 / .5)}
  .arrow:hover{border-color:var(--accent)}
  .arrow.prev{left:14px}.arrow.next{right:14px}
  .arrow svg{width:16px;height:16px;fill:none;stroke:var(--accent);stroke-width:2.6;stroke-linecap:round;stroke-linejoin:round}
}
.empty{margin:18px 0;padding:12px 14px;border-radius:10px;border:1px solid var(--line);background:var(--panel);color:var(--ink);font-size:.96rem}
.foot{margin:6px 0 0;color:var(--muted);font-size:.84rem;line-height:1.45}
@media (prefers-reduced-motion:reduce){.bubble,.topic{transition:none}.bubble:hover,.topic:hover{transform:none}.rail{scroll-behavior:auto}}
</style>
</head><body>
<div class="bg" aria-hidden="true"></div>
<div class="stage">
  <main class="page">
    <a class="back" href="/">&larr; Home</a>
    <h1>Top 10 News</h1>
    <p class="st" id="st" aria-live="polite"></p>
    <div class="topics" id="topics" role="group" aria-label="Choose a topic"></div>
    <p class="hint" id="hint">Choose a topic to see its 10 latest top stories.</p>

    <section class="board" id="board" hidden aria-live="polite">
      <h2>Top 10 · <span id="bt"></span></h2>
      <div class="rail-wrap">
        <button class="arrow prev" id="prev" type="button" aria-label="Scroll left"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10.5 2.5 5 8l5.5 5.5"/></svg></button>
        <ol class="rail" id="rail"></ol>
        <button class="arrow next" id="next" type="button" aria-label="Scroll right"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M5.5 2.5 11 8l-5.5 5.5"/></svg></button>
      </div>
      <p class="empty" id="empty" hidden></p>
      <p class="foot">Updated once a day at 6am. Summaries are the article's own key sentences; tap a card to read the full story, or Copy to share a short summary in WhatsApp, Telegram or email.</p>
    </section>
  </main>
</div>

<script>
var $ = function (s) { return document.querySelector(s); };
var topic = null, poll = 0;

// placeholder picture and topic icon (static markup)
var PH = '<svg viewBox="0 0 64 64" aria-hidden="true"><path d="M8 50 C22 22 38 14 56 10" fill="none" stroke="#e3b34a" stroke-width="2.4" stroke-dasharray="4 4" stroke-linecap="round"/><circle cx="56" cy="10" r="4" fill="#e3b34a"/><rect x="4" y="50" width="14" height="7" rx="1.5" fill="#cbbd92" stroke="#000"/></svg>';
// one small icon per topic (static markup); anything not listed gets the first one
var S = 'fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"';
var ICONS = {
  'Hypersonic': '<path d="M6 30 C14 12 24 8 34 6" ' + S + ' stroke-dasharray="3 3"/><circle cx="34" cy="6" r="3.5" fill="currentColor"/><rect x="3" y="30" width="10" height="6" rx="1" ' + S + '/>',
  'Air & Missile Defence': '<path d="M5 32 A15 15 0 0 1 35 32" ' + S + ' stroke-dasharray="3 3"/><path d="M20 32 V14" ' + S + '/><path d="M16 18 L20 12 L24 18" ' + S + '/>',
  'Ballistic Missiles': '<path d="M4 34 Q20 -6 36 34" ' + S + ' stroke-dasharray="3 3"/><circle cx="20" cy="14" r="3" fill="currentColor"/>',
  'Drones & Loitering Munitions': '<path d="M20 14 V26 M8 20 H32" ' + S + '/><circle cx="8" cy="20" r="4" ' + S + '/><circle cx="32" cy="20" r="4" ' + S + '/><rect x="16" y="16" width="8" height="8" rx="1.5" fill="currentColor"/>'
};
function icon(t) { return '<svg viewBox="0 0 40 40" aria-hidden="true">' + (ICONS[t] || ICONS['Hypersonic']) + '</svg>'; }

function el(tag, text, cls) { var e = document.createElement(tag); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
function safeUrl(u) { return /^https?:\/\//i.test(u || '') ? u : ''; }
function ago(ts) { var h = (Date.now() / 1000 - ts) / 3600; return h < 1 ? 'just now' : h < 24 ? Math.round(h) + 'h ago' : Math.round(h / 24) + 'd ago'; }

function card(d, rank) {
  var li = el('li', null, 'item'), a = el('a', null, 'bubble'), link = safeUrl(d.link);
  if (link) { a.href = link; a.target = '_blank'; a.rel = 'noopener'; }
  a.setAttribute('aria-label', 'Number ' + rank + ': ' + d.title);
  var pic = el('div', null, 'pic');
  function overlays() {
    var n = el('span', String(rank), 'rank'); n.setAttribute('aria-hidden', 'true');
    pic.append(n);
    if (d.source) pic.append(el('span', d.source, 'src'));
  }
  function fallback() { pic.replaceChildren(); var ph = el('div', null, 'ph'); ph.innerHTML = PH; pic.append(ph); overlays(); }
  var img = safeUrl(d.image);
  if (img) {
    var im = document.createElement('img');
    im.alt = ''; im.loading = 'lazy'; im.referrerPolicy = 'no-referrer'; im.onerror = fallback; im.src = img;
    pic.append(im); overlays();
  } else { fallback(); }
  var txt = el('div', null, 'txt');
  txt.append(el('h3', d.title, 'ttl'), el('p', ago(d.ts), 'meta'), el('p', 'Summary', 'sumh'));
  if (d.points && d.points.length) {
    var ul = el('ul', null, 'sum');
    d.points.forEach(function (p) { ul.append(el('li', p)); });
    txt.append(ul);
  } else {
    txt.append(el('p', 'Open the article for the full story.', 'sum none'));
  }
  a.append(pic, txt); li.append(a, copyButton(d));
  return li;
}

/* ---- copy a short, plain-text summary to share in WhatsApp, Telegram, Teams or email ---- */
function shareText(d) {
  var date = new Date(d.ts * 1000).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  // keep it a quick read: the first summary sentence, plus the second only if both stay short
  var pts = d.points || [], ctx = pts[0] || '';
  if (pts[1] && (ctx + ' ' + pts[1]).length <= 300) ctx += ' ' + pts[1];
  var lines = ['Top 10 News · ' + topic, 'Headline: ' + d.title, 'Date of article: ' + date];
  if (d.source) lines.push('Source: ' + d.source);
  if (d.countries && d.countries.length) lines.push('Countries: ' + d.countries.join(', '));
  if (d.companies && d.companies.length) lines.push('Companies: ' + d.companies.join(', '));
  if (ctx) lines.push('Context: ' + ctx);
  if (safeUrl(d.link)) lines.push('Link: ' + d.link);
  lines.push('Shared from GW University');
  return lines.join('\n');
}
function copyOld(text) {   // older browsers, or when the clipboard API says no: copy through a hidden text box
  return new Promise(function (ok, fail) {
    var t = el('textarea'); t.value = text; t.setAttribute('readonly', ''); t.style.position = 'fixed'; t.style.opacity = '0';
    document.body.append(t); t.select();
    try { document.execCommand('copy') ? ok() : fail(); } catch (e) { fail(); } finally { t.remove(); }
  });
}
function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text).catch(function () { return copyOld(text); });
  return copyOld(text);
}
var COPY_ICON = '<svg viewBox="0 0 16 16" aria-hidden="true"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M11 5V3.5A1.5 1.5 0 0 0 9.5 2h-6A1.5 1.5 0 0 0 2 3.5v6A1.5 1.5 0 0 0 3.5 11H5"/></svg>';
function copyButton(d) {
  var b = el('button', null, 'copy'); b.type = 'button';
  b.setAttribute('aria-label', 'Copy a short summary of this story to share');
  function label(text) { b.innerHTML = COPY_ICON; b.append(document.createTextNode(text)); }   // icon is static markup
  label('Copy');
  b.onclick = function (e) {
    e.preventDefault(); e.stopPropagation();
    copyText(shareText(d)).then(function () { b.classList.add('done'); label('Copied ✓'); },
                                function () { label('Copy failed'); })
      .then(function () { setTimeout(function () { b.classList.remove('done'); label('Copy'); }, 2000); });
  };
  return b;
}

function renderTopics(list) {
  var box = $('#topics');
  if (box.childElementCount) { box.querySelectorAll('.topic').forEach(function (b) { b.setAttribute('aria-pressed', String(b.dataset.t === topic)); }); return; }
  list.forEach(function (t) {
    var b = el('button', null, 'topic'); b.type = 'button'; b.dataset.t = t;
    b.setAttribute('aria-pressed', String(t === topic));
    b.innerHTML = icon(t);            // static icon markup only (topic names come from the server's fixed list)
    b.append(document.createTextNode(t));
    b.onclick = function () { choose(t); };
    box.append(b);
  });
}

function choose(t) {
  topic = t; poll = 0;
  renderTopics([]);
  $('#hint').hidden = true;
  $('#board').hidden = false;
  $('#bt').textContent = t;
  load();
  $('#board').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function load() {
  fetch('/api/news' + (topic ? '?topic=' + encodeURIComponent(topic) : ''))
    .then(function (r) { return r.json(); })
    .then(function (j) {
      renderTopics(j.topics || []);
      var st = $('#st');
      st.textContent = j.updated_label ? 'Updated ' + j.updated_label + ' · Next update ' + j.next_label
        : j.updating ? 'Preparing today\'s update. The stories will appear shortly.'
        : (j.error || 'No stories saved yet.') + ' The next update is ' + j.next_label + '.';
      if (!topic) { if (j.updating && poll++ < 30) setTimeout(load, 8000); return; }
      var rail = $('#rail'), items = j.items || [];
      rail.replaceChildren();
      items.forEach(function (d, i) { rail.append(card(d, i + 1)); });
      rail.scrollLeft = 0;
      var empty = $('#empty');
      empty.hidden = items.length > 0;
      empty.textContent = j.updating ? 'Collecting today\'s ' + topic.toLowerCase() + ' stories. This fills in shortly.'
                                     : 'No ' + topic.toLowerCase() + ' stories saved yet. The next update is ' + j.next_label + '.';
      if (j.updating && poll++ < 30) setTimeout(load, 8000);
    })
    .catch(function () { $('#st').textContent = 'Could not reach the server. The free host may be waking up; reload in a minute.'; });
}

function scrollRail(dir) { var r = $('#rail'); r.scrollBy({ left: dir * r.clientWidth * 0.8, behavior: 'smooth' }); }
$('#prev').onclick = function () { scrollRail(-1); };
$('#next').onclick = function () { scrollRail(1); };
load();
</script>
</body></html>"""


# =============================================================================
# Battle Lab: a world map of publicly reported weapon systems, by category.
# Names, status, range and speed are headline figures from public reporting only
# (marked reported / claimed / estimated); each system links to Wikipedia.
# It is a visual reference: rings show distance from a launcher, nothing more.
# Each country lists the systems it operates, whether built locally or bought (made_in names the
# selling country); inventories come from public sources such as the SIPRI arms-transfers
# database, defence ministry releases and defence news. Last reviewed October 2026.
# To update: edit the systems below (ask Claude to check current public sources).
# =============================================================================
# Countries: code -> (name, ISO numeric id used by the map, [lon, lat] of the map counter)
_COUNTRY = {
    "RUS": ("Russia", "643", [92, 62]), "CHN": ("China", "156", [103, 34]),
    "USA": ("United States", "840", [-99, 39]), "PRK": ("North Korea", "408", [126.5, 40.5]),
    "KOR": ("South Korea", "410", [128, 36.3]), "JPN": ("Japan", "392", [140, 37]),
    "TWN": ("Taiwan", "158", [121, 23.7]), "IND": ("India", "356", [78, 22]),
    "PAK": ("Pakistan", "586", [69.5, 30]), "IRN": ("Iran", "364", [54, 32]),
    "ISR": ("Israel", "376", [35, 31]), "TUR": ("Turkey", "792", [35, 39]),
    "ARE": ("United Arab Emirates", "784", [54.4, 23.8]), "SAU": ("Saudi Arabia", "682", [45, 24]),
    "UKR": ("Ukraine", "804", [31.5, 49]), "POL": ("Poland", "616", [19.2, 52]),
    "DEU": ("Germany", "276", [10.4, 51]), "FRA": ("France", "250", [2.5, 46.5]),
    "GBR": ("United Kingdom", "826", [-2, 53.5]), "ITA": ("Italy", "380", [12.5, 42.8]),
    "NOR": ("Norway", "578", [9, 61.5]), "SWE": ("Sweden", "752", [15, 62]),
    "SGP": ("Singapore", "702", [103.82, 1.36]), "MYS": ("Malaysia", "458", [102, 4]),
    "IDN": ("Indonesia", "360", [113.5, -1]), "PHL": ("Philippines", "608", [121, 15.5]),
    "VNM": ("Vietnam", "704", [105.8, 21]), "AUS": ("Australia", "036", [134, -25]),
    "BRA": ("Brazil", "076", [-51, -10]),
}


def _sys(name, kind, status, wiki, desc, rng, speed, ring=None, ring_note="", maker="", made_in=None):
    """ring: radius in km for the map ring (None = no published range, no ring); ring_note says which figure.
    maker: the company that makes it. made_in: the country it is bought from (None = built by the operator)."""
    return {"name": name, "kind": kind, "status": status, "wiki": wiki, "desc": desc, "range": rng,
            "speed": speed, "ring_km": ring, "ring_note": ring_note, "maker": maker, "made_in": made_in}


def _as(base, made_in=None, **changes):
    """The same system as operated by another country: a copy marked with where it was bought, plus changes."""
    s = dict(base)
    if "ring" in changes:   # same short name as in _sys()
        changes["ring_km"] = changes.pop("ring")
    unknown = set(changes) - set(base)
    assert not unknown, "unknown field(s) %s for %s" % (unknown, base["name"])
    s.update(changes)
    s["made_in"] = made_in
    return s


def _cty(code, *systems):
    name, iso, at = _COUNTRY[code]
    return {"name": name, "code": code, "iso": iso, "at": at, "systems": list(systems)}


# ---- systems used by several countries (defined once, then reused with each country's notes) ----
PATRIOT = _sys("Patriot", "Long-range surface-to-air", "In service", "MIM-104 Patriot",
               "US surface-to-air missile system, in service since the 1980s and used by many countries.",
               "Up to about 160 km against aircraft; shorter against ballistic missiles (reported)", "Over Mach 4 (reported)",
               160, "Upper figure, against aircraft", "Raytheon (RTX); PAC-3 MSE interceptors by Lockheed Martin")
PAC3 = _sys("Patriot PAC-3 MSE", "Lower-tier missile defence", "In service", "MIM-104 Patriot",
            "Hit-to-kill interceptor used by Patriot batteries against ballistic and cruise missiles.",
            "About 45 to 60 km (reported)", "Not published", 60, "Upper reported figure", "Lockheed Martin")
THAAD = _sys("THAAD", "High-altitude missile defence", "In service", "Terminal High Altitude Area Defense",
             "System that intercepts ballistic missiles near the top of the atmosphere, in US service since 2008.",
             "About 200 km (reported)", "Over Mach 8 (reported)", 200, "Reported figure", "Lockheed Martin")
SAMPT = _sys("SAMP/T", "Long-range surface-to-air", "In service", "SAMP/T",
             "French-Italian surface-to-air system using the Aster 30 missile.",
             "About 120 km; about 150 km for SAMP/T NG (reported)", "About Mach 4.5 (reported)",
             120, "In-service version", "Eurosam (MBDA and Thales)")
NASAMS = _sys("NASAMS", "Medium-range surface-to-air", "In service", "NASAMS",
              "Norwegian-US system that fires AMRAAM missiles from ground launchers.",
              "About 30 to 50 km depending on missile (reported)", "Not published",
              50, "Upper figure, AMRAAM-ER", "Kongsberg with Raytheon (RTX)")
IRIST = _sys("IRIS-T SLM", "Medium-range surface-to-air", "In service", "IRIS-T SLM",
             "German medium-range system, widely used in Ukraine since 2022.",
             "About 40 km (reported)", "About Mach 3 (reported)", 40, "Reported figure", "Diehl Defence")
SPYDER = _sys("Spyder", "Short/medium-range surface-to-air", "In service", "SPYDER",
              "Israeli truck-mounted system firing Python and Derby missiles, in short-range (SR) and medium-range (MR) versions.",
              "About 20 km (SR) to 35 km or more (MR) (reported)", "Not published", 20, "Short-range version, reported",
              "Rafael Advanced Defense Systems")
STARSTREAK = _sys("Starstreak", "Very short-range air defence", "In service", "Starstreak",
                  "British high-velocity missile for close-in air defence.",
                  "About 7 km (reported)", "Over Mach 3 (reported)", 7, "Reported figure", "Thales UK")
EXOCET = _sys("Exocet", "Ship / coastal anti-ship", "In service", "Exocet",
              "French sea-skimming anti-ship missile family; the MM40 is the ship-launched version.",
              "About 70 km (MM40 Block 2) to about 180 km (MM40 Block 3) (reported)", "High subsonic (reported)",
              180, "MM40 Block 3, reported", "MBDA")
NSM = _sys("NSM", "Coastal / ship anti-ship", "In service", "Naval Strike Missile",
           "Norwegian Naval Strike Missile, used on ships and in coastal batteries.",
           "Over 185 km (reported)", "High subsonic (reported)", 185, "Lower figure (reported as over 185 km)",
           "Kongsberg")
BRAHMOS = _sys("BrahMos", "Coastal / ship anti-ship", "In service", "BrahMos",
               "Indian-Russian supersonic cruise missile, fired from ships, aircraft and coastal launchers.",
               "About 290 km; extended versions about 450 km (reported)", "Mach 2.8 to 3 (reported)",
               450, "Extended-range version, reported", "BrahMos Aerospace (India–Russia joint venture)")
HARPOON = _sys("Harpoon", "Ship / coastal anti-ship", "In service", "Harpoon (missile)",
               "Long-serving US anti-ship missile, used on ships, aircraft and in coastal batteries.",
               "About 125 km; extended-range version about 250 km (reported)", "High subsonic (reported)",
               250, "Extended-range version", "Boeing")
TOMAHAWK = _sys("Tomahawk", "Land-attack cruise", "In service", "Tomahawk (missile)",
                "Long-range land-attack cruise missile in US service since the 1980s; now also fired from the Army's Typhon ground launcher.",
                "About 1,600 km (reported)", "About Mach 0.75 (subsonic, reported)", 1600, "Reported figure",
                "Raytheon (RTX)")
JASSM = _sys("JASSM-ER", "Air-launched cruise", "In service", "AGM-158 JASSM",
             "US stealthy air-launched cruise missile.",
             "About 900 to 1,000 km (reported)", "Subsonic (reported)", 900, "Lower reported figure", "Lockheed Martin")
ATACMS = _sys("ATACMS", "Short-range ballistic", "In service", "MGM-140 ATACMS",
              "Army Tactical Missile System, fired from HIMARS and M270 launchers since the 1990s.",
              "About 300 km (reported)", "About Mach 3 (reported)", 300, "Reported figure", "Lockheed Martin")
STORM = _sys("Storm Shadow", "Air-launched cruise", "In service", "Storm Shadow",
             "Anglo-French air-launched cruise missile (SCALP in France), in service since 2003.",
             "Over 250 km (export); about 550 km (reported)", "About Mach 0.8 (subsonic, reported)",
             550, "Upper reported figure", "MBDA")
S400 = _sys("S-400", "Long-range surface-to-air", "In service", "S-400 missile system",
            "Russian long-range surface-to-air system, in service since 2007, using several missile types.",
            "Up to about 400 km with its longest-range missile (claimed)", "About Mach 6 (48N6, reported)",
            400, "Claimed maximum with the longest-range missile", "Almaz-Antey")
ARROW3 = _sys("Arrow 3", "Missile defence in space", "In service", "Arrow 3",
              "Israeli-US interceptor that stops ballistic missiles above the atmosphere, in Israeli service since 2017.",
              "About 2,400 km (reported)", "Not published", 2400, "Reported figure", "Israel Aerospace Industries with Boeing")
BASTION = _sys("Bastion (P-800 Oniks)", "Coastal anti-ship", "In service", "P-800 Oniks",
               "Coastal defence system firing the supersonic P-800 Oniks missile.",
               "About 300 to 600 km (reported)", "About Mach 2.5 (reported)", 600, "Upper reported figure",
               "NPO Mashinostroyeniya")
RBS70 = _sys("RBS 70", "Very short-range air defence", "In service", "RBS 70",
             "Swedish laser-guided short-range missile system.",
             "About 8 km (reported)", "About Mach 2 (reported)", 8, "Reported figure", "Saab")


# Each category: group ("Strike" draws red rings, "Defence" blue), a short label for the
# header button, a one-line description for the picker, and its countries and systems.
BATTLE_LAB = {
    "Hypersonic": {
        "group": "Strike", "short": "Hypersonic",
        "blurb": "Weapons that fly faster than Mach 5 for much of their flight: glide vehicles and scramjet cruise missiles.",
        "countries": [
            _cty("RUS",
                 _sys("Avangard", "Glide vehicle", "In service", "Avangard (hypersonic glide vehicle)",
                      "Glide vehicle carried on an intercontinental ballistic missile. Russia declared it operational in 2019.",
                      "Intercontinental (launched on an ICBM)", "Up to Mach 20+ (claimed)",
                      5500, "Minimum for an intercontinental weapon; the real reach is longer", "NPO Mashinostroyeniya"),
                 _sys("Kinzhal", "Air-launched ballistic", "In service", "Kh-47M2 Kinzhal",
                      "Air-launched ballistic missile carried by MiG-31K aircraft, in Russian service since 2017.",
                      "About 480 km; up to 2,000 km including the aircraft's flight (claimed)", "Up to Mach 10 (claimed)",
                      2000, "Claimed reach including the carrier aircraft's flight"),
                 _sys("Zircon", "Cruise (scramjet)", "In service", "3M22 Zircon",
                      "Scramjet-powered cruise missile for ships and submarines, declared in service in 2023.",
                      "About 500 to 1,000 km (reported)", "Mach 8 to 9 (claimed)", 1000, "Upper reported figure",
                      "NPO Mashinostroyeniya"),
                 _sys("Oreshnik", "Intermediate-range ballistic, manoeuvring warheads", "In service", "Oreshnik (missile)",
                      "Intermediate-range missile that Russia describes as hypersonic. Declared on combat duty in December 2025, deployed to Belarus and used against Ukraine.",
                      "About 3,000 to 5,500 km (Russian statement; estimates vary)", "Up to Mach 10 (claimed)",
                      5500, "Upper Russian figure")),
            _cty("CHN",
                 _sys("DF-17", "Ballistic + glide vehicle", "In service", "DF-17",
                      "Medium-range ballistic missile carrying a hypersonic glide vehicle, first shown publicly in 2019.",
                      "About 1,800 to 2,500 km (reported)", "Mach 5 to 10 (reported)", 2500, "Upper reported figure",
                      "CASIC (10th Research Institute)"),
                 _sys("DF-27", "Ballistic + glide vehicle", "Reported in service", "DF-27",
                      "Longer-range ballistic missile with a glide vehicle, described in US Department of Defense reports.",
                      "About 5,000 to 8,000 km (US DoD estimate)", "Not published", 8000, "Upper US DoD estimate"),
                 _sys("YJ-21", "Anti-ship", "Reported in service", "YJ-21",
                      "Anti-ship ballistic missile launched from Type 055 destroyers, first shown publicly in 2022.",
                      "About 1,000 to 1,500 km (reported)", "Up to Mach 10 in the final phase (reported)", 1500, "Upper reported figure")),
            _cty("USA",
                 _sys("Dark Eagle", "Ground-launched glide", "Fielding", "Long-Range Hypersonic Weapon",
                      "The US Army's Long-Range Hypersonic Weapon. The first battery is at Joint Base Lewis-McChord; full fielding has not yet been declared and a second battery is due in late 2026.",
                      "About 2,775 km (reported)", "Over Mach 5 (reported)", 2775, "Reported figure",
                      "Lockheed Martin (integrator); glide body by Dynetics"),
                 _sys("Conventional Prompt Strike", "Sea-launched glide", "Tested", "Conventional Prompt Strike",
                      "The US Navy's version of the common hypersonic glide body, for destroyers and submarines. Flown in a joint Army–Navy test in March 2026.",
                      "Not published; uses the same glide body as Dark Eagle", "Over Mach 5 (reported)",
                      maker="Lockheed Martin; glide body by Dynetics"),
                 _sys("ARRW", "Air-launched glide", "Tested", "AGM-183 ARRW",
                      "Air-launched glide weapon tested from B-52 aircraft. Cancelled in 2023, then revived, with procurement funding planned from FY2026.",
                      "About 1,600 km (reported)", "Mach 6.5 to 8 on average (reported)", 1600, "Reported figure", "Lockheed Martin"),
                 _sys("HACM", "Cruise (scramjet)", "In development", "Hypersonic Attack Cruise Missile",
                      "Air-breathing scramjet cruise missile for the US Air Force, due for delivery by 2027; part of the joint US–Australia SCIFiRE programme.",
                      "Not published", "Over Mach 5 (planned)", maker="Raytheon (RTX) with Northrop Grumman (scramjet)")),
            _cty("PRK",
                 _sys("Hwasong-8", "Ballistic + glide vehicle", "Tested", "Hwasong-8",
                      "Ballistic missile with a glide vehicle, first tested by North Korea in 2021.",
                      "Not published", "Hypersonic (claimed)"),
                 _sys("Hwasong-16B", "Ballistic + glide vehicle", "Tested", "Hwasong-16B",
                      "Solid-fuel intermediate-range missile with a glide vehicle, tested in 2024.",
                      "Intermediate range, over 3,000 km (estimated)", "Hypersonic (claimed)",
                      3000, "Lower estimate (reported as over 3,000 km)")),
            _cty("KOR",
                 _sys("Hycore", "Cruise (scramjet)", "In development", "Hycore",
                      "South Korean ground-launched scramjet cruise missile; reported to have exceeded Mach 6 in a 2024 test.",
                      "Not published", "Over Mach 6 (reported)", maker="Agency for Defense Development")),
            _cty("IND",
                 _sys("LRAShM", "Anti-ship glide", "Tested", "Long Range Anti-Ship Missile (India)",
                      "Indian anti-ship glide missile. Its first long-range flight test was announced in November 2024.",
                      "About 1,500 km (stated by DRDO)", "Up to Mach 10 (reported)", 1500, "Figure stated by DRDO", "DRDO"),
                 _sys("HSTDV", "Scramjet demonstrator", "Tested", "Hypersonic Technology Demonstrator Vehicle",
                      "Indian scramjet technology demonstrator, flown in a scramjet test in 2020.",
                      "Not applicable (demonstrator)", "About Mach 6 in the 2020 test (reported)", maker="DRDO"),
                 _sys("BrahMos-II", "Cruise (scramjet)", "In development", "BrahMos-II",
                      "Planned hypersonic successor to the BrahMos cruise missile; the maker says it will not be exported.",
                      "About 1,500 km (planned, reported)", "Mach 7 to 8 (planned)", 1500, "Planned figure, reported",
                      "BrahMos Aerospace (India–Russia joint venture)")),
            _cty("IRN",
                 _sys("Fattah-1", "Ballistic, manoeuvring", "Claimed", "Fattah (missile)",
                      "Missile unveiled in 2023 that Iran describes as hypersonic. Independent analysts dispute the claim.",
                      "About 1,400 km (claimed)", "Mach 13 to 15 (claimed)", 1400, "Claimed figure", "IRGC Aerospace Force"),
                 _sys("Fattah-2", "Glide vehicle", "Claimed", "Fattah-2",
                      "Upgraded version unveiled in 2023 with a glide vehicle; analysts doubt it is ready for combat.",
                      "About 1,400 to 1,500 km (claimed)", "Up to Mach 15 (claimed)", 1500, "Upper claimed figure",
                      "IRGC Aerospace Force")),
            _cty("FRA",
                 _sys("V-MaX", "Glide demonstrator", "Tested", "V-MaX",
                      "French hypersonic glide vehicle demonstrator, first flight-tested in 2023.",
                      "Not published", "Over Mach 5 (reported)", maker="ArianeGroup")),
            _cty("JPN",
                 _sys("HVGP", "Ground-launched glide", "In development", "Hyper Velocity Gliding Projectile",
                      "Japanese truck-launched glide weapon for the defence of remote islands; deployment planned for 2026, with 2,000 km and 3,000 km versions to follow.",
                      "About 500 km for Block 1 (reported; some reports say 900 km)", "Over Mach 5 (reported)",
                      500, "Lower reported Block 1 figure", "Mitsubishi Heavy Industries"),
                 _sys("Hypersonic cruise missile", "Cruise (scramjet)", "In development", "Hypersonic Cruise Missile (Japan)",
                      "Japanese scramjet-powered cruise missile under development by the Ministry of Defense.",
                      "Not published", "Over Mach 5 (planned)")),
        ]},

    "Ballistic missiles": {
        "group": "Strike", "short": "Ballistic",
        "blurb": "Ground-launched rockets that fly a high arcing path, from short range up to intermediate range.",
        "countries": [
            _cty("RUS",
                 _sys("Iskander-M", "Short-range ballistic", "In service", "9K720 Iskander",
                      "Road-mobile short-range ballistic missile system, in Russian service since 2006.",
                      "About 500 km (official figure)", "Up to about Mach 6 (reported)", 500, "Official figure", "KB Mashinostroyeniya")),
            _cty("CHN",
                 _sys("DF-16", "Short/medium-range ballistic", "In service", "DF-16",
                      "Road-mobile ballistic missile first shown in 2015.",
                      "About 800 to 1,000 km (reported)", "Not published", 1000, "Upper reported figure"),
                 _sys("DF-21", "Medium-range ballistic", "In service", "DF-21",
                      "Road-mobile medium-range ballistic missile; the DF-21D variant is described as an anti-ship weapon.",
                      "About 1,500 to 1,750 km (reported)", "Up to Mach 10 (reported)", 1750, "Upper reported figure"),
                 _sys("DF-26", "Intermediate-range ballistic", "In service", "DF-26",
                      "Road-mobile intermediate-range ballistic missile, first shown publicly in 2015.",
                      "About 4,000 km (reported)", "Not published", 4000, "Reported figure")),
            _cty("USA",
                 _sys("PrSM", "Short-range ballistic", "In service", "Precision Strike Missile",
                      "The US Army's Precision Strike Missile, replacing ATACMS; first missiles delivered in 2023.",
                      "Over 500 km (Increment 1, reported)", "Not published", 500, "Lower figure (reported as over 500 km)",
                      "Lockheed Martin"),
                 ATACMS),
            _cty("AUS",
                 _sys("PrSM", "Short-range ballistic", "In service", "Precision Strike Missile",
                      "Australia is a co-development partner. Its army first fired PrSM from HIMARS in July 2025, hitting a target more than 300 km away.",
                      "Over 500 km (Increment 1, reported)", "Not published", 500, "Lower figure (reported as over 500 km)",
                      "Lockheed Martin (co-developed with Australia)", "United States")),
            _cty("PRK",
                 _sys("Hwasong-11", "Short-range ballistic", "Reported in service", "Hwasong-11",
                      "Solid-fuel short-range ballistic missile family (also known as KN-23), first tested in 2019.",
                      "About 450 to 690 km (reported)", "Not published", 690, "Upper reported figure"),
                 _sys("Hwasong-12", "Intermediate-range ballistic", "Reported in service", "Hwasong-12",
                      "Liquid-fuel intermediate-range ballistic missile, first flown successfully in 2017.",
                      "About 4,500 km (estimated)", "Not published", 4500, "Estimated figure")),
            _cty("KOR",
                 _sys("Hyunmoo-2", "Short-range ballistic", "In service", "Hyunmoo-2",
                      "South Korean short-range ballistic missile family, with variants of increasing range.",
                      "About 300 to 800 km depending on variant (reported)", "Not published", 800, "Longest-range variant, reported",
                      "Agency for Defense Development"),
                 _sys("Hyunmoo-4", "Short-range ballistic", "Reported in service", "Hyunmoo-4",
                      "Heavy South Korean ballistic missile first tested in 2020; officials have said little publicly.",
                      "About 800 km (reported)", "Not published", 800, "Reported figure", "Agency for Defense Development")),
            _cty("TWN", _as(ATACMS, "United States",
                            desc="Taiwan's first ATACMS arrived in January 2025, fired from HIMARS launchers; a larger order was approved in December 2025.")),
            _cty("UKR", _as(ATACMS, "United States",
                            desc="Supplied by the United States since 2023 and used against targets in Russian-occupied territory.")),
            _cty("IND",
                 _sys("Prithvi-II", "Short-range ballistic", "In service", "Prithvi (missile)",
                      "Indian short-range ballistic missile, in service since the early 2000s.",
                      "About 350 km (reported)", "Not published", 350, "Reported figure", "DRDO; built by Bharat Dynamics"),
                 _sys("Pralay", "Short-range quasi-ballistic", "Tested", "Pralay (missile)",
                      "Indian manoeuvring short-range missile; first-phase tests completed and induction approved.",
                      "About 150 to 500 km (reported)", "Not published", 500, "Upper reported figure", "DRDO"),
                 _sys("Agni-P", "Medium-range ballistic", "Tested", "Agni-P",
                      "Canister-launched medium-range ballistic missile, first tested in 2021.",
                      "About 1,000 to 2,000 km (reported)", "Not published", 2000, "Upper reported figure", "DRDO")),
            _cty("PAK",
                 _sys("Abdali", "Short-range ballistic", "In service", "Abdali-I",
                      "Pakistani short-range ballistic missile (Hatf-II), test-fired again in May 2025.",
                      "About 450 km (claimed; independent estimates 180 to 200 km)", "Not published", 450, "Claimed figure"),
                 _sys("Ghauri", "Medium-range ballistic", "In service", "Ghauri (missile)",
                      "Liquid-fuel medium-range ballistic missile (Hatf-V).",
                      "About 1,300 km (official figure)", "Not published", 1300, "Official figure"),
                 _sys("Shaheen-III", "Medium-range ballistic", "Reported in service", "Shaheen-III",
                      "Pakistani solid-fuel medium-range ballistic missile, first tested in 2015.",
                      "About 2,750 km (claimed)", "Not published", 2750, "Claimed figure")),
            _cty("IRN",
                 _sys("Fateh-110", "Short-range ballistic", "In service", "Fateh-110",
                      "Iranian solid-fuel short-range ballistic missile family.",
                      "About 300 km (reported)", "Over Mach 3 (reported)", 300, "Reported figure"),
                 _sys("Zolfaghar", "Short-range ballistic", "In service", "Zolfaghar (missile)",
                      "Longer-range development of the Fateh-110 family, used in combat since 2017.",
                      "About 700 km (claimed)", "Not published", 700, "Claimed figure"),
                 _sys("Kheibar Shekan", "Medium-range ballistic", "In service", "Kheibar Shekan",
                      "Iranian solid-fuel medium-range ballistic missile, unveiled in 2022.",
                      "About 1,450 km (claimed)", "Not published", 1450, "Claimed figure"),
                 _sys("Sejjil", "Medium-range ballistic", "In service", "Sejjil",
                      "Two-stage solid-fuel medium-range ballistic missile.",
                      "About 2,000 km (reported)", "Not published", 2000, "Reported figure")),
            _cty("ISR",
                 _sys("LORA", "Short-range ballistic", "In service", "LORA (missile)",
                      "Israeli precision ballistic missile, offered for land and ship launch.",
                      "About 400 km (reported)", "Not published", 400, "Reported figure", "Israel Aerospace Industries")),
            _cty("TUR",
                 _sys("Tayfun", "Short-range ballistic", "In service", "Tayfun (missile)",
                      "Turkish short-range ballistic missile; a much larger Block 4 version was unveiled in 2025 without a published range.",
                      "About 560 km for Block 1 (reported from tests)", "Not published", 560, "Block 1, reported", "Roketsan"),
                 _sys("Bora", "Short-range ballistic", "In service", "Bora (missile)",
                      "Turkish tactical ballistic missile, first used in combat in 2022.",
                      "About 280 km (reported)", "Not published", 280, "Reported figure", "Roketsan")),
            _cty("IDN",
                 _sys("Khan", "Short-range ballistic", "Fielding", "Bora (missile)",
                      "Export version of Turkey's Bora. Indonesia is the first Southeast Asian ballistic missile operator: the first batch was stationed in East Kalimantan in 2025, with a second batch expected in 2026.",
                      "About 280 km (reported)", "Not published", 280, "Reported figure", "Roketsan", "Turkey")),
        ]},

    "Cruise missiles": {
        "group": "Strike", "short": "Cruise",
        "blurb": "Missiles that fly low and level like a small aircraft, mostly below the speed of sound, to strike land targets.",
        "countries": [
            _cty("USA", TOMAHAWK, JASSM),
            _cty("RUS",
                 _sys("Kalibr", "Land-attack cruise", "In service", "3M-54 Kalibr",
                      "Russian cruise missile family fired from ships and submarines; the land-attack version is the 3M-14.",
                      "About 1,500 to 2,500 km (reported)", "Subsonic (reported)", 2500, "Upper reported figure", "Novator"),
                 _sys("Kh-101", "Air-launched cruise", "In service", "Kh-101",
                      "Long-range air-launched cruise missile carried by Russian strategic bombers.",
                      "About 2,500 to 2,800 km (reported)", "Subsonic (reported)", 2800, "Upper reported figure", "Raduga"),
                 _sys("Iskander-K", "Ground-launched cruise", "In service", "9K720 Iskander",
                      "Cruise missile version of the Iskander system, fired from the same road-mobile launchers.",
                      "About 500 km (official figure)", "Subsonic (reported)", 500, "Official figure", "Novator")),
            _cty("CHN",
                 _sys("CJ-10", "Ground-launched cruise", "In service", "CJ-10 (missile)",
                      "Chinese ground-launched land-attack cruise missile, first shown publicly in 2009.",
                      "About 1,500 to 2,000 km (reported)", "Subsonic (reported)", 2000, "Upper reported figure"),
                 _sys("CJ-100", "Ground-launched cruise", "In service", "CJ-100",
                      "Larger, faster Chinese land-attack cruise missile, first shown in 2019.",
                      "Over 2,000 km (reported)", "Supersonic (reported)", 2000, "Lower figure (reported as over 2,000 km)")),
            _cty("GBR", STORM,
                 _as(TOMAHAWK, "United States", desc="Fired by Royal Navy submarines.")),
            _cty("FRA",
                 _as(STORM, None, name="SCALP", desc="French name for the Storm Shadow air-launched cruise missile."),
                 _sys("MdCN", "Ship / submarine land-attack cruise", "In service", "MdCN",
                      "French naval cruise missile fired from frigates and submarines, in service since 2017.",
                      "About 1,000 km (reported)", "Subsonic (reported)", 1000, "Reported figure", "MBDA")),
            _cty("DEU",
                 _sys("Taurus", "Air-launched cruise", "In service", "Taurus KEPD 350",
                      "German-Swedish air-launched cruise missile.",
                      "Over 500 km (reported)", "Subsonic (reported)", 500, "Lower figure (reported as over 500 km)",
                      "Taurus Systems (MBDA Germany and Saab)")),
            _cty("UKR",
                 _as(STORM, "United Kingdom", desc="Supplied by the UK (and SCALP by France) since 2023, launched from Ukrainian aircraft.")),
            _cty("IND",
                 _sys("LRLACM", "Ground-launched cruise", "Tested", "Long Range Land Attack Cruise Missile",
                      "Indian long-range land-attack cruise missile developed from the Nirbhay programme, flight-tested from 2023.",
                      "About 1,000 km or more (reported)", "Subsonic (reported)", 1000, "Lower reported figure", "DRDO"),
                 _as(STORM, "France", name="SCALP", desc="Carried by the Indian Air Force's Rafale fighters.")),
            _cty("PAK",
                 _sys("Babur", "Ground-launched cruise", "In service", "Babur (cruise missile)",
                      "Pakistani subsonic cruise missile family; the Babur-1B was tested in 2021.",
                      "About 700 km; over 900 km for Babur-1B (reported)", "Subsonic (reported)", 900, "Babur-1B, reported")),
            _cty("KOR",
                 _sys("Hyunmoo-3", "Ground-launched cruise", "In service", "Hyunmoo-3",
                      "South Korean land-attack cruise missile family with versions of increasing range.",
                      "About 500 to 1,500 km depending on version (reported)", "Subsonic (reported)", 1500, "Longest-range version, reported",
                      "Agency for Defense Development")),
            _cty("JPN",
                 _as(TOMAHAWK, "United States", status="Fielding",
                     desc="Japan is buying 400 Tomahawks (deliveries 2025–2027). The destroyer Chokai became the first equipped in March 2026 and fired one in July 2026.")),
            _cty("TWN",
                 _sys("Hsiung Feng IIE", "Ground-launched cruise", "In service", "Hsiung Feng IIE",
                      "Taiwanese land-attack cruise missile, fired from road-mobile launchers.",
                      "About 600 to 1,200 km (reported)", "Subsonic (reported)", 1200, "Upper reported figure", "NCSIST")),
            _cty("TUR",
                 _sys("SOM", "Air-launched cruise", "In service", "SOM (missile)",
                      "Turkish stealthy air-launched cruise missile.",
                      "About 250 km (reported)", "Subsonic (reported)", 250, "Reported figure", "Roketsan")),
            _cty("IRN",
                 _sys("Hoveyzeh", "Ground-launched cruise", "In service", "Hoveyzeh (cruise missile)",
                      "Iranian ground-launched land-attack cruise missile, unveiled in 2019.",
                      "About 1,350 km (claimed)", "Subsonic (claimed)", 1350, "Claimed figure"),
                 _sys("Paveh", "Ground-launched cruise", "Claimed", "Paveh (missile)",
                      "Iranian long-range cruise missile unveiled in 2023.",
                      "About 1,650 km (claimed)", "Subsonic (claimed)", 1650, "Claimed figure")),
            _cty("AUS",
                 _as(TOMAHAWK, "United States", desc="Bought for Royal Australian Navy destroyers; first fired by Australia in 2024."),
                 _as(JASSM, "United States", desc="Carried by RAAF Super Hornets and, in future, F-35A fighters; the government cites a 900 km reach.")),
            _cty("ARE",
                 _as(STORM, "France", name="Black Shaheen", ring=250, ring_note="Lower estimated figure",
                     range="About 250 to 300 km (estimated; export version)",
                     desc="Export version of Storm Shadow/SCALP, carried by UAE Mirage 2000-9 fighters since about 2008.")),
            _cty("BRA",
                 _sys("MTC-300", "Ground-launched cruise", "Tested", "AV-TM 300",
                      "Brazil's first home-built cruise missile, fired from ASTROS launchers; final tests before service validation.",
                      "About 300 km (reported)", "Subsonic (reported)", 300, "Reported figure", "Avibras")),
        ]},

    "Anti-ship missiles": {
        "group": "Strike", "short": "Anti-ship",
        "blurb": "Missiles built to hit ships at sea, from ships, aircraft and coastal launchers on land.",
        "countries": [
            _cty("NOR", NSM),
            _cty("USA", HARPOON,
                 _sys("LRASM", "Air / ship anti-ship", "In service", "AGM-158C LRASM",
                      "Stealthy long-range anti-ship missile derived from JASSM-ER.",
                      "Over 370 km (reported)", "Subsonic (reported)", 370, "Lower figure (reported as over 370 km)", "Lockheed Martin"),
                 _as(NSM, "Norway", desc="Used by the US Navy and by the Marine Corps' NMESIS coastal launchers.")),
            _cty("RUS", BASTION,
                 _sys("Bal (Kh-35)", "Coastal anti-ship", "In service", "Kh-35",
                      "Truck-mounted coastal system firing the subsonic Kh-35 missile.",
                      "About 120 to 260 km depending on version (reported)", "High subsonic (reported)", 260, "Kh-35U, reported",
                      "Tactical Missiles Corporation")),
            _cty("CHN",
                 _sys("YJ-12", "Ship / air / coastal anti-ship", "In service", "YJ-12",
                      "Chinese supersonic anti-ship missile, with ship, aircraft and coastal versions.",
                      "About 400 km (reported)", "Up to Mach 3 (reported)", 400, "Reported figure"),
                 _sys("YJ-18", "Ship / submarine anti-ship", "In service", "YJ-18",
                      "Chinese anti-ship missile with a supersonic final sprint, fired from ships and submarines.",
                      "About 220 to 540 km (reported)", "Up to Mach 3 in the final phase (reported)", 540, "Upper reported figure")),
            _cty("FRA", EXOCET),
            _cty("ITA",
                 _sys("Teseo / Otomat", "Ship anti-ship", "In service", "Otomat",
                      "Italian-French anti-ship missile family (Teseo in Italy).",
                      "About 180 km (reported)", "High subsonic (reported)", 180, "Reported figure", "MBDA Italia")),
            _cty("SWE",
                 _sys("RBS-15", "Ship / coastal anti-ship", "In service", "RBS-15",
                      "Swedish anti-ship missile used from ships, aircraft and coastal batteries.",
                      "Over 200 km for Mk3 (reported)", "High subsonic (reported)", 200, "Lower figure, Mk3", "Saab")),
            _cty("ISR",
                 _sys("Gabriel V", "Ship anti-ship", "In service", "Gabriel (missile)",
                      "Latest Israeli Gabriel anti-ship missile.",
                      "Over 200 km (reported)", "High subsonic (reported)", 200, "Lower figure (reported as over 200 km)",
                      "Israel Aerospace Industries")),
            _cty("TUR",
                 _sys("Atmaca", "Ship / coastal anti-ship", "In service", "Atmaca",
                      "Turkish anti-ship missile, also built in a land-based (Kara Atmaca) version.",
                      "Over 220 km (reported)", "High subsonic (reported)", 220, "Lower figure (reported as over 220 km)", "Roketsan")),
            _cty("IND", BRAHMOS),
            _cty("JPN",
                 _sys("Type 12 (improved)", "Coastal anti-ship / land attack", "In service", "Type 12 surface-to-ship missile",
                      "Upgraded truck-mounted Type 12, deployed from March 2026 at Camp Kengun and Camp Fuji; the original version reaches about 200 km.",
                      "About 1,000 km (reported)", "Subsonic (reported)", 1000, "Improved version, reported", "Mitsubishi Heavy Industries")),
            _cty("KOR",
                 _sys("Haeseong", "Ship anti-ship", "In service", "SSM-700K Haeseong",
                      "South Korean ship-launched anti-ship missile.",
                      "About 150 km (reported)", "High subsonic (reported)", 150, "Reported figure", "LIG Nex1")),
            _cty("TWN",
                 _sys("Hsiung Feng III", "Ship / coastal anti-ship", "In service", "Hsiung Feng III",
                      "Taiwanese supersonic anti-ship missile, on ships and coastal launchers.",
                      "About 150 to 400 km (reported)", "Up to Mach 3 (reported)", 400, "Upper reported figure", "NCSIST")),
            _cty("UKR",
                 _sys("Neptune", "Coastal anti-ship", "In service", "R-360 Neptune",
                      "Ukrainian truck-mounted coastal anti-ship missile, in service since 2021; longer-range land-attack versions are reported.",
                      "About 280 km (reported)", "Subsonic (reported)", 280, "Original version, reported", "Luch Design Bureau")),
            _cty("IRN",
                 _sys("Noor", "Coastal / ship anti-ship", "In service", "Noor (missile)",
                      "Iranian anti-ship missile derived from the Chinese C-802.",
                      "About 120 to 170 km (reported)", "Subsonic (reported)", 170, "Upper reported figure"),
                 _sys("Qader", "Coastal / ship anti-ship", "In service", "Qader (missile)",
                      "Longer-range Iranian development of the Noor.",
                      "About 200 to 300 km (claimed)", "Subsonic (claimed)", 300, "Upper claimed figure")),
            _cty("AUS", _as(NSM, "Norway", desc="Replacing Harpoon on Royal Australian Navy frigates and destroyers; first fired by Australia in 2024.")),
            _cty("SGP",
                 _sys("Blue Spear", "Ship anti-ship / land attack", "Fielding", "Gabriel (missile)",
                      "Singapore–Israel missile for targets at sea and on land, replacing Harpoon on the Formidable-class frigates; integration was reported complete in 2026.",
                      "About 290 km (IAI and Janes)", "High subsonic (reported)", 290, "Figure stated by IAI",
                      "Proteus Advanced Systems (ST Engineering and Israel Aerospace Industries)"),
                 _as(HARPOON, "United States", desc="Carried by Republic of Singapore Navy frigates and corvettes; being replaced by Blue Spear on the frigates.")),
            _cty("MYS",
                 _as(EXOCET, "France", ring=70, ring_note="MM40 Block 2 version",
                     desc="MM40 Block 2 on the Navy's Lekiu-class frigates and Kasturi-class corvettes. Malaysia's planned NSM purchase was cancelled by Norway in May 2026.")),
            _cty("IDN",
                 _as(EXOCET, "France", desc="MM40 Block 3 on the Navy's Martadinata-class frigates (eight each); older versions on other ships. A 2024 effort to restock was delayed by licensing issues."),
                 _sys("C-705", "Ship anti-ship", "In service", "C-704",
                      "Chinese light anti-ship missile carried by the Navy's fast attack craft, bought in 2016 after an earlier local-production plan stalled.",
                      "About 140 km (reported)", "High subsonic (reported)", 140, "Reported figure", "CASIC", "China"),
                 _as(BRAHMOS, "India", status="On order", ring=290, ring_note="Export version, reported",
                     range="About 290 km (export version, reported)",
                     desc="Contract signed on 7 July 2026 during the Indian prime minister's visit to Jakarta; quantity, value and delivery dates have not been published."),
                 _sys("Atmaca", "Ship / coastal anti-ship", "On order", "Atmaca",
                      "Ordered from Turkey with local assembly planned in Indonesia; first delivery to the Army was reported in April 2026.",
                      "Over 220 km (reported)", "High subsonic (reported)", 220, "Lower figure (reported as over 220 km)",
                      "Roketsan", "Turkey")),
            _cty("PHL",
                 _as(BRAHMOS, "India", ring=290, ring_note="Export version, reported",
                     range="About 290 km (export version, reported)",
                     desc="Philippine Marine Corps coastal batteries in western Luzon; three batteries ordered in 2022, the first unveiled in November 2025.")),
            _cty("VNM",
                 _as(BASTION, "Russia", desc="Vietnam's coastal defence system, bought from Russia."),
                 _as(BRAHMOS, "India", status="On order", ring=290, ring_note="Export version, reported",
                     range="About 290 km (export version, reported)",
                     desc="India's defence secretary said in May 2026 that the deal was signed; neither government has formally announced it.")),
        ]},

    "Long-range air & missile defence": {
        "group": "Defence", "short": "Long-range AD",
        "blurb": "Surface-to-air systems that defend large areas against aircraft, cruise missiles and some ballistic missiles.",
        "countries": [
            _cty("USA", PATRIOT,
                 _sys("SM-6", "Ship-launched long-range air defence", "In service", "RIM-174 Standard ERAM",
                      "US Navy interceptor against aircraft, cruise missiles and ballistic missiles in their final phase.",
                      "About 240 km (reported; newer versions longer)", "About Mach 3.5 (reported)", 240, "Reported figure", "Raytheon (RTX)")),
            _cty("RUS", S400,
                 _sys("S-500", "Long-range air & missile defence", "Reported in service", "S-500 missile system",
                      "Russian successor to the S-400, intended also against ballistic missiles; first units reported in 2021.",
                      "About 600 km (claimed)", "Not published", 600, "Claimed figure", "Almaz-Antey"),
                 _sys("S-300V4", "Long-range surface-to-air", "In service", "S-300V",
                      "Russian Army long-range system, also used against ballistic missiles.",
                      "Up to about 400 km (claimed)", "Not published", 400, "Claimed figure", "Almaz-Antey")),
            _cty("CHN",
                 _sys("HQ-9", "Long-range surface-to-air", "In service", "HQ-9",
                      "Chinese long-range surface-to-air missile system, on land and on ships.",
                      "About 200 to 250 km (reported)", "About Mach 4 (reported)", 250, "Upper reported figure")),
            _cty("FRA", SAMPT),
            _cty("ITA", _as(SAMPT, None, desc="Italy co-developed SAMP/T with France and operates its own batteries.")),
            _cty("ISR",
                 _sys("David's Sling", "Medium/long-range air & missile defence", "In service", "David's Sling",
                      "Israeli system for the middle layer of its air defence, between Iron Dome and Arrow.",
                      "About 40 to 300 km (reported)", "Not published", 300, "Upper reported figure",
                      "Rafael Advanced Defense Systems with Raytheon (RTX)"),
                 _sys("Barak MX", "Medium/long-range air defence", "In service", "Barak 8",
                      "Israeli naval and land air defence system with several interceptor types.",
                      "About 150 km with the extended-range interceptor (reported)", "Not published", 150, "Extended-range interceptor, reported",
                      "Israel Aerospace Industries")),
            _cty("TUR",
                 _sys("Siper", "Long-range surface-to-air", "In service", "SİPER",
                      "Turkish long-range system; Block 1 entered service in late 2024, with longer-range blocks in development.",
                      "Over 100 km (Block 1); about 150 km for Block 2 (reported)", "Not published", 100, "Block 1 figure",
                      "Aselsan and Roketsan"),
                 _as(S400, "Russia", desc="Bought from Russia in 2019.")),
            _cty("IND", _as(S400, "Russia", desc="India's first squadrons were delivered from 2021.")),
            _cty("PAK",
                 _sys("HQ-9/P", "Long-range surface-to-air", "In service", "HQ-9",
                      "Pakistani version of China's HQ-9, in service since 2021.",
                      "About 125 km (reported)", "Not published", 125, "Reported figure", "China Precision Machinery Import-Export Corp.", "China")),
            _cty("IRN",
                 _sys("Bavar-373", "Long-range surface-to-air", "In service", "Bavar-373",
                      "Iranian long-range surface-to-air system, unveiled in 2019.",
                      "About 200 to 300 km (claimed)", "Not published", 300, "Upper claimed figure"),
                 _sys("Khordad-15", "Medium/long-range surface-to-air", "In service", "Khordad 15",
                      "Iranian surface-to-air system unveiled in 2019.",
                      "About 120 km (claimed)", "Not published", 120, "Claimed figure")),
            _cty("TWN",
                 _sys("Tien Kung III", "Long-range surface-to-air", "In service", "Sky Bow III",
                      "Taiwanese long-range system that replaced the US Hawk; deployed along the northern coast.",
                      "About 200 km (reported)", "Not published", 200, "Reported figure", "NCSIST"),
                 _as(PATRIOT, "United States", desc="Part of Taiwan's layered 'T-Dome' air defence; more PAC-3 MSE interceptors are on order.")),
            _cty("JPN", _as(PATRIOT, "United States", desc="Licence-built in Japan by Mitsubishi Heavy Industries.")),
            _cty("KOR", _as(PATRIOT, "United States", desc="South Korea's lower-tier missile defence, alongside its own Cheongung and L-SAM.")),
            _cty("DEU", _as(PATRIOT, "United States", desc="Germany operates Patriot and has supplied batteries to Ukraine.")),
            _cty("POL", _as(PATRIOT, "United States", desc="Poland's 'Wisła' programme; first batteries operational from 2023.")),
            _cty("SWE", _as(PATRIOT, "United States", desc="Sweden's long-range air defence since 2021.")),
            _cty("UKR",
                 _as(PATRIOT, "United States", desc="Supplied by the US, Germany and others since 2023; used against Russian ballistic missiles."),
                 _as(SAMPT, "France and Italy", desc="Supplied by France and Italy in 2023.")),
            _cty("SAU", _as(PATRIOT, "United States", desc="Saudi Arabia's main air defence system.")),
            _cty("ARE", _as(PATRIOT, "United States", desc="Works alongside the UAE's THAAD batteries.")),
            _cty("SGP",
                 _as(SAMPT, "France and Italy", name="Aster 30 SAMP/T", ring=70, ring_note="Singapore MINDEF figure against fighter aircraft",
                     range="Up to 70 km against fighter aircraft (Singapore MINDEF)",
                     desc="Singapore's long-range air defence; replaced I-Hawk, standing operations since August 2020 and fully operational.")),
        ]},

    "Ballistic missile defence": {
        "group": "Defence", "short": "Missile defence",
        "blurb": "Interceptors built to stop ballistic missiles, high in the atmosphere or in space.",
        "countries": [
            _cty("USA", THAAD,
                 _sys("SM-3", "Missile defence in space", "In service", "RIM-161 Standard Missile 3",
                      "Interceptor that hits ballistic missiles in space; fired from ships and from Aegis Ashore land sites.",
                      "Up to about 1,200 km (Block IIA, reported)", "Not published", 1200, "Block IIA, reported",
                      "Raytheon (RTX); Block IIA co-developed with Mitsubishi Heavy Industries"),
                 _sys("GMD", "Homeland missile defence", "In service", "Ground-Based Midcourse Defense",
                      "Interceptors in Alaska and California designed to stop long-range missiles in space.",
                      "Not published", "Not published", maker="Boeing"),
                 PAC3),
            _cty("ISR", ARROW3,
                 _sys("Arrow 2", "High-altitude missile defence", "In service", "Arrow (missile family)",
                      "Earlier Arrow interceptor that stops ballistic missiles in the upper atmosphere.",
                      "About 90 to 150 km (reported)", "About Mach 9 (reported)", 150, "Upper reported figure",
                      "Israel Aerospace Industries with Boeing")),
            _cty("DEU", _as(ARROW3, "Israel", desc="Germany's first battery reached initial operational capability on 3 December 2025 at Holzdorf; a second site is planned.")),
            _cty("ARE", _as(THAAD, "United States", desc="In UAE service since about 2016; used in combat against a ballistic missile in January 2022.")),
            _cty("SAU", _as(THAAD, "United States", desc="First Saudi battery commissioned in July 2025; seven batteries ordered.")),
            _cty("JPN",
                 _as(PAC3, "United States", desc="Licence-built in Japan by Mitsubishi Heavy Industries."),
                 _sys("SM-3 Block IIA", "Missile defence in space", "In service", "RIM-161 Standard Missile 3",
                      "Fired from Japan's Aegis destroyers; co-developed by the US and Japan.",
                      "Up to about 1,200 km (reported)", "Not published", 1200, "Reported figure",
                      "Raytheon (RTX) with Mitsubishi Heavy Industries", "United States")),
            _cty("KOR",
                 _sys("L-SAM", "Upper-tier missile defence", "In development", "L-SAM",
                      "South Korean long-range interceptor; mass production from 2025, deployment planned from 2028.",
                      "About 150 km (Block 1, reported)", "Not published", 150, "Block 1, reported", "LIG Nex1"),
                 _as(PAC3, "United States", desc="South Korea's lower-tier ballistic missile defence.")),
            _cty("CHN",
                 _sys("HQ-19", "Missile defence in space", "Reported in service", "HQ-19",
                      "Chinese interceptor reported to target ballistic missiles in the midcourse phase.",
                      "Not published", "Not published")),
            _cty("RUS",
                 _sys("A-235", "Homeland missile defence", "Reported in service", "A-235 anti-ballistic missile system",
                      "Missile defence system around Moscow, replacing the older A-135.",
                      "Not published", "Not published", maker="Almaz-Antey")),
            _cty("IND",
                 _sys("PDV / AD-1", "Ballistic missile defence", "Tested", "Indian Ballistic Missile Defence Programme",
                      "Interceptors of India's two-tier ballistic missile defence programme.",
                      "Not published", "Not published", maker="DRDO")),
        ]},

    "Short/medium-range air defence": {
        "group": "Defence", "short": "Short-range AD",
        "blurb": "Systems that protect cities, bases and troops against rockets, drones, cruise missiles and aircraft at closer range.",
        "countries": [
            _cty("ISR",
                 _sys("Iron Dome", "Short-range air defence", "In service", "Iron Dome",
                      "Israeli system against rockets, artillery and drones, in service since 2011.",
                      "About 4 to 70 km (reported)", "About Mach 2 (reported)", 70, "Upper reported figure",
                      "Rafael Advanced Defense Systems"),
                 SPYDER),
            _cty("NOR", NASAMS),
            _cty("USA",
                 _sys("Stinger", "Very short-range air defence", "In service", "FIM-92 Stinger",
                      "Shoulder-fired and vehicle-mounted infrared-guided missile.",
                      "About 8 km (reported)", "About Mach 2.5 (reported)", 8, "Reported figure", "Raytheon (RTX)"),
                 _as(NASAMS, "Norway", desc="Protects Washington, D.C.")),
            _cty("DEU", IRIST),
            _cty("RUS",
                 _sys("Pantsir", "Short-range air defence", "In service", "Pantsir missile system",
                      "Russian truck-mounted gun and missile system for close-in air defence.",
                      "About 20 km; about 30 km for Pantsir-SM (reported)", "Not published", 20, "In-service missiles, reported",
                      "KBP Instrument Design Bureau"),
                 _sys("Buk-M3", "Medium-range surface-to-air", "In service", "Buk missile system",
                      "Latest version of the Russian Buk medium-range surface-to-air system.",
                      "Up to about 70 km (reported)", "Not published", 70, "Reported figure", "Almaz-Antey"),
                 _sys("Tor-M2", "Short-range air defence", "In service", "Tor missile system",
                      "Tracked short-range system against aircraft, drones and precision munitions.",
                      "About 15 km (reported)", "Not published", 15, "Reported figure", "Almaz-Antey")),
            _cty("CHN",
                 _sys("HQ-16", "Medium-range surface-to-air", "In service", "HQ-16",
                      "Chinese medium-range surface-to-air system, on land and on ships.",
                      "About 40 km; about 70 km for HQ-16B (reported)", "Not published", 40, "Original version, reported"),
                 _sys("HQ-17", "Short-range air defence", "In service", "HQ-17",
                      "Chinese tracked short-range system, developed from the Russian Tor.",
                      "About 15 km (reported)", "Not published", 15, "Reported figure")),
            _cty("GBR",
                 _sys("Sky Sabre", "Short/medium-range air defence", "In service", "Sky Sabre",
                      "British Army system using the CAMM missile, in service since 2021.",
                      "Over 25 km (reported)", "Not published", 25, "Lower figure (reported as over 25 km)", "MBDA"),
                 STARSTREAK),
            _cty("FRA",
                 _sys("VL MICA", "Short-range air defence", "In service", "MICA (missile)",
                      "Vertically launched version of the MICA missile, for ships and land.",
                      "About 20 km (reported)", "Not published", 20, "Reported figure", "MBDA"),
                 _sys("Mistral", "Very short-range air defence", "In service", "Mistral (missile)",
                      "French infrared-guided short-range missile.",
                      "About 6 km (reported)", "About Mach 2.5 (reported)", 6, "Reported figure", "MBDA")),
            _cty("SWE", RBS70),
            _cty("POL",
                 _sys("Piorun", "Very short-range air defence", "In service", "Piorun (missile)",
                      "Polish shoulder-fired missile, widely used in Ukraine.",
                      "About 6.5 km (reported)", "Not published", 6.5, "Reported figure", "Mesko")),
            _cty("TUR",
                 _sys("Hisar-O", "Medium-range surface-to-air", "In service", "Hisar (missile)",
                      "Turkish medium-range surface-to-air system.",
                      "About 25 km (reported)", "Not published", 25, "Reported figure", "Roketsan and Aselsan")),
            _cty("IND",
                 _sys("Akash", "Short/medium-range surface-to-air", "In service", "Akash (missile)",
                      "Indian surface-to-air system; the newer Akash-NG has longer range.",
                      "About 25 to 30 km; about 70 km for Akash-NG (reported)", "About Mach 2.5 (reported)", 30, "In-service version",
                      "DRDO; built by Bharat Dynamics"),
                 _sys("MRSAM", "Medium-range surface-to-air", "In service", "Barak 8",
                      "Indian Army and Air Force version of Barak 8, co-developed with Israel.",
                      "About 70 km (reported)", "Not published", 70, "Reported figure", "DRDO with Israel Aerospace Industries")),
            _cty("KOR",
                 _sys("Cheongung II (KM-SAM)", "Medium-range surface-to-air", "In service", "M-SAM",
                      "South Korean medium-range system that can also intercept ballistic missiles; exported to the UAE and Saudi Arabia.",
                      "About 40 km (reported)", "Not published", 40, "Reported figure", "LIG Nex1")),
            _cty("JPN",
                 _sys("Type 03 Chu-SAM", "Medium-range surface-to-air", "In service", "Type 03 Chū-SAM",
                      "Japanese truck-mounted medium-range surface-to-air system.",
                      "About 50 km (reported)", "Not published", 50, "Reported figure")),
            _cty("UKR",
                 _as(IRIST, "Germany", desc="Supplied by Germany since 2022."),
                 _as(NASAMS, "Norway", desc="Supplied by Norway and the US since 2022.")),
            _cty("SAU",
                 _sys("Cheongung II (KM-SAM)", "Medium-range surface-to-air", "On order", "M-SAM",
                      "Ordered from South Korea in 2024 in a reported US$3.2 billion deal.",
                      "About 40 km (reported)", "Not published", 40, "Reported figure", "LIG Nex1", "South Korea")),
            _cty("SGP",
                 _as(SPYDER, "Israel", name="Spyder-SR",
                     desc="Singapore's short-range air defence; delivered in 2012 and fully operational from 2018."),
                 _as(RBS70, "Sweden", desc="Singapore's man-portable layer, with an intercept range of up to 8 km (MINDEF)."),
                 _sys("Aster 15 (naval)", "Ship-launched surface-to-air", "In service", "Aster (missile family)",
                      "Air defence of the Navy's Formidable-class frigates, first fired in 2008; Aster 30 capability is reported but not confirmed. Also planned for the new Multi-Role Combat Vessels.",
                      "About 30 km (reported)", "About Mach 3 (reported)", 30, "Reported figure", "MBDA", "France"),
                 _sys("VL MICA (naval)", "Ship-launched surface-to-air", "In service", "MICA (missile)",
                      "Air defence of the Navy's Independence-class Littoral Mission Vessels (12 cells each), first fired in 2023; also planned for the Multi-Role Combat Vessels.",
                      "About 20 km (reported)", "Not published", 20, "Reported figure", "MBDA", "France"),
                 _sys("Barak", "Ship-launched surface-to-air", "In service", "Barak 1",
                      "Point defence of the Victory-class missile corvettes since 1997; the corvettes are being replaced by Multi-Role Combat Vessels from 2028.",
                      "About 12 km (reported)", "Not published", 12, "Reported figure",
                      "Israel Aerospace Industries and Rafael", "Israel")),
            _cty("MYS",
                 _as(STARSTREAK, "United Kingdom",
                     desc="Ordered in 2015 with Thales' ForceSHIELD system and fully delivered in 2019; used by the RMAF and the Army."),
                 _sys("Jernas", "Short-range air defence", "Reported in service", "Rapier (missile)",
                      "Latest Rapier version (Rapier FSC), bought for a Malaysian Army air defence regiment; listed among Malaysia's current systems.",
                      "About 8 km (reported)", "Over Mach 2 (reported)", 8, "Reported figure", "MBDA", "United Kingdom"),
                 _sys("Igla", "Very short-range air defence", "Reported in service", "9K38 Igla",
                      "Russian shoulder-fired missile used by Malaysian forces.",
                      "About 5 km (reported)", "Not published", 5, "Reported figure", "KB Mashinostroyeniya", "Russia"),
                 _sys("Anza", "Very short-range air defence", "Reported in service", "Anza (missile)",
                      "Pakistani shoulder-fired missile used by Malaysian forces.",
                      "About 5 km (reported)", "Not published", 5, "Reported figure", "Kahuta Research Laboratories", "Pakistan"),
                 _sys("FN-6", "Very short-range air defence", "Reported in service", "FN-6",
                      "Chinese shoulder-fired missile used by Malaysian forces.",
                      "About 5.5 km (reported)", "Not published", 5.5, "Reported figure", made_in="China"),
                 _sys("Sea Wolf", "Ship-launched surface-to-air", "In service", "Sea Wolf (missile)",
                      "Vertically launched point defence of the Lekiu-class frigates (16 cells); expected to reach the end of its life by about 2030 and be replaced.",
                      "About 6 to 10 km (reported)", "About Mach 3 (reported)", 10, "Upper reported figure, vertical-launch version",
                      "MBDA (originally British Aerospace)", "United Kingdom"),
                 _sys("VL MICA (naval)", "Ship-launched surface-to-air", "On order", "MICA (missile)",
                      "Point defence for the new Maharaja Lela-class littoral combat ships; letter of acceptance signed with MBDA in July 2026, contract pending. The first ship is due in April 2027.",
                      "About 20 km (reported)", "Not published", 20, "Reported figure", "MBDA", "France")),
            _cty("IDN",
                 _as(NASAMS, "Norway", status="Reported in service",
                     desc="Indonesia's first battery is reported deployed to protect Jakarta; delivery dates have not been published."),
                 _as(STARSTREAK, "United Kingdom", desc="Used by Indonesian Army air defence units (reported)."),
                 _sys("VL MICA (naval)", "Ship-launched surface-to-air", "In service", "MICA (missile)",
                      "Air defence of the Navy's Martadinata-class frigates (12 cells each); first publicised firing in April 2023.",
                      "About 20 km (reported)", "Not published", 20, "Reported figure", "MBDA", "France"),
                 _sys("Mistral", "Very short-range air defence", "Reported in service", "Mistral (missile)",
                      "French infrared-guided short-range missile used by Indonesian forces (reported).",
                      "About 6 km (reported)", "About Mach 2.5 (reported)", 6, "Reported figure", "MBDA", "France"),
                 _as(RBS70, "Sweden", status="Reported in service",
                     desc="Long used by the Indonesian Army; Saab has offered the newer RBS 70 NG (reported)."),
                 _sys("Grom", "Very short-range air defence", "Reported in service", "Grom (missile)",
                      "Polish shoulder-fired missile used by the Indonesian Army (reported).",
                      "About 5.5 km (reported)", "Not published", 5.5, "Reported figure", "Mesko", "Poland"),
                 _sys("QW-3 / QW-19", "Very short-range air defence", "In service", "QW-3",
                      "Chinese shoulder-fired missiles of the Indonesian Air Force: QW-3 since 2010, QW-19 ordered in 2023 and likely delivered in early 2024.",
                      "About 6 km (reported)", "Not published", 6, "Reported figure", "CASIC", "China")),
            _cty("PHL",
                 _as(SPYDER, "Israel", name="Spyder-MR", ring=35, ring_note="Medium-range version, lower reported figure",
                     desc="Philippine Air Force air defence, inducted in November 2022; three batteries bought.")),
        ]},
}
# ---- howitzers, rocket systems, drones and counter-drone defences (added October 2026) ----
# Gun ranges depend on the shell: rings use the figure named in ring_note. Drone rings use a published
# range or datalink range, which is a reach from the launch site, not a combat radius.
NA = "Not applicable"
K9 = _sys("K9 Thunder", "Self-propelled howitzer, 155 mm", "In service", "K9 Thunder",
          "South Korean tracked howitzer, the most widely exported of its kind.",
          "About 40 km with extended-range shells (reported)", NA, 40, "With extended-range shells, reported", "Hanwha Aerospace")
PZH2000 = _sys("PzH 2000", "Self-propelled howitzer, 155 mm", "In service", "Panzerhaubitze 2000",
               "German tracked howitzer, in service since 1998 and used heavily in Ukraine.",
               "About 30 km; about 40 km with base-bleed shells (reported)", NA, 40, "With base-bleed shells, reported",
               "KNDS Deutschland (Krauss-Maffei Wegmann)")
CAESAR = _sys("CAESAR", "Truck-mounted howitzer, 155 mm", "In service", "CAESAR self-propelled howitzer",
              "French howitzer mounted on a 6x6 truck, quick to set up and move.",
              "About 40 km with extended-range shells (reported)", NA, 40, "With extended-range shells, reported", "KNDS France (Nexter)")
ARCHER = _sys("Archer", "Truck-mounted howitzer, 155 mm", "In service", "Archer Artillery System",
              "Swedish automated howitzer on a 6x6 truck, operated from an armoured cab.",
              "About 40 km; about 50 km with Excalibur guided shells (reported)", NA, 40, "With standard extended-range shells",
              "BAE Systems Bofors")
M777 = _sys("M777", "Towed howitzer, 155 mm", "In service", "M777 howitzer",
            "Lightweight towed howitzer that can be carried by helicopter.",
            "About 24 km; about 30 km with rocket-assisted shells; about 40 km with Excalibur (reported)", NA,
            30, "With rocket-assisted shells, reported", "BAE Systems")
HIMARS = _sys("HIMARS", "Wheeled rocket launcher", "In service", "M142 HIMARS",
              "US truck-mounted launcher for six GMLRS rockets or one ATACMS / two PrSM missiles (those are listed under Ballistic missiles).",
              "About 70 to 80 km with GMLRS rockets (reported)", NA, 80, "GMLRS rockets, upper reported figure", "Lockheed Martin")
M270 = _sys("M270", "Tracked rocket launcher", "In service", "M270 Multiple Launch Rocket System",
            "Tracked launcher carrying twelve GMLRS rockets, used by the US and several European armies.",
            "About 70 to 80 km with GMLRS rockets (reported)", NA, 80, "GMLRS rockets, upper reported figure", "Lockheed Martin")
CHUNMOO = _sys("K239 Chunmoo", "Wheeled rocket launcher", "In service", "K239 Chunmoo",
               "South Korean launcher for guided rockets and longer-range tactical missiles.",
               "About 80 km with 239 mm guided rockets; about 290 km with CTM-290 missiles (reported)", NA,
               80, "Guided rockets, reported", "Hanwha Aerospace")
ASTROS = _sys("Astros II", "Wheeled rocket launcher", "In service", "Astros II MLRS",
              "Brazilian truck-mounted rocket system firing several rocket sizes.",
              "About 9 to 90 km depending on the rocket (reported)", NA, 90, "Longest-range rocket, reported", "Avibras")
MQ9 = _sys("MQ-9 Reaper", "Armed drone (medium altitude, long endurance)", "In service", "General Atomics MQ-9 Reaper",
           "US armed drone flown by satellite link, in service since 2007.",
           "About 1,850 km range (published)", "About 370 km/h cruise (published)",
           1850, "Published range; it can only strike well inside this from its base", "General Atomics")
MQ9B = _sys("MQ-9B", "Armed drone (medium altitude, long endurance)", "In service", "General Atomics MQ-9B",
            "Newer version of the Reaper certified to fly in civil airspace (SkyGuardian, SeaGuardian, Protector).",
            "Over 40 hours endurance; range not published", "Not published", maker="General Atomics")
TB2 = _sys("Bayraktar TB2", "Armed drone (medium altitude)", "In service", "Bayraktar TB2",
           "Turkish armed drone, exported to over 30 countries and used in several recent wars.",
           "About 150 km line-of-sight datalink range (published)", "About 130 km/h cruise (published)",
           150, "Datalink range from its ground station", "Baykar")
AKINCI = _sys("Bayraktar Akıncı", "Armed drone (high altitude, long endurance)", "In service", "Baykar Bayraktar Akıncı",
              "Larger Turkish armed drone that carries guided bombs and cruise missiles.",
              "About 300 km line-of-sight datalink range; further by satellite (reported)", "About 240 km/h cruise (reported)",
              300, "Line-of-sight datalink range, reported", "Baykar")
WINGLOONG2 = _sys("Wing Loong II", "Armed drone (medium altitude, long endurance)", "In service", "CAIG Wing Loong II",
                  "Chinese armed drone, widely exported in the Middle East and Africa.",
                  "About 1,500 km range (reported)", "About 370 km/h top speed (reported)", 1500, "Reported range",
                  "AVIC (Chengdu Aircraft Design Institute)")
CH4 = _sys("CH-4 Rainbow", "Armed drone (medium altitude, long endurance)", "In service", "CASC Rainbow",
           "Chinese armed drone family, exported to several countries.",
           "Not consistently published; endurance about 30 to 40 hours (reported)", "Not published", maker="CASC")
HAROP = _sys("Harop", "Loitering munition", "In service", "IAI Harop",
             "Israeli loitering munition that can home on radar emissions or be guided by an operator.",
             "About 1,000 km (reported)", "Not published", 1000, "Reported figure", "Israel Aerospace Industries")
HARPY = _sys("Harpy", "Loitering munition (anti-radar)", "In service", "IAI Harpy",
             "Israeli anti-radar loitering munition, the predecessor of Harop.",
             "About 500 km (reported)", "Not published", 500, "Reported figure", "Israel Aerospace Industries")
SB600 = _sys("Switchblade 600", "Loitering munition", "In service", "AeroVironment Switchblade",
             "US tube-launched loitering munition for use against armoured vehicles.",
             "Over 40 km (published)", "About 185 km/h dash (published)", 40, "Published figure", "AeroVironment")
WARMATE = _sys("Warmate", "Loitering munition", "In service", "WB Electronics Warmate",
               "Polish small loitering munition, used by Poland and Ukraine.",
               "About 12 to 30 km depending on version (reported)", "Not published", 30, "Upper reported figure", "WB Group")
SHAHED136 = _sys("Shahed-136", "One-way attack drone", "In service", "HESA Shahed 136",
                 "Iranian long-range one-way attack drone with a piston engine; built in Russia as the Geran-2.",
                 "About 2,000 to 2,500 km (reported)", "About 185 km/h (reported)", 2000, "Lower reported figure",
                 "Shahed Aviation Industries")
GEPARD = _sys("Gepard", "Self-propelled anti-aircraft gun, 35 mm", "In service", "Flakpanzer Gepard",
              "German twin-35 mm gun tank, retired by Germany but highly effective against drones in Ukraine.",
              "About 5.5 km (reported)", NA, 5.5, "Reported figure", "Krauss-Maffei Wegmann")
SKYNEX = _sys("Skynex / Skyshield", "Air-defence gun system, 35 mm", "In service", "Skyshield",
              "Revolver guns firing AHEAD airburst shells, linked to a fire-control radar.",
              "About 4 km (reported)", NA, 4, "Reported figure", "Rheinmetall Air Defence")
COYOTE = _sys("Coyote Block 2", "Interceptor drone", "In service", "Raytheon Coyote",
              "Jet-powered interceptor drone launched from a ground launcher to destroy drones.",
              "About 10 to 15 km (reported)", "About 555 km/h (reported)", 15, "Upper reported figure", "Raytheon (RTX)")

BATTLE_LAB.update({
    "Howitzers": {
        "group": "Strike", "short": "Howitzers",
        "blurb": "Large guns, self-propelled or towed, that fire shells in a high arc. Rings show range with the shell named.",
        "countries": [
            _cty("USA",
                 _sys("M109A7 Paladin", "Self-propelled howitzer, 155 mm", "In service", "M109 howitzer",
                      "Latest version of the long-serving US tracked howitzer.",
                      "About 22 km; about 30 km with rocket-assisted shells (reported)", NA, 30,
                      "With rocket-assisted shells, reported", "BAE Systems"),
                 M777),
            _cty("KOR", K9),
            _cty("DEU", PZH2000),
            _cty("FRA", CAESAR),
            _cty("SWE", ARCHER),
            _cty("GBR",
                 _as(ARCHER, "Sweden", desc="Bought from Sweden in 2023 as an interim replacement for the AS-90 guns given to Ukraine."),
                 _sys("RCH 155", "Wheeled howitzer, 155 mm", "On order", "RCH 155",
                      "German wheeled howitzer chosen to replace the British Army's interim guns.",
                      "About 40 km (reported)", NA, 40, "Reported figure", "KNDS Deutschland", "Germany")),
            _cty("RUS",
                 _sys("2S19 Msta-S", "Self-propelled howitzer, 152 mm", "In service", "2S19 Msta",
                      "Russia's main tracked howitzer.",
                      "About 25 km; about 29 km with base-bleed shells (reported)", NA, 29, "With base-bleed shells, reported",
                      "Uraltransmash"),
                 _sys("2S35 Koalitsiya-SV", "Self-propelled howitzer, 152 mm", "Reported in service", "2S35 Koalitsiya-SV",
                      "Newer Russian howitzer with an uncrewed turret, in limited numbers.",
                      "About 70 km (claimed)", NA, 70, "Claimed figure", "Uraltransmash")),
            _cty("CHN",
                 _sys("PLZ-05", "Self-propelled howitzer, 155 mm", "In service", "PLZ-05",
                      "Chinese tracked howitzer.",
                      "About 39 km; about 53 km with rocket-assisted shells (reported)", NA, 39, "Standard extended-range shells, reported",
                      "Norinco"),
                 _sys("PCL-181", "Truck-mounted howitzer, 155 mm", "In service", "PCL-181",
                      "Chinese truck-mounted howitzer, lighter and faster to deploy than tracked guns.",
                      "About 40 km (reported)", NA, 40, "Reported figure", "Norinco")),
            _cty("IND",
                 _as(K9, "South Korea", name="K9 Vajra-T", desc="Indian version of the K9, built under licence in India by Larsen & Toubro."),
                 _sys("ATAGS", "Towed howitzer, 155 mm", "On order", "Advanced Towed Artillery Gun System",
                      "Indian-designed towed howitzer; a production contract for 307 guns was signed in 2025.",
                      "About 48 km (reported)", NA, 48, "Reported figure", "DRDO with Bharat Forge and Tata Advanced Systems"),
                 _as(M777, "United States", desc="Bought from the US for mountain units; partly assembled in India.")),
            _cty("PAK",
                 _sys("SH-15", "Truck-mounted howitzer, 155 mm", "In service", "SH-15",
                      "Chinese truck-mounted howitzer bought by Pakistan.",
                      "About 40 to 53 km depending on the shell (reported)", NA, 40, "Lower reported figure", "Norinco", "China")),
            _cty("PRK",
                 _sys("Koksan (M1989)", "Self-propelled gun, 170 mm", "In service", "Koksan (gun)",
                      "Very long-barrelled North Korean gun; some were supplied to Russia in 2024.",
                      "About 40 km; about 60 km with rocket-assisted shells (reported)", NA, 60, "With rocket-assisted shells, reported")),
            _cty("TUR",
                 _sys("T-155 Fırtına", "Self-propelled howitzer, 155 mm", "In service", "T-155 Fırtına",
                      "Turkish howitzer based on the K9.",
                      "About 40 km (reported)", NA, 40, "Reported figure", "MKE and Aselsan")),
            _cty("POL",
                 _sys("AHS Krab", "Self-propelled howitzer, 155 mm", "In service", "AHS Krab",
                      "Polish tracked howitzer, also supplied to Ukraine.",
                      "About 40 km (reported)", NA, 40, "Reported figure", "Huta Stalowa Wola"),
                 _as(K9, "South Korea", name="K9PL", desc="Bought from South Korea from 2022; later batches assembled in Poland.")),
            _cty("UKR",
                 _sys("2S22 Bohdana", "Truck-mounted howitzer, 155 mm", "In service", "2S22 Bohdana",
                      "Ukrainian-built truck-mounted howitzer, in production since 2023.",
                      "About 40 km (reported)", NA, 40, "Reported figure"),
                 _as(PZH2000, "Germany", desc="Supplied by Germany and the Netherlands since 2022."),
                 _as(CAESAR, "France", desc="Supplied by France and Denmark since 2022."),
                 _as(M777, "United States", desc="Supplied by the US, Canada and Australia since 2022.")),
            _cty("NOR", _as(K9, "South Korea", name="K9 Vidar", desc="Norwegian Army's K9s, in service since 2019.")),
            _cty("ITA", _as(PZH2000, "Germany", desc="Built under licence in Italy.")),
            _cty("AUS",
                 _as(K9, "South Korea", name="AS9 Huntsman", status="Fielding",
                     desc="Australian version of the K9, built in Geelong, Victoria, by Hanwha Defense Australia."),
                 _as(M777, "United States", desc="Australian Army towed howitzer.")),
            _cty("JPN",
                 _sys("Type 99", "Self-propelled howitzer, 155 mm", "In service", "Type 99 155 mm self-propelled howitzer",
                      "Japanese tracked howitzer.", "About 30 km (reported)", NA, 30, "Reported figure", "Japan Steel Works"),
                 _sys("Type 19", "Truck-mounted howitzer, 155 mm", "Fielding", "Type 19 155 mm wheeled self-propelled howitzer",
                      "Japanese truck-mounted howitzer replacing towed guns.", "Not published", NA, maker="Japan Steel Works")),
            _cty("ISR",
                 _sys("M109 Doher", "Self-propelled howitzer, 155 mm", "In service", "M109 howitzer",
                      "Israeli-upgraded M109, being replaced by a new Elbit howitzer.",
                      "About 22 to 30 km depending on the shell (reported)", NA, 30, "Upper reported figure", "BAE Systems", "United States")),
            _cty("SAU",
                 _as(CAESAR, "France", desc="Royal Saudi Land Forces truck-mounted howitzer."),
                 _sys("PLZ-45", "Self-propelled howitzer, 155 mm", "In service", "PLZ-45",
                      "Chinese tracked howitzer bought by Saudi Arabia.",
                      "About 39 km with base-bleed shells (reported)", NA, 39, "With base-bleed shells, reported", "Norinco", "China")),
            _cty("ARE",
                 _sys("G6 Rhino", "Wheeled howitzer, 155 mm", "In service", "G6 howitzer",
                      "South African six-wheeled howitzer bought by the UAE.",
                      "About 30 km; about 39 km with base-bleed shells (reported)", NA, 39, "With base-bleed shells, reported",
                      "Denel", "South Africa")),
            _cty("SGP",
                 _sys("SSPH Primus", "Self-propelled howitzer, 155 mm", "In service", "SSPH Primus",
                      "Singapore-designed tracked howitzer, in service since 2004.",
                      "19 km; 30 km with rocket-assisted shells (MINDEF)", NA, 30, "With rocket-assisted shells (MINDEF)",
                      "ST Engineering"),
                 _sys("FH-2000", "Towed howitzer, 155 mm", "In service", "FH-2000",
                      "Singapore-designed towed howitzer with a long 52-calibre barrel.",
                      "19 km; about 40 km with base-bleed shells (reported)", NA, 40, "With base-bleed shells, reported",
                      "ST Engineering"),
                 _sys("Pegasus", "Light towed howitzer, 155 mm", "In service", "Pegasus (howitzer)",
                      "Singapore-designed lightweight howitzer that can be lifted by helicopter and drive itself short distances.",
                      "Not officially published", NA, maker="ST Engineering")),
            _cty("MYS",
                 _sys("G5 Mk III", "Towed howitzer, 155 mm", "In service", "G5 howitzer",
                      "South African towed howitzer; the Malaysian Army's guns were upgraded locally with a new fire-control system.",
                      "About 39 km (reported)", NA, 39, "Reported figure", "Denel", "South Africa")),
            _cty("IDN", _as(CAESAR, "France", desc="55 in Indonesian Army service since 2016.")),
            _cty("PHL",
                 _sys("ATMOS 2000", "Truck-mounted howitzer, 155 mm", "In service", "ATMOS 2000",
                      "Israeli truck-mounted howitzer; 12 delivered in 2021 and operational with the Philippine Army since 2022.",
                      "Up to 41 km with extended-range shells (Philippine Army)", NA, 41, "With extended-range shells",
                      "Elbit Systems", "Israel")),
            _cty("BRA",
                 _sys("M109A5+", "Self-propelled howitzer, 155 mm", "In service", "M109 howitzer",
                      "Upgraded US-built M109s in Brazilian Army service.",
                      "About 22 to 30 km depending on the shell (reported)", NA, 30, "Upper reported figure", "BAE Systems", "United States")),
        ]},

    "Rocket Systems": {
        "group": "Strike", "short": "Rocket Systems",
        "blurb": "Multiple rocket launchers on trucks or tracked vehicles, firing unguided and guided rockets.",
        "countries": [
            _cty("USA", HIMARS, M270),
            _cty("RUS",
                 _sys("Tornado-S", "Wheeled rocket launcher, 300 mm", "In service", "9A52-4 Tornado",
                      "Modernised successor to the BM-30 Smerch, with guided rockets.",
                      "About 120 km (reported)", NA, 120, "Reported figure", "Splav (Tecmash)"),
                 _sys("BM-21 Grad", "Wheeled rocket launcher, 122 mm", "In service", "BM-21 Grad",
                      "Soviet-era truck launcher for forty unguided rockets, still widely used.",
                      "About 20 km (reported)", NA, 20, "Reported figure", "Splav (Tecmash)")),
            _cty("CHN",
                 _sys("PHL-16 (PCH-191)", "Wheeled rocket launcher", "In service", "PCH-191",
                      "Chinese launcher that fires rockets and tactical ballistic missiles from the same vehicle.",
                      "About 70 to 130 km with rockets; up to about 350 km with missiles (reported)", NA,
                      130, "Rockets, upper reported figure", "Norinco"),
                 _sys("PHL-03", "Wheeled rocket launcher, 300 mm", "In service", "PHL-03",
                      "Chinese twelve-tube 300 mm rocket launcher.",
                      "About 70 to 130 km (reported)", NA, 130, "Upper reported figure", "Norinco")),
            _cty("KOR", CHUNMOO),
            _cty("PRK",
                 _sys("KN-25", "Wheeled rocket launcher, 600 mm", "In service", "KN-25",
                      "North Korea calls it a super-large rocket launcher; analysts class its rockets as short-range ballistic missiles.",
                      "About 380 km (reported)", NA, 380, "Reported test range")),
            _cty("IND",
                 _sys("Pinaka", "Wheeled rocket launcher, 214 mm", "In service", "Pinaka multi-barrel rocket launcher",
                      "Indian rocket system; guided and extended-range rockets are in production.",
                      "About 40 to 60 km; about 75 to 90 km for guided rockets (reported)", NA, 75, "Guided rockets, lower reported figure",
                      "DRDO, built by Indian industry")),
            _cty("PAK",
                 _sys("Fatah-I", "Guided rocket system", "In service", "Fatah (rocket)",
                      "Pakistani guided rocket system, first tested in 2021.",
                      "About 140 km (reported)", NA, 140, "Reported figure"),
                 _sys("Fatah-II", "Guided rocket system", "Tested", "Fatah (rocket)",
                      "Longer-range Pakistani guided rocket, tested in 2024.",
                      "About 400 km (reported)", NA, 400, "Reported figure"),
                 _sys("A-100", "Wheeled rocket launcher, 300 mm", "In service", "A-100 (rocket launcher)",
                      "Chinese ten-tube rocket launcher bought by Pakistan.",
                      "About 120 km (reported)", NA, 120, "Reported figure", made_in="China")),
            _cty("TUR",
                 _sys("TRG-300 Kasırga", "Guided rocket system, 300 mm", "In service", "TRG-300 Kasırga",
                      "Turkish guided rocket, fired from wheeled launchers.",
                      "About 90 to 120 km (reported)", NA, 120, "Upper reported figure", "Roketsan")),
            _cty("UKR",
                 _sys("Vilkha", "Guided rocket system, 300 mm", "In service", "Vilkha",
                      "Ukrainian guided rocket fired from modified Smerch launchers.",
                      "About 70 km; about 110 km for Vilkha-M (reported)", NA, 110, "Vilkha-M, reported", "Luch Design Bureau"),
                 _as(HIMARS, "United States", desc="Supplied by the US since June 2022."),
                 _as(M270, "United States", desc="Supplied by the UK, Germany and others since 2022.")),
            _cty("POL",
                 _as(CHUNMOO, "South Korea", name="Homar-K", desc="Polish version of Chunmoo on a Polish Jelcz truck, delivered since 2023."),
                 _as(HIMARS, "United States", name="Homar-A (HIMARS)", status="On order", desc="Ordered from the US for the Polish Army."),
                 _sys("WR-40 Langusta", "Wheeled rocket launcher, 122 mm", "In service", "WR-40 Langusta",
                      "Polish modernised Grad-type launcher.",
                      "About 20 to 40 km depending on the rocket (reported)", NA, 40, "Upper reported figure", "Huta Stalowa Wola")),
            _cty("DEU", _as(M270, "United States", name="MARS II (M270)", desc="German Army version of M270.")),
            _cty("FRA", _as(M270, "United States", name="LRU (M270)", desc="French Army version of M270.")),
            _cty("ITA", _as(M270, "United States", desc="Italian Army M270, upgraded to fire GMLRS.")),
            _cty("GBR", _as(M270, "United States", name="M270A2", desc="British Army launchers, upgraded to the A2 standard.")),
            _cty("AUS", _as(HIMARS, "United States", desc="Australian Army launchers, in service since 2024.")),
            _cty("TWN",
                 _sys("Thunderbolt-2000", "Wheeled rocket launcher", "In service", "Thunderbolt-2000",
                      "Taiwanese truck-mounted rocket system.",
                      "About 45 km (reported)", NA, 45, "Reported figure", "NCSIST"),
                 _as(HIMARS, "United States", desc="Delivered to Taiwan since 2024.")),
            _cty("IRN",
                 _sys("Fajr-5", "Rocket launcher, 333 mm", "In service", "Fajr-5",
                      "Iranian heavy rocket, also supplied to allied groups in the region.",
                      "About 75 km (reported)", NA, 75, "Reported figure")),
            _cty("SAU", _as(ASTROS, "Brazil", desc="Royal Saudi Land Forces rocket system.")),
            _cty("ARE", _as(HIMARS, "United States", desc="UAE Land Forces launchers.")),
            _cty("SGP", _as(HIMARS, "United States", desc="Singapore Army launchers, in service since 2011.")),
            _cty("MYS", _as(ASTROS, "Brazil", desc="Malaysian Army Rocket Artillery Brigade, fired each year in exercise Lembing Sakti.")),
            _cty("IDN", _as(ASTROS, "Brazil", name="Astros II Mk6", desc="Indonesian Army launchers.")),
            _cty("BRA", _as(ASTROS, None, name="Astros II Mk6", desc="Brazilian Army rocket system, made in Brazil.")),
        ]},

    "Combat drones": {
        "group": "Strike", "short": "Combat drones",
        "blurb": "Armed uncrewed aircraft. Rings show a published range or datalink range from the launch site.",
        "countries": [
            _cty("USA", MQ9),
            _cty("TUR", TB2, AKINCI),
            _cty("CHN", WINGLOONG2, CH4),
            _cty("ISR",
                 _sys("Hermes 900", "Drone (medium altitude, long endurance)", "In service", "Elbit Hermes 900",
                      "Israeli long-endurance drone; armed versions are reported in Israeli service.",
                      "About 30 hours endurance; range not published", "Not published", maker="Elbit Systems"),
                 _sys("Heron TP (Eitan)", "Drone (medium altitude, long endurance)", "In service", "IAI Eitan",
                      "Large Israeli long-endurance drone; armed versions are reported.",
                      "Over 30 hours endurance; range not published", "Not published", maker="Israel Aerospace Industries")),
            _cty("RUS",
                 _sys("Orion (Inokhodets)", "Armed drone (medium altitude, long endurance)", "In service", "Kronshtadt Orion",
                      "Russian armed drone, used in Syria and Ukraine.",
                      "About 250 km datalink range (reported)", "Not published", 250, "Datalink range, reported", "Kronstadt")),
            _cty("IRN",
                 _sys("Mohajer-6", "Armed drone (medium altitude)", "In service", "Qods Mohajer-6",
                      "Iranian armed drone, also supplied to Russia.",
                      "About 200 km datalink range (reported)", "Not published", 200, "Datalink range, reported", "Qods Aviation Industries"),
                 _sys("Shahed-129", "Armed drone (medium altitude, long endurance)", "In service", "HESA Shahed 129",
                      "Iranian long-endurance armed drone.",
                      "Not reliably published", "Not published", maker="Shahed Aviation Industries")),
            _cty("PAK",
                 _sys("Shahpar-III", "Armed drone (medium altitude, long endurance)", "In service", "GIDS Shahpar",
                      "Pakistani-developed armed drone.", "Not published", "Not published", maker="GIDS"),
                 _as(TB2, "Turkey", desc="Reported in Pakistani service."),
                 _as(AKINCI, "Turkey", desc="Reported in Pakistani service."),
                 _as(WINGLOONG2, "China", desc="Reported in Pakistani service.")),
            _cty("UKR", _as(TB2, "Turkey", desc="Used by Ukraine since 2019, mostly early in the 2022 war.")),
            _cty("POL", _as(TB2, "Turkey", desc="Polish Armed Forces, delivered from 2022.")),
            _cty("GBR", _as(MQ9B, "United States", name="Protector RG1 (MQ-9B)", desc="Royal Air Force version of MQ-9B.")),
            _cty("FRA", _as(MQ9, "United States", desc="French Air and Space Force; armed since 2019.")),
            _cty("ITA", _as(MQ9, "United States", desc="Italian Air Force; cleared to be armed.")),
            _cty("DEU", _sys("Heron TP", "Armed drone (medium altitude, long endurance)", "In service", "IAI Eitan",
                             "Leased from Israel; the Bundestag approved arming it in 2022.",
                             "Over 30 hours endurance; range not published", "Not published", maker="Israel Aerospace Industries",
                             made_in="Israel")),
            _cty("IND", _as(MQ9B, "United States", status="On order", desc="31 ordered from the US in 2024 for the Indian Navy, Army and Air Force.")),
            _cty("SAU",
                 _as(WINGLOONG2, "China", desc="Royal Saudi Air Force."),
                 _as(AKINCI, "Turkey", status="On order", desc="Ordered in 2023, with part of production planned in Saudi Arabia.")),
            _cty("ARE", _as(WINGLOONG2, "China", desc="UAE Air Force.")),
            _cty("IDN", _as(CH4, "China", name="CH-4B", desc="Six bought; flown by Indonesian Air Force Squadron 51 from Supadio air base.")),
            _cty("PRK",
                 _sys("Saetbyol-9", "Armed drone (claimed)", "Claimed", "Saetbyol-9",
                      "North Korean drone shown in 2023 that resembles the MQ-9; its capability is unverified.",
                      "Not published", "Not published")),
        ]},

    "Loitering munitions": {
        "group": "Strike", "short": "Loitering",
        "blurb": "One-way attack drones that fly to an area, wait, and dive onto a target.",
        "countries": [
            _cty("ISR", HAROP, HARPY),
            _cty("USA", SB600),
            _cty("RUS",
                 _sys("Lancet-3", "Loitering munition", "In service", "ZALA Lancet",
                      "Russian loitering munition used widely against vehicles and artillery in Ukraine.",
                      "About 40 to 70 km (reported)", "Not published", 40, "Lower reported figure", "ZALA Aero (Kalashnikov)"),
                 _sys("Geran-2", "One-way attack drone", "In service", "HESA Shahed 136",
                      "Russian-built version of Iran's Shahed-136, made in large numbers at Alabuga.",
                      "About 2,000 km (reported)", "About 185 km/h (reported)", 2000, "Reported figure", "Alabuga (Russian production)")),
            _cty("IRN", SHAHED136),
            _cty("UKR",
                 _sys("Lyutyi", "One-way attack drone", "In service", "AN-196 Liutyi",
                      "Ukrainian long-range attack drone used against targets deep inside Russia.",
                      "About 1,000 km or more (reported)", "Not published", 1000, "Reported figure", "Antonov"),
                 _as(SB600, "United States", desc="Supplied by the US."),
                 _as(WARMATE, "Poland", desc="Supplied by Poland.")),
            _cty("POL", WARMATE),
            _cty("TUR",
                 _sys("Kargu-2", "Loitering munition (quadcopter)", "In service", "STM Kargu",
                      "Small Turkish rotary-wing loitering munition.",
                      "About 10 km (published)", "Not published", 10, "Published figure", "STM")),
            _cty("IND",
                 _as(HAROP, "Israel", desc="Indian Air Force; reported used in May 2025."),
                 _sys("Nagastra-1", "Loitering munition", "In service", "Nagastra-1",
                      "Indian-made small loitering munition, delivered to the Army from 2024.",
                      "About 30 km (reported)", "Not published", 30, "Reported figure", "Solar Industries")),
            _cty("KOR", _as(HARPY, "Israel", desc="Republic of Korea Army."), _as(HAROP, "Israel", desc="Republic of Korea Army.")),
            _cty("TWN",
                 _sys("Switchblade 300", "Loitering munition", "On order", "AeroVironment Switchblade",
                      "Small US loitering munition ordered for Taiwan's army.",
                      "About 10 km (published)", "Not published", 10, "Published figure", "AeroVironment", "United States")),
        ]},

    "Air-defence guns & C-RAM": {
        "group": "Defence", "short": "AD guns & C-RAM",
        "blurb": "Fast-firing guns against aircraft, drones and incoming rockets, artillery and mortar rounds (C-RAM).",
        "countries": [
            _cty("USA",
                 _sys("Centurion C-RAM", "Counter-rocket, artillery and mortar gun", "In service", "Phalanx CIWS",
                      "Land version of the Phalanx naval gun, used to protect bases.",
                      "About 2 km (estimated)", NA, 2, "Estimated", "Raytheon (RTX)")),
            _cty("DEU",
                 _sys("Skyranger 30", "Self-propelled air-defence gun, 30 mm", "Fielding", "Skyranger 30",
                      "German gun system on a Boxer vehicle, ordered mainly to counter drones.",
                      "About 3 km (reported)", NA, 3, "Reported figure", "Rheinmetall")),
            _cty("UKR",
                 _as(GEPARD, "Germany", desc="Supplied by Germany since 2022; prized against Shahed drones."),
                 _as(SKYNEX, "Germany", name="Skynex", desc="Supplied by Germany since 2023.")),
            _cty("RUS",
                 _sys("2K22 Tunguska", "Gun and missile air defence", "In service", "2K22 Tunguska",
                      "Tracked vehicle with twin 30 mm guns and short-range missiles.",
                      "Guns about 4 km; missiles about 8 km (reported)", NA, 8, "Missiles, reported", "KBP Instrument Design Bureau")),
            _cty("CHN",
                 _sys("PGZ-09", "Self-propelled air-defence gun, 35 mm", "In service", "PGZ-09",
                      "Chinese twin 35 mm gun vehicle.", "About 4 km (reported)", NA, 4, "Reported figure", "Norinco")),
            _cty("KOR",
                 _sys("K30 Biho", "Self-propelled air-defence gun, 30 mm", "In service", "K30 Biho",
                      "Twin 30 mm gun vehicle; the Biho II version adds short-range missiles.",
                      "About 3 km (reported)", NA, 3, "Guns, reported", "Hanwha Aerospace")),
            _cty("JPN",
                 _sys("Type 87", "Self-propelled air-defence gun, 35 mm", "In service", "Type 87 self-propelled anti-aircraft gun",
                      "Japanese twin 35 mm gun vehicle.", "About 3 km (reported)", NA, 3, "Reported figure",
                      "Mitsubishi Heavy Industries and Japan Steel Works")),
            _cty("TUR",
                 _sys("Korkut", "Self-propelled air-defence gun, 35 mm", "In service", "Korkut",
                      "Turkish tracked twin 35 mm gun vehicle firing airburst shells.",
                      "About 4 km (reported)", NA, 4, "Reported figure", "Aselsan")),
            _cty("IND",
                 _sys("L70 (upgraded)", "Air-defence gun, 40 mm", "In service", "Bofors 40 mm L/70",
                      "Swedish-designed 40 mm gun, upgraded in India and used against drones.",
                      "About 4 km (reported)", NA, 4, "Reported figure", "Bofors; upgraded by Bharat Electronics", "Sweden")),
            _cty("IDN", _as(SKYNEX, "Germany", name="Skyshield",
                            desc="Indonesian Air Force; seven firing units as of 2025, used by the PASKHAS special forces to protect air bases.")),
        ]},

    "Counter-drone systems": {
        "group": "Defence", "short": "Counter-drone",
        "blurb": "Lasers, microwave weapons and jammers built to stop drones at short range.",
        "countries": [
            _cty("ISR",
                 _sys("Iron Beam", "High-energy laser", "In service", "Iron Beam",
                      "Israeli laser air defence against drones, rockets and mortars; entered service in 2025. Range drops in cloud or rain.",
                      "About 7 to 10 km (reported)", "Speed of light", 10, "Upper reported figure", "Rafael with Elbit Systems"),
                 _sys("Drone Dome", "Counter-drone system (jamming and laser)", "In service", "Drone Dome",
                      "Israeli system that detects drones and jams or burns them.",
                      "Not published", "Not published", maker="Rafael Advanced Defense Systems")),
            _cty("GBR", _sys("Drone Dome", "Counter-drone system (jamming and laser)", "In service", "Drone Dome",
                             "Bought from Israel by the British Army in 2018.", "Not published", "Not published",
                             maker="Rafael Advanced Defense Systems", made_in="Israel")),
            _cty("USA",
                 _sys("Leonidas", "High-power microwave", "In service", "Epirus Leonidas",
                      "US microwave weapon that can disable groups of drones at once.",
                      "Short range; not published", "Speed of light", maker="Epirus")),
            _cty("CHN",
                 _sys("Silent Hunter", "High-energy laser", "In service", "Silent Hunter (laser)",
                      "Chinese truck-mounted laser against drones.",
                      "About 1 to 4 km (reported)", "Speed of light", 4, "Upper reported figure")),
            _cty("KOR",
                 _sys("Cheongwang", "High-energy laser", "In service", "Cheongwang (laser weapon)",
                      "South Korean laser against small drones, deployed from 2024.",
                      "Short range; not published", "Speed of light", maker="Hanwha Aerospace")),
            _cty("TUR",
                 _sys("İhtar", "Counter-drone system (radar, jamming)", "In service", "İhtar",
                      "Turkish system that detects drones and jams their control links.",
                      "Not published", "Not published", maker="Aselsan")),
            _cty("IND",
                 _sys("D4 system", "Counter-drone system (jamming and laser)", "In service", "Drone Detect, Deter and Destroy",
                      "Indian system that detects, jams and can burn small drones.",
                      "About 1 to 2 km for the laser (reported)", "Not published", 2, "Laser, upper reported figure", "DRDO")),
            _cty("SGP",
                 _sys("SAF Counter-UAS", "Counter-drone capability (sensors, jammers, weapons)", "In development",
                      "Counter unmanned air system",
                      "Counter-drone capability built up by the SAF's Counter-UAS Development and Operations group, announced in March 2025, "
                      "to detect and stop small drones. The individual systems have not been named publicly.",
                      "Not published", "Not published", maker="Singapore Armed Forces with DSTA")),
            _cty("FRA",
                 _sys("HELMA-P", "High-energy laser", "In service", "HELMA-P",
                      "French laser against small drones, used to protect the 2024 Paris Olympics.",
                      "About 1 km (reported)", "Speed of light", 1, "Reported figure", "CILAS")),
        ]},

    "Interceptor drones": {
        "group": "Defence", "short": "Interceptor drones",
        "blurb": "Drones built to fly into and destroy other drones.",
        "countries": [
            _cty("USA",
                 COYOTE,
                 _sys("Roadrunner-M", "Interceptor drone (reusable)", "In service", "Anduril Roadrunner",
                      "Jet-powered interceptor that takes off vertically and can land again if not used.",
                      "Not published", "Not published", maker="Anduril")),
            _cty("UKR",
                 _sys("Sting", "Interceptor drone", "In service", "Sting (drone)",
                      "Ukrainian interceptor drone used against Shahed-type attack drones.",
                      "Up to 25 km (maker's claim)", "Not published", 25, "Maker's claim", "Wild Hornets")),
        ]},
})

# Categories are by type of weapon. Anti-ship missiles are cruise missiles, so they are listed there
# (merged by country; a system already listed for that country is not repeated).
_antiship = BATTLE_LAB.pop("Anti-ship missiles")
_cruise = BATTLE_LAB["Cruise missiles"]
_cruise["blurb"] = "Missiles that fly low and level like a small aircraft, against targets on land or at sea; most fly below the speed of sound."
for _c in _antiship["countries"]:
    _home = next((x for x in _cruise["countries"] if x["code"] == _c["code"]), None)
    if _home is None:
        _cruise["countries"].append(_c)
    else:
        _have = {s["name"] for s in _home["systems"]}
        _home["systems"] += [s for s in _c["systems"] if s["name"] not in _have]

# ---- launch platforms: how each country launches each weapon (L land, S ship, U submarine, A aircraft) ----
# From public sources (Wikipedia infoboxes and operator pages; defence news for the country-specific cases noted).
# Only what is in service or delivered for that country; a platform that has only been tested or is planned is left out.
# Land = placed inside the operating country; Ship and Submarine = placed anywhere at sea; Air = no placement yet.
_LAUNCH = {
    # Hypersonic
    ("RUS", "Avangard"): "L", ("RUS", "Kinzhal"): "A", ("RUS", "Oreshnik"): "L",
    ("RUS", "Zircon"): "SUL",            # also fired from Bastion coastal launchers (Defence Blog, 2025-26)
    ("CHN", "DF-17"): "L", ("CHN", "DF-27"): "L", ("CHN", "YJ-21"): "SA",
    ("USA", "Dark Eagle"): "L", ("USA", "Conventional Prompt Strike"): "SU", ("USA", "ARRW"): "A", ("USA", "HACM"): "A",
    ("PRK", "Hwasong-8"): "L", ("PRK", "Hwasong-16B"): "L", ("KOR", "Hycore"): "",
    ("IND", "LRAShM"): "L", ("IND", "HSTDV"): "L", ("IND", "BrahMos-II"): "",
    ("IRN", "Fattah-1"): "L", ("IRN", "Fattah-2"): "L", ("FRA", "V-MaX"): "L",
    ("JPN", "HVGP"): "L", ("JPN", "Hypersonic cruise missile"): "",
    # Cruise (including anti-ship)
    ("USA", "Tomahawk"): "SUL", ("USA", "JASSM-ER"): "A", ("USA", "Harpoon"): "SA", ("USA", "LRASM"): "A", ("USA", "NSM"): "SL",
    ("RUS", "Kalibr"): "SU", ("RUS", "Kh-101"): "A", ("RUS", "Iskander-K"): "L",
    ("RUS", "Bastion (P-800 Oniks)"): "L", ("RUS", "Bal (Kh-35)"): "L",
    ("CHN", "CJ-10"): "L", ("CHN", "CJ-100"): "L", ("CHN", "YJ-12"): "ASL", ("CHN", "YJ-18"): "SU",
    ("GBR", "Storm Shadow"): "A", ("GBR", "Tomahawk"): "U",
    ("FRA", "SCALP"): "A", ("FRA", "MdCN"): "SU", ("FRA", "Exocet"): "SUA",
    ("DEU", "Taurus"): "A", ("UKR", "Storm Shadow"): "A", ("UKR", "Neptune"): "L",
    ("IND", "LRLACM"): "L", ("IND", "SCALP"): "A", ("IND", "BrahMos"): "SLA",
    ("PAK", "Babur"): "LU",              # Babur-3 submarine version deployed 2018 (Wikipedia)
    ("KOR", "Hyunmoo-3"): "L", ("KOR", "Haeseong"): "S", ("JPN", "Tomahawk"): "S", ("JPN", "Type 12 (improved)"): "L",
    ("TWN", "Hsiung Feng IIE"): "L", ("TWN", "Hsiung Feng III"): "SL", ("TUR", "SOM"): "A", ("TUR", "Atmaca"): "SL",
    ("IRN", "Hoveyzeh"): "L", ("IRN", "Paveh"): "L", ("IRN", "Noor"): "SL", ("IRN", "Qader"): "SL",
    ("AUS", "Tomahawk"): "S", ("AUS", "JASSM-ER"): "A", ("AUS", "NSM"): "S",    # fired from Hobart-class destroyers
    ("ARE", "Black Shaheen"): "A", ("BRA", "MTC-300"): "L", ("NOR", "NSM"): "SL",
    ("ITA", "Teseo / Otomat"): "S", ("SWE", "RBS-15"): "SLA", ("ISR", "Gabriel V"): "S",
    ("SGP", "Blue Spear"): "S", ("SGP", "Harpoon"): "S",           # Formidable-class frigates (Janes)
    ("MYS", "Exocet"): "SU",                                        # MM40 frigates, SM39 Scorpène submarines
    ("IDN", "Exocet"): "S", ("IDN", "C-705"): "S", ("IDN", "BrahMos"): "L", ("IDN", "Atmaca"): "S",   # BrahMos: coastal battery
    ("PHL", "BrahMos"): "L", ("VNM", "Bastion (P-800 Oniks)"): "L", ("VNM", "BrahMos"): "L",          # shore-based batteries
    # Air and missile defence with naval versions in service
    ("USA", "SM-6"): "SL", ("USA", "SM-3"): "SL", ("JPN", "SM-3 Block IIA"): "S",
    ("CHN", "HQ-9"): "LS", ("CHN", "HQ-16"): "LS", ("RUS", "Pantsir"): "LS", ("ISR", "Barak MX"): "LS",
    ("FRA", "VL MICA"): "LS", ("FRA", "Mistral"): "LS",
    ("SGP", "Aster 15 (naval)"): "S", ("SGP", "VL MICA (naval)"): "S", ("SGP", "Barak"): "S",
    ("MYS", "Sea Wolf"): "S", ("MYS", "VL MICA (naval)"): "S", ("IDN", "VL MICA (naval)"): "S",
}
_LAUNCH_DEFAULT = {"Hypersonic": None, "Ballistic missiles": "L", "Cruise missiles": None}   # None: must be listed above
for _name, _cat in BATTLE_LAB.items():
    for _c in _cat["countries"]:
        _c["systems"] = [dict(s) for s in _c["systems"]]   # shared definitions become per-country copies
        for _s in _c["systems"]:
            _code = _LAUNCH.get((_c["code"], _s["name"]))
            if _code is None:
                _code = _LAUNCH_DEFAULT.get(_name, "L")
                assert _code is not None, "no launch platform listed for %s %s" % (_c["code"], _s["name"])
            _s["launch"] = [{"L": "Land", "S": "Ship", "U": "Submarine", "A": "Air"}[ch] for ch in "LSUA" if ch in _code]

for _cat in BATTLE_LAB.values():
    for _c in _cat["countries"]:
        for _s in _c["systems"]:
            _s["role"] = "defensive" if _cat["group"] == "Defence" else "offensive"

BATTLE_PAGE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Battle Lab</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400&display=swap">
<style>
/* Layout: the map fills the whole window; the bar (home link, weapon picker, title) floats over its top edge.
   Desktop: the roster is a dark panel down the left. Phone: it becomes a bottom sheet.
   Look: a dark command-map style (this page only); the rest of the site keeps the pastel theme. */
:root{
  color-scheme:dark;
  --bg:#0d1216;
  --ink:#d9e2d6;
  --muted:#8e9c93;
  --line:#34434b;
  --panel:rgb(16 23 28 / .94);
  --panel2:#18222a;
  --sea:#142029;
  --land:#2e3829;
  --land-edge:#5f6d55;
  --has:#4a5a3c;
  --sel:#6f5521;
  --accent:#e3b34a;
  --accent-ink:#1c1505;
  --token:#cbbd92;
  --token-ink:#1b1f17;
  --barh:64px;
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
}
[hidden]{display:none!important}
html,body{height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);overflow:hidden}
button{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}

/* the board fills the window */
#map{position:fixed;inset:0;overflow:hidden;background:var(--sea)}
#map svg{display:block;width:100%;height:100%;cursor:grab}
#map svg:active{cursor:grabbing}
.vig{position:absolute;inset:0;pointer-events:none;background:radial-gradient(ellipse at 50% 55%,transparent 55%,rgb(0 0 0 / .5))}
.land{fill:var(--land);stroke:var(--land-edge);stroke-width:.6;vector-effect:non-scaling-stroke}
.land.has{fill:var(--has);cursor:pointer}
.land.sel{fill:var(--sel);stroke:var(--accent);stroke-width:1.4}
.grid{stroke:rgb(150 185 150 / .14);stroke-width:.8;vector-effect:non-scaling-stroke}
.grid.major{stroke:rgb(227 179 74 / .4);stroke-width:1;stroke-dasharray:6 5}
.edge{fill:none;stroke:rgb(150 185 150 / .35);stroke-width:1.2;vector-effect:non-scaling-stroke}
.tick{font-family:var(--display);font-weight:700;font-size:10px;letter-spacing:.12em;fill:rgb(217 226 214 / .55);paint-order:stroke;stroke:var(--sea);stroke-width:3px;pointer-events:none}
.ctr{cursor:pointer}
.ctr .sh{fill:#000;opacity:.7}
.ctr .face{fill:var(--token);stroke:#000;stroke-width:1.2}
.ctr.on .face{fill:var(--accent)}
.ctr .cd{font-family:var(--display);font-weight:700;font-size:10px;letter-spacing:.08em;fill:var(--token-ink);text-anchor:middle;dominant-baseline:central}
.ctr .bd{fill:var(--panel2);stroke:var(--accent);stroke-width:1.2}
.ctr .bn{font-family:var(--display);font-weight:700;font-size:9px;fill:var(--accent);text-anchor:middle;dominant-baseline:central}
.msg{position:absolute;inset:0;display:grid;place-items:center;padding:16px;pointer-events:none}
.msg p{margin:0;max-width:22rem;padding:12px 18px;border:1px solid var(--line);border-radius:12px;background:var(--panel);text-align:center;font-size:1rem;line-height:1.45}
.msg.err p{color:#f0a08a}

/* floating bar */
.bar{position:fixed;top:0;left:0;right:0;z-index:5;display:grid;grid-template-columns:auto auto 1fr;align-items:center;gap:8px 18px;padding:12px 16px 20px;
  background:linear-gradient(rgb(13 18 22 / .94) 55%,transparent);pointer-events:none}
.bar>*{pointer-events:auto}
.back{color:var(--muted);font-family:var(--display);font-weight:500;font-size:.9rem;letter-spacing:.06em;text-decoration:none}
.back:hover{color:var(--ink);text-decoration:underline}
h1{margin:0;justify-self:end;font-family:var(--display);font-weight:700;font-size:clamp(1.05rem,3vw,1.6rem);letter-spacing:.24em;text-transform:uppercase;line-height:1.1;white-space:nowrap;
  color:var(--ink);text-shadow:2px 2px 0 rgb(227 179 74 / .5)}
.pick{display:inline-flex;align-items:center;gap:8px;justify-self:start;cursor:pointer;padding:5px 11px 5px 13px;border-radius:999px;border:1.5px solid var(--accent);
  background:rgb(227 179 74 / .1);color:var(--accent);font-family:var(--display);font-weight:700;font-size:.7rem;letter-spacing:.14em;text-transform:uppercase;white-space:nowrap;
  box-shadow:0 2px 0 rgb(0 0 0 / .6)}
.pick.set{background:var(--accent);color:var(--accent-ink)}
/* two pickers: strike (red, like its rings) and defence (blue); filled = the one being browsed */
.pick.strike{border-color:#f06050;background:rgb(224 72 60 / .12);color:#f6a493}
.pick.defence{border-color:#78aaff;background:rgb(80 140 240 / .12);color:#a9c8ff}
.pick.strike.set{background:#e0483c;border-color:#f06050;color:#fff}
.pick.defence.set{background:#3f7be0;border-color:#78aaff;color:#fff}
.pick:active{transform:translateY(2px);box-shadow:none}
.pick svg{width:10px;height:10px;fill:none;stroke:currentColor;stroke-width:2.6;stroke-linecap:round;stroke-linejoin:round}
.ctrl{display:flex;flex-wrap:wrap;align-items:center;gap:8px;min-width:0}
/* launcher button: solid and pulsing until a launcher is placed, so it is hard to miss */
.launch{display:inline-flex;align-items:center;justify-content:center;gap:8px;cursor:pointer;padding:8px 16px;border-radius:999px;border:2px solid var(--accent);background:var(--accent);color:var(--accent-ink);
  font-family:var(--display);font-weight:700;font-size:.78rem;letter-spacing:.12em;text-transform:uppercase;white-space:nowrap;box-shadow:0 3px 0 #000}
.launch.nudge{animation:nudge 1.6s ease-out infinite}
@keyframes nudge{0%{box-shadow:0 3px 0 #000,0 0 0 0 rgb(227 179 74 / .7)}70%{box-shadow:0 3px 0 #000,0 0 0 12px rgb(227 179 74 / 0)}100%{box-shadow:0 3px 0 #000,0 0 0 0 rgb(227 179 74 / 0)}}
.launch.moved{background:var(--panel);color:var(--accent)}
.launch[aria-pressed="true"]{background:#c8553d;border-color:#f0a08a;color:#fff}
.launch:active{transform:translateY(2px);box-shadow:none}
.launch svg{width:15px;height:15px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round}
.det .launch{width:100%;box-sizing:border-box;padding:11px 16px;font-size:.8rem;white-space:normal;text-align:center}
.ring-off{fill:rgb(224 72 60 / .13);stroke:rgb(240 96 80 / .85);stroke-width:1.5;stroke-dasharray:7 5;vector-effect:non-scaling-stroke;pointer-events:none}
.ring-def{fill:rgb(80 140 240 / .13);stroke:rgb(120 170 255 / .85);stroke-width:1.5;stroke-dasharray:7 5;vector-effect:non-scaling-stroke;pointer-events:none}
.key{display:inline-block;width:12px;height:12px;margin-right:6px;vertical-align:-2px;border-radius:50%;border:1.5px dashed}
.key.off{background:rgb(224 72 60 / .25);border-color:#f06050}
.key.def{background:rgb(80 140 240 / .25);border-color:#78aaff}
#map svg.placing{cursor:crosshair}
/* systems on the map: chips under the weapon button (red strike, blue defence) */
.chips{grid-column:2 / -1;display:flex;flex-wrap:wrap;gap:6px}
.chip{display:inline-flex;align-items:center;border-radius:999px;border:1.5px solid;background:var(--panel);box-shadow:0 2px 0 #000}
.chip.off{border-color:#f06050;color:#f6a493}
.chip.def{border-color:#78aaff;color:#a9c8ff}
.chip.cur{box-shadow:0 0 0 2px rgb(255 255 255 / .25),0 2px 0 #000}
.chip button{border:0;background:none;color:inherit;cursor:pointer;font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.06em}
.chip .cs{display:inline-flex;align-items:center;gap:6px;padding:4px 4px 4px 10px}
.chip .cs{padding:4px 11px 4px 10px}
.chips .clr{flex:none;cursor:pointer;padding:4px 11px;border-radius:999px;border:1.5px dashed var(--muted);background:var(--panel);color:var(--muted);
  font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.06em}
.chips .clr:hover{color:var(--ink);border-color:var(--ink)}
.chips .clr.armed{border-style:solid;border-color:#f0a08a;background:#c8553d;color:#fff}
.chip{flex:none}
/* in the details: move, remove, place another */
.dacts{display:grid;gap:8px}
.dacts .rmv,.dacts .addl{cursor:pointer;width:100%;box-sizing:border-box;padding:9px 14px;border-radius:999px;font-family:var(--display);font-weight:700;
  font-size:.74rem;letter-spacing:.1em;text-transform:uppercase;white-space:normal;text-align:center}
.dacts .rmv{border:1.5px solid #f0a08a;background:transparent;color:#f6a493}
.dacts .rmv:hover{background:rgb(224 72 60 / .15)}
.dacts .addl{border:1.5px dashed var(--accent);background:transparent;color:var(--accent)}
.dacts .addl:hover{background:rgb(227 179 74 / .1)}
@media (max-width:720px){.chips{flex-wrap:nowrap;overflow-x:auto;scrollbar-width:none;padding-bottom:2px}.chips::-webkit-scrollbar{display:none}}
.cdot{width:8px;height:8px;border-radius:50%;background:currentColor}
.land.held-off{stroke:#f06050;stroke-width:1.2}
.land.held-def{stroke:#78aaff;stroke-width:1.2}
.lnch.off .ring{stroke:#f06050}
.lnch.def .ring{stroke:#78aaff}
.lnch.off .pt{fill:#f06050}
.lnch.def .pt{fill:#78aaff}
.lname{font-family:var(--display);font-weight:700;font-size:10px;letter-spacing:.08em;text-anchor:middle;paint-order:stroke;stroke:#0d1216;stroke-width:3px}
.lnch.off .lname{fill:#f6a493}
.lnch.def .lname{fill:#a9c8ff}
.lnch{cursor:pointer}
.lnch .hitr{fill:transparent}
.lnch .shp,.lnch .sub{display:none}
.lnch.at-ship .veh,.lnch.at-sub .veh{display:none}
.lnch.at-ship .shp,.lnch.at-sub .sub{display:inline}
.lnch .mast{fill:none;stroke:#000;stroke-width:1.6;stroke-linecap:round}
/* platform tags in the weapon list */
.wp{display:flex;flex-wrap:wrap;gap:4px;margin-top:2px}
.ptag{padding:1px 7px;border-radius:999px;border:1px solid rgb(203 189 146 / .5);color:#cbbd92;font-family:var(--display);font-weight:700;font-size:.62rem;letter-spacing:.08em;text-transform:uppercase}
.lnch .lname{pointer-events:none}
.lnch.cur .ring{stroke-width:3;stroke-dasharray:none}
.lnch:focus-visible{outline:none}
.lnch:focus-visible .ring{stroke:#fff;stroke-width:3}
body.placing .lnch{cursor:crosshair}
.lnch .ring{fill:none;stroke:var(--accent);stroke-width:1.5;stroke-dasharray:3 3}
.lnch .pt{fill:var(--accent);stroke:#000;stroke-width:1}
.lnch .body{fill:var(--token);stroke:#000;stroke-width:1.2;stroke-linejoin:round}
.lnch .tube{fill:#9a8d63;stroke:#000;stroke-width:1.2}
.lnch .wh{fill:#1b1f17;stroke:var(--token);stroke-width:1}
.toast{position:fixed;z-index:6;top:calc(var(--barh) + 6px);left:calc(50% + 166px);transform:translateX(-50%);max-width:min(26rem,calc(100% - 360px));box-sizing:border-box;
  padding:10px 16px;border:1px solid var(--accent);border-radius:10px;background:var(--panel);color:var(--ink);font-size:.95rem;line-height:1.4;text-align:center;
  box-shadow:0 10px 24px rgb(0 0 0 / .5)}
.toast.err{border-color:#e07a5f;color:#f4b6a2}

/* zoom controls */
.zoom{position:fixed;right:14px;bottom:14px;z-index:4;display:grid;gap:6px}
.zb{display:grid;place-items:center;width:36px;height:36px;cursor:pointer;border-radius:9px;border:1px solid var(--line);background:var(--panel);
  font-family:var(--display);font-weight:700;font-size:1.1rem;line-height:1;box-shadow:0 2px 0 #000}
.zb:active{transform:translateY(2px);box-shadow:none}
.zb svg{width:16px;height:16px;fill:none;stroke:currentColor;stroke-width:2.2;stroke-linecap:round;stroke-linejoin:round}

/* roster: left panel on desktop */
#roster{position:fixed;z-index:4;left:12px;top:var(--barh);bottom:12px;width:min(320px,calc(100% - 24px));display:flex;flex-direction:column;min-height:0;box-sizing:border-box;
  border:1px solid var(--line);border-radius:14px;background:var(--panel);box-shadow:0 12px 30px rgb(0 0 0 / .45);-webkit-backdrop-filter:blur(6px);backdrop-filter:blur(6px)}
.rh{display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--line)}
.rtog{flex:1;min-width:0;display:grid;gap:2px;padding:12px 14px 10px;border:0;background:none;text-align:left;cursor:default}
.rtog .grip{display:none}
.peekact{display:none}
/* placing mode: one slim strip with the instruction (or error) and Cancel */
.placebar{position:fixed;z-index:6;top:calc(var(--barh) + 6px);left:calc(50% + 166px);transform:translateX(-50%);width:max-content;max-width:min(30rem,calc(100% - 360px));box-sizing:border-box;
  display:flex;align-items:center;gap:10px;padding:8px 8px 8px 14px;border:1.5px solid var(--accent);border-radius:999px;background:var(--panel);box-shadow:0 10px 24px rgb(0 0 0 / .5)}
.placebar svg{flex:none;width:16px;height:16px;fill:none;stroke:var(--accent);stroke-width:2;stroke-linecap:round}
.placebar span{font-size:.92rem;line-height:1.3}
.placebar.err{border-color:#e07a5f}
.placebar.err span{color:#f4b6a2}
.placebar button{flex:none;cursor:pointer;padding:6px 14px;border-radius:999px;border:0;background:#c8553d;color:#fff;
  font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.12em;text-transform:uppercase}
.rt{font-family:var(--display);font-weight:700;font-size:.9rem;letter-spacing:.2em;text-transform:uppercase;color:var(--accent)}
.rc{color:var(--muted);font-size:.86rem}
.rl{overflow-x:hidden;overflow-y:auto;overscroll-behavior:contain;padding:8px 10px 10px;display:grid;gap:6px;align-content:start;flex:1;min-height:0}
.nation{padding:8px;border-radius:10px;border:1px solid transparent}
/* grid children may not grow wider than the panel: long text wraps instead of forcing sideways scrolling */
.rl>*,.det,.det>*{min-width:0;max-width:100%}
.nation.on{border-color:var(--line);background:rgb(255 255 255 / .04)}
.nn{display:flex;align-items:center;gap:10px;width:100%;padding:2px 2px 8px;border:0;background:none;text-align:left}
/* level 1: one row per country */
.crow{display:flex;align-items:center;gap:10px;width:100%;padding:9px 8px;border:1px solid transparent;border-radius:10px;background:none;cursor:pointer;text-align:left}
.crow:hover{border-color:var(--line);background:rgb(255 255 255 / .04)}
.chev{flex:none;color:var(--accent);font-size:1.35rem;line-height:1}
/* level 2: back button and the country's systems */
.back2{justify-self:start;padding:5px 12px;border-radius:999px;border:1px solid var(--line);background:#1f2a32;cursor:pointer;
  font-family:var(--display);font-weight:700;font-size:.7rem;letter-spacing:.12em;text-transform:uppercase}
.back2:hover{border-color:var(--accent);color:var(--accent)}
.wl{display:grid;gap:6px}
.wr{display:grid;gap:3px;width:100%;padding:9px 12px;border-radius:10px;border:1px solid #4a5a62;background:#1f2a32;cursor:pointer;text-align:left;box-shadow:0 2px 0 #000}
.wr:hover{border-color:var(--accent)}
.wr:active{transform:translateY(2px);box-shadow:none}
.wn{display:flex;align-items:center;gap:8px;font-family:var(--display);font-weight:700;font-size:.92rem;letter-spacing:.03em}
.wk{color:var(--muted);font-size:.82rem;padding-left:16px}
.wo{justify-self:start;margin-left:16px;padding:1px 8px;border-radius:999px;border:1px solid rgb(203 189 146 / .5);color:var(--token);font-size:.72rem;letter-spacing:.04em}
.wo.imp{border-color:rgb(120 170 255 / .5);color:#a9c8ff}
.onmap{margin-left:auto;padding:1px 8px;border-radius:999px;border:1px solid var(--accent);color:var(--accent);font-size:.62rem;letter-spacing:.14em;text-transform:uppercase}
.tok{flex:none;display:grid;place-items:center;width:40px;height:24px;border:1px solid #000;border-radius:3px;background:var(--token);color:var(--token-ink);
  font-family:var(--display);font-weight:700;font-size:.68rem;letter-spacing:.08em;box-shadow:2px 2px 0 rgb(0 0 0 / .7)}
.nation.on .tok{background:var(--accent)}
.cn{flex:1;min-width:0;font-weight:600;font-size:1.02rem;line-height:1.2}
.cnt{flex:none;color:var(--muted);font-size:.82rem}
.wrow{display:flex;flex-wrap:wrap;gap:7px}
.wb{display:inline-flex;align-items:center;gap:7px;max-width:100%;cursor:pointer;padding:5px 11px;border-radius:999px;border:1px solid #4a5a62;background:#1f2a32;
  font-family:var(--display);font-weight:500;font-size:.82rem;letter-spacing:.03em;line-height:1.2;text-align:left;box-shadow:0 2px 0 #000}
.wb:hover{border-color:var(--muted)}
.wb:active{transform:translateY(2px);box-shadow:none}
.wb[aria-pressed="true"]{background:var(--accent);border-color:var(--accent);color:var(--accent-ink);font-weight:700}
.dot{flex:none;width:8px;height:8px;border-radius:50%;border:1px solid rgb(0 0 0 / .6)}
.legend{display:flex;flex-wrap:wrap;gap:4px 12px;padding:8px 14px 10px;border-top:1px solid var(--line);color:var(--muted);font-size:.78rem}
.legend span{display:inline-flex;align-items:center;gap:5px}
.s-in-service{background:#6fbf8e}
.s-reported{background:#8fd0c6}
.s-fielding{background:#7d98ec}
.s-tested{background:#e3c35a}
.s-dev{background:transparent;border-color:#c8d0d8!important}
.s-claimed{background:#e08a6a}
.s-order{background:transparent;border-color:var(--accent)!important}
.foot{margin:0;padding:0 14px 10px;color:var(--muted);font-size:.75rem;line-height:1.4}

/* weapon detail, shown under the chosen country */
.det{margin:4px 2px 0;padding:14px 8px 4px;border-top:1px solid var(--line);display:grid;gap:10px}
.dn{margin:0;font-family:var(--display);font-weight:700;font-size:1.25rem;letter-spacing:.05em;line-height:1.2;color:var(--accent);overflow-wrap:anywhere}
.dk{margin:-6px 0 0;color:var(--muted);font-size:.92rem}
.pill{justify-self:start;display:inline-flex;align-items:center;gap:6px;padding:2px 10px;border:1px solid var(--line);border-radius:999px;
  font-family:var(--display);font-weight:700;font-size:.68rem;letter-spacing:.12em;text-transform:uppercase}
.dd{margin:0;font-size:.98rem;line-height:1.5}
.stats{margin:0;display:grid;gap:6px}
.stat{display:grid;grid-template-columns:4.4rem minmax(0,1fr);gap:10px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:rgb(0 0 0 / .2)}
.stat dt{margin:0;padding-top:.2em;font-family:var(--display);font-weight:700;font-size:.68rem;letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}
.stat dd{margin:0;font-size:.95rem;line-height:1.4;overflow-wrap:anywhere}
.dnote{margin:0;color:var(--muted);font-size:.8rem;font-style:italic}
.il{justify-self:start;color:var(--accent);font-weight:600;font-size:.92rem}

/* weapon picker */
dialog{width:min(420px,calc(100vw - 32px));max-height:calc(100dvh - 40px);overflow-y:auto;overscroll-behavior:contain;box-sizing:border-box;padding:18px;border:1px solid var(--line);border-radius:14px;color:var(--ink);background:var(--panel2);box-shadow:0 24px 48px rgb(0 0 0 / .6)}
dialog::backdrop{background:rgb(0 0 0 / .55)}
.dh{margin:0 0 2px;font-family:var(--display);font-weight:700;font-size:.95rem;letter-spacing:.2em;text-transform:uppercase;color:var(--accent)}
.ds{margin:0 0 14px;color:var(--muted);font-size:.92rem}
.opts{display:grid;gap:8px}
.grp{margin:8px 0 0;font-family:var(--display);font-weight:700;font-size:.7rem;letter-spacing:.2em;text-transform:uppercase}
.grp:first-child{margin-top:0}
.grp.off{color:#f6a493}
.grp.def{color:#a9c8ff}
.opt{display:grid;grid-template-columns:auto 1fr;gap:3px 12px;align-items:center;width:100%;cursor:pointer;padding:12px 14px;text-align:left;border:1px solid var(--line);border-radius:10px;
  background:#1f2a32;box-shadow:0 2px 0 #000}
.opt:hover{border-color:var(--muted)}
.opt:active{transform:translateY(2px);box-shadow:none}
.opt[aria-pressed="true"]{border-color:var(--accent)}
.opt svg{grid-row:span 2;width:34px;height:34px}
.on1{font-family:var(--display);font-weight:700;font-size:.9rem;letter-spacing:.14em;text-transform:uppercase}
.on2{color:var(--muted);font-size:.86rem;line-height:1.35}
.soon{margin:12px 0 0;color:var(--muted);font-size:.84rem;font-style:italic;text-align:center}

/* phone: one slim header row; the roster is a bottom sheet that tucks away when you use the map;
   while placing a launcher only the map and the placing strip are shown */
@media (max-width:720px){
  .bar{grid-template-columns:auto minmax(0,1fr) auto;gap:10px;padding:10px 12px 16px}
  .back{font-size:1.25rem;line-height:1}
  .bk{display:none}
  .bar h1{font-size:.8rem;letter-spacing:.14em;white-space:normal;text-align:right;line-height:1.15}
  .chips{grid-column:1 / -1}
  #launch{display:none!important}
  .toast,.placebar{left:50%;max-width:calc(100% - 24px)}
  body.placing .bar,body.placing #roster{display:none}
  body.placing .placebar{top:12px}
  #roster{left:0;right:0;top:auto;bottom:0;width:auto;max-height:58%;border-width:1px 0 0;border-radius:16px 16px 0 0;transition:transform .25s ease-out}
  #roster.closed{transform:translateY(calc(100% - 62px))}
  .rh{position:relative}
  .rtog{cursor:pointer;padding:16px 6px 10px 14px;min-height:62px;box-sizing:border-box}
  .rtog .grip{display:block;position:absolute;left:50%;top:6px;transform:translateX(-50%);width:40px;height:4px;border-radius:2px;background:var(--muted)}
  .rc{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .peekact:not([hidden]){display:inline-flex;flex:none;align-items:center;margin-right:12px;cursor:pointer;padding:8px 14px;border-radius:999px;border:2px solid var(--accent);
    background:var(--accent);color:var(--accent-ink);font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.1em;text-transform:uppercase;box-shadow:0 2px 0 #000}
  .peekact.moved{background:var(--panel);color:var(--accent)}
  .det .launch{display:none}   /* the sheet's strip already has this button */
  .foot{display:none}
  #zin,#zout{display:none}
  .zoom{bottom:74px}
}
@media (prefers-reduced-motion:reduce){#roster{transition:none}.launch.nudge{animation:none}}
</style>
</head><body>
<main id="map" aria-label="World map">
  <div class="vig" aria-hidden="true"></div>
  <div class="msg" id="msg"><p>Loading the map…</p></div>
</main>
<header class="bar" id="bar">
  <a class="back" href="/" aria-label="Home">&larr;<span class="bk"> Home</span></a>
  <div class="ctrl">
    <button class="pick strike" id="pickS" type="button" aria-haspopup="dialog"><span class="pt">Choose your Strike</span>
      <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 6l5 5 5-5"/></svg></button>
    <button class="pick defence" id="pickD" type="button" aria-haspopup="dialog"><span class="pt">Choose your Defence</span>
      <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 6l5 5 5-5"/></svg></button>
    <button class="launch" id="launch" type="button" aria-pressed="false" hidden>
      <svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5"/><path d="M8 1v3M8 12v3M1 8h3M12 8h3"/></svg>
      <span class="lt">Select Launcher Position</span></button>
  </div>
  <h1>Battle Lab</h1>
  <div class="chips" id="chips" aria-label="On the map" hidden></div>
</header>
<div class="toast" id="toast" role="status" aria-live="polite" hidden></div>
<div class="placebar" id="placebar" role="status" aria-live="polite" hidden>
  <svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5"/><path d="M8 1v3M8 12v3M1 8h3M12 8h3"/></svg>
  <span id="placemsg"></span>
  <button type="button" id="placecancel">Cancel</button>
</div>
<div class="zoom" id="zoom" hidden>
  <button class="zb" id="zin" type="button" aria-label="Zoom in">+</button>
  <button class="zb" id="zout" type="button" aria-label="Zoom out">&minus;</button>
  <button class="zb" id="zfit" type="button" aria-label="Show the whole world">
    <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M3 7V3h4M13 3h4v4M17 13v4h-4M7 17H3v-4"/></svg></button>
</div>
<aside id="roster" hidden aria-live="polite">
  <div class="rh">
    <button class="rtog" id="rh" type="button" aria-expanded="true">
      <span class="grip" aria-hidden="true"></span>
      <span class="rt" id="rt"></span>
      <span class="rc" id="rc"></span>
    </button>
    <button class="peekact" id="peekact" type="button" hidden></button>
  </div>
  <div class="rl" id="rl"></div>
  <div class="legend" id="legend"></div>
  <p class="foot">From public reporting. Figures and status are approximate; follow the link for sources.</p>
</aside>

<dialog id="dlg" aria-labelledby="dh">
  <p class="dh" id="dh">Choose your Strike</p>
  <p class="ds" id="ds">Pick a weapon type to see who has it.</p>
  <div class="opts" id="opts"></div>
  <p class="soon">Each country lists the systems it has, made locally or bought, and how each is launched. Land launchers go inside that country; ships and submarines anywhere at sea.</p>
</dialog>

<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3.1.0/dist/topojson-client.min.js"></script>
<script>
var DATA = __BATTLE_DATA__;
var WORLD_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-50m.json';
var STATUS = [['In service', 's-in-service'], ['Reported in service', 's-reported'], ['Fielding', 's-fielding'],
              ['Tested', 's-tested'], ['In development', 's-dev'], ['On order', 's-order'], ['Claimed', 's-claimed']];
var TOP = 84, BOTTOM = -58;            // latitudes shown; the far south is cropped
var SPAN = (TOP - BOTTOM) / 360;       // natural height / width of the map
var MAX_STRETCH = 1.6;                 // desktop: how far north-south may stretch to fill the screen
var PANEL = 332;                       // desktop roster width incl. margin
var MAXK = 400;                        // deepest zoom: enough for Singapore to fill a phone screen
var kFloor = 1;                        // shallowest zoom used when flying to a country (set in build)

function $(s) { return document.querySelector(s); }
function el(tag, text, cls) { var e = document.createElement(tag); if (text != null) e.textContent = text; if (cls) e.className = cls; return e; }
function statusClass(s) { for (var i = 0; i < STATUS.length; i++) if (STATUS[i][0] === s) return STATUS[i][1]; return 's-dev'; }
function phone() { return window.matchMedia('(max-width: 720px)').matches; }
function slow() { return window.matchMedia('(prefers-reduced-motion: reduce)').matches; }
function barH() { return $('#bar').offsetHeight; }

var mode = null, selIso = null, selSys = null;   // the category, country and system being looked at
var placing = false;
// What is on the map: any number of launchers, strike and defence, each { id, cat, c: country, s: system, at: [lon, lat] }.
// Placing never replaces an earlier one; tap a launcher (on the map or its chip) to move or remove it.
var onmap = [], nextId = 1;
var curId = null;      // the launcher being looked at, when the system shown is one on the map
var moveId = null;     // while placing: the launcher being moved (null = placing a new one)
function grpOf(cat) { return DATA[cat].group; }
function entry(id) { return onmap.filter(function (e) { return e.id === id; })[0] || null; }
function selSlot() { var e = entry(curId); return e && selSys && e.s === selSys ? e : null; }
function curLauncher() { var sl = selSlot(); return sl ? sl.at : null; }
function placed() { return onmap; }
function lastOf(s, iso) {   // the most recently placed launcher of this system in this country
  var l = onmap.filter(function (e) { return e.s === s && (!iso || e.c.iso === iso); });
  return l.length ? l[l.length - 1] : null;
}
var features = null, W = 0, H = 0, sy = 1, proj, path, svg, gWorld, gRing, gTicks, gCtr, gLaunch, zoom, box, T = d3.zoomIdentity, hexPat, lands;
var KM_PER_DEG = 111.195;   // great-circle km per degree on a 6,371 km Earth

/* Equirectangular, optionally stretched north-south: lines of latitude and longitude stay straight. */
function P(ll) { var p = proj(ll); return [p[0], p[1] * sy]; }

/* ---------- map ---------- */
function build() {
  var host = $('#map');
  // a hidden or not-yet-laid-out page has no size; wait for the resize observer to call again
  if (!host.clientWidth || !host.clientHeight) return;
  if (svg) svg.remove();
  W = host.clientWidth; H = host.clientHeight;
  var natH = W * SPAN;
  sy = phone() ? 1 : Math.max(1, Math.min(H / natH, MAX_STRETCH));
  proj = d3.geoEquirectangular().scale(W / (2 * Math.PI)).center([0, TOP]).translate([W / 2, 0]);
  var stretch = d3.geoTransform({ point: function (x, y) { this.stream.point(x, y * sy); } });
  path = d3.geoPath({ stream: function (s) { return proj.stream(stretch.stream(s)); } });
  box = [[0, 0], [W, natH * sy]];
  var bw = W, bh = natH * sy;

  svg = d3.select(host).insert('svg', ':first-child').attr('viewBox', '0 0 ' + W + ' ' + H).attr('role', 'img').attr('aria-label', 'Flat world map with latitude and longitude grid');
  var defs = svg.append('defs');
  defs.append('clipPath').attr('id', 'mapclip').append('rect').attr('width', bw).attr('height', bh);
  hexPat = defs.append('pattern').attr('id', 'hex').attr('patternUnits', 'userSpaceOnUse').attr('width', 36).attr('height', 20.78);
  hexPat.append('path').attr('d', 'M0,10.39 L6,0 L18,0 L24,10.39 L18,20.78 L6,20.78 Z M24,10.39 L36,10.39')
    .attr('fill', 'none').attr('stroke', 'rgb(160 200 160 / .07)').attr('stroke-width', 1);

  gWorld = svg.append('g');
  gWorld.append('rect').attr('width', bw).attr('height', bh).attr('fill', 'var(--sea)');
  lands = gWorld.append('g').selectAll('path').data(features).join('path')
    .attr('class', 'land').attr('d', path)
    .on('click', function (ev, d) { if (placing) return; var c = countryByIso(d.id); if (c) pickCountry(c, false, true); });
  lands.append('title').text(function (d) { return d.properties.name; });

  var gGrid = gWorld.append('g');
  for (var lon = -180; lon <= 180; lon += 15) {
    var x = P([lon, 0])[0];
    gGrid.append('line').attr('class', 'grid' + (lon === 0 ? ' major' : '')).attr('x1', x).attr('x2', x).attr('y1', 0).attr('y2', bh);
  }
  for (var lat = -45; lat <= 75; lat += 15) {
    var y = P([0, lat])[1];
    gGrid.append('line').attr('class', 'grid' + (lat === 0 ? ' major' : '')).attr('x1', 0).attr('x2', bw).attr('y1', y).attr('y2', y);
  }
  gRing = gWorld.append('g').attr('clip-path', 'url(#mapclip)');   // the far north and south are cropped
  gWorld.append('rect').attr('class', 'edge').attr('width', bw).attr('height', bh);

  svg.append('rect').attr('width', W).attr('height', H).attr('fill', 'url(#hex)').attr('pointer-events', 'none');
  gTicks = svg.append('g');
  gTicks.selectAll('.lon').data(d3.range(-150, 151, 30)).join('text').attr('class', 'tick lon').attr('text-anchor', 'middle')
    .text(function (d) { return d === 0 ? '0°' : Math.abs(d) + '°' + (d < 0 ? 'W' : 'E'); });
  gTicks.selectAll('.lat').data(d3.range(-45, 76, 15)).join('text').attr('class', 'tick lat').attr('text-anchor', 'end').attr('dominant-baseline', 'middle')
    .text(function (d) { return d === 0 ? 'EQ' : Math.abs(d) + '°' + (d < 0 ? 'S' : 'N'); });
  gCtr = svg.append('g');
  gLaunch = svg.append('g');
  svg.classed('placing', placing);

  zoom = d3.zoom().scaleExtent([1, MAXK]).extent([[0, 0], [W, H]]).translateExtent(box).on('zoom', function (ev) { T = ev.transform; place(); });
  // phone: touching the map tucks the sheet away so it never sits over what you are looking at
  zoom.on('start', function (ev) { if (ev.sourceEvent && phone() && !$('#roster').classList.contains('closed')) setSheet(false); });
  svg.call(zoom).on('dblclick.zoom', null);
  svg.on('click', onMapClick);   // d3-zoom swallows the click that ends a drag, so panning never places
  var cover = Math.max(W / bw, H / bh);
  var k0 = phone() ? cover : Math.min(cover, 1.5);   // phone: fill the height and pan sideways
  kFloor = phone() ? cover : 1;                       // zooming to a country never leaves empty bands on a phone
  var c0 = P(phone() ? [60, 30] : [10, 13]);
  svg.call(zoom.transform, clamp(d3.zoomIdentity.translate(W / 2, H / 2).scale(k0).translate(-c0[0], -c0[1])));
  $('#zoom').hidden = false;
  paint();
}

// zoom.transform does not keep the map inside its edges by itself (only drags do), so views we set go through this
function clamp(t) { return zoom.constrain()(t, [[0, 0], [W, H]], box); }

function place() {
  gWorld.attr('transform', T);
  hexPat.attr('patternTransform', 'translate(' + (T.x % 36) + ',' + (T.y % 20.78) + ')');
  var bar = barH(), top = Math.max(T.applyY(0), bar) + 12, right = Math.min(T.applyX(box[1][0]), W) - 8;
  var left = !phone() && mode ? PANEL : 0, bottom = H - (phone() ? sheetH() : 120);
  gTicks.selectAll('.lon').attr('x', function (d) { return T.applyX(P([d, 0])[0]); }).attr('y', top)
    .attr('visibility', function (d) { var x = T.applyX(P([d, 0])[0]); return x < left + 18 || x > W - 18 ? 'hidden' : 'visible'; });
  gTicks.selectAll('.lat').attr('x', right).attr('y', function (d) { return T.applyY(P([0, d])[1]); })
    .attr('visibility', function (d) { var y = T.applyY(P([0, d])[1]); return y < top + 14 || y > bottom ? 'hidden' : 'visible'; });
  gCtr.selectAll('.ctr').attr('transform', function (d) { var p = T.apply(P(d.at)); return 'translate(' + p[0] + ',' + p[1] + ')'; });
  gLaunch.selectAll('.lnch').attr('transform', function (d) { var p = T.apply(P(d.at)); return 'translate(' + p[0] + ',' + p[1] + ')'; });
}

function cls(sl) { return sl.s.role === 'defensive' ? 'def' : 'off'; }

function ringData() {
  return placed().filter(function (sl) { return sl.s.ring_km; }).map(function (sl) {
    return { geo: d3.geoCircle().center(sl.at).radius(sl.s.ring_km / KM_PER_DEG)(), cls: cls(sl) };
  });
}

// launcher label, e.g. "S-400 · 400 km" (the radius sits with the launcher so labels never collide)
function lnLabel(sl) { return sl.s.name + (sl.s.ring_km ? ' · ' + sl.s.ring_km.toLocaleString('en-US') + ' km' : ''); }

function shown() {
  if (!mode) return [];
  var all = DATA[mode].countries;
  return selIso ? all.filter(function (c) { return c.iso === selIso; }) : all;
}

function paint() {
  if (!lands) return;
  var list = shown(), has = {};
  list.forEach(function (c) { has[c.iso] = true; });
  var held = {};   // countries with launchers on the map, outlined in their ring colour
  onmap.forEach(function (e) { held[e.c.iso] = cls(e); });
  lands.classed('has', function (d) { return !!has[d.id]; }).classed('sel', function (d) { return d.id === selIso; })
    .classed('held-off', function (d) { return held[d.id] === 'off' && d.id !== selIso; })
    .classed('held-def', function (d) { return held[d.id] === 'def' && d.id !== selIso; });
  // once the chosen system's launcher is placed it replaces that country's counter, so the two never overlap
  var hide = curLauncher() && selSlot().on !== 'sea' ? selIso : null;
  var ctr = gCtr.selectAll('.ctr').data(list.filter(function (c) { return c.iso !== hide; }), function (d) { return d.iso; }).join(function (enter) {
    var g = enter.append('g').attr('class', 'ctr').attr('role', 'button').attr('tabindex', 0)
      .attr('aria-label', function (d) { return d.name + ', ' + d.systems.length + ' systems'; })
      .on('click', function (ev, d) { if (!placing) pickCountry(d, false, true); })
      .on('keydown', function (ev, d) { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); pickCountry(d, false, true); } });
    g.append('rect').attr('class', 'sh').attr('x', -16).attr('y', -9).attr('width', 36).attr('height', 22).attr('rx', 2);
    g.append('rect').attr('class', 'face').attr('x', -19).attr('y', -12).attr('width', 36).attr('height', 22).attr('rx', 2);
    g.append('text').attr('class', 'cd').attr('x', -1).attr('y', -1).text(function (d) { return d.code; });
    g.append('circle').attr('class', 'bd').attr('cx', 17).attr('cy', -12).attr('r', 7.5);
    g.append('text').attr('class', 'bn').attr('x', 17).attr('y', -12).text(function (d) { return d.systems.length; });
    return g;
  });
  ctr.classed('on', function (d) { return d.iso === selIso; });
  ctr.filter(function (d) { return d.iso === selIso; }).raise();
  var rings = ringData();
  gRing.selectAll('path').data(rings).join('path').attr('class', function (d) { return 'ring-' + d.cls; }).attr('d', function (d) { return path(d.geo); });
  // generic launcher vehicle: chassis, cab, raised container, wheels; the dot marks the exact spot
  var ln = gLaunch.selectAll('.lnch').data(placed(), function (d) { return d.id; }).join(function (enter) {
    var g = enter.append('g').attr('role', 'button').attr('tabindex', 0)
      .on('click', function (ev, d) { if (placing) return; ev.stopPropagation(); showEntry(d.id); })   // while placing, a tap places
      .on('keydown', function (ev, d) { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); showEntry(d.id); } });
    g.append('circle').attr('class', 'hitr').attr('r', 22);   // a generous tap target
    g.append('circle').attr('class', 'ring').attr('r', 16);
    var v = g.append('g').attr('class', 'veh').attr('transform', 'translate(-17,-25)');   // land: launcher truck
    v.append('rect').attr('class', 'tube').attr('x', 4).attr('y', 3).attr('width', 22).attr('height', 8).attr('rx', 1.5).attr('transform', 'rotate(-14 4 11)');
    v.append('rect').attr('class', 'body').attr('x', 2).attr('y', 12).attr('width', 24).attr('height', 6).attr('rx', 1);
    v.append('path').attr('class', 'body').attr('d', 'M26 18V10h5l3 4v4z');
    [8, 16, 29].forEach(function (x) { v.append('circle').attr('class', 'wh').attr('cx', x).attr('cy', 20).attr('r', 3); });
    var sh = g.append('g').attr('class', 'shp').attr('transform', 'translate(-17,-24)');   // sea: ship
    sh.append('path').attr('class', 'body').attr('d', 'M1 13h32l-5 7H6z');
    sh.append('rect').attr('class', 'body').attr('x', 11).attr('y', 6).attr('width', 11).attr('height', 7).attr('rx', 1);
    sh.append('path').attr('class', 'mast').attr('d', 'M16.5 1v5M13 3h7');
    var sb = g.append('g').attr('class', 'sub').attr('transform', 'translate(-17,-22)');    // sea: submarine
    sb.append('path').attr('class', 'body').attr('d', 'M2 13c0-3 4-5 10-5h12c5 0 9 2 9 5s-4 5-9 5H12C6 18 2 16 2 13z');
    sb.append('rect').attr('class', 'body').attr('x', 13).attr('y', 3).attr('width', 6).attr('height', 6).attr('rx', 1.5);
    g.append('circle').attr('class', 'pt').attr('r', 2.6);
    g.append('text').attr('class', 'lname').attr('y', 18);
    return g;
  });
  ln.attr('class', function (d) {
    var kind = d.on === 'sea' ? (seaWord(d.s) === 'ship' ? ' at-ship' : ' at-sub') : ' at-land';
    return 'lnch ' + cls(d) + kind + (d.id === curId && d.s === selSys ? ' cur' : '');
  })
    .attr('aria-label', function (d) { return lnLabel(d) + ' in ' + d.c.name + '. Open to move or remove.'; });
  ln.select('.lname').text(lnLabel);
  ln.filter(function (d) { return d.id === curId; }).raise();
  place();
}

function sheetH() {
  var r = $('#roster');
  return r.hidden || getComputedStyle(r).display === 'none' ? 0 : Math.max(0, H - r.getBoundingClientRect().top);
}

// The country's main landmass: the shape holding its counter, else its largest shape
// (so France means mainland France, not French Guiana, and Malaysia means the peninsula).
function countryShape(c) {
  var polys = [];
  features.forEach(function (f) {
    if (f.id !== c.iso) return;
    var g = f.geometry;
    (g.type === 'Polygon' ? [g.coordinates] : g.coordinates).forEach(function (p) { polys.push({ type: 'Polygon', coordinates: p }); });
  });
  var home = polys.filter(function (p) { return d3.geoContains(p, c.at); })[0];
  return home || polys.sort(function (a, b) { return d3.geoArea(b) - d3.geoArea(a); })[0] || null;
}

// Where the map should look for a country: the visible area left by the header, panel or sheet,
// and the zoom that makes the country fill most of it (small countries like Singapore zoom right in).
function viewFor(c) {
  var bar = barH(), sh = phone() ? sheetH() : 0;
  var x0 = phone() ? 0 : PANEL, y0 = bar, aw = W - x0, ah = H - bar - sh;
  var view = { cx: x0 + aw / 2, cy: y0 + ah / 2, k: Math.max(kFloor, phone() ? 2.2 : 2.5), p: P(c.at) };
  var shp = countryShape(c);
  if (shp) {
    var b = path.bounds(shp), dx = b[1][0] - b[0][0], dy = b[1][1] - b[0][1];
    if (dx > 0 && dy > 0 && dx < W * 0.6) {   // skip shapes that wrap round the date line
      view.k = Math.max(kFloor, Math.min(aw * 0.7 / dx, ah * 0.7 / dy, MAXK));
      view.p = [(b[0][0] + b[1][0]) / 2, (b[0][1] + b[1][1]) / 2];
    }
  }
  return view;
}

// A ship or submarine at sea: centre on it, zoomed so its range ring fills most of the visible area
// (no ring: a 150 km circle around it, so the vessel and its surroundings show).
function flyToSea(e) {
  if (!svg) return;
  var bar = barH(), sh = phone() ? sheetH() : 0;
  var x0 = phone() ? 0 : PANEL, aw = W - x0, ah = H - bar - sh;
  var circle = d3.geoCircle().center(e.at).radius((e.s.ring_km || 150) / KM_PER_DEG)();
  var b = path.bounds(circle), dx = b[1][0] - b[0][0], dy = b[1][1] - b[0][1], p = P(e.at);
  var k = dx > 0 && dy > 0 && dx < W * 0.9 ? Math.max(kFloor, Math.min(aw * 0.8 / dx, ah * 0.8 / dy, MAXK)) : Math.max(kFloor, 3);
  var t = clamp(d3.zoomIdentity.translate(x0 + aw / 2, bar + ah / 2).scale(k).translate(-p[0], -p[1]));
  (slow() ? svg : svg.transition().duration(750)).call(zoom.transform, t);
}

function flyTo(c, onlyIfSmall) {
  if (!svg) return;
  var v = viewFor(c);
  if (onlyIfSmall && v.k < T.k * 1.5) return;   // already zoomed in enough: leave the user's view alone
  var t = clamp(d3.zoomIdentity.translate(v.cx, v.cy).scale(v.k).translate(-v.p[0], -v.p[1]));
  (slow() ? svg : svg.transition().duration(750)).call(zoom.transform, t);
}

/* ---------- roster and weapon detail ---------- */
function countryByIso(iso) {
  if (!mode) return null;
  return DATA[mode].countries.filter(function (c) { return c.iso === iso; })[0] || null;
}

function detail(c, s) {
  var d = el('section', null, 'det');
  var st = el('span', null, 'pill');
  st.append(el('span', null, 'dot ' + statusClass(s.status)), document.createTextNode(s.status));
  var dl = el('dl', null, 'stats');
  var rows = [['Origin', origin(s)], ['Made by', s.maker || 'Not publicly confirmed'],
              ['Launched from', s.launch.length ? s.launch.map(function (x) { return x + ' launch'; }).join(' · ') : 'Not published'],
              ['Range', s.range], ['Speed', s.speed]], launcher = curLauncher(), sl = selSlot();
  if (launcher) {
    rows.push(['Launcher', fmtLL(launcher) + (sl.on === 'sea' ? ' (at sea, ' + seaWord(s) + ')' : '')]);
    rows.push(['Ring', s.ring_km ? s.ring_km.toLocaleString('en-US') + ' km radius. ' + s.ring_note + '.'
                                 : 'No published range, so no ring is drawn.']);
  }
  rows.forEach(function (r) {
    var row = el('div', null, 'stat'); row.append(el('dt', r[0]), el('dd', r[1])); dl.append(row);
  });
  var opts = placeOpts(s), acts = el('div', null, 'dacts');
  function placeBtn(where, label) {   // same look as the top-bar button
    var b = $('#launch').cloneNode(true);
    b.removeAttribute('id'); b.hidden = false;
    if (label) b.dataset.label = label;
    b.onclick = function () { setPlacing(!placing, launcher ? undefined : null, where); };
    return b;
  }
  if (!opts.length) {
    acts.append(el('p', s.launch.indexOf('Air') >= 0 ? 'Air-launched: placing aircraft on the map is not available yet.'
                                                       : 'Launch platform not published, so it cannot be placed yet.', 'dnote'));
  } else if (launcher) {
    acts.append(placeBtn(sl.on));   // moves this launcher, on land or at sea as placed
    var rm = el('button', 'Remove from map', 'rmv'); rm.type = 'button';
    rm.onclick = function () { removeEntry(curId); };
    acts.append(rm);
    opts.forEach(function (w) {
      var add = el('button', '+ Place another ' + (opts.length > 1 ? (w === 'land' ? 'on land' : 'at sea') : s.name), 'addl'); add.type = 'button';
      add.onclick = function () { setPlacing(true, null, w); };
      acts.append(add);
    });
  } else if (opts.length > 1) {
    acts.append(placeBtn('land', 'Place on land'), placeBtn('sea', 'Place at sea (' + seaWord(s) + ')'));
  } else {
    acts.append(placeBtn(opts[0], opts[0] === 'sea' ? 'Place at sea (' + seaWord(s) + ')' : null));
  }
  var key = null;
  if (launcher && s.ring_km) {
    key = el('p', null, 'dnote');
    var off = s.role !== 'defensive';
    key.append(el('span', null, 'key ' + (off ? 'off' : 'def')),
               document.createTextNode((off ? 'Red ring: offensive system.' : 'Blue ring: defensive system.') + ' Shows the distance from the launcher, not a flight path.'));
  }
  var a = el('a', 'Read on Wikipedia ↗', 'il');
  a.href = 'https://en.wikipedia.org/wiki/Special:Search?go=Go&search=' + encodeURIComponent(s.wiki);
  a.target = '_blank'; a.rel = 'noopener';
  d.append(el('p', s.name, 'dn'), el('p', c.name + ' · ' + s.kind, 'dk'), st, el('p', s.desc, 'dd'), dl, acts);
  if (key) d.append(key);
  d.append(el('p', 'Approximate public figures. Claims are not independently verified.', 'dnote'), a);
  return d;
}

// "Built locally" or "Imported from France", plus the maker when known
function origin(s) { return s.made_in ? 'Imported from ' + s.made_in : 'Built locally'; }
function originLine(s) { return origin(s) + (s.maker ? ' · ' + s.maker : ''); }

function onMap(s) { return onmap.filter(function (e) { return e.s === s; }).length; }
// Where a system can be placed: 'land' (inside its country) and/or 'sea' (a ship or submarine, anywhere at sea).
function placeOpts(s) {
  var o = [];
  if (s.launch.indexOf('Land') >= 0) o.push('land');
  if (s.launch.indexOf('Ship') >= 0 || s.launch.indexOf('Submarine') >= 0) o.push('sea');
  return o;
}
function seaWord(s) { return s.launch.indexOf('Ship') >= 0 ? 'ship' : 'submarine'; }

// Three levels: the category's countries -> one country's systems -> one system's details.
function renderRoster() {
  var m = DATA[mode], n = 0, c = selIso ? countryByIso(selIso) : null;
  m.countries.forEach(function (x) { n += x.systems.length; });
  $('#rt').textContent = mode;
  $('#rc').textContent = !c ? m.countries.length + ' countries · ' + n + ' systems · pick a country'
    : !selSys ? c.name + ' · pick a system'
    : 'Tap ' + selSys.name + ' again to close it';
  if (phone() && selSys) {
    // the phone sheet's strip is often all that is visible, so it says what is on the map
    var launcher = curLauncher();
    $('#rt').textContent = selSys.name;
    $('#rc').textContent = !launcher ? 'Tap here for details'
      : selSys.ring_km ? selSys.ring_km.toLocaleString('en-US') + ' km ring · tap for details'
      : 'Launcher placed · tap for details';
  }
  var list = $('#rl'); list.replaceChildren();
  function count(x) { return x.systems.length + (x.systems.length === 1 ? ' system' : ' systems'); }
  if (!c) {
    // level 1: countries only
    m.countries.forEach(function (x) {
      var b = el('button', null, 'crow'); b.type = 'button'; b.id = 'n-' + x.iso;
      b.append(el('span', x.code, 'tok'), el('span', x.name, 'cn'), el('span', count(x), 'cnt'), el('span', '›', 'chev'));
      b.onclick = function () { pickCountry(x, true, false); };
      list.append(b);
    });
  } else {
    // level 2: this country's systems as a list; level 3: compact buttons with the chosen system's details below
    var back = el('button', '← All countries', 'back2'); back.type = 'button';
    back.onclick = allCountries;
    var sec = el('section', null, 'nation on'); sec.id = 'n-' + c.iso;
    var hd = el('div', null, 'nn');
    hd.append(el('span', c.code, 'tok'), el('span', c.name, 'cn'), el('span', count(c), 'cnt'));
    var row = el('div', null, selSys ? 'wrow' : 'wl');
    c.systems.forEach(function (s) {
      var b = el('button', null, selSys ? 'wb' : 'wr'); b.type = 'button';
      b.setAttribute('aria-pressed', String(s === selSys));
      b.title = s === selSys ? 'Tap again to close' : s.status;
      b.onclick = function () { pickSystem(c, s); };
      if (selSys) {
        b.append(el('span', null, 'dot ' + statusClass(s.status)), document.createTextNode(s.name));
      } else {
        var nm = el('span', null, 'wn');
        nm.append(el('span', null, 'dot ' + statusClass(s.status)), document.createTextNode(s.name));
        var nOn = onMap(s);
        if (nOn) nm.append(el('span', nOn > 1 ? nOn + ' on map' : 'On map', 'onmap'));
        var pf = el('span', null, 'wp');
        (s.launch.length ? s.launch : ['Not published']).forEach(function (x) { pf.append(el('span', x === 'Not published' ? 'Launch platform not published' : x + ' launch', 'ptag')); });
        b.append(nm, el('span', s.kind + ' · ' + s.status, 'wk'), pf, el('span', originLine(s), 'wo' + (s.made_in ? ' imp' : '')));
      }
      row.append(b);
    });
    sec.append(hd, row);
    if (selSys) sec.append(detail(c, selSys));
    list.append(back, sec);
  }
  var lg = $('#legend'); lg.replaceChildren();
  lg.hidden = !!selSys;
  STATUS.forEach(function (s) { var i = el('span'); i.append(el('span', null, 'dot ' + s[1]), document.createTextNode(s[0])); lg.append(i); });
  renderChips();
  updateLaunch();
  syncBar();   // the launcher button can wrap the bar onto another line on narrow screens
}

function setSheet(open) {
  $('#roster').classList.toggle('closed', !open);
  $('#rh').setAttribute('aria-expanded', String(open));
}

/* ---------- launcher placement ---------- */
function fmtLL(ll) {
  return Math.abs(ll[1]).toFixed(1) + '°' + (ll[1] < 0 ? 'S' : 'N') + ', ' + Math.abs(ll[0]).toFixed(1) + '°' + (ll[0] < 0 ? 'W' : 'E');
}

var toastTimer;
function toast(text, err) {
  var t = $('#toast');
  t.textContent = text; t.className = 'toast' + (err ? ' err' : ''); t.hidden = false;
  clearTimeout(toastTimer);
  if (!placing || err) toastTimer = setTimeout(function () { t.hidden = true; }, err ? 3500 : 2500);
}

// keeps the top-bar button, the one in the weapon details and the phone sheet's button in step
function updateLaunch() {
  var launcher = curLauncher(), can = selSys && placeOpts(selSys).length > 0;
  $('#launch').hidden = !can;
  var pk = $('#peekact');
  pk.hidden = !can || placing;
  pk.textContent = launcher ? 'Move' : 'Place launcher';
  pk.classList.toggle('moved', !!launcher);
  document.querySelectorAll('.launch').forEach(function (b) {
    b.setAttribute('aria-pressed', String(placing));
    b.classList.toggle('nudge', !placing && !launcher);
    b.classList.toggle('moved', !placing && !!launcher);
    b.querySelector('.lt').textContent = placing ? 'Cancel' : launcher ? 'Move Launcher' : (b.dataset.label || 'Select Launcher Position');
  });
}

var placeTimer;
function placeMsg(text, err) {
  $('#placemsg').textContent = text;
  $('#placebar').classList.toggle('err', !!err);
}
function placeHint() {
  if (placeOn === 'sea') {
    var what = seaWord(selSys) + ' carrying ' + selSys.name;
    placeMsg(moveId ? 'Tap anywhere at sea to move this ' + what + '.'
                    : 'Tap anywhere at sea to place a ' + (onMap(selSys) ? 'new ' : '') + what + '. Pinch or drag to move the map.');
    return;
  }
  var where = countryByIso(selIso).name;
  placeMsg(moveId ? 'Tap inside ' + where + ' to move this ' + selSys.name + ' launcher.'
                  : 'Tap inside ' + where + ' to place a ' + (onMap(selSys) ? 'new ' : '') + selSys.name + ' launcher. Pinch or drag to move the map.');
}

// Placing mode: on phone everything but the map and the placing strip is hidden.
// id: the launcher to move; null places a new one; left out = move the one being looked at, if any.
// where: 'land' (inside the country) or 'sea' (anywhere at sea); left out = as the launcher was placed, else the first option.
var placeOn = 'land';
function setPlacing(on, id, where) {
  placing = on;
  moveId = !on ? null : id !== undefined ? id : curLauncher() ? curId : null;
  if (on) {
    var mv = entry(moveId), opts = placeOpts(selSys);
    placeOn = where || (mv ? mv.on : opts[0] || 'land');
  }
  if (svg) svg.classed('placing', on);
  document.body.classList.toggle('placing', on);
  clearTimeout(placeTimer);
  $('#placebar').hidden = !on;
  $('#toast').hidden = true;
  if (on) {
    placeHint();
    if (phone()) setSheet(false);
    if (placeOn === 'land') flyTo(countryByIso(selIso), true);   // zoom in if the country is still too small to tap
  }
  updateLaunch();
}

/* ---------- what is on the map: chips in the header ---------- */
// One chip per launcher on the map (red strike, blue defence), e.g. "S-400 · RUS". Tapping a chip (or the
// launcher itself) opens it, with Move and Remove; nothing is removed by accident.
var clearArmed = false, clearTimer;
function renderChips() {
  var box = $('#chips'); box.replaceChildren();
  onmap.forEach(function (e) {
    var chip = el('span', null, 'chip ' + cls(e) + (e.id === curId && e.s === selSys ? ' cur' : ''));
    var show = el('button', null, 'cs'); show.type = 'button';
    show.append(el('span', null, 'cdot'), document.createTextNode(e.s.name + ' · ' + e.c.code + (e.on === 'sea' ? ' · at sea' : '')));
    show.title = 'Open ' + e.s.name + ' in ' + e.c.name + ' to move or remove it';
    show.onclick = function () { showEntry(e.id, true); };
    chip.append(show); box.append(chip);
  });
  if (onmap.length > 1) {
    // two taps instead of a pop-up confirm (some browsers and embedded views block pop-ups, which silently cancels)
    var all = el('button', clearArmed ? 'Tap again to clear ' + onmap.length : 'Clear all', 'clr' + (clearArmed ? ' armed' : ''));
    all.type = 'button';
    all.onclick = function () {
      if (!clearArmed) {
        clearArmed = true; renderChips();
        clearTimeout(clearTimer);
        clearTimer = setTimeout(function () { clearArmed = false; renderChips(); }, 4000);
        return;
      }
      clearArmed = false; clearTimeout(clearTimer);
      var n = onmap.length;
      onmap = []; curId = null;
      if (placing) setPlacing(false);
      if (mode) renderRoster(); else renderChips();
      paint();
      toast(n + ' launchers removed from the map.');
    };
    box.append(all);
  } else {
    clearArmed = false;
  }
  box.hidden = !box.childElementCount;
  updatePickers();
  syncBar();
}

// Open a launcher that is on the map: its category, country and system, ready to move or remove.
function showEntry(id, fly) {
  var e = entry(id);
  if (!e) return;
  if (placing) setPlacing(false);
  if (mode !== e.cat) {
    mode = e.cat;
    $('#roster').hidden = false;
  }
  selIso = e.c.iso; selSys = e.s; curId = e.id;
  updatePickers();
  renderRoster(); paint();
  $('#rl').scrollTop = 0;
  if (phone()) setSheet(true);
  if (fly) { if (e.on === 'sea') flyToSea(e); else flyTo(e.c); }   // a ship can be far from home: go to it
  if (features) $('#msg').hidden = true;
}

function removeEntry(id) {
  var e = entry(id);
  if (!e) return;
  onmap = onmap.filter(function (x) { return x.id !== id; });
  if (curId === id) curId = null;
  if (placing && moveId === id) setPlacing(false);
  renderRoster(); paint();
  toast(e.s.name + ' launcher in ' + e.c.name + ' removed from the map.');
}

function onMapClick(ev) {
  if (!placing || !selSys) return;
  var c = countryByIso(selIso), pt = d3.pointer(ev, svg.node());
  var m = T.invert(pt), ll = proj.invert([m[0], m[1] / sy]);
  function refuse(text) {
    placeMsg(text, true);
    clearTimeout(placeTimer);
    placeTimer = setTimeout(function () { if (placing) placeHint(); }, 3000);
  }
  if (placeOn === 'sea') {
    // at sea = on the map and not on any land shape
    var land = !ll || features.some(function (x) { return d3.geoContains(x, ll); });
    if (land) { refuse('That’s land. A ' + seaWord(selSys) + ' must be placed at sea.'); return; }
  } else {
    // a country can have several map shapes with the same id (e.g. Australia and its offshore territory)
    var inside = ll && features.some(function (x) { return x.id === selIso && d3.geoContains(x, ll); });
    if (!inside) { refuse('That’s not within ' + c.name + '. Tap inside ' + c.name + '.'); return; }
  }
  var moving = entry(moveId), sea = placeOn === 'sea';
  if (moving) {
    moving.at = ll; moving.on = placeOn; curId = moving.id;
  } else {   // every placement adds a launcher; earlier ones stay on the map
    var e = { id: nextId++, cat: mode, c: c, s: selSys, at: ll, on: placeOn };
    onmap.push(e); curId = e.id;
  }
  setPlacing(false);
  renderRoster(); paint();
  toast(sea ? c.name + '’s ' + seaWord(selSys) + ' carrying ' + selSys.name + ' ' + (moving ? 'moved' : 'placed') + ' at ' + fmtLL(ll) + '.'
            : selSys.name + ' launcher ' + (moving ? 'moved' : 'placed') + ' in ' + c.name + ' at ' + fmtLL(ll) + '.');
}

// Choosing a country (in the list or on the map) narrows the panel to that country's systems.
function pickCountry(c, fly, fromMap) {
  if (placing) setPlacing(false);
  if (selIso !== c.iso) { selSys = null; curId = null; }
  selIso = c.iso;
  renderRoster(); paint();
  $('#rl').scrollTop = 0;
  if (fly) flyTo(c);
  if (fromMap && phone()) setSheet(true);
}

function allCountries() {
  if (placing) setPlacing(false);
  var was = selIso;
  selIso = null; selSys = null; curId = null;
  renderRoster(); paint();
  var row = was && document.getElementById('n-' + was);
  if (row) row.scrollIntoView({ block: 'nearest' });
}

// Choosing a system shows its details; choosing it again closes them (back to the country's list).
// Browsing never changes the map: a launcher only goes on the map when it is placed, and placing adds
// to what is there. If the system already has a launcher here, its details open on the latest one.
function pickSystem(c, s) {
  if (placing) setPlacing(false);
  if (selSys === s) {
    selSys = null; curId = null;
    renderRoster(); paint();
    return;
  }
  selIso = c.iso; selSys = s;
  var last = lastOf(s, c.iso); curId = last ? last.id : null;
  renderRoster(); paint();
  $('#rl').scrollTop = 0;
  if (phone()) setSheet(true);
  if (last && last.on === 'sea') flyToSea(last); else flyTo(c);
}

function setMode(m) {
  if (placing) setPlacing(false);
  mode = m; selIso = null; selSys = null; curId = null;   // launchers already on the map stay there
  updatePickers();
  $('#roster').hidden = false;
  setSheet(true);
  renderRoster(); paint();
  if (features) $('#msg').hidden = true;
}

/* ---------- weapon picker ---------- */
// small icons: a strike arc (red) or a defensive dome (blue)
var ICONS = {
  Strike: [['path', { d: 'M6 30 C14 12 24 8 34 6', fill: 'none', stroke: '#f06050', 'stroke-width': '2', 'stroke-dasharray': '3 3', 'stroke-linecap': 'round' }],
           ['circle', { cx: '34', cy: '6', r: '3.5', fill: '#f06050' }],
           ['rect', { x: '3', y: '30', width: '10', height: '6', rx: '1', fill: '#cbbd92', stroke: '#000', 'stroke-width': '1' }]],
  Defence: [['path', { d: 'M5 32 A15 15 0 0 1 35 32', fill: 'rgb(80 140 240 / .2)', stroke: '#78aaff', 'stroke-width': '2', 'stroke-dasharray': '3 3' }],
            ['rect', { x: '15', y: '27', width: '10', height: '6', rx: '1', fill: '#cbbd92', stroke: '#000', 'stroke-width': '1' }]]
};

// Two pickers, one per group. Each button shows the category chosen in its group (the one being
// browsed, or the one whose system is on the map); the filled button is the group being browsed.
var PICK = { Strike: { btn: '#pickS', label: 'Choose your Strike', note: 'Pick a strike weapon type to see who has it. Strike ranges show as red rings.' },
             Defence: { btn: '#pickD', label: 'Choose your Defence', note: 'Pick a defence system type to see who has it. Defence ranges show as blue rings.' } };
function updatePickers() {
  ['Strike', 'Defence'].forEach(function (g) {
    var b = $(PICK[g].btn), browsing = !!mode && grpOf(mode) === g;
    var mine = onmap.filter(function (e) { return grpOf(e.cat) === g; });
    var cat = browsing ? mode : (mine.length ? mine[mine.length - 1].cat : null);
    b.querySelector('.pt').textContent = cat ? DATA[cat].short : PICK[g].label;
    b.classList.toggle('set', browsing);
    b.setAttribute('aria-label', PICK[g].label + (cat ? ': ' + cat : ''));
  });
}
function openPicker(grp) {
  var box = $('#opts'); box.replaceChildren();
  $('#dh').textContent = PICK[grp].label; $('#ds').textContent = PICK[grp].note;
  [grp].forEach(function (grp) {
    Object.keys(DATA).filter(function (k) { return DATA[k].group === grp; }).forEach(function (k) {
      var b = el('button', null, 'opt'); b.type = 'button';
      b.setAttribute('aria-pressed', String(k === mode));
      var ns = 'http://www.w3.org/2000/svg', g = document.createElementNS(ns, 'svg');
      g.setAttribute('viewBox', '0 0 40 40'); g.setAttribute('aria-hidden', 'true');
      ICONS[grp].forEach(function (s) {
        var n = document.createElementNS(ns, s[0]); Object.keys(s[1]).forEach(function (a) { n.setAttribute(a, s[1][a]); }); g.append(n);
      });
      b.append(g, el('span', k, 'on1'), el('span', DATA[k].blurb, 'on2'));
      b.onclick = function () { $('#dlg').close(); setMode(k); };
      box.append(b);
    });
  });
  $('#dlg').showModal();
}
$('#pickS').onclick = function () { openPicker('Strike'); };
$('#pickD').onclick = function () { openPicker('Defence'); };
$('#dlg').addEventListener('click', function (e) { if (e.target === this) this.close(); });
$('#rh').onclick = function () {
  if (!phone()) return;
  var open = $('#roster').classList.contains('closed');
  setSheet(open);
  var row = selIso && document.getElementById('n-' + selIso);
  if (open && row) row.scrollIntoView({ block: 'nearest' });
};
$('#zin').onclick = function () { svg && svg.transition().duration(250).call(zoom.scaleBy, 1.6); };
$('#zout').onclick = function () { svg && svg.transition().duration(250).call(zoom.scaleBy, 1 / 1.6); };
$('#zfit').onclick = function () { svg && svg.transition().duration(400).call(zoom.transform, clamp(d3.zoomIdentity)); };
document.addEventListener('keydown', function (e) {
  if (e.key !== 'Escape' || $('#dlg').open) return;
  if (placing) setPlacing(false);
  else if (selSys) { selSys = null; renderRoster(); paint(); }
  else if (selIso) allCountries();
});
$('#launch').onclick = function () { setPlacing(!placing); };
$('#peekact').onclick = function () { setPlacing(true); };
$('#placecancel').onclick = function () { setPlacing(false); };

/* ---------- start ---------- */
function showMsg(text, err) { var m = $('#msg'); m.hidden = false; m.className = 'msg' + (err ? ' err' : ''); m.firstElementChild.textContent = text; }
function syncBar() { document.documentElement.style.setProperty('--barh', (barH() + 4) + 'px'); }
syncBar();
if (!window.d3 || !window.topojson) {
  showMsg('Could not load the map tools. Check your connection and reload the page.', true);
} else {
  d3.json(WORLD_URL).then(function (world) {
    features = topojson.feature(world, world.objects.countries).features.filter(function (f) { return f.properties.name !== 'Antarctica'; });
    syncBar(); build();
    if (mode) $('#msg').hidden = true; else showMsg('Tap “Choose your Strike” or “Choose your Defence” to begin.');
    // Rebuild when the window changes size (rotation, resizing). Small height changes from the
    // phone's address bar sliding are ignored so the map does not jump while you pan.
    var timer;
    new ResizeObserver(function () {
      clearTimeout(timer);
      timer = setTimeout(function () {
        syncBar();
        var host = $('#map');
        if (!svg || Math.abs(host.clientWidth - W) > 1 || Math.abs(host.clientHeight - H) > 120) build(); else place();
      }, 150);
    }).observe($('#map'));
  }).catch(function () { showMsg('Could not load the world map. Check your connection and reload the page.', true); });
}
</script>
</body></html>"""
BATTLE_HTML = BATTLE_PAGE.replace("__BATTLE_DATA__", json.dumps(BATTLE_LAB).replace("</", "<\\/"))


HOME = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>GW University</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400&display=swap">
<style>
/* Layout: full-window "game select" screen in the Battle Lab command-map style.
   Crest and welcome at the top; Battle Lab is a wide featured banner; the other three apps sit in one row below.
   Phone: everything stacks in one column. */
:root{
  color-scheme:dark;
  --bg:#0d1216;
  --ink:#d9e2d6;
  --muted:#8e9c93;
  --line:#34434b;
  --panel:#151e25;
  --accent:#e3b34a;
  --accent-ink:#1c1505;
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
}
[hidden]{display:none!important}
html,body{min-height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);overflow-x:hidden}
:focus-visible{outline:2px solid var(--accent);outline-offset:3px}

/* background: faint map grid, warm glow, dark edges */
.bg{position:fixed;inset:0;z-index:0;pointer-events:none;background:
  radial-gradient(ellipse at 50% 45%,transparent 45%,rgb(0 0 0 / .6)),
  radial-gradient(50% 40% at 85% 0%,rgb(227 179 74 / .09),transparent 70%),
  linear-gradient(rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  linear-gradient(90deg,rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  var(--bg)}

/* on a PC the whole menu fits the window: header, apps (taking the rest), footer */
.wrap{position:relative;z-index:1;min-height:100vh;min-height:100dvh;box-sizing:border-box;display:grid;grid-template-rows:auto minmax(0,1fr) auto;
  gap:clamp(14px,2.4vh,26px);padding:clamp(14px,2.2vw,28px) clamp(16px,3vw,48px) clamp(12px,1.6vh,18px)}

/* header: crest with the welcome beside it */
.top{display:flex;align-items:center;gap:clamp(14px,1.8vw,24px)}
.crest{flex:none;width:clamp(64px,6.4vw,96px);height:auto;filter:drop-shadow(0 6px 12px rgb(0 0 0 / .5))}
.crest text{font-family:var(--display);font-weight:700}
.welcome{min-width:0}
.inst{margin:0 0 4px;font-family:var(--display);font-weight:500;font-size:.76rem;letter-spacing:.22em;text-transform:uppercase;color:var(--muted)}
.inst b{font-weight:700;color:var(--accent);letter-spacing:.26em}
.welcome h1{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.4rem,3.1vw,2.6rem);letter-spacing:.07em;text-transform:uppercase;line-height:1.1;
  text-shadow:3px 3px 0 rgb(227 179 74 / .4);text-wrap:balance}
.lede{margin:8px 0 0;max-width:52rem;color:var(--muted);font-size:clamp(.95rem,1.2vw,1.08rem);line-height:1.45}

/* app grid: Battle Lab across the top, three apps below */
.apps{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));grid-template-rows:minmax(210px,1.25fr) minmax(200px,1fr);gap:clamp(14px,1.6vw,22px);min-height:0}

.card{position:relative;display:flex;flex-direction:column;justify-content:flex-end;min-width:0;overflow:hidden;box-sizing:border-box;padding:clamp(16px,2vw,26px);
  border-radius:16px;border:1px solid var(--line);background:linear-gradient(160deg,#1b2630,#11181e);color:inherit;text-decoration:none;
  box-shadow:0 14px 30px rgb(0 0 0 / .45);transition:transform .18s ease-out,border-color .18s,box-shadow .18s}
.card:hover,.card:focus-visible{transform:translateY(-4px);border-color:var(--accent);
  box-shadow:0 0 0 1px var(--accent),0 22px 40px rgb(0 0 0 / .55),0 0 36px rgb(227 179 74 / .16)}
.card:active{transform:translateY(-1px)}
.art{position:absolute;inset:0;z-index:0;width:100%;height:100%}
.txt{position:relative;z-index:1;display:grid;justify-items:start;gap:6px}
.tag{padding:3px 10px;border-radius:999px;border:1px solid var(--accent);background:rgb(13 18 22 / .7);color:var(--accent);
  font-family:var(--display);font-weight:700;font-size:.64rem;letter-spacing:.22em;text-transform:uppercase}
.name{margin:4px 0 0;font-family:var(--display);font-weight:700;font-size:clamp(1.1rem,1.7vw,1.45rem);letter-spacing:.16em;text-transform:uppercase;line-height:1.15}
.desc{margin:0;max-width:34rem;color:var(--muted);font-size:.98rem;line-height:1.45}
.go{display:inline-flex;align-items:center;gap:8px;margin-top:8px;padding:7px 16px;border-radius:999px;border:1.5px solid var(--accent);color:var(--accent);
  font-family:var(--display);font-weight:700;font-size:.74rem;letter-spacing:.18em;text-transform:uppercase}
.go svg{width:11px;height:11px;fill:currentColor}
.card:hover .go,.card:focus-visible .go{background:var(--accent);color:var(--accent-ink)}

/* the featured Battle Lab banner */
.hero{grid-column:1 / -1}
.hero .art-fade{position:absolute;inset:0;z-index:0;background:linear-gradient(90deg,rgb(13 18 22 / .92) 0%,rgb(13 18 22 / .6) 38%,transparent 70%),
  linear-gradient(0deg,rgb(13 18 22 / .7),transparent 45%)}
.hero .name{font-size:clamp(1.9rem,4.4vw,3.4rem);letter-spacing:.2em;text-shadow:3px 3px 0 rgb(0 0 0 / .6)}
.hero .desc{color:#b9c4bb;font-size:1.05rem}
.hero .go{padding:10px 22px;font-size:.85rem;background:var(--accent);color:var(--accent-ink);box-shadow:0 3px 0 #000}
.hero:hover .go{filter:brightness(1.1)}

/* the three smaller apps: icon beside the text, both resting on the bottom edge, so they never overlap */
.small{display:grid;grid-template-columns:auto minmax(0,1fr);align-items:end;gap:clamp(12px,1.4vw,18px)}
.icon{position:relative;z-index:1;align-self:start;width:clamp(40px,3.8vw,58px);height:auto}
.small .desc{font-size:.93rem}
.small::before{content:"";position:absolute;inset:0;z-index:0;background:radial-gradient(70% 60% at 90% 0%,rgb(227 179 74 / .07),transparent 70%)}

.foot{margin:0;color:var(--muted);font-size:.82rem;text-align:center}

/* PC: lock the menu to the window so the apps share exactly the space left under the header */
@media (min-width:721px) and (min-height:620px){
  .wrap{height:100vh;height:100dvh}
}
@media (max-width:720px){
  .top{align-items:flex-start}
  .apps{grid-template-columns:1fr;grid-template-rows:none}
  .hero{min-height:300px}
  .small{min-height:170px}
}
@media (prefers-reduced-motion:reduce){.card{transition:none}.card:hover,.card:focus-visible{transform:none}}
</style>
</head><body>
<div class="bg" aria-hidden="true"></div>
<div class="wrap">
  <header class="top">
    <!-- school-style crest: shield, trajectory arc over an open book, ASPC-GW banner; welcome text beside it -->
    <svg class="crest" viewBox="0 0 120 140" role="img" aria-label="ASPC-GW crest">
      <path d="M60 4 112 17v47c0 37-24 61-52 72C32 125 8 101 8 64V17z" fill="#18222a" stroke="#e3b34a" stroke-width="4" stroke-linejoin="round"/>
      <path d="M60 12 104 23v41c0 31-20 52-44 62-24-10-44-31-44-62V23z" fill="none" stroke="#e3b34a" stroke-width="1" opacity=".45"/>
      <path d="M26 62Q52 20 92 36" fill="none" stroke="#e3b34a" stroke-width="2.4" stroke-dasharray="4 4" stroke-linecap="round"/>
      <path d="M92 29l2.2 4.6 5 .7-3.6 3.5.9 5-4.5-2.4-4.5 2.4.9-5-3.6-3.5 5-.7z" fill="#e3b34a"/>
      <rect x="10" y="66" width="100" height="24" rx="3" fill="#e3b34a"/>
      <text x="60" y="83.5" text-anchor="middle" font-size="15.5" letter-spacing="1.2" fill="#1c1505">ASPC-GW</text>
      <path d="M60 104c-8-5-18-6-26-4v18c8-2 18-1 26 4 8-5 18-6 26-4v-18c-8-2-18-1-26 4zM60 104v18" fill="none" stroke="#cbbd92" stroke-width="2" stroke-linejoin="round"/>
    </svg>
    <div class="welcome">
      <p class="inst"><b>ASPC-GW</b> · GW University</p>
      <h1>Welcome to GW University</h1>
      <p class="lede">An academic space for our engineers to explore, learn and experiment with public information. Choose an app to begin.</p>
    </div>
  </header>

  <nav class="apps" aria-label="Apps">
    <a class="card hero" href="/battle-lab">
      <!-- illustration: map grid, hexes, two abstract landmasses, a red and a blue ring with launcher tokens -->
      <svg class="art" viewBox="0 0 1200 500" preserveAspectRatio="xMidYMid slice" aria-hidden="true">
        <defs>
          <pattern id="hx" width="36" height="20.78" patternUnits="userSpaceOnUse">
            <path d="M0,10.39 L6,0 L18,0 L24,10.39 L18,20.78 L6,20.78 Z M24,10.39 L36,10.39" fill="none" stroke="rgb(160 200 160 / .09)"/>
          </pattern>
          <pattern id="gr" width="80" height="80" patternUnits="userSpaceOnUse">
            <path d="M80 0H0V80" fill="none" stroke="rgb(150 185 150 / .16)"/>
          </pattern>
        </defs>
        <rect width="1200" height="500" fill="#142029"/>
        <path d="M520 70c80-30 190-20 260 20s150 10 220 40 140 90 120 160-110 70-170 60-90 60-160 50-100-70-170-80-140 10-150-60 0-160 50-190z" fill="#2e3829" stroke="#5f6d55" stroke-width="2"/>
        <path d="M640 380c40-20 100-10 130 20s10 70-40 80-110-10-120-40 0-50 30-60z" fill="#2e3829" stroke="#5f6d55" stroke-width="2"/>
        <rect width="1200" height="500" fill="url(#gr)"/>
        <rect width="1200" height="500" fill="url(#hx)"/>
        <line x1="0" y1="320" x2="1200" y2="320" stroke="rgb(227 179 74 / .4)" stroke-dasharray="10 8"/>
        <circle cx="820" cy="210" r="165" fill="rgb(224 72 60 / .14)" stroke="rgb(240 96 80 / .85)" stroke-width="2.5" stroke-dasharray="12 8"/>
        <circle cx="1010" cy="290" r="95" fill="rgb(80 140 240 / .14)" stroke="rgb(120 170 255 / .85)" stroke-width="2.5" stroke-dasharray="12 8"/>
        <g transform="translate(803 196)"><rect x="0" y="6" width="30" height="8" rx="1.5" fill="#cbbd92" stroke="#000" stroke-width="1.5"/><rect x="2" y="-4" width="24" height="8" rx="1.5" fill="#9a8d63" stroke="#000" stroke-width="1.5" transform="rotate(-14 2 4)"/><circle cx="17" cy="23" r="3.5" fill="#f06050"/></g>
        <g transform="translate(995 278)"><rect x="0" y="6" width="30" height="8" rx="1.5" fill="#cbbd92" stroke="#000" stroke-width="1.5"/><path d="M4 6a11 11 0 0 1 22 0" fill="#9a8d63" stroke="#000" stroke-width="1.5"/><circle cx="15" cy="23" r="3.5" fill="#78aaff"/></g>
      </svg>
      <span class="art-fade" aria-hidden="true"></span>
      <span class="txt">
        <span class="tag">Featured · Main app</span>
        <span class="name">Battle Lab</span>
        <span class="desc">Choose strike and defence systems, place their launchers on a world map and compare their published reach.</span>
        <span class="go">Play <svg viewBox="0 0 12 12" aria-hidden="true"><path d="M2 1l9 5-9 5z"/></svg></span>
      </span>
    </a>

    <a class="card small" href="/trends">
      <svg class="icon" viewBox="0 0 64 64" aria-hidden="true">
        <rect x="7" y="40" width="10" height="18" rx="2" fill="#4a5a3c" stroke="#cbbd92" stroke-width="2"/>
        <rect x="22" y="30" width="10" height="28" rx="2" fill="#4a5a3c" stroke="#cbbd92" stroke-width="2"/>
        <rect x="37" y="20" width="10" height="38" rx="2" fill="#4a5a3c" stroke="#cbbd92" stroke-width="2"/>
        <polyline points="8,30 23,21 38,25 55,8" fill="none" stroke="#e3b34a" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="55" cy="8" r="4.5" fill="#e3b34a"/>
      </svg>
      <span class="txt">
        <span class="tag">Daily</span>
        <span class="name">Top 10 News</span>
        <span class="desc">The 10 latest top stories for each topic, each with a photo and a short summary.</span>
        <span class="go">Enter <svg viewBox="0 0 12 12" aria-hidden="true"><path d="M2 1l9 5-9 5z"/></svg></span>
      </span>
    </a>

    <a class="card small" href="/research">
      <svg class="icon" viewBox="0 0 64 64" aria-hidden="true">
        <circle cx="26" cy="26" r="16" fill="#1f2a32" stroke="#cbbd92" stroke-width="2.4"/>
        <path d="M17 24a10 10 0 0 1 9-9" fill="none" stroke="#e3b34a" stroke-width="3" stroke-linecap="round"/>
        <line x1="38" y1="38" x2="56" y2="56" stroke="#cbbd92" stroke-width="6" stroke-linecap="round"/>
        <circle cx="26" cy="26" r="4" fill="#e3b34a"/>
      </svg>
      <span class="txt">
        <span class="tag">Tool</span>
        <span class="name">Market Research</span>
        <span class="desc">Find the published systems closest to your technical parameters.</span>
        <span class="go">Enter <svg viewBox="0 0 12 12" aria-hidden="true"><path d="M2 1l9 5-9 5z"/></svg></span>
      </span>
    </a>

    <a class="card small" href="/standards">
      <svg class="icon" viewBox="0 0 64 64" aria-hidden="true">
        <rect x="8" y="12" width="14" height="44" rx="2" fill="#1f2a32" stroke="#cbbd92" stroke-width="2.2"/>
        <rect x="25" y="8" width="14" height="48" rx="2" fill="#4a5a3c" stroke="#cbbd92" stroke-width="2.2"/>
        <rect x="42" y="16" width="14" height="40" rx="2" fill="#1f2a32" stroke="#cbbd92" stroke-width="2.2" transform="rotate(8 49 36)"/>
        <line x1="11" y1="22" x2="19" y2="22" stroke="#e3b34a" stroke-width="2.4" stroke-linecap="round"/>
        <line x1="28" y1="18" x2="36" y2="18" stroke="#e3b34a" stroke-width="2.4" stroke-linecap="round"/>
      </svg>
      <span class="txt">
        <span class="tag">Reference</span>
        <span class="name">Standards Library</span>
        <span class="desc">Free military and space standards, with team notes.</span>
        <span class="go">Enter <svg viewBox="0 0 12 12" aria-hidden="true"><path d="M2 1l9 5-9 5z"/></svg></span>
      </span>
    </a>
  </nav>

  <p class="foot">For learning and discussion. Built from public information only.</p>
</div>
</body></html>"""


RESEARCH_PAGE = r"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Market Research</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@500;700&family=Zilla+Slab:ital,wght@0,400;0,600;1,400;1,600&display=swap">
<style>
/* Layout: the dark command-map look shared with the home page and Battle Lab. Heading and a parameter form
   on the page gutter, then a grid of ranked result cards with photo and specs. */
:root{
  color-scheme:dark;
  --bg:#0d1216;
  --ink:#d9e2d6;
  --muted:#8e9c93;
  --line:#34434b;
  --panel:#151e25;
  --field:#0f161b;
  --accent:#e3b34a;
  --accent-ink:#1c1505;
  --display:"Jost","Futura","Century Gothic","Avenir Next",system-ui,sans-serif;
  --body:"Zilla Slab","Archer","Rockwell",Georgia,serif;
}
[hidden]{display:none!important}
html,body{min-height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--body);overflow-x:hidden}
button,input,select{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}

.bg{position:fixed;inset:0;z-index:0;pointer-events:none;background:
  radial-gradient(ellipse at 50% 45%,transparent 45%,rgb(0 0 0 / .6)),
  radial-gradient(50% 40% at 85% 0%,rgb(227 179 74 / .09),transparent 70%),
  linear-gradient(rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  linear-gradient(90deg,rgb(150 185 150 / .06) 1px,transparent 1px) 0 0 / 64px 64px,
  var(--bg)}
.stage{position:relative;z-index:1;box-sizing:border-box;padding:clamp(16px,2.4vw,32px) clamp(16px,3vw,48px) 48px}
.page{max-width:96rem;margin:0 auto;min-width:0}
.back{display:inline-block;margin-bottom:6px;color:var(--muted);font-family:var(--display);font-weight:500;font-size:.92rem;letter-spacing:.06em;text-decoration:none}
.back:hover{color:var(--ink);text-decoration:underline}
h1{margin:0;font-family:var(--display);font-weight:700;font-size:clamp(1.8rem,5vw,3rem);letter-spacing:.12em;text-transform:uppercase;line-height:1.1;
  text-shadow:3px 3px 0 rgb(227 179 74 / .4)}
.lede{margin:10px 0 0;max-width:52rem;color:var(--muted);font-size:1.02rem;line-height:1.5}

/* form */
.panel{margin-top:20px;padding:clamp(16px,2vw,24px);border:1px solid var(--line);border-radius:14px;background:var(--panel);box-shadow:0 12px 28px rgb(0 0 0 / .4)}
.fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,190px),1fr));gap:14px 16px}
.fld{display:grid;gap:6px;min-width:0}
.fld>span{font-family:var(--display);font-weight:700;font-size:.7rem;letter-spacing:.16em;text-transform:uppercase;color:var(--accent)}
.fld input,.fld select{width:100%;box-sizing:border-box;min-width:0;padding:9px 12px;border:1px solid var(--line);border-radius:10px;background:var(--field);font-size:1rem;line-height:1.3}
.fld input:hover,.fld select:hover{border-color:var(--muted)}
.fld input::placeholder{color:rgb(217 226 214 / .35)}
.fld select option{background:var(--panel);color:var(--ink)}
.actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:18px}
.btn{cursor:pointer;padding:9px 20px;border-radius:999px;border:1.5px solid var(--accent);background:var(--accent);color:var(--accent-ink);
  font-family:var(--display);font-weight:700;font-size:.8rem;letter-spacing:.16em;text-transform:uppercase;box-shadow:0 3px 0 #000;transition:transform .12s,box-shadow .12s}
.btn.alt{background:transparent;color:var(--accent)}
.btn:hover{filter:brightness(1.08)}
.btn:active{transform:translateY(2px);box-shadow:none}
.btn[disabled]{opacity:.6;cursor:progress}
.st{margin:16px 0 0;color:var(--muted);font-size:1rem;line-height:1.45;min-height:1.45em}
.st.err{color:#f4b6a2}

/* results */
.more{display:flex;flex-wrap:wrap;justify-content:center;gap:10px;margin:10px 0 18px}
.more .btn{white-space:normal;max-width:100%}
.xwrap{position:relative;max-width:100%}
.xmenu{position:absolute;left:50%;top:calc(100% + 8px);transform:translateX(-50%);z-index:5;width:min(280px,calc(100vw - 32px));padding:6px;
  border:1px solid var(--line);border-radius:12px;background:#1b2630;box-shadow:0 14px 30px rgb(0 0 0 / .55);display:grid;gap:4px}
.xmenu[hidden]{display:none}
.xmenu button{display:grid;gap:2px;text-align:left;cursor:pointer;padding:9px 12px;border:1px solid transparent;border-radius:8px;background:transparent;color:var(--ink);font:inherit}
.xmenu button:hover,.xmenu button:focus-visible{border-color:var(--accent);background:rgb(227 179 74 / .08)}
.xmenu b{font-family:var(--display);font-weight:700;letter-spacing:.06em;color:var(--accent)}
.xmenu span{color:var(--muted);font-size:.84rem;line-height:1.3}
.res{list-style:none;margin:18px 0 0;padding:0 0 12px;display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,300px),1fr));gap:24px 20px}
.card{display:flex;flex-direction:column;height:100%;box-sizing:border-box;overflow:hidden;min-width:0;
  border:1px solid var(--line);border-radius:16px;background:linear-gradient(160deg,#1b2630,#11181e);box-shadow:0 14px 30px rgb(0 0 0 / .45);
  transition:border-color .18s,box-shadow .18s}
.card:hover{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent),0 20px 36px rgb(0 0 0 / .5)}
.pic{position:relative;aspect-ratio:4 / 3;border-bottom:1px solid var(--line);background:linear-gradient(160deg,#1f2a32,#0f161b)}
.pic img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;display:block}
.pic::after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(to bottom,rgb(13 18 22 / .5),transparent 35%)}
.pic .ph{position:absolute;inset:0;display:grid;place-items:center}
.pic .ph svg{width:30%;height:auto}
.rk{position:absolute;left:10px;top:10px;z-index:2;display:grid;place-items:center;width:36px;height:36px;border-radius:50%;border:1.5px solid #000;background:var(--accent);color:var(--accent-ink);
  font-family:var(--display);font-weight:700;font-size:1.1rem;box-shadow:0 2px 0 #000}
.pct{position:absolute;right:10px;top:12px;z-index:2;padding:3px 10px;border-radius:999px;border:1px solid var(--accent);background:rgb(13 18 22 / .85);color:var(--accent);
  font-family:var(--display);font-weight:700;font-size:.72rem;letter-spacing:.1em;text-transform:uppercase}
.cb{display:flex;flex-direction:column;gap:10px;padding:14px 16px 16px;min-width:0;flex:1}
.nm{margin:0;font-family:var(--display);font-weight:700;font-size:1.15rem;letter-spacing:.04em;line-height:1.2;overflow-wrap:anywhere}
.sub{margin:-4px 0 0;color:var(--muted);font-size:.9rem;line-height:1.35;overflow-wrap:anywhere}
.cov{margin:0;color:var(--muted);font-size:.84rem}
.specs{margin:0;display:grid;gap:4px}
.row{display:grid;grid-template-columns:5.6rem minmax(0,1fr);gap:8px;padding:6px 8px;border-radius:8px;border:1px solid transparent}
.row.m{border-color:rgb(227 179 74 / .35);background:rgb(227 179 74 / .08)}
.row dt{margin:0;font-family:var(--display);font-weight:700;font-size:.66rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);padding-top:.28em}
.row dd{margin:0;font-size:.95rem;line-height:1.35;overflow-wrap:anywhere;min-width:0}
.row dd small{display:block;margin-top:1px;color:var(--accent);font-size:.78rem}
.row dd .na{color:var(--muted);font-style:italic}
.src{margin-top:auto;align-self:flex-start;color:var(--accent);font-family:var(--display);font-weight:500;font-size:.86rem;letter-spacing:.05em}
.foot{margin:24px 0 0;max-width:52rem;color:var(--muted);font-size:.86rem;line-height:1.5}

/* market landscape: range vs speed of every published system of the type, with your matches and parameters */
.land{margin-top:24px;padding:clamp(14px,2vw,22px);border:1px solid var(--line);border-radius:14px;background:#11181e;box-shadow:0 12px 28px rgb(0 0 0 / .4)}
.land h2{margin:0;font-family:var(--display);font-weight:700;font-size:.9rem;letter-spacing:.2em;text-transform:uppercase;color:var(--accent)}
.land .lsub{margin:4px 0 10px;color:var(--muted);font-size:.9rem;line-height:1.4}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;margin:0 0 8px;color:var(--ink);font-size:.86rem}
.legend span{display:inline-flex;align-items:center;gap:7px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:50%}
.legend .k-other{background:#7d8c86}
.legend .k-match{background:#e3b34a}
.legend .k-you{width:6px;height:14px;border-radius:0;background:linear-gradient(#9fc3ff,#9fc3ff) 50% 0 / 2px 100% no-repeat}
.chart{position:relative;min-width:0}
.land .lh3{margin:18px 0 0;font-family:var(--display);font-weight:700;font-size:.78rem;letter-spacing:.18em;text-transform:uppercase;color:var(--ink)}
.land .lsub2{margin:2px 0 4px;color:var(--muted);font-size:.86rem}
.chart svg{display:block;width:100%;height:auto;overflow:visible}
.chart .gl{stroke:#2a3740;stroke-width:1}
.chart .ref{stroke:#4d5d66;stroke-width:1;stroke-dasharray:4 4}
.chart .reft{fill:var(--muted);font-family:var(--display);font-size:10px;letter-spacing:.08em;paint-order:stroke;stroke:#11181e;stroke-width:3px}
.chart .tk{fill:var(--muted);font-family:var(--display);font-size:11px}
.chart .at{fill:var(--muted);font-family:var(--display);font-weight:700;font-size:10px;letter-spacing:.14em;text-transform:uppercase}
.chart .d-other{fill:#7d8c86;stroke:#11181e;stroke-width:2}
.chart .d-match{fill:#e3b34a;stroke:#11181e;stroke-width:2}
.chart .rkl{fill:var(--ink);font-family:var(--display);font-weight:700;font-size:11px;paint-order:stroke;stroke:#11181e;stroke-width:3px}
.chart .you{stroke:#9fc3ff;stroke-width:1.5;stroke-dasharray:5 4}
.chart .youm{fill:#9fc3ff;stroke:#11181e;stroke-width:2}
.chart .youl{fill:#9fc3ff;font-family:var(--display);font-weight:700;font-size:11px;paint-order:stroke;stroke:#11181e;stroke-width:3px}
.chart .hit{fill:transparent;cursor:pointer}
.chart .hit:hover+.dot,.chart .dot.on{stroke:#d9e2d6}
.tip{position:absolute;z-index:3;max-width:240px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:#1b2630;box-shadow:0 8px 20px rgb(0 0 0 / .5);
  font-size:.86rem;line-height:1.35;pointer-events:auto}
.tip b{display:block;font-family:var(--display);font-weight:700;letter-spacing:.03em}
.tip span{color:var(--muted)}
.tip a{display:inline-block;margin-top:4px;color:var(--accent)}
.ltable summary{margin-top:10px;cursor:pointer;color:var(--accent);font-family:var(--display);font-weight:700;font-size:.74rem;letter-spacing:.16em;text-transform:uppercase}
.ltable table{width:100%;margin-top:8px;border-collapse:collapse;font-size:.88rem}
.ltable th{text-align:left;color:var(--muted);font-family:var(--display);font-size:.66rem;letter-spacing:.14em;text-transform:uppercase;padding:4px 8px 6px 0;border-bottom:1px solid var(--line)}
.ltable td{padding:5px 8px 5px 0;border-bottom:1px solid rgb(52 67 75 / .5)}
.ltable td.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.ltable tr.m td:first-child{color:var(--accent)}

@media (prefers-reduced-motion:reduce){.btn,.card{transition:none}}
</style></head><body>
<div class="bg" aria-hidden="true"></div>
<div class="stage">
  <main class="page">
    <header>
      <a class="back" href="/">&larr; Home</a>
      <h1>Market Research</h1>
      <p class="lede">Enter the technical parameters you know. The tool compares them with published specifications of existing systems and lists the five closest.</p>
    </header>

    <form class="panel" id="f" novalidate>
      <div class="fields">
        <label class="fld"><span>System type</span><select id="type"><option value="">Any type</option></select></label>
        <label class="fld"><span>Range (km)</span><input id="range" inputmode="decimal" autocomplete="off" placeholder="e.g. 150"></label>
        <label class="fld"><span>Speed (Mach)</span><input id="speed" inputmode="decimal" autocomplete="off" placeholder="e.g. 0.9"></label>
        <label class="fld"><span>Warhead mass (kg)</span><input id="warhead" inputmode="decimal" autocomplete="off" placeholder="e.g. 220"></label>
        <label class="fld"><span>Seeker capability</span><select id="seeker"><option value="">Any seeker</option></select></label>
        <label class="fld"><span>Guidance</span><select id="guidance"><option value="">Any guidance</option></select></label>
        <label class="fld"><span>Launch platform</span><select id="platform"><option value="">Any platform</option></select></label>
      </div>
      <div class="actions">
        <button class="btn" id="go" type="submit">Find matches</button>
        <button class="btn alt" id="ex" type="button">Try an example</button>
        <button class="btn alt" id="clr" type="button">Clear</button>
      </div>
    </form>

    <p class="st" id="st" aria-live="polite"></p>
    <ol class="res" id="res"></ol>
    <div class="more" id="more" hidden><button class="btn" id="mb" type="button">Show next 5 matches</button><div class="xwrap"><button class="btn alt" id="xl" type="button" aria-expanded="false" aria-controls="xmenu">Export</button>
      <div class="xmenu" id="xmenu" hidden>
        <button type="button" data-fmt="xlsx"><b>Excel</b><span>.xlsx · opens in Excel</span></button>
        <button type="button" data-fmt="csv"><b>Google Sheets</b><span>.csv · in Sheets: File › Import › Upload</span></button>
      </div></div></div>

    <section class="land" id="land" hidden aria-labelledby="lh">
      <h2 id="lh">Market landscape</h2>
      <p class="lsub" id="lsub"></p>
      <div class="legend">
        <span><i class="k-other"></i>Other published systems</span>
        <span><i class="k-match"></i>Your closest matches (numbered)</span>
        <span><i class="k-you"></i>Your value</span>
      </div>
      <h3 class="lh3">Range</h3>
      <p class="lsub2" id="rsub"></p>
      <div class="chart" id="chart-r"></div>
      <h3 class="lh3">Speed</h3>
      <p class="lsub2" id="ssub"></p>
      <div class="chart" id="chart-s"></div>
      <details class="ltable"><summary>Show as a table</summary><div id="ltable"></div></details>
    </section>
    <p class="foot">Specifications come from Wikipedia infoboxes and are published figures, so they may be incomplete, rounded or disputed. Only systems with a Wikipedia infobox are searched. Text and images are available under CC BY-SA licences; open each article for full credits.</p>
  </main>
</div>

<script>
var $ = function (s) { return document.querySelector(s); };
var FIELDS = ['type', 'range', 'speed', 'warhead', 'seeker', 'guidance', 'platform'];
var token = 0, timer = null, tries = 0;

var PH = '<svg viewBox="0 0 64 64" aria-hidden="true"><circle cx="27" cy="27" r="16" fill="#1f2a32" stroke="#cbbd92" stroke-width="2.4"/><path d="M18 25a10 10 0 0 1 9-9" fill="none" stroke="#e3b34a" stroke-width="3" stroke-linecap="round"/><line x1="39" y1="39" x2="56" y2="56" stroke="#cbbd92" stroke-width="6" stroke-linecap="round"/><circle cx="27" cy="27" r="4" fill="#e3b34a"/></svg>';

function el(tag, text, cls) {
  var e = document.createElement(tag);
  if (text != null) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}
function safeUrl(u) { return /^https?:\/\//i.test(u || '') ? u : ''; }
function status(msg, isErr) { var s = $('#st'); s.textContent = msg; s.className = 'st' + (isErr ? ' err' : ''); }

function fillSelect(id, list) {
  var s = $('#' + id);
  list.forEach(function (v) { var o = document.createElement('option'); o.value = v; o.textContent = v; s.append(o); });
}
function loadOptions() {
  fetch('/api/research/options').then(function (r) { return r.json(); }).then(function (o) {
    fillSelect('type', o.types); fillSelect('seeker', o.seekers); fillSelect('guidance', o.guidance); fillSelect('platform', o.platforms);
  }).catch(function () { status('Could not load the form options. Reload the page.', true); });
}

function query() {
  var q = new URLSearchParams();
  FIELDS.forEach(function (id) { var v = $('#' + id).value.trim(); if (v) q.set(id, v); });
  return q;
}

function card(d, rank) {
  var li = el('li');
  var c = el('article', null, 'card');
  var pic = el('div', null, 'pic');
  function fallback() { pic.querySelectorAll('img').forEach(function (i) { i.remove(); }); if (!pic.querySelector('.ph')) { var ph = el('div', null, 'ph'); ph.innerHTML = PH; pic.prepend(ph); } }
  var img = safeUrl(d.image);
  if (img) {
    var im = document.createElement('img');
    im.alt = 'Photo of ' + d.title; im.loading = 'lazy'; im.referrerPolicy = 'no-referrer';
    im.onerror = fallback; im.src = img;
    pic.append(im);
  } else { fallback(); }
  var rk = el('span', String(rank), 'rk'); rk.setAttribute('aria-label', 'Rank ' + rank);
  pic.append(rk, el('span', d.score + '% match', 'pct'));

  var body = el('div', null, 'cb');
  body.append(el('h2', d.title, 'nm'));
  if (d.sub) body.append(el('p', d.sub, 'sub'));
  body.append(el('p', 'Compared on ' + d.known + ' of ' + d.asked + ' parameters you entered', 'cov'));

  var dl = el('dl', null, 'specs');
  d.specs.forEach(function (s) {
    var row = el('div', null, 'row' + (s.match && s.match.state !== 'unknown' ? ' m' : ''));
    row.append(el('dt', s.label));
    var dd = el('dd');
    if (s.text === 'Not published') dd.append(el('span', 'Not published', 'na')); else dd.textContent = s.text;
    if (s.match) {
      var word = { close: 'close', near: 'near', far: 'far off' }[s.match.state];
      dd.append(el('small', s.match.state === 'unknown' ? 'Could not compare · you entered ' + s.match.yours : 'You entered ' + s.match.yours + ' · ' + word));
    }
    row.append(dd);
    dl.append(row);
  });
  body.append(dl);
  var url = safeUrl(d.url);
  if (url) { var a = el('a', 'Read more on Wikipedia', 'src'); a.href = url; a.target = '_blank'; a.rel = 'noopener'; body.append(a); }
  c.append(pic, body);
  li.append(c);
  return li;
}

var shown = 0, lastQ = null;
function render(j, append) {
  var box = $('#res');
  if (!append) { box.replaceChildren(); shown = 0; matches = []; }
  (j.results || []).forEach(function (d, i) { box.append(card(d, shown + i + 1)); matches.push({ title: d.title, rank: shown + i + 1 }); });
  shown += (j.results || []).length;
  if (append) drawLandscape(); else loadLandscape();
  var more = $('#more');
  if (!shown) {
    more.hidden = true;
    status('No published systems have figures for these parameters. Try fewer parameters or another type.');
    return;
  }
  var left = j.total - shown;
  more.hidden = false;
  $('#mb').hidden = left <= 0;
  $('#xl').textContent = 'Export ' + shown + ' matches';
  if (left > 0) $('#mb').textContent = 'Show next ' + Math.min(5, left) + ' matches (' + left + ' more)';
  var first = box.querySelector('.pct');
  status('Showing the ' + shown + ' closest of ' + j.total + ' matching systems (' + j.considered + ' checked)' +
    (left <= 0 ? '. That is every match.' : '.') + (!append && j.results[0].score < 40 ? ' No close matches found, so these are the nearest.' : ''));
}

function run(e) {
  if (e) e.preventDefault();
  var q = query();
  clearTimeout(timer);
  if (!['range', 'speed', 'warhead', 'seeker', 'guidance', 'platform'].some(function (k) { return q.has(k); })) {
    status('Enter at least one parameter to compare.', true);
    return;
  }
  var my = ++token; tries = 0;
  lastQ = q; $('#more').hidden = true;
  $('#go').disabled = true;
  status('Searching…');
  poll(q, my);
}

function poll(q, my) {
  fetch('/api/research?' + q.toString()).then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
    .then(function (x) {
      if (my !== token) return;
      var j = x.j;
      if (!x.ok) { status(j.error || 'Something went wrong. Try again.', true); $('#go').disabled = false; return; }
      if (j.state === 'loading') {
        tries++;
        if (tries > 80) { status('The public data is still loading. Try again in a minute.', true); $('#go').disabled = false; return; }
        status('Loading public specifications for the first time (' + j.progress.ready + ' of ' + j.progress.total + ' groups ready). This can take a minute…');
        timer = setTimeout(function () { poll(q, my); }, 2500);
        return;
      }
      $('#go').disabled = false;
      if (j.state === 'error') { status(j.error, true); $('#res').replaceChildren(); return; }
      render(j);
    })
    .catch(function () { if (my === token) { status('Could not reach the server. Try again.', true); $('#go').disabled = false; } });
}

/* ---- market landscape: range vs speed of every published system of the chosen type ---- */
var NS = 'http://www.w3.org/2000/svg', LAND = null, matches = [], pinned = null;
function sv(tag, attrs, text) {
  var e = document.createElementNS(NS, tag);
  Object.keys(attrs).forEach(function (k) { e.setAttribute(k, attrs[k]); });
  if (text != null) e.textContent = text;
  return e;
}
function fmtKm(v) { return v >= 100 ? Math.round(v).toLocaleString('en-US') : String(+v.toFixed(1)); }
function fmtMach(v) { return String(+v.toFixed(v < 1 ? 2 : 1)); }
function logTicks(lo, hi) {
  var steps = Math.log10(hi / lo) > 2.5 ? [1] : [1, 2, 5], out = [];
  for (var p = Math.floor(Math.log10(lo)); p <= Math.ceil(Math.log10(hi)); p++) {
    steps.forEach(function (s) { var v = s * Math.pow(10, p); if (v >= lo && v <= hi) out.push(+v.toPrecision(3)); });
  }
  return out;
}

function loadLandscape() {
  var t = lastQ ? lastQ.get('type') || '' : '', my = token;
  fetch('/api/research/landscape' + (t ? '?type=' + encodeURIComponent(t) : ''))
    .then(function (r) { return r.json(); })
    .then(function (j) {
      if (my !== token) return;
      if (j.state === 'loading') { setTimeout(function () { if (my === token) loadLandscape(); }, 3000); return; }
      LAND = j; drawLandscape();
    })
    .catch(function () { /* the chart is optional; the results above still stand */ });
}

function drawLandscape() {
  if (!LAND) return;
  var pts = LAND.points, rank = {};
  matches.forEach(function (m) { rank[m.title] = m.rank; });
  var what = LAND.type ? LAND.type.toLowerCase() + 's' : 'systems across all types';
  var withR = pts.filter(function (p) { return p.km; }), withS = pts.filter(function (p) { return p.mach; });
  $('#lsub').textContent = 'Every ' + (LAND.type ? LAND.type.toLowerCase() : 'system') + ' that publishes the figure, out of ' +
    LAND.total + ' ' + what + ' found. Each dot is one system; both scales are logarithmic.';
  $('#land').hidden = false;
  drawStrip($('#chart-r'), $('#rsub'), withR, rank, {
    val: function (p) { return p.km; }, you: parseFloat(lastQ && lastQ.get('range')) || null,
    fmt: function (v) { return fmtKm(v) + ' km'; }, tick: fmtKm, axis: 'Range (km)', floor: 0.5, refs: [],
    count: withR.length + ' of ' + LAND.total + ' publish a range' });
  drawStrip($('#chart-s'), $('#ssub'), withS, rank, {
    val: function (p) { return p.mach; }, you: parseFloat(lastQ && lastQ.get('speed')) || null,
    fmt: function (v) { return 'Mach ' + fmtMach(v); }, tick: fmtMach, axis: 'Speed (Mach)', floor: 0.05,
    refs: [[1, 'Speed of sound', 'Mach 1'], [5, 'Hypersonic (Mach 5)', 'Mach 5']],
    count: withS.length + ' of ' + LAND.total + ' publish a speed' });

  var tbl = el('table'), hd = el('tr');
  ['System', 'Range (km)', 'Speed (Mach)'].forEach(function (h, i) { var th = el('th', h); if (i) th.style.textAlign = 'right'; hd.append(th); });
  tbl.append(hd);
  pts.slice().sort(function (a, b) { return (b.km || 0) - (a.km || 0) || (b.mach || 0) - (a.mach || 0); }).forEach(function (p) {
    var tr = el('tr', null, rank[p.title] ? 'm' : ''), td = el('td');
    var u = safeUrl(p.url), nm = (rank[p.title] ? '#' + rank[p.title] + ' ' : '') + p.title;
    if (u) { var a = el('a', nm); a.href = u; a.target = '_blank'; a.rel = 'noopener'; a.style.color = 'inherit'; td.append(a); } else td.textContent = nm;
    tr.append(td, el('td', p.km ? fmtKm(p.km) : '—', 'n'), el('td', p.mach ? fmtMach(p.mach) : '—', 'n'));
    tbl.append(tr);
  });
  $('#ltable').replaceChildren(tbl);
}

/* One dot per system along a log scale. Dots that would overlap are stacked above and below the
   centre line (a simple beeswarm), so every system stays visible and can be hovered. */
function drawStrip(box, sub, pts, rank, o) {
  box.replaceChildren();
  sub.textContent = o.count + '.';
  if (!pts.length) { box.append(el('p', 'None of these systems publish this figure.', 'cov')); return; }
  var vals = pts.map(o.val).concat(o.you ? [o.you] : []);
  var lo = Math.max(o.floor, Math.min.apply(null, vals) * 0.7), hi = Math.max.apply(null, vals) * 1.4;
  // top margin holds up to two label rows: reference lines (upper) and your value (lower)
  var W = Math.max(280, box.clientWidth), m = { l: 16, r: 16, t: o.refs.length && o.you ? 40 : 26, b: 40 }, pw = W - m.l - m.r;
  function X(v) { return m.l + Math.log(v / lo) / Math.log(hi / lo) * pw; }

  // place dots: sorted by position, each takes the nearest free row to the centre line
  var R = 4, gap = 2 * R + 1, rows = {}, placed = [];
  pts.slice().sort(function (a, b) { return o.val(a) - o.val(b); }).forEach(function (p) {
    var x = X(o.val(p)), k = 0;
    for (var i = 0; i < 400; i++) {
      k = i === 0 ? 0 : (i % 2 ? (i + 1) / 2 : -i / 2);
      if (rows[k] == null || x - rows[k] >= gap) break;
    }
    rows[k] = x;
    placed.push({ p: p, x: x, k: k });
  });
  var kmax = Math.max.apply(null, placed.map(function (d) { return Math.abs(d.k); }));
  var ph = Math.max(60, (2 * kmax + 1) * gap + 12), H = m.t + ph + m.b, mid = m.t + ph / 2;

  var svg = sv('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img',
    'aria-label': o.axis + ' of ' + pts.length + ' systems, one dot each. A table version is below.' });
  logTicks(lo, hi).forEach(function (v) {
    svg.append(sv('line', { 'class': 'gl', x1: X(v), x2: X(v), y1: m.t, y2: m.t + ph }),
               sv('text', { 'class': 'tk', x: X(v), y: m.t + ph + 16, 'text-anchor': 'middle' }, o.tick(v)));
  });
  var labels = [];   // drawn after the dots so they stay readable
  o.refs.forEach(function (r) {
    if (r[0] <= lo || r[0] >= hi) return;
    svg.append(sv('line', { 'class': 'ref', x1: X(r[0]), x2: X(r[0]), y1: m.t - (o.you ? 18 : 4), y2: m.t + ph }));
    var near = X(r[0]) > W - 140;   // keep labels near the right edge inside the chart
    labels.push(sv('text', { 'class': 'reft', x: X(r[0]) + (near ? -4 : 4), y: m.t - (o.you ? 22 : 8), 'text-anchor': near ? 'end' : 'start' },
                   W < 560 ? r[2] : r[1]));   // short labels on phones so they don't collide
  });
  svg.append(sv('text', { 'class': 'at', x: m.l + pw / 2, y: H - 4, 'text-anchor': 'middle' }, o.axis));
  if (o.you) {
    svg.append(sv('line', { 'class': 'you', x1: X(o.you), x2: X(o.you), y1: m.t - 4, y2: m.t + ph }));
    labels.push(sv('text', { 'class': 'youl', x: X(o.you) + 5, y: m.t - 8, 'text-anchor': X(o.you) > W - 70 ? 'end' : 'start' },
                   'You: ' + o.fmt(o.you)));
  }

  var tip = el('div', null, 'tip'); tip.hidden = true;
  function show(p, cx, cy, pin) {
    tip.replaceChildren(el('b', (rank[p.title] ? '#' + rank[p.title] + ' ' : '') + p.title),
                        el('span', [p.km ? 'Range about ' + fmtKm(p.km) + ' km' : '', p.mach ? 'Mach ' + fmtMach(p.mach) : ''].filter(Boolean).join(' · ')));
    var u = safeUrl(p.url);
    if (u) { var a = el('a', 'Read on Wikipedia ↗'); a.href = u; a.target = '_blank'; a.rel = 'noopener'; tip.append(document.createElement('br'), a); }
    tip.hidden = false;
    var left = cx + 12 + 240 > W ? cx - 252 : cx + 12;
    tip.style.left = Math.max(0, left) + 'px'; tip.style.top = Math.max(0, cy - 14) + 'px';
    pinned = pin ? p : null;
  }
  placed.sort(function (a, b) { return (rank[a.p.title] ? 1 : 0) - (rank[b.p.title] ? 1 : 0); });   // matches on top
  placed.forEach(function (d) {
    var p = d.p, cx = d.x, cy = mid + d.k * gap, r = rank[p.title];
    var hit = sv('circle', { 'class': 'hit', cx: cx, cy: cy, r: 9 });
    var dot = sv('circle', { 'class': 'dot ' + (r ? 'd-match' : 'd-other'), cx: cx, cy: cy, r: r ? 6 : R });
    hit.addEventListener('mouseenter', function () { if (!pinned) show(p, cx, cy, false); });
    hit.addEventListener('mouseleave', function () { if (!pinned) tip.hidden = true; });
    hit.addEventListener('click', function (e) { e.stopPropagation(); show(p, cx, cy, true); });
    svg.append(hit, dot);
    if (r) labels.push(sv('text', { 'class': 'rkl', x: cx + 7, y: cy - 6 }, String(r)));
  });
  labels.forEach(function (t) { t.style.pointerEvents = 'none'; svg.append(t); });
  box.onclick = function () { pinned = null; tip.hidden = true; };   // tap elsewhere to close a pinned tooltip
  box.append(svg, tip);
}
var landTimer;
window.addEventListener('resize', function () { clearTimeout(landTimer); landTimer = setTimeout(drawLandscape, 200); });

$('#f').addEventListener('submit', run);
$('#mb').onclick = function () {
  if (!lastQ) return;
  var q = new URLSearchParams(lastQ.toString()); q.set('offset', String(shown));
  var b = $('#mb'); b.disabled = true;
  fetch('/api/research?' + q.toString()).then(function (r) { return r.json(); })
    .then(function (j) { b.disabled = false; if (j.state === 'ready') render(j, true); else status(j.error || 'Could not load more. Try again.', true); })
    .catch(function () { b.disabled = false; status('Could not reach the server. Try again.', true); });
};
function xmenu(open) { $('#xmenu').hidden = !open; $('#xl').setAttribute('aria-expanded', String(open)); }
$('#xl').onclick = function (e) { e.stopPropagation(); xmenu($('#xmenu').hidden); };
document.addEventListener('click', function (e) { if (!e.target.closest('.xwrap')) xmenu(false); });
document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && !$('#xmenu').hidden) { xmenu(false); $('#xl').focus(); } });
document.querySelectorAll('#xmenu button').forEach(function (m) { m.onclick = function () { xmenu(false); exportAs(m.dataset.fmt); }; });
function exportAs(fmt) {   // the matches on screen, one column each
  if (!lastQ || !shown) return;
  var q = new URLSearchParams(lastQ.toString()); q.set('count', String(shown)); q.set('format', fmt);
  var b = $('#xl'), label = b.textContent;
  b.disabled = true; b.textContent = 'Preparing…';
  fetch('/api/research/export?' + q.toString()).then(function (r) {
    if (!r.ok) return r.json().then(function (j) { throw new Error(j.error || 'Export failed.'); });
    var name = (/filename="([^"]+)"/.exec(r.headers.get('Content-Disposition') || '') || [])[1] || 'market-research.' + fmt;
    return r.blob().then(function (blob) {
      var a = document.createElement('a'), url = URL.createObjectURL(blob);
      a.href = url; a.download = name; document.body.append(a); a.click(); a.remove();
      setTimeout(function () { URL.revokeObjectURL(url); }, 4000);
    });
  }).catch(function (ex) { status(ex.message || 'Could not export. Try again.', true); })
    .finally(function () { b.disabled = false; b.textContent = label; });
}
$('#clr').onclick = function () { token++; $('#more').hidden = true; clearTimeout(timer); FIELDS.forEach(function (id) { $('#' + id).value = ''; }); $('#res').replaceChildren(); status(''); $('#go').disabled = false; LAND = null; $('#land').hidden = true; };
$('#ex').onclick = function () {
  FIELDS.forEach(function (id) { $('#' + id).value = ''; });
  $('#type').value = 'Anti-ship missile'; $('#range').value = '150'; $('#speed').value = '0.9'; $('#warhead').value = '200';
  run();
};
loadOptions();
</script>
</body></html>"""


@app.route("/")
def home():
    return HOME


@app.route("/trends")
def trends():
    return PAGE


@app.route("/research")
def research():
    request_types(list(RESEARCH_TYPES), front=False)  # start loading data in the background
    return RESEARCH_PAGE


@app.route("/standards")
def standards():
    return STANDARDS_PAGE


@app.route("/battle-lab")
def battle_lab():
    return BATTLE_HTML


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

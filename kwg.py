#!/usr/bin/env python3
"""
kwg.py - Keyword Generator (web-first, no-login) v1.3

Sources:
- autocomplete: Google suggestqueries (unofficial)
- trends: Google Trends trending RSS
- reddit: Reddit public search JSON
- serp: DuckDuckGo HTML SERP scrape-lite (no login)

Controls:
- --sources autocomplete,trends,reddit,serp
- SERP sub-features:
    --serp-related / --no-serp-related
    --serp-snippets / --no-serp-snippets
    --serp-freshness / --no-serp-freshness
    --serp-ugc / --no-serp-ugc
    --serp-weakness / --no-serp-weakness
    --serp-ngrams / --no-serp-ngrams
- Variants:
    --variants none|basic|all|custom
    --variants-packs question,commercial,compare,local,platform,opensource,howto
    --variants-max-per-term N
    --variants-max-total N
    --variants-seed-only
    --variants-to-sources
    --variants-only
    --variants-include-original / --no-variants-include-original
- Clustering:
    --clusters K  (0 disables)
- Output:
    --format txt|json|csv|md|report
    --out DIR     (writes bundle files)
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import io
import json
import math
import os
import random
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from html import unescape
from typing import Any, Dict, List, Optional, Sequence, Tuple


# ---------------------------
# Defaults / Utils
# ---------------------------

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
)

STOPWORDS = set("""
a an and are as at be by for from has have how i in is it its of on or our
that the this to was we what when where which who why with you your vs
""".split())

UGC_DOMAINS = set("""
reddit.com stackoverflow.com stackexchange.com superuser.com serverfault.com
quora.com github.com medium.com dev.to news.ycombinator.com
learn.microsoft.com community.cloudflare.com discourse.org
""".split())


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def normalize_space(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def word_count(s: str) -> int:
    return len([w for w in re.split(r"\s+", s.strip()) if w])


def parse_window(window: str) -> dt.timedelta:
    m = re.fullmatch(r"(\d+)\s*([dmy])", window.strip().lower())
    if not m:
        raise ValueError(f"Invalid window: {window} (expected 7d, 30d, 12m, 1y)")
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "d":
        return dt.timedelta(days=n)
    if unit == "m":
        return dt.timedelta(days=30 * n)
    if unit == "y":
        return dt.timedelta(days=365 * n)
    raise ValueError("unreachable")


def bucket_geo(geo: str) -> str:
    geo = geo.strip().upper()
    if geo in ("GLOBAL", "WORLD", "WW", ""):
        return "US"
    return geo


def http_get(
    url: str,
    *,
    ua: str = DEFAULT_UA,
    timeout: int = 25,
    headers: Optional[Dict[str, str]] = None,
    retries: int = 2,
    backoff_base_s: float = 0.7,
) -> bytes:
    attempts = max(1, int(retries) + 1)
    retry_http = {408, 409, 425, 429, 500, 502, 503, 504}
    merged_headers: Dict[str, str] = {"User-Agent": ua, "Accept": "*/*"}
    if headers:
        merged_headers.update(headers)

    for attempt in range(attempts):
        req = urllib.request.Request(url, headers=merged_headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as ex:
            if ex.code in retry_http and attempt < attempts - 1:
                time.sleep(backoff_base_s * (2 ** attempt))
                continue
            raise
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError):
            if attempt < attempts - 1:
                time.sleep(backoff_base_s * (2 ** attempt))
                continue
            raise

    raise RuntimeError("unreachable")


def safe_http_get(
    url: str,
    *,
    context: str,
    ua: str = DEFAULT_UA,
    timeout: int = 25,
    headers: Optional[Dict[str, str]] = None,
    retries: int = 2,
) -> Optional[bytes]:
    try:
        return http_get(url, ua=ua, timeout=timeout, headers=headers, retries=retries)
    except urllib.error.HTTPError as ex:
        eprint(f"[warn] {context}: HTTP {ex.code} ({ex.reason})")
    except urllib.error.URLError as ex:
        eprint(f"[warn] {context}: URL error ({ex.reason})")
    except (TimeoutError, socket.timeout):
        eprint(f"[warn] {context}: timeout")
    except Exception as ex:
        eprint(f"[warn] {context}: {type(ex).__name__}: {ex}")
    return None


@dataclasses.dataclass
class Cache:
    cache_dir: str
    ttl_seconds: int = 6 * 3600

    def _path(self, key: str) -> str:
        ensure_dir(self.cache_dir)
        return os.path.join(self.cache_dir, f"{sha1(key)}.json")

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        path = self._path(key)
        if not os.path.exists(path):
            return None
        try:
            st = os.stat(path)
            if (time.time() - st.st_mtime) > self.ttl_seconds:
                return None
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def set(self, key: str, payload: Dict[str, Any]) -> None:
        path = self._path(key)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)


@dataclasses.dataclass
class RateLimiter:
    min_interval_s: float
    _last: float = 0.0

    def wait(self) -> None:
        if self.min_interval_s <= 0:
            return
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval_s:
            time.sleep(self.min_interval_s - delta)
        self._last = time.time()


@dataclasses.dataclass
class Candidate:
    term: str
    sources: List[str] = dataclasses.field(default_factory=list)
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)


# ---------------------------
# Normalization / Light stemming / Intent
# ---------------------------

def light_stem(w: str) -> str:
    for suf in ("ing", "edly", "ed", "ly", "ies", "s"):
        if w.endswith(suf) and len(w) > len(suf) + 2:
            if suf == "ies":
                return w[:-3] + "y"
            return w[:-len(suf)]
    return w


def canonical_key(term: str) -> str:
    t = term.lower()
    t = re.sub(r"[^a-z0-9\s\-']", " ", t)
    t = normalize_space(t)
    toks = []
    for w in t.split():
        w = w.strip("-'")
        if not w or w in STOPWORDS:
            continue
        toks.append(light_stem(w))
    return " ".join(toks)


def detect_intent(term: str) -> str:
    t = term.lower()
    if re.search(r"\b(error|fix|not working|issue|problem|crash|broken|bug)\b", t):
        return "troubleshooting"
    if re.search(r"\b(best|top|buy|price|pricing|deal|coupon|vs|review|compare|alternative)\b", t):
        return "commercial"
    if re.search(r"\b(how|what|why|when|where|who)\b", t):
        return "question"
    if re.search(r"\b(guide|tutorial|explained|meaning|definition|examples)\b", t):
        return "info"
    return "any"


# ---------------------------
# History (Trends recurrence across runs)
# ---------------------------

def history_path(cache_dir: str, geo: str) -> str:
    ensure_dir(cache_dir)
    return os.path.join(cache_dir, f"history_trends_{geo}.jsonl")


def update_trends_history(cache_dir: str, geo: str, terms: List[str]) -> None:
    path = history_path(cache_dir, geo)
    day = now_utc().date().isoformat()
    record = {"day": day, "terms": terms[:200]}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    if obj.get("day") == day:
                        return
        except Exception:
            pass
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_trends_recurrence(cache_dir: str, geo: str, window: dt.timedelta) -> Dict[str, int]:
    path = history_path(cache_dir, geo)
    rec: Dict[str, int] = {}
    if not os.path.exists(path):
        return rec
    cutoff = now_utc().date() - window
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                day = obj.get("day")
                if not day:
                    continue
                d = dt.date.fromisoformat(day)
                if d < cutoff:
                    continue
                for t in obj.get("terms", []):
                    key = canonical_key(str(t))
                    if not key:
                        continue
                    rec[key] = rec.get(key, 0) + 1
    except Exception:
        return rec
    return rec


# ---------------------------
# Variants Engine
# ---------------------------

@dataclasses.dataclass
class VariantConfig:
    mode: str = "basic"  # none|basic|all|custom
    packs: List[str] = dataclasses.field(default_factory=lambda: ["question", "commercial", "compare"])
    max_per_term: int = 30
    max_total: int = 600
    seed_only: bool = False
    to_sources: bool = False
    only: bool = False
    include_original: bool = True


def _is_plural(word: str) -> bool:
    return word.endswith("s") and not word.endswith("ss")


def _pluralize(word: str) -> str:
    if word.endswith("y") and len(word) > 2 and word[-2] not in "aeiou":
        return word[:-1] + "ies"
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    return word + "s"


def _singularize(word: str) -> str:
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("es") and len(word) > 2:
        base = word[:-2]
        if base.endswith(("s", "x", "z", "ch", "sh")):
            return base
    if word.endswith("s") and len(word) > 1:
        return word[:-1]
    return word


def generate_variants_for_term(term: str, cfg: VariantConfig) -> List[str]:
    t = normalize_space(term)
    if not t:
        return []

    base = t
    toks = [x for x in re.split(r"\s+", base.strip()) if x]
    first = toks[0].lower() if toks else ""
    last = toks[-1].lower() if toks else ""

    if cfg.mode == "none":
        packs: List[str] = []
    elif cfg.mode == "basic":
        packs = ["question", "commercial", "compare", "opensource"]
    elif cfg.mode == "all":
        packs = ["question", "commercial", "compare", "local", "platform", "opensource", "howto"]
    else:
        packs = list(cfg.packs)

    out: List[str] = []
    seen: set[str] = set()

    def add(x: str) -> None:
        x = normalize_space(x)
        if not x:
            return
        k = canonical_key(x)
        if not k or k in seen:
            return
        seen.add(k)
        out.append(x)

    if cfg.include_original:
        add(base)

    if toks and len(last) >= 4 and last.isalpha():
        if _is_plural(last):
            add(" ".join(toks[:-1] + [_singularize(last)]))
        else:
            add(" ".join(toks[:-1] + [_pluralize(last)]))

    if "question" in packs:
        add(f"what is {base}")
        add(f"how does {base} work")
        add(f"why is {base} important")
        add(f"{base} explained")
        add(f"{base} examples")
        add(f"{base} tutorial")
        add(f"{base} guide")
        add(f"{base} vs alternatives")
        add(f"{base} meaning")

    if "howto" in packs:
        add(f"how to use {base}")
        add(f"how to build {base}")
        add(f"how to implement {base}")
        add(f"how to install {base}")
        add(f"{base} step by step")

    if "commercial" in packs:
        add(f"best {base}")
        add(f"top {base}")
        add(f"{base} pricing")
        add(f"{base} cost")
        add(f"{base} review")
        add(f"{base} recommendations")
        add(f"{base} pros and cons")

    if "compare" in packs:
        add(f"{base} vs")
        add(f"{base} vs {first or 'alternatives'}")
        add(f"{base} alternatives")
        add(f"{base} comparison")
        add(f"{base} competitor")
        add(f"{base} similar tools")

    if "local" in packs:
        add(f"{base} near me")
        add(f"{base} in {bucket_geo('US')}")
        add(f"local {base}")
        add(f"{base} service near me")

    if "platform" in packs:
        add(f"{base} for windows")
        add(f"{base} for mac")
        add(f"{base} for linux")
        add(f"{base} cli")
        add(f"{base} api")
        add(f"{base} extension")

    if "opensource" in packs:
        add(f"open source {base}")
        add(f"{base} github")
        add(f"{base} repo")
        add(f"{base} self hosted")
        add(f"{base} license")

    return out[: max(0, int(cfg.max_per_term))]


def expand_variants(
    seeds: List[str],
    candidates: Optional[List[Candidate]],
    cfg: VariantConfig,
) -> List[str]:
    base_terms: List[str]
    if cfg.seed_only or not candidates:
        base_terms = seeds[:]
    else:
        c_terms = [normalize_space(c.term) for c in candidates if normalize_space(c.term)]
        c_terms = sorted(set(c_terms), key=lambda s: (abs(word_count(s) - 4), len(s)))
        base_terms = seeds[:] + c_terms[:200]

    out: List[str] = []
    seen: set[str] = set()
    for t in base_terms:
        for v in generate_variants_for_term(t, cfg):
            k = canonical_key(v)
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(v)
            if len(out) >= int(cfg.max_total):
                return out
    return out


# ---------------------------
# Sources (no-login)
# ---------------------------

def src_autocomplete(seed: str, *, geo: str, hl: str, cache: Cache, rl: RateLimiter) -> List[Candidate]:
    geo = bucket_geo(geo)
    out: List[Candidate] = []

    def fetch(q: str) -> List[str]:
        params = {"client": "firefox", "q": q, "hl": hl, "gl": geo}
        url = "https://suggestqueries.google.com/complete/search?" + urllib.parse.urlencode(params)
        key = f"autocomplete|{geo}|{hl}|{q}"
        cached = cache.get(key)
        if cached is not None:
            return list(cached.get("suggestions", []))
        rl.wait()
        raw = safe_http_get(url, context=f"autocomplete:{q}")
        if raw is None:
            return []
        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return []
        suggestions = data[1] if isinstance(data, list) and len(data) > 1 else []
        suggestions = [normalize_space(s) for s in suggestions if isinstance(s, str)]
        cache.set(key, {"suggestions": suggestions, "fetched_at": now_utc().isoformat()})
        return suggestions

    for s in fetch(seed):
        out.append(Candidate(term=s, sources=["autocomplete"], meta={"variant": "base"}))

    expansions = [f"{seed} {ch}" for ch in "abcdefghijklmnopqrstuvwxyz"] + [
        f"{seed} how", f"{seed} best", f"{seed} vs", f"{seed} for",
        f"{seed} near me", f"{seed} app", f"{seed} extension",
        f"{seed} tool", f"{seed} open source",
    ]

    for q in expansions[:40]:
        for s in fetch(q):
            out.append(Candidate(term=s, sources=["autocomplete"], meta={"variant": "expand", "q": q}))

    return out


def src_trends_rss(*, geo: str, cache: Cache, rl: RateLimiter) -> List[Candidate]:
    geo = bucket_geo(geo)
    url = "https://trends.google.com/trending/rss?" + urllib.parse.urlencode({"geo": geo})
    key = f"trends_rss|{geo}"
    cached = cache.get(key)
    if cached is None:
        rl.wait()
        raw = safe_http_get(url, context=f"trends_rss:{geo}")
        if raw is None:
            return []
        cache.set(key, {"rss": raw.decode("utf-8", errors="replace"), "fetched_at": now_utc().isoformat()})
        rss_text = raw.decode("utf-8", errors="replace")
    else:
        rss_text = str(cached.get("rss", ""))

    out: List[Candidate] = []
    terms_for_history: List[str] = []

    try:
        root = ET.fromstring(rss_text)
        for item in root.findall("./channel/item"):
            title_el = item.find("title")
            if title_el is None or not title_el.text:
                continue
            term = normalize_space(title_el.text)
            terms_for_history.append(term)
            out.append(Candidate(term=term, sources=["trends"], meta={"feed": "trending_rss"}))
            if " - " in term:
                out.append(Candidate(term=normalize_space(term.split(" - ", 1)[0]),
                                     sources=["trends"], meta={"feed": "trending_rss", "split": True}))
    except ET.ParseError:
        return out

    update_trends_history(cache.cache_dir, geo, terms_for_history)
    return out


def src_reddit(seed: str, *, cache: Cache, rl: RateLimiter) -> List[Candidate]:
    params = {"q": seed, "sort": "relevance", "t": "month", "limit": "25", "type": "link", "raw_json": "1"}
    q = urllib.parse.urlencode(params)
    urls = [
        "https://www.reddit.com/search.json?" + q,
        "https://old.reddit.com/search.json?" + q,
    ]
    key = f"reddit_search|{seed}"
    cached = cache.get(key)
    if cached is None:
        data: Dict[str, Any] = {}
        for url in urls:
            rl.wait()
            raw = safe_http_get(
                url,
                context=f"reddit:{seed}",
                headers={
                    "Accept": "application/json",
                    "Referer": "https://www.reddit.com/",
                },
                retries=1,
            )
            if raw is None:
                continue
            try:
                obj = json.loads(raw.decode("utf-8", errors="replace"))
            except Exception:
                continue
            if isinstance(obj, dict):
                data = obj
                break
        if data:
            cache.set(key, {"data": data, "fetched_at": now_utc().isoformat()})
        else:
            return []
    else:
        obj = cached.get("data", {})
        data = obj if isinstance(obj, dict) else {}

    titles: List[str] = []
    try:
        children = data.get("data", {}).get("children", [])
        for c in children:
            t = c.get("data", {}).get("title")
            if isinstance(t, str) and t.strip():
                titles.append(normalize_space(t))
    except Exception:
        pass

    def tokenize(s: str) -> List[str]:
        s = s.lower()
        s = re.sub(r"[^a-z0-9\s\-]", " ", s)
        s = normalize_space(s)
        toks = []
        for w in s.split():
            w = w.strip("-")
            if not w or w in STOPWORDS or len(w) <= 2:
                continue
            toks.append(w)
        return toks

    counts: Dict[str, int] = {}
    for t in titles:
        toks = tokenize(t)
        for n in (2, 3, 4, 5):
            for i in range(0, max(0, len(toks) - n + 1)):
                phrase = " ".join(toks[i:i+n])
                counts[phrase] = counts.get(phrase, 0) + 1

    out: List[Candidate] = []
    for phrase, c in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:120]:
        out.append(Candidate(term=phrase, sources=["reddit"], meta={"title_ngram_hits": c}))
    return out


# ---------------------------
# SERP-lite (DuckDuckGo HTML) with sub-feature toggles
# ---------------------------

@dataclasses.dataclass
class SerpFlags:
    related: bool = True
    snippets: bool = True
    freshness: bool = True
    ugc: bool = True
    weakness: bool = True
    ngrams: bool = True


@dataclasses.dataclass
class SerpItem:
    title: str
    snippet: str
    url: str
    domain: str


def domain_of(url: str) -> str:
    try:
        u = urllib.parse.urlparse(url)
        host = (u.netloc or "").lower().split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def strip_tags(s: str) -> str:
    s = re.sub(r"<script.*?</script>", " ", s, flags=re.I | re.S)
    s = re.sub(r"<style.*?</style>", " ", s, flags=re.I | re.S)
    s = re.sub(r"<.*?>", " ", s, flags=re.S)
    return normalize_space(unescape(s))


def parse_ddg_items(html: str, *, want_snippets: bool) -> Tuple[List[SerpItem], List[str]]:
    items: List[SerpItem] = []
    related: List[str] = []

    for m in re.finditer(r"Related Searches.*?(<a.*?</a>){1,40}", html, flags=re.I | re.S):
        block = m.group(0)
        for a in re.finditer(r'<a[^>]+href="[^"]+"[^>]*>(.*?)</a>', block, flags=re.I | re.S):
            txt = strip_tags(a.group(1))
            if txt and len(txt) >= 3:
                related.append(txt)
        break

    title_iter = re.finditer(
        r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
        html, flags=re.I | re.S
    )
    for m in title_iter:
        url = unescape(m.group(1))
        title = strip_tags(m.group(2))
        if not url or not title:
            continue

        snip = ""
        if want_snippets:
            start = m.end()
            window = html[start:start + 3500]
            snip_m = re.search(r'class="result__snippet[^"]*".*?>\s*(.*?)</', window, flags=re.I | re.S)
            if snip_m:
                snip = strip_tags(snip_m.group(1))

        dom = domain_of(url)
        items.append(SerpItem(title=title, snippet=snip, url=url, domain=dom))
        if len(items) >= 25:
            break

    return items, related


def freshness_bucket(text: str) -> str:
    t = (text or "").lower()
    if not t.strip():
        return "none"
    if re.search(r"\b(\d{1,2})\s+(days?|weeks?|months?)\s+ago\b", t) or re.search(r"\b(today|yesterday)\b", t):
        return "fresh"
    years = [int(y) for y in re.findall(r"\b(20\d{2})\b", t)]
    if not years:
        return "none"
    cur = now_utc().year
    if any(y >= cur - 1 for y in years):
        return "fresh"
    return "dated"


def src_serp_ddg(seed: str, *, kl: str, flags: SerpFlags, cache: Cache, rl: RateLimiter) -> List[Candidate]:
    params = {"q": seed}
    if kl:
        params["kl"] = kl
    url = "https://duckduckgo.com/html/?" + urllib.parse.urlencode(params)
    key = f"ddg_html|{kl}|{seed}"
    cached = cache.get(key)
    if cached is None:
        rl.wait()
        raw = safe_http_get(url, context=f"serp_ddg:{seed}")
        if raw is None:
            return []
        html = raw.decode("utf-8", errors="replace")
        cache.set(key, {"html": html, "fetched_at": now_utc().isoformat()})
    else:
        html = str(cached.get("html", ""))

    items, related = parse_ddg_items(html, want_snippets=flags.snippets)
    domains = [it.domain for it in items if it.domain]
    domain_diversity = len(set(domains))

    ugc_ratio = 0.0
    if flags.ugc and domains:
        ugc_hits = sum(1 for d in domains if d in UGC_DOMAINS or d.endswith(".discourse.org"))
        ugc_ratio = ugc_hits / len(domains)

    buckets = {"fresh": 0, "dated": 0, "none": 0}
    if flags.freshness:
        for it in items[:20]:
            b = freshness_bucket(f"{it.title} {it.snippet}")
            buckets[b] += 1

    out: List[Candidate] = []

    if flags.related:
        for r in related[:30]:
            out.append(Candidate(term=r, sources=["serp"], meta={"ddg_related": True}))

    if flags.ngrams:
        def tokenize(s: str) -> List[str]:
            s = s.lower()
            s = re.sub(r"[^a-z0-9\s\-]", " ", s)
            s = normalize_space(s)
            toks = []
            for w in s.split():
                w = w.strip("-")
                if not w or w in STOPWORDS or len(w) <= 2:
                    continue
                toks.append(w)
            return toks

        counts: Dict[str, int] = {}
        texts = [(it.title + " " + it.snippet).strip() for it in items[:20]]
        for tx in texts:
            toks = tokenize(tx)
            for n in (2, 3, 4, 5):
                for i in range(0, max(0, len(toks) - n + 1)):
                    phrase = " ".join(toks[i:i + n])
                    counts[phrase] = counts.get(phrase, 0) + 1

        for phrase, c in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:160]:
            meta: Dict[str, Any] = {"ddg_ngram_hits": c}
            if flags.weakness:
                meta["ddg_domain_diversity"] = domain_diversity
            if flags.ugc:
                meta["ddg_ugc_ratio"] = round(ugc_ratio, 3)
            if flags.freshness:
                meta["ddg_fresh"] = buckets["fresh"]
                meta["ddg_dated"] = buckets["dated"]
                meta["ddg_nodate"] = buckets["none"]
            out.append(Candidate(term=phrase, sources=["serp"], meta=meta))

    meta_seed: Dict[str, Any] = {"ddg_items": len(items)}
    if flags.weakness:
        meta_seed["ddg_domain_diversity"] = domain_diversity
    if flags.ugc:
        meta_seed["ddg_ugc_ratio"] = round(ugc_ratio, 3)
    if flags.freshness:
        meta_seed["ddg_fresh"] = buckets["fresh"]
        meta_seed["ddg_dated"] = buckets["dated"]
        meta_seed["ddg_nodate"] = buckets["none"]
    if flags.related:
        meta_seed["ddg_related_count"] = len(related)

    if items or related:
        out.append(Candidate(term=seed, sources=["serp"], meta=meta_seed))
    return out


# ---------------------------
# Scoring
# ---------------------------

@dataclasses.dataclass
class Scored:
    term: str
    score: float
    sources: List[str]
    intent: str
    reasons: Dict[str, Any]


def score_candidates(
    cands: List[Candidate],
    *,
    intent_filter: str,
    trends_rec: Dict[str, int],
    serp_flags: SerpFlags,
) -> List[Scored]:
    merged: Dict[str, Candidate] = {}

    for c in cands:
        term = normalize_space(c.term)
        if not term:
            continue
        key = canonical_key(term)
        if not key:
            continue

        if key not in merged:
            merged[key] = Candidate(term=term, sources=list(c.sources), meta=dict(c.meta))
        else:
            cur = merged[key].term

            def desirability(t: str) -> Tuple[int, int]:
                wc = word_count(t)
                good = 1 if 2 <= wc <= 8 else 0
                return (good, len(t))

            if desirability(term) > desirability(cur):
                merged[key].term = term

            for s in c.sources:
                if s not in merged[key].sources:
                    merged[key].sources.append(s)

            for mk, mv in c.meta.items():
                if mk not in merged[key].meta:
                    merged[key].meta[mk] = mv
                else:
                    if isinstance(mv, (int, float)) and isinstance(merged[key].meta[mk], (int, float)):
                        merged[key].meta[mk] = max(merged[key].meta[mk], mv)

    out: List[Scored] = []
    for key, c in merged.items():
        term = c.term
        intent = detect_intent(term)
        if intent_filter != "any" and intent != intent_filter:
            continue

        srcs = set(c.sources)
        base = 45.0
        reasons: Dict[str, Any] = {}

        if "autocomplete" in srcs:
            base += 18
            reasons["autocomplete"] = +18
        if "trends" in srcs:
            base += 16
            reasons["trends"] = +16
        if "reddit" in srcs:
            base += 8
            reasons["reddit"] = +8
        if "serp" in srcs:
            base += 10
            reasons["serp"] = +10
        if "variants" in srcs:
            base += 4
            reasons["variants"] = +4

        if len(srcs) >= 2:
            bonus = 10 + 4 * (len(srcs) - 2)
            base += bonus
            reasons["multi_source_bonus"] = bonus

        rec = trends_rec.get(key, 0)
        if rec:
            boost = clamp(3.0 * rec, 0.0, 18.0)
            base += boost
            reasons["trends_recurrence_boost"] = round(boost, 2)

        if serp_flags.weakness:
            ddg_div = c.meta.get("ddg_domain_diversity")
            if isinstance(ddg_div, int) and ddg_div > 0:
                weakness = clamp((8 - ddg_div) * 1.5, -6.0, 12.0)
                base += weakness
                reasons["serp_domain_diversity_proxy"] = round(weakness, 2)

        if serp_flags.ugc:
            ugc_ratio = c.meta.get("ddg_ugc_ratio")
            if isinstance(ugc_ratio, (int, float)) and ugc_ratio > 0:
                ugc_boost = clamp(float(ugc_ratio) * 18.0, 0.0, 12.0)
                base += ugc_boost
                reasons["serp_ugc_dominance_boost"] = round(ugc_boost, 2)

        if serp_flags.freshness:
            ddg_fresh = c.meta.get("ddg_fresh")
            ddg_dated = c.meta.get("ddg_dated")
            ddg_nodate = c.meta.get("ddg_nodate")
            if all(isinstance(x, int) for x in (ddg_fresh, ddg_dated, ddg_nodate)):
                total = int(ddg_fresh) + int(ddg_dated) + int(ddg_nodate)
                if total > 0:
                    stale_ratio = (int(ddg_dated) + int(ddg_nodate)) / total
                    adj = clamp((stale_ratio - 0.5) * 10.0, -6.0, 8.0)
                    base += adj
                    reasons["serp_staleness_proxy"] = round(adj, 2)

        wc = word_count(term)
        if wc <= 1:
            base -= 18
            reasons["too_short"] = -18
        elif 2 <= wc <= 6:
            base += 6
            reasons["good_length"] = +6
        elif wc >= 10:
            base -= 10
            reasons["too_long"] = -10

        if intent in ("question", "commercial", "troubleshooting"):
            base += 3
            reasons["intent_bonus"] = +3

        t = term.lower()
        if re.search(r"\b(free money|make money fast|casino|porn|xxx)\b", t):
            base -= 40
            reasons["low_quality_penalty"] = -40

        score = clamp(base, 0.0, 100.0)
        out.append(Scored(term=term, score=score, sources=sorted(srcs), intent=intent, reasons=reasons))

    out.sort(key=lambda s: (s.score, len(s.sources), word_count(s.term)), reverse=True)
    return out


# ---------------------------
# Output + Bundling
# ---------------------------

def render_keywords(scored: List[Scored], fmt: str, *, explain: bool) -> str:
    fmt = fmt.lower()
    if fmt == "json":
        payload = []
        for s in scored:
            obj: Dict[str, Any] = {"term": s.term, "score": round(s.score, 2), "sources": s.sources, "intent": s.intent}
            if explain:
                obj["reasons"] = s.reasons
            payload.append(obj)
        return json.dumps(payload, ensure_ascii=False, indent=2)

    if fmt == "csv":
        out = io.StringIO()
        headers = ["term", "score", "sources", "intent"]
        if explain:
            headers.append("reasons_json")
        out.write(",".join(headers) + "\n")
        for s in scored:
            term = s.term.replace('"', '""')
            sources = "|".join(s.sources).replace('"', '""')
            row = ['"' + term + '"', str(round(s.score, 2)), '"' + sources + '"', '"' + s.intent + '"']
            if explain:
                rj = json.dumps(s.reasons, ensure_ascii=False).replace('"', '""')
                row.append('"' + rj + '"')
            out.write(",".join(row) + "\n")
        return out.getvalue()

    if fmt in ("md", "report"):
        lines: List[str] = []
        for i, s in enumerate(scored, 1):
            tag = ",".join(s.sources)
            if explain:
                lines.append(
                    f"{i}. **{s.term}** — {round(s.score,2)} ({tag}) `[{s.intent}]`  \n"
                    f"   - reasons: `{json.dumps(s.reasons, ensure_ascii=False)}`"
                )
            else:
                lines.append(f"{i}. **{s.term}** — {round(s.score,2)} ({tag}) `[{s.intent}]`")
        return "\n".join(lines)

    lines = []
    for i, s in enumerate(scored, 1):
        tag = ",".join(s.sources)
        if explain:
            lines.append(
                f"{i}. {s.term}  score={round(s.score,2)}  sources={tag}  intent={s.intent}  "
                f"reasons={json.dumps(s.reasons, ensure_ascii=False)}"
            )
        else:
            lines.append(f"{i}. {s.term}  ({round(s.score,2)})  [{tag}] [{s.intent}]")
    return "\n".join(lines)


def write_bundle(out_dir: str, *, scored: List[Scored], clusters_payload: Optional[Dict[str, Any]], report_md: Optional[str]) -> None:
    ensure_dir(out_dir)
    with open(os.path.join(out_dir, "keywords.json"), "w", encoding="utf-8") as f:
        f.write(render_keywords(scored, "json", explain=True))
    with open(os.path.join(out_dir, "keywords.csv"), "w", encoding="utf-8") as f:
        f.write(render_keywords(scored, "csv", explain=False))
    if clusters_payload is not None:
        with open(os.path.join(out_dir, "clusters.json"), "w", encoding="utf-8") as f:
            json.dump(clusters_payload, f, ensure_ascii=False, indent=2)
    if report_md is not None:
        with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as f:
            f.write(report_md)


# ---------------------------
# Clustering (dependency-free)
# ---------------------------

def tokenize_for_vec(text: str) -> List[str]:
    t = text.lower()
    t = re.sub(r"[^a-z0-9\s\-]", " ", t)
    t = normalize_space(t)
    toks: List[str] = []
    for w in t.split():
        w = w.strip("-")
        if not w or w in STOPWORDS or len(w) <= 2:
            continue
        toks.append(light_stem(w))
    return toks


def hash_vec(tokens: List[str], dim: int = 256) -> List[float]:
    v = [0.0] * dim
    for tok in tokens:
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        idx = h % dim
        v[idx] += 1.0
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / norm for x in v]


def cosine(a: List[float], b: List[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def kmeans_cosine(vectors: List[List[float]], k: int, iters: int = 12, seed: int = 1337) -> List[int]:
    n = len(vectors)
    if k <= 1 or n == 0:
        return [0] * n
    k = min(k, n)
    random.seed(seed)
    centers = [vectors[i] for i in random.sample(range(n), k)]
    assign = [0] * n

    for _ in range(iters):
        changed = False
        for i, v in enumerate(vectors):
            best_j = 0
            best_sim = -1e9
            for j, c in enumerate(centers):
                sim = cosine(v, c)
                if sim > best_sim:
                    best_sim = sim
                    best_j = j
            if assign[i] != best_j:
                assign[i] = best_j
                changed = True

        new_centers = [[0.0] * len(vectors[0]) for _ in range(k)]
        counts = [0] * k
        for i, v in enumerate(vectors):
            j = assign[i]
            counts[j] += 1
            for d, val in enumerate(v):
                new_centers[j][d] += val

        for j in range(k):
            if counts[j] == 0:
                new_centers[j] = vectors[random.randrange(n)]
                continue
            inv = 1.0 / counts[j]
            new_centers[j] = [x * inv for x in new_centers[j]]
            norm = math.sqrt(sum(x * x for x in new_centers[j])) or 1.0
            new_centers[j] = [x / norm for x in new_centers[j]]

        centers = new_centers
        if not changed:
            break

    return assign


def label_cluster(terms: List[str]) -> str:
    freq: Dict[str, int] = {}
    for t in terms[:20]:
        for tok in tokenize_for_vec(t):
            freq[tok] = freq.get(tok, 0) + 1
    top = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:3]
    return " / ".join([w for (w, _) in top]) if top else "cluster"


def build_clusters(scored: List[Scored], k: int) -> Tuple[Dict[str, Any], str]:
    terms = [s.term for s in scored]
    vectors = [hash_vec(tokenize_for_vec(t)) for t in terms]
    assign = kmeans_cosine(vectors, k)

    buckets: Dict[int, List[int]] = {}
    for i, a in enumerate(assign):
        buckets.setdefault(a, []).append(i)

    cluster_list = []
    md_lines = ["# kwg report", "", "## Clusters", ""]

    for cid, idxs in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True):
        idxs_sorted = sorted(idxs, key=lambda i: scored[i].score, reverse=True)
        top_terms = [scored[i].term for i in idxs_sorted[:30]]
        label = label_cluster(top_terms)

        intents: Dict[str, int] = {}
        for i in idxs:
            intents[scored[i].intent] = intents.get(scored[i].intent, 0) + 1

        cluster_list.append({
            "cluster_id": cid,
            "label": label,
            "size": len(idxs),
            "intent_counts": intents,
            "top_terms": [
                {"term": scored[i].term, "score": round(scored[i].score, 2), "sources": scored[i].sources, "intent": scored[i].intent}
                for i in idxs_sorted[:30]
            ],
        })

        md_lines.append(f"### Cluster {cid}: {label} ({len(idxs)} terms)")
        md_lines.append("")
        md_lines.append(f"- intent mix: `{json.dumps(intents, ensure_ascii=False)}`")
        md_lines.append("")
        for rank, i in enumerate(idxs_sorted[:20], 1):
            s = scored[i]
            md_lines.append(f"{rank}. **{s.term}** — {round(s.score,2)} ({','.join(s.sources)}) `[{s.intent}]`")
        md_lines.append("")

    payload = {"k": k, "clusters": cluster_list}
    report = "\n".join(md_lines)
    return payload, report


# ---------------------------
# Pipeline
# ---------------------------

def gather_candidates(
    seeds: Sequence[str],
    *,
    sources: Sequence[str],
    geo: str,
    hl: str,
    kl: str,
    serp_flags: SerpFlags,
    cache: Cache,
    rl: RateLimiter,
) -> List[Candidate]:
    all_cands: List[Candidate] = []
    srcset = set([s.strip().lower() for s in sources if s.strip()])

    if "trends" in srcset:
        try:
            all_cands.extend(src_trends_rss(geo=geo, cache=cache, rl=rl))
        except Exception as ex:
            eprint(f"[warn] source trends failed: {type(ex).__name__}: {ex}")

    for seed in seeds:
        seed = normalize_space(seed)
        if not seed:
            continue
        if "autocomplete" in srcset:
            try:
                all_cands.extend(src_autocomplete(seed, geo=geo, hl=hl, cache=cache, rl=rl))
            except Exception as ex:
                eprint(f"[warn] source autocomplete failed for '{seed}': {type(ex).__name__}: {ex}")
        if "reddit" in srcset:
            try:
                all_cands.extend(src_reddit(seed, cache=cache, rl=rl))
            except Exception as ex:
                eprint(f"[warn] source reddit failed for '{seed}': {type(ex).__name__}: {ex}")
        if "serp" in srcset:
            try:
                all_cands.extend(src_serp_ddg(seed, kl=kl, flags=serp_flags, cache=cache, rl=rl))
            except Exception as ex:
                eprint(f"[warn] source serp failed for '{seed}': {type(ex).__name__}: {ex}")

    return all_cands


# ---------------------------
# CLI
# ---------------------------

def add_serp_toggles(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("SERP feature toggles")
    g.add_argument("--serp-related", dest="serp_related", action="store_true", default=True)
    g.add_argument("--no-serp-related", dest="serp_related", action="store_false")
    g.add_argument("--serp-snippets", dest="serp_snippets", action="store_true", default=True)
    g.add_argument("--no-serp-snippets", dest="serp_snippets", action="store_false")
    g.add_argument("--serp-freshness", dest="serp_freshness", action="store_true", default=True)
    g.add_argument("--no-serp-freshness", dest="serp_freshness", action="store_false")
    g.add_argument("--serp-ugc", dest="serp_ugc", action="store_true", default=True)
    g.add_argument("--no-serp-ugc", dest="serp_ugc", action="store_false")
    g.add_argument("--serp-weakness", dest="serp_weakness", action="store_true", default=True)
    g.add_argument("--no-serp-weakness", dest="serp_weakness", action="store_false")
    g.add_argument("--serp-ngrams", dest="serp_ngrams", action="store_true", default=True)
    g.add_argument("--no-serp-ngrams", dest="serp_ngrams", action="store_false")


def add_variants_toggles(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("Variants engine")
    g.add_argument("--variants", default="basic", choices=["none", "basic", "all", "custom"])
    g.add_argument(
        "--variants-packs",
        default="question,commercial,compare,opensource",
        help="comma-separated packs for --variants custom: question,commercial,compare,local,platform,opensource,howto",
    )
    g.add_argument("--variants-max-per-term", type=int, default=30)
    g.add_argument("--variants-max-total", type=int, default=600)
    g.add_argument("--variants-seed-only", action="store_true", help="only generate variants from the seed terms")
    g.add_argument("--variants-to-sources", action="store_true", help="feed variants back into sources as extra seeds")
    g.add_argument("--variants-only", action="store_true", help="skip web sources; output only variants")
    g.add_argument("--variants-include-original", dest="variants_include_original", action="store_true", default=True)
    g.add_argument("--no-variants-include-original", dest="variants_include_original", action="store_false")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="kwg", add_help=True)
    sub = p.add_subparsers(dest="cmd")

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--sources", default="autocomplete,trends,reddit,serp",
                        help="comma-separated: autocomplete,trends,reddit,serp")
        sp.add_argument("--geo", default="US", help="2-letter country code (US, TH, GB) or GLOBAL")
        sp.add_argument("--window", default="30d", help="e.g. 7d, 30d, 90d, 12m, 1y")
        sp.add_argument("--hl", default="en", help="language for autocomplete (best-effort)")
        sp.add_argument("--kl", default="us-en", help="DuckDuckGo region/lang bucket (e.g., us-en, th-en, th-th)")
        sp.add_argument("--intent", default="any", choices=["any", "question", "commercial", "troubleshooting", "info"])
        sp.add_argument("--format", default="txt", choices=["txt", "json", "csv", "md", "report"])
        sp.add_argument("--explain", action="store_true", help="include scoring breakdown")
        sp.add_argument("--clusters", type=int, default=0, help="0 disables; e.g. 8 for 8 clusters")
        sp.add_argument("--out", default="", help="output directory for bundle (keywords.json/csv, clusters.json, report.md)")
        sp.add_argument("--cache-dir", default=os.path.expanduser("~/.kwg/cache"))
        sp.add_argument("--cache-ttl", type=int, default=6 * 3600, help="seconds")
        sp.add_argument("--rate-limit", type=float, default=0.6, help="seconds between web requests")
        add_serp_toggles(sp)
        add_variants_toggles(sp)

    pop = sub.add_parser("pop", help="generate candidates from sources + variants, rank, output")
    pop.add_argument("seeds", nargs="+", help="seed keyword(s)")
    pop.add_argument("--top", type=int, default=100)
    add_common(pop)

    exp = sub.add_parser("expand", help="generate candidates only (no ranking), output terms")
    exp.add_argument("seeds", nargs="+", help="seed keyword(s)")
    exp.add_argument("--limit", type=int, default=500)
    add_common(exp)

    sc = sub.add_parser("score", help="score an existing list of terms from stdin or a file")
    sc.add_argument("--from", dest="src", default="stdin", choices=["stdin", "file"])
    sc.add_argument("--file", default="", help="path if --from file")
    sc.add_argument("--top", type=int, default=200)
    add_common(sc)

    return p


def main(argv: List[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv[1:])
    if not args.cmd:
        parser.print_help()
        return 2

    cache = Cache(cache_dir=args.cache_dir, ttl_seconds=args.cache_ttl)
    rl = RateLimiter(min_interval_s=max(0.0, float(args.rate_limit)))
    sources = [s.strip().lower() for s in args.sources.split(",") if s.strip()]

    try:
        win_td = parse_window(args.window)
    except Exception as ex:
        eprint(str(ex))
        return 2

    serp_flags = SerpFlags(
        related=bool(args.serp_related),
        snippets=bool(args.serp_snippets),
        freshness=bool(args.serp_freshness),
        ugc=bool(args.serp_ugc),
        weakness=bool(args.serp_weakness),
        ngrams=bool(args.serp_ngrams),
    )

    var_cfg = VariantConfig(
        mode=str(args.variants),
        packs=[x.strip().lower() for x in str(args.variants_packs).split(",") if x.strip()],
        max_per_term=int(args.variants_max_per_term),
        max_total=int(args.variants_max_total),
        seed_only=bool(args.variants_seed_only),
        to_sources=bool(args.variants_to_sources),
        only=bool(args.variants_only),
        include_original=bool(args.variants_include_original),
    )
    if var_cfg.max_per_term < 0:
        eprint("Invalid --variants-max-per-term: must be >= 0")
        return 2
    if var_cfg.max_total < 0:
        eprint("Invalid --variants-max-total: must be >= 0")
        return 2

    if args.cmd == "expand":
        cands: List[Candidate] = []
        if not var_cfg.only:
            cands = gather_candidates(args.seeds, sources=sources, geo=args.geo, hl=args.hl, kl=args.kl,
                                      serp_flags=serp_flags, cache=cache, rl=rl)

        variants: List[str] = []
        if var_cfg.mode != "none":
            variants = expand_variants(list(args.seeds), cands if not var_cfg.seed_only else None, var_cfg)

        if (not var_cfg.only) and var_cfg.to_sources and variants:
            extra = gather_candidates(variants[:80], sources=sources, geo=args.geo, hl=args.hl, kl=args.kl,
                                      serp_flags=serp_flags, cache=cache, rl=rl)
            cands.extend(extra)

        terms = sorted({normalize_space(c.term) for c in cands if normalize_space(c.term)})
        if variants:
            terms = sorted(set(terms + variants), key=lambda x: (len(x), x))
        terms = terms[: max(1, int(args.limit))]

        if args.format == "json":
            print(json.dumps([{"term": t} for t in terms], ensure_ascii=False, indent=2))
        elif args.format == "csv":
            print("term")
            for t in terms:
                esc = t.replace('"', '""').replace("\n", " ").replace("\r", " ")
                print('"' + esc + '"')
        else:
            for i, t in enumerate(terms, 1):
                print(f"{i}. {t}")
        return 0

    if args.cmd == "score":
        if args.src == "stdin":
            raw = sys.stdin.read()
        else:
            if not args.file:
                eprint("Missing --file PATH")
                return 2
            with open(args.file, "r", encoding="utf-8") as f:
                raw = f.read()
        terms = [normalize_space(x) for x in raw.splitlines()]
        terms = [t for t in terms if t]
        cands = [Candidate(term=t, sources=["input"]) for t in terms]
        trends_rec = load_trends_recurrence(cache.cache_dir, bucket_geo(args.geo), win_td)
        scored = score_candidates(cands, intent_filter=args.intent, trends_rec=trends_rec, serp_flags=serp_flags)[: max(1, int(args.top))]
        print(render_keywords(scored, args.format, explain=args.explain))
        return 0

    # pop
    cands2: List[Candidate] = []
    base_seeds = list(args.seeds)

    if not var_cfg.only:
        cands2 = gather_candidates(base_seeds, sources=sources, geo=args.geo, hl=args.hl, kl=args.kl,
                                   serp_flags=serp_flags, cache=cache, rl=rl)

    variants2: List[str] = []
    if var_cfg.mode != "none":
        variants2 = expand_variants(base_seeds, cands2 if (cands2 and not var_cfg.seed_only) else None, var_cfg)
        for v in variants2:
            cands2.append(Candidate(term=v, sources=["variants"], meta={"variant_pack": var_cfg.mode}))

        if (not var_cfg.only) and var_cfg.to_sources and variants2:
            extra = gather_candidates(variants2[:80], sources=sources, geo=args.geo, hl=args.hl, kl=args.kl,
                                      serp_flags=serp_flags, cache=cache, rl=rl)
            cands2.extend(extra)

    trends_rec = load_trends_recurrence(cache.cache_dir, bucket_geo(args.geo), win_td)
    scored2 = score_candidates(cands2, intent_filter=args.intent, trends_rec=trends_rec, serp_flags=serp_flags)[: max(1, int(args.top))]
    if not scored2:
        eprint("[warn] no keywords produced (all sources may have failed or returned no data)")

    clusters_payload: Optional[Dict[str, Any]] = None
    report_md: Optional[str] = None
    if int(args.clusters) and int(args.clusters) > 0:
        clusters_payload, report_md = build_clusters(scored2, int(args.clusters))

    if args.format == "report":
        if clusters_payload is None:
            clusters_payload, report_md = build_clusters(scored2, 8)
        print(report_md or "")
    else:
        print(render_keywords(scored2, args.format, explain=args.explain))

    if args.out:
        write_bundle(args.out, scored=scored2, clusters_payload=clusters_payload, report_md=report_md)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv))
    except KeyboardInterrupt:
        eprint("\nInterrupted.")
        raise SystemExit(130)

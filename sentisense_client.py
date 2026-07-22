"""
sentisense_client.py — SentiSense API client with DB-backed daily caching.

All responses are cached once per calendar day in global_state (JSON + timestamp).
This keeps API calls at an absolute minimum across the ecosystem's many cron runs —
a call made at 08:00 ET is reused at 13:00 ET and 17:30 ET without hitting the API again.

Free tier: 1,000 req/mo | PRO: unlimited @ 300 req/min.
Auth header: X-SentiSense-API-Key

Cache keys (all DB global_state, JSON-encoded):
  ss_market_mood         — Market Mood index (once/day)
  ss_congressional       — Congressional trades top-10 (once/day)
  ss_institutional_{SYM} — 13F institutional flows per symbol (once/day)
  ss_insights_{SYM}      — Insider + institutional + sentiment signals per symbol (once/day)
  ss_sentiment_{SYM}     — SentiSense score + social data per symbol (once/day)
"""

import os
import json
import logging
import time
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL    = "https://app.sentisense.ai/api/v1"
_API_KEY    = os.getenv("SENTISENSE_API_KEY", "")
_CACHE_DATE = None   # module-level today string, set at first use


def _today() -> str:
    return date.today().isoformat()


def _headers() -> dict:
    if not _API_KEY:
        raise RuntimeError("SENTISENSE_API_KEY not set in .env")
    return {"X-SentiSense-API-Key": _API_KEY, "Accept": "application/json"}


def _get(path: str, params: dict = None, timeout: int = 15) -> dict:  # Optional[dict]
    """Raw GET with basic retry (1 retry on 429/5xx)."""
    url = f"{BASE_URL}{path}"
    for attempt in range(2):
        try:
            r = requests.get(url, headers=_headers(), params=params or {}, timeout=timeout)
            if r.status_code == 429:
                logger.warning(f"[SentiSense] 429 rate limit on {path} — sleeping 5s")
                time.sleep(5)
                continue
            if r.status_code >= 500:
                logger.warning(f"[SentiSense] {r.status_code} on {path} (attempt {attempt+1})")
                time.sleep(2)
                continue
            if r.status_code != 200:
                # 404 = endpoint not available at this plan tier; not an error worth alerting on
                lvl = logging.DEBUG if r.status_code == 404 else logging.WARNING
                logger.log(lvl, f"[SentiSense] {r.status_code} on {path}: {r.text[:200]}")
                return None
            return r.json()
        except requests.RequestException as e:
            logger.warning(f"[SentiSense] Request failed {path}: {e}")
            if attempt == 0:
                time.sleep(2)
    return None


def _cache_load(db, key: str, cache_days: int = 1) -> dict:  # Optional[dict]
    """Return cached payload if fresh (within cache_days), else None."""
    raw = db.get_state(key)
    if not isinstance(raw, dict):
        return None
    stored = raw.get("date")
    if not stored:
        return None
    if cache_days == 1:
        return raw.get("data") if stored == _today() else None
    try:
        from datetime import date as _date, timedelta
        age = (_date.today() - _date.fromisoformat(stored)).days
        return raw.get("data") if age < cache_days else None
    except Exception:
        return None


def _cache_save(db, key: str, data: dict) -> None:
    db.update_state(key, {"date": _today(), "data": data})


# ── Public API methods ────────────────────────────────────────────────────────

def get_market_mood(db) -> dict:  # Optional[dict]
    """
    SentiSense proprietary Market Mood index (0-100, fear=low / greed=high).
    Cached once per calendar day. Requires auth.
    Returns: {"score": int, "label": str, "signal": str} or None on failure.
    """
    cached = _cache_load(db, "ss_market_mood")
    if cached:
        return cached

    data = _get("/market/mood")
    if not data:
        # Fallback: return last cached value (any age) so the signal isn't blank
        raw = db.get_state("ss_market_mood")
        if isinstance(raw, dict) and raw.get("data"):
            logger.debug(f"[SentiSense] market_mood unavailable — using stale cache from {raw.get('date', '?')}")
            return raw["data"]
        return None

    # API may wrap under "data" key or return flat
    inner = data.get("data") or data
    score = inner.get("score") or inner.get("mood_score") or inner.get("value")
    label = inner.get("label") or inner.get("classification") or inner.get("mood_label") or "Unknown"
    if score is None:
        logger.warning(f"[SentiSense] market_mood: score missing, keys={list(inner.keys())[:8]}")
        return None

    score = int(score)
    if score >= 75:
        signal = "EXTREME GREED — elevated top risk"
    elif score >= 60:
        signal = "GREED — cautious of overbought conditions"
    elif score <= 25:
        signal = "EXTREME FEAR — bottom-hunting window"
    elif score <= 40:
        signal = "FEAR — accumulation zone"
    else:
        signal = "NEUTRAL"

    result = {"score": score, "label": label, "signal": signal}
    _cache_save(db, "ss_market_mood", result)
    logger.info(f"[SentiSense] Market Mood fetched: {score} ({label})")
    return result


def get_institutional_flows(db, ticker: str) -> dict:  # Optional[dict]
    """
    13F institutional flow data for a single ticker.
    Returns net flow summary: net_shares, net_direction, top_buyers, top_sellers.
    Cached once per calendar day per ticker.
    """
    key = f"ss_institutional_{ticker.upper()}"
    cached = _cache_load(db, key)
    if cached:
        return cached

    data = _get("/institutional/flows", params={"ticker": ticker.upper()})
    if not data:
        return None

    # API wraps list under "data" key; also handles flat list
    inner = data.get("data") or data
    flows = inner if isinstance(inner, list) else inner.get("flows") or []
    if not flows:
        logger.info(f"[SentiSense] No institutional flow data for {ticker}")
        return None

    # Aggregate net shares across all filers
    net_shares = 0
    buyers, sellers = [], []
    for f in flows:
        shares = f.get("shares_change") or f.get("change_shares") or f.get("net_shares") or 0
        filer  = f.get("filer_name") or f.get("institution") or f.get("name") or ""
        try:
            shares = int(shares)
        except (TypeError, ValueError):
            shares = 0
        net_shares += shares
        if shares > 0:
            buyers.append((filer, shares))
        elif shares < 0:
            sellers.append((filer, abs(shares)))

    buyers.sort(key=lambda x: x[1], reverse=True)
    sellers.sort(key=lambda x: x[1], reverse=True)
    net_direction = "ACCUMULATING" if net_shares > 0 else ("DISTRIBUTING" if net_shares < 0 else "NEUTRAL")

    result = {
        "ticker":        ticker.upper(),
        "net_shares":    net_shares,
        "net_direction": net_direction,
        "top_buyers":    buyers[:3],
        "top_sellers":   sellers[:3],
        "filer_count":   len(flows),
    }
    _cache_save(db, key, result)
    logger.info(f"[SentiSense] Institutional flows {ticker}: {net_direction} ({net_shares:+,} shares, {len(flows)} filers)")
    return result


def get_insights(db, ticker: str) -> dict:  # Optional[dict]
    """
    AI cross-referenced signals: insider cluster buys/sells, institutional changes,
    volume/sentiment anomalies. Cached once per calendar day per ticker.
    Returns: {"insider_cluster": bool, "insider_direction": str, "signals": list, "urgency": str}
    """
    key = f"ss_insights_{ticker.upper()}"
    cached = _cache_load(db, key)
    if cached:
        return cached

    data = _get(f"/insights/stock/{ticker.upper()}")
    if not data:
        return None

    # API wraps list under "data" key
    inner   = data.get("data") or data
    signals = inner if isinstance(inner, list) else inner.get("insights") or inner.get("signals") or []
    if not signals:
        return None

    insider_signals = [
        s for s in signals
        if "insider" in str(s.get("type", "") or s.get("category", "")).lower()
        or "form 4" in str(s.get("source", "") or s.get("sourceType", "")).lower()
        or "insider" in str(s.get("title", "")).lower()
    ]
    def _is_buy(s):
        d = (s.get("direction") or s.get("action") or s.get("transactionType") or "").upper()
        return "BUY" in d or "PURCHASE" in d
    def _is_sell(s):
        d = (s.get("direction") or s.get("action") or s.get("transactionType") or "").upper()
        return "SELL" in d or "SALE" in d
    cluster_buy  = any(_is_buy(s)  for s in insider_signals)
    cluster_sell = any(_is_sell(s) for s in insider_signals)

    urgency_vals = [s.get("urgency", "").upper() for s in signals]
    urgency = "HIGH" if "HIGH" in urgency_vals else ("MEDIUM" if "MEDIUM" in urgency_vals else "LOW")

    result = {
        "ticker":           ticker.upper(),
        "insider_cluster":  bool(insider_signals),
        "cluster_buy":      cluster_buy,
        "cluster_sell":     cluster_sell,
        "insider_count":    len(insider_signals),
        "total_signals":    len(signals),
        "urgency":          urgency,
        "signals":          signals[:5],  # top 5 for display
    }
    _cache_save(db, key, result)
    logger.info(f"[SentiSense] Insights {ticker}: {len(insider_signals)} insider, {len(signals)} total, urgency={urgency}")
    return result


def get_sentiment(db, ticker: str) -> dict:  # Optional[dict]
    """
    SentiSense Score + social data.
    Response lives under data.{sentisenseScore, scoreLabel, direction, mentions, socialDominance}.
    Score is a signed float (negative = bearish momentum, positive = bullish).
    Cached once per calendar day per ticker.
    """
    key = f"ss_sentiment_{ticker.upper()}"
    cached = _cache_load(db, key)
    if cached:
        return cached

    data = _get(f"/stocks/{ticker.upper()}/sentiment")
    if not data:
        return None

    # API wraps payload under "data" key
    inner = data.get("data") or data
    if not isinstance(inner, dict):
        logger.warning(f"[SentiSense] sentiment {ticker}: unexpected type {type(inner)}")
        return None

    score     = inner.get("sentisenseScore")
    mentions  = inner.get("mentions") or 0
    dominance = inner.get("socialDominance") or 0.0
    label     = inner.get("scoreLabel") or inner.get("direction") or ""
    direction = inner.get("latestDirection") or inner.get("direction") or ""

    if score is None:
        logger.warning(f"[SentiSense] sentiment {ticker}: score field missing, keys={list(inner.keys())[:8]}")
        return None

    try:
        score     = float(score)
        mentions  = int(mentions)
        dominance = float(dominance) * 100  # API returns as fraction (0.0183 → 1.83%)
    except (TypeError, ValueError):
        return None

    # Score is signed float: typically -100 to +100 range
    # Map to lean/meter for display
    lean  = (direction or label or "Neutral").title()
    meter = "HIGH" if abs(score) >= 30 else ("NEUTRAL" if abs(score) >= 10 else "LOW")

    result = {
        "ticker":    ticker.upper(),
        "score":     score,
        "label":     label,
        "direction": direction,
        "mentions":  mentions,
        "dominance": round(dominance, 2),
        "lean":      lean,
        "meter":     meter,
    }
    _cache_save(db, key, result)
    logger.info(f"[SentiSense] Sentiment {ticker}: score={score:.1f}, lean={lean}, mentions={mentions}")
    return result


def get_congressional_trades(db, limit: int = 6) -> list:  # Optional[list]
    """
    Recent STOCK Act congressional trading disclosures.
    Cached once per calendar day. Returns list of trade dicts.
    """
    cached = _cache_load(db, "ss_congressional")
    if cached is not None:
        return cached[:limit]

    data = _get("/politicians/activity", params={"limit": 20})
    if not data:
        return None

    # API wraps list under "data" key
    inner  = data.get("data") or data
    trades = inner if isinstance(inner, list) else []
    if not trades:
        return None

    result = []
    for t in trades[:20]:
        result.append({
            "politician": t.get("politicianName") or t.get("politician_name") or "Unknown",
            "party":      t.get("party") or "",
            "state":      t.get("state") or "",
            "ticker":     (t.get("ticker") or t.get("symbol") or "?").upper(),
            "action":     (t.get("transactionType") or t.get("transaction_type") or t.get("type") or "?").title(),
            "amount":     t.get("amountRange") or t.get("amount_range") or t.get("amount") or "?",
            "date":       t.get("transactionDate") or t.get("transaction_date") or t.get("date") or "?",
        })

    _cache_save(db, "ss_congressional", result)
    logger.info(f"[SentiSense] Congressional trades fetched: {len(result)} records")
    return result[:limit]


# ── Tracker API (pre-built, cached snapshots from SentiSense) ────────────────

def _ticker_from_url(url: str) -> str:  # Optional[str]
    """Extract ticker from tracker row URL: '/stocks/NVDA/sentiment' → 'NVDA'."""
    try:
        parts = (url or "").split("/")
        if len(parts) >= 3 and parts[1] == "stocks":
            return parts[2].upper()
    except Exception:
        pass
    return None


def _find_metric(metrics: list, label: str) -> dict:
    """Return the first metric dict with matching label, or {}."""
    for m in (metrics or []):
        if m.get("label") == label:
            return m
    return {}


def get_reddit_picks(db, limit: int = 10) -> list:  # Optional[list]
    """
    SentiSense Reddit Picks tracker — stocks with high Reddit conviction, curated
    with entry date, return since entry, posture, and peak mention count.

    Refresh interval: 30 days (monthly snapshot). Cached locally for 7 days.
    Cache key: ss_tracker_reddit_picks
    Returns list of:
      {ticker, return_pct, posture, peak_mentions, entry_date, source_url}
    """
    cached = _cache_load(db, "ss_tracker_reddit_picks", cache_days=7)
    if cached is not None:
        return cached[:limit]

    data = _get("/trackers/reddit-picks")
    if not data:
        return None

    rows = (data.get("data") or {}).get("rows") or []
    result = []
    for row in rows:
        ticker = (row.get("rowId") or "").upper()
        if not ticker:
            continue
        metrics = row.get("metrics") or []
        ret_m   = _find_metric(metrics, "Return since entry")
        post_m  = _find_metric(metrics, "Posture")
        peak_m  = _find_metric(metrics, "Peak mentions")
        try:
            return_pct = float(ret_m.get("value", 0))
        except (TypeError, ValueError):
            return_pct = 0.0
        try:
            peak_mentions = int(peak_m.get("value", 0))
        except (TypeError, ValueError):
            peak_mentions = 0
        result.append({
            "ticker":        ticker,
            "return_pct":    return_pct,
            "posture":       (post_m.get("value") or "NEUTRAL").upper(),
            "peak_mentions": peak_mentions,
            "entry_date":    ret_m.get("periodLabel") or row.get("asOf") or "",
            "source_url":    post_m.get("sourceUrl") or ret_m.get("sourceUrl") or "",
        })

    _cache_save(db, "ss_tracker_reddit_picks", result)
    logger.info(f"[SentiSense] Reddit Picks fetched: {len(result)} positions")
    return result[:limit]


def get_sentiment_movers(db, direction: str = "both", limit: int = 10) -> list:  # Optional[list]
    """
    SentiSense Sentiment Movers tracker — stocks with the biggest sentiment score
    change over the past 7 days. Refreshes daily.

    direction: "improving" | "deteriorating" | "both"
    Cache key: ss_tracker_sentiment_movers
    Returns list of:
      {ticker, score, tone, score_change_7d, mentions_7d, category}
    """
    cached = _cache_load(db, "ss_tracker_sentiment_movers", cache_days=1)
    if cached is not None:
        rows = cached
        if direction != "both":
            rows = [r for r in rows if r.get("category") == direction]
        return rows[:limit]

    data = _get("/trackers/sentiment-movers")
    if not data:
        return None

    rows = (data.get("data") or {}).get("rows") or []
    result = []
    for row in rows:
        ticker = _ticker_from_url(row.get("url") or "")
        if not ticker:
            ticker = (row.get("rowId") or "").upper()
            if "/" in ticker or not ticker:
                continue
        metrics = row.get("metrics") or []
        score_m   = _find_metric(metrics, "SentiSense Score")
        tone_m    = _find_metric(metrics, "Score tone")
        change_m  = _find_metric(metrics, "Score change (7d)")
        mention_m = _find_metric(metrics, "Mentions (7d)")
        try:
            score = float(score_m.get("value", 0))
        except (TypeError, ValueError):
            score = 0.0
        try:
            score_change = float(change_m.get("value", 0))
        except (TypeError, ValueError):
            score_change = 0.0
        try:
            mentions = int(mention_m.get("value", 0))
        except (TypeError, ValueError):
            mentions = 0
        result.append({
            "ticker":         ticker,
            "score":          score,
            "tone":           (tone_m.get("value") or "Neutral"),
            "score_change_7d": score_change,
            "mentions_7d":    mentions,
            "category":       row.get("category") or "improving",
        })

    _cache_save(db, "ss_tracker_sentiment_movers", result)
    logger.info(f"[SentiSense] Sentiment Movers fetched: {len(result)} rows")
    if direction != "both":
        result = [r for r in result if r.get("category") == direction]
    return result[:limit]


def get_sentiment_leaderboard(db, side: str = "both", limit: int = 8) -> list:  # Optional[list]
    """
    SentiSense Sentiment Leaderboard tracker — top-ranked stocks by sentiment score
    with driving story and 7-day trend. Refreshes daily.

    side: "bullish" | "bearish" | "both"
    Cache key: ss_tracker_sentiment_leaderboard
    Returns list of:
      {ticker, score, tone, score_7d, mentions_7d, driving_story, story_url, category}
    """
    cached = _cache_load(db, "ss_tracker_sentiment_leaderboard", cache_days=1)
    if cached is not None:
        rows = cached
        if side != "both":
            rows = [r for r in rows if r.get("category") == side]
        return rows[:limit]

    data = _get("/trackers/sentiment-leaderboard")
    if not data:
        return None

    rows = (data.get("data") or {}).get("rows") or []
    result = []
    for row in rows:
        ticker = _ticker_from_url(row.get("url") or "")
        if not ticker:
            ticker = (row.get("rowId") or "").upper()
            if "/" in ticker or not ticker:
                continue
        metrics  = row.get("metrics") or []
        score_m  = _find_metric(metrics, "SentiSense Score")
        tone_m   = _find_metric(metrics, "Score tone")
        score7_m = _find_metric(metrics, "SentiSense Score 7d")
        ment_m   = _find_metric(metrics, "Mentions (7d)")
        story_m  = _find_metric(metrics, "Driving story")
        try:
            score = float(score_m.get("value", 0))
        except (TypeError, ValueError):
            score = 0.0
        try:
            score_7d = float(score7_m.get("value", 0))
        except (TypeError, ValueError):
            score_7d = 0.0
        try:
            mentions = int(ment_m.get("value", 0))
        except (TypeError, ValueError):
            mentions = 0
        result.append({
            "ticker":        ticker,
            "score":         score,
            "tone":          (tone_m.get("value") or "Neutral"),
            "score_7d":      score_7d,
            "mentions_7d":   mentions,
            "driving_story": (story_m.get("value") or ""),
            "story_url":     (story_m.get("sourceUrl") or ""),
            "category":      row.get("category") or "bullish",
        })

    _cache_save(db, "ss_tracker_sentiment_leaderboard", result)
    logger.info(f"[SentiSense] Sentiment Leaderboard fetched: {len(result)} rows")
    if side != "both":
        result = [r for r in result if r.get("category") == side]
    return result[:limit]


# ── Batch helpers — used by wheel_signals to fetch multiple tickers efficiently ─

def batch_institutional_flows(db, tickers: list) -> dict:
    """
    Fetch institutional flows for a list of tickers.
    Returns {ticker: result_dict}. Respects cache — no redundant calls.
    """
    out = {}
    for ticker in tickers:
        try:
            r = get_institutional_flows(db, ticker)
            if r:
                out[ticker.upper()] = r
        except Exception as e:
            logger.warning(f"[SentiSense] batch inst flow {ticker}: {e}")
    return out


def batch_insights(db, tickers: list) -> dict:
    """Fetch AI insights for a list of tickers. Returns {ticker: result_dict}."""
    out = {}
    for ticker in tickers:
        try:
            r = get_insights(db, ticker)
            if r:
                out[ticker.upper()] = r
        except Exception as e:
            logger.warning(f"[SentiSense] batch insights {ticker}: {e}")
    return out


def batch_sentiment(db, tickers: list) -> dict:
    """Fetch SentiSense Score for a list of tickers. Returns {ticker: result_dict}."""
    out = {}
    for ticker in tickers:
        try:
            r = get_sentiment(db, ticker)
            if r:
                out[ticker.upper()] = r
        except Exception as e:
            logger.warning(f"[SentiSense] batch sentiment {ticker}: {e}")
    return out

"""
mes_desk.py — /MES (Micro E-mini S&P 500) session desk → #futures-trading (Discord only).

Trader window: 22:00–00:00 HST = 08:00–10:00 UTC (HST has no DST, so the UTC window is fixed).
That is the London morning session: from Oct 25 (UK clocks back) 22:00 HST is exactly the
London open (08:00 London time).

DATA — no CME feed in the stack (Twelve Data carries no CME futures; Tradier's brokerage API
is equities/options only). The proxy chain:
  CSPX:LSE (iShares Core S&P 500, USD, trades 08:00–16:30 London — live through the window)
    → S&P 500 level   via a daily calibration:
        SPX/SPY  = FRED SP500 close ÷ SPY close (same 16:00 ET print)
        CSPX/SPY = median ratio over the LSE/NYSE overlap (≈ 09:30–11:30 ET) on the latest day
    → /MES price      via cost-of-carry fair value: F = S × (1 + (r − q) × t)
        r = FRED DTB3 (3-month T-bill), q = MES_DIV_YIELD (default 1.2%), t = days to expiry / 365
Absolute levels are approximate (±~5 pts); distances and ranges in points are accurate.

Dispatches (market_scheduler.py, weekdays UTC = Sun–Thu nights HST):
  07:45 UTC (21:45 HST)  plan    — bias, key levels, risk plan, context
  17:10 UTC (07:10 HST)  recap   — window stats, bias grade, model trade, your trades, week/ladder
                                   (morning after: London bars are end-of-day delayed on the
                                   Twelve Data Grow plan — verified Oct 1 2026)
  range  — London opening range + 4/9 EMA state + setup. Built but NOT scheduled: it needs a
           live feed during the window (CME data or Twelve Data Pro). Manual: mes_desk.py range --date

Trade log (run on PythonAnywhere so it writes the live DB):
  python3.10 mes_desk.py trade open  --side long --entry 7712.25 [--contracts 1] [--stop 10] [--target 10] [--note "..."]
  python3.10 mes_desk.py trade close --id 12 --exit 7722.25
  python3.10 mes_desk.py trade status
"""

import os
import sys
import json
import logging
import argparse
import statistics
from datetime import datetime, date, timedelta, timezone

import pytz
import requests
from dotenv import load_dotenv

from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

try:
    from essentials_tools import send_essentials_embed
except ImportError:
    def send_essentials_embed(url, title, desc, color):
        requests.post(url, json={"embeds": [{"title": title, "description": desc, "color": color}]}, timeout=10)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logger = logging.getLogger("MES_Desk")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TD_API_KEY      = os.getenv("TWELVE_DATA_API_KEY")
FRED_API_KEY    = os.getenv("FRED_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")

COLOR_GREEN  = 0x2ecc71
COLOR_YELLOW = 0xf1c40f
COLOR_RED    = 0xe74c3c
COLOR_BLUE   = 0x2980b9

HST    = pytz.timezone("Pacific/Honolulu")
LONDON = pytz.timezone("Europe/London")

# ── Contract + personal risk rules (CLAUDE.md §0-H) ──────────────────────────
MES_POINT_VALUE  = 5.0     # $ per index point per contract
MES_TICK         = 0.25
STOP_PTS         = float(os.getenv("MES_STOP_PTS", "10"))
TARGET_PTS       = float(os.getenv("MES_TARGET_PTS", "10"))
FEES_RT          = float(os.getenv("MES_FEES_RT", "1.50"))   # est. round trip per contract (commission + exchange + NFA)
DIV_YIELD        = float(os.getenv("MES_DIV_YIELD", "0.012"))
WEEKLY_GOAL      = (30.0, 50.0)
NIGHT_MAX_LOSSES = 2
NIGHT_MAX_LOSS_USD = 100.0
LADDER_MIN_TRADES  = 20
LADDER_MAX_DD_USD  = 150.0   # 3 full stops

WINDOW_START_UTC = (8, 0)    # 22:00 HST
WINDOW_END_UTC   = (10, 0)   # 00:00 HST
OR_MINUTES       = 30        # London opening range length
RANGE_ALERT_UTC  = (8, 35)   # 22:35 HST — range alert reads bars up to this time
YELLOW_BAND_PTS  = 1.0       # |EMA4 − EMA9| below this = YELLOW (pullback / possible turn)
EXTENDED_PTS     = 6.0       # close this far from EMA9 = extended, expect a revisit
BIAS_MOVE_PTS    = 3.0       # window move needed to grade the bias WIN/LOSS

CSPX = "CSPX:LSE"

db = EcosystemDatabase()


# =====================================================================
# DATA HELPERS
# =====================================================================
def td_series(symbol, interval, outputsize):
    """Twelve Data time series, ascending, floats, UTC timestamps. [] on failure."""
    try:
        r = requests.get("https://api.twelvedata.com/time_series", params={
            "symbol": symbol, "interval": interval, "outputsize": outputsize,
            "timezone": "UTC", "apikey": TD_API_KEY}, timeout=20).json()
        rows = []
        for v in reversed(r.get("values", [])):
            rows.append({"dt": v["datetime"], "open": float(v["open"]), "high": float(v["high"]),
                         "low": float(v["low"]), "close": float(v["close"]),
                         "volume": float(v.get("volume") or 0)})
        if not rows:
            logger.warning(f"Twelve Data {symbol} {interval}: {r.get('message', 'no values')}")
        return rows
    except Exception as e:
        logger.error(f"Twelve Data {symbol} {interval} failed: {e}")
        return []


def fred_observations(series_id, limit=10):
    """FRED observations (newest first), cached once per UTC day in the DB."""
    today = datetime.now(timezone.utc).date().isoformat()
    key = f"fred_cache_{series_id}"
    cached = db.get_state(key) or {}
    if cached.get("date") == today and cached.get("obs"):
        return cached["obs"]
    try:
        obs = requests.get("https://api.stlouisfed.org/fred/series/observations", params={
            "series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json",
            "sort_order": "desc", "limit": limit}, timeout=15).json()["observations"]
        obs = [(o["date"], float(o["value"])) for o in obs if o["value"] != "."]
        db.update_state(key, {"date": today, "obs": obs})
        return obs
    except Exception as e:
        logger.error(f"FRED {series_id} failed: {e}")
        return cached.get("obs", [])


def round_tick(x):
    return round(x / MES_TICK) * MES_TICK


def fmt(x):
    return f"{x:,.2f}"


def ema(values, n):
    k = 2 / (n + 1)
    out, e = [], None
    for v in values:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def rsi(values, n=14):
    if len(values) <= n:
        return None
    gains = [max(values[i] - values[i - 1], 0) for i in range(1, len(values))]
    losses = [max(values[i - 1] - values[i], 0) for i in range(1, len(values))]
    ag, al = sum(gains[:n]) / n, sum(losses[:n]) / n
    for g, l in zip(gains[n:], losses[n:]):
        ag, al = (ag * (n - 1) + g) / n, (al * (n - 1) + l) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def sma(values, n):
    return sum(values[-n:]) / n if len(values) >= n else None


# =====================================================================
# CONTRACT + CALIBRATION
# =====================================================================
def third_friday(y, m):
    d = date(y, m, 15)
    return d + timedelta(days=(4 - d.weekday()) % 7)


def front_month(today=None):
    """Front quarterly MES contract; rolls 8 days before expiry (CME roll week)."""
    today = today or datetime.now(timezone.utc).date()
    codes = {3: "H", 6: "M", 9: "U", 12: "Z"}
    y = today.year
    for _ in range(6):
        for m in (3, 6, 9, 12):
            exp = third_friday(y, m)
            if today < exp - timedelta(days=8):
                return f"MES{codes[m]}{y % 10}", exp
        y += 1
    raise RuntimeError("no front month found")


def calibrate(spy_daily=None, cspx_5m=None):
    """
    CSPX → /MES multiplier. Cached per UTC day in DB key 'mes_calibration'.
    Reuses series passed in by the caller to save Twelve Data credits.
    """
    today = datetime.now(timezone.utc).date()
    cached = db.get_state("mes_calibration") or {}
    if cached.get("date") == today.isoformat():
        return cached

    try:
        spy_daily = spy_daily or td_series("SPY", "1day", 10)
        cspx_5m = cspx_5m or td_series(CSPX, "5min", 500)
        spy_5m = td_series("SPY", "5min", 300)

        spy_close = {r["dt"][:10]: r["close"] for r in spy_daily}
        sp500 = fred_observations("SP500", 10)
        common = [(d, v) for d, v in sp500 if d in spy_close]
        spx_per_spy = common[0][1] / spy_close[common[0][0]]

        c_map = {r["dt"]: r["close"] for r in cspx_5m}
        s_map = {r["dt"]: r["close"] for r in spy_5m}
        overlap = sorted(set(c_map) & set(s_map))
        last_day = overlap[-1][:10]
        cspx_per_spy = statistics.median(c_map[t] / s_map[t] for t in overlap if t.startswith(last_day))

        contract, expiry = front_month(today)
        days = (expiry - today).days
        dtb3 = fred_observations("DTB3", 5)
        r = dtb3[0][1] / 100 if dtb3 else 0.04
        carry = (r - DIV_YIELD) * days / 365

        cal = {
            "date": today.isoformat(), "contract": contract, "expiry": expiry.isoformat(),
            "days_to_expiry": days, "rate": r, "div_yield": DIV_YIELD, "carry": carry,
            "spx_per_spy": spx_per_spy, "cspx_per_spy": cspx_per_spy,
            "spx_per_cspx": spx_per_spy / cspx_per_spy,
            "k": spx_per_spy / cspx_per_spy * (1 + carry),
            "anchor_date": common[0][0], "overlap_day": last_day, "stale": False,
        }
        db.update_state("mes_calibration", cal)
        logger.info(f"MES calibration: {contract} k={cal['k']:.4f} carry={carry*100:.3f}% (r={r:.4f}, {days}d)")
        return cal
    except Exception as e:
        logger.error(f"MES calibration failed, reusing last cached: {e}")
        if cached:
            cached["stale"] = True
            return cached
        raise


def to_mes(cspx_price, cal):
    return round_tick(cspx_price * cal["k"])


def spy_to_mes(spy_price, cal):
    return round_tick(spy_price * cal["spx_per_spy"] * (1 + cal["carry"]))


def basis_pts(cal, spx_level):
    return spx_level * cal["carry"]


# =====================================================================
# SESSION HELPERS
# =====================================================================
def lse_open_utc(d):
    """LSE continuous trading opens 08:00 London time — 07:00 UTC in BST, 08:00 UTC in GMT."""
    return LONDON.localize(datetime(d.year, d.month, d.day, 8, 0)).astimezone(timezone.utc).replace(tzinfo=None)


def utc_dt(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def hst_label(dt_utc):
    return pytz.utc.localize(dt_utc).astimezone(HST).strftime("%H:%M")


def session_bars(cspx_5m, d):
    """Today's LSE bars for UTC date d."""
    return [r for r in cspx_5m if r["dt"][:10] == d.isoformat()]


def night_label(d):
    """The HST evening a UTC session date belongs to (08:00 UTC = previous HST date)."""
    return (datetime(d.year, d.month, d.day, 8, 0) - timedelta(hours=10)).strftime("%a %b %-d")


def opening_range(bars, d):
    start = lse_open_utc(d)
    end = start + timedelta(minutes=OR_MINUTES)
    rng = [b for b in bars if start <= utc_dt(b["dt"]) < end]
    if len(rng) < OR_MINUTES // 5:
        return None
    return {"high": max(b["high"] for b in rng), "low": min(b["low"] for b in rng),
            "start": start, "end": end}


def band_state(mes_closes):
    """4/9 EMA trend state on 5-min closes (MES points)."""
    if len(mes_closes) < 10:
        return {"state": "N/A", "spread": 0.0, "dist": 0.0, "extended": False, "ema9": None}
    e4, e9 = ema(mes_closes, 4)[-1], ema(mes_closes, 9)[-1]
    spread = e4 - e9
    state = "YELLOW" if abs(spread) < YELLOW_BAND_PTS else ("GREEN" if spread > 0 else "RED")
    dist = mes_closes[-1] - e9
    return {"state": state, "spread": spread, "dist": dist,
            "extended": abs(dist) >= EXTENDED_PTS, "ema9": e9}


BAND_ICON = {"GREEN": "🟢 GREEN (uptrend)", "RED": "🔴 RED (downtrend)",
             "YELLOW": "🟡 YELLOW (pullback / possible turn)", "N/A": "⚪ n/a (too few bars)"}


# =====================================================================
# BIAS
# =====================================================================
def compute_bias(spy_daily, spy_4h, london_bars, cal):
    score, lines = 0, []

    closes_4h = [r["close"] for r in spy_4h]
    s50, s200, px = sma(closes_4h, 50), sma(closes_4h, 200), closes_4h[-1] if closes_4h else None
    if s50 and s200:
        if px > s50 > s200:
            score += 2; lines.append("4H: price > SMA50 > SMA200 `+2`")
        elif px < s50 < s200:
            score -= 2; lines.append("4H: price < SMA50 < SMA200 `−2`")
        elif px > s50:
            score += 1; lines.append("4H: above SMA50, stack mixed `+1`")
        else:
            score -= 1; lines.append("4H: below SMA50, stack mixed `−1`")

    closes_d = [r["close"] for r in spy_daily]
    d200 = sma(closes_d, 200)
    if d200:
        if closes_d[-1] > d200:
            score += 1; lines.append("Daily: above SMA200 `+1`")
        else:
            score -= 1; lines.append("Daily: below SMA200 `−1`")

    if spy_daily:
        last = spy_daily[-1]
        chg = (last["close"] - last["open"]) / last["open"] * 100
        pts = (last["close"] - last["open"]) * cal["spx_per_spy"]
        if chg > 0.25:
            score += 1; tag = "`+1`"
        elif chg < -0.25:
            score -= 1; tag = "`−1`"
        else:
            tag = "`0`"
        lines.append(f"Prior NY session ({last['dt'][:10]}): {chg:+.2f}% ({pts:+.1f} pts) {tag}")

    if len(london_bars) >= 3:
        move = to_mes(london_bars[-1]["close"], cal) - to_mes(london_bars[0]["open"], cal)
        if move > 5:
            score += 1; tag = "`+1`"
        elif move < -5:
            score -= 1; tag = "`−1`"
        else:
            tag = "`0`"
        lines.append(f"London so far: {move:+.2f} pts {tag}")

    direction = "BULLISH" if score >= 2 else ("BEARISH" if score <= -2 else "NEUTRAL")
    return {"direction": direction, "score": score, "lines": lines}


# =====================================================================
# TRADE LOG + RISK STATS (strategy_journal, strategy='MES')
# =====================================================================
def _trade_pnl(t, exit_price):
    c = t["confluences"]
    side = 1 if c.get("side") == "LONG" else -1
    contracts = int(c.get("contracts", 1))
    pts = (exit_price - t["entry_price"]) * side
    usd = pts * MES_POINT_VALUE * contracts - FEES_RT * contracts
    return pts, usd


def mes_trades(days_back=400):
    return db.get_journal_entries(strategy="MES", days_back=days_back, limit=1000)


def trade_stats(session_date=None):
    trades = mes_trades()
    closed = [t for t in trades if t["outcome"] not in (None, "OPEN")]
    open_ = [t for t in trades if t["outcome"] in (None, "OPEN")]
    for t in closed:
        try:
            t["pnl_usd"] = json.loads(t["post_mortem"] or "{}").get("pnl_usd", 0.0)
        except Exception:
            t["pnl_usd"] = 0.0

    now = session_date or datetime.now(timezone.utc).date()
    week = now.isocalendar()[:2]
    session = now.isoformat()

    def sess(t):
        return t["confluences"].get("session_utc", t["entry_date"])

    week_pnl = sum(t["pnl_usd"] for t in closed if date.fromisoformat(sess(t)).isocalendar()[:2] == week)
    night = [t for t in closed if sess(t) == session]
    night_losses = sum(1 for t in night if t["pnl_usd"] < 0)
    night_pnl = sum(t["pnl_usd"] for t in night)

    ordered = sorted(closed, key=lambda t: (t["exit_date"] or "", t["id"]))
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for t in ordered:
        equity += t["pnl_usd"]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    wins = sum(1 for t in closed if t["pnl_usd"] > 0)

    ladder_ok = len(closed) >= LADDER_MIN_TRADES and equity > 0 and max_dd <= LADDER_MAX_DD_USD
    return {
        "open": open_, "closed": closed, "night": night, "night_pnl": night_pnl,
        "night_losses": night_losses, "week_pnl": week_pnl, "total_pnl": equity,
        "max_dd": max_dd, "win_rate": wins / len(closed) if closed else None,
        "ladder_ok": ladder_ok,
        "night_stop": night_losses >= NIGHT_MAX_LOSSES or night_pnl <= -NIGHT_MAX_LOSS_USD,
    }


def risk_lines(stats):
    lo, hi = WEEKLY_GOAL
    goal = "✅ goal met" if stats["week_pnl"] >= lo else f"${lo - stats['week_pnl']:,.0f} to go"
    ladder = ("✅ eligible for 2 MES" if stats["ladder_ok"] else
              f"{len(stats['closed'])}/{LADDER_MIN_TRADES} trades · net `${stats['total_pnl']:+,.2f}` · "
              f"max DD `${stats['max_dd']:,.2f}` (≤ ${LADDER_MAX_DD_USD:,.0f})")
    night = "🛑 NIGHTLY STOP HIT — done for tonight" if stats["night_stop"] else (
        f"{stats['night_losses']}/{NIGHT_MAX_LOSSES} losses · `${stats['night_pnl']:+,.2f}` tonight")
    return [
        f"Week: `${stats['week_pnl']:+,.2f}` vs ${lo:.0f}–{hi:.0f} goal ({goal})",
        f"Tonight: {night}",
        f"Ladder to 2 MES: {ladder}",
    ]


# =====================================================================
# DISPATCHES
# =====================================================================
def _send(title, body, color):
    if not WEBHOOK_FUTURES:
        logger.warning("WEBHOOK_FUTURES_TRADING not set — printing instead.")
        print(f"{title}\n{body}")
        return
    send_essentials_embed(WEBHOOK_FUTURES, title, body, color)


def _ledger_row(signal_type, d):
    with db._get_connection() as conn:
        row = conn.execute(
            "SELECT id, predicted_direction, entry_price, outcome, notes FROM signal_ledger "
            "WHERE signal_type=? AND ticker='MES' AND prediction_date=?", (signal_type, d)).fetchone()
    return row


def _ledger_record(signal_type, n=20):
    with db._get_connection() as conn:
        rows = conn.execute(
            "SELECT outcome FROM signal_ledger WHERE signal_type=? AND ticker='MES' "
            "AND outcome IN ('WIN','LOSS') ORDER BY id DESC LIMIT ?", (signal_type, n)).fetchall()
    wins = sum(1 for (o,) in rows if o == "WIN")
    return wins, len(rows)


def run_plan():
    today = datetime.now(timezone.utc).date()
    spy_daily = td_series("SPY", "1day", 260)
    spy_4h = td_series("SPY", "4h", 260)
    cspx_5m = td_series(CSPX, "5min", 500)
    if not spy_daily or not cspx_5m:
        logger.error("MES plan: missing SPY daily or CSPX data — skipping.")
        return
    cal = calibrate(spy_daily=spy_daily, cspx_5m=cspx_5m)
    london = session_bars(cspx_5m, today)
    bias = compute_bias(spy_daily, spy_4h, london, cal)

    prior = spy_daily[-1]
    pdh, pdl, pdc = (spy_to_mes(prior[k], cal) for k in ("high", "low", "close"))
    est = to_mes(london[-1]["close"], cal) if london else pdc
    spx_now = est / (1 + cal["carry"])

    lse_open = lse_open_utc(today)
    if london:
        lon_hi = to_mes(max(b["high"] for b in london), cal)
        lon_lo = to_mes(min(b["low"] for b in london), cal)
        london_line = f"London so far `{fmt(lon_lo)}`–`{fmt(lon_hi)}` ({lon_hi - lon_lo:.2f} pts)"
    else:
        london_line = f"London open {hst_label(lse_open)} HST — live London data not on current plan"
    or_line = (f"London opening range {hst_label(lse_open)}–"
               f"{hst_label(lse_open + timedelta(minutes=OR_MINUTES))} HST · mark it on your chart; "
               f"graded in tomorrow's recap")

    try:
        from cross_asset import get_economic_calendar_alert
        econ = get_economic_calendar_alert()
    except Exception:
        econ = None
    try:
        regime = HighFidelityAnalyticsEngine().classify_vix_regime()
        vix_line = f"VIX regime: {regime['tier']} (VIXY z `{regime['vixy_z']:+.2f}σ`)"
    except Exception:
        vix_line = "VIX regime: unavailable"

    net_target = TARGET_PTS * MES_POINT_VALUE - FEES_RT
    risk_usd = STOP_PTS * MES_POINT_VALUE + FEES_RT
    breakeven = risk_usd / (risk_usd + net_target) * 100
    stats = trade_stats()
    wins, n = _ledger_record("mes_session_bias")
    record = f"Bias record: {wins}/{n} graded" if n else "Bias record: building (graded nightly)"

    exp = date.fromisoformat(cal["expiry"])
    body = (
        f"**Contract:** {cal['contract']} (exp {exp.strftime('%b %-d')}) · est `{fmt(est)}`\n"
        f"*London proxy — absolute ±~5 pts, trade off your chart; distances are exact*\n\n"
        f"**Bias: {bias['direction']} ({bias['score']:+d})**\n"
        + "".join(f"┣ {l}\n" for l in bias["lines"][:-1])
        + (f"┗ {bias['lines'][-1]}\n\n" if bias["lines"] else "\n")
        + f"**Key Levels (MES-equivalent)**\n"
        f"┣ Prior day high `{fmt(pdh)}` · low `{fmt(pdl)}` · close `{fmt(pdc)}`\n"
        f"┣ {london_line}\n"
        f"┗ {or_line}\n\n"
        f"**Risk Plan — 1 MES**\n"
        f"┣ Stop `{STOP_PTS:g}` pts = ${STOP_PTS * MES_POINT_VALUE:,.0f} · Target `{TARGET_PTS:g}` pts = "
        f"${TARGET_PTS * MES_POINT_VALUE:,.0f} (net ≈ ${net_target:,.2f} after fees)\n"
        f"┣ Break-even win rate ≈ {breakeven:.0f}%\n"
        + "".join(f"┣ {l}\n" for l in risk_lines(stats))
        + f"┗ Nightly stop: {NIGHT_MAX_LOSSES} losses or −${NIGHT_MAX_LOSS_USD:,.0f} → done\n\n"
        f"**Context**\n"
        f"┣ {vix_line}\n"
        f"┣ Events: {econ or 'none flagged'}\n"
        f"┗ {record}\n\n"
        f"Basis +{basis_pts(cal, spx_now):.1f} pts (r {cal['rate']*100:.2f}%, q {cal['div_yield']*100:.1f}%, "
        f"{cal['days_to_expiry']}d){' · ⚠️ stale calibration' if cal.get('stale') else ''}"
    )
    color = COLOR_GREEN if bias["direction"] == "BULLISH" else (COLOR_RED if bias["direction"] == "BEARISH" else COLOR_YELLOW)
    _send(f"🌙 /MES SESSION PLAN | {night_label(today)} · 22:00–00:00 HST", body, color)

    db.log_prediction("mes_session_bias", "MES", bias["direction"], est, 0,
                      notes=f"score {bias['score']:+d}; " + "; ".join(bias["lines"]))
    db.update_state(f"mes_plan_{today.isoformat()}", {"bias": bias["direction"], "score": bias["score"],
                                                      "est": est, "pdh": pdh, "pdl": pdl, "pdc": pdc})
    logger.info(f"MES plan dispatched: {bias['direction']} ({bias['score']:+d}), est {est}")


def run_range_alert(d=None):
    today = d or datetime.now(timezone.utc).date()
    cal = calibrate()
    cspx_5m = td_series(CSPX, "5min", 150 if d is None else 500)
    cutoff = datetime(today.year, today.month, today.day, *RANGE_ALERT_UTC)
    bars = [b for b in session_bars(cspx_5m, today) if utc_dt(b["dt"]) < cutoff]
    orng = opening_range(bars, today)
    if not orng:
        logger.info("MES range: London opening range not available (UK holiday or no data) — skipping.")
        return

    plan = db.get_state(f"mes_plan_{today.isoformat()}") or {}
    bias = plan.get("bias", "NEUTRAL")
    hi, lo = to_mes(orng["high"], cal), to_mes(orng["low"], cal)
    mid = round_tick((hi + lo) / 2)
    closes = [to_mes(b["close"], cal) for b in bars]
    last = closes[-1]
    after = [b for b in bars if utc_dt(b["dt"]) >= orng["end"]]
    a_close = [to_mes(b["close"], cal) for b in after]
    a_high = [to_mes(b["high"], cal) for b in after]
    a_low = [to_mes(b["low"], cal) for b in after]

    broke_up = any(c > hi for c in a_close)
    broke_dn = any(c < lo for c in a_close)
    if broke_up and broke_dn:
        state, setup = "CHOP — both sides of the range taken", "Stand down — no clean direction."
    elif broke_up:
        first = next(i for i, c in enumerate(a_close) if c > hi)
        retest = any(l <= hi + 1.0 for l in a_low[first + 1:]) and last > hi
        state = "BREAK ABOVE + RETEST HOLDING" if retest else "BROKE ABOVE — waiting for retest of range high"
        setup = (f"Long on hold of `{fmt(hi)}` · stop `{fmt(hi - STOP_PTS)}` · target `{fmt(hi + TARGET_PTS)}`")
    elif broke_dn:
        first = next(i for i, c in enumerate(a_close) if c < lo)
        retest = any(h >= lo - 1.0 for h in a_high[first + 1:]) and last < lo
        state = "BREAK BELOW + RETEST HOLDING" if retest else "BROKE BELOW — waiting for retest of range low"
        setup = (f"Short on rejection of `{fmt(lo)}` · stop `{fmt(lo + STOP_PTS)}` · target `{fmt(lo - TARGET_PTS)}`")
    else:
        state, setup = "INSIDE RANGE", (f"No trade until a 5-min close outside `{fmt(lo)}`–`{fmt(hi)}`. "
                                        f"Midpoint `{fmt(mid)}` is the magnet.")

    direction = "LONG" if broke_up and not broke_dn else ("SHORT" if broke_dn and not broke_up else None)
    if direction and bias != "NEUTRAL" and ((direction == "LONG") != (bias == "BULLISH")):
        setup += f"\n┣ ⚠️ Against the {bias} plan bias — skip or wait for a stronger tell."

    bands = band_state(closes)
    r = rsi(closes)
    flags = []
    if bands["extended"]:
        flags.append(f"Extended `{bands['dist']:+.1f}` pts from EMA9 — expect a pullback to the bands, don't chase")
    if r is not None and (r >= 70 or r <= 30):
        flags.append(f"RSI14 `{r:.0f}` stretched — no continuation entries")

    body = (
        f"**London Opening Range** ({hst_label(orng['start'])}–{hst_label(orng['end'])} HST)\n"
        f"┣ High `{fmt(hi)}` · Low `{fmt(lo)}` · Mid `{fmt(mid)}` · Size `{hi - lo:.2f}` pts\n"
        f"┗ Now `{fmt(last)}` ({last - mid:+.2f} pts vs mid)\n\n"
        f"**State: {state}**\n"
        f"┣ Trend (4/9 EMA, 5-min): {BAND_ICON[bands['state']]}\n"
        + (f"┣ RSI14: `{r:.0f}`\n" if r is not None else "")
        + "".join(f"┣ {f}\n" for f in flags)
        + f"┗ Plan bias: {bias}\n\n"
        f"**Setup — 1 MES**\n"
        f"┗ {setup}\n\n"
        f"Retest entries over first breaks. Proxy levels ±~5 pts — confirm on your chart."
    )
    color = COLOR_GREEN if direction == "LONG" else (COLOR_RED if direction == "SHORT" else COLOR_YELLOW)
    _send(f"📏 /MES LONDON RANGE | {night_label(today)}", body, color)
    db.update_state(f"mes_range_{today.isoformat()}", {"hi": hi, "lo": lo, "end": orng["end"].isoformat()})
    logger.info(f"MES range alert dispatched: {state}")


def _model_trade(bars, orng, bias, cal):
    """First 5-min close outside the opening range in the bias direction, inside the window.
    Entry at that close, 10-pt stop / 10-pt target. Same-bar stop+target counts as a LOSS."""
    if bias not in ("BULLISH", "BEARISH") or not orng:
        return None
    hi, lo = to_mes(orng["high"], cal), to_mes(orng["low"], cal)
    w0 = utc_dt(bars[0]["dt"]).replace(hour=WINDOW_START_UTC[0], minute=WINDOW_START_UTC[1])
    w1 = w0.replace(hour=WINDOW_END_UTC[0], minute=WINDOW_END_UTC[1])
    scan = [b for b in bars if max(orng["end"], w0) <= utc_dt(b["dt"]) < w1]
    long_ = bias == "BULLISH"
    for i, b in enumerate(scan):
        c = to_mes(b["close"], cal)
        if (long_ and c > hi) or (not long_ and c < lo):
            stop = c - STOP_PTS if long_ else c + STOP_PTS
            tgt = c + TARGET_PTS if long_ else c - TARGET_PTS
            for f in scan[i + 1:]:
                h, l = to_mes(f["high"], cal), to_mes(f["low"], cal)
                hit_stop = l <= stop if long_ else h >= stop
                hit_tgt = h >= tgt if long_ else l <= tgt
                if hit_stop:
                    return {"entry": c, "time": b["dt"], "result": "LOSS", "pts": -STOP_PTS}
                if hit_tgt:
                    return {"entry": c, "time": b["dt"], "result": "WIN", "pts": TARGET_PTS}
            mtm = (to_mes(scan[-1]["close"], cal) - c) * (1 if long_ else -1)
            return {"entry": c, "time": b["dt"], "result": "OPEN", "pts": mtm}
    return {"entry": None, "result": "NO TRIGGER", "pts": 0.0}


def run_recap(d=None):
    today = d or datetime.now(timezone.utc).date()
    cal = calibrate()
    cspx_5m = td_series(CSPX, "5min", 150 if d is None else 500)
    bars = session_bars(cspx_5m, today)
    w0 = datetime(today.year, today.month, today.day, *WINDOW_START_UTC)
    w1 = datetime(today.year, today.month, today.day, *WINDOW_END_UTC)
    window = [b for b in bars if w0 <= utc_dt(b["dt"]) < w1]
    if not window:
        logger.info("MES recap: no window bars (UK holiday or no data) — skipping.")
        return

    o = to_mes(window[0]["open"], cal)
    c = to_mes(window[-1]["close"], cal)
    h = to_mes(max(b["high"] for b in window), cal)
    l = to_mes(min(b["low"] for b in window), cal)
    move = c - o

    plan = db.get_state(f"mes_plan_{today.isoformat()}") or {}
    bias = plan.get("bias", "NEUTRAL")
    row = _ledger_row("mes_session_bias", today.isoformat())
    if bias == "NEUTRAL" or abs(move) < BIAS_MOVE_PTS:
        grade = "NEUTRAL"
    else:
        grade = "WIN" if (move > 0) == (bias == "BULLISH") else "LOSS"
    if row and row[3] == "PENDING":
        db.grade_prediction(row[0], c, grade, {"WIN": 1.0, "LOSS": -1.0}.get(grade, 0.0),
                            notes=f"{row[4]} | window {o:.2f}→{c:.2f} ({move:+.2f} pts)")

    orng = opening_range(bars, today)
    model = _model_trade(bars, orng, bias, cal)
    if model and model["result"] in ("WIN", "LOSS"):
        if db.log_prediction("mes_or_model", "MES", bias, model["entry"], 0,
                             notes=f"entry {model['time']} UTC"):
            mrow = _ledger_row("mes_or_model", today.isoformat())
            db.grade_prediction(mrow[0], model["entry"] + model["pts"] * (1 if bias == "BULLISH" else -1),
                                model["result"], 1.0 if model["result"] == "WIN" else -1.0,
                                notes=f"entry {model['time']} UTC · {model['pts']:+.1f} pts")
    if not model:
        model_line = "No model trade (NEUTRAL bias)"
    elif model["result"] == "NO TRIGGER":
        model_line = "No trigger — no 5-min close outside the range in the bias direction"
    elif model["result"] == "OPEN":
        model_line = f"Entry `{fmt(model['entry'])}` · still open at window end `{model['pts']:+.2f}` pts"
    else:
        model_line = (f"Entry `{fmt(model['entry'])}` → **{model['result']}** `{model['pts']:+.0f}` pts "
                      f"(${model['pts'] * MES_POINT_VALUE:+,.0f} on 1 MES)")

    stats = trade_stats(today)
    night_lines = []
    for t in stats["night"]:
        cf = t["confluences"]
        night_lines.append(f"#{t['id']} {cf.get('side')} `{fmt(t['entry_price'])}`→`{fmt(t['exit_price'] or 0)}` "
                           f"`${t['pnl_usd']:+,.2f}`")
    for t in stats["open"]:
        night_lines.append(f"#{t['id']} {t['confluences'].get('side')} `{fmt(t['entry_price'])}` — ⚠️ still OPEN in log")
    bw, bn = _ledger_record("mes_session_bias")
    mw, mn = _ledger_record("mes_or_model")

    grade_icon = {"WIN": "✅", "LOSS": "❌", "NEUTRAL": "➖"}[grade]
    body = (
        f"**Window 22:00–00:00 HST**\n"
        f"┣ Open `{fmt(o)}` · High `{fmt(h)}` · Low `{fmt(l)}` · Close `{fmt(c)}`\n"
        f"┗ Net `{move:+.2f}` pts · Range `{h - l:.2f}` pts\n\n"
        f"**Plan Check**\n"
        f"┣ Bias {bias} → {grade_icon} {grade}\n"
        f"┗ Model trade (range break, {STOP_PTS:g}/{TARGET_PTS:g} pts): {model_line}\n\n"
        f"**Your Trades Tonight**\n"
        + ("".join(f"┣ {x}\n" for x in night_lines) if night_lines else "┣ none logged\n")
        + "".join(f"┣ {x}\n" for x in risk_lines(stats)[:-1])
        + f"┗ {risk_lines(stats)[-1]}\n\n"
        f"**Track Record (last 20 graded)**\n"
        f"┣ Bias: {bw}/{bn}" + (f" ({bw / bn * 100:.0f}%)" if bn else "") + "\n"
        f"┗ Model: {mw}/{mn}" + (f" ({mw / mn * 100:.0f}%)" if mn else "")
    )
    color = COLOR_GREEN if grade == "WIN" else (COLOR_RED if grade == "LOSS" else COLOR_YELLOW)
    _send(f"🧾 /MES SESSION RECAP | {night_label(today)}", body, color)
    logger.info(f"MES recap dispatched: bias {bias} {grade}, model {model['result'] if model else 'n/a'}")


# =====================================================================
# TRADE LOG CLI
# =====================================================================
def trade_open(side, entry, contracts=1, stop=STOP_PTS, target=TARGET_PTS, note=""):
    side = side.upper()
    sgn = 1 if side == "LONG" else -1
    stop_px, tgt_px = round_tick(entry - sgn * stop), round_tick(entry + sgn * target)
    session = datetime.now(timezone.utc).date().isoformat()
    stats = trade_stats()
    tid = db.log_journal_entry(
        strategy="MES", event_type="TRADE_OPEN", ticker="MES",
        action="BUY" if side == "LONG" else "SELL", conviction=3, thesis=note or f"{side} {contracts} MES",
        confluences={"side": side, "contracts": contracts, "stop_pts": stop, "target_pts": target,
                     "stop_price": stop_px, "target_price": tgt_px, "session_utc": session},
        conflicts={}, entry_price=entry)
    risk = stop * MES_POINT_VALUE * contracts + FEES_RT * contracts
    warn = "\n┣ 🛑 Nightly stop was already hit — this trade breaks your rule." if stats["night_stop"] else ""
    warn += "\n┣ ⚠️ More than 1 contract before the ladder clears." if contracts > 1 and not stats["ladder_ok"] else ""
    body = (f"┣ Entry `{fmt(entry)}` · {contracts} MES{f' · {note}' if note else ''}\n"
            f"┣ Stop `{fmt(stop_px)}` ({stop:g} pts) · Target `{fmt(tgt_px)}` ({target:g} pts)"
            f"{warn}\n"
            f"┗ Risk ${risk:,.2f} incl. fees")
    _send(f"{'🟢' if side == 'LONG' else '🔴'} /MES {side} OPENED | #{tid}", body, COLOR_BLUE)
    print(f"Logged trade #{tid}")


def trade_close(tid, exit_price, note=""):
    t = next((x for x in mes_trades() if x["id"] == tid), None)
    if not t:
        print(f"No MES trade #{tid} found.")
        return
    if t["outcome"] not in (None, "OPEN"):
        print(f"Trade #{tid} already closed ({t['outcome']}).")
        return
    pts, usd = _trade_pnl(t, exit_price)
    outcome = "WIN" if usd > 0 else ("LOSS" if usd < 0 else "NEUTRAL")
    pct = pts / t["entry_price"] * 100
    db.update_journal_outcome(tid, outcome, exit_price=exit_price, pnl_pct=pct,
                              post_mortem=json.dumps({"pnl_usd": round(usd, 2), "pts": pts,
                                                      "fees": FEES_RT * int(t["confluences"].get("contracts", 1)),
                                                      "note": note}))
    stats = trade_stats()
    body = (f"┣ {t['confluences'].get('side')} `{fmt(t['entry_price'])}` → `{fmt(exit_price)}` · `{pts:+.2f}` pts\n"
            f"┣ P&L `${usd:+,.2f}` after est. fees\n"
            + "".join(f"┣ {l}\n" for l in risk_lines(stats)[:-1])
            + f"┗ {risk_lines(stats)[-1]}")
    icon = "✅" if outcome == "WIN" else ("❌" if outcome == "LOSS" else "➖")
    _send(f"{icon} /MES TRADE CLOSED | #{tid} {outcome}", body,
          COLOR_GREEN if outcome == "WIN" else (COLOR_RED if outcome == "LOSS" else COLOR_YELLOW))
    print(f"Closed #{tid}: {pts:+.2f} pts, ${usd:+,.2f}")


def trade_status():
    stats = trade_stats()
    open_lines = [f"#{t['id']} {t['confluences'].get('side')} `{fmt(t['entry_price'])}` · stop "
                  f"`{fmt(t['confluences'].get('stop_price', 0))}` · target `{fmt(t['confluences'].get('target_price', 0))}`"
                  for t in stats["open"]]
    wr = f"{stats['win_rate'] * 100:.0f}%" if stats["win_rate"] is not None else "n/a"
    body = ("**Open**\n" + ("".join(f"┣ {x}\n" for x in open_lines) if open_lines else "┣ none\n")
            + f"\n**Stats**\n┣ Closed trades `{len(stats['closed'])}` · win rate `{wr}` · net `${stats['total_pnl']:+,.2f}`\n"
            + "".join(f"┣ {l}\n" for l in risk_lines(stats)[:-1])
            + f"┗ {risk_lines(stats)[-1]}")
    _send("📒 /MES TRADE LOG", body, COLOR_BLUE)
    print(body)


# =====================================================================
# CLI
# =====================================================================
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="/MES session desk")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan")
    for _n in ("range", "recap"):
        _sp = sub.add_parser(_n)
        _sp.add_argument("--date", type=str, help="UTC session date YYYY-MM-DD (backfill)")
    sub.add_parser("calibrate")
    tp = sub.add_parser("trade")
    tp.add_argument("action", choices=["open", "close", "status"])
    tp.add_argument("--side", choices=["long", "short"])
    tp.add_argument("--entry", type=float)
    tp.add_argument("--exit", type=float)
    tp.add_argument("--id", type=int)
    tp.add_argument("--contracts", type=int, default=1)
    tp.add_argument("--stop", type=float, default=STOP_PTS)
    tp.add_argument("--target", type=float, default=TARGET_PTS)
    tp.add_argument("--note", type=str, default="")
    a = p.parse_args()

    if a.cmd == "plan":
        run_plan()
    elif a.cmd == "range":
        run_range_alert(date.fromisoformat(a.date) if a.date else None)
    elif a.cmd == "recap":
        run_recap(date.fromisoformat(a.date) if a.date else None)
    elif a.cmd == "calibrate":
        print(json.dumps(calibrate(), indent=2))
    elif a.action == "open":
        if not (a.side and a.entry):
            sys.exit("trade open needs --side and --entry")
        trade_open(a.side, a.entry, a.contracts, a.stop, a.target, a.note)
    elif a.action == "close":
        if not (a.id and a.exit):
            sys.exit("trade close needs --id and --exit")
        trade_close(a.id, a.exit, a.note)
    else:
        trade_status()

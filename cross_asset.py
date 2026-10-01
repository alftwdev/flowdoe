import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, date, timedelta, time as dtime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

try:
    from essentials_tools import send_essentials_embed
except ImportError:
    def send_essentials_embed(url, title, desc, color):
        requests.post(url, json={"embeds": [{"title": title, "description": desc, "color": color}]}, timeout=10)

logger = logging.getLogger("Market_Profile_Matrix")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

COLOR_GREEN  = 0x2ecc71
COLOR_YELLOW = 0xf1c40f
COLOR_RED    = 0xe74c3c

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
db = EcosystemDatabase()
engine = HighFidelityAnalyticsEngine()

ET = pytz.timezone("America/New_York")

# Tracked instruments. "native" = real continuous futures symbol attempted first on Twelve Data.
# "proxy" = ETF/index fallback used only if the native futures symbol has no data on the current plan.
FUTURES_BOARD = {
    # Verified live against Twelve Data's actual /commodities catalog (32 entries total) — WTI/USD
    # and XAU/USD are real spot quotes, not guessed symbols. Confirmed Twelve Data carries NO
    # equity index futures (ES/NQ/YM/RTY) and NO natural gas at any tier, including Venture — those
    # four stay honestly proxy-only rather than guessing at futures-style symbols that 404.
    "Crude Oil":   {"native": "WTI/USD", "proxy": "USO",  "futures_label": "/CL"},
    "Natural Gas": {"native": None,      "proxy": "UNG",  "futures_label": "/NG"},
    "Gold":        {"native": "XAU/USD", "proxy": "GLD",  "futures_label": "/GC"},
    "Dow":         {"native": None,      "proxy": "DIA",  "futures_label": "/YM"},
    "S&P 500":     {"native": None,      "proxy": "SPY",  "futures_label": "/ES"},
    "Nasdaq 100":  {"native": None,      "proxy": "QQQ",  "futures_label": "/NQ"},
    "Russell 2000": {"native": None,     "proxy": "IWM", "futures_label": "/RTY"},
}

# Deep-dive market profile is only computed for the two instruments retail futures traders
# care most about intraday: ES and NQ (via SPY/QQQ proxy — no Level 2/Rithmic feed available).
PROFILE_ASSETS = {"SPY": "/ES", "QQQ": "/NQ"}

# ─────────────────────────────────────────────────────────────────────────────
# ECONOMIC CALENDAR — hardcoded 2026 high-vol events (FOMC, CPI, NFP)
# Board flags "TODAY" or "TOMORROW" so traders know to reduce size / expect vol.
# Update annually. Sources: Fed calendar (federalreserve.gov), BLS schedule.
# ─────────────────────────────────────────────────────────────────────────────
ECON_CALENDAR_2026 = {
    # FOMC decision days (second day of two-day meeting)
    "01-29": "FOMC Decision 📣",
    "03-19": "FOMC Decision 📣",
    "05-07": "FOMC Decision 📣",
    "06-18": "FOMC Decision 📣",
    "07-30": "FOMC Decision 📣",
    "09-17": "FOMC Decision 📣",
    "10-29": "FOMC Decision 📣",
    "12-10": "FOMC Decision 📣",
    # NFP (first Friday each month — adjusted for 2026 calendar)
    "01-09": "Jobs Report / NFP 📊",
    "02-06": "Jobs Report / NFP 📊",
    "03-06": "Jobs Report / NFP 📊",
    "04-03": "Jobs Report / NFP 📊",
    "05-01": "Jobs Report / NFP 📊",
    "06-05": "Jobs Report / NFP 📊",
    "07-02": "Jobs Report / NFP 📊",  # Jul 3 holiday — moved
    "08-07": "Jobs Report / NFP 📊",
    "09-04": "Jobs Report / NFP 📊",
    "10-02": "Jobs Report / NFP 📊",
    "11-06": "Jobs Report / NFP 📊",
    "12-04": "Jobs Report / NFP 📊",
    # CPI (BLS release, typically second or third Tuesday/Wednesday mid-month)
    "01-14": "CPI Release 📊",
    "02-11": "CPI Release 📊",
    "03-11": "CPI Release 📊",
    "04-14": "CPI Release 📊",
    "05-13": "CPI Release 📊",
    "06-11": "CPI Release 📊",
    "07-14": "CPI Release 📊",
    "08-12": "CPI Release 📊",
    "09-11": "CPI Release 📊",
    "10-14": "CPI Release 📊",
    "11-12": "CPI Release 📊",
    "12-11": "CPI Release 📊",
}


def get_economic_calendar_alert():
    """
    Returns a one-line alert string if today or tomorrow has a scheduled high-vol event,
    None otherwise. Futures traders use this to pre-size positions before the print.
    """
    today_key    = date.today().strftime("%m-%d")
    tomorrow_key = (date.today() + timedelta(days=1)).strftime("%m-%d")
    if today_key in ECON_CALENDAR_2026:
        return f"TODAY: {ECON_CALENDAR_2026[today_key]} — reduce size, expect vol"
    if tomorrow_key in ECON_CALENDAR_2026:
        return f"TOMORROW: {ECON_CALENDAR_2026[tomorrow_key]} — prep overnight position"
    return None


def fetch_daily_levels(symbols):
    """
    Fetches PDH / PDL / PDC (previous-day high, low, close) for a list of ETF symbols
    using 1-day bars. Called once per board run for SPY and QQQ so the board can show
    whether price is above/below yesterday's range — the most common level futures traders
    reference at the open and during RTH.
    """
    levels = {}
    for sym in symbols:
        try:
            r = requests.get(
                "https://api.twelvedata.com/time_series",
                params={"symbol": sym, "interval": "1day", "outputsize": 3, "apikey": TD_API_KEY},
                timeout=12,
            ).json()
            vals = r.get("values", [])
            if len(vals) >= 2:
                prev = vals[1]  # index 0 = today (partial), index 1 = yesterday (complete)
                levels[sym] = {
                    "pdh": float(prev["high"]),
                    "pdl": float(prev["low"]),
                    "pdc": float(prev["close"]),
                }
        except Exception as e:
            logger.warning(f"Daily levels fetch failed for {sym}: {e}")
    return levels

# =====================================================================
# SESSION HELPERS — futures trade ~23h/day, RTH-only gating hides the edge
# =====================================================================
def get_session_label(now_et=None):
    """Globex overnight session (18:00-09:30 ET) vs RTH (09:30-16:00 ET) vs maintenance break."""
    now_et = now_et or datetime.now(ET)
    t = now_et.time()
    if dtime(9, 30) <= t <= dtime(16, 0):
        return "RTH"
    if t >= dtime(18, 0) or t < dtime(9, 30):
        return "OVERNIGHT"
    return "MAINTENANCE"  # 16:00-18:00 ET daily settlement break

# =====================================================================
# DATA FETCH
# =====================================================================
def fetch_profile_time_series(symbol, outputsize=190):
    """Pulls 5-min bars covering both the prior overnight session and today's RTH."""
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=5min&outputsize={outputsize}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=12).json()
        if "values" not in res:
            return None
        df = pd.DataFrame(res["values"])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime_est'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
        df['close'] = df['close'].astype(float)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(int)
        return df[::-1].reset_index(drop=True)
    except Exception as e:
        logger.error(f"Failed to fetch profile series data for {symbol}: {e}")
        return None

def fetch_board_quotes():
    """Tries the real spot symbol first (where Twelve Data actually carries one), falls back to ETF proxy."""
    out = {}
    for label, cfg in FUTURES_BOARD.items():
        quote = None
        attempts = [(cfg["proxy"], "PROXY")] if cfg["native"] is None else [(cfg["native"], "LIVE"), (cfg["proxy"], "PROXY")]
        for symbol, mode in attempts:
            try:
                r = requests.get(f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
                if r and "close" in r:
                    quote = {
                        "mode": mode,
                        "label": cfg["futures_label"],
                        "last": float(r["close"]),
                        "change": float(r.get("change", 0.0)),
                        "percent_change": float(r.get("percent_change", 0.0)),
                        "proxy_symbol": symbol,
                    }
                    break
            except Exception as e:
                logger.error(f"Board fetch failed for {symbol}: {e}")
        if quote:
            out[label] = quote
    return out

# =====================================================================
# MARKET PROFILE / VWAP / CVD
# =====================================================================
def compute_market_profile_nodes(df):
    """ORIGINAL 70% VALUE AREA CALCULATION — unchanged math."""
    price_profile = df.groupby('close')['volume'].sum().sort_index()
    poc_price = float(price_profile.idxmax())

    total_volume = price_profile.sum()
    value_area_target = total_volume * 0.70

    prices = price_profile.index.tolist()
    poc_index = prices.index(poc_price)

    left, right = poc_index, poc_index
    current_va_volume = price_profile.iloc[poc_index]

    while current_va_volume < value_area_target:
        vol_left = price_profile.iloc[left - 1] if left > 0 else 0
        vol_right = price_profile.iloc[right + 1] if right < len(prices) - 1 else 0

        if vol_left >= vol_right and left > 0:
            left -= 1
            current_va_volume += vol_left
        elif vol_right > vol_left and right < len(prices) - 1:
            right += 1
            current_va_volume += vol_right
        else:
            break

    return {"poc": poc_price, "vah": float(prices[right]), "val": float(prices[left])}

def split_sessions(df):
    """Splits a 5-min dataframe into the most recent overnight (Globex) session and today's RTH."""
    df['date'] = df['datetime_est'].dt.date
    df['time'] = df['datetime_est'].dt.time
    today = df['date'].max()

    rth_mask = (df['date'] == today) & (df['time'] >= dtime(9, 30)) & (df['time'] <= dtime(16, 0))
    overnight_mask = (df['time'] >= dtime(18, 0)) | (df['time'] < dtime(9, 30))
    overnight_mask &= ~rth_mask

    rth_df = df[rth_mask].copy()
    overnight_df = df[overnight_mask].copy()
    return rth_df, overnight_df

# =====================================================================
# FUTURES BOARD — condensed pulse with directional context
# =====================================================================

# Index futures shown first (most relevant to equity traders sizing positions),
# commodities second as macro context for the session.
INDEX_LABELS     = {"S&P 500", "Nasdaq 100", "Dow", "Russell 2000"}
COMMODITY_LABELS = {"Crude Oil", "Gold", "Natural Gas"}


def build_board_payload(board, session_label, vix_regime=None, econ_alert=None, daily_levels=None, fred_macro=None):
    """
    Compact futures board: price + % change + PDH/PDL context for index futures.
    Divergence and VIX tier appended as actionable signal lines.
    No verbose proxy labels — the futures label (/ES, /NQ etc.) is identifier enough.
    """
    daily_levels = daily_levels or {}
    index_rows, commodity_rows = [], []

    for label, q in board.items():
        pct = q["percent_change"]
        # Dead-band: ±0.05% treated as flat to avoid 🔴▼ on noise (-0.0% prints)
        if abs(pct) < 0.05:
            arrow, color = "—", "⚪"
        else:
            arrow = "▲" if pct > 0 else "▼"
            color = "🟢" if pct > 0 else "🔴"

        # PDH/PDL context for index proxies only — adds "Above PDH" / "Below PDL" / "Inside range"
        # Minimum 0.1% buffer before calling a breakout to filter noise-level price deviations.
        ctx = ""
        sym = q["proxy_symbol"]
        if sym in daily_levels and label in INDEX_LABELS:
            pdh = daily_levels[sym]["pdh"]
            pdl = daily_levels[sym]["pdl"]
            spot = q["last"]
            buf = spot * 0.001   # 0.1% of spot price
            if spot > pdh + buf:
                ctx = f" | Above PDH {pdh:,.2f} ✅"
            elif spot < pdl - buf:
                ctx = f" | Below PDL {pdl:,.2f} 🔴"
            else:
                ctx = f" | Inside range ({pdl:,.2f}–{pdh:,.2f})"

        row = f"┣ {q['label']}: {q['last']:,.2f} {color}{arrow} {pct:+.1f}%{ctx}"
        (index_rows if label in INDEX_LABELS else commodity_rows).append(row)

    # ── ES vs NQ divergence (the single most-watched intermarket relationship in E-mini)
    es_q  = board.get("S&P 500")
    nq_q  = board.get("Nasdaq 100")
    ym_q  = board.get("Dow")
    rty_q = board.get("Russell 2000")
    divergence_line = ""
    if es_q and nq_q:
        div = es_q["percent_change"] - nq_q["percent_change"]
        if abs(div) >= 0.5:
            if div > 0:
                divergence_line = f"┣ Divergence: /ES {es_q['percent_change']:+.1f}% vs /NQ {nq_q['percent_change']:+.1f}% — tech lagging, selective ⚠️\n"
            else:
                divergence_line = f"┣ Divergence: /NQ {nq_q['percent_change']:+.1f}% vs /ES {es_q['percent_change']:+.1f}% — QQQ leading, rotation ⚠️\n"

    # ── VIX regime (drives position sizing)
    vix_line = ""
    if vix_regime:
        z    = vix_regime.get("vixy_z", 0.0)
        tier = vix_regime.get("tier", "NORMAL")
        if tier == "NORMAL":
            vix_line = f"┣ VIX: calm ({z:+.1f}σ) — full size OK\n"
        elif tier == "ELEVATED":
            vix_line = f"┣ VIX: elevated ({z:+.1f}σ) — reduce size 50% ⚠️\n"
        else:
            vix_line = f"┣ VIX: SPIKE ({z:+.1f}σ) — defensive posture 🔴\n"

    # ── Economic calendar alert
    econ_line = f"┣ 📅 {econ_alert}\n" if econ_alert else ""

    # ── FRED macro context — yield curve + Fed Funds (once per day, no extra API cost on cache hit)
    fred_macro_line = ""
    if fred_macro:
        yc = fred_macro.get("yield_curve")
        ff = fred_macro.get("fedfunds")
        if yc:
            spread_str = f"{yc['spread']:+.2f}%"
            yc_label = yc.get("label", "")
            fred_macro_line += f"┣ Yield Curve (T10-T2): {spread_str} — {yc_label}\n"
            t30 = yc.get("t30", 0.0)
            if t30 >= 5.0:
                t30_flag = yc.get("t30_cef_flag", "")
                fred_macro_line += f"┣ 30-yr Treasury: {t30:.2f}% {t30_flag} — CEF premium headwind\n"
            elif t30 > 0:
                fred_macro_line += f"┣ 30-yr Treasury: {t30:.2f}% ✅ BENIGN\n"
        if ff:
            fred_macro_line += f"┣ Fed Funds: {ff:.2f}% [FRED]\n"

    # ── Session bias from index breadth
    # Use dead-banded pct (same ±0.05% threshold as arrow/color) so near-flat instruments
    # don't count as "bulls" and skew the session bias verdict.
    index_pcts = [q["percent_change"] for q in [es_q, nq_q, ym_q, rty_q] if q]
    bulls = sum(1 for p in index_pcts if p > 0.05)
    bears = sum(1 for p in index_pcts if p < -0.05)
    if bulls == len(index_pcts):
        bias = "All indices green — broad risk-on"
    elif bears == len(index_pcts):
        bias = "All indices red — broad risk-off"
    elif bulls >= 3:
        bias = "Broad strength — watch lagging index for rotation"
    elif bears >= 3:
        bias = "Broad weakness — only isolated green pockets"
    else:
        bias = "Mixed — wait for /ES value area confirmation"

    # ── Market breadth (% stocks above 50D SMA) — from tqqq_breadth_cache in DB, zero API cost
    breadth_line = ""
    try:
        _breadth = db.get_state("tqqq_breadth_cache")
        if _breadth is not None:
            _b_pct = float(_breadth) * 100
            _b_icon = "🟢" if _b_pct >= 60 else ("🔴" if _b_pct <= 35 else "🟡")
            breadth_line = f"┣ Breadth (% above 50D SMA): {_b_icon} `{_b_pct:.0f}%`\n"
    except Exception:
        pass

    rows_text = "\n".join(index_rows + commodity_rows)
    return (
        f"{rows_text}\n"
        f"{vix_line}"
        f"{divergence_line}"
        f"{breadth_line}"
        f"{econ_line}"
        f"{fred_macro_line}"
        f"┗ Bias: {bias}"
    )

BOARD_MIN_CHANGE_PCT = 0.05   # composite % move across the board required to re-dispatch
BOARD_HEARTBEAT_HOURS = 4     # dispatch anyway after this long even if nothing moved, so the
                              # channel doesn't go fully dark — confirms the feed is still alive

def _fetch_fred_board_macro() -> dict:
    """
    Yield curve (T10-T2) and Fed Funds rate from FRED — one call per series per day.
    Returns {"yield_curve": dict|None, "fedfunds": float|None}.
    Uses engine's existing cached helpers — zero extra FRED calls if fed.py or
    analytics already fetched today.
    """
    result = {"yield_curve": None, "fedfunds": None}
    if not engine.fred_api_key:
        return result
    try:
        result["yield_curve"] = engine.fetch_yield_curve()
        # Write spread + t30 to DB so monitor.py can detect rapid steepening and long-rate pressure.
        if result["yield_curve"]:
            today_str = datetime.now().strftime("%Y-%m-%d")
            if db.get_state("fred_yield_spread_date") != today_str:
                prev_spread = db.get_state("fred_yield_spread")
                if prev_spread is not None:
                    db.update_state("fred_yield_spread_prev", prev_spread)
                db.update_state("fred_yield_spread",      result["yield_curve"]["spread"])
                db.update_state("fred_yield_spread_date", today_str)
            # Always write full yield curve dict (includes t30) — monitor.py reads fred_yield_curve_data
            db.update_state("fred_yield_curve_data", result["yield_curve"])
    except Exception as e:
        logger.warning(f"FRED yield curve fetch failed: {e}")
    try:
        cache_key_ff = "fred_fedfunds_value"
        cache_date_ff = "fred_fedfunds_date"
        today_str = datetime.now().strftime("%Y-%m-%d")
        if db.get_state(cache_date_ff) == today_str:
            cached = db.get_state(cache_key_ff)
            if cached:
                result["fedfunds"] = float(cached)
        else:
            val = engine._fetch_fred_metric("FEDFUNDS")
            if val and val > 0:
                result["fedfunds"] = round(val, 2)
                db.update_state(cache_key_ff, val)
                db.update_state(cache_date_ff, today_str)
    except Exception as e:
        logger.warning(f"FRED Fed Funds fetch failed: {e}")
    return result


def run_futures_board():
    """
    Change-gated futures board with directional context.
    Only re-dispatches on a real composite move or after BOARD_HEARTBEAT_HOURS of silence.
    Augments the bare price table with: PDH/PDL context, ES/NQ divergence, VIX tier,
    economic calendar alert, and session bias verdict.
    """
    if not WEBHOOK_FUTURES:
        return
    board = fetch_board_quotes()
    if not board:
        return

    last_board        = db.get_state("futures_board_last_quotes", {})
    last_dispatch_iso = db.get_state("futures_board_last_dispatch", "")
    composite_change  = sum(
        abs(q["percent_change"] - last_board.get(label, {}).get("percent_change", 0.0))
        for label, q in board.items()
    )

    heartbeat_due = True
    if last_dispatch_iso:
        try:
            hours_since = (datetime.now() - datetime.fromisoformat(last_dispatch_iso)).total_seconds() / 3600.0
            heartbeat_due = hours_since >= BOARD_HEARTBEAT_HOURS
        except Exception:
            heartbeat_due = True

    if last_board and composite_change < BOARD_MIN_CHANGE_PCT and not heartbeat_due:
        logger.info(f"Futures board unchanged (composite Δ {composite_change:.3f}%) — suppressing repeat dispatch.")
        return

    # Enrich board with context signals (one-time fetch per board run)
    session_label = get_session_label()
    vix_regime    = engine.classify_vix_regime()
    econ_alert    = get_economic_calendar_alert()
    daily_levels  = fetch_daily_levels(["SPY", "QQQ", "DIA", "IWM"])

    # FRED macro context — yield curve + Fed Funds, cached daily via engine methods.
    # fetch_yield_curve() and _fetch_fred_metric() each return quickly on cache hit;
    # on a cache miss they make one FRED call each (two total per calendar day max).
    fred_macro = _fetch_fred_board_macro()

    payload = build_board_payload(board, session_label, vix_regime=vix_regime,
                                  econ_alert=econ_alert, daily_levels=daily_levels,
                                  fred_macro=fred_macro)
    _es_chg  = board.get("S&P 500",    {}).get("percent_change", 0.0)
    _nq_chg  = board.get("Nasdaq 100", {}).get("percent_change", 0.0)
    _idx_chg = (_es_chg + _nq_chg) / 2.0
    _board_color = COLOR_GREEN if _idx_chg > 0.1 else (COLOR_RED if _idx_chg < -0.1 else COLOR_YELLOW)
    send_essentials_embed(WEBHOOK_FUTURES, "FUTURES BOARD", payload, _board_color)
    db.update_state("futures_board_last_quotes", board)
    db.update_state("futures_board_last_dispatch", datetime.now().isoformat())

    # ── Write NQ directional bias to DB for scorecard cross-reference ──────────
    # futures_nq_bias_{date}      = morning directional call (written once — first run of day)
    # futures_nq_actual_dir_{date} = latest actual direction (overwritten every run; EOD = final)
    # Consumers: analytics.generate_announcements_teaser() + generate_ecosystem_scorecard()
    _today_str = datetime.now().strftime("%Y-%m-%d")
    _nq_dir = "BULLISH" if _nq_chg > 0.05 else ("BEARISH" if _nq_chg < -0.05 else "NEUTRAL")
    _bias_key = f"futures_nq_bias_{_today_str}"
    if not db.get_state(_bias_key):   # only set once — morning call is the prediction
        db.update_state(_bias_key, _nq_dir)
    db.update_state(f"futures_nq_actual_dir_{_today_str}", _nq_dir)  # always update — EOD = final

    logger.info(f"Dispatched Futures Board ({session_label}, composite Δ {composite_change:.3f}%, heartbeat={heartbeat_due}, NQ={_nq_dir})")

# =====================================================================
# MARKET PROFILE DB WRITE (SPY/QQQ) — feeds market_analysis.py, no dispatch
# =====================================================================
def run_intraday_futures_update():
    """
    DB-only: writes SPY/QQQ POC/VAH/VAL/VWAP for market_analysis.py's Overnight Market
    Structure section. No Discord dispatch — the FUTURES FLOWSTATE charts and the
    FUTURES → EQUITIES SIGNAL SYNC embed were removed Sept 30 2026 (channel cleanup).
    """
    session_label = get_session_label()
    if session_label == "MAINTENANCE":
        logger.info("Daily settlement break (16:00-18:00 ET) — skipping profile DB write.")
        return

    for sym, label in PROFILE_ASSETS.items():
        df = fetch_profile_time_series(sym)
        if df is None or df.empty:
            continue

        rth_df, overnight_df = split_sessions(df)
        active_df = rth_df if (session_label == "RTH" and not rth_df.empty) else overnight_df
        if active_df.empty:
            active_df = df

        profile = compute_market_profile_nodes(active_df)
        # Degenerate value area (VAH == VAL) on thin data — skip rather than write a zero-width zone.
        if profile["vah"] == profile["val"]:
            logger.info(f"{label}: degenerate value area — skipping this sweep.")
            continue
        active_df = active_df.copy()
        active_df['pv'] = active_df['close'] * active_df['volume']
        vwap = active_df['pv'].sum() / active_df['volume'].sum()

        db.update_state(f"{sym}_poc", profile["poc"])
        db.update_state(f"{sym}_vwap", vwap)
        db.update_state(f"{sym}_vah", profile["vah"])
        db.update_state(f"{sym}_val", profile["val"])
        db.update_state(f"{sym}_session", session_label)
        logger.info(f"{label}: profile written to DB (POC {profile['poc']:.2f}, VWAP {vwap:.2f})")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "board":
        run_futures_board()
    elif mode == "profile":
        run_intraday_futures_update()
    else:
        # Default cron invocation: change-gated board + DB-only profile write
        # (SPY/QQQ POC/VAH/VAL/VWAP for market_analysis.py). The IB breakout scanner
        # was removed Sept 30 2026 — it only ran at 14:45 ET, 4h after the IB sealed.
        # Session-timed /MES alerts now live in mes_desk.py.
        run_futures_board()
        run_intraday_futures_update()

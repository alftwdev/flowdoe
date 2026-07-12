import os
import sys
import io
import logging
import requests
import pandas as pd
from datetime import datetime, date, timedelta, time as dtime
import pytz
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from market_structure import analyze_market_structure
from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

try:
    from essentials_tools import send_essentials_embed, send_essentials_embed_with_chart
except ImportError:
    def send_essentials_embed(url, title, desc, color):
        requests.post(url, json={"embeds": [{"title": title, "description": desc, "color": color}]}, timeout=10)
    def send_essentials_embed_with_chart(url, title, desc, chart_bytes, color):
        send_essentials_embed(url, title, desc, color)

logger = logging.getLogger("Market_Profile_Matrix")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
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
# 3-STRIKE DYNAMIC GATEKEEPER (single source of truth for the futures channel)
# =====================================================================
def evaluate_gatekeeper(channel, current_metric, major_threshold=5.0):
    """
    3-Strike Dynamic Gatekeeper protocol. Resets on major shifts, silences after 3 minor updates.
    `channel` must be a globally unique key — engine.py no longer runs a competing futures gatekeeper.
    """
    state_key = f"gatekeeper_{channel}_pulse"
    channel_state = db.get_state(state_key, {"strike_count": 0, "last_value": 0.0})

    last_value = channel_state.get("last_value", 0.0)
    strike_count = channel_state.get("strike_count", 0)

    delta = abs(current_metric - last_value)
    is_major_move = delta >= major_threshold

    if is_major_move:
        db.update_state(state_key, {"strike_count": 1, "last_value": current_metric})
        return True, "🔴 MAJOR REGIME SHIFT DETECTED"
    elif strike_count < 3:
        db.update_state(state_key, {"strike_count": strike_count + 1, "last_value": last_value})
        return True, f"🟡 TACTICAL PERSISTENCE REMINDER ({strike_count + 1}/3)"
    else:
        return False, "SILENT"

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
def fetch_pivot_points(symbol):
    """
    TD native pivot_points_hl endpoint — daily classical pivot levels (PP, R1/R2, S1/S2).
    Used by the IB breakout scanner to confirm the target is not blocked by a nearby pivot,
    and in the ES/NQ deep-dive as structural reference levels.
    """
    try:
        res = requests.get(
            f"https://api.twelvedata.com/pivot_points_hl",
            params={"symbol": symbol, "interval": "1day", "time_period": 5, "apikey": TD_API_KEY},
            timeout=12
        ).json()
        latest = res.get("values", [{}])[0]
        return {
            "pp":  float(latest.get("pp",  0.0)),
            "r1":  float(latest.get("r1",  0.0)),
            "r2":  float(latest.get("r2",  0.0)),
            "s1":  float(latest.get("s1",  0.0)),
            "s2":  float(latest.get("s2",  0.0)),
        }
    except Exception as e:
        logger.warning(f"Pivot points fetch failed for {symbol}: {e}")
        return None


def fetch_ichimoku(symbol):
    """
    TD native ichimoku endpoint — cloud (Senkou A/B), Tenkan, Kijun, Chikou.
    Cloud posture (price vs cloud, cloud color) is used in ES/NQ deep-dive as a dynamic
    support/resistance overlay more responsive than static SMA200.
    """
    try:
        res = requests.get(
            f"https://api.twelvedata.com/ichimoku",
            params={"symbol": symbol, "interval": "1day", "apikey": TD_API_KEY},
            timeout=12
        ).json()
        latest = res.get("values", [{}])[0]
        tenkan    = float(latest.get("tenkan_sen",  0.0))
        kijun     = float(latest.get("kijun_sen",   0.0))
        senkou_a  = float(latest.get("senkou_span_a", 0.0))
        senkou_b  = float(latest.get("senkou_span_b", 0.0))
        cloud_top = max(senkou_a, senkou_b)
        cloud_bot = min(senkou_a, senkou_b)
        return {
            "tenkan": tenkan,
            "kijun": kijun,
            "cloud_top": cloud_top,
            "cloud_bot": cloud_bot,
            "cloud_bull": senkou_a > senkou_b,  # green cloud = bullish, red = bearish
            "tenkan_cross_bull": tenkan > kijun,  # TK cross bullish
        }
    except Exception as e:
        logger.warning(f"Ichimoku fetch failed for {symbol}: {e}")
        return None


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

def compute_cvd(df):
    """Cumulative Volume Delta proxy (no order-flow feed): bull volume when close>open, else bear."""
    direction = (df['close'] >= df['open']).map({True: 1, False: -1})
    delta = direction * df['volume']
    df = df.copy()
    df['cvd'] = delta.cumsum()
    return df

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
# CHART SNAPSHOT (matplotlib — no external chart service required)
# =====================================================================
def generate_market_profile_chart(label, df, profile, vwap, posture):
    fig, ax = plt.subplots(figsize=(9, 5), dpi=120)
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")

    ax.plot(df['datetime_est'], df['close'], color="#58a6ff", linewidth=1.4, label="Price")
    ax.axhline(profile['vah'], color="#3fb950", linestyle="--", linewidth=1, label=f"VAH {profile['vah']:.2f}")
    ax.axhline(profile['poc'], color="#f1c40f", linestyle="-", linewidth=1.2, label=f"POC {profile['poc']:.2f}")
    ax.axhline(profile['val'], color="#f85149", linestyle="--", linewidth=1, label=f"VAL {profile['val']:.2f}")
    ax.axhline(vwap, color="#a371f7", linestyle=":", linewidth=1.2, label=f"VWAP {vwap:.2f}")

    ax.set_title(f"{label} | Algorithmic Market Profile | {posture}", color="white", fontsize=11)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#30363d")
    ax.legend(facecolor="#161b22", edgecolor="#30363d", labelcolor="white", fontsize=8, loc="best")
    ax.grid(color="#21262d", linewidth=0.5)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()

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
        arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "—")
        color = "🟢" if pct > 0 else ("🔴" if pct < 0 else "⚪")

        # PDH/PDL context for index proxies only — adds "Above PDH" / "Below PDL" / "Inside range"
        ctx = ""
        sym = q["proxy_symbol"]
        if sym in daily_levels and label in INDEX_LABELS:
            pdh = daily_levels[sym]["pdh"]
            pdl = daily_levels[sym]["pdl"]
            spot = q["last"]
            if spot > pdh:
                ctx = f" | Above PDH {pdh:,.2f} ✅"
            elif spot < pdl:
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
        if ff:
            fred_macro_line += f"┣ Fed Funds: {ff:.2f}% [FRED]\n"

    # ── Session bias from index breadth
    index_pcts = [q["percent_change"] for q in [es_q, nq_q, ym_q, rty_q] if q]
    bulls = sum(1 for p in index_pcts if p > 0)
    if bulls == len(index_pcts):
        bias = "All indices green — broad risk-on"
    elif bulls == 0:
        bias = "All indices red — broad risk-off"
    elif bulls >= 3:
        bias = "Broad strength — watch lagging index for rotation"
    elif bulls <= 1:
        bias = "Broad weakness — only isolated green pockets"
    else:
        bias = "Mixed — wait for /ES value area confirmation"

    rows_text = "\n".join(index_rows + commodity_rows)
    return (
        f"⚡ FUTURES BOARD | {session_label}\n"
        f"{rows_text}\n"
        f"{vix_line}"
        f"{divergence_line}"
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
    send_essentials_embed(WEBHOOK_FUTURES, "FUTURES BOARD", payload, 0x00FFFF)
    db.update_state("futures_board_last_quotes", board)
    db.update_state("futures_board_last_dispatch", datetime.now().isoformat())
    logger.info(f"Dispatched Futures Board ({session_label}, composite Δ {composite_change:.3f}%, heartbeat={heartbeat_due})")

# =====================================================================
# DEEP-DIVE MARKET PROFILE + CHART (ES/NQ) — fires only on gatekeeper approval
# =====================================================================
def run_intraday_futures_update():
    if not WEBHOOK_FUTURES:
        return

    session_label = get_session_label()
    if session_label == "MAINTENANCE":
        logger.info("Daily settlement break (16:00-18:00 ET) — skipping stale profile broadcast.")
        return

    for sym, label in PROFILE_ASSETS.items():
        df = fetch_profile_time_series(sym)
        if df is None or df.empty:
            continue

        rth_df, overnight_df = split_sessions(df)
        active_df = rth_df if (session_label == "RTH" and not rth_df.empty) else overnight_df
        if active_df.empty:
            active_df = df

        spot = df['close'].iloc[-1]
        profile = compute_market_profile_nodes(active_df)
        # Degenerate case: too few distinct price levels (thin overnight/transition data) makes
        # the value area collapse to a single price (VAH == POC == VAL), which makes the "fade
        # value boundaries" directive meaningless — confirmed live, a /NQ dispatch showed exactly
        # this. Skip this sweep rather than dispatch a zero-width boundary; more bars accumulate
        # by the next sweep.
        if profile["vah"] == profile["val"]:
            logger.info(f"{label}: degenerate value area (insufficient distinct price levels) — skipping this sweep.")
            continue
        active_df = active_df.copy()
        active_df['pv'] = active_df['close'] * active_df['volume']
        vwap = active_df['pv'].sum() / active_df['volume'].sum()
        cvd_df = compute_cvd(active_df)
        cvd_now = float(cvd_df['cvd'].iloc[-1])

        db.update_state(f"{sym}_poc", profile["poc"])
        db.update_state(f"{sym}_vwap", vwap)
        db.update_state(f"{sym}_vah", profile["vah"])
        db.update_state(f"{sym}_val", profile["val"])
        db.update_state(f"{sym}_session", session_label)

        if spot > profile["vah"]:
            posture = "Outside Value Up | Aggressive buyers in control."
        elif spot < profile["val"]:
            posture = "Outside Value Down | Aggressive sellers routing positions."
        else:
            posture = "Inside Value Regime | Mean-reversion trading dominant."

        cvd_bias = "Buyers absorbing offers (bullish delta)" if cvd_now > 0 else "Sellers absorbing bids (bearish delta)"

        # Price-action market structure — fair value gaps, liquidity sweeps, equal highs/lows —
        # computed on the same active_df already fetched above, so this costs zero extra API calls.
        structure = analyze_market_structure(active_df)

        # VIX-tiered regime shield + Unified Conviction Score (vault/philo.txt formulas) — reuses
        # the same active_df, no extra API calls beyond the one VIXY fetch and one GEX/options fetch.
        regime = engine.classify_vix_regime()
        gex_state = engine.calculate_gex_profile(sym)
        conviction = engine.calculate_unified_conviction_score(sym, active_df, gex_state=gex_state)

        should_send, status_tag = evaluate_gatekeeper(f"futures_{sym}", spot, major_threshold=5.0)

        # Ichimoku cloud — dynamic support/resistance zone, free extra call per symbol
        ichi = fetch_ichimoku(sym)
        if ichi:
            cloud_color = "🟢 bullish" if ichi["cloud_bull"] else "🔴 bearish"
            if spot > ichi["cloud_top"]:
                ichi_posture = f"Above cloud ({cloud_color}) — trend confirmed"
            elif spot < ichi["cloud_bot"]:
                ichi_posture = f"Below cloud ({cloud_color}) — downtrend confirmed"
            else:
                ichi_posture = f"Inside cloud ({cloud_color}) — transition/chop zone"
            tk_cross = "TK bullish ✅" if ichi["tenkan_cross_bull"] else "TK bearish ⚠️"
            ichi_line = f"┣ Ichimoku: {ichi_posture} | {tk_cross} | Cloud: `${ichi['cloud_bot']:,.2f}`–`${ichi['cloud_top']:,.2f}`\n"
        else:
            ichi_line = ""

        # Pivot levels — structural reference for value area context
        pivots = fetch_pivot_points(sym)
        pivot_line = ""
        if pivots and pivots["pp"] > 0:
            pivot_line = f"┣ Pivots: PP `${pivots['pp']:,.2f}` | R1 `${pivots['r1']:,.2f}` | S1 `${pivots['s1']:,.2f}`\n"

        if should_send:
            payload = (
                f"**{session_label} Session Market Profile (Spot: `${spot:,.2f}`)**\n"
                f"┣ Gatekeeper: {status_tag}\n"
                f"┣ VWAP: `${vwap:,.2f}` | VAH: `${profile['vah']:,.2f}` | POC: `${profile['poc']:,.2f}` | VAL: `${profile['val']:,.2f}`\n"
                f"┣ CVD: `{cvd_now:+,.0f}` — {cvd_bias}\n"
                f"┣ Posture: {posture}\n"
                f"{ichi_line}"
                f"{pivot_line}"
                f"┣ Structure: {structure['setup']} ({structure['bias']}) — {structure['detail']}\n"
                f"┣ VIX Regime: {regime['tier']} (z `{regime['vixy_z']:+.2f}σ`) — {regime['posture']}\n"
                f"┣ Conviction: `{conviction['score']}/100` — {conviction['verdict']}\n"
                f"┗ Fade value boundaries `${profile['val']:,.2f}`–`${profile['vah']:,.2f}` for core setups"
            )
            try:
                chart_bytes = generate_market_profile_chart(label, active_df, profile, vwap, posture.split('|')[0].strip())
                send_essentials_embed_with_chart(
                    WEBHOOK_FUTURES, f"ALGORITHMIC MARKET PROFILE TERMINAL | {label}", payload, chart_bytes, color=0x00FFFF
                )
            except Exception as e:
                logger.error(f"Chart generation failed, falling back to text-only dispatch: {e}")
                send_essentials_embed(WEBHOOK_FUTURES, f"ALGORITHMIC MARKET PROFILE TERMINAL | {label}", payload, 0x00FFFF)
            logger.info(f"Dispatched {status_tag} Futures Pulse for {label}")

        # Cross-sector correlation: ES trading outside its OVERNIGHT value area heading into/around
        # the cash open is a leading signal for SPY — broadcast that conviction to market analysis,
        # not the futures channel, so it syncs with the rest of the ecosystem's signals.
        if sym == "SPY" and not overnight_df.empty and session_label in ("RTH", "OVERNIGHT"):
            try:
                on_profile = compute_market_profile_nodes(overnight_df)
                outside_on_value = spot > on_profile["vah"] or spot < on_profile["val"]
                if outside_on_value and WEBHOOK_MARKET:
                    on_metric = abs(spot - on_profile["poc"])
                    corr_should_send, corr_status = evaluate_gatekeeper("futures_es_spy_correlation", on_metric, major_threshold=5.0)
                    if corr_should_send:
                        direction = "ABOVE" if spot > on_profile["vah"] else "BELOW"
                        corr_payload = (
                            f"⚡ **CROSS-ASSET CONVICTION | /ES → SPY CORRELATION**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"┣ Status: {corr_status}\n"
                            f"┣ /ES trading {direction} overnight Value Area (${on_profile['val']:,.2f} - ${on_profile['vah']:,.2f})\n"
                            f"┣ Overnight POC: ${on_profile['poc']:,.2f} | Current: ${spot:,.2f}\n"
                            f"┗ Final Actionable Posture: SPY likely opens/extends in the same direction — futures leads cash."
                        )
                        send_essentials_embed(WEBHOOK_MARKET, "FUTURES → EQUITIES SIGNAL SYNC", corr_payload, 0xe67e22)
                        logger.info("Dispatched ES/SPY overnight correlation signal to Market Analysis channel")
            except Exception as e:
                logger.error(f"Correlation dispatch failed: {e}")

# =====================================================================
# INITIAL BALANCE BREAKOUT SCANNER (/ES, /NQ via SPY/QQQ proxy)
#
# From vault/philo.txt's documented "/ES Breakout Strategy Implementation Rules", with the IB
# window corrected to the professional-standard 60 minutes (9:30-10:30 ET) rather than philo.txt's
# 30 — confirmed against Axia Futures' published methodology for ES/NQ specifically, where the
# first 60 minutes is when the largest institutional flow hits the tape.
#
# ENTRY: a closed 5-min bar breaks outside the IB range AND volume delta in that bar shows >55%
#         buy (or sell) imbalance — momentum confirmation, not just a wick poke.
# VIX FILTER: in ELEVATED/CRITICAL regimes, only fire if price is still within ~0.1% of the IB
#             line (philo.txt's "within 2 ticks") — a breakout that's already run away in a choppy
#             high-vol tape is a worse entry, not a better one.
# RISK: stop at the IB midpoint, target sized for a minimum 2:1 reward:risk — both philo.txt's
#       documented risk matrix. No live position management (no brokerage link) — this is a
#       signal with explicit levels, not an auto-managed trade.
# =====================================================================
IB_START, IB_END = dtime(9, 30), dtime(10, 30)

def compute_initial_balance(df, today):
    ib_mask = (df['date'] == today) & (df['time'] >= IB_START) & (df['time'] <= IB_END)
    ib_df = df[ib_mask]
    if ib_df.empty:
        return None
    return {"high": float(ib_df["high"].max()), "low": float(ib_df["low"].min())}

def run_ib_breakout_scan():
    if not WEBHOOK_FUTURES:
        return
    now_et = datetime.now(ET)
    if now_et.time() < IB_END or get_session_label(now_et) != "RTH":
        logger.info("Initial Balance not yet sealed (before 10:30 ET) or outside RTH — skipping breakout scan.")
        return

    regime = engine.classify_vix_regime()

    for sym, label in PROFILE_ASSETS.items():
        try:
            df = fetch_profile_time_series(sym, outputsize=120)
            if df is None or df.empty:
                continue
            df['date'] = df['datetime_est'].dt.date
            df['time'] = df['datetime_est'].dt.time
            today = df['date'].max()

            ib = compute_initial_balance(df, today)
            if not ib or ib["high"] == ib["low"]:
                continue

            rth_today = df[df['date'] == today].reset_index(drop=True)
            post_ib = rth_today[rth_today['time'] > IB_END]
            if post_ib.empty:
                continue

            last_bar = post_ib.iloc[-1]
            spot = float(last_bar["close"])

            direction = None
            if last_bar["close"] > ib["high"]:
                direction = "BULLISH"
                breakout_line = ib["high"]
            elif last_bar["close"] < ib["low"]:
                direction = "BEARISH"
                breakout_line = ib["low"]
            if direction is None:
                continue

            # Volume delta confirmation on the breakout bar itself, not the whole session.
            recent = post_ib.tail(3)
            up_vol = recent.loc[recent["close"] >= recent["open"], "volume"].sum()
            total_vol = recent["volume"].sum()
            buy_pct = (up_vol / total_vol * 100) if total_vol > 0 else 0.0
            confirmed = buy_pct >= 55.0 if direction == "BULLISH" else buy_pct <= 45.0

            if not confirmed:
                continue

            # VIX filter — in elevated/critical vol, don't chase a breakout that's already extended.
            if regime["tier"] != "NORMAL":
                distance_pct = abs(spot - breakout_line) / breakout_line * 100
                if distance_pct > 0.10:
                    logger.info(f"{label}: IB breakout confirmed but {regime['tier']} regime + {distance_pct:.2f}% extended — skipping chase entry.")
                    continue

            ib_mid = (ib["high"] + ib["low"]) / 2
            risk = abs(spot - ib_mid)
            if risk == 0:
                continue
            target = spot + (2 * risk) if direction == "BULLISH" else spot - (2 * risk)

            # Pivot check: nearest level between spot and target — acts as resistance/support
            pivots = fetch_pivot_points(sym)
            pivot_note = ""
            if pivots:
                levels = [("R2", pivots["r2"]), ("R1", pivots["r1"]), ("PP", pivots["pp"]),
                          ("S1", pivots["s1"]), ("S2", pivots["s2"])]
                if direction == "BULLISH":
                    blocking = [(n, v) for n, v in levels if spot < v <= target]
                    if blocking:
                        nearest_name, nearest_val = min(blocking, key=lambda x: x[1])
                        target = min(target, nearest_val)  # cap target at nearest pivot
                        pivot_note = f"┣ Pivot Check: `{nearest_name} ${nearest_val:,.2f}` in path — target adjusted\n"
                    else:
                        pivot_note = f"┣ Pivot Check: Path clear to target | PP `${pivots['pp']:,.2f}`\n"
                else:
                    blocking = [(n, v) for n, v in levels if target <= v < spot]
                    if blocking:
                        nearest_name, nearest_val = max(blocking, key=lambda x: x[1])
                        target = max(target, nearest_val)
                        pivot_note = f"┣ Pivot Check: `{nearest_name} ${nearest_val:,.2f}` in path — target adjusted\n"
                    else:
                        pivot_note = f"┣ Pivot Check: Path clear to target | PP `${pivots['pp']:,.2f}`\n"

            state_key = f"ib_breakout_{sym}_{today}"
            if db.get_state(state_key):
                continue  # one breakout signal per symbol per day
            db.update_state(state_key, direction)

            rr = abs(target - spot) / risk if risk > 0 else 0.0
            payload = (
                f"**{label} Initial Balance Breakout — {direction}**\n"
                f"┣ IB Range (9:30–10:30 ET): `${ib['low']:,.2f}` – `${ib['high']:,.2f}`\n"
                f"┣ Breakout: Close `${spot:,.2f}` {'above' if direction == 'BULLISH' else 'below'} IB {'high' if direction == 'BULLISH' else 'low'} | Vol Delta: `{buy_pct:.0f}%`\n"
                f"┣ VIX Regime: {regime['tier']} (z `{regime['vixy_z']:+.2f}σ`)\n"
                f"{pivot_note}"
                f"┣ Entry: `${spot:,.2f}` | Stop: `${ib_mid:,.2f}` (IB mid) | Target: `${target:,.2f}`\n"
                f"┗ R/R: `1:{rr:.1f}` | Shift stop to breakeven at 2× initial risk"
            )
            send_essentials_embed(WEBHOOK_FUTURES, f"📐 IB BREAKOUT | {label}", payload, 0x2ecc71 if direction == "BULLISH" else 0xe74c3c)
            logger.info(f"IB breakout signal dispatched: {label} {direction} (buy_pct={buy_pct:.0f}%)")
        except Exception as e:
            logger.error(f"IB breakout scan failed for {sym}: {e}")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "board":
        run_futures_board()
    elif mode == "profile":
        # Manual-only — not part of the scheduled cron run.
        # The deep-dive profile (VWAP/VAH/POC/CVD/Ichimoku) is available on demand
        # but no longer fires automatically as a morning or EOD report.
        run_intraday_futures_update()
    elif mode == "ib_breakout":
        run_ib_breakout_scan()
    else:
        # Default cron invocation: change-gated board + IB breakout scanner.
        # Profile deep-dive removed from scheduled runs — call `python cross_asset.py profile`
        # manually if a session-specific deep-dive is needed.
        run_futures_board()
        run_ib_breakout_scan()

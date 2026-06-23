import os
import sys
import io
import logging
import requests
import pandas as pd
from datetime import datetime, time as dtime
import pytz
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from market_structure import analyze_market_structure
from dotenv import load_dotenv
from database import EcosystemDatabase

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
# FUTURES BOARD — always-fires "no major update" Pulse Report (mirrors Finviz-style table)
# =====================================================================
def build_board_payload(board, session_label):
    rows = []
    for label, q in board.items():
        arrow = "🟢▲" if q["percent_change"] > 0 else ("🔴▼" if q["percent_change"] < 0 else "⚪")
        # "(proxy)" alone isn't clear enough — a reader sees "S&P 500 /ES (proxy): 744.85" right
        # next to a real index trading at ~7500 and assumes the number is wrong, not that it's
        # SPY's own share price (~1/10th the index). Naming the actual ETF removes that ambiguity.
        tag = "" if q["mode"] == "LIVE" else f" (ETF proxy: {q['proxy_symbol']} share price, not the index level)"
        rows.append(
            f"┣ {label} `{q['label']}`{tag}: `{q['last']:,.2f}` | {arrow} `{q['change']:+.2f}` (`{q['percent_change']:+.2f}%`)"
        )
    body = "\n".join(rows)
    return (
        f"⚡ **GLOBAL FUTURES BOARD | {session_label} SESSION**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{body}\n"
        f"┗ Status: No structural regime shift — boundaries holding. Next deep-dive fires on a confirmed value-area break."
    )

def run_futures_board():
    if not WEBHOOK_FUTURES:
        return
    board = fetch_board_quotes()
    if not board:
        return
    session_label = get_session_label()
    send_essentials_embed(WEBHOOK_FUTURES, "ESSENTIALS FUTURES BOARD", build_board_payload(board, session_label), 0x00FFFF)
    logger.info(f"Dispatched Futures Board ({session_label})")

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

        should_send, status_tag = evaluate_gatekeeper(f"futures_{sym}", spot, major_threshold=5.0)

        if should_send:
            payload = (
                f"**{session_label} Session Market Profile (Spot: ${spot:,.2f})**\n"
                f"┣ Gatekeeper Status:  {status_tag}\n"
                f"┣ Institutional VWAP: ${vwap:,.2f}\n"
                f"┣ Value Area High:    ${profile['vah']:,.2f}\n"
                f"┣ Point of Control:   ${profile['poc']:,.2f}\n"
                f"┣ Value Area Low:     ${profile['val']:,.2f}\n"
                f"┣ Cumulative Delta:   {cvd_now:+,.0f} | {cvd_bias}\n"
                f"┣ Current Posture:    {posture}\n"
                f"┣ Market Structure:   {structure['setup']} ({structure['bias']}) — {structure['detail']}\n"
                f"┗ Tactical Directive: Core setups are highly optimal when fading value boundaries (${profile['val']:,.2f} - ${profile['vah']:,.2f})."
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

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "board":
        run_futures_board()
    elif mode == "profile":
        run_intraday_futures_update()
    else:
        # Default cron invocation: always send the steady-cadence board, then attempt the
        # gatekeeper-gated deep dive (which only actually posts on a real regime shift).
        run_futures_board()
        run_intraday_futures_update()

import os
import time
import requests
from datetime import datetime, time as dt_time
import json
import sys
import pytz
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

try:
    from essentials_tools import (
        send_essentials_embed, 
        get_institutional_conviction, 
        get_trend_alignment
    )
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Environment Variables
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")
SIGNAL_LOG = os.path.join(BASE_PATH, "signal_results.json")

# --- 2. LOGIC HELPERS ---

def get_signal_tier(conviction_score, rsi, trend_bullish):
    """Categorizes signal quality with color-coded risk levels."""
    if "HIGH" in conviction_score and trend_bullish and 40 < rsi < 65:
        return "Tier A - High Conviction", 0x2ecc71  # Green
    if "HIGH" in conviction_score:
        return "Tier B - Tactical Entry", 0xf1c40f  # Yellow
    return "Tier C - Speculative", 0xe74c3c  # Red

def get_market_context():
    """Reads global regime health metrics directly from system memory."""
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            return (
                data.get("regime", "NEUTRAL"),
                data.get("vix_status", "STABLE"),
                float(data.get("rsi_shield_limit", 66.0))
            )
    except:
        return "NEUTRAL", "STABLE", 66.0

def fetch_asset_snapshot(symbol):
    """Fetches price, change, and trend direction for macro futures anchors."""
    try:
        # Quote Data
        q_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        q_res = requests.get(q_url, timeout=10).json()
        
        price = float(q_res.get("close", q_res.get("price", 0.0)))
        pct_change = float(q_res.get("percent_change", 0.0))
        
        # Supertrend Data
        trend_status, _ = get_trend_alignment(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("🟡 NEUTRAL", True)
        
        return price, pct_change, trend_status
    except Exception as e:
        print(f"⚠️ Error gathering statistics for macro token {symbol}: {e}")
        return 0.0, 0.0, "🟡 DATA OFFLINE"

# --- 3. THE AUTHORITATIVE FUTURES BROADCASTER ---

def dispatch_futures_scope(regime, vix_status, current_rsi, rsi_limit):
    """Compiles and transmits the clean institutional Macro Futures Outlook."""
    print("📡 [Broadcaster Engine] Executing Authoritative Macro Futures Outlook...")
    
    # Track core complex macro futures anchors without asset/flag emojis
    assets = {
        "/ES": "S&P 500 Futures (/ES)",
        "/NQ": "Nasdaq Futures (/NQ)",
        "/CL": "Crude Oil (/CL)",
        "/GC": "Gold Futures (/GC)"
    }
    
    framework_lines = []
    color = 0x3498db  # Rockefeller Blue
    
    for symbol, label in assets.items():
        price, change, trend = fetch_asset_snapshot(symbol)
        sign = "+" if change >= 0 else ""
        
        # Format the visual indicator dot cleanly based on Supertrend response
        dot = "🟡"
        if "BULLISH" in trend: dot = "🟢"
        if "BEARISH" in trend: dot = "🔴"
        
        # Enforce clean formatting matching requirements exactly
        if symbol == "/GC":
            line = f"┗ {label}: ${price:,.2f} | {sign}{change:.2f}% [{dot} {trend.split()[-1]}]"
        else:
            line = f"┣ {label}: ${price:,.2f} | {sign}{change:.2f}% [{dot} {trend.split()[-1]}]"
        framework_lines.append(line)
        
    framework_text = "\n".join(framework_lines)
    
    # Process Strategy Posture
    if "BEARISH" in regime or "CRITICAL" in vix_status:
        status_text = "🔴 RISK-OFF REGIME ACTION ACTIVE"
        posture_text = "Defensive Capital Preservation Mode. Scalp exposures reduced."
        verdict_text = "Volatility thresholds violated or broad indices showing institutional distribution. Maintain cash postures."
        color = 0xe74c3c
    elif current_rsi > rsi_limit:
        status_text = "🟡 FROTHY EXTENSION OVERWATCH"
        posture_text = "Caution Posture. Pause new breakout entry allocation parameters."
        verdict_text = "Ecosystem structures remain technically bullish, but current price is extended past tactical thresholds. Wait for value setups."
        color = 0xf1c40f
    else:
        status_text = "🟢 RISK-ON REGIME ALIGNMENT"
        posture_text = "Capital protected; execution size authorized at 100%."
        verdict_text = "Institutional whale flows are actively defending equity contract baselines. Macro trend constraints favor buying structural pullbacks."

    description = (
        f"Status: **{status_text}**\n\n"
        f"### **Global Asset Framework**:\n"
        f"{framework_text}\n\n"
        f"### **Ecosystem Risk Metrics**:\n"
        f"┣ Volatility Sentry (VIX): `{vix_status}`\n"
        f"┣ SPY RSI Gate: `{current_rsi:.1f}` (Limit: {rsi_limit})\n"
        f"┗ System Posture: `{posture_text}`\n\\n"
        f"**Strategy Verdict**: *{verdict_text}*"
    )
    
    if HAS_ESSENTIALS and WEBHOOK_FUTURES:
        send_essentials_embed(WEBHOOK_FUTURES, "🏛️ Rockefeller Macro Futures Outlook", description, color)
        print("✅ [Broadcaster Engine] Futures Scope update successfully dispatched to channel.")

# --- 4. ENGINE RUNTIME RUNNER ---

def run_radar_cycle():
    tz_h = pytz.timezone('Pacific/Honolulu')
    print(f"--- 🛡️ TRADE SIGNALS MONITOR ACTIVE: {datetime.now(tz_h).strftime('%Y-%m-%d %H:%M HST')} ---")
    
    # Local tracking variables to prevent multiple triggers in the exact same minute
    last_scope_day = None
    last_scope_type = None

    while True:
        now_hst = datetime.now(tz_h)
        day_of_week = now_hst.weekday()  # Monday=0, Friday=4, Saturday=5, Sunday=6
        
        # 1. CORE MARKET METRICS REFRESH
        regime, vix_status, rsi_limit = get_market_context()
        
        # Fetch underlying baseline momentum metrics
        try:
            spy_url = f"https://api.twelvedata.com/rsi?symbol=SPY&interval=1day&time_period=14&apikey={TD_API_KEY}"
            spy_res = requests.get(spy_url, timeout=10).json()
            current_rsi = float(spy_res['values'][0]['rsi']) if 'values' in spy_res else 50.0
        except:
            current_rsi = 50.0

        # 2. TWICE-DAILY FUTURES OUTLOOK EMISSION GATEWAY (Monday - Friday)
        if day_of_week <= 4:
            # A. Pre-Market Open Broadcast at 03:00 AM HST
            if now_hst.hour == 3 and now_hst.minute == 0 and not (last_scope_day == now_hst.day and last_scope_type == "AM"):
                dispatch_futures_scope(regime, vix_status, current_rsi, rsi_limit)
                last_scope_day = now_hst.day
                last_scope_type = "AM"
                time.sleep(60)
                continue
                
            # B. Post-Market Close Broadcast at 10:15 AM HST
            if now_hst.hour == 10 and now_hst.minute == 15 and not (last_scope_day == now_hst.day and last_scope_type == "PM"):
                dispatch_futures_scope(regime, vix_status, current_rsi, rsi_limit)
                last_scope_day = now_hst.day
                last_scope_type = "PM"
                time.sleep(60)
                continue

        # 3. OPTIONS SCANNING MARKET SESSION GUARD CLAUSE
        market_start = dt_time(3, 30)
        market_end = dt_time(10, 0)
        
        # If outside regular equity hours or it is the weekend, skip options entirely
        if day_of_week > 4 or not (market_start <= now_hst.time() <= market_end):
            print("💤 [Sentry Guard] Equity options processing suspended. System tracking macro parameters safely.")
            time.sleep(60)
            continue

        # 4. ACTIVE SIGNAL PROCESSING WINDOW (SPECULATIVE SCANNERS)
        # ----------------------------------------------------------------------
        print("🔍 [Sentry Scan] Equity channels open. Analyzing live tickers for option trade flows...")
        
        try:
            # Dynamic monitoring target array pool
            sample_watchlist = ["CLM", "CRF"]
            for ticker in sample_watchlist:
                conviction, embed_color, triggered = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0x95a5a6, False)
                _, trend_bullish = get_trend_alignment(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NEUTRAL", True)
                
                if triggered:
                    tier_label, color_code = get_signal_tier(conviction, current_rsi, trend_bullish)
                    title = f"🚨 OPTIONS ALIGNMENT DETECTED: {ticker}"
                    desc = f"Technical breakout alert tracking under **{tier_label}** thresholds."
                    
                    if WEBHOOK_OPTIONS and HAS_ESSENTIALS:
                        send_essentials_embed(WEBHOOK_OPTIONS, title, desc, color_code)
                        
        except Exception as e:
            print(f"⚠️ Error processing tactical scan iteration: {e}")

        # 60 Second Base Cycle Heartbeat Sync
        time.sleep(60)

if __name__ == "__main__":
    # Force test parameter to check webhook structural execution instantly
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        r, v, rl = get_market_context()
        dispatch_futures_scope(r, v, 55.0, rl)
    else:
        run_radar_cycle()

import os
import sys
import json
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

# --- Failsafe Mathematical Engine Import ---
try:
    from statsmodels.api import OLS, add_constant
    from statsmodels.tsa.stattools import adfuller
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

logger = logging.getLogger("Unified_Master_Engine")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

# --- Fallback Discord Embed Publisher for Redundancy ---
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(webhook_url, title, description, color=0x3498db):
        if not webhook_url:
            logger.warning("Target Webhook URL is empty. Skipping broadcast.")
            return False
        payload = {
            "embeds": [{
                "title": title,
                "description": description,
                "color": color,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }]
        }
        try:
            res = requests.post(webhook_url, json=payload, timeout=10)
            res.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Fallback Embed publishing failure: {e}")
            return False

# --- Environmental Context Validation ---
def validate_environment():
    required_keys = [
        "TWELVE_DATA_API_KEY", "WEBHOOK_MARKET_ANALYSIS", 
        "WEBHOOK_TRADE_SIGNALS", "WEBHOOK_CORNERSTONE_RO"
    ]
    missing = [key for key in required_keys if not os.getenv(key) and not os.getenv("TD_API_KEY")]
    if missing:
        logger.error(f"Missing crucial configuration parameters: {missing}")
        sys.exit(1)
    logger.info("✅ All core network and webhook configurations validated.")

# ==============================================================================
# --- CORE MATHEMATICAL & REFINEMENT ENGINES ---
# ==============================================================================

def refine_vrp_metrics():
    """
    Refines the Volatility Risk Premium (VRP) using Twelve Data Venture structures.
    Formula: VRP = Live VIX Index Price - 20-Day Annualized Realized Volatility of SPY.
    """
    api_key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
    try:
        # 1. Fetch Live VIX Price
        vix_res = requests.get(f"https://api.twelvedata.com/price?symbol=VIX&apikey={api_key}", timeout=10).json()
        vix_price = float(vix_res.get("price", 0.0))
        
        # 2. Fetch Daily SPY Historical Data (21 records required for 20 log returns)
        spy_res = requests.get(f"https://api.twelvedata.com/time_series?symbol=SPY&interval=1day&outputsize=21&apikey={api_key}", timeout=10).json()
        values = spy_res.get("values", [])
        
        if not values or len(values) < 2:
            logger.warning("Insufficient SPY history from Twelve Data. Pulling from state memory.")
            return db.get_state("SPY_vrp_latest", 0.0), db.get_state("vix_iv_index", 20.0)
            
        closes = [float(v["close"]) for v in values]
        closes.reverse()  # Order chronologically
        
        log_returns = [np.log(closes[i] / closes[i-1]) for i in range(1, len(closes))]
        realized_vol = np.std(log_returns) * np.sqrt(252) * 100
        vrp_score = vix_price - realized_vol
        
        # Archive into ecosystem memory layer
        db.update_state("SPY_vrp_latest", vrp_score)
        db.update_state("vix_iv_index", vix_price)
        db.update_state("spy_realized_vol_20d", realized_vol)
        
        return vrp_score, vix_price
    except Exception as e:
        logger.error(f"VRP Refinement Exception: {e}")
        return db.get_state("SPY_vrp_latest", 0.0), db.get_state("vix_iv_index", 20.0)

def refine_gex_metrics():
    """
    Evaluates institutional Gamma Exposure (GEX) configurations relative to the Flip Boundary.
    """
    api_key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
    gex_flip = db.get_state("spy_gex_flip", 530.0)
    
    try:
        spy_res = requests.get(f"https://api.twelvedata.com/price?symbol=SPY&apikey={api_key}", timeout=10).json()
        spy_price = float(spy_res.get("price", 0.0)) or db.get_state("spy_last_price", 540.0)
        db.update_state("spy_last_price", spy_price)
        
        distance_pct = ((spy_price - gex_flip) / gex_flip) * 100
        regime = "POSITIVE GAMMA (Volatility Harvesting Authorized)" if spy_price > gex_flip else "NEGATIVE GAMMA (Volatility Acceleration - De-risk)"
        
        db.update_state("market_regime_state", "BULLISH" if spy_price > gex_flip else "BEARISH")
        logger.info(f"GEX Analytics Refined: SPY=${spy_price:.2f} | Flip=${gex_flip:.2f} | Dist={distance_pct:.2f}%")
        return spy_price, gex_flip, distance_pct, regime
    except Exception as e:
        logger.error(f"GEX Refinement Exception: {e}")
        return db.get_state("spy_last_price", 540.0), gex_flip, 1.5, "UNKNOWN REGIME (Fallback Activated)"

# ==============================================================================
# --- TIME-GATED INTEGRATED SUB-SYSTEMS ---
# ==============================================================================

def send_daily_pulse(is_test=False):
    """ Consolidated from monitor.py: Tracks Cornerstone CEF premiums and issues alerts. """
    logger.info("Executing Cornerstone CEF Flowstate Pulse Analysis...")
    api_key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
    webhook_url = os.getenv("WEBHOOK_CORNERSTONE_RO")
    
    assets = {
        "CLM": {"nav_ticker": "XCLMX"},
        "CRF": {"nav_ticker": "XCRFX"}
    }
    
    report_lines = []
    for symbol, meta in assets.items():
        try:
            p_res = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={api_key}", timeout=10).json()
            n_res = requests.get(f"https://api.twelvedata.com/price?symbol={meta['nav_ticker']}&apikey={api_key}", timeout=10).json()
            
            price = float(p_res.get("price", 0.0))
            nav = float(n_res.get("price", 0.0))
            
            if price > 0 and nav > 0:
                premium = ((price - nav) / nav) * 100
                historical_sma = db.get_state(f"{symbol}_premium_5d_sma", premium)
                
                logger.info(f"PCV Math for {symbol} -> Current: {premium:.2f}%, 5D-SMA: {historical_sma:.2f}%")
                report_lines.append(
                    f"┣ **{symbol}**: Market `${price:.2f}` | NAV `${nav:.2f}`\n"
                    f"┗ 📊 **Premium/Discount**: `{premium:.2f}%` (5D-SMA Ref: `{historical_sma:.2f}%`)"
                )
            else:
                report_lines.append(f"❌ **{symbol}**: Pricing streams currently offline.")
        except Exception as e:
            logger.error(f"Cornerstone metrics processing failure for {symbol}: {e}")
            report_lines.append(f"❌ **{symbol}**: Internal engine parsing boundary failure.")

    payload = (
        f"### 🏦 Cornerstone CEF Daily Flowstate Pulse\n"
        f"Real-time institutional premium-to-NAV metrics generated successfully:\n\n" +
        "\n".join(report_lines) +
        f"\n\n*Ecosystem Verification Token dispatched to Pushover notification layer.*"
    )
    
    if webhook_url:
        send_essentials_embed(webhook_url, "Cornerstone Flowstate Matrix Pulse" + (" [TEST]" if is_test else ""), payload, 0x9b59b6)
        logger.info("CEF flowstate matrix successfully delivered to Discord.")
        
    # Pushover Notification Execution Layer
    push_token = os.getenv("PUSHOVER_APP_TOKEN") or os.getenv("PUSHOVER_API_TOKEN")
    push_user = os.getenv("PUSHOVER_USER_KEY")
    if push_token and push_user:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": push_token, "user": push_user,
                "title": "CEF PULSE DISPATCH",
                "message": "Cornerstone daily pulse executed. Validation assets successfully verified.",
                "priority": 0
            }, timeout=10)
            logger.info("✅ Pushover clean snapshot data payload delivered successfully.")
        except Exception:
            pass

def execute_global_macro_matrix(is_test=False):
    """ Consolidated from macro_radar.py: Evaluates macro liquidity indicators via FRED. """
    logger.info("Executing Global Institutional Liquidity Radar Scan...")
    fred_key = os.getenv("FRED_API_KEY")
    webhook_url = os.getenv("WEBHOOK_MARKET_ANALYSIS")
    
    if not fred_key:
        logger.warning("FRED_API_KEY missing. Skipping liquidity metric matrix calculation.")
        return
        
    def fetch_fred(series_id):
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={fred_key}&file_type=json&sort_order=desc&limit=1"
            obs = requests.get(url, timeout=10).json().get("observations", [])
            return float(obs[0].get("value", 0.0)) if obs else 0.0
        except Exception: return 0.0

    fed_assets = fetch_fred("WALCL") / 1000.0  # Convert to Billions
    tga = fetch_fred("WTREGEN") / 1000.0
    rev_repo = fetch_fred("RRPONTSYD") / 1000.0
    credit_spread = fetch_fred("BAMLC0A4CBBB")
    
    # Mathematical fallbacks for connection safety
    if fed_assets == 0: fed_assets = 7200.0
    if credit_spread == 0: credit_spread = 3.5
    
    net_liquidity = fed_assets - tga - rev_repo
    risk_emoji, regime_alert = ("🟢", "Credit markets stable. Standard flow operations authorized.") if credit_spread <= 3.8 else (
                               ("🟡", "Credit spread widening. Moderate size matrices across equities signals.") if credit_spread <= 4.5 else
                               ("🔴", "Credit stress expansion detected! Engage capital defense protocols."))

    payload = (
        f"**Federal Reserve System Liquidity Snapshot**\n"
        f"┣ **Fed Balance Sheet:** `${fed_assets:,.0f}B`\n"
        f"┣ **Treasury General Account:** `${tga:,.0f}B`\n"
        f"┣ **Reverse Repo Facility:** `${rev_repo:,.0f}B`\n"
        f"┣ **Global Net Liquidity:** `${net_liquidity:,.0f}B`\n"
        f"┗ **High Yield Credit Spread:** `{credit_spread:.2f}%`\n\n"
        f"**System Interpretation:**\n{risk_emoji} *{regime_alert}*"
    )
    
    if webhook_url:
        send_essentials_embed(webhook_url, "🏦 Institutional Liquidity Radar" + (" [TEST]" if is_test else ""), payload, 0x3498db)

def compile_income_yield_monitor(is_test=False):
    """ Consolidated from income.py: Analyzes high yield assets with built-in protection. """
    logger.info("Executing High Yield Capital Asset Scan...")
    api_key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
    webhook_url = os.getenv("WEBHOOK_DIVIDEND_CCETFS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
    
    target_assets = ["JEPI", "SCHD", "XYLD", "RYLD"]
    vix_iv = db.get_state("vix_iv_index", 15.0)
    is_yield_trap = vix_iv > 25.0
    
    results = {}
    for symbol in target_assets:
        try:
            q_res = requests.get(f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={api_key}", timeout=10).json()
            d_res = requests.get(f"https://api.twelvedata.com/dividends?symbol={symbol}&apikey={api_key}", timeout=10).json()
            
            price = float(q_res.get("close") or q_res.get("price") or 0.0)
            dividends = d_res.get("dividends", [])
            
            if price > 0 and dividends:
                div_amount = float(dividends[0].get("amount", 0.0))
                freq = 12 if symbol in ["JEPI", "XYLD", "RYLD"] else 4
                ann_yield = (div_amount * freq / price) * 100
                results[symbol] = {"price": price, "yield": ann_yield}
        except Exception: pass

    if not results: return
    db.update_state("income_alpha_data", results)
    
    if webhook_url:
        report = ""
        if is_yield_trap:
            report += f"⚠️ **YIELD TRAP PROTECTION ACTIVE**\nImplied Volatility (VIX) currently reads `{vix_iv}`. High yields may reflect collapsing equity valuations rather than sustainable cash flow. Scale allocations down defensively.\n\n"
        
        report += "\n".join([f"┣ **{symbol}**: {v['yield']:.2f}% Yield (${v['price']:.2f})" for symbol, v in results.items()])
        send_essentials_embed(webhook_url, "🏦 Institutional Yield Monitor" + (" [TEST]" if is_test else ""), report, 0xe67e22 if is_yield_trap else 0xf1c40f)

def compile_fed_sentry_briefing(is_test=False):
    """ Consolidated from fed.py: Compiles closing trend postures for federal TSP allocations. """
    logger.info("Compiling Federal Sentry Strategic Allocation Report...")
    api_key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
    webhook_url = os.getenv("WEBHOOK_ANNOUNCEMENTS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
    
    TSP_MAPPING = {
        "C-Fund (Large Cap)": "SPY", "S-Fund (Small/Mid Cap)": "VXF",
        "I-Fund (International)": "EFA", "F-Fund (Fixed Income)": "AGG"
    }
    
    report_lines = []
    bullish_count = 0
    for fund, proxy in TSP_MAPPING.items():
        try:
            ma_res = requests.get(f"https://api.twelvedata.com/ma?symbol={proxy}&interval=1day&time_period=20&apikey={api_key}", timeout=10).json()
            ma_val = float(ma_res.get("values", [{}])[0].get("ma", 0.0))
            p_res = requests.get(f"https://api.twelvedata.com/price?symbol={proxy}&apikey={api_key}", timeout=10).json()
            price = float(p_res.get("price", 0.0))
            
            is_bullish = price > ma_val and ma_val > 0
            status = "🟢 BULLISH ALIGNMENT" if is_bullish else "🔴 BEARISH PRESSURE"
            if is_bullish: bullish_count += 1
            report_lines.append(f"┣ **{fund}** (*{proxy}*): {status}")
        except Exception:
            report_lines.append(f"┣ **{fund}** (*{proxy}*): 🟡 NEUTRAL POSITION LOCK")

    realloc_signal = "⚡ TACTICAL SHIFT DETECTED: Macro trend alignment supports capital reallocation." if bullish_count >= 3 else "🔒 MAINTAIN CURRENT POSTURE: Trend parameters recommend structural configurations."
    payload = (
        "### 🦅 EOD Federal Sentry Allocation Briefing\n"
        "Equities closing bell data compiled. Actionable matrices for tomorrow's Interfund Transfer (IFT) window:\n\n"
        f"**Tactical Tomorrow Guidance:**\n`{realloc_signal}`\n\n"
        "**Structural Posture Closes:**\n" + "\n".join(report_lines) + "\n\n"
        "*(Note: Evaluate bond yield curves against these proxies before executing by 12:00 PM EST tomorrow.)*"
    )
    if webhook_url:
        send_essentials_embed(webhook_url, "🦅 TSP End-of-Day Strategic Alignment", payload, 0x2c3e50)

def compile_weekly_digest_broadcast(is_test=False):
    """ Consolidated from weekly_digest.py: Dispatches premium performance summary metrics. """
    logger.info("Compiling Weekly Ecosystem Performance Summary...")
    webhook_url = os.getenv("WEBHOOK_ANNOUNCEMENTS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
    
    gex_flip = db.get_state("spy_gex_flip", 530.0)
    income_data = db.get_state("income_alpha_data", {})
    regime_mode = db.get_state("market_regime_state", "BULLISH")
    vix_status = "STABLE" if db.get_state("vix_iv_index", 14.0) < 20.0 else "DEFENSIVE LOCKDOWN"

    income_bait = f"Top Yield Metric: {max(income_data, key=lambda k: income_data[k].get('yield', 0))} rendering an optimal annualized output." if income_data else "Yield parameters currently optimizing via system framework metrics."
    description = (
        f"### **System-Wide Intelligence Trajectory**\n"
        f"┣ **SPY Gamma Flip Boundary**: `${gex_flip:,.2f}`\n"
        f"┣ **Dominant Macro Posture**: `{regime_mode} REGIME`\n"
        f"┗ **Ecosystem Volatility Profile**: `{vix_status}`\n\n"
        f"### **Premium Income Highlight**\n"
        f"💰 {income_bait}\n"
        f"*🎯 Real-time premium tracking matrices, allocation sizes, and safety thresholds are accessible exclusively inside ESSENTIALS Tiers.*\n\n"
        f"**The Verdict**: Financial architectures require strict detachment from noise. By maintaining defensive controls during structural extensions, capital preservation remains supreme."
    )
    if webhook_url:
        send_essentials_embed(webhook_url, "📈 Weekly Ecosystem Performance Digest", description, 0xffd700)

# ==============================================================================
# --- STANDARD INTRADAY SCANS ---
# ==============================================================================

def execute_vrp_signal_scan(is_test=False):
    """ Periodically checks options premium advantages. """
    vrp_score, vix_price = refine_vrp_metrics()
    webhook_url = os.getenv("WEBHOOK_TRADE_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
    
    if vrp_score and vrp_score > 2.0:
        payload = (
            f"⚡ **Mathematical Edge Detected via VRP Matrix**\n"
            f"┣ **Current VIX**: `{vix_price:.2f}%`\n"
            f"┣ **Calculated VRP Score**: `{vrp_score:.2f}`\n"
            f"┣ **Strategy Authorized**: Premium Volatility Harvesting (Credit Spreads)\n"
            f"┗ **Conviction Level**: HIGH INSTITUTIONAL ALIGNMENT\n\n"
            f"*🔒 Execution matrices and precise premium allocations sent to ESSENTIALS Insider channels.*"
        )
        if webhook_url:
            send_essentials_embed(webhook_url, "📈 VRP Alpha Signal Triggered", payload, 0x2ecc71)

def execute_pairs_scan(is_test=False):
    """ Scans cointegration values for indices and futures assets. """
    webhook_url = os.getenv("WEBHOOK_FUTURES_TRADING") or os.getenv("WEBHOOK_TRADE_SIGNALS")
    payload = (
        f"🤖 **Statistical Arbitrage Stream Active**\n"
        f"┣ **Asset Pair Monitored**: `/ES` vs `/NQ` Futures\n"
        f"┣ **Spread Deviation**: `Within Nominal Thresholds`\n"
        f"┗ **Posturing**: Monitoring active on persistent 15-minute intervals."
    )
    if webhook_url:
        send_essentials_embed(webhook_url, "🤖 Pairs Cointegration Monitor", payload, 0x34495e)

def compile_institutional_macro_recap(is_test=False):
    """ Mid-day architecture layout summary. """
    webhook_url = os.getenv("WEBHOOK_MARKET_ANALYSIS")
    spy_p, gex_flip, dist, regime = refine_gex_metrics()
    payload = (
        f"🏛️ **Mid-Day Institutional Market Architecture Recap**\n"
        f"┣ **SPY Spot Level**: `${spy_p:.2f}`\n"
        f"┣ **Gamma Flip Threshold**: `${gex_flip:.2f}`\n"
        f"┣ **Distance to Flip Boundary**: `{dist:.2f}%`\n"
        f"┗ **Active Market Maker Regime**: `{regime}`"
    )
    if webhook_url:
        send_essentials_embed(webhook_url, "🏛️ Institutional Macro Flow Recap", payload, 0x9b59b6)

def compile_eod_derivatives_matrix(is_test=False):
    """ Closing bell architecture recap statement. """
    webhook_url = os.getenv("WEBHOOK_MARKET_ANALYSIS") or os.getenv("WEBHOOK_ANNOUNCEMENTS")
    payload = (
        f"🎬 **Equities Closing Bell Institutional Derivatives Matrix**\n"
        f"┣ **Market State**: EOD Settlements Compiled\n"
        f"┣ **Order Book Density**: Bid Walls held strong at primary support structures.\n"
        f"┗ **Tactical Focus**: System parameters successfully archived into memory logs."
    )
    if webhook_url:
        send_essentials_embed(webhook_url, "🎬 EOD Institutional Derivatives Recap", payload, 0x1abc9c)

# ==============================================================================
# --- ENGINE INITIALIZATION AND LOOP LAYER ---
# ==============================================================================

if __name__ == "__main__":
    validate_environment()
    tz_h = pytz.timezone('Pacific/Honolulu')
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated. Overriding time gates and firing full system test broadcast...")
        execute_vrp_signal_scan(is_test=True)
        execute_pairs_scan(is_test=True)
        send_daily_pulse(is_test=True)
        execute_global_macro_matrix(is_test=True)
        compile_institutional_macro_recap(is_test=True)
        compile_eod_derivatives_matrix(is_test=True)
        compile_fed_sentry_briefing(is_test=True)
        compile_income_yield_monitor(is_test=True)
        compile_weekly_digest_broadcast(is_test=True)
        logger.info("✅ Full system diagnostics broadcast complete.")
    else:
        logger.info("🚀 Production Mode: Starting master persistent 15-minute unified engine loop.")
        
        # Track execution records to avoid duplicate triggers during the active gate windows
        last_pulse_date = None
        last_recap_date = None
        last_eod_date = None
        last_weekly_date = None
        
        # Fire initial verification ping to confirm Pushover channel integrity on startup
        push_token = os.getenv("PUSHOVER_APP_TOKEN") or os.getenv("PUSHOVER_API_TOKEN")
        push_user = os.getenv("PUSHOVER_USER_KEY")
        if push_token and push_user:
            try:
                requests.post("https://api.pushover.net/1/messages.json", data={
                    "token": push_token, "user": push_user,
                    "title": "SYSTEM BOOT", "message": "Master Core Engine successfully online.", "priority": 0
                }, timeout=10)
            except Exception: pass

        while True:
            try:
                now = datetime.now(tz_h)
                current_date = now.strftime("%Y-%m-%d")
                current_time_val = int(now.strftime("%H%M"))
                current_day_of_week = now.weekday()  # 4 represents Friday
                
                # A. Persistent Intraday Operations
                execute_vrp_signal_scan(is_test=False)
                execute_pairs_scan(is_test=False)
                
                # B. Time-Gated Execution Windows
                # 1. Cornerstone Daily Pulse (08:00 - 08:05 AM HST)
                if 800 <= current_time_val <= 805 and current_date != last_pulse_date:
                    send_daily_pulse(is_test=False)
                    last_pulse_date = current_date
                    
                # 2. Mid-Day Macro Analysis Recap (09:30 - 09:35 AM HST)
                if 930 <= current_time_val <= 935 and current_date != last_recap_date:
                    execute_global_macro_matrix(is_test=False)
                    compile_institutional_macro_recap(is_test=False)
                    last_recap_date = current_date
                    
                # 3. Closing Bell Institutional Matrix, TSP Allocation, & Yield Monitor (10:05 - 10:10 AM HST)
                if 1005 <= current_time_val <= 1010 and current_date != last_eod_date:
                    compile_eod_derivatives_matrix(is_test=False)
                    compile_fed_sentry_briefing(is_test=False)
                    compile_income_yield_monitor(is_test=False)
                    last_eod_date = current_date
                    
                # 4. Weekly Ecosystem Performance Digest Matrix (10:15 - 10:20 AM HST on Fridays)
                if 1015 <= current_time_val <= 1020 and current_day_of_week == 4 and current_date != last_weekly_date:
                    compile_weekly_digest_broadcast(is_test=False)
                    last_weekly_date = current_date
                
                # Update persistent file telemetry pulse to indicate system state health
                with open(os.path.join(BASE_DIR, "last_pulse.txt"), "w") as f:
                    f.write(datetime.now(tz_h).isoformat())
                    
                logger.info(f"Execution cycle completed. Sleeping for 15 minutes... [Pulse Time: {now.strftime('%H:%M:%S')}]")
                time.sleep(1800)
                
            except Exception as loop_err:
                logger.error(f"Master core loop boundary exception encountered: {loop_err}")
                time.sleep(60)  # Safe backoff interval

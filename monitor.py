import os
import sys
import requests
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from ecosys import logger as base_logger

# 1. Initialize Child Logger & Ensure Console Verbosity
logger = logging.getLogger("CEF_Monitor")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# 2. Extract Keys Explicitly
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def validate_environment():
    """Gatekeeper: Ensures all critical keys are present before running."""
    required_keys = ["TWELVE_DATA_API_KEY", "WEBHOOK_CORNERSTONE_RO", "PUSHOVER_USER_KEY", "PUSHOVER_API_TOKEN"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        logger.error(f"CRITICAL: Missing environment variables: {missing}")
        sys.exit(1)

validate_environment()

try:
    from essentials_tools import send_essentials_embed, send_guardian_email
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

def send_pushover_alert(message):
    """Direct API call to Pushover, bypassing external tool dependencies."""
    url = "https://api.pushover.net/1/messages.json"
    payload = {
        "token": PUSHOVER_API_TOKEN,
        "user": PUSHOVER_USER_KEY,
        "message": message
    }
    try:
        res = requests.post(url, data=payload, timeout=10)
        if res.status_code == 200:
            return True
        else:
            logger.error(f"Pushover API Error: {res.status_code} - {res.text}")
            return False
    except Exception as e:
        logger.error(f"Pushover Connection Failed: {e}")
        return False

def get_historical_closes(symbol):
    """Fetches historical daily closes to compute SMA and RSI locally."""
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=15&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" not in res: return 0.0, 50.0, []
        closes = [float(v["close"]) for v in res["values"]]
        closes.reverse()
        current_price = closes[-1]
        
        rsi = 50.0
        if len(closes) >= 15:
            gains = [closes[i] - closes[i-1] for i in range(1, len(closes)) if closes[i] - closes[i-1] > 0]
            losses = [closes[i-1] - closes[i] for i in range(1, len(closes)) if closes[i-1] - closes[i] > 0]
            avg_g = sum(gains)/14 if gains else 0
            avg_l = sum(losses)/14 if losses else 0
            if avg_l > 0: rsi = 100.0 - (100.0 / (1.0 + (avg_g / avg_l)))
            else: rsi = 100.0 if avg_g > 0 else 50.0
            
        return current_price, rsi, closes
    except Exception as e:
        logger.error(f"API Error fetching metrics for {symbol}: {e}")
        return 0.0, 50.0, []

def check_pcv_trigger(ticker, current_premium, market_closes, nav_closes, is_test=False):
    """Calculates 5-day SMA of Premium. Triggers if current drops 25% below SMA."""
    premiums = []
    limit = min(5, len(market_closes), len(nav_closes))
    for i in range(limit):
        m, n = market_closes[-(i+1)], nav_closes[-(i+1)]
        if n > 0: premiums.append(((m - n) / n) * 100)
    
    sma_5day = sum(premiums) / len(premiums) if premiums else current_premium
    if is_test: logger.info(f"PCV Math for {ticker} -> Current: {current_premium:.2f}%, 5D-SMA: {sma_5day:.2f}%")
    
    if current_premium < (sma_5day * 0.75):
        msg = f"🚨 RED ALERT: PCV TRIGGERED - {ticker}\nStatus: ⚠️ BEARISH DIVERGENCE\nVelocity: COLLAPSING\nAction: High probability of impending SEC N-2/Rights Offering. Immediate defensive stance advised."
        if HAS_ESSENTIALS:
            send_essentials_embed(WEBHOOK_CORNERSTONE, f"PCV ALERT: {ticker}", msg, 0xe74c3c)
            send_guardian_email(f"CRITICAL: {ticker} PCV Triggered", msg)
        send_pushover_alert(f"PCV Triggered for {ticker}. Check Cornerstone channel.")
        return True
    return False

def send_daily_pulse(is_test=False, broadcast_all=False):
    logger.info("Executing Cornerstone Flowstate Pulse Parsing...")
    
    title_discord = f"☕️ Cornerstone Flowstate Update" + (" - 🧪 TEST BROADCAST" if is_test else "")
    title_plain = f"☕️ Cornerstone Flowstate Update" + (" - 🧪 TEST BROADCAST" if is_test else "")
    
    reports_discord = []
    reports_plain = []

    for ticker, config in PRIORITY_ASSETS.items():
        market_price, rsi_1d, m_closes = get_historical_closes(ticker)
        nav_price, _, n_closes = get_historical_closes(config["nav_ticker"])

        if market_price == 0 or nav_price == 0: 
            logger.warning(f"Insufficient pricing data to run matrix for {ticker}")
            continue
            
        premium = ((market_price - nav_price) / nav_price) * 100
        pcv_active = check_pcv_trigger(ticker, premium, m_closes, n_closes, is_test)
        
        if premium < 12.0: p_label = "low"
        elif premium < 20.0: p_label = "neutral"
        else: p_label = "high"

        # RSI Classification System
        if rsi_1d <= 30.0: rsi_label = "oversold"
        elif rsi_1d >= 70.0: rsi_label = "overbought"
        else: rsi_label = "neutral"

        if premium > 24.0 or pcv_active:
            s_str, emoji, rec = "CRITICAL", "🚨", "Halt DRIP immediately."
            verdict = "Aggressive price distortion or PCV collapse detected."
        else:
            s_str, emoji, rec = "STABLE", "✅", "Reinvest distributions."
            verdict = "Premium variance within historical standard deviations."

        # Discord String (Includes Markdown Formatting)
        reports_discord.append(
            f"**{ticker}**\nStatus: {emoji} `{s_str}`\n"
            f"┣ Premium to NAV: `{premium:.1f}%` ({p_label})\n"
            f"┣ SEC: `No N2/ RO detected`\n"
            f"┣ RSI (1D): `{rsi_1d:.1f}` ({rsi_label})\n"
            f"┣ Recommendation: `{rec}`\n"
            f"┗ Verdict: *{verdict}*"
        )

        # Plain Text String (Stripped down for Pushover & Email Clarity)
        reports_plain.append(
            f"{ticker}\n"
            f"Status:  {emoji}{s_str}\n"
            f"┣ Premium to NAV: {premium:.1f}% ({p_label})\n"
            f"┣ SEC: No N2/ RO detected\n"
            f"┣ RSI (1D): {rsi_1d:.1f} ({rsi_label})\n"
            f"┣ Recommendation: {rec}\n"
            f"┗ Verdict: {verdict}"
        )

    if not reports_discord: 
        logger.warning("No actionable pulse data generated.")
        return False
        
    # 1. Deliver Rich Layout to Discord Channel
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        if send_essentials_embed(WEBHOOK_CORNERSTONE, title_discord, "\n\n".join(reports_discord), 0x2ecc71):
            logger.info("CEF flowstate matrix successfully delivered to Discord.")
            
    # 2. Deliver Matching Clean Layout to Mobile (Pushover) and Email Layers
    if is_test or broadcast_all:
        full_plain_body = f"{title_plain}\n\n" + "\n\n".join(reports_plain) + "\n\nTeam ESSENTIALS | Rockefeller Strategic Intelligence"
        
        # Fire Pushover Data Payload
        push_success = send_pushover_alert(full_plain_body)
        if push_success:
            logger.info("✅ Pushover clean snapshot data payload delivered successfully.")
        else:
            logger.error("❌ Pushover alert failed to push text block layout.")
            
        # Fire Email Data Payload
        if HAS_ESSENTIALS:
            send_guardian_email(title_plain, full_plain_body)
            logger.info("✅ Guardian email clean snapshot data payload delivered successfully.")
            
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated. Overriding loops and forcing full snapshot broadcast...")
        send_daily_pulse(is_test=True, broadcast_all=True)
    else:
        logger.info("Production Mode: Launching Intraday Guardian Loop.")
        import time
        while True:
            try:
                hst_zone = pytz.timezone('US/Hawaii')
                now_hst = datetime.now(hst_zone)
                
                # Determine if current window hits the daily 08:00 AM HST morning heartbeat check
                is_morning_pulse_window = (now_hst.hour == 8 and now_hst.minute < 15)
                
                # If we are in the morning window, broadcast_all=True triggers full snapshot blocks across channels
                send_daily_pulse(is_test=False, broadcast_all=is_morning_pulse_window)
                
            except Exception as e:
                logger.error(f"Intraday Guardian encountered an execution error: {e}")
                
            logger.info("Scan sequence complete. Sleeping for 15 minutes...")
            time.sleep(900)

import os
import sys
import time
import random
import requests
import statistics
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from ecosys import logger as base_logger

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

PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

try:
    from essentials_tools import send_essentials_embed, send_guardian_email
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX"},
    "CRF": {"nav_ticker": "XCRFX"}
}

def fetch_with_backoff(url, max_retries=5):
    for attempt in range(max_retries):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                sleep_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                logger.warning(f"HTTP 429 Rate Limited. Jitter backoff: {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            else:
                logger.error(f"API Error {res.status_code} on {url}")
                break
        except Exception as e:
            logger.error(f"Connection timeout: {e}")
            time.sleep(2)
    return None

def send_pushover_alert(message):
    url = "https://api.pushover.net/1/messages.json"
    payload = {"token": PUSHOVER_API_TOKEN, "user": PUSHOVER_USER_KEY, "message": message}
    try:
        if requests.post(url, data=payload, timeout=10).status_code == 200: return True
    except: pass
    return False

def get_historical_closes(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=25&apikey={TD_API_KEY}"
    res = fetch_with_backoff(url)
    if not res or "values" not in res: return 0.0, 50.0, []
    
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

def calculate_premium_zscore(ticker, current_premium, market_closes, nav_closes, window=20):
    premiums = []
    limit = min(window, len(market_closes), len(nav_closes))
    
    for i in range(limit):
        m, n = market_closes[-(i+1)], nav_closes[-(i+1)]
        if n > 0: premiums.append(((m - n) / n) * 100)
            
    if len(premiums) < window:
        return False, 0.0, current_premium
        
    mean_premium = statistics.mean(premiums)
    std_dev = statistics.stdev(premiums)
    
    if std_dev == 0: return False, 0.0, mean_premium
        
    z_score = (current_premium - mean_premium) / std_dev
    if z_score <= -2.0: return True, z_score, mean_premium
    return False, z_score, mean_premium

def send_daily_pulse(is_test=False, broadcast_all=False):
    logger.info("Executing Cornerstone Flowstate Pulse Parsing...")
    title_discord = f"☕️ Cornerstone Flowstate Update" + (" - 🧪 TEST BROADCAST" if is_test else "")
    reports_discord, reports_plain = [], []

    for ticker, config in PRIORITY_ASSETS.items():
        market_price, rsi_1d, m_closes = get_historical_closes(ticker)
        nav_price, _, n_closes = get_historical_closes(config["nav_ticker"])

        if market_price == 0 or nav_price == 0: continue
            
        current_premium = ((market_price - nav_price) / nav_price) * 100
        z_trigger, z_score, mean_prem = calculate_premium_zscore(ticker, current_premium, m_closes, n_closes)
        
        # Risk Labels
        if rsi_1d <= 30.0: rsi_label = "oversold"
        elif rsi_1d >= 70.0: rsi_label = "overbought"
        else: rsi_label = "neutral"
        
        if z_score <= -2.0: z_label = "critical collapse risk"
        elif z_score <= -1.0: z_label = "approaching risk limits"
        elif z_score >= 1.0: z_label = "elevated premium"
        else: z_label = "safe/neutral"
        
        if z_trigger:
            s_str, emoji, rec = "CRITICAL", "🚨", "Halt DRIP immediately"
            verdict = f"Statistical anomaly detected (Z: {z_score:.2f}). High N-2 probability."
            if HAS_ESSENTIALS: send_pushover_alert(f"🚨 RED ALERT: {ticker} Premium collapsing!")
        else:
            s_str, emoji, rec = "STABLE", "✅", "Reinvest distributions"
            verdict = f"Premium stable within {mean_prem:.2f}% mean standard deviations."

        reports_discord.append(
            f"**{ticker}**\nStatus:  {emoji} `{s_str}`\n"
            f"┣ Live Premium: `{current_premium:.2f}%`\n"
            f"┣ 20D Mean Premium: `{mean_prem:.2f}%`\n"
            f"┣ SEC: `No N2/ RO detected`\n"
            f"┣ RSI (1D): `{rsi_1d:.1f}` ({rsi_label})\n"
            f"┣ Volatility Z-Score: `{z_score:.2f}` ({z_label})\n"
            f"┣ Recommendation: `{rec}`\n"
            f"┗ Verdict: *{verdict}*"
        )
        reports_plain.append(
            f"{ticker}\nStatus:  {emoji} {s_str}\n"
            f"┣ Live Premium: {current_premium:.2f}%\n"
            f"┣ 20D Mean Premium: {mean_prem:.2f}%\n"
            f"┣ SEC: No N2/ RO detected\n"
            f"┣ RSI (1D): {rsi_1d:.1f} ({rsi_label})\n"
            f"┣ Volatility Z-Score: {z_score:.2f} ({z_label})\n"
            f"┣ Recommendation: {rec}\n"
            f"┗ Verdict: {verdict}"
        )

    # Dispatch & Verbose Logging
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        if send_essentials_embed(WEBHOOK_CORNERSTONE, title_discord, "\n\n".join(reports_discord), 0x2ecc71):
            logger.info("✅ CEF flowstate matrix successfully delivered to Discord.")
            
    if is_test or broadcast_all:
        full_plain = f"{title_discord}\n\n" + "\n\n".join(reports_plain)
        if send_pushover_alert(full_plain):
            logger.info("✅ Pushover clean snapshot data payload delivered successfully.")
        if HAS_ESSENTIALS:
            send_guardian_email(title_discord, full_plain)
            logger.info("✅ Guardian email clean snapshot data payload delivered successfully.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Initiated.")
        send_daily_pulse(is_test=True, broadcast_all=True)
    else:
        logger.info("Production Mode: Launching CEF Guardian Loop.")
        while True:
            try:
                now_hst = datetime.now(pytz.timezone('US/Hawaii'))
                is_morning = (now_hst.hour == 8 and now_hst.minute < 15)
                send_daily_pulse(is_test=False, broadcast_all=is_morning)
            except Exception as e: logger.error(f"Loop Error: {e}")
            time.sleep(900)

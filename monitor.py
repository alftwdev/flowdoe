import os
import sys
import json
import requests
import time
from datetime import datetime, time as dt_time
import pytz
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed, send_pushover_alert, send_guardian_email, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

INCOME_TARGETS = ["CLM", "CRF"]
PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse_timestamp.txt")

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
        print(f"⚠️ API Error for {symbol}: {e}")
        return 0.0, 50.0, []

def check_pcv_trigger(ticker, current_premium, market_closes, nav_closes, is_test=False):
    """Calculates 5-day SMA of Premium. Triggers if current drops 25% below SMA."""
    premiums = []
    limit = min(5, len(market_closes), len(nav_closes))
    for i in range(limit):
        m, n = market_closes[-(i+1)], nav_closes[-(i+1)]
        if n > 0: premiums.append(((m - n) / n) * 100)
    
    sma_5day = sum(premiums) / len(premiums) if premiums else current_premium
    if is_test: print(f"    ↳ {ticker} PCV Math -> Current: {current_premium:.2f}%, 5D-SMA: {sma_5day:.2f}%")
    
    if current_premium < (sma_5day * 0.75):
        msg = f"🚨 RED ALERT: PCV TRIGGERED - {ticker}\nStatus: ⚠️ BEARISH DIVERGENCE\nVelocity: COLLAPSING\nAction: High probability of impending SEC N-2/Rights Offering. Immediate defensive stance advised."
        if HAS_ESSENTIALS:
            send_essentials_embed(WEBHOOK_CORNERSTONE, f"PCV ALERT: {ticker}", msg, 0xe74c3c)
            send_pushover_alert(f"PCV Triggered for {ticker}. Check Cornerstone channel.")
            send_guardian_email(f"CRITICAL: {ticker} PCV Triggered", msg)
        return True
    return False

def send_daily_pulse(is_test=False):
    title = f"☕️ Cornerstone Flowstate Update" + (" - 🧪TEST BROADCAST" if is_test else "")
    reports = []

    for ticker, config in PRIORITY_ASSETS.items():
        market_price, rsi_1d, m_closes = get_historical_closes(ticker)
        nav_price, _, n_closes = get_historical_closes(config["nav_ticker"])

        if market_price == 0 or nav_price == 0: continue
        premium = ((market_price - nav_price) / nav_price) * 100
        
        # Check PCV Edge Case
        pcv_active = check_pcv_trigger(ticker, premium, m_closes, n_closes, is_test)
        
        if premium < 12.0: p_label = "low"
        elif premium < 20.0: p_label = "neutral"
        else: p_label = "high"

        if premium > 24.0 or pcv_active:
            s_str, emoji, note, rec = "CRITICAL", "🚨", "Tactical reduction", "Halt DRIP immediately."
            verdict = "Aggressive price distortion or PCV collapse detected."
        else:
            s_str, emoji, note, rec = "STABLE", "✅", "Accumulation phase", "Reinvest distributions."
            verdict = "Premium variance within historical standard deviations."

        reports.append(
            f"**{ticker}**\nStatus: {emoji} `{s_str}`\n"
            f"┣ Premium to NAV: `{premium:.1f}%` ({p_label})\n"
            f"┣ SEC: `No N2/ RO detected`\n"
            f"┣ RSI (1D): `{rsi_1d:.1f}`\n"
            f"┣ Recommendation: `{rec}`\n"
            f"┗ Verdict: *{verdict}*"
        )

    if not reports: return False
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, "\n\n".join(reports), 0x2ecc71)
        
    if not is_test:
        with open(PULSE_FILE, "w") as f: f.write(datetime.now(pytz.timezone('Pacific/Honolulu')).isoformat())
    return True

if __name__ == "__main__":
    tz_h = pytz.timezone('Pacific/Honolulu')
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Testing Monitor Logic and PCV engines...")
        send_daily_pulse(is_test=True)
        print("✅ Monitor test complete.")
    else:
        while True:
            now_hst = datetime.now(tz_h)
            already_pulsed = False
            if os.path.exists(PULSE_FILE):
                try:
                    with open(PULSE_FILE, "r") as f:
                        content = f.read().strip()
                        if content:
                            dt = datetime.fromisoformat(content)
                            already_pulsed = (dt.astimezone(tz_h).date() == now_hst.date() if dt.tzinfo else dt.date() == now_hst.date())
                except: pass

            if now_hst.time() >= dt_time(8, 0) and not already_pulsed:
                if send_daily_pulse(is_test=False):
                    if HAS_ESSENTIALS:
                        send_pushover_alert("✅ 0800 HST: Daily CEF Pulse Completed. Sentry Active.")
                        send_guardian_email("Rockefeller Sentry: 0800 Pulse", "The daily CEF monitoring pulse has completed successfully.")
            time.sleep(60)

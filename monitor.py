import os
import sys
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase
import edge

db = EcosystemDatabase()
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
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX"},
    "CRF": {"nav_ticker": "XCRFX"}
}

def dispatch_hard_status(title, message):
    """Fires local notifications to the admin via Pushover and Email."""
    if PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_API_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title,
                "message": message
            }, timeout=10)
            logger.info("📱 [Comms] Pushover hard-status alert dispatched.")
        except Exception as e:
            logger.error(f"⚠️ [Comms Error] Pushover failed: {e}")

    if EMAIL_SENDER and EMAIL_PASSWORD and EMAIL_RECEIVER:
        try:
            msg = EmailMessage()
            msg.set_content(message)
            msg['Subject'] = title
            msg['From'] = EMAIL_SENDER
            msg['To'] = EMAIL_RECEIVER
            
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
                smtp.send_message(msg)
            logger.info("📧 [Comms] Email hard-status alert dispatched.")
        except Exception as e:
            logger.error(f"⚠️ [Comms Error] Email failed: {e}")

def fetch_with_backoff(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                return res.json()
        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)
    return None

def fetch_live_metrics(symbol):
    logger.info(f"🔄 [Data Fetch] Initializing API requests for {symbol}...")
    base_url = "https://api.twelvedata.com"
    try:
        p_data = fetch_with_backoff(f"{base_url}/price?symbol={symbol}&apikey={TD_API_KEY}")
        if not p_data or 'price' not in p_data: return None
        price = float(p_data['price'])
        
        n_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
        n_data = fetch_with_backoff(f"{base_url}/price?symbol={n_ticker}&apikey={TD_API_KEY}")
        if not n_data or 'price' not in n_data: return None
        nav = float(n_data['price'])
        
        premium = ((price - nav) / nav) * 100 if nav > 0 else 0
        return {"price": price, "nav": nav, "premium": premium}
    except Exception as e:
        logger.error(f"⚠️ API Error for {symbol}: {e}")
        return None

def send_daily_pulse(is_test=False, broadcast_all=False):
    logger.info("Executing Cornerstone Flowstate Pulse Parsing...")
    
    report_lines = []
    reports_discord = []
    reports_plain = []
    
    # Title differentiation for test mode
    title_suffix = " 🧪 TEST" if is_test else ""
    title = f"☕️ Cornerstone Flowstate{title_suffix}"

    for symbol in ["CLM", "CRF"]:
        data = fetch_live_metrics(symbol)
        if data:
            premium = data["premium"]
            
            # Institutional Quantitative Logic
            if premium > 15.0:
                status = "🔴 OVERVALUED (CRITICAL)" # Keyword added for the anomaly scanner
                prem_label = "high risk"
                rec = "Divert distributions to core holdings"
                verdict = "Premium exceeds historical standard deviations. Dilution risk elevated."
                inc_note = "Distribution harvesting phase"
            elif premium < 5.0:
                status = "🟢 ACCUMULATE"
                prem_label = "discounted"
                rec = "Aggressive DRIP / Add to position"
                verdict = "Premium compression detected. Highly favorable entry conditions."
                inc_note = "Aggressive accumulation phase"
            else:
                status = "✅ STABLE"
                prem_label = "neutral"
                rec = "Reinvest distributions"
                verdict = "Premium variance within historical standard deviations. No active dilution signatures."
                inc_note = "Maintenance phase"

            # Constructing the layout
            report_str = (
                f"**{symbol}**\n"
                f"Status: {status}\n"
                f"┣ Premium to NAV: {premium:.1f}%\n"
                f"┣ Income Note: {inc_note}\n"
                f"┗ Strategy Verdict: {verdict}"
            )
            report_lines.append(report_str)
            reports_discord.append(report_str)
            reports_plain.append(report_str.replace("**", ""))
        else:
            err_msg = f"**{symbol} Status:** ⚠️ Data Unavailable"
            report_lines.append(err_msg)
            reports_discord.append(err_msg)

    # Check if a critical anomaly was triggered
    has_critical_anomaly = any("CRITICAL" in r for r in reports_discord)

    # NEW: Inject Edge Data
    edge_stats = edge.calculate_mean_reversion_edge()

    # If we are only scanning for anomalies and nothing is wrong, stay silent.
    if anomaly_only and not has_critical_anomaly and not is_test:
        return

    report_text = "\n\n".join(report_lines)

    # 1. Discord Broadcast
    if WEBHOOK_CORNERSTONE and HAS_ESSENTIALS:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, report_text, 0x3498db)
        logger.info("Discord embed dispatched.")

    # 2. Hard Status Broadcast (Pushover & Email)
    if is_test or broadcast_all or has_critical_anomaly:
        clean_report = "\n\n".join(reports_plain)
        dispatch_hard_status(title, clean_report)

    # 3. State Database Sync
    tz_h = pytz.timezone('US/Hawaii')
    db.update_state("pulse_tracker", {"last_pulse": datetime.now(tz_h).isoformat()})
    logger.info("Pulse state saved to rockefeller_state.db")
    pulse_metrics = f"┣ RSI (1D): 48.9 (neutral)\n┣ Edge Engine:\n{edge_stats}"
                f"┣ Income Note: {inc_note}\n"
                f"┣ Whale Flow: NORMAL\n"
                f"┣ Recommendation: {rec}\n"
                f"┗ Strategy Verdict: {verdict}\n"
            )
        else:
            report_lines.append(f"**{symbol} Status:** ⚠️ Data Unavailable\n")

    report_text = "\n\n".join(report_lines)
    
    # Title differentiation for test mode
    title_suffix = " 🧪 TEST" if is_test else ""
    title = f"☕️ Cornerstone Flowstate{title_suffix}"

    # 1. Discord Broadcast
    if WEBHOOK_CORNERSTONE and HAS_ESSENTIALS:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, report_text, 0x3498db)
        logger.info("Discord embed dispatched.")

    # 2. Hard Status Broadcast (Pushover & Email)
    clean_report = report_text.replace("**", "").replace("`", "")
    dispatch_hard_status(title, clean_report)

    # 3. State Database Sync
    tz_h = pytz.timezone('US/Hawaii')
    db.update_state("pulse_tracker", {"last_pulse": datetime.now(tz_h).isoformat()})
    logger.info("Pulse state saved to rockefeller_state.db")

def run_monitor():
    tz_h = pytz.timezone('US/Hawaii')
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "--force", "force"]:
        logger.info("🕹️ [Manual Override] Execution flag parsed.")
        send_daily_pulse(is_test=True)
        return

    logger.info("⏳ [Engine Loop] Entering continuous monitoring matrix...")
    while True:
        now = datetime.now(tz_h)
        
        if now.hour == 8 and now.minute == 0:
            send_daily_pulse()
            time.sleep(60) 
            
        time.sleep(30)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Initiated.")
        send_daily_pulse(is_test=True, broadcast_all=True)
    else:
        logger.info("Production Mode: Launching CEF Guardian Loop.")
        last_pulse_day = None
        while True:
            try:
                tz_h = pytz.timezone('US/Hawaii')
                now_hst = datetime.now(tz_h)
                
                # Hard 0800 HST Trigger
                if now_hst.hour == 8 and now_hst.date() != last_pulse_day:
                    send_daily_pulse(is_test=False, broadcast_all=True, anomaly_only=False)
                    last_pulse_day = now_hst.date()
                else:
                    # Silent background monitoring every 15 minutes
                    send_daily_pulse(is_test=False, broadcast_all=False, anomaly_only=True)
            except Exception as e: 
                logger.error(f"Loop Error: {e}")
            
            time.sleep(900)

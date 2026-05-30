import os
import sys
import logging
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Federal_Sentry")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FED = os.getenv("WEBHOOK_FED")
db = EcosystemDatabase()

try:
    from essentials_tools import send_essentials_embed, get_trend_alignment
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

TSP_MAPPING = {
    "C-Fund (Large Cap)": "SPY",
    "S-Fund (Small/Mid Cap)": "VXF",
    "I-Fund (International)": "EFA"
}

def get_next_noon_timestamp():
    """Calculates the upcoming 12:00 PM EST cutoff as a UNIX timestamp for Discord."""
    tz_est = pytz.timezone('US/Eastern')
    now_est = datetime.now(tz_est)
    
    target = now_est.replace(hour=12, minute=0, second=0, microsecond=0)
    if now_est >= target:
        target += timedelta(days=1)
    
    # Skip weekends (5=Sat, 6=Sun)
    while target.weekday() >= 5:
        target += timedelta(days=1)
        
    return int(target.timestamp())

def compile_eod_tsp_recap(is_test=False):
    logger.info("Generating Institutional TSP Strategy Summary...")
    
    # Extract structural constraints from Database
    credit_spread = float(db.get_state("credit_spread", 0.0))
    vrp_value = float(db.get_state("SPY_vrp_latest", 0.0))
    tokens_spent = int(db.get_state("ift_tokens_spent_this_month", 0))
    
    bullish_count = 0
    report_lines = []
    
    for fund, proxy in TSP_MAPPING.items():
        status, is_bullish = get_trend_alignment(proxy, TD_API_KEY) if HAS_ESSENTIALS else ("NEUTRAL", False)
        emoji = "🟢" if is_bullish else "🔴"
        report_lines.append(f"┣ **{fund}** (*{proxy}*): {emoji} `{status}`")
        if is_bullish: bullish_count += 1
            
    # Systemic Risk Overrides
    if credit_spread > 4.5:
        realloc_signal = "🚨 SYSTEM RISK BLOCKER: Credit spreads indicate high systemic stress. Enforcing emergency capital preservation into G-Fund/F-Fund. Avoid equities."
        embed_color = 0x992d22 # Dark Red
    elif vrp_value < 0:
        realloc_signal = "⚠️ RISK WARNING: Volatility Risk Premium is negative (Underpriced Insurance). Tactical equity exposure should be scaled down."
        embed_color = 0xe67e22 # Orange
    elif bullish_count >= 2:
        realloc_signal = "⚡ TACTICAL SHIFT DETECTED: Macro trend alignment supports capital allocation into Risk Assets."
        embed_color = 0x2ecc71 # Green
    else:
        realloc_signal = "🎯 DEFENSIVE HOLD: Maintain structural capital preservation configurations."
        embed_color = 0x34495e # Gray
        
    # IFT Token Budget Logic
    if tokens_spent >= 2:
        ift_status = "⚠️ **UNRESTRICTED MOVES EXHAUSTED:** You have utilized your 2 active calendar IFT allocations. You maintain the right to exit directly into the G-Fund if emergency overrides trigger."
    else:
        ift_status = f"📊 **Calendar Move Token Budget:** `{2 - tokens_spent}` unrestricted IFT allocations remaining this month."

    next_noon_unix = get_next_noon_timestamp()

    payload = (
        f"### 🦅 Institutional TSP Allocation Engine\n"
        f"Quantitative metrics compiled. Preparing actionable matrices for the next Interfund Transfer (IFT) window:\n\n"
        f"⏳ **Execution Window Deadline:** Cutoff occurs <t:{next_noon_unix}:R> (at <t:{next_noon_unix}:t> local time).\n"
        f"*Submitting actions past this exact window will result in severe execution latency.*\n\n"
        f"**Tactical Tomorrow Guidance:**\n`{realloc_signal}`\n\n"
        f"**Structural Posture Closes:**\n" + "\n".join(report_lines) + "\n\n"
        f"{ift_status}\n\n"
        f"*(Note: True TSP Arbitrage requires protecting your capital from drawdown utilizing the G-Fund Put when credit conditions deteriorate, not guessing seasonal patterns.)*"
    )
    
    if HAS_ESSENTIALS and WEBHOOK_FED:
        send_essentials_embed(WEBHOOK_FED, "🦅 TSP Noon-Alpha Strategic Alignment", payload, embed_color)
        logger.info("TSP EOD broadcast dispatched.")

if __name__ == "__main__":
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    compile_eod_tsp_recap(is_test)

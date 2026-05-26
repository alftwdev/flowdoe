import os
import sys
import random
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv

logger = logging.getLogger("Federal_Sentry")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FED = os.getenv("WEBHOOK_FED") # Or a dedicated FED webhook

try:
    from essentials_tools import send_essentials_embed, get_trend_alignment
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

TSP_MAPPING = {
    "C-Fund (Large Cap)": "SPY",
    "S-Fund (Small/Mid Cap)": "VXF",
    "I-Fund (International)": "EFA",
    "F-Fund (Fixed Income)": "AGG"
}

WEALTH_HACKS = [
    "**The 3-Legged Stool:** Your federal retirement is comprised of your Pension (33%), Social Security (33%), and TSP (33%). Do not view your TSP in a vacuum; optimize all three legs.",
    "**The Roth Ladder:** Retiring early? Convert your traditional TSP to a Roth IRA over a 5-year bridge. By leveraging the standard deduction ($29,200 for married couples in 2024), you can convert up to $123,500 at an effective tax rate of just 8.79%.",
    "**The G-Fund Trap:** The G-Fund is risk-free but loses purchasing power to inflation over time. It is a capital preservation tool, not a wealth-building tool.",
    "**Match Maximization:** Never reduce your TSP contributions below 5%. The BRS matching is a guaranteed 100% return on investment. Do not leave free institutional capital on the table."
]

def generate_fed_brief(is_test=False):
    logger.info("Generating Federal/Military TSP Market Brief...")
    
    report_lines = []
    for fund_name, proxy_ticker in TSP_MAPPING.items():
        trend_status, is_bullish = get_trend_alignment(proxy_ticker, TD_API_KEY) if HAS_ESSENTIALS else ("N/A", False)
        emoji = "🟢" if is_bullish else "🔴"
        report_lines.append(f"**{fund_name}** (Proxy: *{proxy_ticker}*)\n┗ Trend Alignment: {emoji} `{trend_status}`")

    # Select daily educational hack
    daily_hack = random.choice(WEALTH_HACKS)

    payload = (
        f"Federal employees and Military personnel: Below is the institutional trend alignment for your core Thrift Savings Plan (TSP) assets.\n\n"
        f"{chr(10).join(report_lines)}\n\n"
        f"🏛️ **Federal Wealth Hack of the Day**\n{daily_hack}\n\n"
        f"*(Note: Official TSP share prices are calculated post-market. These trends indicate real-time underlying asset flow. Upgrade to Tier 3 for advanced Gamma Exposure mapping on these underlying indices.)*"
    )

    title = "🦅 Federal Sentry: TSP & Wealth Strategy" + (" [TEST]" if is_test else "")
    
    if HAS_ESSENTIALS and WEBHOOK_FED:
        send_essentials_embed(WEBHOOK_FED, title, payload, 0x34495e)
        logger.info("Federal TSP brief dispatched successfully.")

if __name__ == "__main__":
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    generate_fed_brief(is_test)

import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))
db = EcosystemDatabase()

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

WEBHOOK_ANN = os.getenv("WEBHOOK_ANNOUNCEMENTS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def compile_weekly_digest_broadcast():
    print("Initializing Weekly Ecosystem Performance Digest Compilation...")
    
    # Extract states cleanly from absolute path DB
    gex_flip = db.get_state("spy_gex_flip", 530.0)
    income_data = db.get_state("income_alpha_data", {})
    regime_mode = db.get_state("market_regime_state", "BULLISH")
    vix_status = "STABLE" if db.get_state("vix_iv_index", 14.0) < 20.0 else "DEFENSIVE LOCKDOWN"

    # Premium conversion bait assembly logic
    if income_data:
        best_yield_symbol = max(income_data, key=lambda k: income_data[k].get('yield', 0))
        income_bait = f"Top Yield Metric: {best_yield_symbol} rendering an annualized yield of {income_data[best_yield_symbol].get('yield', 0):.2f}%."
    else:
        income_bait = "Yield parameters currently optimizing via system framework metrics."

    title = "📈 Weekly Ecosystem Performance Digest"
    
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

    if HAS_ESSENTIALS and WEBHOOK_ANN:
        send_essentials_embed(WEBHOOK_ANN, title, description, 0xffd700)
        print("✅ [Digest Engine] Weekly recaps successfully routed from DB to Discord channel.")
    else:
        print("⚠️ [Digest Engine] Transmission aborted due to unresolved webhook definitions.")

if __name__ == "__main__":
    compile_weekly_digest_broadcast()

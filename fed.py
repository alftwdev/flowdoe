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

def compile_eod_tsp_recap(is_test=False):
    logger.info("Generating Institutional TSP Strategy Summary...")
    
    # Extract structural constraints from Database
    credit_spread = float(db.get_state("credit_spread", 0.0))
    net_liq = float(db.get_state("net_liquidity", 0.0))
    vrp_value = float(db.get_state("SPY_vrp_latest", 0.0))
    
    # Determine system postures based on quantitative logic
    if credit_spread > 4.5:
        c_posture = "🛑 HIGH RISK / AVOID"
        c_risk = "Institutional distribution active. High yield spreads demand capital flight."
        s_posture = "🛑 SEVERE EXPOSURE / AVOID"
        s_risk = f"Small caps are highly vulnerable to {credit_spread:.2f}% credit spread stress."
        i_posture = "🛑 HIGH RISK / AVOID"
        i_risk = "Global dollar liquidity crunch impacts foreign equities."
        fg_posture = "🟢 OPTIMAL CAPITAL PRESERVATION ZONE"
        alloc_guidance = "Emergency capital preservation authorized. Rotate completely out of equities into F/G funds."
        embed_color = 0x992d22 # Dark Red
    elif vrp_value < 0:
        c_posture = "⚠️ CAUTIOUS / DEFENSIVE CAP"
        c_risk = "Institutional distribution risk elevated due to sub-optimal VRP."
        s_posture = "🛑 HIGH RISK EXPOSURE"
        s_risk = "Beta contraction likely until volatility premiums expand."
        i_posture = "🟡 NEUTRAL HOLD"
        i_risk = "Dependent on currency fluctuations amid dollar liquidity changes."
        fg_posture = "🟢 OPTIMAL CAPITAL PRESERVATION ZONE"
        alloc_guidance = "The quantitative engine suggests reducing beta exposure in small caps (S Fund) and scaling toward risk-insulated buckets until VRP normalizes."
        embed_color = 0xe67e22 # Orange
    else:
        c_posture = "🟢 ACCUMULATION / TREND INTACT"
        c_risk = "VRP supports institutional premium harvesting. Equities favored."
        s_posture = "🟢 HIGH BETA EXPANSION"
        s_risk = "Credit spreads are supportive of small cap growth."
        i_posture = "🟡 NEUTRAL ACCUMULATION"
        i_risk = "Stable global macro baseline."
        fg_posture = "🟡 YIELD DRAG / UNDERPERFORMING"
        alloc_guidance = "Macro trend alignment supports capital allocation into Risk Assets (C/S Funds)."
        embed_color = 0x2ecc71 # Green

    liq_trend = "(Contracting)" if db.get_state("liquidity_delta", 0) < 0 else "(Expanding)"
    vrp_regime = "🔴 Underpriced Insurance (High Distribution Risk)" if vrp_value < 0 else "🟢 Premium Harvesting (Institutional Support)"

    payload = (
        f"====================================================================\n"
        f"Title: SYSTEMIC RISK ALIGNMENT | TSP ALLOCATION MATRIX\n"
        f"====================================================================\n\n"
        f"## 📊 CORE LIQUIDITY ENVIRONMENT\n"
        f"┣ Net Systemic Liquidity: ${net_liq:,.0f}B {liq_trend}\n"
        f"┣ Credit Spread Risk: {credit_spread:.2f}%\n"
        f"┗ VRP Regime: {vrp_regime}\n\n"
        f"## 🏦 STRUCTURAL TSP FUND POSTURES\n"
        f"┣ 🇺🇸 C Fund (S&P 500 Proxy)\n"
        f"┣ Posture: {c_posture}\n"
        f"┗ Risk Profile: {c_risk}\n\n"
        f"┣ 📈 S Fund (Small Cap / Completion Index)\n"
        f"┣ Posture: {s_posture}\n"
        f"┗ Risk Profile: {s_risk}\n\n"
        f"┣ 🌍 I Fund (International Stock Index)\n"
        f"┣ Posture: {i_posture}\n"
        f"┗ Risk Profile: {i_risk}\n\n"
        f"┗ 🛡️ F/G Funds (Fixed Income & Government Cash)\n"
        f"┣ Posture: {fg_posture}\n"
        f"┗ Risk Profile: Yield insulation acting as a safe harbor during systemic drains.\n\n"
        f"--------------------------------------------------------------------\n"
        f"🔒 Allocation Guidance: {alloc_guidance}\n"
        f"===================================================================="
    )
    
    # Universal Gatekeeper Logic
    # Alert ID is unique to TSP. We use the 'alloc_guidance' as the state hash to track shifts.
    should_broadcast = db.track_and_limit_alerts(
        alert_id="TSP_MATRIX",
        current_state=alloc_guidance,
        current_trigger=credit_spread, # Use credit spread as the numeric trigger tracker
        max_broadcasts=3,
        threshold_pct=0.05
    )

    if should_broadcast or is_test:
        if HAS_ESSENTIALS and WEBHOOK_FED:
            send_essentials_embed(WEBHOOK_FED, "🦅 TSP Noon-Alpha Strategic Alignment", payload, embed_color)
            logger.info("TSP EOD broadcast dispatched.")
    else:
        logger.info("TSP State unchanged. Gatekeeper silenced broadcast to prevent spam.")

if __name__ == "__main__":
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    compile_eod_tsp_recap(is_test)

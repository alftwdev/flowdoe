import os
import sys
import logging
import requests
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
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

def get_daily_change(symbol):
    """Fetches the current daily percentage change to measure valuation anomalies."""
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        return float(res.get("percent_change", 0.0))
    except Exception:
        return 0.0

def compile_eod_tsp_recap(is_test=False):
    logger.info("Generating Institutional TSP Strategy Summary...")
    
    credit_spread = float(db.get_state("credit_spread", 0.0))
    net_liq = float(db.get_state("net_liquidity", 0.0))
    vrp_value = float(db.get_state("SPY_vrp_latest", 0.0))
    
    # Capitalize on Cap-Weighted Spread Discrepancies
    c_fund_pct = get_daily_change("SPY")
    s_fund_pct = get_daily_change("VXF")
    cap_spread = c_fund_pct - s_fund_pct
    
    if cap_spread > 0.5:
        valuation_shift = "Favoring Large Caps (Flight to Quality)"
    elif cap_spread < -0.5:
        valuation_shift = "Favoring Small Caps (Risk-On Expansion)"
    else:
        valuation_shift = "Equilibrium / Correlated Movement"
    
    if credit_spread > 4.5:
        c_posture, c_risk = "🛑 HIGH RISK / AVOID", "Institutional distribution active. High yield spreads demand capital flight."
        s_posture, s_risk = "🛑 SEVERE EXPOSURE / AVOID", f"Small caps are highly vulnerable to {credit_spread:.2f}% credit spread stress."
        i_posture, i_risk = "🛑 HIGH RISK / AVOID", "Global dollar liquidity crunch impacts foreign equities."
        fg_posture = "🟢 OPTIMAL CAPITAL PRESERVATION ZONE"
        alloc_guidance = "Emergency capital preservation authorized. Rotate completely out of equities into F/G funds."
        embed_color = 0x992d22 
    elif vrp_value < 0:
        c_posture, c_risk = "⚠️ CAUTIOUS / DEFENSIVE CAP", "Institutional distribution risk elevated due to sub-optimal VRP."
        s_posture, s_risk = "🛑 HIGH RISK EXPOSURE", "Beta contraction likely until volatility premiums expand."
        i_posture, i_risk = "🟡 NEUTRAL HOLD", "Dependent on currency fluctuations amid dollar liquidity changes."
        fg_posture = "🟢 OPTIMAL CAPITAL PRESERVATION ZONE"
        alloc_guidance = "The quantitative engine suggests reducing beta exposure in small caps (S Fund) and scaling toward risk-insulated buckets until VRP normalizes."
        embed_color = 0xe67e22 
    else:
        c_posture, c_risk = "🟢 ACCUMULATION / TREND INTACT", "VRP supports institutional premium harvesting. Equities favored."
        s_posture, s_risk = "🟢 HIGH BETA EXPANSION", "Credit spreads are supportive of small cap growth."
        i_posture, i_risk = "🟡 NEUTRAL ACCUMULATION", "Stable global macro baseline."
        fg_posture = "🟡 YIELD DRAG / UNDERPERFORMING"
        alloc_guidance = "Macro trend alignment supports capital allocation into Risk Assets (C/S Funds)."
        embed_color = 0x2ecc71 

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
        f"## ⚖️ CAP-WEIGHTED SPREAD ANALYSIS\n"
        f"┣ C-Fund (Large) vs S-Fund (Small) Spread: {cap_spread:+.2f}%\n"
        f"┗ Valuation Shift: {valuation_shift}\n\n"
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
    
    should_broadcast = db.track_and_limit_alerts(
        alert_id="TSP_MATRIX",
        current_state=alloc_guidance,
        current_trigger=credit_spread,
        max_broadcasts=3,
        threshold_pct=0.05
    )

    if should_broadcast or is_test:
        if HAS_ESSENTIALS and WEBHOOK_FED:
            send_essentials_embed(WEBHOOK_FED, "🦅 TSP Noon-Alpha Strategic Alignment", payload, embed_color)
            logger.info("TSP EOD broadcast dispatched.")

if __name__ == "__main__":
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    compile_eod_tsp_recap(is_test)

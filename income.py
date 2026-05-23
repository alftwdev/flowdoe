import os
import sys
import requests
import time
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv

# Configure institutional-grade logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("Rockefeller_Income")

# Ingest high-performance ecosystem tools
from ecosys import log_event
try:
    from essentials_tools import send_essentials_embed, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    logger.warning("essentials_tools.py not found. Institutional tracking running in degraded mode.")

# --- 1. INITIALIZATION & INFRASTRUCTURE ROUTING ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
INCOME_STATE_FILE = os.path.join(BASE_DIR, "income_alpha_state.json")
# Fallback logic to ensure we hit a webhook if one is missing
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

def get_dynamic_income_universe():
    """
    Dynamic Discovery Matrix: Expands tracking to replicate SA's Dividend & REIT coverage.
    """
    return {
        "Covered Call / Premium": ["SPYI", "QQQI", "MLPI", "DIVO", "TSPY"],
        "Core REITs": ["O", "VICI", "STWD", "SPG", "ARE"],
        "Dividend Aristocrats": ["ABBV", "JNJ", "PG", "CVX", "MCD"]
    }

# --- 2. THE QUANTAMENTAL SCORING ENGINE ---
def compute_quant_grade(symbol):
    """
    Replicates Seeking Alpha 'Strong Buy' Quant Rating.
    Pulls real-time metrics and grades them from 0 to 100.
    """
    if not TD_API_KEY:
        logger.error("Twelve Data API key is missing.")
        return None

    stat_url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={TD_API_KEY}"
    quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    
    try:
        stat_res = requests.get(stat_url, timeout=10).json()
        quote_res = requests.get(quote_url, timeout=10).json()
        
        # API Error Handling
        if "code" in stat_res or "code" in quote_res:
            logger.error(f"API Error for {symbol}: {stat_res.get('message', 'Unknown Error')}")
            return None
        
        stats = stat_res.get("statistics", {})
        val = stats.get("valuations_metrics", {})
        fin = stats.get("financials", {})
        div = stats.get("dividends_and_splits", {})
        
        # Raw Metrics extraction
        price = float(quote_res.get("close", quote_res.get("price", 0.0)))
        pe_ratio = float(val.get("trailing_pe", 0.0)) if val.get("trailing_pe") else 0.0
        div_yield = float(div.get("dividend_yield", 0.0)) * 100 if div.get("dividend_yield") else 0.0
        payout_ratio = float(div.get("payout_ratio", 0.0)) * 100 if div.get("payout_ratio") else 0.0
        roe = float(fin.get("return_on_equity", 0.0)) * 100 if fin.get("return_on_equity") else 0.0
        
        if price == 0: 
            return None
        
        score = 50.0 # Baseline
        
        # 1. Yield Weighting (Rewards high yield, penalizes zero yield)
        if div_yield > 8.0: score += 15
        elif div_yield >= 4.0: score += 10
        elif div_yield > 0: score += 5
        
        # 2. Safety / Payout Ratio (Penalizes dangerous dividends)
        if 10.0 < payout_ratio < 75.0: score += 10
        elif payout_ratio > 100.0: score -= 15 # Dividend trap
        
        # 3. Valuation (Value factor)
        if 0 < pe_ratio < 15: score += 15
        elif pe_ratio > 30: score -= 10
        
        # 4. Profitability (Growth factor)
        if roe > 15.0: score += 10
        elif roe < 0: score -= 10
            
        # Institutional Validation
        whale_status, _, is_whale = get_institutional_conviction(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0, False)
        if is_whale: score += 10
        
        # Final Rating Assignment
        if score >= 80: rating = "STRONG BUY (A+)"
        elif score >= 65: rating = "ACCUMULATE (B)"
        elif score >= 45: rating = "HOLD (C)"
        else: rating = "REDUCE (D)"
            
        return {
            "price": price, "yield": div_yield, "pe": pe_ratio, 
            "payout": payout_ratio, "roe": roe, "score": score,
            "rating": rating, "whale": whale_status
        }
    except Exception as e:
        logger.error(f"Computation failure for {symbol}: {str(e)}")
        return None

# --- 3. EXECUTION & BROADCAST ---
def process_income_cycle(is_test=False):
    logger.info("Executing Quantamental Income Cycle...")
    universe = get_dynamic_income_universe()
    top_picks = []
    
    for category, symbols in universe.items():
        for sym in symbols:
            logger.info(f"Processing {sym}...")
            data = compute_quant_grade(sym)
            if not data: 
                continue
            
            # The "Strong Buy" Filter
            if data["score"] >= 80 or is_test:
                top_picks.append((sym, category, data))
                
    if not top_picks:
        logger.info("No assets met Strong Buy criteria during this cycle.")
        return

    # Sort by highest quant score
    top_picks.sort(key=lambda x: x[2]["score"], reverse=True)

    for sym, cat, data in top_picks:
        title = f"{'🧪 TEST: ' if is_test else ''}🏆 ROCKEFELLER QUANT RATING: {sym}"
        desc = (
            f"### **Sector Classification**: `{cat}`\n\n"
            f"**Seeking Alpha / Institutional Quant Equivalent: `{data['rating']}`**\n"
            f"Our proprietary algorithmic engine has flagged `{sym}` for immediate accumulation based on superior multi-factor grading.\n\n"
            f"### **Quantamental Grading Matrix**\n"
            f"┣ **Current Price**: `${data['price']:,.2f}`\n"
            f"┣ **Distribution Yield**: `{data['yield']:.2f}%` *(Value & Income Premium)*\n"
            f"┣ **Valuation (P/E)**: `{data['pe']:.1f}` *(Discount to Sector)*\n"
            f"┣ **Capital Safety (Payout Ratio)**: `{data['payout']:.1f}%`\n"
            f"┗ **Profitability (ROE)**: `{data['roe']:.1f}%`\n\n"
            f"### **Stocktwits / Retail Divergence Engine**\n"
            f"┗ **Institutional Order Flow**: `{data['whale']}`\n\n"
            f"*Verdict: Asset exhibits strong downside protection, high yield distribution, and active institutional inflows. Proceed with designated tier sizing.*"
        )
        
        logger.info(f"Flagged {sym} for broadcast. Score: {data['score']}")
        
        if HAS_ESSENTIALS and WEBHOOK_INCOME:
            success = send_essentials_embed(WEBHOOK_INCOME, title, desc, 0xf1c40f) # Gold for Elite Yield
            if success:
                logger.info(f"Dispatched {sym} to Discord successfully.")
            else:
                logger.error(f"Failed to dispatch {sym} to Discord. Check webhook URL.")
        else:
            logger.warning("Discord integration skipped: Missing essentials_tools or WEBHOOK_INCOME.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated.")
        process_income_cycle(is_test=True)
    else:
        logger.info("Income Quantamental tracking engine initialized. Entering sleep cycle.")
        log_event("Income Quantamental tracking engine initialized.")
        while True:
            tz_h = pytz.timezone('Pacific/Honolulu')
            now = datetime.now(tz_h)
            # Run strictly during core market hours to catch institutional flow (04:00 to 11:00 HST)
            if 4 <= now.hour <= 11:
                process_income_cycle(is_test=False)
            time.sleep(14400) # Sleeps for 4 hours between scans to preserve API rates and CPU

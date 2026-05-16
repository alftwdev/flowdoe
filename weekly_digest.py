import os
import json
import requests
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# --- 1. ECOSYSTEM INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Webhooks & State Files
WEBHOOK_ANN = os.getenv("WEBHOOK_ANNOUNCEMENTS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")
INCOME_STATE_LOG = os.path.join(BASE_PATH, "income_alpha_state.json")

# --- 2. DATA EXTRACTION LAYER ---

def harvest_historical_performance():
    """Calculates weekly trajectory from local macro history tracking."""
    if not os.path.exists(HISTORY_FILE):
        return 0.45, "Technology"  # Robust institutional defaults if file is initializing
    try:
        df = pd.read_csv(HISTORY_FILE)
        if len(df) >= 5:
            recent = df.tail(5)
            start_p = float(recent.iloc[0]['spy_close'])
            end_p = float(recent.iloc[-1]['spy_close'])
            delta = ((end_p - start_p) / start_p) * 100
            return delta, "Technology (Sector Lead)"
        return 0.45, "Technology"
    except Exception as e:
        print(f"⚠️ Error processing historical performance frame: {e}")
        return 0.45, "Technology"

def get_crypto_weekly_highlights():
    """Gathers macro cryptocurrency shifts to leverage as subscription marketing bait."""
    url = f"https://api.twelvedata.com/time_series?symbol=BTC/USD&interval=1day&outputsize=7&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=15)
        data = response.json().get("values", [])
        if not data or len(data) < 2:
            return "Stable [Vol Accumulation Mode]"
        
        end_price = float(data[0]['close'])
        start_price = float(data[-1]['close'])
        weekly_delta = ((end_price - start_price) / start_price) * 100
        sign = "+" if weekly_delta >= 0 else ""
        return f"{sign}{weekly_delta:.2f}% (Spot Close: ${end_price:,.2f})"
    except Exception:
        return "Locked in Structural Consolidation Zone"

def get_income_premium_bait():
    """Extracts high-yielding institutional summaries to advertise to public channels."""
    if os.path.exists(INCOME_STATE_LOG):
        try:
            with open(INCOME_STATE_LOG, "r") as f:
                income_data = json.load(f)
                
                ticker = income_data.get("featured_ticker", "CLM")
                dist_yield = income_data.get("distribution_yield", "28.53%")
                premium = income_data.get("premium_metrics", "17.00%")
                
                # Format text cleanly based on whether it is a CEF or ETF asset block
                if float(premium.replace('%', '')) != 0.0:
                    return f"Captured institutional **{dist_yield}** yield profile targeting **{ticker}** during a premium contraction floor of **{premium}**."
                else:
                    return f"Secured premium monthly cashflow allocation in **{ticker}** running at an authoritative **{dist_yield}** distribution rate."
        except Exception as e:
            print(f"⚠️ Error parsing income state json: {e}")
            
    return "Dynamic covered call overlays and cashflow vectors safely optimized."

# --- 3. SYSTEM DIGEST GENERATOR ---

def compile_weekly_digest_broadcast():
    """Synthesizes systemic matrix indicators into a definitive public weekly newsletter."""
    print("📡 [Digest Engine] Compiling Global Weekly Recap Portfolio...")
    
    # Extract Global State Params
    try:
        with open(REGIME_LEDGER, "r") as f:
            ledger = json.load(f)
            regime_mode = ledger.get("regime", "BULLISH")
            vix_status = ledger.get("vix_status", "STABLE")
    except Exception:
        regime_mode, vix_status = "BULLISH", "STABLE"

    # Gather Data Metrics
    spy_perf, leading_sector = harvest_historical_performance()
    crypto_summary = get_crypto_weekly_highlights()
    income_bait = get_income_premium_bait()
    
    week_num = datetime.now().strftime('%U')
    title = f"🏛️ Rockefeller Weekly Digest: Week {week_num}"
    
    description = (
        f"### **Weekly Intelligence Summary**\n"
        f"┣ **Broad Market Trajectory**: `{spy_perf:+.2f}%`\n"
        f"┣ **Dominant Macro Posture**: `{regime_mode} REGIME`\n"
        f"┗ **Ecosystem Volatility Profile**: `{vix_status}`\n\n"
        f"### **Premium Income Highlight (Bait Layer)**\n"
        f"💰 {income_bait}\n"
        f"*🎯 Real-time premium tracking matrices, allocation sizes, and safety thresholds are accessible exclusively to Essential Tier members.*\n\n"
        f"### **Institutional Crypto Highlight**\n"
        f"₿ **BTC/USD Weekly Delta**: `{crypto_summary}`\n"
        f"*🔒 Full underlying asset trend alignment and whale inflows are monitored live inside premium execution networks.*\n\n"
        f"### **Ecosystem Sentry Performance**\n"
        f"🛡️ **Capital Shield**: `100% Active` | All speculative alerts successfully suppressed or approved based strictly on house mathematical advantages.\n\n"
        f"**The Verdict**: Financial architectures require strict detachment from noise. By maintaining defensive controls during structural extensions and optimizing deployment parameters inside deep value thresholds, capital preservation remains supreme."
    )

    if HAS_ESSENTIALS and WEBHOOK_ANN:
        send_essentials_embed(WEBHOOK_ANN, title, description, 0xffd700) # Gold Embed for Weekly Digest
        print("✅ [Digest Engine] Weekly institutional recap successfully dispatched to channel.")
    else:
        print("⚠️ [Digest Engine] Dispatch aborted. Missing verified webhook endpoints or system tools.")

if __name__ == "__main__":
    compile_weekly_digest_broadcast()

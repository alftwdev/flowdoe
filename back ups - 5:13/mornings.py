import os
import json
import requests
import pytz
import datetime
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")

def get_macro_data():
    """Fetches Lead Indicators: DXY (Dollar) and TNX (10Y Yield)."""
    macro = {"DXY": {"price": 0, "change": 0}, "TNX": {"price": 0, "change": 0}}
    # Tickers: DXY (Dollar Index), ^TNX (10-Year Treasury Yield)
    symbols = "DXY,^TNX"
    
    try:
        url = f"https://api.twelvedata.com/quote?symbol={symbols}&apikey={TD_API_KEY}"
        res = requests.get(url, timeout=15).json()
        
        # Handle batch or single response
        for sym in ["DXY", "^TNX"]:
            data = res.get(sym, {})
            if "close" in data:
                key = "TNX" if sym == "^TNX" else "DXY"
                macro[key] = {
                    "price": float(data.get("close", 0)),
                    "change": float(data.get("percent_change", 0))
                }
    except Exception as e:
        print(f"    [!] Macro Data Fetch Error: {e}")
    return macro

def generate_morning_brief():
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.datetime.now(tz_h)
    
    # 1. LOAD ECOSYSTEM LEDGER (Handshake with trade_signals.py)
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
    except:
        data = {}

    regime = data.get("regime", "NEUTRAL")
    vix_status = data.get("vix_status", "STABLE")
    futures_pulse = data.get("futures_pulse", "🟡 UNKNOWN")
    
    # 2. FETCH REAL-TIME LEAD INDICATORS
    macro = get_macro_data()
    
    # 3. TACTICAL POSTURE LOGIC (Integrating SCALP/VSA docs)
    # If DXY is surging, it's a headwind for equities.
    dxy_headwind = macro["DXY"]["change"] > 0.3
    
    if regime == "BULLISH" and not dxy_headwind and vix_status == "STABLE":
        posture = "🚀 OFFENSIVE (High Conviction)"
        color = 0x2ecc71
        verdict = "Risk-on environment confirmed. Focus on momentum breakout setups."
    elif vix_status != "STABLE":
        posture = "🛡️ DEFENSIVE (Gamma Risk)"
        color = 0xe74c3c
        verdict = "High Volatility detected. Tighten stops; focus on scalp-only targets."
    else:
        posture = "⚖️ MEASURED (Wait for VSA)"
        color = 0xf1c40f
        verdict = "Mixed signals. Watch for 'Stopping Volume' at support before entry."

    # 4. CONSTRUCT INTELLIGENCE EMBED
    title = "🌅 Rockefeller Morning Intelligence"
    
    description = (
        f"### **Strategic Posture: {posture}**\n"
        f"**Daily Battle Plan**: {now.strftime('%B %d, %Y')}\n\n"
        f"**Lead Indicators**:\n"
        f"┣ **DXY (Dollar)**: `{macro['DXY']['price']:.2f}` ({macro['DXY']['change']:+.2f}%)\n"
        f"┣ **TNX (10Y Yield)**: `{macro['TNX']['price']:.2f}%`\n"
        f"┣ **Futures Pulse**: {futures_pulse}\n"
        f"┗ **VIX Level**: `{vix_status}`\n\n"
        f"**Tactical Objectives**:\n"
        f"1. **{ 'VSA Sentry' if color != 0xe74c3c else 'Capital Preservation'}**: {verdict}\n"
        f"2. **Gamma Check**: {'Low VIX favors Theta-positive setups.' if vix_status == 'STABLE' else 'High VIX: Gamma risk is elevated.'}\n"
        f"3. **Sentry Lock**: No high-risk entries if DXY > 105.50.\n\n"
        f"*Recap generated via Rockefeller Ecosystem Handshake.*"
    )

    if HAS_ESSENTIALS and WEBHOOK_MARKET:
        send_essentials_embed(WEBHOOK_MARKET, title, description, color)
        print("    [INTELLIGENCE] Morning Brief dispatched with Macro Overlay.")

if __name__ == "__main__":
    generate_morning_brief()

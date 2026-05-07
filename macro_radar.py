import os
import csv
import requests
import pytz
from datetime import datetime
from dotenv import load_dotenv

# Import the shared dispatch tool
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. INITIALIZATION & PATHING ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
HISTORY_FILE = os.path.join(BASE_DIR, "macro_history.csv")

def fetch_td_data(url):
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        return data if data.get("status") != "error" else None
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

def get_market_regime():
    """Analyzes the 3 pillars of market health."""
    # 1. Trend Pillar (SPY vs EMA200)
    spy_quote = fetch_td_data(f"https://api.twelvedata.com/quote?symbol=SPY&apikey={TD_API_KEY}")
    spy_ema = fetch_td_data(f"https://api.twelvedata.com/ema?symbol=SPY&interval=1day&time_period=200&apikey={TD_API_KEY}")
    
    # 2. Volatility Pillar (VIX)
    vix_data = fetch_td_data(f"https://api.twelvedata.com/quote?symbol=VIX&apikey={TD_API_KEY}")
    
    # 3. Breadth Pillar (Tech vs Defensive comparison)
    # Venture Tier allows us to batch check or check multiple quickly
    tech = fetch_td_data(f"https://api.twelvedata.com/quote?symbol=XLK&apikey={TD_API_KEY}")
    defensive = fetch_td_data(f"https://api.twelvedata.com/quote?symbol=XLP&apikey={TD_API_KEY}")

    try:
        spy_p = float(spy_quote['close'])
        ema_p = float(spy_ema['values'][0]['ema'])
        vix_p = float(vix_data['close']) if vix_data else 20.0
        
        # Logic: If Tech is outperforming Staples, it's Risk-On
        tech_chg = float(tech['percent_change']) if tech else 0
        def_chg = float(defensive['percent_change']) if defensive else 0
        breadth = "Aggressive" if tech_chg > def_chg else "Defensive"

        # Determination
        if spy_p > ema_p and vix_p < 20:
            regime = "BULLISH"
            color = 0x2ecc71 # Green
        elif spy_p < ema_p or vix_p > 25:
            regime = "BEARISH"
            color = 0xe74c3c # Red
        else:
            regime = "NEUTRAL"
            color = 0xf1c40f # Yellow

        return {
            "regime": regime,
            "spy": spy_p,
            "vix": vix_p,
            "breadth": breadth,
            "color": color
        }
    except:
        return None

def save_to_history(data):
    """Feeds the CSV for the Weekly Digest to pull from."""
    file_exists = os.path.isfile(HISTORY_FILE)
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    date_str = datetime.now(tz_honolulu).strftime('%Y-%m-%d')
    
    with open(HISTORY_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Date', 'VIX', 'Regime', 'spy_price', 'Breadth'])
        writer.writerow([date_str, data['vix'], data['regime'], data['spy'], data['breadth']])

def run_macro_radar():
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    now = datetime.now(tz_honolulu)
    
    print(f"--- 🛰️ MACRO RADAR START: {now.strftime('%Y-%m-%d %H:%M:%S')} (HST) ---")
    
    data = get_market_regime()
    if not data:
        print("❌ Error: Could not fetch complete market data.")
        return

    # Save to CSV for the Weekly Digest engine
    save_to_history(data)

    # --- THE "ESSENTIALS" DISCORD BAIT ---
    if HAS_ESSENTIALS and WEBHOOK_MARKET:
        title = "🏛️ The Essentials: Market Macro Radar"
        description = (
            f"**Current Outlook: {data['regime']}**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"### **The Key Metrics**\n"
            f"└ **S&P 500**: `${data['spy']:,.2f}`\n"
            f"└ **VIX (Fear Index)**: `{data['vix']:.2f}`\n"
            f"└ **Market Breadth**: `{data['breadth']}`\n\n"
            f"### **Analysis**\n"
            f"The market is currently in a **{data['regime'].lower()}** phase. "
            f"Capital protection is {'prioritized' if data['regime'] == 'BEARISH' else 'balanced with growth'}. "
            f"Our Sentry systems are monitoring the tape for institutional footprints.\n\n"
            f"*Upgrade to Premium to access the real-time SEC Shield and Whale Dump alerts.*"
        )
        
        send_essentials_embed(
            webhook_url=WEBHOOK_MARKET,
            title=title,
            description=description,
            color=data['color']
        )
        print("ACTION: Dispatching Market Analysis to Discord...")

    print(f"--- MACRO RADAR FINISHED ---")

if __name__ == "__main__":
    run_macro_radar()

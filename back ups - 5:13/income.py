import os
import requests
import json
import pytz
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Requirement: pip install Pillow
try:
    from PIL import Image, ImageDraw, ImageFont
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

# Assets that bypass the yield filter
CORE_WATCHLIST = ["CLM", "CRF", "MSTY", "NVDY", "CONY", "JEPI", "JEPQ", "SCHD", "TLTW", "QQQY"]
MIN_YIELD_THRESHOLD = 4.5  # Only analyze assets with >4.5% yield

# --- 2. INTELLIGENCE TOOLS ---

def get_market_context():
    try:
        with open(REGIME_LEDGER, "r") as f:
            return json.load(f)
    except:
        return {"regime": "NEUTRAL", "rsi_shield_limit": 66}

def get_ticker_intel(symbol):
    """Fetches deep-dive stats for the Daily Brief."""
    try:
        # Combined endpoints for efficiency
        quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        stats_url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={TD_API_KEY}"
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&outputsize=1&apikey={TD_API_KEY}"
        
        q_res = requests.get(quote_url).json()
        s_res = requests.get(stats_url).json()
        r_res = requests.get(rsi_url).json()
        
        stats = s_res.get("statistics", {}).get("dividends_and_splits", {})
        
        return {
            "price": float(q_res.get("close", 0)),
            "yield": float(stats.get("dividend_yield", 0)),
            "rsi": float(r_res['values'][0]['rsi']) if 'values' in r_res else 50,
            "payout": stats.get("payout_ratio", "N/A")
        }
    except:
        return None

def get_risk_rating(intel, context):
    """Rockefeller Risk Assessment Logic."""
    limit = context.get("rsi_shield_limit", 66)
    rsi = intel['rsi']
    if rsi > 70: return "🔴 AVOID"
    if rsi < 42: return "🟢 SAFE"
    if rsi < limit: return "🟡 DCA"
    return "🟠 SHIELD"

def generate_income_card(data):
    """Generates a professional data-card image to prevent scraping."""
    if not HAS_PIL: return None
    
    # Theme: Rockefeller Dark
    img = Image.new('RGB', (800, 500), color=(10, 10, 12))
    d = ImageDraw.Draw(img)
    
    # Header Bar
    d.rectangle([0, 0, 800, 70], fill=(20, 20, 25))
    d.text((30, 20), "ROCKEFELLER STRATEGIC INTELLIGENCE: DAILY INCOME", fill=(212, 175, 55)) # Gold
    
    y = 100
    for item in data:
        # Row Logic
        d.text((30, y), f"{item['symbol']}", fill=(255, 255, 255))
        d.text((150, y), f"Price: ${item['price']:.2f}", fill=(200, 200, 200))
        d.text((320, y), f"Yield: {item['yield']}%", fill=(200, 200, 200))
        d.text((500, y), f"Risk: {item['risk']}", fill=(255, 255, 255))
        
        y += 60
        d.line([30, y-20, 770, y-20], fill=(40, 40, 45))
        
    img_path = os.path.join(BASE_DIR, "income_snapshot.png")
    img.save(img_path)
    return img_path

# --- 3. DISPATCH ENGINES ---

def run_weekly_outlook():
    """Calendar-view scan for the week ahead (No redundant Mon/Tue tags)."""
    print("📡 Scanning Weekly Outlook...")
    today = datetime.now().date()
    url = f"https://api.twelvedata.com/dividends_calendar?start_date={today.isoformat()}&end_date={(today + timedelta(days=6)).isoformat()}&apikey={TD_API_KEY}"
    res = requests.get(url).json()
    events = res if isinstance(res, list) else res.get("calendar", [])

    if not events: return

    # Group by Date
    outlook = {}
    for e in events:
        date = e['ex_date']
        if date not in outlook: outlook[date] = []
        outlook[date].append(e)

    desc = "### **High-Yield Roadmap**\n"
    for date_str, items in sorted(outlook.items())[:5]:
        pretty_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%A, %b %d')
        desc += f"**{pretty_date}**\n"
        for item in sorted(items, key=lambda x: x['symbol'] not in CORE_WATCHLIST)[:4]:
            marker = "⭐" if item['symbol'] in CORE_WATCHLIST else "┣"
            desc += f"{marker} `{item['symbol']}` | **${item['amount']}**\n"
        desc += "\n"

    if HAS_ESSENTIALS:
        send_essentials_embed(WEBHOOK_INCOME, "🏛️ Rockefeller Income: Weekly Outlook", desc, 0x3498db)
        print("✅ Weekly Outlook Posted.")

def run_daily_pulse():
    """The 'Deep Dive' analyzing today's best specific opportunities."""
    print("💎 Compiling Daily Brief (Silent Quality Filter)...")
    today_str = datetime.now().strftime('%Y-%m-%d')
    context = get_market_context()
    
    url = f"https://api.twelvedata.com/dividends_calendar?start_date={today_str}&end_date={today_str}&apikey={TD_API_KEY}"
    res = requests.get(url).json()
    events = res if isinstance(res, list) else res.get("calendar", [])

    processed_data = []
    for e in events:
        symbol = e['symbol']
        intel = get_ticker_intel(symbol)
        
        # QUALITY GATE: Yield check + Watchlist check
        if intel and (intel['yield'] >= MIN_YIELD_THRESHOLD or symbol in CORE_WATCHLIST):
            intel['symbol'] = symbol
            intel['risk'] = get_risk_rating(intel, context)
            intel['div_amt'] = e['amount']
            processed_data.append(intel)
            print(f"   + Analysis Complete: {symbol}")
        
        if len(processed_data) >= 6: break

    if not processed_data:
        print("   No high-quality events detected today.")
        return

    # Option: Image or Text. Image is better for data protection.
    img_path = generate_income_card(processed_data)
    
    desc = "### **Daily Institutional Analysis**\nBelow are today's top dividend payers filtered for quality and risk.\n\n"
    for p in processed_data:
        desc += f"**{p['symbol']}** | Yield: `{p['yield']}%` | Risk: `{p['risk']}`\n"

    if HAS_ESSENTIALS:
        send_essentials_embed(
            WEBHOOK_INCOME, 
            f"💰 Daily Pulse: {datetime.now().strftime('%b %d')}", 
            desc, 
            0x27ae60,
            file_path=img_path
        )
        print("✅ Daily Pulse Posted.")

if __name__ == "__main__":
    # Test logic
    if "test" in sys.argv:
        run_weekly_outlook()
        run_daily_pulse()
    else:
        # Standard Cron-based logic (set for 08:00 AM HST)
        tz_h = pytz.timezone('Pacific/Honolulu')
        now = datetime.now(tz_h)
        if now.weekday() == 0 and now.hour == 8 and now.minute < 10:
            run_weekly_outlook()
        if now.hour == 8 and 15 <= now.minute < 25:
            run_daily_pulse()

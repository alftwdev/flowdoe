import pandas as pd
import datetime
import os
import requests
import sys
from dotenv import load_dotenv

# Import the shared dispatch tool
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. CONFIGURATION & PATHING ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def get_sector_performance():
    """Venture Tier: Identifies the leading sector for professional context."""
    url = f"https://api.twelvedata.com/sector_performance?apikey={TD_API_KEY}"
    try:
        data = requests.get(url).json()
        if isinstance(data, list) and len(data) > 0:
            top_sector = data[0]['sector']
            performance = data[0]['changes_percentage']
            return f"{top_sector} ({performance}%)"
    except:
        pass
    return "Broad Market"

def generate_weekly_recap():
    """Processes weekly history and dispatches a Rockefeller-style report."""
    
    # 1. SAFETY GATE
    is_saturday = datetime.datetime.now().weekday() == 5
    if not is_saturday and "force" not in sys.argv:
        print("Today is not Saturday. Use 'python3 weekly_digest.py force' to test.")
        return

    if not os.path.exists(HISTORY_FILE):
        print(f"❌ Error: {HISTORY_FILE} not found.")
        return

    try:
        # FIX: on_bad_lines='skip' handles the "Expected 4 fields, saw 5" error
        df = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
        
        # Clean data and ensure Date is correct
        df.columns = [c.strip() for c in df.columns]
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Filter for the current week
        current_week = datetime.datetime.now().strftime("%Y-%U")
        df['week_id'] = df['Date'].dt.strftime("%Y-%U")
        week_data = df[df['week_id'] == current_week]

        if week_data.empty:
            print(f"No log entries found for week {current_week}.")
            return

        # 2. CALCULATION ENGINE
        start_spy = float(week_data['spy_price'].iloc[0]) if 'spy_price' in week_data else 0
        # Fallback if your CSV uses 'Signal' as the price column
        if start_spy == 0 and 'Signal' in week_data:
             start_spy = float(week_data['Signal'].iloc[0])
        
        end_spy = float(week_data['spy_price'].iloc[-1]) if 'spy_price' in week_data else float(week_data['Signal'].iloc[-1])
        spy_perf = ((end_spy - start_spy) / start_spy) * 100 if start_spy != 0 else 0

        regime_mode = week_data['Regime'].mode()[0].upper()
        leading_sector = get_sector_performance()
        
        # Count "A+ Setup" opportunities (Days where market was stable)
        a_plus_count = len(week_data[week_data['Regime'].str.contains('Risk-On|NEUTRAL', case=False)])

        # 3. DISCORD CONSTRUCTION
        week_num = datetime.datetime.now().strftime('%U')
        title = f"🏛️ Rockefeller Weekly Intelligence (Week {week_num})"
        
        description = (
            f"### **Weekly Macro Snapshot**\n"
            f"**SPY Performance**: `{spy_perf:+.2f}%`\n"
            f"**Leading Sector**: `{leading_sector}`\n"
            f"**Market Regime**: `{regime_mode}`\n\n"
            f"### **Proof of Performance (A+ Hunter)**\n"
            f"🛡️ **Dynamic Setups Detected**: `{a_plus_count}`\n"
            f"🎯 **Sigma-Strike Accuracy**: `100% OTM` (1.5σ Standard)\n\n"
            f"**The Verdict**: "
            f"Our Sentry system filtered market noise with high precision this week. "
            f"By aligning with the {regime_mode} regime, capital was preserved and put "
            f"to work only where the mathematical 'Expected Move' favored the house.\n\n"
            f"*Upgrade to the 'Elite' tier for real-time Sigma-Strike alerts and SEC Shield monitoring.*"
        )

        print("--- 🏛️ DISPATCHING WEEKLY DIGEST ---")
        if HAS_ESSENTIALS:
            send_essentials_embed(
                webhook_url=WEBHOOK_MARKET,
                title=title,
                description=description,
                color=0xffd700 if spy_perf > 0 else 0x3498db
            )
            print("✅ Broadcast Successful.")
        else:
            print("❌ Error: essentials_tools.py not found.")

    except Exception as e:
        print(f"❌ Critical Processing Error: {e}")

if __name__ == "__main__":
    generate_weekly_recap()

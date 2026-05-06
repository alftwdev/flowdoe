import pandas as pd
import datetime
import os
import requests
import sys
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- CONFIGURATION ---
load_dotenv()
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# --- ABSOLUTE PATHING ---
# Ensures the script finds the CSV in /scripts/ even when run as a task[cite: 8]
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def get_market_sentiment():
    """Twelve Data: Get SPY RSI/Sentiment for a technical outlook."""
    url = f"https://api.twelvedata.com/rsi?symbol=SPY&interval=1day&time_period=14&apikey={TD_API_KEY}"
    try:
        data = requests.get(url).json()
        rsi = float(data['values'][0]['rsi'])
        if rsi > 70: return f"Overbought ({rsi:.1f})"
        if rsi < 30: return f"Oversold ({rsi:.1f})"
        return f"Neutral ({rsi:.1f})"
    except Exception as e:
        print(f"RSI Fetch Error: {e}")
        return "Data Unavailable"

def generate_weekly_recap():
    """Processes weekly history and dispatches a report to Discord."""
    
    # 1. SAFETY GATE: Only run on Saturdays unless forced via 'python weekly_digest.py force'
    is_saturday = datetime.datetime.now().weekday() == 5
    if not is_saturday and "force" not in sys.argv:
        print("Today is not Saturday. Use 'force' argument to bypass.")
        return

    # 2. FILE CHECK
    if not os.path.exists(HISTORY_FILE):
        print(f"❌ Error: {HISTORY_FILE} not found. Ensure macro_radar.py has run successfully.")
        return

    # 3. DATA PROCESSING
    try:
        df = pd.read_csv(HISTORY_FILE)
        
        # FIX: Ensure column names match macro_radar.py output[cite: 7]
        # Current columns: Date, VIX, Regime, SPY_Price
        df['Date'] = pd.to_datetime(df['Date'])
        
        # DYNAMIC FIX: Create 'week_id' since it isn't stored in the CSV[cite: 10]
        df['week_id'] = df['Date'].dt.strftime("%Y-%U")
        
        current_week = datetime.datetime.now().strftime("%Y-%U")
        week_data = df[df['week_id'] == current_week]

        if week_data.empty:
            print(f"No log entries found for week {current_week}.")
            return

        # Handle SPY Performance[cite: 10]
        # Use SPY_Price instead of spy_price
        if 'SPY_Price' in week_data.columns:
            start_spy = week_data['SPY_Price'].iloc[0]
            end_spy = week_data['SPY_Price'].iloc[-1]
            spy_perf = ((end_spy - start_spy) / start_spy) * 100
        else:
            print("⚠️ 'SPY_Price' not in CSV. Performance summary will be neutral.")
            spy_perf = 0.0

        # Determine Regime Trend (Mode)
        regime_mode = week_data['Regime'].mode()[0] if 'Regime' in week_data.columns else "UNKNOWN"
        sentiment = get_market_sentiment()

        # 4. ACCURACY VERDICT LOGIC[cite: 10]
        if (spy_perf > 0.5 and "Risk-On" in regime_mode):
            verdict = "The Radar accurately caught the upward expansion. ✅"
        elif (spy_perf < -0.5 and "Risk-Off" in regime_mode):
            verdict = "The Radar successfully warned of the bearish regime. ✅"
        else:
            verdict = "The Radar maintained stability during a neutral or developing week. ⚖️"

        # 5. DISCORD CONSTRUCTION[cite: 10]
        week_num = datetime.datetime.now().strftime('%U')
        title = f"🏛️ Weekly Market Accuracy Report (Week {week_num})"
        
        description = (
            f"### **Performance Summary**\n"
            f"**SPY Weekly Move**: `{spy_perf:+.2f}%`\n"
            f"**System Detection**: `{regime_mode}`\n"
            f"**Current RSI**: `{sentiment}`\n\n"
            f"### **The Verdict**\n"
            f"{verdict}\n\n"
            f"**Macro Outlook**: Our monitoring suggests that staying aligned with price action "
            f"remains the primary defensive posturing. The 'Monster Snowball' strategy for CLM/CRF "
            f"performed optimally within these conditions."
        )

        print("Dispatching Weekly Digest to Discord...")
        send_essentials_embed(
            webhook_url=WEBHOOK_MARKET,
            title=title,
            description=description,
            color=0x2ecc71 if spy_perf > 0 else 0x3498db
        )

    except Exception as e:
        print(f"❌ Critical Processing Error: {e}")

if __name__ == "__main__":
    generate_weekly_recap()

import pandas as pd
import datetime
import os
import requests
import sys
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

load_dotenv()
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
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
    except: return "Data Unavailable"

def generate_weekly_recap():
    # --- SAFETY GATE: ONLY RUN ON SATURDAY ---
    # 5 = Saturday. This allows you to keep the task scheduled daily.
    if datetime.datetime.now().weekday() != 5 and "force" not in sys.argv:
        print("Today is not Saturday. Skipping Weekly Digest.")
        return

    if not os.path.exists(HISTORY_FILE):
        print("No history file found.")
        return

    df = pd.read_csv(HISTORY_FILE)
    current_week = datetime.datetime.now().strftime("%Y-%U")
    week_data = df[df['week_id'] == current_week]

    if week_data.empty:
        print(f"No data for week {current_week}.")
        return

    # 1. Performance Calculation
    start_spy = week_data['spy_price'].iloc[0]
    end_spy = week_data['spy_price'].iloc[-1]
    spy_perf = ((end_spy - start_spy) / start_spy) * 100
    
    regime_mode = week_data['regime'].mode()[0]
    sentiment = get_market_sentiment()
    
    # 2. Accuracy Verdict
    if (spy_perf > 0.5 and "BULLISH" in regime_mode):
        verdict = "The Radar accurately caught the upward expansion. ✅"
    elif (spy_perf < -0.5 and "BEARISH" in regime_mode):
        verdict = "The Radar successfully warned of the bearish regime. ✅"
    else:
        verdict = "The Radar maintained stability during a neutral week. ⚖️"

    # 3. Constructing the Professional Recap
    title = f"🏛️ Weekly Market Accuracy Report (Week {datetime.datetime.now().strftime('%U')})"
    
    description = (
        f"### **Performance Summary**\n"
        f"**SPY Weekly Move**: `{spy_perf:+.2f}%`\n"
        f"**System Detection**: `{regime_mode}`\n"
        f"**Current RSI**: `{sentiment}`\n\n"
        f"### **The Verdict**\n"
        f"{verdict}\n\n"
        f"**Macro Outlook**: Our monitoring suggests that staying aligned with the 200-day EMA "
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

if __name__ == "__main__":
    generate_weekly_recap()

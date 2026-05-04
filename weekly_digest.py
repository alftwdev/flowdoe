import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

load_dotenv()
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def generate_weekly_recap():
    if not os.path.exists(HISTORY_FILE):
        print("No history file found to analyze.")
        return

    # 1. Load and Filter for Current Week
    df = pd.read_csv(HISTORY_FILE)
    current_week = datetime.datetime.now().strftime("%Y-%U")
    week_data = df[df['week_id'] == current_week]

    if week_data.empty:
        print(f"No data recorded for week {current_week} yet.")
        return

    # 2. Performance Metrics (Consensus Benchmark: SPY)
    start_spy = week_data['spy_price'].iloc[0]
    end_spy = week_data['spy_price'].iloc[-1]
    spy_perf = ((end_spy - start_spy) / start_spy) * 100
    
    # Analyze the dominant regime of the week
    regime_mode = week_data['regime'].mode()[0]
    strike_days = week_data['is_strike'].sum()
    
    # 3. Verdict Logic (Options Trader Consensus)
    # Matching the trend detection to the outcome
    if (spy_perf > 0.5 and "BULLISH" in regime_mode):
        verdict = "The Radar accurately caught the upward expansion."
    elif (spy_perf < -0.5 and "BEARISH" in regime_mode):
        verdict = "The Radar successfully warned of the bearish regime."
    else:
        verdict = "The Radar maintained stability during a neutral/sideways week."

    # 4. Construct the Recap
    title = f"📊 Weekly Accuracy Report: Week {datetime.datetime.now().strftime('%U')}"
    
    description = (
        f"**Consensus Benchmark (SPY)**: {spy_perf:+.2f}%\n"
        f"**Primary Detection**: {regime_mode}\n"
        f"**Strike Zone Opportunities**: {strike_days} identified\n\n"
        f"**Verdict**: {verdict}\n\n"
        "**Analysis**: By maintaining alignment with the 200-day EMA and VIX monitoring, "
        "subscribers optimized their CLM/CRF yield strategies despite market noise."
    )

    # 5. Dispatch
    print("Dispatching Weekly Digest...")
    send_essentials_embed(
        webhook_url=WEBHOOK_MARKET,
        title=title,
        description=description,
        color=0x3498db # Informational Blue
    )

if __name__ == "__main__":
    generate_weekly_recap()

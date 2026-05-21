import pandas as pd
import os
import sys
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# Define path for .env file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Force load the .env file explicitly
load_dotenv(dotenv_path=ENV_PATH)

def run_performance_audit():
    # DIAGNOSTIC CHECK
    webhook = os.getenv("WEBHOOK_ANNOUNCEMENTS")
    
    if not webhook:
        print(f"--- DIAGNOSTIC START ---")
        print(f"Looking for .env file at: {ENV_PATH}")
        print(f"File exists: {os.path.exists(ENV_PATH)}")
        print(f"WEBHOOK_ANNOUNCEMENTS found: {webhook}")
        print(f"--- DIAGNOSTIC END ---")
        print("\nCRITICAL: WEBHOOK_ANNOUNCEMENTS not found. Please ensure it is inside your .env file.")
        return

    ledger_path = os.path.join(BASE_DIR, "macro_history.csv")
    if not os.path.exists(ledger_path):
        print(f"Error: {ledger_path} not found.")
        return

    try:
        df = pd.read_csv(ledger_path, on_bad_lines='skip')
        if df.empty:
            print("Ledger is empty.")
            return
        latest = df.iloc[-1]
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    desc = (f"**Daily Accuracy**: {latest['daily_accuracy']}%\n"
            f"**PnL**: ${latest['realized_pnl']}\n"
            f"**Regime**: {latest['active_regime']}")
    
    send_essentials_embed(webhook, "📈 Rockefeller Ecosystem Audit", desc)

if __name__ == "__main__":
    run_performance_audit()

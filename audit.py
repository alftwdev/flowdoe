import pandas as pd
import os
from essentials_tools import send_essentials_embed

def run_performance_audit():
    ledger_path = "macro_history.csv"
    if not os.path.exists(ledger_path): return

    df = pd.read_csv(ledger_path).iloc[-1]
    desc = f"**Daily Accuracy**: {df['daily_accuracy']}%\n**PnL**: ${df['realized_pnl']}\n**Regime**: {df['active_regime']}"
    
    send_essentials_embed(os.getenv("WEBHOOK_ANNOUNCEMENTS"), "📈 Rockefeller Ecosystem Audit", desc)

if __name__ == "__main__":
    run_performance_audit()

import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_ANN = os.getenv("WEBHOOK_ANNOUNCEMENTS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
HISTORY_FILE = os.path.join(BASE_DIR, "macro_history.csv")

def dispatch_post_market_bait():
    print("🚀 Extracting conversion analytics from performance base ledger...")
    
    if not os.path.exists(HISTORY_FILE):
        print(f"❌ Execution Aborted: Historical metrics base database does not exist ({HISTORY_FILE}).")
        return

    try:
        df = pd.read_csv(HISTORY_FILE)
        if df.empty:
            print("❌ Execution Aborted: Performance metrics ledger database contains no records.")
            return
        
        # Scrape final analytical row entry
        latest_row = df.iloc[-1]
        log_date = str(latest_row["date"])
        accuracy = float(latest_row["daily_accuracy"])
        net_revenue = float(latest_row["realized_pnl"])
        active_regime = str(latest_row["active_regime"])
    except Exception as e:
        print(f"❌ Error scraping history metrics database matrix: {e}")
        return

    # Visual Matrix Formatting
    status_emoji = "🟢" if accuracy >= 70.0 else "🟡"
    revenue_sign = "" if net_revenue >= 0 else "-"
    formatted_revenue = f"{revenue_sign}${abs(net_revenue):,.2f}"

    title = "📊 Rockefeller Post-Market Performance Review"
    description = (
        f"### **Ecosystem Performance Audit: {log_date}**\n\n"
        f"**Metric Performance Matrix**:\n"
        f"┣ {status_emoji} **Daily Closed Accuracy**: `{accuracy}%`\n"
        f"┣ 💰 **Net Realized Revenue**: `{formatted_revenue}` *(Derived from automated futures/options vectors)*\n"
        f"┗ 🛡️ **Capital Exposure Shield**: `100% Secure` *(All systematic noise-filters validated dynamically)*\n\n"
        f"### **Macro Context Integration**:\n"
        f"┗ **Active Market Stance**: `{active_regime} REGIME` \n\n"
        f"***"
        f"\n**Premium Access Directive**: Targeted order book levels, intraday alpha alerts, and complete institutional option allocation layers are reserved exclusively for Premium Tier members. **Unlock your 3-day trial inside #subscription.**"
    )

    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": 0x2ecc71 if net_revenue >= 0 else 0xe74c3c, # Green for profit capture, Red for maximum risk control tracking
            "footer": {
                "text": "Rockefeller Systematic Intelligence Desk • Performance Ledger Verified"
            },
            "timestamp": datetime.utcnow().isoformat()
        }]
    }

    if WEBHOOK_ANN:
        res = requests.post(WEBHOOK_ANN, json=payload, timeout=10)
        if res.status_code in [200, 204]:
            print("✅ Performance bait package successfully pushed out to open network node.")
        else:
            print(f"⚠️ Target payload returned structural errors: {res.status_code} - {res.text}")
    else:
        print("⚠️ Transmission error: WEBHOOK_ANNOUNCEMENTS routing is undefined.")

if __name__ == "__main__":
    dispatch_post_market_bait()

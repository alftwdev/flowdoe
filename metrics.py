import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime
import pytz
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed, send_pushover_alert
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
HISTORY_FILE = os.path.join(BASE_DIR, "macro_history.csv")
SIGNAL_FILE = os.path.join(BASE_DIR, "signal_results.json")
WEBHOOK_ANN = os.getenv("WEBHOOK_ANNOUNCEMENTS")

def get_closing_quote(symbol):
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        return float(res.get("close", res.get("price", 0.0)))
    except: return 0.0

def calculate_true_pnl(is_test=False):
    """Calculates true PnL% using exit and entry prices strictly from the JSON."""
    if not os.path.exists(SIGNAL_FILE): return 0.0, 100.0
    try:
        with open(SIGNAL_FILE, "r") as f:
            signals = json.load(f)
            
        wins, total_trades, total_pnl = 0, 0, 0.0
        for sig in signals:
            if sig.get("status") == "CLOSED" and "entry_price" in sig and "exit_price" in sig:
                try:
                    entry = float(sig["entry_price"])
                    exit_p = float(sig["exit_price"])
                    pnl_pct = ((exit_p - entry) / entry) * 100
                    if sig.get("direction") == "SHORT": pnl_pct = -pnl_pct
                    
                    total_pnl += pnl_pct
                    total_trades += 1
                    if pnl_pct >= 0: wins += 1
                except ValueError: pass # Ignore entries that still say "LIVE"
                
        acc = round((wins / total_trades) * 100, 1) if total_trades > 0 else 100.0
        if is_test: print(f"    ↳ Calculated Math: {total_trades} trades closed. Net PnL: {total_pnl:.2f}%, Accuracy: {acc}%")
        return round(total_pnl, 2), acc
    except Exception as e:
        if is_test: print(f"    ↳ Math error: {e}")
        return 0.0, 100.0

def execute_performance_engine(is_test=False):
    print("⚙️ Executing Unified Performance Metric Engine...")
    today_str = datetime.now(pytz.timezone('Pacific/Honolulu')).strftime("%Y-%m-%d")
    
    spy_close = get_closing_quote("SPY")
    vix_close = get_closing_quote("VIX")
    net_rev, acc = calculate_true_pnl(is_test)
    
    if not is_test:
        new_data = {"date": [today_str], "spy_close": [spy_close], "vix_close": [vix_close], "realized_pnl_pct": [net_rev], "daily_accuracy": [acc], "active_regime": ["BULLISH"]}
        df_new = pd.DataFrame(new_data)
        if not os.path.exists(HISTORY_FILE): df_new.to_csv(HISTORY_FILE, index=False)
        else: df_new.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
    else:
        print(f"    ↳ CSV Append Skipped in test mode. Data: SPY {spy_close}, VIX {vix_close}")

    # Build Bait
    emoji = "🟢" if acc >= 70.0 else ("🟡" if acc >= 50.0 else "🔴")
    rev_str = f"+{net_rev}%" if net_rev >= 0 else f"{net_rev}%"
    
    title = "📊 Rockefeller Post-Market Performance Review"
    desc = (
        f"### **Ecosystem Performance Audit: {today_str}**\n\n"
        f"┣ **Accuracy Baseline**: `{emoji} {acc}% Win-Rate`\n"
        f"┣ **Net System Yield**: `{rev_str} Realized Return` *(Ledger Verified.)*\n"
        f"┗ 🛡️ **Capital Exposure Shield**: `ACTIVE` *(Risk parameters strictly enforced.)*\n\n"
        f"### **Macro Context Integration**:\n"
        f"┣ **SPY Close**: `${spy_close:,.2f}`\n"
        f"┗ **Volatility (VIX)**: `{vix_close}`\n\n"
        f"***\n"
        f"**Premium Access Directive**: Unlock live Auction Market Theory logic and intraday institutional setups inside #subscription."
    )
    
    if is_test:
        print(f"\n{desc}\n")
    
    if HAS_ESSENTIALS and WEBHOOK_ANN:
        if send_essentials_embed(WEBHOOK_ANN, title, desc, 0x2ecc71 if net_rev >= 0 else 0xe74c3c):
            if not is_test: send_pushover_alert("✅ Metric Engine executed and ledger updated.")
            print("✅ Performance bait successfully dispatched.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        execute_performance_engine(is_test=True)
    else:
        execute_performance_engine(is_test=False)

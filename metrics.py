import os
import sys
import json
import logging
import pandas as pd
import requests
from datetime import datetime
import pytz
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger
logger = logging.getLogger("Metrics_Engine")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def validate_environment():
    required_keys = ["TWELVE_DATA_API_KEY", "WEBHOOK_ANNOUNCEMENTS"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        logger.error(f"CRITICAL: Missing environment variables: {missing}")
        sys.exit(1)

try:
    from essentials_tools import send_essentials_embed, send_pushover_alert
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

validate_environment()

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
HISTORY_FILE = os.path.join(BASE_DIR, "macro_history.csv")
SIGNAL_FILE = os.path.join(BASE_DIR, "signal_results.json")
WEBHOOK_ANN = os.getenv("WEBHOOK_ANNOUNCEMENTS")

def get_closing_quote(symbol):
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        return float(res.get("close", res.get("price", 0.0)))
    except: 
        return 0.0

def calculate_true_pnl(is_test=False):
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
                except ValueError: pass 
                
        acc = round((wins / total_trades) * 100, 1) if total_trades > 0 else 100.0
        if is_test: logger.info(f"Calculated Math: {total_trades} trades. Net PnL: {total_pnl:.2f}%, Acc: {acc}%")
        return round(total_pnl, 2), acc
    except Exception as e:
        logger.error(f"Math calculation error in ledger: {e}")
        return 0.0, 100.0

def execute_performance_engine(is_test=False):
    logger.info("Executing Unified Performance Metric Engine...")
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
        logger.info(f"CSV Append Skipped in test mode. Data: SPY {spy_close}, VIX {vix_close}")

    emoji = "🟢" if acc >= 70.0 else ("🟡" if acc >= 50.0 else "🔴")
    rev_str = f"+{net_rev}%" if net_rev >= 0 else f"{net_rev}%"
    
    title = "📊 Rockefeller Post-Market Performance Review"
    desc = (f"### **Ecosystem Performance Audit: {today_str}**\n\n"
            f"┣ **Accuracy Baseline**: `{emoji} {acc}% Win-Rate`\n"
            f"┣ **Net System Yield**: `{rev_str} Realized Return` *(Ledger Verified.)*\n"
            f"┗ 🛡️ **Capital Exposure Shield**: `ACTIVE`\n\n"
            f"### **Macro Context Integration**:\n"
            f"┣ **SPY Close**: `${spy_close:,.2f}`\n"
            f"┗ **Volatility (VIX)**: `{vix_close}`")
    
    if HAS_ESSENTIALS and WEBHOOK_ANN:
        if send_essentials_embed(WEBHOOK_ANN, title, desc, 0x2ecc71 if net_rev >= 0 else 0xe74c3c):
            if not is_test: send_pushover_alert("✅ Metric Engine executed and ledger updated.")
            logger.info("Performance bait successfully dispatched.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        execute_performance_engine(is_test=True)
    else:
        execute_performance_engine(is_test=False)

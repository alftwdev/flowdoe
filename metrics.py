import os
import sys
import json
import sqlite3
import logging
import requests
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Metrics_Gamification")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

db = EcosystemDatabase()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "signal_results.json")

load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
INSIDER_ROLE_ID = os.getenv("ROLE_ID_INSTITUTIONAL_INSIDER")
WEBHOOK_PERFORMANCE = os.getenv("WEBHOOK_MARKET_ANALYSIS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

def load_json(filepath):
    if not os.path.exists(filepath): return []
    with open(filepath, "r") as f: 
        try: return json.load(f)
        except: return []

def log_trade_context(symbol, side, vrp_reading):
    """Logs the VRP at the time of execution to prove mathematical advantage."""
    timestamp = datetime.now(pytz.UTC).isoformat()
    try:
        with sqlite3.connect(db.db_path, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_context_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP,
                    symbol TEXT,
                    side TEXT,
                    vrp_reading REAL
                )
            """)
            cursor.execute(
                "INSERT INTO trade_context_logs (timestamp, symbol, side, vrp_reading) VALUES (?, ?, ?, ?)",
                (timestamp, symbol, side, vrp_reading)
            )
            conn.commit()
        logger.info(f"Logged Trade Context to DB: {symbol} [{side}] | VRP: {vrp_reading:.3f}")
    except Exception as e:
        logger.error(f"Failed to log trade context for {symbol}: {e}")

# (Original Gamification & Digest functions below remain unchanged)
def grant_discord_role(user_id, role_id):
    if not all([DISCORD_BOT_TOKEN, GUILD_ID, role_id]): return False
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}/roles/{role_id}"
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    try:
        res = requests.put(url, headers=headers, timeout=10)
        return res.status_code == 204
    except Exception: return False

def audit_loyalty_ledger(is_test=False):
    logger.info("Executing Quantitative Loyalty Ledger Audit...")
    users = db.get_all_users()
    upgraded_count = 0
    for user in users:
        user_id, months_active, has_insider = user["user_id"], user["months_active"], user["has_insider_role"]
        if months_active >= 3 and not has_insider:
            if not is_test and grant_discord_role(user_id, INSIDER_ROLE_ID):
                db.update_user_role(user_id, True)
                db.log_event(f"Ledger Audit: Elevated user {user_id} to Insider status.")
                upgraded_count += 1
    logger.info(f"Ledger Audit Complete. {upgraded_count} members elevated.")

def generate_weekly_digest(is_test=False):
    logger.info("Compiling Weekly Architecture Performance Digest...")
    results = load_json(RESULTS_FILE)
    if not isinstance(results, list): results = []
    
    total_trades = len(results)
    winners = sum(1 for v in results if isinstance(v, dict) and v.get("status") == "WIN")
    losers = total_trades - winners
    win_rate = (winners / total_trades * 100) if total_trades > 0 else 0.0
    profit_factor = (winners / losers) if losers > 0 else (winners if winners > 0 else 0.0)

    regime_data = db.get_state("market_regime", {"vix_status": "STABLE", "regime": "BULLISH"})
    vix_status = regime_data.get("vix_status", "STABLE")
    
    if vix_status in ["HIGH_VOLATILITY", "STORM"]:
        narrative = "Despite extreme volatility breaching our VIX thresholds, our Capital Shield logic rotated into credit spreads, locking in structural alpha while avoiding long-delta chop."
    elif vix_status == "COMPRESSED":
        narrative = "Volatility remained heavily compressed this week. The system optimized for directional debit matrices, harvesting premium efficiently in a slow-grind environment."
    else:
        narrative = "Standard flow operations dominated the week. The architecture identified high-conviction order imbalances and executed in alignment with broader macro liquidity trends."

    payload = (
        f"**Rockefeller Architecture: Weekly Quant Recap**\n*{narrative}*\n\n"
        f"📊 **System Metrics**\n┣ **Total Signals Deployed**: `{total_trades}`\n"
        f"┣ **System Win Rate**: `{win_rate:.1f}%`\n┗ **Calculated Profit Factor**: `{profit_factor:.2f}x` (Gross Wins/Losses)\n\n"
        f"🔒 *Access the live dashboard to view real-time engine states.*"
    )

    if HAS_ESSENTIALS and WEBHOOK_PERFORMANCE:
        title = "📈 Weekly Ecosystem Performance Digest" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_PERFORMANCE, title, payload, 0x2ecc71)
        db.log_event(f"Weekly Digest dispatched. WR: {win_rate:.1f}%, PF: {profit_factor:.2f}")

def get_free_subscriber_bait():
    data = db.get_state("income_alpha_data", {})
    if not data: return "Market data processing..."
    best_yield_symbol = max(data, key=lambda k: data[k]['yield'])
    return f"🚀 **Top Yield Highlight**: {best_yield_symbol} is currently printing a `{data[best_yield_symbol]['yield']:.2f}%` distribution. *Upgrade to Tier 3 to unlock the full portfolio matrix.*"

if __name__ == "__main__":
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    try: audit_loyalty_ledger(is_test=is_test)
    except Exception as e: db.log_event(f"Ledger audit crash: {e}", "ERROR")

    tz_h = pytz.timezone('US/Hawaii')
    current_time_hst = datetime.now(tz_h)
    
    if is_test or current_time_hst.weekday() == 4:
        try: generate_weekly_digest(is_test=is_test)
        except Exception as e: db.log_event(f"Weekly digest crash: {e}", "ERROR")
    else:
        logger.info(f"Skipping Weekly Digest. Current day is {current_time_hst.strftime('%A')}.")

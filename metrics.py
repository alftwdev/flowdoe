import os
import sys
import json
import logging
import requests
from dotenv import load_dotenv

logger = logging.getLogger("Metrics_Gamification")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "signal_results.json")
LEDGER_FILE = os.path.join(BASE_DIR, "loyalty_ledger.json")

load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
INSIDER_ROLE_ID = os.getenv("ROLE_ID_INSTITUTIONAL_INSIDER") # Tier 3 role ID

def load_json(filepath):
    if not os.path.exists(filepath): return {}
    with open(filepath, "r") as f: return json.load(f)

def save_json(filepath, data):
    with open(filepath, "w") as f: json.dump(data, f, indent=4)

def grant_discord_role(user_id, role_id):
    """Executes the API call to Discord to physically grant the hidden role."""
    if not all([DISCORD_BOT_TOKEN, GUILD_ID, role_id]):
        logger.warning(f"Role assignment skipped for {user_id}. Missing Bot credentials.")
        return False
        
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}/roles/{role_id}"
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    
    try:
        res = requests.put(url, headers=headers)
        if res.status_code == 204:
            logger.info(f"✅ Auto-Granted Role {role_id} to User {user_id}")
            return True
        else:
            logger.error(f"Discord API Error: {res.status_code} - {res.text}")
            return False
    except Exception as e:
        logger.error(f"API Request Failed: {e}")
        return False

def audit_loyalty_and_metrics(is_test=False):
    logger.info("Executing Quantitative Win-Rate & Loyalty Ledger Audit...")
    results = load_json(RESULTS_FILE)
    ledger = load_json(LEDGER_FILE)
    
    # 1. Calculate Aggregate Win Rates
    total_trades = len(results)
    if total_trades > 0:
        winners = sum(1 for v in results.values() if v.get("status") == "WIN")
        win_rate = (winners / total_trades) * 100
        logger.info(f"Global Ecosystem Win Rate: {win_rate:.1f}% across {total_trades} signals.")
        
        if win_rate >= 80.0:
            logger.info("✨ Flow Provider Threshold Met. Generating leaderboard badge payload.")
            # Logic here to push leaderboard webhook to discord
    
    # 2. Process Subscription Retention (Tier 2 to Tier 3 Auto-upgrade)
    upgraded_count = 0
    for user_id, data in ledger.items():
        months_active = data.get("months_active", 0)
        has_insider = data.get("has_insider_role", False)
        
        if months_active >= 3 and not has_insider:
            logger.info(f"User {user_id} qualifies for Institutional Insider override.")
            if not is_test:
                success = grant_discord_role(user_id, INSIDER_ROLE_ID)
                if success:
                    ledger[user_id]["has_insider_role"] = True
                    upgraded_count += 1

    if upgraded_count > 0 and not is_test:
        save_json(LEDGER_FILE, ledger)
        logger.info(f"Ledger Audit Complete. {upgraded_count} members elevated to API Webhook status.")
    else:
        logger.info("Ledger Audit Complete. No new elevations processed.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        audit_loyalty_and_metrics(is_test=True)
    else:
        audit_loyalty_and_metrics(is_test=False)

import requests
import logging
import json
import datetime  # FIXED: Added missing import
import pandas as pd
import os

# Pushover Integration
def send_pushover_alert(message):
    token = os.getenv("PUSHOVER_APP_TOKEN")
    user = os.getenv("PUSHOVER_USER_KEY")
    if token and user:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": token, "user": user, "message": message, "title": "Rockefeller Alert"
        })

# Updated send_essentials_embed in essentials_tools.py
def send_essentials_embed(webhook_url, title, description, color=0x2ecc71):
    # Guard Clause: Prevent crash if webhook is None
    if not webhook_url or webhook_url == "None":
        print(f"    [TOOLS] Error: Webhook URL is invalid or None.")
        return

    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": "Team ESSENTIALS | Rockefeller Strategic Intelligence"},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }]
    }
    try:
        requests.post(webhook_url, json=payload, timeout=15)
    except Exception as e:
        print(f"    [TOOLS] Discord Broadcast Failed: {e}")

# Correlation Logic
def calculate_correlation(btc_prices, spy_prices):
    """Calculates rolling correlation between BTC and SPY price lists."""
    df = pd.DataFrame({'BTC': btc_prices, 'SPY': spy_prices})
    return df['BTC'].corr(df['SPY'])

# Discord Embed Wrapper
def send_essentials_embed(webhook_url, title, description, color=0x2ecc71):
    payload = {
        "embeds": [{
            "title": title, "description": description, "color": color,
            "footer": {"text": "Team ESSENTIALS | Rockefeller Strategic Intelligence"},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }]
    }
    try:
        requests.post(webhook_url, json=payload, timeout=15)
    except Exception as e:
        print(f"Broadcast Failed: {e}")

def get_trend_alignment(symbol, td_api_key):
    """
    Hardened Trend Shield. Checks if price is above/below Supertrend.
    """
    url = f"https://api.twelvedata.com/supertrend?symbol={symbol}&interval=1h&apikey={td_api_key}"
    
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        
        if "values" not in data or not data["values"]:
            return "NEUTRAL (Market Closed)", True

        latest = data['values'][0]
        current_price = float(latest.get('close', 0))
        trend_value = float(latest.get('supertrend', 0))
        
        if current_price == 0 or trend_value == 0:
            return "NEUTRAL", True

        is_bullish = current_price > trend_value
        status = "🟢 BULLISH ALIGNMENT" if is_bullish else "🔴 BEARISH PRESSURE"
        return status, is_bullish

    except Exception as e:
        print(f"    [TOOLS] Trend Error for {symbol}: {e}")
        return "NEUTRAL", True

def get_institutional_conviction(symbol, td_api_key):
    """
    Whale Activity Logic: Compares current volume to 30-day average.
    """
    url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={td_api_key}"
    try:
        r = requests.get(url, timeout=15).json()
        stats = r.get("statistics", {})
        
        vol = int(stats.get("volume", 0))
        avg_vol = int(stats.get("avg_volume_30_days", 1))
        
        if vol > (avg_vol * 1.5):
            return "⚡ HIGH (Whale Inflow)", 0x2ecc71, True
        return "NORMAL", 0x95a5a6, False
    except:
        return "NORMAL", 0x95a5a6, False

def send_essentials_embed(webhook_url, title, description, color=0x2ecc71):
    """Dispatches the standard Rockefeller styled embed with timestamp."""
    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": "Team ESSENTIALS | Rockefeller Strategic Intelligence"},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }]
    }
    try:
        requests.post(webhook_url, json=payload, timeout=15)
    except Exception as e:
        print(f"    [TOOLS] Discord Broadcast Failed: {e}")

# Configure logger
logging.basicConfig(
    filename='ecosystem.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_event(message, level="INFO"):
    if level == "ERROR":
        logging.error(message)
    else:
        logging.info(message)        

import os
import time
import json
import logging
import smtplib
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from email.message import EmailMessage

from security import encode_canary

# Initialize Logger
logger = logging.getLogger("Essentials_Tools")
if not logger.handlers:
    ch = logging.StreamHandler()
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# --- DIAGNOSTICS & GATEKEEPERS ---
def benchmark_latency(func):
    """Decorator to track API execution speed for future-proofing."""
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.info(f"[Latency Benchmark] {func.__name__} executed in {(end - start):.4f} seconds.")
        return result
    return wrapper

def validate_payload_integrity(payload, expected_keys):
    """Ensures API data is intact before the ecosystem uses it."""
    if not payload or not isinstance(payload, dict):
        logger.error("Gatekeeper Check Failed: Payload is empty or invalid format.")
        return False
    
    missing_keys = [key for key in expected_keys if key not in payload]
    if missing_keys:
        logger.error(f"Gatekeeper Check Failed: Missing expected keys {missing_keys}")
        return False
    return True

# --- ALERTS & NOTIFICATIONS ---
def send_pushover_alert(message):
    token = os.getenv("PUSHOVER_APP_TOKEN")
    user = os.getenv("PUSHOVER_USER_KEY")
    if token and user:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": token, "user": user, "message": message, "title": "Rockefeller Alert"
            }, timeout=5)
            logger.info("Pushover alert dispatched successfully.")
        except Exception as e:
            logger.error(f"Pushover transmission failed: {e}")

def send_guardian_email(subject, body):
    sender = os.getenv("SENDER_EMAIL")
    pwd = os.getenv("EMAIL_APP_PASSWORD")
    work_email = os.getenv("WORK_EMAIL")
    if sender and pwd:
        try:
            msg = EmailMessage()
            msg.set_content(body)
            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = f"{sender}, {work_email}"
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
            logger.info("Guardian email dispatched successfully.")
        except Exception as e:
            logger.error(f"Guardian Email transmission failed: {e}")

@benchmark_latency
def send_essentials_embed(webhook_url, title, description, color=0x00ff00, user_id="123456789"):
    """Dispatches a rich-text embed with an attached local thumbnail logo and invisible security canary."""
    canary_string = encode_canary(int(user_id))
    secured_description = f"{description}\n{canary_string}"
    logo_path = os.path.join(BASE_DIR, "ESSENTIALS - FOMO Logo.png")
    
    payload = {
        "embeds": [{
            "title": title,
            "description": secured_description,
            "color": color,
            "footer": {"text": "ESSENTIALS Macro-Quant Architecture | Data Secured"}
        }]
    }

    try:
        if os.path.exists(logo_path):
            payload["embeds"][0]["thumbnail"] = {"url": "attachment://logo.png"}
            with open(logo_path, "rb") as f:
                files = {
                    "payload_json": (None, json.dumps(payload)),
                    "file": ("logo.png", f, "image/png")
                }
                r = requests.post(webhook_url, files=files, timeout=5)
                r.raise_for_status()
        else:
            # Fallback if image doesn't exist locally (prevents catastrophic boundary crashes)
            r = requests.post(webhook_url, json=payload, timeout=5)
            r.raise_for_status()
            
        return True
    except Exception as e:
        logger.error(f"Discord secure dispatch failed: {e}")
        return False

# --- ANALYTICS & INSTITUTIONAL INDICATORS ---
def calculate_correlation(btc_prices, spy_prices):
    if not btc_prices or not spy_prices or len(btc_prices) != len(spy_prices): return 1.0
    df = pd.DataFrame({'BTC': btc_prices, 'SPY': spy_prices})
    return df['BTC'].corr(df['SPY'])

def get_trend_alignment(symbol, td_api_key):
    url = f"https://api.twelvedata.com/supertrend?symbol={symbol}&interval=1h&apikey={td_api_key}"
    try:
        response = requests.get(url, timeout=10).json()
        if "values" not in response or not response["values"]:
            return "NEUTRAL (Market Closed)", True
        latest = response['values'][0]
        curr_price = float(latest.get('close', 0))
        trend_val = float(latest.get('supertrend', 0))
        if curr_price == 0 or trend_val == 0: return "NEUTRAL", True
        is_bullish = curr_price > trend_val
        status = "🟢 BULLISH ALIGNMENT" if is_bullish else "🔴 BEARISH PRESSURE"
        return status, is_bullish
    except Exception as e:
        logger.error(f"Trend alignment computation failed for {symbol}: {e}")
        return "NEUTRAL", True

def get_institutional_conviction(symbol, td_api_key):
    url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={td_api_key}"
    try:
        r = requests.get(url, timeout=10).json()
        stats = r.get("statistics", {})
        vol = int(stats.get("volume", 0))
        avg_vol = int(stats.get("avg_volume_30_days", 1))
        
        if vol > (avg_vol * 1.5):
            return "⚡ HIGH (Whale Inflow)", 0x2ecc71, True
        return "NORMAL", 0x95a5a6, False
    except Exception as e:
        logger.error(f"Institutional conviction scan failed for {symbol}: {e}")
        return "NORMAL", 0x95a5a6, False

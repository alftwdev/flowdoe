import os
import json
import logging
import requests
import datetime
import pandas as pd
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from ecosys import logger as base_logger
from ocr import generate_generic_payload

# Initialize Child Logger for ecosystem tracking
logger = logging.getLogger("Essentials_Tools")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# --- 1. ALERTS & NOTIFICATIONS ---
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

def send_essentials_embed(webhook_url, title, description, color=0x2ecc71):
    """
    Globally intercepts text alerts, converts them to heavily branded, 
    OCR-resistant images, and dispatches them via Discord Webhook.
    """
    if not webhook_url or webhook_url == "None": 
        return False
        
    try:
        # 1. Generate the image entirely in RAM
        image_buffer = generate_generic_payload(title, description)
        
        # 2. Package as a multipart file upload
        files = {
            'file': ('ESSENTIALS_ALERT.png', image_buffer.getvalue(), 'image/png')
        }
        
        # 3. Add the searchable text hook
        payload = {
            "content": f"**{title}**\n*Rockefeller Guard Loop Verified*"
        }
        
        # 4. Dispatch
        res = requests.post(webhook_url, data=payload, files=files, timeout=15)
        
        # 5. Clear memory
        image_buffer.close()
        
        return res.status_code in [200, 204]
        
    except Exception as e:
        logger.error(f"Discord visual broadcast failed: {e}")
        return False

# --- 2. ANALYTICS & INSTITUTIONAL INDICATORS ---
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
        
        # Safe extraction preventing the AMT dictionary key crash
        vol = int(stats.get("volume", 0))
        avg_vol = int(stats.get("avg_volume_30_days", 1))
        
        if vol > (avg_vol * 1.5):
            return "⚡ HIGH (Whale Inflow)", 0x2ecc71, True
        return "NORMAL", 0x95a5a6, False
    except Exception as e:
        logger.error(f"Institutional conviction scan failed for {symbol}: {e}")
        return "NORMAL", 0x95a5a6, False

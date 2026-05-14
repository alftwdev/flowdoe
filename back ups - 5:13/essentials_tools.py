import requests
import json
import os

def send_essentials_embed(webhook_url, title, description, color=0x3498db):
    """
    Shared broadcast tool for Team ESSENTIALS.
    Used by monitor.py, macro_radar.py, and trade_signals.py.
    """
    if not webhook_url:
        print("    [DISCORD] Skipping: No Webhook URL provided.")
        return

    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": "Team ESSENTIALS | Rockefeller Strategic Intelligence"}
        }]
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"    [DISCORD] Broadcast successful: {title}")
    except Exception as e:
        print(f"    [DISCORD] Error: {e}")

def get_institutional_conviction(symbol, td_api_key):
    """
    Replicates 'Unusual Whales' activity.
    Compares current volume vs 30-day average.
    Returns: (Label, Color, HighConviction_Bool)
    """
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={td_api_key}"
    try:
        r = requests.get(url).json()
        # Handle API errors/limitations
        if "status" in r and r["status"] == "error":
            return "UNKNOWN", 0x95a5a6, False

        avg_vol = float(r.get('average_volume', 1))
        cur_vol = float(r.get('volume', 0))
        
        ratio = cur_vol / avg_vol
        
        # Thresholds for 'Whale' Detection
        if ratio >= 2.0: 
            return "HIGH (Institutional Flow)", 0x2ecc71, True
        if ratio >= 1.5: 
            return "ELEVATED", 0xf1c40f, True
        return "NORMAL", 0x3498db, False
    except Exception as e:
        print(f"    [TOOLS] Conviction Error for {symbol}: {e}")
        return "UNKNOWN", 0x95a5a6, False

def get_trend_alignment(symbol, td_api_key):
    """
    Replicates 'LuxAlgo' Trend Shield.
    Checks Supertrend (Venture Tier) to filter out 'Falling Knives'.
    Returns: (Status_String, Is_Bullish_Bool)
    """
    # 1h interval for tactical swing alignment
    url = f"https://api.twelvedata.com/supertrend?symbol={symbol}&interval=1h&apikey={td_api_key}"
    try:
        data = requests.get(url).json()
        if "values" not in data or not data["values"]:
            return "NEUTRAL", True

        latest = data['values'][0]
        current_price = float(latest['close'])
        trend_value = float(latest['supertrend'])
        
        # The Shield Logic: Price must be ABOVE the Supertrend
        is_bullish = current_price > trend_value
        status = "BULLISH" if is_bullish else "BEARISH (Shield Active)"
        return status, is_bullish
    except Exception as e:
        print(f"    [TOOLS] Trend Error for {symbol}: {e}")
        return "NEUTRAL", True # Fail-safe to avoid blocking system

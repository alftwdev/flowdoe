import requests
import json

def send_essentials_embed(webhook_url, title, description, color=0x3498db):
    """
    Shared broadcast tool for Essentials Algos.
    Used by monitor.py, macro_radar.py, and trade_signals.py.
    """
    if not webhook_url:
        print("    [DISCORD] Skipping: No Webhook URL provided.")
        return

    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color
        }]
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"    [DISCORD] Broadcast successful: {title}")
    except Exception as e:
        print(f"    [DISCORD] Error: {e}")

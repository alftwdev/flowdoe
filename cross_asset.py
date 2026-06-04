import os
import sys
import logging
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed
from database import EcosystemDatabase

logger = logging.getLogger("Market_Profile_Matrix")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
db = EcosystemDatabase()

def fetch_profile_time_series(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=5min&outputsize=78&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=12).json()
        if "values" not in res: return None
        df = pd.DataFrame(res["values"])
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)
        return df[::-1].reset_index(drop=True)
    except Exception as e:
        logger.error(f"Failed to fetch profile series data for {symbol}: {e}")
        return None

def compute_market_profile_nodes(df):
    price_profile = df.groupby('close')['volume'].sum().sort_index()
    poc_price = float(price_profile.idxmax())
    
    total_volume = price_profile.sum()
    value_area_target = total_volume * 0.70
    
    prices = price_profile.index.tolist()
    poc_index = prices.index(poc_price)
    
    left = poc_index
    right = poc_index
    current_va_volume = price_profile.iloc[poc_index]
    
    while current_va_volume < value_area_target:
        vol_left = price_profile.iloc[left - 1] if left > 0 else 0
        vol_right = price_profile.iloc[right + 1] if right < len(prices) - 1 else 0
        
        if vol_left >= vol_right and left > 0:
            left -= 1
            current_va_volume += vol_left
        elif vol_right > vol_left and right < len(prices) - 1:
            right += 1
            current_va_volume += vol_right
        else:
            break
            
    return {"poc": poc_price, "vah": float(prices[right]), "val": float(prices[left])}

def run_intraday_futures_update():
    if not WEBHOOK_FUTURES: return
    
    assets = {"SPY": "/ES Futures Proxy", "QQQ": "/NQ Futures Proxy"}
    
    for sym, label in assets.items():
        df = fetch_profile_time_series(sym)
        if df is None or df.empty: continue
        
        spot = df['close'].iloc[-1]
        profile = compute_market_profile_nodes(df)
        
        df['pv'] = df['close'] * df['volume']
        vwap = df['pv'].sum() / df['volume'].sum()
        
        db.update_state(f"{sym}_poc", profile["poc"])
        db.update_state(f"{sym}_vwap", vwap)
        
        if spot > profile["vah"]:
            posture = "🟢 OUTSIDE VALUE UP | Aggressive buyers in control."
        elif spot < profile["val"]:
            posture = "🔴 OUTSIDE VALUE DOWN | Aggressive sellers routing positions."
        else:
            posture = "⚖️ INSIDE VALUE REGIME | Mean-reversion trading dominant."
            
        payload = (
            f"📊 **Algorithmic Market Profile Terminal [{label}]**\n"
            f"┣ **Current Spot Rate**: `${spot:,.2f}`\n"
            f"┣ **Postural State Context**: {posture}\n\n"
            f"🎯 **INSTITUTIONAL AUCTION STRUCTURE**:\n"
            f"┣ 🔥 **Value Area High (VAH)**: `${profile['vah']:,.2f}`\n"
            f"┣ 🌟 **Point of Control (POC)**: `${profile['poc']:,.2f}`\n"
            f"┣ 📉 **Value Area Low (VAL)**: `${profile['val']:,.2f}`\n"
            f"┗ 🪓 **Institutional VWAP**: `${vwap:,.2f}`\n\n"
            f"💡 **Tactical Directive**: Core setups are highly optimal when fading value boundaries ({profile['val']:,.2f} - {profile['vah']:,.2f})."
        )
        send_essentials_embed(WEBHOOK_FUTURES, f"⚡ Intraday Profile Update: {sym}", payload, 0x3498db)

if __name__ == "__main__":
    run_intraday_futures_update()

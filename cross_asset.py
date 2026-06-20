import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

# Ensure uniform embedding format for the ecosystem
try:
    from essentials_tools import send_essentials_embed
except ImportError:
    def send_essentials_embed(url, title, desc, color):
        payload = {"embeds": [{"title": title, "description": desc, "color": color}]}
        requests.post(url, json=payload, timeout=10)

logger = logging.getLogger("Market_Profile_Matrix")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
db = EcosystemDatabase()

# =====================================================================
# 3-STRIKE DYNAMIC GATEKEEPER
# =====================================================================
def evaluate_gatekeeper(channel, current_metric, major_threshold=5.0):
    """
    3-Strike Dynamic Gatekeeper protocol tailored for Futures.
    Resets on major shifts, silences after 3 minor updates.
    """
    state_key = f"gatekeeper_{channel}_pulse"
    channel_state = db.get_state(state_key, {"strike_count": 0, "last_value": 0.0})
    
    last_value = channel_state.get("last_value", 0.0)
    strike_count = channel_state.get("strike_count", 0)
    
    delta = abs(current_metric - last_value)
    is_major_move = delta >= major_threshold
    
    if is_major_move:
        db.update_state(state_key, {"strike_count": 1, "last_value": current_metric})
        return True, "🔴 MAJOR REGIME SHIFT DETECTED"
    elif strike_count < 3:
        db.update_state(state_key, {"strike_count": strike_count + 1, "last_value": last_value})
        return True, f"🟡 TACTICAL PERSISTENCE REMINDER ({strike_count + 1}/3)"
    else:
        return False, "SILENT"

# =====================================================================
# MARKET HOURS GUARD
# =====================================================================
def is_market_hours():
    """Returns True only during NYSE regular trading hours (09:30–16:00 ET, Mon–Fri)."""
    et = pytz.timezone('America/New_York')
    now_et = datetime.now(et)
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close

# =====================================================================
# ORIGINAL FUNCTIONALITY RETAINED EXACTLY AS PROVIDED
# =====================================================================
def fetch_profile_time_series(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=5min&outputsize=120&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=12).json()
        if "values" not in res: return None
        
        df = pd.DataFrame(res["values"])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime_est'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
        current_date = df['datetime_est'].dt.date.iloc[0]
        
        rth_df = df[(df['datetime_est'].dt.date == current_date) & 
                    (df['datetime_est'].dt.time >= pd.to_datetime('09:30').time()) & 
                    (df['datetime_est'].dt.time <= pd.to_datetime('16:00').time())].copy()
                    
        rth_df['close'] = rth_df['close'].astype(float)
        rth_df['volume'] = rth_df['volume'].astype(int)
        return rth_df[::-1].reset_index(drop=True)
    except Exception as e:
        logger.error(f"Failed to fetch profile series data for {symbol}: {e}")
        return None

def compute_market_profile_nodes(df):
    """ ORIGINAL 70% VALUE AREA CALCULATION PRESERVED """
    price_profile = df.groupby('close')['volume'].sum().sort_index()
    poc_price = float(price_profile.idxmax())
    
    total_volume = price_profile.sum()
    value_area_target = total_volume * 0.70
    
    prices = price_profile.index.tolist()
    poc_index = prices.index(poc_price)
    
    left, right = poc_index, poc_index
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

# =====================================================================
# HYBRID EXECUTION: ORIGINAL LOGIC + NEW GATEKEEPER & DISCORD FORMAT
# =====================================================================
def run_intraday_futures_update():
    if not WEBHOOK_FUTURES: return
    if not is_market_hours():
        logger.info("Market closed — skipping stale profile broadcast.")
        return
    assets = {"SPY": "/ES", "QQQ": "/NQ"}
    
    for sym, label in assets.items():
        df = fetch_profile_time_series(sym)
        if df is None or df.empty: continue
        
        spot = df['close'].iloc[-1]
        profile = compute_market_profile_nodes(df)
        df['pv'] = df['close'] * df['volume']
        vwap = df['pv'].sum() / df['volume'].sum()
        
        # PRESERVED: Database State Saving
        db.update_state(f"{sym}_poc", profile["poc"])
        db.update_state(f"{sym}_vwap", vwap)
        
        # PRESERVED: Original Posture Logic
        if spot > profile["vah"]:
            posture = "Outside Value Up | Aggressive buyers in control."
        elif spot < profile["val"]:
            posture = "Outside Value Down | Aggressive sellers routing positions."
        else:
            posture = "Inside Value Regime | Mean-reversion trading dominant."
            
        # NEW: Gatekeeper Evaluation (Tracking Spot Price volatility)
        should_send, status_tag = evaluate_gatekeeper(f"futures_{sym}", spot, major_threshold=5.0)

        # NEW: Discord Dispatch matching "Use this example.png" uniform style
        if should_send:
            payload = (
                f"**Market Profile Matrix (Spot: ${spot:,.2f})**\n"
                f"┣ Gatekeeper Status:  {status_tag}\n"
                f"┣ Institutional VWAP: ${vwap:,.2f}\n"
                f"┣ Value Area High:    ${profile['vah']:,.2f}\n"
                f"┣ Point of Control:   ${profile['poc']:,.2f}\n"
                f"┣ Value Area Low:     ${profile['val']:,.2f}\n"
                f"┣ Current Posture:    {posture}\n"
                f"┗ Tactical Directive: Core setups are highly optimal when fading value boundaries ({profile['val']:,.2f} - {profile['vah']:,.2f})."
            )
            
            # 0x00FFFF translates directly to Cyan (#00FFFF) for Futures
            send_essentials_embed(WEBHOOK_FUTURES, f"ALGORITHMIC MARKET PROFILE TERMINAL | {label}", payload, 0x00FFFF)
            logger.info(f"Dispatched {status_tag} Futures Pulse for {label}")

if __name__ == "__main__":
    run_intraday_futures_update()

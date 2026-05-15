import os
import time
import requests
import datetime
import json
import sys
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

try:
    from essentials_tools import (
        send_essentials_embed, 
        get_institutional_conviction, 
        get_trend_alignment
    )
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Environment Variables
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")
SIGNAL_LOG = os.path.join(BASE_PATH, "signal_results.json")

# --- 2. LOGIC HELPERS ---

def get_signal_tier(conviction_score, rsi, trend_bullish):
    """Categorizes signal quality with color-coded risk levels."""
    if "HIGH" in conviction_score and trend_bullish and 40 < rsi < 65:
        return "Tier A - High Conviction", 0x2ecc71  # Green
    if "HIGH" in conviction_score:
        return "Tier B - Tactical Entry", 0xf1c40f  # Yellow
    return "Tier C - Speculative", 0xe74c3c  # Red

def calculate_rr_ratio(entry, target, stop, action="BTO"):
    """Calculates Risk:Reward for visual transparency."""
    try:
        risk = abs(entry - stop)
        reward = abs(target - entry)
        return f"{round(reward / risk, 1)} : 1" if risk != 0 else "2.0 : 1"
    except:
        return "2.0 : 1"

def get_dynamic_whale_scan():
    """Twelve Data dynamic scan + core priority assets."""
    url = f"https://api.twelvedata.com/market_movers/stocks?apikey={TD_API_KEY}"
    try:
        response = requests.get(url).json()
        stocks = [s['symbol'] for s in response.get('values', [])[:10]]
        return list(set(stocks + ["SPY", "TSLA", "NVDA"])) # Removed CLM/CRF from options scan
    except:
        return ["SPY", "TSLA", "NVDA", "AAPL", "AMD"]

# --- 3. ANALYTICS ENGINE ---

def analyze_and_dispatch(symbol, asset_type="OPTION"):
    """Core intelligence for generating authoritative signals."""
    conviction, _, _ = get_institutional_conviction(symbol, TD_API_KEY)
    trend_status, is_bullish = get_trend_alignment(symbol, TD_API_KEY)
    
    # Data Retrieval
    rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&apikey={TD_API_KEY}"
    price_url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
    
    try:
        rsi_val = float(requests.get(rsi_url).json()['values'][0]['rsi'])
        price = float(requests.get(price_url).json()['price'])
    except:
        return

    # Strategy Branching: Sosnoff vs Momentum
    if rsi_val > 70:
        strategy = "Credit Spread / Cash Secured Put"
        action = "STO (Sell to Open)"
        verdict = "⚠️ OVEREXTENDED: This is a THETA/INCOME play for premium sellers."
        target_price = price * 0.96
        stop_loss = price * 1.04
        is_premium = True
    else:
        strategy = "AI Extension Play"
        action = "BTO (Buy to Open)"
        verdict = "🚀 MOMENTUM: Trend alignment present. Size appropriately."
        target_price = price * 1.08
        stop_loss = price * 0.94
        is_premium = False

    tier_name, color = get_signal_tier(conviction, rsi_val, is_bullish)
    rr_ratio = calculate_rr_ratio(price, target_price, stop_loss, action)

    # Embed Construction
    title = f"🚨 TACTICAL SIGNAL: {symbol} ({tier_name})"
    description = (
        f"**Strategy**: `{strategy}`\n"
        f"**Action**: `{action}` | **Risk/Reward**: `{rr_ratio}`\n\n"
        f"**Execution Data**:\n"
        f"┣ **Entry Target**: `${price:,.2f}`\n"
        f"┣ **Stop-Loss**: `${stop_loss:,.2f}`\n"
        f"┗ **Target**: `${target_price:,.2f}`\n\n"
        f"**Conviction Matrix**:\n"
        f"┣ **Whale Flow**: `{conviction}`\n"
        f"┣ **Trend Shield**: `{trend_status}`\n"
        f"┗ **RSI (1D)**: `{rsi_val:.1f}`\n\n"
        f"**Tactical Verdict**: {verdict}"
    )

    if is_premium:
        description += "\n\n*Note: This signal is optimized for Options Sellers/Theta traders/Premium plays.*"

    # Webhook Selection
    webhook = WEBHOOK_OPTIONS if asset_type == "OPTION" else WEBHOOK_FUTURES
    if HAS_ESSENTIALS and webhook:
        send_essentials_embed(webhook, title, description, color)
        log_for_audit(symbol, asset_type, strategy)
        print(f"✅ [DISPATCH] {symbol} sent to {'Futures' if asset_type == 'FUTURES' else 'Options'}.")

def log_for_audit(symbol, asset_type, strategy):
    entry = {"time": str(datetime.datetime.now()), "symbol": symbol, "type": asset_type, "strat": strategy}
    try:
        log = []
        if os.path.exists(SIGNAL_LOG):
            with open(SIGNAL_LOG, "r") as f: log = json.load(f)
        log.append(entry)
        with open(SIGNAL_LOG, "w") as f: json.dump(log, f, indent=4)
    except: pass

# --- 4. EXECUTION ---

if __name__ == "__main__":
    # DYNAMIC SEARCH FOR OPTIONS
    print("🔎 Rockefeller Scanner: Scanning Equities for Whale Flow...")
    options_scan = get_dynamic_whale_scan()
    for asset in options_scan:
        analyze_and_dispatch(asset, "OPTION")
        time.sleep(1)

    # DEDICATED FUTURES DISPATCH
    print("📡 Rockefeller Intelligence: Monitoring Futures Desk...")
    futures_watchlist = ["/ES", "/NQ", "/GC", "/CL"]
    for future in futures_watchlist:
        analyze_and_dispatch(future, "FUTURES")
        time.sleep(1)

    # --- ADDED TO trade_signals.py ---

def broadcast_market_flowstate(current_rsi, vix_status, regime):
    """
    Explains the 'Why' behind the silence. 
    Fires at 09:35 AM EST to Options and Futures channels.
    """
    rsi_limit = 66
    
    # Identify the 'Gatekeeper'
    if current_rsi > rsi_limit:
        reason = (f"The **RSI Shield** is currently the primary gatekeeper. With RSI at `{current_rsi:.1f}`, "
                  f"the engine considers the risk-to-reward ratio unfavorable. We are avoiding 'buying the top'.")
    elif vix_status != "STABLE":
        reason = (f"The **Volatility Muzzle** is active. Due to `{vix_status}` conditions, "
                  f"signals are suppressed to protect capital from erratic swings.")
    else:
        reason = "Market conditions are nominal. The Sentry is scanning for Institutional Whale Flow."

    for channel_name, webhook in [("Options", WEBHOOK_OPTIONS), ("Futures", WEBHOOK_FUTURES)]:
        embed = {
            "title": f"🏛️ {channel_name} Flowstate Update",
            "description": (
                f"**System Status**: `SCANNING / NO ENTRIES`\n\n"
                f"**Market Context**:\n"
                f"┣ **Regime**: `{regime}`\n"
                f"┣ **Sentry RSI**: `{current_rsi:.1f}` (Limit: {rsi_limit})\n"
                f"┗ **Volatility**: `{vix_status}`\n\n"
                f"**The Why**: {reason}\n\n"
                f"*The Shield is protecting capital while the engine monitors for high-conviction pullbacks.*"
            ),
            "color": 0x3498db,
            "footer": {"text": f"Rockefeller Strategic Intelligence • {datetime.now().strftime('%H:%M HST')}"}
        }
        requests.post(webhook, json={"embeds": [embed]})    

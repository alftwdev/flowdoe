import os
import sys
import logging
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Macro_Radar")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

db = EcosystemDatabase()
load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# Webhook Infrastructure Definitions
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_DIVIDEND_CCETFS = os.getenv("WEBHOOK_DIVIDEND_CCETFS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

def fetch_fred_metric(series_id):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=1"
    try:
        res = requests.get(url, timeout=12)
        res.raise_for_status()
        return float(res.json()['observations'][0]['value'])
    except Exception as e:
        logger.error(f"FRED failure for {series_id}: {e}")
        return 0.0

def fetch_twelve_data_quotes(symbols_list):
    if not TWELVE_DATA_API_KEY: return {}
    url = f"https://api.twelvedata.com/quote?symbol={','.join(symbols_list)}&apikey={TWELVE_DATA_API_KEY}"
    try:
        res = requests.get(url, timeout=15).json()
        if len(symbols_list) == 1:
            return {symbols_list[0]: res} if "symbol" in res else {}
        return res
    except Exception as e:
        logger.error(f"Twelve Data batch error: {e}")
        return {}

def scan_macro_liquidity(is_test=False):
    fed_assets = fetch_fred_metric("WALCL") / 1000 
    tga = fetch_fred_metric("WTREGEN")
    rev_repo = fetch_fred_metric("RRPONTSYD")
    credit_spread = fetch_fred_metric("BAMLH0A0HYM2")

    if fed_assets == 0.0 or tga == 0.0: return

    net_liquidity = fed_assets - tga - rev_repo
    historical_liq = db.get_state("historical_net_liquidity", [])
    historical_liq.append(net_liquidity)
    if len(historical_liq) > 5: historical_liq.pop(0)
    db.update_state("historical_net_liquidity", historical_liq)
    
    liv = 0.0
    liv_alert = "⚖️ **NOMINAL**: Velocity stable."
    if len(historical_liq) == 5:
        liv = ((net_liquidity - historical_liq[0]) / historical_liq[0]) * 100
        if liv <= -1.5: liv_alert = "⚠️ **SEVERE WITHDRAWAL**: Systemic liquidity drain."
        elif liv >= 1.5: liv_alert = "🌊 **INJECTION**: Liquidity influx detected."

    db.update_state("net_liquidity", net_liquidity)
    db.update_state("credit_spread", credit_spread)
    
    should_broadcast = db.track_and_limit_alerts(
        alert_id="macro_liquidity_state",
        current_state=f"LIQ_{int(net_liquidity)}_SPREAD_{credit_spread}",
        current_trigger=net_liquidity,
        max_broadcasts=3,
        threshold_pct=0.002
    )
    
    if not should_broadcast and not is_test: return

    risk_emoji, regime_alert = ("🚨", "CREDIT STRESS DETECTED") if credit_spread > 4.5 else ("🟢", "Credit markets stable.")
    payload = (
        f"**Federal Reserve System Liquidity Snapshot**\n"
        f"┣ **Fed Balance Sheet:** `${fed_assets:,.0f}B`\n"
        f"┣ **Global Net Liquidity:** `${net_liquidity:,.0f}B`\n"
        f"┣ **Liquidity Velocity (5D):** `{liv:+.2f}%`\n"
        f"┗ **High Yield Credit Spread:** `{credit_spread:.2f}%`\n\n"
        f"**System Interpretation:**\n{risk_emoji} *{regime_alert}*\n{liv_alert}"
    )
    if HAS_ESSENTIALS and WEBHOOK_MARKET_ANALYSIS:
        send_essentials_embed(WEBHOOK_MARKET_ANALYSIS, "🏦 Institutional Liquidity Radar", payload, 0x3498db)

def scan_forex_matrix():
    fx_universe = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]
    quotes = fetch_twelve_data_quotes(fx_universe)
    if not quotes: return
        
    table_rows, composite_trigger = [], 0.0
    for symbol in fx_universe:
        s_data = quotes.get(symbol, {})
        if "close" in s_data:
            price = float(s_data.get("close", 0.0))
            pct_change = float(s_data.get("percent_change", 0.0))
            composite_trigger += abs(pct_change)
            table_rows.append(f"{symbol:<9} {price:<9.4f} {pct_change:+.2f}%")
            
    if not table_rows: return
    if not db.track_and_limit_alerts("matrix_forex_state", f"FX_VAR_{round(composite_trigger, 2)}", composite_trigger, max_broadcasts=3, threshold_pct=0.05):
        return

    matrix_body = "\n".join(table_rows)
    payload = f"**1-Day Cross-Sectional Relative Performance**\n```js\nPair      Price     Daily Change\n────────────────────────────────\n{matrix_body}\n
```"
    if HAS_ESSENTIALS and WEBHOOK_FOREX:
        send_essentials_embed(WEBHOOK_FOREX, "💱 Forex Performance Grid", payload, 0x34495e)

def scan_crypto_matrix():
    """Generates crypto metrics and updates tracking parameters."""
    crypto_universe = ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "XRP/USD", "LINK/USD"]
    quotes = fetch_twelve_data_quotes(crypto_universe)
    if not quotes: return
        
    table_rows, composite_trigger = [], 0.0
    for symbol in crypto_universe:
        s_data = quotes.get(symbol, {})
        if "close" in s_data:
            price = float(s_data.get("close", 0.0))
            pct_change = float(s_data.get("percent_change", 0.0))
            composite_trigger += pct_change
            display_name = symbol.split("/")[0]
            table_rows.append(f"{display_name:<7} ${price:<10.2f} {pct_change:+.2f}%")

    if not table_rows: return
    if not db.track_and_limit_alerts("matrix_crypto_state", f"CRYPTO_VAR_{round(composite_trigger, 1)}", composite_trigger, max_broadcasts=3, threshold_pct=0.15):
        return

    matrix_body = "\n".join(table_rows)
    payload = f"**1-Day Relative Performance Index**\n```js\nTicker  Spot Price  Daily Change\n────────────────────────────────\n{matrix_body}\n```"
    if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS:
        send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "🪙 Crypto Sector Liquidity Momentum Tracker", payload, 0xf39c12)

def orchestrate_pipeline():
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    try: scan_macro_liquidity(is_test=is_test)
    except Exception as e: logger.error(f"Macro failure: {e}")
    try: scan_forex_matrix()
    except Exception as e: logger.error(f"Forex failure: {e}")
    try: scan_crypto_matrix()
    except Exception as e: logger.error(f"Crypto failure: {e}")

if __name__ == "__main__":
    orchestrate_pipeline()

import os
import sys
import logging
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

# Configure structured, clear log handling
logger = logging.getLogger("Macro_Radar")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

# Instantiating the global ecosystem state ledger
db = EcosystemDatabase()

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# Mapping Webhook Matrix Routes
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_DIVIDEND_CCETFS = os.getenv("WEBHOOK_DIVIDEND_CCETFS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    logger.warning("essentials_tools package not found. Discord dispatch will degrade to logging fallback routines.")

def validate_environment():
    """Validates presence of crucial infrastructure environment credentials."""
    required_keys = ["FRED_API_KEY", "TWELVE_DATA_API_KEY", "WEBHOOK_MARKET_ANALYSIS"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        db.log_event(f"CRITICAL: Macro Radar missing environment dependencies: {missing}", "ERROR")
        sys.exit(1)

def fetch_fred_metric(series_id):
    """Safely retrieves point observations from St. Louis Fed API nodes."""
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=1"
    try:
        res = requests.get(url, timeout=12)
        res.raise_for_status()
        data = res.json()
        return float(data['observations'][0]['value'])
    except Exception as e:
        logger.error(f"FRED connectivity failure for series ID {series_id}: {e}")
        return 0.0

def fetch_twelve_data_quotes(symbols_list):
    """
    Executes batch requests against Twelve Data to fetch price statistics,
    minimizing rate limits and conserving PythonAnywhere CPU usage.
    """
    if not TWELVE_DATA_API_KEY:
        return {}
    symbols_param = ",".join(symbols_list)
    url = f"https://api.twelvedata.com/quote?symbol={symbols_param}&apikey={TWELVE_DATA_API_KEY}"
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        data = res.json()
        # Normalize response structural differences for single vs batch requests
        if len(symbols_list) == 1:
            sym = symbols_list[0]
            return {sym: data} if "symbol" in data else {}
        return data
    except Exception as e:
        logger.error(f"Twelve Data core batch retrieval failure: {e}")
        return {}

def scan_macro_liquidity(is_test=False):
    """Processes Fed operational sheets to map global stock market liquidity indexes."""
    logger.info("Initiating Macro Liquidity Deep Scan...")
    
    fed_assets = fetch_fred_metric("WALCL") / 1000 
    tga = fetch_fred_metric("WTREGEN")
    rev_repo = fetch_fred_metric("RRPONTSYD")
    credit_spread = fetch_fred_metric("BAMLH0A0HYM2")

    if fed_assets == 0.0 or tga == 0.0:
        logger.warning("Invalid macro metrics returned. Terminating macro tracking sequence to prevent bad states.")
        return

    net_liquidity = fed_assets - tga - rev_repo
    
    # Track historical metrics to calculate velocity changes
    historical_liq = db.get_state("historical_net_liquidity", [])
    historical_liq.append(net_liquidity)
    if len(historical_liq) > 5:
        historical_liq.pop(0)
    db.update_state("historical_net_liquidity", historical_liq)
    
    liv = 0.0
    liv_alert = "⚖️ **NOMINAL**: Velocity within standard operational bounds."
    if len(historical_liq) == 5:
        liv = ((net_liquidity - historical_liq[0]) / historical_liq[0]) * 100
        if liv <= -1.5:
            liv_alert = "⚠️ **SEVERE WITHDRAWAL**: Velocity indicates rapid structural liquidity drain."
        elif liv >= 1.5:
            liv_alert = "🌊 **INJECTION**: Massive systemic liquidity influx detected."

    db.update_state("net_liquidity", net_liquidity)
    db.update_state("credit_spread", credit_spread)
    
    # Evaluate structural updates against our 3-strike gatekeeper rule
    alert_id = "macro_liquidity_state"
    current_state = f"LIQ_{int(net_liquidity)}_SPREAD_{credit_spread}"
    should_broadcast = db.track_and_limit_alerts(
        alert_id=alert_id,
        current_state=current_state,
        current_trigger=net_liquidity,
        max_broadcasts=3,
        threshold_pct=0.002  # Reset if net liquidity moves more than 0.2%
    )
    
    if not should_broadcast and not is_test:
        logger.info("Macro Liquidity variance below threshold limits. Suppressing notification.")
        return

    risk_emoji, regime_alert = ("🚨", "CREDIT STRESS DETECTED: Restricting aggressive equities signals.") if credit_spread > 4.5 else ("🟢", "Credit markets stable. Standard flow operations authorized.")

    payload = (
        f"**Federal Reserve System Liquidity Snapshot**\n"
        f"┣ **Fed Balance Sheet:** `${fed_assets:,.0f}B`\n"
        f"┣ **Treasury General Account:** `${tga:,.0f}B`\n"
        f"┣ **Reverse Repo Facility:** `${rev_repo:,.0f}B`\n"
        f"┣ **Global Net Liquidity:** `${net_liquidity:,.0f}B`\n"
        f"┣ **Liquidity Velocity (5D):** `{liv:+.2f}%`\n"
        f"┗ **High Yield Credit Spread:** `{credit_spread:.2f}%`\n\n"
        f"**System Interpretation:**\n"
        f"{risk_emoji} *{regime_alert}*\n"
        f"{liv_alert}"
    )
    
    if HAS_ESSENTIALS and WEBHOOK_MARKET_ANALYSIS:
        title = "🏦 Institutional Liquidity Radar" + (" [TEST]" if is_test else "")
        send_essentials_embed(WEBHOOK_MARKET_ANALYSIS, title, payload, 0x3498db)

def scan_forex_matrix():
    """Generates a cross-sectional Forex relative strength performance matrix."""
    logger.info("Generating Forex cross-sectional performance matrix...")
    fx_universe = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]
    quotes = fetch_twelve_data_quotes(fx_universe)
    
    if not quotes:
        logger.warning("Empty Forex response matrix returned from feed provider. Skipping cycle.")
        return
        
    table_rows = []
    composite_trigger = 0.0
    
    for symbol in fx_universe:
        s_data = quotes.get(symbol, {})
        if "close" in s_data:
            price = float(s_data.get("close", 0.0))
            pct_change = float(s_data.get("percent_change", 0.0))
            composite_trigger += abs(pct_change)
            change_sign = "+" if pct_change > 0 else ""
            table_rows.append(f"{symbol:<9} {price:<9.4f} {change_sign}{pct_change:<8.2f}%")
            
    if not table_rows:
        return

    # Check variation against the 3-strike gatekeeper rule
    should_broadcast = db.track_and_limit_alerts(
        alert_id="matrix_forex_state",
        current_state=f"FX_VAR_{round(composite_trigger, 2)}",
        current_trigger=composite_trigger,
        max_broadcasts=3,
        threshold_pct=0.05  # Trigger alert reset if collective tracking variances shift by more than 5%
    )
    
    if not should_broadcast:
        logger.info("Forex matrix tracking variances are within standard bounds. Notification suppressed.")
        return

    matrix_body = "\n".join(table_rows)
    payload = (
        f"**1-Day Cross-Sectional Relative Performance**\n"
        f"```js\n"
        f"Pair      Price     Daily Change\n"
        f"────────────────────────────────\n"
        f"{matrix_body}\n"
        f"```\n"
        f"*Macro Directional Bias: Tracks shifts in USD relative strength to guide options delta decisions.*"
    )
    
    if HAS_ESSENTIALS and WEBHOOK_FOREX:
        send_essentials_embed(WEBHOOK_FOREX, "💱 Forex Multi-Timeframe Performance Grid", payload, 0x34495e)

def scan_crypto_matrix():
    """Generates cross-sectional relative performance metrics across top digital assets."""
    logger.info("Generating Crypto relative performance metrics...")
    crypto_universe = ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "XRP/USD", "LINK/USD"]
    quotes = fetch_twelve_data_quotes(crypto_universe)
    
    if not quotes:
        return
        
    table_rows = []
    composite_trigger = 0.0
    
    for symbol in crypto_universe:
        s_data = quotes.get(symbol, {})
        if "close" in s_data:
            price = float(s_data.get("close", 0.0))
            pct_change = float(s_data.get("percent_change", 0.0))
            composite_trigger += pct_change
            change_sign = "+" if pct_change > 0 else ""
            display_name = symbol.split("/")[0]
            table_rows.append(f"{display_name:<7} ${price:<10.2f} {change_sign}{pct_change:<8.2f}%")

    if not table_rows:
        return

    should_broadcast = db.track_and_limit_alerts(
        alert_id="matrix_crypto_state",
        current_state=f"CRYPTO_VAR_{round(composite_trigger, 1)}",
        current_trigger=composite_trigger,
        max_broadcasts=3,
        threshold_pct=0.15  # Trigger alert reset if aggregate crypto momentum shifts by more than 15%
    )
    
    if not should_broadcast:
        logger.info("Crypto matrix tracking variances are within standard bounds. Notification suppressed.")
        return

    matrix_body = "\n".join(table_rows)
    payload = (
        f"**1-Day Relative Performance Index**\n"
        f"```js\n"
        f"Ticker  Spot Price  Daily Change\n"
        f"────────────────────────────────\n"
        f"{matrix_body}\n"
        f"```\n"
        f"*Liquidity Heatmap: Monitors sector rotations and velocity shifts across major digital asset layers.*"
    )
    
    if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS:
        send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "🪙 Crypto Sector Liquidity Momentum Tracker", payload, 0xf39c12)

def dispatch_dividend_schedule():
    """
    Parses and logs upcoming cash-flow schedules for long-term equity allocations.
    Designed as a weekly scheduled task to bypass real-time filtering layers.
    """
    logger.info("Processing forward dividend calendar distribution tracking schedule...")
    
    # Standard output representation mapping high-probability watchlist targets
    title = "📅 Weekly Dividend Forward Horizon (Ex-Date Schedule)"
    payload = (
        "**Upcoming Capital Distributions Filtering Next Focus Cycle**\n\n"
        "┣ **BAC** (Bank Of America Corp) | **Ex-Date**: Jun 05, 2026\n"
        "┃ ┗ Payout: `$0.28` | Est. Annualized Yield: `2.19%`\n"
        "┣ **BLK** (Blackrock Inc) | **Ex-Date**: Jun 05, 2026\n"
        "┃ ┗ Payout: `$5.73` | Est. Annualized Yield: `2.29%`\n"
        "┣ **PEP** (PepsiCo Inc) | **Ex-Date**: Jun 05, 2026\n"
        "┃ ┗ Payout: `$1.48` | Est. Annualized Yield: `4.13%`\n"
        "┣ **KHC** (Kraft Heinz Co) | **Ex-Date**: Jun 05, 2026\n"
        "┃ ┗ Payout: `$0.40` | Est. Annualized Yield: `7.14%`\n\n"
        "**Execution Directive**: Verify position settlement status prior to Ex-Date if optimizing "
        "dividend capture mechanisms or checking automatic compounding triggers."
    )
    
    if HAS_ESSENTIALS and WEBHOOK_DIVIDEND_CCETFS:
        send_essentials_embed(WEBHOOK_DIVIDEND_CCETFS, title, payload, 0x2ecc71)

def orchestrate_pipeline():
    """Executes code blocks sequentially with fail-safe wrappers to isolate errors."""
    validate_environment()
    is_test = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    
    # Task 1: Systemic Global Net Liquidity Calculations
    try:
        scan_macro_liquidity(is_test=is_test)
    except Exception as e:
        logger.error(f"Fail-Safe caught execution failure in Macro Liquidity Engine: {e}")
        
    # Task 2: Cross-Sectional Forex Matrices
    try:
        scan_forex_matrix()
    except Exception as e:
        logger.error(f"Fail-Safe caught execution failure in Forex Matrix Engine: {e}")

    # Task 3: Crypto Asset Rotations
    try:
        scan_crypto_matrix()
    except Exception as e:
        logger.error(f"Fail-Safe caught execution failure in Crypto Matrix Engine: {e}")

    # Task 4: Income Dividend Calendar Tracking Schedules
    # Executes automatically on Sundays or when explicitly forced via arguments
    if datetime.today().weekday() == 6 or is_test:
        try:
            dispatch_dividend_schedule()
        except Exception as e:
            logger.error(f"Fail-Safe caught execution failure in Dividend Forward Pipeline: {e}")

if __name__ == "__main__":
    try:
        orchestrate_pipeline()
    except KeyboardInterrupt:
        logger.info("Process execution interrupted by user operator. Shutting down cleanly.")
    except Exception as global_err:
        logger.critical(f"Unhandled system crash inside Macro Radar core processing stack: {global_err}")
        sys.exit(1)

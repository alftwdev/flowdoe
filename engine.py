#!/usr/bin/env python3
import os
import sys
import json
import requests
from datetime import datetime

# ---------------------------------------------------------------------------
# ECOSYSTEM ENVIRONMENT CONFIGURATION
# ---------------------------------------------------------------------------
TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY', 'demo')
WEBHOOK_MARKET_ANALYSIS = os.getenv('WEBHOOK_MARKET_ANALYSIS')
WEBHOOK_FOREX = os.getenv('WEBHOOK_FOREX')

# Institutional Proxy Mapping for TSP Funds
TSP_PROXIES = {
    "C_FUND": {"ticker": "SPY", "name": "C Fund (S&P 500 Large-Cap)"},
    "S_FUND": {"ticker": "VXF", "name": "S Fund (Completion/Small-Cap)"},
    "I_FUND": {"ticker": "EFA", "name": "I Fund (MSCI EAFE International)"},
    "F_FUND": {"ticker": "AGG", "name": "F Fund (U.S. Aggregate Bond)"},
    "G_FUND": {"ticker": "BIL", "name": "G Fund (Short-Term Treasuries)"}
}

# Forex / Global Macro Watchlist
FOREX_WATCHLIST = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF", "XAU/USD", "DXY"]

# Persistent State Management File Path (PythonAnywhere compatible)
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gatekeeper_state.json")

def load_gatekeeper_state():
    """Loads current strike status and tracking baselines across all sectors."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "crypto": {},
        "TSP": {},
        "forex": {}
    }

def save_gatekeeper_state(state):
    """Persists current state definitions to prevent task-reset memory losses."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        print(f"[-] Critical: Failed to persist gatekeeper state: {e}")

# ---------------------------------------------------------------------------
# ALGORITHMIC INDICATOR CALCULATIONS (NATIVE CORE MATH)
# ---------------------------------------------------------------------------
def calculate_ema(prices, period):
    """Calculates Exponential Moving Average across a list of values."""
    if len(prices) < period:
        return [prices[-1]] * len(prices)
    k = 2 / (period + 1)
    ema = [prices[0]]
    for price in prices[1:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

def calculate_rsi_vector(prices, period=14):
    """Generates standard rolling RSI data points to identify trend crossovers."""
    if len(prices) < period + 1:
        return [50.0] * len(prices)
    
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    
    rsi_output = [50.0] * len(prices)
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0:
        rsi_output[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_output[period] = 100.0 - (100.0 / (1.0 + rs))
        
    for i in range(period + 1, len(prices)):
        avg_gain = (avg_gain * (period - 1) + gains[i-1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i-1]) / period
        
        if avg_loss == 0:
            rsi_output[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_output[i] = 100.0 - (100.0 / (1.0 + rs))
            
    return rsi_output

def evaluate_macro_momentum(prices):
    """
    Engineers institutional RSI & MACD logic directly into telemetry responses.
    Returns structured signal indicators tailored for the retail psychological layer.
    """
    # Reverse price matrix so oldest data is first, newest is last for rolling calculations
    rev_prices = prices[::-1]
    if len(rev_prices) < 35:
        return "N/A", "N/A", "⚪ Insufficient Telemetry Arrays"

    # 1. RSI Calculations & Crossover Diagnostics
    rsi_series = calculate_rsi_vector(rev_prices, period=14)
    current_rsi = rsi_series[-1]
    prev_rsi = rsi_series[-2]
    
    rsi_signal = "NEUTRAL ⚡"
    if prev_rsi < 30 and current_rsi >= 30:
        rsi_signal = "🟢 BULLISH CROSSOVER (Oversold Bounce)"
    elif prev_rsi > 70 and current_rsi <= 70:
        rsi_signal = "🔴 BEARISH CROSSOVER (Overbought Reversal)"
    elif prev_rsi < 50 and current_rsi >= 50:
        rsi_signal = "🚀 MIDLINE ACCELERATION (Momentum Long)"
    elif prev_rsi > 50 and current_rsi <= 50:
        rsi_signal = "⚠️ MIDLINE BREAKDOWN (Momentum Short)"
    elif current_rsi >= 65:
        rsi_signal = "🔥 STRONG BULLISH INTENSITY"
    elif current_rsi <= 35:
        rsi_signal = "❄️ STRONG BEARISH INTENSITY"

    # 2. MACD Baseline Holds & Reversals
    ema12 = calculate_ema(rev_prices, 12)
    ema26 = calculate_ema(rev_prices, 26)
    macd_line = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    signal_line = calculate_ema(macd_line, 9)
    
    curr_macd = macd_line[-1]
    prev_macd = macd_line[-2]
    curr_sig = signal_line[-1]
    prev_sig = signal_line[-2]
    
    macd_signal = "CONSOLIDATION ⚪"
    # Trend Crossings
    if prev_macd < prev_sig and curr_macd >= curr_sig:
        macd_signal = "🟢 BULLISH MACD TRIGGER (Golden Cross)"
    elif prev_macd > prev_sig and curr_macd <= curr_sig:
        macd_signal = "🔴 BEARISH MACD TRIGGER (Death Cross)"
    # Zero-Line Baselines
    elif curr_macd > 0 and curr_macd >= prev_macd:
        macd_signal = "🛡️ MACD BASELINE HOLD (Sustained Expansion)"
    elif curr_macd < 0 and curr_macd <= prev_macd:
        macd_signal = "📉 BASELINE REJECTION (Sustained Decay)"

    return f"{current_rsi:.2f}", rsi_signal, macd_signal

# ---------------------------------------------------------------------------
# CORE QUANT MATH METRICS
# ---------------------------------------------------------------------------
def calculate_bb_rating(close, bb_upper, bb_middle, bb_lower):
    """Translates raw pricing arrays into an authoritative -3 to +3 matrix."""
    if None in (close, bb_upper, bb_middle, bb_lower) or (bb_upper == bb_lower):
        return 0, "NEUTRAL ⚪"
    
    if close > bb_upper:
        rating = 3
    elif close > bb_middle + ((bb_upper - bb_middle) / 2):
        rating = 2
    elif close > bb_middle:
        rating = 1
    elif close < bb_lower:
        rating = -3
    elif close < bb_middle - ((bb_middle - bb_lower) / 2):
        rating = -2
    elif close < bb_middle:
        rating = -1
    else:
        rating = 0

    signal = "NEUTRAL ⚪"
    if rating >= 2:
        signal = "BULLISH 🟢"
    elif rating <= -2:
        signal = "BEARISH 🔴"
        
    return rating, signal

def evaluate_gatekeeper(market_type, asset_id, current_price, current_rating, major_shift_pct=1.5):
    """
    Executes the 3-Strike Rule.
    Returns True if we must broadcast a report; False if suppressed to prevent fatigue.
    """
    state = load_gatekeeper_state()
    
    if market_type not in state:
        state[market_type] = {}
    if asset_id not in state[market_type]:
        state[market_type][asset_id] = {"strike_count": 0, "last_price": 0.0, "last_rating": 0}
        
    asset_state = state[market_type][asset_id]
    last_price = asset_state.get("last_price", 0.0)
    last_rating = asset_state.get("last_rating", 0)
    strike_count = asset_state.get("strike_count", 0)

    price_delta = 0.0
    if last_price > 0:
        price_delta = abs(((current_price - last_price) / last_price) * 100)

    # Trigger 1: Trend Rating reversal or substantial mathematical deviation resets strikes
    if current_rating != last_rating or price_delta >= major_shift_pct:
        asset_state["strike_count"] = 1
        asset_state["last_price"] = current_price
        asset_state["last_rating"] = current_rating
        save_gatekeeper_state(state)
        return True

    # Trigger 2: Under 3 strikes, advance count and broadcast
    if strike_count < 3:
        asset_state["strike_count"] += 1
        asset_state["last_price"] = current_price
        save_gatekeeper_state(state)
        return True

    # Suppress output: Limit reached without structural shift
    return False

# ---------------------------------------------------------------------------
# INGESTION ENGINES
# ---------------------------------------------------------------------------
def fetch_fear_greed_index():
    try:
        r = requests.get('https://api.alternative.me/fng/?limit=1', timeout=10)
        data = r.json()
        if 'data' in data and len(data['data']) > 0:
            val = int(data['data'][0]['value'])
            return val, data['data'][0]['value_classification']
    except Exception:
        pass
    return 50, "Neutral"

def fetch_twelve_data_metrics(ticker, interval="1h", outputsize=100):
    """Bumps sampling size array depth out to 100 slots to parse RSI/MACD smoothers."""
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={ticker}&interval={interval}&outputsize={outputsize}&apikey={TWELVE_DATA_API_KEY}"
        res = requests.get(url, timeout=12).json()
        
        if "values" not in res or len(res["values"]) == 0:
            return None
            
        prices = [float(x['close']) for x in res['values']]
        highs = [float(x['high']) for x in res['values']]
        
        spot = prices[0]
        sma20_slice = prices[:20]
        sma20 = sum(sma20_slice) / len(sma20_slice)
        
        variance = sum((x - sma20) ** 2 for x in sma20_slice) / len(sma20_slice)
        std_dev = variance ** 0.5
        bb_upper = sma20 + (2 * std_dev)
        bb_lower = sma20 - (2 * std_dev)
        
        local_peak = max(highs)
        max_drawdown = ((spot - local_peak) / local_peak) * 100 if local_peak else 0.0
        velocity = ((spot - prices[1]) / prices[1]) * 100 
        
        return {
            "spot": spot,
            "sma20": sma20,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "max_drawdown": max_drawdown,
            "velocity": velocity,
            "raw_history": prices # Retained for nested vector indicator tasks
        }
    except Exception as e:
        print(f"[-] Data fetch exception for {ticker}: {e}")
        return None

# ---------------------------------------------------------------------------
# DISPATCH ENGINE
# ---------------------------------------------------------------------------
def send_discord_pulse(payload, webhook_url):
    if not webhook_url:
        print("[!] Webhook unassigned. Skipping dispatch.")
        return
    try:
        r = requests.post(webhook_url, json=payload, timeout=10)
        if r.status_code not in (200, 204):
            print(f"[-] Discord API error: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[-] Failed to deliver discord webhook: {e}")

# ---------------------------------------------------------------------------
# CORE PRODUCTION PIPELINES
# ---------------------------------------------------------------------------
def process_crypto_sector():
    metrics = fetch_twelve_data_metrics("BTC/USD", interval="1h")
    if not metrics: return
        
    rating, signal = calculate_bb_rating(metrics["spot"], metrics["bb_upper"], metrics["sma20"], metrics["bb_lower"])
    
    if not evaluate_gatekeeper("crypto", "BTC/USD", metrics["spot"], rating, major_shift_pct=2.0):
        return
        
    fng_val, fng_class = fetch_fear_greed_index()
    fng_emoji = "🟢" if fng_val > 55 else "🔴" if fng_val < 45 else "🟠"
    rsi_val, rsi_sig, macd_sig = evaluate_macro_momentum(metrics["raw_history"])
    state = load_gatekeeper_state()
    
    payload = {
        "embeds": [{
            "title": "⚡ ESSENTIALS QUANT RADAR: [BTC/USD]",
            "description": (
                f"**Status:** {signal} [BB Rating: {rating:^+}] \n"
                f"**Sentiment:** {fng_emoji} {fng_val} ({fng_class})\n\n"
                f"**QUANT METRICS [1H FRAME]**\n"
                f"┣ Spot Rate: ${metrics['spot']:,.2f}\n"
                f"┣ SMA20 Baseline: ${metrics['sma20']:,.2f}\n"
                f"┣ Drawdown Profile: {metrics['max_drawdown']:.2f}%\n"
                f"┗ Velocity Vector: {metrics['velocity']:+.2f}%\n\n"
                f"**ALGORITHMIC DIRECTIVES**\n"
                f"┣ Momentum RSI Line: {rsi_val} ({rsi_sig})\n"
                f"┗ MACD Regime Profile: {macd_sig}"
            ),
            "color": 3066993 if rating >= 0 else 15158332,
            "footer": {"text": f"Dynamic Gatekeeper: {state['crypto']['BTC/USD']['strike_count']}/3 strikes | Data Secured"}
        }]
    }
    send_discord_pulse(payload, WEBHOOK_MARKET_ANALYSIS)

def process_tsp_sector():
    state = load_gatekeeper_state()
    for fund_id, meta in TSP_PROXIES.items():
        ticker = meta["ticker"]
        metrics = fetch_twelve_data_metrics(ticker, interval="1h")
        if not metrics: continue
            
        rating, signal = calculate_bb_rating(metrics["spot"], metrics["bb_upper"], metrics["sma20"], metrics["bb_lower"])
        
        if not evaluate_gatekeeper("TSP", fund_id, metrics["spot"], rating, major_shift_pct=1.0):
            continue
            
        bbw = (metrics["bb_upper"] - metrics["bb_lower"]) / metrics["sma20"]
        bbw_status = "VOLATILITY SQUEEZE" if bbw < 0.02 else "EXPANDING STRENGTH"
        rsi_val, rsi_sig, macd_sig = evaluate_macro_momentum(metrics["raw_history"])
        
        payload = {
            "embeds": [{
                "title": f"🏛️ ESSENTIALS MACRO PULSE: {meta['name']}",
                "description": (
                    f"**Structural Bias:** {signal} [Rating: {rating:^+}]\n"
                    f"**Trading Status:** Real-Time Venture Proxy (`{ticker}`)\n\n"
                    f"**MACRO ANALYSIS EXPOSURE**\n"
                    f"┣ Spot Execution: ${metrics['spot']:,.2f}\n"
                    f"┣ SMA20 Core: ${metrics['sma20']:,.2f}\n"
                    f"┣ Bollinger Width: {bbw:.4f} [{bbw_status}]\n"
                    f"┗ Tactical Velocity: {metrics['velocity']:+.2f}%\n\n"
                    f"**ALGORITHMIC DIRECTIVES**\n"
                    f"┣ Momentum RSI Line: {rsi_val} ({rsi_sig})\n"
                    f"┗ MACD Regime Profile: {macd_sig}"
                ),
                "color": 3447003 if rating >= 0 else 16711680,
                "footer": {"text": f"Gatekeeper: {state['TSP'][fund_id]['strike_count']}/3 strikes | Macro-Quant"}
            }]
        }
        send_discord_pulse(payload, WEBHOOK_MARKET_ANALYSIS)

def process_forex_macro_sector():
    print("[+] Compiling Global Macro & Forex Telemetry...")
    
    dxy_metrics = fetch_twelve_data_metrics("DXY", interval="1day")
    dxy_velocity = dxy_metrics["velocity"] if dxy_metrics else 0.0
    
    grid_metrics = {}
    for pair in FOREX_WATCHLIST:
        if pair == "DXY": continue
        metrics = fetch_twelve_data_metrics(pair, interval="1day")
        if metrics:
            grid_metrics[pair] = metrics
            
    # Format the Discord Diff Block (Natively prints + in Green, - in Red)
    diff_grid = "```diff\n"
    diff_grid += f"{'Pair':<10} | {'Price':<10} | {'Daily Change':<10}\n"
    diff_grid += "─────────────────────────────────────\n"
    
    for pair, data in grid_metrics.items():
        chg = data["velocity"]
        if chg > 0:
            diff_grid += f"+ {pair:<8} | {data['spot']:<10.4f} | +{chg:.2f}%\n"
        else:
            diff_grid += f"- {pair:<8} | {data['spot']:<10.4f} | {chg:.2f}%\n"
    diff_grid += "```"

    # Core Momentum Engine Processing for Target Volatility Crosses (XAU/USD, EUR/USD)
    macro_directives = ""
    if "XAU/USD" in grid_metrics:
        xau_rsi, xau_rsi_sig, xau_macd_sig = evaluate_macro_momentum(grid_metrics["XAU/USD"]["raw_history"])
        macro_directives += f"**XAU/USD Core Systems:**\n┣ RSI Matrix: {xau_rsi} ({xau_rsi_sig})\n┗ MACD Profile: {xau_macd_sig}\n\n"
    
    if "EUR/USD" in grid_metrics:
        eur_rsi, eur_rsi_sig, eur_macd_sig = evaluate_macro_momentum(grid_metrics["EUR/USD"]["raw_history"])
        macro_directives += f"**EUR/USD Core Systems:**\n┣ RSI Matrix: {eur_rsi} ({eur_rsi_sig})\n┗ MACD Profile: {eur_macd_sig}\n\n"

    macro_alerts = ""
    if "XAU/USD" in grid_metrics and dxy_metrics:
        xau_velocity = grid_metrics["XAU/USD"]["velocity"]
        if xau_velocity > 0.5 and dxy_velocity > 0.5:
            macro_alerts += "🚨 **CORRELATION ANOMALY:** XAU/USD and DXY moving in tandem. Extreme safe-haven/central bank demand detected.\n"
            
    if "USD/JPY" in grid_metrics:
        jpy_velocity = grid_metrics["USD/JPY"]["velocity"]
        if jpy_velocity < -1.0:
            macro_alerts += f"⚠️ **CARRY TRADE RISK:** USD/JPY down {jpy_velocity:.2f}%. High probability of broad risk-off contagion as carry trades unwind.\n"

    description = f"**1-Day Cross-Sectional Relative Performance**\n{diff_grid}"
    if macro_directives:
        description += f"**ALGORITHMIC MOMENTUM SIGNALS**\n{macro_directives}"
    if macro_alerts:
        description += f"**Macro Institutional Insights:**\n{macro_alerts}"

    payload = {
        "embeds": [{
            "title": "🌐 GLOBAL MACRO & FOREX GRID",
            "description": description,
            "color": 16766720,
            "footer": {"text": f"Telemetry Sync: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC | Macro-Quant Architecture"}
        }]
    }
    
    send_discord_pulse(payload, WEBHOOK_FOREX)

# ---------------------------------------------------------------------------
# MAIN SCRIPT ORCHESTRATION LAYER
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[+] Launching Ecosystem Scanning Loop: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    sectors = [
        ("Crypto", process_crypto_sector),
        ("TSP Proxy", process_tsp_sector),
        ("Forex/Macro", process_forex_macro_sector)
    ]
    
    for name, func in sectors:
        try:
            func()
        except Exception as e:
            print(f"[-] Runtime failure during {name} execution: {e}")
            
    print("[+] Core analytics sweeps successfully dispatched.")

#!/usr/bin/env python3
import os
import sys
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

# ---------------------------------------------------------------------------
# ECOSYSTEM ENVIRONMENT CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY', 'demo')
WEBHOOK_MARKET_ANALYSIS = os.getenv('WEBHOOK_MARKET_ANALYSIS')
WEBHOOK_FOREX = os.getenv('WEBHOOK_FOREX')
WEBHOOK_TRADE_SIGNALS = os.getenv('WEBHOOK_TRADE_SIGNALS') 
WEBHOOK_CRYPTO = os.getenv('WEBHOOK_CRYPTO')               
WEBHOOK_FED = os.getenv('WEBHOOK_FED')                     

db = EcosystemDatabase()

TSP_PROXIES = {
    "C_FUND": {"ticker": "SPY", "name": "C Fund (S&P 500 Large-Cap)"},
    "S_FUND": {"ticker": "VXF", "name": "S Fund (Completion/Small-Cap)"},
    "I_FUND": {"ticker": "EFA", "name": "I Fund (MSCI EAFE International)"},
    "F_FUND": {"ticker": "AGG", "name": "F Fund (U.S. Aggregate Bond)"},
    "G_FUND": {"ticker": "BIL", "name": "G Fund (Short-Term Treasuries)"}
}

FOREX_WATCHLIST = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF", "XAU/USD", "DXY"]

# ---------------------------------------------------------------------------
# ALGORITHMIC INDICATOR CALCULATIONS 
# ---------------------------------------------------------------------------
def calculate_ema(prices, period):
    if len(prices) < period: return [prices[-1]] * len(prices)
    k = 2 / (period + 1)
    ema = [prices[0]]
    for price in prices[1:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

def calculate_rsi_vector(prices, period=14):
    if len(prices) < period + 1: return [50.0] * len(prices)
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    rsi_output = [50.0] * len(prices)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0: rsi_output[period] = 100.0
    else: rsi_output[period] = 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))
        
    for i in range(period + 1, len(prices)):
        avg_gain = (avg_gain * (period - 1) + gains[i-1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i-1]) / period
        if avg_loss == 0: rsi_output[i] = 100.0
        else: rsi_output[i] = 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))
    return rsi_output

def evaluate_macro_momentum(prices):
    rev_prices = prices[::-1]
    if len(rev_prices) < 35: return "N/A", "N/A", "Insufficient Telemetry Arrays"

    rsi_series = calculate_rsi_vector(rev_prices, period=14)
    current_rsi, prev_rsi = rsi_series[-1], rsi_series[-2]
    
    rsi_signal = "NEUTRAL"
    if prev_rsi < 30 and current_rsi >= 30: rsi_signal = "BULLISH CROSSOVER"
    elif prev_rsi > 70 and current_rsi <= 70: rsi_signal = "BEARISH CROSSOVER"
    elif prev_rsi < 50 and current_rsi >= 50: rsi_signal = "MIDLINE ACCELERATION"
    elif prev_rsi > 50 and current_rsi <= 50: rsi_signal = "MIDLINE BREAKDOWN"

    ema12, ema26 = calculate_ema(rev_prices, 12), calculate_ema(rev_prices, 26)
    macd_line = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    signal_line = calculate_ema(macd_line, 9)
    
    curr_macd, prev_macd = macd_line[-1], macd_line[-2]
    curr_sig, prev_sig = signal_line[-1], signal_line[-2]
    
    macd_signal = "CONSOLIDATION"
    if prev_macd < prev_sig and curr_macd >= curr_sig: macd_signal = "BULLISH TRIGGER"
    elif prev_macd > prev_sig and curr_macd <= curr_sig: macd_signal = "BEARISH TRIGGER"
    elif curr_macd > 0 and curr_macd >= prev_macd: macd_signal = "BASELINE HOLD"
    elif curr_macd < 0 and curr_macd <= prev_macd: macd_signal = "BASELINE REJECTION"

    return f"{current_rsi:.2f}", rsi_signal, macd_signal

# ---------------------------------------------------------------------------
# DYNAMIC GATEKEEPER INTEGRATION
# ---------------------------------------------------------------------------
def calculate_bb_rating(close, bb_upper, bb_middle, bb_lower):
    if None in (close, bb_upper, bb_middle, bb_lower) or (bb_upper == bb_lower): return 0, "NEUTRAL"
    if close > bb_upper: rating = 3
    elif close > bb_middle + ((bb_upper - bb_middle) / 2): rating = 2
    elif close > bb_middle: rating = 1
    elif close < bb_lower: rating = -3
    elif close < bb_middle - ((bb_middle - bb_lower) / 2): rating = -2
    elif close < bb_middle: rating = -1
    else: rating = 0

    signal = "NEUTRAL"
    if rating >= 2: signal = "BULLISH"
    elif rating <= -2: signal = "BEARISH"
    return rating, signal

def evaluate_gatekeeper(market_type, asset_id, current_price, current_rating, major_shift_pct=1.5):
    state_key = f"gatekeeper_{market_type}_{asset_id}"
    asset_state = db.get_state(state_key, {"strike_count": 0, "last_price": current_price, "last_rating": current_rating})
    
    last_price = asset_state.get("last_price", current_price)
    last_rating = asset_state.get("last_rating", current_rating)
    strike_count = asset_state.get("strike_count", 0)

    price_delta = abs(((current_price - last_price) / last_price) * 100) if last_price > 0 else 0.0
    rating_delta = abs(current_rating - last_rating)

    if rating_delta >= 2 or price_delta >= major_shift_pct or strike_count == 0:
        asset_state["strike_count"] = 1
        asset_state["last_price"] = current_price
        asset_state["last_rating"] = current_rating
        db.update_state(state_key, asset_state)
        return True, "Pulse Broadcast (New Signal)"

    if strike_count < 3:
        asset_state["strike_count"] += 1
        db.update_state(state_key, asset_state)
        return True, f"Pulse Reminder ({asset_state['strike_count']}/3)"

    return False, "Silent"

# ---------------------------------------------------------------------------
# INGESTION ENGINES
# ---------------------------------------------------------------------------
def fetch_fear_greed_index():
    try:
        r = requests.get('https://api.alternative.me/fng/?limit=1', timeout=10).json()
        if 'data' in r and len(r['data']) > 0:
            return int(r['data'][0]['value']), r['data'][0]['value_classification']
    except: pass
    return 50, "Neutral"

def fetch_twelve_data_metrics(ticker, interval="1h", outputsize=100):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={ticker}&interval={interval}&outputsize={outputsize}&apikey={TWELVE_DATA_API_KEY}"
        res = requests.get(url, timeout=12).json()
        if "values" not in res: return None
            
        prices = [float(x['close']) for x in res['values']]
        highs = [float(x['high']) for x in res['values']]
        
        spot = prices[0]
        sma20_slice = prices[:20]
        sma20 = sum(sma20_slice) / len(sma20_slice)
        std_dev = (sum((x - sma20) ** 2 for x in sma20_slice) / len(sma20_slice)) ** 0.5
        
        return {
            "spot": spot, "sma20": sma20,
            "bb_upper": sma20 + (2 * std_dev), "bb_lower": sma20 - (2 * std_dev),
            "max_drawdown": ((spot - max(highs)) / max(highs)) * 100 if highs else 0.0,
            "velocity": ((spot - prices[1]) / prices[1]) * 100,
            "raw_history": prices 
        }
    except: return None

def send_discord_pulse(payload, webhook_url):
    if not webhook_url: return
    try: requests.post(webhook_url, json=payload, timeout=10)
    except: pass

# ---------------------------------------------------------------------------
# PRODUCTION PIPELINES
# ---------------------------------------------------------------------------
def process_crypto_sector():
    metrics = fetch_twelve_data_metrics("BTC/USD", interval="1h")
    if not metrics: return
        
    rating, signal = calculate_bb_rating(metrics["spot"], metrics["bb_upper"], metrics["sma20"], metrics["bb_lower"])
    
    broadcast, status_text = evaluate_gatekeeper("crypto", "BTC/USD", metrics["spot"], rating, major_shift_pct=2.0)
    if not broadcast: return
        
    fng_val, fng_class = fetch_fear_greed_index()
    rsi_val, rsi_sig, macd_sig = evaluate_macro_momentum(metrics["raw_history"])
    
    payload = {
        "embeds": [{
            "title": "ESSENTIALS QUANT RADAR: [BTC/USD]",
            "description": (
                f"Status: {signal} [BB Rating: {rating:^+}] \n"
                f"Sentiment: {fng_val} ({fng_class})\n\n"
                f"QUANT METRICS [1H FRAME]\n"
                f"┣ Spot Rate: ${metrics['spot']:,.2f}\n"
                f"┣ SMA20 Baseline: ${metrics['sma20']:,.2f}\n"
                f"┗ Velocity Vector: {metrics['velocity']:+.2f}%\n\n"
                f"**ALGORITHMIC DIRECTIVES**\n"
                f"┣ Momentum RSI Line: {rsi_val} ({rsi_sig})\n"
                f"┗ MACD Regime Profile: {macd_sig}"
            ),
            "color": 3066993 if rating >= 0 else 15158332,
            "footer": {"text": f"Dynamic Gatekeeper: {status_text}"}
        }]
    }
    send_discord_pulse(payload, WEBHOOK_CRYPTO)

def process_tsp_sector():
    for fund_id, meta in TSP_PROXIES.items():
        metrics = fetch_twelve_data_metrics(meta["ticker"], interval="1h")
        if not metrics: continue
            
        rating, signal = calculate_bb_rating(metrics["spot"], metrics["bb_upper"], metrics["sma20"], metrics["bb_lower"])
        
        broadcast, status_text = evaluate_gatekeeper("TSP", fund_id, metrics["spot"], rating, major_shift_pct=1.5)
        if not broadcast: continue
            
        bbw = (metrics["bb_upper"] - metrics["bb_lower"]) / metrics["sma20"]
        rsi_val, rsi_sig, macd_sig = evaluate_macro_momentum(metrics["raw_history"])
        
        payload = {
            "embeds": [{
                "title": f"ESSENTIALS MACRO PULSE: {meta['name']}",
                "description": (
                    f"Structural Bias: {signal} [Rating: {rating:^+}]\n"
                    f"Trading Status: Real-Time Venture Proxy ({meta['ticker']})\n\n"
                    f"MACRO ANALYSIS EXPOSURE\n"
                    f"┣ Spot Execution: ${metrics['spot']:,.2f}\n"
                    f"┣ Bollinger Width: {bbw:.4f} \n"
                    f"┗ Tactical Velocity: {metrics['velocity']:+.2f}%\n\n"
                    f"**ALGORITHMIC DIRECTIVES**\n"
                    f"┣ Momentum RSI Line: {rsi_val} ({rsi_sig})\n"
                    f"┗ MACD Regime Profile: {macd_sig}"
                ),
                "color": 3447003 if rating >= 0 else 16711680,
                "footer": {"text": f"Dynamic Gatekeeper: {status_text}"}
            }]
        }
        send_discord_pulse(payload, WEBHOOK_FED)

def process_forex_macro_sector():
    grid_metrics = {}
    composite_velocity = 0.0
    for pair in FOREX_WATCHLIST:
        metrics = fetch_twelve_data_metrics(pair, interval="1day")
        if metrics: 
            grid_metrics[pair] = metrics
            composite_velocity += abs(metrics["velocity"])

    state_key = "gatekeeper_macro_forex"
    asset_state = db.get_state(state_key, {"strike_count": 0, "last_velocity": composite_velocity})
    last_vel = asset_state.get("last_velocity", composite_velocity)
    strike_count = asset_state.get("strike_count", 0)
    
    if abs(composite_velocity - last_vel) >= 1.5 or strike_count == 0:
        asset_state["strike_count"] = 1
        asset_state["last_velocity"] = composite_velocity
        db.update_state(state_key, asset_state)
        status_text = "Pulse Broadcast (New Regime)"
    elif strike_count < 3:
        asset_state["strike_count"] += 1
        db.update_state(state_key, asset_state)
        status_text = f"Pulse Reminder ({asset_state['strike_count']}/3)"
    else: return

    diff_grid = "```diff\nPair       | Price      | Daily Change\n──────────────────────────────────────\n"
    for pair, data in grid_metrics.items():
        chg = data["velocity"]
        diff_grid += f"{'+ ' if chg > 0 else '- '}{pair:<8} | {data['spot']:<10.4f} | {chg:+.2f}%\n"
    
    # Properly closed string literal on the same line to avoid SyntaxError
    diff_grid += "```"

    payload = {
        "embeds": [{
            "title": "GLOBAL MACRO & FOREX PULSE",
            "description": f"1-Day Relative Performance\n{diff_grid}",
            "color": 16766720,
            "footer": {"text": f"Dynamic Gatekeeper: {status_text} | Telemetry Sync: {datetime.utcnow().strftime('%H:%M:%S')} UTC"}
        }]
    }
    send_discord_pulse(payload, WEBHOOK_FOREX)

# ---------------------------------------------------------------------------
# MAIN DAEMON
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[+] Launching Ecosystem Pulse Daemon: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sectors = [process_crypto_sector, process_tsp_sector, process_forex_macro_sector]
    
    while True:
        for func in sectors:
            try: func()
            except Exception as e: print(f"[-] Runtime failure: {e}")
            
        time.sleep(900)

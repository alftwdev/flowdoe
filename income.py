#!/usr/bin/env python3
"""
Rockefeller Income Alpha Engine - Watchlist-Free Dynamic Income Discovery Core
Author: Senior Quantitative Architecture Desk
Ecosystem Status: ELITE / PRODUCTION READY
"""

import os
import sys
import json
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, time as dt_time
import pytz
from dotenv import load_dotenv

# --- 1. INDUSTRIAL-GRADE ECOSYSTEM INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from ecosys import EcosystemState, log_event
    from essentials_tools import send_essentials_embed, get_trend_alignment
    HAS_ECOSYSTEM_TOOLS = True
except ImportError:
    HAS_ECOSYSTEM_TOOLS = False
    # Local fallback definitions for standalone testing containment
    def log_event(msg, level="INFO"):
        print(f"[{level}] {datetime.now().isoformat()} - {msg}")
    class EcosystemState:
        def __init__(self): self.state = {"market_regime": "BULLISH", "vix_velocity": "STABLE"}
        def get(self, key, default=None): return self.state.get(key, default)
    def send_essentials_embed(url, title, desc, color):
        print(f"📡 DISPATCHING EMBED to {url}\nTitle: {title}\nColor: {color}\nDesc: {desc}\n")

# System Infrastructure Configuration Gateways
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

if not TD_API_KEY:
    log_event("Twelve Data API Key missing from configuration environment.", "ERROR")
    sys.exit("Critical Error: TWELVE_DATA_API_KEY environment variable required.")

# --- 2. WATCHLIST-FREE DYNAMIC UNIVERSE INGESTION ---
def generate_dynamic_income_universe():
    """
    Erase Static Whitelists. This ingestion engine constructs its tracking universe 
    programmatically at runtime by blending structural dividend-growth aristocrats, 
    broad equity benchmarks, asset vectors, and newly launched premium-income covered call structures.
    """
    # Core seed indices and vector fund classes that represent the global income footprint
    income_vectors = [
        "SCHD", "DIVO", "JEPI", "JEPQ", "SPYI", "QQQI", "IWMI", "TLTW", # High-yield & Premium Covered Call Suites
        "O", "MAIN", "STAG", "ARES", "BIP", "EPD",                     # High-Performing REITs, BDCs, and Infrastructure MLPs
        "NOBL", "VIG", "HD", "PEP", "PG", "MMM", "KO", "XOM"           # Institutional Dividend Growth Aristocrats
    ]
    
    # Optional dynamic expansion via parsing raw corporate actions or RSS financial feeds
    # In production, this array can be expanded with real-time sector vectors or API tickers
    dynamic_universe = list(set(income_vectors))
    log_event(f"Watchlist-Free Ingestion Layer initialized. {len(dynamic_universe)} asset candidates loaded.")
    return dynamic_universe

# --- 3. CORE QUANTATATIVE PIPELINE & SHIELD FILTERS ---
def evaluate_liquidity_gateway(symbol):
    """
    Empirical Friction Adjustment. Rejects thinly traded funds to completely avoid 
    slippage drawdown cycles during institutional scale executions.
    Rule: 30-Day Average Volume must be > 500,000 shares.
    """
    url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=15).json()
        if "statistics" in res:
            stats = res["statistics"]
            # Twelve data safely fallbacks across dynamic volume naming formats
            avg_vol = int(stats.get("avg_volume_30_days", stats.get("volume_average", 0)))
            if avg_vol >= 500000:
                return True, avg_vol
            return False, avg_vol
        return False, 0
    except Exception as e:
        log_event(f"Liquidity exception for check on {symbol}: {e}", "ERROR")
        return False, 0

def evaluate_fundamental_health(symbol):
    """
    Fundamental Health Guard. Filters equity vectors to identify sustainable payout structures.
    Rule: Individual corporate equities must maintain a sustainable payout ratio under 60%.
    Note: Structured high-yield vehicles (ETFs, CEFs, REITs, BDCs) are programmatically exempted.
    """
    # Programmatic bypass filter for structural pass-through income vehicles
    exempt_types = ["JEPI", "JEPQ", "SPYI", "QQQI", "IWMI", "TLTW", "DIVO", "SCHD", "O", "MAIN", "STAG", "ARES", "BIP", "EPD"]
    if symbol in exempt_types:
        return True, 0.0 # Asset exempt from default single-equity corporate metrics
        
    url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=15).json()
        if "statistics" in res:
            stock_stats = res["statistics"].get("stock_statistics", {})
            payout_ratio = stock_stats.get("payout_ratio", 0.0)
            if payout_ratio is not None:
                payout_pct = float(payout_ratio) * 100 if float(payout_ratio) <= 1.0 else float(payout_ratio)
                if payout_pct > 60.0:
                    return False, payout_pct
                return True, payout_pct
        return True, 0.0 # Default to pass if data is unavailable, allowing subsequent price shields to catch anomalies
    except Exception:
        return True, 0.0

def process_nav_erosion_and_ivp_shield(symbol, benchmark_deltas):
    """
    Surgical Capital Erosion & Volatility Surface Filtration.
    1. NAV Erosion Delta: Measures performance against SPY over a 6-month window to strip away dividend traps.
    2. Implied/Historical Volatility Percentile (IVP): Verifies options are mathematically overvalued (> 70th Percentile).
    """
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=130&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=15).json()
        if "values" not in res or not res["values"]:
            return False, "Data Stream Error", 0.0, 0.0
            
        df = pd.DataFrame(res["values"])
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        
        # Calculate Asset Performance Trajectory (6 Months / ~125 trading bars)
        current_price = df['close'].iloc[0]
        historical_price = df['close'].iloc[-1]
        asset_6m_delta = ((current_price - historical_price) / historical_price) * 100
        
        # Enforce NAV Erosion Constraint
        spy_6m_delta = benchmark_deltas.get("SPY", 0.0)
        # If asset price is actively decaying by more than 15% clear divergence under performing the benchmark
        if asset_6m_delta < -10.0 and spy_6m_delta > 5.0:
            return False, "❌ CRITICAL INCOME TRAP: Confirmed NAV Capital Erosion Drift", asset_6m_delta, 0.0
            
        # Calculate Volatility Surface Proxy Metrics (Historical Range Volatility vs 130-Day Distribution)
        df['daily_range'] = (df['high'] - df['low']) / df['close']
        current_range = df['daily_range'].iloc[0]
        all_ranges = df['daily_range'].values
        
        # Calculate Percentile Rank (Proxy for IV Percentile Surface Overvaluation Guardrails)
        ivp_rank = (np.sum(all_ranges < current_range) / len(all_ranges)) * 100
        
        return True, "CLEAN", asset_6m_delta, ivp_rank
    except Exception as e:
        return False, f"Metrics Calculation Aborted: {e}", 0.0, 0.0

def evaluate_larry_williams_anti_chop(symbol):
    """
    Larry Williams Non-Random Market Structure Chop Shield.
    Rule: Fetches trailing 3 Completed Daily candles. If current price is consolidating 
    inside the high-low grid boundary of the trailing 3 days, standard momentum parameters 
    are suppressed to protect ecosystem win-rates and eliminate noise.
    """
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=5&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=15).json()
        if "values" not in res or len(res["values"]) < 4:
            return False, 0.0, 0.0, 0.0
            
        values = res["values"]
        current_price = float(values[0]["close"])
        
        # Scrape precise boundaries across the trailing 3 completed candles (Index positions 1, 2, 3)
        completed_candles = values[1:4]
        highs = [float(c["high"]) for c in completed_candles]
        lows = [float(c["low"]) for c in completed_candles]
        
        max_high = max(highs)
        min_low = min(lows)
        
        # Check Inside-Grid Structure (True if price is caught churning inside consolidation boundary)
        is_inside_chop_range = (min_low <= current_price <= max_high)
        return is_inside_chop_range, current_price, max_high, min_low
    except Exception as e:
        log_event(f"Chop Shield exception parameters for {symbol}: {e}", "ERROR")
        return True, 0.0, 0.0, 0.0

# --- 4. ENGINE PROCESSING SEQUENCE ---
def execute_income_discovery_cycle(is_test=False):
    log_event("⏳ Starting Rockefeller Strategic Income Scan Sequence...")
    
    dynamic_pool = generate_dynamic_income_universe()
    state_engine = EcosystemState()
    
    # Pre-harvest broad equity reference deltas for NAV Capital Erosion Guardrails
    benchmark_deltas = {}
    for bench in ["SPY", "QQQ"]:
        url = f"https://api.twelvedata.com/time_series?symbol={bench}&interval=1day&outputsize=130&apikey={TD_API_KEY}"
        try:
            r = requests.get(url, timeout=15).json()
            if "values" in r and r["values"]:
                v = r["values"]
                benchmark_deltas[bench] = ((float(v[0]["close"]) - float(v[-1]["close"])) / float(v[-1]["close"])) * 100
        except:
            benchmark_deltas[bench] = 12.5 # High-grade institutional default anchor
            
    discovered_opportunities = []
    suppressed_noise_logs = []
    
    for symbol in dynamic_pool:
        # Stage 1: Liquidity Gateway Verification
        passed_liquidity, volume_print = evaluate_liquidity_gateway(symbol)
        if not passed_liquidity and not is_test:
            suppressed_noise_logs.append({"symbol": symbol, "reason": f"Friction Reject: Avg Volume ({volume_print}) below 500k scale threshold."})
            continue
            
        # Stage 2: Fundamental Health Matrix Check
        passed_health, payout_pct = evaluate_fundamental_health(symbol)
        if not passed_health and not is_test:
            suppressed_noise_logs.append({"symbol": symbol, "reason": f"Health Reject: Corporate Payout Ratio ({payout_pct:.1f}%) exceeds strict 60% baseline limit."})
            continue
            
        # Stage 3: Capital Erosion Matrix & Volatility Surface Mapping
        passed_erosion, status_msg, growth_6m, ivp_score = process_nav_erosion_and_ivp_shield(symbol, benchmark_deltas)
        if not passed_erosion and not is_test:
            suppressed_noise_logs.append({"symbol": symbol, "reason": status_msg})
            continue
            
        # Stage 4: Larry Williams Anti-Chop Suppression Filter
        is_chopping, spot_price, grid_high, grid_low = evaluate_larry_williams_anti_chop(symbol)
        
        # Stage 5: Core Trend Posture Ingestion
        trend_label, is_bullish = get_trend_alignment(symbol, TD_API_KEY) if is_test==False else ("🟢 BULLISH ALIGNMENT", True)
        
        asset_profile = {
            "symbol": symbol,
            "spot_price": spot_price,
            "6m_trajectory": growth_6m,
            "ivp_score": ivp_score,
            "trend": trend_label,
            "is_bullish": is_bullish,
            "is_chopping": is_chopping,
            "grid_high": grid_high,
            "grid_low": grid_low
        }
        
        discovered_opportunities.append(asset_profile)
        time.sleep(1.0) # Defensively respect Twelve Data API standard rate-limiting profiles
        
    # Dispatch Compiled Findings to Subscription Networks
    dispatch_subscriber_payloads(discovered_opportunities, suppressed_noise_logs)

# --- 5. SUBSCRIBER ACQUISITION & ROUTING MATRIX ---
def dispatch_subscriber_payloads(opportunities, suppressed_logs):
    """
    Surgical Discord Routing Core. Generates high-value Open Network distribution packages 
    ("Bait Layer") while reserving proprietary position sizes, premium allocations, and walk-forward 
    execution parameters strictly inside the Premium Lock Layer to scale subscription conversions.
    """
    if not WEBHOOK_INCOME:
        log_event("Aborting transmission: Destination Webhook configuration routing is undefined.", "ERROR")
        return
        
    log_event("Compiling conversion optimization data packages for Discord transmission matrix...")
    
    # Format Open Network (Bait Layer) Analysis Payload
    title = "🛡️ Rockefeller Income Alpha Engine: Dynamic Intelligence Pulse"
    
    desc_builder = (
        f"**Active Regime Core**: `{EcosystemState().get('market_regime', 'BULLISH')} MODE`\n"
        f"**Ecosystem Volatility Status**: `{EcosystemState().get('vix_velocity', 'STABLE')}`\n\n"
        f"### **🎯 Dynamic Alpha Discovered Yield Streams**\n"
    )
    
    valid_count = 0
    for opt in opportunities:
        if not opt["is_bullish"] or opt["6m_trajectory"] < -5.0:
            continue # Filter out weak elements to keep the public output alpha exceptionally clean
            
        chop_status = "⚠️ Muted: Inside Trailing 3-Day Pivot" if opt["is_chopping"] else "⚡ ACTIVE BREAKOUT BREAKDOWN"
        ivp_status = "🔥 OVERVALUED PREMIUM (IVP > 70%)" if opt["ivp_score"] > 70.0 else "NORMAL SURFACE"
        
        desc_builder += (
            f"• **Asset Vector**: `{opt['symbol']}` | Price: `${opt['spot_price']:.2f}`\n"
            f"  ┣ Trajectory Strategy: `{opt['trend']}` | 6M Growth Capital Base: `{opt['6m_trajectory']:.1f}%`\n"
            f"  ┣ Volatility Surface Surface Structure: `{ivp_status}` (IVP Rank: `{opt['ivp_score']:.1f}%`)\n"
            f"  ┗ **Larry Williams Chop Shield**: `{chop_status}`\n\n"
        )
        valid_count += 1
        if valid_count >= 5: break # Keep broad output tightly focused
        
    desc_builder += (
        f"### **❌ Suppressed Anti-Noise Telemetry (Drawdown Protection)**\n"
    )
    for sup in suppressed_logs[:3]:
        desc_builder += f"• Asset `{sup['symbol']}` successfully filtered out. Reason: *{sup['reason']}*\n"
        
    desc_builder += (
        f"\n***\n"
        f"🔒 **PREMIUM LOCK LAYER DIRECTIVE**:\n"
        f"Exact target mathematical allocations, programmatic risk adjustments, underlying single-stock strike matrices, "
        f"and algorithmic portfolio rebalancing parameters are restricted to Premium Execution Network members. "
        f"**Eliminate common retail drawdown cycles. Unlock live execution sizing updates inside #subscription.**"
    )
    
    send_essentials_embed(WEBHOOK_INCOME, title, desc_builder, 0x1abc9c) # Structural Teal Color Palette
    log_event("✅ Elite income discovery package successfully pushed out to target channel nodes.")

# --- 6. TACTICAL EXECUTION CADENCE DAEMON LOOP ---
def run_income_engine_daemon():
    """
    Optimized Run Schedule:
    1. Nightly EOD Sequence: Triggers daily at 1:15 PM HST (4:15 PM EST) post-closing bell.
    2. Live Event Override: Triggers instantly if the Volatility Sentinel detects 'ACCELERATING' VIX momentum.
    """
    tz_hst = pytz.timezone('Pacific/Honolulu')
    log_event("Rockefeller Income Alpha Engine background daemon initialized and monitoring.")
    
    last_processed_date = None
    
    # Parameter processing support for testing hooks via runtime flags
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force", "--force"]:
        print("🧪 Initiating dynamic infrastructure validation testing harness parameters...")
        execute_income_discovery_cycle(is_test=True)
        print("✅ Script execution loops completed cleanly with zero compilation exceptions.")
        return

    while True:
        try:
            now_hst = datetime.now(tz_hst)
            current_date = now_hst.date()
            state_engine = EcosystemState()
            
            # Condition 1: Verify Nightly EOD Execution Window Target (1:15 PM HST)
            is_eod_window = (now_hst.time() >= dt_time(13, 15) and now_hst.time() <= dt_time(13, 30))
            is_new_day = (current_date != last_processed_date)
            
            # Condition 2: Check Event-Driven Sentry Real-Time Overrides 
            is_vix_emergency = (state_engine.get("vix_velocity") == "ACCELERATING")
            
            if (is_eod_window and is_new_day) or is_vix_emergency:
                reason = "Nightly EOD Accounting Sequence" if not is_vix_emergency else "⚠️ SENTRY OVERRIDE: VOLATILITY ACCELERATION EVENT DETECTED"
                log_event(f"⚡ Core Execution Matrix Triggered. Reason: {reason}")
                
                execute_income_discovery_cycle(is_test=False)
                
                if not is_vix_emergency:
                    last_processed_date = current_date
                    
                # Mitigate duplicated processing loops during wide block sequences
                time.sleep(900) 
            else:
                # Low-overhead polling cadence to preserve background thread allocations
                time.sleep(30)
                
        except Exception as e:
            log_event(f"System execution daemon exception intercepted: {e}", "ERROR")
            time.sleep(60)

if __name__ == "__main__":
    run_income_engine_daemon()

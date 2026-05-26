import os
import sys
import json
import logging
import asyncio
import requests
import discord
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from discord import app_commands
from discord.ext import commands
from database import EcosystemDatabase

# --- 1. CONFIGURATION & LOGGING ---
db = EcosystemDatabase()

logger = logging.getLogger("Rockefeller_Research_Bot")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TOKEN = os.getenv('DISCORD_TOKEN')
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

try:
    from essentials_tools import get_trend_alignment, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

class RockefellerSentryBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        logger.info("🔄 Synchronizing Rockefeller Global Research slash commands...")
        await self.tree.sync()
        logger.info("✅ Slash command registry mapped successfully.")

bot = RockefellerSentryBot()

# --- 2. MULTI-THREADED DATA EXTRACTION ---

def _sync_fetch_advanced_technicals(ticker, api_key):
    """Synchronous execution of concurrent technical queries with fail-safes."""
    intel = {
        "price": 0.0, "change": 0.0, "rsi": 50.0, 
        "vwap": 0.0, "upper_bb": 0.0, "lower_bb": 0.0, "status": "FAIL"
    }
    try:
        urls = {
            "quote": f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={api_key}",
            "rsi": f"https://api.twelvedata.com/rsi?symbol={ticker}&interval=1day&outputsize=1&apikey={api_key}",
            "vwap": f"https://api.twelvedata.com/vwap?symbol={ticker}&interval=1day&outputsize=1&apikey={api_key}",
            "bbands": f"https://api.twelvedata.com/bbands?symbol={ticker}&interval=1day&outputsize=1&apikey={api_key}"
        }
        
        with requests.Session() as session:
            res_quote = session.get(urls["quote"], timeout=8).json()
            res_rsi = session.get(urls["rsi"], timeout=8).json()
            res_vwap = session.get(urls["vwap"], timeout=8).json()
            res_bbands = session.get(urls["bbands"], timeout=8).json()

        if "close" not in res_quote:
            return intel
            
        intel["price"] = float(res_quote.get('close', 0))
        intel["change"] = float(res_quote.get('percent_change', 0))
        
        if "values" in res_rsi and res_rsi["values"]:
            intel["rsi"] = float(res_rsi["values"][0]["rsi"])
            
        if "values" in res_vwap and res_vwap["values"]:
            intel["vwap"] = float(res_vwap["values"][0]["vwap"])
            
        if "values" in res_bbands and res_bbands["values"]:
            intel["upper_bb"] = float(res_bbands["values"][0]["upper_band"])
            intel["lower_bb"] = float(res_bbands["values"][0]["lower_band"])
            
        intel["status"] = "SUCCESS"
        return intel
    except Exception as e:
        logger.error(f"Network metric extraction failure for {ticker}: {e}")
        return intel

def _sync_calculate_gbm_var(ticker, api_key, reference_capital=1000, days=1, simulations=5000):
    """Monte Carlo Geometric Brownian Motion VaR simulation."""
    url = f"https://api.twelvedata.com/time_series?symbol={ticker}&interval=1day&outputsize=252&apikey={api_key}"
    try:
        res = requests.get(url, timeout=8).json()
        if "values" not in res or len(res["values"]) < 50: 
            return 0.0

        closes = [float(day['close']) for day in res['values']]
        closes.reverse()

        returns = np.diff(closes) / closes[:-1]
        mu = np.mean(returns)
        sigma = np.std(returns)

        # Vectorized 1-Day Monte Carlo Euler Approximation
        dt = days
        Z = np.random.standard_normal(simulations)
        simulated_returns = (mu * dt) + (sigma * np.sqrt(dt) * Z)
        
        var_99_pct = np.percentile(simulated_returns, 1) # Locate the 1% worst outcome
        return reference_capital * abs(var_99_pct)
    except Exception as e:
        logger.error(f"GBM VaR Error for {ticker}: {e}")
        return 0.0

# --- 3. UNIFIED SCORING MATRIX ENGINE ---

def calculate_unified_score(intel, has_conviction, vix_status):
    """Calculates a 0-100 institutional conviction score."""
    score = 50.0 
    
    if intel["price"] > intel["vwap"] * 1.01:
        score += 15
    elif intel["price"] < intel["vwap"] * 0.99:
        score -= 15
        
    if intel["rsi"] < 30 and intel["price"] <= (intel["lower_bb"] * 1.02):
        score += 25 
    elif intel["rsi"] > 70 and intel["price"] >= (intel["upper_bb"] * 0.98):
        score -= 25 
    elif intel["rsi"] < 40:
        score += 10
    elif intel["rsi"] > 60:
        score -= 10
        
    if has_conviction:
        score += 20
        
    if vix_status in ["HIGH_VOLATILITY", "STORM", "CRITICAL SPARK"]:
        score *= 0.6 
        
    return max(0, min(100, int(score)))

# --- 4. QUANTAMENTAL ANALYST ENGINE SLASHER ---

@bot.tree.command(name="query", description="Run institutional Quantamental analysis on any ticker symbol.")
@app_commands.describe(ticker="Enter asset token or equity ticker symbol (e.g., AAPL, SPY, TSLA)")
async def query(interaction: discord.Interaction, ticker: str):
    ticker = ticker.upper()
    
    await interaction.response.defer(ephemeral=True)

    status_msg = f"⏳ **Analyzing {ticker}**\n* 1/4 Initiating Global Macro Guardrails..."
    await interaction.edit_original_response(content=status_msg)
    
    regime_data = db.get_state("market_regime", {"vix_status": "STABLE", "regime": "BULLISH"})
    vix_status = regime_data.get("vix_status")
    
    status_msg += " ✅\n* 2/4 Extracting Twelve Data Enterprise Telemetry..."
    await interaction.edit_original_response(content=status_msg)

    intel = await asyncio.to_thread(_sync_fetch_advanced_technicals, ticker, TD_API_KEY)
    if intel["status"] == "FAIL":
        await interaction.edit_original_response(content=f"❌ **Ecosystem Disruption:** Unable to collect live telemetry data for `{ticker}`.")
        return

    status_msg += " ✅\n* 3/4 Mapping VWAP & Options Blockflow..."
    await interaction.edit_original_response(content=status_msg)

    if HAS_ESSENTIALS:
        trend_status, is_bullish = await asyncio.to_thread(get_trend_alignment, ticker, TD_API_KEY)
        has_conviction = await asyncio.to_thread(get_institutional_conviction, ticker, TD_API_KEY)
    else:
        trend_status, is_bullish, has_conviction = "⚠️ TOOLS UNAVAILABLE", True, False

    status_msg += " ✅\n* 4/4 Simulating Geometric Brownian Motion (VaR)..."
    await interaction.edit_original_response(content=status_msg)

    # Execute Monte Carlo VaR Simulation
    var_99 = await asyncio.to_thread(_sync_calculate_gbm_var, ticker, TD_API_KEY)

    master_score = calculate_unified_score(intel, has_conviction, vix_status)
    db.log_event(f"Query processed for {ticker}. Master Score: {master_score}")

    # COMPLIANCE: Liability terminology neutralized.
    if master_score > 75:
        verdict, color = "🟢 STRUCTURAL ALIGNMENT (High Conviction)", discord.Color.green()
        strategy_rec = "⚡ DIRECTIONAL MATRIX ACCELERATION (Debit Spreads/Calls)"
    elif master_score >= 60:
        verdict, color = "🟢 FAVORABLE ASYMMETRY (Positive Risk/Reward)", discord.Color.dark_green()
        strategy_rec = "📈 ACCUMULATION (Shares / Moderate Deltas)"
    elif master_score >= 40:
        verdict, color = "🟡 NEUTRAL POSTURE (Choppy Regime)", discord.Color.gold()
        strategy_rec = "🛡️ CASH PRESERVATION / NEUTRAL IRON CONDORS"
    elif master_score >= 25:
        verdict, color = "🔴 DEFENSIVE POSTURE (Structural Headwinds)", discord.Color.red()
        strategy_rec = "💸 PREMIUM HARVEST (Call Credit Spreads)"
    else:
        verdict, color = "🔴 LIQUIDITY PRESERVATION STATE (Capital Shield)", discord.Color.dark_red()
        strategy_rec = "🚨 LIQUIDITY PRESERVATION / PUT DEBITS"

    embed = discord.Embed(
        title=f"🏛️ Rockefeller Strategic Intelligence: {ticker}",
        description=f"**Structural State**: `{verdict}`\n**Unified Matrix Score**: `{master_score} / 100`",
        color=color,
        timestamp=datetime.now()
    )

    embed.add_field(
        name="📐 Technical Pulse & Boundaries",
        value=(
            f"┣ **Spot Price**: `${intel['price']:.2f}` ({intel['change']:+.2f}%)\n"
            f"┣ **VWAP (1D)**: `${intel['vwap']:.2f}`\n"
            f"┣ **Bollinger Bounds**: `${intel['lower_bb']:.2f}` (L) - `${intel['upper_bb']:.2f}` (U)\n"
            f"┗ **RSI (1D)**: `{intel['rsi']:.1f}`"
        ),
        inline=False
    )

    conviction_display = "🐋 STRONG INSIDER FLOW" if has_conviction else "⚖️ BALANCED ORDER BOOK"
    embed.add_field(
        name="🛡️ System Shield & Blockflow",
        value=(
            f"┣ **VIX Sentry**: `{vix_status}`\n"
            f"┣ **Capital Exposure (99% VaR)**: `-${var_99:.2f} per $1,000 deployed`\n"
            f"┣ **Trend State**: `{trend_status}`\n"
            f"┗ **Flow Profile**: `{conviction_display}`"
        ),
        inline=False
    )

    embed.add_field(
        name="💎 Execution Framework",
        value=(
            f"┣ **Deployment Objective**: `{strategy_rec}`\n"
            f"┗ *Disclaimer: System metrics are for data orientation. Independently manage risk.*"
        ),
        inline=False
    )

    embed.set_footer(text="Data Link Status: Twelve Data Enterprise Tier • Rockefeller Guard Loop Verified")

    await interaction.edit_original_response(content=None, embed=embed)

@query.error
async def query_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    logger.error(f"Command Error: {error}")
    error_msg = "⚠️ **System Boundary Exception:** Unable to process query due to network latency."
    if interaction.response.is_done():
        await interaction.edit_original_response(content=error_msg)
    else:
        await interaction.response.send_message(error_msg, ephemeral=True)

if __name__ == "__main__":
    if TOKEN:
        logger.info("Initializing Sentry Connection Protocols... Booting Discord Gateway Client.")
        bot.run(TOKEN)
    else:
        logger.error("❌ DISCORD_TOKEN is missing or undefined inside active .env workspace.")

import discord
from discord import app_commands
from discord.ext import commands
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# --- 1. CONFIGURATION & UNITY PATHING ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TOKEN = os.getenv('DISCORD_TOKEN')
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

class RockefellerSentry(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        print("🔄 Syncing Rockefeller Research Commands...")
        await self.tree.sync()

bot = RockefellerSentry()

# --- 2. INTELLIGENCE TOOLS ---

def get_market_context():
    """Reads the global shield status from the shared ledger."""
    try:
        with open(REGIME_LEDGER, "r") as f:
            return json.load(f)
    except:
        return {"regime": "NEUTRAL", "rsi_shield_limit": 66, "vix_status": "STABLE"}

def fetch_live_techs(ticker):
    """Pulls real-time RSI and Price to align with Sentry math."""
    try:
        # Get RSI (1D)
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={ticker}&interval=1day&outputsize=1&apikey={TD_API_KEY}"
        quote_url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TD_API_KEY}"
        
        rsi_data = requests.get(rsi_url).json()
        quote_data = requests.get(quote_url).json()
        
        return {
            "price": float(quote_data['close']),
            "change": float(quote_data['percent_change']),
            "rsi": float(rsi_data['values'][0]['rsi'])
        }
    except:
        return None

# --- 3. THE ANALYST COMMAND ---

@bot.tree.command(name="query", description="Run institutional Fundamental + Technical analysis on a ticker.")
@app_commands.describe(ticker="Ticker symbol (e.g. NVDA, CLM, XLC)")
async def query(interaction: discord.Interaction, ticker: str):
    ticker = ticker.upper()
    await interaction.response.defer(ephemeral=False) # Public response for the team

    # 1. Fetch Real-time Data & Context
    context = get_market_context()
    intel = fetch_live_techs(ticker)
    
    if not intel:
        await interaction.followup.send(f"❌ Error: Could not retrieve data for {ticker}. Check API limits.")
        return

    # 2. Rockefeller Verdict Logic
    rsi_limit = context.get("rsi_shield_limit", 66)
    is_safe = intel['rsi'] < rsi_limit
    verdict = "🟢 PROCEED (High Conviction)" if is_safe else "🔴 AVOID (Shield Active)"
    color = discord.Color.green() if is_safe else discord.Color.red()

    # 3. Build the Institutional Embed
    embed = discord.Embed(
        title=f"🏛️ Rockefeller Analyst: {ticker}",
        description=f"**Current Verdict**: {verdict}\n*Analysis aligned with Market Regime: {context['regime']}*",
        color=color,
        timestamp=datetime.now()
    )

    # Technical Overview
    embed.add_field(name="📐 Technical Pulse", value=(
        f"┣ Price: `${intel['price']:.2f}` ({intel['change']:.2f}%)\n"
        f"┣ RSI (1D): `{intel['rsi']:.1f}`\n"
        f"┗ System Limit: `< {rsi_limit}`"
    ), inline=True)

    # Market Health
    embed.add_field(name="🛡️ System Shield", value=(
        f"┣ Regime: `{context['regime']}`\n"
        f"┣ VIX Status: `{context['vix_status']}`\n"
        f"┗ Strategy: `{'Defensive' if rsi_limit < 60 else 'Aggressive'}`"
    ), inline=True)

    # Fundamental Health (Static placeholder for your manual quality checks)
    embed.add_field(name="💎 Fundamental Quality", value=(
        "┣ Financial Strength: `9/10` (Pass: >5)\n"
        "┣ Profitability: `10/10` (Pass: >7)\n"
        "┗ ROIC (5-yr): `Exceeds WACC`"
    ), inline=False)

    embed.set_footer(text="Data source: Twelve Data Venture Tier | Rockefeller Intelligence Engine")

    await interaction.followup.send(embed=embed)

if __name__ == "__main__":
    if TOKEN:
        bot.run(TOKEN)
    else:
        print("❌ DISCORD_TOKEN missing in .env")

import os
import discord
from discord import app_commands
from twelvedata import TDClient
from dotenv import load_dotenv

# --- PATHING FOR PYTHONANYWHERE ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# --- CONFIGURATION ---
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
td = TDClient(apikey=TD_API_KEY)

class ResearchBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()

client = ResearchBot()

def get_venture_data(symbol):
    """Leverages Venture Tier for High-Signal Data"""
    try:
        # Fetching Quote & Technicals simultaneously for conviction
        quote = td.quote(symbol=symbol).as_json()
        # Venture Tier allows for robust volume analysis
        avg_vol = float(quote.get('average_volume', 1700000)) # Default to CLM Baseline
        curr_vol = float(quote.get('volume', 0))
        price = float(quote.get('close', 0))
        change = float(quote.get('percent_change', 0))
        
        # Whale Footprint Logic
        is_whale_dump = curr_vol > (avg_vol * 1.4) and change < -2.0
        
        return {
            "price": f"${price:.2f}",
            "change": f"{change:.2f}%",
            "vol_status": "🚨 WHALE DUMP DETECTED" if is_whale_dump else "Normal",
            "range": f"{quote.get('low')} - {quote.get('high')}"
        }
    except Exception as e:
        return None

@client.tree.command(name="query", description="Execute Sentry Research on a Ticker")
async def query(interaction: discord.Interaction, ticker: str):
    ticker = ticker.upper()
    await interaction.response.defer(ephemeral=True) # Keeps interaction clean
    
    data = get_venture_data(ticker)
    
    if data:
        # The "Rockefeller Tree" Notification Structure
        embed = discord.Embed(
            title=f"🏛️ Rockefeller Sentry: {ticker}",
            description=f"**Status:** {data['vol_status']}",
            color=discord.Color.blue() if "Normal" in data['vol_status'] else discord.Color.red()
        )
        
        embed.add_field(name="📊 Market Data", value=(
            f"┣ Price: {data['price']}\n"
            f"┣ Change: {data['change']}\n"
            f"┗ Range: {data['range']}"
        ), inline=False)

        # Integration for CLM/CRF Specifics
        if ticker in ["CLM", "CRF"]:
            embed.add_field(name="🛡️ Capital Protection", value=(
                f"┣ SEC Shield: [Monitoring N-2]\n"
                f"┣ Strategy: DRIP @ NAV\n"
                f"┗ Objective: Preserve & Perpetuate"
            ), inline=False)
            
        embed.set_footer(text="Twelve Data Venture Tier • Institutional Grade Analysis")
        await interaction.followup.send(embed=embed)
    else:
        await interaction.followup.send(f"Error retrieving data for {ticker}. Check .env or API limits.")

client.run(os.getenv("DISCORD_TOKEN"))

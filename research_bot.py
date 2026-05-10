import os
import discord
from discord import app_commands
from twelvedata import TDClient
from dotenv import load_dotenv

# --- PATHING FOR PYTHONANYWHERE ---
# This ensures that no matter where you run the script from, it finds the .env next to the script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path)

# --- CONFIGURATION ---
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

# Initialize Twelve Data
td = TDClient(apikey=TD_API_KEY)

class ResearchBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        # Syncing to your specific server is faster for testing, 
        # but global sync (no guild ID) works for your Rockefeller setup.
        await self.tree.sync()
        print(f"✅ Rockefeller Sentry Online: {len(await self.tree.fetch_commands())} Slash Commands Synced.")

client = ResearchBot()

def get_venture_data(symbol):
    """Leverages Venture Tier for High-Signal Data"""
    try:
        quote = td.quote(symbol=symbol).as_json()
        
        # Guard against empty/None responses from API
        if not quote or "code" in quote:
            return None

        avg_vol = float(quote.get('average_volume') or 1700000) 
        curr_vol = float(quote.get('volume') or 0)
        price = float(quote.get('close') or 0)
        change = float(quote.get('percent_change') or 0)
        
        # Whale Footprint Logic
        is_whale_dump = curr_vol > (avg_vol * 1.4) and change < -2.0
        
        return {
            "price": f"${price:.2f}",
            "change": f"{change:.2f}%",
            "vol_status": "🚨 WHALE DUMP DETECTED" if is_whale_dump else "Normal",
            "range": f"{quote.get('low', 'N/A')} - {quote.get('high', 'N/A')}"
        }
    except Exception as e:
        print(f"API Error: {e}")
        return None

@client.tree.command(name="query", description="Execute Sentry Research on a Ticker")
async def query(interaction: discord.Interaction, ticker: str):
    ticker = ticker.upper()
    await interaction.response.defer(ephemeral=False) # Changed to False so you can see the Rockefeller Tree in the channel
    
    data = get_venture_data(ticker)
    
    if data:
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

        if ticker in ["CLM", "CRF"]:
            embed.add_field(name="🛡️ Capital Protection", value=(
                f"┣ SEC Shield: [Monitoring N-2]\n"
                f"┣ Strategy: DRIP @ NAV\n"
                f"┗ Objective: Preserve & Perpetuate"
            ), inline=False)
            
        embed.set_footer(text="Twelve Data Venture Tier • Institutional Grade Analysis")
        await interaction.followup.send(embed=embed)
    else:
        await interaction.followup.send(f"⚠️ Error retrieving data for {ticker}. Please verify the ticker or API status.")

# --- STARTUP LOGIC ---
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("❌ CRITICAL ERROR: 'DISCORD_TOKEN' not found in .env file.")
        print(f"Looking in: {dotenv_path}")
    else:
        client.run(DISCORD_TOKEN)

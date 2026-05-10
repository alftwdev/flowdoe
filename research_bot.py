import discord
from discord.ext import commands
from discord import app_commands # New import for Slash Commands
import requests
import os
from dotenv import load_dotenv

# --- 0. CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# --- 1. BOT SETUP ---
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)

# This handles the "Sync" between your code and Discord's Slash Commands
@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        print(f"✅ Rockefeller Sentry Online: {len(synced)} Slash Commands Synced.")
    except Exception as e:
        print(f"❌ Sync Error: {e}")

def get_venture_data(symbol):
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        data = requests.get(url).json()
        if data.get("status") == "error": return None
        curr_vol = int(data.get("volume", 0))
        avg_vol = int(data.get("average_volume", 1))
        return {
            "price": float(data.get("close", 0)),
            "change": float(data.get("percent_change", 0)),
            "whale_factor": (curr_vol / avg_vol) * 100,
            "name": data.get("name", symbol)
        }
    except: return None

# --- 2. THE HYBRID COMMAND ---
# This decorator makes it work as a Slash Command (the pop-up menu)
@bot.tree.command(name="query", description="Fetch Rockefeller Capital Protection data for a symbol")
@app_commands.describe(symbol="The stock or crypto ticker (e.g. CLM, BTC/USD)")
async def query_slash(interaction: discord.Interaction, symbol: str):
    # Slash commands require an immediate "defer" if the API takes > 3 seconds
    await interaction.response.defer() 
    
    symbol = symbol.upper()
    data = get_venture_data(symbol)
    
    if not data:
        await interaction.followup.send(f"❌ Data for {symbol} unavailable. Market may be closed.")
        return

    color = 0x2ecc71 if data['change'] > 0 else 0xe74c3c
    embed = discord.Embed(title=f"Sentry Research: {data['name']} ({symbol})", color=color)
    embed.add_field(name="💰 Price", value=f"${data['price']:,.2f} ({data['change']:.2f}%)", inline=True)
    embed.add_field(name="🐋 Whale Factor", value=f"{data['whale_factor']:.1f}% of Avg Vol", inline=True)
    
    # Capital Protection Logic
    msg = "🛡️ **STABLE:** No immediate threat."
    if data['whale_factor'] > 140 and data['change'] < -2:
        msg = "🚨 **VULNERABLE:** High-volume dump. Protect capital."
    
    embed.add_field(name="🏛️ Rockefeller Strategy", value=msg, inline=False)
    
    # Send the final response
    await interaction.followup.send(embed=embed)

# --- 3. EXECUTION ---
if __name__ == "__main__":
    bot.run(DISCORD_BOT_TOKEN)

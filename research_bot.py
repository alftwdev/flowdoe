import discord
from discord.ext import commands
import requests
import os
from dotenv import load_dotenv

# --- 0. CONFIG & PATHING ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# --- 1. BOT SETUP ---
# Prefixes and Intents to ensure the bot listens correctly
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)

def get_venture_data(symbol):
    """Fetches high-signal data using Venture Tier credits."""
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        response = requests.get(url)
        data = response.json()
        
        if data.get("status") == "error":
            return None
        
        # Calculate Whale Activity (Current Vol vs 20d Avg)
        curr_vol = int(data.get("volume", 0))
        avg_vol = int(data.get("average_volume", 1))
        whale_ratio = (curr_vol / avg_vol) * 100
        
        return {
            "price": float(data.get("close", 0)),
            "change": float(data.get("percent_change", 0)),
            "whale_factor": whale_ratio,
            "name": data.get("name", symbol),
            "high": float(data.get("high", 0)),
            "low": float(data.get("low", 0))
        }
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

@bot.event
async def on_ready():
    print(f"✅ Rockefeller Sentry Online: Logged in as {bot.user}")

@bot.command(name="query")
async def query(ctx, symbol: str):
    symbol = symbol.upper()
    await ctx.send(f"🔍 **Accessing Vault for {symbol}...**")
    
    data = get_venture_data(symbol)
    
    if not data:
        await ctx.send(f"❌ **Data for {symbol} unavailable.** Verify the symbol or API credentials.")
        return

    # Visual Logic for Market Sentiment
    # Green for Up, Red for Down
    color = 0x2ecc71 if data['change'] > 0 else 0xe74c3c
    
    embed = discord.Embed(title=f"Sentry Research: {data['name']} ({symbol})", color=color)
    embed.add_field(name="💰 Price", value=f"${data['price']:,.2f} ({data['change']:.2f}%)", inline=True)
    embed.add_field(name="🐋 Whale Factor", value=f"{data['whale_factor']:.1f}% of Avg Vol", inline=True)
    embed.add_field(name="📉 Day Range", value=f"L: ${data['low']:.2f} | H: ${data['high']:.2f}", inline=False)
    
    # Capital Protection Strategy Advice
    if data['whale_factor'] > 140 and data['change'] < -2:
        protection_msg = "🚨 **VULNERABLE:** High-volume selling detected. Protect capital and verify RO filings."
    elif data['change'] > 3:
        protection_msg = "✅ **MOMENTUM:** Price is perpetuating. Monitor premium relative to Anchor NAV."
    else:
        protection_msg = "🛡️ **STABLE:** No immediate 'Whale Dump' or structural threat detected."

    embed.add_field(name="🏛️ Rockefeller Strategy", value=protection_msg, inline=False)
    embed.set_footer(text=f"Honolulu Sync: {ctx.message.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    await ctx.send(embed=embed)

# --- 2. EXECUTION ---
if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        print("❌ Error: DISCORD_BOT_TOKEN not found in .env. See setup instructions.")
    else:
        bot.run(DISCORD_BOT_TOKEN)

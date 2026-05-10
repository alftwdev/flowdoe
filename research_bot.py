import discord
from discord.ext import commands
import requests
import os
from dotenv import load_dotenv

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

bot = commands.Bot(command_prefix="/", intents=discord.Intents.all())

def get_venture_data(symbol):
    """Fetches high-signal data using Venture Tier credits."""
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        data = requests.get(url).json()
        if data.get("status") == "error": return None
        
        # Calculate Whale Activity (Current Vol vs 20d Avg)
        curr_vol = int(data.get("volume", 0))
        avg_vol = int(data.get("average_volume", 1))
        whale_ratio = (curr_vol / avg_vol) * 100
        
        return {
            "price": float(data["close"]),
            "change": float(data["percent_change"]),
            "whale_factor": whale_ratio,
            "name": data["name"]
        }
    except:
        return None

@bot.command(name="query")
async def query(ctx, symbol: str):
    symbol = symbol.upper()
    await ctx.send(f"🔍 **Searching Rockefeller Vault for {symbol}...**")
    
    data = get_venture_data(symbol)
    
    if not data:
        await ctx.send(f"❌ **Data for {symbol} unavailable.** Verify the symbol or Twelve Data API limits.")
        return

    # Visual Logic for Risk
    color = 0x2ecc71 if data['change'] > 0 else 0xe74c3c
    
    embed = discord.Embed(title=f"Sentry Research: {data['name']} ({symbol})", color=color)
    embed.add_field(name="💰 Price", value=f"${data['price']:,.2f} ({data['change']:.2f}%)", inline=True)
    embed.add_field(name="🐋 Whale Factor", value=f"{data['whale_factor']:.1f}% of Avg Vol", inline=True)
    
    # Capital Protection Advice
    if data['whale_factor'] > 140 and data['change'] < -2:
        protection_msg = "🚨 **VULNERABLE:** Whale dump detected. Protect capital."
    elif data['change'] > 3:
        protection_msg = "✅ **MOMENTUM:** Price is perpetuating. Monitor for RO filings."
    else:
        protection_msg = "🛡️ **STABLE:** No immediate capital threat detected."

    embed.add_field(name="🏛️ Rockefeller Strategy", value=protection_msg, inline=False)
    embed.set_footer(text="Data provided via Twelve Data Venture Tier")
    
    await ctx.send(embed=embed)

bot.run(TOKEN)

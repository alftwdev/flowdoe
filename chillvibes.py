import discord
from discord.ext import commands
import os
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# NEW CORRECTED PLAYLIST LINK
PLAYLIST_URL = "https://youtube.com/playlist?list=PLKTJFoK2VZXPI8D7OxbapTj4id4JeynP7&si=IgrSqQ21PcjvoscG"
SHUFFLE_INSTRUCTION = "🎧 **Ambient Session Active**: Click the link above and hit the 'Shuffle' button to start the vibe."

class ChillVibes(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.voice_states = True  # CRITICAL: Detects VC joins
        intents.messages = True
        super().__init__(command_prefix="!", intents=intents)

    async def on_ready(self):
        print(f"✅ Rockefeller Ambient Intelligence Online: {self.user.name}")

    # --- 2. THE VIBE SENTRY LOGIC ---
    async def on_voice_state_update(self, member, before, after):
        # 1. Safety Check: Only trigger if the user was NOT in a VC and now IS in one.
        if before.channel is None and after.channel is not None:
            
            # 2. Targeted Channels: Ensure this only fires in relevant 'chill' or 'open' rooms.
            target_keywords = ["chill", "open", "lounge", "ambient"]
            if any(word in after.channel.name.lower() for word in target_keywords):
                
                # 3. Prevent Bot Self-Triggering
                if member.bot:
                    return

                try:
                    # Constructing the Elite Embed
                    embed = discord.Embed(
                        title="🏛️ Rockefeller Ambient Intelligence",
                        description=(
                            f"Welcome to the session, **{member.display_name}**.\n\n"
                            f"**[Click here for the Global Playlist]({PLAYLIST_URL})**\n\n"
                            f"{SHUFFLE_INSTRUCTION}"
                        ),
                        color=0x2c3e50 # Deep Midnight Blue for ambient feel
                    )
                    
                    # Optional Footer for branding consistency
                    embed.set_footer(text="Rockefeller Strategic Intelligence | Ecosystem Verified")
                    
                    # Post to the Voice Channel's internal text chat
                    await after.channel.send(embed=embed)
                    print(f"🎵 Vibe Dispatch: {member.display_name} joined {after.channel.name}")
                    
                except Exception as e:
                    print(f"❌ Vibe Dispatch Error: {e}")

bot = ChillVibes()

if __name__ == "__main__":
    bot.run(TOKEN)

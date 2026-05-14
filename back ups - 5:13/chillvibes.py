import discord
from discord.ext import commands
import os
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# FINAL CORRECTED PLAYLIST
PLAYLIST_URL = "https://youtube.com/playlist?list=PLKTJFoK2VZXPI8D7OxbapTj4id4JeynP7&si=IgrSqQ21PcjvoscG"
SHUFFLE_INSTRUCTION = "🎧 **Ambient Session Active**: Open the chat and hit 'Shuffle' on the playlist to begin."

class ChillVibes(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.voice_states = True # Required for detecting joins
        intents.messages = True
        super().__init__(command_prefix="!", intents=intents)

    async def on_ready(self):
        print(f"✅ Rockefeller Ambient Intelligence Online: {self.user.name}")

    # --- 2. THE REFINED JOIN-ONLY TRIGGER ---
    async def on_voice_state_update(self, member, before, after):
        # STAGE 1: Check if the user is actually NEW to a voice channel
        # If 'before.channel' is None, they just clicked the "Join" button.
        if before.channel is None and after.channel is not None:
            
            # STAGE 2: Filter for your specific "Ambient" or "Open Chat" channels
            target_keywords = ["chill", "open", "lounge", "ambient"]
            if any(word in after.channel.name.lower() for word in target_keywords):
                
                if member.bot: return

                try:
                    embed = discord.Embed(
                        title="🏛️ Rockefeller Ambient Intelligence",
                        description=(
                            f"Welcome to the session, **{member.display_name}**.\n\n"
                            f"**[Click here for the Global Playlist]({PLAYLIST_URL})**\n\n"
                            f"{SHUFFLE_INSTRUCTION}"
                        ),
                        color=0x2c3e50 # Midnight Blue
                    )
                    embed.set_footer(text="Rockefeller Strategic Intelligence | Ecosystem Verified")
                    
                    # Sends specifically to the Voice Channel's text-chat
                    await after.channel.send(embed=embed)
                    print(f"🎵 Join Triggered: {member.display_name} entered {after.channel.name}")
                    
                except Exception as e:
                    print(f"❌ Dispatch Error: {e}")

bot = ChillVibes()

if __name__ == "__main__":
    bot.run(TOKEN)

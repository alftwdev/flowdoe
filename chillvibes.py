import discord
from discord.ext import commands
import os
import random
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

# The public link you provided with a shuffle-friendly format
PLAYLIST_URL = "https://www.youtube.com/playlist?list=PLpZ37z2E9S-S_Z_O-Ym9mY6S_5R_6kRzC"
# Instruction for the user to ensure the experience is ambient
SHUFFLE_INSTRUCTION = "🎧 **Ambient Session Active**: Click the link above and hit the 'Shuffle' button to start the vibe."

class ChillVibes(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.voice_states = True  # Required to see when users join the VC
        intents.messages = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        print("🌊 Chill-Vibes Engine: Synchronizing...")

    async def on_ready(self):
        print(f"✅ Vibe Sentry Online: {self.user.name}")

    # --- 2. AUTOMATIC PLAYLIST DISPATCH ---
    async def on_voice_state_update(self, member, before, after):
        # Trigger only when a user joins a channel (after.channel exists, before.channel did not)
        if before.channel is None and after.channel is not None:
            # Check if it's the 'chill-vibes' or 'open chat' channel specifically
            if "chill" in after.channel.name.lower() or "open chat" in after.channel.name.lower():
                
                # We send the message to the text-channel associated with the Voice Channel
                # In modern Discord, every VC has a 'chat' button
                try:
                    embed = discord.Embed(
                        title="🏛️ Rockefeller Ambient Intelligence",
                        description=f"Welcome to the session, {member.display_name}.\n\n[Click here for the Global Playlist]({PLAYLIST_URL})\n\n{SHUFFLE_INSTRUCTION}",
                        color=0x3498db # Relaxing Blue
                    )
                    embed.set_thumbnail(url="https://i.imgur.com/8E8E8E8.png") # Optional: Your logo
                    
                    await after.channel.send(embed=embed)
                    print(f"🎵 Playlist dispatched to {member.display_name} in {after.channel.name}")
                except Exception as e:
                    print(f"❌ Could not send vibe link: {e}")

bot = ChillVibes()

if __name__ == "__main__":
    bot.run(TOKEN)

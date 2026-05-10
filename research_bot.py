import discord
from discord.ext import commands
import logging

# 1. Setup Basic Logging to catch errors in the console
logging.basicConfig(level=logging.INFO)

class RockefellerSentry(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True  # Required for reading commands if using prefix
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        """
        This runs before the bot connects to Discord. 
        It's the best place to sync slash commands.
        """
        try:
            print("🔄 Syncing Slash Commands...")
            # OPTION A: Sync globally (can take up to 1 hour to update)
            # await self.tree.sync()
            
            # OPTION B: Sync to a specific guild (Instant update - Recommended for dev)
            # Replace 'YOUR_GUILD_ID' with your actual Server ID
            # guild = discord.Object(id=YOUR_GUILD_ID)
            # self.tree.copy_global_to(guild=guild)
            # synced = await self.tree.sync(guild=guild)
            
            synced = await self.tree.sync()
            print(f"✅ Rockefeller Sentry Online: {len(synced)} Slash Commands Synced.")
            
        except Exception as e:
            print(f"❌ Failed to sync commands: {e}")

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')

bot = RockefellerSentry()

# --- Slash Commands ---
@bot.tree.command(name="status", description="Check the status of the sentry")
async def status(interaction: discord.Interaction):
    await interaction.response.send_message("🛡️ Rockefeller Sentry is standing by. Market monitoring active.")

# --- Run the Bot ---
# Ensure you replace 'YOUR_BOT_TOKEN' with your actual token
if __name__ == "__main__":
    bot.run('YOUR_BOT_TOKEN')

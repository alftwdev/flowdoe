import requests
import random
import os
from dotenv import load_dotenv

# --- 0. CONFIG ---
load_dotenv()
WEBHOOK_URL = os.getenv("WEBHOOK_CHILLVIBES")

# A curated list of "Gold Standard" focus music
LOFI_PLAYLIST = [
    {"name": "Old School Hip Hop Mix", "url": "https://youtu.be/yaILIardwh0?si=9JbNBSlHCqhv2kBH"},
    {"name": "90s Underground Hip Hop", "url": "https://youtu.be/xcKvPfQqFFM?si=W7V5X-EAdx4N3u3k"},
    {"name": "CSunset R&B Mix", "url": "https://youtu.be/u04dzwSUv2M?si=v8PRRk9GzkMJHLIm"},
    {"name": "the soundtrack of the 2000s", "url": "https://youtu.be/XEuU5vhr8c4?si=kEMDLVhdderpxh6V"},
    {"name": "OG Chill Rap 420 | West Coast After Dark • Slow Bass • Stoner Flow", "url": "https://youtu.be/r0tZUS50T7A?si=Yw0BQmEu5li_V7bE"}
]

def post_daily_vibe():
    if not WEBHOOK_URL:
        print("Error: WEBHOOK_LOFI not found in .env")
        return

    # Pick a random vibe
    vibe = random.choice(LOFI_PLAYLIST)
    
    # Structure the Discord message
    message = {
        "content": (
            f"☕ **Today's Trading Vibe: {vibe['name']}**\n"
            "Maintain your flow state while monitoring the Algos.\n"
            f"{vibe['url']}"
        )
    }

    response = requests.post(WEBHOOK_URL, json=message)
    
    if response.status_code in [200, 204]:
        print(f"Successfully posted: {vibe['name']}")
    else:
        print(f"Failed to post. Status code: {response.status_code}")

if __name__ == "__main__":
    post_daily_vibe()

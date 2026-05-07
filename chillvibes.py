import requests
import random
import os
import pytz
from datetime import datetime
from dotenv import load_dotenv

# --- 0. CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_URL = os.getenv("WEBHOOK_CHILLVIBES")

# A curated list of "Gold Standard" focus music
LOFI_PLAYLIST = [
    {"name": "Old School Hip Hop Mix", "url": "https://youtu.be/yaILIardwh0?si=9JbNBSlHCqhv2kBH"},
    {"name": "90s Underground Hip Hop", "url": "https://youtu.be/xcKvPfQqFFM?si=W7V5X-EAdx4N3u3k"},
    {"name": "Sunset R&B Mix", "url": "https://youtu.be/u04dzwSUv2M?si=v8PRRk9GzkMJHLIm"},
    {"name": "The Soundtrack of the 2000s", "url": "https://youtu.be/XEuU5vhr8c4?si=kEMDLVhdderpxh6V"},
    {"name": "OG Chill Rap 420 | West Coast After Dark", "url": "https://youtu.be/r0tZUS50T7A?si=Yw0BQmEu5li_V7bE"}
]

def post_daily_vibe():
    if not WEBHOOK_URL:
        print("❌ Error: WEBHOOK_CHILLVIBES not found in .env")
        return

    # --- THE FIX: SEEDING BY DATE ---
    # This ensures the "random" choice is the same for the entire day, 
    # but DIFFERENT every single day.
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    today_str = datetime.now(tz_honolulu).strftime('%Y-%m-%d')
    random.seed(today_str) 
    
    vibe = random.choice(LOFI_PLAYLIST)
    
    # Structure the Discord message with "Essentials" branding
    message = {
        "embeds": [{
            "title": "☕ The Essentials: Focus Session",
            "description": (
                f"**Today's Vibe:** {vibe['name']}\n"
                f"Maintain your flow state while monitoring the Algos.\n\n"
                f"[Click to Play on YouTube]({vibe['url']})"
            ),
            "color": 0x3498db, # Essentials Blue
            "footer": {"text": f"Daily Vibe • {today_str}"}
        }]
    }

    try:
        response = requests.post(WEBHOOK_URL, json=message)
        if response.status_code in [200, 204]:
            print(f"✅ Successfully posted: {vibe['name']}")
        else:
            print(f"❌ Failed. Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Request Error: {e}")

if __name__ == "__main__":
    post_daily_vibe()

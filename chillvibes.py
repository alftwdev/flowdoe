import requests
import os
import pytz
from datetime import datetime
from dotenv import load_dotenv

# --- 0. CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_URL = os.getenv("WEBHOOK_CHILLVIBES")

# Your Personal Gold Standard Playlist
# Appending shuffle parameters to ensure a fresh start each time
PERSONAL_PLAYLIST_URL = "https://youtube.com/playlist?list=PLKTJFoK2VZXPI8D7OxbapTj4id4JeynP7&si=v983o-N0Wxrct7Uk&index=1&shuffle=1"

def get_session_context():
    """Determines the branding based on Honolulu local time."""
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    hour = datetime.now(tz_honolulu).hour
    
    if 5 <= hour < 9:
        return {"name": "Sunrise Session", "color": 0xff9f43} # Orange
    if 9 <= hour < 16:
        return {"name": "Market Hours Focus", "color": 0x2ecc71} # Green
    if 16 <= hour < 21:
        return {"name": "Sunset R&B / Chill", "color": 0x9b59b6} # Purple
    return {"name": "Late Night Flow", "color": 0x34495e} # Dark Blue

def post_vibe_update():
    if not WEBHOOK_URL:
        print("❌ Error: WEBHOOK_CHILLVIBES not found in .env")
        return

    context = get_session_context()
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    time_str = datetime.now(tz_honolulu).strftime('%I:%M %p HST')

    message = {
        "embeds": [{
            "title": f"☕ The Essentials: {context['name']}",
            "description": (
                "**Current Session**: Your Personal Vault\n"
                "Maintain your flow state. The playlist below is synced for your curated focus music.\n\n"
                f"📺 **[Click to Launch Shuffled Playlist]({PERSONAL_PLAYLIST_URL})**"
            ),
            "color": context['color'],
            "footer": {"text": f"Sentry Flow • Sync: {time_str}"}
        }]
    }

    try:
        response = requests.post(WEBHOOK_URL, json=message)
        if response.status_code in [200, 204]:
            print(f"✅ Successfully posted {context['name']} to Discord.")
        else:
            print(f"❌ Discord error: {response.status_code}")
    except Exception as e:
        print(f"❌ Request Error: {e}")

if __name__ == "__main__":
    post_vibe_update()

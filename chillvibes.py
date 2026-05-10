import requests
import os
import pytz
from datetime import datetime
from dotenv import load_dotenv

# --- 0. CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_URL = os.getenv("WEBHOOK_CHILLVIBES")

# VIBE CATEGORIES (Mixing Live Streams for 24/7 continuity)
VIBE_MATRIX = {
    "MORNING": { # 05:00 - 09:00 HST
        "name": "Sunrise Lo-Fi & Coffee",
        "url": "https://www.youtube.com/live/jfKfPfyJRdk", # Lofi Girl Live
        "color": 0xff9f43 
    },
    "MARKET_HOURS": { # 09:00 - 16:00 HST
        "name": "High-Focus Boom Bap / 90s Underground",
        "url": "https://youtu.be/xcKvPfQqFFM", 
        "color": 0x2ecc71
    },
    "EVENING": { # 16:00 - 21:00 HST
        "name": "Sunset R&B / West Coast After Dark",
        "url": "https://youtu.be/r0tZUS50T7A",
        "color": 0x9b59b6
    },
    "NIGHT_OWL": { # 21:00 - 05:00 HST
        "name": "Deep Space / Midnight Study",
        "url": "https://www.youtube.com/live/S_MOd40zlYU",
        "color": 0x34495e
    }
}

def get_current_vibe():
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    hour = datetime.now(tz_honolulu).hour
    
    if 5 <= hour < 9: return VIBE_MATRIX["MORNING"]
    if 9 <= hour < 16: return VIBE_MATRIX["MARKET_HOURS"]
    if 16 <= hour < 21: return VIBE_MATRIX["EVENING"]
    return VIBE_MATRIX["NIGHT_OWL"]

def post_vibe_update():
    if not WEBHOOK_URL:
        return

    vibe = get_current_vibe()
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    time_str = datetime.now(tz_honolulu).strftime('%I:%M %p HST')

    message = {
        "embeds": [{
            "title": f"☕ The Essentials: {vibe['name']}",
            "description": (
                f"**Current Session**: Active\n"
                f"Maintain your flow state. The music is currently synced for your local time.\n\n"
                f"📺 **[Click to Open 24/7 Stream]({vibe['url']})**"
            ),
            "color": vibe['color'],
            "footer": {"text": f"Sentry Flow • Updated at {time_str}"}
        }]
    }

    requests.post(WEBHOOK_URL, json=message)

if __name__ == "__main__":
    post_vibe_update()

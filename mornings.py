import os
import requests
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase
from visuals import generate_institutional_chart

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Morning_Digest")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_URL = os.getenv("WEBHOOK_MARKET_ANALYSIS")
db = EcosystemDatabase()

def send_digest_with_visual(webhook_url, title, payload, color, image_path=None):
    """Natively handles multipart/form-data to upload the visuals.py chart to Discord."""
    embed = {
        "title": title,
        "description": payload,
        "color": color,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if image_path and os.path.exists(image_path):
        embed["image"] = {"url": f"attachment://{os.path.basename(image_path)}"}
        
        with open(image_path, "rb") as f:
            files = {
                "file": (os.path.basename(image_path), f, "image/png")
            }
            payload_json = {"embeds": [embed]}
            response = requests.post(
                webhook_url, 
                data={"payload_json": __import__('json').dumps(payload_json)},
                files=files
            )
    else:
        response = requests.post(webhook_url, json={"embeds": [embed]})
        
    if response.status_code in [200, 204]:
        logger.info("Morning Digest and Visuals successfully dispatched.")
    else:
        logger.error(f"Failed to push digest. Status: {response.status_code}, Response: {response.text}")

def compile_morning_digest():
    logger.info("Compiling Unified Morning Digest...")
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.now(tz_h)
    
    # Core Data Retrieval
    net_liq = float(db.get_state("net_liquidity", 0.0))
    vix_iv = float(db.get_state("vix_iv_index", 0.0))
    spy_poc = float(db.get_state("SPY_poc", 0.0))
    vrp = float(db.get_state("SPY_vrp_latest", 0.0))
    
    vrp_status = "🔴 NEGATIVE (Risk Off / Tail Risk Underpriced)" if vrp < 0 else "🟢 POSITIVE (Premium Harvesting Active)"
    
    payload_lines = [
        f"### 🌐 SYSTEMIC TELEMETRY",
        f"┣ **Federal Net Liquidity**: `${net_liq:,.0f}B`",
        f"┣ **VIX Implied Volatility**: `{vix_iv}`",
        f"┣ **Volatility Risk Premium**: {vrp_status}",
        f"┗ **SPY Point of Control (POC)**: `${spy_poc:,.2f}`\n"
    ]

    # Weekend Integration: Triggered on Fridays (Weekday 4)
    if now.weekday() == 4:
        logger.info("Friday detected: Appending Weekend Executive Summary & RSS Polarity Framework.")
        rss_state = "BEARISH DIVERGENCE" if vrp < 0 and vix_iv > 20 else "BULLISH CONVERGENCE"
        payload_lines.extend([
            f"--------------------------------------------------------------------",
            f"### 📊 WEEKEND EXECUTIVE SUMMARY (RSS Polarity Framework)",
            f"┣ **Macro Regime**: `{rss_state}`",
            f"┣ **Capital Allocation Bias**: {'Preservation / Cash Heavy' if vrp < 0 else 'Risk-On Expansion'}",
            f"┗ **Weekly Liquidity Shift**: Review institutional order flow at major POC nodes before Monday open.\n"
        ])

    payload_lines.append(f"🧠 *Institutional chart generated via ROCKEFELLER VISUAL ENGINE.*")
    final_payload = "\n".join(payload_lines)
    
    # Trigger visuals.py
    logger.info("Engaging Visual Engine for SPY Matrix...")
    chart_path = generate_institutional_chart("SPY")
    
    if WEBHOOK_URL:
        send_digest_with_visual(
            webhook_url=WEBHOOK_URL,
            title="🌅 ROCKEFELLER STRATEGIC INTELLIGENCE: Morning Matrix",
            payload=final_payload,
            color=0x3498db,
            image_path=chart_path
        )

if __name__ == "__main__":
    compile_morning_digest()

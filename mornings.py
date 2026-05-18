import os
import json
import pytz
import datetime
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. ARCHITECTURE CONFIGURATION & STATE LEDGERS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
STATE_FILE = os.path.join(BASE_DIR, "last_alert.json")

def load_crypto_state_context():
    """
    Ingests asset states from last_alert.json to extract raw pricing telemetry 
    and calculate dynamic derivative implied distributions for the brief.
    """
    context = {"btc_price": "N/A", "eth_price": "N/A", "has_crypto": False}
    if not os.path.exists(STATE_FILE):
        return context
    try:
        with open(STATE_FILE, "r") as f:
            state_data = json.load(f)
            
        btc_raw = state_data.get("crypto_BTC_USD")
        eth_raw = state_data.get("crypto_ETH_USD")
        
        if btc_raw:
            price_val = float(btc_raw)
            # Re-verify baseline premium: 12.50% annualized yield math
            est_mo = (price_val * 0.125) / 12.0
            context["btc_price"] = f"${price_val:,.2f} (Est: ${est_mo:,.2f}/mo premium)"
            context["has_crypto"] = True
            
        if eth_raw:
            price_val = float(eth_raw)
            # Re-verify baseline premium: 13.80% annualized yield math
            est_mo = (price_val * 0.138) / 12.0
            context["eth_price"] = f"${price_val:,.2f} (Est: ${est_mo:,.2f}/mo premium)"
            context["has_crypto"] = True
            
        return context
    except Exception as e:
        print(f"⚠️ Crypto State Ingestion Omitted: {e}")
        return context

def generate_morning_brief():
    """Consumes multiple Ecosystem Ledgers to produce a high-conviction cross-asset daily battle plan."""
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.datetime.now(tz_h)
    
    # 1. Load Data from Core Market Regime Ledger
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            regime = data.get("regime", "NEUTRAL")
            vix_status = data.get("vix_status", "STABLE")
            rsi_limit = data.get("rsi_shield_limit", 66)
            futures_pulse = data.get("futures_pulse", "🟡 NEUTRAL")
            system_override = data.get("system_override", False)
            active_threats = data.get("active_threats", [])
    except Exception as e:
        print(f"⚠️ Core Ledger Load Failed: {e}")
        regime, vix_status, rsi_limit, system_override = "NEUTRAL", "UNKNOWN", 66, False
        futures_pulse = "⚪ UNKNOWN"
        active_threats = []

    # 2. Ingest Sovereign Crypto Contextual Layer
    crypto_ctx = load_crypto_state_context()

    # 3. Determine Rockefeller Tone & Tactical Posture
    if system_override or vix_status == "STORM":
        color = 0xe74c3c  # Red (Defensive)
        posture = "🛡️ DEFENSIVE: Shield is ACTIVE. Capital preservation priority."
        objectives = [
            "1. NO NEW ENTRIES: Market volatility exceeds safety thresholds.",
            "2. EXIT TRAILING: Tighten stops on all open positions.",
            "3. CASH IS A POSITION: Wait for VIX to mean-revert below 20."
        ]
    elif vix_status == "ELEVATED":
        color = 0xf1c40f  # Yellow (Cautious)
        posture = "⚖️ MEASURED: Volatility is rising. Size positions at 50%."
        objectives = [
            "1. TIGHT SHIELD: Only entries with RSI < 55 are valid.",
            "2. SCALP FOCUS: Target 4-8 tick moves only.",
            "3. MONITOR VIX: Any spike above 22 triggers immediate lockdown."
        ]
    else:
        color = 0x2ecc71  # Green (Offensive)
        posture = "🚀 OFFENSIVE: Conditions are STABLE. Execute at full size."
        objectives = [
            "1. ALPHA HUNT: Follow #trade-signals for high-conviction entries.",
            "2. TREND ALIGNMENT: Prioritize assets showing structural Whale Accumulation.",
            "3. RSI LIMIT: Standard < 66 threshold applies."
        ]

    # 4. Contextual Strategy Overrides (Cross-Asset Intelligence Inject)
    if vix_status == "STABLE" and crypto_ctx["has_crypto"]:
        # If macro radar states imply top-heavy conditions, write dynamic defensive guide
        objectives.append("4. THETA HARVEST: Crypto vectors running hot; prioritize premium credit options over spot equity chase.")

    # 5. Build Intelligence Embed
    title = "🌅 Rockefeller Morning Intelligence Briefing"
    objective_text = "\n".join(objectives)
    
    # Construct base layout block
    desc_lines = [
        f"### **Daily Battle Plan: {now.strftime('%b %d, %Y')}**",
        f"**Tactical Posture**: {posture}",
        "",
        f"📊 **Core Equity Index Metrics**:",
        f"┣ **Market Regime**: `{regime}`",
        f"┣ **Volatility Sentry**: `{vix_status}`",
        f"┣ **Futures Pulse**: `{futures_pulse}`",
        f"┗ **RSI Shield**: `Active < {rsi_limit}`",
        ""
    ]

    # Append Sovereign Crypto Telemetry block if confirmed active in files
    if crypto_ctx["has_crypto"]:
        desc_lines.extend([
            f"🏛️ **Sovereign Liquidity Snapshot**:",
            f"┣ **BTC Core Matrix**: `{crypto_ctx['btc_price']}`",
            f"┗ **ETH Core Matrix**: `{crypto_ctx['eth_price']}`",
            ""
        ])

    desc_lines.extend([
        f"🛡️ **Tactical Objectives**:",
        f"{objective_text}",
        "",
        f"*Briefing generated by Rockefeller Cross-Asset Architecture. Dispatched at {now.strftime('%H:%M HST')}.*"
    ])

    description = "\n".join(desc_lines)

    if HAS_ESSENTIALS and WEBHOOK_MARKET:
        send_essentials_embed(WEBHOOK_MARKET, title, description, color)
        print(f"✅ Cross-Asset Morning Intelligence brief dispatched for {vix_status} regime.")
    else:
        # Fallback local stdout dump if webhook credentials are detached
        print("❌ Dispatch skipped: Direct stdout buffer visualization below:\n")
        print(f"Title: {title}\nColor: {color}\n{description}")

if __name__ == "__main__":
    generate_morning_brief()

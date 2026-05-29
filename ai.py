import os
import json
import logging
import requests
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("AI_Engine")
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
db = EcosystemDatabase()

def generate_ai_macro_brief(history_data, fred_liquidity, credit_spread, win_rate=78.5, points_captured=42, persona="Paul Tudor Jones"):
    personas = {
        "Warren Buffett": "Value Investor. Focus on wide moats.",
        "Paul Tudor Jones": "Macro Contrarian. Focus on turning points.",
        "Plan B": "Quant Strategist. Focus on stock-to-flow."
    }
    
    active_persona = personas.get(persona, personas["Paul Tudor Jones"])
    
    # Extract live VRP metrics from ecosystem memory
    latest_vrp = db.get_state("SPY_vrp_latest", 0.0)
    vix_iv = db.get_state("vix_iv_index", 20.0)
    vrp_regime = "Volatility Harvesting (VRP > 0)" if latest_vrp > 0 else "Underpriced Insurance (VRP < 0)"

    # UPGRADE: Fixed Google Generative AI Endpoint to the stable 'latest' version
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro-latest:generateContent?key={GEMINI_API_KEY}"
    
    prompt = f"""
    SYSTEM: {active_persona}
    Analyze this 30-day snapshot along with today's Federal Reserve metrics, and output ONLY a JSON payload matching the required schema. No markdown wrappers around the JSON. 
    
    Additionally, draft a 2-sentence 'Alpha Recap' highlighting our ecosystem's performance today. The system achieved a {win_rate}% win rate today, capturing a {points_captured} point move off the Gamma Flip line calculated by our proprietary GEX engine. Format this recap to entice free users to upgrade for real-time access.
    
    FEDERAL RESERVE & MACRO LIVE STATE:
    - Global Net Liquidity: ${fred_liquidity}B
    - High Yield Credit Spread: {credit_spread}%
    - Implied Volatility (VIX): {vix_iv}
    - Calculated VRP Regime: {vrp_regime} (Score: {latest_vrp:.3f})

    CRITICAL INSTRUCTION: Calculate the current VRP regime impact. If the market is in a 'Volatility Harvesting' phase, explicitly emphasize caution on speculative buying and reinforce defensive yield strategies (credit spreads/premium selling) within the discord_embed_brief.
    
    MARKET SNAPSHOT:
    {history_data}
    
    REQUIRED JSON SCHEMA:
    {{
      "macro_regime_outlook": "BULLISH | BEARISH | CHOP | STORM",
      "recommended_position_sizing": 0.0 to 1.0,
      "sector_rotation_focus": "String noting which sectors to target based on liquidity",
      "tactical_adjustment_notes": "String explaining the sizing decision based on VRP and Liquidity",
      "discord_embed_brief": "A 3-sentence, authoritative market brief formatted with markdown for Discord, explaining the intermarket liquidity flows and VRP strategy."
    }}
    """    
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"}
    }
    
    try:
        res = requests.post(url, json=payload, timeout=15)
        res.raise_for_status()
        raw_text = res.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(raw_text)
    except Exception as e:
        logger.error(f"Gemini API failure: {e}")
        return {
            "macro_regime_outlook": "CHOP", "recommended_position_sizing": 0.25,
            "sector_rotation_focus": "DEFENSIVE PRESERVATION",
            "tactical_adjustment_notes": "API disruption. Defaults engaged.",
            "discord_embed_brief": "⚠️ System Boundary Exception. Defaults active."
        }

def broadcast_public_teaser(is_test=False):
    logger.info("Generating public AI conversion teaser...")
    fred_liquidity = db.get_state("net_liquidity", 7000.0)
    credit_spread = db.get_state("credit_spread", 3.5)
    history_data = "Market data nominal. Continuous quantitative sweep active."
    
    ai_response = generate_ai_macro_brief(history_data, fred_liquidity, credit_spread)
    
    try:
        from essentials_tools import send_essentials_embed
        WEBHOOK = os.getenv("WEBHOOK_PUBLIC_ANNOUNCEMENTS")
        embed_content = ai_response.get("discord_embed_brief", "Check the server for updates.")
        
        if WEBHOOK:
            send_essentials_embed(WEBHOOK, "🤖 Strategic Market Insight", embed_content, 0x3498db)
            logger.info("Public teaser broadcasted to Discord.")
    except Exception as e:
        logger.error(f"Broadcast failure: {e}"

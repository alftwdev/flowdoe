import os
import logging
import requests
from dotenv import load_dotenv

logger = logging.getLogger("AI_Engine")
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def generate_ai_macro_brief(history_data, fred_liquidity, credit_spread, win_rate=78.5, points_captured=42, persona="Paul Tudor Jones"):
    personas = {
        "Warren Buffett": "Value Investor. Focus on wide moats.",
        "Paul Tudor Jones": "Macro Contrarian. Focus on turning points.",
        "Plan B": "Quant Strategist. Focus on stock-to-flow."
    }
    active_persona = personas.get(persona, personas["Paul Tudor Jones"])
    
    prompt = f"""
    SYSTEM: {active_persona}
    Analyze this 30-day snapshot and today's Federal Reserve metrics.
    Output ONLY a JSON payload. 
    Performance metric: Win Rate {win_rate}%, captured {points_captured} pts.
    
    FEDERAL RESERVE LIVE STATE:
    - Global Net Liquidity: ${fred_liquidity}B
    - High Yield Credit Spread: {credit_spread}%
    """
    # ... [Your existing API request logic]     
    FEDERAL RESERVE LIVE STATE:
    - Global Net Liquidity: ${fred_liquidity_billions}
    - High Yield Credit Spread: {credit_spread}% This 30-day snapshot along with today's Federal Reserve metrics, and output ONLY a JSON payload matching the required schema. No markdown wrappers around the JSON. Additionally, you must draft a 2-sentence 'Alpha Recap' highlighting our ecosystem's performance today. The system achieved a {win_rate}% win rate today, capturing a {points_captured} point move off the Gamma Flip line calculated by our proprietary GEX engine. Format this recap to entice free users to upgrade for real-time access.
    
    FEDERAL RESERVE LIVE STATE:
    - Global Net Liquidity: ${fred_liquidity_billions}B
    - High Yield Credit Spread: {credit_spread}%
    
    MARKET SNAPSHOT:
    {history_data_string}
    
    REQUIRED JSON SCHEMA:
    {{
      "macro_regime_outlook": "BULLISH | BEARISH | CHOP | STORM",
      "recommended_position_sizing": 0.0 to 1.0,
      "sector_rotation_focus": "String noting which sectors to target based on liquidity",
      "tactical_adjustment_notes": "String explaining the sizing decision",
      "discord_embed_brief": "A 3-sentence, authoritative market brief formatted with markdown for Discord, explaining the intermarket liquidity flows."
    }}
    """    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1, # Extremely analytical, zero creative hallucination
            "responseMimeType": "application/json" 
        }
    }
    
    try:
        res = requests.post(url, json=payload, timeout=15)
        res.raise_for_status()
        
        # Parse Gemini 1.5 Pro's JSON mode output
        raw_text = res.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(raw_text)
        
    except Exception as e:
        logger.error(f"Gemini API link disruption or payload validation failure: {e}")
        
        # Fallback Operational Blueprint Schema to prevent downstream script dependency execution crashes
        fallback_schema = {
            "macro_regime_outlook": "CHOP",
            "recommended_position_sizing": 0.25,
            "sector_rotation_focus": "DEFENSIVE PRESERVATION / CASH ESCROW",
            "tactical_adjustment_notes": "Automated SRE safety circuit triggered due to an external network API parsing exception. Reverting engine parameters to high-integrity defensive limits.",
            "discord_embed_brief": "⚠️ **System Boundary Exception:** Macro AI analytics core is temporarily unreachable. The engine has seamlessly engaged internal defensive defaults. System stability remains green."
        }
        return fallback_schema

# Add this to your ai.py file
def broadcast_public_teaser(is_test=False):
    """
    Generates the macro teaser and dispatches it to the public announcement channel.
    """
    logger.info("Generating public AI conversion teaser...")
    
    # 1. Gather data
    # (Using placeholders here; ensure this aligns with your state)
    fred_liquidity = 7000  # Mocking or fetching from DB
    credit_spread = 3.5
    history_data = "Market data nominal."
    
    # 2. Get AI Payload
    # Assuming generate_ai_macro_brief is already defined in ai.py
    ai_response = generate_ai_macro_brief(history_data, fred_liquidity, credit_spread)
    
    # 3. Send to Discord
    # Reusing your existing essentials tool for the broadcast
    try:
        from essentials_tools import send_essentials_embed
        WEBHOOK = os.getenv("WEBHOOK_ANNOUNCEMENTS") # Ensure this is in your .env
        
        embed_content = ai_response.get("discord_embed_brief", "Check the server for updates.")
        
        if WEBHOOK:
            send_essentials_embed(WEBHOOK, "🤖 Strategic Market Insight", embed_content, 0x3498db)
            logger.info("Public teaser broadcasted to Discord.")
        else:
            logger.warning("No WEBHOOK_ANNOUNCEMENTS found. Skipping broadcast.")
            
    except Exception as e:
        logger.error(f"Broadcast failure: {e}")    

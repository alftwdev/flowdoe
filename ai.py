import os
import json
import logging
import requests
from dotenv import load_dotenv

logger = logging.getLogger("AI_Engine")

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def generate_ai_macro_brief(history_data_string, fred_liquidity_billions, credit_spread):
    """Feeds ecosystem and FRED data to Gemini Pro and parses the JSON response."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent?key={GEMINI_API_KEY}"
    
    prompt = f"""
    SYSTEM: You are the Chief Quantitative Strategist for Rockefeller Strategic Intelligence.
    Analyze this 30-day snapshot along with today's Federal Reserve metrics, and output ONLY a JSON payload matching the required schema. No markdown wrappers around the JSON.
    
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

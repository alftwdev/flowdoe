import os
import logging
from database import EcosystemDatabase

logger = logging.getLogger("AI_Engine")
db = EcosystemDatabase()

def generate_ai_macro_brief(history_data=None, fred_liquidity=0.0, credit_spread=0.0, **kwargs):
    """
    LLM BYPASSED. Pulls real-time quantitative state natively from the SQLite ledger 
    and formats it for immediate Discord dispatch.
    """
    latest_vrp = float(db.get_state("SPY_vrp_latest", 0.0))
    vix_iv = float(db.get_state("vix_iv_index", 20.0))
    
    regime = "🟢 Volatility Harvesting (VRP > 0)" if latest_vrp > 0 else "🔴 Underpriced Insurance (VRP < 0)"

    raw_payload = (
        f"**Systemic Base Liquidity**: `${float(fred_liquidity):,.0f}B`\n"
        f"**Credit Spread Risk**: `{float(credit_spread)}%`\n"
        f"**VIX Implied Volatility**: `{vix_iv}`\n"
        f"**Current VRP Regime**: `{regime}`"
    )

    return {
        "macro_regime_outlook": "QUANTITATIVE NORMALIZATION",
        "discord_embed_brief": raw_payload
    }

def broadcast_public_teaser(is_test=False):
    pass

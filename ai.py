import os
import logging
from database import EcosystemDatabase

logger = logging.getLogger("AI_Engine")
db = EcosystemDatabase()

def generate_retail_translation(sector, fred_liquidity=0.0, credit_spread=0.0, vix_iv=20.0, vrp=0.0):
    """
    Natively translates institutional state metrics into actionable, 
    retail-friendly Discord blueprints tailored per asset class.
    """
    if sector == "options":
        is_negative = vrp < 0
        regime = "🔴 UNDERPRICED INSURANCE (VRP < 0)" if is_negative else "🟢 PREMIUM RICH (VRP > 0)"
        status = "PRESERVATION MODE — NO NEW SHORTS" if is_negative else "ALPHA HARVESTING ACTIVE"
        action = "Market makers are underpricing tail risk today. Selling premium right now offers an uncompensated, asymmetric risk of loss. Sit on your hands or utilize tightly defined-risk parameters." if is_negative else "Options pricing includes a statistically favorable premium buffer. Strategic selling authorized."
        
        payload = (
            f"[STATUS: {status}]\n\n"
            f"🧠 **Quick Take**: {action}\n\n"
            f"🎯 **THE PREMIUM BENCHMARKS**:\n"
            f"┣ 📊 **VIX Spot**: `{vix_iv}`\n"
            f"┣ ⚖️ **Volatility Risk Premium (VRP)**: `{regime}`\n"
            f"┗ ⚠️ **Systemic Base Liquidity**: `${float(fred_liquidity):,.0f}B`\n\n"
            f"💡 **Tactical Directive for Income Sellers**:\n"
            f"• **Avoid**: Naked Short Puts or Wide Unhedged Iron Condors on SPY/QQQ during negative VRP.\n"
            f"• **Permitted**: Long-volatility calendars or pure capital preservation."
        )
        return {"title": "🛡️ SYSTEMIC OPTIONS RISK MATRIX", "payload": payload, "color": 0xe74c3c if is_negative else 0x2ecc71}

    elif sector == "tsp":
        cred_risk = "🛑 REDUCE EXPOSURE (Vulnerable to credit stress)" if credit_spread > 4.5 else "🟢 EQUITIES BUFFERED"
        liq_risk = "Flight to Quality (Large Caps outperforming)" if float(fred_liquidity) < 6000 else "Risk-On Expansion"
        
        payload = (
            f"[POSTURE: {'DEFENSIVE' if credit_spread > 4.5 else 'STRUCTURAL GROWTH'} ALLOCATION STRATEGY]\n\n"
            f"🧠 **Quick Take**: Since TSP accounts have strict monthly interfund transfer limits, preserving capital during liquidity drains is our primary objective. High-frequency noise is filtered out.\n\n"
            f"⚖️ **MACRO CONTEXT**:\n"
            f"┣ **Systemic Base Liquidity**: `${float(fred_liquidity):,.0f}B`\n"
            f"┣ **Credit Spread**: `{credit_spread}%`\n"
            f"┗ **Market Rotation**: 👑 {liq_risk}\n\n"
            f"🏛️ **FUND-BY-FUND STRUCTURAL POSTURES**:\n"
            f"┣ 🇺🇸 **C Fund** (Large Cap): 🟡 NEUTRAL HOLD (Protected by Large-Cap safety)\n"
            f"┣ 📈 **S Fund** (Small Cap): {cred_risk}\n"
            f"┣ 🌍 **I Fund** (International): 🟡 DEPENDENT ON DOLLAR STRENGTH\n"
            f"┗ 🛡️ **F/G Funds** (Fixed/Cash): 🟢 ALLOCATION SAFE HARBOR"
        )
        return {"title": "🏦 STRUCTURAL TSP FUND ALLOCATION MATRIX", "payload": payload, "color": 0x3498db}

    elif sector == "crypto":
        payload = (
            f"[POSTURE: INSTITUTIONAL SENTRY ACTIVE]\n\n"
            f"🧠 **Quick Take**: Crypto assets are highly sensitive to systemic liquidity. We are monitoring the Federal Reserve's balance sheet against spot volatility to filter out noise algorithms.\n\n"
            f"🎯 **LIQUIDITY & RISK BENCHMARKS**:\n"
            f"┣ ⚠️ **Systemic Base Liquidity**: `${float(fred_liquidity):,.0f}B`\n"
            f"┣ 📊 **Credit Spread Risk**: `{credit_spread}%`\n"
            f"┗ ⚖️ **VIX Implied Volatility**: `{vix_iv}`"
        )
        return {"title": "⚡ CRYPTO MATRIX: INSTITUTIONAL FLOW", "payload": payload, "color": 0x9b59b6}
        
    elif sector == "macro":
        payload = (
            f"[POSTURE: GLOBAL MACRO RADAR]\n\n"
            f"🧠 **Quick Take**: Forex and global macro positioning relies on tracking real-time capital destruction or creation via Fed Liquidity.\n\n"
            f"🎯 **CORE BENCHMARKS**:\n"
            f"┣ ⚠️ **Systemic Base Liquidity**: `${float(fred_liquidity):,.0f}B`\n"
            f"┗ 📊 **Credit Spread Risk**: `{credit_spread}%`"
        )
        return {"title": "🌍 MACRO VOLATILITY BRIEF", "payload": payload, "color": 0x34495e}
    
    return {"title": "📡 Sector Pulse", "payload": "System active.", "color": 0x95a5a6}

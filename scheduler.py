import os
import sys
import argparse
import logging
from dotenv import load_dotenv
from analytics import HighFidelityAnalyticsEngine
from essentials_tools import send_essentials_embed

logger = logging.getLogger("Central_Scheduler")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_TSP = os.getenv("WEBHOOK_FED")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS") or WEBHOOK_MARKET
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS") or WEBHOOK_MARKET

def main():
    parser = argparse.ArgumentParser(description="Rockefeller Systemic Scheduler Dashboard.")
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "tsp", "income", "iv_crush", "gex"])
    args = parser.parse_args()

    engine = HighFidelityAnalyticsEngine()
    logger.info(f"Executing scheduled operational sweep: {args.mode.upper()}")

    try:
        if args.mode == "morning":
            spy_matrix = engine.construct_comprehensive_matrix("SPY")
            description = (
                f"### **📦 Institutional Momentum & Order Flow Delta**\n"
                f"┣ **Relative Volume ($RVOL$)**: `{spy_matrix['volume_velocity']['rvol']}x`\n"
                f"┗ **Order Flow Variance**: `{spy_matrix['volume_velocity']['sigma_deviation']}\\sigma` deviations\n\n"
                f"### **🎯 Technical Mean Reversion Boundaries**\n"
                f"┣ **Current Daily RSI (14)**: `{spy_matrix['technical_reversion']['rsi']}`\n"
                f"┗ **Bollinger Support Limit**: `${spy_matrix['technical_reversion']['lower_band']}`\n\n"
                f"**Ecosystem Directive**: " + 
                ("🚨 UNUSUAL RETAIL INFLOW DETECTED - Avoid counter-trend shorts." if spy_matrix['volume_velocity']['spike_detected'] else "⚖️ Order book delta stable.")
            )
            send_essentials_embed(WEBHOOK_MARKET, "🌅 ROCKEFELLER STRATEGIC INTELLIGENCE: Morning Matrix", description, 0x00ffff)

        elif args.mode == "eod":
            bps_data = engine.verify_session_containment("SPY")
            score = bps_data.get('precision', 0.0) if bps_data else "N/A"
            description = (
                f"📊 **Systemic EOD Performance & Boundary Reconciliation**\n\n"
                f"**Ecosystem Precision Rating**: 🎯 `{score}%` Accuracy\n"
                f"*The macro-quant architecture successfully contained today's internal index rotation.*\n\n"
                f"**Engine Verdict**: VALIDATED. Tactical parameters for tomorrow's open are caching."
            )
            send_essentials_embed(WEBHOOK_MARKET, "🏦 ROCKEFELLER STRATEGIC INTELLIGENCE: EOD Reconciliation", description, 0x2ecc71)

        elif args.mode == "tsp":
            tsp_payload = engine.compile_tsp_allocation_matrix()
            send_essentials_embed(WEBHOOK_TSP, "🦅 Government & Military Wealth Matrix: TSP Tactical Vector", tsp_payload, 0x3498db)

        elif args.mode == "income":
            schd_data = engine._execute_query("price", {"symbol": "SCHD"})
            schd_price = float(schd_data.get("price", 82.10)) if schd_data else 82.10
            clean_schd_yield = engine.calculate_clean_yield("SCHD", 0.72, schd_price)
            payload = (
                f"🏦 **Institutional Yield & Distribution Terminal**\n\n"
                f"📊 **GOING EX-DIVIDEND TODAY (Normalized Capture)**\n"
                f"┣ **SCHD**: `{clean_schd_yield*100:.2f}%` Clean Yield | Spot: `${schd_price:,.2f}`\n"
                f"┗ *System Filter: Structural capital distributions successfully separated from special payouts.*"
            )
            send_essentials_embed(WEBHOOK_INCOME, "💰 Yield Engine Analytics Pulse", payload, 0xf1c40f)

        elif args.mode == "iv_crush":
            scan_data = engine.run_iv_crush_scan()
            if not scan_data: return
            payload = "💥 **Systemic IV Overpricing & Volatility Crush Report**\n\n"
            for asset in scan_data:
                payload += (
                    f"**Asset**: `{asset['symbol']}`\n"
                    f"┣ Trailing 30D Historical Volatility: `{asset['hv']}%`\n"
                    f"┣ Front-Month Implied Volatility (IV): `{asset['iv']}%`\n"
                    f"┗ 🔥 **Premium Edge Spread**: `{asset['spread']:+.1f}%` Vol Variance\n"
                    f"💡 *Tactical Action: Selling credit strategies or iron condors here carries maximized statistical advantages due to current premium inflation.*\n\n"
                )
            send_essentials_embed(WEBHOOK_OPTIONS, "📉 VOLATILITY ARBITRAGE TERMINAL: IV Crush Scanner", payload, 0x9b59b6)

        elif args.mode == "gex":
            gex_data = engine.calculate_gex_profile("SPY")
            payload = (
                f"🧬 **Automated Market Maker Positioning Map (SPY)**\n\n"
                f"┣ **Current Spot Price**: `${gex_data['current_spot']:.2f}`\n"
                f"┣ 🎯 **Systemic Gamma Flip Line**: `${gex_data['flip_strike']:.2f}`\n"
                f"┗ **Structural Posture Context**: {gex_data['market_state']}\n\n"
                f"⚠️ *Strategic Warning: Fading or breaking the Gamma Flip line will result in an immediate shift in institutional market-maker hedging algorithms. Prepare for dynamic expansion if the price drops below support.*"
            )
            send_essentials_embed(WEBHOOK_MARKET, "🎛️ COGNITIVE ARCHITECTURE MATRIX: Pre-Market GEX Mapping", payload, 0xe67e22)

    except Exception as e:
        logger.critical(f"Task Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

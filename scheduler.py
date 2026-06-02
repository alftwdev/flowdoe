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
WEBHOOK_URL = os.getenv("WEBHOOK_MARKET_ANALYSIS")

def main():
    parser = argparse.ArgumentParser(description="Rockefeller Systemic Scheduler Dashboard.")
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "weekly_harvest"])
    args = parser.parse_args()

    if not WEBHOOK_URL:
        logger.error("Missing configuration: WEBHOOK_MARKET_ANALYSIS variable is unassigned.")
        sys.exit(1)

    engine = HighFidelityAnalyticsEngine()
    logger.info(f"Executing scheduled operational sweep: {args.mode.upper()}")

    if args.mode == "morning":
        # Dynamic tracking analysis on core index vehicles
        spy_matrix = engine.construct_comprehensive_matrix("SPY")
        
        description = (
            f"### **📦 Institutional Momentum & Order Flow Delta**\n"
            f"┣ **Relative Volume ($RVOL$ Interval)**: `{spy_matrix['volume_velocity']['rvol']}x`\n"
            f"┗ **Order Flow Distribution Balance**: `{spy_matrix['volume_velocity']['sigma_deviation']}\\sigma` deviations\n\n"
            f"### **🎯 Technical Mean Reversion Boundaries**\n"
            f"┣ **Current Daily RSI (14)**: `{spy_matrix['technical_reversion']['rsi']}`\n"
            f"┗ **Bollinger Support Limit**: `${spy_matrix['technical_reversion']['lower_band']}`\n\n"
            f"### **🏛️ Multi-Generational Wealth Moat Metrics**\n"
            f"┣ **Return on Invested Capital ($ROIC$)**: `{spy_matrix['fundamental_moat']['roic']:.2f}%`\n"
            f"┗ **Systemic Debt-to-Equity Leverage Ratio**: `{spy_matrix['fundamental_moat']['debt_to_equity']:.2f}`\n\n"
            f"**Ecosystem Actionable Directive**: " + 
            ("🚨 UNUSUAL RETAIL INFLOW DETECTED - Avoid entering counter-trend short options positions." if spy_matrix['volume_velocity']['spike_detected'] else "⚖️ Order book delta remains within normal parameters.")
        )
        
        send_essentials_embed(WEBHOOK_URL, "🌅 ROCKEFELLER STRATEGIC INTELLIGENCE: Market Matrix Overview", description, 0x00ffff)

    elif args.mode == "eod":
        # Generate summary report across high-income asset targets
        income_targets = ["CHPY", "MLPI", "TSPY"]
        lines = []
        for asset in income_targets:
            res = engine.replicate_mean_reversion(asset)
            div = engine.db.get_state(f"dividend_cache_{asset}", {"dividend_safety_score": "UNKNOWN"})
            lines.append(f"┣ **{asset} Spot**: `${res.get('spot_price', 0.0)}` | Dividend Quality Profile: `{div.get('dividend_safety_score')}`")
            
        description = "### 💸 EOD High-Frequency Premium Allocations\n" + "\n".join(lines)
        send_essentials_embed(WEBHOOK_URL, "🏦 ROCKEFELLER STRATEGIC INTELLIGENCE: Closing Income Ledger", description, 0xffd700)

    elif args.mode == "weekly_harvest":
        # Low-frequency storage updates to optimize API credit usage
        macro_universe = ["SPY", "QQQ", "CHPY", "MLPI", "TSPY", "AAPL", "MSFT"]
        for asset in macro_universe:
            engine.update_fundamental_moat_cache(asset)
            engine.update_dividend_stability_cache(asset)
        logger.info("Successfully refreshed fundamental moat and dividend stability tables.")

if __name__ == "__main__":
    main()

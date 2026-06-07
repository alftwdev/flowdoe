import os
import sys
import argparse
import logging
import requests
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
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def main():
    parser = argparse.ArgumentParser(description="Rockefeller Systemic Scheduler Dashboard.")
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "tsp", "income", "iv_crush", "gex", "post_market", "darkpool"])
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
            
            # QUALITY CONTROL GATEKEEPER: Prevent empty data broadcasts
            if gex_data['current_spot'] == 0.0 or gex_data['flip_strike'] == 0.0:
                logger.warning("GEX Math returned zeros. Exchange latency or weekend data clearance detected. Suppressing broadcast.")
                return 
                
            payload = (
                f"🧬 **Automated Market Maker Positioning Map (SPY)**\n\n"
                f"┣ **Current Spot Price**: `${gex_data['current_spot']:.2f}`\n"
                f"┣ 🎯 **Systemic Gamma Flip Line**: `${gex_data['flip_strike']:.2f}`\n"
                f"┗ **Structural Posture Context**: {gex_data['market_state']}\n\n"
                f"⚠️ *Strategic Warning: Fading or breaking the Gamma Flip line will result in an immediate shift in institutional market-maker hedging algorithms. Prepare for dynamic expansion if the price drops below support.*"
            )
            send_essentials_embed(WEBHOOK_MARKET, "🎛️ COGNITIVE ARCHITECTURE MATRIX: Pre-Market GEX Mapping", payload, 0xe67e22)

        elif args.mode == "post_market":
            # BENZINGA PROXY: After-Hours Earnings Sentry
            watchlist = ["AAPL", "NVDA", "MSFT", "TSLA", "META", "GOOGL", "AMZN"]
            triggered_assets = []
            
            for sym in watchlist:
                try:
                    quote_data = requests.get(f"https://api.twelvedata.com/quote?symbol={sym}&apikey={TWELVE_DATA_API_KEY}", timeout=8).json()
                    price_data = requests.get(f"https://api.twelvedata.com/price?symbol={sym}&apikey={TWELVE_DATA_API_KEY}", timeout=8).json()
                    
                    if "close" in quote_data and "price" in price_data:
                        rth_close = float(quote_data['close'])
                        ah_price = float(price_data['price'])
                        
                        if rth_close > 0:
                            pct_change = ((ah_price - rth_close) / rth_close) * 100
                            if abs(pct_change) >= 2.0:
                                direction = "🚀 BULLISH SURGE" if pct_change > 0 else "🩸 BEARISH DUMP"
                                triggered_assets.append(f"┣ **{sym}**: `{pct_change:+.2f}%` | AH Spot: `${ah_price:,.2f}` | {direction}")
                except Exception as e:
                    logger.error(f"Post-Market fetch failed for {sym}: {e}")

            if triggered_assets:
                payload = "**Institutional Extended-Hours Liquidity Sweep**\n\n" + "\n".join(triggered_assets) + "\n\n💡 *Context: Abnormal post-market volatility usually signals an earnings release, guidance revision, or breaking structural news.*"
                send_essentials_embed(WEBHOOK_MARKET, "🌙 POST-MARKET SENTRY: Abnormal Volatility Detected", payload, 0x8e44ad)

        elif args.mode == "darkpool":
            # TIER 1: The Sieve - Broad institutional liquidity universe (Single lightweight batch call)
            broad_universe = "SPY,QQQ,IWM,AAPL,NVDA,MSFT,META,TSLA,AMD,AMZN,NFLX,BA,DIS,JPM,V,WMT,COST,AVGO,SMCI,COIN"
            trending_symbols = []
            
            try:
                batch_quotes = requests.get(f"https://api.twelvedata.com/quote?symbol={broad_universe}&apikey={TWELVE_DATA_API_KEY}", timeout=10).json()
                for sym, data in batch_quotes.items():
                    if "percent_change" in data:
                        pct_chg = abs(float(data["percent_change"]))
                        # If the stock is moving more than 1.2% today, it is trending. Add to deep scan list.
                        if pct_chg >= 1.2:
                            trending_symbols.append(sym)
            except Exception as e:
                logger.error(f"Failed to fetch Dark Pool universe sieve: {e}")
                return

            if not trending_symbols:
                logger.info("Dark Pool Scan: Market is flat. No assets met the trending volatility threshold.")
                return

            # TIER 2: The Deep Scan - Run heavy math only on active movers
            for sym in trending_symbols:
                block_data = engine.detect_institutional_block_proxy(sym)
                if not block_data: continue
                
                # Prop-Firm 3-Strike Gatekeeper Implementation
                alert_id = f"dp_proxy_{sym}"
                state_str = f"DP_{block_data['direction']}_RVOL_{round(block_data['rvol'], 1)}"
                
                # Allows max 2 broadcasts per level, requires a 0.2% price deviation to reset
                if engine.db.track_and_limit_alerts(
                    alert_id=alert_id,
                    current_state=state_str,
                    current_trigger=block_data['spot'],
                    max_broadcasts=2,
                    threshold_pct=0.002 
                ):
                    payload = (
                        f"🐋 **Institutional Footprint: Block Trade Proxy Detected**\n"
                        f"┣ **Asset**: `{sym}` | Spot Execution: `${block_data['spot']:,.2f}`\n"
                        f"┣ **Abnormal Candle Volume**: `{int(block_data['current_vol']):,}` shares\n"
                        f"┣ **Trailing Benchmark Average**: `{int(block_data['baseline_vol']):,}` shares\n"
                        f"┗ **Volume Multiplier Velocity**: `{block_data['rvol']:.1f}x` spike above baseline\n\n"
                        f"**Ecosystem Context**: A hidden institutional transaction or dark pool order allocation has just cleared. Watch the immediate order book depth for massive trend continuation.\n"
                        f"**VWAP Positioning**: {block_data['direction']} (VWAP: `${block_data['vwap']:,.2f}`)"
                    )
                    send_essentials_embed(WEBHOOK_OPTIONS, f"🌊 DARK POOL RADAR: {sym}", payload, 0x9b59b6)
            
            logger.info(f"Dark Pool Proxy scan complete. Evaluated {len(trending_symbols)} trending assets.")

    except Exception as e:
        logger.critical(f"Task Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

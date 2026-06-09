import os
import sys
import argparse
import logging
import requests
from datetime import datetime
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
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_ANNOUNCEMENTS = os.getenv("WEBHOOK_ANNOUNCEMENTS") or WEBHOOK_MARKET
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def main():
    parser = argparse.ArgumentParser(description="Rockefeller Systemic Scheduler Dashboard.")
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "tsp", "income", "iv_crush", "gex", "post_market", "darkpool", "macro"])
    args = parser.parse_args()

    engine = HighFidelityAnalyticsEngine()
    logger.info(f"Executing scheduled operational sweep: {args.mode.upper()}")

    try:
        if args.mode == "macro":
            liq_payload = engine.generate_macro_liquidity_payload()
            if liq_payload and WEBHOOK_MARKET:
                send_essentials_embed(WEBHOOK_MARKET, "🏦 Institutional Liquidity Radar", liq_payload, 0x3498db)
            
            fx_payload = engine.generate_forex_matrix_payload()
            if fx_payload and WEBHOOK_FOREX:
                send_essentials_embed(WEBHOOK_FOREX, "💱 Forex Performance Grid", fx_payload, 0x34495e)
            
            crypto_payload = engine.generate_crypto_matrix_payload()
            if crypto_payload and WEBHOOK_OPTIONS:
                send_essentials_embed(WEBHOOK_OPTIONS, "🪙 Crypto Sector Liquidity Tracker", crypto_payload, 0xf39c12)
                
            logger.info("Macro matrix compilation and dispatch completed.")

        elif args.mode == "morning":
            for ticker in ["SPY", "QQQ"]:
                primer_payload = engine.generate_premarket_primer(ticker)
                if primer_payload and WEBHOOK_MARKET:
                    send_essentials_embed(WEBHOOK_MARKET, f"🌅 STRATEGIC INTELLIGENCE: {ticker} Pre-Market Primer", primer_payload, 0x00ffff)
            logger.info("Morning primers successfully compiled and dispatched.")

        elif args.mode == "eod":
            # 1. End-Of-Day Reconciliation & Audit
            for ticker in ["SPY", "QQQ"]:
                eod_payload = engine.generate_eod_reconciliation(ticker)
                if eod_payload and WEBHOOK_MARKET:
                    send_essentials_embed(WEBHOOK_MARKET, f"🏦 SYSTEMIC RECONCILIATION: {ticker} Tape Audit", eod_payload, 0x2ecc71)
            
            # 2. Daily Quant Forecast Accuracy Index Pipeline
            today_str = datetime.now().strftime("%Y-%m-%d")
            prediction_key = f"market_prediction_SPY_{today_str}"
            saved_state = engine.db.get_state(prediction_key)
            
            if saved_state:
                try:
                    predicted_target = float(saved_state)
                    price_data = engine._execute_query("price", {"symbol": "SPY"})
                    
                    if price_data and "price" in price_data:
                        actual_close = float(price_data["price"])
                        accuracy_score = engine.calculate_accuracy_rating(predicted_target, actual_close)
                        
                        acc_payload = (
                            f"🔮 **Quant Forecast Accuracy Index**\n"
                            f"┣ **Session Date**: `{today_str}`\n"
                            f"┣ **Model Predictive Accuracy**: `🎯 {accuracy_score}%`\n\n"
                            f"📊 **Session Performance Breakdown**:\n"
                            f"┣ **Algorithmic Target Projected**: `${predicted_target:,.2f}`\n"
                            f"┣ **Institutional Closing Print**: `${actual_close:,.2f}`\n"
                            f"┗ **Net Variance Delta**: `${abs(actual_close - predicted_target):,.2f}`\n\n"
                            f"*Ecosystem Performance Verification: Session calculation finalized and archived.*"
                        )
                        
                        if WEBHOOK_ANNOUNCEMENTS:
                            send_essentials_embed(WEBHOOK_ANNOUNCEMENTS, "🔮 SESSION QUANT PERFORMANCE VERIFICATION", acc_payload, 0x00ffcc)
                            logger.info("Quant Forecast Accuracy Index successfully broadcasted.")
                    else:
                        logger.warning("EOD Accuracy: Failed to fetch final closing price from Twelve Data.")
                except Exception as e:
                    logger.error(f"EOD Accuracy Calculation Error: {e}")
            else:
                logger.warning(f"EOD Accuracy: No morning prediction key found for {prediction_key}. (Server timezone rollover likely).")

            logger.info("End-of-day tape audits successfully compiled and dispatched.")

        elif args.mode == "tsp":
            tsp_payload = engine.compile_tsp_allocation_matrix()
            send_essentials_embed(WEBHOOK_TSP, "🦅 Government & Military Wealth Matrix: TSP Tactical Vector", tsp_payload, 0x3498db)

        elif args.mode == "income":
            # Premium Subscriber Upgrade: Multi-Asset Yield Tracking
            income_universe = [("SCHD", 0.72), ("JEPQ", 0.42), ("JEPI", 0.35), ("DIVO", 0.14)]
            payload_lines = [
                "🏦 **Institutional Yield & Distribution Terminal**\n",
                "📊 **EX-DIVIDEND & COVERED CALL YIELD MATRIX**"
            ]
            
            for ticker, est_div in income_universe:
                data = engine._execute_query("price", {"symbol": ticker})
                if data and "price" in data:
                    price = float(data["price"])
                    clean_yield = engine.calculate_clean_yield(ticker, est_div, price)
                    payload_lines.append(f"┣ **{ticker}**: `{clean_yield*100:.2f}%` Clean Yield | Spot: `${price:,.2f}`")
            
            payload_lines.append("┗ *System Filter: Structural capital distributions successfully separated from special payouts.*")
            
            payload = "\n".join(payload_lines)
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
            if gex_data['current_spot'] == 0.0 or gex_data['flip_strike'] == 0.0:
                logger.warning("GEX Math returned zeros. Suppressing broadcast.")
                return 
                
            payload = (
                f"🧬 **Automated Market Maker Positioning Map (SPY)**\n\n"
                f"┣ **Current Spot Price**: `${gex_data['current_spot']:.2f}`\n"
                f"┣ 🎯 **Systemic Gamma Flip Line**: `${gex_data['flip_strike']:.2f}`\n"
                f"┗ **Structural Posture Context**: {gex_data['market_state']}\n\n"
                f"⚠️ *Strategic Warning: Fading or breaking the Gamma Flip line will result in an immediate shift in institutional market-maker hedging algorithms.*"
            )
            send_essentials_embed(WEBHOOK_MARKET, "🎛️ COGNITIVE ARCHITECTURE MATRIX: Pre-Market GEX Mapping", payload, 0xe67e22)

        elif args.mode == "post_market":
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
                payload = "**Institutional Extended-Hours Liquidity Sweep**\n\n" + "\n".join(triggered_assets) + "\n\n💡 *Context: Abnormal post-market volatility usually signals an earnings release or breaking structural news.*"
                send_essentials_embed(WEBHOOK_MARKET, "🌙 POST-MARKET SENTRY: Abnormal Volatility Detected", payload, 0x8e44ad)

        elif args.mode == "darkpool":
            broad_universe = "SPY,QQQ,IWM,AAPL,NVDA,MSFT,META,TSLA,AMD,AMZN,NFLX,BA,DIS,JPM,V,WMT,COST,AVGO,SMCI,COIN"
            trending_symbols = []
            
            try:
                batch_quotes = requests.get(f"https://api.twelvedata.com/quote?symbol={broad_universe}&apikey={TWELVE_DATA_API_KEY}", timeout=10).json()
                for sym, data in batch_quotes.items():
                    if "percent_change" in data:
                        pct_chg = abs(float(data["percent_change"]))
                        if pct_chg >= 1.2:
                            trending_symbols.append(sym)
            except Exception as e:
                logger.error(f"Failed to fetch Dark Pool universe sieve: {e}")
                return

            if not trending_symbols:
                return

            for sym in trending_symbols:
                block_data = engine.detect_institutional_block_proxy(sym)
                if not block_data: continue
                
                alert_id = f"dp_proxy_{sym}"
                state_str = f"DP_{block_data['direction']}_RVOL_{round(block_data['rvol'], 1)}"
                
                if engine.db.track_and_limit_alerts(
                    alert_id=alert_id, current_state=state_str, current_trigger=block_data['spot'],
                    max_broadcasts=2, threshold_pct=0.002 
                ):
                    payload = (
                        f"🐋 **Institutional Footprint: Block Trade Proxy Detected**\n"
                        f"┣ **Asset**: `{sym}` | Spot Execution: `${block_data['spot']:,.2f}`\n"
                        f"┣ **Abnormal Candle Volume**: `{int(block_data['current_vol']):,}` shares\n"
                        f"┣ **Trailing Benchmark Average**: `{int(block_data['baseline_vol']):,}` shares\n"
                        f"┗ **Volume Multiplier Velocity**: `{block_data['rvol']:.1f}x` spike above baseline\n\n"
                        f"**Ecosystem Context**: A hidden institutional transaction or dark pool order allocation has just cleared.\n"
                        f"**VWAP Positioning**: {block_data['direction']} (VWAP: `${block_data['vwap']:,.2f}`)"
                    )
                    send_essentials_embed(WEBHOOK_OPTIONS, f"🌊 DARK POOL RADAR: {sym}", payload, 0x9b59b6)

    except Exception as e:
        logger.critical(f"Task Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

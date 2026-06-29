#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from analytics import HighFidelityAnalyticsEngine
from essentials_tools import (
    send_essentials_embed, send_essentials_embed_with_chart, generate_candlestick_chart,
    generate_line_comparison_chart, calculate_correlation, get_trend_alignment,
)

logger = logging.getLogger("Central_Scheduler")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Verified structural environmental hooks
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS")        # Primary Directional Signals Channel
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS")       # Dedicated Income Audience Channel
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO") 
WEBHOOK_TSP = os.getenv("WEBHOOK_FED")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_ANNOUNCEMENTS = os.getenv("WEBHOOK_ANNOUNCEMENTS") 
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def dispatch_conviction_sync(engine, snap, report_label):
    """
    The reverse feed: every sector channel (futures/crypto/forex/TQQQ) already pushes a
    cross-asset signal INTO Market Analysis. This closes the loop — Market Analysis pushes a
    condensed version of its synthesized conviction back OUT to each child channel, so the whole
    ecosystem starts the day reading from the same master view, not just feeding a one-way sink.
    Gated to fire once per report per day (dedup via DB), so this doesn't become a fourth alert
    stream competing with each channel's own native content.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    dedupe_key = f"conviction_sync_{report_label}_{today_str}"
    if engine.db.get_state(dedupe_key):
        return
    engine.db.update_state(dedupe_key, True)

    bias, score = snap["conviction_bias"], snap["conviction_score"]
    color = 0x2ecc71 if score >= 2 else (0xe74c3c if score <= -2 else 0x95a5a6)
    header = f"⚡ **MARKET ANALYSIS CONVICTION SYNC | {report_label.upper()}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    footer = f"┗ Master Conviction: {bias} (score {score:+d}/4)\n*Full cross-asset breakdown in Market Analysis.*"

    targets = {
        WEBHOOK_FUTURES: (
            f"{header}┣ SPY POC ${snap['futures_poc']:,.2f} | Gamma: {snap['gex']['market_state']}\n"
            f"┣ VIXY z {snap['vixy_z']:+.2f}σ | Breadth {snap['breadth']:.0%}\n{footer}"
        ),
        WEBHOOK_CRYPTO: (
            f"{header}┣ Fear & Greed: {snap['fng']['value']} ({snap['fng']['label']})\n" if snap.get("fng") else header
        ) + f"┣ Macro Risk Regime: {snap['risk_regime']}\n{footer}",
        WEBHOOK_FOREX: (
            f"{header}┣ Synthetic Dollar Index {snap['synthetic_dxy']:+.2f}%\n"
            f"┣ Risk Regime: {snap['risk_regime']} | USD/JPY {snap.get('usdjpy_chg', 0):+.2f}% | Gold {snap.get('gold_chg', 0):+.2f}%\n{footer}"
        ),
        WEBHOOK_OPTIONS: (
            f"{header}┣ Gamma: {snap['gex']['market_state']} | Flip ${snap['gex']['flip_strike']:,.2f}\n"
            f"┣ VIXY z {snap['vixy_z']:+.2f}σ\n{footer}"
        ),
        WEBHOOK_INCOME: (
            f"{header}┣ Credit Stress (HY Spread): {snap['credit_spread']:.2f}%\n"
            f"┣ Macro Risk Regime: {snap['risk_regime']}\n{footer}"
        ),
    }
    for webhook, payload in targets.items():
        if webhook:
            send_essentials_embed(webhook, f"Market Analysis Sync | {report_label}", payload, color)
    logger.info(f"Conviction sync ({report_label}) cross-dispatched to {sum(1 for w in targets if w)} channels.")


def main():
    parser = argparse.ArgumentParser(description="Rockefeller Systemic Scheduler Dashboard.")
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "tsp", "income", "iv_crush", "gex", "post_market", "darkpool", "macro", "market_intraday", "weekly_scorecard", "wheel_signals", "wheel_position"])
    parser.add_argument("--action", type=str, choices=["open", "close"], help="wheel_position mode: open or close a position")
    parser.add_argument("--symbol", type=str, help="wheel_position mode: underlying ticker")
    parser.add_argument("--type", type=str, dest="position_type", choices=["CSP", "CC"], help="wheel_position mode: CSP or CC")
    parser.add_argument("--strike", type=float, help="wheel_position mode: strike price")
    parser.add_argument("--expiration", type=str, help="wheel_position mode: YYYY-MM-DD")
    parser.add_argument("--premium", type=float, help="wheel_position mode: premium collected per contract, in dollars")
    parser.add_argument("--contracts", type=int, default=1, help="wheel_position mode: number of contracts")
    parser.add_argument("--position-id", type=int, dest="position_id", help="wheel_position mode: id to close")
    parser.add_argument("--status", type=str, default="CLOSED", choices=["CLOSED", "ASSIGNED", "EXPIRED", "ROLLED"], help="wheel_position mode: close status")
    args = parser.parse_args()

    engine = HighFidelityAnalyticsEngine()
    logger.info(f"Executing scheduled operational sweep: {args.mode.upper()}")

    try:
        if args.mode == "macro":
            liq_payload = engine.generate_macro_liquidity_payload()
            if liq_payload and WEBHOOK_MARKET:
                send_essentials_embed(WEBHOOK_MARKET, "Institutional Liquidity Radar", liq_payload, 0x3498db)
            
            fx_payload = engine.generate_forex_matrix_payload()
            if fx_payload and WEBHOOK_FOREX:
                send_essentials_embed(WEBHOOK_FOREX, "Forex Performance Grid", fx_payload, 0x34495e)

                # Refresh ATR-based expected-range bounds for the streamed pairs — this is what
                # stream.py's real-time perimeter alerts read; without this refresh those bounds
                # default to 0 and the alert silently never fires.
                for pair in engine.FX_STREAMED_PAIRS:
                    try:
                        engine.update_fx_volatility_bounds(pair)
                    except Exception as e:
                        logger.error(f"FX volatility bounds refresh failed for {pair}: {e}")

                # Mover-of-the-day deep dive: pivot levels + multi-timeframe trend confluence +
                # chart snapshot (mirrors the Finviz-style candlestick reference image), only for
                # whichever pair actually moved — avoids spamming a chart every cron tick.
                try:
                    fx_quotes = engine._fetch_twelve_data_quotes(engine.FX_UNIVERSE)
                    mover = engine.find_biggest_mover(fx_quotes, engine.FX_UNIVERSE)
                    if mover and abs(mover[3]) >= 0.35:
                        symbol, price, change, pct_change = mover
                        pivots = engine.calculate_fx_pivot_points(symbol)
                        confluence_tag, confluence_detail = engine.calculate_fx_trend_confluence(symbol)
                        ohlc = engine.fetch_crypto_ohlc(symbol, outputsize=60)
                        if ohlc is not None and not ohlc.empty:
                            chart_bytes = generate_candlestick_chart(symbol, ohlc, last_change=change, last_change_pct=pct_change)
                            pivot_block = ""
                            if pivots:
                                pivot_block = (
                                    f"┣ Pivot: `{pivots['pivot']:.4f}`\n"
                                    f"┣ R1: `{pivots['r1']:.4f}`\n"
                                    f"┣ S1: `{pivots['s1']:.4f}`\n"
                                )
                            fx_deep_dive = (
                                f"┣ Spot: `{price:,.4f}`\n"
                                f"┣ Move: `{pct_change:+.2f}%`\n"
                                f"{pivot_block}"
                                f"┣ Trend Confluence (1h/4h/1D): {confluence_tag}\n"
                                f"┗ Trajectory: [{confluence_detail}]"
                            )
                            send_essentials_embed_with_chart(
                                WEBHOOK_FOREX, f"💱 FX MOVER OF THE DAY: {symbol}", fx_deep_dive, chart_bytes, color=0x34495e
                            )
                            logger.info(f"Dispatched FX mover chart for {symbol} ({pct_change:+.2f}%)")
                except Exception as e:
                    logger.error(f"FX mover chart dispatch failed: {e}")

                # Cross-sector sync: USD/JPY (carry-trade barometer) + Gold direction gives a quick
                # risk-on/off read. When it's unambiguous, broadcast it to Market Analysis so the
                # whole ecosystem (equities, futures, crypto) is reading the same macro tape.
                try:
                    regime, explanation, usdjpy_chg, gold_chg = engine.assess_risk_sentiment_regime(fx_quotes)
                    if regime != "🟡 MIXED" and WEBHOOK_MARKET:
                        if engine.db.track_and_limit_alerts("fx_risk_regime_sync", regime, usdjpy_chg, max_broadcasts=2, threshold_pct=0.3):
                            regime_payload = (
                                f"⚡ **CROSS-ASSET CONVICTION | CARRY-TRADE RISK REGIME**\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"┣ Regime: {regime}\n"
                                f"┣ USD/JPY: `{usdjpy_chg:+.2f}%` | Gold (XAU/USD): `{gold_chg:+.2f}%`\n"
                                f"┗ {explanation}"
                            )
                            send_essentials_embed(WEBHOOK_MARKET, "FOREX → GLOBAL MACRO SIGNAL SYNC", regime_payload, 0x16a085)
                            logger.info(f"Dispatched FX risk regime sync ({regime})")

                            # Ledger: only logged when the regime is unambiguous (RISK-ON/OFF), same
                            # gate as the dispatch above — MIXED makes no claim, so nothing is logged.
                            spy_price_data = engine._execute_query("price", {"symbol": "SPY"})
                            if spy_price_data and "price" in spy_price_data:
                                direction = "UP" if regime == "🟢 RISK-ON" else "DOWN"
                                today_str = datetime.now().strftime("%Y-%m-%d")
                                engine.log_ledger_prediction(
                                    "forex", f"SPY_{today_str}", direction, float(spy_price_data["price"]),
                                    ticker="SPY", context=regime
                                )
                except Exception as e:
                    logger.error(f"FX risk regime sync failed: {e}")

            crypto_payload = engine.generate_crypto_matrix_payload()
            if crypto_payload and WEBHOOK_CRYPTO:
                # Dynamic translation matching image layout properties (Yellow Warning/Scan Bar)
                send_essentials_embed(WEBHOOK_CRYPTO, "Crypto Sector Liquidity Tracker", crypto_payload, 0xf1c40f)

                # Chart snapshot for whichever coin moved the most today (BTC/ETH/SOL/ADA/XRP/LINK/HBAR).
                # Only attach a chart on a real move — avoids spamming an image every cron tick.
                try:
                    crypto_quotes = engine._fetch_twelve_data_quotes(engine.CRYPTO_UNIVERSE)
                    mover = engine.find_biggest_crypto_mover(crypto_quotes)
                    if mover and abs(mover[3]) >= 3.0:
                        symbol, price, change, pct_change = mover
                        ohlc = engine.fetch_crypto_ohlc(symbol, outputsize=60)
                        if ohlc is not None and not ohlc.empty:
                            chart_bytes = generate_candlestick_chart(symbol, ohlc, last_change=change, last_change_pct=pct_change)
                            send_essentials_embed_with_chart(
                                WEBHOOK_CRYPTO, f"🪙 CRYPTO MOVER OF THE DAY: {symbol}",
                                f"┣ Spot: `${price:,.2f}`\n┗ 1-Day Move: `{pct_change:+.2f}%` — largest swing in the tracked universe today.",
                                chart_bytes, color=0xf39c12
                            )
                            logger.info(f"Dispatched crypto chart snapshot for {symbol} ({pct_change:+.2f}%)")
                except Exception as e:
                    logger.error(f"Crypto chart snapshot failed: {e}")

                # Cross-sector sync: BTC/USD trend alignment with SPY informs options scalpers
                # ahead of the cash open — broadcast to Market Analysis, not the crypto channel,
                # so the signal unifies with the rest of the ecosystem.
                try:
                    btc_ohlc = engine.fetch_crypto_ohlc("BTC/USD", outputsize=20)
                    spy_ohlc = engine.fetch_crypto_ohlc("SPY", outputsize=20)
                    if btc_ohlc is not None and spy_ohlc is not None and len(btc_ohlc) == len(spy_ohlc) and WEBHOOK_MARKET:
                        corr = calculate_correlation(btc_ohlc['close'].tolist(), spy_ohlc['close'].tolist())
                        btc_trend, btc_bullish = get_trend_alignment("BTC/USD", TWELVE_DATA_API_KEY)
                        spy_trend, spy_bullish = get_trend_alignment("SPY", TWELVE_DATA_API_KEY)
                        # Explicit None check — get_trend_alignment returns None (not a default
                        # direction) when a read genuinely fails. Without this guard, two
                        # independent failures would both be None and "agree" by accident,
                        # fabricating an alignment signal from a pair of missing data points.
                        if btc_bullish is not None and spy_bullish is not None and abs(corr) >= 0.6 and btc_bullish == spy_bullish:
                            if engine.db.track_and_limit_alerts("btc_spy_correlation_sync", f"ALIGN_{btc_bullish}", corr, max_broadcasts=2, threshold_pct=0.2):
                                posture = "RISK-ON ALIGNMENT" if btc_bullish else "RISK-OFF ALIGNMENT"
                                corr_payload = (
                                    f"⚡ **CROSS-ASSET CONVICTION | BTC ↔ SPY TREND SYNC**\n"
                                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                    f"┣ Correlation (20D close): `{corr:+.2f}`\n"
                                    f"┣ BTC/USD: {btc_trend}\n"
                                    f"┣ SPY: {spy_trend}\n"
                                    f"┗ Final Actionable Posture: {posture} — crypto sentiment is leaning the same way equities are pricing in. Useful pre-market context for options scalpers."
                                )
                                send_essentials_embed(WEBHOOK_MARKET, "CRYPTO → EQUITIES SIGNAL SYNC", corr_payload, 0x9b59b6)
                                logger.info(f"Dispatched BTC/SPY correlation sync ({posture})")
                except Exception as e:
                    logger.error(f"BTC/SPY correlation sync failed: {e}")

            logger.info("Macro matrix compilation and dispatch completed.")

        elif args.mode == "morning":
            # ── MARKET ANALYSIS: Single Unified Morning Brief + reverse-feed conviction sync ──
            # Folds SPY/QQQ expected-move primers directly into the one brief below — three
            # separate embeds covering the same overnight session collapsed into one report.
            try:
                morning_brief, morning_snap = engine.generate_market_analysis_morning_report()
                if morning_brief and WEBHOOK_MARKET:
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | MORNING BRIEF", morning_brief, 0x1abc9c)
                    dispatch_conviction_sync(engine, morning_snap, "morning")
            except Exception as e:
                logger.error(f"Market analysis morning brief failed: {e}")

            # ── OPTIONS CHANNEL: Pre-Market GEX + VIX Brief ──────────────
            # Gives options traders their day-start context before the open.
            try:
                gex = engine.calculate_gex_profile("SPY")
                # "VIX" 404s at this Twelve Data plan tier — VIXY proxy + its OWN z-score (relative
                # fear-spike, not an absolute level that drifts with VIXY's contango decay over time).
                vix_spot, vix_z = engine.fetch_vixy_proxy()
                spy_spot = gex.get("current_spot", 0.0)
                flip = gex.get("flip_strike", 0.0)
                gex_state = gex.get("market_state", "UNKNOWN")
                pc_ratio = gex.get("pc_oi_ratio", 1.0)
                pc_tag = gex.get("pc_tag", "N/A")

                # Determine premium environment — relative to VIXY's own recent baseline, not a
                # fixed absolute threshold (see fetch_vixy_proxy() docstring for why).
                if vix_z < -0.75:
                    premium_env = "SUPPRESSED — Low relative premium, avoid naked shorts. Prefer debit structures."
                elif vix_z < 0.75:
                    premium_env = "BALANCED — Moderate IV. Credit spreads and iron condors viable."
                else:
                    premium_env = "RICH — Elevated IV. Premium sellers have statistical edge today."

                gamma_context = (
                    "Dealers are SHORT gamma — expect accelerated moves in the direction of price."
                    if "NEGATIVE" in gex_state else
                    "Dealers are LONG gamma — expect mean-reversion and pinning behavior near key strikes."
                )

                options_brief = (
                    f"Pre-market options environment for today's session:\n\n"
                    f"┣ VIXY: `{vix_spot:.2f}` (z {vix_z:+.2f}σ) | Premium: {premium_env}\n"
                    f"┣ SPY Spot: `${spy_spot:.2f}` | GEX Flip: `${flip:.2f}`\n"
                    f"┣ Gamma Regime: {gex_state} | P/C OI: `{pc_ratio:.2f}` ({pc_tag})\n"
                    f"┗ Dealer Behavior: {gamma_context}\n\n"
                    f"Bias: {'Favor BUY setups (positive gamma suppresses downside).' if 'POSITIVE' in gex_state else 'Elevated tail risk. Size down on directional plays. Spreads preferred.'}"
                )
                if WEBHOOK_OPTIONS:
                    send_essentials_embed(WEBHOOK_OPTIONS, "OPTIONS DESK | Pre-Market Conditions Brief", options_brief, 0x00ffff)
            except Exception as e:
                logger.error(f"Morning options brief failed: {e}")

            logger.info("Morning primers successfully compiled and dispatched.")

        elif args.mode == "market_intraday":
            # Mid-day check-in: is today tracking the morning call, or has the tape diverged?
            # No new cron slot exists for this yet — add one around 12:00-13:00 ET to PythonAnywhere's
            # scheduled tasks: `python3.10 /home/alftw/scripts/scheduler.py --mode market_intraday`
            try:
                intraday_brief = engine.generate_market_analysis_intraday_report()
                if intraday_brief and WEBHOOK_MARKET:
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | INTRADAY PULSE", intraday_brief, 0xf1c40f)
                    logger.info("Intraday pulse dispatched.")
            except Exception as e:
                logger.error(f"Market analysis intraday report failed: {e}")

        elif args.mode == "weekly_scorecard":
            # Cross-sector accuracy proof point — public to Announcements (the "bait"), full
            # depth also mirrored to Market Analysis for subscribers. New cron slot needed, once
            # weekly (e.g. Friday EOD): `python3.10 /home/alftw/scripts/scheduler.py --mode weekly_scorecard`
            try:
                scorecard = engine.generate_ecosystem_scorecard()
                if scorecard:
                    if WEBHOOK_ANNOUNCEMENTS:
                        send_essentials_embed(WEBHOOK_ANNOUNCEMENTS, "ECOSYSTEM WEEKLY SCORECARD", scorecard, 0x00ffcc)
                    if WEBHOOK_MARKET:
                        send_essentials_embed(WEBHOOK_MARKET, "ECOSYSTEM WEEKLY SCORECARD", scorecard, 0x00ffcc)
                    logger.info("Weekly ecosystem scorecard dispatched.")
            except Exception as e:
                logger.error(f"Weekly scorecard generation failed: {e}")

        elif args.mode == "eod":
            try:
                graded = engine.sweep_and_grade_pending("forex", min_age_days=1)
                if graded:
                    logger.info(f"Forex ledger: graded {graded} pending risk-regime call(s).")
            except Exception as e:
                logger.error(f"Forex ledger sweep failed: {e}")

            # ── MARKET ANALYSIS: Single Unified EOD Recap + reverse-feed conviction sync ──
            # Folds SPY/QQQ tape audits and the VIX CVR reversal signal directly into the one
            # recap below — four separate end-of-day embeds collapsed into one report.
            eod_snap = None
            try:
                eod_brief, eod_snap = engine.generate_market_analysis_eod_report()
                if eod_brief and WEBHOOK_MARKET:
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | END-OF-DAY RECAP", eod_brief, 0x2c3e50)
                    dispatch_conviction_sync(engine, eod_snap, "eod")
            except Exception as e:
                logger.error(f"Market analysis EOD recap failed: {e}")

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

                        # Public, non-paywalled "bait" channel — proves the math works without
                        # giving away the full depth. Includes a real cross-asset stat (crypto
                        # mover, Fear & Greed) alongside the accuracy number for extra pull.
                        if eod_snap is not None:
                            acc_payload = engine.generate_announcements_teaser(accuracy_score, predicted_target, actual_close, eod_snap)
                        else:
                            acc_payload = (
                                f"Quant Forecast Accuracy Index\n"
                                f"┣ Session Date: `{today_str}`\n"
                                f"┣ Model Predictive Accuracy: `{accuracy_score}%`\n\n"
                                f"Session Performance Breakdown:\n"
                                f"┣ Algorithmic Target Projected: `${predicted_target:,.2f}`\n"
                                f"┣ Institutional Closing Print: `${actual_close:,.2f}`\n"
                                f"┗ Net Variance Delta: `${abs(actual_close - predicted_target):,.2f}`\n\n"
                                f"*Ecosystem Performance Verification: Session calculation finalized and archived.*"
                            )

                        if WEBHOOK_ANNOUNCEMENTS:
                            send_essentials_embed(WEBHOOK_ANNOUNCEMENTS, "SESSION QUANT PERFORMANCE VERIFICATION", acc_payload, 0x00ffcc)
                    else:
                        logger.warning("EOD Accuracy: Failed to fetch final closing price from Twelve Data.")
                except Exception as e:
                    logger.error(f"EOD Accuracy Calculation Error: {e}")

            logger.info("End-of-day tape audit successfully compiled and dispatched.")

        elif args.mode == "tsp":
            tsp_payload, fund_series = engine.generate_tsp_eod_report()
            if tsp_payload and WEBHOOK_TSP:
                try:
                    if fund_series:
                        chart_bytes = generate_line_comparison_chart(
                            fund_series, "TSP Individual Funds | 90-Day Relative Performance (Rebased to 100)"
                        )
                        send_essentials_embed_with_chart(
                            WEBHOOK_TSP, "Government & Military Wealth Matrix: TSP Tactical Vector",
                            tsp_payload, chart_bytes, color=0x3498db
                        )
                    else:
                        send_essentials_embed(WEBHOOK_TSP, "Government & Military Wealth Matrix: TSP Tactical Vector", tsp_payload, 0x3498db)
                except Exception as e:
                    logger.error(f"TSP chart dispatch failed, falling back to text-only: {e}")
                    send_essentials_embed(WEBHOOK_TSP, "Government & Military Wealth Matrix: TSP Tactical Vector", tsp_payload, 0x3498db)
                logger.info("TSP End-of-Day report dispatched with official tsp.gov data.")
            else:
                logger.info("TSP report skipped — already reported today's official close, or data unavailable.")

        elif args.mode == "income":
            logger.info("Executing Income Channel: CC ETF Pulse + Wheel Candidates + Ex-Div Radar...")

            # ── SEGMENT 1: CC ETF & INCOME FUND PULSE ─────────────────────────
            # Covers: JEPI, JEPQ, DIVO, XYLD, QYLD, RYLD, SCHD, O, MAIN, ARCC
            # Surfaces: next ex-date, annualized yield, urgency tags, moat rating
            try:
                etf_data = engine.generate_income_etf_pulse()
                if etf_data:
                    etf_payload = "Monthly & Weekly Income Fund Tracker — Ex-Dividend Urgency Board\n\n"
                    for item in etf_data:
                        etf_payload += (
                            f"**{item['symbol']}** {item['moat']} | {item['type']} | {item['freq']}\n"
                            f"┣ Spot: `${item['spot']:.2f}` | Div: `${item['div_amount']:.4f}` | Yield: `{item['ann_yield']:.1f}%` ann.\n"
                            f"┣ Ex-Date: `{item['ex_date']}` ({item['days_away']} days)\n"
                            f"┗ Status: {item['urgency']}\n\n"
                        )
                    etf_payload += (
                        "✅ = Wide-moat, institutionally vetted | ⚠️ = Yield chase risk — verify payout sustainability\n"
                        "Directive: Deploy capital before ex-date. Position must settle (T+1) to capture distribution."
                    )
                    state_key = f"ETFPULSE_{len(etf_data)}_{'_'.join([e['symbol'] for e in etf_data[:3]])}"
                    if engine.db.track_and_limit_alerts("income_etf_pulse", state_key, float(len(etf_data)), max_broadcasts=3, threshold_pct=0.1):
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "INCOME ETF PULSE | CC ETF & Dividend Fund Radar", etf_payload, 0x1abc9c)
                            logger.info(f"CC ETF pulse dispatched: {len(etf_data)} funds tracked.")
            except Exception as e:
                logger.error(f"Income ETF pulse segment failed: {e}")

            # ── SEGMENT 2: DIVIDEND WHEEL CANDIDATES v2 ───────────────────────
            # Enhanced scanner: RSI-14, Bollinger %B, IVR proxy, theta, break-even,
            # Finnhub safety grade, 3% capital sizing. Returns top 5.
            try:
                wheel_candidates = engine.generate_dividend_wheel_candidates()
                if wheel_candidates:
                    composite_trigger = sum(c['strike'] for c in wheel_candidates)
                    state_str = "_".join(f"{c['symbol']}{c['strike']}" for c in wheel_candidates)

                    if engine.db.track_and_limit_alerts(
                        alert_id="dividend_wheel_v2_daily",
                        current_state=state_str,
                        current_trigger=composite_trigger,
                        max_broadcasts=3,
                        threshold_pct=0.01
                    ):
                        wheel_payload = "Cash-Secured Put Setups on Dividend Stocks — Institutional-Grade Screen\n\n"
                        avg_pop = 0.0

                        for c in wheel_candidates:
                            div_growth_tag = ""
                            if c.get("div_growth_5y") is not None:
                                div_growth_tag = f" | 5yr Div Growth: `{c['div_growth_5y']:.1f}%`"
                            payout_tag = ""
                            if c.get("payout_ratio") is not None:
                                payout_tag = f" | Payout Ratio: `{c['payout_ratio']:.0f}%`"
                            div_line = ""
                            if c.get("div_yield") is not None:
                                div_line = f"┣ Dividend: Yield `{c['div_yield']:.1f}%` | {c['div_freq']} | Amount `${c['div_amount']:.4f}`/share\n"

                            wheel_payload += (
                                f"**{c['symbol']}** | Spot: `${c['spot']:.2f}` | {c['trend']} {c['sma50_tag']}\n"
                                f"┣ Strategy: {c['strategy']}\n"
                                f"┣ RSI-14: `{c['rsi14']}` {c['rsi_tag']} | BB Zone: {c['bb_zone']}\n"
                                f"┣ Setup: `STO ${c['strike']:.1f} Put` | Exp: `{c['expiration']}` ({c['dte']} DTE)\n"
                                f"┣ Greeks: Δ `{c['delta']:.2f}` | θ ~`${c['theta_daily']:.3f}`/day | IV `{c['iv']:.1f}%` | IVR `{c['ivr_proxy']:.0f}%` ({c['ivr_tag']})\n"
                                f"┣ Liquidity: Volume `{c['volume']:,}` | OI Range `{c['oi_low']:,}`–`{c['oi_high']:,}`\n"
                                f"┣ Premium: `${c['premium']*100:.0f}/contract` | PoP: `{c['pop']:.1f}%` | Ann. ROI: `{c['annualized_roi']:.1f}%`\n"
                                f"┣ Break-Even: `${c['break_even']:.2f}` | Downside Protected: `{c['pct_downside']:.1f}%`\n"
                                f"{div_line}"
                                f"┣ Sizing (3% rule): `{c['contracts_10k']}x @ $10k` | `{c['contracts_25k']}x @ $25k`\n"
                                f"┗ Div Safety: {c['safety_grade']}{payout_tag}{div_growth_tag}\n\n"
                            )
                            avg_pop += c['pop']

                        avg_pop = avg_pop / len(wheel_candidates)
                        setup_color = 0x2ecc71 if avg_pop >= 75.0 else 0xf1c40f

                        wheel_payload += (
                            "Wheel Strategy Path: CSP → Assignment → Covered Call → Repeat\n"
                            "Capital Rule: 3% max per position. Scale contracts to account size."
                        )

                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "DIVIDEND WHEEL v2 | Premium Selling Setups", wheel_payload, setup_color)
                            logger.info(f"Wheel candidates dispatched: {len(wheel_candidates)} setups, avg PoP {avg_pop:.1f}%.")
                    else:
                        logger.info("Dividend Wheel v2 blocked by gatekeeper — state unchanged.")
                else:
                    logger.info("No wheel candidates passed filters this session.")
            except Exception as e:
                logger.error(f"Dividend wheel v2 segment failed: {e}")

            # ── SEGMENT 3: EX-DIVIDEND RADAR ──────────────────────────────────
            # 14-day countdown for the broader dividend universe (10 tickers).
            # Separate from ETF pulse — covers individual stocks too.
            try:
                ex_div_data = engine.generate_ex_dividend_radar()
                if ex_div_data:
                    ex_payload = "Targeted Capital Deployment Timeline — Next 14 Days\n\n"
                    composite_ex_trigger = 0.0
                    for item in ex_div_data:
                        if item['days_away'] <= 1:
                            urgency_tag = "🔥 **TOMORROW — LAST CHANCE**"
                        elif item['days_away'] <= 3:
                            urgency_tag = "⚡ **IMMINENT**"
                        elif item['days_away'] <= 7:
                            urgency_tag = "📅 **THIS WEEK**"
                        else:
                            urgency_tag = f"🔍 **In {item['days_away']} Days**"

                        ex_payload += (
                            f"**{item['symbol']}** | {urgency_tag}\n"
                            f"┣ Ex-Dividend Date: `{item['ex_date']}`\n"
                            f"┗ Declared Payout: `${item['amount']:,.4f}` per share\n\n"
                        )
                        composite_ex_trigger += item['amount']

                    ex_payload += (
                        "Directive: Position must be held at market open on ex-date. "
                        "Settlement is T+1 — buy no later than the day before ex-date."
                    )

                    if engine.db.track_and_limit_alerts(
                        "ex_dividend_radar_weekly",
                        f"EX_DIV_{len(ex_div_data)}_AMT_{round(composite_ex_trigger, 2)}",
                        composite_ex_trigger,
                        max_broadcasts=2,
                        threshold_pct=0.05
                    ):
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "EX-DIVIDEND RADAR | Yield Capture Countdown", ex_payload, 0xf1c40f)
                            logger.info(f"Ex-dividend radar dispatched: {len(ex_div_data)} upcoming events.")
            except Exception as e:
                logger.error(f"Ex-dividend radar segment failed: {e}")

            # ── SEGMENT 4: NEW/TRENDING CC ETF SCREENER (Module 3) ─────────────
            # YieldMax / Roundhill / NEOS / TappAlpha discovery feed — surfaces names worth
            # adding to the wheel universe. Yield, age, and AUM filters all pull real data;
            # nothing here is a static watchlist.
            try:
                new_etfs = engine.generate_new_income_etf_screener()
                if new_etfs:
                    state_str = "_".join(f"{e['symbol']}{e['ann_yield']}" for e in new_etfs[:8])
                    if engine.db.track_and_limit_alerts(
                        "new_income_etf_screener_daily", state_str,
                        sum(e['ann_yield'] for e in new_etfs), max_broadcasts=2, threshold_pct=0.05
                    ):
                        new_payload = "Trending Weekly/Monthly Income ETF Discovery — YieldMax / Roundhill / NEOS / TappAlpha\n\n"
                        for e in new_etfs[:8]:
                            new_payload += (
                                f"**{e['symbol']}** | {e['family']} | {e['freq']}\n"
                                f"┣ Spot: `${e['spot']:.2f}` | Yield: `{e['ann_yield']:.1f}%` ann. | AUM: `{e['aum']}`\n"
                                f"┣ Trading History: `{e['trading_days']}` sessions\n"
                                f"┗ Next Est. Pay Date: `{e['next_ex_date']}`\n\n"
                            )
                        new_payload += (
                            "Filters: yield > 10% (real div history) | monthly/weekly pay | "
                            "> 6mo trading history | AUM > $50M (where Twelve Data reports it)\n"
                            "Directive: Research-stage only — confirm distribution sustainability before adding to wheel universe."
                        )
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "NEW INCOME ETF RADAR | Trending CC ETF Discovery", new_payload, 0x9b59b6)
                            logger.info(f"New income ETF screener dispatched: {len(new_etfs)} candidates.")
            except Exception as e:
                logger.error(f"New income ETF screener segment failed: {e}")

        elif args.mode == "wheel_signals":
            # Both modules dispatch to WEBHOOK_INCOME (#dividend-ccetfs), not WEBHOOK_TRADE_SIGNALS
            # — wheeling these Tier 2 holdings (MAIN/MLPI/GPIQ/KQQQ/TDAQ) for long-term income is
            # income-channel content, per explicit operator direction.
            logger.info("Executing Wheel Signals: Tier 2 IV Rank Screener + Position DTE Countdown...")

            # ── MODULE 1: TIER 2 IV RANK SCREENER ──────────────────────────────
            try:
                flagged = engine.generate_tier2_iv_rank_alerts()
                if flagged:
                    state_str = "_".join(f"{f['symbol']}{f['ivr_proxy']}" for f in flagged)
                    if engine.db.track_and_limit_alerts(
                        "tier2_iv_rank_screener", state_str,
                        sum(f['ivr_proxy'] for f in flagged), max_broadcasts=3, threshold_pct=0.05
                    ):
                        ivr_payload = "Tier 2 Wheel Underlyings — Elevated IV Rank Detected\n\n"
                        for f in flagged:
                            setup_line = ""
                            csp = f.get("csp_setup")
                            if csp:
                                setup_line = (
                                    f"┣ Strategy: CSP (Cash-Secured Put)\n"
                                    f"┣ Setup: `STO ${csp['strike']:.1f} Put` | Exp: `{csp['expiration']}` ({csp['dte']} DTE) | Δ `{csp['delta']:.2f}`\n"
                                    f"┣ Premium: `${csp['premium']*100:.0f}/contract` | Volume: `{csp['volume']:,}` | OI Range `{csp['oi_low']:,}`–`{csp['oi_high']:,}`\n"
                                )
                            div_line = ""
                            if f.get("div_yield") is not None:
                                div_line = f"┣ Dividend: Yield `{f['div_yield']:.1f}%` | {f['div_freq']} | Amount `${f['div_amount']:.4f}`/share\n"
                            ivr_payload += (
                                f"**{f['symbol']}** | Spot: `${f['spot']:.2f}`\n"
                                f"┣ IV: `{f['iv']:.1f}%` | HV30: `{f['hv30']:.1f}%` | IVR Proxy: `{f['ivr_proxy']:.0f}%`\n"
                                f"{setup_line}"
                                f"{div_line}"
                                f"┗ Spread Check: `{f['spread_pct']:.1f}%` of mid | Earnings Window: Clear\n\n"
                            )
                        ivr_payload += "Directive: Premium-selling environment is favorable — screen for CSP entries on these names."
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "IV RANK ALERT | Tier 2 Wheel Screener", ivr_payload, 0xe67e22)
                            logger.info(f"Tier 2 IV Rank alert dispatched: {len(flagged)} symbol(s) > 35% IVR.")
            except Exception as e:
                logger.error(f"Tier 2 IV Rank screener failed: {e}")

            # ── MODULE 2: WHEEL POSITION DTE COUNTDOWN ─────────────────────────
            try:
                open_positions = engine.db.get_open_wheel_positions()
                today = datetime.now().date()
                for pos in open_positions:
                    exp_date = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if dte < 0:
                        continue
                    alert_dte = None
                    if dte <= 14 and pos.get("last_alert_dte") != 14 and (pos.get("last_alert_dte") is None or pos["last_alert_dte"] > 14):
                        alert_dte = 14
                        urgency = "🔴 CLOSE/ROLL DEADLINE"
                    elif dte <= 21 and pos.get("last_alert_dte") is None:
                        alert_dte = 21
                        urgency = "🟡 ROLL DECISION WINDOW"
                    if alert_dte is not None:
                        dte_payload = (
                            f"**{pos['symbol']}** | {pos['position_type']} @ `${pos['strike']:.2f}`\n"
                            f"┣ Expiration: `{pos['expiration']}` ({dte} DTE)\n"
                            f"┣ Premium Collected: `${pos['premium_collected']:.2f}` x {pos['contracts']}\n"
                            f"┗ {urgency}"
                        )
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "WHEEL POSITION | DTE Countdown", dte_payload, 0xf1c40f if alert_dte == 21 else 0xe74c3c)
                            engine.db.mark_wheel_position_alerted(pos["id"], alert_dte)
                            logger.info(f"Wheel DTE alert dispatched: {pos['symbol']} at {alert_dte} DTE.")
            except Exception as e:
                logger.error(f"Wheel position DTE countdown failed: {e}")

        elif args.mode == "wheel_position":
            if args.action == "open":
                if not all([args.symbol, args.position_type, args.strike, args.expiration, args.premium]):
                    logger.error("wheel_position open requires --symbol --type --strike --expiration --premium")
                else:
                    pos_id = engine.db.open_wheel_position(
                        args.symbol.upper(), args.position_type, args.strike,
                        args.expiration, args.premium, args.contracts
                    )
                    logger.info(f"Opened wheel position #{pos_id}: {args.symbol.upper()} {args.position_type} ${args.strike} exp {args.expiration}")
            elif args.action == "close":
                if not args.position_id:
                    logger.error("wheel_position close requires --position-id")
                else:
                    ok = engine.db.close_wheel_position(args.position_id, status=args.status)
                    if ok:
                        logger.info(f"Closed wheel position #{args.position_id} as {args.status}. "
                                    f"Total premium ledger now: ${engine.db.get_total_premium_collected():,.2f}")
                    else:
                        logger.error(f"Could not close position #{args.position_id} — not found or not OPEN.")
            else:
                logger.error("wheel_position mode requires --action open|close")

        elif args.mode == "iv_crush":
            iv_dispatched = False
            flow_dispatched = False

            # ── SEGMENT 1: IV CRUSH SCANNER (expanded universe: 15 tickers) ──
            scan_data = engine.run_iv_crush_scan()
            if scan_data:
                payload = "Systemic IV Overpricing & Volatility Crush Report\n\n"
                for asset in scan_data:
                    edge_tag = "EXTREME EDGE" if asset['spread'] >= 20 else ("STRONG EDGE" if asset['spread'] >= 12 else "MODERATE EDGE")
                    payload += (
                        f"**{asset['symbol']}** | {edge_tag}\n"
                        f"┣ 30D Historical Volatility (HV30): `{asset['hv']}%`\n"
                        f"┣ Front-Month Implied Volatility (IV): `{asset['iv']}%`\n"
                        f"┗ Premium Edge Spread: `{asset['spread']:+.1f}%` vol variance\n"
                        f"Edge: Selling credit (spreads, iron condors, covered calls) statistically favored.\n\n"
                    )
                send_essentials_embed(WEBHOOK_OPTIONS, "VOLATILITY ARBITRAGE TERMINAL | IV Crush Scanner", payload, 0xf1c40f)
                iv_dispatched = True
                logger.info(f"IV crush scan dispatched: {len(scan_data)} elevated-premium assets.")

            # ── SEGMENT 2: UNUSUAL FLOW SCANNER (Cheddar Flow / UW replacement) ──
            flow_data = engine.scan_unusual_options_flow()
            if flow_data:
                flow_payload = "Institutional Sweep & OI Positioning Intelligence\n\n"
                for signal in flow_data:
                    if signal["type"] == "SWEEP":
                        flow_payload += (
                            f"**{signal['symbol']}** | {signal['direction']} SWEEP — {signal['conviction']} CONVICTION\n"
                            f"┣ Strike: `${signal['strike']:.0f}` | Expiry: `{signal['expiration']}` ({signal['dte']} DTE)\n"
                            f"┣ Volume: `{signal['volume']:,}` contracts | OI: `{signal['open_interest']:,}`\n"
                            f"┣ Vol:OI Ratio: `{signal['vol_oi_ratio']:.1f}x` (threshold: 2.0x = sweep)\n"
                            f"┗ IV: `{signal['iv']:.1f}%` | Fresh directional positioning detected\n\n"
                        )
                    else:  # OI_SKEW
                        flow_payload += (
                            f"**{signal['symbol']}** | OI SKEW — {signal['direction']}\n"
                            f"┣ Put:Call OI Ratio: `{signal['vol_oi_ratio']:.2f}`\n"
                            f"┗ Total Open Interest: `{signal['open_interest']:,}` contracts across chain\n\n"
                        )
                flow_payload += (
                    "Methodology: Sweeps (Vol:OI > 2x) signal fresh institutional conviction — "
                    "they are buying direction, not just hedging. OI skew reveals macro positioning bias."
                )
                alert_id = "unusual_flow_scan"
                state_str = "_".join([f"{s['symbol']}{s['direction'][:3]}" for s in flow_data[:3]])
                if engine.db.track_and_limit_alerts(alert_id, state_str, float(len(flow_data)), max_broadcasts=3, threshold_pct=0.5):
                    send_essentials_embed(WEBHOOK_OPTIONS, "INSTITUTIONAL FLOW RADAR | Sweep & OI Intelligence", flow_payload, 0x9b59b6)
                    flow_dispatched = True
                    logger.info(f"Unusual flow dispatch: {len(flow_data)} signals found.")

            # ── FALLBACK: Market Conditions Snapshot ─────────────────────────
            # Breaks channel silence when no IV crush or flow signals exist.
            # Provides meaningful context even on quiet days.
            if not iv_dispatched and not flow_dispatched:
                gex = engine.calculate_gex_profile("SPY")
                # "VIX" 404s at this Twelve Data plan tier — VIXY proxy + its OWN z-score.
                vix_spot, vix_z = engine.fetch_vixy_proxy()

                if vix_z < -0.75:
                    vix_env = "LOW RELATIVE VOLATILITY"
                    vix_detail = "Premium sellers in a drought. Prefer debit spreads or condors."
                elif vix_z < 0.75:
                    vix_env = "MODERATE VOLATILITY"
                    vix_detail = "Balanced premium. Credit spreads statistically favorable."
                else:
                    vix_env = "ELEVATED VOLATILITY"
                    vix_detail = "Rich premium. Ideal for iron condors & covered calls."

                gex_state = gex.get("market_state", "UNKNOWN")
                flip = gex.get("flip_strike", 0.0)

                outlook_payload = (
                    f"┣ VIXY: `{vix_spot:.2f}` (z {vix_z:+.2f}σ)\n"
                    f"┣ Regime: {vix_env} — {vix_detail}\n"
                    f"┣ Whale Flow: Normal — no IV crush or unusual flow signals detected this session\n"
                    f"┣ SPY Gamma Posture: {gex_state}\n"
                    f"┣ GEX Flip Level: `${flip:.2f}` (dealer hedging pivot)\n"
                    f"┗ Directive: {'Wait for a volatility expansion for optimal credit premium.' if vix_z < 0 else 'Premium environment is active. Screen for setups on earnings or macro events.'}\n\n"
                    f"Context: When both IV and flow are quiet, capital preservation > new entries. "
                    f"Watch for a volatility spike or unusual volume tomorrow morning."
                )
                send_essentials_embed(WEBHOOK_OPTIONS, "Options Market Flowstate", outlook_payload, 0x3498db)
                logger.info("Options fallback market conditions snapshot dispatched.")

        elif args.mode == "gex":
            gex_data = engine.calculate_gex_profile("SPY")
            if gex_data['current_spot'] == 0.0 or gex_data['flip_strike'] == 0.0:
                logger.warning("GEX Math returned zeros. Suppressing broadcast.")
                return 
                
            payload = (
                f"Automated Market Maker Positioning Map (SPY)\n\n"
                f"┣ Current Spot Price: `${gex_data['current_spot']:.2f}`\n"
                f"┣ Systemic Gamma Flip Line: `${gex_data['flip_strike']:.2f}`\n"
                f"┗ Structural Posture Context: {gex_data['market_state']}\n\n"
                f"Strategic Warning: Fading or breaking the Gamma Flip line will result in an immediate shift in institutional market-maker hedging algorithms."
            )
            # Dynamic look for market state: Red for Negative Gamma environments, Green for stable Positive Gamma environments
            gex_color = 0x2ecc71 if "POSITIVE" in gex_data['market_state'].upper() else 0xe74c3c
            send_essentials_embed(WEBHOOK_MARKET, "COGNITIVE ARCHITECTURE MATRIX: Pre-Market GEX Mapping", payload, gex_color)

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
                                direction = "BULLISH SURGE" if pct_change > 0 else "BEARISH DUMP"
                                triggered_assets.append(f"┣ {sym}: `{pct_change:+.2f}%` | AH Spot: `${ah_price:,.2f}` | {direction}")
                except Exception as e:
                    logger.error(f"Post-Market fetch failed for {sym}: {e}")

            if triggered_assets:
                payload = "Institutional Extended-Hours Liquidity Sweep\n\n" + "\n".join(triggered_assets) + "\n\nContext: Abnormal post-market volatility usually signals an earnings release or breaking structural news."
                send_essentials_embed(WEBHOOK_MARKET, "POST-MARKET SENTRY: Abnormal Volatility Detected", payload, 0xe74c3c)

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
                        f"Institutional Footprint: Block Trade Proxy Detected\n"
                        f"┣ Asset: `{sym}` | Spot Execution: `${block_data['spot']:,.2f}`\n"
                        f"┣ Abnormal Candle Volume: `{int(block_data['current_vol']):,}` shares\n"
                        f"┣ Trailing Benchmark Average: `{int(block_data['baseline_vol']):,}` shares\n"
                        f"┗ Volume Multiplier Velocity: `{block_data['rvol']:.1f}x` spike above baseline\n\n"
                        f"Ecosystem Context: A hidden institutional transaction or dark pool order allocation has just cleared.\n"
                        f"VWAP Positioning: {block_data['direction']} (VWAP: `${block_data['vwap']:,.2f}`)"
                    )
                    # Assign dynamic color depending on execution bias direction
                    dp_color = 0x2ecc71 if "BULLISH" in block_data['direction'].upper() else 0xe74c3c
                    send_essentials_embed(WEBHOOK_OPTIONS, f"DARK POOL RADAR: {sym}", payload, dp_color)

    except Exception as e:
        logger.critical(f"Task Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

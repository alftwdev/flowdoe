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
WEBHOOK_FED = os.getenv("WEBHOOK_FED")      # reserved for fed.py (not yet built)
WEBHOOK_ANNOUNCEMENTS = os.getenv("WEBHOOK_ANNOUNCEMENTS") 
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def dispatch_conviction_sync(engine, snap, report_label):
    """
    The reverse feed: every sector channel (futures/crypto/TQQQ) already pushes a
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
        # WEBHOOK_FUTURES intentionally omitted — futures trade ~23h/day; an
        # "EOD" equity-session conviction sync landing mid-futures-session is
        # contextually wrong and adds noise to a channel with its own cadence.
        WEBHOOK_CRYPTO: (
            f"{header}┣ Fear & Greed: {snap['fng']['value']} ({snap['fng']['label']})\n" if snap.get("fng") else header
        ) + f"┣ Macro Risk Regime: {snap['risk_regime']}\n{footer}",
        WEBHOOK_OPTIONS: (
            f"{header}┣ VIXY z {snap['vixy_z']:+.2f}σ | Breadth {snap['breadth']:.0%}\n"
            f"┣ Conviction: {snap['conviction_bias']} ({snap['conviction_score']:+d}/4)\n{footer}"
        ),
        # WEBHOOK_INCOME intentionally omitted — income channel is a dedicated
        # dividend/wheel audience; cross-posting the macro conviction sync there
        # is noise for that subscriber segment (confirmed by operator review).
    }
    for webhook, payload in targets.items():
        if webhook:
            send_essentials_embed(webhook, f"Market Analysis Sync | {report_label}", payload, color)
    logger.info(f"Conviction sync ({report_label}) cross-dispatched to {sum(1 for w in targets if w)} channels.")


def main():
    parser = argparse.ArgumentParser(description="Rockefeller Systemic Scheduler Dashboard.")
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "income", "iv_crush", "gex", "post_market", "options_flow", "macro", "market_intraday", "weekly_scorecard", "wheel_signals", "wheel_position", "trending_plays", "crypto_social", "futures_social", "spx_income", "store_daily_iv", "cef_calibrate"])
    parser.add_argument("--action", type=str, choices=["open", "close"], help="wheel_position mode: open or close a position")
    parser.add_argument("--symbol", type=str, help="wheel_position mode: underlying ticker")
    parser.add_argument("--type", type=str, dest="position_type", choices=["CSP", "CC"], help="wheel_position mode: CSP or CC")
    parser.add_argument("--strike", type=float, help="wheel_position mode: strike price")
    parser.add_argument("--expiration", type=str, help="wheel_position mode: YYYY-MM-DD")
    parser.add_argument("--premium", type=float, help="wheel_position mode: premium collected per contract, in dollars")
    parser.add_argument("--contracts", type=int, default=1, help="wheel_position mode: number of contracts")
    parser.add_argument("--position-id", type=int, dest="position_id", help="wheel_position mode: id to close")
    parser.add_argument("--status", type=str, default="CLOSED", choices=["CLOSED", "ASSIGNED", "EXPIRED", "ROLLED"], help="wheel_position mode: close status")
    parser.add_argument("--cost-basis", type=float, dest="cost_basis", help="wheel_position open: per-share cost basis (defaults to strike)")
    parser.add_argument("--open-fees", type=float, dest="open_fees", default=0.0, help="wheel_position open: total commission paid to open, in dollars (e.g. 1.30)")
    parser.add_argument("--close-fees", type=float, dest="close_fees", default=0.0, help="wheel_position close: total commission paid to close, in dollars")
    parser.add_argument("--close-price", type=float, dest="close_price", help="wheel_position close --status CLOSED: per-share BTC price (e.g. 0.45 if you bought back at $0.45)")
    parser.add_argument("--roll-group", type=str, dest="roll_group_id", help="wheel_position open: shared UUID to link all legs of a roll chain (generate once with: python -c \"import uuid; print(uuid.uuid4())\")")
    args = parser.parse_args()

    engine = HighFidelityAnalyticsEngine()
    logger.info(f"Executing scheduled operational sweep: {args.mode.upper()}")

    try:
        if args.mode == "macro":
            liq_payload = engine.generate_macro_liquidity_payload()
            if liq_payload and WEBHOOK_MARKET:
                send_essentials_embed(WEBHOOK_MARKET, "Credit & Liquidity Check", liq_payload, 0x3498db)

            # Cross-sector carry-trade regime: USD/JPY + Gold gives a clean risk-on/off read.
            # Dispatches to #market-analysis only when unambiguous (not MIXED) — no forex channel.
            try:
                fx_quotes = engine._fetch_twelve_data_quotes(["USD/JPY", "XAU/USD"])
                regime, explanation, usdjpy_chg, gold_chg = engine.assess_risk_sentiment_regime(fx_quotes)
                if regime != "🟡 MIXED" and WEBHOOK_MARKET:
                    if engine.db.track_and_limit_alerts("fx_risk_regime_sync", regime, usdjpy_chg, max_broadcasts=2, threshold_pct=0.3):
                        regime_payload = (
                            f"┣ Regime: {regime}\n"
                            f"┣ USD/JPY: `{usdjpy_chg:+.2f}%` | Gold (XAU/USD): `{gold_chg:+.2f}%`\n"
                            f"┗ {explanation}"
                        )
                        send_essentials_embed(WEBHOOK_MARKET, "Carry Trade Risk Regime", regime_payload, 0x16a085)
                        logger.info(f"Dispatched carry-trade regime sync ({regime})")

                        spy_price_data = engine._execute_query("price", {"symbol": "SPY"})
                        if spy_price_data and "price" in spy_price_data:
                            direction = "UP" if regime == "🟢 RISK-ON" else "DOWN"
                            today_str = datetime.now().strftime("%Y-%m-%d")
                            engine.log_ledger_prediction(
                                "forex", f"SPY_{today_str}", direction, float(spy_price_data["price"]),
                                ticker="SPY", context=regime
                            )
            except Exception as e:
                logger.error(f"Carry-trade regime sync failed: {e}")

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

            # ── FRED: Yield Curve + Macro Snapshot ──────────────────────────
            try:
                yc = engine.fetch_yield_curve()
                fred_snap = engine.fetch_fred_macro_snapshot()
                real_vix = engine.fetch_real_vix()
                if (yc or fred_snap) and WEBHOOK_MARKET:
                    yc_line = (
                        f"┣ Yield Curve (10Y-2Y): `{yc['spread']:+.3f}%` — {yc['label']}\n"
                        f"┣ 10Y: `{yc['t10']:.3f}%` | 2Y: `{yc['t2']:.3f}%`\n"
                        if yc else ""
                    )
                    vix_line = f"┣ VIX (FRED VIXCLS): `{real_vix:.2f}`\n" if real_vix else ""
                    macro_lines = ""
                    if fred_snap:
                        cpi = fred_snap.get("cpi_yoy")
                        macro_lines = (
                            f"┣ Fed Funds Rate: `{fred_snap.get('fedfunds', 'N/A'):.2f}%`\n"
                            f"┣ CPI YoY: `{cpi:.2f}%`\n" if cpi else ""
                        ) + f"┗ Unemployment Rate: `{fred_snap.get('unrate', 'N/A'):.1f}%`\n"
                    fred_payload = (
                        f"┣ **FRED Macro Overlay — Real Data**\n"
                        f"{yc_line}{vix_line}{macro_lines}"
                    )
                    send_essentials_embed(WEBHOOK_MARKET, "Treasury & Macro Conditions (FRED)", fred_payload, 0x2980b9)
                    logger.info("Dispatched FRED yield curve + macro snapshot")
            except Exception as e:
                logger.error(f"FRED macro dispatch failed: {e}")

            logger.info("Macro matrix compilation and dispatch completed.")

        elif args.mode == "morning":
            # ── MARKET ANALYSIS: Single Unified Morning Brief + reverse-feed conviction sync ──
            # Folds SPY/QQQ expected-move primers directly into the one brief below — three
            # separate embeds covering the same overnight session collapsed into one report.
            try:
                morning_brief, morning_snap = engine.generate_market_analysis_morning_report()
                if morning_brief and WEBHOOK_MARKET:
                    # 4 Pillars header (Andy Tanner framework): orients every report around the
                    # fundamental → technical → cash flow → risk decision sequence.
                    pillars_header = (
                        "**4 Pillars Framework** — Fundamental → Technical → Cash Flow → Risk\n"
                        "─────────────────────────────────────────────────\n"
                    )
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | MORNING BRIEF", pillars_header + morning_brief, 0x1abc9c)
                    dispatch_conviction_sync(engine, morning_snap, "morning")
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error(f"Market analysis morning brief failed: {e}\n{tb}")
                try:
                    import requests as _req
                    _po_token = os.getenv("PUSHOVER_API_TOKEN")
                    _po_user  = os.getenv("PUSHOVER_USER_KEY")
                    if _po_token and _po_user:
                        _req.post("https://api.pushover.net/1/messages.json", data={
                            "token": _po_token, "user": _po_user,
                            "title": "⚠️ Morning Brief FAILED",
                            "message": f"{e} | {tb[-300:]}",
                            "priority": 1,
                        }, timeout=10)
                except Exception:
                    pass

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
            # Cron: daily at 20:30 UTC — Friday gate below ensures it only dispatches on Fridays.
            # Add to PythonAnywhere: daily 20:30 UTC
            #   python3.10 /home/alftw/scripts/scheduler.py --mode weekly_scorecard
            if datetime.now().weekday() != 4:   # 4 = Friday
                logger.info("Weekly scorecard: not Friday, skipping.")
            else:
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

            try:
                graded = engine.sweep_and_grade_pending("cornerstone", min_age_days=3)
                if graded:
                    logger.info(f"Cornerstone ledger: graded {graded} pending RO risk call(s).")
            except Exception as e:
                logger.error(f"Cornerstone ledger sweep failed: {e}")

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

        elif args.mode == "income":
            logger.info("Executing Income Channel: Wheel Candidates + New CC ETF Screener...")

            # ── SEGMENT 1: DIVIDEND WHEEL CANDIDATES v2 ───────────────────────
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
                        avg_pop = sum(c["pop"] for c in wheel_candidates) / len(wheel_candidates)
                        setup_color = 0x2ecc71 if avg_pop >= 75.0 else 0xf1c40f
                        lines = []
                        for c in wheel_candidates:
                            div_badge = " 💰" if c.get("div_freq") == "Monthly" else ""
                            div_note = f" | Div `{c['div_yield']:.1f}%`" if c.get("div_yield") else ""
                            pop_icon = "✅" if c["pop"] >= 75 else "⚠️"
                            macd_note = "→ compressing" if c.get("macd_compressing") else ("▲ bull" if c.get("macd_hist", 0) > 0 else "▼ bear")
                            lines.append(
                                f"{pop_icon} **{c['symbol']}**{div_badge} `STO ${c['strike']:.0f}P` exp `{c['expiration']}` ({c['dte']}d) "
                                f"| `${c['premium']*100:.0f}/ct` | Δ`{c['delta']:.2f}` IVR`{c['ivr_proxy']:.0f}%` PoP`{c['pop']:.0f}%`\n"
                                f"┣ BE: `${c['break_even']:.2f}` ({c['pct_downside']:.1f}% protected){div_note} | {c['trend']} | RSI `{c['rsi14']}`\n"
                                f"┗ StochRSI K: `{c['stochrsi_k']:.0f}` {c['stochrsi_tag']} | MACD hist: `{c['macd_hist']:+.3f}` {macd_note}"
                            )
                        wheel_payload = "\n\n".join(lines)

                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "DIVIDEND WHEEL v2 | Premium Selling Setups", wheel_payload, setup_color)
                            logger.info(f"Wheel candidates dispatched: {len(wheel_candidates)} setups, avg PoP {avg_pop:.1f}%.")
                    else:
                        logger.info("Dividend Wheel v2 blocked by gatekeeper — state unchanged.")
                else:
                    logger.info("No wheel candidates passed filters this session.")
            except Exception as e:
                logger.error(f"Dividend wheel v2 segment failed: {e}")

            # ── SEGMENT 2: NEW/TRENDING CC ETF SCREENER ────────────────────────
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

                        # Cache top result for weekly scorecard income spotlight
                        try:
                            top_etf = new_etfs[0]
                            engine.db.update_state("cc_etf_spotlight_latest", {
                                "symbol": top_etf["symbol"],
                                "family": top_etf["family"],
                                "ann_yield": top_etf["ann_yield"],
                                "freq": top_etf["freq"],
                                "spot": top_etf["spot"],
                                "next_ex_date": top_etf["next_ex_date"],
                                "aum": top_etf["aum"],
                            })
                        except Exception as e:
                            logger.warning(f"CC ETF spotlight cache write failed: {e}")
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
                        ivr_payload = "Wheel Scanner — Elevated IV Rank Detected\n\n"
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
                            assigned_line = ""
                            if f.get("div_yield") is not None:
                                div_line = f"┣ Dividend: Yield `{f['div_yield']:.1f}%` | {f['div_freq']} | Amount `${f['div_amount']:.4f}`/share\n"
                                if f.get("div_freq") == "Monthly" and csp and csp.get("premium") and csp.get("strike"):
                                    premium_yield = (csp["premium"] / csp["strike"]) * 100
                                    monthly_div_yield = f["div_yield"] / 12
                                    combined_monthly = premium_yield + monthly_div_yield
                                    assigned_line = (
                                        f"┣ 💰 If Assigned: `${f['div_amount']:.4f}`/share/mo ({f['div_yield']:.1f}% annual) — keep earning while selling CCs\n"
                                        f"┣ Combined Return: Premium `{premium_yield:.2f}%` + Div `{monthly_div_yield:.2f}%` = `{combined_monthly:.2f}%`/mo\n"
                                    )
                            iv_hv = f.get("iv_hv_ratio")
                            iv_context = (
                                f"┣ IV/HV Ratio: `{iv_hv:.2f}x` HV30 — selling premium at a `{(iv_hv - 1) * 100:.0f}%` statistical premium to realized vol\n"
                                if iv_hv and iv_hv > 1.0 else ""
                            )
                            ivr_src = f.get("ivr_source", "proxy")
                            ivr_label = "IVR" if ivr_src == "Tradier" else "IVR est"
                            ivr_payload += (
                                f"**{f['symbol']}** | Spot: `${f['spot']:.2f}`\n"
                                f"┣ IV: `{f['iv']:.1f}%` | HV30: `{f['hv30']:.1f}%` | {ivr_label}: `{f['ivr_proxy']:.0f}%` [{ivr_src}]\n"
                                f"{iv_context}"
                                f"{setup_line}"
                                f"{div_line}"
                                f"{assigned_line}"
                                f"┗ Spread Check: `{f['spread_pct']:.1f}%` of mid | Earnings Window: Clear\n\n"
                            )
                        ivr_payload += "Directive: IV elevated above realized vol — favorable CSP entry. Size to collateral you can hold if assigned."
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "IV RANK ALERT | Wheel Strategy Scanner", ivr_payload, 0xe67e22)
                            logger.info(f"Tier 2 IV Rank alert dispatched: {len(flagged)} symbol(s) > 35% IVR.")

                        # Cache top candidate for weekly scorecard income spotlight
                        try:
                            top = flagged[0]
                            csp = top.get("csp_setup") or {}
                            engine.db.update_state("wheel_spotlight_latest", {
                                "symbol": top["symbol"],
                                "ivr_proxy": top["ivr_proxy"],
                                "spot": top["spot"],
                                "csp_setup": {
                                    "strike": csp.get("strike"),
                                    "dte": csp.get("dte"),
                                    "premium": csp.get("premium"),
                                    "expiration": csp.get("expiration"),
                                },
                                "div_yield": top.get("div_yield"),
                                "div_freq": top.get("div_freq"),
                            })
                        except Exception as e:
                            logger.warning(f"Wheel spotlight cache write failed: {e}")
            except Exception as e:
                logger.error(f"Tier 2 IV Rank screener failed: {e}")

            # ── MODULE 2: WHEEL POSITION MONITOR (DTE + P&L alerts) ────────────
            try:
                from tradier_client import TradierClient
                tc_wheel = TradierClient()
                open_positions = engine.db.get_open_wheel_positions()
                today = datetime.now().date()
                for pos in open_positions:
                    exp_date = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if dte < 0:
                        continue

                    alert_dte = None
                    urgency = ""
                    if dte <= 14 and pos.get("last_alert_dte") != 14 and (pos.get("last_alert_dte") is None or pos["last_alert_dte"] > 14):
                        alert_dte = 14
                        urgency = "🔴 CLOSE/ROLL DEADLINE"
                    elif dte <= 21 and pos.get("last_alert_dte") is None:
                        alert_dte = 21
                        urgency = "🟡 ROLL DECISION WINDOW"

                    # P&L check via Tradier current market price (50% profit / 200% loss triggers)
                    pnl_line = ""
                    try:
                        if tc_wheel.api_key:
                            csp_now = tc_wheel.find_csp_strike(
                                pos["symbol"], target_delta=0.20,
                                dte_min=max(1, dte - 3), dte_max=dte + 3,
                            )
                            if csp_now and csp_now.get("mid"):
                                current_val = csp_now["mid"]
                                entry_prem = pos.get("premium_collected", 0)
                                if entry_prem > 0:
                                    pct_decay = (entry_prem - current_val) / entry_prem * 100
                                    pnl_line = f"┣ Current value: `${current_val:.2f}` | Decay: `{pct_decay:.0f}%`\n"
                                    # 50% profit alert
                                    if pct_decay >= 50:
                                        profit_key = f"wheel_profit50_{pos['id']}"
                                        if not engine.db.get_state(profit_key):
                                            engine.db.update_state(profit_key, True)
                                            profit_payload = (
                                                f"**{pos['symbol']}** | {pos['position_type']} @ `${pos['strike']:.2f}`\n"
                                                f"┣ Expiration: `{pos['expiration']}` ({dte} DTE)\n"
                                                f"┣ Entry premium: `${entry_prem:.2f}` | Now: `${current_val:.2f}`\n"
                                                f"┣ Profit: `{pct_decay:.0f}%` of max\n"
                                                f"┗ 🟢 50% PROFIT TARGET HIT — consider closing early (Tasty rule)"
                                            )
                                            if WEBHOOK_INCOME:
                                                send_essentials_embed(WEBHOOK_INCOME, "WHEEL | 50% Profit Target", profit_payload, 0x2ecc71)
                                    # 200% loss alert (position value 3x entry = deep ITM breach)
                                    elif pct_decay <= -200:
                                        loss_key = f"wheel_loss200_{pos['id']}"
                                        if not engine.db.get_state(loss_key):
                                            engine.db.update_state(loss_key, True)
                                            loss_payload = (
                                                f"**{pos['symbol']}** | {pos['position_type']} @ `${pos['strike']:.2f}`\n"
                                                f"┣ Expiration: `{pos['expiration']}` ({dte} DTE)\n"
                                                f"┣ Entry premium: `${entry_prem:.2f}` | Now: `${current_val:.2f}`\n"
                                                f"┣ Current loss: `{abs(pct_decay):.0f}%` of premium received\n"
                                                f"┗ 🔴 DEEP ITM BREACH — roll down+out for credit or prep for assignment"
                                            )
                                            if WEBHOOK_INCOME:
                                                send_essentials_embed(WEBHOOK_INCOME, "WHEEL | Deep ITM Alert", loss_payload, 0xe74c3c)
                    except Exception:
                        pass

                    if alert_dte is not None:
                        dte_payload = (
                            f"**{pos['symbol']}** | {pos['position_type']} @ `${pos['strike']:.2f}`\n"
                            f"┣ Expiration: `{pos['expiration']}` ({dte} DTE)\n"
                            f"┣ Premium Collected: `${pos['premium_collected']:.2f}` x {pos['contracts']}\n"
                            f"{pnl_line}"
                            f"┗ {urgency}"
                        )
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "WHEEL POSITION | DTE Countdown", dte_payload, 0xf1c40f if alert_dte == 21 else 0xe74c3c)
                            engine.db.mark_wheel_position_alerted(pos["id"], alert_dte)
                            logger.info(f"Wheel DTE alert dispatched: {pos['symbol']} at {alert_dte} DTE.")
            except Exception as e:
                logger.error(f"Wheel position monitor failed: {e}")

            # ── MODULE 3: IV ENVIRONMENT STANDING-DOWN POST ────────────────────
            # Posts every session regardless of screener results — members see activity
            # even on low-IV days. Explains WHY the wheel is quiet and what to watch.
            try:
                _, vixy_z = engine.fetch_vixy_proxy() if hasattr(engine, "fetch_vixy_proxy") else (0, 0)
                if vixy_z < 0.5:
                    iv_env = "LOW — implied vol below recent baseline. Options sellers have thin edge."
                    directive = "Stand down on new wheel entries. Watch for IVR > 35% to open positions."
                elif vixy_z < 1.0:
                    iv_env = "MODERATE — some premium available. Be selective."
                    directive = "Selective entries only. Prioritize high-quality underlyings with IVR > 35%."
                else:
                    iv_env = "ELEVATED — premium environment favorable for wheel."
                    directive = "Active scanning. Run wheel screener for setups."

                # Show IVR proxy for top 5 WHEEL_UNIVERSE symbols
                sample_universe = ["NVDA", "AAPL", "TSLA", "QQQ", "PLTR"]
                ivr_lines = []
                for sym in sample_universe:
                    try:
                        hv30 = engine.calculate_historical_volatility(sym, lookback=30)
                        # hv30 is already in % (e.g. 42.4 = 42.4% annualized vol)
                        iv_est = hv30 * 1.15          # IV estimate in %
                        ivr_proxy = min(iv_est, 99)   # already in %, no ×100
                        bar = "🟢" if ivr_proxy >= 35 else ("🟡" if ivr_proxy >= 20 else "🔴")
                        ivr_lines.append(f"┣ {bar} **{sym}** IVR est `{ivr_proxy:.0f}%` (HV30 `{hv30:.1f}%`)")
                    except Exception:
                        pass

                standing_payload = (
                    f"**IV Environment:** {iv_env}\n"
                    f"┣ VIXY z-score: `{vixy_z:+.2f}σ`\n"
                    + ("\n".join(ivr_lines) + "\n" if ivr_lines else "")
                    + f"┗ {directive}"
                )
                if WEBHOOK_INCOME:
                    send_essentials_embed(WEBHOOK_INCOME, "🎡 WHEEL STATUS | Daily IV Environment", standing_payload, 0x3498db)
                    logger.info("Wheel IV environment standing-down post dispatched.")
            except Exception as e:
                logger.error(f"Wheel IV environment post failed: {e}")

            # ── MODULE 4: VIX-ADJUSTED ENTRY PARAMETERS ───────────────────────
            # Tells members WHICH delta and DTE to use TODAY based on VIX regime.
            try:
                real_vix = engine.fetch_real_vix() or 20.0
                vix_params = engine.get_vix_adjusted_params(real_vix)
                tier = vix_params["tier"]
                tier_color = {"LOW": 0x2ecc71, "NORMAL": 0x3498db, "ELEVATED": 0xf1c40f, "PANIC": 0xe74c3c}.get(tier, 0x95a5a6)
                vix_payload = (
                    f"VIX Regime: **{tier}** (VIX `{real_vix:.1f}` [FRED])\n\n"
                    f"┣ Target Delta: `{vix_params['delta_target']:.2f}` "
                    f"({int(vix_params['delta_target']*100)}% OTM probability)\n"
                    f"┣ DTE Window: `{vix_params['dte_min']}–{vix_params['dte_max']} days`\n"
                    f"┣ Size Scalar: `{vix_params['size_scalar']:.0%}` of normal position\n"
                    f"┗ Rationale: {'Low vol = can go closer to ATM for more premium.' if tier == 'LOW' else 'Elevated vol = go further OTM, tastytrade data shows 95% OTM expiry at this delta.' if tier == 'ELEVATED' else 'Panic regime = min size, max OTM. Wait for VIX < 25 before entering new positions.' if tier == 'PANIC' else 'Standard parameters apply.'}"
                )
                if WEBHOOK_INCOME:
                    send_essentials_embed(WEBHOOK_INCOME, "📐 WHEEL PARAMS | VIX-Adjusted Entry Guide", vix_payload, tier_color)
                    logger.info(f"VIX-adjusted params dispatched: {tier} regime (VIX {real_vix:.1f}).")
            except Exception as e:
                logger.error(f"VIX-adjusted params post failed: {e}")

            # ── MODULE 5: EARNINGS PROXIMITY SCANNER ──────────────────────────
            # Flags wheel universe symbols with earnings within 21 DTE.
            # Thetagang rule: never hold a short option through an earnings event.
            try:
                from tradier_client import TradierClient
                tc_earn = TradierClient()
                if tc_earn.api_key:
                    WHEEL_UNIVERSE = [
                        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AMD",
                        "SCHD", "JEPI", "JEPQ", "O", "ARCC",
                        "TSLA", "COIN", "SOFI", "PLTR",
                        "SPY", "QQQ", "IWM", "GLD", "XLE",
                    ]
                    earn_map = tc_earn.get_earnings_proximity(WHEEL_UNIVERSE, days_ahead=30)
                    flagged_force  = [(s, d) for s, d in earn_map.items() if d["flag"] == "FORCE_CLOSE"]
                    flagged_review = [(s, d) for s, d in earn_map.items() if d["flag"] == "REVIEW"]
                    if flagged_force or flagged_review:
                        earn_payload = "Earnings Proximity Scan — Wheel Universe\n\n"
                        if flagged_force:
                            earn_payload += "🔴 **FORCE CLOSE** (≤ 7 days to earnings)\n"
                            for sym, d in sorted(flagged_force, key=lambda x: x[1]["days_to_earnings"]):
                                earn_payload += f"┣ **{sym}** — earnings `{d['date']}` ({d['days_to_earnings']}d) — **EXIT NOW**\n"
                            earn_payload += "\n"
                        if flagged_review:
                            earn_payload += "🟡 **REVIEW** (≤ 21 days to earnings)\n"
                            for sym, d in sorted(flagged_review, key=lambda x: x[1]["days_to_earnings"]):
                                earn_payload += f"┣ **{sym}** — earnings `{d['date']}` ({d['days_to_earnings']}d) — no new entries\n"
                            earn_payload += "\n"
                        earn_payload += "┗ Rule: close or roll before earnings — IV crush post-earnings destroys premium value."
                        color = 0xe74c3c if flagged_force else 0xf1c40f
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "📅 EARNINGS WATCH | Wheel Universe", earn_payload, color)
                            logger.info(f"Earnings proximity: {len(flagged_force)} force-close, {len(flagged_review)} review.")
                    else:
                        logger.info("Earnings proximity: all wheel universe symbols clear (>21 DTE to any earnings).")
            except Exception as e:
                logger.error(f"Earnings proximity scanner failed: {e}")

            # ── MODULE 6: SENTISENSE CONVICTION LAYER ─────────────────────────
            # Institutional 13F flows + Insider Form 4 cluster signals for the
            # top-9 highest-IVR names from the wheel universe.
            # Adds the "stars align" cross-confirmation layer missing from pure
            # technical screens — when IVR qualifies AND institutions accumulate
            # AND insiders cluster-buy, conviction is at its highest.
            try:
                import sentisense_client as ss
                # Only scan the higher-priority core names to stay CPU/API lean.
                CONVICTION_UNIVERSE = [
                    "NVDA", "AAPL", "TSLA", "META", "MSFT",
                    "AMZN", "GOOGL", "AMD", "PLTR", "COIN",
                ]
                flows_map   = ss.batch_institutional_flows(engine.db, CONVICTION_UNIVERSE)
                insights_map = ss.batch_insights(engine.db, CONVICTION_UNIVERSE)

                conviction_lines = []
                for sym in CONVICTION_UNIVERSE:
                    flow    = flows_map.get(sym)
                    insight = insights_map.get(sym)
                    if not flow and not insight:
                        continue

                    # Build conviction tags
                    tags = []
                    flow_dir = flow["net_direction"] if flow else None
                    if flow_dir == "ACCUMULATING":
                        tags.append(f"🏦 inst ACCUM ({flow['filer_count']} filers, {flow['net_shares']:+,.0f} sh)")
                    elif flow_dir == "DISTRIBUTING":
                        tags.append(f"🏦 inst DIST ({flow['filer_count']} filers, {flow['net_shares']:+,.0f} sh)")

                    if insight and insight.get("cluster_buy"):
                        tags.append(f"👤 insider cluster BUY ({insight['insider_count']} filings)")
                    elif insight and insight.get("cluster_sell"):
                        tags.append(f"👤 insider cluster SELL ({insight['insider_count']} filings)")

                    if not tags:
                        continue

                    stars = len([t for t in tags if "ACCUM" in t or "BUY" in t])
                    align_emoji = "⭐" * stars if stars else ""
                    line = f"┣ **{sym}** {align_emoji}  " + " | ".join(tags)
                    conviction_lines.append((stars, line))

                if conviction_lines:
                    conviction_lines.sort(key=lambda x: x[0], reverse=True)
                    conv_payload = (
                        "Cross-confirms wheel signals with real institutional + insider data.\n\n"
                        + "\n".join(line for _, line in conviction_lines)
                        + "\n\n┗ ⭐ = institutional + insider confluence — highest conviction entry"
                    )
                    if WEBHOOK_INCOME:
                        send_essentials_embed(
                            WEBHOOK_INCOME,
                            "🔭 CONVICTION LAYER | Inst Flows + Insider Signals",
                            conv_payload, 0x8e44ad
                        )
                        logger.info(f"SentiSense conviction layer dispatched: {len(conviction_lines)} symbols.")
                else:
                    logger.info("SentiSense conviction layer: no notable institutional/insider signals today.")
            except Exception as e:
                logger.error(f"SentiSense conviction layer failed: {e}")

        elif args.mode == "wheel_position":
            if args.action == "open":
                if not all([args.symbol, args.position_type, args.strike, args.expiration, args.premium]):
                    logger.error("wheel_position open requires --symbol --type --strike --expiration --premium")
                else:
                    pos_id = engine.db.open_wheel_position(
                        args.symbol.upper(), args.position_type, args.strike,
                        args.expiration, args.premium, args.contracts,
                        cost_basis=args.cost_basis,
                        open_fees=args.open_fees or 0.0,
                        roll_group_id=args.roll_group_id,
                    )
                    cb_note   = f" | cost basis ${args.cost_basis:.2f}" if args.cost_basis else ""
                    fee_note  = f" | fees ${args.open_fees:.2f}" if args.open_fees else ""
                    roll_note = f" | roll group {args.roll_group_id}" if args.roll_group_id else ""
                    logger.info(f"Opened wheel position #{pos_id}: {args.symbol.upper()} {args.position_type} "
                                f"${args.strike} exp {args.expiration}{cb_note}{fee_note}{roll_note}")
            elif args.action == "close":
                if not args.position_id:
                    logger.error("wheel_position close requires --position-id")
                else:
                    ok = engine.db.close_wheel_position(
                        args.position_id,
                        status=args.status,
                        close_price_per_share=args.close_price,
                        close_fees=args.close_fees or 0.0,
                    )
                    if ok:
                        cp_note = f" | BTC at ${args.close_price:.2f}/share" if args.close_price else ""
                        logger.info(f"Closed wheel position #{args.position_id} as {args.status}{cp_note}. "
                                    f"Total premium ledger now: ${engine.db.get_total_premium_collected():,.2f}")
                    else:
                        logger.error(f"Could not close position #{args.position_id} — not found or not OPEN.")
            else:
                logger.error("wheel_position mode requires --action open|close")

        elif args.mode == "iv_crush":
            iv_dispatched = False
            flow_dispatched = False

            # Fetch chains once — shared by both segments (no double-fetch)
            iv_chains = engine._fetch_iv_crush_chains()

            # ── SEGMENT 1: IV CRUSH SCANNER ──
            scan_data = engine.run_iv_crush_scan(chains=iv_chains)
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
            flow_data = engine.scan_unusual_options_flow(chains=iv_chains)
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

        elif args.mode == "options_flow":
            # ── OPTIONS SETUP SCANNER ─────────────────────────────────────────
            # Screens the dynamic universe for high-conviction directional setups.
            # Sources: RVOL, RSI, MACD, ATR (strike zone), short interest, 52W range,
            # social sentiment (StockTwits/WSB). No fake dark pool — every signal is
            # derived from real publicly available equity data.
            # Run once per session around 10:00-10:30 ET after the open settles.
            try:
                setups = engine.generate_options_setup_scan()
                if not setups:
                    logger.info("Options flow scan: no qualifying setups this session.")
                else:
                    today_label = datetime.now().strftime("%b %-d")
                    header = f"OPTIONS SETUP SCANNER — {today_label}\n"
                    for s in setups:
                        direction_icon = "🟢 CALL" if s["direction"] == "CALL" else "🔴 PUT"
                        squeeze_line = f"┣ Short squeeze risk: {s['short_pct']:.1f}% of float short\n" if s["short_pct"] > 5.0 else ""
                        social_line  = f"┣ Social: {s['social_meter']} buzz — {s['social_lean']}\n" if s.get("social_meter") else ""
                        payload = (
                            f"{s['symbol']} — {direction_icon} BIAS\n"
                            f"┣ Spot: ${s['spot']:,.2f} | RVOL: {s['rvol']:.1f}x | RSI: {s['rsi']:.0f}\n"
                            f"┣ MACD: {s['macd_tag']} | ATR(14): ${s['atr']:.2f}\n"
                            f"┣ Strike zone: ${s['strike_lo']:,.0f}–${s['strike_hi']:,.0f} | DTE: 21–30\n"
                            f"┣ 52W range: {s['range_pct']:.0f}% ({s['range_tag']})\n"
                            f"{squeeze_line}"
                            f"{social_line}"
                            f"┗ {s['verdict']}"
                        )
                        color = 0x2ecc71 if s["direction"] == "CALL" else 0xe74c3c
                        if WEBHOOK_OPTIONS:
                            send_essentials_embed(WEBHOOK_OPTIONS, f"OPTIONS SETUP: {s['symbol']}", payload, color)
                    logger.info(f"Options flow scan dispatched: {len(setups)} setup(s).")
            except Exception as e:
                logger.error(f"Options flow scan failed: {e}")

        elif args.mode == "trending_plays":
            # ── SOCIAL SENTIMENT TRENDING OPTIONS PLAYS ─────────────────────
            # Sources: StockTwits + Reddit WSB + Finviz top movers/unusual volume (3-source scoring).
            # Meter: HIGH = 2+ sources | NEUTRAL = 1 source.
            # BTO conviction block fires when HIGH + momentum confirmed + RSI 45-68 + not meme.
            # Run once per trading session — e.g. 09:30 ET after open.
            try:
                plays = engine.generate_trending_options_plays(max_results=5)
                if not plays:
                    logger.info("Trending plays: no qualifying plays found this session.")
                else:
                    today_label = datetime.now().strftime("%b %-d")
                    payload = f"**TRENDING OPTIONS PLAYS — {today_label}**\n\n"
                    for p in plays:
                        chg_arrow = "▲" if p["chg_5d"] >= 0 else "▼"
                        bto = p.get("bto_setup")
                        bto_block = ""
                        if bto:
                            bto_block = (
                                f"┣ BTO {bto['direction']} | Strike ~${bto['strike']:.2f} | {bto['dte']} DTE\n"
                                f"┣ Est. ${bto['prem_lo']:.2f}–${bto['prem_hi']:.2f}/contract (verify live chain)\n"
                                f"┣ Target +100% (~${bto['target']:.2f}) | Stop -50% (~${bto['stop']:.2f})\n"
                                f"┣ R/R 2:1\n"
                            )
                        # SentiSense enrichment line (shown only when available)
                        ss_line = ""
                        if p.get("ss_score") is not None:
                            dom_str = f" · {p['ss_dominance']:.2f}% share of voice" if p.get("ss_dominance") else ""
                            men_str = f" · {p['ss_mentions']:,} mentions" if p.get("ss_mentions") else ""
                            ss_line = f"┣ SentiSense: `{p['ss_score']:.1f}/10` {p['lean']}{men_str}{dom_str}\n"
                        payload += (
                            f"**{p['symbol']}** `${p['spot']:.2f}`  "
                            f"{chg_arrow} {abs(p['chg_5d']):.1f}% (5D)\n"
                            f"┣ Buzz: {p['meter']} · {p['lean']}\n"
                            f"┣ Vol: {p['vol_ratio']:.1f}x avg · RSI {p['rsi']:.0f}\n"
                            f"{ss_line}"
                            f"{bto_block}"
                            f"┗ {p['verdict']}\n\n"
                        )
                    payload += (
                        "─────────────────────────\n"
                        "Not financial advice — for informational/educational use only."
                    )
                    if WEBHOOK_OPTIONS:
                        send_essentials_embed(
                            WEBHOOK_OPTIONS,
                            "OPTIONS DESK | Trending Plays",
                            payload, 0x9b59b6
                        )
                        logger.info(f"Trending plays dispatched: {len(plays)} plays.")
            except Exception as e:
                logger.error(f"Trending plays scanner failed: {e}")

        elif args.mode == "crypto_social":
            # ── CRYPTO SOCIAL SNAPSHOT → #crypto ─────────────────────────────
            # Fear & Greed + Reddit crypto mentions + spot prices (BTC/ETH/SOL/AVAX/LINK/DOGE)
            # + Binance perpetual funding rates + NVDA/BTC 30-day correlation.
            try:
                snap    = engine.generate_crypto_social_snapshot()
                funding = engine.fetch_funding_rates()
                corr    = engine.calculate_nvda_btc_correlation()
                fng     = snap["fear_greed"]
                today_l = datetime.now().strftime("%b %-d")

                v = fng["value"]
                if v <= 25:      fng_bar = "Extreme Fear"
                elif v <= 45:    fng_bar = "Fear"
                elif v <= 55:    fng_bar = "Neutral"
                elif v <= 75:    fng_bar = "Greed"
                else:            fng_bar = "Extreme Greed"

                payload = f"**CRYPTO DESK — {today_l}**\n\n"
                payload += f"**Fear & Greed:** {fng['value']}/100 — {fng_bar}\n\n"

                if snap["trending"]:
                    payload += "**Spot Prices**\n"
                    for token, data in snap["trending"][:6]:
                        arrow = "▲" if data["chg_1d"] >= 0 else "▼"
                        buzz  = " · Reddit" if token in snap["reddit_counts"] else ""
                        payload += f"┣ `{token}` ${data['price']:,.2f} {arrow}{abs(data['chg_1d']):.1f}% (1D){buzz}\n"
                    payload = payload.rstrip("┣ \n") + "\n\n"

                if funding:
                    payload += "**Perp Funding Rates (8h / annualized)**\n"
                    for f in funding:
                        sign = "+" if f["rate_8h"] >= 0 else ""
                        payload += (
                            f"┣ `{f['symbol']}` {sign}{f['rate_8h']:.4f}% · "
                            f"{sign}{f['rate_ann']:.1f}%/yr — {f['sentiment']}\n"
                        )
                    payload = payload.rstrip("┣ \n") + f"\n┗ Next settlement: {funding[0]['next_funding']}\n\n"

                if corr:
                    arrow_n = "▲" if corr["nvda_ret"] >= 0 else "▼"
                    arrow_b = "▲" if corr["btc_ret"]  >= 0 else "▼"
                    payload += (
                        f"**NVDA / BTC Correlation ({corr['lookback']}D)**\n"
                        f"┣ Pearson r: `{corr['correlation']:+.3f}` — {corr['label']}\n"
                        f"┗ Period returns: NVDA {arrow_n}{abs(corr['nvda_ret']):.1f}% · "
                        f"BTC {arrow_b}{abs(corr['btc_ret']):.1f}%\n\n"
                    )

                # ── Binance Derivatives Intelligence (OI, L/S, taker volume) ──
                # These are the signals institutional desks watch before price moves.
                # All free Binance FAPI public endpoints — no API key required.
                try:
                    deriv = engine.fetch_binance_derivatives()
                    if deriv:
                        payload += "**Derivatives Signal Stack**\n"
                        for sym_d, d in deriv.items():
                            oi_b = d["oi_usd"] / 1e9
                            ls_ratio = d["global_ls"]
                            top_ls   = d["top_ls"]
                            tb_pct   = d["taker_buy_pct"]
                            # Divergence between retail (global) and smart money (top trader) L/S
                            smart_vs_retail = ""
                            if top_ls > 1.1 and ls_ratio < 1.0:
                                smart_vs_retail = " ← smart money diverging long"
                            elif top_ls < 0.9 and ls_ratio > 1.1:
                                smart_vs_retail = " ← smart money diverging short"
                            tb_label = "sellers in control" if tb_pct < 45 else ("buyers in control" if tb_pct > 55 else "balanced")
                            payload += (
                                f"┣ **{sym_d}** OI: `${oi_b:.1f}B` | "
                                f"Global L/S: `{ls_ratio:.2f}` | Top Trader: `{top_ls:.2f}`{smart_vs_retail}\n"
                                f"┣ Taker Buy: `{tb_pct:.0f}%` — {tb_label}\n"
                            )
                        payload = payload.rstrip("┣ \n") + "\n\n"
                except Exception as e:
                    logger.warning(f"Binance derivatives fetch failed: {e}")

                # ── Crypto Cycle Top Score ──────────────────────────────────────
                try:
                    cycle_top = engine.calculate_crypto_top_score()
                    ct_score  = cycle_top["score"]
                    ct_label  = cycle_top["label"]
                    ct_sigs   = cycle_top.get("signals", {})
                    dom       = ct_sigs.get("btc_dominance", 0.0)
                    streak    = ct_sigs.get("fg_extreme_streak", 0)
                    sm_div    = ct_sigs.get("sm_divergence", "None")
                    # Color: green = safe, yellow = caution, orange = reduce, red = exit
                    cycle_color_text = (
                        "🟢 No top signal" if ct_score < 40
                        else "🟡 Late-cycle caution" if ct_score < 65
                        else "🟠 Reduce Tier 3" if ct_score < 80
                        else "🔴 EXIT Tier 3"
                    )
                    payload += (
                        f"**Cycle Top Score: `{ct_score}/100` — {cycle_color_text}**\n"
                        f"┣ {ct_label}\n"
                        f"┣ BTC Dominance: `{dom:.1f}%` | "
                        f"Extreme Greed Streak: `{streak}d`\n"
                        f"┗ Smart Money: {sm_div}\n\n"
                    )
                except Exception as e:
                    logger.warning(f"Crypto cycle top score failed: {e}")

                payload += (
                    "─────────────────────────\n"
                    "Sources: Alternative.me · Reddit r/Cryptocurrency · Binance FAPI · CoinGecko · Twelve Data\n"
                    "Not financial advice — for informational/educational use only."
                )

                if WEBHOOK_CRYPTO:
                    send_essentials_embed(WEBHOOK_CRYPTO, "CRYPTO DESK | Social + Funding + Derivatives", payload, 0xf39c12)
                    logger.info("Crypto social snapshot dispatched.")
            except Exception as e:
                logger.error(f"Crypto social scan failed: {e}")

        elif args.mode == "futures_social":
            # ── FUTURES-ADJACENT SOCIAL SCAN + PATTERN SCAN → #futures-trading ─
            # Segment 1: StockTwits + Reddit WSB filtered to energy/metals/rates/ag.
            # Segment 2: Finviz TA pattern scan (bullish/bearish setups on volume).
            try:
                snap    = engine.generate_futures_social_snapshot()
                plays   = snap.get("plays", [])
                patterns = engine.fetch_finviz_pattern_scan()
                today_l = datetime.now().strftime("%b %-d")

                # ── Segment 1: Commodity social buzz ──
                if plays:
                    payload = f"**COMMODITY / MACRO BUZZ — {today_l}**\n\n"
                    for p in plays[:8]:
                        arrow = "▲" if p["chg_5d"] >= 0 else "▼"
                        payload += (
                            f"**{p['symbol']}** `${p['spot']:.2f}` {arrow}{abs(p['chg_5d']):.1f}% (5D)\n"
                            f"┣ Buzz: {p['meter']} · {p['lean']}\n"
                            f"┗ Vol: {p['vol_ratio']:.1f}x avg\n\n"
                        )
                    payload += "Social overlay for #futures context — not a directional call."
                    if WEBHOOK_FUTURES:
                        send_essentials_embed(WEBHOOK_FUTURES, "FUTURES DESK | Commodity & Macro Buzz", payload, 0xe67e22)
                        logger.info(f"Futures social dispatched: {len(plays)} names.")
                else:
                    logger.info("Futures social: no futures-adjacent names trending this session.")

                # ── Segment 2: Finviz TA pattern scan ──
                bullish = patterns.get("bullish", [])
                bearish = patterns.get("bearish", [])
                if bullish or bearish:
                    pat_payload = f"**S&P TECHNICAL PATTERNS — {today_l}**\n\n"
                    if bullish:
                        pat_payload += "**Bullish Setups**\n"
                        seen = set()
                        for item in bullish:
                            if item["symbol"] not in seen:
                                seen.add(item["symbol"])
                                sign = "+" if item["chg"] >= 0 else ""
                                pat_payload += f"┣ `{item['symbol']}` ${item['price']:.2f} {sign}{item['chg']:.1f}% — {item['pattern']}\n"
                        pat_payload = pat_payload.rstrip("┣ \n") + "\n\n"
                    if bearish:
                        pat_payload += "**Bearish Setups**\n"
                        seen = set()
                        for item in bearish:
                            if item["symbol"] not in seen:
                                seen.add(item["symbol"])
                                sign = "+" if item["chg"] >= 0 else ""
                                pat_payload += f"┣ `{item['symbol']}` ${item['price']:.2f} {sign}{item['chg']:.1f}% — {item['pattern']}\n"
                        pat_payload = pat_payload.rstrip("┣ \n") + "\n\n"
                    pat_payload += (
                        "─────────────────────────\n"
                        "Source: Finviz TA Screener · >500K avg daily volume filter\n"
                        "Not financial advice — for informational/educational use only."
                    )
                    if WEBHOOK_FUTURES:
                        send_essentials_embed(WEBHOOK_FUTURES, "FUTURES DESK | S&P Pattern Scan", pat_payload, 0x3498db)
                        logger.info(f"Pattern scan dispatched: {len(bullish)} bullish, {len(bearish)} bearish.")
                else:
                    logger.info("Pattern scan: no qualifying patterns returned (may be outside market hours).")
            except Exception as e:
                logger.error(f"Futures social scan failed: {e}")

        # ── STORE DAILY IV — 21:30 UTC cron, saves ATM IV for IVR tracker ─────
        elif args.mode == "store_daily_iv":
            try:
                from tradier_client import TradierClient
                from database import EcosystemDatabase
                tc = TradierClient()
                db_iv = EcosystemDatabase()
                UNIVERSE = [
                    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AMD",
                    "SCHD", "JEPI", "JEPQ", "O", "ARCC",
                    "TSLA", "COIN", "SOFI", "PLTR",
                    "SPY", "QQQ", "IWM", "GLD", "XLE",
                    "TQQQ", "MAIN", "MLPI", "KQQQ", "TDAQ",
                ]
                stored, skipped = 0, 0
                for sym in UNIVERSE:
                    try:
                        iv = tc.get_atm_iv(sym, option_type="call", dte_min=20, dte_max=50)
                        if iv > 0:
                            db_iv.store_daily_iv(sym, iv)
                            stored += 1
                        else:
                            skipped += 1
                    except Exception as sym_e:
                        logger.warning(f"store_daily_iv: skipped {sym}: {sym_e}")
                        skipped += 1
                logger.info(f"store_daily_iv: stored={stored} skipped={skipped}")
            except Exception as e:
                logger.error(f"store_daily_iv failed: {e}")

        # ── SPX 0DTE INCOME — defined-risk iron condor scout → #options-wheel ──
        elif args.mode == "spx_income":
            try:
                from tradier_client import TradierClient
                tc = TradierClient()

                # Gate: VIXY z-score < 0.5 + SPY breadth > 60% (low-vol only)
                vixy_price, vixy_z = engine.fetch_vixy_proxy()
                snap = engine._gather_cross_asset_snapshot()
                breadth = snap.get("breadth", 0.5)

                if vixy_z >= 0.5:
                    logger.info(f"SPX income: gated — VIXY z {vixy_z:+.2f}σ (need < 0.5). No dispatch.")
                elif breadth < 0.60:
                    logger.info(f"SPX income: gated — breadth {breadth:.0%} (need > 60%). No dispatch.")
                else:
                    condor = tc.get_spx_0dte_condor(wing_width=5, target_delta=0.10)
                    if not condor.get("valid"):
                        logger.info(f"SPX income: {condor.get('reason','no valid condor')}")
                    else:
                        rr = condor["rr_ratio"]
                        payload = (
                            f"**SPX 0DTE Iron Condor Scout**\n"
                            f"┣ Expiration: {condor['expiration']} (0DTE)\n"
                            f"┣ Call side: Sell {condor['call_sell']:.0f} / Buy {condor['call_buy']:.0f}\n"
                            f"┣ Put side:  Sell {condor['put_sell']:.0f} / Buy {condor['put_buy']:.0f}\n"
                            f"┣ Credit: ${condor['credit']:.2f} (${condor['credit_dollars']} per spread)\n"
                            f"┣ Max risk: ${condor['max_risk']:.0f} | R:R = 1:{rr}\n"
                            f"┣ Gate: VIXY z {vixy_z:+.2f}σ ✅ | Breadth {breadth:.0%} ✅\n"
                            f"┗ Defined risk — max loss is the wing width minus credit collected.\n"
                            f"─────────────────────────\n"
                            f"4 Pillars: Cash Flow play — premium expires worthless if SPX stays between wings.\n"
                            f"Not financial advice — educational/informational only."
                        )
                        color = 0x2ecc71 if rr <= 3 else 0xf1c40f
                        if WEBHOOK_OPTIONS:
                            send_essentials_embed(WEBHOOK_OPTIONS, "SPX Income | 0DTE Condor", payload, color)
                            logger.info(f"SPX income condor dispatched: credit ${condor['credit']:.2f}, R:R 1:{rr}")
            except Exception as e:
                logger.error(f"SPX income mode failed: {e}")

        # ── CEF PREMIUM Z-SCORE CALIBRATION — 22:30 UTC daily ────────────────
        # Pulls 252-day premium history from CEFConnect and updates monitor.py's
        # z-score baseline (mu/sigma) in DB. Safe to re-run — always overwrites
        # with latest rolling 252-day window.
        elif args.mode == "cef_calibrate":
            try:
                for ticker in ["CLM", "CRF"]:
                    result = engine.calibrate_cef_premium_zscore(ticker)
                    if result:
                        logger.info(
                            f"CEF calibrate {ticker}: mu={result['mu']:.2f}% "
                            f"sigma={result['sigma']:.2f}% n={result['n']}"
                        )
                    else:
                        logger.warning(f"CEF calibrate {ticker}: failed — DB unchanged.")
            except Exception as e:
                logger.error(f"cef_calibrate failed: {e}")

    except Exception as e:
        logger.critical(f"Task Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

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

# Shared embed color palette — green/yellow/red maps to bullish/neutral/bearish.
# All dynamic embeds use this so the Discord left bar is instantly readable.
COLOR_GREEN  = 0x2ecc71   # bullish / safe / risk-on
COLOR_YELLOW = 0xf1c40f   # neutral / mixed / caution
COLOR_RED    = 0xe74c3c   # bearish / danger / risk-off

def _bias_color(score: float, bull_threshold: float = 1.0, bear_threshold: float = -1.0) -> int:
    """Return green/yellow/red based on a numeric score."""
    if score >= bull_threshold:  return COLOR_GREEN
    if score <= bear_threshold:  return COLOR_RED
    return COLOR_YELLOW

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
    parser.add_argument("--mode", type=str, required=True, choices=["morning", "eod", "income", "iv_crush", "gex", "post_market", "options_flow", "macro", "market_intraday", "weekly_scorecard", "wheel_signals", "wheel_position", "trending_plays", "crypto_social", "futures_social", "store_daily_iv", "cef_calibrate", "mlpi_entry", "personal_scorecard"])
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
                # Color from live HY spread: green=safe, yellow=watch, red=stress
                _hy = engine.fetch_hy_spread() or 0.0
                _liq_color = COLOR_RED if _hy > 4.5 else (COLOR_YELLOW if _hy > 3.5 else COLOR_GREEN)
                send_essentials_embed(WEBHOOK_MARKET, "Credit & Liquidity Check", liq_payload, _liq_color)

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
                        _regime_color = COLOR_GREEN if "RISK-ON" in regime else COLOR_RED
                        send_essentials_embed(WEBHOOK_MARKET, "Carry Trade Risk Regime", regime_payload, _regime_color)
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
                            _mover_color = COLOR_GREEN if pct_change > 0 else COLOR_RED
                            send_essentials_embed_with_chart(
                                WEBHOOK_CRYPTO, f"🪙 CRYPTO MOVER OF THE DAY: {symbol}",
                                f"┣ Spot: `${price:,.2f}`\n┗ 1-Day Move: `{pct_change:+.2f}%` — largest swing in the tracked universe today.",
                                chart_bytes, color=_mover_color
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
                                _btc_spy_color = COLOR_GREEN if btc_bullish else COLOR_RED
                                send_essentials_embed(WEBHOOK_MARKET, "CRYPTO → EQUITIES SIGNAL SYNC", corr_payload, _btc_spy_color)
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
                    # Color from yield curve: positive=green, flat zone=yellow, inverted=red
                    _yc_spread = yc["spread"] if yc else 0.0
                    _macro_color = COLOR_RED if _yc_spread < 0 else (COLOR_YELLOW if _yc_spread < 0.25 else COLOR_GREEN)
                    send_essentials_embed(WEBHOOK_MARKET, "Treasury & Macro Conditions (FRED)", fred_payload, _macro_color)
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
                    _morning_color = _bias_color(morning_snap.get("conviction_score", 0))
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | MORNING BRIEF", pillars_header + morning_brief, _morning_color)
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
                    premium_env = "BALANCED — Moderate IV. Wheel CSPs and covered calls viable."
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
                    _options_pm_color = COLOR_GREEN if vix_z >= 0.75 else COLOR_YELLOW
                    send_essentials_embed(WEBHOOK_OPTIONS, "OPTIONS DESK | Pre-Market Conditions Brief", options_brief, _options_pm_color)
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
                    _intra_bias = engine.db.get_state("market_analysis_bias") or {}
                    _intra_score = _intra_bias.get("score", 0) if isinstance(_intra_bias, dict) else 0
                    _intra_color = _bias_color(_intra_score)
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | INTRADAY PULSE", intraday_brief, _intra_color)
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
                    _eod_color = _bias_color(eod_snap.get("conviction_score", 0))
                    send_essentials_embed(WEBHOOK_MARKET, "MARKET ANALYSIS | END-OF-DAY RECAP", eod_brief, _eod_color)
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

            # ── SEGMENT 2: SOCIAL-FIRST CC ETF RADAR ─────────────────────────
            # Flow: StockTwits buzz scan across full ~35-ticker CC ETF universe
            # → top 3 by community activity → yield/AUM enrichment → single embed.
            # Social discovery drives what surfaces; yield data confirms it's viable.
            # Replaces the old static-watchlist screener + separate buzz embed.
            try:
                buzz_ranked = engine.scan_ccincome_social_buzz(top_n=15)  # wide net — yield filter cuts to ~3

                if buzz_ranked:
                    buzz_syms = [b["symbol"] for b in buzz_ranked]
                    buzz_meta = {b["symbol"]: b for b in buzz_ranked}

                    # Enrich with yield/pay/AUM — only buzz-surfaced tickers, not the full universe
                    enriched = engine.generate_new_income_etf_screener(tickers=buzz_syms)

                    # Primary sort: buzz score DESC. Tiebreaker: yield DESC.
                    # StockTwits caps at 30 msgs/call so ties are common on high-activity days.
                    buzz_scores = {b["symbol"]: b["buzz_score"] for b in buzz_ranked}
                    enriched.sort(key=lambda x: (
                        -buzz_scores.get(x["symbol"], 0),
                        -x["ann_yield"]
                    ))

                    # Cap at top 3 that passed yield enrichment.
                    # If buzz universe didn't yield 3, pad with top static screener results
                    # (excluding any already in enriched) so the embed always shows 3.
                    top3 = enriched[:3]
                    if len(top3) < 3:
                        try:
                            already = {e["symbol"] for e in top3}
                            static_fill = [
                                e for e in engine.generate_new_income_etf_screener()
                                if e["symbol"] not in already
                            ]
                            for e in static_fill:
                                if len(top3) >= 3:
                                    break
                                buzz_meta[e["symbol"]] = {
                                    "buzz_score": 0, "msg_count": 0,
                                    "bullish": 0, "bearish": 0,
                                    "bull_pct": 0, "lean": "NEUTRAL",
                                }
                                top3.append(e)
                        except Exception:
                            pass

                    if top3:
                        state_str = "_".join(f"{e['symbol']}{e['ann_yield']}" for e in top3)
                        if engine.db.track_and_limit_alerts(
                            "new_income_etf_screener_daily", state_str,
                            sum(e['ann_yield'] for e in top3), max_broadcasts=2, threshold_pct=0.05
                        ):
                            # SentiSense underlying sentiment enrichment
                            underlying_map = {
                                "MSTY": "MSTR", "NVDY": "NVDA", "TSLY": "TSLA",
                                "CONY": "COIN", "GOOY": "GOOGL", "AMDY": "AMD",
                                "YMAX": "QQQ",  "XDTE": "SPY",  "QDTE": "QQQ",
                                "RDTE": "IWM",  "QQQI": "QQQ",  "SPYI": "SPY",
                                "BTCI": "BTC",  "MAGY": "META",  "YMAG": "QQQ",
                                "PLTY": "PLTR", "AMZY": "AMZN", "METY": "META",
                                "JPMY": "JPM",  "FEPI": "SPY",  "SVOL": "VXX",
                                "DIVO": "SPY",  "SCHD": "SPY",  "JEPI": "SPY",
                                "JEPQ": "QQQ",  "XYLD": "SPY",  "QYLD": "QQQ",
                                "RYLD": "IWM",
                            }
                            ss_map = {}
                            try:
                                import sentisense_client as ss
                                for e in top3:
                                    ul = underlying_map.get(e["symbol"])
                                    if ul and ul != "BTC" and ul != "VXX":
                                        sent = ss.get_sentiment(engine.db, ul)
                                        if sent:
                                            ss_map[e["symbol"]] = sent
                            except Exception:
                                pass

                            new_payload = ""
                            for rank, e in enumerate(top3, 1):
                                bz = buzz_meta.get(e["symbol"], {})
                                buzz_score = bz.get("buzz_score", 0)
                                msg_count  = bz.get("msg_count", 0)
                                lean       = bz.get("lean", "")
                                bull_pct   = bz.get("bull_pct", 0)
                                lean_emoji = "🟢" if lean == "BULLISH" else ("🔴" if lean == "BEARISH" else "🟡")

                                sent = ss_map.get(e["symbol"])
                                ss_line = ""
                                if sent:
                                    ss_line = (
                                        f"┣ Underlying ({underlying_map.get(e['symbol'], '?')}): "
                                        f"`{sent['score']:+.0f}` {sent['lean']} ({sent['mentions']} mentions)\n"
                                    )

                                if buzz_score > 0:
                                    buzz_line = f"┣ Buzz: {lean_emoji} `{msg_count}` msgs — `{bull_pct}%` bullish (score `{buzz_score}`)\n"
                                    source_line = "┗ Source: StockTwits community activity + yield filter >10%\n\n"
                                else:
                                    buzz_line = "┣ Buzz: — (yield-sorted fill — no social signal today)\n"
                                    source_line = "┗ Source: yield-sorted fallback\n\n"

                                new_payload += (
                                    f"**#{rank} {e['symbol']}** | {e['family']} | {e['freq']}\n"
                                    f"{buzz_line}"
                                    f"┣ Spot: `${e['spot']:.2f}` | Yield: `{e['ann_yield']:.1f}%` ann. | AUM: `{e['aum']}`\n"
                                    f"┣ Next pay: `{e['next_ex_date']}`\n"
                                    f"{ss_line}"
                                    f"{source_line}"
                                )
                            new_payload = new_payload.rstrip()

                            if WEBHOOK_INCOME:
                                send_essentials_embed(
                                    WEBHOOK_INCOME,
                                    "📡 CC INCOME RADAR | Top 3 by Community Buzz",
                                    new_payload, 0x9b59b6
                                )
                                logger.info(f"Social-first CC ETF radar dispatched: top {len(top3)} by buzz.")

                            # Cache top result for weekly scorecard income spotlight
                            try:
                                top_etf = top3[0]
                                engine.db.update_state("cc_etf_spotlight_latest", {
                                    "symbol":    top_etf["symbol"],
                                    "family":    top_etf["family"],
                                    "ann_yield": top_etf["ann_yield"],
                                    "freq":      top_etf["freq"],
                                    "spot":      top_etf["spot"],
                                    "next_ex_date": top_etf["next_ex_date"],
                                    "aum":       top_etf["aum"],
                                    "buzz_score": buzz_meta.get(top_etf["symbol"], {}).get("buzz_score", 0),
                                })
                            except Exception as e:
                                logger.warning(f"CC ETF spotlight cache write failed: {e}")
                    else:
                        # Buzz tickers didn't pass yield filter — fall back to yield-sorted static scan
                        logger.info("Buzz tickers below yield threshold — falling back to static screener.")
                        fallback = engine.generate_new_income_etf_screener()[:3]
                        if fallback and WEBHOOK_INCOME:
                            fb_payload = ""
                            for e in fallback:
                                fb_payload += (
                                    f"**{e['symbol']}** | {e['family']} | {e['freq']}\n"
                                    f"┣ Spot: `${e['spot']:.2f}` | Yield: `{e['ann_yield']:.1f}%` ann.\n"
                                    f"┣ Next pay: `{e['next_ex_date']}`\n"
                                    f"┗ Source: yield-sorted fallback (no buzz signal today)\n\n"
                                )
                            send_essentials_embed(
                                WEBHOOK_INCOME,
                                "CC INCOME RADAR | Top 3 by Yield (Fallback)",
                                fb_payload.rstrip(), 0x9b59b6
                            )
                else:
                    logger.info("CC income social buzz: no CC income tickers trending today.")
            except Exception as e:
                logger.error(f"Social-first CC ETF radar failed: {e}")

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

                    # ── Earnings proximity check for THIS open position ──────────
                    # Entry scanner (Module 5) guards new entries; this guards existing holds.
                    try:
                        if tc_wheel.api_key:
                            earn_check = tc_wheel.get_earnings_proximity([pos["symbol"]], days_ahead=30)
                            pos_earn   = earn_check.get(pos["symbol"])
                            if pos_earn and pos_earn.get("flag") in ("FORCE_CLOSE", "REVIEW"):
                                _earn_key = f"wheel_earn_{pos['id']}_{pos_earn.get('date','')}"
                                if not engine.db.get_state(_earn_key):
                                    engine.db.update_state(_earn_key, True)
                                    _days_e = pos_earn.get("days_to_earnings", 0)
                                    _flag_e = pos_earn["flag"]
                                    _earn_payload = (
                                        f"**{pos['symbol']}** | {pos['position_type']} @ `${pos['strike']:.2f}`\n"
                                        f"┣ Expiration: `{pos['expiration']}` ({dte} DTE)\n"
                                        f"┣ 📅 Earnings: `{pos_earn.get('date','?')}` — `{_days_e}d` away\n"
                                        f"┣ Premium held: `${pos['premium_collected']:.2f}` × {pos['contracts']} contracts\n"
                                        f"┗ {'🔴 **CLOSE NOW** — earnings within 7 days, IV crush risk' if _flag_e == 'FORCE_CLOSE' else '🟡 **REVIEW** — no new entries on this name, monitor strike distance'}"
                                    )
                                    _earn_color = 0xe74c3c if _flag_e == "FORCE_CLOSE" else 0xf1c40f
                                    if WEBHOOK_INCOME:
                                        send_essentials_embed(
                                            WEBHOOK_INCOME,
                                            "⚠️ EARNINGS WARNING | Open Wheel Position",
                                            _earn_payload, _earn_color,
                                        )
                                        logger.info(f"Earnings warning fired: {pos['symbol']} earn {pos_earn.get('date')} ({_flag_e})")
                    except Exception as _ee:
                        logger.warning(f"Earnings check for open position {pos['symbol']} failed: {_ee}")

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

            # ── MODULE 3: SOCIAL + IV CONVERGENCE WHEEL CANDIDATES ────────────
            # Replaces static 5-ticker watchlist. Pulls today's social-conviction
            # candidates (StockTwits + SS reddit-picks + Finviz + SS leaderboard),
            # runs real IVR from Tradier (fallback: HV30 proxy labeled correctly),
            # and surfaces names where both signals align.
            try:
                import math as _math
                from tradier_client import TradierClient as _TC3
                _tc3 = _TC3()

                _, vixy_z = engine.fetch_vixy_proxy() if hasattr(engine, "fetch_vixy_proxy") else (0, 0)
                if vixy_z < 0.5:
                    iv_env = "LOW"
                    directive = "Selective entries only — watch for HIGH conviction + IVR > 35%."
                elif vixy_z < 1.0:
                    iv_env = "MODERATE"
                    directive = "Active scanning. Prioritize HIGH conviction names with IVR > 35%."
                else:
                    iv_env = "ELEVATED"
                    directive = "Premium environment favorable. HIGH conviction entries are priority."

                plays = engine.generate_trending_options_plays(max_results=8)
                _DTE_MID = 37
                _T = _DTE_MID / 365.0
                _2PI_SQRT = (2 * _math.pi) ** 0.5

                candidate_lines = []   # (meter, ivr, display_line)
                snapshot_top   = []   # [{sym, ivr, strike}] — top HIGH entries for DB
                for play in (plays or []):
                    sym  = play.get("symbol", "")
                    spot = play.get("spot", 0)
                    if not sym or not spot:
                        continue
                    try:
                        meter = play.get("meter", "NEUTRAL")

                        # Real IVR from Tradier (1h cache — no redundant calls).
                        # Falls back to HV30×1.15 proxy; labels clarify which source.
                        ivr_val    = 0.0
                        iv_dec     = 0.0
                        iv_reliable = False
                        iv_label   = ""
                        if _tc3.api_key:
                            try:
                                _ivr_data = _tc3.get_iv_rank(sym, engine.db)
                                _cur_iv   = _ivr_data.get("current_iv", 0.0)
                                if _cur_iv > 0:
                                    ivr_val    = _ivr_data.get("ivr", 0.0)
                                    iv_dec     = _cur_iv
                                    iv_reliable = _ivr_data.get("reliable", False)
                                    iv_label   = f"IVR `{ivr_val:.0f}%`{'✅' if iv_reliable else '~'}"
                            except Exception:
                                pass
                        if iv_dec == 0.0:
                            hv30    = engine.calculate_historical_volatility(sym, lookback=30)
                            iv_dec  = (hv30 * 1.15) / 100.0
                            ivr_val = min(hv30 * 1.15, 99.0)   # NOT a rank — just IV est
                            iv_label = f"IV est `{ivr_val:.0f}%`~proxy"

                        # 0.20-delta put strike at DTE_MID via Black-Scholes approximation
                        strike   = round(spot * _math.exp(-0.84 * iv_dec * _math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
                        # Premium estimate (half-normal approximation of ATM option price)
                        est_prem = round(strike * iv_dec * _math.sqrt(_T) / _2PI_SQRT * 100)

                        ivr_bar = "🟢" if ivr_val >= 35 else ("🟡" if ivr_val >= 20 else "🔴")

                        # CLAUDE.md decision tree: stocks > $100 → credit spread, not naked CSP
                        if spot > 100:
                            setup_tag = f"→ spread: sell `${strike}` / buy `${strike - 5}` put · est `${est_prem}` cr"
                        else:
                            setup_tag = f"→ CSP `${strike}` strike · est `${est_prem}` cr"

                        if meter == "HIGH" and ivr_val >= 35:
                            line = f"┣ {ivr_bar} **{sym}** — HIGH | {iv_label} | Δ0.20 {setup_tag}"
                            snapshot_top.append({"sym": sym, "ivr": int(ivr_val), "strike": strike})
                        elif meter == "HIGH":
                            line = f"┣ {ivr_bar} **{sym}** — HIGH conviction | {iv_label} | IV thin — watch"
                        else:
                            line = f"┣ {ivr_bar} **{sym}** — Watch | {iv_label}"
                        candidate_lines.append((meter, ivr_val, line))
                    except Exception:
                        pass

                # HIGH + IVR>35 first, then HIGH, then Watch; within each tier sort by IVR desc
                candidate_lines.sort(key=lambda x: (
                    0 if (x[0] == "HIGH" and x[1] >= 35) else (1 if x[0] == "HIGH" else 2),
                    -x[1]
                ))
                high_count = sum(1 for m, ivr, _ in candidate_lines if m == "HIGH" and ivr >= 35)

                if candidate_lines:
                    if high_count > 0:
                        sub = f"{high_count} name{'s' if high_count != 1 else ''} where conviction is HIGH and IVR supports premium selling"
                    else:
                        sub = "No HIGH conviction + IVR entries today — stand down on new positions"
                    candidates_payload = (
                        f"**IV Environment:** {iv_env} (VIXY `{vixy_z:+.2f}σ`)\n"
                        f"{sub}\n\n"
                        + "\n".join(l for _, _, l in candidate_lines[:7])
                        + f"\n┗ {directive} Stocks >$100 → spread shown."
                    )
                else:
                    candidates_payload = (
                        f"**IV Environment:** {iv_env} (VIXY `{vixy_z:+.2f}σ`)\n"
                        f"No social candidates surfaced today.\n"
                        f"┗ {directive}"
                    )

                # Persist snapshot for announcements scorecard (zero extra API calls —
                # snapshot_top was built during the loop above from already-fetched HV30)
                try:
                    engine.db.update_state("wheel_candidates_snapshot", {
                        "date":           datetime.now().strftime("%Y-%m-%d"),
                        "high_count":     high_count,
                        "iv_env":         iv_env,
                        "top_candidates": snapshot_top[:3],
                    })
                except Exception as _e:
                    logger.warning(f"wheel_candidates_snapshot write failed: {_e}")

                # ── Kelly position size footer (shared across all candidates) ──
                # Reads PORTFOLIO_VALUE_APPROX from .env (user updates periodically).
                # Falls back to a prompt if not set — no crash, no wrong numbers.
                kelly_footer = ""
                try:
                    _port_val = float(os.getenv("PORTFOLIO_VALUE_APPROX", "0") or "0")
                    _real_vix = engine.fetch_real_vix() or 20.0
                    if _port_val > 0 and high_count > 0:
                        _kelly = engine.kelly_position_size(_port_val, _real_vix)
                        _using = "empirical" if _kelly["using_empirical"] else f"bootstrap prior ({_kelly['sample_trades']} trades)"
                        kelly_footer = (
                            f"\n\n📐 **Kelly Sizing** (per underlying, {_using})\n"
                            f"┣ Win rate: `{_kelly['win_rate']:.0%}` | VIX scalar: `{_kelly['vix_scalar']:.2f}×`\n"
                            f"┗ Max per position: `${_kelly['position_dollars']:,.0f}` ({_kelly['position_pct']:.1f}% of `${_port_val:,.0f}` portfolio)"
                        )
                    elif _port_val == 0:
                        kelly_footer = "\n\n📐 Set `PORTFOLIO_VALUE_APPROX` in .env for Kelly sizing guidance."
                except Exception as _ke:
                    logger.debug(f"Kelly sizing footer failed: {_ke}")

                if WEBHOOK_INCOME:
                    send_essentials_embed(WEBHOOK_INCOME, "🎡 WHEEL CANDIDATES | Social + IV Convergence", candidates_payload + kelly_footer, 0x3498db)
                    logger.info(f"Wheel candidates social+IV dispatched: {high_count} HIGH entries, {len(candidate_lines)} total.")
            except Exception as e:
                logger.error(f"Wheel candidates social+IV post failed: {e}")

            # ── MODULE 4: VIX-ADJUSTED ENTRY PARAMETERS ───────────────────────
            # Tells members WHICH delta and DTE to use TODAY based on VIX regime,
            # cycle scores (from tqqq.py), and daily bias (from market_analysis.py).
            # All reads from DB — zero new API calls.
            try:
                real_vix   = engine.fetch_real_vix() or 20.0
                vix_params = engine.get_vix_adjusted_params(real_vix)
                tier       = vix_params["tier"]
                tier_color = {"LOW": 0x2ecc71, "NORMAL": 0x3498db, "ELEVATED": 0xf1c40f, "PANIC": 0xe74c3c}.get(tier, 0x95a5a6)

                # Cross-script inputs: tqqq.py cycle scores + market_analysis.py daily bias
                bottom_score = int(engine.db.get_state("tqqq_bottom_score") or 0)
                top_score    = int(engine.db.get_state("tqqq_top_score")    or 0)
                bias_data    = engine.db.get_state("market_analysis_bias") or {}
                bias_label   = bias_data.get("label", "NEUTRAL") if isinstance(bias_data, dict) else "NEUTRAL"
                bias_date    = bias_data.get("date",  "") if isinstance(bias_data, dict) else ""

                # Compute enhanced delta directive layering all three inputs:
                # VIX regime is the foundation; cycle score and daily bias refine it.
                base_delta = vix_params["delta_target"]
                if bottom_score >= 65 and bias_label == "BEARISH":
                    # High fear + bearish regime: IV is elevated, go closer to ATM for premium
                    adj_delta   = round(min(base_delta + 0.05, 0.30), 2)
                    delta_note  = f"⬆️ raised to `{adj_delta:.2f}` — high bottom_score ({bottom_score}) + BEARISH bias = thick premium"
                elif top_score >= 65 or bias_label == "BEARISH":
                    # Market stretched or bearish: go further OTM to reduce assignment risk
                    adj_delta   = round(max(base_delta - 0.05, 0.15), 2)
                    delta_note  = f"⬇️ reduced to `{adj_delta:.2f}` — {'top_score ' + str(top_score) + ' ' if top_score >= 65 else ''}{'BEARISH bias' if bias_label == 'BEARISH' else ''} = reduce assignment risk"
                elif bias_label == "BULLISH" and tier in ("LOW", "NORMAL"):
                    # Bullish + calm vol: standard delta, size up slightly
                    adj_delta  = round(base_delta, 2)
                    delta_note = f"✅ standard `{adj_delta:.2f}` — BULLISH bias + {tier} VIX"
                else:
                    adj_delta  = round(base_delta, 2)
                    delta_note = f"✅ standard `{adj_delta:.2f}` — NEUTRAL bias"

                today_str  = datetime.now().strftime("%Y-%m-%d")
                bias_fresh = "✅" if bias_date == today_str else "⚠️ stale"
                vix_payload = (
                    f"VIX Regime: **{tier}** (VIX `{real_vix:.1f}` prev close)\n\n"
                    f"┣ Base Delta (VIX): `{base_delta:.2f}`\n"
                    f"┣ Cycle Scores: bottom `{bottom_score}` | top `{top_score}`\n"
                    f"┣ Daily Bias: `{bias_label}` {bias_fresh}\n"
                    f"┣ **Final Delta: {delta_note}**\n"
                    f"┣ DTE Window: `{vix_params['dte_min']}–{vix_params['dte_max']} days`\n"
                    f"┣ Size Scalar: `{vix_params['size_scalar']:.0%}` of normal position\n"
                    f"┗ Rationale: {'Low vol — can go closer to ATM for premium.' if tier == 'LOW' else 'Elevated vol — go further OTM to reduce assignment risk.' if tier == 'ELEVATED' else 'Panic regime — min size, max OTM. Wait for VIX < 25.' if tier == 'PANIC' else 'Standard parameters.'}"
                )
                if WEBHOOK_INCOME:
                    send_essentials_embed(WEBHOOK_INCOME, "📐 WHEEL PARAMS | VIX + Cycle + Bias-Adjusted", vix_payload, tier_color)
                    logger.info(f"VIX-adjusted params dispatched: {tier} (VIX {real_vix:.1f}) | bias={bias_label} | adj_delta={adj_delta:.2f}.")
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
                        _total_stars = sum(s for s, _ in conviction_lines)
                        _conv_layer_color = COLOR_GREEN if _total_stars > 0 else COLOR_YELLOW
                        send_essentials_embed(
                            WEBHOOK_INCOME,
                            "🔭 CONVICTION LAYER | Inst Flows + Insider Signals",
                            conv_payload, _conv_layer_color
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
                        f"Edge: Selling premium (CSPs, covered calls) statistically favored.\n\n"
                    )
                _best_spread = max(a['spread'] for a in scan_data)
                _crush_color = COLOR_GREEN if _best_spread >= 20 else COLOR_YELLOW
                send_essentials_embed(WEBHOOK_OPTIONS, "VOLATILITY ARBITRAGE TERMINAL | IV Crush Scanner", payload, _crush_color)
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
                    _bull_flow = sum(1 for s in flow_data if s.get("direction", "").upper() in ("CALL", "BULLISH", "BULL"))
                    _bear_flow = sum(1 for s in flow_data if s.get("direction", "").upper() in ("PUT", "BEARISH", "BEAR"))
                    _flow_color = COLOR_GREEN if _bull_flow > _bear_flow else (COLOR_RED if _bear_flow > _bull_flow else COLOR_YELLOW)
                    send_essentials_embed(WEBHOOK_OPTIONS, "INSTITUTIONAL FLOW RADAR | Sweep & OI Intelligence", flow_payload, _flow_color)
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
                    vix_detail = "Premium sellers in a drought. Wait for IV expansion before wheeling."
                elif vix_z < 0.75:
                    vix_env = "MODERATE VOLATILITY"
                    vix_detail = "Balanced premium. Credit spreads statistically favorable."
                else:
                    vix_env = "ELEVATED VOLATILITY"
                    vix_detail = "Rich premium. Ideal for CSPs and covered calls — wheel conditions optimal."

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
                _flowstate_color = COLOR_GREEN if vix_z >= 0.75 else COLOR_YELLOW
                send_essentials_embed(WEBHOOK_OPTIONS, "Options Market Flowstate", outlook_payload, _flowstate_color)
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

                    # ── Tier 3 exit Pushover alert (Pushover only, weekly dedup) ──
                    # Triggers when all three exit conditions converge:
                    #   BTC dominance < 40% (alt-season peak) AND
                    #   Extreme Greed streak >= 3 days AND
                    #   cycle top score >= 80 (system confirms overbought)
                    if ct_score >= 80 and dom < 40.0 and streak >= 3:
                        _exit_week = datetime.now().strftime("%Y-W%W")
                        _exit_key  = f"tier3_exit_alert_{_exit_week}"
                        if not engine.db.get_state(_exit_key):
                            engine.db.update_state(_exit_key, True)
                            _p_tok = os.getenv("PUSHOVER_API_TOKEN")
                            _p_usr = os.getenv("PUSHOVER_USER_KEY")
                            if _p_tok and _p_usr:
                                try:
                                    import requests as _req
                                    _req.post(
                                        "https://api.pushover.net/1/messages.json",
                                        data={
                                            "token": _p_tok, "user": _p_usr,
                                            "title": "🔴 TIER 3 EXIT SIGNAL",
                                            "message": (
                                                f"Cycle top score {ct_score}/100\n"
                                                f"BTC dominance {dom:.1f}% (<40%)\n"
                                                f"Extreme Greed {streak}d streak\n"
                                                "Action: exit BITA/YBTC — rotate cash to margin paydown"
                                            ),
                                            "priority": 1,
                                        },
                                        timeout=10,
                                    )
                                    logger.info(f"Tier 3 exit Pushover fired — score {ct_score}, dom {dom:.1f}%, streak {streak}d")
                                except Exception as _pe:
                                    logger.warning(f"Tier 3 exit Pushover failed: {_pe}")
                except Exception as e:
                    logger.warning(f"Crypto cycle top score failed: {e}")

                payload += (
                    "─────────────────────────\n"
                    "Sources: Alternative.me · Reddit r/Cryptocurrency · Binance FAPI · CoinGecko · Twelve Data\n"
                    "Not financial advice — for informational/educational use only."
                )

                if WEBHOOK_CRYPTO:
                    _crypto_color = COLOR_GREEN if ct_score < 40 else (COLOR_RED if ct_score >= 70 else COLOR_YELLOW)
                    send_essentials_embed(WEBHOOK_CRYPTO, "CRYPTO DESK | Social + Funding + Derivatives", payload, _crypto_color)
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
                        _buzz_avg = sum(p.get("chg_5d", 0) for p in plays[:8]) / max(len(plays[:8]), 1)
                        _buzz_color = COLOR_GREEN if _buzz_avg > 0.5 else (COLOR_RED if _buzz_avg < -0.5 else COLOR_YELLOW)
                        send_essentials_embed(WEBHOOK_FUTURES, "FUTURES DESK | Commodity & Macro Buzz", payload, _buzz_color)
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
                        _pat_color = COLOR_GREEN if len(bullish) > len(bearish) else (COLOR_RED if len(bearish) > len(bullish) else COLOR_YELLOW)
                        send_essentials_embed(WEBHOOK_FUTURES, "FUTURES DESK | S&P Pattern Scan", pat_payload, _pat_color)
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

        elif args.mode == "mlpi_entry":
            # ── MLPI ENTRY SIGNAL ─────────────────────────────────────────────
            # Fires when energy sector (XLE) drops ≥ 1.5% OR yield curve steepens
            # ≥ 20bps in one session, AND MLPI itself is down ≥ 0.5%.
            # Both conditions produce the same outcome: MLPI cheaper than yesterday.
            # Notification: Pushover (personal alert) + Discord #dividend-ccetfs.
            # Runs 2× per RTH day (10:30 HST + 14:00 HST). 24h dedupe via DB.
            try:
                dedupe_key = f"mlpi_entry_fired_{datetime.now().strftime('%Y-%m-%d')}"
                if engine.db.get_state(dedupe_key):
                    logger.info("MLPI entry: already fired today — skipping.")
                else:
                    xle_data  = engine._execute_query("price", {"symbol": "XLE"})
                    mlpi_data = engine._execute_query("price", {"symbol": "MLPI"})
                    xle_chg   = float((xle_data  or {}).get("percent_change", 0.0))
                    mlpi_chg  = float((mlpi_data or {}).get("percent_change", 0.0))
                    xle_price = float((xle_data  or {}).get("price", 0.0))
                    mlpi_price = float((mlpi_data or {}).get("price", 0.0))

                    # XLE RSI for oversold confirmation
                    xle_rsi = None
                    try:
                        rsi_data = engine._execute_query("rsi", {"symbol": "XLE", "interval": "1day", "time_period": 14})
                        xle_rsi  = float((rsi_data or {}).get("rsi", 50.0))
                    except Exception:
                        pass

                    # Yield curve steepening from cached DB value (cross_asset.py writes daily)
                    today_str  = datetime.now().strftime("%Y-%m-%d")
                    yc_spread  = engine.db.get_state("fred_yield_spread")
                    yc_prev    = engine.db.get_state("fred_yield_spread_prev")
                    yc_date    = engine.db.get_state("fred_yield_spread_date")
                    rate_spike = (
                        yc_date == today_str
                        and yc_spread is not None and yc_prev is not None
                        and (float(yc_spread) - float(yc_prev)) >= 0.20
                    )
                    energy_red = xle_chg <= -1.5
                    mlpi_down  = mlpi_chg <= -0.5

                    if (energy_red or rate_spike) and mlpi_down:
                        triggers = []
                        if energy_red:
                            triggers.append(f"XLE {xle_chg:+.1f}%")
                        if rate_spike:
                            bps = (float(yc_spread) - float(yc_prev)) * 100
                            triggers.append(f"T10-T2 +{bps:.0f}bps rate spike")
                        rsi_note = f" | XLE RSI `{xle_rsi:.0f}` {'🟢 oversold' if xle_rsi and xle_rsi < 40 else ''}" if xle_rsi else ""

                        payload = (
                            f"Energy sector + rate conditions align for MLPI accumulation.\n\n"
                            f"┣ Triggers: {' | '.join(triggers)}\n"
                            f"┣ MLPI: `${mlpi_price:.2f}` ({mlpi_chg:+.1f}% session){rsi_note}\n"
                            f"┣ XLE: `${xle_price:.2f}` ({xle_chg:+.1f}% session)\n"
                            f"┣ Yield: `{float(yc_spread):.2f}%` T10-T2 spread\n"
                            f"┣ Action: Cash buy only — no new margin for Tier 2 entries\n"
                            f"┗ Sizing: 1 tranche. Watch for 3-session XLE weakness for full position."
                        )
                        if WEBHOOK_INCOME:
                            send_essentials_embed(WEBHOOK_INCOME, "🛢️ MLPI ENTRY WINDOW | Accumulation Signal", payload, COLOR_YELLOW)

                        # Pushover personal alert (financial signal — direct notification)
                        try:
                            import requests as _req
                            pushover_token = os.getenv("PUSHOVER_APP_TOKEN", "")
                            pushover_user  = os.getenv("PUSHOVER_USER_KEY", "")
                            if pushover_token and pushover_user:
                                msg = f"🛢️ MLPI ENTRY — {' | '.join(triggers)} | MLPI ${mlpi_price:.2f} ({mlpi_chg:+.1f}%) — Cash buy window open."
                                _req.post("https://api.pushover.net/1/messages.json", data={
                                    "token": pushover_token, "user": pushover_user,
                                    "message": msg, "title": "MLPI Accumulation Signal",
                                    "priority": 1,
                                }, timeout=10)
                        except Exception as pe:
                            logger.warning(f"MLPI Pushover alert failed: {pe}")

                        engine.db.update_state(dedupe_key, True)
                        logger.info(f"MLPI entry signal fired: {', '.join(triggers)} | MLPI {mlpi_chg:+.1f}%")
                    else:
                        conds = []
                        if not energy_red:  conds.append(f"XLE {xle_chg:+.1f}% (need ≤ -1.5%)")
                        if not rate_spike:  conds.append("no rate spike")
                        if not mlpi_down:   conds.append(f"MLPI {mlpi_chg:+.1f}% (need ≤ -0.5%)")
                        logger.info(f"MLPI entry: conditions not met — {' | '.join(conds)}")
            except Exception as e:
                logger.error(f"mlpi_entry mode failed: {e}")

        elif args.mode == "personal_scorecard":
            # ── PERSONAL SCORECARD → Pushover ONLY (never Discord) ────────────
            # Weekly snapshot of all 3 strategy pillars. Financial data stays private.
            # Designed to run Sundays at 18:00 HST (PA cron: 04:00 UTC Monday).
            # All data sourced from DB — zero new API calls.
            try:
                from datetime import date as _date
                today_s = _date.today().isoformat()

                # ── Strategy 1: CLM/CRF Carry Health ──────────────────────────
                carry_raw = engine.db.get_state("carry_spread_data") or {}
                carry_sp  = carry_raw.get("spread")
                carry_mr  = carry_raw.get("margin_rate")
                carry_t2  = carry_raw.get("tier2_yield")
                carry_dt  = carry_raw.get("date", "N/A")
                carry_icon = "✅" if carry_sp and carry_sp >= 5.0 else ("⚠️" if carry_sp and carry_sp >= 2.0 else "🚨")
                carry_line = (
                    f"{carry_icon} Carry: {carry_t2:.1f}% − {carry_mr:.2f}% = {carry_sp:+.1f}% ({carry_dt})"
                    if carry_sp is not None else "Carry: data pending"
                )

                # CLM/CRF z-scores from DB (monitor.py writes these)
                clm_z = engine.db.get_state("clm_premium_z") or "N/A"
                crf_z = engine.db.get_state("crf_premium_z") or "N/A"
                hy    = engine.db.get_state("hy_spread_cached") or "N/A"
                try: hy_str = f"{float(hy):.2f}%"
                except Exception: hy_str = str(hy)

                strat1 = (
                    f"STRATEGY 1 — CLM/CRF SNOWBALL\n"
                    f"  {carry_line}\n"
                    f"  CLM z-score: {clm_z} | CRF z-score: {crf_z}\n"
                    f"  HY spread: {hy_str}"
                )

                # ── Strategy 2: Wheel Performance ─────────────────────────────
                open_pos   = engine.db.get_open_wheel_positions() or []
                total_prem = engine.db.get_total_premium_collected() or 0.0
                try:
                    dist = engine.db.get_wheel_outcome_distribution(lookback_days=90) or []
                    wins = sum(d["count"] for d in dist if d["outcome"] in ("CLOSED", "EXPIRED"))
                    total_closed = sum(d["count"] for d in dist)
                    win_rate_str = f"{wins/total_closed:.0%}" if total_closed > 0 else "N/A"
                except Exception:
                    wins, total_closed, win_rate_str = 0, 0, "N/A"

                ivr_accum = engine.db.get_state("iv_daily_count") or "building"
                kelly_val = ""
                try:
                    _pv = float(os.getenv("PORTFOLIO_VALUE_APPROX", "0") or "0")
                    _rv = engine.fetch_real_vix() or 20.0
                    if _pv > 0:
                        _k = engine.kelly_position_size(_pv, _rv)
                        kelly_val = f"\n  Kelly size: ${_k['position_dollars']:,.0f} ({_k['position_pct']:.1f}%) | VIX scalar {_k['vix_scalar']:.2f}×"
                except Exception:
                    pass

                strat2 = (
                    f"STRATEGY 2 — WHEEL\n"
                    f"  Open positions: {len(open_pos)}\n"
                    f"  Total premium collected (all-time): ${total_prem:,.2f}\n"
                    f"  90-day win rate: {win_rate_str} ({wins}/{total_closed} closed){kelly_val}\n"
                    f"  IVR history: {ivr_accum} trading days stored"
                )

                # ── Strategy 3: TQQQ Cycle Posture ────────────────────────────
                bottom = int(engine.db.get_state("tqqq_bottom_score") or 0)
                top    = int(engine.db.get_state("tqqq_top_score")    or 0)
                bias   = (engine.db.get_state("market_analysis_bias") or {})
                bias_l = bias.get("label", "NEUTRAL") if isinstance(bias, dict) else "NEUTRAL"
                real_v = engine.fetch_real_vix() or "N/A"
                from tqqq import get_leap_seasonal_params as _gsp
                _scalar, _seas_note = _gsp()
                strat3 = (
                    f"STRATEGY 3 — TQQQ LEAP DESK\n"
                    f"  Bottom score: {bottom}/100 | Top score: {top}/100\n"
                    f"  Daily bias: {bias_l} | VIX: {real_v:.1f}\n"
                    f"  Seasonal: {_seas_note}"
                )

                # ── Carry spread alert flag ────────────────────────────────────
                alert_note = ""
                if carry_sp is not None and carry_sp < 5.0:
                    alert_note = f"\n⚠️ CARRY SPREAD COMPRESSED ({carry_sp:+.1f}%) — review margin draw pace"

                msg = f"{strat1}\n\n{strat2}\n\n{strat3}{alert_note}"

                _p_tok = os.getenv("PUSHOVER_API_TOKEN")
                _p_usr = os.getenv("PUSHOVER_USER_KEY")
                if _p_tok and _p_usr:
                    import requests as _rq
                    _rq.post(
                        "https://api.pushover.net/1/messages.json",
                        data={
                            "token": _p_tok, "user": _p_usr,
                            "title": f"📊 Personal Scorecard — {today_s}",
                            "message": msg,
                            "priority": 0,
                        },
                        timeout=15,
                    )
                    logger.info("Personal scorecard dispatched via Pushover.")
                else:
                    logger.warning("Personal scorecard: PUSHOVER_API_TOKEN or PUSHOVER_USER_KEY not set.")

            except Exception as e:
                logger.error(f"personal_scorecard mode failed: {e}")

    except Exception as e:
        logger.critical(f"Task Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

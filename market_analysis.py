"""
market_analysis.py — Always-on Morning Command Center.

Runs as the 6th PythonAnywhere always-on task. Internal 60-second tick loop
with DB-deduped firing at three daily windows:

  18:00 UTC (08:00 HST) → Full morning synthesis brief
  20:20 UTC (10:20 HST) → Mid-session pulse (intraday context update)
  23:40 UTC (13:40 HST) → EOD brief (market close recap)

Synthesizes ALL ecosystem feeds into a single #market-analysis embed:
  • FRED macro (VIX, yield curve, Fed Funds, HY spread)
  • VIXY z-score regime
  • SPY/QQQ premarket quotes (Twelve Data)
  • Fear & Greed (Alternative.me)
  • CLM/CRF premium z-score (from monitor.py DB state)
  • TQQQ cycle scores (from tqqq.py DB state)
  • Wheel open positions (from DB)
  • Bias-flag scoring → BULLISH / NEUTRAL / BEARISH posture label

PythonAnywhere CPU rules: REST only, no SDK threads, all FRED cached daily.
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("MarketAnalysis")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s [MarketAnalysis] %(levelname)s %(message)s"))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
TWELVE_DATA_API_KEY     = os.getenv("TWELVE_DATA_API_KEY")

# Fire times (UTC hour, minute) and their DB dedup keys
FIRE_SCHEDULE = [
    (12,  0, "ma_morning",   "morning"),   # 08:00 ET pre-market | 02:00 HST
    (17,  0, "ma_intraday",  "intraday"),  # 13:00 ET mid-session | 07:00 HST
    (21, 30, "ma_eod",       "eod"),       # 17:30 ET after close | 11:30 HST
]
FIRE_WINDOW_MIN = 2   # ± minutes around target time


# ── Helpers ───────────────────────────────────────────────────────────────────

def _in_window(now_h: int, now_m: int, t_h: int, t_m: int) -> bool:
    return abs((now_h * 60 + now_m) - (t_h * 60 + t_m)) <= FIRE_WINDOW_MIN


def _already_fired(db: EcosystemDatabase, key: str, date_str: str) -> bool:
    return bool(db.get_state(f"mktana_fired_{key}_{date_str}"))


def _mark_fired(db: EcosystemDatabase, key: str, date_str: str):
    db.update_state(f"mktana_fired_{key}_{date_str}", True)


def _send_embed(title: str, description: str, color: int):
    if not WEBHOOK_MARKET_ANALYSIS:
        logger.warning("WEBHOOK_MARKET_ANALYSIS not set — skipping Discord dispatch.")
        return
    try:
        payload = {
            "embeds": [{
                "title":       title,
                "description": description,
                "color":       color,
                "footer":      {"text": "Not financial advice — educational/informational use only."},
            }]
        }
        r = requests.post(WEBHOOK_MARKET_ANALYSIS, json=payload, timeout=10)
        if r.status_code not in (200, 204):
            logger.error(f"Discord dispatch failed: HTTP {r.status_code} — {r.text[:200]}")
        else:
            logger.info(f"Dispatched: {title}")
    except Exception as e:
        logger.error(f"Discord dispatch error: {e}")


# ── Data Fetchers ─────────────────────────────────────────────────────────────

def _fetch_fear_and_greed() -> tuple:
    """Returns (value: int, classification: str). Fallback (50, 'Neutral')."""
    try:
        r = requests.get("https://api.alternative.me/fng/", timeout=8).json()
        val   = int(r["data"][0]["value"])
        label = r["data"][0]["value_classification"]
        return val, label
    except Exception as e:
        logger.warning(f"Fear & Greed fetch failed: {e}")
        return 50, "Neutral"


def _fetch_spy_qqq_quote(engine: HighFidelityAnalyticsEngine) -> dict:
    """
    Twelve Data /quote for SPY and QQQ — returns price + percent_change.
    Uses engine's cached quote method to avoid redundant API hits.
    """
    result = {}
    for sym in ["SPY", "QQQ"]:
        try:
            url = f"https://api.twelvedata.com/quote?symbol={sym}&apikey={TWELVE_DATA_API_KEY}"
            r = requests.get(url, timeout=10).json()
            if r.get("status") == "error":
                continue
            result[sym] = {
                "price":          float(r.get("close", 0) or r.get("price", 0) or 0),
                "percent_change": float(r.get("percent_change", 0) or 0),
                "previous_close": float(r.get("previous_close", 0) or 0),
                "open":           float(r.get("open", 0) or 0),
            }
        except Exception as e:
            logger.warning(f"Quote fetch failed for {sym}: {e}")
    return result


def _fetch_futures_context(engine: HighFidelityAnalyticsEngine) -> dict:
    """
    /NQ and /ES proxies via QQQ + SPY futures quotes from Twelve Data.
    Returns direction flags for the bias scorer.
    """
    try:
        # Use existing SPY/QQQ data from engine cache if available
        spy_data = {}
        qqq_data = {}
        for sym, store in [("SPY", spy_data), ("QQQ", qqq_data)]:
            try:
                url = f"https://api.twelvedata.com/quote?symbol={sym}&apikey={TWELVE_DATA_API_KEY}"
                r = requests.get(url, timeout=10).json()
                chg = float(r.get("percent_change", 0) or 0)
                store["chg"] = chg
            except Exception:
                store["chg"] = 0.0
        return {"spy_chg": spy_data.get("chg", 0.0), "qqq_chg": qqq_data.get("chg", 0.0)}
    except Exception:
        return {"spy_chg": 0.0, "qqq_chg": 0.0}


# ── Bias-Flag Scoring ─────────────────────────────────────────────────────────

def _calculate_bias_score(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> dict:
    """
    8-10 boolean flags, each weighted. Sum → bias_score → label.
    BULLISH: ≥ +20 | NEUTRAL: -19 to +19 | BEARISH: ≤ -20

    Returns dict with bias_score, label, flags_detail, and raw signal values.
    """
    score   = 0
    details = []
    signals = {}

    # ── 1. FRED Real VIX ──────────────────────────────────────────────────────
    try:
        real_vix = engine.fetch_real_vix()
        if real_vix is None:
            real_vix = float(db.get_state("fred_vix_value") or 20.0)
        signals["real_vix"] = real_vix
        if real_vix < 15:
            score += 20
            details.append(f"VIX {real_vix:.1f} → LOW vol regime (+20)")
        elif real_vix < 20:
            score += 10
            details.append(f"VIX {real_vix:.1f} → calm (+10)")
        elif real_vix > 30:
            score -= 20
            details.append(f"VIX {real_vix:.1f} → PANIC regime (-20)")
        elif real_vix > 22:
            score -= 10
            details.append(f"VIX {real_vix:.1f} → elevated (-10)")
    except Exception as e:
        logger.warning(f"Bias: VIX fetch failed: {e}")
        real_vix = 20.0
        signals["real_vix"] = real_vix

    # ── 2. VIXY z-score (intraday fear regime) ────────────────────────────────
    try:
        vixy_price, vixy_z = engine.fetch_vixy_proxy()
        signals["vixy_z"] = vixy_z
        if vixy_z < -0.5:
            score += 10
            details.append(f"VIXY z {vixy_z:+.2f}σ → suppressed fear (+10)")
        elif vixy_z > 1.5:
            score -= 15
            details.append(f"VIXY z {vixy_z:+.2f}σ → fear spike (-15)")
        elif vixy_z > 0.8:
            score -= 7
            details.append(f"VIXY z {vixy_z:+.2f}σ → rising fear (-7)")
    except Exception as e:
        logger.warning(f"Bias: VIXY failed: {e}")
        vixy_price, vixy_z = 20.0, 0.0
        signals["vixy_z"] = 0.0

    # ── 3. Yield curve (FRED) ─────────────────────────────────────────────────
    try:
        yc = engine.fetch_yield_curve()
        signals["yield_spread"] = yc["spread"] if yc else None
        if yc:
            if not yc["inverted"] and yc["spread"] > 0.5:
                score += 10
                details.append(f"Yield curve +{yc['spread']:.2f}% → normal (+10)")
            elif yc["inverted"] and yc["spread"] < -0.3:
                score -= 10
                details.append(f"Yield curve {yc['spread']:.2f}% → inverted (-10)")
    except Exception as e:
        logger.warning(f"Bias: yield curve failed: {e}")

    # ── 4. HY credit spread (FRED) ────────────────────────────────────────────
    try:
        hy = engine.fetch_hy_spread()
        signals["hy_spread"] = hy
        if hy > 0:
            if hy < 4.0:
                score += 10
                details.append(f"HY spread {hy:.2f}% → healthy credit (+10)")
            elif hy > 6.0:
                score -= 15
                details.append(f"HY spread {hy:.2f}% → credit stress (-15)")
            elif hy > 5.0:
                score -= 7
                details.append(f"HY spread {hy:.2f}% → credit caution (-7)")
    except Exception as e:
        logger.warning(f"Bias: HY spread failed: {e}")

    # ── 5. Fear & Greed ───────────────────────────────────────────────────────
    try:
        fg_val, fg_class = _fetch_fear_and_greed()
        signals["fear_greed"] = fg_val
        if fg_val >= 70:
            score += 10
            details.append(f"F&G {fg_val} ({fg_class}) → greed (+10)")
        elif fg_val <= 30:
            score -= 10
            details.append(f"F&G {fg_val} ({fg_class}) → fear (-10)")
        elif fg_val >= 55:
            score += 5
            details.append(f"F&G {fg_val} → mild greed (+5)")
        elif fg_val <= 45:
            score -= 5
            details.append(f"F&G {fg_val} → mild fear (-5)")
    except Exception as e:
        logger.warning(f"Bias: F&G failed: {e}")
        fg_val, fg_class = 50, "Neutral"
        signals["fear_greed"] = 50

    # ── 6. SPY premarket direction ────────────────────────────────────────────
    try:
        futures = _fetch_futures_context(engine)
        spy_chg = futures["spy_chg"]
        qqq_chg = futures["qqq_chg"]
        signals["spy_chg"] = spy_chg
        signals["qqq_chg"] = qqq_chg
        if spy_chg > 0.5:
            score += 15
            details.append(f"SPY {spy_chg:+.2f}% → strong premarket (+15)")
        elif spy_chg > 0.1:
            score += 7
            details.append(f"SPY {spy_chg:+.2f}% → mild premarket bid (+7)")
        elif spy_chg < -0.8:
            score -= 15
            details.append(f"SPY {spy_chg:+.2f}% → strong premarket sell (-15)")
        elif spy_chg < -0.3:
            score -= 7
            details.append(f"SPY {spy_chg:+.2f}% → mild premarket weakness (-7)")
    except Exception as e:
        logger.warning(f"Bias: SPY/QQQ failed: {e}")
        spy_chg, qqq_chg = 0.0, 0.0
        signals.update({"spy_chg": 0.0, "qqq_chg": 0.0})

    # ── 7. CLM/CRF premium z-score (from monitor.py DB) ──────────────────────
    try:
        clm_z = float(db.get_state("clm_last_z_premium") or 0.0)
        crf_z = float(db.get_state("crf_last_z_premium") or 0.0)
        avg_z = (clm_z + crf_z) / 2
        signals["cef_premium_z"] = round(avg_z, 2)
        if avg_z >= 2.0:
            score -= 10
            details.append(f"CLM/CRF premium z {avg_z:+.1f}σ → RO risk elevated (-10)")
        elif avg_z < 0:
            score += 5
            details.append(f"CLM/CRF premium z {avg_z:+.1f}σ → below mean (+5)")
    except Exception as e:
        logger.warning(f"Bias: CLM/CRF z-score read failed: {e}")

    # ── 8. TQQQ cycle signal (from tqqq.py DB) ───────────────────────────────
    try:
        bottom_score = int(db.get_state("tqqq_bottom_score") or 0)
        top_score    = int(db.get_state("tqqq_top_score") or 0)
        signals["tqqq_bottom"] = bottom_score
        signals["tqqq_top"]    = top_score
        if bottom_score >= 55:
            score += 10
            details.append(f"TQQQ bottom score {bottom_score}/100 → CALL desk unlocked (+10)")
        if top_score >= 55:
            score -= 10
            details.append(f"TQQQ top score {top_score}/100 → PUT desk unlocked (-10)")
    except Exception as e:
        logger.warning(f"Bias: TQQQ cycle read failed: {e}")

    # ── Label ─────────────────────────────────────────────────────────────────
    if score >= 20:
        label = "BULLISH"
        color = 0x2ecc71
    elif score <= -20:
        label = "BEARISH"
        color = 0xe74c3c
    else:
        label = "NEUTRAL"
        color = 0xf1c40f

    return {
        "bias_score":  score,
        "label":       label,
        "color":       color,
        "details":     details,
        "signals":     signals,
        "fg_val":      fg_val,
        "fg_class":    fg_class,
        "vixy_z":      vixy_z,
        "vixy_price":  vixy_price,
        "real_vix":    real_vix,
        "spy_chg":     spy_chg,
        "qqq_chg":     qqq_chg,
    }


# ── Report Builders ───────────────────────────────────────────────────────────

def _build_morning_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    Full morning synthesis brief (0800 HST). Returns (title, description, color).
    """
    now_label = datetime.now().strftime("%a %b %-d | %H:%M HST")

    bias = _calculate_bias_score(engine, db)

    # Write daily bias to DB — scheduler.py wheel_signals Module 4 reads this to
    # layer directional posture on top of VIX-adjusted delta parameters.
    try:
        db.update_state("market_analysis_bias", {
            "label": bias["label"],
            "score": bias["bias_score"],
            "date":  datetime.now().strftime("%Y-%m-%d"),
        })
    except Exception:
        pass

    sigs = bias["signals"]

    # ── MACRO ENVIRONMENT ─────────────────────────────────────────────────────
    try:
        yc   = engine.fetch_yield_curve()
        snap = engine.fetch_fred_macro_snapshot()
        hy   = engine.fetch_hy_spread()
        real_vix = bias["real_vix"]
        vix_line = f"`{real_vix:.1f}` (prev close) — {'Calm. Options cheap.' if real_vix < 15 else 'Low vol.' if real_vix < 20 else 'Elevated. Size down.' if real_vix < 30 else 'PANIC. Defensive posture.'}"
        yc_line  = f"`{yc['spread']:+.2f}%` {yc['label']}" if yc else "N/A"
        ff_line  = f"`{snap.get('fedfunds', '?')}%` Fed Funds"
        hy_line  = f"`{hy:.2f}%` {'✅ healthy' if hy < 4.5 else '⚠️ stress' if hy < 6 else '🔴 crisis'}" if hy else "N/A"
        cpi_line = f"`{snap.get('cpi_yoy', '?')}%` CPI YoY" if snap.get("cpi_yoy") else ""
    except Exception as e:
        logger.warning(f"Morning: macro section failed: {e}")
        yc_line = hy_line = ff_line = cpi_line = "N/A"
        vix_line = f"`{bias['real_vix']:.1f}`"

    macro_section = (
        "**MACRO ENVIRONMENT**\n"
        f"┣ VIX: {vix_line}\n"
        f"┣ Yield Curve: {yc_line}\n"
        f"┣ {ff_line} | HY Spread: {hy_line}\n"
    )
    if cpi_line:
        macro_section += f"┗ {cpi_line}\n"
    else:
        macro_section = macro_section.rstrip("┣ \n").replace("┣ VIXY", "┗ VIXY") + "\n"
        # Properly close the last line
        macro_section = _swap_last_bullet(macro_section)

    # ── EQUITY PULSE ──────────────────────────────────────────────────────────
    spy_chg = bias["spy_chg"]
    qqq_chg = bias["qqq_chg"]
    vixy_z  = bias["vixy_z"]
    fg_val  = bias["fg_val"]
    fg_class = bias["fg_class"]

    def _arrow(v): return "▲" if v >= 0 else "▼"
    vol_label = "Low vol regime" if vixy_z < -0.5 else ("Rising fear" if vixy_z > 1.0 else "Normal vol")

    equity_section = (
        "\n**EQUITY PULSE**\n"
        f"┣ SPY: {_arrow(spy_chg)}{abs(spy_chg):.2f}% session | "
        f"QQQ: {_arrow(qqq_chg)}{abs(qqq_chg):.2f}% session\n"
        f"┣ VIXY z-score: `{vixy_z:+.2f}σ` — {vol_label}\n"
        f"┗ Fear & Greed: `{fg_val}` ({fg_class})\n"
    )

    # ── CROSS-CHANNEL SIGNALS ─────────────────────────────────────────────────
    try:
        clm_z     = float(db.get_state("clm_last_z_premium") or 0.0)
        crf_z     = float(db.get_state("crf_last_z_premium") or 0.0)
        clm_ro    = db.get_state("clm_last_ro_tier") or "LOW"
        crf_ro    = db.get_state("crf_last_ro_tier") or "LOW"
        clm_prem  = float(db.get_state("clm_last_premium") or 0.0)
        crf_prem  = float(db.get_state("crf_last_premium") or 0.0)
        clm_score = int(db.get_state("clm_last_ro_score") or 0)
        crf_score = int(db.get_state("crf_last_ro_score") or 0)
        cef_line = (
            f"CLM z:`{clm_z:+.1f}σ` prem:`{clm_prem:.1f}%` RO:`{clm_score}/100` ({clm_ro}) | "
            f"CRF z:`{crf_z:+.1f}σ` prem:`{crf_prem:.1f}%` RO:`{crf_score}/100` ({crf_ro})"
        )
    except Exception:
        cef_line = "CLM/CRF: data pending monitor.py pulse"

    try:
        bottom_score = int(db.get_state("tqqq_bottom_score") or 0)
        top_score    = int(db.get_state("tqqq_top_score") or 0)
        call_locked  = "🟢 UNLOCKED" if bottom_score >= 55 else f"🔒 locked ({bottom_score}/100)"
        put_locked   = "🟢 UNLOCKED" if top_score >= 55 else f"🔒 locked ({top_score}/100)"
        tqqq_line    = f"CALL desk {call_locked} | PUT desk {put_locked}"
    except Exception:
        tqqq_line = "TQQQ: awaiting cycle update"

    try:
        open_pos     = db.get_open_wheel_positions()
        pos_count    = len(open_pos)
        nearest_exp  = None
        notional     = 0.0
        if open_pos:
            today = datetime.now().date()
            exps  = []
            for p in open_pos:
                try:
                    d = datetime.strptime(p["expiration"], "%Y-%m-%d").date()
                    exps.append((d - today).days)
                except Exception:
                    pass
                try:
                    notional += float(p.get("strike", 0)) * int(p.get("contracts", 1)) * 100
                except Exception:
                    pass
            if exps:
                nearest_exp = min(exps)
        notional_str = f" | Notional: `${notional:,.0f}`" if notional > 0 else ""
        wheel_line = (
            f"{pos_count} open position{'s' if pos_count != 1 else ''}"
            + (f" | Nearest exp: {nearest_exp}d" if nearest_exp is not None else "")
            + notional_str
        ) if pos_count > 0 else "No open positions"
    except Exception:
        wheel_line = "Wheel: DB read pending"

    # MLPI entry signal — uses price data already fetched for bias scorer (no extra API calls).
    # Fires when energy sector (XLE) drops ≥ 1.5% OR yield curve steepened ≥ 20bps,
    # AND MLPI itself is also down (better entry price). All reads from cached DB values.
    mlpi_entry_line = ""
    try:
        xle_data  = engine._execute_query("price", {"symbol": "XLE"})
        mlpi_data = engine._execute_query("price", {"symbol": "MLPI"})
        xle_chg   = float((xle_data  or {}).get("percent_change", 0.0))
        mlpi_chg  = float((mlpi_data or {}).get("percent_change", 0.0))
        today_str = datetime.now().strftime("%Y-%m-%d")
        yc_spread = db.get_state("fred_yield_spread")
        yc_prev   = db.get_state("fred_yield_spread_prev")
        yc_date   = db.get_state("fred_yield_spread_date")
        rate_spike = (
            yc_date == today_str
            and yc_spread is not None and yc_prev is not None
            and (float(yc_spread) - float(yc_prev)) >= 0.20
        )
        energy_red = xle_chg <= -1.5
        mlpi_down  = mlpi_chg <= -0.5
        if (energy_red or rate_spike) and mlpi_down:
            triggers = []
            if energy_red:  triggers.append(f"XLE {xle_chg:+.1f}%")
            if rate_spike:  triggers.append(f"T10-T2 +{float(yc_spread)-float(yc_prev):.2f}% rate spike")
            mlpi_entry_line = (
                f"┣ 🛢️ MLPI ENTRY WINDOW — {' | '.join(triggers)} | MLPI {mlpi_chg:+.1f}% — "
                f"Accumulation conditions. Cash buy (no new margin).\n"
            )
    except Exception:
        pass

    signals_section = (
        "\n**CROSS-CHANNEL SIGNALS**\n"
        f"┣ CLM/CRF: {cef_line}\n"
        f"┣ TQQQ: {tqqq_line}\n"
        f"┣ Wheel: {wheel_line}\n"
        f"{mlpi_entry_line}"
        f"┗ Synthesized: #cornerstone · #crypto · #futures · #options-wheel\n"
    )

    # ── BIAS + DIRECTIVES ─────────────────────────────────────────────────────
    vix_params = engine.get_vix_adjusted_params(bias["real_vix"])
    wheel_directive = (
        f"Δ {vix_params['delta_target']:.2f} | {vix_params['dte_min']}–{vix_params['dte_max']} DTE | "
        f"{vix_params['size_scalar']:.0%} size ({vix_params['tier']} VIX regime)"
    )
    score_sign = f"+{bias['bias_score']}" if bias['bias_score'] >= 0 else str(bias['bias_score'])
    directive_label = {
        "BULLISH":  "Favor longs and wheel setups. Bias toward calls on dips.",
        "BEARISH":  "Defensive posture. No new margin draws. Watch puts.",
        "NEUTRAL":  "Selective entries only. Wait for clearer bias before sizing up.",
    }[bias["label"]]

    directives_section = (
        f"\n**TODAY'S POSTURE: {bias['label']} (Score: {score_sign})**\n"
        f"┣ Bias: {directive_label}\n"
        f"┗ Wheel params: {wheel_directive}\n"
    )

    # ── SENTISENSE CONFLUENCE BLOCK ───────────────────────────────────────────
    # Market Mood + Congressional trades. One API call each (cached daily).
    # Adds the "stars align" layer: when macro + market mood + insider/political
    # activity all converge, it reinforces the posture with external data.
    ss_section = ""
    try:
        import sentisense_client as ss

        # Market Mood — proprietary equity-native fear/greed (not crypto-origin)
        mood = ss.get_market_mood(db)
        if mood:
            mood_emoji = "🔴" if mood["score"] <= 25 else ("🟢" if mood["score"] >= 75 else "🟡")
            mood_line  = f"┣ Market Mood: {mood_emoji} `{mood['score']}` · {mood['label']} — {mood['signal']}\n"
        else:
            mood_line = ""

        # Congressional trades — top 4 most recent disclosures
        trades = ss.get_congressional_trades(db, limit=4)
        trade_lines = ""
        if trades:
            trade_lines = "┣ Congressional trades (STOCK Act):\n"
            for t in trades:
                party_tag = f"({t['party']}-{t['state']})" if t.get("party") and t.get("state") else ""
                trade_lines += (
                    f"  ┣ {t['politician']} {party_tag}: "
                    f"{t['action']} **{t['ticker']}** {t['amount']} ({t['date']})\n"
                )

        if mood_line or trade_lines:
            ss_section = (
                "\n**MARKET INTELLIGENCE (SentiSense)**\n"
                + mood_line
                + trade_lines
                + "┗ Source: SentiSense API — sentiment + STOCK Act filings\n"
            )
    except Exception as e:
        logger.warning(f"Morning: SentiSense section failed: {e}")

    description = macro_section + equity_section + signals_section + ss_section + directives_section
    title = f"MORNING BRIEF — {now_label}"
    return title, description, bias["color"]


def _build_intraday_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    Mid-session pulse — lightweight bias re-score with updated SPY/QQQ + VIXY.
    Fires at 20:20 UTC (10:20 HST, ~3 hours into cash session).
    """
    now_label = datetime.now().strftime("%H:%M HST")
    bias = _calculate_bias_score(engine, db)
    spy_chg  = bias["spy_chg"]
    qqq_chg  = bias["qqq_chg"]
    vixy_z   = bias["vixy_z"]
    fg_val   = bias["fg_val"]
    real_vix = bias["real_vix"]

    def _arrow(v): return "▲" if v >= 0 else "▼"
    score_sign = f"+{bias['bias_score']}" if bias['bias_score'] >= 0 else str(bias['bias_score'])

    desc = (
        f"**Mid-Session Bias: {bias['label']} (Score: {score_sign})**\n"
        f"┣ SPY: {_arrow(spy_chg)}{abs(spy_chg):.2f}% | QQQ: {_arrow(qqq_chg)}{abs(qqq_chg):.2f}%\n"
        f"┣ VIX: `{real_vix:.1f}` | VIXY z: `{vixy_z:+.2f}σ`\n"
        f"┣ Fear & Greed: `{fg_val}` ({bias['fg_class']})\n"
    )
    # Surface any open TQQQ signals
    try:
        bottom = int(db.get_state("tqqq_bottom_score") or 0)
        top    = int(db.get_state("tqqq_top_score") or 0)
        if bottom >= 55:
            desc += f"┣ 🟢 TQQQ CALL desk UNLOCKED — bottom score {bottom}/100\n"
        if top >= 55:
            desc += f"┣ 🟢 TQQQ PUT desk UNLOCKED — top score {top}/100\n"
    except Exception:
        pass
    desc += "┗ Intraday context — full brief at 0800 HST daily."
    return "MID-SESSION PULSE", desc, bias["color"]


def _build_eod_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    EOD brief (23:40 UTC / 13:40 HST — after cash close).
    Summarizes the session and flags anything to act on before tomorrow's open.
    """
    now_label = datetime.now().strftime("%a %b %-d")
    bias = _calculate_bias_score(engine, db)
    spy_chg  = bias["spy_chg"]
    qqq_chg  = bias["qqq_chg"]
    vixy_z   = bias["vixy_z"]
    real_vix = bias["real_vix"]

    def _arrow(v): return "▲" if v >= 0 else "▼"
    score_sign = f"+{bias['bias_score']}" if bias['bias_score'] >= 0 else str(bias['bias_score'])

    desc = (
        f"**Session Close — Bias: {bias['label']} (Score: {score_sign})**\n"
        f"┣ SPY: {_arrow(spy_chg)}{abs(spy_chg):.2f}% | QQQ: {_arrow(qqq_chg)}{abs(qqq_chg):.2f}%\n"
        f"┣ VIX close: `{real_vix:.1f}` | VIXY z: `{vixy_z:+.2f}σ`\n"
        f"┣ Fear & Greed: `{bias['fg_val']}` ({bias['fg_class']})\n"
        f"┗ HY Spread: `{bias['signals'].get('hy_spread', 0.0):.2f}%`\n"
    )

    # Wheel position DTE countdown — appended after fixed lines if any positions open
    try:
        open_pos = db.get_open_wheel_positions()
        if open_pos:
            today = datetime.now().date()
            desc += "\n**Open Wheel Positions**\n"
            for pos in open_pos[:5]:
                try:
                    exp_d = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
                    dte   = (exp_d - today).days
                    urgency = " 🔴 ROLL/CLOSE SOON" if dte <= 7 else (" 🟡 WATCH" if dte <= 14 else "")
                    desc += (
                        f"┣ **{pos['symbol']}** {pos['position_type']} ${pos['strike']:.0f} "
                        f"exp {pos['expiration']} ({dte}d){urgency}\n"
                    )
                except Exception:
                    pass
            desc = _swap_last_bullet(desc)
    except Exception as e:
        logger.warning(f"EOD: wheel position read failed: {e}")

    return f"EOD BRIEF — {now_label}", desc, bias["color"]


def _swap_last_bullet(text: str) -> str:
    """Replace the last ┣ line prefix with ┗ for proper Discord formatting."""
    lines = text.rstrip("\n").split("\n")
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].startswith("┣"):
            lines[i] = "┗" + lines[i][1:]
            break
    return "\n".join(lines) + "\n"


# ── Main Loop ─────────────────────────────────────────────────────────────────

def run():
    db     = EcosystemDatabase()
    engine = HighFidelityAnalyticsEngine()
    logger.info("Market Analysis online. Loop: 60s.")

    BUILDERS = {
        "morning":  _build_morning_report,
        "intraday": _build_intraday_report,
        "eod":      _build_eod_report,
    }

    while True:
        try:
            now_utc  = datetime.now(timezone.utc)
            weekday  = now_utc.weekday()
            is_wkday = weekday < 5
            date_str = now_utc.strftime("%Y-%m-%d")
            h, m     = now_utc.hour, now_utc.minute

            for (t_h, t_m, db_key, mode) in FIRE_SCHEDULE:
                if not is_wkday:
                    continue
                if not _in_window(h, m, t_h, t_m):
                    continue
                if _already_fired(db, db_key, date_str):
                    continue

                logger.info(f"Firing {mode} brief...")
                _mark_fired(db, db_key, date_str)
                try:
                    builder = BUILDERS[mode]
                    title, description, color = builder(engine, db)
                    _send_embed(title, description, color)
                except Exception as e:
                    logger.error(f"{mode} brief build failed: {e}")

        except Exception as e:
            logger.error(f"Loop tick error: {e}")

        # Align to wall-clock minute boundary (prevents slow drift over trading day)
        now_ts = time.time()
        time.sleep(60 - (now_ts % 60))


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Market Analysis stopped by operator.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Market Analysis crashed: {e}")
        sys.exit(1)

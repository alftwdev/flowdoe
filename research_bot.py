"""
research_bot.py — Discord slash command bot: /query <ticker>

Runs as a persistent async process (PythonAnywhere always-on task, slot 6).
All responses are ephemeral (visible only to the requester).

Data sources wired in:
  Tradier    — real ATM IV, IVR, delta-proxied CSP strike, earnings proximity,
               P/C OI ratio (from options chain)
  SentiSense — sentiment score, institutional 13F flow, insider cluster,
               congressional trades (all daily-cached)
  DB         — tqqq_bottom/top score, market_analysis_bias, vixy_price_realtime,
               fred_vix_value, fred_hy_spread_value, fred_yield_spread, vix_term_slope
               (written by ecosystem scripts, read-only here)
  Twelve Data via HighFidelityAnalyticsEngine — spot, HV30, OHLCV matrix,
               52-week range, RSI14, HV21 (via fetch_symbol_enrichment)
"""

import os
import math
import json
import base64
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv

import requests as _requests
import discord
from discord import app_commands
import anthropic

from analytics import HighFidelityAnalyticsEngine
from tradier_client import TradierClient
import sentisense_client as ss

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("research_bot")

_CRYPTO_TICKERS = {"BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "AVAX"}
_INCOME_TICKERS = {"SCHD", "JEPI", "JEPQ", "DIVO", "O", "MO", "ARCC", "MAIN",
                   "MLPI", "TDAQ", "KQQQ", "CLM", "CRF"}

_DTE_MID = 37
_T       = _DTE_MID / 365.0

# Seasonal LEAP CALL size scalars — mirrors tqqq.py constants
_SEASONAL_CALL_SCALAR = {
    1: 1.25, 2: 1.0, 3: 0.50, 4: 0.75, 5: 0.50,
    6: 1.0,  7: 1.0, 8: 0.75, 9: 0.50, 10: 1.25,
    11: 1.0, 12: 1.0,
}


# ── Bot setup ──────────────────────────────────────────────────────────────────

class QueryBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree     = app_commands.CommandTree(self)
        self.engine   = HighFidelityAnalyticsEngine()
        self.tradier  = TradierClient()
        self.claude   = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

    async def setup_hook(self):
        await self.tree.sync()
        logger.info("Slash commands synced.")

bot = QueryBot()


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _db_float(db, key: str, default: float = 0.0) -> float:
    try:
        return float(db.get_state(key) or default)
    except (TypeError, ValueError):
        return default

def _db_int(db, key: str, default: int = 0) -> int:
    try:
        return int(db.get_state(key) or default)
    except (TypeError, ValueError):
        return default


# ── Shared signal helpers ──────────────────────────────────────────────────────

def _cycle_bias(db) -> str:
    bottom = _db_int(db, "tqqq_bottom_score")
    top    = _db_int(db, "tqqq_top_score")
    if bottom >= 55:
        return f"🟢 Bottom {bottom}/100 — CALL desk active"
    if top >= 55:
        return f"🔴 Top {top}/100 — PUT desk active"
    return f"⚪ Neutral — bottom {bottom}/top {top}"


def _market_bias_line(db) -> str:
    try:
        bias = _db_int(db, "market_analysis_bias")
        if bias >= 2:
            return f"🟢 BULLISH ({bias:+d}/12 flags)"
        if bias <= -2:
            return f"🔴 BEARISH ({bias:+d}/12 flags)"
        return f"🟡 NEUTRAL ({bias:+d}/12 flags)"
    except Exception:
        return "N/A"


def _macro_line(db) -> str:
    vix   = _db_float(db, "fred_vix_value")
    hy    = _db_float(db, "fred_hy_spread_value")
    yc    = _db_float(db, "fred_yield_spread")
    vixy  = _db_float(db, "vixy_price_realtime")
    parts = []
    if vix:   parts.append(f"VIX `{vix:.1f}`")
    if vixy:  parts.append(f"VIXY `{vixy:.2f}`")
    if hy:    parts.append(f"HY `{hy:.2f}%`")
    if yc:
        sign = "+" if yc >= 0 else ""
        parts.append(f"T10-T2 `{sign}{yc:.2f}%`")
    return " · ".join(parts) if parts else "N/A"


def _ss_sentiment(db, ticker: str) -> tuple:
    """Returns (score, lean, mentions_str) or (0, 'N/A', '')."""
    try:
        data = ss.get_sentiment(db, ticker)
        if not data:
            return 0, "N/A", ""
        score    = data.get("score", 0)
        lean     = data.get("lean") or data.get("direction") or "Neutral"
        mentions = data.get("mentions", 0)
        return score, lean, f"{mentions:,} mentions"
    except Exception:
        return 0, "N/A", ""


def _iv_and_strike(tradier: TradierClient, db, ticker: str, spot: float) -> tuple:
    """Returns (iv_pct, ivr, ivr_tag, strike, iv_reliable)."""
    try:
        iv_rank  = tradier.get_iv_rank(ticker, db)
        iv_dec   = iv_rank.get("current_iv", 0.0)
        ivr      = iv_rank.get("ivr", 0.0)
        tag      = iv_rank.get("tag", "")
        reliable = iv_rank.get("reliable", False)
        if iv_dec > 0:
            iv_pct = round(iv_dec * 100, 1)
            strike = round(spot * math.exp(-0.84 * iv_dec * math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
            return iv_pct, ivr, tag, strike, reliable
    except Exception as e:
        logger.warning(f"Tradier IV failed for {ticker}: {e}")
    return None, None, None, None, False


def _earnings_tag(tradier: TradierClient, ticker: str) -> tuple:
    """Returns (display_str, flag) where flag is FORCE_CLOSE | REVIEW | CLEAR."""
    try:
        prox = tradier.get_earnings_proximity([ticker])
        ep   = prox.get(ticker, {})
        flag = ep.get("flag", "CLEAR")
        days = ep.get("days_to_earnings")
        if flag == "FORCE_CLOSE":
            return f"⛔ earnings in {days}d — avoid new entries", "FORCE_CLOSE"
        if flag == "REVIEW":
            return f"⚠️ earnings in {days}d — review before entry", "REVIEW"
        if days is not None:
            return f"✅ {days}d to earnings", "CLEAR"
        return "✅ no near-term earnings", "CLEAR"
    except Exception:
        return "N/A", "CLEAR"


def _pc_ratio_line(tradier: TradierClient, ticker: str, spot: float) -> tuple:
    """
    P/C OI ratio from front-2 expirations within 60 DTE.
    Chain data is already cached in TradierClient if IV was fetched above.
    Returns (ratio, display_tag).
    """
    try:
        exps  = tradier.get_expirations(ticker)
        today = datetime.utcnow().date()
        total_call = 0.0
        total_put  = 0.0
        counted    = 0
        for exp_str in sorted(exps)[:6]:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = (exp_date - today).days
            if dte < 0 or dte > 60:
                continue
            chain = tradier.get_options_chain(ticker, exp_str, greeks=False)
            for c in chain:
                strike = float(c.get("strike", 0))
                if not (spot * 0.88 <= strike <= spot * 1.12):
                    continue
                oi = float(c.get("open_interest") or 0)
                if c.get("option_type") == "call":
                    total_call += oi
                else:
                    total_put += oi
            counted += 1
            if counted >= 2:
                break
        if total_call == 0:
            return None, "N/A"
        ratio = round(total_put / total_call, 2)
        if ratio > 1.20:
            tag = "🔴 PUT-HEAVY — heavy hedging, bearish skew"
        elif ratio < 0.80:
            tag = "🟢 CALL-HEAVY — bullish lean"
        else:
            tag = "🟡 BALANCED"
        return ratio, f"`{ratio:.2f}` {tag}"
    except Exception:
        return None, "N/A"


# ── SentiSense intel helpers ───────────────────────────────────────────────────

def _institutional_line(db, ticker: str) -> str:
    try:
        flows = ss.get_institutional_flows(db, ticker)
        if not flows:
            return "N/A"
        direction  = flows.get("net_direction", "NEUTRAL")
        net        = flows.get("net_shares", 0)
        filers     = flows.get("filer_count", 0)
        top_buyers = flows.get("top_buyers", [])
        icon       = "🟢" if direction == "ACCUMULATING" else ("🔴" if direction == "DISTRIBUTING" else "🟡")
        line = f"{icon} {direction} (`{net:+,}` shares · {filers} filers)"
        if top_buyers:
            line += f" · Lead: {top_buyers[0][0]}"
        return line
    except Exception:
        return "N/A"


def _insider_line(db, ticker: str) -> str:
    try:
        insights = ss.get_insights(db, ticker)
        if not insights or not insights.get("insider_cluster"):
            return "No cluster activity"
        buy      = insights.get("cluster_buy", False)
        sell     = insights.get("cluster_sell", False)
        count    = insights.get("insider_count", 0)
        urgency  = insights.get("urgency", "LOW")
        if buy and not sell:
            icon, action = "🟢", "BUY cluster"
        elif sell and not buy:
            icon, action = "🔴", "SELL cluster"
        else:
            icon, action = "🟡", "mixed (buy + sell)"
        urgent = "  ⚡ HIGH URGENCY" if urgency == "HIGH" else ""
        return f"{icon} {count} insiders — {action}{urgent}"
    except Exception:
        return "N/A"


def _congressional_line(db, ticker: str) -> str:
    try:
        trades   = ss.get_congressional_trades(db, limit=30)
        if not trades:
            return "No recent trades"
        relevant = [t for t in trades if t.get("ticker", "").upper() == ticker.upper()]
        if not relevant:
            return "No recent trades"
        t      = relevant[0]
        action = t.get("action", "?")
        amount = t.get("amount", "?")
        name   = t.get("politician", "?")
        date   = t.get("date", "?")
        party  = t.get("party", "")
        party_str = f" ({party})" if party else ""
        icon   = "🟢" if any(k in action.lower() for k in ("buy", "purchase")) else "🔴"
        return f"{icon} {name}{party_str} — {action} `{amount}` · {date}"
    except Exception:
        return "N/A"


# ── Composite scoring ──────────────────────────────────────────────────────────

def _wheel_score(ivr, range_pct, earnings_flag, ss_score, pc_ratio) -> tuple:
    """
    0–100 composite wheel setup quality score.
    Components:
      IVR rank        (0–30)  — premium environment
      52w range pos   (0–20)  — mean-reversion cushion (lower = better)
      Earnings safety (0–20)  — assignment risk from catalyst
      Social sentiment(0–15)  — directional tailwind
      P/C OI skew     (0–15)  — options market positioning confirmation
    Returns (score, label).
    """
    pts = 0

    # IVR (0–30)
    if ivr is not None:
        if ivr >= 80:   pts += 30
        elif ivr >= 60: pts += 22
        elif ivr >= 35: pts += 14

    # 52w range position (0–20): lower position = more cushion for CSP
    if range_pct is not None:
        if range_pct <= 20:   pts += 20
        elif range_pct <= 40: pts += 14
        elif range_pct <= 60: pts += 8
        else:                 pts += 3
    else:
        pts += 10  # neutral if no data

    # Earnings (0–20)
    if earnings_flag == "CLEAR":    pts += 20
    elif earnings_flag == "REVIEW": pts += 8
    # FORCE_CLOSE = 0

    # Social sentiment (0–15)
    if ss_score > 15:    pts += 15
    elif ss_score > 5:   pts += 10
    elif ss_score > -5:  pts += 6
    # bearish = 0

    # P/C skew (0–15): put-heavy = hedging demand = safer to be short puts
    if pc_ratio is not None:
        if pc_ratio > 1.20:   pts += 15   # heavy put OI = hedging = more premium, safer CSP
        elif pc_ratio > 0.90: pts += 10   # balanced
        else:                 pts += 4    # call-heavy = speculative bullish, CSP riskier

    if pts >= 75:    tag = "🟢 STRONG SETUP"
    elif pts >= 55:  tag = "🟡 MODERATE SETUP"
    elif pts >= 35:  tag = "🟠 WEAK — size down or wait"
    else:            tag = "🔴 AVOID — conditions poor"

    return pts, tag


# ── Ecosystem Confluence Engine ────────────────────────────────────────────────

def _ecosystem_confluence(db, ticker: str, spot: float,
                           ivr: float, matrix: dict, ss_score: float,
                           range_pct=None, pc_ratio=None) -> str:
    """
    The cross-asset unity layer.

    Reads all live DB signals (equity bias, TQQQ cycle, VIX regime, HY spread,
    yield curve, social momentum, 52w positioning) and synthesises a unified
    conviction verdict that connects Strategy 1 (CLM/CRF margin environment),
    Strategy 2 (wheel premium climate), and Strategy 3 (LEAP cycle desk).

    Other Discord servers surface raw numbers.
    This function turns them into a single authoritative verdict.
    """
    bias      = _db_int(db, "market_analysis_bias")
    bottom    = _db_int(db, "tqqq_bottom_score")
    top       = _db_int(db, "tqqq_top_score")
    vix       = _db_float(db, "fred_vix_value")
    hy        = _db_float(db, "fred_hy_spread_value")
    yc        = _db_float(db, "fred_yield_spread")
    slope     = _db_float(db, "vix_term_slope")    # VIXY/VXZ ratio — written by tqqq.py
    clm_z     = _db_float(db, "clm_last_z_premium")
    crf_z     = _db_float(db, "crf_last_z_premium")
    sigma     = matrix.get("sigma", 0.0)

    bull = []
    bear = []

    # Equity regime (Strategy 2 + 3 gate)
    if bias >= 3:    bull.append("mkt strong BULLISH")
    elif bias >= 1:  bull.append("mkt lean bullish")
    elif bias <= -3: bear.append("mkt strong BEARISH")
    elif bias <= -1: bear.append("mkt lean bearish")

    # TQQQ cycle (Strategy 3 cross-signal)
    if bottom >= 55:  bull.append(f"LEAP CALL desk live ({bottom}/100)")
    elif bottom >= 40: bull.append(f"CALL desk watching ({bottom}/100)")
    if top >= 55:     bear.append(f"LEAP PUT desk live ({top}/100)")
    elif top >= 40:   bear.append(f"PUT desk watching ({top}/100)")

    # VIX regime (premium climate for Strategy 2)
    if vix and vix >= 28:    bear.append(f"VIX fear spike `{vix:.0f}` — CSP delta down")
    elif vix and vix >= 20:  bear.append(f"VIX elevated `{vix:.0f}`")
    elif vix and vix < 14:   bull.append(f"VIX calm `{vix:.0f}` — premium sellers' market")

    # VIX term structure (contango = calm, backwardation = fear spike incoming)
    if slope and slope < 0.95:   bear.append("VIX backwardation — fear spike risk")
    elif slope and slope > 1.08: bull.append("VIX contango — calm regime")

    # HY credit spread (Strategy 1 carry spread context)
    if hy and hy > 5.0:  bear.append(f"HY spread `{hy:.2f}%` — credit stress")
    elif hy and hy < 3.5: bull.append(f"HY spread tight `{hy:.2f}%` — credit calm")

    # Yield curve (recession watch / LEAP PUT conviction)
    if yc and yc < -0.1: bear.append("curve inverted — recession watch")
    elif yc and yc > 0.3: bull.append("curve normal — expansion")

    # IVR as premium environment signal (Strategy 2)
    if ivr and ivr >= 60: bull.append(f"IVR `{ivr:.0f}%` → premium cycle active")

    # Stock-specific volume/order-flow
    if sigma and sigma > 2.0:    bull.append(f"vol surge +{sigma:.1f}σ")
    elif sigma and sigma < -2.0: bear.append(f"vol dump {sigma:.1f}σ")

    # Social momentum (corroborating signal)
    if ss_score > 20:   bull.append("strong social momentum")
    elif ss_score > 8:  bull.append("social lean bullish")
    elif ss_score < -15: bear.append("social bearish")

    # 52w range — mean reversion context for CSP
    if range_pct is not None:
        if range_pct < 15:   bull.append("near 52w low — mean reversion zone")
        elif range_pct > 88: bear.append("near 52w high — extended, assignment risk elevated")

    # CLM/CRF premium health (Strategy 1 context — only show if elevated)
    avg_cef_z = (clm_z + crf_z) / 2 if clm_z and crf_z else None
    if avg_cef_z and avg_cef_z >= 1.5:
        bear.append(f"CLM/CRF premium stretched `{avg_cef_z:.1f}σ` — RO watch")
    elif avg_cef_z and avg_cef_z <= -0.5:
        bull.append(f"CLM/CRF premium safe `{avg_cef_z:.1f}σ`")

    # Net verdict
    net = len(bull) - len(bear)
    if net >= 4:
        verdict = "🟢 BULLISH CONFLUENCE — fully aligned across equity · macro · cycle"
    elif net >= 2:
        verdict = "🟡 LEAN BULLISH — majority signals positive"
    elif net >= 0:
        verdict = "⚪ MIXED — no clear directional edge · size conservatively"
    elif net >= -2:
        verdict = "🟠 LEAN BEARISH — caution flags dominate"
    else:
        verdict = "🔴 BEARISH CONFLUENCE — headwinds across all layers · reduce size"

    bull_str = " · ".join(bull) if bull else "none"
    bear_str = " · ".join(bear) if bear else "none"

    return (
        f"┣ Verdict: {verdict}\n"
        f"┣ 📈 Bull: {bull_str}\n"
        f"┗ 📉 Bear: {bear_str}"
    )


# ── RSI + MACD display ─────────────────────────────────────────────────────────

def _rsi_line(rsi14: float, hv21: float = None) -> str:
    if not rsi14:
        return "N/A"
    if rsi14 >= 70:   rsi_tag = "🔴 overbought"
    elif rsi14 <= 30: rsi_tag = "🟢 oversold"
    else:             rsi_tag = "🟡 mid-range"
    hv_str = f"  · HV21 `{hv21:.1f}%`" if hv21 else ""
    return f"RSI14 `{rsi14:.1f}` {rsi_tag}{hv_str}"


# ── Intel builders ─────────────────────────────────────────────────────────────

def build_equity_intel(engine, tradier, ticker: str) -> tuple:
    """
    Full-spectrum equity intel block.
    Sections:
      1. IV / Wheel Setup      — IV, IVR, EM, CSP, BEP, ROI, spread alt, earnings
      2. Positioning           — P/C OI, institutional 13F, insider cluster, congressional
      3. Wheel Score           — 0-100 composite (IVR + range + earnings + sentiment + P/C)
      4. Market Signal         — order flow, RSI, social sentiment
      5. Ecosystem Confluence  — unified verdict connecting equity · macro · LEAP cycle · CLM/CRF
    """
    try:
        spot = engine._execute_query("price", {"symbol": ticker})
        spot = float((spot or {}).get("price", 0))
        if not spot:
            return None, None

        db     = engine.db
        matrix = engine.calculate_ohlcv_matrix(ticker)
        hv30   = engine.calculate_historical_volatility(ticker, lookback=30)
        enrich = engine.fetch_symbol_enrichment(ticker)

        # IV — Tradier first, HV30 proxy fallback
        iv_pct, ivr, ivr_tag, strike, iv_reliable = _iv_and_strike(tradier, db, ticker, spot)
        if iv_pct is None:
            iv_dec      = (hv30 or 20.0) / 100 * 1.15
            iv_pct      = round(iv_dec * 100, 1)
            ivr         = iv_pct
            strike      = round(spot * math.exp(-0.84 * iv_dec * math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
            iv_reliable = False
        else:
            iv_dec = iv_pct / 100

        # CSP metrics
        est_prem  = round(strike * iv_dec * math.sqrt(_T) / (2 * math.pi) ** 0.5 * 100)
        bep       = round(strike - est_prem / 100, 2)
        ann_roi   = round((est_prem / 100 / strike) * (365 / _DTE_MID) * 100, 1)
        em_dollar = round(spot * iv_dec * math.sqrt(_T), 2)
        em_pct    = round(em_dollar / spot * 100, 1)

        # VRP: edge to seller (IV - HV30)
        vrp = round(iv_pct - hv30, 1) if hv30 else None
        vrp_str = (f"  · VRP `+{vrp:.1f}%` edge to seller" if vrp and vrp > 0
                   else (f"  · VRP `{vrp:.1f}%` options cheap" if vrp else ""))

        # IVR environment
        if ivr >= 60:
            env_icon, env = "🟢", "Elevated — premium crush favorable"
        elif ivr >= 35:
            env_icon, env = "🟡", "Mid-range — sellable"
        else:
            env_icon, env = "🔴", "Low — wait or use defined-risk"

        # Put credit spread alternative (per Strategy 2: use spread when price > $100)
        spread_block = ""
        if spot > 100:
            spread_width    = 5
            long_cost_est   = round(est_prem * 0.42)   # approx long leg at strike-5
            spread_credit   = est_prem - long_cost_est
            spread_max_loss = (spread_width * 100) - spread_credit
            spread_bep      = round(strike - spread_credit / 100, 2)
            spread_block = (
                f"┣ Alt (spread): STO `${strike}`/`${strike - spread_width}` put spread"
                f" · `${spread_credit}` cr · max loss `${spread_max_loss}` · BEP `${spread_bep}`\n"
            )

        # Earnings
        earnings_str, earnings_flag = _earnings_tag(tradier, ticker)

        # 52w range
        range_pct  = enrich.get("range_pct")
        low_52     = enrich.get("low_52")
        high_52    = enrich.get("high_52")
        rsi14      = enrich.get("rsi14")
        hv21       = enrich.get("hv21")
        if range_pct is not None:
            range_line = f"`${spot:,.2f}`  ·  52w: bottom `{range_pct:.0f}%` (lo `${low_52}` · hi `${high_52}`)"
        else:
            range_line = f"`${spot:,.2f}`"

        # P/C OI ratio (chain cache from IV call above — usually free)
        pc_ratio_val, pc_line = _pc_ratio_line(tradier, ticker, spot)

        # SentiSense
        ss_score, ss_lean, ss_mentions = _ss_sentiment(db, ticker)
        ss_str = f"{ss_lean} (`{ss_score:+.0f}`) · {ss_mentions}" if ss_mentions else ss_lean

        inst_line    = _institutional_line(db, ticker)
        insider_str  = _insider_line(db, ticker)
        congress_str = _congressional_line(db, ticker)

        # Wheel score
        wheel_pts, wheel_tag = _wheel_score(ivr, range_pct, earnings_flag, ss_score, pc_ratio_val)

        # Ecosystem confluence
        confluence = _ecosystem_confluence(
            db, ticker, spot, ivr, matrix, ss_score, range_pct, pc_ratio_val
        )

        # Order flow
        flow  = ("ACCUMULATION" if matrix.get("volume_surge") and matrix.get("sigma", 0) > 0
                 else ("DISTRIBUTION" if matrix.get("volume_surge") else "NOMINAL"))
        sigma = matrix.get("sigma", 0.0)
        rsi_str = _rsi_line(rsi14, hv21)

        desc = (
            f"**Spot:** {range_line}\n\n"
            f"**IV / Wheel Setup**\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}"
            f"  ·  IVR: {env_icon} `{ivr:.0f}%` — {env}\n"
            f"┣ Expected Move ({_DTE_MID} DTE): ±`${em_dollar}` (±`{em_pct}%`){vrp_str}\n"
            f"┣ CSP: STO `${strike}` put · {_DTE_MID} DTE · est `${est_prem}` credit"
            f" · BEP `${bep}` · Ann. ROI `{ann_roi}%`\n"
            f"{spread_block}"
            f"┗ Earnings: {earnings_str}\n\n"
            f"**Positioning**\n"
            f"┣ P/C OI: {pc_line}\n"
            f"┣ Institutional (13F): {inst_line}\n"
            f"┣ Insider: {insider_str}\n"
            f"┗ Congressional: {congress_str}\n\n"
            f"**Wheel Score: `{wheel_pts}/100`** — {wheel_tag}\n\n"
            f"**Market Signal**\n"
            f"┣ Order flow: `{flow}` ({sigma:+.2f}σ)\n"
            f"┣ {rsi_str}\n"
            f"┗ Sentiment: {ss_str}\n\n"
            f"**Ecosystem Confluence**\n"
            f"{confluence}"
        )

        if ivr >= 60 and wheel_pts >= 65:   color = 0x2ecc71   # green — strong setup
        elif ivr >= 35 and wheel_pts >= 45: color = 0xf1c40f   # yellow — moderate
        else:                               color = 0xe67e22   # orange — weak/wait

        return desc, color

    except Exception as e:
        logger.error(f"equity_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}` — verify ticker or try again shortly.", 0xe74c3c


def build_tqqq_intel(engine, tradier) -> tuple:
    """Dedicated TQQQ handler — LEAP desk context + full regime stack + ecosystem verdict."""
    try:
        spot = engine._execute_query("price", {"symbol": "TQQQ"})
        spot = float((spot or {}).get("price", 0))
        if not spot:
            return None, None

        db     = engine.db
        matrix = engine.calculate_ohlcv_matrix("TQQQ")
        bottom = _db_int(db, "tqqq_bottom_score")
        top    = _db_int(db, "tqqq_top_score")
        bias   = _db_int(db, "market_analysis_bias")
        vixy   = _db_float(db, "vixy_price_realtime")
        vix    = _db_float(db, "fred_vix_value")
        hy     = _db_float(db, "fred_hy_spread_value")
        yc     = _db_float(db, "fred_yield_spread")
        slope  = _db_float(db, "vix_term_slope")

        month     = datetime.now().month
        scalar    = _SEASONAL_CALL_SCALAR.get(month, 1.0)
        scalar_pct = int((scalar - 1.0) * 100)
        if scalar_pct > 0:
            size_tag = f"🟢 +{scalar_pct}% (strong entry month)"
        elif scalar_pct < 0:
            size_tag = f"🔴 {scalar_pct}% (weak month — wait for 3 green days)"
        else:
            size_tag = "🟡 neutral size"

        call_status = (f"🟢 ACTIVE — score {bottom}/100" if bottom >= 55
                       else (f"🟡 WATCHING — score {bottom}/100 (need 55)" if bottom >= 40
                             else f"⚪ DORMANT — score {bottom}/100"))
        put_status  = (f"🔴 ACTIVE — score {top}/100" if top >= 55
                       else (f"🟡 WATCHING — score {top}/100" if top >= 40
                             else f"⚪ DORMANT — score {top}/100"))

        bias_tag = (f"🟢 BULLISH ({bias:+d}/12)" if bias >= 2
                    else (f"🔴 BEARISH ({bias:+d}/12)" if bias <= -2
                          else f"🟡 NEUTRAL ({bias:+d}/12)"))
        vix_tag  = ("🔴 FEAR SPIKE — close PUT profit → rotate CALLS" if vix >= 30
                    else ("🟡 ELEVATED" if vix >= 20 else "🟢 CALM"))
        yc_tag   = ("🔴 inverted" if yc and yc < 0 else ("🟢 normal" if yc and yc > 0.2 else "🟡 flat"))
        slope_tag = ("backwardation ⚠️" if slope and slope < 0.95
                     else ("contango 🟢" if slope and slope > 1.05 else "flat"))

        iv_pct, ivr, _, _, iv_reliable = _iv_and_strike(tradier, db, "TQQQ", spot)
        if iv_pct is None:
            hv30   = engine.calculate_historical_volatility("TQQQ", lookback=30) or 50.0
            iv_pct = round(hv30 * 1.15, 1)
            iv_reliable = False

        ss_score, ss_lean, ss_mentions = _ss_sentiment(db, "TQQQ")
        inst_line = _institutional_line(db, "TQQQ")

        cascade_line = ""
        if bottom >= 55:
            cascade_line = "┣ 📌 On TP1/TP2: route proceeds → MLPI → expanded margin → CLM/CRF DCA\n"

        confluence = _ecosystem_confluence(
            db, "TQQQ", spot, ivr or 0, matrix, ss_score
        )

        desc = (
            f"TQQQ @ `${spot:,.2f}`\n\n"
            f"**LEAP CALL Desk**\n"
            f"┣ Status: {call_status}\n"
            f"┣ Seasonal size: {size_tag}\n"
            f"{cascade_line}"
            f"┣ LEAP target: Δ0.72 · 270–540 DTE · TP1 +50% / TP2 +100%\n"
            f"┗ LEAP PUT Desk: {put_status}\n\n"
            f"**Regime Stack**\n"
            f"┣ Market bias: {bias_tag}\n"
            f"┣ VIX `{vix:.1f}` {vix_tag}{f'  · VIXY `{vixy:.2f}`' if vixy else ''}\n"
            f"┣ VIX term: `{slope:.3f}` {slope_tag}\n"
            f"┣ HY spread: `{hy:.2f}%`{'  🔴 credit stress' if hy > 4.5 else ''}\n"
            f"┣ Yield curve: `{yc:+.2f}%` {yc_tag}\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n"
            f"┣ Institutional (13F): {inst_line}\n"
            f"┣ Sentiment: {ss_lean} (`{ss_score:+.0f}`){f' · {ss_mentions}' if ss_mentions else ''}\n\n"
            f"**Ecosystem Confluence**\n"
            f"{confluence}"
        )

        color = 0x2ecc71 if bottom >= 55 else (0xe74c3c if top >= 55 else 0xf1c40f)
        return desc, color

    except Exception as e:
        logger.error(f"tqqq_intel: {e}")
        return f"Data unavailable for `TQQQ` — try again shortly.", 0xe74c3c


def build_income_intel(engine, tradier, ticker: str) -> tuple:
    """Income / CEF / dividend ticker — yield, CSP setup, fair value, confluence."""
    try:
        spot = engine._execute_query("price", {"symbol": ticker})
        spot = float((spot or {}).get("price", 0))
        if not spot:
            return None, None

        db = engine.db
        iv_pct, ivr, ivr_tag, strike, iv_reliable = _iv_and_strike(tradier, db, ticker, spot)

        if iv_pct is None:
            hv30   = engine.calculate_historical_volatility(ticker, lookback=30) or 15.0
            iv_dec = hv30 / 100 * 1.15
            iv_pct = round(iv_dec * 100, 1)
            strike = round(spot * 0.95, 1)
            iv_reliable = False
        else:
            iv_dec = iv_pct / 100

        est_prem = round(strike * iv_dec * math.sqrt(_T) / (2 * math.pi) ** 0.5 * 100)
        ann_roi  = round((est_prem / 100 / strike) * (365 / _DTE_MID) * 100, 1)

        ss_score, ss_lean, ss_mentions = _ss_sentiment(db, ticker)
        inst_line    = _institutional_line(db, ticker)
        congress_str = _congressional_line(db, ticker)
        cycle_line   = _cycle_bias(db)
        bias_line    = _market_bias_line(db)

        # CLM/CRF fair-value floor
        _ANN_DIST = {"CLM": 1.4268, "CRF": 1.3824}
        fv_line = ""
        if ticker in _ANN_DIST:
            fv = round(_ANN_DIST[ticker] / 0.19, 2)
            fv_line = (
                f"┣ Fair value floor: `${fv}` (19% yield target)"
                f"{'  🟢 at/below — accumulate' if spot <= fv else '  🔴 above — wait'}\n"
            )

        # Tax character (seeded via db_tools --seed-tax-character)
        tax_line = ""
        if ticker in _ANN_DIST:
            tc = db.get_state(f"{ticker.lower()}_dist_tax_char") or {}
            if isinstance(tc, dict) and "roc_pct" in tc:
                ann_d  = _ANN_DIST[ticker]
                nav    = float(db.get_state(f"{ticker.lower()}_last_nav") or (6.45 if ticker == "CLM" else 6.18))
                hl_y   = ann_d / nav * 100
                marg   = float(os.getenv("MARGINAL_TAX_RATE", "22")) / 100
                at_y   = hl_y * (tc["roc_pct"]/100 * 1.0 + tc["qdi_pct"]/100 * 0.85 + tc["ord_pct"]/100 * (1 - marg))
                tax_line = (
                    f"┣ Tax char ({tc.get('year','?')} 1099): ROC `{tc['roc_pct']:.0f}%`"
                    f" · QDI `{tc['qdi_pct']:.0f}%` · after-tax yield `~{at_y:.1f}%`\n"
                )

        # Simplified confluence for income tickers (fewer signals matter)
        matrix     = engine.calculate_ohlcv_matrix(ticker)
        confluence = _ecosystem_confluence(db, ticker, spot, ivr or 0, matrix, ss_score)

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**Wheel / Income Setup**\n"
            f"┣ CSP strike: `${strike}` · {_DTE_MID} DTE · est `${est_prem}` credit · Ann. ROI `{ann_roi}%`\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n"
            f"{fv_line}"
            f"{tax_line}"
            f"**Context**\n"
            f"┣ Institutional (13F): {inst_line}\n"
            f"┣ Congressional: {congress_str}\n"
            f"┣ Sentiment: {ss_lean} (`{ss_score:+.0f}`){f' · {ss_mentions}' if ss_mentions else ''}\n"
            f"┣ Market bias: {bias_line}\n"
            f"┣ TQQQ cycle: {cycle_line}\n\n"
            f"**Ecosystem Confluence**\n"
            f"{confluence}"
        )
        return desc, 0xf1c40f

    except Exception as e:
        logger.error(f"income_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}`.", 0xe74c3c


def build_crypto_intel(engine, ticker: str) -> tuple:
    """Crypto intel with smart money, futures basis, cross-asset equity confluence."""
    try:
        td_sym    = ticker if "/" in ticker else f"{ticker}/USD"
        spot_data = engine._execute_query("price", {"symbol": td_sym})
        spot      = float((spot_data or {}).get("price", 0))
        if not spot:
            return None, None

        db         = engine.db
        ss_score, ss_lean, ss_mentions = _ss_sentiment(db, ticker.split("/")[0])
        cycle_line = _cycle_bias(db)
        bias_line  = _market_bias_line(db)
        macro_line = _macro_line(db)

        # Binance derivatives (written by scheduler.py crypto_social)
        btc_oi      = _db_float(db, "binance_btc_oi")
        btc_top_ls  = _db_float(db, "binance_btc_top_ls")
        btc_gl_ls   = _db_float(db, "binance_btc_global_ls")
        btc_taker   = _db_float(db, "binance_btc_taker_buy_pct")

        sm_line = ""
        if btc_top_ls and btc_gl_ls:
            if btc_top_ls > 1.1 and btc_gl_ls < 1.0:
                sm_line = "┣ Smart money: 🟢 DIVERGING LONG — top traders long, retail short\n"
            elif btc_top_ls < 0.9 and btc_gl_ls > 1.1:
                sm_line = "┣ Smart money: 🔴 DIVERGING SHORT — top traders short, retail long\n"
            else:
                sm_line = f"┣ Smart money: 🟡 ALIGNED — top L/S `{btc_top_ls:.2f}` · retail L/S `{btc_gl_ls:.2f}`\n"

        taker_line = ""
        if btc_taker:
            taker_icon = "🟢" if btc_taker > 55 else ("🔴" if btc_taker < 45 else "🟡")
            taker_line = f"┣ Taker buy pct: {taker_icon} `{btc_taker:.1f}%` ({'aggressive buying' if btc_taker > 55 else ('aggressive selling' if btc_taker < 45 else 'neutral')})\n"

        oi_line = f"┣ OI: `${btc_oi/1e9:.2f}B`\n" if btc_oi else ""

        # Cross-asset note: crypto ↔ equity cycle link
        vix   = _db_float(db, "fred_vix_value")
        bottom = _db_int(db, "tqqq_bottom_score")
        cross_note = ""
        if bottom >= 55:
            cross_note = "┣ ⚡ LEAP CALL desk active — dual-asset capitulation aligns BTC + equity bottom signal\n"
        elif vix and vix >= 25:
            cross_note = "┣ ⚠️ VIX elevated — crypto + equity risk-off alignment, size down\n"

        support = round(spot * 0.94, 2)
        resist  = round(spot * 1.08, 2)

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**Key Levels**\n"
            f"┣ Resistance: `${resist:,.2f}` (+8%)\n"
            f"┣ Support: `${support:,.2f}` (−6%)\n"
            f"┣ Entry range: `${round(spot*0.97,2):,.2f}` – `${round(spot*0.99,2):,.2f}`\n"
            f"┗ Invalidation: `${round(spot*0.92,2):,.2f}` (−8%)\n\n"
            f"**Futures / Derivatives**\n"
            f"{oi_line}"
            f"{sm_line}"
            f"{taker_line}"
            f"**Cross-Asset**\n"
            f"{cross_note}"
            f"┣ Equity bias: {bias_line}\n"
            f"┣ Macro: {macro_line}\n"
            f"┣ TQQQ cycle: {cycle_line}\n"
            f"┗ Sentiment: {ss_lean} (`{ss_score:+.0f}`){f' · {ss_mentions}' if ss_mentions else ''}"
        )
        return desc, 0xf39c12

    except Exception as e:
        logger.error(f"crypto_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}`.", 0xe74c3c


def route_query(engine, tradier, ticker: str) -> tuple:
    base = ticker.split("/")[0].upper()
    if base == "TQQQ":
        return build_tqqq_intel(engine, tradier)
    if base in _CRYPTO_TICKERS or "BTC" in ticker or "ETH" in ticker:
        return build_crypto_intel(engine, ticker)
    if ticker in _INCOME_TICKERS:
        return build_income_intel(engine, tradier, ticker)
    return build_equity_intel(engine, tradier, ticker)


# ── Chart vision helpers ───────────────────────────────────────────────────────

_VISION_PROMPT = """
You are a professional technical analyst. Analyze this trading chart and return ONLY a valid JSON
object — no markdown, no code fences, no extra text. Use these exact keys:

{
  "pattern":        "brief pattern name (e.g. Bull flag, Double bottom, H&S, Range breakout)",
  "timeframe_guess":"timeframe if visible on chart (e.g. 1H, 4H, Daily) or 'Unknown'",
  "bias":           "BULLISH" or "BEARISH" or "NEUTRAL",
  "entry":          <float — realistic entry price readable from chart>,
  "stop":           <float — stop loss price where the setup is invalidated>,
  "tp1":            <float — first profit target>,
  "tp2":            <float — second, more extended profit target>,
  "rr1":            <float — R:R ratio to TP1, e.g. 1.5>,
  "rr2":            <float — R:R ratio to TP2, e.g. 2.8>,
  "quality":        "A" (3+ confluences, clean structure) or "B" (moderate) or "C" (speculative),
  "key_level":      "brief: what is the entry based on? e.g. 'breakout above descending trendline'",
  "conversation":   "2-3 sentence plain-language read of the setup — what you see, why this entry, what invalidates it"
}

Rules:
  - entry, stop, tp1, tp2 must be prices derivable from what is visible on the chart.
  - rr1 = abs(tp1 - entry) / abs(entry - stop), rr2 = abs(tp2 - entry) / abs(entry - stop).
  - If prices are unclear or the chart is low-resolution, estimate conservatively.
  - quality A = clean structure + 3+ confluences. B = 1-2 confluences. C = single signal.
  - Return ONLY the JSON. Nothing else.
""".strip()


def _fetch_image_b64(url: str) -> tuple:
    """Download image from Discord CDN and return (base64_str, media_type)."""
    try:
        r = _requests.get(url, timeout=10)
        r.raise_for_status()
        ct = r.headers.get("content-type", "image/png").split(";")[0].strip()
        if ct not in ("image/png", "image/jpeg", "image/gif", "image/webp"):
            ct = "image/png"
        return base64.standard_b64encode(r.content).decode("utf-8"), ct
    except Exception as e:
        logger.warning(f"Image download failed: {e}")
        return None, None


def _call_vision(claude_client: anthropic.Anthropic, img_b64: str, media_type: str) -> dict:
    """Send chart image to Claude vision and parse the structured JSON response."""
    msg = claude_client.messages.create(
        model="claude-haiku-4-5-20251001",   # fast + cheap for vision parse
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type":  "image",
                    "source": {
                        "type":       "base64",
                        "media_type": media_type,
                        "data":       img_b64,
                    },
                },
                {"type": "text", "text": _VISION_PROMPT},
            ],
        }],
    )
    raw = msg.content[0].text.strip()
    # Strip accidental markdown fences if model adds them
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw)


def _format_analyze_response(setup: dict, ticker: str | None, engine, tradier) -> tuple:
    """
    Format vision setup into Discord embed description.
    If ticker provided, cross-reference live options data for wheel overlay.
    """
    bias     = setup.get("bias", "NEUTRAL")
    pattern  = setup.get("pattern", "—")
    tf       = setup.get("timeframe_guess", "—")
    entry    = setup.get("entry", 0.0)
    stop     = setup.get("stop", 0.0)
    tp1      = setup.get("tp1", 0.0)
    tp2      = setup.get("tp2", 0.0)
    rr1      = setup.get("rr1", 0.0)
    rr2      = setup.get("rr2", 0.0)
    quality  = setup.get("quality", "B")
    key_lvl  = setup.get("key_level", "—")
    convo    = setup.get("conversation", "—")

    bias_icon = "🟢" if bias == "BULLISH" else ("🔴" if bias == "BEARISH" else "🟡")
    q_icon    = "🔵" if quality == "A" else ("🟡" if quality == "B" else "🟠")

    risk_per_share = round(abs(entry - stop), 2) if entry and stop else 0

    desc = (
        f"**Pattern:** {pattern}  ·  **Timeframe:** {tf}\n"
        f"**Bias:** {bias_icon} {bias}  ·  **Setup Quality:** {q_icon} Grade {quality}\n"
        f"**Key Level:** {key_lvl}\n\n"
        f"**Trade Setup**\n"
        f"┣ Entry:  `${entry:,.2f}`\n"
        f"┣ Stop:   `${stop:,.2f}`  (risk `${risk_per_share}` / share)\n"
        f"┣ TP1:    `${tp1:,.2f}`  ({rr1:.1f}:1 R:R)\n"
        f"┗ TP2:    `${tp2:,.2f}`  ({rr2:.1f}:1 R:R)\n\n"
    )

    # Wheel overlay — only if ticker provided and entry > 0
    if ticker and entry > 0:
        try:
            db       = engine.db
            iv_pct, ivr, _, strike, iv_reliable = _iv_and_strike(tradier, db, ticker, entry)
            if iv_pct:
                iv_dec   = iv_pct / 100
                est_prem = round(strike * iv_dec * math.sqrt(_T) / (2 * math.pi) ** 0.5 * 100)
                bep      = round(strike - est_prem / 100, 2)

                # Key overlay: does chart stop sit above the CSP break-even?
                # If BEP < chart stop → CSP is protected even if chart setup fails
                if bep < stop:
                    bep_note = f"✅ BEP `${bep}` < chart stop `${stop}` — CSP protected even if setup fails"
                else:
                    bep_note = f"⚠️ BEP `${bep}` > chart stop `${stop}` — chart stop does not protect CSP"

                em = round(entry * iv_dec * math.sqrt(_T), 2)

                desc += (
                    f"**Wheel Overlay ({ticker})**\n"
                    f"┣ IVR: `{ivr:.0f}%`  ·  ATM IV: `{iv_pct:.1f}%`\n"
                    f"┣ CSP: STO `${strike}` put · 37 DTE · est `${est_prem}` credit · BEP `${bep}`\n"
                    f"┣ Expected Move (37 DTE): ±`${em}`\n"
                    f"┗ {bep_note}\n\n"
                )
        except Exception:
            pass

    # Ecosystem bias alignment check
    try:
        db        = engine.db
        mkt_bias  = _db_int(db, "market_analysis_bias")
        bottom    = _db_int(db, "tqqq_bottom_score")
        top       = _db_int(db, "tqqq_top_score")
        vix       = _db_float(db, "fred_vix_value")

        aligned = []
        conflict = []

        if bias == "BULLISH":
            if mkt_bias >= 2:   aligned.append("market BULLISH")
            elif mkt_bias <= -2: conflict.append("market BEARISH")
            if bottom >= 55:    aligned.append("LEAP CALL desk active")
            if top >= 55:       conflict.append("LEAP PUT desk active")
            if vix and vix >= 25: conflict.append(f"VIX elevated `{vix:.0f}`")
        elif bias == "BEARISH":
            if mkt_bias <= -2:   aligned.append("market BEARISH")
            elif mkt_bias >= 2:  conflict.append("market BULLISH")
            if top >= 55:        aligned.append("LEAP PUT desk active")
            if bottom >= 55:     conflict.append("LEAP CALL desk active")
            if vix and vix < 16: conflict.append(f"VIX calm `{vix:.0f}` — no fear")

        if aligned or conflict:
            a_str = " · ".join(aligned) if aligned else "none"
            c_str = " · ".join(conflict) if conflict else "none"
            align_icon = "🟢" if aligned and not conflict else ("🔴" if conflict and not aligned else "🟡")
            desc += (
                f"**Ecosystem Alignment**\n"
                f"┣ {align_icon} Aligned: {a_str}\n"
                f"┗ ⚠️ Conflicting: {c_str}\n\n"
            )
    except Exception:
        pass

    desc += f"**Analysis**\n{convo}"

    color = 0x2ecc71 if bias == "BULLISH" else (0xe74c3c if bias == "BEARISH" else 0xf1c40f)
    return desc, color


# ── Slash commands ─────────────────────────────────────────────────────────────

@bot.tree.command(name="analyze", description="Upload any chart screenshot — get AI trade setup + live wheel overlay.")
@app_commands.describe(
    chart="Chart screenshot (PNG/JPG from TradingView or any platform)",
    ticker="Optional: ticker for live wheel/options overlay (e.g. HIMS, PLTR)"
)
async def analyze_chart(interaction: discord.Interaction,
                        chart: discord.Attachment,
                        ticker: str = None):
    await interaction.response.defer(ephemeral=True, thinking=True)
    ticker_up = ticker.upper().strip() if ticker else None
    logger.info(f"/analyze {ticker_up or 'no ticker'} by {interaction.user} — {chart.filename}")

    if not chart.content_type or not chart.content_type.startswith("image/"):
        await interaction.followup.send(
            "Please attach a chart image (PNG or JPG).", ephemeral=True
        )
        return

    try:
        # 1. Download image
        img_b64, media_type = await asyncio.to_thread(_fetch_image_b64, chart.url)
        if not img_b64:
            await interaction.followup.send(
                "Could not download the image. Try again or paste the chart URL.", ephemeral=True
            )
            return

        # 2. Vision analysis
        setup = await asyncio.to_thread(_call_vision, bot.claude, img_b64, media_type)

        # 3. Format with optional live overlay
        desc, color = await asyncio.to_thread(
            _format_analyze_response, setup, ticker_up, bot.engine, bot.tradier
        )

        title = f"📸 CHART ANALYSIS{f' — {ticker_up}' if ticker_up else ''}"
        embed = discord.Embed(title=title, description=desc, color=color)
        embed.set_thumbnail(url=chart.url)
        embed.set_footer(text="Claude Vision · Tradier IV overlay · Ecosystem confluence  |  Research only.")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except json.JSONDecodeError:
        await interaction.followup.send(
            "Vision model returned an unexpected format — try a clearer chart screenshot.", ephemeral=True
        )
    except Exception as e:
        logger.error(f"/analyze critical failure: {e}")
        await interaction.followup.send(
            "Analysis failed — try again in a moment.", ephemeral=True
        )


@bot.tree.command(name="query", description="On-demand intel: spot, IV, wheel setup, sentiment, macro, LEAP context.")
@app_commands.describe(ticker="Ticker symbol (e.g. HIMS, PLTR, COIN, BTC, SCHD, CLM, TQQQ)")
async def query_asset(interaction: discord.Interaction, ticker: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    ticker = ticker.upper().strip()
    logger.info(f"/query {ticker} by {interaction.user}")

    try:
        desc, color = await asyncio.to_thread(route_query, bot.engine, bot.tradier, ticker)

        if not desc:
            await interaction.followup.send(
                f"Could not resolve `{ticker}`. Check the ticker format and try again.",
                ephemeral=True
            )
            return

        embed = discord.Embed(title=f"📊 {ticker}", description=desc, color=color)
        embed.set_footer(
            text="Tradier · SentiSense · Twelve Data · FRED  |  Research only — not financial advice."
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        logger.error(f"/query {ticker} critical failure: {e}")
        await interaction.followup.send(
            "API timeout or data error — try again in a moment.", ephemeral=True
        )


@bot.tree.error
async def on_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    logger.error(f"Command error: {error}")
    if not interaction.response.is_done():
        await interaction.response.send_message("Unexpected error.", ephemeral=True)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not DISCORD_BOT_TOKEN:
        logger.critical("DISCORD_BOT_TOKEN missing from .env")
    else:
        logger.info("Starting /query bot...")
        bot.run(DISCORD_BOT_TOKEN)

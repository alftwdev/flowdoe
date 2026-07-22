"""
research_bot.py — Discord slash command bot: /query <ticker>

Runs as a persistent async process (PythonAnywhere always-on task, slot 6).
All responses are ephemeral (visible only to the requester).

Data sources wired in:
  Tradier   — real ATM IV, IVR, delta-proxied strike, earnings proximity
  SentiSense — sentiment score + social lean (daily cached)
  DB        — tqqq_bottom/top score, market_analysis_bias, vixy_price_realtime,
               fred_vix, fred_hy_spread, fred_yield_spread (written by other scripts)
  Twelve Data via HighFidelityAnalyticsEngine — spot, HV30, RSI, MACD, OHLCV matrix
"""

import os
import math
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv

import discord
from discord import app_commands

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
        self.tree    = app_commands.CommandTree(self)
        self.engine  = HighFidelityAnalyticsEngine()
        self.tradier = TradierClient()

    async def setup_hook(self):
        await self.tree.sync()
        logger.info("Slash commands synced.")

bot = QueryBot()


# ── Shared helpers ─────────────────────────────────────────────────────────────

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
            return f"🟢 BULLISH ({bias:+d}/8 flags)"
        if bias <= -2:
            return f"🔴 BEARISH ({bias:+d}/8 flags)"
        return f"🟡 NEUTRAL ({bias:+d}/8 flags)"
    except Exception:
        return "N/A"


def _macro_line(db) -> str:
    """One-liner: live VIX + HY spread + yield curve from DB (written by FRED fetchers)."""
    vix   = _db_float(db, "fred_vix")
    hy    = _db_float(db, "fred_hy_spread")
    yc    = _db_float(db, "fred_yield_spread")
    vixy  = _db_float(db, "vixy_price_realtime")
    parts = []
    if vix:    parts.append(f"VIX `{vix:.1f}`")
    if vixy:   parts.append(f"VIXY `{vixy:.2f}`")
    if hy:     parts.append(f"HY `{hy:.2f}%`")
    if yc:
        sign = "+" if yc >= 0 else ""
        parts.append(f"T10-T2 `{sign}{yc:.2f}%`")
    return " · ".join(parts) if parts else "N/A"


def _ss_line(db, ticker: str) -> str:
    try:
        data = ss.get_sentiment(db, ticker)
        if not data:
            return "N/A"
        score    = data.get("score", 0)
        lean     = data.get("lean") or data.get("direction") or "Neutral"
        mentions = data.get("mentions", 0)
        sign     = "+" if score >= 0 else ""
        return f"{lean} ({sign}{score:.0f}) · {mentions:,} mentions"
    except Exception:
        return "N/A"


def _iv_and_strike(tradier: TradierClient, db, ticker: str, spot: float) -> tuple:
    """Returns (iv_pct, ivr, ivr_tag, strike, reliable)."""
    try:
        iv_rank = tradier.get_iv_rank(ticker, db)
        iv_dec  = iv_rank.get("current_iv", 0.0)
        ivr     = iv_rank.get("ivr", 0.0)
        tag     = iv_rank.get("tag", "")
        reliable = iv_rank.get("reliable", False)
        if iv_dec > 0:
            iv_pct = round(iv_dec * 100, 1)
            strike = round(spot * math.exp(-0.84 * iv_dec * math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
            return iv_pct, ivr, tag, strike, reliable
    except Exception as e:
        logger.warning(f"Tradier IV failed for {ticker}: {e}")
    return None, None, None, None, False


def _earnings_tag(tradier: TradierClient, ticker: str) -> str:
    try:
        prox = tradier.get_earnings_proximity([ticker])
        ep   = prox.get(ticker, {})
        flag = ep.get("flag", "")
        days = ep.get("days_to_earnings")
        if flag == "FORCE_CLOSE":
            return f"⛔ earnings in {days}d — avoid new entries"
        if flag == "REVIEW":
            return f"⚠️ earnings in {days}d — review before entry"
        if days is not None:
            return f"✅ {days}d to earnings"
        return "✅ no near-term earnings"
    except Exception:
        return "N/A"


def _rsi_macd_line(matrix: dict) -> str:
    rsi     = matrix.get("rsi_14", 0.0) or matrix.get("rsi", 0.0)
    macd    = matrix.get("macd_histogram", 0.0) or matrix.get("macd_hist", 0.0)
    if not rsi and not macd:
        return "N/A"
    rsi_tag  = "🔴 overbought" if rsi >= 70 else ("🟢 oversold" if rsi <= 30 else "🟡 mid")
    macd_dir = "▲ bull" if macd > 0 else "▼ bear"
    return f"RSI `{rsi:.1f}` {rsi_tag} | MACD `{macd:+.3f}` {macd_dir}"


# ── Intel builders ─────────────────────────────────────────────────────────────

def build_tqqq_intel(engine, tradier) -> tuple:
    """Dedicated TQQQ handler — surfaces LEAP desk context + full regime stack."""
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
        vix    = _db_float(db, "fred_vix")
        hy     = _db_float(db, "fred_hy_spread")
        yc     = _db_float(db, "fred_yield_spread")

        month  = datetime.now().month
        scalar = _SEASONAL_CALL_SCALAR.get(month, 1.0)
        scalar_pct = int((scalar - 1.0) * 100)
        if scalar_pct > 0:
            size_tag = f"🟢 +{scalar_pct}% (strong entry month)"
        elif scalar_pct < 0:
            size_tag = f"🔴 {scalar_pct}% (weak entry — wait for 3 green days)"
        else:
            size_tag = "🟡 neutral"

        # LEAP CALL desk status
        if bottom >= 55:
            call_status = f"🟢 ACTIVE — score {bottom}/100 (threshold 55)"
        elif bottom >= 40:
            call_status = f"🟡 WATCHING — score {bottom}/100 (need 55)"
        else:
            call_status = f"⚪ DORMANT — score {bottom}/100"

        # LEAP PUT desk status
        if top >= 55:
            put_status = f"🔴 ACTIVE — score {top}/100"
        elif top >= 40:
            put_status = f"🟡 WATCHING — score {top}/100"
        else:
            put_status = f"⚪ DORMANT — score {top}/100"

        # Regime
        if bias >= 2:
            bias_tag = f"🟢 BULLISH ({bias:+d}/8)"
        elif bias <= -2:
            bias_tag = f"🔴 BEARISH ({bias:+d}/8)"
        else:
            bias_tag = f"🟡 NEUTRAL ({bias:+d}/8)"

        # VIX context
        if vix >= 30:
            vix_tag = "🔴 FEAR SPIKE — PUT profit → rotate to CALLS"
        elif vix >= 20:
            vix_tag = "🟡 ELEVATED"
        else:
            vix_tag = "🟢 CALM"

        # Yield curve
        yc_tag = "🔴 inverted" if yc and yc < 0 else ("🟢 normal" if yc and yc > 0.2 else "🟡 flat")

        # IV for sniper context
        iv_pct, ivr, _, _, iv_reliable = _iv_and_strike(tradier, db, "TQQQ", spot)
        if iv_pct is None:
            hv30   = engine.calculate_historical_volatility("TQQQ", lookback=30) or 50.0
            iv_pct = round(hv30 * 1.15, 1)
            iv_reliable = False

        ss_line = _ss_line(db, "TQQQ")
        rsi_macd = _rsi_macd_line(matrix)

        # Profit cascade reminder if CALL is active
        cascade_line = ""
        if bottom >= 55:
            cascade_line = "┣ 📌 On TP1/TP2: route proceeds → MLPI → expanded margin → CLM/CRF DCA\n"

        desc = (
            f"TQQQ @ `${spot:,.2f}`\n\n"
            f"**LEAP CALL Desk**\n"
            f"┣ Status: {call_status}\n"
            f"┣ Seasonal size: {size_tag}\n"
            f"{cascade_line}"
            f"┗ LEAP PUT Desk: {put_status}\n\n"
            f"**Regime Stack**\n"
            f"┣ Market bias: {bias_tag}\n"
            f"┣ VIX `{vix:.1f}` {vix_tag}{f'  · VIXY `{vixy:.2f}`' if vixy else ''}\n"
            f"┣ HY spread: `{hy:.2f}%`{'  🔴 credit stress' if hy > 4.5 else ''}\n"
            f"┣ Yield curve: `{yc:+.2f}%` {yc_tag}\n"
            f"┣ {rsi_macd}\n"
            f"┗ Sentiment: {ss_line}\n\n"
            f"**TQQQ Options Context**\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n"
            f"┗ LEAP target: Δ0.72 deep ITM · 270–540 DTE · TP1 +50% / TP2 +100%"
        )

        if bottom >= 55:
            color = 0x2ecc71
        elif top >= 55:
            color = 0xe74c3c
        else:
            color = 0xf1c40f

        return desc, color

    except Exception as e:
        logger.error(f"tqqq_intel: {e}")
        return f"Data unavailable for `TQQQ` — try again shortly.", 0xe74c3c


def build_equity_intel(engine, tradier, ticker: str) -> tuple:
    try:
        spot = engine._execute_query("price", {"symbol": ticker})
        spot = float((spot or {}).get("price", 0))
        if not spot:
            return None, None

        db     = engine.db
        matrix = engine.calculate_ohlcv_matrix(ticker)
        hv30   = engine.calculate_historical_volatility(ticker, lookback=30)

        # IV — Tradier first, HV30 proxy fallback
        iv_pct, ivr, ivr_tag, strike, iv_reliable = _iv_and_strike(tradier, db, ticker, spot)
        if iv_pct is None:
            iv_dec  = (hv30 or 20.0) / 100 * 1.15
            iv_pct  = round(iv_dec * 100, 1)
            ivr     = iv_pct
            strike  = round(spot * math.exp(-0.84 * iv_dec * math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
            iv_reliable = False
        else:
            iv_dec = iv_pct / 100

        # Premium estimate
        est_prem = round(strike * iv_dec * math.sqrt(_T) / (2 * math.pi) ** 0.5 * 100)

        # IVR environment
        if ivr >= 60:
            env = "Elevated — premium crush favorable"; env_icon = "🟢"
        elif ivr >= 35:
            env = "Mid-range — sellable premium"; env_icon = "🟡"
        else:
            env = "Low — consider defined-risk or wait"; env_icon = "🔴"

        flow  = "ACCUMULATION" if matrix.get("volume_surge") and matrix.get("sigma", 0) > 0 else \
                ("DISTRIBUTION" if matrix.get("volume_surge") else "NOMINAL")
        sigma = matrix.get("sigma", 0.0)

        earnings_tag = _earnings_tag(tradier, ticker)
        rsi_macd     = _rsi_macd_line(matrix)
        ss_line      = _ss_line(db, ticker)
        cycle_line   = _cycle_bias(db)
        bias_line    = _market_bias_line(db)
        macro_line   = _macro_line(db)

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**IV / Wheel Setup**\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n"
            f"┣ IVR: {env_icon} `{ivr:.0f}%` — {env}\n"
            f"┣ CSP setup: STO `${strike}` put · {_DTE_MID} DTE · est `${est_prem}` credit\n"
            f"┗ Earnings: {earnings_tag}\n\n"
            f"**Market Signal**\n"
            f"┣ Order flow: `{flow}` ({sigma:+.2f}σ)\n"
            f"┣ {rsi_macd}\n"
            f"┣ Sentiment: {ss_line}\n"
            f"┣ Market bias: {bias_line}\n"
            f"┣ Macro: {macro_line}\n"
            f"┗ TQQQ cycle: {cycle_line}"
        )
        color = 0x2ecc71 if ivr >= 35 else 0xe67e22
        return desc, color

    except Exception as e:
        logger.error(f"equity_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}` — verify ticker or try again shortly.", 0xe74c3c


def build_income_intel(engine, tradier, ticker: str) -> tuple:
    """Income/CEF/dividend ticker — CSP wheel setup focus."""
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

        ss_line    = _ss_line(db, ticker)
        cycle_line = _cycle_bias(db)
        bias_line  = _market_bias_line(db)

        # CLM/CRF get fair-value floor context
        fv_line = ""
        if ticker == "CLM":
            fv = 7.51
            fv_line = f"┣ Fair value floor: `${fv}` (19% yield target){' 🟢 at/below — accumulate' if spot <= fv else ' 🔴 above — wait'}\n"
        elif ticker == "CRF":
            fv = 7.28
            fv_line = f"┣ Fair value floor: `${fv}` (19% yield target){' 🟢 at/below — accumulate' if spot <= fv else ' 🔴 above — wait'}\n"

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**Wheel / Income Setup**\n"
            f"┣ CSP strike: `${strike}` · {_DTE_MID} DTE\n"
            f"┣ Est. credit: `${est_prem}` per contract\n"
            f"┣ Annualized ROI: `~{ann_roi}%`\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n"
            f"{fv_line}"
            f"**Context**\n"
            f"┣ Sentiment: {ss_line}\n"
            f"┣ Market bias: {bias_line}\n"
            f"┗ TQQQ cycle: {cycle_line}"
        )
        return desc, 0xf1c40f

    except Exception as e:
        logger.error(f"income_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}`.", 0xe74c3c


def build_crypto_intel(engine, ticker: str) -> tuple:
    try:
        td_sym    = ticker if "/" in ticker else f"{ticker}/USD"
        spot_data = engine._execute_query("price", {"symbol": td_sym})
        spot      = float((spot_data or {}).get("price", 0))
        if not spot:
            return None, None

        db         = engine.db
        support    = round(spot * 0.94, 2)
        resist     = round(spot * 1.08, 2)
        ss_line    = _ss_line(db, ticker.split("/")[0])
        cycle_line = _cycle_bias(db)
        bias_line  = _market_bias_line(db)

        # Binance-style signals from DB (written by scheduler.py crypto_social)
        btc_oi    = _db_float(db, "binance_btc_oi")
        btc_top_ls = _db_float(db, "binance_btc_top_ls")
        btc_gl_ls  = _db_float(db, "binance_btc_global_ls")
        sm_line = ""
        if btc_top_ls and btc_gl_ls:
            if btc_top_ls > 1.1 and btc_gl_ls < 1.0:
                sm_line = "┣ Smart money: 🟢 DIVERGING LONG (top L/S > 1.1, retail short)\n"
            elif btc_top_ls < 0.9 and btc_gl_ls > 1.1:
                sm_line = "┣ Smart money: 🔴 DIVERGING SHORT (top L/S < 0.9, retail long)\n"

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**Key Levels**\n"
            f"┣ Resistance: `${resist:,.2f}` (+8%)\n"
            f"┣ Support: `${support:,.2f}` (−6%)\n"
            f"┣ Entry range: `${round(spot*0.97,2):,.2f} – ${round(spot*0.99,2):,.2f}`\n"
            f"┗ Invalidation: `${round(spot*0.92,2):,.2f}` (−8%)\n\n"
            f"**Context**\n"
            f"┣ Sentiment: {ss_line}\n"
            f"{sm_line}"
            f"┣ Market bias (equity): {bias_line}\n"
            f"┗ TQQQ cycle: {cycle_line}"
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


# ── Slash commands ─────────────────────────────────────────────────────────────

@bot.tree.command(name="query", description="On-demand intel: spot, IV, wheel setup, sentiment, macro, LEAP context.")
@app_commands.describe(ticker="Ticker symbol (e.g. AAPL, COIN, BTC, SCHD, CLM, TQQQ)")
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
        embed.set_footer(text="Tradier · SentiSense · Twelve Data · FRED  |  Research only — not financial advice.")
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

"""
research_bot.py — Discord slash command bot: /query <ticker>

Runs as a persistent async process (PythonAnywhere always-on task, slot 6).
All responses are ephemeral (visible only to the requester).

Data sources:
  - Tradier: real ATM IV, IVR, delta-proxied strike
  - SentiSense: sentiment score + social lean (daily cached)
  - DB: tqqq_bottom_score / tqqq_top_score (written by tqqq.py)
  - Twelve Data via HighFidelityAnalyticsEngine: spot, HV30, OHLCV matrix
"""

import os
import math
import asyncio
import logging
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


# ── Bot setup ─────────────────────────────────────────────────────────────────

class QueryBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree   = app_commands.CommandTree(self)
        self.engine = HighFidelityAnalyticsEngine()
        self.tradier = TradierClient()

    async def setup_hook(self):
        await self.tree.sync()
        logger.info("Slash commands synced.")

bot = QueryBot()


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _cycle_bias(db) -> str:
    """One-line cycle posture from tqqq.py scores in DB."""
    try:
        bottom = int(db.get_state("tqqq_bottom_score") or 0)
        top    = int(db.get_state("tqqq_top_score")    or 0)
        if bottom >= 55:
            return f"Bottom score {bottom}/100 — CALL desk active 🟢"
        if top >= 55:
            return f"Top score {top}/100 — PUT desk active 🔴"
        return f"Neutral — bottom {bottom} / top {top}"
    except Exception:
        return "N/A"


def _ss_line(db, ticker: str) -> str:
    """One-line SentiSense sentiment summary."""
    try:
        data = ss.get_sentiment(db, ticker)
        if not data:
            return "N/A"
        score = data.get("score", 0)
        lean  = data.get("lean") or data.get("direction") or "Neutral"
        mentions = data.get("mentions", 0)
        sign  = "+" if score >= 0 else ""
        return f"{lean} ({sign}{score:.0f}) · {mentions:,} mentions"
    except Exception:
        return "N/A"


def _iv_and_strike(tradier: TradierClient, db, ticker: str, spot: float) -> tuple:
    """
    Returns (iv_pct, ivr, ivr_tag, strike, reliable).
    iv_pct is percentage (e.g. 45.0 for 45%).
    Falls back to HV30×1.15 proxy when Tradier returns nothing.
    """
    try:
        iv_rank = tradier.get_iv_rank(ticker, db)
        iv_dec  = iv_rank.get("current_iv", 0.0)
        ivr     = iv_rank.get("ivr", 0.0)
        tag     = iv_rank.get("tag", "")
        reliable = iv_rank.get("reliable", False)
        if iv_dec > 0:
            iv_pct = round(iv_dec * 100, 1)
            # 0.20-delta put strike via BS approximation at DTE_MID
            strike = round(spot * math.exp(-0.84 * iv_dec * math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
            return iv_pct, ivr, tag, strike, reliable
    except Exception as e:
        logger.warning(f"Tradier IV failed for {ticker}: {e}")
    return None, None, None, None, False


# ── Intel builders ─────────────────────────────────────────────────────────────

def build_equity_intel(engine, tradier, ticker: str) -> tuple:
    """Returns (description_str, embed_color)."""
    try:
        spot = engine._execute_query("price", {"symbol": ticker})
        spot = float((spot or {}).get("price", 0))
        if not spot:
            return None, None

        db = engine.db
        matrix = engine.calculate_ohlcv_matrix(ticker)
        hv30   = engine.calculate_historical_volatility(ticker, lookback=30)

        # IV — Tradier first, HV30 proxy fallback
        iv_pct, ivr, ivr_tag, strike, iv_reliable = _iv_and_strike(tradier, db, ticker, spot)
        if iv_pct is None:
            iv_dec  = (hv30 or 20.0) / 100 * 1.15
            iv_pct  = round(iv_dec * 100, 1)
            ivr     = iv_pct           # best guess
            ivr_tag = "~proxy (HV30×1.15)"
            strike  = round(spot * math.exp(-0.84 * iv_dec * math.sqrt(_T) + 0.5 * iv_dec**2 * _T))
            iv_reliable = False

        # Premium estimate (mid-point of strike × IV × √T)
        iv_dec_final = iv_pct / 100
        est_prem = round(strike * iv_dec_final * math.sqrt(_T) / (2 * math.pi) ** 0.5 * 100)

        # Direction / flow
        flow   = "ACCUMULATION" if matrix.get("volume_surge") and matrix.get("sigma", 0) > 0 else \
                 ("DISTRIBUTION" if matrix.get("volume_surge") else "NOMINAL")
        sigma  = matrix.get("sigma", 0.0)

        # Cycle + sentiment
        cycle_line = _cycle_bias(db)
        ss_line    = _ss_line(db, ticker)

        # IVR environment tag
        if ivr >= 60:
            env = "Elevated — premium crush favorable"
            env_icon = "🟢"
        elif ivr >= 35:
            env = "Mid-range — sellable premium"
            env_icon = "🟡"
        else:
            env = "Low — consider defined-risk or wait"
            env_icon = "🔴"

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**IV / Premium**\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n"
            f"┣ IVR: {env_icon} `{ivr:.0f}%` — {env}\n"
            f"┣ Wheel setup: STO `${strike}` put · {_DTE_MID} DTE · est `${est_prem}` credit\n\n"
            f"**Market Signal**\n"
            f"┣ Order flow: `{flow}` ({sigma:+.2f}σ)\n"
            f"┣ Sentiment: {ss_line}\n"
            f"┗ Cycle bias: {cycle_line}"
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

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**Wheel / Income Setup**\n"
            f"┣ Strike: `${strike}` CSP · {_DTE_MID} DTE\n"
            f"┣ Est. credit: `${est_prem}` per contract\n"
            f"┣ Annualized ROI: `~{ann_roi}%`\n"
            f"┣ ATM IV: `{iv_pct:.1f}%`{'  ✅' if iv_reliable else '  ~proxy'}\n\n"
            f"**Context**\n"
            f"┣ Sentiment: {ss_line}\n"
            f"┗ Cycle bias: {cycle_line}"
        )
        return desc, 0xf1c40f

    except Exception as e:
        logger.error(f"income_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}`.", 0xe74c3c


def build_crypto_intel(engine, ticker: str) -> tuple:
    """Crypto spot + support/resistance + sentiment."""
    try:
        td_sym = ticker if "/" in ticker else f"{ticker}/USD"
        spot_data = engine._execute_query("price", {"symbol": td_sym})
        spot = float((spot_data or {}).get("price", 0))
        if not spot:
            return None, None

        db = engine.db
        support = round(spot * 0.94, 2)
        resist  = round(spot * 1.08, 2)
        ss_line = _ss_line(db, ticker.split("/")[0])
        cycle_line = _cycle_bias(db)

        desc = (
            f"**Spot:** `${spot:,.2f}`\n\n"
            f"**Levels**\n"
            f"┣ Resistance: `${resist:,.2f}` (+8%)\n"
            f"┣ Support: `${support:,.2f}` (−6%)\n"
            f"┣ Entry range: `${round(spot*0.97,2):,.2f} – ${round(spot*0.99,2):,.2f}`\n"
            f"┣ Invalidation: `${round(spot*0.92,2):,.2f}` (−8%)\n\n"
            f"**Context**\n"
            f"┣ Sentiment: {ss_line}\n"
            f"┗ Cycle bias (equity): {cycle_line}"
        )
        return desc, 0xf39c12

    except Exception as e:
        logger.error(f"crypto_intel {ticker}: {e}")
        return f"Data unavailable for `{ticker}`.", 0xe74c3c


def route_query(engine, tradier, ticker: str) -> tuple:
    base = ticker.split("/")[0].upper()
    if base in _CRYPTO_TICKERS or "BTC" in ticker or "ETH" in ticker:
        return build_crypto_intel(engine, ticker)
    if ticker in _INCOME_TICKERS:
        return build_income_intel(engine, tradier, ticker)
    return build_equity_intel(engine, tradier, ticker)


# ── Slash commands ─────────────────────────────────────────────────────────────

@bot.tree.command(name="query", description="On-demand intel for any ticker — options setup, IV, sentiment, cycle bias.")
@app_commands.describe(ticker="Ticker symbol (e.g. AAPL, COIN, BTC, SCHD, CLM)")
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
        embed.set_footer(text="Tradier · SentiSense · Twelve Data  |  For research only — not financial advice.")
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

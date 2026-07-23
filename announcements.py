"""
announcements.py — Weekly Accuracy Scorecard + Prediction Grader
Cashflow ZZZ Machine | Rockefeller Ecosystem

Runs weekly (Friday 20:45 UTC via market_scheduler SCHEDULE_FRIDAY_ONLY).
Also callable: python announcements.py [--grade-only | --publish-only | --test]

Prediction sources (logged by other scripts via db.log_prediction()):
  market_direction  → market_analysis.py bias (BULLISH/BEARISH) → grade next-day SPY
  tqqq_call         → tqqq.py bottom_score ≥ 55 → grade TQQQ at T+30
  tqqq_put          → tqqq.py top_score ≥ 55 → grade QQQ at T+30
  clm_floor         → monitor.py yield floor 🟢 → grade CLM at T+14
  btc_sentiment     → scheduler.py F&G < 25 → grade BTC at T+7

Grading thresholds:
  market_direction  WIN: SPY moved in predicted direction next trading day
  tqqq_call         WIN: TQQQ +10% from entry within 30 days
  tqqq_put          WIN: QQQ −8% from entry within 30 days
  clm_floor         WIN: CLM +2% from entry within 14 days (near-FV accumulation play)
  btc_sentiment     WIN: BTC +5% from entry within 7 days

Output: #announcements channel (free-tier teaser — locked content described but gated)
"""

import os
import sys
import logging
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [Announcements] %(levelname)s %(message)s")
logger = logging.getLogger("Announcements")

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_ANNOUNCEMENTS = os.getenv("WEBHOOK_ANNOUNCEMENTS")

# ── Win thresholds ────────────────────────────────────────────────────────────
WIN_THRESHOLDS = {
    "market_direction": 0.0,   # any move in predicted direction
    "tqqq_call":       10.0,   # TQQQ +10%
    "tqqq_put":        -8.0,   # QQQ -8%
    "clm_floor":        2.0,   # CLM +2%
    "btc_sentiment":    5.0,   # BTC +5%
}

# ── Human labels ─────────────────────────────────────────────────────────────
SIGNAL_LABELS = {
    "market_direction": "/NQ direction",
    "tqqq_call":        "TQQQ CALL entry",
    "tqqq_put":         "QQQ PUT entry",
    "clm_floor":        "CLM accumulation",
    "btc_sentiment":    "BTC sentiment",
}

# ── Price fetch ───────────────────────────────────────────────────────────────

def _fetch_price(symbol: str, session: requests.Session) -> float:
    """Fetch current price from Twelve Data. Returns 0.0 on failure."""
    try:
        res = session.get(
            f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}",
            timeout=15
        ).json()
        if res.get("code") == 429:
            import time
            time.sleep(62)
            res = session.get(
                f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}",
                timeout=15
            ).json()
        return float(res.get("price", 0.0))
    except Exception as e:
        logger.warning(f"Price fetch failed for {symbol}: {e}")
        return 0.0


# ── Ticker map per signal type ────────────────────────────────────────────────
GRADE_TICKERS = {
    "market_direction": "SPY",
    "tqqq_call":        "TQQQ",
    "tqqq_put":         "QQQ",
    "clm_floor":        "CLM",
    "btc_sentiment":    "BTC/USD",
}


def _pct_change(entry: float, exit_: float) -> float:
    if not entry:
        return 0.0
    return (exit_ - entry) / entry * 100.0


def _grade_one(pred: dict, exit_price: float) -> tuple:
    """
    Returns (outcome, score, notes) for a single prediction.
    outcome: WIN | LOSS | NEUTRAL
    """
    entry = pred.get("entry_price") or 0.0
    direction = pred.get("predicted_direction", "NEUTRAL")
    stype = pred["signal_type"]
    pct = _pct_change(entry, exit_price)

    if stype == "market_direction":
        # WIN = SPY moved in the predicted direction
        if direction == "BULLISH" and pct > 0:
            return "WIN", 1.0, f"SPY {pct:+.2f}%"
        elif direction == "BEARISH" and pct < 0:
            return "WIN", 1.0, f"SPY {pct:+.2f}%"
        elif direction == "NEUTRAL" and abs(pct) < 0.3:
            return "WIN", 1.0, f"SPY {pct:+.2f}% (chop as expected)"
        else:
            return "LOSS", -1.0, f"SPY {pct:+.2f}% — opposite direction"

    threshold = WIN_THRESHOLDS.get(stype, 0.0)
    ticker = GRADE_TICKERS.get(stype, stype)

    if stype == "tqqq_put":
        # Bearish: need price to DROP by threshold
        if pct <= threshold:  # threshold is negative
            return "WIN", 1.0, f"{ticker} {pct:+.2f}% vs target {threshold:+.1f}%"
        elif pct >= 0:
            return "LOSS", -1.0, f"{ticker} {pct:+.2f}% — reversed"
        else:
            return "NEUTRAL", 0.0, f"{ticker} {pct:+.2f}% — partial move"
    else:
        # Bullish: need price to RISE by threshold
        if pct >= threshold:
            return "WIN", 1.0, f"{ticker} {pct:+.2f}% vs target +{threshold:.1f}%"
        elif pct < 0:
            return "LOSS", -1.0, f"{ticker} {pct:+.2f}% — reversed"
        else:
            return "NEUTRAL", 0.0, f"{ticker} {pct:+.2f}% — not yet at target"


# ── Grader ────────────────────────────────────────────────────────────────────

def grade_pending(db, is_test: bool = False) -> int:
    """
    Fetches current prices for all pending predictions past their target_date.
    Grades each and writes the result back to signal_ledger.
    Returns count of graded predictions.
    """
    pending = db.get_pending_predictions()
    if not pending:
        logger.info("No pending predictions to grade.")
        return 0

    session = requests.Session()
    price_cache: dict[str, float] = {}
    graded = 0

    for pred in pending:
        stype = pred["signal_type"]
        grade_ticker = GRADE_TICKERS.get(stype)
        if not grade_ticker:
            continue

        if grade_ticker not in price_cache:
            px = _fetch_price(grade_ticker, session)
            if px > 0:
                price_cache[grade_ticker] = px
            else:
                logger.warning(f"Could not fetch price for {grade_ticker} — skipping grade")
                continue

        exit_price = price_cache[grade_ticker]
        outcome, score, notes = _grade_one(pred, exit_price)

        if is_test:
            logger.info(f"[TEST] Would grade #{pred['id']} {stype}/{pred['ticker']} → {outcome} ({notes})")
        else:
            db.grade_prediction(pred["id"], exit_price, outcome, score, notes)
            logger.info(f"Graded #{pred['id']} {stype} → {outcome} ({notes})")
        graded += 1

    return graded


# ── Scorecard publisher ───────────────────────────────────────────────────────

def _outcome_emoji(outcome: str) -> str:
    return {"WIN": "✅", "LOSS": "❌", "NEUTRAL": "➖"}.get(outcome, "⏳")


def _direction_label(d: str, stype: str) -> str:
    if d == "BULLISH":
        return "Bullish 📈"
    if d == "BEARISH":
        return "Bearish 📉"
    return "Neutral ➖"


def build_scorecard_embed(db, week_label: str = None):
    """Builds the Discord embed payload for the weekly scorecard."""
    week_label = week_label or date.today().strftime("Week of %b %d, %Y")
    rows = db.get_scorecard_window(days_back=7)

    if not rows:
        logger.info("No graded predictions in the last 7 days — nothing to publish.")
        return None

    mtd_wins, mtd_total = db.get_mtd_accuracy()
    week_wins = sum(1 for r in rows if r["outcome"] == "WIN")
    week_total = len(rows)
    week_pct = round(week_wins / week_total * 100) if week_total else 0
    mtd_pct   = round(mtd_wins / mtd_total * 100)  if mtd_total else 0

    # Header
    accuracy_bar = "🎯" if week_pct >= 75 else ("📊" if week_pct >= 50 else "📉")

    # Signal rows — max 6 most recent to keep mobile-readable
    signal_lines = []
    for r in rows[:6]:
        label   = SIGNAL_LABELS.get(r["signal_type"], r["signal_type"])
        pred    = _direction_label(r["predicted_direction"], r["signal_type"])
        actual  = r.get("notes", "—")
        emoji   = _outcome_emoji(r["outcome"])
        signal_lines.append(f"`{label:<18}` | {pred:<12} | {actual:<18} | {emoji}")

    table_header = f"`{'Signal':<18}` | `{'Predicted':<12}` | `{'Actual':<18}` | Score"
    table_sep    = "─" * 60

    # Locked content tease — the hook that drives conversion
    locked_tease = (
        "\n🔒 **Subscribers this week received:**\n"
        "┣ Full morning conviction brief (8-flag bias)\n"
        "┣ TQQQ LEAP desk cycle score + entry alerts\n"
        "┣ Wheel strike targets + Kelly-sized positions\n"
        "┣ CLM/CRF yield-floor accumulation signals\n"
        "┗ → [Join Pro tier to unlock](https://discord.gg/your-server)"
    )

    description = (
        f"📊 **WEEKLY ACCURACY SCORECARD — {week_label}**\n"
        f"{table_sep}\n"
        f"{table_header}\n"
        + "\n".join(signal_lines)
        + f"\n{table_sep}\n"
        f"**WEEK: {week_wins}/{week_total} — {week_pct}% {accuracy_bar}**"
        f"  |  MTD: {mtd_wins}/{mtd_total} — {mtd_pct}%"
        + locked_tease
    )

    color = 0x2ecc71 if week_pct >= 75 else (0xe67e22 if week_pct >= 50 else 0xe74c3c)

    return {
        "embeds": [{
            "title": "Rockefeller Intelligence | Free Tier Scorecard",
            "description": description,
            "color": color,
            "footer": {"text": "Research only — not financial advice. Predictions logged at signal time, graded at target date."}
        }]
    }


def publish_scorecard(db, is_test: bool = False) -> bool:
    """Builds and posts the weekly scorecard to #announcements."""
    embed = build_scorecard_embed(db)
    if not embed:
        return False

    if is_test:
        import json
        logger.info(f"[TEST] Scorecard embed:\n{json.dumps(embed, indent=2)}")
        return True

    if not WEBHOOK_ANNOUNCEMENTS:
        logger.error("WEBHOOK_ANNOUNCEMENTS not set — cannot publish scorecard.")
        return False

    try:
        r = requests.post(WEBHOOK_ANNOUNCEMENTS, json=embed, timeout=10)
        if r.status_code in (200, 204):
            logger.info("Weekly scorecard published to #announcements.")
            return True
        else:
            logger.error(f"Webhook failed: {r.status_code} {r.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"publish_scorecard error: {e}")
        return False


# ── CLI entry ─────────────────────────────────────────────────────────────────

def run(is_test: bool = False, grade_only: bool = False, publish_only: bool = False):
    from database import EcosystemDatabase
    db = EcosystemDatabase()

    if not publish_only:
        graded = grade_pending(db, is_test=is_test)
        logger.info(f"Graded {graded} prediction(s).")

    if not grade_only:
        publish_scorecard(db, is_test=is_test)


if __name__ == "__main__":
    args = set(sys.argv[1:])
    run(
        is_test       = "--test"         in args,
        grade_only    = "--grade-only"   in args,
        publish_only  = "--publish-only" in args,
    )

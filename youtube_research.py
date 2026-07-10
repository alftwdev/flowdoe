"""
youtube_research.py — YouTube Playlist Research Pipeline

Pipeline:
  1. Fetch playlist video IDs via YouTube Data API v3
  2. Pull transcript per video via youtube-transcript-api (no OAuth needed)
  3. Extract numbered key points via Claude API
  4. Email numbered list to personal inbox daily (new videos only)
  5. Approve points with: python youtube_research.py --approve "1,2,4"
  6. Approval triggers integration analysis → maps points to existing scripts/channels
  7. Integration report emailed for your review

PythonAnywhere cron (daily, after market close):
  22:00 UTC  python3.10 /home/alftw/scripts/youtube_research.py

Dependencies (pip install):
  youtube-transcript-api
  anthropic
"""

import os
import sys
import json
import re
import logging
import smtplib
import argparse
import sqlite3
import requests
from datetime import datetime
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("YouTubeResearch")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL")
EMAIL_PASSWORD  = os.getenv("EMAIL_APP_PASSWORD")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ecosystem.db")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db():
    return sqlite3.connect(DB_PATH)

def get_known_video_ids(playlist_id: str) -> set:
    with _db() as conn:
        rows = conn.execute(
            "SELECT video_id FROM youtube_videos WHERE playlist_id = ?", (playlist_id,)
        ).fetchall()
    return {r[0] for r in rows}

def register_video(video_id: str, playlist_id: str, title: str):
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO youtube_videos (video_id, playlist_id, title) VALUES (?,?,?)",
            (video_id, playlist_id, title),
        )
        conn.commit()

def mark_video_processed(video_id: str):
    with _db() as conn:
        conn.execute(
            "UPDATE youtube_videos SET transcript_fetched=1, processed_date=? WHERE video_id=?",
            (datetime.now().strftime("%Y-%m-%d"), video_id),
        )
        conn.commit()

def store_key_points(video_id: str, points: list[str]):
    """points is a list of strings already stripped of leading numbers."""
    with _db() as conn:
        for i, content in enumerate(points, start=1):
            conn.execute(
                "INSERT INTO youtube_key_points (video_id, point_number, content) VALUES (?,?,?)",
                (video_id, i, content),
            )
        conn.commit()

def get_pending_key_points() -> list[dict]:
    """Returns all unprocessed key points with their global sequential ID."""
    with _db() as conn:
        rows = conn.execute("""
            SELECT kp.id, kp.video_id, yv.title, kp.point_number, kp.content
            FROM youtube_key_points kp
            JOIN youtube_videos yv ON kp.video_id = yv.video_id
            WHERE kp.approved = 0 AND kp.integration_notes IS NULL
            ORDER BY kp.id
        """).fetchall()
    return [
        {"id": r[0], "video_id": r[1], "title": r[2], "point_number": r[3], "content": r[4]}
        for r in rows
    ]

def approve_points(point_ids: list[int], integration_notes: str):
    with _db() as conn:
        for pid in point_ids:
            conn.execute(
                "UPDATE youtube_key_points SET approved=1, integration_notes=? WHERE id=?",
                (integration_notes, pid),
            )
        conn.commit()

def get_approved_points() -> list[dict]:
    with _db() as conn:
        rows = conn.execute("""
            SELECT kp.id, yv.title, kp.content, kp.integration_notes
            FROM youtube_key_points kp
            JOIN youtube_videos yv ON kp.video_id = yv.video_id
            WHERE kp.approved = 1
            ORDER BY kp.id
        """).fetchall()
    return [{"id": r[0], "title": r[1], "content": r[2], "notes": r[3]} for r in rows]


# ── YouTube API ───────────────────────────────────────────────────────────────

def fetch_playlist_videos(playlist_id: str) -> list[dict]:
    """Returns [{video_id, title}] for all items in the playlist (handles pagination)."""
    if not YOUTUBE_API_KEY:
        logger.error("YOUTUBE_API_KEY not set in .env")
        return []

    videos = []
    url    = "https://www.googleapis.com/youtube/v3/playlistItems"
    params = {
        "part":       "snippet,contentDetails",
        "playlistId": playlist_id,
        "maxResults": 50,
        "key":        YOUTUBE_API_KEY,
    }
    while True:
        resp = requests.get(url, params=params, timeout=15).json()
        if "error" in resp:
            logger.error(f"YouTube API error: {resp['error'].get('message')}")
            break
        for item in resp.get("items", []):
            videos.append({
                "video_id": item["contentDetails"]["videoId"],
                "title":    item["snippet"]["title"],
            })
        next_page = resp.get("nextPageToken")
        if not next_page:
            break
        params["pageToken"] = next_page
    return videos


# ── Transcript ────────────────────────────────────────────────────────────────

def fetch_transcript(video_id: str) -> str:
    """
    Fetches auto-generated or manual captions via youtube-transcript-api.
    Returns plain text, or empty string on failure (private video, no captions).
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["en", "en-US"])
        return " ".join(seg["text"] for seg in transcript)
    except Exception as e:
        logger.warning(f"[{video_id}] Transcript unavailable: {e}")
        return ""


# ── Claude API key point extraction ──────────────────────────────────────────

def extract_key_points(title: str, transcript: str) -> list[str]:
    """
    Calls Claude API to extract actionable investing key points from the transcript.
    Returns a list of plain strings (no leading numbers).
    Falls back to empty list on failure.
    """
    if not ANTHROPIC_KEY:
        logger.error("ANTHROPIC_API_KEY not set in .env — add it to enable key point extraction.")
        return []
    if not transcript:
        return []

    # Trim transcript to ~6,000 words to stay within context limits
    words      = transcript.split()
    trimmed    = " ".join(words[:6000])
    word_count = len(words)
    if word_count > 6000:
        trimmed += f"\n[Transcript trimmed at 6,000 of {word_count} words]"

    prompt = f"""You are analyzing a YouTube video titled: "{title}"

Below is the transcript. Extract the most actionable investing/income key points — things
a real investor could actually implement. Focus on:
- Specific strategies, entry/exit criteria, or timing rules
- Named tickers, funds, or asset types with concrete use case
- Risk management rules or position sizing guidelines
- Income mechanics (dividend capture, premium selling, yield thresholds)

Ignore generic financial advice, ads, channel plugs, and filler commentary.

Return ONLY a numbered list. Each item: one clear, specific, actionable sentence.
Maximum 10 points. If the video has no actionable investing content, return "NO_ACTIONABLE_POINTS".

Transcript:
{trimmed}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()
        if "NO_ACTIONABLE_POINTS" in raw:
            logger.info(f"No actionable points in: {title}")
            return []
        # Parse numbered lines: "1. ..." or "1) ..."
        points = []
        for line in raw.splitlines():
            line = line.strip()
            match = re.match(r"^\d+[\.\)]\s+(.+)$", line)
            if match:
                points.append(match.group(1).strip())
        return points
    except Exception as e:
        logger.error(f"Claude API extraction failed: {e}")
        return []


# ── Integration analysis ──────────────────────────────────────────────────────

# Ecosystem context injected into the integration prompt so Claude can map
# approved points to real files/functions without hallucinating.
ECOSYSTEM_CONTEXT = """
Ecosystem scripts and their roles:
- monitor.py: CLM/CRF protection. Functions: check_sec_edgar() (N-2/13D EDGAR), fetch_live_metrics(), detect_dark_pool_activity(), detect_premium_compression(), send_daily_pulse(). Dispatches to #cornerstone.
- analytics.py: HighFidelityAnalyticsEngine. Key functions: generate_dividend_wheel_candidates() (RSI/BB/IVR-filtered CSP setups), generate_tier2_iv_rank_alerts() (IVR > 35% screener), generate_new_income_etf_screener() (YieldMax/Roundhill/NEOS/TappAlpha), generate_market_analysis_morning_report() (0800 HST brief), run_iv_crush_scan(), scan_unusual_options_flow(). WHEEL_UNIVERSE: AAPL, MSFT, GOOGL, AMZN, META, NVDA, AMD, SCHD, JEPI, JEPQ, O, ARCC, TSLA, COIN, SOFI, PLTR, HIMS, SPY, QQQ, IWM, GLD, XLE.
- tqqq.py: TQQQ sniper (0.20 delta BTO calls, 90-180 DTE) + insurance put renewal (14 DTE). Dispatches to #options-wheel.
- cross_asset.py: ES/NQ futures board (4x/day), Initial Balance breakout scanner, volume-delta confirmation. Dispatches to #futures-trading.
- scheduler.py modes: morning, eod, income, iv_crush, wheel_signals, wheel_position, trending_plays, crypto_social, futures_social, spx_income, store_daily_iv.
- stream.py: BTC/USD hourly vol alerts, SPY/QQQ perimeter alerts, VIXY real-time price.
- tradier_client.py: Real options chain (IV, delta, OI, bid/ask), get_atm_iv(), get_iv_rank(), get_gex(), find_csp_strike(), get_spx_0dte_condor().

Data sources:
- Twelve Data: price, OHLCV, RSI, time series, EDGAR filings, dividends history
- Tradier: real options chains, greeks, OI — IV becoming reliable ~Aug 22 2026 after 30 days stored
- Discord channels: #cornerstone, #market-analysis, #futures-trading, #options-wheel, #dividend-ccetfs, #crypto, #announcements
- Notifications: Discord webhooks + Pushover (personal/sensitive data) + Gmail SMTP

Strategy framework (CLAUDE.md):
- Tier 1: CLM/CRF — DRIP at NAV, rights offering dodge, never interrupted
- Tier 2: MAIN/MLPI/TDAQ/KQQQ — cash dividends to margin paydown (in cornerstone/monitor only)
- Wheel: 0.20 delta CSP, 30-45 DTE, IVR > 35%, close 50% profit, roll 21 DTE
- TQQQ sniper: BTO calls when QQQ > 21 EMA + VIX < 20; insurance puts always open at 14 DTE
- Margin rule: never exceed 25% of portfolio value
"""

def analyze_integration(approved_points: list[dict]) -> str:
    """
    Calls Claude API to map approved key points to specific ecosystem integration opportunities.
    Returns a formatted report string.
    """
    if not ANTHROPIC_KEY:
        return "ANTHROPIC_API_KEY not set — cannot run integration analysis."
    if not approved_points:
        return "No approved points to analyze."

    points_text = "\n".join(
        f"{i+1}. [{p['title']}] {p['content']}"
        for i, p in enumerate(approved_points)
    )

    prompt = f"""You are a senior Python developer working on a trading/income ecosystem.
Below is a description of the ecosystem, then a list of investing tactics approved by the operator.

For each tactic, determine:
A) Whether it is ALREADY IMPLEMENTED (cite the exact function/file)
B) Whether it is PARTIALLY IMPLEMENTED (what's missing)
C) Whether it is NEW SCOPE (not yet built — describe what to add and where)

Be specific: name the file, function, variable, or Discord channel. If a tactic requires
Twelve Data or Tradier data, say which endpoint. If it needs a new DB column, say so.
If a tactic conflicts with the existing strategy framework, flag the conflict.

Output format per point:
[N] STATUS: ALREADY IMPLEMENTED / PARTIAL / NEW SCOPE
    File/Function: ...
    Gap or Action: ...

ECOSYSTEM:
{ECOSYSTEM_CONTEXT}

APPROVED TACTICS:
{points_text}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-sonnet-5",
                "max_tokens": 2048,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"].strip()
    except Exception as e:
        logger.error(f"Integration analysis failed: {e}")
        return f"Integration analysis failed: {e}"


# ── Email ─────────────────────────────────────────────────────────────────────

def send_email(subject: str, body: str):
    if not SENDER_EMAIL or not EMAIL_PASSWORD:
        logger.error("Email credentials not set in .env")
        return
    try:
        msg            = EmailMessage()
        msg["Subject"] = subject
        msg["From"]    = SENDER_EMAIL
        msg["To"]      = SENDER_EMAIL   # personal inbox only — never Discord
        msg.set_content(body)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SENDER_EMAIL, EMAIL_PASSWORD)
            smtp.send_message(msg)
        logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Email failed: {e}")


# ── Daily run ─────────────────────────────────────────────────────────────────

def run_daily(playlist_id: str):
    """
    Fetches new videos from the playlist, extracts key points, emails the digest.
    Only processes videos not yet in the DB — skips previously seen ones.
    """
    logger.info(f"Starting daily run for playlist: {playlist_id}")
    all_videos  = fetch_playlist_videos(playlist_id)
    known_ids   = get_known_video_ids(playlist_id)
    new_videos  = [v for v in all_videos if v["video_id"] not in known_ids]

    if not new_videos:
        logger.info("No new videos in playlist — nothing to process.")
        # Still send pending points digest if there are unreviewed ones
        pending = get_pending_key_points()
        if pending:
            _email_pending_digest(pending, note="(No new videos today — resending pending review)")
        return

    logger.info(f"New videos to process: {len(new_videos)}")
    new_points_count = 0

    for video in new_videos:
        vid_id = video["video_id"]
        title  = video["title"]
        logger.info(f"Processing: {title} ({vid_id})")

        register_video(vid_id, playlist_id, title)

        transcript = fetch_transcript(vid_id)
        if not transcript:
            logger.warning(f"Skipping {vid_id} — no transcript available.")
            mark_video_processed(vid_id)
            continue

        points = extract_key_points(title, transcript)
        if points:
            store_key_points(vid_id, points)
            new_points_count += len(points)
            logger.info(f"Stored {len(points)} key points for: {title}")
        else:
            logger.info(f"No actionable points extracted for: {title}")

        mark_video_processed(vid_id)

    # Email all pending (unreviewed) key points including today's new ones
    pending = get_pending_key_points()
    if pending:
        _email_pending_digest(pending)
    else:
        logger.info("No pending key points to email.")


def _email_pending_digest(pending: list[dict], note: str = ""):
    """Formats and sends the pending key points digest to personal inbox."""
    # Group by video title for readability
    by_video: dict[str, list] = {}
    for p in pending:
        by_video.setdefault(p["title"], []).append(p)

    lines = ["Review the key points below and approve the ones you want integrated."]
    lines.append("Reply command: python youtube_research.py --approve \"1,3,5\"")
    lines.append("")
    if note:
        lines.append(note)
        lines.append("")

    global_num = 1
    for title, points in by_video.items():
        lines.append(f"VIDEO: {title}")
        lines.append("-" * len(f"VIDEO: {title}"))
        for p in points:
            lines.append(f"{global_num}. [ID:{p['id']}] {p['content']}")
            global_num += 1
        lines.append("")

    lines.append("─" * 60)
    lines.append("To approve: python youtube_research.py --approve \"1,2,4\"")
    lines.append("Use the ID numbers shown above (1, 2, 3...) — not the [ID:N] values.")
    lines.append("Approved points will be analyzed for ecosystem integration.")

    body    = "\n".join(lines)
    subject = f"📚 YouTube Research Digest — {len(pending)} points pending review ({datetime.now().strftime('%Y-%m-%d')})"
    send_email(subject, body)
    logger.info(f"Digest emailed: {len(pending)} pending key points.")


# ── Approval run ──────────────────────────────────────────────────────────────

def run_approve(approve_str: str):
    """
    Parses approval string like "1,2,4" or "1, 2 and 4", marks those global-sequence
    positions as approved, runs integration analysis, emails the report.
    """
    # Parse flexible input: "1,2,4" / "1, 2 and 4" / "1 2 4"
    raw_nums = re.findall(r"\d+", approve_str)
    if not raw_nums:
        logger.error(f"Could not parse approval numbers from: {approve_str!r}")
        sys.exit(1)

    positions = [int(n) for n in raw_nums]
    logger.info(f"Approving positions: {positions}")

    # Map positions to DB IDs using the same ordering as the email digest
    pending = get_pending_key_points()
    if not pending:
        logger.info("No pending key points to approve.")
        return

    approved_items = []
    for pos in positions:
        if 1 <= pos <= len(pending):
            approved_items.append(pending[pos - 1])
        else:
            logger.warning(f"Position {pos} out of range (1–{len(pending)}) — skipped.")

    if not approved_items:
        logger.error("No valid positions matched — nothing approved.")
        return

    logger.info(f"Running integration analysis on {len(approved_items)} point(s)...")
    analysis = analyze_integration(approved_items)

    # Store integration notes per point
    for item in approved_items:
        with _db() as conn:
            conn.execute(
                "UPDATE youtube_key_points SET approved=1, integration_notes=? WHERE id=?",
                (analysis, item["id"]),
            )
            conn.commit()

    # Email the integration report
    lines = [
        f"Integration Analysis — {len(approved_items)} approved tactic(s)",
        f"Analyzed: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 60,
        "",
    ]
    for i, item in enumerate(approved_items, 1):
        lines.append(f"[{i}] {item['content']}")
        lines.append(f"     Source: {item['title']}")
        lines.append("")
    lines.append("INTEGRATION ANALYSIS:")
    lines.append("-" * 60)
    lines.append(analysis)
    lines.append("")
    lines.append("─" * 60)
    lines.append("Next step: implement the NEW SCOPE items in the ecosystem.")

    body    = "\n".join(lines)
    subject = f"✅ Integration Report — {len(approved_items)} tactic(s) approved ({datetime.now().strftime('%Y-%m-%d')})"
    send_email(subject, body)
    logger.info("Integration report emailed.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="YouTube Playlist Research Pipeline")
    parser.add_argument("--playlist", type=str, help="YouTube playlist ID to process")
    parser.add_argument("--approve", type=str, help='Approve key points: --approve "1,2,4"')
    parser.add_argument("--list",    action="store_true", help="List all pending key points")
    parser.add_argument("--approved", action="store_true", help="List all approved points")
    args = parser.parse_args()

    if args.list:
        pending = get_pending_key_points()
        if not pending:
            print("No pending key points.")
        for i, p in enumerate(pending, 1):
            print(f"{i}. [ID:{p['id']}] [{p['title'][:40]}] {p['content']}")
        return

    if args.approved:
        approved = get_approved_points()
        if not approved:
            print("No approved points yet.")
        for p in approved:
            print(f"[ID:{p['id']}] {p['content']}")
            if p["notes"]:
                print(f"  → {p['notes'][:120]}...")
        return

    if args.approve:
        run_approve(args.approve)
        return

    # Default: daily run
    playlist_id = args.playlist or os.getenv("YOUTUBE_PLAYLIST_ID")
    if not playlist_id:
        logger.error("Provide --playlist PLxxxxxxx or set YOUTUBE_PLAYLIST_ID in .env")
        sys.exit(1)
    run_daily(playlist_id)


if __name__ == "__main__":
    main()

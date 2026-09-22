"""
post_ro_thread.py — One-shot manual X thread: CLM/CRF RO scarcity + open-market edge.
Run once from local Mac or PA. No dedup key — manual post, not scheduled.

Usage:
  python post_ro_thread.py          # dry-run (prints tweets, does NOT post)
  python post_ro_thread.py --post   # actually posts the thread to X
"""

import os
import sys
import time
import logging
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RO_Thread")

TWITTER_API_KEY             = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET          = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN        = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET", "")

GUMROAD_LINK = "https://bit.ly/4Am3uCo"

# ── Thread content ────────────────────────────────────────────────────────────

TWEETS = [
    # Tweet 1 — Hook: scarcity + live event
    (
        "CLM dropped 8.5% in a single session last month.\n\n"
        "Most income investors sold, panicked, or did nothing.\n\n"
        "This event has only happened 3 times in the last 14 years.\n"
        "Here's what it actually is — and why the window is closing."
    ),

    # Tweet 2 — What an RO is (problem-framed, not "buy this")
    (
        "A Rights Offering is when a CEF issues new shares at a NAV discount.\n\n"
        "But here's what most investors miss:\n\n"
        "Cornerstone needs a 15–20% premium above NAV before they can even do this.\n"
        "CLM/CRF are at 2% premium today. That window just closed for 18–37 months."
    ),

    # Tweet 3 — Current data: open-market buyer beats RO participants
    (
        "Today's numbers:\n\n"
        "CLM: $6.37 | NAV: ~$6.31 | Sub: ~$6.56\n"
        "CRF: $6.13 | NAV: ~$6.12 | Sub: ~$6.37\n\n"
        "Both are BELOW the subscription price right now.\n\n"
        "Open-market buyers pay less than rights holders.\n"
        "No rights needed. No filing. Just buy at market."
    ),

    # Tweet 4 — Two catalysts still ahead
    (
        "Two more catalysts before this RO cycle ends:\n\n"
        "~Oct 1–2 — N-2/A files: confirms sub price, 1–3 day sell pressure\n"
        "~Oct 13–16 — Record date: cycle low in 2022 and 2025 both landed here\n\n"
        "Both = potential add windows.\n"
        "Expiration ~Nov 7–10: overhang clears, recovery begins."
    ),

    # Tweet 5 — The math that drives recovery (structural, not a prediction)
    (
        "At $6.37, CLM yields 22.9%.\n\n"
        "Income buyers don't ignore 22.9% forever.\n"
        "The premium mean-reverts from 2% → historical avg as they return.\n\n"
        "This isn't a call. It's the mechanics.\n"
        "The only question is whether you're in before or after the recovery."
    ),

    # Tweet 6 — CTA: gate the actionable part
    (
        f"The live RO Risk score, entry zones, and N-2/A alert aren't posted here.\n\n"
        f"Subscriber-only — timing catalysts wrong costs more than the premium.\n\n"
        f"Free tier + live alerts 🔒\n"
        f"{GUMROAD_LINK}\n\n"
        f"#ClosedEndFunds #DividendInvesting #FinTwit #PassiveIncome #IncomeInvesting"
    ),
]


def dry_run():
    print("\n" + "="*60)
    print("DRY RUN — CLM/CRF RO Thread (6 tweets)")
    print("="*60)
    for i, tweet in enumerate(TWEETS, 1):
        char_count = len(tweet)
        status = "✅" if char_count <= 280 else f"❌ OVER ({char_count} chars)"
        print(f"\n[Tweet {i}/6] {char_count} chars {status}")
        print("-"*40)
        print(tweet)
    print("\n" + "="*60)
    print("Run with --post to actually publish.")
    print("="*60 + "\n")


def post_thread():
    over_limit = [(i+1, len(t)) for i, t in enumerate(TWEETS) if len(t) > 280]
    if over_limit:
        for num, chars in over_limit:
            logger.error(f"Tweet {num} is {chars} chars — exceeds 280 limit. Aborting.")
        sys.exit(1)

    try:
        import tweepy
    except ImportError:
        logger.error("tweepy not installed — run: pip3.10 install tweepy")
        sys.exit(1)

    if not all([TWITTER_API_KEY, TWITTER_API_SECRET,
                TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET]):
        logger.error("Missing TWITTER_* credentials in .env")
        sys.exit(1)

    client = tweepy.Client(
        consumer_key=TWITTER_API_KEY,
        consumer_secret=TWITTER_API_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )

    prev_id = None
    for i, text in enumerate(TWEETS):
        try:
            kwargs = {"text": text}
            if prev_id:
                kwargs["in_reply_to_tweet_id"] = prev_id
            resp = client.create_tweet(**kwargs)
            prev_id = resp.data["id"]
            logger.info(f"Posted tweet {i+1}/{len(TWEETS)} (id={prev_id})")
            if i < len(TWEETS) - 1:
                time.sleep(4)  # 4s gap keeps thread cadence natural
        except Exception as e:
            logger.error(f"Failed on tweet {i+1}: {e}")
            sys.exit(1)

    logger.info("RO thread posted successfully.")


if __name__ == "__main__":
    if "--post" in sys.argv:
        post_thread()
    else:
        dry_run()

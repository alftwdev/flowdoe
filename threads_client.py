"""
threads_client.py — Instagram Threads API client
Meta Threads API v1.0 | Text posts + reply chains

Token lifecycle:
  Long-lived access tokens expire every 60 days.
  This module auto-refreshes when the token is within 7 days of expiry,
  storing the new token in the DB so .env never needs manual updates.

One-time setup (do once — requires a browser):
  1. Create a Meta Developer app at developers.facebook.com
     → Add product: Threads
     → Permissions needed: threads_basic, threads_content_publish
     → Note your App ID and App Secret

  2. Get your Threads User ID and initial access token:
     a. In development mode (up to 5 test users — no app review needed):
        - Add yourself as a test user in the app dashboard
        - Run the OAuth flow below to get a code
     b. In production mode (public posting, requires Meta app review):
        - Submit for review (threads_content_publish permission)
        - Typically approved in 3-5 days for content publishing use

  3. OAuth browser flow (one-time, generates initial short-lived token):
     Open in browser (replace YOUR_APP_ID and YOUR_REDIRECT_URI):
     https://threads.net/oauth/authorize
       ?client_id=YOUR_APP_ID
       &redirect_uri=YOUR_REDIRECT_URI
       &scope=threads_basic,threads_content_publish
       &response_type=code

     Redirect URI can be https://localhost or any URL you control.
     Grab the "code" param from the redirect URL.

  4. Exchange code for short-lived token (curl or requests):
     POST https://graph.threads.net/oauth/access_token
       client_id=YOUR_APP_ID
       client_secret=YOUR_APP_SECRET
       grant_type=authorization_code
       redirect_uri=YOUR_REDIRECT_URI
       code=THE_CODE_FROM_STEP_3

     Returns: {"access_token": "...", "user_id": "123456789"}
     → Save user_id → THREADS_USER_ID in .env

  5. Exchange for long-lived token (valid 60 days):
     GET https://graph.threads.net/access_token
       ?grant_type=th_exchange_token
       &client_secret=YOUR_APP_SECRET
       &access_token=SHORT_LIVED_TOKEN

     Returns: {"access_token": "LONG_LIVED_TOKEN", "expires_in": 5183944}
     → Save → THREADS_ACCESS_TOKEN in .env

  6. Seed expiry in DB (run once):
     python3.10 threads_client.py --seed-expiry

  7. Add to .env:
     THREADS_AUTO_POST_ENABLED=true
     THREADS_ACCESS_TOKEN=<long-lived token from step 5>
     THREADS_USER_ID=<user_id from step 4>
     THREADS_APP_ID=<your Meta app ID>
     THREADS_APP_SECRET=<your Meta app secret>

After initial setup, auto-refresh handles everything.
Token is refreshed in DB; .env THREADS_ACCESS_TOKEN is the fallback only.
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("threads_client")

THREADS_API_BASE    = "https://graph.threads.net/v1.0"
THREADS_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN", "")
THREADS_USER_ID      = os.getenv("THREADS_USER_ID", "")
THREADS_APP_ID       = os.getenv("THREADS_APP_ID", "")
THREADS_APP_SECRET   = os.getenv("THREADS_APP_SECRET", "")

# DB keys
_DB_KEY_TOKEN  = "threads_access_token_live"
_DB_KEY_EXPIRY = "threads_token_expiry"


# ─────────────────────────────────────────────────────────────────────────────
# TOKEN LIFECYCLE
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_token(db=None) -> str:
    """
    Returns the current valid token, refreshing if within 7 days of expiry.
    Priority: DB live token > .env fallback.
    """
    if db is None:
        return THREADS_ACCESS_TOKEN

    # Check expiry
    expiry_str = db.get_state(_DB_KEY_EXPIRY)
    if expiry_str:
        try:
            expiry    = datetime.fromisoformat(expiry_str)
            days_left = (expiry - datetime.now(timezone.utc)).days
            if days_left <= 7:
                logger.info(f"Threads token expires in {days_left}d — auto-refreshing...")
                current = db.get_state(_DB_KEY_TOKEN) or THREADS_ACCESS_TOKEN
                refreshed = _do_refresh(current)
                if refreshed and refreshed != current:
                    new_expiry = (datetime.now(timezone.utc) + timedelta(days=60)).isoformat()
                    db.update_state(_DB_KEY_TOKEN,  refreshed)
                    db.update_state(_DB_KEY_EXPIRY, new_expiry)
                    logger.info("Threads token refreshed — new expiry set in DB.")
                    return refreshed
        except Exception as e:
            logger.warning(f"Threads token expiry check failed: {e}")

    return db.get_state(_DB_KEY_TOKEN) or THREADS_ACCESS_TOKEN


def _do_refresh(token: str) -> str:
    """Exchanges existing long-lived token for a fresh 60-day token."""
    try:
        r = requests.get(
            f"{THREADS_API_BASE}/refresh_access_token",
            params={"grant_type": "th_refresh_token", "access_token": token},
            timeout=15,
        )
        r.raise_for_status()
        new_token = r.json().get("access_token")
        if new_token:
            logger.info("Threads token refresh successful.")
            return new_token
        logger.warning(f"Threads refresh — unexpected response: {r.json()}")
    except Exception as e:
        logger.error(f"Threads token refresh failed: {e}")
    return token


# ─────────────────────────────────────────────────────────────────────────────
# POST
# ─────────────────────────────────────────────────────────────────────────────

def _create_container(text: str, token: str, reply_to_id: str = None) -> str | None:
    """Creates a Threads media container. Returns container ID or None."""
    user_id = THREADS_USER_ID
    if not user_id or not token:
        logger.warning("THREADS_USER_ID or token missing — cannot create container")
        return None
    params = {
        "media_type":   "TEXT",
        "text":         text[:500],  # Threads 500-char limit
        "access_token": token,
    }
    if reply_to_id:
        params["reply_to_id"] = reply_to_id
    try:
        r = requests.post(
            f"{THREADS_API_BASE}/{user_id}/threads",
            params=params,
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("id")
    except Exception as e:
        logger.error(f"Threads container creation failed: {e}")
        return None


def _publish_container(container_id: str, token: str) -> str | None:
    """Publishes a container. Returns published post ID or None."""
    user_id = THREADS_USER_ID
    try:
        r = requests.post(
            f"{THREADS_API_BASE}/{user_id}/threads_publish",
            params={"creation_id": container_id, "access_token": token},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("id")
    except Exception as e:
        logger.error(f"Threads publish failed: {e}")
        return None


def post_thread(posts: list[str], db=None, label: str = "") -> bool:
    """
    Posts a list of strings as a Threads reply chain.
    First post is the root; each subsequent is a reply to the previous.
    Returns True if all posts succeeded.
    """
    token = _resolve_token(db)
    if not token:
        logger.warning("No Threads access token — skipping")
        return False
    if not THREADS_USER_ID:
        logger.warning("THREADS_USER_ID not set — skipping")
        return False

    tag     = f" [{label}]" if label else ""
    prev_id = None

    for i, text in enumerate(posts):
        container_id = _create_container(text, token, reply_to_id=prev_id)
        if not container_id:
            logger.error(f"Threads post {i+1}/{len(posts)} — container failed{tag}")
            return False
        post_id = _publish_container(container_id, token)
        if not post_id:
            logger.error(f"Threads post {i+1}/{len(posts)} — publish failed{tag}")
            return False
        prev_id = post_id
        logger.info(f"Threads post {i+1}/{len(posts)} published (id={post_id}){tag}")
        if i < len(posts) - 1:
            time.sleep(3)  # same courteous cadence as X

    return True


# ─────────────────────────────────────────────────────────────────────────────
# SEED EXPIRY  (run once after initial token setup)
# ─────────────────────────────────────────────────────────────────────────────

def seed_expiry():
    """
    Seeds the token expiry in DB to today + 60 days.
    Run once after storing THREADS_ACCESS_TOKEN in .env:
        python3.10 threads_client.py --seed-expiry
    After seeding, auto-refresh kicks in 7 days before expiry.
    """
    try:
        from database import EcosystemDatabase
        db     = EcosystemDatabase()
        expiry = (datetime.now(timezone.utc) + timedelta(days=60)).isoformat()
        db.update_state(_DB_KEY_EXPIRY, expiry)
        if THREADS_ACCESS_TOKEN:
            db.update_state(_DB_KEY_TOKEN, THREADS_ACCESS_TOKEN)
        print(f"Threads token expiry seeded: {expiry}")
        print(f"Token stored in DB: {'yes' if THREADS_ACCESS_TOKEN else 'NO — set THREADS_ACCESS_TOKEN in .env first'}")
    except Exception as e:
        print(f"Seed failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if "--seed-expiry" in sys.argv:
        seed_expiry()
    else:
        print("Usage: python3.10 threads_client.py --seed-expiry")
        print("       (run once after setting THREADS_ACCESS_TOKEN in .env)")

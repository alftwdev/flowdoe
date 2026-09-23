"""
threads_setup.py — One-time interactive OAuth flow for Threads API

Run this ONCE on your local Mac to get THREADS_ACCESS_TOKEN + THREADS_USER_ID.
After this, threads_client.py handles all token refreshes automatically.

Prerequisites (do in Meta App Dashboard first):
  1. Go to your Meta app → Threads → Settings
  2. Under "Redirect callback URLs" add exactly:  https://localhost
  3. Under "Permissions" confirm threads_basic + threads_content_publish are added

Usage:
    python3 threads_setup.py
"""

import os
import sys
import webbrowser
import requests
from urllib.parse import urlencode, urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

THREADS_APP_ID     = os.getenv("THREADS_APP_ID", "")
THREADS_APP_SECRET = os.getenv("THREADS_APP_SECRET", "")
REDIRECT_URI       = "https://localhost"

AUTH_URL    = "https://threads.net/oauth/authorize"
TOKEN_URL   = "https://graph.threads.net/oauth/access_token"
LONGLIVE_URL = "https://graph.threads.net/access_token"


def step1_get_auth_url() -> str:
    params = {
        "client_id":     THREADS_APP_ID,
        "redirect_uri":  REDIRECT_URI,
        "scope":         "threads_basic,threads_content_publish",
        "response_type": "code",
    }
    return f"{AUTH_URL}?{urlencode(params)}"


def step2_exchange_code(code: str) -> tuple[str, str]:
    """Exchange authorization code for short-lived token + user_id."""
    r = requests.post(TOKEN_URL, data={
        "client_id":     THREADS_APP_ID,
        "client_secret": THREADS_APP_SECRET,
        "grant_type":    "authorization_code",
        "redirect_uri":  REDIRECT_URI,
        "code":          code.strip(),
    }, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data["access_token"], str(data["user_id"])


def step3_get_long_lived_token(short_token: str) -> str:
    """Exchange short-lived token for 60-day long-lived token."""
    r = requests.get(LONGLIVE_URL, params={
        "grant_type":    "th_exchange_token",
        "client_secret": THREADS_APP_SECRET,
        "access_token":  short_token,
    }, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def main():
    if not THREADS_APP_ID or not THREADS_APP_SECRET:
        print("ERROR: Set THREADS_APP_ID and THREADS_APP_SECRET in .env first, then re-run.")
        sys.exit(1)

    print("=" * 60)
    print("Threads OAuth Setup — one-time run")
    print("=" * 60)
    print()
    print("STEP 1 of 3: Authorize in your browser")
    print()
    auth_url = step1_get_auth_url()
    print(f"Opening this URL:\n{auth_url}")
    print()
    print("If your browser doesn't open automatically, copy the URL above and paste it.")
    print()
    webbrowser.open(auth_url)

    print("-" * 60)
    print("After you click 'Authorize' in the browser, Meta will redirect")
    print("to https://localhost — the page will show a connection error.")
    print("That is EXPECTED. Look at the URL bar — it will look like:")
    print()
    print("  https://localhost/?code=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#_")
    print()
    raw = input("Paste the FULL redirect URL here (or just the code value): ").strip()

    # Extract code from full URL or bare code
    if raw.startswith("http"):
        parsed = urlparse(raw)
        code = parse_qs(parsed.query).get("code", [""])[0].split("#")[0]
    else:
        code = raw.split("#")[0].strip()

    if not code:
        print("ERROR: Could not extract code. Paste the full URL or just the code value.")
        sys.exit(1)

    print()
    print("STEP 2 of 3: Exchanging code for short-lived token...")
    try:
        short_token, user_id = step2_exchange_code(code)
        print(f"  User ID:           {user_id}")
        print(f"  Short-lived token: {short_token[:20]}...  (valid 1 hour)")
    except Exception as e:
        print(f"ERROR: Code exchange failed: {e}")
        print("Common cause: code already used (each code is one-use) — restart from Step 1.")
        sys.exit(1)

    print()
    print("STEP 3 of 3: Exchanging for long-lived token (valid 60 days)...")
    try:
        long_token = step3_get_long_lived_token(short_token)
        print(f"  Long-lived token:  {long_token[:20]}...  (valid 60 days, auto-refreshed)")
    except Exception as e:
        print(f"ERROR: Long-lived token exchange failed: {e}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("SUCCESS — add these two lines to your .env on PA:")
    print("=" * 60)
    print()
    print(f"THREADS_ACCESS_TOKEN={long_token}")
    print(f"THREADS_USER_ID={user_id}")
    print()
    print("Then run once on PA to seed the token expiry in DB:")
    print("  python3.10 threads_client.py --seed-expiry")
    print()
    print("Then set:")
    print("  THREADS_AUTO_POST_ENABLED=true")
    print()


if __name__ == "__main__":
    main()

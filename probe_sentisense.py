"""
probe_sentisense.py — Discover available SentiSense API endpoints.

Phase 1: Test web-app /private/ endpoints with API key auth.
Phase 2: Test known public API endpoints for comparison.

Run locally: python probe_sentisense.py
"""
import os, json, requests
from dotenv import load_dotenv

load_dotenv()
API_KEY  = os.getenv("SENTISENSE_API_KEY", "")
BASE     = "https://app.sentisense.ai/api/v1"

api_headers = {
    "X-SentiSense-API-Key": API_KEY,
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
}

def probe(label, url, params=None):
    try:
        r = requests.get(url, headers=api_headers, params=params or {}, timeout=12)
        if r.status_code == 200:
            try:
                data = r.json()
                keys = list(data.keys()) if isinstance(data, dict) else f"list[{len(data)}]"
                snippet = r.text[:300].replace("\n", " ")
                print(f"✅  {label}")
                print(f"    keys={keys}")
                print(f"    {snippet}\n")
            except Exception:
                print(f"✅  {label} → 200 (non-JSON body, len={len(r.text)})\n")
        elif r.status_code == 401:
            print(f"🔑  {label} → 401 (API key rejected on this path)")
        elif r.status_code == 403:
            print(f"🔒  {label} → 403 (plan restriction or cookie-only auth)")
        elif r.status_code == 404:
            print(f"❌  {label} → 404 not found")
        else:
            print(f"⚠️   {label} → {r.status_code}: {r.text[:120]}")
    except Exception as e:
        print(f"💥  {label} → ERROR: {e}")


print("=" * 60)
print("PHASE 1 — /private/ endpoints (found in browser network tab)")
print("=" * 60)

probe("AI Market Summary v2",
      f"{BASE}/private/zero-state/market-summary-v2")

probe("Trending Stories (14d, expanded)",
      f"{BASE}/private/documents/stories",
      params={"limit": 20, "days": 14, "offset": 0, "expanded": "true"})

probe("Market Mood (180d history)",
      f"{BASE}/private/market-mood",
      params={"days": 180})

probe("User Insights (limit 20)",
      f"{BASE}/private/insights/user",
      params={"limit": 20})

probe("Market Status",
      f"{BASE}/private/stocks/market-status")

probe("Stock Lists",
      f"{BASE}/private/stock-lists")


print()
print("=" * 60)
print("PHASE 2 — Known public API endpoints (current client uses these)")
print("=" * 60)

probe("Market Mood (public path)",
      f"{BASE}/market/mood")

probe("Reddit Picks",
      f"{BASE}/trackers/reddit-picks")

probe("Sentiment Movers",
      f"{BASE}/trackers/sentiment-movers")

probe("Sentiment Leaderboard",
      f"{BASE}/trackers/sentiment-leaderboard")


print()
print("=" * 60)
print("PHASE 3 — Alternative private path guesses")
print("=" * 60)

for path in [
    "/private/market/summary",
    "/private/ai/summary",
    "/private/market/ai-summary",
    "/private/news/stories",
    "/private/stories",
]:
    probe(path, f"{BASE}{path}")

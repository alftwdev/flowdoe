"""
tradier_client.py — Tradier Pro REST wrapper with in-process TTL cache.

All network I/O → zero PythonAnywhere CPU-seconds (counted as I/O, not CPU).
Rate limit: 200 req/min on Tradier Pro — cache prevents redundant hits within
the same script invocation and across the 5-min monitor loop.

Usage:
    from tradier_client import TradierClient
    tc = TradierClient()
    chain = tc.get_options_chain("SPY")
    quote = tc.get_quote("TQQQ")
    iv = tc.get_atm_iv("TQQQ", option_type="call", dte_min=20, dte_max=50)
    ivr = tc.get_iv_rank("TQQQ", db)   # requires iv_daily table populated
    gex = tc.get_gex("SPY")
"""

import os
import time
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logger = logging.getLogger("TradierClient")
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

# In-process cache: {cache_key: (timestamp, data)}
_cache: dict = {}

TRADIER_BASE = "https://api.tradier.com/v1"
TRADIER_STREAM = "https://stream.tradier.com/v1"


class TradierClient:
    def __init__(self):
        self.api_key = os.getenv("TRADIER_API_KEY", "")
        if not self.api_key:
            logger.warning("TRADIER_API_KEY not set — Tradier calls will fail.")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        })

    # ── Cache helpers ──────────────────────────────────────────────────────────

    def _cached(self, key: str, ttl: int, fetch_fn):
        """Return cached value if fresh; otherwise call fetch_fn() and cache it."""
        now = time.time()
        if key in _cache:
            ts, val = _cache[key]
            if now - ts < ttl:
                return val
        val = fetch_fn()
        _cache[key] = (now, val)
        return val

    def _get(self, path: str, params: dict = None):
        """Raw GET against Tradier API. Returns parsed JSON dict or None."""
        url = f"{TRADIER_BASE}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            logger.warning(f"[Tradier] HTTP {e.response.status_code} for {path}: {e}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"[Tradier] Request error for {path}: {type(e).__name__}")
        except Exception as e:
            logger.warning(f"[Tradier] Unexpected error for {path}: {e}")
        return None

    # ── Quotes ─────────────────────────────────────────────────────────────────

    def get_quote(self, symbol: str, ttl: int = 60) -> dict:
        """Return latest quote dict for a symbol. Cached 60s."""
        def _fetch():
            data = self._get("/markets/quotes", {"symbols": symbol, "greeks": "false"})
            if not data:
                return {}
            try:
                q = data.get("quotes", {}).get("quote", {})
                if isinstance(q, list):
                    q = q[0]
                return q or {}
            except Exception:
                return {}
        return self._cached(f"quote_{symbol}", ttl, _fetch)

    def get_spot(self, symbol: str) -> float:
        """Return last price for symbol (0.0 on failure)."""
        q = self.get_quote(symbol)
        return float(q.get("last") or q.get("close") or 0.0)

    # ── Options chain ──────────────────────────────────────────────────────────

    def get_options_chain(self, symbol: str, expiration: str = None,
                          greeks: bool = True, ttl: int = 3600) -> list:
        """
        Return full options chain for symbol as list of dicts.
        If expiration not specified, uses the nearest monthly ≥20 DTE.
        Cached 1 hour (chain data moves slowly intraday vs quote data).
        """
        cache_key = f"chain_{symbol}_{expiration or 'nearest'}"

        def _fetch():
            exp = expiration or self._nearest_expiration(symbol)
            if not exp:
                return []
            data = self._get("/markets/options/chains", {
                "symbol": symbol,
                "expiration": exp,
                "greeks": "true" if greeks else "false",
            })
            if not data:
                return []
            try:
                options = data.get("options", {}).get("option", [])
                return options if isinstance(options, list) else []
            except Exception:
                return []

        return self._cached(cache_key, ttl, _fetch)

    def get_expirations(self, symbol: str, ttl: int = 3600) -> list:
        """Return list of expiration date strings (YYYY-MM-DD) for symbol."""
        def _fetch():
            data = self._get("/markets/options/expirations", {
                "symbol": symbol,
                "includeAllRoots": "true",
                "strikes": "false",
            })
            if not data:
                return []
            try:
                exps = data.get("expirations", {}).get("date", [])
                return exps if isinstance(exps, list) else ([exps] if exps else [])
            except Exception:
                return []
        return self._cached(f"expirations_{symbol}", ttl, _fetch)

    def _nearest_expiration(self, symbol: str, dte_min: int = 20) -> str:
        """Return nearest expiration at least dte_min days out."""
        exps = self.get_expirations(symbol)
        today = datetime.utcnow().date()
        for exp_str in sorted(exps):
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                if (exp_date - today).days >= dte_min:
                    return exp_str
            except Exception:
                continue
        return ""

    # ── IV helpers ─────────────────────────────────────────────────────────────

    def get_atm_iv(self, symbol: str, option_type: str = "call",
                   dte_min: int = 20, dte_max: int = 50,
                   ttl: int = 3600) -> float:
        """
        Return ATM implied volatility (as decimal, e.g. 0.45 = 45%) for symbol.
        Uses put/call parity — takes median IV of contracts within 2% of spot
        in the dte_min–dte_max window. Returns 0.0 on failure.
        """
        cache_key = f"atm_iv_{symbol}_{option_type}_{dte_min}_{dte_max}"

        def _fetch():
            today = datetime.utcnow().date()
            exps = self.get_expirations(symbol)
            spot = self.get_spot(symbol)
            if spot == 0.0:
                return 0.0

            ivs = []
            for exp_str in sorted(exps):
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if dte < dte_min or dte > dte_max:
                        continue
                    chain = self.get_options_chain(symbol, exp_str, greeks=True)
                    for c in chain:
                        if c.get("option_type") != option_type:
                            continue
                        strike = float(c.get("strike", 0))
                        if abs(strike - spot) / spot > 0.02:
                            continue
                        iv = float(c.get("greeks", {}).get("smv_vol") or c.get("implied_volatility") or 0)
                        if iv > 0:
                            ivs.append(iv)
                except Exception:
                    continue

            if not ivs:
                return 0.0
            ivs.sort()
            mid = len(ivs) // 2
            return ivs[mid]

        return self._cached(cache_key, ttl, _fetch)

    # ── IVR (requires iv_daily table populated via store_daily_iv) ─────────────

    def get_iv_rank(self, symbol: str, db, ttl: int = 3600) -> dict:
        """
        Return IV Rank using stored daily IV history from database.
        Returns dict: {ivr: float 0-100, current_iv: float, days_history: int,
                       tag: str, reliable: bool}
        reliable=True once 30+ trading days of data exist.
        """
        cache_key = f"ivr_{symbol}"

        def _fetch():
            current_iv = self.get_atm_iv(symbol)
            if current_iv == 0.0:
                return {"ivr": 0.0, "current_iv": 0.0, "days_history": 0,
                        "tag": "NO DATA", "reliable": False}
            try:
                history = db.get_iv_history(symbol, days=252)
            except Exception:
                history = []

            days = len(history)
            if days < 5:
                return {"ivr": 0.0, "current_iv": current_iv, "days_history": days,
                        "tag": f"BUILDING ({days}/30 days)", "reliable": False}

            iv_values = [float(row[0]) for row in history if row[0]]
            low_52 = min(iv_values)
            high_52 = max(iv_values)
            ivr = ((current_iv - low_52) / (high_52 - low_52) * 100) if high_52 > low_52 else 0.0
            ivr = round(max(0.0, min(100.0, ivr)), 1)
            tag = "LOW IVR" if ivr < 35 else ("ELEVATED IVR" if ivr > 60 else "MID IVR")
            return {"ivr": ivr, "current_iv": current_iv, "days_history": days,
                    "tag": tag, "reliable": days >= 30}

        return self._cached(cache_key, ttl, _fetch)

    # ── GEX (Gamma Exposure) ────────────────────────────────────────────────────

    def get_gex(self, symbol: str = "SPY", ttl: int = 3600) -> dict:
        """
        Compute Gamma Exposure (GEX) from Tradier options chain OI.
        GEX = Σ (call_OI - put_OI) × gamma × spot × 100
        Positive GEX → dealers short gamma → volatility suppression.
        Negative GEX → dealers long gamma → volatility amplification.

        Returns dict: {flip_strike, current_spot, market_state, gex_total,
                       pc_oi_ratio, pc_tag}
        """
        cache_key = f"gex_{symbol}"

        def _fetch():
            spot = self.get_spot(symbol)
            if spot == 0.0:
                return _gex_empty(symbol)

            today = datetime.utcnow().date()
            exps = self.get_expirations(symbol)

            net_gamma_by_strike: dict = {}

            for exp_str in sorted(exps)[:4]:  # nearest 4 expirations dominate GEX
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if dte < 0 or dte > 60:
                        continue
                    chain = self.get_options_chain(symbol, exp_str, greeks=True)
                    for c in chain:
                        strike = float(c.get("strike", 0))
                        if not (spot * 0.90 <= strike <= spot * 1.10):
                            continue
                        oi = float(c.get("open_interest") or 0)
                        gamma = float((c.get("greeks") or {}).get("gamma") or 0)
                        if oi == 0 or gamma == 0:
                            continue
                        exposure = oi * gamma * spot * 100
                        if c.get("option_type") == "call":
                            net_gamma_by_strike[strike] = net_gamma_by_strike.get(strike, 0) + exposure
                        else:
                            net_gamma_by_strike[strike] = net_gamma_by_strike.get(strike, 0) - exposure
                except Exception:
                    continue

            if not net_gamma_by_strike:
                return _gex_empty(symbol)

            gex_total = sum(net_gamma_by_strike.values())
            # Flip strike = strike closest to zero net gamma
            flip_strike = min(net_gamma_by_strike, key=lambda k: abs(net_gamma_by_strike[k]))
            market_state = "🟢 POSITIVE GAMMA" if spot > flip_strike else "🔴 NEGATIVE GAMMA"

            # Put/Call OI ratio across all exps in window
            total_call_oi = 0.0
            total_put_oi = 0.0
            for exp_str in sorted(exps)[:4]:
                try:
                    chain = self.get_options_chain(symbol, exp_str, greeks=False)
                    for c in chain:
                        strike = float(c.get("strike", 0))
                        if not (spot * 0.90 <= strike <= spot * 1.10):
                            continue
                        oi = float(c.get("open_interest") or 0)
                        if c.get("option_type") == "call":
                            total_call_oi += oi
                        else:
                            total_put_oi += oi
                except Exception:
                    continue

            pc_oi_ratio = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 1.0
            if pc_oi_ratio > 1.15:
                pc_tag = "🔴 PUT-HEAVY (bearish skew)"
            elif pc_oi_ratio < 0.85:
                pc_tag = "🟢 CALL-HEAVY (bullish skew)"
            else:
                pc_tag = "🟡 BALANCED"

            return {
                "flip_strike": flip_strike,
                "current_spot": spot,
                "market_state": market_state,
                "gex_total": round(gex_total / 1e9, 2),  # in billions
                "pc_oi_ratio": pc_oi_ratio,
                "pc_tag": pc_tag,
            }

        return self._cached(cache_key, ttl, _fetch)

    # ── Wheel strike finder ────────────────────────────────────────────────────

    def find_csp_strike(self, symbol: str, target_delta: float = 0.20,
                        dte_min: int = 30, dte_max: int = 45) -> dict:
        """
        Find the best cash-secured put strike at target_delta within DTE window.
        Returns dict: {strike, delta, bid, ask, mid, dte, expiration,
                       spread_pct, oi, volume, premium_yield}
        """
        today = datetime.utcnow().date()
        spot = self.get_spot(symbol)
        if spot == 0.0:
            return {}

        exps = self.get_expirations(symbol)
        best = None
        best_delta_diff = 999.0

        for exp_str in sorted(exps):
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if dte < dte_min or dte > dte_max:
                    continue
                chain = self.get_options_chain(symbol, exp_str, greeks=True)
                for c in chain:
                    if c.get("option_type") != "put":
                        continue
                    greeks_d = c.get("greeks") or {}
                    delta = abs(float(greeks_d.get("delta") or 0))
                    if delta == 0:
                        continue
                    diff = abs(delta - target_delta)
                    if diff < best_delta_diff:
                        bid = float(c.get("bid") or 0)
                        ask = float(c.get("ask") or 0)
                        mid = (bid + ask) / 2
                        oi = float(c.get("open_interest") or 0)
                        vol = float(c.get("volume") or 0)
                        strike = float(c.get("strike") or 0)
                        spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999.0
                        premium_yield = (mid / strike * 100) if strike > 0 else 0.0
                        best_delta_diff = diff
                        best = {
                            "strike": strike,
                            "delta": round(delta, 3),
                            "bid": bid,
                            "ask": ask,
                            "mid": round(mid, 2),
                            "dte": dte,
                            "expiration": exp_str,
                            "spread_pct": round(spread_pct, 1),
                            "oi": int(oi),
                            "volume": int(vol),
                            "premium_yield": round(premium_yield, 2),
                        }
            except Exception:
                continue

        return best or {}

    # ── SPX iron condor (0DTE / same-day) ─────────────────────────────────────

    def get_spx_0dte_condor(self, wing_width: int = 5,
                             target_delta: float = 0.10) -> dict:
        """
        Find a same-day SPX iron condor for SPX income mode.
        Returns {call_sell, call_buy, put_sell, put_buy, credit, max_risk,
                 expiration, dte, valid, reason}
        Gated externally by VIXY z and breadth — this just finds the strikes.
        """
        symbol = "SPX"
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        spot = self.get_spot(symbol)
        if spot == 0.0:
            return {"valid": False, "reason": "SPX quote unavailable"}

        chain = self.get_options_chain(symbol, today_str, greeks=True)
        if not chain:
            # Try $SPXW (CBOE weekly)
            chain = self.get_options_chain("SPXW", today_str, greeks=True)
            if not chain:
                return {"valid": False, "reason": "No 0DTE chain found for SPX/SPXW"}

        calls = [c for c in chain if c.get("option_type") == "call"]
        puts = [c for c in chain if c.get("option_type") == "put"]

        def _find_strike(contracts, td):
            best, best_diff = None, 999.0
            for c in contracts:
                greeks_d = c.get("greeks") or {}
                d = abs(float(greeks_d.get("delta") or 0))
                if d == 0:
                    continue
                diff = abs(d - td)
                if diff < best_diff:
                    best_diff = diff
                    best = c
            return best

        call_sell_c = _find_strike(calls, target_delta)
        put_sell_c = _find_strike(puts, target_delta)

        if not call_sell_c or not put_sell_c:
            return {"valid": False, "reason": "Could not find target delta strikes"}

        call_sell_strike = float(call_sell_c.get("strike", 0))
        put_sell_strike = float(put_sell_c.get("strike", 0))
        call_buy_strike = call_sell_strike + wing_width
        put_buy_strike = put_sell_strike - wing_width

        # Estimate credit from bid/ask mids
        call_credit = (float(call_sell_c.get("bid") or 0) + float(call_sell_c.get("ask") or 0)) / 2
        put_credit = (float(put_sell_c.get("bid") or 0) + float(put_sell_c.get("ask") or 0)) / 2
        total_credit = round(call_credit + put_credit, 2)
        max_risk = round(wing_width * 100 - total_credit * 100, 0)

        if total_credit <= 0:
            return {"valid": False, "reason": "Zero credit — market likely closed"}

        return {
            "valid": True,
            "reason": "OK",
            "call_sell": call_sell_strike,
            "call_buy": call_buy_strike,
            "put_sell": put_sell_strike,
            "put_buy": put_buy_strike,
            "credit": total_credit,
            "credit_dollars": int(total_credit * 100),
            "max_risk": max_risk,
            "rr_ratio": round(max_risk / (total_credit * 100), 1) if total_credit > 0 else 0,
            "expiration": today_str,
            "dte": 0,
        }


    def get_earnings_proximity(self, symbols: list, days_ahead: int = 30) -> dict:
        """
        Tradier /markets/calendar — find earnings dates within days_ahead for each symbol.
        Returns dict keyed by symbol: {"days_to_earnings": int, "date": str, "flag": str}.
        flag: "FORCE_CLOSE" if ≤ 7 DTE to earnings, "REVIEW" if ≤ 21 DTE, "CLEAR" otherwise.
        Uses Tradier's earnings calendar endpoint (included in $10/mo plan).
        """
        if not self.api_key:
            return {}
        today    = datetime.now().date()
        end_date = today + timedelta(days=days_ahead)
        results  = {}
        try:
            # Tradier calendar returns event data including earnings per date range
            r = self._get("/markets/calendar", {
                "month": today.month,
                "year":  today.year,
            })
            days_data = r.get("calendar", {}).get("days", {}).get("day", [])
            if isinstance(days_data, dict):
                days_data = [days_data]

            # Build a lookup: symbol → nearest earnings date
            earnings_map: dict = {}
            for day in days_data:
                date_str = day.get("date", "")
                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if event_date < today or event_date > end_date:
                    continue
                # Earnings entries live under day["earnings"]["earning"] (list or dict)
                earning_items = day.get("earnings", {}).get("earning", [])
                if isinstance(earning_items, dict):
                    earning_items = [earning_items]
                for item in earning_items:
                    sym = str(item.get("symbol", "")).upper()
                    if sym and sym not in earnings_map:
                        earnings_map[sym] = date_str

            for sym in symbols:
                sym_upper = sym.upper()
                if sym_upper in earnings_map:
                    days_left = (datetime.strptime(earnings_map[sym_upper], "%Y-%m-%d").date() - today).days
                    if days_left <= 7:
                        flag = "FORCE_CLOSE"
                    elif days_left <= 21:
                        flag = "REVIEW"
                    else:
                        flag = "CLEAR"
                    results[sym_upper] = {
                        "days_to_earnings": days_left,
                        "date":             earnings_map[sym_upper],
                        "flag":             flag,
                    }
                else:
                    results[sym_upper] = {"days_to_earnings": None, "date": None, "flag": "CLEAR"}

        except Exception as e:
            logger.warning(f"[EarningsProximity] Tradier calendar fetch failed: {e}")
        return results


def _gex_empty(symbol):
    return {
        "flip_strike": 0.0,
        "current_spot": 0.0,
        "market_state": "UNKNOWN",
        "gex_total": 0.0,
        "pc_oi_ratio": 1.0,
        "pc_tag": "N/A",
    }

import os
import sys
import time
import logging
import requests
import numpy as np
import pandas as pd
import pytz
from datetime import datetime, timezone
from dotenv import load_dotenv
from scipy.stats import norm
from scipy.optimize import brentq
from database import EcosystemDatabase
from essentials_tools import send_essentials_embed
from market_structure import analyze_market_structure

logger = logging.getLogger("TQQQ_Sniper")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
FRED_API_KEY        = os.getenv("FRED_API_KEY")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")

# Documented reference thresholds adapted from a production TQQQ regime-switching architecture
# (9-step decision tree: SMA master switch, ATR extreme lockout, Nasdaq breadth filter, VIX crisis
# override). Treat as sensible starting defaults, not scraped historical fact — tune via the DB if
# live performance suggests otherwise.
ATR_EXTREME = 0.035        # TQQQ daily ATR% above this = leveraged-ETF chop, stand down
ATR_ELEVATED = 0.025
ATR_NORMAL = 0.018
BREADTH_COLLAPSE = 0.35    # % of top-10 QQQ holdings above their own 200D SMA
BREADTH_STRONG = 0.70
VIX_CRISIS_Z = 1.5         # VIXY z-score vs its own 20D mean — see fetch_vix() note on why
RISK_FREE_RATE = 0.045
LIVE_SIGNAL_COOLDOWN_DAYS = 5  # retained for reference, no longer used as a gate — see execute_sniper_sweep

# =========================================================================
# TQQQ LEAP DESK — Deep ITM long-dated calls on red days / bearish setups.
# Thesis: buy TIME on a red day. Recovery is virtually certain over a 9-18
# month horizon on a Nasdaq-100 3× ETF. Defined risk = premium paid only.
#
# WARNING: TQQQ is already 3× leveraged. A LEAP on TQQQ = leverage on
# leverage. Treat as high-conviction, small-size (max 2-3% portfolio).
# =========================================================================
LEAP_DTE_MIN = 270          # 9 months minimum runway
LEAP_DTE_MAX = 540          # 18 months maximum runway
LEAP_DELTA_TARGET = 0.72    # deep ITM — high intrinsic, lower theta-decay %
LEAP_DELTA_BAND = 0.06      # accept delta in [0.66, 0.78]
LEAP_COOLDOWN_HOURS = 2     # min hours between LEAP entry signals — re-evaluates the bottom on continued downtrends
LEAP_CUT_THRESHOLD = -30.0  # % underlying move → reassessment alert (not auto-close)
LEAP_TP1_PCT = 50.0         # % gain → scale 50% out
LEAP_TP2_PCT = 100.0        # % gain → close remainder
LEAP_ROLL_DTE = 90          # DTE remaining → roll forward consideration

# PUT desk (top-hunting) — uses QQQ puts, not TQQQ (better liquidity, lower theta decay)
LEAP_PUT_DTE_MIN = 180         # 6 months — tops can run; give it time
LEAP_PUT_DTE_MAX = 365         # 12 months — don't over-extend, reroll if needed
LEAP_PUT_DELTA_TARGET = -0.72  # deep ITM put (negative delta)
LEAP_PUT_DELTA_BAND = 0.06     # accept delta in [-0.78, -0.66]
LEAP_PUT_COOLDOWN_HOURS = 2    # same 2-hour re-evaluation cadence as CALL desk
LEAP_PUT_SYMBOL = "QQQ"        # puts on QQQ — TQQQ puts have brutal theta + thin OI

# Cycle position score thresholds — how oversold/overbought before a LEAP fires
CYCLE_BOTTOM_THRESHOLD = 55    # bottom_score >= this unlocks CALL desk
CYCLE_TOP_THRESHOLD = 55       # top_score >= this unlocks PUT desk


def is_market_hours():
    """Returns True only during NYSE RTH (09:30–16:00 ET, Mon–Fri)."""
    et = pytz.timezone('America/New_York')
    now_et = datetime.now(et)
    if now_et.weekday() >= 5:
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close


# =========================================================================
# BLACK-SCHOLES GREEKS — self-derived conviction layer, independent of
# whatever (often stale/missing) delta field the live options chain returns.
# IV is estimated from TQQQ's own realized volatility with a VIX floor, the
# same approach used in the reference architecture (RV20 * 1.15, floored by
# VIX/100 * 3.2 since TQQQ runs ~3x QQQ's vol).
# =========================================================================

def estimate_iv(rv20, vix=None, min_iv=0.55, iv_premium_mult=1.15, tqqq_vix_mult=3.2):
    iv = rv20 * iv_premium_mult
    if vix is not None and not np.isnan(vix):
        iv = max(iv, (vix / 100.0) * tqqq_vix_mult)
    return max(iv, min_iv)


def fetch_tqqq_atm_iv(db=None) -> float:
    """
    Return real ATM IV for TQQQ from Tradier (30–50 DTE window, call side).
    Falls back to 0.0 — caller must then use estimate_iv() proxy.
    Caches result in DB for 1 hour to avoid redundant chain fetches across
    the monitor loop's 5-min ticks.
    """
    import time
    try:
        cache_key = "tqqq_atm_iv_tradier"
        if db is not None:
            cached = db.get_state(cache_key)
            if cached:
                ts, val = cached.get("ts", 0), cached.get("iv", 0.0)
                if time.time() - ts < 3600 and val > 0:
                    return float(val)

        from tradier_client import TradierClient
        tc = TradierClient()
        if not tc.api_key:
            return 0.0
        iv = tc.get_atm_iv("TQQQ", option_type="call", dte_min=30, dte_max=50)
        if iv > 0 and db is not None:
            db.update_state(cache_key, {"ts": time.time(), "iv": iv})
        return iv
    except Exception:
        return 0.0


def bs_d1(S, K, T, r, sigma):
    return (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))


def bs_price(S, K, T, r, sigma, option_type="call"):
    if T <= 1e-8 or sigma <= 1e-8:
        return max(0.0, S - K) if option_type == "call" else max(0.0, K - S)
    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == "call":
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def bs_greeks(S, K, T, r, sigma, option_type="call"):
    """Delta, Gamma, Theta (per day), Vega (per 1% IV move), and risk-neutral prob. ITM at expiry."""
    if T <= 1e-8 or sigma <= 1e-8:
        delta = (1.0 if S > K else 0.0) if option_type == "call" else (-1.0 if S < K else 0.0)
        return {"delta": delta, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "prob_itm": 1.0 if delta != 0 else 0.0}
    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * np.sqrt(T)
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T) / 100.0
    if option_type == "call":
        delta = norm.cdf(d1)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * norm.cdf(d2)) / 365.0
        prob_itm = float(norm.cdf(d2))
    else:
        delta = norm.cdf(d1) - 1.0
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * norm.cdf(-d2)) / 365.0
        prob_itm = float(norm.cdf(-d2))
    return {"delta": float(delta), "gamma": float(gamma), "theta": float(theta), "vega": float(vega), "prob_itm": prob_itm}


def find_strike_for_delta(S, T, r, sigma, target_delta, option_type="call"):
    """Root-finds the strike whose BS delta matches target_delta (e.g. 0.40 call / -0.40 put)."""
    if T <= 1e-8:
        return round(S, 2)
    def objective(K):
        return bs_greeks(S, K, T, r, sigma, option_type)["delta"] - target_delta
    try:
        return round(brentq(objective, S * 0.5, S * 1.8, xtol=0.01), 2)
    except Exception:
        return round(S * (1.05 if option_type == "call" else 0.95), 2)


def calculate_atr_pct(df, period=14):
    """True-Range-based ATR as a % of price — the most direct read on TQQQ's own leveraged chop."""
    high_low = df["high"] - df["low"]
    high_cp = (df["high"] - df["close"].shift()).abs()
    low_cp = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return float(atr / df["close"].iloc[-1]) if df["close"].iloc[-1] else 0.0


class TQQQTacticalSniper:
    # Full top-20 QQQ holdings by weight, matching the reference architecture exactly — restored
    # from a cost-trimmed top-10 now that the live key is confirmed on Twelve Data's Venture tier
    # (610 credits/min vs. free tier's 8/min), which removes the original cost constraint. Still
    # cached once per day, not re-fetched every sweep, since breadth is an inherently slow-moving signal.
    BREADTH_HOLDINGS = [
        "MSFT", "AAPL", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
        "NFLX", "AMD", "ADBE", "QCOM", "INTC", "INTU", "CSCO", "TXN", "AMGN", "HON",
    ]

    def __init__(self):
        self.symbol = "TQQQ"
        self.proxy_symbol = "QQQ"
        self.base_url = "https://api.twelvedata.com"

    def fetch_adx_macd(self):
        """
        ADX: trend strength filter — ADX < 20 = chop, entries unreliable.
        MACD: histogram direction confirms momentum aligns with trade direction.
        Both on QQQ daily. Returns dict or None on failure.
        """
        try:
            adx_res = requests.get(
                f"{self.base_url}/adx",
                params={"symbol": self.proxy_symbol, "interval": "1day",
                        "time_period": 14, "apikey": TWELVE_DATA_API_KEY},
                timeout=12
            ).json()
            adx = float(adx_res.get("values", [{}])[0].get("adx", 0.0))

            macd_res = requests.get(
                f"{self.base_url}/macd",
                params={"symbol": self.proxy_symbol, "interval": "1day",
                        "fast_period": 12, "slow_period": 26, "signal_period": 9,
                        "apikey": TWELVE_DATA_API_KEY},
                timeout=12
            ).json()
            latest_macd = macd_res.get("values", [{}])[0]
            hist = float(latest_macd.get("macd_hist", 0.0))
            prev_hist = float(macd_res.get("values", [{}, {}])[1].get("macd_hist", hist))
            return {
                "adx": adx,
                "macd_hist": hist,
                "macd_expanding": abs(hist) > abs(prev_hist),  # histogram widening = momentum building
                "macd_bull": hist > 0,
            }
        except Exception as e:
            logger.warning(f"ADX/MACD fetch failed: {e}")
            return None

    def fetch_daily_baseline(self):
        """QQQ daily series for SMA200/SMA50 macro posture. Cached once per trading day —
        SMA200 doesn't change meaningfully between 15-min sweeps."""
        today_str = datetime.now().strftime("%Y-%m-%d")
        cache_key = f"tqqq_daily_baseline_{today_str}"
        cached = db.get_state(cache_key)
        if cached:
            return cached

        params = {"symbol": self.proxy_symbol, "interval": "1day", "outputsize": "200", "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res:
                return None
            df = pd.DataFrame(res["values"])
            df["close"] = df["close"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            result = {
                "spot": float(df["close"].iloc[-1]),
                "sma200": float(df["close"].rolling(window=200).mean().iloc[-1]),
                "sma50": float(df["close"].rolling(window=50).mean().iloc[-1]),
                "ema21": float(df["close"].ewm(span=21, adjust=False).mean().iloc[-1]),
            }
            db.update_state(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Daily baseline fetch failed: {e}")
            return None

    def fetch_intraday_metrics(self):
        """QQQ 5-min series for VWAP Z-score and volume whale flow detection."""
        params = {"symbol": self.proxy_symbol, "interval": "5min", "outputsize": "100", "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res:
                return None
            df = pd.DataFrame(res["values"])
            df["close"] = df["close"].astype(float)
            df["volume"] = df["volume"].astype(int)
            df = df.iloc[::-1].reset_index(drop=True)

            df['pv'] = df['close'] * df['volume']
            df['vwap'] = (
                df['pv'].rolling(window=78, min_periods=1).sum() /
                df['volume'].rolling(window=78, min_periods=1).sum()
            )
            vwap_std = df['close'].rolling(window=78, min_periods=2).std()
            df['z_score'] = (df['close'] - df['vwap']) / vwap_std

            vol_mean = df['volume'].rolling(window=20).mean()
            vol_std = df['volume'].rolling(window=20).std()
            df['vol_z'] = (df['volume'] - vol_mean) / vol_std

            latest = df.iloc[-1]
            spot = latest["close"]


            return {
                "spot": spot,
                "vwap": latest["vwap"],
                "z_score": latest["z_score"] if pd.notna(latest["z_score"]) else 0.0,
                "vol_z": latest["vol_z"] if pd.notna(latest["vol_z"]) else 0.0,
            }
        except Exception as e:
            logger.error(f"Intraday metrics fetch failed: {e}")
            return None

    def fetch_tqqq_daily_series(self, outputsize=30):
        """TQQQ's own daily OHLC — for ATR% and RV20. Cached once per trading day."""
        today_str = datetime.now().strftime("%Y-%m-%d")
        cache_key = f"tqqq_daily_series_{today_str}"
        cached = db.get_state(cache_key)
        if cached:
            try:
                return pd.DataFrame(cached)
            except Exception:
                pass

        params = {"symbol": self.symbol, "interval": "1day", "outputsize": str(outputsize), "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res:
                return None
            df = pd.DataFrame(res["values"])
            for col in ("open", "high", "low", "close"):
                df[col] = df[col].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            db.update_state(cache_key, df.to_dict(orient="list"))
            return df
        except Exception as e:
            logger.error(f"TQQQ daily series fetch failed: {e}")
            return None

    def fetch_vix(self):
        """
        The bare "VIX" symbol 404s on every Twelve Data endpoint at this plan tier (cash index
        access requires a higher tier) — confirmed this is silently broken ecosystem-wide
        (analytics.py / scheduler.py / stream.py all reference it and have been falling back to
        hardcoded defaults). VIXY (VIX futures ETF) resolves fine, but its absolute price isn't on
        the same scale as the VIX index and decays via contango, so instead of comparing it to a
        VIX-scale threshold, this measures its z-score vs its own 20D mean — a fear SPIKE, not a
        fear LEVEL, which is what actually matters for a "is volatility blowing out right now" gate.
        Returns (vixy_price, z_score).
        """
        try:
            res = requests.get(
                f"{self.base_url}/time_series",
                params={"symbol": "VIXY", "interval": "1day", "outputsize": "20", "apikey": TWELVE_DATA_API_KEY},
                timeout=10,
            ).json()
            values = res.get("values", [])
            if len(values) < 10:
                return 20.0, 0.0
            closes = np.array([float(v["close"]) for v in values], dtype=float)
            current, mean, std = closes[0], closes.mean(), closes.std()
            z = (current - mean) / std if std > 0 else 0.0
            return float(current), float(z)
        except Exception as e:
            logger.error(f"VIX (VIXY proxy) fetch failed: {e}")
            return 20.0, 0.0

    def fetch_breadth(self):
        """
        Nasdaq-100 breadth: % of top-10 QQQ holdings trading above their own 200D SMA.
        Breadth deteriorating WHILE price/SMA200 still looks bullish is the classic early-warning
        divergence — cached once/day since it's slow-moving and otherwise costs 10 API calls/sweep.
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        if db.get_state("tqqq_breadth_cache_date") == today_str:
            cached = db.get_state("tqqq_breadth_cache")
            if cached is not None:
                return float(cached)

        above, total = 0, 0
        for sym in self.BREADTH_HOLDINGS:
            try:
                res = requests.get(
                    f"{self.base_url}/time_series",
                    params={"symbol": sym, "interval": "1day", "outputsize": "200", "apikey": TWELVE_DATA_API_KEY},
                    timeout=10,
                ).json()
                values = res.get("values", [])
                if len(values) < 200:
                    continue
                closes = [float(v["close"]) for v in values]
                if closes[0] > np.mean(closes[:200]):
                    above += 1
                total += 1
            except Exception:
                continue

        breadth = (above / total) if total > 0 else 0.60
        db.update_state("tqqq_breadth_cache", breadth)
        db.update_state("tqqq_breadth_cache_date", today_str)
        logger.info(f"Breadth refreshed: {above}/{total} = {breadth:.0%}")
        return breadth

    def enrich_with_options_chain(self, setup):
        """
        Fetches the live TQQQ options chain to find specific:
        strike, expiration, DTE, delta, IV, and mid-price.
        TQQQ optimal DTE: 10-21 days (3x leveraged ETF — theta burns fast).
        """
        try:
            chain = requests.get(
                f"{self.base_url}/options/chain",
                params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY},
                timeout=12
            ).json()
            if not chain or "data" not in chain or not chain["data"]:
                return setup

            df = pd.DataFrame(chain["data"])
            df["strike"] = df["strike"].astype(float)
            df["expiration_date"] = pd.to_datetime(df["expiration_date"])
            df["dte"] = (df["expiration_date"] - pd.Timestamp.today()).dt.days
            df["implied_volatility"] = pd.to_numeric(
                df.get("implied_volatility", 0), errors="coerce"
            ).fillna(0)
            df["open_interest"] = pd.to_numeric(
                df.get("open_interest", 0), errors="coerce"
            ).fillna(0)
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0) if "volume" in df.columns else 0

            if "delta" in df.columns:
                df["delta"] = pd.to_numeric(df["delta"], errors="coerce").fillna(0).abs()
            else:
                tqqq_spot = setup["tqqq_spot"]
                df["delta"] = ((tqqq_spot - df["strike"]) / tqqq_spot).clip(0.01, 0.99)

            if "bid" in df.columns and "ask" in df.columns:
                df["mid"] = (
                    pd.to_numeric(df["bid"], errors="coerce") +
                    pd.to_numeric(df["ask"], errors="coerce")
                ) / 2
            else:
                df["mid"] = df["strike"] * 0.03

            target_type = "call" if setup["contract"] == "CALL" else "put"

            candidates = df[
                (df["type"] == target_type) &
                (df["dte"].between(10, 21)) &
                (df["delta"].between(0.30, 0.50)) &
                (df["open_interest"] >= 10)
            ].copy()

            if candidates.empty:
                candidates = df[
                    (df["type"] == target_type) &
                    (df["dte"].between(7, 30)) &
                    (df["delta"].between(0.25, 0.60))
                ].copy()

            if candidates.empty:
                return setup

            candidates["score"] = (
                (1 - (candidates["delta"] - 0.40).abs()) +
                (1 - (candidates["dte"] - 15).abs() / 15.0).clip(0, 1)
            )
            best = candidates.loc[candidates["score"].idxmax()]

            iv_pct = float(best["implied_volatility"])
            if iv_pct < 5.0:
                iv_pct = iv_pct * 100

            setup["real_strike"] = float(best["strike"])
            setup["real_dte"] = int(best["dte"])
            setup["real_delta"] = round(float(best["delta"]), 2)
            setup["real_iv"] = round(iv_pct, 1)
            setup["real_premium"] = round(float(best.get("mid", 0.0)), 2)
            setup["real_expiry"] = best["expiration_date"].strftime("%m/%d/%Y")
            setup["real_volume"] = int(best.get("volume", 0))
            setup["real_oi"] = int(best.get("open_interest", 0))
            setup["real_oi_low"] = int(candidates["open_interest"].min())
            setup["real_oi_high"] = int(candidates["open_interest"].max())

        except Exception as e:
            logger.warning(f"Options chain enrichment failed: {e}")

        return setup

    def enrich_with_self_derived_greeks(self, setup, tqqq_daily):
        """
        Black-Scholes Greeks computed from TQQQ's own realized volatility — independent
        conviction check that doesn't depend on the (often stale or missing) broker chain delta.
        Shown alongside the live chain data so a mismatch is visible, not hidden.
        """
        if tqqq_daily is None or len(tqqq_daily) < 21:
            return setup
        try:
            returns = tqqq_daily["close"].pct_change().dropna()
            rv20 = float(returns.tail(20).std() * np.sqrt(252))
            vix = setup.get("vix", 18.0)
            # Try real Tradier IV first; fall back to RV20 proxy if unavailable.
            real_iv = fetch_tqqq_atm_iv(db)
            iv = real_iv if real_iv > 0 else estimate_iv(rv20, vix)

            dte = setup.get("real_dte", 15)
            T = dte / 365.0
            spot = setup["tqqq_spot"]
            option_type = "call" if setup["contract"] == "CALL" else "put"
            target_delta = 0.40 if option_type == "call" else -0.40

            strike = setup.get("real_strike") or find_strike_for_delta(spot, T, RISK_FREE_RATE, iv, target_delta, option_type)
            greeks = bs_greeks(spot, strike, T, RISK_FREE_RATE, iv, option_type)
            theo_price = bs_price(spot, strike, T, RISK_FREE_RATE, iv, option_type)

            atr_pct = setup.get("atr_pct_tqqq", 0.02)
            expected_move = spot * atr_pct * np.sqrt(max(dte, 1))
            breakeven = strike + theo_price if option_type == "call" else strike - theo_price

            setup["bs_rv20"] = round(rv20 * 100, 1)
            setup["bs_iv_est"] = round(iv * 100, 1)
            setup["bs_iv_source"] = "Tradier" if real_iv > 0 else "RV20 proxy"
            setup["bs_strike"] = strike
            setup["bs_delta"] = round(greeks["delta"], 3)
            setup["bs_gamma"] = round(greeks["gamma"], 4)
            setup["bs_theta"] = round(greeks["theta"], 3)
            setup["bs_vega"] = round(greeks["vega"], 3)
            setup["bs_prob_itm"] = round(greeks["prob_itm"] * 100, 1)
            setup["bs_theo_price"] = round(theo_price, 2)
            setup["bs_breakeven"] = round(breakeven, 2)
            setup["expected_move"] = round(expected_move, 2)

            real_premium = setup.get("real_premium", 0.0)
            if real_premium > 0:
                reward = expected_move
                risk = real_premium
                setup["risk_reward"] = round(reward / risk, 2) if risk > 0 else None
        except Exception as e:
            logger.warning(f"Self-derived Greeks calculation failed: {e}")
        return setup

    def evaluate_snipe(self, daily, intraday, vix_price, vix_z, breadth, atr_pct_tqqq):
        spot = daily["spot"]
        sma200 = daily["sma200"]
        sma50 = daily["sma50"]
        z_score = intraday["z_score"]
        vol_z = intraday["vol_z"]

        macro_bull = spot > sma200
        macro_bear = spot < sma200 or spot > (sma50 * 1.08)

        # Volatility/regime gates — leveraged ETF chop and crisis VIX kill premium fast, so a
        # raw Z-score extreme alone is no longer sufficient for a LIVE signal in those regimes.
        atr_extreme = atr_pct_tqqq >= ATR_EXTREME
        breadth_collapsing = breadth < BREADTH_COLLAPSE
        vix_crisis = vix_z >= VIX_CRISIS_Z

        if macro_bull and z_score <= -2.0 and vol_z >= 2.0:
            action, contract = "Buy to Open (BTO)", "CALL"
        elif macro_bear and z_score >= 2.0 and vol_z >= 2.0:
            action, contract = "Buy to Open (BTO)", "PUT"
        elif abs(z_score) >= 1.8:
            action = "MONITORING SETUP"
            contract = "CALL" if z_score < 0 else "PUT"
            # In a bull regime, a positive z-score means QQQ is extended above VWAP —
            # normal on strong up-days. Only flag mean-reversion PUT if truly extreme (≥3.0σ),
            # otherwise the signal fires every time the market rallies with conviction.
            if contract == "PUT" and macro_bull and z_score < 3.0:
                return None
        else:
            return None

        # Golden-Setup downgrade: a contradicting macro signature drops a would-be LIVE execution
        # back to MONITORING — this is what keeps trade frequency low and conviction high, per the
        # explicit "I don't want to trade often, I want highly successful setups" requirement.
        # Explicit 21 EMA gate for calls — claude.md's "QQQ above 21 EMA + VIX < 20" entry rule.
        ema21 = daily.get("ema21", 0.0)
        below_21ema = ema21 > 0 and spot < ema21

        adx_data = daily.get("adx_macd", {}) or {}
        adx = adx_data.get("adx", 25.0)  # default 25 = trending (safe fallback)
        adx_chop = adx < 20.0  # < 20 = no trend = directional options unreliable

        downgrade_reason = None
        if action == "Buy to Open (BTO)":
            if contract == "CALL" and (atr_extreme or breadth_collapsing or vix_crisis or below_21ema or adx_chop):
                downgrade_reason = (
                    "ATR_EXTREME" if atr_extreme else
                    ("BREADTH_COLLAPSE" if breadth_collapsing else
                     ("VIX_CRISIS" if vix_crisis else
                      ("BELOW_21EMA" if below_21ema else "ADX_CHOP")))
                )
            elif contract == "PUT" and (atr_extreme or adx_chop):
                downgrade_reason = "ATR_EXTREME" if atr_extreme else "ADX_CHOP"
            if downgrade_reason:
                action = "MONITORING SETUP"

        try:
            tqqq_spot = float(requests.get(
                f"{self.base_url}/price",
                params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY},
                timeout=8
            ).json().get("price", 0.0))
        except Exception:
            tqqq_spot = 0.0

        if tqqq_spot == 0.0:
            return None

        setup = {
            "action": action,
            "contract": contract,
            "z_score": z_score,
            "vol_z": vol_z,
            "qqq_spot": spot,
            "qqq_vwap": intraday["vwap"],
            "tqqq_spot": tqqq_spot,
            "sma200": sma200,
            "sma50": sma50,
            "vix": vix_price,
            "vix_z": vix_z,
            "breadth": breadth,
            "atr_pct_tqqq": atr_pct_tqqq,
            "downgrade_reason": downgrade_reason,
            "adx": adx,
            "adx_chop": adx_chop,
            "macd_hist": adx_data.get("macd_hist", 0.0),
            "macd_expanding": adx_data.get("macd_expanding", False),
            "macd_bull": adx_data.get("macd_bull", True),
        }
        setup = self.enrich_with_options_chain(setup)
        return setup

    def dispatch_intelligence(self, setup, tqqq_daily):
        """Dispatches signal via send_essentials_embed with full contract specification."""
        setup = self.enrich_with_self_derived_greeks(setup, tqqq_daily)
        is_live = setup['action'] != "MONITORING SETUP"

        # Price-action structure (FVGs, liquidity sweeps, equal highs/lows) on TQQQ's own daily
        # series — already fetched for the Greeks calc above, so this is free. Used as a
        # confluence booster on the dispatched language, not a hard gate, so it doesn't further
        # reduce an already-conservative signal frequency.
        structure = analyze_market_structure(tqqq_daily) if tqqq_daily is not None else None
        expected_bias = "BULLISH" if setup["contract"] == "CALL" else "BEARISH"
        structure_confirms = structure is not None and structure["bias"] == expected_bias

        if is_live:
            title = f"TQQQ OPTIONS SNIPER | BTO {setup['contract']} EXECUTION"
            status_tag = (
                "🎯🎯 GOLDEN SETUP — STRUCTURE-CONFIRMED LIVE EXECUTION" if structure_confirms
                else "🎯 GOLDEN SETUP — LIVE EXECUTION SIGNAL"
            )
            color = 0x2ecc71 if setup["contract"] == "CALL" else 0xe74c3c
        else:
            title = "TQQQ OPTIONS DESK | Setup Under Construction"
            reason = f" ({setup['downgrade_reason']} — downgraded from live execution)" if setup.get("downgrade_reason") else ""
            status_tag = f"⚠️ SETUP FORMING — Monitor Closely{reason}"
            color = 0xf1c40f

        if "real_strike" in setup:
            contract_line = (
                f"${setup['real_strike']:.2f} {setup['contract']} — "
                f"{setup['real_expiry']} ({setup['real_dte']} DTE)"
            )
            delta_so_what = f"Δ {setup['real_delta']:.2f} — {abs(setup['real_delta']):.0%} probability ITM at expiry"
            cost_line = f"┣ Cost: ~${setup['real_premium'] * 100:.0f}/contract (mid-market)\n"
            liquidity_line = f"┣ Liquidity: Volume `{setup.get('real_volume', 0):,}` | OI `{setup.get('real_oi', 0):,}`\n"
        else:
            est_strike = setup['tqqq_spot'] * (1.05 if setup['contract'] == "CALL" else 0.95)
            contract_line = f"~${est_strike:.2f} {setup['contract']} — 10-21 DTE"
            delta_so_what = "Δ 0.35–0.45 target — moderate leverage, ~35-45% prob ITM at expiry"
            cost_line = ""  # no Tradier data — omit rather than show placeholder
            liquidity_line = ""

        bs_block = ""
        if "bs_delta" in setup:
            theta_dollar = setup['bs_theta'] * 100  # per contract
            vega_dollar  = setup['bs_vega'] * 100
            bs_block = (
                f"┣ Math Check (RV20-based Black-Scholes):\n"
                f"┃  Strike ${setup['bs_strike']:.2f} | Theo ${setup['bs_theo_price']:.2f}"
                + (f" (chain mid ~${setup['real_premium']*100:.0f})" if "real_strike" in setup else "")
                + f" | IV {setup['bs_iv_est']:.1f}% vs RV20 {setup['bs_rv20']:.1f}%\n"
                f"┃  Decay: {theta_dollar:+.2f}/day per contract | IV sensitivity: ${vega_dollar:.2f} per 1% IV move\n"
            )

        prob_rr_line = ""
        if "bs_prob_itm" in setup:
            rr_str = f" | R/R 1:{setup['risk_reward']:.1f}" if setup.get("risk_reward") else ""
            prob_rr_line = f"┣ Prob. ITM at Expiry: {setup['bs_prob_itm']:.1f}%{rr_str} | Breakeven: ${setup['bs_breakeven']:.2f}\n"

        atr_move_line = ""
        if setup.get("expected_move"):
            atr_dir = "+" if setup["contract"] == "CALL" else "-"
            atr_move_line = f"┣ ATR-Projected Move ({setup.get('real_dte', 15)}D): {atr_dir}${setup['expected_move']:.2f} from current\n"

        structure_line = ""
        if structure and structure["setup"] != "NO STRUCTURE SETUP":
            confirm_tag = " ✅ CONFIRMS DIRECTION" if structure_confirms else ""
            structure_line = f"┣ Market Structure: {structure['setup']} ({structure['bias']}){confirm_tag} — {structure['detail']}\n"

        posture = "ABOVE VWAP" if setup["qqq_spot"] > setup["qqq_vwap"] else "BELOW VWAP"
        macro = "BULL REGIME" if setup["qqq_spot"] > setup["sma200"] else "BEAR REGIME"
        vix_tag = "🔴 FEAR SPIKE" if setup["vix_z"] >= VIX_CRISIS_Z else ("🟡 ELEVATED" if setup["vix_z"] >= 0.75 else "🟢 CALM")
        breadth_tag = "🔴 COLLAPSING" if setup["breadth"] < BREADTH_COLLAPSE else ("🟢 STRONG" if setup["breadth"] >= BREADTH_STRONG else "🟡 MIXED")
        atr_tag = "🔴 EXTREME (high chop risk)" if setup["atr_pct_tqqq"] >= ATR_EXTREME else ("🟡 ELEVATED" if setup["atr_pct_tqqq"] >= ATR_ELEVATED else "🟢 NORMAL")

        adx_val = setup.get("adx", 0.0)
        adx_tag = "🔴 CHOP (<20)" if adx_val < 20 else ("🟡 WEAK (20-25)" if adx_val < 25 else "🟢 TRENDING (>25)")
        macd_hist = setup.get("macd_hist", 0.0)
        macd_dir = "▲ expanding" if setup.get("macd_expanding") and macd_hist > 0 else \
                   ("▼ expanding" if setup.get("macd_expanding") and macd_hist < 0 else "→ compressing")
        macd_tag = f"{'🟢' if setup.get('macd_bull') else '🔴'} {macd_hist:+.3f} {macd_dir}"

        # Embed 1: Market regime/conditions — the "why now" context
        regime_payload = (
            f"QQQ Proxy | Macro: {macro} | Intraday: {posture}\n"
            f"┣ QQQ Spot: `${setup['qqq_spot']:,.2f}` | VWAP: `${setup['qqq_vwap']:,.2f}`\n"
            f"┣ VWAP Z-Score: `{setup['z_score']:+.2f}σ` | Volume Surge Z: `{setup['vol_z']:+.2f}σ`\n"
            f"┣ ADX (14): `{adx_val:.1f}` {adx_tag} | MACD Hist: {macd_tag}\n"
            f"┣ VIXY `{setup['vix']:.2f}` (z `{setup['vix_z']:+.2f}σ` {vix_tag}) | Breadth: `{setup['breadth']:.0%}` {breadth_tag}\n"
            f"┗ ATR% (TQQQ daily range): `{setup['atr_pct_tqqq']:.1%}` {atr_tag}"
        )

        # Embed 2: Contract/execution — the "what to do" block
        execution_payload = (
            f"TQQQ @ `${setup['tqqq_spot']:.2f}`\n"
            f"┣ 🎯 {setup['action']}: {contract_line}\n"
            f"{cost_line}"
            f"{liquidity_line}"
            f"┣ Entry Delta: {delta_so_what}\n"
            f"{bs_block}"
            f"{prob_rr_line}"
            f"{atr_move_line}"
            f"{structure_line}"
            f"┣ Stop: −35% premium or 15m pivot break\n"
            f"┗ Take Profit: Scale 50% at +100%, trail remainder with 15m 21-EMA\n\n"
            f"Exit Signal: Will fire automatically when Z-score reverts or reverses — watch this channel."
        )

        if WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, title, regime_payload, color)
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"{'BTO EXECUTION' if is_live else 'CONTRACT SETUP'} | TQQQ {setup['contract']}", execution_payload, color)

        if is_live:
            db.update_state("tqqq_last_live_signal_date", datetime.now().strftime("%Y-%m-%d"))
            prediction_id = f"{setup['contract']}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            db.update_state("tqqq_open_position", {
                "contract": setup["contract"],
                "entry_tqqq_spot": setup["tqqq_spot"],
                "entry_z_score": setup["z_score"],
                "strike": setup.get("real_strike") or setup.get("bs_strike"),
                "expiry": setup.get("real_expiry"),
                "dte_at_entry": setup.get("real_dte", 15),
                "entry_time": datetime.now().isoformat(),
                "prediction_id": prediction_id,
            })
            try:
                from analytics import HighFidelityAnalyticsEngine
                direction = "UP" if setup["contract"] == "CALL" else "DOWN"
                HighFidelityAnalyticsEngine().log_ledger_prediction(
                    "tqqq", prediction_id, direction, setup["tqqq_spot"], ticker="TQQQ",
                    context=f"{setup['contract']} @ Z{setup['z_score']:+.2f}σ"
                )
            except Exception as e:
                logger.error(f"TQQQ ledger logging failed: {e}")
            logger.info(f"Open position recorded: {setup['contract']} @ TQQQ ${setup['tqqq_spot']:.2f}")

    def check_open_position_for_exit(self, intraday, atr_pct_tqqq):
        """
        Sniper strategy = entries AND exits. Once a BTO fires, this checks every sweep for the
        mirror-image exit conditions: mean reversion complete, reversal, or volatility-extreme
        capital protection. Fires an explicit STC alert the moment any condition is met.
        """
        position = db.get_state("tqqq_open_position")
        if not position:
            return

        z_score = intraday["z_score"]
        contract = position["contract"]
        exit_reason = None

        if abs(z_score) <= 0.5:
            exit_reason = "Mean reversion complete — VWAP extreme has normalized."
        elif contract == "CALL" and z_score >= 1.5:
            exit_reason = "Reversal detected — momentum has flipped against the CALL."
        elif contract == "PUT" and z_score <= -1.5:
            exit_reason = "Reversal detected — momentum has flipped against the PUT."
        elif atr_pct_tqqq >= ATR_EXTREME:
            exit_reason = "Volatility extreme — protect capital, exit before further chop."

        if not exit_reason:
            return

        tqqq_spot = 0.0
        if not tqqq_spot:
            try:
                tqqq_spot = float(requests.get(
                    f"{self.base_url}/price", params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY}, timeout=8
                ).json().get("price", 0.0))
            except Exception:
                tqqq_spot = 0.0

        pnl_proxy = ((tqqq_spot - position["entry_tqqq_spot"]) / position["entry_tqqq_spot"] * 100) if tqqq_spot else 0.0
        if contract == "PUT":
            pnl_proxy *= -1

        payload = (
            f"🔔 **EXIT SIGNAL — Sell to Close {contract}**\n"
            f"┣ Entered @ TQQQ ${position['entry_tqqq_spot']:.2f} (Z {position['entry_z_score']:+.2f}σ)\n"
            f"┣ Now @ TQQQ ${tqqq_spot:.2f} | Underlying Move Since Entry: {pnl_proxy:+.2f}%\n"
            f"┣ Strike ${position.get('strike', 0):.2f} | Expiry {position.get('expiry', 'n/a')}\n"
            f"┗ Reason: {exit_reason}"
        )
        if WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "TQQQ OPTIONS SNIPER | EXIT SIGNAL", payload, 0x3498db)
        logger.info(f"Exit signal dispatched: {exit_reason}")

        if tqqq_spot and "prediction_id" in position:
            try:
                from analytics import HighFidelityAnalyticsEngine
                HighFidelityAnalyticsEngine().grade_ledger_prediction("tqqq", position["prediction_id"], tqqq_spot)
            except Exception as e:
                logger.error(f"TQQQ ledger grading failed: {e}")

        db.update_state("tqqq_open_position", None)
        db.update_state("tqqq_last_dispatched_state", None)  # cleared so next entry signal fires fresh

    def dispatch_market_outlook(self, daily, intraday, vix_price, vix_z, breadth, atr_pct_tqqq, tqqq_daily=None):
        """
        Sends a brief options conditions snapshot when no active TQQQ entry exists.
        Breaks silence with value — prevents the channel going dark.
        Gatekeeper-protected: max 1 broadcast per significant market shift.
        """
        spot = intraday["spot"]
        vwap = intraday["vwap"]
        z = intraday["z_score"]
        vol_z = intraday["vol_z"]
        sma200 = daily["sma200"]
        sma50 = daily["sma50"]

        bias = "BULL REGIME" if spot > sma200 else "BEAR REGIME"
        vwap_pos = "ABOVE VWAP" if spot > vwap else "BELOW VWAP"
        condition = "COMPRESSION ZONE" if abs(z) < 1.0 else "BUILDING PRESSURE"

        structure = analyze_market_structure(tqqq_daily) if tqqq_daily is not None else None
        structure_line = ""
        if structure and structure["setup"] != "NO STRUCTURE SETUP":
            structure_line = f"┣ Market Structure: {structure['setup']} ({structure['bias']}) — {structure['detail']}\n"

        outlook = (
            f"No active TQQQ entry setup — current conditions:\n\n"
            f"┣ QQQ Spot: ${spot:,.2f}\n"
            f"┣ VWAP: ${vwap:,.2f} ({vwap_pos})\n"
            f"┣ VWAP Z-Score: {z:+.2f}σ\n"
            f"┣ Volume Pressure: {vol_z:+.2f}σ\n"
            f"┣ Macro Regime: {bias}\n"
            f"┣ SMA200: ${sma200:,.2f}\n"
            f"┣ SMA50: ${sma50:,.2f}\n"
            f"┣ VIXY {vix_price:.2f} (z {vix_z:+.2f}σ)\n"
            f"┣ Nasdaq-100 Breadth: {breadth:.0%}\n"
            f"┣ TQQQ ATR%: {atr_pct_tqqq:.1%}\n"
            f"{structure_line}"
            f"┗ Status: {condition} — await Z ≥ ±1.8σ with vol surge for entry"
        )

        logger.debug(f"TQQQ Flowstate (no active setup): {condition}, z={z:+.2f}σ — suppressed, no dispatch.")

    def dispatch_regime_vital_sign(self, daily, breadth, vix_price, vix_z):
        """
        QQQ/TQQQ regime is a genuine market-wide vital sign, not just an options-scalper concern —
        a SMA200 cross or Nasdaq breadth collapse gets cross-posted to Market Analysis so the rest
        of the ecosystem (equities, futures, crypto) reads the same underlying deterioration signal.
        """
        if not WEBHOOK_MARKET_ANALYSIS:
            return
        spot, sma200 = daily["spot"], daily["sma200"]
        regime = "BULL" if spot > sma200 else "BEAR"
        breadth_state = "COLLAPSE" if breadth < BREADTH_COLLAPSE else ("STRONG" if breadth >= BREADTH_STRONG else "MIXED")
        state_key = f"{regime}_{breadth_state}_{'VIXSPIKE' if vix_z >= VIX_CRISIS_Z else 'NORMAL'}"

        if db.track_and_limit_alerts("tqqq_regime_vital_sign", state_key, spot, max_broadcasts=1, threshold_pct=0.01):
            # BLUF verdict
            if regime == "BULL" and breadth_state != "COLLAPSE" and vix_z < VIX_CRISIS_Z:
                bluf = "✅"
                verdict = "TQQQ calls eligible — bull regime, breadth holding"
                color = 0x2ecc71
            elif regime == "BEAR" or breadth_state == "COLLAPSE":
                bluf = "🔴"
                verdict = "TQQQ calls OFF — bear regime or breadth collapse. Puts/cash only"
                color = 0xe74c3c
            else:
                bluf = "⚠️"
                verdict = "TQQQ calls with reduced size — mixed breadth or elevated vol"
                color = 0xf39c12

            # VIXY translated to TQQQ-specific action
            if vix_z >= VIX_CRISIS_Z:
                vixy_note = f"z {vix_z:+.1f}σ SPIKE — close puts at profit, rotate into TQQQ calls (fear peak SOP)"
            elif vix_z >= 0.75:
                vixy_note = f"z {vix_z:+.1f}σ elevated — reduce call size 50%, renew put if ≤14 DTE"
            else:
                vixy_note = f"z {vix_z:+.1f}σ calm — low IV, calls cheap; good window to renew put if ≤14 DTE"

            above_pct = ((spot / sma200) - 1) * 100
            payload = (
                f"{bluf} **{verdict}**\n"
                f"┣ QQQ `${spot:,.2f}` vs SMA200 `${sma200:,.2f}` ({above_pct:+.1f}%) | Breadth: `{breadth:.0%}` {breadth_state}\n"
                f"┗ VIXY `{vix_price:.2f}` {vixy_note}"
            )
            logger.info(f"TQQQ regime gate: {state_key} — suppressed from trade-signals.")

    def check_insurance_put_renewal(self):
        """
        Module 4 (insurance leg) — the "homeowners insurance" put is a separate, always-on
        position from the directional sniper above: claude.md's rule is 1 active 30 DTE put,
        rinse-and-repeat, regardless of what the sniper's z-score/breadth signals are doing.
        This only tracks the renewal clock for whatever put was logged via
        `python tqqq.py --log-put --strike X --expiration YYYY-MM-DD --premium X`; it does not
        invent a position that wasn't actually entered.
        """
        put = db.get_state("tqqq_insurance_put")
        if not put:
            if db.track_and_limit_alerts("tqqq_insurance_put_missing", "NO_PUT", 0.0, max_broadcasts=1, threshold_pct=1.0):
                payload = (
                    "┣ No active insurance put on record.\n"
                    "┗ Log one with: `python tqqq.py --log-put --strike X --expiration YYYY-MM-DD --premium X`"
                )
                logger.info("Insurance put: none on record — suppressed from trade-signals.")
            return

        exp_date = datetime.strptime(put["expiration"], "%Y-%m-%d").date()
        dte = (exp_date - datetime.now().date()).days

        if dte < 0:
            db.update_state("tqqq_insurance_put", None)
            return

        if dte <= 14:
            state_key = f"PUT_RENEWAL_DTE_{dte}"
            if db.track_and_limit_alerts("tqqq_insurance_put_renewal", state_key, float(dte), max_broadcasts=2, threshold_pct=1.0):
                payload = (
                    f"┣ Strike: `${put['strike']:.2f}` | Expiration: `{put['expiration']}` ({dte} DTE)\n"
                    f"┣ Premium Paid: `${put['premium']:.2f}`\n"
                    f"┗ Roll/renew now — never skip a month (homeowners insurance model)."
                )
                if WEBHOOK_TRADE_SIGNALS:
                    send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "🛡️ TQQQ INSURANCE PUT | RENEWAL DUE", payload, 0xf39c12)
                    logger.info(f"Insurance put renewal alert dispatched at {dte} DTE.")

    def check_wave_position_status(self, tqqq_spot: float):
        """
        3-day wave check-in while a BTO position is open — not a new signal, just a status
        update so the user knows the wave is still intact without having to re-read the entry.
        Fires only if: (a) position is open, (b) ≥3 days since last update,
        (c) TQQQ has moved ≥3% from last update price (avoids noise on flat days).
        """
        position = db.get_state("tqqq_open_position")
        if not position or tqqq_spot == 0.0:
            return

        last_update_str  = db.get_state("tqqq_wave_last_update_date", "")
        last_update_price = float(db.get_state("tqqq_wave_last_update_price", tqqq_spot))
        today_str        = datetime.now().strftime("%Y-%m-%d")

        if last_update_str:
            days_since = (datetime.strptime(today_str, "%Y-%m-%d") - datetime.strptime(last_update_str, "%Y-%m-%d")).days
            if days_since < 3:
                return
        move_pct = abs((tqqq_spot - last_update_price) / last_update_price * 100) if last_update_price else 0.0
        if move_pct < 3.0 and last_update_str:
            return  # flat days — skip the check-in

        # Compute wave stats
        entry_price = position.get("entry_tqqq_spot", tqqq_spot)
        entry_time  = position.get("entry_time", today_str)
        contract    = position.get("contract", "?")
        strike      = position.get("strike") or 0.0
        expiry      = position.get("expiry") or "?"
        dte_entry   = position.get("dte_at_entry", 0)

        try:
            entry_dt  = datetime.fromisoformat(entry_time)
            day_n     = (datetime.now() - entry_dt).days + 1
        except Exception:
            day_n = "?"

        pnl_proxy     = (tqqq_spot - entry_price) / entry_price * 100 if entry_price else 0.0
        if contract == "PUT":
            pnl_proxy *= -1

        # Estimate DTE remaining from entry DTE and days held
        dte_remaining = max(0, dte_entry - (day_n if isinstance(day_n, int) else 0))

        if dte_remaining <= 14:
            status_tag = "⚠️ 14DTE ALERT — Consider rolling or closing"
        elif pnl_proxy >= 90.0:
            status_tag = "🎯 APPROACHING TARGET — Watch for exit signal"
        elif pnl_proxy <= -30.0:
            status_tag = "🔴 STOP ZONE — Review position"
        else:
            status_tag = "🌊 RIDING"

        payload = (
            f"**TQQQ WAVE — Day {day_n}**\n"
            f"┣ Position: BTO {contract} | Strike `${strike:.2f}` | Exp `{expiry}`\n"
            f"┣ Entry: `${entry_price:.2f}` → Now: `${tqqq_spot:.2f}` ({pnl_proxy:+.1f}% underlying move)\n"
            f"┣ Est. DTE Remaining: ~{dte_remaining} days\n"
            f"┗ Status: {status_tag}"
        )
        if WEBHOOK_TRADE_SIGNALS:
            color = 0x2ecc71 if pnl_proxy >= 0 else 0xe74c3c
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, "TQQQ WAVE UPDATE", payload, color)
        db.update_state("tqqq_wave_last_update_date", today_str)
        db.update_state("tqqq_wave_last_update_price", tqqq_spot)
        logger.info(f"Wave status update dispatched — Day {day_n}, {pnl_proxy:+.1f}%.")

    def check_regime_flip(self, qqq_spot: float, sma200: float):
        """
        When the macro regime flips BULL↔BEAR (QQQ crosses its 200 SMA), dispatch a brief
        regime-change note — not a trade signal, just a heads-up that a new wave direction
        may be forming. Rate-limited to 1 per calendar day (regimes don't flip hourly).
        """
        current_regime = "BULL" if qqq_spot > sma200 else "BEAR"
        prev_regime    = db.get_state("tqqq_macro_regime", "")

        if prev_regime and prev_regime != current_regime:
            today_str = datetime.now().strftime("%Y-%m-%d")
            flip_key  = f"tqqq_regime_flip_{today_str}"
            if not db.get_state(flip_key, ""):
                db.update_state(flip_key, "fired")
                dist_pct = (qqq_spot / sma200 - 1) * 100
                payload = (
                    f"**Macro Regime: {prev_regime} → {current_regime}**\n"
                    f"┣ QQQ: `${qqq_spot:.2f}` vs 200 SMA: `${sma200:.2f}` ({dist_pct:+.2f}%)\n"
                    f"┣ Wave Direction: {'BULLISH — watch for CALL setup' if current_regime == 'BULL' else 'BEARISH — watch for PUT setup'}\n"
                    f"┗ Not a signal yet — monitoring for wave setup conditions..."
                )
                if WEBHOOK_TRADE_SIGNALS:
                    color = 0x2ecc71 if current_regime == "BULL" else 0xe74c3c
                    send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"⚡ REGIME FLIP: {prev_regime} → {current_regime}", payload, color)
                logger.info(f"Regime flip alert dispatched: {prev_regime} → {current_regime}.")

        db.update_state("tqqq_macro_regime", current_regime)

    # =========================================================================
    # CYCLE POSITION SCORING — feeds both CALL and PUT LEAP desks
    # =========================================================================

    def fetch_qqq_extended_metrics(self):
        """RSI14, 52w high/low, CNN Fear & Greed. Cached once per trading day."""
        today_str = datetime.now().strftime("%Y-%m-%d")
        cache_key = f"qqq_extended_{today_str}"
        cached = db.get_state(cache_key)
        if cached:
            return cached

        result = {
            "rsi14": 50.0, "high_52w": 0.0, "low_52w": 0.0, "fear_greed": 50.0,
            "put_call_ratio": 1.0,      # SPY aggregate OI P/C — neutral default
            "put_call_ratio_z": 0.0,
            "vix_term_slope": 0.0,      # VIX9D - VIX3M: negative = contango (calm), positive = backwardation (fear)
            "vix9d": 0.0, "vix3m": 0.0,
            "real_vix": None,           # FRED VIXCLS — actual CBOE VIX daily close
        }

        try:
            r = requests.get(
                f"{self.base_url}/rsi",
                params={"symbol": self.proxy_symbol, "interval": "1day",
                        "time_period": 14, "apikey": TWELVE_DATA_API_KEY},
                timeout=10
            ).json()
            result["rsi14"] = float(r.get("values", [{}])[0].get("rsi", 50.0))
        except Exception as e:
            logger.warning(f"RSI fetch failed: {e}")

        try:
            r = requests.get(
                f"{self.base_url}/time_series",
                params={"symbol": self.proxy_symbol, "interval": "1day",
                        "outputsize": "252", "apikey": TWELVE_DATA_API_KEY},
                timeout=12
            ).json()
            closes = [float(v["close"]) for v in r.get("values", [])]
            if closes:
                result["high_52w"] = max(closes)
                result["low_52w"] = min(closes)
        except Exception as e:
            logger.warning(f"52w high/low fetch failed: {e}")

        try:
            fg_res = requests.get(
                "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                timeout=8, headers={"User-Agent": "Mozilla/5.0"}
            ).json()
            result["fear_greed"] = float(fg_res.get("fear_and_greed", {}).get("score", 50.0))
        except Exception as e:
            logger.warning(f"CNN Fear & Greed fetch failed: {e}")

        # VIX term structure via ETF proxies (VIX9D/VIX3M unavailable at this Twelve Data tier)
        # VIXY = short-term VIX futures (~1-month); VXZ = medium-term VIX futures (~5-month)
        # VIXY/VXZ ratio > 1 = front-month fear > back-month = backwardation = sustained fear
        # VIXY/VXZ ratio < 1 = contango = near-term calm vs longer-dated uncertainty
        # Normalized slope: (VIXY/VXZ - 1) * 10 maps to roughly the same scale as VIX9D-VIX3M pts
        try:
            vix_batch = requests.get(
                f"{self.base_url}/price",
                params={"symbol": "VIXY,VXZ", "apikey": TWELVE_DATA_API_KEY},
                timeout=10
            ).json()
            vixy_p = float((vix_batch.get("VIXY") or {}).get("price", 0.0))
            vxz_p = float((vix_batch.get("VXZ") or {}).get("price", 0.0))
            if vixy_p > 0 and vxz_p > 0:
                ratio = vixy_p / vxz_p
                result["vix9d"] = vixy_p   # labeled as vix9d for downstream compat
                result["vix3m"] = vxz_p    # labeled as vix3m
                result["vix_term_slope"] = round((ratio - 1) * 10, 2)  # + = backwardation
        except Exception as e:
            logger.warning(f"VIX term structure fetch failed: {e}")

        # Real VIX from FRED VIXCLS — confirms VIXY proxy reading, removes ETF roll-cost distortion.
        # FRED updates after US close (~4:30 PM ET). Cached separately from the extended metrics
        # dict (daily) so a stale cache hit still returns the current FRED value.
        if FRED_API_KEY:
            try:
                fred_cache_key  = "fred_vix_value"
                fred_cache_date = "fred_vix_date"
                if db.get_state(fred_cache_date) == today_str:
                    cached_vix = db.get_state(fred_cache_key)
                    if cached_vix:
                        result["real_vix"] = float(cached_vix)
                else:
                    r_vix = requests.get(
                        "https://api.stlouisfed.org/fred/series/observations",
                        params={"series_id": "VIXCLS", "api_key": FRED_API_KEY,
                                "file_type": "json", "sort_order": "desc", "limit": 1},
                        timeout=12
                    ).json()
                    vix_val = float(r_vix["observations"][0]["value"])
                    if vix_val > 0:
                        result["real_vix"] = round(vix_val, 2)
                        db.update_state(fred_cache_key, vix_val)
                        db.update_state(fred_cache_date, today_str)
            except Exception as e:
                logger.warning(f"FRED VIXCLS fetch failed: {e}")

        # Put/Call ratio from SPY options chain (Tradier) — OI across TWO near-term expiries.
        # SPY structurally has high put OI (institutions hedge here, buy calls via QQQ/TQQQ),
        # so raw ratio isn't meaningful alone. We store a 30-day rolling average in DB and
        # score on the Z-score vs that baseline — a spike vs YOUR OWN history is the signal.
        try:
            from tradier_client import TradierClient
            tc = TradierClient()
            if tc.api_key:
                # Aggregate OI across two nearest expirations to smooth weekly-expiry skew
                exps = tc.get_expirations("SPY")
                today_d = datetime.utcnow().date()
                near_exps = sorted(
                    [e for e in exps if (datetime.strptime(e, "%Y-%m-%d").date() - today_d).days >= 7],
                    key=lambda e: datetime.strptime(e, "%Y-%m-%d").date()
                )[:2]
                put_oi_total = call_oi_total = 0
                for exp in near_exps:
                    chain = tc.get_options_chain("SPY", expiration=exp, greeks=False)
                    if chain:
                        put_oi_total += sum(int(c.get("open_interest") or 0) for c in chain if c.get("option_type", "").lower() == "put")
                        call_oi_total += sum(int(c.get("open_interest") or 0) for c in chain if c.get("option_type", "").lower() == "call")

                if call_oi_total > 0:
                    raw_pc = round(put_oi_total / call_oi_total, 2)
                    # Update rolling 30-day history in DB
                    pc_history = db.get_state("spy_pc_ratio_history") or []
                    pc_history.append(raw_pc)
                    if len(pc_history) > 30:
                        pc_history = pc_history[-30:]
                    db.update_state("spy_pc_ratio_history", pc_history)
                    # Z-score vs rolling baseline — this is the actual signal
                    if len(pc_history) >= 5:
                        import statistics
                        pc_mean = statistics.mean(pc_history)
                        pc_std = statistics.stdev(pc_history) if len(pc_history) > 1 else 1.0
                        pc_z = (raw_pc - pc_mean) / pc_std if pc_std > 0 else 0.0
                    else:
                        pc_z = 0.0
                    result["put_call_ratio"] = raw_pc
                    result["put_call_ratio_z"] = round(pc_z, 2)
        except Exception as e:
            logger.warning(f"P/C ratio fetch failed: {e}")

        db.update_state(cache_key, result)
        return result

    def calculate_cycle_score(self, daily, vix_z, breadth, ext):
        """
        Composite bottom/top scores (0-100) for LEAP desk gating.

        bottom_score: higher = more oversold/fearful = BTO CALL window
        top_score:    higher = more overbought/greedy = BTO PUT window

        Signal inputs (all available at Twelve Data tier, no external license needed):
          VIXY z-score, RSI14, breadth, 52w high/low drawdown, CNN F&G, SMA200 extension
        """
        spot = daily.get("spot", 0.0)
        sma200 = daily.get("sma200", spot)
        ema21 = daily.get("ema21", spot)
        rsi = ext.get("rsi14", 50.0)
        high_52w = ext.get("high_52w", spot)
        low_52w = ext.get("low_52w", spot)
        fg = ext.get("fear_greed", 50.0)
        pc_ratio = ext.get("put_call_ratio", 1.0)
        pc_z = ext.get("put_call_ratio_z", 0.0)  # z-score vs 30-day rolling baseline
        vix_term_slope = ext.get("vix_term_slope", 0.0)  # normalized VIXY/VXZ ratio slope
        vix9d = ext.get("vix9d", 0.0)
        vix3m = ext.get("vix3m", 0.0)
        adx_data = daily.get("adx_macd", {}) or {}
        macd_hist = adx_data.get("macd_hist", 0.0)

        drawdown = (spot - high_52w) / high_52w * 100 if high_52w > 0 else 0.0   # negative = below 52w high
        extension = (spot - low_52w) / low_52w * 100 if low_52w > 0 else 0.0    # positive = above 52w low
        pct_vs_sma200 = (spot - sma200) / sma200 * 100 if sma200 > 0 else 0.0
        ema21_pct = (spot - ema21) / ema21 * 100 if ema21 > 0 else 0.0

        # ── BOTTOM SCORE ──────────────────────────────────────────────────────
        b = 0
        # VIXY fear (max 30)
        if vix_z >= 3.0:    b += 30
        elif vix_z >= 2.0:  b += 22
        elif vix_z >= 1.5:  b += 15
        elif vix_z >= 0.75: b += 8

        # RSI oversold (max 25)
        if rsi < 25:   b += 25
        elif rsi < 30: b += 18
        elif rsi < 40: b += 10
        elif rsi < 45: b += 5

        # Breadth collapse (max 20)
        if breadth < 0.20:   b += 20
        elif breadth < 0.30: b += 14
        elif breadth < 0.40: b += 8
        elif breadth < 0.50: b += 4

        # Drawdown from 52w high (max 15)
        if drawdown <= -30:   b += 15
        elif drawdown <= -20: b += 11
        elif drawdown <= -10: b += 6
        elif drawdown <= -5:  b += 3

        # CNN Fear & Greed fear (max 10)
        if fg < 15:   b += 10
        elif fg < 25: b += 7
        elif fg < 35: b += 4

        # Below SMA200 (max 5)
        if pct_vs_sma200 < -5:  b += 5
        elif pct_vs_sma200 < 0: b += 3

        # Put/Call ratio Z-score vs 30-day baseline (max 15)
        # SPY P/C is structurally high (institutions hedge here); raw ratio misleads.
        # A spike ABOVE that baseline = unusual hedging surge = genuine fear = CALL signal.
        if pc_z >= 2.0:   b += 15
        elif pc_z >= 1.2: b += 10
        elif pc_z >= 0.5: b += 5

        # VIX term structure backwardation (max 12)
        # VIX9D > VIX3M (positive slope) = market pricing SUSTAINED fear, not a spike
        # This is the strongest structural confirmation that fear is real, not noise
        if vix_term_slope >= 3.0:  b += 12
        elif vix_term_slope >= 1.5: b += 8
        elif vix_term_slope >= 0.5: b += 4

        # MACD bearish (max 3) — tie-breaker, not a primary signal
        if macd_hist < 0: b += 3

        bottom_score = min(b, 100)

        # ── TOP SCORE ──────────────────────────────────────────────────────────
        t = 0
        # VIXY complacency (max 20)
        if vix_z <= -1.5:   t += 20
        elif vix_z <= -1.0: t += 14
        elif vix_z <= -0.5: t += 8
        elif vix_z <= 0.0:  t += 4

        # RSI overbought (max 25)
        if rsi > 80:   t += 25
        elif rsi > 75: t += 18
        elif rsi > 70: t += 12
        elif rsi > 65: t += 6

        # Breadth extended — nearly everything overbought (max 15)
        if breadth > 0.90:   t += 15
        elif breadth > 0.80: t += 10
        elif breadth > 0.70: t += 5

        # Extension from 52w low (max 15)
        if extension >= 60:   t += 15
        elif extension >= 40: t += 10
        elif extension >= 25: t += 5

        # CNN Fear & Greed greed (max 10)
        if fg > 85:   t += 10
        elif fg > 75: t += 7
        elif fg > 65: t += 4

        # Extended above SMA200 (max 10)
        if pct_vs_sma200 > 15:  t += 10
        elif pct_vs_sma200 > 10: t += 7
        elif pct_vs_sma200 > 5:  t += 4

        # Extended above EMA21 (max 5)
        if ema21_pct > 5:   t += 5
        elif ema21_pct > 3: t += 3

        # Put/Call ratio Z-score — below baseline = less hedging than usual = complacency (max 15)
        # A DROP in P/C vs rolling mean = unusual calm = PUT signal (top forming)
        if pc_z <= -2.0:   t += 15
        elif pc_z <= -1.2: t += 10
        elif pc_z <= -0.5: t += 5

        # VIX term structure contango depth (max 10)
        # VIX9D << VIX3M (deep negative slope) = near-term calm, market not pricing any near risk
        # Extreme contango is a complacency signal — the vol market is asleep
        if vix_term_slope <= -3.0:  t += 10
        elif vix_term_slope <= -2.0: t += 7
        elif vix_term_slope <= -1.0: t += 4

        # MACD bullish (max 3)
        if macd_hist > 0: t += 3

        top_score = min(t, 100)

        signals = {
            "rsi14": rsi, "fear_greed": fg,
            "put_call_ratio": pc_ratio, "put_call_ratio_z": pc_z,
            "vix_term_slope": vix_term_slope, "vix9d": vix9d, "vix3m": vix3m,
            "drawdown_from_high_pct": drawdown,
            "extension_from_low_pct": extension,
            "pct_vs_sma200": pct_vs_sma200,
            "ema21_pct": ema21_pct,
            "real_vix": ext.get("real_vix"),  # FRED VIXCLS — None if unavailable
        }
        # Persist scores to DB for cross-script reads (market_analysis.py morning brief)
        db.update_state("tqqq_bottom_score", bottom_score)
        db.update_state("tqqq_top_score",    top_score)

        return {"bottom_score": bottom_score, "top_score": top_score, "signals": signals}

    # =========================================================================
    # LEAP DESK METHODS
    # =========================================================================

    def evaluate_leap_entry(self, daily, intraday, vix_price, vix_z, breadth, tqqq_daily, cycle=None):
        """
        Returns leap_setup dict if conditions meet the red-day CALL entry window, else None.
        Gated by 2-hour cooldown so a continued downtrend re-fires every 2 hours.
        Entry is intentionally permissive — the sniper catches confirmation; the
        LEAP desk catches fear. False positives have defined, bounded loss.
        """
        import time as _time
        last_ts = db.get_state("tqqq_last_leap_signal_ts", 0)
        if last_ts:
            try:
                elapsed_hours = (_time.time() - float(last_ts)) / 3600
                if elapsed_hours < LEAP_COOLDOWN_HOURS:
                    logger.debug(f"LEAP cooldown active — {elapsed_hours:.1f}h since last signal ({LEAP_COOLDOWN_HOURS}h required)")
                    return None
            except Exception:
                pass

        # Cycle score gate — bottom_score must show at least moderate oversold conditions.
        # A pure red day on no fear (score < threshold) is distribution, not capitulation.
        bottom_score = cycle.get("bottom_score", 0) if cycle else 0
        if bottom_score < CYCLE_BOTTOM_THRESHOLD:
            logger.debug(f"LEAP CALL: bottom_score {bottom_score} < {CYCLE_BOTTOM_THRESHOLD} — skip (not oversold enough)")
            return None

        qqq_spot = daily["spot"]
        ema21 = daily.get("ema21", 0.0)

        intraday_spot = intraday["spot"] if intraday else qqq_spot
        intraday_chg_pct = (intraday_spot - qqq_spot) / qqq_spot * 100 if qqq_spot > 0 else 0.0

        is_red_day = intraday_chg_pct < -0.5
        is_below_ema21 = ema21 > 0 and qqq_spot < ema21
        adx_data = daily.get("adx_macd", {}) or {}
        is_macd_bear = adx_data.get("macd_hist", 0.0) < 0

        if not (is_red_day or is_below_ema21):
            logger.debug(f"LEAP CALL: no entry condition — intraday {intraday_chg_pct:+.2f}%, above EMA21")
            return None

        try:
            tqqq_spot = float(requests.get(
                f"{self.base_url}/price",
                params={"symbol": self.symbol, "apikey": TWELVE_DATA_API_KEY},
                timeout=8
            ).json().get("price", 0.0))
        except Exception:
            tqqq_spot = 0.0

        if tqqq_spot == 0.0:
            return None

        atr_pct_tqqq = calculate_atr_pct(tqqq_daily) if tqqq_daily is not None else 0.02
        sigs = cycle.get("signals", {}) if cycle else {}

        return {
            "tqqq_spot": tqqq_spot,
            "qqq_spot": qqq_spot,
            "qqq_ema21": ema21,
            "intraday_chg_pct": intraday_chg_pct,
            "is_red_day": is_red_day,
            "is_below_ema21": is_below_ema21,
            "is_macd_bear": is_macd_bear,
            "vix_price": vix_price,
            "vix_z": vix_z,
            "breadth": breadth,
            "atr_pct_tqqq": atr_pct_tqqq,
            "panic_mode": vix_z >= 2.0,
            "macro_regime": "BEAR" if qqq_spot < daily.get("sma200", qqq_spot * 2) else "BULL",
            "sma200": daily.get("sma200", 0.0),
            "bottom_score": bottom_score,
            "rsi14": sigs.get("rsi14", 50.0),
            "fear_greed": sigs.get("fear_greed", 50.0),
            "drawdown_from_high_pct": sigs.get("drawdown_from_high_pct", 0.0),
            "put_call_ratio": sigs.get("put_call_ratio", 1.0),
            "put_call_ratio_z": sigs.get("put_call_ratio_z", 0.0),
            "vix_term_slope": sigs.get("vix_term_slope", 0.0),
            "vix9d": sigs.get("vix9d", 0.0),
            "vix3m": sigs.get("vix3m", 0.0),
            "real_vix": cycle.get("signals", {}).get("real_vix") if cycle else None,
        }

    def enrich_leap_with_tradier_chain(self, leap_setup):
        """
        Find the best deep ITM TQQQ call in the 270-540 DTE window via Tradier.
        Targets delta 0.70-0.75: high intrinsic value, manageable time-decay %.
        """
        try:
            from tradier_client import TradierClient
            tc = TradierClient()
            if not tc.api_key:
                return leap_setup

            today = datetime.utcnow().date()
            exps = tc.get_expirations(self.symbol)
            tqqq_spot = leap_setup["tqqq_spot"]

            best_contract = None
            best_delta_diff = float("inf")

            for exp_str in sorted(exps):
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if dte < LEAP_DTE_MIN or dte > LEAP_DTE_MAX:
                        continue

                    chain = tc.get_options_chain(self.symbol, exp_str, greeks=True)
                    for c in chain:
                        if c.get("option_type") != "call":
                            continue
                        strike = float(c.get("strike", 0))
                        if strike >= tqqq_spot:  # only ITM calls
                            continue

                        greeks = c.get("greeks") or {}
                        delta = float(greeks.get("delta") or 0.0)
                        iv = float(greeks.get("smv_vol") or c.get("implied_volatility") or 0.0)

                        if delta <= 0 or delta > 1.0:
                            continue

                        delta_diff = abs(delta - LEAP_DELTA_TARGET)
                        if delta_diff > LEAP_DELTA_BAND or delta_diff >= best_delta_diff:
                            continue

                        bid = float(c.get("bid") or 0.0)
                        ask = float(c.get("ask") or 0.0)
                        mid = (bid + ask) / 2 if (bid + ask) > 0 else 0.0
                        if mid <= 0:
                            continue

                        best_contract = {
                            "strike": strike,
                            "expiry": exp_str,
                            "dte": dte,
                            "delta": round(delta, 2),
                            "iv": round(iv * 100 if iv < 5.0 else iv, 1),
                            "bid": round(bid, 2),
                            "ask": round(ask, 2),
                            "mid": round(mid, 2),
                            "oi": int(c.get("open_interest") or 0),
                            "volume": int(c.get("volume") or 0),
                        }
                        best_delta_diff = delta_diff
                except Exception:
                    continue

            if best_contract:
                intrinsic = max(0.0, tqqq_spot - best_contract["strike"])
                time_val = max(0.0, best_contract["mid"] - intrinsic)
                leap_setup.update({
                    "real_strike": best_contract["strike"],
                    "real_expiry": best_contract["expiry"],
                    "real_dte": best_contract["dte"],
                    "real_delta": best_contract["delta"],
                    "real_iv": best_contract["iv"],
                    "real_bid": best_contract["bid"],
                    "real_ask": best_contract["ask"],
                    "real_mid": best_contract["mid"],
                    "real_oi": best_contract["oi"],
                    "real_volume": best_contract["volume"],
                    "real_cost_per_contract": round(best_contract["mid"] * 100, 0),
                    "real_intrinsic": round(intrinsic, 2),
                    "real_time_value": round(time_val, 2),
                })
        except Exception as e:
            logger.warning(f"LEAP Tradier chain enrichment failed: {e}")
        return leap_setup

    def enrich_leap_with_greeks(self, leap_setup, tqqq_daily):
        """
        Black-Scholes Greeks for the long-dated ITM LEAP contract.
        Key metrics: theta per day (the ongoing cost of time), total theta over DTE,
        breakeven at expiry, and how much QQQ needs to move to break even.
        """
        if tqqq_daily is None or len(tqqq_daily) < 21:
            return leap_setup
        try:
            returns = tqqq_daily["close"].pct_change().dropna()
            rv20 = float(returns.tail(20).std() * np.sqrt(252))
            real_iv = fetch_tqqq_atm_iv(db)
            iv = real_iv if real_iv > 0 else estimate_iv(rv20, leap_setup.get("vix_price", 18.0))

            dte = leap_setup.get("real_dte", 365)
            T = dte / 365.0
            spot = leap_setup["tqqq_spot"]

            strike = leap_setup.get("real_strike")
            if not strike:
                strike = find_strike_for_delta(spot, T, RISK_FREE_RATE, iv, LEAP_DELTA_TARGET, "call")

            greeks = bs_greeks(spot, strike, T, RISK_FREE_RATE, iv, "call")
            theo_price = bs_price(spot, strike, T, RISK_FREE_RATE, iv, "call")

            theta_per_day = greeks["theta"] * 100          # per contract per day
            total_theta = abs(theta_per_day) * dte          # total decay cost over full DTE
            breakeven = strike + theo_price
            breakeven_pct = (breakeven / spot - 1) * 100
            # TQQQ is ~3× QQQ — QQQ breakeven is approx 1/3 of TQQQ breakeven pct
            qqq_breakeven_pct = breakeven_pct / 3.0

            leap_setup.update({
                "bs_strike": round(strike, 2),
                "bs_iv": round(iv * 100, 1),
                "bs_rv20": round(rv20 * 100, 1),
                "bs_delta": round(greeks["delta"], 3),
                "bs_theta_per_day": round(theta_per_day, 2),
                "bs_total_theta": round(total_theta, 0),
                "bs_vega": round(greeks["vega"] * 100, 2),
                "bs_prob_itm": round(greeks["prob_itm"] * 100, 1),
                "bs_theo_price": round(theo_price, 2),
                "bs_breakeven": round(breakeven, 2),
                "bs_breakeven_pct": round(breakeven_pct, 1),
                "bs_qqq_breakeven_pct": round(qqq_breakeven_pct, 1),
            })
        except Exception as e:
            logger.warning(f"LEAP Greeks calculation failed: {e}")
        return leap_setup

    def dispatch_leap_signal(self, leap_setup):
        """
        Two Discord embeds: (1) why now — red day conditions, (2) LEAP contract setup.
        Orange color (0xe67e22) distinguishes LEAP signals from sniper (green/red) and
        monitoring (yellow) so they're immediately recognizable in the channel.
        """
        color = 0xe67e22
        is_panic = leap_setup.get("panic_mode", False)

        # --- Embed 1: Context ---
        red_tag = "🔴 RED DAY" if leap_setup["is_red_day"] else ""
        ema_tag = " | BELOW EMA21" if leap_setup["is_below_ema21"] else ""
        header_tag = (red_tag + ema_tag).strip(" |") or "BEARISH SETUP"

        intraday_line = f"┣ Intraday Move: `{leap_setup['intraday_chg_pct']:+.2f}%` {red_tag}\n"
        ema_line = (
            f"┣ QQQ `${leap_setup['qqq_spot']:.2f}` BELOW EMA21 `${leap_setup['qqq_ema21']:.2f}` — bearish structure\n"
            if leap_setup["is_below_ema21"] else
            f"┣ QQQ `${leap_setup['qqq_spot']:.2f}` above EMA21 `${leap_setup['qqq_ema21']:.2f}` — pullback within uptrend\n"
        )
        macro_line = (
            f"┣ Macro: {leap_setup['macro_regime']} REGIME | SMA200 `${leap_setup['sma200']:.2f}`\n"
        )
        if is_panic:
            fear_note = "🚨 PANIC MODE — split into 2 tranches; first entry now, second on stabilization"
        elif leap_setup["vix_z"] >= 0.75:
            fear_note = "⚠️ ELEVATED FEAR — optimal LEAP window (cheaper spot + time value elevated)"
        else:
            fear_note = "🟢 CALM — red day on low fear = distribution signal, not capitulation"
        fear_line = f"┣ VIXY `{leap_setup['vix_price']:.2f}` (z `{leap_setup['vix_z']:+.2f}σ`) — {fear_note}\n"
        macd_line = (
            "┣ MACD: Bearish histogram — momentum confirms downside pressure\n"
            if leap_setup["is_macd_bear"] else
            "┣ MACD: Bullish histogram — structural pullback, not a momentum collapse\n"
        )
        breadth_line = f"┣ Breadth: `{leap_setup['breadth']:.0%}` of QQQ top-20 above SMA200\n"
        score = leap_setup.get("bottom_score", 0)
        pc = leap_setup.get("put_call_ratio", 1.0)
        pc_z_v = leap_setup.get("put_call_ratio_z", 0.0)
        vts = leap_setup.get("vix_term_slope", 0.0)
        vix9d_v = leap_setup.get("vix9d", 0.0)
        vix3m_v = leap_setup.get("vix3m", 0.0)
        term_label = ("BACKWARDATION ⚠️ — sustained fear" if vts > 1.5 else
                      "flat" if abs(vts) < 0.5 else "contango — calm structure")
        pc_label = f"z `{pc_z_v:+.1f}σ` vs 30D mean — {'SPIKE ⚠️ fear surge' if pc_z_v >= 1.2 else 'elevated' if pc_z_v >= 0.5 else 'normal'}"
        rsi_line = f"┣ RSI14: `{leap_setup.get('rsi14', 50):.1f}` | F&G: `{leap_setup.get('fear_greed', 50):.0f}/100` | Drawdown: `{leap_setup.get('drawdown_from_high_pct', 0):.1f}%` from 52w high\n"
        real_vix = leap_setup.get("real_vix")
        real_vix_str = f" | **VIX: `{real_vix:.1f}`** [FRED]" if real_vix else ""
        pc_line = f"┣ SPY P/C: `{pc:.2f}` ({pc_label}) | VIX Term: VIXY `{vix9d_v:.2f}` / VXZ `{vix3m_v:.2f}` = `{vts:+.2f}` {term_label}{real_vix_str}\n"
        score_bar = "█" * (score // 10) + "░" * (10 - score // 10)
        score_line = f"┗ Bottom Score: `{score}/100` [{score_bar}] — {'HIGH conviction' if score >= 75 else 'MODERATE conviction' if score >= 55 else 'LOW'}"

        regime_payload = (
            f"TQQQ LEAP Entry Window — {header_tag}\n"
            + intraday_line + ema_line + macro_line + fear_line + macd_line + breadth_line + rsi_line + pc_line + score_line
        )

        # --- Embed 2: Contract setup ---
        if "real_strike" in leap_setup:
            contract_line = (
                f"${leap_setup['real_strike']:.2f} CALL — "
                f"{leap_setup['real_expiry']} ({leap_setup['real_dte']} DTE / "
                f"~{leap_setup['real_dte']//30} months)"
            )
            delta_line = (
                f"┣ Delta: Δ {leap_setup['real_delta']:.2f} — "
                f"~{leap_setup['real_delta']:.0%} probability ITM at expiry\n"
            )
            cost_line = (
                f"┣ Cost: ~${leap_setup['real_cost_per_contract']:.0f}/contract "
                f"(mid ${leap_setup['real_mid']:.2f} | bid ${leap_setup['real_bid']:.2f} / ask ${leap_setup['real_ask']:.2f})\n"
                f"┣ Intrinsic: ${leap_setup['real_intrinsic']:.2f} | Time Value (your insurance premium): ${leap_setup['real_time_value']:.2f}\n"
            )
            liquidity_line = (
                f"┣ Liquidity: Volume `{leap_setup.get('real_volume', 0):,}` | OI `{leap_setup.get('real_oi', 0):,}`\n"
            )
        else:
            bs_strike_est = leap_setup.get("bs_strike", round(leap_setup["tqqq_spot"] * 0.88, 2))
            contract_line = (
                f"~${bs_strike_est:.2f} CALL — 270-540 DTE (9-18 months) | "
                "Tradier chain unavailable — verify strike manually"
            )
            delta_line = f"┣ Delta: Δ ~{LEAP_DELTA_TARGET:.2f} target (deep ITM)\n"
            cost_line = ""
            liquidity_line = ""

        bs_block = ""
        if "bs_delta" in leap_setup:
            chain_note = f" (chain mid ${leap_setup['real_mid']:.2f})" if "real_mid" in leap_setup else ""
            bs_block = (
                f"┣ Black-Scholes (IV {leap_setup['bs_iv']:.1f}% vs RV20 {leap_setup['bs_rv20']:.1f}%):\n"
                f"┃  Theo ${leap_setup['bs_theo_price']:.2f}{chain_note} | "
                f"Delta Δ {leap_setup['bs_delta']:.3f} | Prob ITM {leap_setup['bs_prob_itm']:.1f}%\n"
                f"┃  Theta: ${leap_setup['bs_theta_per_day']:+.2f}/day per contract"
                f" | Total time decay over DTE: ~${leap_setup['bs_total_theta']:.0f}\n"
                f"┃  Vega: ${leap_setup['bs_vega']:.2f} per 1% IV move\n"
            )

        breakeven_block = ""
        if "bs_breakeven" in leap_setup:
            dte_months = leap_setup.get("real_dte", 365) // 30
            breakeven_block = (
                f"┣ Breakeven at Expiry: TQQQ `${leap_setup['bs_breakeven']:.2f}` "
                f"({leap_setup['bs_breakeven_pct']:+.1f}% from now)\n"
                f"┣   QQQ needs ~{leap_setup['bs_qqq_breakeven_pct']:+.1f}% over {dte_months} months "
                f"(TQQQ magnifies ~3×)\n"
            )

        tranche_note = (
            "┣ ⚠️ HIGH FEAR: split into 2 tranches — buy 50% now, 50% if more red in next 3 days\n"
            if is_panic else ""
        )

        execution_payload = (
            f"TQQQ @ `${leap_setup['tqqq_spot']:.2f}` | Buy Time on the Pullback\n"
            f"┣ 🎯 BTO LEAP: {contract_line}\n"
            + delta_line + cost_line + liquidity_line + bs_block + breakeven_block
            + "┣ Sizing: Max 2-3% of portfolio (TQQQ 3× leveraged = leverage on leverage)\n"
            + tranche_note
            + "┣ Scale Out: 50% at +50% premium gain | Full close at +100%\n"
            "┣ Stop: −30% underlying move → reassessment alert (review thesis, not auto-close)\n"
            "┣ Roll: Alert fires at 90 DTE remaining — roll forward if still bullish\n"
            f"┗ Log entry: `python tqqq.py --log-leap --strike X --expiration YYYY-MM-DD "
            f"--premium X --entry-price {leap_setup['tqqq_spot']:.2f}`"
        )

        if WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(
                WEBHOOK_TRADE_SIGNALS,
                "TQQQ LEAP DESK | BTO CALL — Red Day Entry Window",
                regime_payload, color
            )
            send_essentials_embed(
                WEBHOOK_TRADE_SIGNALS,
                "TQQQ LEAP DESK | CONTRACT SETUP",
                execution_payload, color
            )

        import time as _time
        db.update_state("tqqq_last_leap_signal_ts", _time.time())
        logger.info(f"LEAP signal dispatched — TQQQ ${leap_setup['tqqq_spot']:.2f}, {header_tag}")

    def check_leap_position_status(self, tqqq_spot: float):
        """
        Monitors all open LEAP positions. Alert thresholds:
        - -30% underlying move → reassessment flag (review thesis; LEAP has time, not auto-cut)
        - +50% → scale out 50% of position
        - +100% → close remainder
        - 90 DTE remaining → roll forward consideration
        - Monthly check-in while holding (every 30 calendar days)
        """
        positions = db.get_state("tqqq_leap_positions")
        if not positions or not isinstance(positions, list) or tqqq_spot == 0.0:
            return

        today_str = datetime.now().strftime("%Y-%m-%d")
        today = datetime.strptime(today_str, "%Y-%m-%d")
        updated_positions = []

        for pos in positions:
            try:
                entry_price = float(pos.get("entry_tqqq_spot", tqqq_spot))
                premium = float(pos.get("premium", 0.0))
                strike = float(pos.get("strike", 0.0))
                expiry_str = pos.get("expiration", "")
                entry_date_str = pos.get("entry_date", today_str)

                pnl_proxy = (tqqq_spot - entry_price) / entry_price * 100 if entry_price > 0 else 0.0

                dte_remaining = 999
                try:
                    exp_date = datetime.strptime(expiry_str, "%Y-%m-%d")
                    dte_remaining = max(0, (exp_date - today).days)
                except Exception:
                    pass

                if dte_remaining == 0:
                    logger.info(f"LEAP expired — removing: ${strike} exp {expiry_str}")
                    continue

                days_held = 0
                try:
                    days_held = (today - datetime.strptime(entry_date_str, "%Y-%m-%d")).days
                except Exception:
                    pass

                pos_id = f"{entry_date_str}_{strike}"
                alert_key = f"tqqq_leap_alert_{pos_id}"
                last_alert = db.get_state(alert_key, "")

                def _fire(title, body, color_val, tag):
                    db.update_state(alert_key, f"{tag}_{today_str}")
                    if WEBHOOK_TRADE_SIGNALS:
                        send_essentials_embed(WEBHOOK_TRADE_SIGNALS, title, body, color_val)

                base_line = (
                    f"┣ Entry: `${entry_price:.2f}` → Now: `${tqqq_spot:.2f}` ({pnl_proxy:+.1f}% underlying move)\n"
                    f"┣ Strike `${strike:.2f}` | Exp `{expiry_str}` ({dte_remaining} DTE remaining) | Held: {days_held}d\n"
                )

                if pnl_proxy <= LEAP_CUT_THRESHOLD and f"REASSESS_{today_str}" not in last_alert:
                    _fire(
                        "🔴 TQQQ LEAP | REASSESS FLAG",
                        base_line
                        + "┣ ⚠️ -30% threshold hit — review thesis before acting\n"
                        f"┗ {dte_remaining//30} months of runway remain. TQQQ recoveries are typical over that horizon.",
                        0xe74c3c, "REASSESS"
                    )
                elif pnl_proxy >= LEAP_TP2_PCT and f"TP2_{today_str}" not in last_alert:
                    _fire(
                        "🎯 TQQQ LEAP | FULL TARGET HIT",
                        base_line + "┗ +100% target reached — close full position or trail with tight stop.",
                        0x2ecc71, "TP2"
                    )
                elif pnl_proxy >= LEAP_TP1_PCT and f"TP1_{today_str}" not in last_alert:
                    _fire(
                        "✅ TQQQ LEAP | FIRST TARGET HIT",
                        base_line + "┗ Scale out 50% of position — hold remainder for the full double.",
                        0x2ecc71, "TP1"
                    )
                elif dte_remaining <= LEAP_ROLL_DTE and f"ROLL_{today_str}" not in last_alert:
                    _fire(
                        "⚠️ TQQQ LEAP | ROLL CONSIDERATION",
                        base_line + "┗ 90 DTE remaining — consider selling and buying next 9-18 month expiry.",
                        0xf39c12, "ROLL"
                    )
                else:
                    # Monthly check-in
                    monthly_key = f"tqqq_leap_monthly_{pos_id}"
                    last_monthly = db.get_state(monthly_key, "")
                    if not last_monthly:
                        days_since = 31
                    else:
                        try:
                            days_since = (today - datetime.strptime(last_monthly, "%Y-%m-%d")).days
                        except Exception:
                            days_since = 31
                    if days_since >= 30:
                        db.update_state(monthly_key, today_str)
                        color_val = 0x2ecc71 if pnl_proxy >= 0 else 0xe74c3c
                        if WEBHOOK_TRADE_SIGNALS:
                            send_essentials_embed(
                                WEBHOOK_TRADE_SIGNALS,
                                f"🕐 TQQQ LEAP | MONTH {days_held//30} CHECK-IN",
                                base_line
                                + f"┣ Original premium: ${premium:.2f}/share | "
                                f"Cost basis: ${premium * 100:.0f}/contract\n"
                                + f"┗ Thesis intact — holding time. Next alert: {LEAP_ROLL_DTE} DTE or profit target.",
                                color_val
                            )

                updated_positions.append(pos)
            except Exception as e:
                logger.warning(f"LEAP monitor failed for position {pos}: {e}")
                updated_positions.append(pos)

        if len(updated_positions) != len(positions):
            db.update_state("tqqq_leap_positions", updated_positions)

    # =========================================================================
    # LEAP PUT DESK — top-hunting, BTO deep ITM QQQ puts on green/overbought days
    # =========================================================================

    def evaluate_leap_put_entry(self, daily, intraday, vix_price, vix_z, breadth, cycle):
        """
        PUT desk: fires on green days / extended overbought conditions when top_score is high.
        Uses QQQ puts (not TQQQ) — TQQQ puts have brutal theta decay and thin OI long-dated.
        Thesis: buying TIME for the inevitable correction. 6-12 month DTE absorbs timing risk.
        """
        import time as _time
        last_ts = db.get_state("tqqq_last_leap_put_signal_ts", 0)
        if last_ts:
            try:
                elapsed_hours = (_time.time() - float(last_ts)) / 3600
                if elapsed_hours < LEAP_PUT_COOLDOWN_HOURS:
                    logger.debug(f"LEAP PUT cooldown — {elapsed_hours:.1f}h since last signal")
                    return None
            except Exception:
                pass

        top_score = cycle.get("top_score", 0) if cycle else 0
        if top_score < CYCLE_TOP_THRESHOLD:
            logger.debug(f"LEAP PUT: top_score {top_score} < {CYCLE_TOP_THRESHOLD} — skip")
            return None

        qqq_spot = daily["spot"]
        ema21 = daily.get("ema21", 0.0)
        intraday_spot = intraday["spot"] if intraday else qqq_spot
        intraday_chg_pct = (intraday_spot - qqq_spot) / qqq_spot * 100 if qqq_spot > 0 else 0.0

        ema21_pct = (qqq_spot - ema21) / ema21 * 100 if ema21 > 0 else 0.0
        is_green_day = intraday_chg_pct > 0.5
        is_extended_above_ema21 = ema21 > 0 and ema21_pct > 2.0

        if not (is_green_day or is_extended_above_ema21):
            logger.debug(f"LEAP PUT: no entry condition — {intraday_chg_pct:+.2f}% intraday, {ema21_pct:+.1f}% above EMA21")
            return None

        adx_data = daily.get("adx_macd", {}) or {}
        is_macd_bull = adx_data.get("macd_hist", 0.0) > 0
        sigs = cycle.get("signals", {}) if cycle else {}

        return {
            "qqq_spot": qqq_spot,
            "qqq_ema21": ema21,
            "ema21_pct": ema21_pct,
            "intraday_chg_pct": intraday_chg_pct,
            "is_green_day": is_green_day,
            "is_extended_above_ema21": is_extended_above_ema21,
            "is_macd_bull": is_macd_bull,
            "vix_price": vix_price,
            "vix_z": vix_z,
            "breadth": breadth,
            "sma200": daily.get("sma200", 0.0),
            "macro_regime": "BULL" if qqq_spot > daily.get("sma200", 0.0) else "BEAR",
            "top_score": top_score,
            "rsi14": sigs.get("rsi14", 50.0),
            "fear_greed": sigs.get("fear_greed", 50.0),
            "extension_from_low_pct": sigs.get("extension_from_low_pct", 0.0),
            "pct_vs_sma200": sigs.get("pct_vs_sma200", 0.0),
            "complacency_mode": vix_z <= -1.0,
            "put_call_ratio": sigs.get("put_call_ratio", 1.0),
            "put_call_ratio_z": sigs.get("put_call_ratio_z", 0.0),
            "vix_term_slope": sigs.get("vix_term_slope", 0.0),
            "vix9d": sigs.get("vix9d", 0.0),
            "vix3m": sigs.get("vix3m", 0.0),
        }

    def enrich_leap_put_with_tradier_chain(self, put_setup):
        """
        Find the best deep ITM QQQ put in the 180-365 DTE window via Tradier.
        Targets delta -0.72: high intrinsic, meaningful time value, liquid OI.
        """
        try:
            from tradier_client import TradierClient
            tc = TradierClient()
            if not tc.api_key:
                return put_setup

            today = datetime.utcnow().date()
            expirations = tc.get_expirations(LEAP_PUT_SYMBOL)
            if not expirations:
                return put_setup

            best_contract = None
            best_delta_diff = float("inf")

            for exp_str in expirations:
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                    if dte < LEAP_PUT_DTE_MIN or dte > LEAP_PUT_DTE_MAX:
                        continue

                    chain = tc.get_options_chain(LEAP_PUT_SYMBOL, exp_str, greeks=True)
                    if not chain:
                        continue

                    qqq_spot = put_setup["qqq_spot"]
                    for c in chain:
                        if c.get("option_type", "").lower() != "put":
                            continue
                        strike = float(c.get("strike", 0.0))
                        if strike <= 0 or strike >= qqq_spot:
                            continue  # deep ITM puts have strike below spot; skip OTM
                        greeks = c.get("greeks") or {}
                        delta = float(greeks.get("delta", 0.0))
                        if delta >= 0 or abs(delta) < 0.40:
                            continue
                        iv = float(greeks.get("mid_iv") or greeks.get("smv_vol") or 0.0)
                        bid = float(c.get("bid") or 0.0)
                        ask = float(c.get("ask") or 0.0)
                        mid = (bid + ask) / 2 if bid and ask else 0.0
                        if mid <= 0 or (ask - bid) / mid > 0.15:
                            continue  # skip wide markets

                        delta_diff = abs(abs(delta) - abs(LEAP_PUT_DELTA_TARGET))
                        if delta_diff > LEAP_PUT_DELTA_BAND or delta_diff >= best_delta_diff:
                            continue

                        best_contract = {
                            "strike": strike,
                            "expiry": exp_str,
                            "dte": dte,
                            "delta": round(delta, 2),
                            "iv": round(iv * 100 if iv < 5.0 else iv, 1),
                            "bid": round(bid, 2),
                            "ask": round(ask, 2),
                            "mid": round(mid, 2),
                            "oi": int(c.get("open_interest") or 0),
                            "volume": int(c.get("volume") or 0),
                        }
                        best_delta_diff = delta_diff
                except Exception:
                    continue

            if best_contract:
                qqq_spot = put_setup["qqq_spot"]
                intrinsic = max(0.0, best_contract["strike"] - qqq_spot)
                time_val = max(0.0, best_contract["mid"] - intrinsic)
                put_setup.update({
                    "real_strike": best_contract["strike"],
                    "real_expiry": best_contract["expiry"],
                    "real_dte": best_contract["dte"],
                    "real_delta": best_contract["delta"],
                    "real_iv": best_contract["iv"],
                    "real_bid": best_contract["bid"],
                    "real_ask": best_contract["ask"],
                    "real_mid": best_contract["mid"],
                    "real_oi": best_contract["oi"],
                    "real_volume": best_contract["volume"],
                    "real_cost_per_contract": round(best_contract["mid"] * 100, 0),
                    "real_intrinsic": round(intrinsic, 2),
                    "real_time_value": round(time_val, 2),
                })
        except Exception as e:
            logger.warning(f"LEAP PUT Tradier chain lookup failed: {e}")

        return put_setup

    def dispatch_leap_put_signal(self, put_setup):
        """
        Two Discord embeds for PUT desk: (1) top context, (2) QQQ put contract setup.
        Blue (0x3498db) — distinct from CALL desk (orange) and sniper (green/red).
        """
        color = 0x3498db
        is_complacent = put_setup.get("complacency_mode", False)

        green_tag = "🟢 GREEN DAY" if put_setup["is_green_day"] else ""
        ema_tag = " | EXTENDED ABOVE EMA21" if put_setup["is_extended_above_ema21"] else ""
        header_tag = (green_tag + ema_tag).strip(" |") or "OVERBOUGHT SETUP"

        intraday_line = f"┣ Intraday Move: `{put_setup['intraday_chg_pct']:+.2f}%` {green_tag}\n"
        ema_line = (
            f"┣ QQQ `${put_setup['qqq_spot']:.2f}` extended `{put_setup['ema21_pct']:+.1f}%` above EMA21 `${put_setup['qqq_ema21']:.2f}` — stretched\n"
            if put_setup["is_extended_above_ema21"] else
            f"┣ QQQ `${put_setup['qqq_spot']:.2f}` near EMA21 `${put_setup['qqq_ema21']:.2f}` — green day on support\n"
        )
        macro_line = f"┣ Macro: {put_setup['macro_regime']} REGIME | SMA200 `${put_setup['sma200']:.2f}` ({put_setup['pct_vs_sma200']:+.1f}%)\n"

        if is_complacent:
            fear_note = "🟢 DEEP COMPLACENCY — optimal PUT window (cheap vol, elevated risk of surprise reversal)"
        elif put_setup["vix_z"] <= 0.0:
            fear_note = "🟡 LOW FEAR — market calm, structural extended"
        else:
            fear_note = "⚠️ ELEVATED VIXY — unusual for a top setup; verify context"
        fear_line = f"┣ VIXY `{put_setup['vix_price']:.2f}` (z `{put_setup['vix_z']:+.2f}σ`) — {fear_note}\n"

        macd_line = (
            "┣ MACD: Bullish histogram — momentum extended, reversal risk elevated\n"
            if put_setup["is_macd_bull"] else
            "┣ MACD: Histogram turning — momentum weakening while price elevated\n"
        )
        breadth_line = f"┣ Breadth: `{put_setup['breadth']:.0%}` of QQQ top-20 above SMA200\n"
        rsi_line = f"┣ RSI14: `{put_setup.get('rsi14', 50):.1f}` | F&G: `{put_setup.get('fear_greed', 50):.0f}/100` | Extension from 52w low: `+{put_setup.get('extension_from_low_pct', 0):.1f}%`\n"
        pc2 = put_setup.get("put_call_ratio", 1.0)
        pc_z_v2 = put_setup.get("put_call_ratio_z", 0.0)
        vts2 = put_setup.get("vix_term_slope", 0.0)
        vix9d_v2 = put_setup.get("vix9d", 0.0)
        vix3m_v2 = put_setup.get("vix3m", 0.0)
        term_label2 = ("deep contango 🟢 — extreme complacency" if vts2 <= -2.0 else
                       "contango — calm" if vts2 < -0.5 else "flat/backwardation — unusual for top setup")
        pc_label2 = f"z `{pc_z_v2:+.1f}σ` vs 30D mean — {'DROP ⚠️ complacency surge' if pc_z_v2 <= -1.2 else 'low' if pc_z_v2 <= -0.5 else 'normal'}"
        pc_line2 = f"┣ SPY P/C: `{pc2:.2f}` ({pc_label2}) | VIX Term: VIXY `{vix9d_v2:.2f}` / VXZ `{vix3m_v2:.2f}` = `{vts2:+.2f}` {term_label2}\n"

        score = put_setup.get("top_score", 0)
        score_bar = "█" * (score // 10) + "░" * (10 - score // 10)
        score_line = f"┗ Top Score: `{score}/100` [{score_bar}] — {'HIGH conviction' if score >= 75 else 'MODERATE conviction' if score >= 55 else 'LOW'}"

        context_payload = (
            f"QQQ LEAP PUT Entry Window — {header_tag}\n"
            + intraday_line + ema_line + macro_line + fear_line + macd_line + breadth_line + rsi_line + pc_line2 + score_line
        )

        # Contract setup
        if "real_strike" in put_setup:
            contract_line = (
                f"${put_setup['real_strike']:.2f} PUT — "
                f"{put_setup['real_expiry']} ({put_setup['real_dte']} DTE / "
                f"~{put_setup['real_dte']//30} months)"
            )
            delta_line = f"┣ Delta: Δ {put_setup['real_delta']:.2f} — `{abs(put_setup['real_delta']):.0%}` probability ITM at expiry\n"
            cost_line = (
                f"┣ Cost: ~${put_setup['real_cost_per_contract']:.0f}/contract "
                f"(mid ${put_setup['real_mid']:.2f} | bid ${put_setup['real_bid']:.2f} / ask ${put_setup['real_ask']:.2f})\n"
                f"┣ Intrinsic: ${put_setup['real_intrinsic']:.2f} | Time Value (your insurance premium): ${put_setup['real_time_value']:.2f}\n"
            )
            liquidity_line = f"┣ Liquidity: Volume `{put_setup.get('real_volume', 0):,}` | OI `{put_setup.get('real_oi', 0):,}`\n"
        else:
            bs_strike_est = round(put_setup["qqq_spot"] * 1.08, 2)
            contract_line = (
                f"~${bs_strike_est:.2f} PUT — 180-365 DTE (6-12 months) | "
                "Tradier chain unavailable — verify strike manually"
            )
            delta_line = f"┣ Delta: Δ ~{LEAP_PUT_DELTA_TARGET:.2f} target (deep ITM put)\n"
            cost_line = ""
            liquidity_line = ""

        tranche_note = (
            "┣ 🟢 LOW FEAR (complacency peak): split 2 tranches — 50% now, 50% if market pushes higher next week\n"
            if is_complacent else ""
        )

        execution_payload = (
            f"QQQ @ `${put_setup['qqq_spot']:.2f}` | Buy Time on the Extension\n"
            f"┣ 🎯 BTO LEAP PUT: {contract_line}\n"
            + delta_line + cost_line + liquidity_line
            + "┣ Sizing: Max 1-2% of portfolio (defined risk = premium paid)\n"
            + tranche_note
            + "┣ Scale Out: 50% at +50% premium gain | Full close at +100%\n"
            "┣ Stop: −30% underlying move (QQQ continues UP) → reassessment (review thesis)\n"
            "┣ Roll: Alert fires at 90 DTE remaining — roll forward if thesis intact\n"
            f"┗ Log entry: `python tqqq.py --log-leap-put --strike X --expiration YYYY-MM-DD "
            f"--premium X --entry-price {put_setup['qqq_spot']:.2f}`"
        )

        if WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(
                WEBHOOK_TRADE_SIGNALS,
                "TQQQ LEAP DESK | BTO PUT — Green Day / Top Hunting",
                context_payload, color
            )
            send_essentials_embed(
                WEBHOOK_TRADE_SIGNALS,
                "TQQQ LEAP DESK | QQQ PUT CONTRACT SETUP",
                execution_payload, color
            )

        import time as _time
        db.update_state("tqqq_last_leap_put_signal_ts", _time.time())
        logger.info(f"LEAP PUT signal dispatched — QQQ ${put_setup['qqq_spot']:.2f}, {header_tag}")

    def check_leap_put_position_status(self, qqq_spot: float):
        """Monitors open LEAP put positions. Same thresholds as CALL desk, inverted direction."""
        positions = db.get_state("tqqq_leap_put_positions") or []
        if not positions or qqq_spot <= 0:
            return

        today_str = datetime.now().strftime("%Y-%m-%d")
        color_val = 0x3498db
        updated_positions = []

        for pos in positions:
            try:
                entry_price = float(pos.get("entry_price", 0.0))
                strike = float(pos.get("strike", 0.0))
                premium = float(pos.get("premium", 0.0))
                expiry_str = pos.get("expiration", "")
                last_alert = pos.get("last_alert", "")

                if not expiry_str or premium <= 0:
                    updated_positions.append(pos)
                    continue

                exp_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
                dte_remaining = (exp_date - datetime.utcnow().date()).days

                if dte_remaining <= 0:
                    logger.info(f"LEAP PUT {strike}P expired — removing from tracker")
                    continue

                # Intrinsic P&L proxy: for a put, intrinsic = max(0, strike - qqq_spot)
                intrinsic_now = max(0.0, strike - qqq_spot)
                pnl_proxy = (intrinsic_now - premium) / premium * 100 if premium > 0 else 0.0

                # Monthly check-in
                last_monthly = pos.get("last_monthly_check", "")
                days_since_monthly = (datetime.utcnow().date() - datetime.strptime(last_monthly, "%Y-%m-%d").date()).days if last_monthly else 999
                if days_since_monthly >= 30:
                    base_line = (
                        f"┣ Position: QQQ ${strike:.2f} PUT exp {expiry_str} ({dte_remaining} DTE)\n"
                        f"┣ QQQ Spot: ${qqq_spot:.2f} | Put P&L proxy: `{pnl_proxy:+.1f}%`\n"
                    )
                    send_essentials_embed(
                        WEBHOOK_TRADE_SIGNALS,
                        "LEAP PUT DESK | Monthly Check-In",
                        base_line
                        + f"┣ Premium paid: ${premium:.2f}/share | Cost basis: ${premium * 100:.0f}/contract\n"
                        + f"┗ Thesis: QQQ correction to drive PUT into profit. Next alert: {LEAP_ROLL_DTE} DTE or profit target.",
                        color_val
                    )
                    pos["last_monthly_check"] = today_str

                if pnl_proxy <= LEAP_CUT_THRESHOLD and f"REASSESS_{today_str}" not in last_alert:
                    send_essentials_embed(
                        WEBHOOK_TRADE_SIGNALS, "LEAP PUT DESK | ⚠️ Reassessment Flag",
                        f"┣ QQQ PUT ${strike:.2f} exp {expiry_str}\n"
                        f"┣ QQQ moved HIGHER — PUT P&L proxy: `{pnl_proxy:+.1f}%` (threshold: {LEAP_CUT_THRESHOLD}%)\n"
                        "┣ QQQ extended further than expected. Review top thesis.\n"
                        "┗ Options: (1) Hold — still have time. (2) Roll up strike if QQQ keeps pushing. (3) Close.",
                        0xe74c3c
                    )
                    pos["last_alert"] = f"REASSESS_{today_str}"

                elif pnl_proxy >= LEAP_TP2_PCT and f"TP2_{today_str}" not in last_alert:
                    send_essentials_embed(
                        WEBHOOK_TRADE_SIGNALS, "LEAP PUT DESK | 🎯 TP2 — Close Remainder",
                        f"┣ QQQ PUT ${strike:.2f} exp {expiry_str}\n"
                        f"┗ P&L proxy `+{pnl_proxy:.1f}%` — close full position. Target achieved.",
                        color_val
                    )
                    pos["last_alert"] = f"TP2_{today_str}"

                elif pnl_proxy >= LEAP_TP1_PCT and f"TP1_{today_str}" not in last_alert:
                    send_essentials_embed(
                        WEBHOOK_TRADE_SIGNALS, "LEAP PUT DESK | 📊 TP1 — Scale 50% Out",
                        f"┣ QQQ PUT ${strike:.2f} exp {expiry_str}\n"
                        f"┗ P&L proxy `+{pnl_proxy:.1f}%` — sell half, let remainder ride.",
                        color_val
                    )
                    pos["last_alert"] = f"TP1_{today_str}"

                elif dte_remaining <= LEAP_ROLL_DTE and f"ROLL_{today_str}" not in last_alert:
                    send_essentials_embed(
                        WEBHOOK_TRADE_SIGNALS, "LEAP PUT DESK | 🔄 Roll Consideration",
                        f"┣ QQQ PUT ${strike:.2f} exp {expiry_str} — `{dte_remaining} DTE` remaining\n"
                        f"┣ P&L proxy: `{pnl_proxy:+.1f}%`\n"
                        "┗ Approach 90 DTE: roll forward if thesis intact, or close if target hit.",
                        color_val
                    )
                    pos["last_alert"] = f"ROLL_{today_str}"

                updated_positions.append(pos)
            except Exception as e:
                logger.warning(f"LEAP PUT monitor failed for position {pos}: {e}")
                updated_positions.append(pos)

        if len(updated_positions) != len(positions):
            db.update_state("tqqq_leap_put_positions", updated_positions)

    def execute_sniper_sweep(self):
        if not is_market_hours():
            logger.info("Market closed — TQQQ sniper standing down.")
            return

        daily = self.fetch_daily_baseline()
        intraday = self.fetch_intraday_metrics()
        if not daily or not intraday:
            return

        daily["adx_macd"] = self.fetch_adx_macd()  # injected into evaluate_snipe via daily dict

        tqqq_daily = self.fetch_tqqq_daily_series()
        vix_price, vix_z = self.fetch_vix()
        breadth = self.fetch_breadth()
        atr_pct_tqqq = calculate_atr_pct(tqqq_daily) if tqqq_daily is not None else 0.02

        # Exits take priority over new entries every sweep.
        self.check_open_position_for_exit(intraday, atr_pct_tqqq)

        # Wave status check-in (3-day cadence, only when position is open).
        tqqq_spot_now = float(tqqq_daily["close"].iloc[-1]) if tqqq_daily is not None and not tqqq_daily.empty else 0.0
        self.check_wave_position_status(tqqq_spot_now)

        # Regime flip detection (silent except on actual BULL/BEAR crossing).
        self.check_regime_flip(daily.get("spot", 0.0), daily.get("sma200", 0.0))

        # Defensive sweep: a position that's gone 25+ days without a natural exit trigger (e.g. a
        # script restart lost the open-position state's exit watch) shouldn't sit ungraded forever —
        # max realistic DTE here is ~21-30 days, so anything older is force-graded and cleared.
        try:
            from analytics import HighFidelityAnalyticsEngine
            HighFidelityAnalyticsEngine().sweep_and_grade_pending("tqqq", min_age_days=25)
        except Exception as e:
            logger.error(f"TQQQ defensive ledger sweep failed: {e}")

        # Market-wide regime vital sign, independent of whether a trade setup exists.
        self.dispatch_regime_vital_sign(daily, breadth, vix_price, vix_z)

        # Insurance put renewal clock — runs every sweep, fully independent of the sniper signal.
        self.check_insurance_put_renewal()

        # Cycle position score — shared by both CALL and PUT desks.
        # RSI, 52w high/low, CNN F&G fetched once and reused for both directions.
        ext_metrics = self.fetch_qqq_extended_metrics()
        cycle = self.calculate_cycle_score(daily, vix_z, breadth, ext_metrics)
        logger.debug(f"Cycle score — bottom: {cycle['bottom_score']}, top: {cycle['top_score']}")

        # LEAP CALL desk: BTO deep ITM TQQQ calls on red days / oversold conditions.
        # Gated by bottom_score >= CYCLE_BOTTOM_THRESHOLD so low-fear red days don't fire.
        leap_setup = self.evaluate_leap_entry(daily, intraday, vix_price, vix_z, breadth, tqqq_daily, cycle)
        if leap_setup:
            leap_setup = self.enrich_leap_with_tradier_chain(leap_setup)
            leap_setup = self.enrich_leap_with_greeks(leap_setup, tqqq_daily)
            self.dispatch_leap_signal(leap_setup)

        # LEAP PUT desk: BTO deep ITM QQQ puts on green days / overbought conditions.
        # Gated by top_score >= CYCLE_TOP_THRESHOLD so low-conviction green days don't fire.
        put_setup = self.evaluate_leap_put_entry(daily, intraday, vix_price, vix_z, breadth, cycle)
        if put_setup:
            put_setup = self.enrich_leap_put_with_tradier_chain(put_setup)
            self.dispatch_leap_put_signal(put_setup)

        # Monitor any open LEAP positions (monthly check-ins, TP/stop/roll alerts).
        self.check_leap_position_status(tqqq_spot_now)
        qqq_spot_now = daily.get("spot", 0.0)
        self.check_leap_put_position_status(qqq_spot_now)

        setup = self.evaluate_snipe(daily, intraday, vix_price, vix_z, breadth, atr_pct_tqqq)

        # Position guard: never stack a second entry while already in a trade — we're riding
        # the wave, not layering risk. Silently stand down (no channel noise while in position).
        if setup and setup["action"] != "MONITORING SETUP" and db.get_state("tqqq_open_position"):
            logger.info(f"Position already open — standing down, riding existing trade")
            return

        if setup:
            # Fire once per distinct setup — dispatch only when direction or action level changes.
            # Identical setup persisting across sweeps means we're already in or watching it;
            # re-broadcasting the same contract every 5 minutes is noise, not signal.
            # State encodes direction + whether it's a live entry or monitoring-only:
            #   "BTO_CALL", "BTO_PUT", "MON_CALL", "MON_PUT"
            # A MONITORING → BTO upgrade on the same contract IS a new state and fires.
            # Position close clears this key so the next entry fires cleanly.
            action_tag = "BTO" if setup["action"] != "MONITORING SETUP" else "MON"
            setup_state = f"{action_tag}_{setup['contract']}"
            last_state = db.get_state("tqqq_last_dispatched_state")
            if setup_state == last_state:
                logger.info(f"Setup unchanged ({setup_state}) — already dispatched, standing by for regime change")
                return

            db.update_state("tqqq_last_dispatched_state", setup_state)
            if action_tag == "BTO":
                db.update_state("tqqq_last_live_signal_date", datetime.now().strftime("%Y-%m-%d"))
            self.dispatch_intelligence(setup, tqqq_daily)
        else:
            self.dispatch_market_outlook(daily, intraday, vix_price, vix_z, breadth, atr_pct_tqqq, tqqq_daily)


if __name__ == "__main__":
    if "--test" in sys.argv:
        # Force one full sweep regardless of market hours or dedup state.
        # Clears the last-dispatched-state so the signal always fires.
        logger.info("🧪 TEST MODE — forcing one sniper sweep")
        db.update_state("tqqq_last_dispatched_state", None)
        sniper = TQQQTacticalSniper()
        daily = sniper.fetch_daily_baseline()
        intraday = sniper.fetch_intraday_metrics()
        tqqq_daily = sniper.fetch_tqqq_daily_series()
        vix_price, vix_z = sniper.fetch_vix()
        breadth = sniper.fetch_breadth()
        atr_pct_tqqq = calculate_atr_pct(tqqq_daily) if tqqq_daily is not None else 0.02
        setup = sniper.evaluate_snipe(daily, intraday, vix_price, vix_z, breadth, atr_pct_tqqq) if daily and intraday else None
        if setup:
            sniper.dispatch_intelligence(setup, tqqq_daily)
        else:
            sniper.dispatch_market_outlook(daily, intraday, vix_price, vix_z, breadth, atr_pct_tqqq, tqqq_daily)
        sys.exit(0)

    if "--test-leap" in sys.argv:
        logger.info("🧪 TEST MODE — forcing LEAP CALL desk evaluation")
        db.update_state("tqqq_last_leap_signal_ts", 0)
        sniper = TQQQTacticalSniper()
        daily = sniper.fetch_daily_baseline()
        intraday = sniper.fetch_intraday_metrics()
        tqqq_daily = sniper.fetch_tqqq_daily_series()
        vix_price, vix_z = sniper.fetch_vix()
        breadth = sniper.fetch_breadth()
        if daily and intraday:
            daily["adx_macd"] = sniper.fetch_adx_macd()
            ext_metrics = sniper.fetch_qqq_extended_metrics()
            cycle = sniper.calculate_cycle_score(daily, vix_z, breadth, ext_metrics)
            logger.info(f"Cycle scores — bottom: {cycle['bottom_score']}, top: {cycle['top_score']}")
            leap_setup = sniper.evaluate_leap_entry(daily, intraday, vix_price, vix_z, breadth, tqqq_daily, cycle)
            if leap_setup:
                leap_setup = sniper.enrich_leap_with_tradier_chain(leap_setup)
                leap_setup = sniper.enrich_leap_with_greeks(leap_setup, tqqq_daily)
                sniper.dispatch_leap_signal(leap_setup)
                logger.info("LEAP CALL test dispatch complete.")
            else:
                logger.info(f"LEAP CALL: no entry conditions met (bottom_score={cycle['bottom_score']}).")
        sys.exit(0)

    if "--test-leap-put" in sys.argv:
        logger.info("🧪 TEST MODE — forcing LEAP PUT desk evaluation")
        db.update_state("tqqq_last_leap_put_signal_ts", 0)
        sniper = TQQQTacticalSniper()
        daily = sniper.fetch_daily_baseline()
        intraday = sniper.fetch_intraday_metrics()
        vix_price, vix_z = sniper.fetch_vix()
        breadth = sniper.fetch_breadth()
        if daily and intraday:
            daily["adx_macd"] = sniper.fetch_adx_macd()
            ext_metrics = sniper.fetch_qqq_extended_metrics()
            cycle = sniper.calculate_cycle_score(daily, vix_z, breadth, ext_metrics)
            logger.info(f"Cycle scores — bottom: {cycle['bottom_score']}, top: {cycle['top_score']}")
            put_setup = sniper.evaluate_leap_put_entry(daily, intraday, vix_price, vix_z, breadth, cycle)
            if put_setup:
                put_setup = sniper.enrich_leap_put_with_tradier_chain(put_setup)
                sniper.dispatch_leap_put_signal(put_setup)
                logger.info("LEAP PUT test dispatch complete.")
            else:
                logger.info(f"LEAP PUT: no entry conditions met (top_score={cycle['top_score']}).")
        sys.exit(0)

    if "--log-leap" in sys.argv:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--log-leap", action="store_true")
        parser.add_argument("--strike", type=float, required=True)
        parser.add_argument("--expiration", type=str, required=True, help="YYYY-MM-DD")
        parser.add_argument("--premium", type=float, required=True, help="Premium paid per share")
        parser.add_argument("--entry-price", type=float, required=True, help="TQQQ spot price at entry")
        args = parser.parse_args()
        positions = db.get_state("tqqq_leap_positions") or []
        new_pos = {
            "strike": args.strike,
            "expiration": args.expiration,
            "premium": args.premium,
            "entry_tqqq_spot": args.entry_price,
            "entry_date": datetime.now().strftime("%Y-%m-%d"),
        }
        positions.append(new_pos)
        db.update_state("tqqq_leap_positions", positions)
        logger.info(
            f"LEAP logged: ${args.strike} CALL exp {args.expiration} "
            f"premium ${args.premium:.2f}/share | TQQQ @ ${args.entry_price:.2f}"
        )
        sys.exit(0)

    if "--log-put" in sys.argv:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--log-put", action="store_true")
        parser.add_argument("--strike", type=float, required=True)
        parser.add_argument("--expiration", type=str, required=True, help="YYYY-MM-DD")
        parser.add_argument("--premium", type=float, required=True)
        args = parser.parse_args()
        db.update_state("tqqq_insurance_put", {
            "strike": args.strike, "expiration": args.expiration, "premium": args.premium
        })
        logger.info(f"Insurance put logged: ${args.strike} exp {args.expiration} premium ${args.premium:.2f}")
        sys.exit(0)

    if "--log-leap-put" in sys.argv:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--log-leap-put", action="store_true")
        parser.add_argument("--strike", type=float, required=True)
        parser.add_argument("--expiration", type=str, required=True, help="YYYY-MM-DD")
        parser.add_argument("--premium", type=float, required=True, help="Premium paid per share")
        parser.add_argument("--entry-price", type=float, required=True, help="QQQ spot price at entry")
        args = parser.parse_args()
        positions = db.get_state("tqqq_leap_put_positions") or []
        positions.append({
            "strike": args.strike,
            "expiration": args.expiration,
            "premium": args.premium,
            "entry_qqq_spot": args.entry_price,
            "entry_date": datetime.now().strftime("%Y-%m-%d"),
        })
        db.update_state("tqqq_leap_put_positions", positions)
        logger.info(
            f"LEAP PUT logged: ${args.strike} PUT exp {args.expiration} "
            f"premium ${args.premium:.2f}/share | QQQ @ ${args.entry_price:.2f}"
        )
        sys.exit(0)

    logger.info("Initializing TQQQ Tactical Sniper Daemon...")

    # WS removed: shared_ws.py created a new connection per process restart (module-level
    # singleton doesn't survive across PythonAnywhere restarts), causing concurrent
    # connection storms. REST polling every sweep covers all price data needed.

    sniper = TQQQTacticalSniper()
    while True:
        try:
            sniper.execute_sniper_sweep()
        except Exception as e:
            logger.error(f"Daemon error: {e}")
        # TQQQ doesn't trade off-hours — extend sleep to 30min outside RTH.
        # RTH: 13:30–20:00 UTC (use 13:00–21:00 for buffer). Off-hours: 1800s.
        now_utc_h = datetime.now(timezone.utc).hour
        sleep_secs = 900 if 13 <= now_utc_h < 21 else 1800
        time.sleep(sleep_secs)

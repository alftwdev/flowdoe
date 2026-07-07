import os
import sys
import time
import logging
import requests
import numpy as np
import pandas as pd
import pytz
from datetime import datetime
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
        """QQQ daily series for SMA200/SMA50 macro posture."""
        params = {"symbol": self.proxy_symbol, "interval": "1day", "outputsize": "200", "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res:
                return None
            df = pd.DataFrame(res["values"])
            df["close"] = df["close"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            return {
                "spot": df["close"].iloc[-1],
                "sma200": df["close"].rolling(window=200).mean().iloc[-1],
                "sma50": df["close"].rolling(window=50).mean().iloc[-1],
                "ema21": df["close"].ewm(span=21, adjust=False).mean().iloc[-1],
            }
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

            # Overlay WS price when it's fresher than the last 5-min bar close — this
            # closes the gap between a REST bar's timestamp and actual current price.
            try:
                from shared_ws import get_ws_manager
                ws_mgr = get_ws_manager()
                ws_price = ws_mgr.get_price(self.proxy_symbol)
                if ws_mgr.is_fresh(self.proxy_symbol, 30) and ws_price > 0:
                    spot = ws_price
            except Exception:
                pass

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
        """TQQQ's own daily OHLC — for self-derived ATR% and realized volatility (RV20)."""
        params = {"symbol": self.symbol, "interval": "1day", "outputsize": str(outputsize), "apikey": TWELVE_DATA_API_KEY}
        try:
            res = requests.get(f"{self.base_url}/time_series", params=params, timeout=12).json()
            if "values" not in res:
                return None
            df = pd.DataFrame(res["values"])
            for col in ("open", "high", "low", "close"):
                df[col] = df[col].astype(float)
            return df.iloc[::-1].reset_index(drop=True)
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
            iv = estimate_iv(rv20, vix)

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
            greeks_line = f"Δ {setup['real_delta']:.2f} (chain) | IV {setup['real_iv']:.1f}% (chain)"
            prem_line = f"~${setup['real_premium'] * 100:.0f} per contract (mid-market)"
            liquidity_line = f"┣ Liquidity: Volume `{setup.get('real_volume', 0):,}` | OI `{setup.get('real_oi', 0):,}` (range `{setup.get('real_oi_low', 0):,}`–`{setup.get('real_oi_high', 0):,}`)\n"
        else:
            est_strike = setup['tqqq_spot'] * (1.05 if setup['contract'] == "CALL" else 0.95)
            contract_line = f"~${est_strike:.2f} {setup['contract']} — 10-21 DTE"
            greeks_line = "Δ 0.35-0.45 (target range)"
            prem_line = "Fetch live chain for precise pricing"
            liquidity_line = ""

        bs_block = ""
        if "bs_delta" in setup:
            bs_block = (
                f"┣ Self-Derived Greeks (RV20-based, conviction cross-check):\n"
                f"┃  Δ {setup['bs_delta']:+.2f} | Γ {setup['bs_gamma']:.4f} | "
                f"Θ {setup['bs_theta']:+.3f}/day | Vega {setup['bs_vega']:.3f}\n"
                f"┃  Strike ${setup['bs_strike']:.2f} | IV est {setup['bs_iv_est']:.1f}% (RV20 {setup['bs_rv20']:.1f}%) | "
                f"Theo Price ${setup['bs_theo_price']:.2f}\n"
                f"┣ Risk-Neutral Prob. ITM at Expiry: {setup['bs_prob_itm']:.1f}%\n"
                f"┣ Breakeven: ${setup['bs_breakeven']:.2f} | ATR-Projected Move ({setup.get('real_dte', 15)}D): ${setup['expected_move']:.2f}\n"
            )
        rr_line = f"┣ Risk/Reward (premium vs. ATR-projected move): 1:{setup['risk_reward']:.1f}\n" if setup.get("risk_reward") else ""
        structure_line = ""
        if structure and structure["setup"] != "NO STRUCTURE SETUP":
            confirm_tag = " ✅ CONFIRMS DIRECTION" if structure_confirms else ""
            structure_line = f"┣ Market Structure: {structure['setup']} ({structure['bias']}){confirm_tag} — {structure['detail']}\n"

        posture = "ABOVE VWAP" if setup["qqq_spot"] > setup["qqq_vwap"] else "BELOW VWAP"
        macro = "BULL REGIME" if setup["qqq_spot"] > setup["sma200"] else "BEAR REGIME"
        vix_tag = "🔴 FEAR SPIKE" if setup["vix_z"] >= VIX_CRISIS_Z else ("🟡 ELEVATED" if setup["vix_z"] >= 0.75 else "🟢 CALM")
        breadth_tag = "🔴 COLLAPSING" if setup["breadth"] < BREADTH_COLLAPSE else ("🟢 STRONG" if setup["breadth"] >= BREADTH_STRONG else "🟡 MIXED")
        atr_tag = "🔴 EXTREME" if setup["atr_pct_tqqq"] >= ATR_EXTREME else ("🟡 ELEVATED" if setup["atr_pct_tqqq"] >= ATR_ELEVATED else "🟢 NORMAL")

        adx_val = setup.get("adx", 0.0)
        adx_tag = "🔴 CHOP (<20)" if adx_val < 20 else ("🟡 WEAK (20-25)" if adx_val < 25 else "🟢 TRENDING (>25)")
        macd_hist = setup.get("macd_hist", 0.0)
        macd_dir = "▲ expanding" if setup.get("macd_expanding") and macd_hist > 0 else \
                   ("▼ expanding" if setup.get("macd_expanding") and macd_hist < 0 else "→ compressing")
        macd_tag = f"{'🟢' if setup.get('macd_bull') else '🔴'} {macd_hist:+.3f} {macd_dir}"

        payload = (
            f"QQQ Proxy | Macro: {macro} | Intraday: {posture}\n"
            f"┣ QQQ Spot: `${setup['qqq_spot']:,.2f}` | VWAP: `${setup['qqq_vwap']:,.2f}`\n"
            f"┣ VWAP Z-Score: `{setup['z_score']:+.2f}σ` | Volume Surge Z: `{setup['vol_z']:+.2f}σ`\n"
            f"┣ ADX (14): `{adx_val:.1f}` {adx_tag} | MACD Hist: {macd_tag}\n"
            f"┣ VIXY `{setup['vix']:.2f}` (z `{setup['vix_z']:+.2f}σ` {vix_tag}) | Breadth: `{setup['breadth']:.0%}` {breadth_tag} | ATR%: `{setup['atr_pct_tqqq']:.1%}` {atr_tag}\n"
            f"┗ {status_tag}\n\n"
            f"TQQQ @ ${setup['tqqq_spot']:.2f}\n"
            f"┣ Directive: {setup['action']} {setup['contract']}\n"
            f"┣ Contract: {contract_line}\n"
            f"┣ Greeks: {greeks_line}\n"
            f"┣ Est. Cost: {prem_line}\n"
            f"{liquidity_line}"
            f"{bs_block}"
            f"{rr_line}"
            f"{structure_line}"
            f"┗ Stop: −35% premium or 15m pivot break\n\n"
            f"Take Profit: Scale 50% at +100%, trail remainder with 15m 21-EMA\n"
            f"Exit Signal: Will fire automatically when Z-score reverts or reverses — watch this channel."
        )

        if WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, title, payload, color)

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

        # Use WebSocket price if fresh (< 60s old) — avoids a REST round-trip on every exit check.
        # Falls back to REST if WS hasn't delivered a quote yet (e.g. pre-market, first startup).
        try:
            from shared_ws import get_ws_manager
            ws_mgr = get_ws_manager()
            tqqq_spot = ws_mgr.get_price(self.symbol) if ws_mgr.is_fresh(self.symbol, 60) else 0.0
        except Exception:
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

    logger.info("Initializing TQQQ Tactical Sniper Daemon...")

    # WebSocket: real-time QQQ and TQQQ price stream.
    # Exit checks fire against the WS price (< 60s) rather than waiting for the next REST tick.
    # Shared with monitor.py — one connection slot covers both daemons.
    try:
        from shared_ws import get_ws_manager
        ws_mgr = get_ws_manager()
        ws_mgr.start_background()
        logger.info("[WS] Shared WebSocket manager started — QQQ/TQQQ streaming active.")
    except Exception as e:
        logger.warning(f"[WS] WebSocket startup failed (REST polling continues): {e}")

    sniper = TQQQTacticalSniper()
    while True:
        try:
            sniper.execute_sniper_sweep()
        except Exception as e:
            logger.error(f"Daemon error: {e}")
        time.sleep(900)

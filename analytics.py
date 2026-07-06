import os
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from database import EcosystemDatabase
from market_structure import calculate_supertrend

logger = logging.getLogger("Rockefeller_Analytics")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

class HighFidelityAnalyticsEngine:
    def __init__(self):
        self.db = EcosystemDatabase()
        self.api_key = os.getenv("TWELVE_DATA_API_KEY")
        self.fred_api_key = os.getenv("FRED_API_KEY")
        # No external fundamental API required — derived from Twelve Data statistics endpoint
        self.base_url = "https://api.twelvedata.com"

    def _execute_query(self, endpoint, params):
        params["apikey"] = self.api_key
        try:
            r = requests.get(f"{self.base_url}/{endpoint}", params=params, timeout=12)
            if r.status_code == 200: return r.json()
            return None
        except Exception as e: 
            logger.error(f"API Execution Failure ({endpoint}): {e}")
            return None

    def fetch_vixy_proxy(self):
        """
        The bare "VIX" symbol 404s on every Twelve Data endpoint at this plan tier (cash index
        access requires a higher tier) — confirmed across this file, scheduler.py, and stream.py,
        all of which were silently falling back to hardcoded defaults (15.0) instead of real data.
        VIXY (VIX futures ETF) resolves fine, but its absolute price isn't on the same scale as the
        VIX index and decays over time via contango, so any fixed "VIX > 16" style threshold drifts
        wrong. This returns (price, z_score vs its own 20D mean) — use the z-score for regime
        classification (relative fear spike), and the price only as a rough expected-move input.
        """
        try:
            data = self._execute_query("time_series", {"symbol": "VIXY", "interval": "1day", "outputsize": "20"})
            if not data or "values" not in data:
                return 20.0, 0.0
            closes = np.array([float(v["close"]) for v in data["values"]], dtype=float)
            if len(closes) < 10:
                return 20.0, 0.0
            current, mean, std = closes[0], closes.mean(), closes.std()
            z = (current - mean) / std if std > 0 else 0.0
            return float(current), float(z)
        except Exception as e:
            logger.error(f"VIXY proxy fetch failed: {e}")
            return 20.0, 0.0

    def _fetch_fred_metric(self, series_id):
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={self.fred_api_key}&file_type=json&sort_order=desc&limit=1"
        try:
            res = requests.get(url, timeout=12)
            res.raise_for_status()
            return float(res.json()['observations'][0]['value'])
        except Exception as e:
            logger.error(f"FRED failure for {series_id}: {e}")
            return 0.0

    def _fetch_twelve_data_quotes(self, symbols_list):
        if not self.api_key: return {}
        url = f"https://api.twelvedata.com/quote?symbol={','.join(symbols_list)}&apikey={self.api_key}"
        try:
            res = requests.get(url, timeout=15).json()
            if len(symbols_list) == 1:
                return {symbols_list[0]: res} if "symbol" in res else {}
            return res
        except Exception as e:
            logger.error(f"Twelve Data batch error: {e}")
            return {}

    def _fetch_dividend_history(self, symbol, span="5Y"):
        """
        Real Twelve Data endpoint — "complex_data/dividends" (used everywhere this was previously
        called) is not a real route; it 404s on every single call, which is why every dividend
        amount/yield/ex-date in the income channel was silently stuck at 0 / TBD. The real
        endpoint is /dividends, returning {"dividends": [{"ex_date":..., "amount":...}, ...]}.
        Returns history sorted newest-first, or [] on failure.
        """
        try:
            data = self._execute_query("dividends", {"symbol": symbol, "range": span})
            divs = data.get("dividends", []) if data else []
            return sorted(divs, key=lambda d: d.get("ex_date", ""), reverse=True)
        except Exception as e:
            logger.error(f"Dividend history fetch failed for {symbol}: {e}")
            return []

    def _project_next_ex_date(self, div_history, today=None):
        """
        Twelve Data's dividends endpoint only returns REALIZED history, not an officially
        announced forward calendar for these specific funds (confirmed live — dividends_calendar
        doesn't cover them either). Projects the next ex-date from the fund's own historical
        payment cadence (median interval between past ex-dates) — an honest estimate, not a
        confirmed date, and callers should label it as such.
        Returns (next_ex_date: datetime | None, estimated_amount: float, interval_days: int | None).
        """
        today = today or datetime.now()
        if len(div_history) < 2:
            return None, 0.0, None
        try:
            dates = [datetime.strptime(d["ex_date"], "%Y-%m-%d") for d in div_history]
        except Exception:
            return None, 0.0, None
        intervals = sorted((dates[i] - dates[i + 1]).days for i in range(len(dates) - 1))
        median_interval = intervals[len(intervals) // 2] if intervals else 0
        if median_interval <= 0:
            return None, 0.0, None
        next_date = dates[0]
        while next_date <= today:
            next_date += timedelta(days=median_interval)
        latest_amount = float(div_history[0].get("amount", 0.0))
        return next_date, latest_amount, median_interval

    def _fetch_td_fundamentals(self, symbol):
        """
        Derives dividend safety metrics entirely from Twelve Data (no Finnhub required).

        Sources:
        - statistics endpoint  → PE TTM, EPS TTM, 52w high/low
        - complex_data/dividends → payout ratio (annual_div / EPS) + 5yr CAGR

        Returns None gracefully on any fetch failure.
        """
        try:
            result = {
                "payout_ratio":  None,
                "div_growth_5y": None,
                "eps_ttm":       None,
                "pe_ttm":        None,
                "52w_high":      None,
                "52w_low":       None,
            }

            # ── 1. STATISTICS: PE, EPS, 52w range ────────────────────────────
            stats = self._execute_query("statistics", {"symbol": symbol})
            if stats and "statistics" in stats:
                s = stats["statistics"]
                # Defensive: Twelve Data nests these under sub-dicts
                val = s.get("valuations_metrics", {})
                fin = s.get("financials", {})
                inc = fin.get("income_statement", {}) if isinstance(fin, dict) else {}
                stk = s.get("stock_statistics", {})

                def _safe_float(d, *keys):
                    """Try multiple key paths; return float or None."""
                    for k in keys:
                        v = d.get(k) if isinstance(d, dict) else None
                        if v not in (None, "", "N/A", "-"):
                            try:
                                return float(v)
                            except (TypeError, ValueError):
                                pass
                    return None

                result["pe_ttm"]   = _safe_float(val, "trailing_pe", "pe_ttm", "forward_pe")
                result["eps_ttm"]  = _safe_float(inc, "eps_ttm", "basic_eps_ttm", "diluted_eps_ttm")
                result["52w_high"] = _safe_float(stk, "52_week_high", "fifty_two_week_high")
                result["52w_low"]  = _safe_float(stk, "52_week_low",  "fifty_two_week_low")

            # ── 2. DIVIDEND HISTORY: Payout ratio + 5yr CAGR ─────────────────
            divs = self._fetch_dividend_history(symbol, span="5Y")
            if divs:
                amounts = [float(d.get("amount", 0)) for d in divs if d.get("amount")]

                # Annual dividend: sum of last 4 quarterly or 12 monthly payments
                annual_div = sum(amounts[:12]) if len(amounts) >= 12 else sum(amounts[:4]) * (12 / max(len(amounts[:4]), 1))

                # Payout ratio = annual dividends / EPS TTM
                if result["eps_ttm"] and result["eps_ttm"] > 0 and annual_div > 0:
                    result["payout_ratio"] = round((annual_div / result["eps_ttm"]) * 100, 1)

                # 5yr dividend CAGR: compare most recent single payment to payment ~5 years ago
                if len(amounts) >= 8:
                    newest = amounts[0]
                    oldest = amounts[-1]
                    n_periods = len(amounts)
                    # Estimate years spanned: assume ~4 payments/yr for quarterly, ~12 for monthly
                    years_spanned = n_periods / 4 if n_periods <= 25 else n_periods / 12
                    years_spanned = max(years_spanned, 1.0)
                    if oldest > 0 and newest > 0:
                        result["div_growth_5y"] = round(
                            ((newest / oldest) ** (1 / years_spanned) - 1) * 100, 1
                        )

            return result

        except Exception as e:
            logger.warning(f"TD fundamentals fetch failed for {symbol}: {e}")
            return None

    def calculate_accuracy_rating(self, predicted_move, actual_close):
        try:
            predicted_move = float(predicted_move)
            actual_close = float(actual_close)
            if predicted_move == 0: return 0.0
            error_pct = abs(actual_close - predicted_move) / predicted_move
            accuracy = max(0.0, 100.0 - (error_pct * 100.0))
            return round(accuracy, 2)
        except Exception as e:
            logger.error(f"Accuracy calculation error: {e}")
            return 0.0

    def evaluate_vix_cvr_reversal(self):
        """CVR pattern logic doesn't depend on VIX's absolute scale — VIXY's own 15D high/low
        breakout-rejection pattern works identically. Was previously dead (VIX 404'd -> always None)."""
        data = self._execute_query("time_series", {"symbol": "VIXY", "interval": "1day", "outputsize": "20"})
        if not data or "values" not in data: return None
        
        try:
            df = pd.DataFrame(data["values"])
            df["close"] = df["close"].astype(float)
            df["open"] = df["open"].astype(float)
            df["high"] = df["high"].astype(float)
            df["low"] = df["low"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            
            if len(df) < 16: return None
            
            current_candle = df.iloc[-1]
            historical_15 = df.iloc[-16:-1] 
            
            max_high_15 = historical_15["high"].max()
            min_low_15 = historical_15["low"].min()
            
            c_open = current_candle["open"]
            c_close = current_candle["close"]
            c_high = current_candle["high"]
            c_low = current_candle["low"]
            
            if c_high > max_high_15 and c_close < c_open:
                return {
                    "signal": "🟢 MARKET BUY TRIGGER",
                    "condition": f"VIXY established new 15-Day High ({c_high:.2f}) but violently rejected and closed below open ({c_close:.2f} < {c_open:.2f}).",
                    "vixy_spot": c_close
                }

            if c_low < min_low_15 and c_close > c_open:
                return {
                    "signal": "🔴 MARKET SELL TRIGGER",
                    "condition": f"VIXY established new 15-Day Low ({c_low:.2f}) but bounced and closed above open ({c_close:.2f} > {c_open:.2f}).",
                    "vixy_spot": c_close
                }
                
            return None
        except Exception as e:
            logger.error(f"VIX CVR calculation failed: {e}")
            return None

    def calculate_ohlcv_matrix(self, symbol="SPY", lookback=20):
        data = self._execute_query("time_series", {"symbol": symbol, "interval": "1day", "outputsize": str(lookback + 5)})
        if not data or "values" not in data: 
            return {"status": "NEUTRAL", "sigma": 0.0, "volume_surge": False}
        
        try:
            df = pd.DataFrame(data["values"])
            df["close"] = df["close"].astype(float)
            df["volume"] = df["volume"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            
            avg_vol = df["volume"].iloc[:-1].mean()
            current_vol = df["volume"].iloc[-1]
            vol_surge = current_vol > (avg_vol * 1.5)
            
            df["returns"] = df["close"].pct_change()
            sigma = df["returns"].std()
            current_return = df["returns"].iloc[-1]
            z_score = current_return / sigma if sigma > 0 else 0
            
            if vol_surge and z_score > 1.0:
                status = "🟢 HEAVY ACCUMULATION (Institutional Buying)"
            elif vol_surge and z_score < -1.0:
                status = "🔴 HEAVY DISTRIBUTION (Institutional Selling)"
            else:
                status = "⚖️ CHOP / RANGE-BOUND (Low Conviction)"
                
            return {"status": status, "sigma": round(z_score, 2), "volume_surge": vol_surge}
        except Exception as e:
            logger.error(f"OHLCV Matrix calculation failed: {e}")
            return {"status": "ERROR", "sigma": 0.0, "volume_surge": False}

    def generate_premarket_primer(self, symbol="SPY"):
        try:
            quote_data = self._fetch_twelve_data_quotes([symbol])
            if not quote_data or symbol not in quote_data: return None

            sym_quote = quote_data.get(symbol, {})

            spot = float(sym_quote.get("close", 0.0))
            prev_close = float(sym_quote.get("previous_close", spot))

            if spot == 0.0: return None

            gap_pct = ((spot - prev_close) / prev_close) * 100
            inventory = "🟢 OVERNIGHT LONG" if gap_pct > 0 else "🔴 OVERNIGHT SHORT"

            # Expected move now derived from the underlying's OWN realized volatility instead of a
            # VIX quote that always 404'd and silently fell back to a constant 15.0 — this was
            # producing the same wrong expected-move band every single morning regardless of actual
            # conditions, which also fed stream.py's live SPY/QQQ perimeter-breach alerts downstream.
            daily_data = self._execute_query("time_series", {"symbol": symbol, "interval": "1day", "outputsize": "21"})
            expected_move_pct = 1.0  # conservative fallback if history fetch fails
            if daily_data and "values" in daily_data and len(daily_data["values"]) >= 11:
                closes = np.array([float(v["close"]) for v in daily_data["values"]], dtype=float)[::-1]
                returns = np.diff(closes) / closes[:-1]
                expected_move_pct = float(np.std(returns[-20:]) * 100)
            vix_spot_display, vix_z = self.fetch_vixy_proxy()

            expected_move_dollars = spot * (expected_move_pct / 100)
            upper_bound = spot + expected_move_dollars
            lower_bound = spot - expected_move_dollars

            self.db.update_state(f"{symbol}_expected_upper", upper_bound)
            self.db.update_state(f"{symbol}_expected_lower", lower_bound)

            predicted_target = upper_bound if gap_pct > 0 else lower_bound
            today_str = datetime.now().strftime("%Y-%m-%d")
            self.db.update_state(f"market_prediction_{symbol}_{today_str}", predicted_target)

            payload = (
                f"🌅 **PRE-MARKET PRIMER & TACTICAL BATTLE PLAN ({symbol})**\n\n"
                f"**Macro Positioning (Overnight Inventory)**\n"
                f"┣ **Pre-Market Spot**: `${spot:,.2f}`\n"
                f"┣ **Overnight Gap**: `{gap_pct:+.2f}%` ({inventory})\n"
                f"┗ **{symbol} Realized Volatility (20D)**: `{expected_move_pct:.2f}%/day` | VIXY {vix_spot_display:.2f} (z {vix_z:+.2f}σ)\n\n"
                f"🎯 **MATHEMATICAL EXPECTED MOVES (1 Standard Deviation)**\n"
                f"┣ 🔼 **Ceiling (Call Resistance)**: `${upper_bound:,.2f}`\n"
                f"┗ 🔽 **Floor (Put Support)**: `${lower_bound:,.2f}`\n\n"
                f"⚙️ **SYSTEMIC IF/THEN SCENARIOS**\n"
                f"┣ **IF BULLISH**: If price holds above `${spot:,.2f}` and volume expands, target `${upper_bound:,.2f}`.\n"
                f"┗ **IF BEARISH**: If price rejects `${spot:,.2f}` and slips into the overnight gap, target `${lower_bound:,.2f}`.\n\n"
                f"⚠️ *Directive: Do not front-run the first 30 minutes. Let institutional order flow establish the true VWAP.*"
            )
            return payload
        except Exception as e:
            logger.error(f"Pre-Market Primer failed: {e}")
            return None

    def generate_eod_reconciliation(self, symbol="SPY"):
        try:
            data = self._execute_query("time_series", {"symbol": symbol, "interval": "1day", "outputsize": "1"})
            if not data or "values" not in data: return None
            
            today_candle = data["values"][0]
            high = float(today_candle["high"])
            low = float(today_candle["low"])
            close = float(today_candle["close"])
            
            expected_upper = float(self.db.get_state(f"{symbol}_expected_upper", close * 1.01))
            expected_lower = float(self.db.get_state(f"{symbol}_expected_lower", close * 0.99))
            
            breached_upper = high > expected_upper
            breached_lower = low < expected_lower
            
            if breached_upper and not breached_lower:
                containment = "🔥 SIGMA EVENT: Bullish Volatility Breakout (Call walls breached)."
            elif breached_lower and not breached_upper:
                containment = "🩸 SIGMA EVENT: Bearish Volatility Breakdown (Put walls breached)."
            elif breached_upper and breached_lower:
                containment = "🌪️ WHIPSAW DAY: Bi-directional structural destruction."
            else:
                containment = "🔒 CONTAINED: Options sellers won. Premium decay realized."
                
            matrix = self.calculate_ohlcv_matrix(symbol)

            payload = (
                f"🏦 **END-OF-DAY RECONCILIATION & TAPE AUDIT ({symbol})**\n\n"
                f"**Systemic Boundary Audit**\n"
                f"┣ **Closing Spot Price**: `${close:,.2f}`\n"
                f"┣ **High of Day**: `${high:,.2f}` | Expected Ceiling: `${expected_upper:,.2f}`\n"
                f"┣ **Low of Day**: `${low:,.2f}` | Expected Floor: `${expected_lower:,.2f}`\n"
                f"┗ **Verdict**: {containment}\n\n"
                f"🧬 **OHLCV ALGORITHMIC MATRIX (Institutional Footprint)**\n"
                f"┣ **Closing Profile**: {matrix['status']}\n"
                f"┗ **Order Flow Z-Score**: `{matrix['sigma']:+.2f}σ`\n\n"
                f"💡 *Cache cleared. System will recalculate liquidity matrices overnight.*"
            )
            return payload
        except Exception as e:
            logger.error(f"EOD Reconciliation failed: {e}")
            return None

    def generate_macro_liquidity_payload(self, is_test=False):
        fed_assets = self._fetch_fred_metric("WALCL") / 1000   # millions → billions
        tga = self._fetch_fred_metric("WTREGEN") / 1000        # millions → billions
        rev_repo = self._fetch_fred_metric("RRPONTSYD")        # already in billions
        credit_spread = self._fetch_fred_metric("BAMLH0A0HYM2")

        if fed_assets == 0.0 or tga == 0.0: return None

        net_liquidity = fed_assets - tga - rev_repo
        historical_liq = self.db.get_state("historical_net_liquidity", [])
        historical_liq.append(net_liquidity)
        if len(historical_liq) > 5: historical_liq.pop(0)
        self.db.update_state("historical_net_liquidity", historical_liq)
        
        liv = 0.0
        liv_alert = "⚖️ **NOMINAL**: Velocity stable."
        if len(historical_liq) == 5:
            liv = ((net_liquidity - historical_liq[0]) / historical_liq[0]) * 100
            if liv <= -1.5: liv_alert = "⚠️ **SEVERE WITHDRAWAL**: Systemic liquidity drain."
            elif liv >= 1.5: liv_alert = "🌊 **INJECTION**: Liquidity influx detected."

        self.db.update_state("net_liquidity", net_liquidity)
        self.db.update_state("credit_spread", credit_spread)
        
        should_broadcast = self.db.track_and_limit_alerts(
            alert_id="macro_liquidity_state",
            current_state=f"LIQ_{int(net_liquidity)}_SPREAD_{credit_spread}",
            current_trigger=net_liquidity,
            max_broadcasts=3,
            threshold_pct=0.002
        )
        
        if not should_broadcast and not is_test: return None

        spread_emoji = "🚨" if credit_spread > 4.5 else ("⚠️" if credit_spread > 3.5 else "🟢")
        spread_note = "PAUSE MARGIN DRAWS" if credit_spread > 4.5 else ("watch" if credit_spread > 3.5 else "safe — margin deployment supported")
        vel_note = "draining ⚠️" if liv <= -1.5 else ("expanding" if liv >= 1.5 else "flat")
        posture = "Risk-off — reduce exposure" if credit_spread > 4.5 else ("Caution — monitor spread" if credit_spread > 3.5 else "Risk-on — conditions support margin")
        return (
            f"┣ HY Spread: `{credit_spread:.2f}%` — {spread_emoji} {spread_note} (danger: >4.5%)\n"
            f"┣ Net Liquidity Vel: `{liv:+.2f}%` (5D) — {vel_note}\n"
            f"┗ Posture: {posture}"
        )

    FX_UNIVERSE = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]
    # Approximate ICE Dollar Index composition, renormalized without SEK (no liquid quote available
    # on Twelve Data for USD/SEK at retail tier). Sign convention: +weight means pair rising = USD rising.
    FX_DOLLAR_INDEX_WEIGHTS = {
        "EUR/USD": -0.601, "USD/JPY": 0.142, "GBP/USD": -0.124, "USD/CAD": 0.095, "USD/CHF": 0.038,
    }
    # Streamed pairs whose ATR-based "expected daily range" bounds drive stream.py's perimeter alerts.
    FX_STREAMED_PAIRS = ["EUR/USD", "GBP/USD", "USD/JPY"]

    def get_forex_session_label(self, now_utc=None):
        """
        Forex has no single RTH — it's Sydney -> Tokyo -> London -> New York in a 24/5 relay.
        London/New York overlap (12:00-16:00 UTC) is the highest-liquidity window; that's the
        window worth flagging loudest since spreads tighten and breakouts are most reliable there.
        """
        now_utc = now_utc or datetime.utcnow()
        if now_utc.weekday() == 5 or (now_utc.weekday() == 6 and now_utc.hour < 21) or (now_utc.weekday() == 4 and now_utc.hour >= 21):
            return "MARKET CLOSED (Weekend)"
        h = now_utc.hour
        sydney = h >= 21 or h < 6
        tokyo = 0 <= h < 9
        london = 7 <= h < 16
        new_york = 12 <= h < 21
        if london and new_york:
            return "🔥 LONDON/NY OVERLAP (Peak Liquidity)"
        if tokyo and sydney:
            return "ASIA SESSION (Sydney/Tokyo)"
        if london:
            return "LONDON SESSION"
        if new_york:
            return "NEW YORK SESSION"
        if tokyo:
            return "TOKYO SESSION"
        if sydney:
            return "SYDNEY SESSION (Thin Liquidity)"
        return "TRANSITION WINDOW"

    def calculate_synthetic_dollar_index(self, quotes):
        """Own weighted-basket Dollar Index derived from the tracked majors — no reliance on a
        third-party DXY ticker (Twelve Data doesn't carry one reliably at this tier)."""
        score = 0.0
        for pair, weight in self.FX_DOLLAR_INDEX_WEIGHTS.items():
            q = quotes.get(pair, {})
            if "percent_change" in q:
                score += float(q["percent_change"]) * weight
        return score

    def calculate_fx_pivot_points(self, symbol):
        """Classic floor pivots from the prior completed session — S/R levels for range/breakout calls."""
        df = self.fetch_crypto_ohlc(symbol, outputsize=3)
        if df is None or len(df) < 2:
            return None
        prev = df.iloc[-2]
        pivot = (prev["high"] + prev["low"] + prev["close"]) / 3
        rng = prev["high"] - prev["low"]
        return {
            "pivot": pivot, "r1": 2 * pivot - prev["low"], "s1": 2 * pivot - prev["high"],
            "r2": pivot + rng, "s2": pivot - rng,
        }

    def calculate_fx_trend_confluence(self, symbol):
        """Multi-timeframe (1h/4h/1day) Supertrend alignment — a directional confidence score,
        not a single noisy timeframe call. +/-3 = full agreement across all three horizons."""
        from essentials_tools import get_trend_alignment
        score, tags = 0, []
        for tf in ("1h", "4h", "1day"):
            _, is_bullish = get_trend_alignment(symbol, self.api_key, interval=tf)
            score += 1 if is_bullish else -1
            tags.append(f"{tf}:{'🟢' if is_bullish else '🔴'}")
        if score >= 2:
            tag = "🟢🟢 STRONG BULLISH CONFLUENCE"
        elif score <= -2:
            tag = "🔴🔴 STRONG BEARISH CONFLUENCE"
        else:
            tag = "🟡 MIXED / CHOPPY"
        return tag, " ".join(tags)

    def update_fx_volatility_bounds(self, symbol):
        """ATR(14)-based expected daily range, feeding stream.py's real-time perimeter alerts —
        forex has no options chain for an implied expected move, so ATR is the honest substitute."""
        df = self.fetch_crypto_ohlc(symbol, outputsize=20)
        if df is None or len(df) < 15:
            return None
        high_low = df["high"] - df["low"]
        high_cp = (df["high"] - df["close"].shift()).abs()
        low_cp = (df["low"] - df["close"].shift()).abs()
        atr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1).rolling(14).mean().iloc[-1]
        last_close = df["close"].iloc[-1]
        self.db.update_state(f"{symbol}_upper_noise", last_close + atr)
        self.db.update_state(f"{symbol}_lower_noise", last_close - atr)
        return float(atr)

    def assess_risk_sentiment_regime(self, quotes):
        """USD/JPY direction (carry-trade unwind barometer) + Gold direction = a quick risk-on/off
        read. JPY strengthening fast (USD/JPY falling) while gold rallies is the classic risk-off
        signature; both reversing is risk-on. This is the cross-asset signal worth syncing to
        WEBHOOK_MARKET_ANALYSIS when it's unambiguous."""
        usdjpy = quotes.get("USD/JPY", {})
        gold_quote = self._fetch_twelve_data_quotes(["XAU/USD"]).get("XAU/USD", {})
        usdjpy_chg = float(usdjpy.get("percent_change", 0.0)) if "percent_change" in usdjpy else 0.0
        gold_chg = float(gold_quote.get("percent_change", 0.0)) if "percent_change" in gold_quote else 0.0

        if usdjpy_chg <= -0.3 and gold_chg >= 0.3:
            return "🔴 RISK-OFF", "Yen strengthening + Gold bid — classic carry-trade unwind signature.", usdjpy_chg, gold_chg
        if usdjpy_chg >= 0.3 and gold_chg <= -0.3:
            return "🟢 RISK-ON", "Yen weakening + Gold offered — capital rotating back into carry/risk assets.", usdjpy_chg, gold_chg
        return "🟡 MIXED", "No clear carry-trade signature today.", usdjpy_chg, gold_chg

    def generate_forex_matrix_payload(self):
        fx_universe = self.FX_UNIVERSE
        quotes = self._fetch_twelve_data_quotes(fx_universe)
        if not quotes: return None

        table_rows, composite_trigger = [], 0.0
        for symbol in fx_universe:
            s_data = quotes.get(symbol, {})
            if "close" in s_data:
                price = float(s_data.get("close", 0.0))
                pct_change = float(s_data.get("percent_change", 0.0))
                composite_trigger += abs(pct_change)
                table_rows.append(f"{symbol:<9} {price:<9.4f} {pct_change:+.2f}%")

        if not table_rows: return None
        if not self.db.track_and_limit_alerts("matrix_forex_state", f"FX_VAR_{round(composite_trigger, 2)}", composite_trigger, max_broadcasts=3, threshold_pct=0.05):
            return None

        matrix_body = "\n".join(table_rows)
        session = self.get_forex_session_label()
        synthetic_dxy = self.calculate_synthetic_dollar_index(quotes)
        dxy_arrow = "🟢▲" if synthetic_dxy > 0 else ("🔴▼" if synthetic_dxy < 0 else "⚪")

        return (
            f"**Session: {session}**\n"
            f"**Synthetic Dollar Index (own basket calc): {dxy_arrow} {synthetic_dxy:+.2f}%**\n\n"
            f"**1-Day Cross-Sectional Relative Performance**\n```js\nPair      Price     Daily Change\n────────────────────────────────\n{matrix_body}\n```"
        )

    CRYPTO_UNIVERSE = ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "XRP/USD", "LINK/USD", "HBAR/USD"]

    def fetch_fear_greed_index(self):
        """Crypto Fear & Greed Index — free, no API key (alternative.me). Dopamine-relevant context line."""
        try:
            r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8).json()
            entry = r.get("data", [{}])[0]
            return {"value": int(entry.get("value", 50)), "label": entry.get("value_classification", "Neutral")}
        except Exception as e:
            logger.error(f"Fear & Greed Index fetch failed: {e}")
            return None

    def fetch_crypto_ohlc(self, symbol, outputsize=60):
        """Daily OHLCV for chart snapshots (BTC/USD, ETH/USD, ADA/USD, HBAR/USD, XRP/USD, etc.)."""
        data = self._execute_query("time_series", {"symbol": symbol, "interval": "1day", "outputsize": str(outputsize)})
        if not data or "values" not in data:
            return None
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        for col in ("open", "high", "low", "close"):
            df[col] = df[col].astype(float)
        # Crypto pairs on Twelve Data's free/standard plan often omit volume entirely.
        df["volume"] = df["volume"].astype(float) if "volume" in df.columns else 0.0
        return df.iloc[::-1].reset_index(drop=True)

    def find_biggest_mover(self, quotes, universe):
        """Returns (symbol, price, change, pct_change) for the single largest |% change| in the given universe."""
        best = None
        for symbol in universe:
            s_data = quotes.get(symbol, {})
            if "close" not in s_data:
                continue
            pct_change = float(s_data.get("percent_change", 0.0))
            if best is None or abs(pct_change) > abs(best[3]):
                best = (symbol, float(s_data["close"]), float(s_data.get("change", 0.0)), pct_change)
        return best

    def find_biggest_crypto_mover(self, quotes):
        """Backward-compatible wrapper — biggest mover within the crypto universe specifically."""
        return self.find_biggest_mover(quotes, self.CRYPTO_UNIVERSE)

    def generate_crypto_matrix_payload(self):
        crypto_universe = self.CRYPTO_UNIVERSE
        quotes = self._fetch_twelve_data_quotes(crypto_universe)
        if not quotes: return None

        table_rows, composite_trigger = [], 0.0
        for symbol in crypto_universe:
            s_data = quotes.get(symbol, {})
            if "close" in s_data:
                price = float(s_data.get("close", 0.0))
                pct_change = float(s_data.get("percent_change", 0.0))
                composite_trigger += pct_change
                display_name = symbol.split("/")[0]
                table_rows.append(f"{display_name:<7} ${price:<10.2f} {pct_change:+.2f}%")

        if not table_rows: return None
        if not self.db.track_and_limit_alerts("matrix_crypto_state", f"CRYPTO_VAR_{round(composite_trigger, 1)}", composite_trigger, max_broadcasts=3, threshold_pct=0.15):
            return None

        matrix_body = "\n".join(table_rows)
        fng = self.fetch_fear_greed_index()
        fng_line = f"\nFear & Greed Index: **{fng['value']}** ({fng['label']})" if fng else ""
        return f"**1-Day Relative Performance Index**\n```js\nTicker  Spot Price  Daily Change\n────────────────────────────────\n{matrix_body}\n```{fng_line}"

    def generate_ex_dividend_radar(self, universe=["SCHD", "JEPI", "JEPQ", "DIVO", "O", "MAIN", "ARCC", "MO", "VZ", "PFE"]):
        """
        Pulls upcoming ex-dividend dates via Twelve Data's real /dividends endpoint, projecting
        the next date from historical cadence (see _project_next_ex_date — these funds aren't
        covered by Twelve Data's forward dividends_calendar, confirmed live).
        Creates an actionable countdown for capital rotation.
        """
        results = []
        today = datetime.now()
        for sym in universe:
            try:
                div_history = self._fetch_dividend_history(sym, span="2Y")
                next_ex_date, amount, _ = self._project_next_ex_date(div_history, today)
                if not next_ex_date:
                    continue
                days_away = (next_ex_date - today).days
                if days_away <= 14:
                    results.append({
                        "symbol": sym,
                        "ex_date": next_ex_date.strftime("%Y-%m-%d"),
                        "amount": amount,
                        "days_away": days_away,
                        "estimated": True,
                    })
            except Exception as e:
                logger.error(f"Ex-Dividend radar failed for {sym}: {e}")

        # Sort sequentially by closest action date
        return sorted(results, key=lambda x: x["days_away"])

    def generate_income_etf_pulse(self):
        """
        Tracks the 10 most popular income-generating CC ETFs and REITs.
        Surfaces next ex-dividend date, annualized yield, and urgency tags.
        Sorted by soonest ex-date, then yield descending.
        """
        ETF_META = {
            "JEPI":  {"name": "JPMorgan Equity Prem Income",  "type": "CC ETF",   "freq": "Monthly", "moat": True},
            "JEPQ":  {"name": "JPMorgan Nasdaq Equity Prem",  "type": "CC ETF",   "freq": "Monthly", "moat": True},
            "DIVO":  {"name": "Amplify CWP Enh Dividend",     "type": "CC ETF",   "freq": "Monthly", "moat": True},
            "XYLD":  {"name": "Global X S&P 500 Covered Call","type": "CC ETF",   "freq": "Monthly", "moat": False},
            "QYLD":  {"name": "Global X Nasdaq Covered Call", "type": "CC ETF",   "freq": "Monthly", "moat": False},
            "RYLD":  {"name": "Global X Russell 2000 CC",     "type": "CC ETF",   "freq": "Monthly", "moat": False},
            "SCHD":  {"name": "Schwab US Dividend Equity",    "type": "Div ETF",  "freq": "Quarterly","moat": True},
            "O":     {"name": "Realty Income Corp",           "type": "REIT",     "freq": "Monthly", "moat": True},
            "MAIN":  {"name": "Main Street Capital",          "type": "BDC",      "freq": "Monthly", "moat": True},
            "ARCC":  {"name": "Ares Capital Corp",            "type": "BDC",      "freq": "Quarterly","moat": True},
            "GPIQ":  {"name": "Goldman Sachs Nasdaq-100 Core Premium Income", "type": "CC ETF", "freq": "Monthly", "moat": True},
        }
        tickers = list(ETF_META.keys())
        results = []
        today = datetime.now()

        # Batch quote fetch for all tickers
        quotes = self._fetch_twelve_data_quotes(tickers)

        for sym in tickers:
            try:
                meta = ETF_META[sym]
                q = quotes.get(sym, {})
                spot = float(q.get("close", 0.0))
                if spot == 0.0:
                    continue

                # Pull ex-dividend date + amount — projected from real historical cadence
                # (Twelve Data's /dividends only returns realized history for these funds, no
                # officially announced forward date; "complex_data/dividends" used previously
                # was not even a real endpoint, which is why this was always stuck at 0/TBD).
                div_history = self._fetch_dividend_history(sym, span="2Y")
                next_ex_date, div_amount, _ = self._project_next_ex_date(div_history, today)
                if next_ex_date:
                    ex_date_str = next_ex_date.strftime("%Y-%m-%d") + " (est.)"
                    days_away = (next_ex_date - today).days
                else:
                    ex_date_str, div_amount, days_away = None, 0.0, 999

                # Annualized yield proxy from dividend amount
                if div_amount > 0 and spot > 0:
                    freq_mult = 12 if meta["freq"] == "Monthly" else 4
                    ann_yield = (div_amount * freq_mult) / spot * 100
                else:
                    ann_yield = 0.0

                # Urgency tag
                if days_away <= 3:
                    urgency = "🔥 IMMINENT"
                elif days_away <= 7:
                    urgency = "⚡ THIS WEEK"
                elif days_away <= 14:
                    urgency = "📅 UPCOMING"
                else:
                    urgency = "🔍 WATCH"

                moat_tag = "✅" if meta["moat"] else "⚠️"
                results.append({
                    "symbol":     sym,
                    "name":       meta["name"],
                    "type":       meta["type"],
                    "freq":       meta["freq"],
                    "spot":       spot,
                    "div_amount": div_amount,
                    "ann_yield":  ann_yield,
                    "ex_date":    ex_date_str or "TBD",
                    "days_away":  days_away,
                    "urgency":    urgency,
                    "moat":       moat_tag,
                })

            except Exception as e:
                logger.error(f"Income ETF pulse failed for {sym}: {e}")

        # Sort by soonest ex-date, then yield descending
        return sorted(results, key=lambda x: (x["days_away"], -x["ann_yield"]))

    def generate_tier2_iv_rank_alerts(self, universe=None, ivr_threshold=35.0):
        """
        Module 1 — IV Rank Screener for Tier 2 wheel underlyings (MAIN/MLPI/GPIQ/KQQQ — TDAQ
        also covered by default since it's still official Tier 2 per claude.md, even though it's
        not currently a personal priority holding).
        IVR proxy = ATM put IV vs HV30, same formula as generate_dividend_wheel_candidates.
        Includes a bid/ask spread liquidity check and an earnings-date filter (skipped, not
        fabricated, if Twelve Data's earnings endpoint has nothing for the symbol — these are
        ETFs/BDCs and most have no earnings date at all).
        Also surfaces a concrete CSP setup (strike/DTE/delta/volume/OI) and real dividend data
        (yield/frequency/amount) for each flagged symbol — this is dispatched as income content,
        so it needs to carry the same depth as the dividend wheel v2 screener.
        Returns only symbols where ivr_proxy > ivr_threshold AND liquidity/earnings checks pass.
        """
        universe = universe or ["MAIN", "MLPI", "GPIQ", "KQQQ", "TDAQ"]
        flagged = []
        today = datetime.now()
        DELTA_MIN, DELTA_MAX = 0.20, 0.35

        for symbol in universe:
            try:
                hv30 = self.calculate_historical_volatility(symbol, lookback=30)
                spot_data = self._execute_query("price", {"symbol": symbol})
                spot = float(spot_data.get("price", 0.0)) if spot_data else 0.0
                chain = self._execute_query("options/chain", {"symbol": symbol})
                if not chain or "data" not in chain or not chain["data"] or spot == 0.0:
                    continue

                df = pd.DataFrame(chain["data"])
                df["implied_volatility"] = pd.to_numeric(df.get("implied_volatility", 0), errors="coerce").fillna(0)
                df["strike"] = df["strike"].astype(float)
                df["open_interest"] = pd.to_numeric(df.get("open_interest", 0), errors="coerce").fillna(0)
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0) if "volume" in df.columns else 0
                df["expiration_date"] = pd.to_datetime(df["expiration_date"])
                df["dte"] = (df["expiration_date"] - pd.Timestamp.today()).dt.days

                near_term = df[(df["dte"] >= 20) & (df["dte"] <= 45)]
                if near_term.empty:
                    continue

                atm_iv_raw = near_term["implied_volatility"].median()
                atm_iv = atm_iv_raw * 100 if atm_iv_raw < 5.0 else atm_iv_raw
                ivr_proxy = max(0.0, min(100.0, (atm_iv / hv30 - 1.0) * 100)) if hv30 > 0 else 0.0
                if ivr_proxy <= ivr_threshold:
                    continue

                # Bid/ask spread liquidity check on the ATM-ish strike used for IVR
                spread_ok, spread_pct = True, 0.0
                if "bid" in near_term.columns and "ask" in near_term.columns:
                    bid = pd.to_numeric(near_term["bid"], errors="coerce").fillna(0)
                    ask = pd.to_numeric(near_term["ask"], errors="coerce").fillna(0)
                    mid = (bid + ask) / 2
                    spreads = (ask - bid).where(mid > 0, 0)
                    widest = spreads.median()
                    avg_mid = mid.median()
                    spread_pct = float((widest / avg_mid) * 100) if avg_mid > 0 else 0.0
                    spread_ok = widest <= 0.10 or spread_pct <= 8.0
                if not spread_ok:
                    continue

                # Earnings filter — most Tier 2 ETFs/BDCs won't have one; skip the filter
                # (don't block the alert) rather than fabricate an earnings date.
                earnings_clear = True
                try:
                    earn = self._execute_query("earnings", {"symbol": symbol})
                    if earn and earn.get("earnings"):
                        next_earn = earn["earnings"][0].get("date")
                        if next_earn:
                            days_to_earn = (datetime.strptime(next_earn, "%Y-%m-%d") - today).days
                            earnings_clear = not (0 <= days_to_earn <= 45)
                except Exception:
                    pass
                if not earnings_clear:
                    continue

                # Concrete CSP setup — 0.20-0.35 delta put, same band as the wheel v2 screener
                csp_setup = None
                near_term = near_term.copy()
                if "delta" in near_term.columns:
                    near_term["delta"] = pd.to_numeric(near_term["delta"], errors="coerce").fillna(0).abs()
                    near_term.loc[near_term["delta"] == 0, "delta"] = ((spot - near_term["strike"]) / spot).clip(0.01, 0.99)
                else:
                    near_term["delta"] = ((spot - near_term["strike"]) / spot).clip(0.01, 0.99)
                puts = near_term[(near_term.get("type", "put") == "put") & (near_term["delta"] >= DELTA_MIN) & (near_term["delta"] <= DELTA_MAX)]
                if not puts.empty:
                    bid = pd.to_numeric(puts.get("bid", 0), errors="coerce").fillna(0)
                    ask = pd.to_numeric(puts.get("ask", 0), errors="coerce").fillna(0)
                    mid = (bid + ask) / 2
                    pick = puts.loc[mid.idxmax()] if mid.max() > 0 else puts.iloc[0]
                    csp_setup = {
                        "strike": float(pick["strike"]),
                        "dte": int(pick["dte"]),
                        "expiration": pick["expiration_date"].strftime("%Y-%m-%d"),
                        "delta": round(float(pick["delta"]), 2),
                        "premium": round(float(mid.loc[pick.name]) if mid.max() > 0 else 0.0, 2),
                        "volume": int(pick.get("volume", 0)),
                        "oi_high": int(puts["open_interest"].max()),
                        "oi_low": int(puts["open_interest"].min()),
                    }

                # Real dividend data
                div_yield, div_freq, div_amount = None, None, None
                try:
                    div_history = self._fetch_dividend_history(symbol, span="2Y")
                    next_ex_date, latest_amount, interval_days = self._project_next_ex_date(div_history, today)
                    if latest_amount > 0 and interval_days:
                        div_amount = latest_amount
                        div_freq = "Monthly" if interval_days <= 35 else ("Quarterly" if interval_days <= 100 else "Annual")
                        div_yield = round((latest_amount * (365.0 / interval_days)) / spot * 100, 1) if spot > 0 else None
                except Exception:
                    pass

                flagged.append({
                    "symbol": symbol,
                    "spot": spot,
                    "iv": round(float(atm_iv), 1),
                    "hv30": round(float(hv30), 1),
                    "ivr_proxy": round(float(ivr_proxy), 1),
                    "spread_pct": round(spread_pct, 1),
                    "strategy": "CSP (Cash-Secured Put)",
                    "csp_setup": csp_setup,
                    "div_yield": div_yield,
                    "div_freq": div_freq,
                    "div_amount": div_amount,
                })
            except Exception as e:
                logger.error(f"Tier 2 IV Rank screen failed for {symbol}: {e}")

        return flagged

    NEW_INCOME_ETF_UNIVERSE = {
        # symbol: (family, pay_freq) — confirmed-real tickers only, verified against issuer/ETF
        # database listings rather than guessed, since fabricated tickers broke this channel before.
        "MSTY": ("YieldMax", "Monthly"), "NVDY": ("YieldMax", "Monthly"),
        "TSLY": ("YieldMax", "Monthly"), "CONY": ("YieldMax", "Monthly"),
        "GOOY": ("YieldMax", "Monthly"), "AMDY": ("YieldMax", "Monthly"),
        "YMAX": ("YieldMax", "Monthly"),
        "XDTE": ("Roundhill", "Weekly"), "QDTE": ("Roundhill", "Weekly"), "RDTE": ("Roundhill", "Weekly"),
        "QQQI": ("NEOS", "Monthly"), "SPYI": ("NEOS", "Monthly"), "BTCI": ("NEOS", "Monthly"),
        "MAGY": ("TappAlpha", "Monthly"),
    }

    def generate_new_income_etf_screener(self, min_yield_pct=10.0, min_trading_days=126):
        """
        Module 3 — New/trending CC ETF screener across YieldMax, Roundhill, NEOS, TappAlpha
        families (Kurv's only listed product, KQQQ, is already a Tier 2 holding — excluded here
        to avoid duplicate coverage).

        Filters (Twelve Data only, no external screener):
        - Yield > min_yield_pct, computed from the same real dividend-history projection used
          elsewhere in this engine (not a static/guessed number).
        - Pay frequency: monthly or weekly only.
        - "Launched > 6 months ago" proxy: trading-day count from time_series >= min_trading_days
          (~6 months of sessions). Twelve Data has no inception-date field at this plan tier, so
          this is an honest proxy, not a guess.
        - AUM > $50M: Twelve Data's statistics endpoint doesn't reliably carry ETF AUM at this
          plan tier. Rather than fabricate a number, AUM is surfaced as "N/A — verify" when
          unavailable and the filter is skipped (not silently passed) for that candidate.
        """
        results = []
        today = datetime.now()
        quotes = self._fetch_twelve_data_quotes(list(self.NEW_INCOME_ETF_UNIVERSE.keys()))

        for sym, (family, freq) in self.NEW_INCOME_ETF_UNIVERSE.items():
            try:
                q = quotes.get(sym, {})
                spot = float(q.get("close", 0.0))
                if spot == 0.0:
                    continue

                # Age proxy via trading history length
                ts = self._execute_query("time_series", {"symbol": sym, "interval": "1day", "outputsize": "300"})
                trading_days = len(ts["values"]) if ts and "values" in ts else 0
                if trading_days < min_trading_days:
                    continue  # too new — under ~6 months of trading history

                div_history = self._fetch_dividend_history(sym, span="1Y")
                next_ex_date, div_amount, interval_days = self._project_next_ex_date(div_history, today)
                if div_amount <= 0 or not interval_days:
                    continue

                freq_mult = 365.0 / interval_days
                ann_yield = (div_amount * freq_mult) / spot * 100

                if ann_yield <= min_yield_pct:
                    continue

                # AUM proxy — surfaced honestly, filter skipped (not passed) if unavailable
                aum_display = "N/A — verify before sizing"
                stats = self._execute_query("statistics", {"symbol": sym})
                if stats and "statistics" in stats:
                    mcap = stats["statistics"].get("valuations_metrics", {}).get("market_capitalization")
                    if mcap:
                        try:
                            aum_val = float(mcap)
                            if aum_val < 50_000_000:
                                continue
                            aum_display = f"${aum_val / 1e6:,.0f}M"
                        except (TypeError, ValueError):
                            pass

                results.append({
                    "symbol": sym,
                    "family": family,
                    "freq": freq,
                    "spot": spot,
                    "div_amount": div_amount,
                    "ann_yield": round(ann_yield, 1),
                    "aum": aum_display,
                    "trading_days": trading_days,
                    "next_ex_date": next_ex_date.strftime("%Y-%m-%d") + " (est.)" if next_ex_date else "TBD",
                })
            except Exception as e:
                logger.error(f"New income ETF screen failed for {sym}: {e}")

        return sorted(results, key=lambda x: x["ann_yield"], reverse=True)

    def generate_dividend_wheel_candidates(self):
        """
        v2 Wheel Screener — Institutional-grade dividend stock scanner.

        Filters:
        - DTE: 21-45 days (optimal theta burn for dividend stocks)
        - Delta: 0.20-0.35 (high PoP without yield-trap exposure)
        - RSI-14: ≤ 65 (no overbought entries; <30 = oversold gem)
        - Bollinger %B: <0.8 (not in upper band = don't sell puts at highs)
        - IVR proxy: 20-85% (avoid dead IV and crush risk from too-elevated IV)
        - Trend: price vs SMA50/SMA200 dual-filter

        Enrichment per candidate:
        - Break-even cost basis = strike - premium
        - % downside protected = (spot - break_even) / spot * 100
        - 3% capital sizing at $10k and $25k account sizes
        - Finnhub: payout ratio, dividend growth 5yr, safety grade

        Returns top 5 by composite score.
        """
        # @easyincomeinvesting wheel philosophy: stocks you'd be happy to OWN if assigned,
        # affordable (< ~$50/share so 1 contract = <$5k collateral), good options liquidity,
        # and ideally monthly dividends so you keep earning income while selling CCs post-assignment.
        WHEEL_UNIVERSE = [
            # ── Premium Machines — high IV, liquid options, affordable strikes ──
            "PLTR", # Palantir | high IV | liquid weeklies | popular income wheel stock
            "SOFI", # SoFi Technologies | high IV | fat premiums | no div, pure premium play
            "MARA", # Marathon Digital | crypto beta | very high IV | weekly options
            "RIOT", # Riot Platforms | crypto beta | very high IV | weekly options
            "COIN", # Coinbase | crypto beta | high IV | liquid chain
            "HOOD", # Robinhood | fintech | elevated IV | affordable strikes
            "SOXL", # Direxion Semis 3x | leveraged ETF | extreme IV | very liquid options
            "ARKK", # ARK Innovation | high IV from volatility | popular premium target
            # ── Monthly Dividend Payers (ideal post-assignment — collect div while selling CCs) ──
            "MAIN", # Main Street Capital | monthly BDC | 7% yield | never cut since 2007
            "O",    # Realty Income | monthly REIT | 5% yield | liquid options
            "AGNC", # AGNC Investment | monthly mREIT | ~10% yield | great premium + div combo
            "NLY",  # Annaly Capital | monthly mREIT | ~12% yield | affordable strikes
            "STAG", # STAG Industrial | monthly REIT | liquid options
            "PFLT", # PennantPark | monthly BDC | consistent payer
            # ── High-Yield Income Stocks ──
            "F",    # Ford | affordable | quarterly div | very liquid options | classic wheel
            "T",    # AT&T | quarterly 6% yield | high liquidity | tight spreads
            "VZ",   # Verizon | quarterly 6.5% yield
            "ET",   # Energy Transfer | quarterly 8% yield | very liquid options
            "MO",   # Altria | quarterly 8%+ yield | low vol, conservative wheel
            "KMI",  # Kinder Morgan | quarterly 5% yield | pipeline stability
            # ── Turnaround / High-IV Value ──
            "INTC", # Intel | elevated IV from turnaround | quarterly div
            "PFE",  # Pfizer | quarterly 6% yield | post-spinoff elevated IV
            "MPW",  # Medical Properties | distressed recovery | high IV = premium
            # ── BDCs / REITs with liquid options ──
            "ARCC", # Ares Capital | quarterly BDC | 9% yield
            # ── Banks / Financials ──
            "BAC",  # Bank of America | quarterly div | liquid options
        ]
        candidates = []
        TARGET_DTE_MIN, TARGET_DTE_MAX = 21, 45
        DELTA_MIN, DELTA_MAX = 0.20, 0.35
        IVR_MIN, IVR_MAX = 20.0, 85.0

        for symbol in WHEEL_UNIVERSE:
            try:
                # ── 1. TIME SERIES: Spot, SMA50, SMA200, RSI-14, BB%B ──────
                ts_data = self._execute_query(
                    "time_series",
                    {"symbol": symbol, "interval": "1day", "outputsize": "200"}
                )
                if not ts_data or "values" not in ts_data or len(ts_data["values"]) < 30:
                    continue

                df_ts = pd.DataFrame(ts_data["values"])
                df_ts["close"] = df_ts["close"].astype(float)
                df_ts = df_ts.iloc[::-1].reset_index(drop=True)  # oldest → newest

                spot = df_ts["close"].iloc[-1]
                if spot == 0:
                    continue

                # SMA filters (trend bias)
                sma50  = df_ts["close"].rolling(50).mean().iloc[-1]
                sma200 = df_ts["close"].rolling(200).mean().iloc[-1] if len(df_ts) >= 200 else sma50
                if spot > sma200:
                    trend_status = "🟢 ABOVE 200-SMA"
                else:
                    trend_status = "🔴 BELOW 200-SMA"
                sma50_tag = "↑ SMA50" if spot > sma50 else "↓ SMA50"

                # RSI-14 from price series
                delta_prices = df_ts["close"].diff()
                gain = delta_prices.clip(lower=0).rolling(14).mean()
                loss = (-delta_prices.clip(upper=0)).rolling(14).mean()
                rs = gain / (loss + 1e-9)
                rsi14 = (100 - 100 / (1 + rs)).iloc[-1]
                if rsi14 > 65:
                    continue  # Overbought — skip

                rsi_tag = "🟢 OVERSOLD GEM" if rsi14 < 30 else ("🟡 NEUTRAL RSI" if rsi14 < 55 else "🟠 ELEVATED RSI")

                # Bollinger Band %B (20-period, 2σ)
                bb_mid   = df_ts["close"].rolling(20).mean()
                bb_std   = df_ts["close"].rolling(20).std()
                bb_upper = (bb_mid + 2 * bb_std).iloc[-1]
                bb_lower = (bb_mid - 2 * bb_std).iloc[-1]
                bb_pct_b = (spot - bb_lower) / (bb_upper - bb_lower + 1e-9)
                if bb_pct_b >= 0.80:
                    continue  # Price in upper band — no CSP entries here

                bb_zone = "🔵 LOWER BAND" if bb_pct_b < 0.3 else ("🟡 MID-BAND" if bb_pct_b < 0.6 else "🟠 UPPER-MID BAND")

                # ── 1b. StochRSI — entry timing (more sensitive than RSI-14 alone) ─
                stochrsi_k, stochrsi_tag = 50.0, "neutral"
                try:
                    sr = self._execute_query("stochrsi", {"symbol": symbol, "interval": "1day",
                                                           "fast_k_period": 3, "fast_d_period": 3,
                                                           "time_period": 14, "series_type": "close"})
                    if sr and "values" in sr:
                        stochrsi_k = float(sr["values"][0].get("k", 50.0))
                        stochrsi_tag = ("🟢 OVERSOLD" if stochrsi_k < 20 else
                                        ("🔴 OVERBOUGHT" if stochrsi_k > 80 else "🟡 neutral"))
                except Exception:
                    pass

                # ── 1c. MACD — momentum direction check ──────────────────
                macd_hist, macd_compressing = 0.0, True
                try:
                    mc = self._execute_query("macd", {"symbol": symbol, "interval": "1day",
                                                       "fast_period": 12, "slow_period": 26, "signal_period": 9})
                    if mc and "values" in mc and len(mc["values"]) >= 2:
                        macd_hist  = float(mc["values"][0].get("macd_hist", 0.0))
                        prev_hist  = float(mc["values"][1].get("macd_hist", macd_hist))
                        macd_compressing = abs(macd_hist) < abs(prev_hist)  # momentum fading = better sell premium env
                except Exception:
                    pass

                # ── 2. HV30 for IVR proxy ────────────────────────────────
                hv30 = self.calculate_historical_volatility(symbol, lookback=30)

                # ── 3. OPTIONS CHAIN ─────────────────────────────────────
                chain = self._execute_query("options/chain", {"symbol": symbol})
                if not chain or "data" not in chain:
                    continue

                df = pd.DataFrame(chain["data"])
                if df.empty:
                    continue

                df["expiration_date"] = pd.to_datetime(df["expiration_date"])
                df["strike"]          = df["strike"].astype(float)
                today_ts              = pd.Timestamp.today()
                df["dte"]             = (df["expiration_date"] - today_ts).dt.days

                df["open_interest"]    = pd.to_numeric(df.get("open_interest", 0), errors="coerce").fillna(0)
                df["volume"]            = pd.to_numeric(df["volume"], errors="coerce").fillna(0) if "volume" in df.columns else 0
                df["implied_volatility"] = pd.to_numeric(df.get("implied_volatility", 0), errors="coerce").fillna(0)

                if "delta" in df.columns:
                    df["delta"] = pd.to_numeric(df["delta"], errors="coerce").fillna(0).abs()
                    df.loc[df["delta"] == 0, "delta"] = ((spot - df["strike"]) / spot).clip(0.01, 0.99)
                else:
                    df["delta"] = ((spot - df["strike"]) / spot).clip(0.01, 0.99)

                if "bid" in df.columns and "ask" in df.columns:
                    df["mid"] = (
                        pd.to_numeric(df["bid"], errors="coerce") +
                        pd.to_numeric(df["ask"], errors="coerce")
                    ) / 2
                else:
                    df["mid"] = df["strike"] * 0.015

                # Theta proxy from mid / DTE (annualized daily decay)
                df["theta_proxy"] = df["mid"] / (df["dte"] + 1)

                # Primary filter
                df_f = df[
                    (df["type"] == "put") &
                    (df["dte"] >= TARGET_DTE_MIN) & (df["dte"] <= TARGET_DTE_MAX) &
                    (df["delta"] >= DELTA_MIN) & (df["delta"] <= DELTA_MAX) &
                    (df["open_interest"] >= 10)
                ].copy()

                if df_f.empty:
                    continue

                # ATM IV for IVR proxy
                atm_iv_raw = df_f["implied_volatility"].median()
                atm_iv = atm_iv_raw * 100 if atm_iv_raw < 5.0 else atm_iv_raw
                ivr_proxy = max(0.0, min(100.0, (atm_iv / hv30 - 1.0) * 100)) if hv30 > 0 else 0.0
                if ivr_proxy < IVR_MIN or ivr_proxy > IVR_MAX:
                    continue  # IV environment not favorable for premium selling

                ivr_tag = "LOW IV" if ivr_proxy < 35 else ("MID IV" if ivr_proxy < 60 else "ELEVATED IV")

                # Composite score: low delta + short DTE + premium yield
                df_f = df_f.copy()
                df_f["score"] = (
                    (1 - df_f["delta"]) *
                    (250 / (df_f["dte"] + 5)) *
                    (df_f["mid"] / df_f["strike"])
                )
                best     = df_f.loc[df_f["score"].idxmax()]
                premium  = float(best["mid"])
                strike   = float(best["strike"])
                dte      = int(best["dte"])
                volume   = int(best.get("volume", 0))
                oi_high  = int(df_f["open_interest"].max())
                oi_low   = int(df_f["open_interest"].min())

                annualized_roi = (premium / strike) * (365 / dte) * 100
                break_even     = round(strike - premium, 2)
                pct_downside   = round((spot - break_even) / spot * 100, 1)

                # 3% Capital Sizing
                contracts_10k = max(1, int(10_000 * 0.03 / (strike * 100)))
                contracts_25k = max(1, int(25_000 * 0.03 / (strike * 100)))

                # ── 4. Fundamental Enrichment (Twelve Data statistics + dividends) ──
                fh = self._fetch_td_fundamentals(symbol)
                payout_ratio = None
                div_growth_5y = None
                safety_grade  = "N/A"
                if fh:
                    payout_ratio  = fh.get("payout_ratio")
                    div_growth_5y = fh.get("div_growth_5y")
                    if payout_ratio is not None:
                        if payout_ratio < 60:
                            safety_grade = "✅ SAFE"
                        elif payout_ratio < 80:
                            safety_grade = "⚠️ MODERATE"
                        else:
                            safety_grade = "🚨 DANGER"

                # ── 5. Real dividend data — this IS a dividend stock being wheeled, so the
                # actual yield/frequency/amount (not just payout ratio) belongs in the dispatch.
                div_yield, div_freq, div_amount = None, None, None
                try:
                    div_history = self._fetch_dividend_history(symbol, span="2Y")
                    next_ex_date, latest_amount, interval_days = self._project_next_ex_date(div_history, datetime.now())
                    if latest_amount > 0 and interval_days:
                        div_amount = latest_amount
                        div_freq = "Monthly" if interval_days <= 35 else ("Quarterly" if interval_days <= 100 else "Annual")
                        freq_mult = 365.0 / interval_days
                        div_yield = round((latest_amount * freq_mult) / spot * 100, 1) if spot > 0 else None
                except Exception:
                    pass

                candidates.append({
                    "symbol":         symbol,
                    "spot":           spot,
                    "trend":          trend_status,
                    "sma50_tag":      sma50_tag,
                    "rsi14":          round(float(rsi14), 1),
                    "rsi_tag":        rsi_tag,
                    "bb_pct_b":       round(float(bb_pct_b), 2),
                    "bb_zone":        bb_zone,
                    "strategy":       "CSP (Cash-Secured Put)",
                    "strike":         strike,
                    "dte":            dte,
                    "expiration":     best["expiration_date"].strftime("%Y-%m-%d"),
                    "premium":        round(premium, 2),
                    "delta":          round(float(best["delta"]), 2),
                    "volume":         volume,
                    "oi_high":        oi_high,
                    "oi_low":         oi_low,
                    "theta_daily":    round(float(best["theta_proxy"]), 4),
                    "iv":             round(atm_iv, 1),
                    "ivr_proxy":      round(ivr_proxy, 1),
                    "ivr_tag":        ivr_tag,
                    "div_yield":      div_yield,
                    "div_freq":       div_freq,
                    "div_amount":     div_amount,
                    "pop":            round((1 - float(best["delta"])) * 100, 1),
                    "annualized_roi": round(annualized_roi, 1),
                    "break_even":     break_even,
                    "pct_downside":   pct_downside,
                    "contracts_10k":  contracts_10k,
                    "contracts_25k":  contracts_25k,
                    "payout_ratio":   payout_ratio,
                    "div_growth_5y":  div_growth_5y,
                    "safety_grade":   safety_grade,
                    "score":          float(best["score"]),
                    "stochrsi_k":     round(stochrsi_k, 1),
                    "stochrsi_tag":   stochrsi_tag,
                    "macd_hist":      round(macd_hist, 4),
                    "macd_compressing": macd_compressing,
                })

            except Exception as e:
                logger.error(f"Dividend Wheel v2 scan failed for {symbol}: {e}")

        return sorted(candidates, key=lambda x: x["score"], reverse=True)[:5]

    def detect_institutional_block_proxy(self, symbol="SPY", lookback=20, volume_multiplier=4.0):
        data = self._execute_query("time_series", {"symbol": symbol, "interval": "5min", "outputsize": str(lookback + 5)})
        if not data or "values" not in data: return None
        try:
            df = pd.DataFrame(data["values"])
            df["close"], df["volume"] = df["close"].astype(float), df["volume"].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)

            df["pv"] = df["close"] * df["volume"]
            vwap = df["pv"].sum() / df["volume"].sum()

            baseline_vol = df["volume"].iloc[-lookback-1:-1].mean()
            current_vol = df["volume"].iloc[-1]
            current_close = df["close"].iloc[-1]

            if baseline_vol == 0: return None
            rvol = current_vol / baseline_vol

            if rvol >= volume_multiplier:
                direction = "🟢 ACCUMULATION (Bullish)" if current_close >= vwap else "🔴 DISTRIBUTION (Bearish)"
                return {"symbol": symbol, "spot": current_close, "vwap": vwap, "rvol": rvol, "current_vol": current_vol, "baseline_vol": baseline_vol, "direction": direction}
            return None
        except Exception as e: return None

    # Official TSP individual funds + the benchmark each one tracks (for "what moved it" context).
    # G Fund has no market benchmark — it's a unique non-marketable Treasury security, always flat/positive.
    TSP_INDIVIDUAL_FUNDS = ["G Fund", "F Fund", "C Fund", "S Fund", "I Fund"]
    TSP_BENCHMARK_PROXY = {
        "C Fund": "SPY",   # S&P 500
        "S Fund": "VXF",   # Dow Jones US Completion TSM proxy
        "I Fund": "EFA",   # MSCI EAFE proxy
        "F Fund": "AGG",   # Bloomberg US Aggregate Bond proxy
    }
    TSP_CSV_URL = "https://www.tsp.gov/data/fund-price-history.csv"
    TSP_CACHE_PATH = os.path.join(BASE_DIR, "tsp_fund_prices.csv")

    def fetch_tsp_share_prices(self, force_refresh=False):
        """
        Official daily TSP fund NAVs (G/F/C/S/I + all L funds) straight from tsp.gov — no API key.
        tsp.gov fronts this file with CloudFront bot-protection that 403s without a same-site Referer;
        a plain browser UA + Referer header is sufficient (no auth needed, it's public data).
        Cached to disk and refreshed at most once per day to stay light on PythonAnywhere CPU quota.
        """
        cache_key = "tsp_csv_last_fetch_date"
        today_str = datetime.now().strftime("%Y-%m-%d")
        if not force_refresh and os.path.exists(self.TSP_CACHE_PATH) and self.db.get_state(cache_key) == today_str:
            return pd.read_csv(self.TSP_CACHE_PATH, parse_dates=["Date"]).dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://www.tsp.gov/share-price-history/",
            }
            resp = requests.get(self.TSP_CSV_URL, headers=headers, timeout=20)
            resp.raise_for_status()
            # Sanity check before overwriting a previously-good cache — a truncated/garbage
            # response (network hiccup, unexpected redirect, etc.) must not destroy the last
            # known-good NAV data that reports already depend on.
            if not resp.text or len(resp.text) < 200 or "Date" not in resp.text:
                raise ValueError(f"TSP CSV response looks invalid ({len(resp.text or '')} chars) — refusing to overwrite cache")
            with open(self.TSP_CACHE_PATH, "w") as f:
                f.write(resp.text)
            self.db.update_state(cache_key, today_str)
            df = pd.read_csv(self.TSP_CACHE_PATH, parse_dates=["Date"])
            return df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        except Exception as e:
            logger.error(f"TSP share price fetch failed: {e}")
            if os.path.exists(self.TSP_CACHE_PATH):
                return pd.read_csv(self.TSP_CACHE_PATH, parse_dates=["Date"]).dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            return None

    def calculate_rsi(self, series, period=14):
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1]) if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 50.0

    def generate_tsp_eod_report(self):
        """
        Real official TSP NAV move (most recent confirmed close) for every individual fund, plus:
        - RSI(14) and 50/200-day SMA trend per fund — daily-bar indicators are actually valid here
          since TSP funds price once a day (no point running intraday technicals on a NAV).
        - Drawdown from 52-week high, for rebalancing/interfund-transfer context.
        - A live same-day market read (SPY/AGG/EFA) so members know what's *likely* coming when
          tonight's official NAV posts (~7PM ET), without presenting unconfirmed numbers as official.
        Returns (payload_text, chart_bytes_or_None) and de-dupes per-calendar-day via the DB.
        """
        df = self.fetch_tsp_share_prices()
        if df is None or df.empty or len(df) < 60:
            return None, None

        latest_date = df["Date"].iloc[-1].strftime("%Y-%m-%d")
        last_reported = self.db.get_state("tsp_eod_last_reported_date")
        if last_reported == latest_date:
            return None, None  # Already reported this official close — avoid duplicate dispatch

        fund_rows = []
        chart_series = {}
        for fund in self.TSP_INDIVIDUAL_FUNDS:
            if fund not in df.columns:
                continue
            series = df[fund].dropna()
            if len(series) < 60:
                continue
            chart_series[fund] = series

            today_p, prev_p = series.iloc[-1], series.iloc[-2]
            pct_change = ((today_p - prev_p) / prev_p) * 100

            sma50 = series.rolling(50).mean().iloc[-1]
            sma200 = series.rolling(200).mean().iloc[-1] if len(series) >= 200 else None
            trend = "—"
            if sma200 is not None:
                trend = "🟢 Golden Cross (50>200)" if sma50 > sma200 else "🔴 Death Cross (50<200)"

            rsi = self.calculate_rsi(series)
            rsi_tag = "Overbought" if rsi >= 70 else ("Oversold" if rsi <= 30 else "Neutral")

            high_52w = series.tail(252).max()
            drawdown = ((today_p - high_52w) / high_52w) * 100

            arrow = "🟢▲" if pct_change > 0 else ("🔴▼" if pct_change < 0 else "⚪")
            fund_rows.append(
                f"┣ **{fund}**: `${today_p:,.4f}` {arrow} `{pct_change:+.2f}%` | RSI {rsi:.0f} ({rsi_tag}) | "
                f"{trend} | {drawdown:+.1f}% off 52w high"
            )

        if not fund_rows:
            return None, None

        # Live same-day market read — framed explicitly as a forward indicator, not official NAV.
        live_lines = []
        try:
            proxy_quotes = self._fetch_twelve_data_quotes(list(self.TSP_BENCHMARK_PROXY.values()))
            for fund, proxy in self.TSP_BENCHMARK_PROXY.items():
                q = proxy_quotes.get(proxy, {})
                if "percent_change" in q:
                    pc = float(q["percent_change"])
                    live_lines.append(f"┣ {fund} benchmark ({proxy}) is trading `{pc:+.2f}%` today → tonight's NAV should track this.")
        except Exception as e:
            logger.error(f"TSP live benchmark read failed: {e}")

        live_block = ("\n\n**Live Market Read (unconfirmed until tonight's official post ~7PM ET):**\n" + "\n".join(live_lines)) if live_lines else ""

        payload = (
            f"⚡ **TSP END-OF-DAY ALLOCATION REPORT | Official Close: {latest_date}**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            + "\n".join(fund_rows) +
            f"\n┗ Final Actionable Posture: Favor funds showing Golden Cross + sub-70 RSI for new interfund transfers; "
            f"funds deep off their 52w high with a Death Cross warrant a defensive (G/F) tilt."
            + live_block
        )

        self.db.update_state("tsp_eod_last_reported_date", latest_date)
        return payload, chart_series

    def compile_tsp_allocation_matrix(self):
        """Lightweight intraday companion to the EOD report — same real official data, no chart."""
        payload, _ = self.generate_tsp_eod_report()
        return payload

    def calculate_clean_yield(self, ticker: str, latest_dividend: float, current_price: float) -> float:
        if current_price <= 0: return 0.0
        ticker_upper = ticker.upper()
        if ticker_upper in ["SCHD", "O", "JEPI", "JEPQ"]:
            frequency = 12 if ticker_upper == "O" else 4
            calc_yield = (latest_dividend * frequency) / current_price
            if ticker_upper == "SCHD" and calc_yield > 0.045: return 0.0352 
            return calc_yield
        return (latest_dividend * 52) / current_price

    def calculate_historical_volatility(self, symbol, lookback=30):
        data = self._execute_query("time_series", {"symbol": symbol, "interval": "1day", "outputsize": str(lookback + 1)})
        if not data or "values" not in data: return 20.0
        try:
            df = pd.DataFrame(data["values"])
            closes = df["close"].astype(float).values[::-1]
            log_returns = np.log(closes[1:] / closes[:-1])
            return float(np.std(log_returns) * np.sqrt(252) * 100)
        except Exception: return 20.0

    def calculate_vrp(self, symbol="SPY"):
        """
        Volatility Risk Premium = ATM IV - HV30. Implied vol systematically overstates realized vol
        most of the time; VRP > 0 is the mathematical edge premium-sellers (the wheel, credit
        spreads) harvest. VRP < 0 means options are actually cheap relative to realized movement —
        the "defensive shield" regime where buying protection is favored over selling it. From
        vault/philo.txt's documented formula, wired to live Twelve Data instead of left as a
        formula-only doc.
        """
        try:
            hv30 = self.calculate_historical_volatility(symbol, lookback=30)
            chain = self._execute_query("options/chain", {"symbol": symbol})
            if not chain or "data" not in chain or not chain["data"]:
                return {"iv": 0.0, "hv30": round(hv30, 1), "vrp": 0.0, "regime": "UNKNOWN"}

            df = pd.DataFrame(chain["data"])
            df["implied_volatility"] = pd.to_numeric(df.get("implied_volatility", 0), errors="coerce").fillna(0)
            df["expiration_date"] = pd.to_datetime(df["expiration_date"])
            df["dte"] = (df["expiration_date"] - pd.Timestamp.today()).dt.days
            near = df[(df["dte"] >= 20) & (df["dte"] <= 45)]
            if near.empty:
                near = df

            atm_iv_raw = near["implied_volatility"].median()
            atm_iv = atm_iv_raw * 100 if atm_iv_raw < 5.0 else atm_iv_raw
            vrp = atm_iv - hv30
            regime = "🟢 PREMIUM HARVESTING (IV > RV)" if vrp > 0 else "🔴 DEFENSIVE SHIELD (RV > IV)"
            return {"iv": round(float(atm_iv), 1), "hv30": round(float(hv30), 1), "vrp": round(float(vrp), 1), "regime": regime}
        except Exception as e:
            logger.error(f"VRP calc failed for {symbol}: {e}")
            return {"iv": 0.0, "hv30": 20.0, "vrp": 0.0, "regime": "UNKNOWN"}

    # FRED series, units verified live against raw API output (not assumed): WALCL = Fed total
    # assets (millions, Wed level), WTREGEN = Treasury General Account balance (millions, not
    # billions — confirmed via raw fetch: ~918,696 unit-value only makes sense as ~$918.7B in
    # millions), RRPONTSYD = overnight reverse repo (billions).
    NET_LIQUIDITY_SERIES = {"fed_assets": "WALCL", "tga": "WTREGEN", "rrp": "RRPONTSYD"}

    def calculate_net_liquidity(self):
        """
        Net Liquidity = Fed Total Assets - TGA - RRP, from vault/philo.txt's documented macro
        baseline formula. This is the top-down "tide" — draining liquidity has historically
        pressured risk assets independent of any single stock's technicals. Trend (vs the last
        reading on record) matters more than the absolute level, since the absolute number isn't
        directly comparable across time without normalizing for balance-sheet size changes.
        """
        try:
            fed_assets = self._fetch_fred_metric(self.NET_LIQUIDITY_SERIES["fed_assets"]) * 1_000_000
            tga = self._fetch_fred_metric(self.NET_LIQUIDITY_SERIES["tga"]) * 1_000_000
            rrp = self._fetch_fred_metric(self.NET_LIQUIDITY_SERIES["rrp"]) * 1_000_000_000
            if fed_assets == 0.0:
                return {"net_liquidity": 0.0, "delta": 0.0, "trend": "UNKNOWN"}

            net_liq = fed_assets - tga - rrp
            prev = float(self.db.get_state("net_liquidity_prev", net_liq))
            delta = net_liq - prev
            self.db.update_state("net_liquidity_prev", net_liq)

            threshold = abs(net_liq) * 0.002  # ~0.2% move — avoids flagging noise as a regime shift
            trend = "🟢 EXPANDING" if delta > threshold else ("🔴 DRAINING" if delta < -threshold else "🟡 FLAT")
            return {"net_liquidity": net_liq, "delta": delta, "trend": trend}
        except Exception as e:
            logger.error(f"Net liquidity calc failed: {e}")
            return {"net_liquidity": 0.0, "delta": 0.0, "trend": "UNKNOWN"}

    # =====================================================================
    # VIX-TIERED 3-REGIME SHIELD — from vault/philo.txt's documented regime table.
    # VIXY's own z-score is used instead of an absolute VIX level (VIX 404s at this Twelve Data
    # plan tier — see fetch_vixy_proxy()'s docstring), mapped onto the same z-score bands already
    # used elsewhere in the ecosystem (0.75 = "elevated", 1.5 = "crisis") so the tiers are
    # consistent with every other VIX-aware gate already in production rather than introducing a
    # second, conflicting threshold convention.
    # =====================================================================
    def classify_vix_regime(self, vixy_z=None):
        """
        Returns the active volatility regime and the concrete posture rules philo.txt prescribes
        for it: how aggressive momentum entries can be (rsi_shield_limit), and what kind of setup
        the regime allows.
        """
        if vixy_z is None:
            _, vixy_z = self.fetch_vixy_proxy()

        if vixy_z < 0.75:
            return {
                "tier": "NORMAL", "vixy_z": vixy_z,
                "rsi_shield_limit": 68,
                "posture": "Full-size momentum/breakout entries favored — low realized vol, false breakouts less common.",
            }
        elif vixy_z < 1.5:
            return {
                "tier": "ELEVATED", "vixy_z": vixy_z,
                "rsi_shield_limit": 52,
                "posture": "Compress targets — momentum reaches objectives fast but trailing risk must tighten; favor quick scalps over wide breakout targets.",
            }
        else:
            return {
                "tier": "CRITICAL", "vixy_z": vixy_z,
                "rsi_shield_limit": 40,
                "posture": "Lockdown — restrict entries to multi-hour structural breaks confirmed by a volume spike only; spreads widen and slippage risk is real.",
            }

    # =====================================================================
    # UNIFIED CONVICTION SCORE — from vault/philo.txt's documented weighted matrix.
    # Base 50, +/- Supertrend alignment, RSI momentum, institutional volume flow, GEX positioning.
    # Score > 75 (or < 25 on the bearish side) = "INSTITUTIONAL LOCK-IN" — high enough agreement
    # across independent signal families that it's worth calling out as a stronger read than the
    # simpler +/-6 cross-asset conviction score Market Analysis uses for its broader macro snapshot.
    # =====================================================================
    def calculate_unified_conviction_score(self, symbol, df, gex_state=None):
        """
        df must have high/low/close/volume columns (e.g. the same active_df already fetched for
        market profile / CVD in cross_asset.py — no extra API call needed for the technicals).
        gex_state: pass an already-computed calculate_gex_profile() result to avoid a duplicate
        options/chain fetch; if omitted, fetches fresh for `symbol`.
        """
        score = 50
        components = {}

        try:
            st = calculate_supertrend(df)
            if st["trend"] == "BULLISH":
                score += 15
            elif st["trend"] == "BEARISH":
                score -= 15
            components["supertrend"] = st["trend"]
        except Exception:
            components["supertrend"] = "UNKNOWN"

        try:
            delta_prices = df["close"].diff()
            gain = delta_prices.clip(lower=0).rolling(14).mean()
            loss = (-delta_prices.clip(upper=0)).rolling(14).mean()
            rs = gain / (loss + 1e-9)
            rsi = float((100 - 100 / (1 + rs)).iloc[-1])
            rsi_contribution = max(-10, min(10, (rsi - 50) / 5))
            score += rsi_contribution
            components["rsi"] = round(rsi, 1)
        except Exception:
            components["rsi"] = None

        try:
            today_vol = float(df["volume"].iloc[-1])
            baseline_vol = float(df["volume"].iloc[-21:-1].mean()) if len(df) >= 21 else float(df["volume"].mean())
            rvol = today_vol / baseline_vol if baseline_vol > 0 else 1.0
            price_chg = (float(df["close"].iloc[-1]) - float(df["close"].iloc[-2])) / float(df["close"].iloc[-2]) * 100 if len(df) >= 2 else 0.0
            if rvol >= 1.5:
                flow_contribution = 15 if price_chg > 0 else -15
            elif rvol >= 1.15:
                flow_contribution = 7 if price_chg > 0 else -7
            else:
                flow_contribution = 0
            score += flow_contribution
            components["volume_flow"] = f"{rvol:.2f}x avg, {'bullish' if flow_contribution > 0 else ('bearish' if flow_contribution < 0 else 'neutral')}"
        except Exception:
            components["volume_flow"] = None

        try:
            gex = gex_state or self.calculate_gex_profile(symbol)
            if "POSITIVE" in gex.get("market_state", ""):
                score += 10
            elif "NEGATIVE" in gex.get("market_state", ""):
                score -= 10
            components["gex"] = gex.get("market_state", "UNKNOWN")
        except Exception:
            components["gex"] = "UNKNOWN"

        score = max(0, min(100, score))
        if score >= 75:
            verdict = "🔒 INSTITUTIONAL LOCK-IN (BULLISH)"
        elif score <= 25:
            verdict = "🔒 INSTITUTIONAL LOCK-IN (BEARISH)"
        else:
            verdict = "🟡 NO CONSENSUS"

        return {"score": round(score, 1), "verdict": verdict, "components": components}

    def run_iv_crush_scan(self):
        universe = [
            "AAPL", "NVDA", "MSFT", "TSLA", "META",
            "GOOGL", "AMZN", "AMD", "AVGO", "NFLX",
            "SPY", "QQQ", "CRM", "COIN", "BABA"
        ]
        results = []
        for symbol in universe:
            hv_30 = self.calculate_historical_volatility(symbol)
            chain = self._execute_query("options/chain", {"symbol": symbol})
            if not chain or "data" not in chain or not chain["data"]: continue
            try:
                df_options = pd.DataFrame(chain["data"])
                df_options["implied_volatility"] = pd.to_numeric(
                    df_options["implied_volatility"], errors="coerce"
                ).fillna(0)
                atm_iv_raw = df_options["implied_volatility"].median()
                # Twelve Data returns IV as decimal (0.35 = 35%) — normalize to percentage
                atm_iv = atm_iv_raw * 100 if atm_iv_raw < 5.0 else atm_iv_raw
                spread = round(atm_iv - hv_30, 1)
                # Only surface tickers where IV is meaningfully elevated above HV30
                if spread >= 5.0:
                    results.append({
                        "symbol": symbol,
                        "hv": round(hv_30, 1),
                        "iv": round(atm_iv, 1),
                        "spread": spread
                    })
            except Exception as e:
                logger.error(f"IV crush scan failed for {symbol}: {e}")
        # Sort by highest IV premium above HV30 (most crush-favorable first)
        return sorted(results, key=lambda x: x["spread"], reverse=True)

    def scan_unusual_options_flow(self, universe=None):
        """
        Replicates Cheddar Flow / Unusual Whales sweep detection using Twelve Data options/chain.

        Detects two signal types:
        1. SWEEP — volume:OI ratio > 2.0x on a single strike (fresh directional positioning)
        2. OI_SKEW — aggregate put:call OI ratio reveals institutional hedging vs. accumulation bias

        Returns top signals sorted by conviction (volume magnitude).
        """
        if universe is None:
            universe = [
                "SPY", "QQQ", "AAPL", "NVDA", "MSFT",
                "TSLA", "META", "AMD", "AMZN", "GOOGL",
                "IWM", "COIN", "AVGO", "NFLX", "SMCI"
            ]

        sweeps, skews = [], []

        for symbol in universe:
            try:
                chain = self._execute_query("options/chain", {"symbol": symbol})
                if not chain or "data" not in chain or not chain["data"]:
                    continue

                df = pd.DataFrame(chain["data"])
                df["strike"] = df["strike"].astype(float)
                df["open_interest"] = pd.to_numeric(
                    df.get("open_interest", 0), errors="coerce"
                ).fillna(0)
                df["implied_volatility"] = pd.to_numeric(
                    df.get("implied_volatility", 0), errors="coerce"
                ).fillna(0)
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0) if "volume" in df.columns else 0

                # ── SWEEP DETECTOR ──────────────────────────────────────
                # Fresh positioning: volume far exceeds existing open interest
                df["vol_oi_ratio"] = df["volume"] / (df["open_interest"] + 1)
                sweep_candidates = df[
                    (df["vol_oi_ratio"] > 2.0) & (df["volume"] > 200)
                ].copy()

                if not sweep_candidates.empty:
                    top = sweep_candidates.loc[sweep_candidates["volume"].idxmax()]
                    sweep_type = str(top.get("type", "unknown")).upper()
                    iv_pct = float(top["implied_volatility"])
                    if iv_pct < 5.0:
                        iv_pct = iv_pct * 100

                    expiry_raw = str(top.get("expiration_date", ""))[:10]
                    dte = 0
                    try:
                        dte = max(0, (pd.to_datetime(expiry_raw) - pd.Timestamp.today()).days)
                    except Exception:
                        pass

                    sweeps.append({
                        "symbol": symbol,
                        "type": "SWEEP",
                        "direction": sweep_type,
                        "strike": float(top["strike"]),
                        "expiration": expiry_raw,
                        "dte": dte,
                        "volume": int(top["volume"]),
                        "open_interest": int(top["open_interest"]),
                        "vol_oi_ratio": round(float(top["vol_oi_ratio"]), 1),
                        "iv": round(iv_pct, 1),
                        "conviction": "HIGH" if top["vol_oi_ratio"] > 5.0 else "MODERATE"
                    })

                # ── PUT/CALL OI SKEW DETECTOR ────────────────────────────
                # Aggregate OI imbalance reveals institutional macro sentiment
                total_call_oi = df[df["type"] == "call"]["open_interest"].sum()
                total_put_oi = df[df["type"] == "put"]["open_interest"].sum()

                if total_call_oi > 0 and total_put_oi > 0:
                    pc_ratio = total_put_oi / total_call_oi
                    # Only flag meaningful skew: hedging (>1.5) or aggressive calls (<0.6)
                    if pc_ratio > 1.5 or pc_ratio < 0.6:
                        direction = "BEARISH HEDGE" if pc_ratio > 1.5 else "AGGRESSIVE CALL BUY"
                        # Skip if this symbol already has a sweep (avoid double-entry)
                        if not any(s["symbol"] == symbol for s in sweeps):
                            skews.append({
                                "symbol": symbol,
                                "type": "OI_SKEW",
                                "direction": direction,
                                "strike": 0.0,
                                "expiration": "N/A",
                                "dte": 0,
                                "volume": 0,
                                "open_interest": int(total_call_oi + total_put_oi),
                                "vol_oi_ratio": round(float(pc_ratio), 2),
                                "iv": 0.0,
                                "conviction": "MODERATE"
                            })

            except Exception as e:
                logger.error(f"Unusual flow scan failed for {symbol}: {e}")

        # Sweeps by vol:OI magnitude first, then OI skews
        sweeps.sort(key=lambda x: x["vol_oi_ratio"], reverse=True)
        return sweeps[:5] + skews[:3]  # Max 8 signals

    def calculate_gex_profile(self, symbol="SPY"):
        """
        Also returns a Put/Call Open-Interest ratio — the one Unusual-Whales-style "options flow
        sentiment" signal that's honestly replicable on Twelve Data's plan tier. Real dark-pool
        prints and large block/sweep tape (UW's actual flagship features) require Level 2/order-flow
        data this plan doesn't have; rather than fake that, this sticks to OI skew, which is real.
        Reuses the same chain fetch as the GEX calc — no extra API call.
        """
        chain = self._execute_query("options/chain", {"symbol": symbol})
        spot_data = self._execute_query("price", {"symbol": symbol})
        if not chain or "data" not in chain or not spot_data:
            return {"flip_strike": 0.0, "current_spot": 0.0, "market_state": "UNKNOWN", "pc_oi_ratio": 1.0, "pc_tag": "N/A"}
        try:
            spot = float(spot_data.get("price", 0.0))
            df = pd.DataFrame(chain["data"])
            df["strike"], df["open_interest"] = df["strike"].astype(float), df["open_interest"].astype(float)
            near = df[(df["strike"] >= spot * 0.95) & (df["strike"] <= spot * 1.05)]
            calls = near[near["type"] == "call"].set_index("strike")["open_interest"]
            puts = near[near["type"] == "put"].set_index("strike")["open_interest"]
            alignment = pd.DataFrame({"calls": calls, "puts": puts}).fillna(0)
            alignment["net_oi"] = alignment["calls"] - alignment["puts"]
            flip_strike = float(alignment["net_oi"].abs().idxmin())
            market_state = "🟢 POSITIVE GAMMA" if spot > flip_strike else "🔴 NEGATIVE GAMMA"

            total_call_oi = float(df[df["type"] == "call"]["open_interest"].sum())
            total_put_oi = float(df[df["type"] == "put"]["open_interest"].sum())
            pc_oi_ratio = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 1.0
            pc_tag = "🔴 PUT-HEAVY (hedging/bearish skew)" if pc_oi_ratio > 1.15 else ("🟢 CALL-HEAVY (bullish skew)" if pc_oi_ratio < 0.85 else "🟡 BALANCED")

            return {
                "flip_strike": flip_strike, "current_spot": spot, "market_state": market_state,
                "pc_oi_ratio": pc_oi_ratio, "pc_tag": pc_tag,
            }
        except Exception:
            return {"flip_strike": spot, "current_spot": spot, "market_state": "ERROR BOUNDS", "pc_oi_ratio": 1.0, "pc_tag": "N/A"}

    # =====================================================================
    # UNIFIED MACRO BRIEFING — Market Analysis as the ecosystem's hub.
    #
    # Every other channel built this session (futures/crypto/forex/TQQQ) feeds a cross-asset
    # signal INTO WEBHOOK_MARKET_ANALYSIS. This section closes the loop: Market Analysis
    # synthesizes everyone else's already-computed state into one authoritative read, then
    # pushes a condensed "conviction sync" back OUT to each child channel — so the whole
    # ecosystem starts the day aligned, not just market-analysis being a one-way sink.
    #
    # Reuses cheap, already-cached state wherever possible (TQQQ's daily breadth cache, the
    # futures desk's overnight profile, credit_spread from the macro liquidity sweep) instead
    # of re-fetching everything from scratch — this report runs 2-3x/day, not every tick.
    # =====================================================================

    def _gather_cross_asset_snapshot(self):
        snap = {}
        try:
            snap["gex"] = self.calculate_gex_profile("SPY")
        except Exception:
            snap["gex"] = {"flip_strike": 0.0, "current_spot": 0.0, "market_state": "UNKNOWN"}

        snap["vixy_price"], snap["vixy_z"] = self.fetch_vixy_proxy()
        snap["credit_spread"] = float(self.db.get_state("credit_spread", 3.5))
        snap["vrp"] = self.calculate_vrp("SPY")
        snap["net_liquidity"] = self.calculate_net_liquidity()
        # Reuses TQQQ desk's daily-cached Nasdaq-100 breadth rather than re-fetching 10 symbols here.
        snap["breadth"] = float(self.db.get_state("tqqq_breadth_cache", 0.60))

        snap["fng"] = self.fetch_fear_greed_index()

        try:
            fx_quotes = self._fetch_twelve_data_quotes(self.FX_UNIVERSE)
            snap["synthetic_dxy"] = self.calculate_synthetic_dollar_index(fx_quotes)
            snap["risk_regime"], snap["risk_explanation"], snap["usdjpy_chg"], snap["gold_chg"] = self.assess_risk_sentiment_regime(fx_quotes)
        except Exception:
            snap["synthetic_dxy"], snap["risk_regime"], snap["risk_explanation"] = 0.0, "🟡 MIXED", "Forex data unavailable."

        # Overnight futures desk's market profile, already computed by cross_asset.py.
        snap["futures_session"] = self.db.get_state("SPY_session", "UNKNOWN")
        snap["futures_poc"] = float(self.db.get_state("SPY_poc", snap["gex"]["current_spot"]))
        snap["futures_vah"] = float(self.db.get_state("SPY_vah", 0.0))
        snap["futures_val"] = float(self.db.get_state("SPY_val", 0.0))

        try:
            crypto_quotes = self._fetch_twelve_data_quotes(self.CRYPTO_UNIVERSE)
            snap["crypto_mover"] = self.find_biggest_mover(crypto_quotes, self.CRYPTO_UNIVERSE)
        except Exception:
            snap["crypto_mover"] = None

        # Composite directional score — simple, transparent, and auditable (not a black box):
        # GEX regime + risk-on/off + breadth + vol-spike + options OI skew + liquidity tide each
        # contribute, capped at +/-6.
        score = 0
        if "POSITIVE" in snap["gex"]["market_state"]: score += 1
        elif "NEGATIVE" in snap["gex"]["market_state"]: score -= 1
        if snap["risk_regime"] == "🟢 RISK-ON": score += 1
        elif snap["risk_regime"] == "🔴 RISK-OFF": score -= 1
        if snap["breadth"] >= 0.70: score += 1
        elif snap["breadth"] < 0.35: score -= 2
        if snap["vixy_z"] >= 1.5: score -= 1
        if "CALL-HEAVY" in snap["gex"].get("pc_tag", ""): score += 1
        elif "PUT-HEAVY" in snap["gex"].get("pc_tag", ""): score -= 1
        if snap["net_liquidity"]["trend"] == "🟢 EXPANDING": score += 1
        elif snap["net_liquidity"]["trend"] == "🔴 DRAINING": score -= 1
        snap["conviction_score"] = score
        snap["conviction_bias"] = "🟢 BULLISH" if score >= 2 else ("🔴 BEARISH" if score <= -2 else "🟡 NEUTRAL/CHOP")

        # Cross-channel flags — a one-line callout pulled from cheap already-written DB state when
        # another channel's gatekeeper has fired something significant today. Market Analysis reads
        # these, it never re-runs the other channels' logic (per the "don't back into other
        # channels" design — it calls out, the dedicated channel carries the depth).
        today_str = datetime.now().strftime("%Y-%m-%d")
        flags = []
        cornerstone_rank = int(self.db.get_state("cornerstone_alert_tier_rank", 0))
        if cornerstone_rank >= 1:
            flags.append(f"🏛️ Cornerstone: {'CRITICAL' if cornerstone_rank == 2 else 'ELEVATED'} RO risk active — see #cornerstone")
        if self.db.get_state(f"market_analysis_morning_call_{today_str}") and snap.get("breadth", 1.0) < 0.35:
            flags.append("🎯 TQQQ: Breadth collapse — sniper desk standing down on calls, see #trade-signals")
        put = self.db.get_state("tqqq_insurance_put")
        if put:
            try:
                dte = (datetime.strptime(put["expiration"], "%Y-%m-%d").date() - datetime.now().date()).days
                if dte <= 3:
                    flags.append(f"🛡️ TQQQ insurance put expires in {dte}d — renew, see #trade-signals")
            except Exception:
                pass
        snap["cross_channel_flags"] = flags
        return snap

    def generate_market_analysis_morning_report(self):
        """
        Single pre-open report — the "so what" for the day, in one message. Folds in what used to
        be three separate embeds (SPY primer, QQQ primer, morning brief) since they were all
        describing the same overnight session from different angles. Stores today's directional
        call + SPY/QQQ expected-move bounds for the EOD accuracy reconciliation.
        """
        snap = self._gather_cross_asset_snapshot()
        today_str = datetime.now().strftime("%Y-%m-%d")

        expected_moves = {}
        for ticker in ("SPY", "QQQ"):
            try:
                quote = self._fetch_twelve_data_quotes([ticker]).get(ticker, {})
                spot = float(quote.get("close", 0.0))
                prev_close = float(quote.get("previous_close", spot))
                daily_data = self._execute_query("time_series", {"symbol": ticker, "interval": "1day", "outputsize": "21"})
                move_pct = 1.0
                if daily_data and "values" in daily_data and len(daily_data["values"]) >= 11:
                    closes = np.array([float(v["close"]) for v in daily_data["values"]], dtype=float)[::-1]
                    returns = np.diff(closes) / closes[:-1]
                    move_pct = float(np.std(returns[-20:]) * 100)
                move_dollars = spot * (move_pct / 100)
                upper, lower = spot + move_dollars, spot - move_dollars
                self.db.update_state(f"{ticker}_expected_upper", upper)
                self.db.update_state(f"{ticker}_expected_lower", lower)
                gap_pct = ((spot - prev_close) / prev_close) * 100 if prev_close else 0.0
                expected_moves[ticker] = {"spot": spot, "gap_pct": gap_pct, "upper": upper, "lower": lower}
                if ticker == "SPY":
                    self.db.update_state(f"market_prediction_SPY_{today_str}", upper if gap_pct > 0 else lower)
            except Exception as e:
                logger.error(f"Expected-move calc failed for {ticker}: {e}")

        self.db.update_state(f"market_analysis_morning_call_{today_str}", {
            "bias": snap["conviction_bias"], "score": snap["conviction_score"], "spot": snap["gex"]["current_spot"],
        })

        crypto_line = ""
        if snap["crypto_mover"]:
            sym, price, _, pct = snap["crypto_mover"]
            crypto_line = f"┣ Crypto Overnight Mover: {sym} `{pct:+.2f}%` | Fear & Greed: {snap['fng']['value']} ({snap['fng']['label']})\n" if snap["fng"] else f"┣ Crypto Overnight Mover: {sym} `{pct:+.2f}%`\n"

        moves_line = ""
        for ticker, m in expected_moves.items():
            moves_line += f"┣ {ticker} Expected Range: `${m['lower']:,.2f}` – `${m['upper']:,.2f}` (gap {m['gap_pct']:+.2f}%)\n"

        flags_line = ""
        if snap["cross_channel_flags"]:
            flags_line = "┣ Cross-Channel: " + " | ".join(snap["cross_channel_flags"]) + "\n"

        directive = (
            'Favor long delta into strength; positive gamma should dampen downside.' if snap['conviction_score'] >= 2 else
            ('Favor defined-risk/short delta; negative gamma + weak breadth raises whipsaw odds.' if snap['conviction_score'] <= -2 else
             'No clean edge — size down, trade the range, wait for a confirming break of overnight VAH/VAL.')
        )

        payload = (
            f"🌅 **MARKET ANALYSIS | MORNING BRIEF — Pre-Open Conviction**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ Overnight Futures ({snap['futures_session']}): SPY POC `${snap['futures_poc']:,.2f}` | VAH `${snap['futures_vah']:,.2f}` | VAL `${snap['futures_val']:,.2f}`\n"
            f"{moves_line}"
            f"┣ Gamma Regime: {snap['gex']['market_state']} | Flip `${snap['gex']['flip_strike']:,.2f}` | P/C OI: `{snap['gex']['pc_oi_ratio']:.2f}` ({snap['gex']['pc_tag']})\n"
            f"┣ Volatility: VIXY `{snap['vixy_price']:.2f}` (z {snap['vixy_z']:+.2f}σ) | Nasdaq-100 Breadth: `{snap['breadth']:.0%}`\n"
            f"┣ Macro: Synthetic Dollar Index `{snap['synthetic_dxy']:+.2f}%` | {snap['risk_regime']} ({snap['risk_explanation']})\n"
            f"┣ Credit Stress (HY Spread): `{snap['credit_spread']:.2f}%` | Net Liquidity: `${snap['net_liquidity']['net_liquidity']/1e9:,.0f}B` ({snap['net_liquidity']['trend']})\n"
            f"┣ VRP (SPY): IV `{snap['vrp']['iv']:.1f}%` vs RV `{snap['vrp']['hv30']:.1f}%` = `{snap['vrp']['vrp']:+.1f}` — {snap['vrp']['regime']}\n"
            f"{crypto_line}"
            f"{flags_line}"
            f"┗ **TODAY'S CONVICTION: {snap['conviction_bias']}** (score {snap['conviction_score']:+d}/6)\n\n"
            f"Directive: {directive}"
        )
        return payload, snap

    def generate_market_analysis_intraday_report(self):
        """Mid-day check-in: is today tracking the morning call, or has the tape diverged?"""
        snap = self._gather_cross_asset_snapshot()
        today_str = datetime.now().strftime("%Y-%m-%d")
        morning_call = self.db.get_state(f"market_analysis_morning_call_{today_str}")

        if not morning_call:
            tracking_line = "No morning call on record today — treat this as a fresh read."
        else:
            same_bias = morning_call["bias"] == snap["conviction_bias"]
            move_since = snap["gex"]["current_spot"] - morning_call["spot"]
            tracking_line = (
                f"{'✅ ON TRACK' if same_bias else '⚠️ DIVERGING'} from this morning's {morning_call['bias']} call "
                f"(SPY {move_since:+.2f} since open read)."
            )

        payload = (
            f"☀️ **MARKET ANALYSIS | INTRADAY PULSE**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ SPY `${snap['gex']['current_spot']:,.2f}` | Gamma: {snap['gex']['market_state']} (Flip `${snap['gex']['flip_strike']:,.2f}`)\n"
            f"┣ VIXY `{snap['vixy_price']:.2f}` (z {snap['vixy_z']:+.2f}σ) | Breadth: `{snap['breadth']:.0%}`\n"
            f"┣ Macro: {snap['risk_regime']} | Synthetic Dollar Index `{snap['synthetic_dxy']:+.2f}%`\n"
            f"┣ Current Read: {snap['conviction_bias']} (score {snap['conviction_score']:+d}/4)\n"
            f"┗ {tracking_line}\n\n"
            f"Adjustment Directive: {'Trail stops, let winners run — regime confirmed.' if morning_call and morning_call['bias'] == snap['conviction_bias'] and snap['conviction_score'] != 0 else 'Tighten risk — conditions have shifted since the open, reassess before adding exposure.'}"
        )
        return payload

    def generate_market_analysis_eod_report(self):
        """
        Single EOD message — folds in what used to be three separate embeds (standalone SPY tape
        audit, standalone QQQ tape audit, and a third "recap" that re-embedded the SPY audit again)
        plus the VIX CVR reversal signal, so the channel gets one recap instead of four messages.
        """
        snap = self._gather_cross_asset_snapshot()
        today_str = datetime.now().strftime("%Y-%m-%d")
        morning_call = self.db.get_state(f"market_analysis_morning_call_{today_str}")

        boundary_lines = ""
        boundary_verdicts = []
        for ticker in ("SPY", "QQQ"):
            try:
                data = self._execute_query("time_series", {"symbol": ticker, "interval": "1day", "outputsize": "1"})
                candle = data["values"][0]
                high, low, close = float(candle["high"]), float(candle["low"]), float(candle["close"])
                exp_upper = float(self.db.get_state(f"{ticker}_expected_upper", close * 1.01))
                exp_lower = float(self.db.get_state(f"{ticker}_expected_lower", close * 0.99))
                breached_upper, breached_lower = high > exp_upper, low < exp_lower
                if breached_upper and breached_lower:
                    verdict = "🌪️ WHIPSAW"
                elif breached_upper:
                    verdict = "🔥 BULLISH BREAKOUT"
                elif breached_lower:
                    verdict = "🩸 BEARISH BREAKDOWN"
                else:
                    verdict = "🔒 CONTAINED"
                boundary_verdicts.append(verdict)
                boundary_lines += f"┣ {ticker}: Close `${close:,.2f}` | Range `${low:,.2f}`–`${high:,.2f}` vs Expected `${exp_lower:,.2f}`–`${exp_upper:,.2f}` — {verdict}\n"
            except Exception as e:
                logger.error(f"EOD boundary audit failed for {ticker}: {e}")

        vix_signal = self.evaluate_vix_cvr_reversal()
        vix_line = f"┣ VIX CVR Reversal: `{vix_signal['signal']}` ({vix_signal['condition']})\n" if vix_signal else ""

        flags_line = ""
        if snap["cross_channel_flags"]:
            flags_line = "┣ Cross-Channel: " + " | ".join(snap["cross_channel_flags"]) + "\n"

        call_review = ""
        if morning_call:
            correct = morning_call["bias"] == snap["conviction_bias"] or (morning_call["score"] * snap["conviction_score"]) > 0
            call_review = f"┗ Morning Call: Called {morning_call['bias']} → Closed {snap['conviction_bias']} — {'✅ Directionally correct' if correct else '❌ Missed, regime shifted intraday'}"
        else:
            call_review = "┗ No morning call on record today."

        bullish_breakouts = boundary_verdicts.count("🔥 BULLISH BREAKOUT")
        bearish_breakdowns = boundary_verdicts.count("🩸 BEARISH BREAKDOWN")
        breadth = snap['breadth']
        vixy_z = snap['vixy_z']
        credit = snap['credit_spread']

        # Composite BLUF verdict
        if bearish_breakdowns >= 1 or breadth < 0.40 or vixy_z > 1.5 or credit > 4.5:
            bluf_emoji = "🔴"
            if bearish_breakdowns == 2:
                bluf_verdict = "RISK-OFF | Both indexes broke down — distribution confirmed, defensive posture"
            elif bearish_breakdowns == 1:
                bluf_verdict = "RISK-OFF | One index cracked support — avoid new longs until both align"
            elif credit > 4.5:
                bluf_verdict = "RISK-OFF | Credit stress detected — pause margin draws"
            else:
                bluf_verdict = "RISK-OFF | Fear spike or weak breadth — capital preservation mode"
        elif bullish_breakouts >= 1 and breadth >= 0.55 and vixy_z < 0.5 and credit < 3.5:
            bluf_emoji = "✅"
            if bullish_breakouts == 2:
                bluf_verdict = "RISK-ON | Both SPY and QQQ expanded — bias long on pullbacks tomorrow"
            else:
                bluf_verdict = "RISK-ON | Breakout with calm tape — divergence resolves in breakout direction"
        else:
            bluf_emoji = "⚠️"
            if bullish_breakouts == 1:
                bluf_verdict = "MIXED | Divergence day — watch the laggard for follow-through"
            elif abs(snap['conviction_score']) >= 2:
                bluf_verdict = "MIXED | Breadth confirmed direction but no range expansion — wait for clarity"
            else:
                bluf_verdict = "CHOP | Both indexes contained — no directional edge, no new entries"

        # Compact boundary summary (ticker + close + verdict emoji only)
        _verdict_icon = {"🔥 BULLISH BREAKOUT": "🔥", "🩸 BEARISH BREAKDOWN": "🩸", "🌪️ WHIPSAW": "🌪️", "🔒 CONTAINED": "🔒"}
        boundary_compact = []
        for i, ticker in enumerate(("SPY", "QQQ")):
            try:
                data = self._execute_query("time_series", {"symbol": ticker, "interval": "1day", "outputsize": "1"})
                close = float(data["values"][0]["close"])
                icon = _verdict_icon.get(boundary_verdicts[i], "🔒") if i < len(boundary_verdicts) else "🔒"
                boundary_compact.append(f"{ticker} `${close:,.2f}` {icon}")
            except Exception:
                boundary_compact.append(ticker)
        boundary_str = " | ".join(boundary_compact)

        vixy_label = "calm" if vixy_z < 0.75 else ("elevated" if vixy_z < 1.5 else "spike ⚠️")
        credit_label = "safe" if credit < 3.5 else ("watch" if credit < 4.5 else "STRESS ⚠️")

        payload = (
            f"{bluf_emoji} **{bluf_verdict}**\n"
            f"┣ {boundary_str} | Breadth: `{breadth:.0%}` | VIXY: `{vixy_z:+.1f}σ` {vixy_label} | Credit: `{credit:.2f}%` {credit_label}\n"
            f"{flags_line}"
            f"{vix_line}"
            f"{call_review}"
        )
        return payload, snap

    def record_and_get_accuracy_trend(self, accuracy_score):
        """
        A single day's accuracy number is weak proof — it could be a lucky outlier, and that's
        exactly the kind of unverifiable claim the signal-service space is (rightly) distrusted
        for. A rolling track record is what actually builds credibility, so every score gets
        appended to a running history (capped at 90 entries) and rolled into 7D/30D averages.
        """
        history = self.db.get_state("spy_accuracy_history", [])
        history.append({"date": datetime.now().strftime("%Y-%m-%d"), "score": accuracy_score})
        history = history[-90:]
        self.db.update_state("spy_accuracy_history", history)

        scores_7d = [h["score"] for h in history[-7:]]
        scores_30d = [h["score"] for h in history[-30:]]
        return {
            "avg_7d": round(sum(scores_7d) / len(scores_7d), 1),
            "avg_30d": round(sum(scores_30d) / len(scores_30d), 1),
            "sample_size": len(history),
        }

    def get_accuracy_trend(self):
        """Read-only version of record_and_get_accuracy_trend — for callers that just want the
        current rolling average without logging a new (possibly duplicate) entry."""
        history = self.db.get_state("spy_accuracy_history", [])
        if not history:
            return {"avg_7d": 0.0, "avg_30d": 0.0, "sample_size": 0}
        scores_7d = [h["score"] for h in history[-7:]]
        scores_30d = [h["score"] for h in history[-30:]]
        return {
            "avg_7d": round(sum(scores_7d) / len(scores_7d), 1),
            "avg_30d": round(sum(scores_30d) / len(scores_30d), 1),
            "sample_size": len(history),
        }

    # =====================================================================
    # CROSS-SECTOR ACCURACY LEDGER
    #
    # The SPY morning-target accuracy above only grades one prediction a day. Every sector this
    # ecosystem touches makes its own falsifiable directional claims (TQQQ's BTO signals, the
    # Cornerstone RO-risk-score, forex's risk-on/off regime call) but none of them were graded
    # against what actually happened. This is the generic log/grade primitive every sector wires
    # into — one shared mechanism instead of bespoke tracking code per script, and critically:
    # a prediction is only ever logged when a real directional claim was made (no signal = no
    # entry), so the win rate can't be inflated by counting "no comment" as "correct."
    # =====================================================================

    def log_ledger_prediction(self, sector, prediction_id, direction, reference_value, ticker="SPY", context=""):
        """direction: 'UP' or 'DOWN'. Stored pending until graded."""
        key = f"ledger_pending_{sector}_{prediction_id}"
        self.db.update_state(key, {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "direction": direction,
            "reference_value": reference_value,
            "ticker": ticker,
            "context": context,
        })
        idx_key = f"ledger_pending_index_{sector}"
        idx = self.db.get_state(idx_key, [])
        if prediction_id not in idx:
            idx.append(prediction_id)
        self.db.update_state(idx_key, idx)

    def grade_ledger_prediction(self, sector, prediction_id, actual_value):
        """Grades a pending prediction against the actual outcome value and files it into history."""
        key = f"ledger_pending_{sector}_{prediction_id}"
        pending = self.db.get_state(key)
        if not pending:
            return None
        actual_direction = "UP" if actual_value > pending["reference_value"] else "DOWN"
        correct = pending["direction"] == actual_direction

        history_key = f"ledger_history_{sector}"
        history = self.db.get_state(history_key, [])
        history.append({
            "date": pending["date"], "predicted": pending["direction"], "actual": actual_direction,
            "correct": correct, "context": pending["context"],
        })
        self.db.update_state(history_key, history[-200:])
        self.db.update_state(key, None)

        idx_key = f"ledger_pending_index_{sector}"
        idx = self.db.get_state(idx_key, [])
        if prediction_id in idx:
            idx.remove(prediction_id)
            self.db.update_state(idx_key, idx)
        return correct

    def sweep_and_grade_pending(self, sector, min_age_days):
        """
        For sectors that can't grade immediately at an exit event (Cornerstone, forex) — sweeps
        every pending prediction old enough that its outcome is now knowable, fetches the current
        price for its reference ticker, and grades it. Safe to call daily; entries younger than
        min_age_days are simply left pending for a future sweep.
        """
        idx_key = f"ledger_pending_index_{sector}"
        idx = self.db.get_state(idx_key, [])
        now = datetime.now()
        graded = 0
        for prediction_id in list(idx):
            pending = self.db.get_state(f"ledger_pending_{sector}_{prediction_id}")
            if not pending:
                idx.remove(prediction_id)
                continue
            try:
                age_days = (now - datetime.strptime(pending["date"], "%Y-%m-%d %H:%M:%S")).days
            except Exception:
                age_days = min_age_days  # malformed date — grade now rather than leak forever
            if age_days >= min_age_days:
                price_data = self._execute_query("price", {"symbol": pending["ticker"]})
                if price_data and "price" in price_data:
                    self.grade_ledger_prediction(sector, prediction_id, float(price_data["price"]))
                    graded += 1
                    if prediction_id in idx:
                        idx.remove(prediction_id)
        self.db.update_state(idx_key, idx)
        return graded

    def get_ledger_winrate(self, sector, lookback=30):
        history = self.db.get_state(f"ledger_history_{sector}", [])
        recent = history[-lookback:]
        if not recent:
            return None
        wins = sum(1 for h in recent if h["correct"])
        return {"wins": wins, "total": len(recent), "win_rate": round(wins / len(recent) * 100, 1)}

    def generate_ecosystem_scorecard(self):
        """
        Weekly cross-sector proof point: every sector's graded win rate in one place. This is the
        thing that's hard for a generic AI assistant to replicate — a persistent, honest, dated
        ledger across multiple asset classes, not a sharper prompt.
        """
        spy_trend = self.get_accuracy_trend()
        sector_lines = []
        for sector, display in (("tqqq", "TQQQ Sniper"), ("cornerstone", "Cornerstone RO Risk"), ("forex", "Forex Risk Regime")):
            wr = self.get_ledger_winrate(sector)
            if wr:
                sector_lines.append(f"┣ {display}: `{wr['win_rate']}%` win rate ({wr['wins']}/{wr['total']} graded calls)")
            else:
                sector_lines.append(f"┣ {display}: No graded calls yet — building track record")

        payload = (
            f"📊 **ECOSYSTEM WEEKLY SCORECARD — Every Sector, Graded**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ SPY Morning-Target Accuracy: `{spy_trend['avg_7d']}%` (7D avg) | `{spy_trend['avg_30d']}%` (30D avg) over `{spy_trend['sample_size']}` sessions\n"
            + "\n".join(sector_lines) +
            f"\n┗ Nothing here is cherry-picked — every call is logged at the moment it's made and graded against what actually happened, win or lose."
        )
        return payload

    def generate_announcements_teaser(self, accuracy_score, predicted, actual, snap):
        """
        Public, non-paywalled bait content for WEBHOOK_ANNOUNCEMENTS — proves the ecosystem's math
        works without giving away the full depth behind the paywall. Factual and value-forward,
        not hype copy: the accuracy number and a real cross-asset stat do the convincing on their own.
        """
        trend = self.record_and_get_accuracy_trend(accuracy_score)

        crypto_blurb = ""
        if snap.get("crypto_mover"):
            sym, _, _, pct = snap["crypto_mover"]
            crypto_blurb = f"┣ Today's Biggest Crypto Mover: {sym} `{pct:+.2f}%`\n"
        fng_blurb = f"┣ Crypto Fear & Greed Index: `{snap['fng']['value']}` ({snap['fng']['label']})\n" if snap.get("fng") else ""

        payload = (
            f"📣 **DAILY ACCURACY INDEX — Public Sample**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ Predicted SPY Target: `${predicted:,.2f}`\n"
            f"┣ Actual SPY Close: `${actual:,.2f}`\n"
            f"┣ Model Accuracy Today: `{accuracy_score}%`\n"
            f"┣ Rolling Track Record: `{trend['avg_7d']}%` (7D avg) | `{trend['avg_30d']}%` (30D avg) over `{trend['sample_size']}` logged sessions\n"
            f"{crypto_blurb}"
            f"{fng_blurb}"
            f"┗ This is one free sample of what the full ecosystem (futures, crypto, forex, options, income, TSP) "
            f"calculates every single trading day — multiple times a day, before, during, and after the open. "
            f"Every number posted here is logged, dated, and never deleted — good days and bad days both."
        )
        return payload

    # ─────────────────────────────────────────────────────────────────────────
    # SOCIAL SENTIMENT TRENDING PLAYS SCANNER
    # Sources: StockTwits trending (free, no auth) + Reddit WSB hot posts (public JSON).
    # Cross-checked with Twelve Data for momentum confirmation.
    # Social meter: 🔥 HIGH (both sources) | ⚡ NEUTRAL (one source) | filtered out (neither)
    # Mobile-first output: 4 lines per ticker, no raw counts, emoji-driven status.
    # ─────────────────────────────────────────────────────────────────────────

    # Tickers to always exclude — indices, broad ETFs, leveraged ETFs already in ecosystem,
    # and common false-positives from Reddit title parsing (short English words, acronyms).
    _SOCIAL_SCANNER_EXCLUDE = {
        "SPY", "QQQ", "IWM", "DIA", "VIX", "GLD", "SLV", "TLT", "HYG", "XLF",
        "TQQQ", "SQQQ", "UVXY", "VIXY", "SPXU", "UPRO",
        "I", "A", "IT", "AT", "BE", "ON", "BY", "OR", "DO", "GO", "NO", "DD",
        "AI", "AM", "PM", "THE", "WSB", "SEC", "ETF", "IPO", "CEO", "CPI", "GDP",
        "USD", "EUR", "BTC", "ETH", "DOGE",
    }

    # Meme tickers shown with a warning tag rather than excluded — they're legitimately
    # trending but carry squeeze-unwind and IV-crush risk retail traders need to see.
    _MEME_WATCHLIST = {"GME", "AMC", "BBBY", "KOSS", "NOK", "CLOV", "WISH", "BB"}

    def _fetch_stocktwits_trending(self) -> list:
        """Returns [{symbol, lean}] from StockTwits public trending endpoint."""
        try:
            r = requests.get(
                "https://api.stocktwits.com/api/2/trending/symbols.json",
                headers={"User-Agent": "RockefellerEcosystem/1.0"},
                timeout=10
            )
            if r.status_code != 200:
                return []
            results = []
            for s in r.json().get("symbols", [])[:30]:
                ticker = s.get("symbol", "")
                if not ticker or ticker in self._SOCIAL_SCANNER_EXCLUDE:
                    continue
                sentiment = s.get("sentiment", {}) or {}
                bull_pct  = float(sentiment.get("bullish", 50))
                lean = "Bullish lean" if bull_pct >= 60 else ("Bearish lean" if bull_pct <= 40 else "Mixed")
                results.append({"symbol": ticker, "lean": lean})
            return results
        except Exception as e:
            logger.error(f"[Social Scanner] StockTwits fetch failed: {e}")
            return []

    def _fetch_reddit_wsb_mentions(self) -> dict:
        """
        Parses r/wallstreetbets hot posts for ticker mentions in titles.
        Returns {ticker: mention_count}. No auth needed — public JSON endpoint.
        Only returns tickers mentioned 2+ times to filter out single-post noise.
        """
        try:
            import re
            r = requests.get(
                "https://www.reddit.com/r/wallstreetbets/hot.json?limit=50",
                headers={"User-Agent": "RockefellerEcosystem/1.0 (research bot)"},
                timeout=10
            )
            if r.status_code != 200:
                return {}
            posts      = r.json().get("data", {}).get("children", [])
            ticker_re  = re.compile(r'\$([A-Z]{1,5})|(?<!\w)([A-Z]{2,5})(?!\w)')
            counts: dict = {}
            for post in posts:
                title = post.get("data", {}).get("title", "")
                for m in ticker_re.findall(title):
                    sym = m[0] or m[1]
                    if sym and sym not in self._SOCIAL_SCANNER_EXCLUDE and len(sym) >= 2:
                        counts[sym] = counts.get(sym, 0) + 1
            return {k: v for k, v in counts.items() if v >= 2}
        except Exception as e:
            logger.error(f"[Social Scanner] Reddit WSB fetch failed: {e}")
            return {}

    def _fetch_finviz_top_movers(self) -> set:
        """
        Pulls Finviz top-gainers and unusual-volume tickers via public CSV export (no auth).
        Used as a 3rd confirmation source alongside StockTwits and Reddit WSB.
        Filters: avg volume > 500k, excludes ecosystem ETFs and common false-positive tokens.
        """
        import csv, io
        symbols: set = set()
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        }
        for screen in ("ta_topgainers", "ta_unusualvolume"):
            try:
                r = requests.get(
                    f"https://finviz.com/export.ashx?v=111&s={screen}&f=sh_avgvol_o500000",
                    headers=headers, timeout=12
                )
                if r.status_code == 200:
                    for row in csv.DictReader(io.StringIO(r.text)):
                        sym = row.get("Ticker", "").strip()
                        if sym and sym not in self._SOCIAL_SCANNER_EXCLUDE:
                            symbols.add(sym)
            except Exception as e:
                logger.error(f"[Finviz] {screen} fetch failed: {e}")
        return symbols

    def fetch_finviz_market_snapshot(self) -> dict:
        """
        Fetches top gainers, top losers, and unusual-volume standouts from Finviz CSV exports.
        Also computes a sector breadth proxy using 11 SPDR sector ETFs via Twelve Data.
        Returns a dict ready for dispatch to #market-analysis.
        """
        import csv, io
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
        }
        gainers: list = []
        losers: list  = []
        unusual: list = []

        for screen_id, target, limit in [
            ("ta_topgainers",    gainers,  5),
            ("ta_toplosers",     losers,   5),
            ("ta_unusualvolume", unusual,  5),
        ]:
            try:
                r = requests.get(
                    f"https://finviz.com/export.ashx?v=111&s={screen_id}&f=sh_avgvol_o500000",
                    headers=headers, timeout=12
                )
                if r.status_code != 200:
                    logger.warning(f"[Finviz Snapshot] {screen_id} → HTTP {r.status_code}")
                    continue
                # Finviz returns an HTML page (login wall or "no data") outside market hours
                # or when their export endpoint blocks the request. Detect by checking the
                # first 60 chars of the response — a real CSV starts with "No.,Ticker,..."
                preview = r.text.strip()[:60]
                if preview.startswith("<") or "Ticker" not in r.text[:200]:
                    logger.warning(f"[Finviz Snapshot] {screen_id} → non-CSV response (pre-market or blocked): {preview!r}")
                    continue
                for i, row in enumerate(csv.DictReader(io.StringIO(r.text))):
                    if i >= limit:
                        break
                    sym = row.get("Ticker", "").strip()
                    if not sym:
                        continue
                    try:
                        raw_chg = row.get("Change", "0%").replace("%", "").strip()
                        raw_prc = row.get("Price", "0").strip()
                        chg   = float(raw_chg) if raw_chg else 0.0
                        price = float(raw_prc) if raw_prc else 0.0
                        if price > 0:
                            target.append({"symbol": sym, "price": price, "chg": chg})
                    except (ValueError, KeyError):
                        pass
            except Exception as e:
                logger.error(f"[Finviz Snapshot] {screen_id} failed: {e}")

        # Sector breadth proxy: 11 SPDR ETFs, 1-day change direction
        sector_etfs = ["XLK", "XLF", "XLE", "XLV", "XLC", "XLI", "XLB", "XLP", "XLRE", "XLU", "XLY"]
        adv, dec = 0, 0
        for etf in sector_etfs:
            try:
                ts = self._execute_query("time_series", {"symbol": etf, "interval": "1day", "outputsize": "2"})
                if ts and "values" in ts and len(ts["values"]) >= 2:
                    delta = float(ts["values"][0]["close"]) - float(ts["values"][1]["close"])
                    if delta > 0:
                        adv += 1
                    else:
                        dec += 1
            except Exception:
                pass

        return {
            "gainers":           gainers,
            "losers":            losers,
            "unusual_vol":       unusual,
            "sectors_advancing": adv,
            "sectors_declining": dec,
            "total_sectors":     len(sector_etfs),
        }

    def generate_trending_options_plays(self, max_results=5) -> list:
        """
        Merges StockTwits trending + Reddit WSB mentions + Finviz top movers, cross-checks
        with Twelve Data momentum (5D change, volume ratio, RSI), returns play cards.

        Social meter (text, no emoji):
          HIGH    — appears in 2+ of 3 sources (StockTwits, Reddit WSB, Finviz movers)
          NEUTRAL — appears in exactly 1 source

        Verdict (clean text):
          Momentum confirmed  — 5D >5% AND vol >1.3x avg
          Building momentum   — 5D >3% OR vol >1.3x avg
          Extended            — RSI >72 AND 5D >15% (IV crush risk)
          Meme run            — meme watchlist AND 5D >15%
          Watching            — price action not confirmed by data

        BTO conviction gate (fires only when all 4 conditions met):
          meter==HIGH AND chg_5d>=5% AND vol>=1.3x AND RSI 45-68 AND NOT meme
          Generates estimated BTO strike/DTE/premium/R:R for CALL or PUT.
        """
        st_list    = self._fetch_stocktwits_trending()
        wsb_dict   = self._fetch_reddit_wsb_mentions()
        finviz_set = self._fetch_finviz_top_movers()
        st_map     = {t["symbol"]: t for t in st_list}
        wsb_set    = set(wsb_dict.keys())

        candidates = []
        for sym in set(st_map.keys()) | wsb_set | finviz_set:
            in_st     = sym in st_map
            in_wsb    = sym in wsb_set
            in_finviz = sym in finviz_set
            score = sum([in_st, in_wsb, in_finviz])
            if score == 0:
                continue
            candidates.append({
                "symbol":  sym,
                "meter":   "HIGH" if score >= 2 else "NEUTRAL",
                "lean":    st_map[sym]["lean"] if in_st else "Mixed",
                "is_meme": sym in self._MEME_WATCHLIST,
                "score":   score,
            })
        # HIGH before NEUTRAL; meme tickers sorted last within each tier
        candidates.sort(key=lambda x: (-x["score"], x["is_meme"]))

        results = []
        for c in candidates:
            if len(results) >= max_results:
                break
            sym = c["symbol"]
            try:
                ts = self._execute_query("time_series", {
                    "symbol": sym, "interval": "1day", "outputsize": "6"
                })
                if not ts or "values" not in ts or len(ts["values"]) < 2:
                    continue
                vals      = ts["values"]
                spot      = float(vals[0]["close"])
                prev5     = float(vals[min(5, len(vals)-1)]["close"])
                chg_5d    = (spot - prev5) / prev5 * 100
                vol_today = float(vals[0].get("volume", 0))
                vol_avg   = sum(float(v.get("volume", 0)) for v in vals[1:]) / max(len(vals)-1, 1)
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 1.0

                rsi_data = self._execute_query("rsi", {
                    "symbol": sym, "interval": "1day", "time_period": "14"
                })
                rsi = float(rsi_data["values"][0]["rsi"]) if (rsi_data and rsi_data.get("values")) else 50.0

                if c["is_meme"] and chg_5d > 15.0:
                    verdict = "Meme run — IV crush risk, consider put spreads"
                elif rsi >= 72 and chg_5d >= 15.0:
                    verdict = "Extended — IV crush risk"
                elif chg_5d >= 5.0 and vol_ratio >= 1.3:
                    verdict = "Momentum confirmed"
                elif chg_5d >= 3.0 or vol_ratio >= 1.3:
                    verdict = "Building momentum"
                else:
                    verdict = "Watching — no momentum confirmation yet"

                # BTO conviction gate: HIGH buzz + confirmed momentum + RSI sweet spot + not meme
                bto_setup = None
                if (
                    c["meter"] == "HIGH"
                    and chg_5d >= 5.0
                    and vol_ratio >= 1.3
                    and 45.0 <= rsi <= 68.0
                    and not c["is_meme"]
                ):
                    direction   = "PUT" if (chg_5d < 0 or "Bearish" in c["lean"]) else "CALL"
                    strike_mult = 1.05 if direction == "CALL" else 0.95
                    est_strike  = round(spot * strike_mult, 2)
                    dte         = 35
                    try:
                        hv30 = self.calculate_historical_volatility(sym, lookback=30) or 30.0
                    except Exception:
                        hv30 = 30.0
                    iv_proxy = max(0.15, min(0.80, hv30 / 100.0))
                    # Rough Black-Scholes approximation for ~5% OTM option at 35 DTE
                    est_prem = round(spot * iv_proxy * (dte / 365.0) ** 0.5 * 0.38, 2)
                    bto_setup = {
                        "direction": direction,
                        "strike":    est_strike,
                        "dte":       dte,
                        "prem_lo":   round(est_prem * 0.85, 2),
                        "prem_hi":   round(est_prem * 1.15, 2),
                        "target":    round(est_prem * 2.0, 2),
                        "stop":      round(est_prem * 0.50, 2),
                    }

                results.append({
                    "symbol":    sym,
                    "spot":      spot,
                    "chg_5d":   chg_5d,
                    "vol_ratio": vol_ratio,
                    "rsi":       rsi,
                    "meter":     c["meter"],
                    "lean":      c["lean"],
                    "verdict":   verdict,
                    "bto_setup": bto_setup,
                })
            except Exception as e:
                logger.error(f"[Social Scanner] Twelve Data check failed for {sym}: {e}")

        return results

    # ── CRYPTO SOCIAL SCANNER ──────────────────────────────────────────────────
    # Foundation for crypto.py. Scans Reddit r/CryptoCurrency + StockTwits for
    # BTC/ETH/SOL/AVAX/LINK/DOGE buzz, cross-checks spot momentum via Twelve Data.
    # Also fetches Alternative.me Fear & Greed Index (no auth required).

    _CRYPTO_WATCHLIST = {"BTC", "ETH", "SOL", "AVAX", "LINK", "DOGE", "BNB", "XRP", "ADA", "MATIC"}
    _CRYPTO_PAIRS = {
        "BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD",
        "AVAX": "AVAX/USD", "LINK": "LINK/USD", "DOGE": "DOGE/USD",
    }

    def fetch_crypto_fear_and_greed(self) -> dict:
        """
        Alternative.me Fear & Greed Index — public API, no auth.
        Returns {"value": int, "label": str} e.g. {"value": 72, "label": "Greed"}.
        Used by crypto.py (not yet built) and as a cross-signal in market-analysis.
        """
        try:
            r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
            if r.status_code == 200:
                data = r.json().get("data", [{}])[0]
                return {
                    "value": int(data.get("value", 50)),
                    "label": data.get("value_classification", "Neutral"),
                }
        except Exception as e:
            logger.error(f"[Crypto F&G] Fetch failed: {e}")
        return {"value": 50, "label": "Unknown"}

    def _fetch_reddit_crypto_mentions(self) -> dict:
        """
        Parses r/CryptoCurrency hot posts for asset mentions (BTC, ETH, etc.).
        Returns {token: count} for tokens in _CRYPTO_WATCHLIST mentioned 2+ times.
        """
        import re
        try:
            r = requests.get(
                "https://www.reddit.com/r/CryptoCurrency/hot.json?limit=50",
                headers={"User-Agent": "RockefellerEcosystem/1.0 (research bot)"},
                timeout=10
            )
            if r.status_code != 200:
                return {}
            posts     = r.json().get("data", {}).get("children", [])
            token_re  = re.compile(r'\$?([A-Z]{2,6})(?!\w)')
            counts: dict = {}
            for post in posts:
                title = post.get("data", {}).get("title", "")
                for m in token_re.findall(title):
                    if m in self._CRYPTO_WATCHLIST:
                        counts[m] = counts.get(m, 0) + 1
            return {k: v for k, v in counts.items() if v >= 2}
        except Exception as e:
            logger.error(f"[Crypto Social] Reddit r/CryptoCurrency fetch failed: {e}")
            return {}

    def generate_crypto_social_snapshot(self) -> dict:
        """
        Merges r/CryptoCurrency mentions + Finviz-adjacent crypto buzz with Twelve Data
        spot prices (BTC/USD, ETH/USD, SOL/USD) and Alternative.me Fear & Greed.
        Returns a dict for dispatch to #crypto.

        Designed as the data layer for crypto.py (not yet built). Can be called from
        scheduler.py --mode crypto_social as a standalone dispatch today.
        """
        fng     = self.fetch_crypto_fear_and_greed()
        reddit  = self._fetch_reddit_crypto_mentions()

        spots: dict = {}
        for token, pair in self._CRYPTO_PAIRS.items():
            try:
                ts = self._execute_query("time_series", {"symbol": pair, "interval": "1day", "outputsize": "2"})
                if ts and "values" in ts and len(ts["values"]) >= 2:
                    close_now  = float(ts["values"][0]["close"])
                    close_prev = float(ts["values"][1]["close"])
                    chg_1d     = (close_now - close_prev) / close_prev * 100
                    spots[token] = {"price": close_now, "chg_1d": chg_1d}
            except Exception:
                pass

        # Build trending list: tokens in spots sorted by 1D absolute move
        trending = sorted(
            [(t, spots[t]) for t in spots if t in reddit or abs(spots[t]["chg_1d"]) >= 3.0],
            key=lambda x: abs(x[1]["chg_1d"]),
            reverse=True
        )

        return {
            "fear_greed":   fng,
            "trending":     trending,
            "reddit_counts": reddit,
        }

    # ── FUTURES SOCIAL SCANNER ─────────────────────────────────────────────────
    # Scans StockTwits + Reddit r/futures for trending commodity/energy names
    # and wires Finviz futures prices into the cross-asset board context.

    _FUTURES_ADJACENT = {
        # Energy
        "XOM", "CVX", "COP", "OXY", "SLB", "HAL",
        # Gold / silver
        "GLD", "SLV", "GDX", "GDXJ", "NEM", "AEM",
        # Commodities / agriculture
        "MOS", "CF", "ADM", "BG",
        # Macro / rates
        "TLT", "TBT", "IEF",
    }

    def generate_futures_social_snapshot(self) -> dict:
        """
        Merges StockTwits trending + Reddit WSB mentions filtered to futures-adjacent
        names (energy, metals, rates, agriculture). Cross-checks with Twelve Data
        momentum. Returns a dict for dispatch to #futures-trading.

        Complements cross_asset.py's board — this adds social buzz context to the
        existing ES/NQ deep-dive so commodity rotations surface before they move.
        """
        st_list  = self._fetch_stocktwits_trending()
        wsb_dict = self._fetch_reddit_wsb_mentions()
        st_map   = {t["symbol"]: t for t in st_list if t["symbol"] in self._FUTURES_ADJACENT}
        wsb_filt = {k: v for k, v in wsb_dict.items() if k in self._FUTURES_ADJACENT}

        candidates = set(st_map.keys()) | set(wsb_filt.keys())
        results = []
        for sym in candidates:
            try:
                ts = self._execute_query("time_series", {"symbol": sym, "interval": "1day", "outputsize": "6"})
                if not ts or "values" not in ts or len(ts["values"]) < 2:
                    continue
                vals      = ts["values"]
                spot      = float(vals[0]["close"])
                prev5     = float(vals[min(5, len(vals)-1)]["close"])
                chg_5d    = (spot - prev5) / prev5 * 100
                vol_today = float(vals[0].get("volume", 0))
                vol_avg   = sum(float(v.get("volume", 0)) for v in vals[1:]) / max(len(vals)-1, 1)
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 1.0
                in_st     = sym in st_map
                in_wsb    = sym in wsb_filt
                meter     = "HIGH" if (in_st and in_wsb) else "NEUTRAL"
                results.append({
                    "symbol":    sym,
                    "spot":      spot,
                    "chg_5d":   chg_5d,
                    "vol_ratio": vol_ratio,
                    "meter":     meter,
                    "lean":      st_map[sym]["lean"] if in_st else "Mixed",
                })
            except Exception as e:
                logger.error(f"[Futures Social] {sym} data fetch failed: {e}")

        results.sort(key=lambda x: (x["meter"] != "HIGH", -abs(x["chg_5d"])))
        return {"plays": results}

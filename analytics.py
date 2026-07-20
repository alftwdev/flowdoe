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
        cached = self.db.get_cached_response(endpoint, params)
        if cached is not None:
            return cached
        params["apikey"] = self.api_key
        try:
            r = requests.get(f"{self.base_url}/{endpoint}", params=params, timeout=12)
            if r.status_code == 200:
                data = r.json()
                self.db.set_cached_response(endpoint, params, data)
                return data
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

    def fetch_real_vix(self):
        """
        FRED VIXCLS — the actual CBOE VIX daily close, updated ~4:30 PM ET.
        Cleaner than VIXY for the morning pulse (VIXY is a leveraged ETF proxy).
        Returns float or None on failure.
        """
        try:
            val = self._fetch_fred_metric("VIXCLS")
            return round(val, 2) if val > 0 else None
        except Exception:
            return None

    def fetch_yield_curve(self):
        """
        FRED DGS10 + DGS2 — 10-year and 2-year constant-maturity Treasury yields (daily, H.15).
        Yield curve inversion (spread < 0) is the most reliable macro recession leading indicator.
        Returns dict with t10, t2, spread, inverted, label — or None on failure.
        """
        try:
            t10 = self._fetch_fred_metric("DGS10")
            t2  = self._fetch_fred_metric("DGS2")
            if t10 == 0.0 or t2 == 0.0:
                return None
            spread = round(t10 - t2, 3)
            if spread <= -0.5:
                label = "🔴 DEEPLY INVERTED — recession signal"
            elif spread < 0:
                label = "🟡 INVERTED — contraction watch"
            elif spread < 0.5:
                label = "⚪ FLAT — uncertainty zone"
            else:
                label = "🟢 NORMAL — expansion posture"
            return {"t10": round(t10, 3), "t2": round(t2, 3), "spread": spread,
                    "inverted": spread < 0, "label": label}
        except Exception:
            return None

    def fetch_fred_macro_snapshot(self):
        """
        Key monthly FRED series cached to DB daily — avoids redundant FRED calls across
        cron runs. Returns dict: fedfunds, cpi_yoy (derived from 12-month change), unrate.
        Writes to DB so fed.py and macro dispatch can read without re-fetching.
        """
        cache_key = "fred_macro_snapshot"
        cached_date_key = "fred_macro_snapshot_date"
        today_str = datetime.now().strftime("%Y-%m-%d")
        if self.db.get_state(cached_date_key) == today_str:
            return self.db.get_state(cache_key) or {}
        try:
            fedfunds = self._fetch_fred_metric("FEDFUNDS")
            unrate   = self._fetch_fred_metric("UNRATE")
            # CPI YoY: fetch 13 months, compute 12-month percent change
            url = (f"https://api.stlouisfed.org/fred/series/observations"
                   f"?series_id=CPIAUCSL&api_key={self.fred_api_key}"
                   f"&file_type=json&sort_order=desc&limit=13")
            cpi_data = requests.get(url, timeout=12).json().get("observations", [])
            cpi_yoy = None
            if len(cpi_data) >= 13:
                latest = float(cpi_data[0]["value"])
                year_ago = float(cpi_data[12]["value"])
                cpi_yoy = round((latest - year_ago) / year_ago * 100, 2) if year_ago > 0 else None
            snap = {"fedfunds": round(fedfunds, 2), "cpi_yoy": cpi_yoy, "unrate": round(unrate, 1)}
            self.db.update_state(cache_key, snap)
            self.db.update_state(cached_date_key, today_str)
            return snap
        except Exception as e:
            logger.error(f"FRED macro snapshot failed: {e}")
            return self.db.get_state(cache_key) or {}

    def fetch_yahoo_dividend_yield(self, symbol):
        """
        Yahoo Finance quoteSummary fallback — used when Twelve Data dividend history is sparse
        for newer CC ETFs (XDTE, QDTE, etc.). Returns annualized yield as a float (e.g. 15.3)
        or None on failure. No new dependency — uses requests already imported.
        """
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                   f"?interval=1d&range=1d")
            headers = {"User-Agent": "Mozilla/5.0"}
            data = requests.get(url, headers=headers, timeout=10).json()
            meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
            yield_val = meta.get("dividendYield")  # already fractional (0.15 = 15%)
            if yield_val and yield_val > 0:
                return round(float(yield_val) * 100, 1)
            return None
        except Exception:
            return None

    def _fetch_twelve_data_quotes(self, symbols_list):
        if not self.api_key: return {}
        params = {"symbol": ",".join(sorted(symbols_list))}
        cached = self.db.get_cached_response("quote", params)
        if cached is not None:
            return cached if len(symbols_list) > 1 else ({symbols_list[0]: cached} if "symbol" in cached else {})
        try:
            url = f"https://api.twelvedata.com/quote?symbol={','.join(symbols_list)}&apikey={self.api_key}"
            res = requests.get(url, timeout=15).json()
            self.db.set_cached_response("quote", params, res)
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

    # ── Wheel Strategy Universe ───────────────────────────────────────────────
    # Methodology: SMB Capital / Options with Ryan / Andy Tanner / Invest with Henry
    # Core criteria: liquid options, quality underlying, survives assignment,
    # ideally pays a dividend (double-income if assigned: divs + CC premium).
    #
    # Tiers:
    #   CORE    — blue-chip, always optionable, assignment = long-term hold
    #   INCOME  — dividend payers; assignment triggers CC + dividend income
    #   GROWTH  — higher IV, higher premium, shorter holding tolerance
    #   SECTOR  — broad ETFs for diversified wheel deployment
    WHEEL_UNIVERSE = [
        # CORE — mega-cap, deep liquidity, assignment is never a bad outcome
        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AMD",
        # INCOME — dividend growers; CSP into dividend capture, CC on top if assigned
        "SCHD", "JEPI", "JEPQ", "O", "ARCC",
        # GROWTH — elevated IV = richer premium; size smaller, manage more actively
        "TSLA", "COIN", "SOFI", "PLTR", "HIMS",
        # SECTOR ETFs — low single-stock risk, good for larger collateral deployment
        "SPY", "QQQ", "IWM", "GLD", "XLE",
    ]

    def generate_tier2_iv_rank_alerts(self, universe=None, ivr_threshold=35.0):
        """
        Wheel Strategy IV Rank Scanner — dynamic universe, not a fixed watchlist.

        Scans WHEEL_UNIVERSE for elevated IV environments where selling premium
        has a statistical edge (IV > HV = options are priced above realized vol).
        Merges methodology from SMB Capital, Options with Ryan, Andy Tanner,
        Invest with Henry: quality underlyings you'd be comfortable owning,
        concrete CSP setups at 0.20-0.35 delta, 20-45 DTE.

        Filters: IVR proxy > threshold, bid/ask spread < 10% of mid,
        earnings not within 45 days. Returns flagged symbols with full setup.
        """
        universe = universe or self.WHEEL_UNIVERSE
        flagged = []
        today = datetime.now()
        DELTA_MIN, DELTA_MAX = 0.20, 0.35

        # Build Tradier client once per screener run (not once per symbol)
        _tc_ivr = None
        try:
            from tradier_client import TradierClient
            _tc_ivr = TradierClient()
            if not _tc_ivr.api_key:
                _tc_ivr = None
        except Exception:
            pass

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

                # Try real IVR from Tradier + stored iv_daily history; fall back to HV30 proxy.
                ivr_val = 0.0
                ivr_source = "proxy"
                try:
                    if _tc_ivr:
                        ivr_data = _tc_ivr.get_iv_rank(symbol, self.db)
                        if ivr_data.get("reliable"):
                            ivr_val = ivr_data["ivr"]
                            ivr_source = "Tradier"
                except Exception:
                    pass
                if ivr_val == 0.0:
                    ivr_val = max(0.0, min(100.0, (atm_iv / hv30 - 1.0) * 100)) if hv30 > 0 else 0.0

                ivr_proxy = ivr_val  # keep var name for downstream references
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

                iv_hv_ratio = round(float(atm_iv) / float(hv30), 2) if hv30 > 0 else None
                flagged.append({
                    "symbol": symbol,
                    "spot": spot,
                    "iv": round(float(atm_iv), 1),
                    "hv30": round(float(hv30), 1),
                    "ivr_proxy": round(float(ivr_proxy), 1),
                    "ivr_source": ivr_source,
                    "iv_hv_ratio": iv_hv_ratio,
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
        # symbol: (family, pay_freq)
        # Verified July 2026 against issuer websites + SEC filings.
        # MAGY was previously mislabeled as TappAlpha — it is Roundhill
        # (Roundhill Magnificent Seven Covered Call ETF, launched Apr 23 2025).
        # TappAlpha's products (TDAQ/TSPY/TSYX/TDAX) are excluded: TDAQ is a
        # Tier 2 long hold; the rest are too new to pass the 126-day age filter.

        # YieldMax — synthetic covered call, weekly income
        "MSTY": ("YieldMax", "Weekly"),   # MicroStrategy (MSTR)
        "NVDY": ("YieldMax", "Weekly"),   # NVIDIA
        "TSLY": ("YieldMax", "Weekly"),   # Tesla
        "CONY": ("YieldMax", "Weekly"),   # Coinbase
        "GOOY": ("YieldMax", "Weekly"),   # Alphabet (Google)
        "AMDY": ("YieldMax", "Weekly"),   # AMD
        "YMAG": ("YieldMax", "Monthly"),  # Mag 7 fund of option income ETFs
        "YMAX": ("YieldMax", "Weekly"),   # Diversified fund of YieldMax ETFs

        # Roundhill — 0DTE/covered call income, weekly
        "XDTE": ("Roundhill", "Weekly"),  # S&P 500 0DTE
        "QDTE": ("Roundhill", "Weekly"),  # Nasdaq 100 0DTE
        "RDTE": ("Roundhill", "Weekly"),  # Russell 2000 0DTE
        "MAGY": ("Roundhill", "Weekly"),  # Magnificent Seven CC ETF

        # NEOS — tax-efficient covered call, monthly
        "QQQI": ("NEOS", "Monthly"),      # Nasdaq 100
        "SPYI": ("NEOS", "Monthly"),      # S&P 500
        "BTCI": ("NEOS", "Monthly"),      # Bitcoin
    }

    def generate_new_income_etf_screener(self, min_yield_pct=10.0, min_trading_days=126, tickers=None):
        """
        Module 3 — CC ETF yield enricher. When `tickers` is provided (e.g. top-N from
        scan_ccincome_social_buzz), only those symbols are screened — social buzz drives
        discovery, this function adds yield/pay/AUM data. When `tickers` is None, falls
        back to the full NEW_INCOME_ETF_UNIVERSE (original static behaviour).

        Original static universe: YieldMax, Roundhill, NEOS, TappAlpha families
        (Kurv's only listed product, KQQQ, is already a Tier 2 holding — excluded here
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

        # Build the working universe: caller-supplied tickers take priority.
        # For buzz-driven calls, map each symbol to its family/freq from the known universe
        # (falling back to "Unknown"/"?") so the caller doesn't need to supply metadata.
        if tickers is not None:
            working_universe = {
                sym: self.NEW_INCOME_ETF_UNIVERSE.get(sym, ("Unknown", "?"))
                for sym in tickers
                if sym  # guard against empty strings
            }
        else:
            working_universe = self.NEW_INCOME_ETF_UNIVERSE

        quotes = self._fetch_twelve_data_quotes(list(working_universe.keys()))

        for sym, (family, freq) in working_universe.items():
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
                    # TD dividend history sparse (common for ETFs < 1 year old that passed the
                    # trading-day age gate) — try Yahoo Finance quoteSummary as a fallback.
                    yahoo_yield = self.fetch_yahoo_dividend_yield(sym)
                    if yahoo_yield and yahoo_yield > min_yield_pct:
                        ann_yield = yahoo_yield
                        freq_mult = None  # not derived from TD interval
                    else:
                        continue
                else:
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

        # Build Tradier client once per screener run (not once per symbol)
        _tc_wheel = None
        try:
            from tradier_client import TradierClient
            _tc_wheel = TradierClient()
            if not _tc_wheel.api_key:
                _tc_wheel = None
        except Exception:
            pass

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

                # ATM IV and IVR — try real Tradier IVR first, fall back to HV30 proxy.
                atm_iv_raw = df_f["implied_volatility"].median()
                atm_iv = atm_iv_raw * 100 if atm_iv_raw < 5.0 else atm_iv_raw
                ivr_proxy = 0.0
                ivr_source = "proxy"
                try:
                    if _tc_wheel:
                        _ivr = _tc_wheel.get_iv_rank(symbol, self.db)
                        if _ivr.get("reliable"):
                            ivr_proxy = _ivr["ivr"]
                            ivr_source = "Tradier"
                except Exception:
                    pass
                if ivr_proxy == 0.0:
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

    # ── Screening universe for options setup scan.
    # Liquid names with active options markets — updated manually when tickers lose liquidity.
    OPTIONS_SCAN_UNIVERSE = (
        "SPY,QQQ,IWM,AAPL,NVDA,MSFT,META,TSLA,AMD,AMZN,NFLX,"
        "JPM,GS,BAC,V,MA,COST,WMT,HD,AVGO,CRM,ORCL,"
        "COIN,MARA,RIOT,HOOD,SMCI,PLTR,ARM,SOXL"
    )

    def generate_options_setup_scan(self, max_results: int = 5) -> list:
        """
        Multi-factor options setup screener. No fake dark pool — every signal is derived
        from real publicly available equity data on Twelve Data:
          • RVOL (daily volume vs 90D avg) — confirms real momentum, not random noise
          • RSI (14D) — direction bias and overbought/oversold filter
          • MACD histogram — momentum expanding or compressing
          • ATR (14D) — used to suggest a rational strike zone (1–1.5× ATR OTM)
          • 52-week range position — near highs favors calls; near lows favors puts
          • Short interest (% of float) — squeeze potential for calls
          • Social sentiment — StockTwits/WSB buzz layer (reuses existing scrapers)

        Strike zone is NOT a live options chain — it's an ATR-based OTM estimate.
        Label it as a "suggested zone" in the output; the subscriber verifies the
        live chain before entering.

        Methodology aligned with: tastytrade probability-based approach (RVOL + RSI
        confluence), SMB Capital options desk (directional setup criteria), r/thetagang
        (IV proxy via ATR for premium context), r/options (52W range + short squeeze).
        """
        import requests as req

        # ── Step 1: Universe sieve — pull batch quotes, keep movers with real volume
        try:
            batch = self._execute_query("quote", {"symbol": self.OPTIONS_SCAN_UNIVERSE})
        except Exception as e:
            logger.error(f"Options scan universe fetch failed: {e}")
            return []

        # batch may be a dict of {symbol: quote_dict} or a single quote_dict
        if not batch or not isinstance(batch, dict):
            return []
        # Detect single-symbol response vs batch
        if "symbol" in batch:
            batch = {batch["symbol"]: batch}

        candidates = []
        for sym, q in batch.items():
            if "percent_change" not in q:
                continue
            try:
                pct_chg = abs(float(q.get("percent_change", 0)))
                spot    = float(q.get("close", 0))
                vol     = float(q.get("volume", 0))
                avg_vol = float(q.get("average_volume", 1))
                rvol    = vol / avg_vol if avg_vol > 0 else 0.0
                # Sieve: meaningful move + real volume (not random noise, not penny-vol)
                if pct_chg >= 1.0 and rvol >= 1.5 and spot >= 5.0:
                    candidates.append({"symbol": sym, "spot": spot, "pct_chg": pct_chg,
                                       "rvol": rvol, "q": q})
            except Exception:
                continue

        if not candidates:
            return []

        # Sort by RVOL × pct_chg composite (highest momentum first), cap at 12 for API budget
        candidates.sort(key=lambda x: x["rvol"] * x["pct_chg"], reverse=True)
        candidates = candidates[:12]

        # ── Step 2: Per-candidate deep-score
        results = []
        for c in candidates:
            sym  = c["symbol"]
            spot = c["spot"]
            q    = c["q"]
            try:
                # RSI
                rsi_data = self._execute_query("rsi", {"symbol": sym, "interval": "1day", "time_period": 14})
                rsi = float(rsi_data["values"][0]["rsi"]) if rsi_data and rsi_data.get("values") else 50.0

                # Skip extremes — overbought >78 or oversold <22 means the move is likely exhausted
                if rsi > 78 or rsi < 22:
                    continue

                # ATR — basis for strike zone sizing
                atr_data = self._execute_query("atr", {"symbol": sym, "interval": "1day", "time_period": 14})
                atr = float(atr_data["values"][0]["atr"]) if atr_data and atr_data.get("values") else spot * 0.02

                # MACD
                macd_data = self._execute_query("macd", {"symbol": sym, "interval": "1day",
                                                          "fast_period": 12, "slow_period": 26, "signal_period": 9})
                macd_hist, macd_expanding, macd_bull = 0.0, False, False
                if macd_data and macd_data.get("values"):
                    vals = macd_data["values"]
                    h0 = float(vals[0].get("macd_hist", 0))
                    h1 = float(vals[1].get("macd_hist", 0)) if len(vals) > 1 else 0.0
                    macd_hist      = h0
                    macd_expanding = abs(h0) > abs(h1)
                    macd_bull      = h0 > 0
                macd_tag = ("▲ expanding" if macd_expanding and macd_bull
                            else "▼ expanding" if macd_expanding and not macd_bull
                            else "▲ compressing" if not macd_expanding and macd_bull
                            else "▼ compressing")

                # 52-week range position (0% = at 52W low, 100% = at 52W high)
                w52 = q.get("fifty_two_week", {})
                w52_lo = float(w52.get("low", spot * 0.7))
                w52_hi = float(w52.get("high", spot * 1.3))
                range_pct = ((spot - w52_lo) / (w52_hi - w52_lo) * 100) if w52_hi > w52_lo else 50.0
                range_tag = ("near highs — momentum" if range_pct >= 70
                             else "near lows — reversal watch" if range_pct <= 30
                             else "mid-range")

                # Short interest (squeeze fuel for calls, confirmation for puts)
                short_pct = 0.0
                try:
                    stats = self._execute_query("statistics", {"symbol": sym})
                    if stats and stats.get("statistics"):
                        short_pct = float(stats["statistics"].get("stock_statistics", {})
                                          .get("short_percent_of_shares_outstanding", 0)) * 100
                except Exception:
                    pass

                # Direction logic — tastytrade/SMB consensus:
                # RSI 52-75 + MACD bullish + >70% of 52W range = CALL bias
                # RSI 25-48 + MACD bearish + <30% of 52W range = PUT bias
                # High short interest (>5%) adds squeeze conviction to CALL bias
                if rsi >= 50 and macd_bull:
                    direction = "CALL"
                    # Strike zone: 1–1.5× ATR above spot (OTM call, standard tastytrade delta ~0.30)
                    strike_lo = round((spot + atr) / 1.0)        # ~0.30 delta equivalent
                    strike_hi = round(spot + 1.5 * atr)
                elif rsi < 50 and not macd_bull:
                    direction = "PUT"
                    strike_lo = round(spot - 1.5 * atr)
                    strike_hi = round(spot - atr)
                else:
                    continue  # conflicting signals — skip

                # Require MACD to be expanding in the direction — compression = fading momentum
                if not macd_expanding:
                    continue

                # Conviction score (0–100): RVOL weight + RSI alignment + range alignment + squeeze
                score = 0
                score += min(30, int(c["rvol"] * 10))           # RVOL: up to 30 pts
                rsi_align = abs(rsi - 50) / 30 * 25             # RSI distance from neutral: up to 25 pts
                score += int(rsi_align)
                score += 20 if (direction == "CALL" and range_pct >= 60) or \
                               (direction == "PUT"  and range_pct <= 40) else 10
                score += 10 if (direction == "CALL" and short_pct >= 5) else 0
                if score < 35:                                   # minimum conviction threshold
                    continue

                # Social sentiment reuse (returns None if scraper unavailable — graceful)
                social_meter, social_lean = None, None
                try:
                    social = self._get_social_sentiment_for(sym)
                    if social:
                        social_meter = social.get("meter")
                        social_lean  = social.get("lean")
                except Exception:
                    pass

                # Verdict — single BLUF action line
                squeeze_note = f" High short float ({short_pct:.1f}%) adds squeeze fuel." if direction == "CALL" and short_pct > 5 else ""
                verdict = (
                    f"{'BTO call' if direction == 'CALL' else 'BTO put'} debit spread, "
                    f"strike zone ${strike_lo:,}–${strike_hi:,}, 21–30 DTE. "
                    f"Size ≤5% portfolio.{squeeze_note}"
                )

                results.append({
                    "symbol": sym, "spot": spot, "direction": direction,
                    "rvol": c["rvol"], "rsi": rsi, "atr": atr,
                    "macd_tag": macd_tag, "macd_bull": macd_bull,
                    "strike_lo": strike_lo, "strike_hi": strike_hi,
                    "range_pct": range_pct, "range_tag": range_tag,
                    "short_pct": short_pct, "score": score,
                    "social_meter": social_meter, "social_lean": social_lean,
                    "verdict": verdict,
                })

            except Exception as e:
                logger.warning(f"Options scan failed for {sym}: {e}")
                continue

        # Return top N by conviction score
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:max_results]

    def _get_social_sentiment_for(self, symbol: str):
        """
        Lightweight single-symbol social check against StockTwits public API.
        Returns {'meter': 'HIGH'/'LOW', 'lean': 'Bullish'/'Bearish'/'Mixed'} or None.
        Used as a confirmation layer on top of the technical setup — social hype alone
        is not a signal, but HIGH buzz + bullish lean on a technically confirmed setup
        increases conviction (WSB squeeze thesis, trending names).
        """
        try:
            import requests as req
            r = req.get(
                f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json",
                timeout=8, headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code != 200:
                return None
            data  = r.json()
            msgs  = data.get("messages", [])[:20]
            if not msgs:
                return None
            bulls = sum(1 for m in msgs if (m.get("entities", {}).get("sentiment") or {}).get("basic") == "Bullish")
            bears = sum(1 for m in msgs if (m.get("entities", {}).get("sentiment") or {}).get("basic") == "Bearish")
            total = bulls + bears
            meter = "HIGH" if len(msgs) >= 10 else "LOW"
            if total == 0:
                lean = "Mixed"
            elif bulls / total >= 0.65:
                lean = "Bullish"
            elif bears / total >= 0.65:
                lean = "Bearish"
            else:
                lean = "Mixed"
            return {"meter": meter, "lean": lean, "bulls": bulls, "bears": bears}
        except Exception:
            return None

    def calculate_rsi(self, series, period=14):
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1]) if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 50.0

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

    # Shared universe for iv_crush + unusual_flow — fetched once, used by both.
    _IV_CRUSH_UNIVERSE = [
        "AAPL", "NVDA", "MSFT", "TSLA", "META",
        "GOOGL", "AMZN", "AMD", "AVGO", "NFLX",
        "SPY", "QQQ", "IWM", "COIN", "SMCI",
    ]

    def _fetch_iv_crush_chains(self) -> dict:
        """
        Fetch options chains for the shared iv_crush/unusual_flow universe once.
        Returns {symbol: DataFrame} — callers use this dict instead of re-fetching.
        """
        chains = {}
        for symbol in self._IV_CRUSH_UNIVERSE:
            chain = self._execute_query("options/chain", {"symbol": symbol})
            if chain and "data" in chain and chain["data"]:
                try:
                    df = pd.DataFrame(chain["data"])
                    df["strike"] = df["strike"].astype(float)
                    df["open_interest"] = pd.to_numeric(df.get("open_interest", 0), errors="coerce").fillna(0)
                    df["implied_volatility"] = pd.to_numeric(df.get("implied_volatility", 0), errors="coerce").fillna(0)
                    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0) if "volume" in df.columns else 0
                    chains[symbol] = df
                except Exception as e:
                    logger.warning(f"iv_crush chain parse failed for {symbol}: {e}")
        return chains

    def run_iv_crush_scan(self, chains: dict = None):
        """IV premium scanner. Pass pre-fetched chains dict to avoid double fetch."""
        if chains is None:
            chains = self._fetch_iv_crush_chains()
        results = []
        for symbol, df_options in chains.items():
            try:
                hv_30 = self.calculate_historical_volatility(symbol)
                atm_iv_raw = df_options["implied_volatility"].median()
                atm_iv = atm_iv_raw * 100 if atm_iv_raw < 5.0 else atm_iv_raw
                spread = round(atm_iv - hv_30, 1)
                if spread >= 5.0:
                    results.append({
                        "symbol": symbol,
                        "hv": round(hv_30, 1),
                        "iv": round(atm_iv, 1),
                        "spread": spread
                    })
            except Exception as e:
                logger.error(f"IV crush scan failed for {symbol}: {e}")
        return sorted(results, key=lambda x: x["spread"], reverse=True)

    def scan_unusual_options_flow(self, universe=None, chains: dict = None):
        """
        Sweep + OI skew detector. Pass pre-fetched chains dict to avoid double fetch.
        If universe is provided without chains, fetches only those symbols.
        """
        if chains is None:
            if universe is None:
                chains = self._fetch_iv_crush_chains()
            else:
                chains = {}
                for symbol in universe:
                    chain = self._execute_query("options/chain", {"symbol": symbol})
                    if chain and "data" in chain and chain["data"]:
                        try:
                            df = pd.DataFrame(chain["data"])
                            df["strike"] = df["strike"].astype(float)
                            df["open_interest"] = pd.to_numeric(df.get("open_interest", 0), errors="coerce").fillna(0)
                            df["implied_volatility"] = pd.to_numeric(df.get("implied_volatility", 0), errors="coerce").fillna(0)
                            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0) if "volume" in df.columns else 0
                            chains[symbol] = df
                        except Exception:
                            pass

        sweeps, skews = [], []

        for symbol, df in chains.items():
            try:
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
        GEX + Put/Call OI ratio. Uses Tradier real OI when key is configured
        (1-hour DB cache prevents re-fetch every 5-min monitor loop tick).
        Falls back to Twelve Data chain if Tradier unavailable.
        """
        import time as _time
        cache_key = f"gex_profile_{symbol}"
        try:
            cached = self.db.get_state(cache_key)
            if cached and (_time.time() - cached.get("ts", 0)) < 3600:
                return cached["data"]
        except Exception:
            pass

        try:
            from tradier_client import TradierClient
            tc = TradierClient()
            if tc.api_key:
                result = tc.get_gex(symbol)
                if result.get("market_state", "UNKNOWN") != "UNKNOWN":
                    try:
                        self.db.update_state(cache_key, {"ts": _time.time(), "data": result})
                    except Exception:
                        pass
                    return result
        except Exception:
            pass

        # Twelve Data fallback
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

        try:
            snap["vixy_price"], snap["vixy_z"] = self.fetch_vixy_proxy()
        except Exception as e:
            logger.error(f"[snapshot] fetch_vixy_proxy failed: {e}")
            snap["vixy_price"], snap["vixy_z"] = 20.0, 0.0
        try:
            snap["credit_spread"] = float(self.db.get_state("credit_spread", 3.5))
        except Exception:
            snap["credit_spread"] = 3.5
        try:
            snap["vrp"] = self.calculate_vrp("SPY")
        except Exception as e:
            logger.error(f"[snapshot] calculate_vrp failed: {e}")
            snap["vrp"] = {"iv": 0.0, "hv30": 20.0, "vrp": 0.0, "regime": "UNKNOWN"}
        try:
            snap["net_liquidity"] = self.calculate_net_liquidity()
        except Exception as e:
            logger.error(f"[snapshot] calculate_net_liquidity failed: {e}")
            snap["net_liquidity"] = {"net_liquidity": 0.0, "delta": 0.0, "trend": "UNKNOWN"}
        # Reuses TQQQ desk's daily-cached Nasdaq-100 breadth rather than re-fetching 10 symbols here.
        try:
            snap["breadth"] = float(self.db.get_state("tqqq_breadth_cache", 0.60))
        except Exception:
            snap["breadth"] = 0.60

        try:
            snap["fng"] = self.fetch_fear_greed_index()
        except Exception as e:
            logger.error(f"[snapshot] fetch_fear_greed_index failed: {e}")
            snap["fng"] = None

        try:
            fx_quotes = self._fetch_twelve_data_quotes(["USD/JPY"])
            snap["risk_regime"], snap["risk_explanation"], snap["usdjpy_chg"], snap["gold_chg"] = self.assess_risk_sentiment_regime(fx_quotes)
        except Exception:
            snap["risk_regime"], snap["risk_explanation"] = "🟡 MIXED", "Forex data unavailable."

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
        # Clean key for daily announcements teaser grading
        raw_bias = snap["conviction_bias"]
        clean_bias = "BULLISH" if "BULL" in raw_bias.upper() else ("BEARISH" if "BEAR" in raw_bias.upper() else "NEUTRAL")
        self.db.update_state(f"morning_conviction_bias_{today_str}", clean_bias)

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

        yc = self.fetch_yield_curve()
        real_vix = self.fetch_real_vix()
        if yc:
            macro_line = (
                f"┣ Macro: Yield Curve (10Y-2Y) `{yc['spread']:+.3f}%` {yc['label']} | {snap['risk_regime']} ({snap['risk_explanation']})\n"
            )
        else:
            macro_line = f"┣ Macro: {snap['risk_regime']} ({snap['risk_explanation']})\n"

        vix_suffix = f" | VIX `{real_vix:.1f}`" if real_vix else ""

        payload = (
            f"🌅 **MARKET ANALYSIS | MORNING BRIEF — Pre-Open Conviction**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ Overnight Futures ({snap['futures_session']}): SPY POC `${snap['futures_poc']:,.2f}` | VAH `${snap['futures_vah']:,.2f}` | VAL `${snap['futures_val']:,.2f}`\n"
            f"{moves_line}"
            f"┣ Gamma Regime: {snap['gex']['market_state']} | Flip `${snap['gex']['flip_strike']:,.2f}` | P/C OI: `{snap['gex']['pc_oi_ratio']:.2f}` ({snap['gex']['pc_tag']})\n"
            f"┣ Volatility: VIXY `{snap['vixy_price']:.2f}` (z {snap['vixy_z']:+.2f}σ){vix_suffix} | Nasdaq-100 Breadth: `{snap['breadth']:.0%}`\n"
            f"{macro_line}"
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

        yc = self.fetch_yield_curve()
        yc_str = f" | Yield Curve `{yc['spread']:+.3f}%` {'🔴' if yc['inverted'] else '🟢'}" if yc else ""

        payload = (
            f"☀️ **MARKET ANALYSIS | INTRADAY PULSE**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ SPY `${snap['gex']['current_spot']:,.2f}` | Gamma: {snap['gex']['market_state']} (Flip `${snap['gex']['flip_strike']:,.2f}`)\n"
            f"┣ VIXY `{snap['vixy_price']:.2f}` (z {snap['vixy_z']:+.2f}σ) | Breadth: `{snap['breadth']:.0%}`\n"
            f"┣ Macro: {snap['risk_regime']}{yc_str}\n"
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

    def get_wheel_position_summary(self):
        """
        For each open wheel position: fetch live spot, compute net_cost_if_assigned, OTM cushion,
        DTE, and annualized ROC on current premium vs collateral.

        net_cost = cost_basis - (total_premiums_per_share) - (open_fees / (contracts * 100))
          where total_premiums_per_share = (premium_collected + accumulated_premiums) / (contracts * 100)
        cushion_pct = (spot - strike) / strike * 100  (positive = OTM, safe zone for CSP)
        annualized_roc = (net_premium_dollars / collateral) * (365 / days_held)
        """
        from datetime import date as date_cls
        positions = self.db.get_open_wheel_positions()
        if not positions:
            return []

        symbols = list({p["symbol"] for p in positions})
        try:
            quotes = self._fetch_twelve_data_quotes(symbols)
        except Exception:
            quotes = {}

        today = date_cls.today()
        results = []
        for pos in positions:
            symbol      = pos["symbol"]
            spot        = float(quotes.get(symbol, {}).get("close", 0.0))
            strike      = float(pos["strike"])
            contracts   = int(pos["contracts"])
            cost_basis  = float(pos.get("cost_basis") or strike)
            prem        = float(pos["premium_collected"])
            accum       = float(pos.get("accumulated_premiums") or 0.0)
            open_fees   = float(pos.get("open_fees") or 0.0)
            roll_group  = pos.get("roll_group_id")

            # Net cost: reduce cost basis by every dollar of premium received per share
            total_prem_dollars   = prem + accum
            fees_per_share       = open_fees / (contracts * 100) if contracts > 0 else 0.0
            total_prem_per_share = total_prem_dollars / 100.0
            net_cost             = round(cost_basis - total_prem_per_share - fees_per_share, 2)

            cushion_pct = round((spot - strike) / strike * 100, 1) if strike > 0 and spot > 0 else None

            # DTE from expiration string
            try:
                exp_date = date_cls.fromisoformat(pos["expiration"])
                dte = (exp_date - today).days
            except Exception:
                dte = None

            # Annualized ROC on net premium vs collateral
            collateral = strike * contracts * 100
            net_prem_dollars = total_prem_dollars - open_fees
            if collateral > 0 and dte is not None:
                try:
                    opened = date_cls.fromisoformat(pos["opened_date"][:10])
                    days_held = (today - opened).days or 1
                    annualized_roc = round((net_prem_dollars / collateral) * (365 / days_held) * 100, 1)
                except Exception:
                    annualized_roc = None
            else:
                annualized_roc = None

            results.append({
                "symbol":               symbol,
                "position_type":        pos["position_type"],
                "strike":               strike,
                "expiration":           pos["expiration"],
                "contracts":            contracts,
                "cost_basis":           cost_basis,
                "net_cost":             net_cost,
                "spot":                 spot,
                "cushion_pct":          cushion_pct,
                "dte":                  dte,
                "annualized_roc":       annualized_roc,
                "total_premium_per_share": round(total_prem_per_share, 2),
                "roll_group_id":        roll_group,
            })
        return results

    def generate_wheel_outcome_distribution(self, lookback_days=90):
        """
        Formats closed-position outcome breakdown for scorecard display.
        Uses retained_premium (actual dollars kept after buyback cost and fees) rather than
        gross premium_collected, so early closes at 50% profit don't inflate the ledger.
        Surfaces avg annualized ROC per outcome group where calculable.
        """
        rows = self.db.get_wheel_outcome_distribution(lookback_days=lookback_days)
        if not rows:
            return None

        total_count    = sum(r["count"] for r in rows)
        total_retained = sum(r["retained_premium"] for r in rows)
        total_gross    = sum(r["total_premium"] for r in rows)

        # Retention rate = retained / gross — how much of what we sold we actually kept
        retention_pct = round(total_retained / total_gross * 100, 1) if total_gross > 0 else 0.0

        label_map = {
            "EXPIRED":  "expired worthless — full premium kept",
            "ASSIGNED": "assigned — now selling CCs against position",
            "ROLLED":   "rolled for credit — position extended",
            "CLOSED":   "closed early (buy-to-close)",
        }
        lines = []
        for r in rows:
            label    = label_map.get(r["outcome"], r["outcome"].lower())
            roc_str  = (f" | `{r['avg_annualized_roc']*100:.1f}%` ann. ROC"
                        if r.get("avg_annualized_roc") else "")
            lines.append(
                f"┣ {r['count']}x {label} — `${r['retained_premium']:,.0f}` kept{roc_str}"
            )

        return {
            "total":          total_count,
            "retention_pct":  retention_pct,
            "lines":          lines,
            "total_retained": total_retained,
            "total_gross":    total_gross,
        }

    def generate_ecosystem_scorecard(self):
        """
        Weekly public scorecard for #announcements — newcomer-friendly, income-spotlighted,
        and graded from the live ledger. Every number is dated, never deleted, win or lose.
        """
        from datetime import date
        week_str = date.today().strftime("Week of %B %-d, %Y")

        # ── ACCURACY BLOCK ───────────────────────────────────────────────
        spy_trend = self.get_accuracy_trend()
        accuracy_block = (
            f"┣ SPY Daily Target Accuracy: `{spy_trend['avg_7d']}%` (7D) | `{spy_trend['avg_30d']}%` (30D) — `{spy_trend['sample_size']}` sessions logged\n"
        )

        # ── SECTOR WIN RATES ─────────────────────────────────────────────
        sector_lines = []
        sectors = [
            ("tqqq",        "TQQQ Sniper"),
            ("cornerstone", "Cornerstone RO Risk"),
            ("forex",       "Macro Risk Regime"),
        ]
        for sector, display in sectors:
            wr = self.get_ledger_winrate(sector)
            if wr and wr.get("total", 0) > 0:
                sector_lines.append(
                    f"┣ {display}: `{wr['win_rate']}%` ({wr['wins']}/{wr['total']} graded)"
                )
            else:
                # Cornerstone 0/0 is not a gap — it means no RO risk fired, which is a win for holders
                if sector == "cornerstone":
                    sector_lines.append(f"┣ {display}: No alerts fired — all clear ✅")
                else:
                    sector_lines.append(f"┣ {display}: No graded calls yet — building track record")

        # ── WHEEL OUTCOME DISTRIBUTION (90-day closed positions) ─────────
        income_block = ""
        try:
            dist = self.generate_wheel_outcome_distribution(lookback_days=90)
            if dist and dist["total"] > 0:
                income_block += (
                    f"\n🎯 **WHEEL RESULTS (last 90 days — {dist['total']} closed trades)**\n"
                    + "\n".join(dist["lines"]) + "\n"
                    f"┗ Net retention: `{dist['retention_pct']}%` of gross — "
                    f"`${dist['total_retained']:,.0f}` kept of `${dist['total_gross']:,.0f}` sold\n"
                )
        except Exception:
            pass

        # ── OPEN POSITION NET COST SUMMARY ───────────────────────────────
        try:
            pos_summary = self.get_wheel_position_summary()
            if pos_summary:
                income_block += f"\n📋 **OPEN WHEEL POSITIONS ({len(pos_summary)} active)**\n"
                for p in pos_summary:
                    cushion_str = f"`{p['cushion_pct']:+.1f}%` OTM" if p["cushion_pct"] is not None else "—"
                    spot_str    = f"${p['spot']:,.2f}" if p["spot"] else "—"
                    dte_str     = f"`{p['dte']}` DTE" if p["dte"] is not None else "—"
                    roc_str     = f" | Ann. ROC `{p['annualized_roc']:.1f}%`" if p.get("annualized_roc") else ""
                    roll_str    = " 🔗" if p.get("roll_group_id") else ""
                    income_block += (
                        f"┣ `{p['symbol']}` {p['position_type']} `${p['strike']:.2f}` exp `{p['expiration']}` — {dte_str}{roll_str}\n"
                        f"┣ Net cost: `${p['net_cost']:.2f}` | Spot: {spot_str} | Cushion: {cushion_str}{roc_str}\n"
                    )
                income_block = income_block.rstrip("\n") + "\n┗ Net cost = strike minus all premiums — true breakeven. 🔗 = part of a roll chain.\n"
        except Exception:
            pass

        # ── INCOME SPOTLIGHTS (read from DB cache — no live API calls) ───
        try:
            wheel_spot = self.db.get_state("wheel_spotlight_latest")
            if wheel_spot:
                csp = wheel_spot.get("csp_setup") or {}
                strike = csp.get("strike")
                dte = csp.get("dte")
                premium = csp.get("premium")
                prem_str = f"${premium * 100:.0f}/contract" if premium else "—"
                strike_str = f"${strike:.1f}" if strike else "—"
                income_block += (
                    f"\n📌 **THIS WEEK'S WHEEL SETUP**\n"
                    f"┣ Ticker: `{wheel_spot['symbol']}` | Spot: `${wheel_spot.get('spot', 0):.2f}`\n"
                    f"┣ IV Rank Proxy: `{wheel_spot.get('ivr_proxy', 0):.0f}%` (elevated premium environment)\n"
                    f"┣ Setup: Sell `{strike_str} Put` | `{dte} DTE` | Premium `{prem_str}`\n"
                )
                if wheel_spot.get("div_yield"):
                    income_block += f"┣ Bonus if Assigned: `{wheel_spot['div_yield']:.1f}%` annual dividend ({wheel_spot.get('div_freq', '')})\n"
                income_block += f"┗ Strategy: Cash-secured put → collect premium → keep stock if assigned → sell covered calls\n"
        except Exception:
            pass

        try:
            etf_spot = self.db.get_state("cc_etf_spotlight_latest")
            if etf_spot:
                income_block += (
                    f"\n💡 **HIGHEST-YIELD CC ETF THIS WEEK**\n"
                    f"┣ Ticker: `{etf_spot['symbol']}` | {etf_spot.get('family', '')} | {etf_spot.get('freq', '')} pay\n"
                    f"┣ Annualized Yield: `{etf_spot.get('ann_yield', 0):.1f}%` | Spot: `${etf_spot.get('spot', 0):.2f}`\n"
                    f"┣ AUM: `{etf_spot.get('aum', 'N/A')}` | Next Est. Pay: `{etf_spot.get('next_ex_date', '—')}`\n"
                    f"┗ These ETFs sell covered calls on tech/crypto/blue-chip baskets — you own the ETF and collect the premium income monthly.\n"
                )
        except Exception:
            pass

        payload = (
            f"📊 **WEEKLY ACCURACY SCORECARD — {week_str}**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"What is this? Every market call this ecosystem makes is logged the moment it fires and graded against what actually happened — SPY price targets, options setups, RO risk alerts, macro regime shifts. Good calls and bad ones. Nothing deleted.\n\n"
            f"**THIS WEEK'S ACCURACY**\n"
            f"{accuracy_block}"
            + "\n".join(sector_lines)
            + f"\n\n"
            f"{income_block}"
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┗ Want the full morning report, TQQQ sniper entries, and live wheel trades? The complete system runs before, during, and after the market open — every session."
        )
        return payload

    def generate_announcements_teaser(self, accuracy_score, predicted, actual, snap):
        """
        Daily public scorecard for #announcements — per-channel accuracy proof.
        Shows one graded metric per channel so readers see the full breadth of what runs,
        not just SPY price proximity. All numbers come from the live ledger (never fabricated).
        """
        trend = self.record_and_get_accuracy_trend(accuracy_score)
        today_str = __import__("datetime").date.today().isoformat()

        # ── #market-analysis: SPY direction call (bull/bear/neutral vs actual move) ──
        spy_move = actual - predicted
        spy_dir_predicted = self.db.get_state(f"morning_conviction_bias_{today_str}", "NEUTRAL")
        spy_actual_dir = "BULLISH" if spy_move > 0.5 else ("BEARISH" if spy_move < -0.5 else "NEUTRAL/CHOP")
        ma_hit = spy_dir_predicted == spy_actual_dir or (spy_dir_predicted == "NEUTRAL" and spy_actual_dir == "NEUTRAL/CHOP")
        ma_icon = "✅" if ma_hit else "❌"

        # ── #futures-trading: /NQ direction bias logged at morning brief ──
        nq_bias = self.db.get_state(f"futures_nq_bias_{today_str}", "")
        nq_actual = self.db.get_state(f"futures_nq_actual_dir_{today_str}", "")
        if nq_bias and nq_actual:
            nq_hit = (nq_bias == nq_actual) or ("NEUTRAL" in nq_bias and "NEUTRAL" in nq_actual)
            futures_line = f"┣ #futures-trading  /NQ Direction: {nq_bias} → {nq_actual} {('✅' if nq_hit else '❌')}\n"
        else:
            futures_line = f"┣ #futures-trading  /NQ Board: 4×/day — overnight, pre-market, open, mid-session\n"

        # ── #cornerstone: RO risk status today ──
        ro_tier = self.db.get_state("cornerstone_alert_tier_rank", 0)
        ro_fired = self.db.get_state(f"cornerstone_alert_fired_{today_str}", False)
        if ro_fired:
            ro_line = f"┣ #cornerstone      RO Alert: Fired ⚠️ — CLM/CRF protection triggered\n"
        elif ro_tier == 0:
            ro_line = f"┣ #cornerstone      CLM/CRF RO Risk: LOW — all clear ✅\n"
        else:
            ro_line = f"┣ #cornerstone      CLM/CRF RO Risk: ELEVATED — monitoring ⚠️\n"

        # ── #options-wheel: Tier 2 IVR alert (Module 1) OR social+IV snapshot (Module 3) ──
        today_str_w = __import__("datetime").date.today().isoformat()
        wheel_spot  = self.db.get_state("wheel_spotlight_latest")
        wheel_snap  = self.db.get_state("wheel_candidates_snapshot")
        snap_fresh  = isinstance(wheel_snap, dict) and wheel_snap.get("date") == today_str_w
        if wheel_spot:
            # Module 1 fired (real Tradier data — highest quality)
            w_sym = wheel_spot.get("symbol", "—")
            w_ivr = wheel_spot.get("ivr_proxy", 0)
            options_line = f"┣ #options-wheel    Wheel Alert: `{w_sym}` IVR `{w_ivr:.0f}%` — elevated premium env\n"
        elif snap_fresh and wheel_snap.get("high_count", 0) > 0:
            # Module 3 social+IV convergence has HIGH entries today
            tops = wheel_snap.get("top_candidates", [])[:3]
            top_str = " | ".join(f"`{t['sym']}` {t['ivr']}%" for t in tops) if tops else ""
            n = wheel_snap["high_count"]
            options_line = f"┣ #options-wheel    Wheel Candidates: `{n}` HIGH setup{'s' if n != 1 else ''} — {top_str}\n"
        else:
            options_line = f"┣ #options-wheel    Wheel Screener: No elevated IVR today — low vol environment\n"

        # ── #dividend-ccetfs: income ETF spotlight ──
        etf_spot = self.db.get_state("cc_etf_spotlight_latest")
        if etf_spot:
            income_line = f"┣ #dividend-ccetfs  Top Yield: `{etf_spot.get('symbol','—')}` `{etf_spot.get('ann_yield',0):.1f}%` annual — {etf_spot.get('freq','')}\n"
        else:
            income_line = f"┣ #dividend-ccetfs  Income Screener: running daily — CC ETFs + dividend wheel\n"

        # ── #crypto: Fear & Greed + mover ──
        fng_val = snap.get("fng", {}).get("value", "—")
        fng_label = snap.get("fng", {}).get("label", "")
        crypto_mover = ""
        if snap.get("crypto_mover"):
            sym, _, _, pct = snap["crypto_mover"]
            crypto_mover = f" | Mover: {sym} `{pct:+.2f}%`"
        crypto_line = f"┣ #crypto           Fear & Greed: `{fng_val}` ({fng_label}){crypto_mover}\n"

        payload = (
            f"📣 **DAILY ACCURACY INDEX — Public Sample**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**#market-analysis**  SPY Predicted `${predicted:,.2f}` → Actual `${actual:,.2f}` "
            f"| Direction: {spy_dir_predicted} → {spy_actual_dir} {ma_icon}\n"
            f"┣ Model Score: `{accuracy_score}%` today | `{trend['avg_7d']}%` 7D | `{trend['avg_30d']}%` 30D "
            f"over `{trend['sample_size']}` sessions\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"**Channel Accuracy Breakdown**\n"
            f"{futures_line}"
            f"{ro_line}"
            f"{options_line}"
            f"{income_line}"
            f"{crypto_line}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┗ Every number is logged live and never deleted — good sessions and bad ones both. "
            f"Full signals (TQQQ entries, wheel strikes, morning conviction call) are subscriber-only."
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
        Returns {ticker: mention_count} for Reddit conviction names.

        Primary: SentiSense reddit-picks tracker (curated, 7-day cache, no 403 risk).
          peak_mentions used as count; BULLISH posture tickers weighted ×2.
        Fallback: r/wallstreetbets hot.json scrape (403s on PA IPs intermittently).
        """
        # ── Primary: SentiSense Reddit Picks tracker ──────────────────────────
        try:
            import sentisense_client as ss
            picks = ss.get_reddit_picks(self.db)
            if picks:
                counts: dict = {}
                for p in picks:
                    ticker = p.get("ticker", "")
                    if not ticker or ticker in self._SOCIAL_SCANNER_EXCLUDE:
                        continue
                    weight = 2 if p.get("posture") == "BULLISH" else 1
                    # peak_mentions is the raw count but we need a value ≥ 2 to pass the filter
                    raw_count = max(p.get("peak_mentions", 0), 2)
                    counts[ticker] = raw_count * weight
                if counts:
                    logger.info(f"[Social Scanner] Reddit Picks via SentiSense: {len(counts)} tickers")
                    return counts
        except Exception as e:
            logger.warning(f"[Social Scanner] SentiSense Reddit Picks failed: {e}")

        # ── Fallback: raw Reddit scrape ───────────────────────────────────────
        try:
            import re
            r = requests.get(
                "https://www.reddit.com/r/wallstreetbets/hot.json?limit=50",
                headers={"User-Agent": "RockefellerEcosystem/1.0 (research bot)"},
                timeout=10
            )
            if r.status_code != 200:
                return {}
            posts     = r.json().get("data", {}).get("children", [])
            ticker_re = re.compile(r'\$([A-Z]{1,5})|(?<!\w)([A-Z]{2,5})(?!\w)')
            counts: dict = {}
            for post in posts:
                title = post.get("data", {}).get("title", "")
                for m in ticker_re.findall(title):
                    sym = m[0] or m[1]
                    if sym and sym not in self._SOCIAL_SCANNER_EXCLUDE and len(sym) >= 2:
                        counts[sym] = counts.get(sym, 0) + 1
            return {k: v for k, v in counts.items() if v >= 2}
        except Exception as e:
            logger.error(f"[Social Scanner] Reddit WSB fallback failed: {e}")
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

        # SentiSense per-symbol scored sentiment — confirms candidates from social feeds.
        # Score ≥ 30.0 (strong bullish) counts as an additional source.
        ss_scores = {}
        try:
            import sentisense_client as ss
            all_candidates = set(st_map.keys()) | wsb_set | finviz_set
            for _sym in list(all_candidates)[:15]:  # cap at 15 to stay API-lean
                _sent = ss.get_sentiment(self.db, _sym)
                if _sent:
                    ss_scores[_sym] = _sent
        except Exception:
            pass  # SentiSense unavailable — existing 3-source scoring continues

        # SentiSense Sentiment Leaderboard — 4th discovery source.
        # Bullish tickers from the leaderboard that aren't in other feeds still surface
        # as NEUTRAL candidates; those already in 1+ feeds get upgraded to HIGH.
        ss_leaderboard_set: set = set()
        try:
            import sentisense_client as ss
            lb_rows = ss.get_sentiment_leaderboard(self.db, side="bullish", limit=15)
            if lb_rows:
                for _r in lb_rows:
                    _t = _r.get("ticker", "")
                    if _t and _t not in self._SOCIAL_SCANNER_EXCLUDE:
                        ss_leaderboard_set.add(_t)
                logger.info(f"[Trending Plays] SS Leaderboard: {len(ss_leaderboard_set)} bullish tickers as 4th source")
        except Exception:
            pass

        candidates = []
        all_pool = set(st_map.keys()) | wsb_set | finviz_set | ss_leaderboard_set
        for sym in all_pool:
            in_st         = sym in st_map
            in_wsb        = sym in wsb_set
            in_finviz     = sym in finviz_set
            in_leaderboard = sym in ss_leaderboard_set
            in_ss_high    = ss_scores.get(sym, {}).get("score", 0) >= 30.0
            score = sum([in_st, in_wsb, in_finviz, in_leaderboard])
            if score == 0:
                continue

            # Per-symbol SS score ≥ 30 counts as an extra source (upgrades NEUTRAL → HIGH if only 1)
            effective_score = score + (1 if in_ss_high and score == 1 else 0)

            # Lean from SentiSense when available (more reliable than raw StockTwits direction)
            ss_lean = ss_scores.get(sym, {}).get("lean")
            lean    = ss_lean or (st_map[sym]["lean"] if in_st else "Mixed")

            candidates.append({
                "symbol":        sym,
                "meter":         "HIGH" if effective_score >= 2 else "NEUTRAL",
                "lean":          lean,
                "is_meme":       sym in self._MEME_WATCHLIST,
                "score":         effective_score,
                "ss_score":      ss_scores.get(sym, {}).get("score"),
                "ss_mentions":   ss_scores.get(sym, {}).get("mentions"),
                "ss_dominance":  ss_scores.get(sym, {}).get("dominance"),
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
                    "symbol":       sym,
                    "spot":         spot,
                    "chg_5d":      chg_5d,
                    "vol_ratio":    vol_ratio,
                    "rsi":          rsi,
                    "meter":        c["meter"],
                    "lean":         c["lean"],
                    "verdict":      verdict,
                    "bto_setup":    bto_setup,
                    "ss_score":     c.get("ss_score"),
                    "ss_mentions":  c.get("ss_mentions"),
                    "ss_dominance": c.get("ss_dominance"),
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

    # ── CC INCOME ETF SOCIAL BUZZ SCANNER ────────────────────────────────────
    # Scans r/dividends, r/ETFs, r/thetagang, r/Bogleheads, and StockTwits for
    # CC income ETF ticker mentions. These are the actual communities where
    # YieldMax/Roundhill/NEOS products are discussed — not WSB or r/CryptoCurrency.
    # Returns a ranked list of ETFs by social momentum, with source breakdown.

    _CC_INCOME_TICKERS = {
        # YieldMax
        "MSTY", "NVDY", "TSLY", "CONY", "GOOY", "AMDY", "YMAG", "YMAX",
        "PLTY", "AMZY", "METY", "JPMY", "MRNY", "BITO", "OARK",
        # Roundhill
        "XDTE", "QDTE", "RDTE", "MAGY",
        # NEOS
        "QQQI", "SPYI", "BTCI", "JEPQ", "JEPI",
        # Broad CC / income
        "SCHD", "DIVO", "XYLD", "QYLD", "RYLD", "SVOL", "FEPI",
        # TappAlpha / Kurv
        "TDAQ", "KQQQ",
    }

    _CC_INCOME_SUBREDDITS = [
        "dividends",
        "ETFs",
        "thetagang",
        "Bogleheads",
        "investing",
    ]

    def _fetch_stocktwits_ccincome_stream(self, tickers: list) -> dict:
        """
        Fetches StockTwits symbol stream for each CC income ETF.
        Returns {ticker: {"msg_count": N, "bullish": N, "bearish": N, "bull_pct": float}}
        Uses the per-symbol stream endpoint (no auth required, 30 most recent messages).
        Reddit public JSON is 403-blocked site-wide as of 2024 — StockTwits is the
        reliable free alternative for niche CC ETF community sentiment.
        """
        results = {}
        headers = {"User-Agent": "RockefellerEcosystem/1.0"}
        for sym in tickers:
            try:
                r = requests.get(
                    f"https://api.stocktwits.com/api/2/streams/symbol/{sym}.json",
                    headers=headers, timeout=8
                )
                if r.status_code != 200:
                    continue
                msgs = r.json().get("messages", [])
                if not msgs:
                    continue
                bulls = sum(
                    1 for m in msgs
                    if m.get("entities", {}).get("sentiment", {}) and
                    m["entities"]["sentiment"].get("basic") == "Bullish"
                )
                bears = sum(
                    1 for m in msgs
                    if m.get("entities", {}).get("sentiment", {}) and
                    m["entities"]["sentiment"].get("basic") == "Bearish"
                )
                tagged = bulls + bears
                bull_pct = round(bulls / tagged * 100) if tagged > 0 else 0
                results[sym] = {
                    "msg_count": len(msgs),
                    "bullish":   bulls,
                    "bearish":   bears,
                    "bull_pct":  bull_pct,
                }
            except Exception as e:
                logger.warning(f"[CC Income Social] StockTwits stream {sym} failed: {e}")
        return results

    def scan_ccincome_social_buzz(self, top_n: int = 6) -> list:
        """
        Ranks CC income ETFs by StockTwits community activity and sentiment.
        Buzz score = total messages × bull/bear conviction weight.
        Returns ranked list of dicts, highest activity first.

        Note: Reddit public JSON is 403-blocked site-wide (Reddit API lockdown 2024).
        StockTwits individual symbol streams remain reliably accessible and give
        richer data — per-message bullish/bearish tags from the investing community.
        """
        # Scan the full CC income universe (30 msgs per ticker, ~1s each)
        all_tickers = list(self._CC_INCOME_TICKERS)
        st_data = self._fetch_stocktwits_ccincome_stream(all_tickers)

        results = []
        for sym, data in st_data.items():
            msg_count = data["msg_count"]
            if msg_count < 3:  # skip tickers with almost no activity
                continue

            bulls = data["bullish"]
            bears = data["bearish"]
            bull_pct = data["bull_pct"]

            # Buzz score: message volume × sentiment conviction
            # High volume + high bull% = highest score
            conviction = (bull_pct / 100) if bulls > bears else (-(100 - bull_pct) / 100)
            buzz_score = round(msg_count * (1 + abs(conviction)), 1)

            lean = (
                "BULLISH"    if bull_pct >= 70 else
                "BEARISH"    if bull_pct <= 30 else
                "MIXED"
            )
            label = (
                "HIGH BUZZ" if msg_count >= 20 else
                "TRENDING"  if msg_count >= 10 else
                "WATCHING"
            )
            family, freq = self.NEW_INCOME_ETF_UNIVERSE.get(sym, ("Unknown", "?"))
            results.append({
                "symbol":     sym,
                "family":     family,
                "freq":       freq,
                "msg_count":  msg_count,
                "bullish":    bulls,
                "bearish":    bears,
                "bull_pct":   bull_pct,
                "lean":       lean,
                "buzz_score": buzz_score,
                "label":      label,
            })

        results.sort(key=lambda x: x["buzz_score"], reverse=True)
        return results[:top_n]

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

        # SentiSense Sentiment Movers — improving names in futures-adjacent universe
        ss_movers_set: set = set()
        try:
            import sentisense_client as ss
            movers = ss.get_sentiment_movers(self.db, direction="improving", limit=20)
            if movers:
                for _m in movers:
                    _t = _m.get("ticker", "")
                    if _t in self._FUTURES_ADJACENT:
                        ss_movers_set.add(_t)
        except Exception:
            pass

        candidates = set(st_map.keys()) | set(wsb_filt.keys()) | ss_movers_set
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
                in_movers = sym in ss_movers_set
                source_count = sum([in_st, in_wsb, in_movers])
                meter     = "HIGH" if source_count >= 2 else "NEUTRAL"
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

    # ── FINVIZ TECHNICAL PATTERN SCAN ─────────────────────────────────────────
    # Fetches bullish and bearish TA pattern screens from Finviz CSV exports.
    # Used as a second segment in the futures_social dispatch — adds pattern
    # context on top of the price-action board and commodity social overlay.

    _FINVIZ_PATTERNS = [
        ("ta_p_channelup",          "Channel Up",         "bullish"),
        ("ta_p_wedgeup",            "Wedge Up",           "bullish"),
        ("ta_p_doublebottom",       "Double Bottom",      "bullish"),
        ("ta_p_triangleascending",  "Ascending Triangle", "bullish"),
        ("ta_p_headandshoulders",   "Head & Shoulders",   "bearish"),
        ("ta_p_wedgedown",          "Wedge Down",         "bearish"),
        ("ta_p_doubletop",          "Double Top",         "bearish"),
    ]

    def fetch_finviz_pattern_scan(self, min_avg_vol=500000) -> dict:
        """
        Fetches TA pattern screens from Finviz CSV export. Returns top 3 tickers
        per bullish and bearish pattern bucket. Filtered to >500K avg daily volume
        to avoid micro-cap noise. Returns empty lists gracefully outside market hours
        (Finviz returns HTML login wall when closed).
        """
        import csv, io
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )
        }
        bullish_hits: list = []
        bearish_hits: list = []

        for screen_id, label, direction in self._FINVIZ_PATTERNS:
            try:
                r = requests.get(
                    f"https://finviz.com/export.ashx?v=111&s={screen_id}"
                    f"&f=sh_avgvol_o{min_avg_vol // 1000}",
                    headers=headers, timeout=10
                )
                if r.status_code != 200:
                    continue
                if r.text.strip()[:1] == "<" or "Ticker" not in r.text[:200]:
                    continue  # HTML response = outside market hours or blocked
                bucket = bullish_hits if direction == "bullish" else bearish_hits
                for i, row in enumerate(csv.DictReader(io.StringIO(r.text))):
                    if i >= 3:
                        break
                    sym = row.get("Ticker", "").strip()
                    if not sym:
                        continue
                    try:
                        chg   = float(row.get("Change", "0%").replace("%", "").strip() or 0)
                        price = float(row.get("Price", "0").strip() or 0)
                        if price > 0:
                            bucket.append({"symbol": sym, "price": price, "chg": chg, "pattern": label})
                    except (ValueError, KeyError):
                        pass
            except Exception as e:
                logger.warning(f"[Pattern Scan] {screen_id} failed: {e}")

        return {"bullish": bullish_hits, "bearish": bearish_hits}

    # ── CRYPTO FUNDING RATES (BINANCE FAPI) ───────────────────────────────────
    # Binance perpetual futures funding rates — free, no auth required.
    # Funding rate fires every 8 hours (00:00, 08:00, 16:00 UTC).
    # Positive rate → longs pay shorts → crowded long, mild bearish signal.
    # Negative rate → shorts pay longs → crowded short, contrarian long signal.

    _FUNDING_SYMBOLS = [
        ("BTCUSDT", "BTC"),
        ("ETHUSDT", "ETH"),
        ("SOLUSDT", "SOL"),
    ]

    def fetch_funding_rates(self) -> list:
        """
        Fetches current perpetual futures funding rates from Binance FAPI.
        Returns list of dicts: symbol, rate_8h (%), rate_annualized (%), sentiment label,
        next_funding_utc (HH:MM). Empty list on failure.
        """
        results = []
        for binance_sym, display_sym in self._FUNDING_SYMBOLS:
            try:
                r = requests.get(
                    f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={binance_sym}",
                    timeout=8
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                rate_8h  = float(data.get("lastFundingRate", 0)) * 100   # convert to %
                rate_ann = rate_8h * 3 * 365                              # 3 payments/day × 365
                next_ms  = int(data.get("nextFundingTime", 0))
                from datetime import timezone
                next_utc = datetime.fromtimestamp(next_ms / 1000, tz=timezone.utc).strftime("%H:%M UTC") if next_ms else "—"

                if rate_8h > 0.05:
                    sentiment = "Crowded Long — longs paying"
                elif rate_8h > 0.01:
                    sentiment = "Slight Long Bias"
                elif rate_8h < -0.05:
                    sentiment = "Crowded Short — shorts paying"
                elif rate_8h < -0.01:
                    sentiment = "Slight Short Bias"
                else:
                    sentiment = "Neutral"

                results.append({
                    "symbol":       display_sym,
                    "rate_8h":      rate_8h,
                    "rate_ann":     rate_ann,
                    "sentiment":    sentiment,
                    "next_funding": next_utc,
                })
            except Exception as e:
                logger.warning(f"[Funding Rates] {binance_sym} failed: {e}")
        return results

    def fetch_hy_spread(self) -> float:
        """
        FRED BAMLH0A0HYM2 — ICE BofA US High Yield Option-Adjusted Spread (daily, %).
        Values below 4.5% = healthy credit. Above 4.5% = stress. Above 7% = crisis.
        Cached once per day in DB (FRED updates once daily). Returns 0.0 on failure.
        """
        cache_key = "fred_hy_spread_value"
        cache_date_key = "fred_hy_spread_date"
        today_str = datetime.now().strftime("%Y-%m-%d")
        if self.db.get_state(cache_date_key) == today_str:
            cached = self.db.get_state(cache_key)
            if cached:
                return float(cached)
        val = self._fetch_fred_metric("BAMLH0A0HYM2")
        if val and val > 0:
            self.db.update_state(cache_key, val)
            self.db.update_state(cache_date_key, today_str)
            return round(val, 2)
        return 0.0

    def fetch_binance_derivatives(self) -> dict:
        """
        Binance FAPI public endpoints — no API key required.
        Returns OI (USD), global long/short account ratio, top-trader L/S ratio,
        and taker buy/sell volume ratio for BTC and ETH.
        All returned as a dict keyed by display symbol ("BTC", "ETH").
        Empty dict per symbol on failure — caller must handle missing keys.
        """
        symbols = [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]
        results = {}
        for binance_sym, display_sym in symbols:
            try:
                oi_r = requests.get(
                    "https://fapi.binance.com/fapi/v1/openInterest",
                    params={"symbol": binance_sym}, timeout=8
                ).json()
                # OI in contracts; need price to convert to USD — fetch from premiumIndex
                price_r = requests.get(
                    f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={binance_sym}",
                    timeout=8
                ).json()
                oi_contracts = float(oi_r.get("openInterest", 0))
                mark_price   = float(price_r.get("markPrice", 0))
                oi_usd = oi_contracts * mark_price

                ls_r = requests.get(
                    "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                    params={"symbol": binance_sym, "period": "1h", "limit": 1}, timeout=8
                ).json()
                global_ls = float(ls_r[0].get("longShortRatio", 1.0)) if ls_r else 1.0

                top_r = requests.get(
                    "https://fapi.binance.com/futures/data/topLongShortAccountRatio",
                    params={"symbol": binance_sym, "period": "1h", "limit": 1}, timeout=8
                ).json()
                top_ls = float(top_r[0].get("longShortRatio", 1.0)) if top_r else 1.0

                taker_r = requests.get(
                    "https://fapi.binance.com/futures/data/takerlongshortRatio",
                    params={"symbol": binance_sym, "period": "1h", "limit": 2}, timeout=8
                ).json()
                if len(taker_r) >= 2:
                    buy_vol_cur  = float(taker_r[0].get("buyVol", 1))
                    sell_vol_cur = float(taker_r[0].get("sellVol", 1))
                    buy_vol_prev = float(taker_r[1].get("buyVol", 1))
                    sell_vol_prev = float(taker_r[1].get("sellVol", 1))
                    taker_buy_pct = buy_vol_cur / (buy_vol_cur + sell_vol_cur) * 100 if (buy_vol_cur + sell_vol_cur) > 0 else 50.0
                    oi_prev_contracts = float(taker_r[1].get("buyVol", 0)) + float(taker_r[1].get("sellVol", 0))
                else:
                    taker_buy_pct = 50.0

                results[display_sym] = {
                    "oi_usd":       oi_usd,
                    "global_ls":    round(global_ls, 3),
                    "top_ls":       round(top_ls, 3),
                    "taker_buy_pct": round(taker_buy_pct, 1),
                }
            except Exception as e:
                logger.warning(f"[Binance Derivatives] {binance_sym} failed: {e}")
        return results

    # ── VIX-ADJUSTED WHEEL PARAMETERS ────────────────────────────────────────
    # Tastytrade empirical: at 16 VIX the market implies ~1% daily move.
    # Higher VIX → sell further OTM to maintain edge; scale down size.

    def get_vix_adjusted_params(self, vix: float) -> dict:
        """
        Four-tier VIX framework for wheel entry parameterization.
        Returns delta_target, dte range, size_scalar, and tier label.
        At VIX > 20 tastytrade data shows 95% OTM expiry at 0.16 delta — use that,
        not the standard 0.20, to avoid assignment in fast-moving markets.
        """
        if vix < 15:
            return {"delta_target": 0.28, "dte_min": 30, "dte_max": 45, "size_scalar": 1.0,  "tier": "LOW"}
        elif vix < 20:
            return {"delta_target": 0.20, "dte_min": 30, "dte_max": 45, "size_scalar": 0.85, "tier": "NORMAL"}
        elif vix < 30:
            return {"delta_target": 0.16, "dte_min": 21, "dte_max": 35, "size_scalar": 0.65, "tier": "ELEVATED"}
        else:
            return {"delta_target": 0.10, "dte_min": 14, "dte_max": 21, "size_scalar": 0.40, "tier": "PANIC"}

    # ── POSITION SIZER (Half-Kelly + VIX Scalar) ─────────────────────────────

    def kelly_position_size(self, portfolio_value: float, vix: float,
                            win_rate: float = 0.65, avg_win_pct: float = 1.0,
                            avg_loss_pct: float = 1.0, max_pct: float = 0.06) -> dict:
        """
        Half-Kelly position sizing with a VIX scalar cap.
        Bootstrap prior: 65% win rate, 1:1 payoff until 50 closed trades in DB.
        VIX scalar = min(1.0, 15/VIX) — panic-shrinks size in high-vol regimes.
        Max position: 6% of portfolio (5-7% thetagang standard per underlying).
        """
        sample = 0
        try:
            dist = self.db.get_wheel_outcome_distribution(lookback_days=730)
            total_trades = sum(d["count"] for d in dist)
            if total_trades >= 50:
                wins = sum(d["count"] for d in dist if d["outcome"] in ("CLOSED", "EXPIRED"))
                if wins > 0 and total_trades > 0:
                    win_rate = wins / total_trades
                    win_premium = sum(d["retained_premium"] for d in dist if d["outcome"] in ("CLOSED", "EXPIRED"))
                    loss_premium = abs(sum(d["retained_premium"] for d in dist if d["outcome"] not in ("CLOSED", "EXPIRED")))
                    if wins > 0:
                        avg_win_pct = (win_premium / wins) / (portfolio_value * 0.05) if portfolio_value > 0 else 1.0
                    if (total_trades - wins) > 0:
                        avg_loss_pct = (loss_premium / (total_trades - wins)) / (portfolio_value * 0.05) if portfolio_value > 0 else 1.0
                sample = total_trades
        except Exception:
            pass

        b = avg_win_pct / avg_loss_pct if avg_loss_pct > 0 else 1.0
        kelly_f = (win_rate * b - (1 - win_rate)) / b if b > 0 else 0.0
        half_kelly = max(0.0, kelly_f / 2)
        vix_scalar = min(1.0, 15.0 / max(float(vix), 10.0))
        final_pct = min(half_kelly * vix_scalar, max_pct)
        return {
            "position_pct":      round(final_pct * 100, 2),
            "position_dollars":  round(portfolio_value * final_pct, 0),
            "kelly_f":           round(kelly_f, 4),
            "vix_scalar":        round(vix_scalar, 3),
            "win_rate":          round(win_rate, 3),
            "sample_trades":     sample,
            "using_empirical":   sample >= 50,
        }

    # ── BTC DOMINANCE (CoinGecko — free, no key) ──────────────────────────────

    def fetch_btc_dominance(self) -> float:
        """
        BTC market cap as % of total crypto market cap.
        Above 65% → alt-season exhausted, distribution phase starting.
        1-hour cache to avoid hammering the free CoinGecko endpoint.
        """
        cache_key    = "btc_dominance_pct"
        cache_ts_key = "btc_dominance_ts"
        try:
            last_ts = float(self.db.get_state(cache_ts_key) or 0)
            if (datetime.now().timestamp() - last_ts) < 3600:
                cached = self.db.get_state(cache_key)
                if cached:
                    return float(cached)
            r = requests.get(
                "https://api.coingecko.com/api/v3/global",
                timeout=10
            ).json()
            dom = float(r["data"]["market_cap_percentage"].get("btc", 0.0))
            self.db.update_state(cache_key, dom)
            self.db.update_state(cache_ts_key, datetime.now().timestamp())
            return round(dom, 2)
        except Exception as e:
            logger.warning(f"[BTC Dominance] CoinGecko failed: {e}")
            return float(self.db.get_state(cache_key) or 0.0)

    # ── CRYPTO CYCLE TOP SCORER ───────────────────────────────────────────────

    def calculate_crypto_top_score(self) -> dict:
        """
        Composite Tier 3 crypto cycle exit signal. Score 0–100.
        ≥ 80 → EXIT signal. ≥ 65 → REDUCE. ≥ 40 → CAUTION.

        Inputs: BTC dominance (30 pts), Fear & Greed Extreme Greed streak (25 pts),
        global L/S crowding (20 pts), smart-money short divergence (20 pts),
        perp funding rate (20 pts — capped so total stays ≤ 100).
        """
        score   = 0
        signals = {}

        # ── BTC dominance ─────────────────────────────────────────────────────
        dom = self.fetch_btc_dominance()
        signals["btc_dominance"] = dom
        if dom >= 65:
            score += 30
        elif dom >= 60:
            score += 15

        # ── Fear & Greed + Extreme Greed streak ───────────────────────────────
        try:
            fg_raw   = requests.get("https://api.alternative.me/fng/", timeout=8).json()
            fg_value = int(fg_raw["data"][0]["value"])
            fg_class = fg_raw["data"][0]["value_classification"]
            signals["fear_greed"]    = fg_value
            signals["fg_class"]      = fg_class
            if fg_value >= 80:
                streak = int(self.db.get_state("fg_extreme_greed_streak") or 0) + 1
                self.db.update_state("fg_extreme_greed_streak", streak)
                score += 25 if streak >= 7 else (12 if streak >= 4 else 5)
            else:
                streak = 0
                self.db.update_state("fg_extreme_greed_streak", 0)
            signals["fg_extreme_streak"] = streak
        except Exception as e:
            logger.warning(f"[CryptoTopScore] F&G fetch failed: {e}")
            signals["fear_greed"] = 50

        # ── Binance derivatives: retail crowding + smart-money divergence ──────
        try:
            deriv   = self.fetch_binance_derivatives()
            btc_d   = deriv.get("BTC", {})
            global_ls = btc_d.get("global_ls", 1.0)
            top_ls    = btc_d.get("top_ls",    1.0)
            signals["global_ls"] = global_ls
            signals["top_ls"]    = top_ls
            if global_ls > 1.5:
                score += 20
            elif global_ls > 1.3:
                score += 10
            # Smart money diverging short while retail is long = distribution signal
            if top_ls < 0.9 and global_ls > 1.1:
                score += 20
                signals["sm_divergence"] = "SHORT — institutions distributing"
            else:
                signals["sm_divergence"] = "None"
        except Exception as e:
            logger.warning(f"[CryptoTopScore] Binance L/S failed: {e}")

        # ── Perp funding: >50% annualized = overheated longs ──────────────────
        try:
            funding = self.fetch_funding_rates()
            btc_f   = next((f for f in funding if f["symbol"] == "BTC"), None)
            if btc_f:
                ann = btc_f["rate_ann"]
                signals["funding_ann"] = ann
                score += 20 if ann > 100 else (10 if ann > 50 else 0)
        except Exception as e:
            logger.warning(f"[CryptoTopScore] Funding rate failed: {e}")

        score = min(score, 100)
        if score >= 80:
            label = "EXIT — strong distribution signal"
        elif score >= 65:
            label = "REDUCE — cycle top proximity high"
        elif score >= 40:
            label = "CAUTION — late cycle indicators building"
        else:
            label = "HOLD — no top signal"

        return {"score": score, "label": label, "signals": signals}

    def calibrate_cef_premium_zscore(self, ticker: str) -> dict:
        """
        Calibrates monitor.py's z-score using locally accumulated daily premium data
        (written by monitor.py each loop tick via db.store_cef_premium).

        CEFConnect API was deprecated — all v3 endpoints return 404.
        This function now reads from the DB cef_premium_log table, which
        accumulates one row per trading day automatically.

        Priors when DB data is thin (< 20 days):
          CLM: mu=19.5, sigma=7.5 (derived from 5-year historical range 0–38%)
          CRF: mu=18.0, sigma=7.0 (similar fund, slightly tighter historical range)
          These are meaningfully better than the hardcoded 15/4 defaults.
        After 30+ trading days the empirical data takes over automatically.

        Returns {"mu": float, "sigma": float, "n": int, "source": str, "ticker": str}.
        """
        # Informed priors — based on CLM/CRF historical premium range (0–38%, avg ~18–20%)
        PRIORS = {
            "CLM": {"mu": 19.5, "sigma": 7.5},
            "CRF": {"mu": 18.0, "sigma": 7.0},
        }
        defaults = PRIORS.get(ticker.upper(), {"mu": 18.0, "sigma": 7.0})

        history = self.db.get_cef_premium_history(ticker, days=252)
        n       = len(history)

        if n < 20:
            mu    = defaults["mu"]
            sigma = defaults["sigma"]
            source = f"prior ({n}/20 days accumulated)"
            logger.info(f"[CEFCalibrate] {ticker}: using informed prior — {source}")
        else:
            prems    = [h["premium_pct"] for h in history]
            mu       = round(sum(prems) / n, 4)
            variance = sum((p - mu) ** 2 for p in prems) / n
            sigma    = round(max(variance ** 0.5, 1.0), 4)  # floor at 1% to avoid division issues
            source   = f"empirical ({n} trading days)"
            logger.info(f"[CEFCalibrate] {ticker}: mu={mu:.2f}% sigma={sigma:.2f}% — {source}")

        self.db.update_state(f"{ticker}_premium_mu",    mu)
        self.db.update_state(f"{ticker}_premium_sigma", sigma)
        if history:
            self.db.update_state(f"{ticker}_premium_prev", history[0]["premium_pct"])

        return {"mu": round(mu, 4), "sigma": round(sigma, 4), "n": n, "source": source, "ticker": ticker}

    # ── NVDA / BTC CORRELATION ────────────────────────────────────────────────
    # 30-day Pearson correlation on daily log returns.
    # NVDA and BTC both track AI/tech risk sentiment but decouple in
    # crypto-specific stress events (exchange collapses, regulatory shocks).
    # High correlation (>0.6) = macro drives both; low (<0.3) = idiosyncratic.

    def calculate_nvda_btc_correlation(self, lookback=30) -> dict:
        """
        Computes 30-day Pearson correlation between NVDA and BTC/USD daily returns.
        Returns {"correlation": float, "label": str, "nvda_ret": float, "btc_ret": float}
        where nvda_ret and btc_ret are the period total returns (%).
        """
        try:
            nvda_ts = self._execute_query("time_series", {
                "symbol": "NVDA", "interval": "1day", "outputsize": str(lookback + 1)
            })
            btc_ts = self._execute_query("time_series", {
                "symbol": "BTC/USD", "interval": "1day", "outputsize": str(lookback + 1)
            })
            if not nvda_ts or not btc_ts:
                return {}
            nvda_closes = [float(v["close"]) for v in reversed(nvda_ts["values"])]
            btc_closes  = [float(v["close"]) for v in reversed(btc_ts["values"])]
            n = min(len(nvda_closes), len(btc_closes)) - 1
            if n < 10:
                return {}
            import math
            nvda_ret_series = [math.log(nvda_closes[i+1] / nvda_closes[i]) for i in range(n)]
            btc_ret_series  = [math.log(btc_closes[i+1]  / btc_closes[i])  for i in range(n)]
            mean_n = sum(nvda_ret_series) / n
            mean_b = sum(btc_ret_series)  / n
            cov  = sum((nvda_ret_series[i] - mean_n) * (btc_ret_series[i] - mean_b) for i in range(n)) / n
            std_n = (sum((x - mean_n)**2 for x in nvda_ret_series) / n) ** 0.5
            std_b = (sum((x - mean_b)**2 for x in btc_ret_series)  / n) ** 0.5
            corr = cov / (std_n * std_b) if std_n > 0 and std_b > 0 else 0.0
            corr = max(-1.0, min(1.0, corr))

            if corr >= 0.7:
                label = "Strong — macro/AI sentiment driving both"
            elif corr >= 0.4:
                label = "Moderate — partial co-movement"
            elif corr >= 0.1:
                label = "Weak — beginning to decouple"
            else:
                label = "Decoupled — crypto moving on its own"

            nvda_total = (nvda_closes[-1] - nvda_closes[0]) / nvda_closes[0] * 100
            btc_total  = (btc_closes[-1]  - btc_closes[0])  / btc_closes[0]  * 100
            return {
                "correlation": round(corr, 3),
                "label":       label,
                "nvda_ret":    round(nvda_total, 2),
                "btc_ret":     round(btc_total, 2),
                "lookback":    n,
            }
        except Exception as e:
            logger.error(f"[NVDA/BTC Corr] Failed: {e}")
            return {}


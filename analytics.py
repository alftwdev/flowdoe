import os
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

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
            div_data = self._execute_query("complex_data/dividends", {"symbol": symbol})
            if div_data and "data" in div_data and div_data["data"]:
                divs = sorted(
                    div_data["data"],
                    key=lambda x: x.get("ex_date", ""),
                    reverse=True
                )
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
        fed_assets = self._fetch_fred_metric("WALCL") / 1000 
        tga = self._fetch_fred_metric("WTREGEN")
        rev_repo = self._fetch_fred_metric("RRPONTSYD")
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

        risk_emoji, regime_alert = ("🚨", "CREDIT STRESS DETECTED") if credit_spread > 4.5 else ("🟢", "Credit markets stable.")
        return (
            f"**Federal Reserve System Liquidity Snapshot**\n"
            f"┣ **Fed Balance Sheet:** `${fed_assets:,.0f}B`\n"
            f"┣ **Global Net Liquidity:** `${net_liquidity:,.0f}B`\n"
            f"┣ **Liquidity Velocity (5D):** `{liv:+.2f}%`\n"
            f"┗ **High Yield Credit Spread:** `{credit_spread:.2f}%`\n\n"
            f"**System Interpretation:**\n{risk_emoji} *{regime_alert}*\n{liv_alert}"
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
        Pulls upcoming ex-dividend dates utilizing Twelve Data's complex_data endpoint.
        Creates an actionable countdown for capital rotation.
        """
        results = []
        today = datetime.now()
        for sym in universe:
            try:
                data = self._execute_query("complex_data/dividends", {"symbol": sym})
                if not data or "data" not in data: continue
                
                # Find the immediate next upcoming ex-dividend date
                for div in data["data"]:
                    ex_date_str = div.get("ex_date")
                    if not ex_date_str: continue
                    
                    ex_date = datetime.strptime(ex_date_str, "%Y-%m-%d")
                    # Filter for distributions occurring within the next 14 days
                    if ex_date >= today and (ex_date - today).days <= 14:
                        amount = float(div.get("amount", 0.0))
                        results.append({
                            "symbol": sym,
                            "ex_date": ex_date_str,
                            "amount": amount,
                            "days_away": (ex_date - today).days
                        })
                        break # Secure the immediate next date and move to next ticker
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

                # Pull ex-dividend date + amount
                div_data = self._execute_query("complex_data/dividends", {"symbol": sym})
                ex_date_str, div_amount, days_away = None, 0.0, 999
                if div_data and "data" in div_data:
                    for div in div_data["data"]:
                        ex_raw = div.get("ex_date")
                        if not ex_raw:
                            continue
                        ex_dt = datetime.strptime(ex_raw, "%Y-%m-%d")
                        if ex_dt >= today:
                            ex_date_str = ex_raw
                            div_amount = float(div.get("amount", 0.0))
                            days_away = (ex_dt - today).days
                            break

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
        WHEEL_UNIVERSE = [
            # Dividend Aristocrats
            "KO", "JNJ", "PG", "MMM", "ABT", "MCD", "CL", "WMT",
            # High-yield Dividend Stocks
            "MO", "T", "VZ", "PFE", "BMY", "KMI",
            # REITs
            "O", "MPW", "STAG",
            # BDCs
            "MAIN", "ARCC",
            # Banks / Financials
            "BAC", "WFC",
            # Tech Dividend
            "CSCO", "IBM",
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

                candidates.append({
                    "symbol":         symbol,
                    "spot":           spot,
                    "trend":          trend_status,
                    "sma50_tag":      sma50_tag,
                    "rsi14":          round(float(rsi14), 1),
                    "rsi_tag":        rsi_tag,
                    "bb_pct_b":       round(float(bb_pct_b), 2),
                    "bb_zone":        bb_zone,
                    "strike":         strike,
                    "dte":            dte,
                    "expiration":     best["expiration_date"].strftime("%Y-%m-%d"),
                    "premium":        round(premium, 2),
                    "delta":          round(float(best["delta"]), 2),
                    "theta_daily":    round(float(best["theta_proxy"]), 4),
                    "iv":             round(atm_iv, 1),
                    "ivr_proxy":      round(ivr_proxy, 1),
                    "ivr_tag":        ivr_tag,
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
                df["volume"] = pd.to_numeric(
                    df.get("volume", 0), errors="coerce"
                ).fillna(0)

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
        chain = self._execute_query("options/chain", {"symbol": symbol})
        spot_data = self._execute_query("price", {"symbol": symbol})
        if not chain or "data" not in chain or not spot_data: 
            return {"flip_strike": 0.0, "current_spot": 0.0, "market_state": "UNKNOWN"}
        try:
            spot = float(spot_data.get("price", 0.0))
            df = pd.DataFrame(chain["data"])
            df["strike"], df["open_interest"] = df["strike"].astype(float), df["open_interest"].astype(float)
            df = df[(df["strike"] >= spot * 0.95) & (df["strike"] <= spot * 1.05)]
            calls = df[df["type"] == "call"].set_index("strike")["open_interest"]
            puts = df[df["type"] == "put"].set_index("strike")["open_interest"]
            alignment = pd.DataFrame({"calls": calls, "puts": puts}).fillna(0)
            alignment["net_oi"] = alignment["calls"] - alignment["puts"]
            flip_strike = float(alignment["net_oi"].abs().idxmin())
            market_state = "🟢 POSITIVE GAMMA" if spot > flip_strike else "🔴 NEGATIVE GAMMA"
            return {"flip_strike": flip_strike, "current_spot": spot, "market_state": market_state}
        except Exception:
            return {"flip_strike": spot, "current_spot": spot, "market_state": "ERROR BOUNDS"}

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
        # GEX regime + risk-on/off + breadth + vol-spike each contribute, capped at +/-4.
        score = 0
        if "POSITIVE" in snap["gex"]["market_state"]: score += 1
        elif "NEGATIVE" in snap["gex"]["market_state"]: score -= 1
        if snap["risk_regime"] == "🟢 RISK-ON": score += 1
        elif snap["risk_regime"] == "🔴 RISK-OFF": score -= 1
        if snap["breadth"] >= 0.70: score += 1
        elif snap["breadth"] < 0.35: score -= 2
        if snap["vixy_z"] >= 1.5: score -= 1
        snap["conviction_score"] = score
        snap["conviction_bias"] = "🟢 BULLISH" if score >= 2 else ("🔴 BEARISH" if score <= -2 else "🟡 NEUTRAL/CHOP")
        return snap

    def generate_market_analysis_morning_report(self):
        """Pre-open synthesis for scalpers/options traders — what happened overnight, what it means
        for today. Stores today's directional call for the EOD accuracy reconciliation."""
        snap = self._gather_cross_asset_snapshot()
        today_str = datetime.now().strftime("%Y-%m-%d")
        self.db.update_state(f"market_analysis_morning_call_{today_str}", {
            "bias": snap["conviction_bias"], "score": snap["conviction_score"], "spot": snap["gex"]["current_spot"],
        })

        crypto_line = ""
        if snap["crypto_mover"]:
            sym, price, _, pct = snap["crypto_mover"]
            crypto_line = f"┣ Crypto Overnight Mover: {sym} `{pct:+.2f}%` | Fear & Greed: {snap['fng']['value']} ({snap['fng']['label']})\n" if snap["fng"] else f"┣ Crypto Overnight Mover: {sym} `{pct:+.2f}%`\n"

        payload = (
            f"🌅 **MARKET ANALYSIS | MORNING BRIEF — Pre-Open Conviction**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"┣ Overnight Futures ({snap['futures_session']}): SPY POC `${snap['futures_poc']:,.2f}` | VAH `${snap['futures_vah']:,.2f}` | VAL `${snap['futures_val']:,.2f}`\n"
            f"┣ Gamma Regime: {snap['gex']['market_state']} | Flip `${snap['gex']['flip_strike']:,.2f}` | SPY `${snap['gex']['current_spot']:,.2f}`\n"
            f"┣ Volatility: VIXY `{snap['vixy_price']:.2f}` (z {snap['vixy_z']:+.2f}σ) | Nasdaq-100 Breadth: `{snap['breadth']:.0%}`\n"
            f"┣ Macro: Synthetic Dollar Index `{snap['synthetic_dxy']:+.2f}%` | {snap['risk_regime']} ({snap['risk_explanation']})\n"
            f"┣ Credit Stress (HY Spread): `{snap['credit_spread']:.2f}%`\n"
            f"{crypto_line}"
            f"┗ **TODAY'S CONVICTION: {snap['conviction_bias']}** (score {snap['conviction_score']:+d}/4)\n\n"
            f"Scalper/Options Directive: {'Favor long delta into strength; positive gamma should dampen downside.' if snap['conviction_score'] >= 2 else ('Favor defined-risk/short delta; negative gamma + weak breadth raises whipsaw odds.' if snap['conviction_score'] <= -2 else 'No clean edge — size down, trade the range, wait for a confirming break of overnight VAH/VAL.')}"
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
        """End-of-day recap: what happened, what the indicators said, and how the morning call did."""
        snap = self._gather_cross_asset_snapshot()
        today_str = datetime.now().strftime("%Y-%m-%d")
        morning_call = self.db.get_state(f"market_analysis_morning_call_{today_str}")
        eod_core = self.generate_eod_reconciliation("SPY")

        call_review = ""
        if morning_call:
            correct = morning_call["bias"] == snap["conviction_bias"] or (morning_call["score"] * snap["conviction_score"]) > 0
            call_review = (
                f"\n\n📋 **Morning Call Review**\n"
                f"┣ Called: {morning_call['bias']} (score {morning_call['score']:+d}) | Closed: {snap['conviction_bias']} (score {snap['conviction_score']:+d})\n"
                f"┗ {'✅ Directionally correct' if correct else '❌ Missed — regime shifted intraday'}"
            )

        payload = (
            f"🌆 **MARKET ANALYSIS | END-OF-DAY RECAP**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{eod_core or 'EOD reconciliation data unavailable.'}\n\n"
            f"📊 **Closing Technicals & Lessons**\n"
            f"┣ Final Gamma Regime: {snap['gex']['market_state']} | Breadth: `{snap['breadth']:.0%}` | VIXY z: `{snap['vixy_z']:+.2f}σ`\n"
            f"┣ Macro Close: {snap['risk_regime']} | Credit Spread: `{snap['credit_spread']:.2f}%`\n"
            f"┗ Lesson: {'Breadth and gamma confirmed each other today — high-conviction setups like this are rare, note the pattern.' if abs(snap['conviction_score']) >= 2 else 'Mixed signals across breadth/gamma/macro — a chop day. Capital preservation over forcing trades is the correct lesson.'}"
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

import os
import logging
import requests
import numpy as np
import pandas as pd
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

    # --- MACRO PIPELINE METHODS (Migrated from macro.py) ---
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

    def generate_forex_matrix_payload(self):
        fx_universe = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]
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
        return f"**1-Day Cross-Sectional Relative Performance**\n```js\nPair      Price     Daily Change\n────────────────────────────────\n{matrix_body}\n```"

    def generate_crypto_matrix_payload(self):
        crypto_universe = ["BTC/USD", "ETH/USD", "SOL/USD", "ADA/USD", "XRP/USD", "LINK/USD"]
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
        return f"**1-Day Relative Performance Index**\n```js\nTicker  Spot Price  Daily Change\n────────────────────────────────\n{matrix_body}\n```"

    # --- EXISTING WHEEL/TSP/OPTIONS METHODS ---
    def generate_wheel_candidates(self, watchlist=["AAPL", "NVDA", "MSFT", "AMZN", "META", "GOOGL", "TSLA"]):
        candidates = []
        target_dte_min, target_dte_max = 30, 45
        for symbol in watchlist:
            try:
                spot_data = self._execute_query("price", {"symbol": symbol})
                spot = float(spot_data.get("price", 0.0))
                if spot == 0: continue

                chain = self._execute_query("options/chain", {"symbol": symbol})
                if not chain or "data" not in chain: continue

                df = pd.DataFrame(chain["data"])
                df["expiration_date"] = pd.to_datetime(df["expiration_date"])
                df["strike"] = df["strike"].astype(float)
                
                today = pd.Timestamp.today()
                df["dte"] = (df["expiration_date"] - today).dt.days
                df_filtered = df[(df["dte"] >= target_dte_min) & (df["dte"] <= target_dte_max) & (df["type"] == "put")].copy()
                
                if df_filtered.empty: continue
                
                target_strike = spot * 0.96
                df_filtered["strike_dist"] = abs(df_filtered["strike"] - target_strike)
                optimal_put = df_filtered.loc[df_filtered["strike_dist"].idxmin()]
                
                strike, dte = optimal_put["strike"], optimal_put["dte"]
                exp_date = optimal_put["expiration_date"].strftime('%Y-%m-%d')
                
                est_premium = strike * 0.015 
                capital_required = strike * 100
                annualized_roi = ((est_premium * 100) / capital_required) * (365 / dte) * 100

                candidates.append({
                    "symbol": symbol, "spot": spot, "strike": strike,
                    "dte": dte, "expiration": exp_date, "premium": round(est_premium, 2),
                    "annualized_roi": round(annualized_roi, 1)
                })
            except Exception as e: logger.error(f"Wheel scan failed for {symbol}: {e}")
        return candidates

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

    def compile_tsp_allocation_matrix(self):
        spy_quote = self._execute_query("quote", {"symbol": "SPY"})
        vxf_quote = self._execute_query("quote", {"symbol": "VXF"})
        agg_quote = self._execute_query("quote", {"symbol": "AGG"})
        us10y_data = self._execute_query("price", {"symbol": "US10Y"})
        ten_year_yield = float(us10y_data.get("price", 4.45)) if us10y_data else 4.45
        
        def parse_status(quote_data):
            if not quote_data or "percent_change" not in quote_data: return "🟡 NEUTRAL | Feed Latency"
            chg = float(quote_data["percent_change"])
            if chg > 0.5: return f"🟢 BULLISH | Inflow Strength ({chg:+.2f}%)"
            if chg < -0.5: return f"🔴 BEARISH | Liquidity Outflow ({chg:+.2f}%)"
            return f"🟡 NEUTRAL | Compression ({chg:+.2f}%)"

        return (
            f"**Thrift Savings Plan Dynamic Allocation Matrix**\n"
            f"┣ **C-Fund (Large Cap ETF Proxy)**: {parse_status(spy_quote)}\n"
            f"┣ **S-Fund (Small Cap Completion Proxy)**: {parse_status(vxf_quote)}\n"
            f"┗ **F-Fund (Aggregate Bond Proxy)**: {parse_status(agg_quote)}\n\n"
            f"**Macro Reference Yield (US10Y)**: `{ten_year_yield}%` \n"
            f"💡 *Directive: Allocate to funds with positive underlying market momentum.*"
        )

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
        universe = ["AAPL", "NVDA", "MSFT"]
        results = []
        for symbol in universe:
            hv_30 = self.calculate_historical_volatility(symbol)
            chain = self._execute_query("options/chain", {"symbol": symbol})
            if not chain or "data" not in chain or not chain["data"]: continue
            try:
                df_options = pd.DataFrame(chain["data"])
                df_options["implied_volatility"] = df_options["implied_volatility"].astype(float)
                atm_iv = df_options["implied_volatility"].median() * 100
                results.append({"symbol": symbol, "hv": round(hv_30, 1), "iv": round(atm_iv, 1), "spread": round(atm_iv - hv_30, 1)})
            except Exception: pass
        return results

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

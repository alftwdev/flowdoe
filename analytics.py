import os
import sys
import json
import sqlite3
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
        self.base_url = "https://api.twelvedata.com"

    def _execute_query(self, endpoint, params):
        params["apikey"] = self.api_key
        try:
            r = requests.get(f"{self.base_url}/{endpoint}", params=params, timeout=12)
            if r.status_code == 200:
                return r.json()
            logger.error(f"Twelve Data Endpoint Connection Failure [{endpoint}]: Status {r.status_code}")
            return None
        except Exception as e:
            logger.error(f"Network error routing telemetry from endpoint {endpoint}: {e}")
            return None

    def replicate_volume_velocity(self, symbol):
        """
        [Kenan Grace Matrix]
        Tracks intraday momentum by calculating log-volume distributions.
        Flags unusual activity when volume spikes beyond +3 standard deviations.
        """
        data = self._execute_query("time_series", {"symbol": symbol, "interval": "5min", "outputsize": "50"})
        if not data or "values" not in data:
            return {"status": "INSUFFICIENT_DATA", "spike_detected": False}

        df = pd.DataFrame(data["values"])
        volumes = df["volume"].astype(float).values[::-1]
        
        current_volume = volumes[-1]
        historical_baseline = volumes[:-1]
        
        mean_v = np.mean(historical_baseline)
        std_v = np.std(historical_baseline)
        rvol = current_volume / mean_v if mean_v > 0 else 1.0
        
        threshold = mean_v + (3 * std_v)
        spike = current_volume > threshold

        return {
            "current_volume": int(current_volume),
            "rvol": round(rvol, 2),
            "spike_detected": spike,
            "sigma_deviation": round((current_volume - mean_v) / std_v, 2) if std_v > 0 else 0.0
        }

    def replicate_mean_reversion(self, symbol):
        """
        [Chris Sain Matrix]
        Tracks support levels and psychological exhaustion boundaries.
        Flags entries when daily RSI falls under 30 alongside a lower Bollinger Band breach.
        """
        rsi_res = self._execute_query("rsi", {"symbol": symbol, "interval": "1day", "time_period": "14"})
        bb_res = self._execute_query("bbands", {"symbol": symbol, "interval": "1day", "time_period": "20", "sd": "2"})
        price_res = self._execute_query("price", {"symbol": symbol})

        if not (rsi_res and bb_res and price_res) or "values" not in rsi_res or "values" not in bb_res:
            return {"reversion_candidate": False}

        current_rsi = float(rsi_res["values"][0]["rsi"])
        lower_band = float(bb_res["values"][0]["lower_band"])
        spot_price = float(price_res.get("price", 0.0))

        oversold_condition = current_rsi <= 30.0
        band_breach_condition = spot_price <= lower_band

        return {
            "spot_price": spot_price,
            "rsi": round(current_rsi, 2),
            "lower_band": round(lower_band, 2),
            "reversion_candidate": oversold_condition and band_breach_condition
        }

    def update_fundamental_moat_cache(self, symbol):
        """
        [Wallstreet Trapper Matrix]
        Evaluates long-term stability by assessing core financial strength.
        Filters out highly leveraged entities by analyzing ROIC and Debt-to-Equity ratios.
        """
        fin_data = self._execute_query("key_metrics", {"symbol": symbol})
        if not fin_data or "metrics" not in fin_data:
            return False

        metrics = fin_data["metrics"]
        # Extract operational efficiency statistics
        roic = float(metrics.get("return_on_invested_capital", 0.0)) * 100
        debt_to_equity = float(metrics.get("debt_to_equity_ratio", 0.0))
        gross_margin = float(metrics.get("gross_profit_margin", 0.0)) * 100

        payload = {"roic": roic, "debt_to_equity": debt_to_equity, "gross_margin": gross_margin}
        self.db.update_state(f"moat_cache_{symbol}", payload)
        return True

    def update_dividend_stability_cache(self, symbol):
        """
        [Joseph Carlson Matrix]
        Evaluates dividend safety by cross-referencing payout rates with Free Cash Flow.
        Builds a defensive cash flow sustainability matrix.
        """
        div_data = self._execute_query("dividends", {"symbol": symbol})
        fin_data = self._execute_query("key_metrics", {"symbol": symbol})
        
        if not div_data or "dividends" not in div_data or not fin_data:
            return False

        historical_divs = div_data["dividends"]
        if not historical_divs: return False

        annual_payout = float(historical_divs[0].get("amount", 0.0)) * 4  # Annualized estimate
        fcf_payout_ratio = float(fin_data.get("metrics", {}).get("free_cash_flow_payout_ratio", 1.0)) * 100

        payload = {
            "annual_payout": annual_payout,
            "fcf_payout_ratio": fcf_payout_ratio,
            "dividend_safety_score": "SECURE" if fcf_payout_ratio < 65.0 else "VULNERABLE"
        }
        self.db.update_state(f"dividend_cache_{symbol}", payload)
        return True

    def construct_comprehensive_matrix(self, symbol):
        """Combines structural, fundamental, and momentum layers into a single profile."""
        vol_metrics = self.replicate_volume_velocity(symbol)
        tech_metrics = self.replicate_mean_reversion(symbol)
        
        moat = self.db.get_state(f"moat_cache_{symbol}", {"roic": 0.0, "debt_to_equity": 0.0, "gross_margin": 0.0})
        dividend = self.db.get_state(f"dividend_cache_{symbol}", {"annual_payout": 0.0, "fcf_payout_ratio": 0.0, "dividend_safety_score": "UNKNOWN"})

        return {
            "symbol": symbol,
            "volume_velocity": vol_metrics,
            "technical_reversion": tech_metrics,
            "fundamental_moat": moat,
            "dividend_sustainability": dividend
        }

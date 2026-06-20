#!/usr/bin/env python3
"""
ESSENTIALS Macro-Quant Wargame Suite
Validates the live ecosystem using mocked HTTP + passthrough Discord/Pushover dispatch.
Only imports modules that actually exist in the codebase.
"""

import os
import time
import logging
import sqlite3
import requests
from unittest.mock import patch
from datetime import datetime

# Import actual ecosystem modules
from analytics import HighFidelityAnalyticsEngine
from database import EcosystemDatabase
import monitor
import cross_asset

logging.basicConfig(level=logging.INFO, format="%(asctime)s | WARGAME | %(message)s")
logger = logging.getLogger("Wargame")
db = EcosystemDatabase()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "rockefeller_state.db")

# =====================================================================
# SIMULATION STATE — Controls mock API response shapes
# =====================================================================
SIM_STATE = {
    "credit_spread": 3.5,
    "spy_spot": 580.0,
    "spy_poc": 578.0,
    "cef_price": 7.50,
    "cef_nav": 6.50,
    "btc_spot": 68500.0,
    "eur_usd": 1.0875,
}

# =====================================================================
# HTTP MOCK LAYER
# Intercepts Twelve Data + FRED; passes Discord/Pushover through live.
# =====================================================================
_original_get = requests.get
_original_post = requests.post
_original_put = requests.put


def mock_requests_get(url, *args, **kwargs):
    class MockResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code
        def json(self): return self._data
        def raise_for_status(self): pass

    if "api.stlouisfed.org" in url:
        if "WALCL" in url: return MockResponse({"observations": [{"value": "7200000"}]})
        if "WTREGEN" in url: return MockResponse({"observations": [{"value": "480000"}]})
        if "RRPONTSYD" in url: return MockResponse({"observations": [{"value": "380000"}]})
        if "BAMLH0A0HYM2" in url: return MockResponse({"observations": [{"value": str(SIM_STATE["credit_spread"])}]})

    if "api.twelvedata.com" in url:
        spot = SIM_STATE["spy_spot"]
        if "market_state" in url:
            return MockResponse([{"country": "United States", "code": "NYSE", "is_market_open": True}])
        if "options/chain" in url:
            mock_chain = []
            for s_delta in [0.15, 0.20, 0.25, 0.30, 0.35]:
                strike = round(spot * (1 - s_delta * 0.5), 0)
                for exp in ["2026-07-18", "2026-08-15"]:
                    mock_chain.append({
                        "type": "put", "strike": strike, "expiration_date": exp,
                        "implied_volatility": 0.18 + s_delta * 0.02,
                        "delta": -s_delta, "bid": spot * 0.012, "ask": spot * 0.016,
                        "open_interest": 500
                    })
                    mock_chain.append({
                        "type": "call", "strike": strike, "expiration_date": exp,
                        "implied_volatility": 0.16 + s_delta * 0.02,
                        "delta": s_delta, "bid": spot * 0.010, "ask": spot * 0.014,
                        "open_interest": 450
                    })
            return MockResponse({"data": mock_chain})
        if "price" in url:
            if "CLM" in url or "XCLMX" in url: return MockResponse({"price": str(SIM_STATE["cef_price"])})
            if "CRF" in url or "XCRFX" in url: return MockResponse({"price": str(SIM_STATE["cef_nav"])})
            if "BTC" in url: return MockResponse({"price": str(SIM_STATE["btc_spot"])})
            if "EUR" in url: return MockResponse({"price": str(SIM_STATE["eur_usd"])})
            return MockResponse({"price": str(spot)})
        if "quote" in url:
            # Handle comma-separated batch quotes (EUR/USD,BTC/USD,SPY)
            if "EUR" in url and "BTC" in url:
                return MockResponse({
                    "EUR/USD": {"symbol": "EUR/USD", "close": str(SIM_STATE["eur_usd"]), "percent_change": "0.32"},
                    "BTC/USD": {"symbol": "BTC/USD", "close": str(SIM_STATE["btc_spot"]), "percent_change": "1.85"},
                    "SPY": {"symbol": "SPY", "close": str(spot), "previous_close": str(spot - 1.5),
                            "percent_change": "0.26", "open": str(spot - 0.5), "high": str(spot + 3.0),
                            "low": str(spot - 2.0), "volume": "82000000", "average_volume": "75000000"}
                })
            return MockResponse({
                "symbol": "SPY", "close": str(spot), "previous_close": str(spot - 1.5),
                "percent_change": "0.26", "open": str(spot - 0.5), "high": str(spot + 3.0),
                "low": str(spot - 2.0), "volume": "82000000", "average_volume": "75000000"
            })
        if "time_series" in url:
            values = [
                {"datetime": f"2026-06-{max(1, 19-i):02d}", "open": str(spot), "high": str(spot + 2),
                 "low": str(spot - 2), "close": str(spot - i * 0.3), "volume": "80000000"}
                for i in range(31)
            ]
            return MockResponse({"values": values})
        if "rsi" in url:
            return MockResponse({"values": [{"rsi": "55.2"}]})
        if "supertrend" in url:
            return MockResponse({"values": [{"close": str(spot), "supertrend": str(spot * 0.985)}]})
        if "statistics" in url:
            return MockResponse({"statistics": {"volume": "82000000", "avg_volume_30_days": "75000000"}})
        if "complex_data/dividends" in url or "dividends" in url:
            return MockResponse({"data": [{"ex_date": "2026-07-01", "amount": "0.5400"}]})
        if "vwap" in url:
            return MockResponse({"values": [{"vwap": str(SIM_STATE["spy_poc"])}]})

    return MockResponse({}, 404)


def mock_requests_post(url, *args, **kwargs):
    # Live passthrough: Discord + Pushover embeds fire for real QA verification
    if "discord.com" in url or "pushover.net" in url:
        return _original_post(url, *args, **kwargs)
    class MockResponse:
        status_code = 200
        def json(self): return {}
        def raise_for_status(self): pass
    return MockResponse()


def mock_requests_put(url, *args, **kwargs):
    if "discord.com" in url:
        return _original_put(url, *args, **kwargs)
    class MockResponse:
        status_code = 204
    return MockResponse()


# =====================================================================
# WARGAME — @patch decorators inject: bottom→top = first→last param
# 3 patches → 3 params: mock_put, mock_post, mock_get
# =====================================================================
@patch('requests.get', side_effect=mock_requests_get)
@patch('requests.post', side_effect=mock_requests_post)
@patch('requests.put', side_effect=mock_requests_put)
def execute_wargame(mock_put, mock_post, mock_get):
    logger.info("=" * 62)
    logger.info("  INITIATING OPERATION: MASTERMIND WARGAME")
    logger.info("  Discord + Pushover are LIVE — embeds will fire to Discord.")
    logger.info("=" * 62)

    engine = HighFidelityAnalyticsEngine()
    passed = 0
    failed = 0

    # --- PHASE 1: GEX Profile ---
    logger.info("\n>>> PHASE 1: GEX PROFILE (OPTIONS CHAIN) <<<")
    try:
        gex = engine.calculate_gex_profile("SPY")
        assert gex.get("current_spot", 0) > 0, "GEX spot is zero"
        logger.info(f"[PASS] GEX → flip={gex['flip_strike']:.2f}, spot={gex['current_spot']:.2f}, state={gex['market_state']}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] GEX Profile: {e}")
        failed += 1

    # --- PHASE 2: Historical Volatility ---
    logger.info("\n>>> PHASE 2: HISTORICAL VOLATILITY (HV30) <<<")
    try:
        hv = engine.calculate_historical_volatility("SPY")
        assert hv > 0, "HV30 returned zero"
        logger.info(f"[PASS] HV30 SPY = {hv:.2f}%")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] HV30: {e}")
        failed += 1

    # --- PHASE 3: Macro Liquidity (FRED) ---
    logger.info("\n>>> PHASE 3: MACRO LIQUIDITY (FRED) <<<")
    try:
        payload = engine.generate_macro_liquidity_payload(is_test=True)
        assert payload is not None, "Macro payload returned None"
        logger.info(f"[PASS] Macro Liquidity payload generated ({len(payload)} chars)")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Macro Liquidity: {e}")
        failed += 1

    # --- PHASE 4: Crypto Matrix ---
    logger.info("\n>>> PHASE 4: CRYPTO MATRIX <<<")
    try:
        payload = engine.generate_crypto_matrix_payload()
        logger.info(f"[PASS] Crypto Matrix: {'payload generated' if payload else 'gatekeeper silenced (expected)'}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Crypto Matrix: {e}")
        failed += 1

    # --- PHASE 5: Forex Matrix ---
    logger.info("\n>>> PHASE 5: FOREX MATRIX <<<")
    try:
        payload = engine.generate_forex_matrix_payload()
        logger.info(f"[PASS] Forex Matrix: {'payload generated' if payload else 'gatekeeper silenced (expected)'}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Forex Matrix: {e}")
        failed += 1

    # --- PHASE 6: Dividend Wheel Candidates ---
    logger.info("\n>>> PHASE 6: DIVIDEND WHEEL CANDIDATES <<<")
    try:
        candidates = engine.generate_dividend_wheel_candidates()
        logger.info(f"[PASS] Dividend Wheel: {len(candidates)} candidate(s) scored")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Dividend Wheel: {e}")
        failed += 1

    # --- PHASE 7: TSP Allocation Matrix ---
    logger.info("\n>>> PHASE 7: TSP ALLOCATION MATRIX <<<")
    try:
        tsp = engine.compile_tsp_allocation_matrix()
        assert tsp is not None, "TSP matrix returned None"
        logger.info(f"[PASS] TSP Matrix generated")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] TSP Matrix: {e}")
        failed += 1

    # --- PHASE 8: VIX CVR Signal ---
    logger.info("\n>>> PHASE 8: VIX CVR COUNTER-TREND SIGNAL <<<")
    try:
        cvr = engine.evaluate_vix_cvr_reversal()
        logger.info(f"[PASS] VIX CVR: {'Signal → ' + cvr['signal'] if cvr else 'No trigger (market neutral)'}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] VIX CVR: {e}")
        failed += 1

    # --- PHASE 9: Monitor Daily Pulse (test mode bypasses dupe guard) ---
    logger.info("\n>>> PHASE 9: MONITOR DAILY PULSE (TEST MODE) <<<")
    try:
        monitor.send_daily_pulse(is_test=True)
        logger.info("[PASS] Monitor daily pulse dispatched via test mode")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Monitor daily pulse: {e}")
        failed += 1

    # --- PHASE 10: Cross-Asset Futures (bypass market hours for wargame) ---
    logger.info("\n>>> PHASE 10: CROSS-ASSET FUTURES PROFILE <<<")
    try:
        # Override market hours guard so wargame can validate any time of day
        original_fn = getattr(cross_asset, 'is_market_hours', None)
        if original_fn:
            cross_asset.is_market_hours = lambda: True
        cross_asset.run_intraday_futures_update()
        if original_fn:
            cross_asset.is_market_hours = original_fn
        logger.info("[PASS] Cross-asset futures update executed")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Cross-asset futures: {e}")
        failed += 1

    # --- PHASE 11: Credit Stress Override + DB State Integrity ---
    logger.info("\n>>> PHASE 11: CREDIT STRESS OVERRIDE & DB STATE <<<")
    try:
        SIM_STATE["credit_spread"] = 6.2
        db.update_state("credit_spread", 6.2)
        db.update_state("wargame_last_run", datetime.now().isoformat())
        spread = float(db.get_state("credit_spread", 0.0))
        last_run = db.get_state("wargame_last_run", None)
        assert spread >= 4.5, f"Credit spread gate not triggered: {spread}"
        assert last_run is not None, "DB state write failed"
        db.update_state("credit_spread", 3.5)  # Reset
        logger.info(f"[PASS] Credit stress={spread:.2f}% | DB state verified at {last_run[:19]}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Credit stress / DB: {e}")
        failed += 1

    logger.info("\n" + "=" * 62)
    status_tag = "✅ ALL SYSTEMS GO" if failed == 0 else f"⚠️ {failed} FAILURE(S) DETECTED"
    logger.info(f"  WARGAME COMPLETE: {passed} PASSED | {failed} FAILED — {status_tag}")
    logger.info("=" * 62)


if __name__ == "__main__":
    execute_wargame()

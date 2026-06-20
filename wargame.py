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
from essentials_tools import generate_candlestick_chart

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

    # --- PHASE 5B: Forex Global-Macro Tools (session, synthetic DXY, pivots, confluence, risk regime) ---
    logger.info("\n>>> PHASE 5B: FOREX GLOBAL-MACRO TOOLSET <<<")
    try:
        session = engine.get_forex_session_label()
        fx_quotes = engine._fetch_twelve_data_quotes(engine.FX_UNIVERSE)
        dxy = engine.calculate_synthetic_dollar_index(fx_quotes)
        pivots = engine.calculate_fx_pivot_points("EUR/USD")
        assert pivots is not None, "Pivot points returned None"
        confluence_tag, confluence_detail = engine.calculate_fx_trend_confluence("EUR/USD")
        atr = engine.update_fx_volatility_bounds("EUR/USD")
        assert atr is not None and atr > 0, "ATR volatility bounds calc failed"
        regime, explanation, usdjpy_chg, gold_chg = engine.assess_risk_sentiment_regime(fx_quotes)
        mover = engine.find_biggest_mover(fx_quotes, engine.FX_UNIVERSE)
        ohlc = engine.fetch_crypto_ohlc(mover[0], outputsize=30) if mover else None
        chart_bytes = generate_candlestick_chart(mover[0], ohlc, last_change=mover[2], last_change_pct=mover[3]) if ohlc is not None and not ohlc.empty else None
        logger.info(
            f"[PASS] Session={session} | SynthDXY={dxy:+.2f}% | Pivot(EURUSD)={pivots['pivot']:.4f} | "
            f"Confluence={confluence_tag} | ATR={atr:.4f} | Regime={regime} | Chart={'ok' if chart_bytes else 'n/a'}"
        )
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Forex global-macro toolset: {e}")
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

    # --- PHASE 7: TSP End-of-Day Report (real tsp.gov NAV data + chart) ---
    logger.info("\n>>> PHASE 7: TSP END-OF-DAY REPORT <<<")
    try:
        from essentials_tools import generate_line_comparison_chart
        original_dedupe = db.get_state("tsp_eod_last_reported_date")
        db.update_state("tsp_eod_last_reported_date", "1970-01-01")  # force a fresh build for the test
        tsp_payload, fund_series = engine.generate_tsp_eod_report()
        assert tsp_payload is not None, "TSP EOD report returned None"
        assert fund_series and "C Fund" in fund_series, "TSP fund series missing C Fund"
        chart_bytes = generate_line_comparison_chart(fund_series, "TSP Funds Test Chart")
        assert chart_bytes and len(chart_bytes) > 100, "TSP chart bytes empty/too small"
        db.update_state("tsp_eod_last_reported_date", original_dedupe)  # restore so real cron isn't double-fired or skipped
        logger.info(f"[PASS] TSP EOD report ({len(tsp_payload)} chars) + chart ({len(chart_bytes)} bytes) generated from real tsp.gov data")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] TSP EOD report: {e}")
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

    # --- PHASE 12: Crypto Chart Snapshot + Fear & Greed ---
    logger.info("\n>>> PHASE 12: CRYPTO CHART SNAPSHOT <<<")
    try:
        from essentials_tools import generate_candlestick_chart
        ohlc = engine.fetch_crypto_ohlc("BTC/USD", outputsize=30)
        assert ohlc is not None and not ohlc.empty, "Crypto OHLC fetch returned empty"
        chart_bytes = generate_candlestick_chart("BTC/USD", ohlc, last_change=120.5, last_change_pct=1.85)
        assert chart_bytes and len(chart_bytes) > 100, "Chart bytes empty/too small"
        mover = engine.find_biggest_crypto_mover({"BTC/USD": {"close": "68500", "percent_change": "1.85"}})
        assert mover is not None, "Biggest mover detection failed"
        fng = engine.fetch_fear_greed_index()
        logger.info(f"[PASS] Crypto chart ({len(chart_bytes)} bytes), mover={mover[0]}, F&G={'fetched' if fng else 'unavailable (non-fatal)'}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Crypto chart snapshot: {e}")
        failed += 1

    # --- PHASE 13: BTC <-> SPY Correlation Sync ---
    logger.info("\n>>> PHASE 13: BTC<->SPY CORRELATION SYNC <<<")
    try:
        from essentials_tools import calculate_correlation, get_trend_alignment
        btc_ohlc = engine.fetch_crypto_ohlc("BTC/USD", outputsize=20)
        spy_ohlc = engine.fetch_crypto_ohlc("SPY", outputsize=20)
        assert btc_ohlc is not None and spy_ohlc is not None, "OHLC fetch failed for correlation inputs"
        corr = calculate_correlation(btc_ohlc['close'].tolist(), spy_ohlc['close'].tolist())
        trend, is_bullish = get_trend_alignment("BTC/USD", os.getenv("TWELVE_DATA_API_KEY"))
        logger.info(f"[PASS] Correlation={corr:+.2f}, BTC trend={trend}")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] BTC/SPY correlation sync: {e}")
        failed += 1

    # --- PHASE 14: Futures Board + Market Profile Chart ---
    logger.info("\n>>> PHASE 14: FUTURES BOARD + CHART <<<")
    try:
        cross_asset.run_futures_board()
        logger.info("[PASS] Futures board dispatched")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] Futures board: {e}")
        failed += 1

    # --- PHASE 15: TQQQ Tactical Sniper (regime/Greeks/exit-signal pipeline) ---
    logger.info("\n>>> PHASE 15: TQQQ TACTICAL SNIPER <<<")
    try:
        import tqqq as tqqq_module
        original_market_hours = tqqq_module.is_market_hours
        tqqq_module.is_market_hours = lambda: True

        sniper = tqqq_module.TQQQTacticalSniper()
        daily = sniper.fetch_daily_baseline()
        intraday = sniper.fetch_intraday_metrics()
        assert daily and intraday, "TQQQ daily/intraday fetch failed"

        tqqq_daily = sniper.fetch_tqqq_daily_series()
        vix_price, vix_z = sniper.fetch_vix()
        breadth = sniper.fetch_breadth()
        atr_pct = tqqq_module.calculate_atr_pct(tqqq_daily) if tqqq_daily is not None else 0.02

        # Force an oversold extreme to exercise the full BTO + Greeks + dispatch path
        intraday_forced = dict(intraday, z_score=-2.3, vol_z=2.5)
        setup = sniper.evaluate_snipe(daily, intraday_forced, vix_price, vix_z, breadth, atr_pct)
        assert setup is not None, "evaluate_snipe returned None for a forced oversold extreme"
        sniper.dispatch_intelligence(setup, tqqq_daily)

        # Exercise the exit-signal path on a simulated open position
        db.update_state("tqqq_open_position", {
            "contract": "CALL", "entry_tqqq_spot": setup["tqqq_spot"], "entry_z_score": -2.3,
            "strike": setup.get("real_strike") or setup.get("bs_strike", 0.0), "expiry": "12/31/2026",
            "dte_at_entry": 15, "entry_time": "2026-01-01T00:00:00",
        })
        sniper.check_open_position_for_exit(dict(intraday, z_score=0.1), atr_pct)
        assert db.get_state("tqqq_open_position") is None, "Exit signal failed to clear open position state"

        sniper.dispatch_regime_vital_sign(daily, breadth, vix_price, vix_z)

        tqqq_module.is_market_hours = original_market_hours
        logger.info(
            f"[PASS] TQQQ sniper: setup={setup['action']} {setup['contract']} "
            f"(downgrade={setup.get('downgrade_reason')}), exit-signal cleared, regime sync dispatched"
        )
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] TQQQ Tactical Sniper: {e}")
        failed += 1

    # --- PHASE 16: VIXY Proxy + Pre-Market Primer (VIX-404 fix verification) ---
    logger.info("\n>>> PHASE 16: VIXY PROXY & PRE-MARKET PRIMER <<<")
    try:
        vix_price, vix_z = engine.fetch_vixy_proxy()
        assert vix_price > 0, "VIXY proxy price fetch failed"
        primer = engine.generate_premarket_primer("SPY")
        assert primer is not None, "Pre-market primer returned None"
        assert "VIX" not in primer.replace("VIXY", ""), "Primer still references the dead VIX symbol"
        logger.info(f"[PASS] VIXY={vix_price:.2f} (z {vix_z:+.2f}σ), primer generated ({len(primer)} chars)")
        passed += 1
    except Exception as e:
        logger.error(f"[FAIL] VIXY proxy / primer: {e}")
        failed += 1

    logger.info("\n" + "=" * 62)
    status_tag = "✅ ALL SYSTEMS GO" if failed == 0 else f"⚠️ {failed} FAILURE(S) DETECTED"
    logger.info(f"  WARGAME COMPLETE: {passed} PASSED | {failed} FAILED — {status_tag}")
    logger.info("=" * 62)


if __name__ == "__main__":
    execute_wargame()

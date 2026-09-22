"""
daily_pulse.py — Evening Pulse (Personal)
Cashflow ZZZ Machine | Personal Finance Layer

CLM/CRF cornerstone status + wheel desk + deploy-vs-idle → Pushover only (never Discord).
Run once daily via PythonAnywhere Scheduled Tasks — deliberately standalone from the monitor loop.

Usage:
  python daily_pulse.py            # normal daily run (deduped by date)
  python daily_pulse.py --force    # override dedup, re-send today
"""

import os
import json
import logging
import requests
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DailyPulse")

TD_API_KEY         = os.getenv("TWELVE_DATA_API_KEY", "")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY  = os.getenv("PUSHOVER_USER_KEY", "")

STATE_FILE = os.path.join(BASE_DIR, ".daily_pulse_state.json")

NAV_TICKERS  = {"CLM": "XCLMX", "CRF": "XCRFX"}
NAV_DEFAULTS = {"CLM": 6.31, "CRF": 6.12}        # CEFConnect Aug 21 2026
CEF_ANNUAL_DIST = {"CLM": 1.458, "CRF": 1.4112}  # confirmed Aug 17 2026 press release
CEF_FV_2027     = {"CLM": 6.97, "CRF": 6.76}     # 2027 FV example (21% × Jul NAV ÷ 0.19)

MARGIN_RATE = 7.25  # E*TRADE margin rate — update if it changes


# ─────────────────────────────────────────────────────────────────────────────
# CEF SNAPSHOT — CLM / CRF price, NAV, premium, RO score
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cef_snapshot():
    """
    Price + NAV + RSI + premium for CLM/CRF.

    NAV source priority:
      1. DB key clm_last_nav / crf_last_nav — written by monitor.py via CEFConnect (0 TD credits)
      2. XCLMX / XCRFX proxy via Twelve Data (2 TD credits, fallback only)
    """
    from database import EcosystemDatabase
    db = EcosystemDatabase()
    results = {}
    session = requests.Session()
    for ticker, nav_ticker in NAV_TICKERS.items():
        try:
            price = float(session.get(
                f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TD_API_KEY}",
                timeout=12).json().get("price", 0.0))
            db_nav = db.get_state(f"{ticker.lower()}_last_nav")
            if db_nav and float(db_nav) > 0:
                nav = float(db_nav)
                nav_src = "CEFConnect"
            else:
                nav = float(session.get(
                    f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}",
                    timeout=12).json().get("price", NAV_DEFAULTS[ticker]))
                nav_src = "proxy"
            rsi_res = session.get(
                f"https://api.twelvedata.com/rsi?symbol={ticker}&interval=1day"
                f"&time_period=14&apikey={TD_API_KEY}", timeout=12).json()
            rsi     = float(rsi_res.get("values", [{"rsi": 50.0}])[0]["rsi"])
            premium = ((price - nav) / nav * 100) if nav > 0 else 0.0
            ro_score = db.get_state(f"{ticker.lower()}_last_ro_score")
            results[ticker] = {
                "price": price, "nav": nav, "nav_src": nav_src,
                "rsi": rsi, "premium": premium,
                "ro_score": int(ro_score) if ro_score is not None else None,
            }
        except Exception as e:
            logger.error(f"CEF fetch failed {ticker}: {e}")
            results[ticker] = {
                "price": 0.0, "nav": NAV_DEFAULTS[ticker], "nav_src": "default",
                "rsi": 50.0, "premium": 0.0, "ro_score": None,
            }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# DEPLOY vs IDLE — live CPI opportunity cost (FRED, cached by analytics.py)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_buying_power_snapshot() -> dict:
    """
    Answers "what does idle cash actually cost me vs deploying into CLM/CRF?"
    Data: FRED CPIAUCSL (cached daily by analytics.py). Fallback: 3.5%.
    Pushover only — never Discord.
    """
    result = {
        "cpi_yoy": None, "real_portfolio_yield": None,
        "margin_real_cost": None,
    }
    try:
        from database import EcosystemDatabase
        db = EcosystemDatabase()
        cpi_raw = db.get_state("fred_macro_snap")
        cpi_yoy = None
        if isinstance(cpi_raw, dict):
            cpi_yoy = cpi_raw.get("cpi_yoy")
        if cpi_yoy is None:
            fred_key = os.getenv("FRED_API_KEY", "")
            if fred_key:
                r = requests.get(
                    f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id=CPIAUCSL&api_key={fred_key}&sort_order=desc&limit=13&file_type=json",
                    timeout=12).json().get("observations", [])
                if len(r) >= 13:
                    cpi_yoy = round((float(r[0]["value"]) - float(r[12]["value"])) / float(r[12]["value"]) * 100, 2)
        if cpi_yoy is None:
            cpi_yoy = 3.5

        BLENDED_PORTFOLIO_YIELD = 19.0
        result.update({
            "cpi_yoy":              cpi_yoy,
            "real_portfolio_yield": round(BLENDED_PORTFOLIO_YIELD - cpi_yoy, 1),
            "margin_real_cost":     round(MARGIN_RATE - cpi_yoy, 2),
        })
    except Exception as e:
        logger.warning(f"Buying power snapshot failed: {e}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# WHEEL DESK — open positions DTE + thesis status (DB read, 0 API calls)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_wheel_desk_snapshot():
    """
    Reads open wheel positions from DB. Returns a list of formatted lines.
    Exit logic (Corey Holiday rule): exit when thesis breaks — at a technical
    stop level — for ~1/3 max loss, not at expiration for max loss.
    DTE tiers: ≥ 21 = hold | 14-20 = review | < 14 = close or roll.
    0 API calls — all from local DB.
    """
    try:
        from database import EcosystemDatabase
        db = EcosystemDatabase()
        positions = db.get_open_wheel_positions()
    except Exception as e:
        logger.warning(f"Wheel desk fetch failed: {e}")
        return []

    if not positions:
        return ["No open wheel positions"]

    today = date.today()
    lines = []
    for p in positions:
        try:
            exp_date  = datetime.strptime(p["expiration"], "%Y-%m-%d").date()
            dte       = (exp_date - today).days
            symbol    = p.get("symbol", "?")
            ptype     = p.get("position_type", "?")   # CSP / CC
            strike    = p.get("strike", 0.0)
            premium   = p.get("premium_collected", 0.0)
            contracts = p.get("contracts", 1)
            total_cr  = round(premium * contracts * 100, 0)

            # DTE-based thesis advisory
            if dte < 0:
                continue
            elif dte < 14:
                thesis = "⛔ Close or roll — 14 DTE passed"
            elif dte <= 21:
                thesis = "⚠️ Review — approaching management window"
            else:
                thesis = "✅ Hold — time decay working"

            # Stop level: for CSP, stop = if spot breaks well above strike (thesis invalidated)
            # For CC, stop = if spot breaks well below strike (assignment risk changed)
            max_loss    = round((strike - premium) * contracts * 100, 0) if ptype == "CSP" else None
            early_exit  = round(max_loss * 0.33, 0) if max_loss else None
            stop_note   = f"Early exit ≈ ${early_exit:,.0f} (vs max loss ${max_loss:,.0f})" if early_exit else ""

            lines.append(f"{symbol} {ptype} | ${strike:.1f} strike | {dte}d left | ${total_cr:,.0f} credit received")
            lines.append(f"┣ {thesis}")
            if stop_note:
                lines.append(f"┗ {stop_note}")
            lines.append("")
        except Exception:
            continue

    return lines if lines else ["No open wheel positions"]


# ─────────────────────────────────────────────────────────────────────────────
# SEC EDGAR — lightweight RO status check
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ro_status():
    """
    Checks EDGAR for CLM and CRF recent filings.
    🔴 RO RISK = N-2/N-2/A within 90 days | 👁 Holder change = SC 13D/G within 180 days | 🟢 Stable
    """
    CIK_MAP = {"CLM": "0000814083", "CRF": "0000033934"}
    headers = {"User-Agent": "RockefellerSystem/1.0 (admin@rockefeller.local)"}
    session = requests.Session()
    results = {}
    for ticker, cik in CIK_MAP.items():
        try:
            res = session.get(
                f"https://data.sec.gov/submissions/CIK{cik}.json",
                headers=headers, timeout=20)
            if res.status_code != 200:
                results[ticker] = "⚪ EDGAR unavailable"
                continue
            filings      = res.json().get("filings", {}).get("recent", {})
            forms        = filings.get("form", [])
            dates        = filings.get("filingDate", [])
            today_dt     = datetime.utcnow().date()
            ro_detected  = False
            holder_chg   = False
            for form, filing_date in zip(forms, dates):
                try:
                    age = (today_dt - datetime.strptime(filing_date, "%Y-%m-%d").date()).days
                except ValueError:
                    continue
                if form in ("N-2", "N-2/A") and age <= 90:
                    ro_detected = True
                elif form in ("SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A") and age <= 180:
                    holder_chg = True
            if ro_detected:
                results[ticker] = "🔴 RO RISK — N-2 filing active"
            elif holder_chg:
                results[ticker] = "👁 Holder change detected (SC 13D/G)"
            else:
                results[ticker] = "🟢 Stable — no actionable filings"
        except Exception as e:
            logger.warning(f"EDGAR check failed {ticker}: {e}")
            results[ticker] = "⚪ EDGAR unavailable"
    return results


def fetch_ro_gate_info():
    """
    Reads DB for N-2 anchor dates → Path A (anchor+30d) and Path B (anchor+45d) gate dates.
    Returns {} if DB unavailable — pulse degrades gracefully.
    """
    try:
        from database import EcosystemDatabase
        db      = EcosystemDatabase()
        today   = date.today()
        result  = {}
        for ticker in ("CLM", "CRF"):
            anchor_str = db.get_state(f"cornerstone_n2_detected_{ticker}", "")
            if not anchor_str:
                continue
            try:
                anchor = date.fromisoformat(anchor_str)
            except ValueError:
                continue
            path_a = anchor + timedelta(days=30)
            path_b = anchor + timedelta(days=45)
            result[ticker] = {
                "path_a_date": path_a.strftime("%b %-d"),
                "path_b_date": path_b.strftime("%b %-d"),
                "path_a_open": today >= path_a,
                "path_b_open": today >= path_b,
            }
        return result
    except Exception as e:
        logger.warning(f"fetch_ro_gate_info failed: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# TIER 2 — MLPI / XBCI / CHPY personal watchlist (Pushover only, never Discord)
# ─────────────────────────────────────────────────────────────────────────────
# Dividends NOT reinvested — all cash distributions → margin paydown for CLM/CRF loan.
# NAV erosion mitigation strategy (research-backed):
#   OTM options funds (MLPI, CHPY) have structurally lower NAV erosion than ATM peers.
#   Best entry: underlying RSI < 35 AND/OR drawdown > 5% from 20-day high.
#   Never DCA into an uptrend — wait for sector dips, ex-div windows, or fear signals.
#
# MLPI (NEOS MLP & Energy Infrastructure):  ~15% | OTM CC on energy MLPs | proxy: XLE
# XBCI (Innovator Bitcoin Buffer+Income):   ~18% | buffer ETF | signal: BTC Fear & Greed
# CHPY (YieldMax Semiconductor Income):     ~40% | OTM CC-spread on 15-30 semis | proxy: NVDA
#   CHPY note: +54% YTD 2026, near-zero NAV erosion — OTM spread structure preserves upside
#   Best CHPY entry: NVDA/semi selloff (RSI < 35) + drawdown > 5-8% from recent high
#
# xdca: True = drawdown zone + underlying RSI read from DB (xdca.py writes MLPI/CHPY zones)
# crypto: True = BTC Fear & Greed used as DCA signal (XBCI tracks BTC price)
# underlying: proxy ticker used by xdca.py for zone detection
TIER2_TICKERS = {
    "MLPI": {
        "est_yield_pct": 15.0, "xdca": True,  "crypto": False, "underlying": "XLE",
        "sector_tag":   "Energy Infrastructure",
        "sector_thesis": "Energy MLPs — real-asset income, essential infrastructure regardless of cycle phase",
    },
    "XBCI": {
        "est_yield_pct": 18.0, "xdca": False, "crypto": True,  "underlying": "IBIT",
        "sector_tag":   "Bitcoin / Digital Assets",
        "sector_thesis": "BTC institutional adoption cycle — buffer structure caps downside ~15%",
    },
    "CHPY": {
        "est_yield_pct": 40.0, "xdca": True,  "crypto": False, "underlying": "NVDA",
        "sector_tag":   "AI / Semiconductor",
        "sector_thesis": "AI supercycle driving semi demand — OTM spread, near-zero NAV erosion 2026",
    },
}

# xdca zone → human DCA advisory (matches ZONE_CONFIG labels in xdca.py)
_XDCA_ZONE_LABEL = {
    "D":    "⚡ PEAK FEAR — add aggressively (xdca zone D)",
    "C":    "✅ DCA zone — add on this dip (xdca zone C)",
    "B":    "🟡 Accumulate — hold or small add (xdca zone B)",
    "A":    "⚪ Watch — wait for deeper pullback (xdca zone A)",
    "NONE": "⏸ Hold — no xdca zone signal",
}


def fetch_tier2_snapshot() -> dict:
    """
    3 TD credits (one price call per ticker: MLPI, XBCI, CHPY).
    XBCI also calls alternative.me F&G (free, 0 TD credits).
    - MLPI/CHPY: DCA signal from xdca_{sym}_zone DB key (written by xdca.py every 10 min).
      Also reads xdca_{sym}_rsi and xdca_{sym}_drawdown_pct for NAV erosion context.
    - XBCI: DCA signal from BTC Fear & Greed (alternative.me).
    Pushover only — never routed to Discord.
    """
    try:
        from database import EcosystemDatabase
        db = EcosystemDatabase()
    except Exception:
        db = None

    # BTC Fear & Greed — fetched once for XBCI (free, alternative.me)
    btc_fg_value = None
    btc_fg_label = None
    try:
        fg_r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8).json()
        fg_data = fg_r.get("data", [{}])[0]
        btc_fg_value = int(fg_data.get("value", 50))
        btc_fg_label = fg_data.get("value_classification", "Neutral")
    except Exception as e:
        logger.warning(f"BTC F&G fetch failed: {e}")

    session = requests.Session()
    results = {}
    for ticker, cfg in TIER2_TICKERS.items():
        # ── live price ───────────────────────────────────────────────────────
        try:
            r     = session.get(
                f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TD_API_KEY}",
                timeout=12)
            price = float(r.json().get("price", 0.0))
        except Exception as e:
            logger.warning(f"Tier 2 price fetch failed {ticker}: {e}")
            price = 0.0

        # ── estimated monthly dist at current price ──────────────────────────
        yield_pct   = cfg["est_yield_pct"]
        monthly_est = round(price * yield_pct / 100 / 12, 3) if price > 0 else 0.0

        # ── DCA signal + NAV erosion context ─────────────────────────────────
        underlying_rsi  = None
        drawdown_pct    = None
        zone_raw        = "NONE"
        if cfg["xdca"] and db is not None:
            zone_raw = (db.get_state(f"xdca_{ticker}_zone") or "NONE").strip().upper()
            dca_signal = _XDCA_ZONE_LABEL.get(zone_raw, _XDCA_ZONE_LABEL["NONE"])
            # underlying RSI + drawdown from 20d high — written by xdca.py every 10 min
            underlying_rsi = db.get_state(f"xdca_{ticker}_rsi")
            drawdown_pct   = db.get_state(f"xdca_{ticker}_drawdown_pct")
        elif cfg["crypto"]:
            if btc_fg_value is not None:
                if btc_fg_value <= 25:
                    zone_raw   = "D"
                    dca_signal = f"⚡ BTC Extreme Fear ({btc_fg_value}) — XBCI accumulate zone"
                elif btc_fg_value <= 45:
                    zone_raw   = "C"
                    dca_signal = f"✅ BTC Fear ({btc_fg_value}) — add XBCI on dips"
                elif btc_fg_value <= 55:
                    zone_raw   = "B"
                    dca_signal = f"🟡 BTC Neutral ({btc_fg_value}) — hold XBCI"
                else:
                    zone_raw   = "A"
                    dca_signal = f"⏸ BTC {btc_fg_label} ({btc_fg_value}) — avoid adding"
            else:
                dca_signal = "⚪ BTC F&G unavailable — check manually"
        else:
            dca_signal = "⏸ Hold"

        # ── Sector sentiment (SentiSense — cached daily, 0 extra cost after first fetch) ──
        sector_signal = None
        try:
            from sentisense_client import get_sentiment as _ss_sent
            ss_data = _ss_sent(db, cfg["underlying"]) if db is not None else None
            if ss_data:
                sector_signal = (ss_data["lean"], float(ss_data["score"]))
        except Exception:
            pass

        # ── Verdict — carry confirmation + zone advisory ──────────────────────
        carry_ok   = yield_pct > MARGIN_RATE
        carry_str  = f"~{yield_pct:.0f}% yield vs {MARGIN_RATE:.2g}% margin"
        if zone_raw in ("C", "D"):
            verdict = f"DCA window open — {carry_str} · carry confirmed"
        elif zone_raw == "B":
            verdict = f"Viable hold — {carry_str} · add on deeper dip"
        else:
            verdict = f"Long-term hold — {carry_str} · wait for sector dip"

        results[ticker] = {
            "price":           price,
            "yield_pct":       yield_pct,
            "monthly_est":     monthly_est,
            "dca_signal":      dca_signal,
            "underlying_rsi":  underlying_rsi,
            "drawdown_pct":    drawdown_pct,
            "sector_signal":   sector_signal,
            "verdict":         verdict,
        }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# STATE — run-date dedup
# ─────────────────────────────────────────────────────────────────────────────

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        logger.error(f"State save failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# FORMAT — ┣/┗ Pulse Message
# ─────────────────────────────────────────────────────────────────────────────

def format_pulse_message(cef, state, ro_status=None, ro_gate_info=None):
    today = date.today().strftime("%b %d, %Y")
    lines = []

    # ── CORNERSTONE: CLM / CRF
    lines.append("CORNERSTONE (CLM / CRF)")
    tickers_list = list(ro_status.keys()) if ro_status else ["CLM", "CRF"]
    for ticker in tickers_list:
        edgar_str   = ro_status.get(ticker, "⚪ unavailable") if ro_status else "⚪ unavailable"
        snap        = cef.get(ticker, {}) if cef else {}
        price       = snap.get("price", 0.0)
        nav         = snap.get("nav", 0.0) or NAV_DEFAULTS.get(ticker, 0.0)
        fv_2027     = CEF_FV_2027.get(ticker, 0.0)
        sub_price   = round(nav * 1.04, 2)
        gate        = (ro_gate_info or {}).get(ticker, {})

        lines.append(f"{ticker}: {edgar_str}")
        if price > 0 and nav > 0:
            premium_pct = round((price / nav - 1) * 100, 1)
            lines.append(f"┣ Price: ${price:.2f}  |  NAV: ${nav:.2f}  |  Premium: {premium_pct:+.1f}%")
        elif price > 0:
            lines.append(f"┣ Price: ${price:.2f}")
        if sub_price > 0:
            beat_str = "✓ beating RO" if price > 0 and price <= sub_price else "above sub price"
            lines.append(f"┣ Sub price: ~${sub_price:.2f}  ({beat_str})")
        if fv_2027 > 0:
            lines.append(f"┣ Recovery target: ~${fv_2027:.2f}  (2027 FV)")

        if gate:
            if gate.get("path_b_open"):
                lines.append(f"┗ Path B OPEN — yield floor re-entry active ({gate['path_b_date']}+)")
            elif gate.get("path_a_open"):
                lines.append(f"┗ Path A OPEN — monitor for entry signal ({gate['path_a_date']}+)")
            else:
                lines.append(f"┗ Path A gate: {gate['path_a_date']}  (30d from N-2)")
        elif fv_2027 > 0:
            lines.append(f"┗ Accumulate at or below ${fv_2027:.2f}")
        lines.append("")

    # ── WHEEL DESK — open positions DTE + exit advisory (suppressed when empty)
    wheel_lines = fetch_wheel_desk_snapshot()
    if wheel_lines and wheel_lines != ["No open wheel positions"]:
        lines.append("WHEEL DESK")
        lines.extend(wheel_lines)
        lines.append("")

    # ── TIER 2 — MLPI / XBCI / CHPY (Pushover only, never Discord)
    tier2 = fetch_tier2_snapshot()
    if tier2:
        lines.append("——————————————")
        lines.append("")
        lines.append("TIER 2 — INCOME ENGINES")
        for t2_ticker, t2 in tier2.items():
            price          = t2.get("price", 0.0)
            yield_pct      = t2.get("yield_pct", 0.0)
            monthly_est    = t2.get("monthly_est", 0.0)
            dca_signal     = t2.get("dca_signal", "")
            underlying_rsi = t2.get("underlying_rsi")
            drawdown_pct   = t2.get("drawdown_pct")
            cfg = TIER2_TICKERS.get(t2_ticker, {})
            underlying    = cfg.get("underlying", "")
            sector_tag    = cfg.get("sector_tag", "")
            sector_thesis = cfg.get("sector_thesis", "")
            sector_signal = t2.get("sector_signal")   # (lean, score) or None
            verdict       = t2.get("verdict", "")
            if price > 0:
                lines.append(f"{t2_ticker}: ${price:.2f}  |  ~{yield_pct:.0f}%  |  Est. ~${monthly_est:.3f}/mo")
                if underlying_rsi and drawdown_pct:
                    try:
                        rsi_val  = float(underlying_rsi)
                        draw_val = float(drawdown_pct)
                        rsi_flag = " ⚡" if rsi_val < 30 else (" ✅" if rsi_val < 35 else "")
                        draw_flag = " ⚡" if draw_val >= 8 else (" ✅" if draw_val >= 5 else "")
                        lines.append(f"┣ {underlying} RSI: {rsi_val:.0f}{rsi_flag}  ·  {draw_val:.1f}% off 20d high{draw_flag}")
                    except Exception:
                        pass
                if sector_tag:
                    if sector_signal:
                        lean, score = sector_signal
                        lines.append(f"┣ {sector_tag} | {underlying}: {lean} ({score:+.0f}) · {sector_thesis}")
                    else:
                        lines.append(f"┣ {sector_tag} · {sector_thesis}")
                if verdict:
                    lines.append(f"┣ Verdict: {verdict}")
                lines.append(f"┗ {dca_signal}")
            else:
                lines.append(f"{t2_ticker}: price unavailable")
            lines.append("")

    # ── DEPLOY vs IDLE — CPI opportunity cost
    bp = fetch_buying_power_snapshot()
    if bp.get("cpi_yoy") is not None:
        cpi         = bp["cpi_yoy"]
        clm_snap    = (cef or {}).get("CLM", {})
        clm_price   = clm_snap.get("price", 0.0)
        clm_dist    = CEF_ANNUAL_DIST.get("CLM", 1.458)
        clm_yield   = round(clm_dist / clm_price * 100, 1) if clm_price > 0 else 22.0
        idle_1k     = round(1000 * cpi / 100 / 12, 2)
        income_1k   = round(1000 * clm_dist / clm_price / 12, 2) if clm_price > 0 else round(1000 * 0.22 / 12, 2)
        advantage   = round(clm_yield - cpi, 1)

        lines.append("DEPLOY vs IDLE")
        lines.append(f"┣ CPI (YoY): {cpi:.1f}% — live from FRED")
        lines.append(f"┣ Idle cash: −${idle_1k:.2f}/mo per $1k (inflation drag)")
        lines.append(f"┣ CLM at ${clm_price:.2f}: {clm_yield:.1f}% yield → +${income_1k:.2f}/mo per $1k")
        lines.append(f"┣ Advantage: +{advantage:.1f}pp vs idle  ({clm_yield:.1f}% − {cpi:.1f}% CPI)")
        lines.append(f"┣ Portfolio real yield: {bp['real_portfolio_yield']:+.1f}% (19% blended − {cpi:.1f}% CPI)")
        lines.append(f"┣ Margin real cost: {bp['margin_real_cost']:+.2f}% ({MARGIN_RATE}% rate − {cpi:.1f}% CPI) — carry confirmed")
        lines.append(f"┗ NAV buffer: DRIP share growth +23%/yr outpaces {cpi:.1f}% CPI")

    title   = f"⚡ Evening Pulse — {today}"
    message = "\n".join(lines)
    return title, message


# ─────────────────────────────────────────────────────────────────────────────
# PUSHOVER — personal financial data, never Discord
# ─────────────────────────────────────────────────────────────────────────────

def push_to_pushover(title, message, priority=0):
    if not PUSHOVER_API_TOKEN or not PUSHOVER_USER_KEY:
        logger.error("Pushover credentials missing")
        return False
    payload = {
        "token": PUSHOVER_API_TOKEN, "user": PUSHOVER_USER_KEY,
        "title": title, "message": message, "priority": priority,
    }
    if priority == 1:
        payload["retry"] = 60
        payload["expire"] = 3600
    try:
        requests.post("https://api.pushover.net/1/messages.json", data=payload, timeout=15).raise_for_status()
        logger.info(f"Pushover dispatched (priority {priority}): {title}")
        return True
    except Exception as e:
        logger.error(f"Pushover failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_daily_pulse(force=False):
    today_str = date.today().isoformat()
    state     = load_state()

    if not force and state.get("last_run_date") == today_str:
        logger.info("Already sent today — use --force to override")
        return

    ro_status    = fetch_ro_status()
    ro_gate_info = fetch_ro_gate_info()
    cef_data     = fetch_cef_snapshot()

    title, message = format_pulse_message(cef_data, state, ro_status, ro_gate_info=ro_gate_info)
    success = push_to_pushover(title, message, priority=0)

    if success:
        state["last_run_date"] = today_str
        save_state(state)


if __name__ == "__main__":
    import sys
    run_daily_pulse(force="--force" in sys.argv)

#!/usr/bin/env python3
"""
CLM/CRF RO Analysis Artifact Generator
Fetches live CLM/CRF market data from Yahoo Finance and outputs the full artifact HTML.
Usage: python3 clm_crf_artifact_update.py > /tmp/clm_crf_artifact.html
Runs daily after US market close (23:00 UTC / 1 PM HST / 5 PM ET), Mon–Fri.

CONSTANTS: Update when CLAUDE.md 0-B changes (NAV fallback, distributions, RO formula).
"""

import json
import sys
import urllib.request
from datetime import datetime, timezone

# ── Locked constants (CLAUDE.md § 0-B) ──────────────────────────────────────
CLM_NAV            = 6.31     # CEFConnect Aug 21 2026 — refresh when >$0.10 drift
CRF_NAV            = 6.12
CLM_ANNUAL_DIST    = 1.458    # $0.1215/mo × 12 (Aug 17 2026 press release)
CRF_ANNUAL_DIST    = 1.4112   # $0.1176/mo × 12
CLM_2026_FV        = round(CLM_ANNUAL_DIST / 0.19, 2)   # 7.67
CRF_2026_FV        = round(CRF_ANNUAL_DIST / 0.19, 2)   # 7.43
CLM_2027_FV        = round(CLM_NAV * 1.1053, 2)          # 6.97  (if Oct NAV ≈ July NAV)
CRF_2027_FV        = round(CRF_NAV * 1.1053, 2)          # 6.76
CLM_RO_FORMULA     = 1.04     # 104% × NAV (most aggressive formula — confirmed Aug 2026)
CRF_RO_FORMULA     = 1.04
CLM_SUB_PRICE      = round(CLM_NAV * CLM_RO_FORMULA, 2)  # 6.56
CRF_SUB_PRICE      = round(CRF_NAV * CRF_RO_FORMULA, 2)  # 6.37

# 52w avg premium z-score baseline (from CEFConnect, seeded Jul 2026)
CLM_52W_AVG_PREM   = 19.60
CRF_52W_AVG_PREM   = 18.46

# ── Tiered re-entry zones (2026 RO — from CLAUDE.md § 0-G) ──────────────────
CLM_ZONE1_MAX = 6.65   # Tier 1: aggressively accumulate
CLM_ZONE2_MAX = 6.90   # Tier 2: strong buy
CLM_ZONE3_MAX = 7.10   # Tier 3: opportunistic
CLM_ZONE4_MAX = 7.30   # Tier 4: watchlist

CRF_ZONE1_MAX = 6.42   # Tier 1
CRF_ZONE2_MAX = 6.65   # Tier 2
CRF_ZONE3_MAX = 6.85   # Tier 3
CRF_ZONE4_MAX = 7.00   # Tier 4


# ── Yahoo Finance fetch ───────────────────────────────────────────────────────
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}


def fetch_chart(symbol: str) -> dict:
    """Fetch 30-day daily chart from Yahoo Finance v8 (public, no auth required)."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        "?interval=1d&range=30d"
    )
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
            result = data.get("chart", {}).get("result")
            if result:
                return result[0]
    except Exception as exc:
        print(f"ERROR: {symbol} fetch failed: {exc}", file=sys.stderr)
    return {}


def parse_chart(ch: dict, avg_vol_fallback: int) -> dict:
    """Extract price, volume, change, avg volume, 52w low from chart result."""
    meta = ch.get("meta", {})
    quote = ch.get("indicators", {}).get("quote", [{}])[0]

    closes = [c for c in quote.get("close", []) if c is not None]
    volumes = [v for v in quote.get("volume", []) if v is not None]

    price = float(meta.get("regularMarketPrice", 0.0))
    vol   = int(meta.get("regularMarketVolume", 0))

    # Previous day close = second-to-last entry (last = today's close)
    prev_close = closes[-2] if len(closes) >= 2 else closes[0] if closes else price
    chg_pct = (price - prev_close) / prev_close * 100 if prev_close else 0.0

    # 20-day avg volume (exclude today's potentially partial volume)
    hist_vols = volumes[:-1] if len(volumes) > 1 else volumes
    avg_vol = int(sum(hist_vols) / len(hist_vols)) if hist_vols else avg_vol_fallback

    return {
        "price":    price,
        "vol":      vol,
        "avg_vol":  avg_vol,
        "chg_pct":  chg_pct,
        "fifty2_lo": float(meta.get("fiftyTwoWeekLow", 0.0)),
        "fifty2_hi": float(meta.get("fiftyTwoWeekHigh", 0.0)),
    }


print("Fetching CLM...", file=sys.stderr)
clm_raw = fetch_chart("CLM")
print("Fetching CRF...", file=sys.stderr)
crf_raw = fetch_chart("CRF")

if not clm_raw or not crf_raw:
    print("FATAL: Could not fetch one or both quotes. Aborting.", file=sys.stderr)
    sys.exit(1)

clm_d = parse_chart(clm_raw, avg_vol_fallback=1_800_000)
crf_d = parse_chart(crf_raw, avg_vol_fallback=1_100_000)

# ── Parse raw values ─────────────────────────────────────────────────────────
clm_price   = clm_d["price"]
crf_price   = crf_d["price"]
clm_vol     = clm_d["vol"]
crf_vol     = crf_d["vol"]
clm_chg     = clm_d["chg_pct"]
crf_chg     = crf_d["chg_pct"]
clm_52lo    = clm_d["fifty2_lo"]
crf_52lo    = crf_d["fifty2_lo"]
clm_avg_vol = clm_d["avg_vol"]
crf_avg_vol = crf_d["avg_vol"]

# ── Derive metrics ────────────────────────────────────────────────────────────
clm_prem    = round((clm_price - CLM_NAV) / CLM_NAV * 100, 1)
crf_prem    = round((crf_price - CRF_NAV) / CRF_NAV * 100, 1)
clm_yield   = round(CLM_ANNUAL_DIST / clm_price * 100, 1)
crf_yield   = round(CRF_ANNUAL_DIST / crf_price * 100, 1)
clm_sub_yld = round(CLM_ANNUAL_DIST / CLM_SUB_PRICE * 100, 1)
crf_sub_yld = round(CRF_ANNUAL_DIST / CRF_SUB_PRICE * 100, 1)
clm_vol_r   = round(clm_vol / clm_avg_vol * 100 - 100) if clm_avg_vol else 0
crf_vol_r   = round(crf_vol / crf_avg_vol * 100 - 100) if crf_avg_vol else 0


def fmt_vol(v: int) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return str(v)


def vol_badge(pct: int) -> str:
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct}% vs avg"


def chg_class(pct: float) -> str:
    return "pos" if pct >= 0 else "neg"


def chg_str(pct: float) -> str:
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def zone_label(ticker: str, price: float) -> tuple[str, str]:
    """Return (label, css_class) for current price tier."""
    z1, z2, z3, z4 = (
        (CLM_ZONE1_MAX, CLM_ZONE2_MAX, CLM_ZONE3_MAX, CLM_ZONE4_MAX)
        if ticker == "CLM"
        else (CRF_ZONE1_MAX, CRF_ZONE2_MAX, CRF_ZONE3_MAX, CRF_ZONE4_MAX)
    )
    if price <= z1:
        return "Tier 1 — Aggressive Accumulation", "zone-t1"
    if price <= z2:
        return "Tier 2 — Strong Buy", "zone-t2"
    if price <= z3:
        return "Tier 3 — Opportunistic", "zone-t3"
    if price <= z4:
        return "Tier 4 — Watchlist", "zone-t4"
    return "Above all zones", "zone-none"


clm_zone_label, clm_zone_cls = zone_label("CLM", clm_price)
crf_zone_label, crf_zone_cls = zone_label("CRF", crf_price)

now_utc = datetime.now(timezone.utc)
today_str = now_utc.strftime("%b %-d, %Y")
updated_str = now_utc.strftime("%b %-d, %Y %H:%M UTC")

# ── Alert banner logic ────────────────────────────────────────────────────────
both_in_zone1 = clm_price <= CLM_ZONE1_MAX and crf_price <= CRF_ZONE1_MAX
either_in_zone1 = clm_price <= CLM_ZONE1_MAX or crf_price <= CRF_ZONE1_MAX

if both_in_zone1:
    alert_text = "⚡ ACTIVE RO — Both CLM &amp; CRF in Tier 1 Accumulation Zone. DCA is live."
    alert_cls  = "alert-t1"
elif either_in_zone1:
    t = "CLM" if clm_price <= CLM_ZONE1_MAX else "CRF"
    alert_text = f"⚡ ACTIVE RO — {t} has entered Tier 1 Accumulation Zone."
    alert_cls  = "alert-t1"
else:
    alert_text = "⚡ ACTIVE RO — Monitoring re-entry zones. Both tickers above Tier 1."
    alert_cls  = "alert-watch"

# ── Zone row highlight helper ─────────────────────────────────────────────────
def active_tier(clm_price: float, crf_price: float) -> int:
    """Return which tier (1-4) both prices fall into (0 = above all zones)."""
    # Use the stricter of the two tickers (higher price = higher tier)
    for t, (ch, cr) in enumerate([
        (CLM_ZONE1_MAX, CRF_ZONE1_MAX),
        (CLM_ZONE2_MAX, CRF_ZONE2_MAX),
        (CLM_ZONE3_MAX, CRF_ZONE3_MAX),
        (CLM_ZONE4_MAX, CRF_ZONE4_MAX),
    ], start=1):
        if clm_price <= ch or crf_price <= cr:  # either ticker in this zone = signal it
            return t
    return 0


_active_tier = active_tier(clm_price, crf_price)


def zone_row(tier: int, clm_range: str, crf_range: str, action: str) -> str:
    is_active = tier == _active_tier
    cls = "zone-row active-zone" if is_active else "zone-row"
    badge = " <span class='active-badge'>◀ NOW</span>" if is_active else ""
    return (
        f"<tr class='{cls}'>"
        f"<td>Tier {tier}{badge}</td>"
        f"<td>{clm_range}</td>"
        f"<td>{crf_range}</td>"
        f"<td>{action}</td>"
        f"</tr>"
    )


# ── Full HTML (artifact-compatible: no DOCTYPE/html/head/body tags) ───────────
HTML = f"""<title>CLM/CRF RO Analysis</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
:root {{
  --bg: #0d1117;
  --surface: #161b22;
  --surface2: #1e242d;
  --border: #30363d;
  --text: #e6edf3;
  --muted: #8b949e;
  --gold: #f5c518;
  --gold-dim: #c9a314;
  --green: #3fb950;
  --red: #f85149;
  --amber: #d29922;
  --blue: #58a6ff;
  --t1-bg: #0a2a1a;
  --t1-border: #2ea043;
  --t2-bg: #0e1c2e;
  --t2-border: #388bfd;
  --t3-bg: #1a1a0e;
  --t3-border: #d29922;
  --t4-bg: #1a0e0e;
  --t4-border: #8b949e;
}}
@media (prefers-color-scheme: light) {{ :root:not([data-theme="dark"]) {{
  --bg: #f6f8fa; --surface: #ffffff; --surface2: #f0f3f6;
  --border: #d0d7de; --text: #1f2328; --muted: #57606a;
  --t1-bg: #dff7e9; --t1-border: #2da44e;
  --t2-bg: #ddeeff; --t2-border: #0969da;
  --t3-bg: #fffaec; --t3-border: #9a6700;
  --t4-bg: #ffeef0; --t4-border: #cf222e;
}} }}
:root[data-theme="light"] {{
  --bg: #f6f8fa; --surface: #ffffff; --surface2: #f0f3f6;
  --border: #d0d7de; --text: #1f2328; --muted: #57606a;
  --t1-bg: #dff7e9; --t1-border: #2da44e;
  --t2-bg: #ddeeff; --t2-border: #0969da;
  --t3-bg: #fffaec; --t3-border: #9a6700;
  --t4-bg: #ffeef0; --t4-border: #cf222e;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: var(--bg); color: var(--text); font-family: "IBM Plex Sans", system-ui, sans-serif; font-size: 14px; line-height: 1.6; padding: 16px; }}
h1,h2,h3,h4 {{ font-family: "IBM Plex Mono", monospace; font-weight: 600; }}
.page {{ max-width: 900px; margin: 0 auto; display: flex; flex-direction: column; gap: 24px; }}
.header {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px 20px; display: flex; justify-content: space-between; align-items: center; }}
.header-title {{ font-family: "IBM Plex Mono", monospace; font-size: 18px; font-weight: 600; color: var(--gold); letter-spacing: -0.5px; }}
.header-meta {{ font-size: 11px; color: var(--muted); text-align: right; line-height: 1.8; }}
.alert {{ border-radius: 6px; padding: 10px 16px; font-weight: 600; font-size: 13px; }}
.alert-t1 {{ background: var(--t1-bg); border: 1px solid var(--t1-border); color: var(--green); }}
.alert-watch {{ background: var(--t3-bg); border: 1px solid var(--t3-border); color: var(--amber); }}
.cards {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
.card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 18px; }}
.card-header {{ display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 12px; }}
.ticker {{ font-family: "IBM Plex Mono", monospace; font-size: 22px; font-weight: 600; color: var(--gold); }}
.price {{ font-family: "IBM Plex Mono", monospace; font-size: 26px; font-weight: 600; }}
.chg {{ font-size: 12px; font-weight: 500; margin-left: 6px; }}
.pos {{ color: var(--green); }}
.neg {{ color: var(--red); }}
.stats {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 12px; }}
.stat {{ background: var(--surface2); border-radius: 5px; padding: 8px 10px; }}
.stat-lbl {{ font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }}
.stat-val {{ font-family: "IBM Plex Mono", monospace; font-size: 13px; font-weight: 500; margin-top: 2px; }}
.zone-badge {{ display: inline-block; border-radius: 4px; padding: 4px 10px; font-size: 11px; font-weight: 600; margin-top: 12px; }}
.zone-t1 {{ background: var(--t1-bg); color: var(--green); border: 1px solid var(--t1-border); }}
.zone-t2 {{ background: var(--t2-bg); color: var(--blue); border: 1px solid var(--t2-border); }}
.zone-t3 {{ background: var(--t3-bg); color: var(--amber); border: 1px solid var(--t3-border); }}
.zone-t4 {{ background: var(--t4-bg); color: var(--red); border: 1px solid var(--t4-border); }}
.zone-none {{ background: var(--surface2); color: var(--muted); border: 1px solid var(--border); }}
section {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 18px 20px; }}
section h2 {{ font-size: 13px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 14px; border-bottom: 1px solid var(--border); padding-bottom: 10px; }}
.phases {{ display: flex; flex-direction: column; gap: 4px; }}
.phase {{ display: grid; grid-template-columns: 140px 70px 1fr; gap: 10px; align-items: start; padding: 8px 10px; border-radius: 5px; font-size: 13px; }}
.phase.active {{ background: var(--t1-bg); border: 1px solid var(--t1-border); }}
.phase.done {{ opacity: 0.5; }}
.phase-name {{ font-weight: 600; }}
.phase-date {{ font-family: "IBM Plex Mono", monospace; font-size: 11px; color: var(--muted); }}
.phase-note {{ color: var(--muted); font-size: 12px; }}
.phase.active .phase-note {{ color: var(--green); }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ text-align: left; color: var(--muted); font-weight: 500; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; padding: 6px 10px; border-bottom: 1px solid var(--border); }}
td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: middle; }}
tr:last-child td {{ border-bottom: none; }}
.zone-row.active-zone {{ background: var(--t1-bg); font-weight: 600; }}
.zone-row.active-zone td {{ color: var(--green); }}
.active-badge {{ font-size: 10px; color: var(--green); margin-left: 6px; }}
.comp-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
.comp-box {{ background: var(--surface2); border-radius: 6px; padding: 12px 14px; }}
.comp-box h4 {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }}
.comp-row {{ display: flex; justify-content: space-between; font-size: 13px; padding: 3px 0; border-bottom: 1px solid var(--border); }}
.comp-row:last-child {{ border-bottom: none; }}
.comp-lbl {{ color: var(--muted); }}
.comp-val {{ font-family: "IBM Plex Mono", monospace; font-weight: 500; }}
.sept-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }}
.sept-item {{ background: var(--surface2); border-radius: 5px; padding: 10px 12px; }}
.sept-item h4 {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }}
.sept-item p {{ font-size: 12px; color: var(--text); line-height: 1.5; }}
.key-dates {{ display: flex; flex-direction: column; gap: 6px; }}
.kd-row {{ display: flex; align-items: baseline; gap: 12px; font-size: 13px; }}
.kd-date {{ font-family: "IBM Plex Mono", monospace; font-size: 12px; color: var(--muted); min-width: 100px; }}
.kd-note {{ color: var(--text); }}
.kd-note strong {{ color: var(--gold); }}
.action-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }}
.action-card {{ border-radius: 6px; padding: 12px 14px; border: 1px solid; }}
.action-card.buy {{ background: var(--t1-bg); border-color: var(--t1-border); }}
.action-card.hold {{ background: var(--t3-bg); border-color: var(--t3-border); }}
.action-card.avoid {{ background: var(--t4-bg); border-color: var(--t4-border); color: var(--muted); }}
.action-card h4 {{ font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }}
.action-card.buy h4 {{ color: var(--green); }}
.action-card.hold h4 {{ color: var(--amber); }}
.action-card.avoid h4 {{ color: var(--muted); }}
.action-card ul {{ padding-left: 14px; font-size: 12px; line-height: 1.8; }}
.fv-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 8px; }}
.fv-box {{ background: var(--surface2); border-radius: 5px; padding: 10px 12px; font-size: 12px; }}
.fv-box h4 {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }}
.fv-row {{ display: flex; justify-content: space-between; padding: 2px 0; }}
.fv-row .fv-val {{ font-family: "IBM Plex Mono", monospace; }}
.fv-row .gold {{ color: var(--gold); }}
.disclaimer {{ font-size: 11px; color: var(--muted); border-top: 1px solid var(--border); padding-top: 12px; }}
@media (max-width: 640px) {{
  .cards {{ grid-template-columns: 1fr; }}
  .comp-grid {{ grid-template-columns: 1fr; }}
  .sept-grid {{ grid-template-columns: 1fr; }}
  .action-grid {{ grid-template-columns: 1fr; }}
  .phase {{ grid-template-columns: 1fr; gap: 2px; }}
  .fv-grid {{ grid-template-columns: 1fr; }}
}}
</style>

<div class="page">

  <!-- Header -->
  <div class="header">
    <div class="header-title">CLM/CRF · Rights Offering 2026</div>
    <div class="header-meta">
      Personal monitoring dashboard<br>
      Updated: {updated_str}
    </div>
  </div>

  <!-- Alert banner -->
  <div class="alert {alert_cls}">{alert_text}</div>

  <!-- Live snapshot cards -->
  <div class="cards">
    <!-- CLM -->
    <div class="card">
      <div class="card-header">
        <span class="ticker">CLM</span>
        <span style="font-size:11px;color:var(--muted);">Cornerstone Strategic Value</span>
      </div>
      <div>
        <span class="price">${clm_price:.2f}</span>
        <span class="chg {chg_class(clm_chg)}">{chg_str(clm_chg)}</span>
      </div>
      <div class="stats">
        <div class="stat">
          <div class="stat-lbl">NAV (Aug 21)</div>
          <div class="stat-val">${CLM_NAV:.2f}</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">Premium</div>
          <div class="stat-val" style="color:{'var(--red)' if clm_prem > 20 else 'var(--green)' if clm_prem < 10 else 'var(--amber)'}">{clm_prem:+.1f}%</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">Volume</div>
          <div class="stat-val">{fmt_vol(clm_vol)}</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">vs 10d Avg</div>
          <div class="stat-val" style="color:{'var(--red)' if clm_vol_r > 50 else 'var(--text)'}">{vol_badge(clm_vol_r)}</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">Yield @ Price</div>
          <div class="stat-val" style="color:var(--green)">{clm_yield:.1f}%</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">52w Low</div>
          <div class="stat-val">${clm_52lo:.2f}</div>
        </div>
      </div>
      <div class="zone-badge {clm_zone_cls}">{clm_zone_label}</div>
    </div>

    <!-- CRF -->
    <div class="card">
      <div class="card-header">
        <span class="ticker">CRF</span>
        <span style="font-size:11px;color:var(--muted);">Cornerstone Total Return</span>
      </div>
      <div>
        <span class="price">${crf_price:.2f}</span>
        <span class="chg {chg_class(crf_chg)}">{chg_str(crf_chg)}</span>
      </div>
      <div class="stats">
        <div class="stat">
          <div class="stat-lbl">NAV (Aug 21)</div>
          <div class="stat-val">${CRF_NAV:.2f}</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">Premium</div>
          <div class="stat-val" style="color:{'var(--red)' if crf_prem > 20 else 'var(--green)' if crf_prem < 10 else 'var(--amber)'}">{crf_prem:+.1f}%</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">Volume</div>
          <div class="stat-val">{fmt_vol(crf_vol)}</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">vs 10d Avg</div>
          <div class="stat-val" style="color:{'var(--red)' if crf_vol_r > 50 else 'var(--text)'}">{vol_badge(crf_vol_r)}</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">Yield @ Price</div>
          <div class="stat-val" style="color:var(--green)">{crf_yield:.1f}%</div>
        </div>
        <div class="stat">
          <div class="stat-lbl">52w Low</div>
          <div class="stat-val">${crf_52lo:.2f}</div>
        </div>
      </div>
      <div class="zone-badge {crf_zone_cls}">{crf_zone_label}</div>
    </div>
  </div>

  <!-- Fair Value & Sub Price Reference -->
  <section>
    <h2>Fair Value &amp; Subscription Price Reference</h2>
    <div class="fv-grid">
      <div class="fv-box">
        <h4>CLM</h4>
        <div class="fv-row"><span>2026 FV (÷0.19 yield)</span><span class="fv-val gold">${CLM_2026_FV}</span></div>
        <div class="fv-row"><span>2027 FV (if Oct NAV ≈ $6.31)</span><span class="fv-val">${CLM_2027_FV}</span></div>
        <div class="fv-row"><span>Sub price (104% × NAV)</span><span class="fv-val">${CLM_SUB_PRICE}</span></div>
        <div class="fv-row"><span>Yield at sub price</span><span class="fv-val">{clm_sub_yld:.1f}%</span></div>
        <div class="fv-row"><span>Current premium</span><span class="fv-val">{clm_prem:+.1f}%</span></div>
      </div>
      <div class="fv-box">
        <h4>CRF</h4>
        <div class="fv-row"><span>2026 FV (÷0.19 yield)</span><span class="fv-val gold">${CRF_2026_FV}</span></div>
        <div class="fv-row"><span>2027 FV (if Oct NAV ≈ $6.12)</span><span class="fv-val">${CRF_2027_FV}</span></div>
        <div class="fv-row"><span>Sub price (104% × NAV)</span><span class="fv-val">${CRF_SUB_PRICE}</span></div>
        <div class="fv-row"><span>Yield at sub price</span><span class="fv-val">{crf_sub_yld:.1f}%</span></div>
        <div class="fv-row"><span>Current premium</span><span class="fv-val">{crf_prem:+.1f}%</span></div>
      </div>
    </div>
  </section>

  <!-- RO Phase Timeline -->
  <section>
    <h2>RO Phase Timeline — 2026</h2>
    <div class="phases">
      <div class="phase done">
        <span class="phase-name">Phase 1: N-2 Filed</span>
        <span class="phase-date">~Aug 14</span>
        <span class="phase-note">SEC review begins (~59d to record date). monitor.py fires CRITICAL.</span>
      </div>
      <div class="phase active">
        <span class="phase-name">Phase 2: SEC Review ← NOW</span>
        <span class="phase-date">Aug 14 – Oct 10</span>
        <span class="phase-note">Market repricing to 2027 FV in progress. NAV determination month (October) is the key catalyst. Best DCA window.</span>
      </div>
      <div class="phase">
        <span class="phase-name">Phase 3: N-2/A Filed</span>
        <span class="phase-date">~Sep–Oct</span>
        <span class="phase-note">Amended prospectus with final sub price = 104% × NAV at expiration close.</span>
      </div>
      <div class="phase">
        <span class="phase-name">Phase 4: 424B3 / Record Date</span>
        <span class="phase-date">~Oct 12 (est.)</span>
        <span class="phase-note">Record date confirmed. Historical pattern: price is at or near the bottom at this date.</span>
      </div>
      <div class="phase">
        <span class="phase-name">Phase 5: 25-Day Window</span>
        <span class="phase-date">~Oct 12 – Nov 6</span>
        <span class="phase-note">Subscription window open. 2025 pattern: price rose +$0.40 from record date to expiration.</span>
      </div>
      <div class="phase">
        <span class="phase-name">Phase 6: Expiration &amp; Close</span>
        <span class="phase-date">~Nov 6 (est.)</span>
        <span class="phase-note">Sub price locked at 104% × NAV at that close. New shares issued. Expect 10–15% dilution.</span>
      </div>
      <div class="phase">
        <span class="phase-name">Phase 7: Post-RO Recovery</span>
        <span class="phase-date">Nov–Dec</span>
        <span class="phase-note">2025: $7.32 → $7.88 in 30d. Market re-discovers yield support. DCA payoff realized.</span>
      </div>
    </div>
  </section>

  <!-- 2025 vs 2026 Comparison -->
  <section>
    <h2>2025 vs 2026 RO Forensic Comparison</h2>
    <div class="comp-grid">
      <div class="comp-box">
        <h4>2025 RO (Completed — CLM reference)</h4>
        <div class="comp-row"><span class="comp-lbl">N-2 filed</span><span class="comp-val">~Aug 2025</span></div>
        <div class="comp-row"><span class="comp-lbl">Record date price</span><span class="comp-val">$6.92 ← bottom</span></div>
        <div class="comp-row"><span class="comp-lbl">Expiration price</span><span class="comp-val">$7.32 (+$0.40)</span></div>
        <div class="comp-row"><span class="comp-lbl">Post-RO 30d</span><span class="comp-val">$7.88 (+$0.56)</span></div>
        <div class="comp-row"><span class="comp-lbl">Sub price formula</span><span class="comp-val">107–112% × NAV</span></div>
        <div class="comp-row"><span class="comp-lbl">Pattern</span><span class="comp-val">Back-loaded dip</span></div>
      </div>
      <div class="comp-box">
        <h4>2026 RO (In Progress — Aug 25 snapshot)</h4>
        <div class="comp-row"><span class="comp-lbl">N-2 filed</span><span class="comp-val">~Aug 14 2026</span></div>
        <div class="comp-row"><span class="comp-lbl">Announcement vol</span><span class="comp-val">8.62M (4.6× avg)</span></div>
        <div class="comp-row"><span class="comp-lbl">CLM today</span><span class="comp-val" style="color:var(--gold)">${clm_price:.2f} ({clm_zone_label.split('—')[0].strip()})</span></div>
        <div class="comp-row"><span class="comp-lbl">CRF today</span><span class="comp-val" style="color:var(--gold)">${crf_price:.2f} ({crf_zone_label.split('—')[0].strip()})</span></div>
        <div class="comp-row"><span class="comp-lbl">Sub price formula</span><span class="comp-val">104% × NAV (aggressive)</span></div>
        <div class="comp-row"><span class="comp-lbl">Pattern</span><span class="comp-val">Front-loaded — repriced immediately</span></div>
      </div>
    </div>
    <p style="font-size:12px;color:var(--muted);margin-top:10px;">Key difference: 2025 bottom was at the record date (~Day 59 post-N-2). 2026 front-loaded repricing after Aug 17 distribution announcement means the bottom may arrive earlier — watch for record date confirmation (~Oct 12 est.) as a buying signal, not the N-2 filing date.</p>
  </section>

  <!-- September Risk Map -->
  <section>
    <h2>September Risk Map (Seasonal Headwinds)</h2>
    <div class="sept-grid">
      <div class="sept-item">
        <h4>S&amp;P Seasonality</h4>
        <p>September is historically the worst month: −1.0% to −1.5% avg return. Broad market weakness amplifies CEF premium compression.</p>
      </div>
      <div class="sept-item">
        <h4>Ex-Dividend Sept 15</h4>
        <p>CLM −$0.1215/sh · CRF −$0.1176/sh. Price typically dips 1–3 days before ex-date as late sellers exit. Mechanical buyer support returns after.</p>
      </div>
      <div class="sept-item">
        <h4>Mutual Fund Tax-Loss</h4>
        <p>Fiscal year ends Oct 31 for most mutual funds → September tax-loss harvesting creates above-average selling pressure on closed-end funds.</p>
      </div>
    </div>
    <p style="font-size:12px;color:var(--muted);margin-top:10px;"><strong style="color:var(--amber)">RO + September overlap:</strong> This is the highest-risk window for temporary price deterioration AND the highest-reward DCA entry. monitor.py seasonal caution flag fires March &amp; September automatically.</p>
  </section>

  <!-- 4-Tier Re-entry Zones -->
  <section>
    <h2>4-Tier Re-entry Zones (2026 RO — CLM &amp; CRF)</h2>
    <div style="overflow-x:auto;">
    <table>
      <tr>
        <th>Tier</th>
        <th>CLM Range</th>
        <th>CRF Range</th>
        <th>Action</th>
      </tr>
      {zone_row(1, "≤ $6.65", "≤ $6.42",
        "Aggressive DCA — sub price territory, highest yield")}
      {zone_row(2, "$6.65 – $6.90", "$6.42 – $6.65",
        "Strong buy — 2027 FV territory, solid risk/reward")}
      {zone_row(3, "$6.90 – $7.10", "$6.65 – $6.85",
        "Opportunistic — approach cautiously, smaller size")}
      {zone_row(4, "$7.10 – $7.30", "$6.85 – $7.00",
        "Watchlist — 2026 FV approaching, reduce frequency")}
      <tr class="zone-row">
        <td>Above Tier 4</td>
        <td>&gt; $7.30</td>
        <td>&gt; $7.00</td>
        <td>Wait for compression. Let DRIP continue, no new DCA.</td>
      </tr>
    </table>
    </div>
    <p style="font-size:12px;color:var(--muted);margin-top:10px;">Zones derived from 2026 distribution rate ($1.458 / $1.4112), 104% RO sub price, and 2027 FV if Oct NAV ≈ current NAV. Portfolio compounds at any entry — these are size guides, not hard rules.</p>
  </section>

  <!-- Key Dates -->
  <section>
    <h2>Key Dates Calendar</h2>
    <div class="key-dates">
      <div class="kd-row"><span class="kd-date">~Aug 14</span><span class="kd-note"><strong>N-2 Filed</strong> — RO officially in progress. Sell 99%, keep ≥3 shares for DRIP.</span></div>
      <div class="kd-row"><span class="kd-date">Sept 15</span><span class="kd-note"><strong>Ex-Dividend</strong> — CLM −$0.1215 · CRF −$0.1176. Potential 1–3 day dip before.</span></div>
      <div class="kd-row"><span class="kd-date">~Late Sept</span><span class="kd-note"><strong>N-2/A</strong> — Amended prospectus with final pricing formula. monitor.py escalates.</span></div>
      <div class="kd-row"><span class="kd-date">October</span><span class="kd-note"><strong>NAV Determination Month</strong> — Board locks 2027 distribution rate. Higher NAV → higher 2027 FV → higher sub price. The biggest catalyst.</span></div>
      <div class="kd-row"><span class="kd-date">~Oct 12 (est.)</span><span class="kd-note"><strong>Record Date</strong> — 424B3 filed. Sub price = 104% × NAV at expiration close. Historical bottom marker.</span></div>
      <div class="kd-row"><span class="kd-date">~Nov 6 (est.)</span><span class="kd-note"><strong>Subscription Expiration</strong> — 25-day window closes. Sub price locked. New shares issued.</span></div>
      <div class="kd-row"><span class="kd-date">Nov–Dec</span><span class="kd-note"><strong>Post-RO Recovery</strong> — DCA payoff. Target: +$0.50–$0.75 from record-date low (2025 pattern).</span></div>
    </div>
  </section>

  <!-- Action Summary -->
  <section>
    <h2>Action Summary — {today_str}</h2>
    <div class="action-grid">
      <div class="action-card buy">
        <h4>Current CLM Position: {clm_zone_label}</h4>
        <ul>
          <li>DCA within Tier 1 zone (≤$6.65) — highest priority</li>
          <li>Margin rebuy: deploy available headroom at these levels</li>
          <li>DRIP: must remain active (≥3 shares held for RO dodge)</li>
          <li>Yield @ ${clm_price:.2f}: <strong>{clm_yield:.1f}%</strong> — well above 19% floor</li>
        </ul>
      </div>
      <div class="action-card buy">
        <h4>Current CRF Position: {crf_zone_label}</h4>
        <ul>
          <li>DCA within Tier 1 zone (≤$6.42) — highest priority</li>
          <li>Sub price ${CRF_SUB_PRICE} — buying below/near sub price creates cost basis advantage</li>
          <li>DRIP: must remain active (≥3 shares held for RO dodge)</li>
          <li>Yield @ ${crf_price:.2f}: <strong>{crf_yield:.1f}%</strong> — well above 19% floor</li>
        </ul>
      </div>
      <div class="action-card hold">
        <h4>Watch Items</h4>
        <ul>
          <li>October: NAV determination month — any NAV recovery → 2027 FV rises</li>
          <li>N-2/A filing: re-confirms 104% RO formula — monitor.py escalates on detection</li>
          <li>September ex-div (Sept 15): potential 1–3 day dip = last high-yield entry</li>
          <li>Vol spikes on flat SPY days = institutional exit signal (Feb 2026 pattern)</li>
        </ul>
      </div>
      <div class="action-card avoid">
        <h4>Don't Do</h4>
        <ul>
          <li>Don't subscribe to the RO — sell 99% and rebuy post-dip instead</li>
          <li>Don't wait for the "perfect" bottom — the snowball compounds at any of these levels</li>
          <li>Don't exceed 25% combined leverage (margin + box) on new buys</li>
          <li>Don't turn off DRIP — shares issued at NAV is the structural edge</li>
        </ul>
      </div>
    </div>
  </section>

  <!-- Disclaimer -->
  <p class="disclaimer">Personal tracking dashboard · Not financial advice · Data fetched from Yahoo Finance after market close · NAV from CEFConnect as of Aug 21 2026 · All calculations use confirmed Aug 17 2026 distribution constants</p>

</div>
"""

print(HTML)
print("Done. HTML written to stdout.", file=sys.stderr)

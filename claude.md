# Cashflow ZZZ Machine — Project Context
*Master brief for Claude Code sessions. Update as ecosystem evolves.*

---

## 1. Business Philosophy (Paycheck2Portfolio / Shawn Grady Model)

```
W2 Paycheck      →  E*TRADE (business operating account)
Bills            →  Paid via E*TRADE Bill Pay (business expenses)
Margin Loan      →  Operating line of credit (like a business LOC / HELOC)
Dividends        →  Revenue (rent from asset properties)
CLM/CRF DRIP     →  Retained earnings reinvested into the business
Options          →  Hedging desk + premium income division
Discord Bots     →  Automated business intelligence layer
```

**Core analogy:**
- Stocks = properties
- Dividends = rent
- Margin loan = mortgage / VA loan
- Portfolio = equity
- Managing from a position of equity

**Velocity Banking mechanic:**
```
$6k W2 deposit → E*TRADE
  ↓
$2k → bills via E*TRADE Bill Pay
$4k net → margin paydown + dividend accumulation
  ↓
Monthly divs (MLPI, MAIN, TDAQ, KQQQ) → margin paydown
CLM/CRF divs → DRIP at NAV only, never touched
  ↓
Margin freed → reborrow → buy more CLM/CRF or Tier 2
```

**Risk guardrails:**
- Margin never exceeds 25% of portfolio value
- Internal red line: if portfolio drops 15% → stop new margin draws
- Keep 1 month of bills (~$2k) in cash buffer at all times

---

## 2. Portfolio Architecture

### Tier 1 — Core Compounder (NEVER interrupted)
| Ticker | Role | Action |
|--------|------|--------|
| CLM | Closed-end fund | DRIP at NAV, dodge Rights Offerings, dip rebuy |
| CRF | Closed-end fund | DRIP at NAV, dodge Rights Offerings, dip rebuy |

- **Yield:** ~19–21% annualized (managed distribution policy at 21% of NAV)
- **DRIP at NAV:** shares issued below market price = built-in alpha
- **Rights Offering dodge:** Sell 99% on N-2 detection → buy back post-offering dip → net more shares than participants
- **Timed DCA months:** March and September (seasonal weakness = accumulation zones)
- **Annual distributions:** CLM $0.1215/share | CRF $0.1176/share (2026 reset)

### Tier 2 — Margin Accelerators (cash dividends only, NO DRIP)
| Ticker | Type | Yield | Frequency | Role |
|--------|------|-------|-----------|------|
| MAIN | BDC | ~8% | Monthly | Stability anchor — never cut dividend since 2007 IPO |
| MLPI | MLP/Energy ETF w/ covered calls | ~15% | Monthly | Real asset base, no K-1 form |
| TDAQ | TappAlpha 0DTE NASDAQ covered call | ~12–17% | Monthly | Higher yield than JEPQ |
| KQQQ | Kurv Tech Titans covered call | ~15% | Monthly | AAPL/MSFT/NVDA/META/GOOGL basket |

**Blended Tier 2 yield:** ~13–15%
**All Tier 2 dividends → margin paydown (never reinvested)**

### Tier 3 — Opportunistic (cycle-dependent, small allocation)
| Ticker | Underlying | Use Case |
|--------|-----------|----------|
| BITA | Bitcoin (BlackRock covered call) | Crypto bull cycle income |
| YBTC | Bitcoin (Roundhill covered call) | Weekly crypto income |
| CHPY | Semiconductor basket | AI momentum phases only |

**Tier 3 rule:** Extract cash weekly → margin paydown. Exit when crypto/AI cycle peaks.

### Deprecated / Removed from Active Scope
- GOOW, NVII — too volatile, NAV decay risk too high for margin paydown role
- TSYX — launched Jan 2026, tiny AUM, 1.3x leverage, 3% yield (skip)
- Forex channel — no correlation to end goal (discontinued)
- TSP channel — no correlation to end goal (discontinued)

---

## 3. Options Strategy

### Wheel (0.20 delta, 30–45 DTE)
**Underlyings:** TDAQ, MLPI, MAIN, KQQQ
```
STEP 1 — Entry filter: IV Rank > 30, bid/ask < $0.10, no earnings in window
STEP 2 — Sell CSP: 0.20 delta, 30–45 DTE, premium ≥ 1% of strike
STEP 3 — Manage: Close at 50% profit | Roll at 21 DTE if untested
          If breached: roll down+out for credit, or take assignment → sell CC
STEP 4 — CC after assignment: ATM/slight OTM, 21–30 DTE
STEP 5 — Capital rule: max 30% of available margin in wheel at any time
```
**Premium income → margin paydown bucket**

### TQQQ Sniper (BTO Calls + Puts)
**Calls (directional, bullish):**
- QQQ above 21 EMA + VIX < 20
- Strike: 10–15% OTM | DTE: 90–180 days
- Size: max 5% of portfolio | Scale: 50% entry → add 50% on confirmation
- Exit: 100% gain OR 14 DTE | Stop: 40% loss → close

**Puts (insurance, always active):**
- Always 1 active put open — 30 DTE, rinse & repeat (homeowners insurance model)
- Strike: 10% OTM | Roll at 14 DTE
- Budget: ≤ 0.5% of portfolio/month
- If VIX > 30 → close puts at profit → rotate into TQQQ calls (fear peak = call entry)

**Seasonal rules (March & September):**
- Calls: reduce size 50%, wait for 3 consecutive green days before entering
- Puts: increase size 50%

---

## 4. Discord Server Architecture

### Channel Map
| Channel | Webhook .env key | Script | Purpose |
|---------|-----------------|--------|---------|
| #announcements | WEBHOOK_ANNOUNCEMENTS | announcements.py | Free tier scorecard/bait — conversion engine |
| #cornerstone | WEBHOOK_CORNERSTONE_RO | monitor.py | CLM/CRF protection engine |
| #market-analysis | WEBHOOK_MARKET_ANALYSIS | market_analysis.py | 0800 HST premarket command center |
| #futures-trading | WEBHOOK_FUTURES_TRADING | cross_asset.py | Futures board, ES/NQ market profile deep-dive, Initial Balance breakout scanner |
| #crypto | WEBHOOK_CRYPTO | crypto.py | BTC/ETH spot, Fear & Greed, on-chain |
| #options-wheel | WEBHOOK_TRADE_SIGNALS | options.py | Wheel strategy + TQQQ sniper signals |
| #dividend-ccetfs | WEBHOOK_DIVIDEND_CCETFS | scheduler.py (`--mode income`) | CC ETF/dividend pulse, wheel v2, ex-div radar, new-ETF discovery |
| #options-wheel | WEBHOOK_TRADE_SIGNALS | scheduler.py (`--mode wheel_signals`) | Tier 2 IV Rank screener + open wheel position DTE countdown |
| #fed | WEBHOOK_FED | fed.py | Fed rate/macro signals |

### Cross-Channel Data Flow (Unity Map)
```
#cornerstone  ──RO Alert──────────────────► #market-analysis (action item)
              ──Dip watch countdown──────► #tqqq-sniper (call entry signal)

#crypto       ──Fear & Greed < 25─────── ► #market-analysis (risk-on signal)
              ──Extreme Fear──────────── ► #tqqq-sniper (TQQQ call cross-signal)

#options-wheel──Premium collected──────► #market-analysis (cashflow log)

#tqqq-sniper  ──Put profit realized────► rotate to TQQQ calls (same channel SOP)

#futures      ──/NQ overnight > +0.5%──► #market-analysis (bullish bias)
              ──/NQ overnight < -1%────► TQQQ put check reminder

#market-analysis ← synthesizes ALL feeds → ACTION ITEMS (single source of truth)
```

### 3-Notification Rule
- Max 3 alerts per sector per 24h rolling window
- Minor changes: noted in DB, NOT broadcast (prevent notification fatigue)
- Next MAJOR change re-opens the broadcast window
- Implemented via: `get_alert_count()` / `can_broadcast()` / `increment_alert_count()`

### Discord Output Format (mobile-first)
```
**Title**
┣ Data 1:
┣ Data 2:
┣ Data 3:
┗ Final:
```

### #announcements Scorecard Format (free tier bait)
```
📊 WEEKLY ACCURACY SCORECARD — Week of [DATE]
Signal          | Predicted | Actual  | Score
/NQ direction   | Bullish   | +1.8%   | ✅
CLM premium     | Accum.    | +3.2%   | ✅
TQQQ put trigger| Renew     | VIX spk | ✅
BTC direction   | Neutral   | -2.1%   | ✅
WEEK ACCURACY: 4/4 — 100% 🎯  |  MTD: 87%
```
**Locked content (subscriber only):** full morning report, whale/dark pool alerts,
TQQQ entries/exits, full cashflow tracker, wheel tickers and strikes.

---

## 5. monitor.py — Current State

**File:** `monitor.py`
**Status:** Updated, syntax verified, deployed to PythonAnywhere via git
**Runs:** PythonAnywhere always-on task | 5-min loop tick | 0800 HST daily pulse

### All Functions (✅ = original preserved | 🆕 = added this session)
```
✅ check_sec_edgar()               — N-2 + SC 13D/G EDGAR watcher
                                     CIKs: CLM=0000814083 | CRF=0000033934
✅ fetch_live_metrics()            — Twelve Data price / RSI / NAV
✅ detect_whale_flow_direction()   — direction-aware (accum. vs distribution)
✅ check_crisis_amplification_risk()— VIXY z-score overlay (threshold: 1.5σ)
✅ calculate_ro_risk_score()       — composite 0–100 RO risk score
✅ build_cornerstone_chart()       — 60D price vs NAV rebased chart
✅ dispatch_cornerstone_alert()    — Discord + Pushover + personal + work email
✅ send_daily_pulse()              — 0800 HST gate, deduped via DB, ledger sweep
✅ check_and_escalate_if_critical()— 5-min loop, tier-transition debounced
✅ run_monitor()                   — main loop, CLI: python monitor.py test|force

🆕 fetch_time_series()            — shared TD helper, SPY fetched once/loop via cache
🆕 detect_dark_pool_activity()    — price drop on below-avg public vol (Feb/Mar 2026 fix)
🆕 detect_premium_compression()   — session-over-session premium collapse (CEF-specific)
🆕 check_macro_correlation()      — CLM/CRF vs SPY: CEF-specific vs macro drag
🆕 is_seasonal_caution_month()    — March / September flag
🆕 check_and_dispatch_seasonal_caution() — routes to #market-analysis + #trade-signals
🆕 format_pulse_report()          — mobile-first ┣/┗ Discord output formatter
🆕 get_alert_count()              — 3-notification rule counter
🆕 can_broadcast()                — gate: major change + under cap = broadcast
🆕 increment_alert_count()        — increments sector alert counter
```

### RO Composite Score Weights
```python
RO_SCORE_WEIGHTS = {
    # Original
    "sec_n2": 60,              # N-2 filing — single highest-conviction signal
    "z_danger": 25,            # premium z-score ≥ 2.0σ
    "z_caution": 12,           # premium z-score ≥ 1.5σ
    "premium_extreme": 10,     # premium > 25%
    "whale_distribution": 15,  # rvol ≥ 1.8x + price drop
    "credit_stress": 10,       # HY credit spread > 4.5%
    "ex_div_relief": -10,      # scheduled ex-div dip suppressor
    "ro_season": 8,            # mid-Feb to mid-Apr historical window
    "crisis_amplification": 12,# VIXY z-score ≥ 1.5σ
    # Added
    "dark_pool": 18,           # price drop on below-avg public volume
    "premium_compression": 15, # fast intra-session premium collapse
    "macro_underperform": 10,  # CEF drops harder than SPY same session
    "13f_holder_exit": 12,     # SC 13D/G large holder change detected
}
# Tier thresholds: LOW < 25 | ELEVATED 25–49 | CRITICAL ≥ 50
```

### Key Constants
```python
EX_DIV_WINDOW_DAYS = range(15, 20)      # mid-month heuristic
RO_FILING_SEASON = (2, 15, 4, 15)       # mid-Feb to mid-Apr
CRISIS_VIXY_Z_THRESHOLD = 1.5
SEASONAL_CAUTION_MONTHS = [3, 9]        # March, September
DARK_POOL_PRICE_DROP_PCT = -1.5         # % session drop threshold
DARK_POOL_VOLUME_RATIO_MAX = 0.75       # public vol < 75% of 20D avg
PREMIUM_COMPRESSION_THRESHOLD = -3.0   # % premium change in one session
ALERT_MAX_PER_SECTOR = 3               # 3-notification rule cap
ALERT_COOLDOWN_HOURS = 24
margin_rate = 7.25                      # benchmark margin cost %
```

---

## 6. Ecosystem Scripts (full repo map)

| File | Status | Purpose |
|------|--------|---------|
| `monitor.py` | ✅ Live | Cornerstone CLM/CRF protection engine |
| `database.py` | ✅ Live | EcosystemDatabase — state management |
| `analytics.py` | ✅ Live | HighFidelityAnalyticsEngine — ledger, grading, OHLC |
| `essentials_tools.py` | ✅ Live | Discord embed senders, chart generators |
| `market_analysis.py` | 🔲 To build | 0800 HST premarket morning report |
| `cross_asset.py` | ✅ Live | Futures board, ES/NQ market profile + CVD + structure, Initial Balance breakout scanner |
| `crypto.py` | 🔲 To build | BTC/ETH spot, Fear & Greed, funding rates |
| `scheduler.py` | ✅ Live | Central dispatcher — morning/eod/income/wheel_signals/wheel_position/iv_crush/gex/etc. |
| `stream.py` | ✅ Live | Real-time REST/WebSocket sentry (FX perimeter, telemetry, traps) |
| `tqqq.py` | ✅ Live | TQQQ directional sniper + standalone insurance-put renewal clock |
| `announcements.py` | 🔲 To build | Weekly accuracy scorecard for free tier |
| `.env` | ✅ Live | All API keys + webhooks (never committed) |

---

## 5b. Income Channel & Wheel Strategy Modules (added this session)

**#dividend-ccetfs** (`python scheduler.py --mode income`) — 4 segments, all real-data, no static watchlist:
1. CC ETF/dividend pulse (existing 10-fund universe — JEPI/JEPQ/DIVO/XYLD/QYLD/RYLD/SCHD/O/MAIN/ARCC)
2. Dividend Wheel v2 screener (existing — RSI/BB/IVR/delta-filtered CSP setups)
3. Ex-dividend radar (existing — 14-day countdown)
4. 🆕 **New Income ETF Radar** — `generate_new_income_etf_screener()` in analytics.py. Scans a verified-real ticker universe across YieldMax (MSTY, NVDY, TSLY, CONY, GOOY, AMDY, YMAX), Roundhill (XDTE, QDTE, RDTE), NEOS (QQQI, SPYI, BTCI), TappAlpha (MAGY). Filters: yield > 10% (from real dividend history, not hardcoded), monthly/weekly pay, > 6 months trading history (proxy for fund age — Twelve Data has no inception-date field), AUM > $50M where Twelve Data reports it (otherwise shown as "N/A — verify," never fabricated).

**Wheel signals** (`python scheduler.py --mode wheel_signals`) — runs every market session, dispatches to **WEBHOOK_DIVIDEND_CCETFS** (re-routed from trade-signals — wheeling these holdings for long-term income is income-channel content per operator direction, not separate "trading signal" content):
1. **Module 1 — Tier 2 IV Rank Screener**: `generate_tier2_iv_rank_alerts()` polls MAIN/MLPI/GPIQ/KQQQ/TDAQ (GPIQ added, TDAQ retained as official Tier 2 even though deprioritized personally), fires when IVR proxy > 35%, includes bid/ask spread check, earnings-date filter (skipped not faked when absent), a concrete CSP setup (strike/DTE/delta/volume/OI), and real dividend yield/frequency/amount when the underlying pays one.
2. **Module 2 — Wheel Position Tracker**: positions logged manually via `python scheduler.py --mode wheel_position --action open|close ...` (no brokerage link, so it never invents a position). DTE countdown at 21/14 days; closing adds premium to the `wheel_premium_collected_total` ledger.

**#trade-signals** (`python tqqq.py`, runs continuously) — two independent legs:
1. **Directional sniper** — gated on QQQ vs 21 EMA + SMA200/breadth/VIXY-z, with Black-Scholes risk/reward (premium vs ATR-projected move) already computed for every setup, live or monitoring-only. Now also: (a) never stacks a second LIVE entry while one is already open — downgrades to a quiet monitoring note instead, (b) hard 5-day cooldown between LIVE execution signals (`LIVE_SIGNAL_COOLDOWN_DAYS`) so entries fire roughly weekly, not daily, (c) dispatch payload now includes contract volume and OI range alongside strike/DTE/delta.
2. **Insurance put renewal clock** — `check_insurance_put_renewal()`, fully separate from the sniper, fires at 14 DTE. Logged via `python tqqq.py --log-put --strike X --expiration YYYY-MM-DD --premium X`. The "buy the dip after a payout, then sell CCs on it" leg is still not automated — only the renewal clock exists.

**LuxAlgo-style price action — already built, not new scope**: `market_structure.py` replicates the Smart Money Concepts toolkit (fair value gaps, liquidity sweeps, equal highs/lows clustering, composite structure classifier) from plain OHLCV math, already wired into the TQQQ sniper's dispatch as a confluence booster. True options-flow/dark-pool replication (LuxAlgo's or Unusual Whales' actual edge) needs Level 2/tape data Twelve Data's plan tier doesn't carry — not faked.

**Vault formulas, now implemented** (`analytics.py`, `market_structure.py`, `cross_asset.py`):
- `classify_vix_regime()` — 3-tier shield (NORMAL/ELEVATED/CRITICAL) keyed to VIXY z-score (real VIX unavailable at this plan tier), each tier carrying philo.txt's documented `rsi_shield_limit` and posture rule. Wired into the ES/NQ deep-dive and the IB breakout scanner's chase-filter.
- `calculate_unified_conviction_score()` — base 50 ± Supertrend (new: `market_structure.calculate_supertrend()`) ± RSI ± institutional volume flow ± GEX, score >75/<25 = INSTITUTIONAL LOCK-IN. Wired into the ES/NQ deep-dive.
- Initial Balance breakout scanner (`cross_asset.py: run_ib_breakout_scan()`) — IB window corrected to the professional-standard 60 min (9:30-10:30 ET, per Axia Futures methodology) rather than philo.txt's 30. Requires volume-delta confirmation (>55% buy/sell on the breakout bars), VIX-regime chase filter (skips if already >0.1% extended in ELEVATED/CRITICAL), stop at IB midpoint, 2:1 minimum R:R, breakeven-shift directive. Gated to one signal per symbol per day.
- **Fixed a real bug while implementing this**: the futures board was dispatching every single cron tick regardless of whether SPY/QQQ/Dow/IWM quotes had actually moved — confirmed live, three consecutive RTH posts showed byte-identical prices for hours. Now gated on composite % change with a 4-hour heartbeat fallback so the channel doesn't go fully dark either.

**Still open**: `/CL`/`/GC` have no deep-dive or breakout module (board only); true options-flow/dark-pool tape (the "Signal Filtering Logic for Options Flows" section of `vault/philo.txt`) still needs Level 2 data this plan tier doesn't carry.

**Removed**: `stream.py`'s `run_wheel_discovery()` called a nonexistent `generate_wheel_candidates()` method and silently failed every run while duplicating the (working) Dividend Wheel v2 segment above to the same channel — deleted rather than fixed, since the v2 segment already covers this with richer output.

---

## 7. .env Webhook Registry
```
WEBHOOK_MARKET_ANALYSIS=
WEBHOOK_TRADE_SIGNALS=       # options-wheel + tqqq-sniper
WEBHOOK_CORNERSTONE_RO=
WEBHOOK_ANNOUNCEMENTS=
WEBHOOK_DIVIDEND_CCETFS=
WEBHOOK_FUTURES_TRADING=
WEBHOOK_CRYPTO=
WEBHOOK_FED=
WEBHOOK_FOREX=               # key retained, channel deprecated
```

---

## 8. Infrastructure & Workflow
- **Data source:** Twelve Data (commercially licensed)
- **Runtime:** PythonAnywhere always-on task or tmux session
- **Notification stack:** Discord webhooks + Pushover + Gmail SMTP (personal + work)
- **Local dev:** MacBook + tmux + neovim
- **Deploy:** `git push origin main` → PythonAnywhere `git pull origin main`
- **Test:** `python monitor.py test` (fires once, skips date gate)
- **Force:** `python monitor.py force` (same as test)

---

## 9. SaaS Pricing Model (Discord subscription tiers)

| Tier | Price | Access |
|------|-------|--------|
| Free | $0 | #announcements only — weekly scorecard, teaser numbers |
| Basic | $19–$29/mo | Morning report + cornerstone alerts |
| Pro | $49–$69/mo | All channels + TQQQ sniper + wheel trades + DMs |
| VIP | $99–$149/mo | Everything + monthly 1:1 strategy call + portfolio review |

**Conversion funnel:** Free → 7–14 day trial → Paid
**Key differentiator:** Twelve Data commercial license = institutional-grade data
**Primary sales tool:** #announcements accuracy scorecard (target: 75–80%+ accuracy)

---

## 10. Stress Test Scenarios

| Scenario | Key Risk | Protection |
|----------|---------|------------|
| Market crash -30% | Margin call, NAV drop | 25% margin cap survives 50%+ drop; TQQQ puts pay out |
| Rights Offering | Share dilution | monitor.py fires → sell 99% → rebuy dip → net more shares |
| Margin rate spike | Higher interest cost | Tier 2 divs absorb increase; reduce draw if rate > div yield |
| Dark pool exit | Unexplained price drop | detect_dark_pool_activity() flags low-vol price drops |
| CEF premium collapse | Fast premium compression | detect_premium_compression() flags intra-session spread collapse |

---

## 11. 10-Year Financial Freedom Roadmap

| Year | CLM/CRF | Tier 2 | Monthly Cash | Milestone |
|------|---------|--------|-------------|-----------|
| 1 | ~$52k | ~$11k | ~$200 | System live, margin cycles active |
| 2 | ~$68k | ~$14k | ~$280 | Wheel premium adding ~$200/mo |
| 3 | ~$89k | ~$17k | ~$380 | TQQQ call profits redeployed |
| 4 | ~$116k | ~$21k | ~$520 | CLM/CRF DRIP self-accelerating |
| 5 | ~$152k | ~$25k | ~$710 | Divs cover margin interest entirely |
| 6 | ~$198k | ~$29k | ~$980 | Options income = second paycheck |
| 7 | ~$259k | ~$33k | ~$1,340 | Semi-retirement threshold |
| 8 | ~$337k | ~$38k | ~$1,820 | Margin cycles optional |
| 9 | ~$439k | ~$44k | ~$2,480 | W2 optional |
| **10** | **~$572k** | **~$51k** | **~$3,400+/mo** | **Financial freedom** |

At Year 10: flip CLM/CRF DRIP to cash → ~$9,800/month gross portfolio income.

---

## 12. Next Priorities for Claude Code Sessions
- [ ] `market_analysis.py` — 0800 HST premarket report aggregating all channel feeds
- [ ] `/CL` `/GC` futures-channel content is still just the steady-cadence board — no deep-dive/breakout module for them yet (ES/NQ have one via `cross_asset.py`)
- [ ] `crypto.py` — BTC/ETH + Fear & Greed + funding rates + NVDA/BTC correlation tracker
- [ ] `announcements.py` — weekly accuracy scorecard, prediction vs actual grader
- [ ] Accuracy scorecard backend — log predictions, grade outcomes, publish to #announcements
- [ ] Subscriber tier gating — lock premium channels, route free tier to #announcements only
- [ ] TQQQ insurance leg: automate "put pays out → buy TQQQ at discount → sell CCs on it" (currently only the 14 DTE renewal clock exists, see Section 5b)
- [ ] Wheel position entry currently manual-only (`scheduler.py --mode wheel_position`) — no brokerage API to auto-detect fills

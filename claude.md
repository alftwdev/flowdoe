# Cashflow ZZZ Machine — Project Context
*Master brief for Claude Code sessions. Update as ecosystem evolves.*
*Last updated: Jul 23 2026*

---

## 0-A. Deployment Runbook (PA — after every git pull)

### Step 1 — Pull on PythonAnywhere
```bash
cd ~/flowdoe_dev && git pull origin main
```

### Step 2 — Kill always-on tasks (copy-paste block)
```bash
pkill -f monitor.py
pkill -f market_scheduler.py
pkill -f market_analysis.py
pkill -f tqqq.py
pkill -f stream.py
```

### Step 3 — Restart via PA web UI
Go to **Web → Always-on tasks** and restart each. Order matters:
1. `market_scheduler.py` — dispatcher for all cron-style jobs
2. `monitor.py` — CLM/CRF protection, 5-min loop
3. `market_analysis.py` — morning/intraday/EOD brief
4. `tqqq.py` — LEAP desk + cycle scorer
5. `stream.py` — VIXY real-time → DB (crisis early warning)

### Step 4 — Verify (30 seconds after restart)
```bash
ps aux | grep -E "monitor|market_scheduler|market_analysis|tqqq|stream" | grep -v grep
```
All five should appear. If `market_analysis_bias` in DB is stale (> 2 days), the always-on
task died — restart it.

### One-time setup (new PA environment only)
```bash
python seed_cef_premiums.py   # seeds CLM/CRF z-score mu/sigma from CEFConnect history
```

---

## 0-B. Known Constants — Lock These, Never Guess

### CLM/CRF Distribution (2026 reset — do NOT use pre-reset values)
```python
# 2026 annual distributions (set once per year after October NAV lock)
CLM_ANNUAL_DIST = 1.4268   # $0.1189/month × 12
CRF_ANNUAL_DIST = 1.3824   # $0.1152/month × 12

# Fair value floor (annual_dist / 0.19 = FV at 19% yield target)
CLM_FAIR_VALUE  = 7.51
CRF_FAIR_VALUE  = 7.28

# NAV fallbacks (updated Jul 23 2026 — refresh whenever CEFConnect NAV changes >0.10)
CLM_NAV_FALLBACK = 6.45
CRF_NAV_FALLBACK = 6.18   # was 6.30 — corrected Jul 23 2026 based on implied NAV math

# Margin rate (E*TRADE)
MARGIN_RATE = 7.25
```

**Rule: Every script that calculates CLM/CRF yield or Div. Yield MUST use these constants.**
The pre-reset values ($0.1224 CLM / $0.1176 CRF → annuals 1.4688 / 1.4112) are dead.
Never use 1.4580, 1.4688, 1.4112, or 1.3984 in any new code — those inflate yield and
misrepresent the distribution reset. If you see those numbers in existing code, fix them.

### Bug fixed Jul 23 2026 — monitor.py distribution mismatch
`get_ticker_report()` line ~1190 was using `1.4580 / 1.4112` (pre-reset) for `y_dist`
(Div. Yield in embed footer) while `check_distribution_yield_floor()` correctly used
`1.4268 / 1.3824`. This caused the footer "Div. Yield" to show ~0.8–1.3% higher than
the floor yield line, creating an internal inconsistency visible in Discord embeds.
**Fix applied:** both paths now use the same 2026 constants.

### Bug fixed Jul 23 2026 — "HIGH PREMIUM" label fired on negative z-score
Status label logic (`send_daily_pulse`) was:
```python
elif ro_tier == "ELEVATED" or z_premium >= 1.5 or premium > 25.0:
    status = "HIGH PREMIUM"
```
When `ro_tier == "ELEVATED"` from non-premium signals (volume anomaly, RO season, etc.)
and `z_premium` was negative (premium BELOW historical average), the label "HIGH PREMIUM"
was factually wrong. Fixed to two separate branches:
- `z_premium >= 1.5 or premium > 25.0` → "HIGH PREMIUM"
- `ro_tier == "ELEVATED"` (when premium is safe) → "RISK ELEVATED"

**Rule: Never combine premium-label conditions with RO-score conditions in one elif.**
The z-score is the authoritative premium signal. A negative z-score always means safe premium,
regardless of what the composite RO score is. Label them separately.

---

## 0-C. API Budget & PA CPU Rules

### Twelve Data rate limit: 144 credits/min (Grow plan)
Each REST call = 1 credit. The 5-min monitor loop must stay well under budget.

**monitor.py loop budget per tick (approx):**
```
2 price/RSI calls (CLM, CRF)        = 2 credits
2 NAV proxy calls (XCLMX, XCRFX)   = 2 credits
1 SPY time_series 200-day           = 1 credit  (cached in spy_chg_cache, not re-fetched)
2 RVOL calls (CLM, CRF volume)      = 2 credits
2 OBV/MFI (conditional, only fires on divergence) = 0–2 credits
FRED HY spread: cached daily        = 0 credits (after first fetch)
VIXY from stream.py DB              = 0 credits (WebSocket, no REST)
─────────────────────────────────────────────────
Per loop: ~7–9 credits out of 720/5-min budget. Extremely lean.
```

**Stagger rule (prevents 429 collision):**
- monitor.py daily pulse: `08:10 HST = 18:10 UTC`
- market_analysis.py morning: `08:00 HST = 18:00 UTC`
- 10-minute gap between the two heaviest Twelve Data consumers. **Never move these closer.**

**stream.py:** WebSocket — 0 REST credits. Subscribes to `BTC/USD,VIXY,SPY,QQQ` and writes
VIXY price to DB for monitor.py to read. This is the zero-cost VIXY early warning layer.
If VIXY z-score is None in DB, stream.py is dead — restart it.

**No TDClient SDK on PA — REST only:**
Every Twelve Data call uses `requests.get()` directly. The SDK spawns WebSocket threads
on every instantiation and exhausts the OS thread limit. Never import or use `TDClient`.

**FRED API:** All FRED fetches cached to DB once per calendar day. The 5-min monitor loop
never hits FRED more than 1×/day. Pattern: `if not cached_today: fetch(); cache()`.

**Binance FAPI:** Free public endpoints — no API key. If returning zeros on PA, check
network egress rules. This doesn't affect the core strategy.

**SentiSense:** All fetches cached to DB per TTL (daily or 7-day depending on endpoint).
Never fetch the same SentiSense endpoint twice in a session.

---

## 0-D. Cross-Script Data Flow (read before adding any new signal)

Scripts communicate through the DB, never by importing each other.

```
stream.py   → DB: vixy_last_price, vixy_z_score (WebSocket, real-time)
monitor.py  → DB: clm_premium_z, crf_premium_z, clm_last_price, crf_last_price,
                   clm_last_nav, crf_last_nav, hy_spread_cached, carry_spread_data,
                   clm_floor_{date} signal_ledger entries
tqqq.py     → DB: tqqq_bottom_score, tqqq_top_score
market_analysis.py → DB: market_analysis_bias, morning_conviction_bias_{date}
scheduler.py → DB: orb_intraday_bias_{date}, orb_{sym}_{date}, wheel_snapshot_top,
                    wheel_candidates_snapshot, btc_sentiment via signal_ledger

Consumers:
  market_analysis.py reads: tqqq_bottom_score, tqqq_top_score, vixy_z_score,
                             clm_premium_z, crf_premium_z, orb_intraday_bias_{date},
                             fred_yield_spread, hy_spread_cached
  tqqq.py reads:            market_analysis_bias, vixy_z_score, orb_intraday_bias_{date}
  scheduler.py reads:       market_analysis_bias, tqqq_bottom_score, tqqq_top_score
```

**Rule: If a signal is already in the DB, READ it — don't re-fetch from the API.**
Example: VIXY z-score is written by stream.py → read from DB in monitor.py and tqqq.py.
Never make a REST call for data another script already provides via DB cache.

---

---

## 0-E. The 3 Personal Strategies (Real Funds — This Is The System)

These are the three live strategies running with actual capital. All ecosystem scripts
exist to serve, protect, and inform these three tracks. Nothing else matters.

---

### Strategy 1 — CLM/CRF Snowball Engine (Core Wealth Builder)

**The thesis:** CEF DRIP at NAV = structural alpha. Every distribution reinvested buys
shares below market price. Margin is velocity — borrowed equity buys more equity,
with Tier 2 dividends covering the interest cost so the loan is effectively free
once yield > margin rate. Simplifi tracks paycheck surplus so every dollar of
idle cash gets deployed immediately rather than sitting in checking.

**The mechanic:**
```
$500/wk auto-deposit + monthly W2 surplus (Simplifi by Quicken monitors leftover)
  → E*TRADE cash buffer
  → Bills paid via E*TRADE Bill Pay (treats portfolio as business operating account)
  → Surplus + Tier 2 dividends (MAIN/MLPI/TDAQ/KQQQ) → margin paydown
  → Margin freed → reborrow conservatively (never exceed 25% of portfolio value)
  → Buy more CLM/CRF on margin + buy more MLPI with cash (preferred — see Tier 2 note)
  → CLM/CRF DRIP at NAV → shares issued below market = built-in alpha every month
  → Tier 2 dividends cover margin interest → loan is structurally free
  → Rinse, repeat → compounding snowball effect
```

**The edge:**
- DRIP at NAV: shares issued at intrinsic value, not inflated market premium
- Rights Offering dodge: sell 99% on N-2 detection → rebuy post-dip → net MORE shares than participants
- Timed DCA: March and September seasonal weakness = deliberate accumulation zones
- Margin arbitrage: borrowing at ~7.25% against 19–21% blended yield = positive carry

**What monitor.py protects:**
- SEC EDGAR N-2 watcher (Rights Offering early warning)
- Dark pool detection (unexplained price drop = off-exchange exit)
- CEF premium compression (fast intra-session collapse)
- VIXY crisis overlay (market vol spike = CEF premium risk)
- Live HY credit spread from FRED (not hardcoded — reacts to real credit stress)
- **NAV Determination Month gate** (October = Cornerstone Board locks next year's distribution rate; heightened sensitivity all month)
- **CEF institutional exit detector** (high lit-market volume + SPY flat = institutions exiting the distribution reset — the Feb 2026 crash pattern)
- **Distribution yield floor** (fair value = annual_dist / 0.19; price > FV×1.10 = overvalued at new rate; price ≤ FV = accumulate zone. CLM 2026 FV: $7.51 | CRF 2026 FV: $7.28)

**Distribution reset cycle — what to watch (learned from Jan–Feb 2026 CLM -15% crash while SPY +3.6%):**
```
Phase 1 — NAV Peak (Oct 8): market priced CLM at premium before Board locked lower NAV
Phase 2 — Quiet Signal (Oct 14, Nov 13-17): 5–6M vol spikes on flat SPY = inst. distribution
Phase 3 — Trap Rally (Jan 2–14): new-year income buyers push to NEW HIGH ($8.51) on old rate
Phase 4 — Capitulation (Feb 13–19): 9.8M shares Feb 18 while SPY +0.5% — CEF-specific flush
Bottom = $7.23 = fair value at 19% yield on new $0.1189/mo distribution
Rebuy zone: price ≤ $7.51 (CLM) / $7.28 (CRF) → yield ≥ 19% → structural income buyer support
```
All three new signals in monitor.py fire as conditional lines in the #cornerstone pulse embed.

**Guardrails:**
- Margin never exceeds 25% of portfolio value
- Internal red line: portfolio drops 15% → stop new margin draws
- Keep ~$2k cash buffer (1 month of bills) at all times

---

### Strategy 2 — Options Wheel + CC ETF Income Arb

**The thesis:** Sell time premium on high-IV names. Collect cash. Pay down margin.
If assigned, you own shares at a discount AND potentially collect dividends while
waiting for the covered call to be exercised. Two parallel sub-tracks.

**Track A — Wheel on high-IV names (HIMS, SOFI, PLTR, COIN, etc.):**
```
Sell CSP (0.20 delta, 30–45 DTE) on high-IV names
  → Collect premium → margin paydown bucket
  → If assigned: own shares at strike - premium (below market cost basis)
  → BONUS: if ticker pays dividends → collect those too while holding
  → Sell CC against assigned shares (ATM/slight OTM, 21–30 DTE)
  → Exit: CC called away (profit realized) OR buy-back at 50% gain
  → Premium collected → margin paydown → rinse, repeat
```

**Dividend bonus rule:** When screening wheel candidates, prefer names that pay
dividends. If assigned, premium income + dividend income while running the CC.
Examples: SOFI (growing dividend), MAIN (if ever wheeled), O (monthly REIT).

**Defined Risk Mode (put credit spread as wheel substitute — NOT a 4th strategy):**
Credit spreads are NOT a standalone strategy. They are a capital-efficiency toggle
inside Track A. Use a put credit spread INSTEAD of a naked CSP when:
- Stock price > $100 (100-share assignment commitment is too capital-heavy at current portfolio stage)
- IVR > 55% AND stock is at/near technical support (IV crush opportunity)
- Want to preserve more margin headroom for CLM/CRF accumulation (primary goal)

Decision tree when wheel_signals fires:
```
Stock price ≤ $100 AND margin headroom comfortable → naked CSP (standard wheel)
Stock price > $100 OR margin is tight            → put credit spread instead
  → Sell short put at 0.20 delta, buy long put 5 points below, same DTE
  → Same premium income → same margin paydown bucket
  → No assignment path (defined max loss = spread width minus credit)
  → Stop-loss: close if spread value = 2× credit collected
  → Never roll if you'd pay a debit to do so — just close and move on
```

**Iron condors are NOT part of this strategy and have been removed from all scripts.**
The system sells CSPs and covered calls only. Iron condors were purged from scheduler.py,
tradier_client.py, and market_scheduler.py (Jul 19 2026). Do not re-introduce them.

Credit spreads were the personal strategy that funded retirement before this system.
That edge is preserved as a tool, not expanded into a competing engine. The CLM/CRF
snowball + margin headroom stays the priority. Spreads serve it, not the other way around.

**Track B — CC ETF income arb (TDAQ/KQQQ/MLPI hold-and-collect):**
```
Hold TDAQ / KQQQ / MLPI as Tier 2 long positions
  → Monthly distributions (~12–17% annualized) → margin paydown
  → These ARE packaged wheels — no manual execution needed
  → Can also wheel the CC ETFs themselves if IV is temporarily elevated
```

**Wheel universe (scheduler.py screens these):**
```python
WHEEL_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AMD",  # CORE
    "SCHD", "JEPI", "JEPQ", "O", "ARCC",                      # INCOME
    "TSLA", "COIN", "SOFI", "PLTR", "HIMS",                   # GROWTH/HIGH-IV
    "SPY", "QQQ", "IWM", "GLD", "XLE",                        # SECTOR
]
```

**Entry filter (scheduler.py --mode wheel_signals):**
```
IVR > 35% | bid-ask spread < 10% of mid | no earnings within 45 days
Premium ≥ 1% of strike | delta ~0.20 | 30–45 DTE
```

**Capital rule:** Max 30% of available margin in wheel positions at any time.

---

### Strategy 3 — TQQQ LEAP Desk (Calculated Time-Buying)

**The thesis:** On red days / bearish trends, buy TIME via deep ITM long-dated calls.
Recovery on a 3× Nasdaq ETF over a 9–18 month horizon is near-certain historically.
You are not predicting the exact bottom — you are buying enough time for the bounce
to come to you. Defined risk = premium paid only. Same logic inverted for tops.

**CALL desk — bottom-hunting (red days, bearish cycles):**
```
Cycle Position Scorer fires when bottom_score ≥ 55/100:
  Inputs: VIXY z-score (30pts) + RSI14 (25pts) + breadth (20pts) +
          52w drawdown (15pts) + SPY P/C z-score (15pts) +
          VIX term structure backwardation (12pts) + CNN F&G fear (10pts) +
          below SMA200 (5pts) + MACD (3pts) + actual VIX via FRED (confirmation)
  → BTO deep ITM TQQQ CALL: delta ~0.72, 270–540 DTE (9–18 months)
  → TP1: close 50% at +50% gain
  → TP2: close remainder at +100% gain
  → 2-hour cooldown: re-evaluates on continued downtrend (not a hard lockout)
```

**PUT desk — top-hunting (extended green days, overbought cycles):**
```
top_score ≥ 55/100 → BTO deep ITM QQQ PUT (NOT TQQQ — better liquidity at long DTE)
  Delta ~-0.72 | 180–365 DTE (6–12 months)
  Same TP1/TP2 structure as CALL desk
```

**Insurance put (always-on margin protection):**
```
Always 1 active SPY/QQQ put open — 30 DTE, rolls at 14 DTE regardless of P&L
Budget: ≤ 0.5% of portfolio/month
If VIX > 30 → close put at profit → rotate into TQQQ calls (fear peak = call entry)
Strike distance tied to live margin utilization (not fixed %):
  < 15% margin → 10% OTM | 15–25% → 7% OTM | 25%+ → 5% OTM
```

**Universe beyond TQQQ (open to expansion):**
| Ticker | Why | Notes |
|--------|-----|-------|
| TQQQ | 3× QQQ, deep ITM calls liquid | Primary CALL desk underlying |
| QQQ | Best PUT liquidity at long DTE | Primary PUT desk underlying |
| SPY | Most liquid options market globally | Lower leverage = more entries |
| NVDA | High IV, massive OI, AI cycle | AI cycle tops/bottoms predictable |
| PLTR | High retail + institutional, big IV swings | Good 6-9mo calls on dips |
| COIN | Tracks crypto cycle, extreme fear-day IV | Pairs with BTC F&G signal |
| SOFI | High IV fintech, dual-use (also wheel candidate) | Wheel + LEAP crossover |
| Social momentum names | GME/AMC-style when Reddit WSB + StockTwits conviction HIGH | `--mode trending_plays` already screens for these |

**Screener for next NVDA:** `scheduler.py --mode trending_plays` watches StockTwits +
Reddit WSB + Finviz for emerging high-conviction names with high IVR + liquid OI.
When social conviction AND options setup align → BTO LEAP alert.

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
- **Annual distributions:** CLM $0.1189/share | CRF $0.1152/share (2026 reset — decreased from $0.1224/$0.1176 due to lower Oct 2025 NAV lock)
- **2026 fair-value floor:** CLM $7.51 | CRF $7.28 (at 19% yield target — accumulate at or below these prices)

### Tier 2 — Margin Accelerators (cash dividends only, NO DRIP)
| Ticker | Type | Yield | Frequency | Role |
|--------|------|-------|-----------|------|
| MAIN | BDC | ~8% | Monthly | Stability anchor — never cut dividend since 2007 IPO |
| MLPI | MLP/Energy ETF w/ covered calls | ~15% | Monthly | Real asset base, no K-1 form |
| TDAQ | TappAlpha 0DTE NASDAQ covered call | ~12–17% | Monthly | Higher yield than JEPQ |
| KQQQ | Kurv Tech Titans covered call | ~15% | Monthly | AAPL/MSFT/NVDA/META/GOOGL basket |

**Blended Tier 2 yield:** ~13–15%
**All Tier 2 dividends → margin paydown (never reinvested)**

**MLPI cash-buy strategy (current focus):**
Buy MLPI with available cash (not margin). As MLPI equity grows, the portfolio's
overall margin capacity expands → use that expanded headroom to buy more CLM/CRF on
margin. MLPI's ~15% monthly distributions also accelerate margin paydown directly.
This makes each MLPI cash purchase a dual-action: income + unlocks more CLM/CRF buying power.
Entry signal: `scheduler.py --mode mlpi_entry` watches XLE ≤ -1.5% or DGS10 +8bps
intraday AND MLPI ≤ -0.5% → Pushover alert + Discord embed (red days = best buy window).

### Tier 3 — Opportunistic (cycle-dependent, small allocation)
| Ticker | Underlying | Use Case |
|--------|-----------|----------|
| BITA | Bitcoin (BlackRock covered call) | Crypto bull cycle income |
| YBTC | Bitcoin (Roundhill covered call) | Weekly crypto income |
| CHPY | Semiconductor basket | AI momentum phases only — under consideration |

**Tier 3 rule:** Extract cash weekly → margin paydown. Exit when crypto/AI cycle peaks.

**CHPY consideration:** Semiconductor/AI basket covered call. Under consideration for
small allocation during AI momentum phases. NOT a long-term hold — 12–18 month frame
maximum, cash-only entry (no margin), exit when AI cycle shows exhaustion signals
(SentiSense leaderboard AI names deteriorating + NVDA/AMD breadth rolling over).
Decision pending — do not add until margin headroom is comfortable post-MLPI purchases.

### Deprecated / Removed from Active Scope
- GOOW, NVII — too volatile, NAV decay risk too high for margin paydown role
- TSYX — launched Jan 2026, tiny AUM, 1.3x leverage, 3% yield (skip)
- Forex channel — no correlation to end goal (discontinued)
- TSP channel — no correlation to end goal (discontinued)

---

## 3. Options Strategy

### Wheel (0.20 delta, 30–45 DTE)
**Underlyings:** Dynamic 25-name universe (not Tier 2 long holds — those are for dividends, not wheeling)
```python
WHEEL_UNIVERSE = [
    # CORE
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AMD",
    # INCOME
    "SCHD", "JEPI", "JEPQ", "O", "ARCC",
    # GROWTH
    "TSLA", "COIN", "SOFI", "PLTR",
    # SECTOR
    "SPY", "QQQ", "IWM", "GLD", "XLE",
]
```
```
STEP 1 — Entry filter: IVR > 35%, (ask-bid)/mid < 10%, no earnings within 45 days
STEP 2 — Sell CSP: 0.20 delta, 30–45 DTE, premium ≥ 1% of strike
STEP 3 — Manage: Close at 50% profit | Roll at 21 DTE if untested
          If breached: roll down+out for credit, or take assignment → sell CC
STEP 4 — CC after assignment: ATM/slight OTM, 21–30 DTE
STEP 5 — Capital rule: max 30% of available margin in wheel at any time
```
**Premium income → margin paydown bucket**

**Current data limitation:** IVR and delta are proxy-calculated (HV30-based). With Tradier ($10/mo), these become real options chain values — accurate 0.20 delta strikes, real bid/ask spread check, real OI/volume for liquidity confirmation. Build 52-week IVR by storing daily IV in DB; after 252 days it's a full historical rank. **IVR tracker is live** (`scheduler.py --mode store_daily_iv`, runs daily at 21:30 UTC) — accumulating IV data now, usable baseline in ~30 days.

### TQQQ LEAP Desk — Bidirectional (tqqq.py)

Three independent strategies run in tqqq.py:
1. **Directional Sniper** — short-dated QQQ/TQQQ options, gated on regime
2. **LEAP CALL Desk** — BTO deep ITM TQQQ calls on red days / bearish cycles (bottom-hunting)
3. **LEAP PUT Desk** — BTO deep ITM QQQ puts on green days / bullish cycles (top-hunting)
4. **Insurance put renewal clock** — 30 DTE SPY/QQQ puts, rolls at 14 DTE

**LEAP CALL constants:**
```python
LEAP_DTE_MIN = 270          # 9 months minimum
LEAP_DTE_MAX = 540          # 18 months maximum
LEAP_DELTA_TARGET = 0.72    # deep ITM
LEAP_COOLDOWN_HOURS = 2     # re-evaluates bottom on continued downtrends
LEAP_TP1_PCT = 50.0         # scale 50% out
LEAP_TP2_PCT = 100.0        # close remainder
```

**LEAP PUT constants (QQQ puts, NOT TQQQ — better liquidity + lower theta decay):**
```python
LEAP_PUT_DTE_MIN = 180         # 6 months
LEAP_PUT_DTE_MAX = 365         # 12 months
LEAP_PUT_DELTA_TARGET = -0.72  # deep ITM put
LEAP_PUT_COOLDOWN_HOURS = 2
LEAP_PUT_SYMBOL = "QQQ"
```

**Cycle Position Scorer** (`calculate_cycle_score()`) — gates both desks:
```python
CYCLE_BOTTOM_THRESHOLD = 55   # bottom_score >= this → CALL desk unlocks
CYCLE_TOP_THRESHOLD = 55      # top_score >= this → PUT desk unlocks
```
Inputs: VIXY z-score (30pts), RSI14 (25pts), breadth (20pts), 52w drawdown (15pts),
SPY P/C z-score (15pts), VIX term structure (12pts), CNN F&G (10pts), SMA200 (5pts), MACD (3pts).
P/C ratio scored on 30-day rolling z-score (raw SPY ratio is structurally ~1.5+ due to institutional hedging — raw number is meaningless alone).
VIX term structure via VIXY/VXZ ETF proxies (VIX9D/VIX3M unavailable at Twelve Data tier).
**Actual VIX (FRED VIXCLS)** also fetched and shown in embed to confirm VIXY proxy reading.

**VIXY Distribution Gate (added Jul 19 2026 — prevents false CALL entries):**
```
Three-signal distribution flag: VIXY z < 0 AND MACD bearish AND QQQ below EMA21
  → bottom_score − 20 (hard dampen — orderly selloff, not capitulation)
Single-condition calm: VIXY z < 0 only
  → bottom_score − 10 (partial dampen)
```
Root cause: Jul 17 2026 signal fired on VIXY z = −1.38σ (calm) + bearish MACD + below EMA21.
The embed correctly labeled it "distribution, not capitulation" but the scorer still crossed 55.
The gate enforces what the embed was already saying. Genuine fear (VIXY z ≥ +1.5σ) unaffected.
**Rule: CALL desk requires actual fear (elevated VIXY), not just a red day.**

**12-Month Seasonal LEAP Calendar** (added Jul 19 2026):
```python
# CALL desk size scalars (PUT desk inverts automatically via 1/max(scalar, 0.5))
Jan: +25%  Feb: neutral  Mar: −50%  Apr: −25%  May: −50%
Jun: neutral  Jul: neutral  Aug: −25%  Sep: −50%  Oct: +25%  Nov: neutral  Dec: neutral
```
Size scalar displayed in every LEAP embed. Mar/Sep/May = weakest entry months (wait for 3 green days).
Jan/Oct = strongest CALL accumulation windows.

**CLI flags:**
- `--test-leap` — clears `tqqq_last_leap_signal_ts` to 0, fires CALL desk
- `--test-leap-put` — fires PUT desk
- `--log-leap-put --strike X --expiration YYYY-MM-DD --premium X` — logs PUT position

**Puts (insurance / margin protection — homeowners insurance model):**
- Always 1 active put open — 30 DTE, rinse & repeat
- Roll at 14 DTE regardless of profit/loss
- Budget: ≤ 0.5% of portfolio/month
- If VIX > 30 → close puts at profit → rotate into TQQQ calls (fear peak = call entry)

**Strike distance tied to margin cushion, not a fixed %:**
```
Conservative margin (< 15% utilization) → 10% OTM (insures against 2008-style 30-40% crash)
Moderate margin (15–25% utilization)    →  7% OTM (tighter trigger, more cushion needed)
Aggressive margin (25%+ utilization)    →  5% OTM (must catch danger zone before margin call)
Rule: run your current equity ratio → find max tolerable decline → set strike inside that threshold
```
**Basis risk caveat:** SPY puts are used for liquidity (CLM/CRF have thin-to-no options markets).
SPY protects against broad market crashes. It does NOT protect against CEF-specific events
(distribution cut, rights offering, premium blowout) where CLM/CRF drops while SPY shrugs.
monitor.py's EDGAR + dark pool + premium compression detection covers the CLM/CRF-specific risk.
SPY puts are best applied at the ~$100K+ portfolio stage using your actual margin-buffer number.

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
| #futures-trading | WEBHOOK_FUTURES_TRADING | cross_asset.py | Futures board (4×/day) + IB breakout scanner + yield curve/Fed Funds from FRED |
| #crypto | WEBHOOK_CRYPTO | crypto.py | BTC/ETH spot, Fear & Greed, on-chain |
| #options-wheel | WEBHOOK_TRADE_SIGNALS | options.py | Wheel strategy + TQQQ sniper signals |
| #options-wheel | WEBHOOK_TRADE_SIGNALS | scheduler.py (`--mode trending_plays`) | Social sentiment scanner (StockTwits + Reddit WSB + Finviz) → top 5 options plays with BTO setup when HIGH conviction |
| #crypto | WEBHOOK_CRYPTO | scheduler.py (`--mode crypto_social`) | Fear & Greed + spot prices + funding rates + Binance derivatives stack (OI/L/S/taker) |
| #futures-trading | WEBHOOK_FUTURES_TRADING | scheduler.py (`--mode futures_social`) | StockTwits + Reddit WSB filtered to energy/metals/rates/ag names |
| #dividend-ccetfs | WEBHOOK_DIVIDEND_CCETFS | scheduler.py (`--mode income`) | Wheel Candidates v2 + New CC ETF Screener |
| #options-wheel | WEBHOOK_TRADE_SIGNALS | scheduler.py (`--mode wheel_signals`) | Tier 2 IV Rank screener + open wheel position DTE countdown |
| #fed | WEBHOOK_FED | fed.py | Fed rate/macro signals |

### #market-analysis — The Command Center (Most Vital Channel)

This is the single source of truth for daily decision-making. Every other channel
feeds into it. The morning brief and EOD brief from this channel are relied on
heavily — they set the bias, posture, and conviction level for the day.

**What it synthesizes:**
- Pre-market: /ES /NQ overnight levels, VIX regime, yield curve, macro backdrop
- Intraday: unusual moves, cross-asset signals, breadth deterioration
- EOD: recap of what fired, what to watch tomorrow, any strategy adjustments
- Cross-signals from #cornerstone (RO alerts), #crypto (F&G extremes), #futures (bias)

**Built and live:** `market_analysis.py` — 0800/10:20/13:40 HST briefs with 8-flag bias scorer. Writes `market_analysis_bias` to DB for cross-script consumption.

### #futures-trading and #crypto — Intelligence / Conviction Channels

These channels don't drive direct trades — they sharpen conviction and provide
macro context that informs all three strategies.

**#futures-trading** (cross_asset.py + scheduler.py futures_social):
- /ES /NQ /CL /GC overnight and session moves → bias for the day
- Yield curve (T10-T2 from FRED) → recession watch, LEAP PUT conviction
- Fed Funds rate → margin cost context
- IB breakout scanner → early session momentum confirmation
- Commodity moves → macro rotation signal

**#crypto** (scheduler.py crypto_social):
- BTC/ETH Fear & Greed → cross-signal for LEAP CALL bottom-hunting
- Binance OI + L/S ratio + taker volume → smart money vs retail divergence
- When retail is panic-shorting + smart money is net long = dual-asset capitulation
  → adds conviction to TQQQ CALL entry when equities are also red
- Funding rates → crowded trade detection

### Cross-Channel Data Flow (Unity Map)
```
#cornerstone  ──RO Alert──────────────────► #market-analysis (action item)
              ──Dip watch countdown──────► #options-wheel (call entry signal)

#crypto       ──Fear & Greed < 25─────── ► #market-analysis (risk-on signal)
              ──Extreme Fear──────────── ► #options-wheel (TQQQ call cross-signal)
              ──Binance L/S divergence──► LEAP CALL bottom signal cross-confirm

#options-wheel──Premium collected──────► #market-analysis (cashflow log)

#options-wheel──Put profit realized────► rotate to TQQQ calls (same channel SOP)

#futures      ──/NQ overnight > +0.5%──► #market-analysis (bullish bias)
              ──/NQ overnight < -1%────► TQQQ put check reminder
              ──Yield curve inverted───► LEAP PUT conviction booster

#market-analysis ← synthesizes ALL feeds → MORNING BRIEF + EOD BRIEF + INTRADAY ALERTS
                   (single source of truth for daily posture and conviction)
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

### All Functions (✅ = original preserved | 🆕 = added)
```
✅ check_sec_edgar()               — N-2 + SC 13D/G EDGAR watcher
                                     CIKs: CLM=0000814083 | CRF=0000033934
✅ fetch_live_metrics()            — Twelve Data price / RSI / NAV
✅ detect_whale_flow_direction()   — direction-aware (accum. vs distribution)
✅ check_crisis_amplification_risk()— VIXY z-score overlay (threshold: 1.5σ)
✅ calculate_ro_risk_score()       — composite 0–100 RO risk score
✅ build_cornerstone_chart()       — 60D dark-theme matplotlib chart (candlesticks + SMA20/50 + volume). Replaced Finviz URL fetch Jul 15 (Finviz dark mode is paid Elite only).
✅ dispatch_cornerstone_alert()    — Discord + Pushover + personal + work email
✅ send_daily_pulse()              — 0800 HST gate, deduped via DB, ledger sweep
✅ check_and_escalate_if_critical()— 5-min loop, tier-transition debounced
✅ run_monitor()                   — main loop, CLI: python monitor.py test|force

🆕 fetch_time_series()            — shared TD helper, SPY fetched once/loop via cache
🆕 fetch_hy_spread_live()         — FRED BAMLH0A0HYM2 live HY credit spread (replaces
                                     hardcoded 4.5%). Cached to DB once/day; fallback
                                     to last cached value if FRED unreachable.
🆕 detect_dark_pool_activity()    — price drop on below-avg public vol
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
    "credit_stress": 10,       # HY credit spread > 4.5% (FRED live, not hardcoded)
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
FRED_API_KEY = os.getenv("FRED_API_KEY") # confirmed in .env
```

---

## 6. Ecosystem Scripts (full repo map)

| File | Status | Purpose |
|------|--------|---------|
| `audit.py` | ✅ Live | Daily DB maintenance — prunes stale alert locks (>24h), caps audit_logs at 500 rows, runs VACUUM. Runs once/day at 09:39 UTC via cron. |
| `monitor.py` | ✅ Live | Cornerstone CLM/CRF protection engine. Live HY spread via FRED (cached daily). New Jul 19: NAV determination month gate (Oct), CEF institutional exit detector (high vol + flat SPY), distribution yield floor (FV at 19% yield target). All zero extra API calls. |
| `database.py` | ✅ Live | EcosystemDatabase — state management |
| `analytics.py` | ✅ Live | HighFidelityAnalyticsEngine — ledger, grading, OHLC, FRED helpers, Binance derivatives |
| `essentials_tools.py` | ✅ Live | Discord embed senders, chart generators |
| `market_analysis.py` | ✅ Live | Always-on (6th PA slot). 0800 HST morning brief + 10:20 HST intraday pulse + 13:40 HST EOD recap → #market-analysis. 8-flag bias scorer (BULLISH/NEUTRAL/BEARISH). Synthesizes FRED + VIXY + SPY/QQQ + F&G + CLM/CRF z-score + TQQQ cycle + wheel positions. |
| `cross_asset.py` | ✅ Live | Futures board (change-gated, 4h heartbeat) + yield curve/Fed Funds from FRED + ES/NQ market profile + CVD + structure + IB breakout scanner |
| `crypto.py` | 🔲 To build | BTC/ETH spot, Fear & Greed, funding rates |
| `scheduler.py` | ✅ Live | Central dispatcher. Active modes: morning/eod/income/iv_crush/post_market/macro/market_intraday/weekly_scorecard/wheel_signals/wheel_position/trending_plays/crypto_social/futures_social/store_daily_iv/cef_calibrate/mlpi_entry/personal_scorecard. Removed: `gex`, `options_flow`, `spx_income` (iron condor — purged Jul 19). wheel_signals: VIX-adjusted params (Module 4) + earnings proximity on open positions (Module 5) + Kelly position size footer. crypto_social: cycle top score + Tier 3 exit Pushover (triple gate). trending_plays: SS leaderboard as 4th source. mlpi_entry: XLE/MLPI dip signal → Pushover + Discord. personal_scorecard: Pushover-only Sunday recap of all 3 strategies from DB. |
| `stream.py` | ✅ Live | WebSocket-only sentry: BTC/USD hourly volatility breach alerts, SPY/QQQ perimeter alerts (RTH only), VIXY real-time price → DB for monitor.py. Subscribes: `BTC/USD,VIXY,SPY,QQQ` (RTH) / `BTC/USD` (off-hours). XAU/USD removed — forex channel deprecated. |
| `tqqq.py` | ✅ Live | Bidirectional LEAP desk (CALL + PUT) + directional sniper + insurance put renewal clock. Real VIX from FRED VIXCLS shown in LEAP embeds. Writes bottom_score/top_score to DB for market_analysis.py. New Jul 19: VIXY distribution gate (prevents false CALL entries on calm red days), 12-month seasonal size scalar for both desks. |
| `market_structure.py` | ✅ Live | SMC toolkit — FVGs, liquidity sweeps, equal highs/lows, Supertrend (REST, no SDK threads). |
| `tradier_client.py` | ✅ Live | Tradier options chain helper. Added `get_earnings_proximity()` — Tradier /markets/calendar, FORCE_CLOSE ≤7d / REVIEW ≤21d flags. |
| `seed_cef_premiums.py` | ✅ One-time tool | Run once on PA to seed CLM/CRF z-score mu/sigma from 252-day CEFConnect premium history. Replaces hardcoded defaults (mu=15, sigma=4) with empirical data. |
| `sentisense_client.py` | ✅ Live | SentiSense API client with full DB caching. Trackers added Jul 15: get_reddit_picks (7-day cache), get_sentiment_movers (daily), get_sentiment_leaderboard (daily). Wired into analytics.py trending_plays + futures_social as additional discovery sources. |
| `announcements.py` | 🔲 To build | Weekly accuracy scorecard for free tier |
| `.env` | ✅ Live | All API keys + webhooks (never committed). Includes FRED_API_KEY + SENTISENSE_API_KEY. |

---

## 6b. FRED Integration (live as of Jul 2026)

All FRED fetches are **cached to DB once per calendar day** — zero redundant API calls across the 5-min monitor loop ticks. Graceful fallback to last cached value on FRED unavailability.

| Signal | FRED Series | Used In | Threshold |
|--------|------------|---------|-----------|
| HY Credit Spread | BAMLH0A0HYM2 | monitor.py RO composite score | > 4.5% = credit_stress +10pts |
| Actual VIX | VIXCLS | tqqq.py cycle scorer + LEAP embed | Confirms VIXY proxy |
| Yield Curve (T10-T2) | DGS10 − DGS2 | cross_asset.py futures board | Inverted = recession watch |
| Fed Funds Rate | FEDFUNDS | cross_asset.py futures board | Context line |

`analytics.py` has shared FRED helpers: `_fetch_fred_metric()`, `fetch_real_vix()`, `fetch_yield_curve()`, `fetch_fred_macro_snapshot()`, `fetch_hy_spread()`.

---

## 6c. Binance Derivatives Stack (live as of Jul 2026)

Added to `scheduler.py --mode crypto_social` → #crypto channel. All **free Binance FAPI public endpoints — no API key required**.

```
analytics.py: fetch_binance_derivatives()
  → BTC + ETH per symbol:
    • open_interest (USD)
    • global_ls  (retail long/short account ratio)
    • top_ls     (top-trader long/short ratio — smart money)
    • taker_buy_pct (% of taker volume that is buys)

Smart-money divergence signal fires when:
  top_ls > 1.1 AND global_ls < 1.0 → smart money diverging long (bullish cross-signal)
  top_ls < 0.9 AND global_ls > 1.1 → smart money diverging short (bearish cross-signal)
```

OI + taker direction cross-signals into LEAP CALL bottom_score context (retail panic-shorting while smart money absorbs = dual-asset capitulation signal).

---

## 6d. SentiSense Integration (live as of Jul 15 2026)

All SentiSense fetches are **cached to DB** — zero redundant API calls across cron runs.

| Function | Endpoint | Cache | Used In |
|----------|----------|-------|---------|
| `get_market_mood()` | `/market/mood` | daily | monitor.py RO score (sentiment_fear flag) |
| `get_sentiment(ticker)` | `/stocks/{SYM}/sentiment` | daily per ticker | trending_plays, wheel_signals |
| `get_insights(ticker)` | `/insights/stock/{SYM}` | daily per ticker | wheel_signals insider cluster signal |
| `get_institutional_flows(ticker)` | `/institutional/flows` | daily per ticker | wheel_signals 13F flow overlay |
| `get_congressional_trades()` | `/politicians/activity` | daily | scheduler.py (available) |
| `get_reddit_picks()` | `/trackers/reddit-picks` | 7-day (monthly refresh) | analytics.py `_fetch_reddit_wsb_mentions()` — primary source, replaces 403-prone Reddit scrape |
| `get_sentiment_movers()` | `/trackers/sentiment-movers` | daily | analytics.py `generate_futures_social_snapshot()` — energy/metals movers |
| `get_sentiment_leaderboard()` | `/trackers/sentiment-leaderboard` | daily | analytics.py `generate_trending_options_plays()` — 4th discovery source (bullish side) |

**monitor.py RO score cross-signals from SentiSense:**
```python
"yield_steepen": 5   # yield curve spread moved >0.20 in one day (DB: fred_yield_spread/prev)
"sentiment_fear": 5  # ss_market_mood score ≤ 25 (Extreme Fear overlay on CLM/CRF risk)
```

**Trending plays source hierarchy (generate_trending_options_plays):**
```
1. StockTwits trending (real-time)
2. Reddit WSB → SentiSense reddit-picks tracker (primary, 7-day cache, no 403 risk)
              → raw Reddit JSON (fallback, may 403 on PA IPs)
3. Finviz top gainers + unusual volume (CSV export, free)
4. SentiSense Sentiment Leaderboard — bullish side (daily, curated by score)
Per-symbol SS score ≥ 30 also counts as +1 source (upgrades NEUTRAL → HIGH)
```

---

## 7. Income Channel & Wheel Strategy Modules

**#dividend-ccetfs** (`python scheduler.py --mode income`) — 4 segments, all real-data:
1. CC ETF/dividend pulse (JEPI/JEPQ/DIVO/XYLD/QYLD/RYLD/SCHD/O/MAIN/ARCC)
2. Dividend Wheel v2 screener (RSI/BB/IVR/delta-filtered CSP setups)
3. Ex-dividend radar (14-day countdown)
4. **New Income ETF Radar** — `generate_new_income_etf_screener()` in analytics.py. Scans YieldMax (MSTY, NVDY, TSLY, CONY, GOOY, AMDY, YMAX), Roundhill (XDTE, QDTE, RDTE), NEOS (QQQI, SPYI, BTCI), TappAlpha (MAGY). Filters: yield > 10%, monthly/weekly pay, > 6 months trading history, AUM > $50M where available.

**Wheel signals** (`python scheduler.py --mode wheel_signals`) → **WEBHOOK_DIVIDEND_CCETFS**:
1. **Tier 2 IV Rank Screener** — `generate_tier2_iv_rank_alerts()`, fires when IVR proxy > 35%
2. **Wheel Position Tracker** — logged manually via `python scheduler.py --mode wheel_position --action open|close ...`

**IVR Tracker** (`python scheduler.py --mode store_daily_iv`, daily at 21:30 UTC):
- Stores daily ATM IV per symbol in DB
- ~30 days = usable rolling IVR baseline
- 252 trading days = full 52-week rank (replaces HV30 proxy permanently)
- **Status:** Live and accumulating since Jul 11 2026 (stored=22 skipped=4 on first run)

---

## 8. .env Webhook Registry + API Keys
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
WEBHOOK_INCOME=              # used by mlpi_entry mode

# API Keys
TWELVE_DATA_API_KEY=         # commercially licensed
FRED_API_KEY=                # free — FRED/STLOUISFED, confirmed in .env
TRADIER_API_KEY=             # $10/mo — options chain enrichment (live)
SENTISENSE_API_KEY=          # SentiSense — sentiment, trackers, congressional trades
```

---

## 9. Infrastructure & Workflow
- **Data source:** Twelve Data (commercially licensed) — price, OHLCV, RSI, time series
- **Macro data:** FRED API (free) — VIX, HY spread, yield curve, Fed Funds, M2
- **Crypto derivatives:** Binance FAPI (free public) — OI, L/S, taker volume, funding rates
- **Options chains:** Tradier ($10/mo) — real IV, delta, OI, bid/ask per strike (live)
- **Runtime:** PythonAnywhere always-on task or tmux session
- **Notification stack:** Discord webhooks + Pushover + Gmail SMTP (personal + work)
- **Local dev:** MacBook + tmux + neovim
- **Deploy:** `git push origin main` → PythonAnywhere `git pull origin main`
- **Test:** `python monitor.py test` (fires once, skips date gate)
- **Force:** `python monitor.py force` (same as test)

### PythonAnywhere CPU / Thread Safety Rules
- **No TDClient SDK** — spawns WebSocket threads on every instantiation, exhausts OS thread limit. All Twelve Data calls use plain `requests.get()` REST only.
- `market_structure.py` Supertrend: REST-only, no SDK. Direction derived by comparing price to supertrend level (REST endpoint doesn't return trend field).
- `monitor.py` RVOL: REST-only.
- All FRED fetches: cached to DB once/day — 5-min monitor loop never hits FRED more than 1×/day.
- `stream.py`: WebSocket-only for BTC/USD + equities (RTH only). REST poller removed (was 2,880 calls/day with no unique value).

### Data Source Gap Map
| Need | Current | With Tradier |
|------|---------|-------------|
| Options IV (wheel IVR) | HV30 × 1.15 proxy ⚠️ | Real ATM IV from chain ✅ |
| Delta at strike | Formula approximation ⚠️ | Real chain delta ✅ |
| Bid/ask spread check | Estimated ⚠️ | Real market prices ✅ |
| OI / volume confirmation | Proxy range ⚠️ | Real per-strike OI ✅ |
| IV Rank (52-week) | Accumulating in DB 🟡 | Full rank after 252 days ✅ |
| GEX (SPY dealer flow) | Returns 0.0 — disabled ❌ | Real strike-by-strike OI → real GEX ✅ |
| CLM/CRF options | N/A (CEF, thin market) | N/A — monitor.py covers via EDGAR ✅ |
| HY Credit Spread | FRED BAMLH0A0HYM2 ✅ | — |
| Actual VIX | FRED VIXCLS ✅ | — |
| Yield Curve | FRED DGS10−DGS2 ✅ | — |
| Crypto OI + L/S | Binance FAPI free ✅ | — |

**GEX note:** `calculate_gex_profile()` disabled (returns 0.0 at Twelve Data tier). Re-enable once Tradier OI is wired — gamma flip is an early warning for CLM/CRF premium compression events.

---

## 10. SaaS Pricing Model (Discord subscription tiers)

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

## 11. Stress Test Scenarios

| Scenario | Key Risk | Protection |
|----------|---------|------------|
| Market crash -30% | Margin call, NAV drop | 25% margin cap survives 50%+ drop; TQQQ puts pay out |
| Rights Offering | Share dilution | monitor.py fires → sell 99% → rebuy dip → net more shares |
| Margin rate spike | Higher interest cost | Tier 2 divs absorb increase; carry spread alert fires if spread < 5% |
| Dark pool exit | Unexplained price drop | detect_dark_pool_activity() flags low-vol price drops |
| CEF premium collapse | Fast premium compression | detect_premium_compression() flags intra-session spread collapse |
| Credit crunch | HY spread spike | FRED live spread → RO score reacts in real time (was hardcoded) |
| Distribution cut (NAV reset) | Price drops -15% while SPY flat | Oct NAV gate + institutional exit detector + yield floor; all three layers fire before/during Feb-style crash |
| False LEAP CALL entry | BTO on calm red day, not capitulation | VIXY distribution gate: calm z + bearish MACD + below EMA21 → score dampened by 20pts, CALL desk stays shut |

---

## 12. 10-Year Financial Freedom Roadmap

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

## 13. Next Priorities for Claude Code Sessions

### Completed in Jul 2026 Sessions ✅
- [x] FRED integration (HY spread, real VIX, yield curve, Fed Funds)
- [x] Binance derivatives (OI, L/S, taker volume, smart money divergence)
- [x] `market_analysis.py` — 0800/10:20/13:40 HST synthesis brief, 8-flag bias scorer (✅ Jul 12)
- [x] CEFConnect premium calibration — `calibrate_cef_premium_zscore()` + `seed_cef_premiums.py` (✅ Jul 12)
- [x] VIX-adjusted wheel params — `get_vix_adjusted_params()` + wheel_signals Module 4 (✅ Jul 12)
- [x] Earnings proximity scanner — `get_earnings_proximity()` + wheel_signals Module 5 (✅ Jul 12)
- [x] Crypto cycle top scorer — `calculate_crypto_top_score()` wired into crypto_social (✅ Jul 12)
- [x] Position sizer — `kelly_position_size()` (half-Kelly + VIX scalar) in analytics.py (✅ Jul 12)
- [x] 8 cross-script data flows wired (yield curve → monitor.py, CLM/CRF z-score → tqqq.py, bias DB → scheduler.py Module 4, MLPI entry signal, etc.) (✅ Jul 15)
- [x] Dark-theme cornerstone chart — matplotlib dark candlestick chart replaces Finviz URL (✅ Jul 15)
- [x] Ex-div display line removed from daily pulse embed (✅ Jul 15)
- [x] SentiSense Tracker API — reddit-picks / sentiment-movers / sentiment-leaderboard wired into analytics.py (✅ Jul 15)
- [x] Reddit 403 fix — SentiSense reddit-picks tracker now primary source for WSB mentions (✅ Jul 15)
- [x] `research_bot.py` upgraded — real Tradier IV/IVR, SentiSense sentiment, DB cycle scores for /query slash commands (✅ Jul 19)
- [x] Strategy 1 hardening — carry spread alert (Tier 2 yield − margin rate; Pushover if < 5%), persisted to DB (✅ Jul 19)
- [x] Strategy 2 hardening — earnings proximity on OPEN wheel positions (deduped per position+date), Kelly size footer in wheel candidates (✅ Jul 19)
- [x] Strategy 3 hardening — VIXY distribution gate (prevents false CALL entries on calm red days), 12-month seasonal LEAP calendar, Tier 3 crypto exit Pushover (triple-gate) (✅ Jul 19)
- [x] Personal scorecard — `scheduler.py --mode personal_scorecard`, Pushover-only Sunday recap, zero new API calls (✅ Jul 19)
- [x] Iron condors purged — removed from scheduler.py, tradier_client.py, market_scheduler.py; strategy is BTO LEAP calls/puts + wheel CSPs/CCs only (✅ Jul 19)
- [x] CLM/CRF distribution reset cycle signals — NAV determination month gate, CEF institutional exit detector (high vol + flat SPY), distribution yield floor; all wired into monitor.py RO score and pulse embed (✅ Jul 19)
- [x] TQQQ false signal forensic analysis — Jul 17 2026 signal validated against actual CLM/CRF price history; VIXY gate confirmed working (✅ Jul 19)

### Weekly Audit Cadence (ongoing discipline)
Capital is deployed and compounding. Each week, check for signals that slipped through:
1. **Did any monitor.py signals fire?** — Review #cornerstone for any ELEVATED/CRITICAL events
2. **Carry spread still ≥ 5%?** — Personal scorecard (Sunday Pushover) surfaces this
3. **Open wheel positions clean?** — DTE countdown, any earnings within 21 days?
4. **LEAP scorer calibrated?** — Did any CALL/PUT signals fire? Was VIXY elevated?
5. **CLM/CRF at or below fair value?** — CLM $7.51 | CRF $7.28; accumulate if at or below
6. **Is October approaching?** — NAV lock month; review premium and position size

### Deployment Checklist (pending on PA)
1. `git pull origin main` on PythonAnywhere
2. Restart `monitor.py` always-on task (picks up distribution reset signals + carry spread fix)
3. Restart `tqqq.py` always-on task (picks up VIXY gate + seasonal calendar)
4. Add `PORTFOLIO_VALUE_APPROX=<your_value>` to `.env` on PA (required for Kelly sizing + personal scorecard)
5. Add `personal_scorecard` to PA cron: `0 4 * * 0 python scheduler.py --mode personal_scorecard` (Sundays 04:00 UTC = 18:00 HST)
6. Run `python seed_cef_premiums.py` once if not already done — seeds CLM/CRF z-score mu/sigma
7. Add `market_analysis.py` as 6th always-on task if not already done

### Data Infrastructure
- [ ] **IVR tracker maturation** — accumulating daily since Jul 11 2026; usable baseline ~Aug 11, full 52-week rank after 252 trading days
- [ ] **GEX re-enable** — wire `calculate_gex_profile()` back in once Tradier OI is confirmed stable; gamma flip = early CEF premium compression warning

### Scripts Still to Build
- [ ] `crypto.py` — dedicated BTC/ETH channel script (currently served by scheduler.py `--mode crypto_social`)
- [ ] `announcements.py` — weekly accuracy scorecard, prediction vs actual grader
- [ ] `/CL` `/GC` deep-dive/breakout module — futures channel board-only for commodities; ES/NQ have full profile

### Options & Automation
- [ ] **SPY put insurance implementation** — log puts via `tqqq.py --log-put`; strike distance tied to live margin utilization ratio; re-evaluate at ~$100K portfolio stage
- [ ] TQQQ insurance leg: automate "put pays out → buy TQQQ at discount → sell CCs on it" (only 14 DTE renewal clock exists now)
- [ ] Wheel position entry still manual-only (`scheduler.py --mode wheel_position`) — no brokerage API

### Monetization
- [ ] Accuracy scorecard backend — log predictions, grade outcomes, publish to #announcements
- [ ] Subscriber tier gating — lock premium channels, route free tier to #announcements only

---

## 14. Honest Gap Analysis & Ideas to Harden the System

### What's Working Well (Keep and Protect)
- CLM/CRF N-2 EDGAR watcher is genuinely rare — no retail tool does this automatically
- DRIP at NAV + RO dodge is a real structural edge most CLM/CRF holders don't execute
- Bidirectional LEAP desk with a composite cycle scorer > any single-indicator approach
- Binance smart money divergence as a LEAP cross-signal is institutional-grade thinking
- Yield curve + HY spread in the futures board gives macro context most Discord servers skip

### Gaps in the Current System

**Gap 1 — ✅ CLOSED** market_analysis.py built and live (✅ Jul 12)

**Gap 2 — ✅ CLOSED** Earnings proximity on open wheel positions live (✅ Jul 19)

**Gap 3 — ✅ CLOSED** Kelly half-Kelly position sizer live in wheel candidates (✅ Jul 19)

**Gap 4 — CLM/CRF premium z-score baseline needs more history**
The z-score compares current premium to a rolling mean/sigma stored in DB. If the DB
is relatively new, the baseline may not reflect the full historical premium range
(CLM/CRF trade anywhere from -5% discount to +40% premium across market cycles).
Consider seeding the DB with historical NAV/price data from CEFConnect or SEC filings
to give the z-score a proper multi-year anchor.

**Gap 5 — No volatility regime filter on wheel entries**
High VIX = high IV = high premium = good for selling. But if VIX is elevated because
of a genuine macro breakdown, assignment risk spikes. The wheel scanner should
cross-reference the VIX regime from `classify_vix_regime()` and either:
  - Reduce delta to 0.15 in ELEVATED/CRITICAL VIX (less probability of assignment)
  - Flag it explicitly in the signal output so the human can decide

**Gap 6 — ✅ CLOSED** Tier 3 crypto exit Pushover live: ct_score ≥ 80 AND BTC dom < 40% AND Extreme Greed streak ≥ 3d → weekly-deduped Pushover alert (✅ Jul 19)

### Ideas to Explore (Hardening, Not Scope Creep)

**Idea 1 — Dividend reinvestment timing optimizer**
CLM/CRF ex-div falls mid-month. NAV-based DRIP shares are issued at NAV, not market
price, but the market price often dips slightly on ex-div day. Tracking the exact
ex-div date and comparing the premium compression pattern around it could reveal
a consistent 1–3 day accumulation window before the price recovers. monitor.py
already has the ex-div window heuristic — refine it with actual historical data.

**Idea 2 — Margin rate vs dividend yield spread alert**
When the Fed raises rates, E*TRADE margin rate rises. If margin rate ever approaches
blended Tier 2 yield (~13–15%), the positive carry disappears. Add a live spread
monitor: `(blended_tier2_yield - margin_rate)` → alert to #market-analysis if spread
drops below 5%. Data: FRED FEDFUNDS (already fetched) + live Tier 2 prices.

**Idea 3 — LEAP desk seasonal calendar**
The LEAP CALL desk already has March/September seasonal rules. Extend this to a full
12-month seasonal calendar based on QQQ/TQQQ historical drawdown/rally patterns:
Jan (post-tax selling recovery), Apr-May (sell in May watch), Aug (summer chop),
Oct (historically the best LEAP CALL entry month of the year). Bakes the seasonal
edge into the cycle scorer as a calendar-weight modifier.

**Idea 4 — Correlation monitor: CLM/CRF premium vs VIX**
Historical data shows CLM/CRF premium compresses during VIX spikes. Quantify this
relationship: when VIX rises X%, premium historically drops Y%. This gives a
predicted premium level during a market shock, which informs whether to hold through
or dodge early. Buildable from existing time series data in DB + FRED VIX history.

**Idea 5 — Weekly premium harvest scorecard (personal)**
A private (non-Discord) weekly summary: total wheel premium collected vs target,
CLM/CRF DRIP shares added this month, margin utilization trend, carry spread.
Feeds the accuracy scorecard and gives a clear picture of whether the snowball
is accelerating or stalling. Currently tracked manually in Simplifi — automate it.

### Competitive Assessment (If Going Public Eventually)

**Strengths vs existing Discord finance servers:**

| What you have | Why it's rare |
|---------------|---------------|
| Automated N-2 EDGAR watcher for CLM/CRF | No other retail bot does this |
| NAV-based DRIP optimization + RO dodge | Unique strategy, zero competitors |
| Bidirectional LEAP cycle scorer (composite 8-signal) | Most servers just say "buy the dip" |
| Binance smart money L/S divergence cross-signal | Institutional signal, retail price |
| Live HY spread + yield curve in futures board | Most servers ignore macro entirely |
| Twelve Data commercial license | Legal edge vs scrapers |

**What you'll need before going public:**
1. `market_analysis.py` built and polished — the morning brief is the flagship product
2. `announcements.py` accuracy scorecard running for at least 60 days with real predictions
3. Subscriber gating implemented — free tier must see enough to want more, not everything
4. A clear track record: "our LEAP CALL desk fired on [date], TQQQ was at $X, now $Y"
5. CLM/CRF RO dodge documentation — this is the hook that no other server offers

**Honest competitive reality:**
The system is differentiated, not just technically but strategically. The CLM/CRF
focus + margin arbitrage + LEAP desk combination targets a specific underserved audience:
W2 employees who want to build wealth systematically without day-trading. That niche
exists and has money. The weak point right now is that the analysis is siloed across
channels — `market_analysis.py` is the glue that makes it feel like one coherent
intelligence system rather than five separate bots. Build that first.

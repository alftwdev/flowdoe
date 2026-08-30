# Cashflow ZZZ Machine — Project Context
*Master brief for Claude Code sessions. Update as ecosystem evolves.*
*Last updated: Aug 24 2026 — distribution constants corrected (press release Aug 17); 2027 preview constants added; 52-week low detector wired; both tickers hit 52w lows today (CLM $6.69 / CRF $6.44); active RO; Oct NAV lock is key catalyst; CLM RO formula corrected to 104% NAV (was 112%)*

---

## 0-A. Deployment Runbook (PA — after every git pull)

### Step 1 — Pull on PythonAnywhere
```bash
cd ~/scripts && git pull origin main
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
python db_tools.py --seed-premiums   # seeds CLM/CRF z-score mu/sigma (one-time)
```

---

## 0-B. Known Constants — Lock These, Never Guess

### CLM/CRF Distribution (2026 confirmed — corrected Aug 24 2026)
```python
# 2026 annual distributions — CONFIRMED by Aug 17, 2026 Cornerstone press release (Q4 declaration)
# Previous CLAUDE.md values ($0.1189 CLM / $0.1152 CRF) were WRONG — based on incorrect Oct 2025 NAV assumption.
CLM_ANNUAL_DIST = 1.458    # $0.1215/month × 12  (was incorrectly 1.4268 / $0.1189/mo)
CRF_ANNUAL_DIST = 1.4112   # $0.1176/month × 12  (was incorrectly 1.3824 / $0.1152/mo)

# Fair value floor (annual_dist / 0.19 = FV at 19% yield target) — CORRECTED Aug 24 2026
CLM_FAIR_VALUE  = 7.67     # $1.458 / 0.19  (was 7.51 — understated by $0.16)
CRF_FAIR_VALUE  = 7.43     # $1.4112 / 0.19 (was 7.28 — understated by $0.15)

# 2027 distribution PREVIEW (Board confirmed 21% continues; actual locked end of October 2026)
# Example based on July 31, 2026 NAV. Higher Oct NAV → higher 2027 dist; lower Oct NAV → lower.
# Market is pricing CLM/CRF toward these 2027 example FV levels (both hit 52w lows Aug 24 2026).
CLM_DIST_2027_EXAMPLE = 1.3236   # $0.1103/month × 12 — if Oct NAV stays near July NAV (~$6.94)
CRF_DIST_2027_EXAMPLE = 1.2816   # $0.1068/month × 12 — if Oct NAV stays near July NAV (~$6.72)
CLM_FV_2027_EXAMPLE   = 6.97     # CLM_DIST_2027_EXAMPLE / 0.19 — market pricing toward this
CRF_FV_2027_EXAMPLE   = 6.74     # CRF_DIST_2027_EXAMPLE / 0.19 — market pricing toward this

# NAV fallbacks (refresh whenever CEFConnect NAV changes >0.10)
CLM_NAV_FALLBACK = 6.31   # updated Aug 25 2026 per CEFConnect (NAV as of Aug 21 2026)
                          # was 6.73 (Aug 16, per N-2 filing) — dropped $0.42 as portfolio fell
CRF_NAV_FALLBACK = 6.12   # updated Aug 25 2026 per CEFConnect (NAV as of Aug 21 2026)
                          # was 6.18 (Jul 23, implied math) — minor update

# Margin rate (E*TRADE)
MARGIN_RATE = 7.25

# RO subscription price formula (can change each cycle — verify against N-2 filing)
# 2026 (CLM + CRF): 104% × NAV only — no market price floor (most aggressive formula ever)
# Prior cycles CLM: max(107–112% × NAV, 65–90% × market price)
# Prior cycles CRF: 104% × NAV (consistent)
# Rule: update monitor.py formula after reading the actual N-2 filing each cycle
CLM_RO_FORMULA_2026 = 1.04   # 104% of NAV — confirmed from user's RO notes Aug 24 2026
CRF_RO_FORMULA_2026 = 1.04   # 104% of NAV (unchanged from prior cycles)
```

**Rule: Every script that calculates CLM/CRF yield or Div. Yield MUST use these constants.**
Forbidden values (DO NOT USE — all inflate or misrepresent distributions):
- Pre-2026 CLM: 1.4688 ($0.1224/mo), 1.4580 — dead
- Old wrong 2026 CLM: 1.4268 ($0.1189/mo) — CORRECTED to 1.458 on Aug 24 2026
- Old wrong 2026 CRF: 1.3824 ($0.1152/mo) — CORRECTED to 1.4112 on Aug 24 2026
- 1.3984 — never a valid value

NOTE: 1.4112 ($0.1176/mo × 12) is the CORRECT 2026 CRF annual distribution.
The prior prohibition "Never use 1.4112" in CLAUDE.md was wrong — it confused the
correct 2026 CRF rate with the pre-2026 rate. 1.4112 IS the right value for CRF.

### Bug fixed Jul 23 2026 — monitor.py distribution mismatch
`get_ticker_report()` was using `1.4580 / 1.4112` (pre-reset) for `y_dist`
(Div. Yield in embed footer) while `check_distribution_yield_floor()` used different values.
**Fix applied Jul 23:** both paths aligned. **Further corrected Aug 24:** both paths updated
to confirmed 2026 rates `1.458 / 1.4112` per Aug 17 2026 Cornerstone press release.

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
- market_analysis.py morning brief: `03:10 HST = 13:10 UTC` (shifted Aug 2026 — now 5h before monitor pulse, no overlap risk)
- scheduler.py `--mode morning` (DB writes + conviction sync): `02:50 HST = 12:50 UTC` — fires first so SPY/QQQ POC/VAH/VAL are in DB before market_analysis.py reads them at 13:10 UTC
- The original 10-minute stagger concern is resolved. Never move market_analysis.py morning back to 18:xx UTC — it caused duplicate conviction labels in the same channel as monitor.py.

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
stream.py   → DB: vixy_price_realtime (WebSocket, real-time raw price only)
                   note: VIXY z-score is NOT written by stream.py — monitor.py
                   computes it independently via 20-bar Twelve Data REST fetch.
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
- **Combined leverage (margin + box spread balloons) never exceeds 25% of portfolio value**
  E*TRADE margin balance + all outstanding box balloon amounts = one number. 25% cap applies to both combined, not each separately.
- Internal red line: portfolio drops 15% → stop new margin draws AND do not open new boxes
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
    "TSLA", "COIN", "SOFI", "PLTR", "HIMS", "PENG",            # GROWTH/HIGH-IV
    "SPY", "QQQ", "IWM", "GLD", "XLE",                        # SECTOR
]
```

**Entry filter (scheduler.py --mode wheel_signals):**
```
IVR > 35% | IV − HV30 ≥ 5pp (VRP gate) | bid-ask spread < 10% of mid | no earnings within 45 days
Premium ≥ 1% of strike | delta ~0.20 | 30–45 DTE
```
**VRP gate (added Aug 11 2026):** IV must exceed HV30 by ≥5 volatility points to confirm the premium edge is structural, not a historical relic of a past spike that has since normalized. Source: triple-confirmation filter validated across ORATS, QuantPedia, Schwab research. Implemented in `analytics.py` `generate_tier2_iv_rank_alerts()`. A 5-year wheel backtest (2015–2025) without this filter produced CAGR ~1% gross — the filter is essential, not optional.

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

### Strategy 4 — SPX Box Spread Borrowing (Structural Low-Rate Loan)

**Status:** Intelligence layer built (Jul 23 2026). Execution DEFERRED until portfolio reaches ~$100k.
At $43k the benefit (~$239/yr interest savings) is too small relative to the execution complexity
and combined leverage headroom consumed. Revisit when portfolio ≥ $100k.
Phase D gates still apply when ready — see bottom of this section.

**The thesis:** A short SPX box spread is a synthetic fixed-rate loan at ~Treasury + 30–50bps
(~4.75% today vs 7.25% E*TRADE margin). Borrow at ~4.75%, deploy into CLM/CRF at 19% yield
= net positive carry of ~14.25% on borrowed capital. The box is OCC-guaranteed, European-style
(no early assignment), and the interest cost is a §1256 capital loss at Dec 31 — offsets
TQQQ LEAP profits at tax time. No monthly payments. Balloon due at expiry.

**Why SPX only — CLM/CRF box spreads are impossible:**
- CLM/CRF have no listed options market (CEF too small)
- Even if they did, American-style options + no institutional arbitrageurs = put-call parity
  not enforced = rates far above Treasury
- Box spreads only work where European-style options + deep institutional OI enforce parity
- Mechanism: borrow via SPX box → cash in account → use cash to buy CLM/CRF

**The 4-leg structure (short box = receive credit today):**
```
Short K1 Call + Long K2 Call + Short K2 Put + Long K1 Put
  K1 = lower strike | K2 = upper strike | width = K2 − K1
  Credit received today ≈ width × 100 × (1 − rate × DTE/365)
  At expiry: owe exactly width × 100 (the "balloon payment")

Example — 100pt box, 357 DTE, SPX ~7,408:
  Strikes: K1=7400, K2=7500
  Credit received: ~$9,555 (mid-price execution)
  Balloon at expiry: $10,000
  Implied rate: ~4.75%
  Annual interest cost: ~$445
  Margin held by E*TRADE: ~$445 (small — OCC guarantees settlement)
```

**CRITICAL — mid-price execution only:**
Use a combo order (all 4 legs simultaneously). The mid-price rate is ~4.75%.
Legging in manually uses bid/ask on each leg — the bid/ask drag alone (~5.75pt)
pushes the effective rate to 11–19%. NEVER leg into a box spread.

**Balloon payment — how it actually works:**
```
At expiry, OCC cash-settles all 4 legs automatically.
Net settlement = exactly the strike width × $100 per contract.
For a 100pt box: E*TRADE debits $10,000 from your account.
You do NOT need to "save up" $10,000 — the ROLL is the default move.

ROLL PROCESS (30 DTE Pushover alert fires from monitor.py):
  Step 1: Run box_spread_scan to check current available rate
  Step 2: Open new box (same or larger width, 1-year DTE) → receive new credit (~$9,555+)
  Step 3: New credit lands in account BEFORE old box expires
  Step 4: At expiry: OCC debits $10,000 | New credit already in account covers $9,555
  Step 5: Net cash outlay for the roll: ~$445 (the annual interest spread — and nothing else)

MLPI + MAIN dividends (~$1,300–1,500/year on active holdings) cover the $445 interest
cost before the balloon even arrives. The box is effectively self-funding from Tier 2 income.
```

**Combined leverage cap — 25% of portfolio, COMBINED margin + box:**
```
Portfolio value: $43,000 → 25% cap = $10,750 total leverage budget

With 1 active box ($9,555 credit):
  Box obligation: $9,555 → uses $9,555 of the $10,750 budget
  Remaining margin headroom: $10,750 − $9,555 = $1,195
  (margin stays minimal until portfolio grows)

At $65,000 portfolio → 25% cap = $16,250:
  Box obligation: $9,555
  Remaining for E*TRADE margin: $6,695 → comfortable
  → Now box + conservative margin work in parallel

RULE: (E*TRADE margin balance + sum of all box balloon obligations) ≤ 25% × portfolio value
Never compute them separately. They are one combined leverage number.
```

**Scaling path — from 1 box to a ladder:**
```
Month 0:    1× 100pt box (Dec 2027) → $9,555 → buy CLM/CRF → DRIP
Month 6:    Portfolio grown → open 2nd box (Jun 2028) → $9,555+ → buy more CLM/CRF
Dec 2027:   Roll Box 1 → Dec 2028 | Jun 2028: Roll Box 2 → Jun 2029
Result:     Staggered 6-month ladder → balloon payments never coincide
            Each new box opened at a larger portfolio = naturally larger credit
```

**RO dodge with active box spreads:**
```
monitor.py fires N-2 CRITICAL alert:
  → Sell 99% CLM/CRF to ≥3 shares (preserves DRIP permanently)
  → ro_dodge_active_{ticker} DB flag set → box pulse shows balloon reminder
  → Box balloon is STILL OWED at expiry — selling CLM/CRF does NOT retire the box
  → Deploy sale proceeds → reduce E*TRADE margin temporarily (no idle cash)
  → Box continues rolling on its own schedule
  → MLPI + MAIN dividends keep flowing → buffer accumulates

Re-entry signals (monitor.py fires one or both):
  Path A: detect_ro_completion_dip() — premium <10% + price ≥10% below 60D high
  Path B: check_yield_floor_reentry() — price ≤ fair value + 45d since N-2 detected
  → ro_dodge_active cleared → re-entry embed dispatched → #cornerstone only
  → Redeploy freed margin + available cash into CLM/CRF rebuy
  → Resume DRIP at NAV — net shares GREATER than before RO
```

**§1256 tax treatment:**
- SPX options are §1256 contracts (60% long-term / 40% short-term capital gains treatment)
- The implied interest cost is recorded as a capital loss at Dec 31 (mark-to-market)
- A ~$445 annual interest cost = ~$445 §1256 capital loss
- This offsets TQQQ LEAP capital gains dollar for dollar
- CLM/CRF DRIP dividends have NO relation to §1256 — separate tax event

**Ecosystem wiring (what was built Jul 23 2026):**
```
tradier_client.py: get_spx_box_rate() — mid-price calc, caches to DB (box_spread_best_rate)
scheduler.py --mode box_spread_scan: fetches daily rate, publishes to #options-wheel
scheduler.py --mode box_position --action open|close|status: logs positions to DB
  → open: stores k1, k2, width, expiration, credit, loan_amount, implied_rate, DTE, interest
  → status: reads all open positions, fires Pushover at 30 DTE (roll alert) and 14 DTE (urgent)
  → close: marks position CLOSED (no matching close trade needed — OCC settles automatically)
market_scheduler.py: box_spread_scan fires 21:15 UTC weekdays (1 Tradier call, 0 TD credits)
                     box_roll_check fires 21:20 UTC weekdays (0 API calls — DB read only)
monitor.py: read_active_box_positions() — reads DB, appended to cornerstone daily pulse
            _format_box_pulse_lines() — DTE countdown + balloon warning ≤60/30 DTE
            N-2 CRITICAL verdict now includes active box balloon context
            check_yield_floor_reentry() — second re-entry path via fair value floor
            Income channel snippet — box efficiency metrics to #dividend-ccetfs (no CLM/CRF data)

Channel routing (locked — never change):
  CLM/CRF + box context + re-entry signals → #cornerstone (WEBHOOK_CORNERSTONE_RO) ONLY
  Box rate scan + daily rate → #options-wheel (WEBHOOK_TRADE_SIGNALS)
  Box efficiency as cost-of-capital metric → #dividend-ccetfs (WEBHOOK_DIVIDEND_CCETFS)
```

**Phase D — execution gates (DO NOT execute until all confirmed):**
1. E*TRADE options level 3 approved (call E*TRADE — required for combo orders)
2. SPX combo order support confirmed on account
3. Confirm OCC margin requirement for your account type (usually just the net debit)
4. Run `python scheduler.py --mode box_spread_scan` to verify Tradier chain pulls correctly
5. Practice with paper: verify the 4 legs at mid-prices sum to the expected credit

---

## 0-G. CLM/CRF RO Historical Patterns (Institutional Memory)

*Updated Aug 25, 2026. Add a new subsection for each completed RO cycle.*

---

### General RO Anatomy (applies to every cycle)

```
Phase 1 — N-2 Filed:
  First public signal. monitor.py EDGAR watcher fires CRITICAL.
  ro_dodge_active set. Execute sell (keep ≥3 shares to preserve DRIP permanently).
  Sub price is UNKNOWN at this point — it is 104%×NAV at expiration close.

Phase 2 — SEC Review (~20 business days):
  Market digests the filing. RO arbitrageurs and institutional holders reduce positions.
  Price typically drifts lower during this window.

Phase 3 — N-2/A Effectiveness:
  SEC declares registration effective. Pricing window opens.
  424B3 filing imminent (sets record date + start of 25-day subscription window).

Phase 4 — 424B3 / Record Date:
  Sub price formula applied to NAV at record date close.
  Open-market buyers who got in below this price beat RO participants.
  HISTORICAL PATTERN: Record date has been the cycle low in 2025 (see below).

Phase 5 — 25-Day Subscription Window:
  Rights trade on exchange. Price tends to RISE toward or above sub price
  as rights-holders subscribe and new income buyers see the yield opportunity.

Phase 6 — Expiration (~25 days after record date):
  New shares issued at 104%×NAV at expiration close.
  Volume spike as rights expire. Post-expiration = RO overhang clears.

Phase 7 — Recovery:
  Premium mean-reverts toward historical average over the following 4–8 weeks.
  Resume DRIP at NAV. Re-enter full position if not already done.
```

**Record date timing heuristic:** ~59 days post-N-2 (based on 2025 data).
**Expiration timing heuristic:** ~84 days post-N-2 (record date + 25 subscription days).

---

### 2025 RO — Forensic Data

```
Ticker: CLM (CRF runs identical concurrent cycle)
Formula: CLM = 112% × NAV | CRF = 104% × NAV (CLM more aggressive in 2025)

N-2 filed:               ~Mar 2025
Record date:              Apr 21, 2025
CLM price at record date: ~$6.92  ← CYCLE LOW (bottom was HERE, not at expiration)
Sub price (112% × ~$5.90 NAV): ~$6.61

Price ROSE during 25-day subscription window:
  Apr 21 (record date):   ~$6.92 (low)
  May 15 (ex-div day):    ~$7.10 intraday low
  May 16 (expiration):    ~$7.32 (higher than record date)
  Jun 5 (1 month post):   ~$7.88 (full mean reversion underway)

Key lesson: Open-market buyers at $6.92 (record date) beat RO subscribers ($6.61)
on a short-term basis — the spread was $0.31, and price recovered $0.96 by June 5.
The 25-day subscription window saw price APPRECIATION, not continuation of selling.
```

---

### 2026 RO — Live Data (Updated Aug 25, 2026)

```
Ticker: CLM + CRF (concurrent cycle, both active)
Formula: 104% × NAV at expiration close (BOTH tickers — most aggressive formula ever;
         prior CLM cycles used market price floor: max(107–112%×NAV, 65–90%×market))

N-2 filed:               Aug 14, 2026 | CLM $7.35
Aug 17 capitulation:     8.62M CLM vol (4.6× avg), Cornerstone press release day.
                         CLM $7.35 → $6.94 in one session (–2027 dist announcement)
Aug 25 (Day 11):         CLM $6.74 (52w low $6.65 intraday), CRF $6.48 (52w low $6.44)
NAV as of Aug 21:        CLM $6.31 | CRF $6.12  (from CEFConnect — refreshed Aug 25 2026)
Current premiums:        CLM 6.81% | CRF 5.56%  (52w avg: 19.60% / 18.46%)
Sub price estimate:      CLM ~$6.56 | CRF ~$6.37  (104% × current NAV; shifts with Oct NAV)
Estimated record date:   ~Oct 12, 2026  (59 days post-N-2 heuristic)
Estimated expiration:    ~Nov 6, 2026
Sept 15 ex-div:          CLM –$0.1215 | CRF –$0.1176 (mechanical drop; CRF falls to sub-price)
Oct NAV lock:            End of October 2026 — Board sets 2027 distribution rate
2027 FV estimate:        CLM ~$6.97 | CRF ~$6.76  (21% of Oct NAV ÷ 19% yield target)

WHY 2026 IS FRONT-LOADED (different from 2025):
  In 2025, CLM traded at ~17% premium when N-2 was filed — the market had room to
  compress premium gradually toward sub price over the 84-day window.
  In 2026, the Aug 17 Cornerstone press release revealed the 2027 distribution preview
  ($0.1103/mo vs $0.1215/mo current). The market repriced to 2027 FV (~$6.97) within
  3 days of the N-2. By Day 11, CLM is BELOW 2027 FV. The sell-off happened
  before the record date, not during the subscription window.
  → The 2025 "record date = bottom" pattern may NOT hold in 2026.
  → The 2026 bottom may have already occurred (Aug 25 52w lows), or be forming now.
  → September seasonality + ex-div Sept 15 add additional downward pressure.

Tiered re-entry zones established Aug 25, 2026:
  Tier 1 (Now, Aug 25–Sept 14):          CLM $6.65–$6.80 | CRF $6.40–$6.55
  Tier 2 (Post-ex-div, Sept 15–19):      CLM $6.50–$6.65 | CRF $6.25–$6.40
  Tier 3 (Record date, ~Oct 8–16):       CLM $6.30–$6.55 | CRF $6.00–$6.25
  Tier 4 (Peak fear, scenario only):     CLM $6.00–$6.30 | CRF $5.75–$6.00

UPDATE THIS BLOCK when the 424B3 is filed (actual record date + sub price),
when expiration occurs, and when the post-expiration recovery level is known.
```

---

### Universal RO Re-entry Signal Rules

```python
# The open-market buyer beats RO participants when:
open_market_price <= sub_price  # buying below 104%×NAV = definitively cheaper than rights

# Below these levels = premium at/near historical lows — near-certain mean reversion:
CLM_PREMIUM_LOW_HISTORICAL = 6.08   # 52w low premium (percent) as of Aug 2026
CRF_PREMIUM_LOW_HISTORICAL = 4.66   # 52w low premium (percent) as of Aug 2026

# If price is near/at 52w low premium AND below 2027 FV → highest-conviction accumulate zone
# Recovery driver: premium mean-reversion from 6% back toward 19% avg = structural tailwind
# Independent of the RO outcome — income buyers see >21% yield and return.

# September ex-div window (both tickers, typically Sept 15):
# Mechanical –$0.12 drop creates 1–3 day accumulation window before income buyers re-enter.
# CRF post-ex-div typically lands at or below estimated sub price — best open-market entry.
```

---

### RO Cycle Comparison Table (add rows for each future cycle)

| Cycle | Formula | N-2 Price | Record Date | Low Price | Low Date | Sub Price | Post-Exp 1mo |
|-------|---------|-----------|-------------|-----------|----------|-----------|--------------|
| 2025 CLM | 112%×NAV | ~$7.35 | Apr 21 | ~$6.92 | Record date | ~$6.61 | ~$7.88 |
| 2026 CLM | 104%×NAV | $7.35 | ~Oct 12 est. | $6.65 intra | Aug 25 | ~$6.56 est. | TBD |
| 2026 CRF | 104%×NAV | ~$7.12 | ~Oct 12 est. | $6.44 intra | Aug 25 | ~$6.37 est. | TBD |

*Update TBD fields after 424B3 filing and post-expiration settlement.*

---

## 0-F. Weekly Maintenance Protocol (Weekend Audit)

This section is the institutional-grade maintenance playbook. Run it every weekend — ideally Saturday after close. It catches signal drift, stale data, DB rot, and script regressions before they affect Monday's trades.

### The Paste-Ready Weekend Prompt

Copy and paste this at the start of every weekend Claude Code session:

```
Weekend maintenance sweep. Work through each checkpoint in order and report findings + recommended actions.

1. DB HEALTH — Run python db_tools.py first (daily maintenance + prune). Then check these critical keys:
   vixy_price_realtime · clm/crf_last_price · clm/crf_last_nav · clm/crf_last_z_premium
   market_analysis_bias (flag if date > 2 days ago) · tqqq_bottom_score · tqqq_top_score
   hy_spread_cached · vix_126d_history · fred_yield_spread · vix_term_slope · tqqq_breadth_cache
   box_spread_best_rate · iv_daily row count · signal_ledger PENDING count
   dark_pool_session_hist_CLM/CRF · ro_dodge_active_CLM/CRF · ro_n2_detected_CLM/CRF
   Flag any key that is None when it should have a value. Flag market_analysis_bias if stale.

2. STRATEGY STATUS — CLM/CRF prices vs fair value floors ($7.51 / $7.28). Active RO? Path A/B/C
   re-entry signals fired? Carry spread ≥ 5% (Tier 2 blended yield − 7.25% margin rate)?
   Open wheel positions: DTE countdown, any earnings within 21 days? LEAP desk: was VIXY elevated
   this week? Did any CALL/PUT signal fire?

3. SCRIPT HEALTH — Verify all 5 PA always-on tasks are healthy:
   market_scheduler.py · monitor.py · market_analysis.py · tqqq.py · stream.py
   Check that market_analysis_bias is fresh. Check stream.py is writing vixy_price_realtime.
   Check iv_daily has new rows from this week's 21:30 UTC store_daily_iv cron.

4. SIGNAL AUDIT — Review signal_ledger: any PENDING predictions overdue for grading?
   Review #cornerstone for ELEVATED/CRITICAL events this week. Any dark pool flags fire?
   Any premium compression alerts? Any SEC EDGAR N-2 or SC 13D/G detections?

5. DATA FRESHNESS — fred_macro_snapshot within 7 days? hy_spread_cached populated?
   cef_premium_log has this week's entries? box_spread_best_rate populated?
   vix_126d_history accumulating (critical for Kelly regime detection)?

6. CRYPTO + FUTURES CHECK — vix_term_slope direction (backwardation = risk-off).
   tqqq_vix_backwardation_active: set or clear? tqqq_breadth_cache: above or below 50%?
   BTC fear/greed direction. Any smart money L/S divergence this week?

7. OPEN SOURCE SWEEP — any new arXiv quant-finance preprints relevant to our signals
   (vol forecasting, CEF premium dynamics, PCR predictive power)? CBOE VIX term structure
   updated? Alternative.me F&G API returning cleanly? SEC EDGAR N-2 search for CLM (CIK
   0000814083) and CRF (CIK 0000033934) for any new filings this week?

8. CLAUDE.md SYNC — any new constants, script changes, or learned behaviors from this
   week that are NOT yet reflected in CLAUDE.md? Any stale entries to remove?
   Update 'Last updated' line if anything changes.

Report a concise punch list: ✅ healthy / ⚠️ degraded / ❌ broken for each area. Then list
recommended actions in priority order (critical first).
```

---

### Institutional-Grade Maintenance Checklist (Full Detail)

#### A. DB Health — The Trading Journal

The SQLite DB is the system's trading journal and cross-script state bus. Treat it like a
production database: any None value in a critical key means a script died or a feed is down.

**Critical key checklist (run every weekend):**
```python
python3 -c "
from database import EcosystemDatabase
import json, sqlite3
db = EcosystemDatabase()

keys = [
    'vixy_price_realtime',        # stream.py heartbeat — None = stream.py dead
    'clm_last_price', 'crf_last_price',
    'clm_last_nav', 'crf_last_nav',
    'clm_last_z_premium', 'crf_last_z_premium',
    'market_analysis_bias',        # stale if date > 2 days = market_analysis.py dead
    'tqqq_bottom_score', 'tqqq_top_score',
    'hy_spread_cached',            # None = FRED unreachable (falls back to last cached)
    'vix_126d_history',            # None = Kelly sizer using defaults — investigate
    'fred_yield_spread',
    'vix_term_slope',              # computed by tqqq.py
    'tqqq_breadth_cache',
    'box_spread_best_rate',
    'ro_dodge_active_CLM', 'ro_dodge_active_CRF',
    'dark_pool_session_hist_CLM', 'dark_pool_session_hist_CRF',
]
for k in keys:
    v = db.get_state(k)
    ok = v is not None and v != '' and v != 'None'
    tag = '✅' if ok else '❌'
    display = str(v)[:70] if v else 'None'
    print(f'{tag}  {k}: {display}')

# market_analysis_bias staleness
bias = db.get_state('market_analysis_bias')
if bias:
    b = json.loads(bias) if isinstance(bias, str) else bias
    print(f'    bias date: {b.get(\"date\")} label: {b.get(\"label\")} score: {b.get(\"score\")}')

# IV accumulation count
conn = sqlite3.connect(db.db_path)
c = conn.cursor()
c.execute('SELECT COUNT(*) FROM iv_daily'); print(f'iv_daily rows: {c.fetchone()[0]}')
c.execute(\"SELECT COUNT(*) FROM signal_ledger WHERE outcome='PENDING'\"); print(f'PENDING signals: {c.fetchone()[0]}')
c.execute('SELECT COUNT(*) FROM cef_premium_log'); print(f'cef_premium_log rows: {c.fetchone()[0]}')
conn.close()
"
```

**What each None means:**
| Key | None means | Fix |
|-----|-----------|-----|
| `vixy_price_realtime` | stream.py dead | Restart stream.py on PA |
| `market_analysis_bias` stale | market_analysis.py dead | Restart on PA |
| `hy_spread_cached` | FRED unreachable | Will self-heal; check FRED_API_KEY |
| `vix_126d_history` | tqqq.py hasn't written yet | Run tqqq.py manually once |
| `tqqq_bottom_score` | tqqq.py dead or low signal | Normal if market calm |
| `box_spread_best_rate` | Tradier API issue or market closed | Ignore on weekend |

**DB maintenance commands:**
```bash
python db_tools.py                 # daily maintenance + prune (safe to run anytime)
python db_tools.py --purge-stale   # one-time: drop dead tables, grade overdue PENDING
python db_tools.py --seed-premiums # one-time: rebuild CLM/CRF z-score baseline
python db_tools.py --rescue        # emergency: DB corruption recovery
```

**signal_ledger grading rule:**
`clm_floor` PENDING signals grade as WIN if price stayed ≥ fair value by target_date, LOSS if not.
`db_tools.py --purge-stale` auto-grades any PENDING signal where target_date has passed.
Run it whenever PENDING count > 3.

#### B. Script Health — Process Verification

```bash
# On PA — check all 5 always-on tasks are alive
ps aux | grep -E "monitor|market_scheduler|market_analysis|tqqq|stream" | grep -v grep

# Check PA logs for silent errors (last 50 lines per script)
tail -50 ~/scripts/logs/market_analysis.log
tail -50 ~/scripts/logs/monitor.log
tail -50 ~/scripts/logs/tqqq.log
```

**Freshness gates:**
- `market_analysis_bias` date must be within 2 days (fires daily at 13:10 UTC)
- `vixy_price_realtime` must be non-None during market hours (stream.py writes on tick)
- `iv_daily` must have new rows this week (cron at 21:30 UTC weekdays)
- `cef_premium_log` must have this week's date (monitor.py writes on pulse)

#### C. Signal & Strategy Audit

**Weekly signal review:**
1. Any ELEVATED or CRITICAL events in #cornerstone this week?
2. Did dark_pool_session_hist_{ticker} ever hit [1,1,x] pattern (2-of-3 trigger)?
3. Did any premium compression alert fire (≤ -3% intra-session)?
4. Did SEC EDGAR watcher catch any new N-2 or SC 13D/G filings?
5. Were any TQQQ LEAP CALL or PUT entries triggered? Were they in the 2-hour cooldown?
6. Did the intraday VIX resolution bonus fire (VIXY/VXZ crossing back below 1.0)?

**CLM/CRF position math (run each week):**
```
Fair value check:
  CLM: annual_dist / 0.19 = $1.4268 / 0.19 = $7.51 (accumulate at or below)
  CRF: annual_dist / 0.19 = $1.3824 / 0.19 = $7.28 (accumulate at or below)

Active RO check:
  ro_dodge_active_CLM/CRF set? → RO in progress; Paths A/B/C active
  If set + price ≤ NAV ($6.73 CLM / $6.18 CRF) → Path C Tier 1 (NAV entry) should have fired
  If set + price ≤ FV ($7.51/$7.28) → Path C Tier 2 (FV entry) should have fired
  If set + 30+ days since N-2 → Path A active (premium collapse + price off 60D high)
  If set + 45+ days since N-2 → Path B active (yield floor re-entry)
```

#### D. Open Source Data Feeds — Staying Current

These are free, institutionally-tracked data sources to verify against our internal signals:

**Price & Volatility:**
- **CBOE VIX term structure** (free daily download): `cboe.com/us/options/market_statistics/daily/` — VIX9D, VIX3M, VIX6M, VIX1Y. Cross-check `vix_term_slope` in DB.
- **CBOE P/C ratios** (daily): Same CBOE page — equity and index P/C. Cross-check our SPY P/C z-score.
- **FRED VIXCLS** — actual VIX (already integrated): `fred.stlouisfed.org/series/VIXCLS`
- **Alternative.me F&G API** (zero auth): `api.alternative.me/fng/` — crypto fear & greed. Already integrated but worth checking weekly.

**Macro & Credit:**
- **FRED dashboard** (integrated): HY spread `BAMLH0A0HYM2`, yield curve `DGS10-DGS2`, Fed Funds `FEDFUNDS`, CPI `CPIAUCSL`, unemployment `UNRATE`. All cached daily.
- **CME FedWatch tool** (free): `cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html` — probability distribution for next FOMC decision. Good for carry spread context.
- **Treasury yield curve** (free JSON): `home.treasury.gov/resource-center/data-chart-center/interest-rates/` — daily par yield curve rates.

**CEF & Options Research:**
- **CEFConnect** (free): `cefconnect.com` — live NAV, premium/discount history for CLM/CRF. Source for z-score calibration. Cross-check `cef_calibrate` mode output.
- **SEC EDGAR EFTS** (free API): Check for new CLM/CRF filings each week:
  ```
  https://efts.sec.gov/LATEST/search-index?q=%22cornerstone+strategic%22&forms=N-2,N-2/A&dateRange=custom&startdt=YYYY-MM-DD
  ```
- **EDGAR full-text search** for SC 13D/G (large holder changes):
  ```
  https://efts.sec.gov/LATEST/search-index?q=%22CLM%22+%22cornerstone%22&forms=SC+13D,SC+13G&dateRange=custom&startdt=YYYY-MM-DD
  ```
- **OpenInsider** (free scraping): `openinsider.com` — Form 4 insider cluster buys. Cross-check `get_insights()` from SentiSense.

**Crypto:**
- **CoinGlass** (free tier): `coinglass.com/api` — BTC/ETH OI, funding rates, liquidation heatmaps. More granular than Binance FAPI.
- **Binance FAPI** (already integrated, free): OI `fapi.binance.com/fapi/v1/openInterest`, L/S `fapi.binance.com/futures/data/globalLongShortAccountRatio`.
- **Glassnode lite** (free signals): `glassnode.com/bitcoin-fundamentals-report` — on-chain weekly digest. MVRV, NUPL, exchange netflow. Cross-check `btc_mvrv_proxy` DB key.

**Quantitative Research (Signal Hardening):**
- **arXiv quant-finance** (free preprints): `arxiv.org/list/q-fin/recent` — filter for vol forecasting, CEF, options pricing. Run a weekly scan for new papers.
- **SSRN finance** (free preprints): Search for CEF discount dynamics, covered call ETF yield, VIX term structure predictive power.
- **QuantPedia** (free tier): `quantpedia.com/strategies/` — validated backtests. Good for checking if any new research validates/invalidates our VRP gate or Kelly approach.
- **The Journal of Derivatives** (abstracts free): Options pricing, volatility surface research.

**What to do with open source findings:**
- If a new paper changes a weight or threshold (like the P/C z-score weight cut from 15→8pts in Aug 2026) → update the relevant constant and log in CLAUDE.md.
- If a new free data endpoint adds a signal we don't have → evaluate against the 3-signal anti-bloat rule (must replace or meaningfully enhance an existing signal, not just add noise).
- If CBOE VIX term data contradicts `vix_term_slope` in DB → investigate tqqq.py VIXY/VXZ ratio calculation.

#### E. Futures & Crypto Channel Integrity

**Futures (`cross_asset.py` → #futures-trading):**
- Fires 4× daily: 07:00, 12:35, 14:00, 18:45 UTC (change-gated, not always a new embed)
- Check: `/ES` and `/NQ` levels populated, yield curve (T10−T2) fresh, IB breakout scanner running
- DB dependency: `fred_yield_spread` (should be non-None), `gex_profile_SPY` (informational)
- Cross-check: `vix_term_slope` from tqqq.py matches CBOE VIX term structure direction

**Crypto (`scheduler.py --mode crypto_social` → #crypto):**
- Fires weekdays at 13:50 UTC
- Check: Binance FAPI returning real OI + L/S ratios (not zeros — PA egress may block Binance FAPI occasionally)
- Check: SentiSense `get_market_mood()` returning score (not None)
- Check: BTC MVRV proxy in DB (`btc_mvrv_proxy`) fresh within 7 days
- If Binance FAPI returns zeros on PA: check PA network egress rules — Binance `fapi.binance.com` must be reachable

**VIX integrity (`tqqq.py` + `stream.py`):**
- `stream.py` writes raw VIXY price to `vixy_price_realtime`
- `tqqq.py` independently fetches VIXY 20-bar series via TD REST and computes z-score
- `vix_term_slope` = VIXY/VXZ ratio; backwardation (<0) = sustained fear; contango (>0) = calm
- `tqqq_vix_backwardation_active` in DB: set when ratio > 1.0; cleared when ratio drops back < 1.0
- Cross-check: `fred_vixcls` from FRED vs VIXY proxy — if they diverge > 20% → investigate
- `vix_126d_history`: should be accumulating in DB (list of daily VIX values, max 252). If None → tqqq.py never wrote it. Run `python tqqq.py` locally once to seed.

#### F. Code Version & Dependency Hygiene

```bash
# Confirm PA is running the same version as local main
git log --oneline -3           # local HEAD
# On PA: git log --oneline -3 — should match

# Check for outdated packages with known CVEs
pip list --outdated 2>/dev/null | head -20

# Check import health — any script that fails to import has a silent always-on crash
python -c "import monitor, market_analysis, scheduler, tqqq, stream, cross_asset, analytics, database" 2>&1

# Syntax check all scripts (catches the class of error that killed market_analysis.py Aug 23)
python -m py_compile monitor.py market_analysis.py scheduler.py tqqq.py stream.py cross_asset.py analytics.py 2>&1
```

**Version discipline:**
- Every change to a constant in CLAUDE.md `0-B` must also be verified in ALL scripts that use it.
- After any NAV update: check `monitor.py`, `research_bot.py`, `scheduler.py`, `daily_pulse.py` — all four reference CLM/CRF NAV fallbacks.
- After any distribution reset: check `monitor.py` `check_distribution_yield_floor()` AND `get_ticker_report()` — both paths must use the same constant (the Jul 23 2026 dual-path bug).

#### G. What "Institutional Firms" Do That We Replicate

| Institutional practice | Our equivalent |
|------------------------|---------------|
| Daily P&L reconciliation | `daily_pulse.py` SimpleFIN balance vs previous day |
| Model parameter review | Weekly CLAUDE.md review — constants match EDGAR/CEFConnect reality |
| Data vendor health check | API credit count, FRED/TD/Tradier response time check |
| Signal false-positive audit | signal_ledger PENDING grading; alert count vs threshold tuning |
| Position limits enforcement | 25% combined leverage cap; 30% max wheel margin |
| Correlation drift monitoring | `check_macro_correlation()` in monitor.py — CLM/CRF vs SPY |
| Risk model recalibration | `calibrate_cef_premium_zscore()` — weekly CEF z-score mu/sigma update |
| Research pipeline | arXiv/SSRN/QuantPedia sweep; apply validated findings to signal weights |
| Disaster recovery drill | Verify `db_tools.py --rescue` can recover; confirm .env backup |
| Deployment parity check | PA git SHA == local git SHA — no silent divergence |
| Log triage | PA task logs weekly — find silent exceptions before they become outages |

#### H. Known DB State as of Aug 23 2026 (baseline for future audits)

```
market_analysis_bias: STALE (2026-07-15) → resolves on market_analysis.py PA restart after SyntaxError fix
vix_126d_history: None → investigate on PA; tqqq.py must seed this key
iv_daily: 0 rows locally (may have rows on PA from store_daily_iv cron)
signal_ledger: 4 PENDING (clm_floor CLM+CRF × 2 dates: Jul 23, Aug 13)
  → run python db_tools.py --purge-stale to grade any whose target_date has passed
cef_premium_log: 4 entries (Jul 23 + Aug 13 for CLM and CRF)
ro_dodge_active_CLM/CRF: 2026-08-14 (active RO, 9 days elapsed as of this writing)
  → Path C Tier 2 eligible: CRF $7.175 ≤ FV $7.28 → re-entry signal should fire on next monitor tick
CLM at exactly $7.51 = fair value floor (Path C Tier 2 eligible if price ticks below)
global_state: 228 rows — healthy
```

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

**Actual cash flow mechanics (Jul 2026 reality — NOT the aspirational Bill Pay model yet):**
```
W2 paycheck → external checking (bills paid from here)
Simplifi by Quicken → tracks monthly surplus after bills
Monthly surplus → manually deposited into E*TRADE
$500/week auto-deposit → E*TRADE (separate stream)
  ↓
Box spread credit → buy CLM/CRF → DRIP at NAV (never touched)
MLPI + MAIN cash dividends → margin paydown (Tier 2 active only)
MLPI purchased on dips (cash only, not margin) → expands margin capacity
  ↓
Margin freed → reborrow conservatively → more CLM/CRF
```

**Note:** E*TRADE Bill Pay is the TARGET design (bills as business expenses). Not yet live.
Simplifi is the bridge — it surfaces monthly investable surplus for manual deployment.

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
| TDAQ | TappAlpha 0DTE NASDAQ covered call | ~12–17% | Monthly | Future candidate — not currently held |
| KQQQ | Kurv Tech Titans covered call | ~15% | Monthly | Future candidate — not currently held |

**Active Tier 2 (Jul 2026):** MLPI + MAIN only. Blended yield: ~11.5%
**TDAQ / KQQQ:** in CLAUDE.md for future reference, not currently held at this portfolio stage.
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
          Thesis-break exit: at entry, note the next technical resistance above the CSP strike.
          If price closes above that level, exit immediately for ~1/3 max loss — do NOT hold to expiration
          for max loss. A thesis break (price piercing resistance) signals the setup is invalid.
          Taking a controlled early loss preserves capital to re-deploy; holding to max loss is a discipline failure.
          If breached but thesis intact: roll down+out for credit, or take assignment → sell CC
STEP 4 — CC after assignment: ATM/slight OTM, 21–30 DTE
STEP 5 — Capital rule: max 30% of available margin in wheel at any time
```
**Premium income → margin paydown bucket**

**Current data limitation:** IVR and delta are proxy-calculated (HV30-based). With Tradier ($10/mo), these become real options chain values — accurate 0.20 delta strikes, real bid/ask spread check, real OI/volume for liquidity confirmation. **IVR tracker is live** (`scheduler.py --mode store_daily_iv`, daily at 21:30 UTC) — accumulating since Jul 11 2026; usable baseline reached Aug 11 2026; full 52-week rank after 252 trading days.

**Kelly position sizer** (`kelly_position_size()` in `analytics.py`) — updated Aug 2026:
Uses 126-day VIX percentile rank (arXiv:2508.16598, peer-reviewed Aug 2025) instead of the simple `15/VIX` scalar. 126d lookback validated as optimal — shorter windows (21–63d) produce erratic sizing.
```
VIX > 75th pct (126d) → 35% of half-Kelly   (panic regime — drastically reduce)
VIX > 60th pct        → 60% of half-Kelly   (elevated — moderate reduction)
VIX < 25th pct        → 100% of half-Kelly  (historically calm — full deploy)
Neutral               → 85% of half-Kelly
```
History stored in DB key `vix_126d_history` (max 252 entries). Regime is backward-looking — the rank builds accuracy over time.

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
Inputs and weights (bottom_score / top_score are symmetric):
```
VIXY z-score              30pts  — primary fear signal; gate requires actual fear
RSI14                     25pts  — oversold/overbought confirmation
Breadth                   20pts  — % stocks above 50D SMA
52w drawdown              15pts  — distance from 52w high
VIX term structure        12pts  — VIXY/VXZ ratio; backwardation = sustained fear
CNN Fear & Greed          10pts  — retail sentiment extreme
SPY P/C z-score            8pts  — vs 30-day rolling baseline (reduced from 15 Aug 2026;
                                   peer-reviewed research found PCR has weaker predictive
                                   power for index options than single-name — HarbourFront
                                   Quant 2025. Raw ratio is meaningless due to structural
                                   institutional hedging on SPY; only the z-score is used.)
VIX resolution bonus       7pts  — fires when VIXY/VXZ ratio crosses back BELOW 1.0
                                   (backwardation→contango resolution). 17-year backtest:
                                   88% win rate at +5d, 91% at +21d after resolution.
                                   Sustained for 48h. State: tqqq_vix_backwardation_active
                                   + tqqq_vix_resolution_ts in DB. Fires ~2–5×/year.
Below SMA200               5pts  — regime confirmation
MACD                       3pts  — tie-breaker
Insider cluster buy       10pts  — SentiSense Form 4 (NVDA/AAPL/MSFT/META/GOOGL)
CLM/CRF z-score cross      8pts  — dual CEF premium compression = systemic stress signal
```
P/C ratio scored on 30-day rolling z-score only — raw ratio is meaningless alone.
VIX term structure via VIXY/VXZ ETF proxies (VIX9D/VIX3M unavailable at Twelve Data tier).
**Actual VIX (FRED VIXCLS)** also fetched and shown in embed to confirm VIXY proxy reading.

**VIX Resolution Bonus rule (Aug 2026):** The edge is in the RESOLUTION (ratio crossing back below 1.0), not the onset (ratio crossing above 1.0). Buying at onset = 51% win rate (near random). Buying at resolution = 88% win rate. Never conflate these. The bonus fires on the resolution tick only and decays after 48h.

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

**Built and live:** `market_analysis.py` — **3 messages/day** (refactored Aug 2026, down from 10). Writes `market_analysis_bias` to DB for cross-script consumption.
```
03:10 HST (13:10 UTC) — Morning Brief (green/yellow/red embed)
  Sections: Overnight Market Structure (SPY POC/VAH/VAL from DB) · Macro Environment
  (VIX, yield curve 10Y/2Y, HY spread, CPI, unemployment) · Equity Pulse (SPY/QQQ/VIXY/F&G) ·
  Cross-Channel Signals (CLM/CRF z-score + accum status + TQQQ deck + wheel) ·
  Market Intelligence (congressional trades, 30-day recency filter) · Today's Posture + wheel params

07:00 HST (17:00 UTC) — Mid-Session Pulse (lightweight update)
  Bias score update · SPY/QQQ · VIX · VIXY z · F&G

10:20 HST (20:20 UTC) — EOD Recap (via scheduler.py --mode eod, not market_analysis.py)
  Session close · morning call accuracy · tomorrow watch items
```
Bias scorer: 12+ signals (expanded from original 8). **Never add more standalone embeds to #market-analysis** — the channel ran 10 messages/day before the refactor due to scheduler.py macro dispatches and market_intraday overlapping with market_analysis.py output. Those duplicates were purged.

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
    "dark_pool": 18,           # TIERED (Aug 2026): 2-of-3 sessions = full +18pts;
                               #   single session = +8pts only. Rolling 3-session window
                               #   stored in DB as dark_pool_session_hist_{ticker}.
                               #   Multi-session clustering validated by 17-year empirical
                               #   study — single-session detection fires near-randomly.
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
| `db_tools.py` | ✅ Live | Unified DB maintenance utility. Modes: default = daily maintenance (09:39 UTC cron) + auto-prune dated global_state keys older than 45 days; `--rescue` = emergency DB recovery; `--seed-premiums` = one-time CLM/CRF z-score init; `--purge-stale` = one-time cleanup (drops dead tables, removes orphaned keys, grades overdue PENDING signals). |
| `monitor.py` | ✅ Live | Cornerstone CLM/CRF protection engine. Dark pool detection upgraded Aug 2026: tiered scoring (single session = +8pts; 2-of-3 session cluster = full +18pts). Rolling 3-session window in DB (`dark_pool_session_hist_{ticker}`). All other signals unchanged. |
| `database.py` | ✅ Live | EcosystemDatabase — state management |
| `analytics.py` | ✅ Live | HighFidelityAnalyticsEngine — ledger, grading, OHLC, FRED helpers, Binance derivatives. Aug 2026: wheel VRP gate raised 2→5pp; Kelly sizer upgraded to 126-day VIX percentile rank (arXiv:2508.16598). |
| `essentials_tools.py` | ✅ Live | Discord embed senders, chart generators |
| `market_analysis.py` | ✅ Live | Always-on (6th PA slot). 3 messages/day → #market-analysis (refactored Aug 2026). Morning brief: 03:10 HST (13:10 UTC). Mid-session: 07:00 HST (17:00 UTC). EOD: via scheduler.py --mode eod at 20:20 UTC. 12+ flag bias scorer (BULLISH/NEUTRAL/BEARISH). Includes Overnight Market Structure section (SPY POC/VAH/VAL from DB), macro with 10Y/2Y+unemployment, congressional trades filtered to 30 days. |
| `cross_asset.py` | ✅ Live | Futures board (change-gated, 4h heartbeat) + yield curve/Fed Funds from FRED + ES/NQ market profile + CVD + structure + IB breakout scanner |
| `crypto.py` | 🔲 To build | BTC/ETH spot, Fear & Greed, funding rates |
| `scheduler.py` | ✅ Live | Central dispatcher. Active modes: morning/eod/income/iv_crush/post_market/macro/weekly_scorecard/wheel_signals/wheel_position/trending_plays/crypto_social/futures_social/store_daily_iv/cef_calibrate/mlpi_entry/personal_scorecard. Removed: `gex`, `options_flow`, `spx_income` (iron condor — purged Jul 19); `market_intraday` (purged Aug 2026 — market_analysis.py always-on handles it); `macro_pm` (purged Aug 2026 — was duplicating morning brief data). `--mode morning` now only writes DB keys and runs conviction sync — no longer dispatches standalone embeds to #market-analysis. |
| `stream.py` | ✅ Live | WebSocket-only sentry: BTC/USD hourly volatility breach alerts, SPY/QQQ perimeter alerts (RTH only), VIXY real-time price → DB for monitor.py. Subscribes: `BTC/USD,VIXY,SPY,QQQ` (RTH) / `BTC/USD` (off-hours). XAU/USD removed — forex channel deprecated. |
| `tqqq.py` | ✅ Live | Bidirectional LEAP desk (CALL + PUT) + directional sniper + insurance put renewal clock. Aug 2026 upgrades: P/C z-score weight 15→8pts (index PCR weaker than single-name per 2025 research); VIX resolution bonus (+7pts when VIXY/VXZ ratio crosses back below 1.0, sustained 48h; state: `tqqq_vix_backwardation_active` + `tqqq_vix_resolution_ts` in DB). |
| `daily_pulse.py` | ✅ Live | Personal financial snapshot → Pushover ONLY (never Discord). Runs as standalone PA cron at 06:00 UTC. SimpleFIN balance fetch with cache fallback: if SimpleFIN unreachable, shows yesterday's cached data + ⚠️ banner instead of $0.00 zeros. State stored in `.daily_pulse_state.json` (isolated from ecosystem DB — intentional, contains personal financial data). MARKET REGIME section removed Aug 2026. |
| `market_structure.py` | ✅ Live | SMC toolkit — FVGs, liquidity sweeps, equal highs/lows, Supertrend (REST, no SDK threads). |
| `tradier_client.py` | ✅ Live | Tradier options chain helper. Added `get_earnings_proximity()` — Tradier /markets/calendar, FORCE_CLOSE ≤7d / REVIEW ≤21d flags. |
| `seed_cef_premiums.py` | 🗑️ Removed | Merged into db_tools.py (`python db_tools.py --seed-premiums`). |
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
WEBHOOK_FOREX=               # .env key retained; channel deprecated; removed from monitor.py Aug 23 2026
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
- [x] DB key mismatch fix — `personal_scorecard` was reading `clm_premium_z`/`crf_premium_z` but monitor.py writes `clm_last_z_premium`/`crf_last_z_premium`; corrected in scheduler.py (✅ Jul 26)
- [x] Accumulation readiness surfaced in morning brief — market_analysis.py now reads `{ticker}_acc_status`/`{ticker}_acc_detail` from DB and injects into CROSS-CHANNEL SIGNALS block (✅ Jul 26)
- [x] research_bot.py fair-value floors computed dynamically — was hardcoded 7.51/7.28; now `annual_dist / 0.19` so floors auto-track if distribution changes (✅ Jul 26)
- [x] mlpi_entry scheduling gap fixed — mode existed in scheduler.py but had no SCHEDULE entry in market_scheduler.py; added at 17:00 UTC weekdays (✅ Jul 26)
- [x] CRF NAV fallback corrected — daily_pulse.py had stale 6.30; fixed to 6.18 matching monitor.py (✅ Jul 26)
- [x] Bias scorer label fixed — market_analysis.py logged `bias_score/8` but scorer has 9 flags; corrected to /9 (✅ Jul 26)
- [x] VIX / market-analysis sharpening — 4 zero-cost improvements added (✅ Jul 26):
  1. `tqqq.py` now writes `vix_term_slope` to DB after computing it (VIXY/VXZ ratio)
  2. `market_analysis.py` Flag 10: VIX term structure (reads `vix_term_slope` from DB; backwardation = -12pts, deep contango = +8pts)
  3. `market_analysis.py` Flag 11: SPY 50-day SMA regime (above SMA50 = +5/+10, below = -5/-12)
  4. `market_analysis.py` Flag 12: Market breadth (reads `tqqq_breadth_cache` from DB; ≥70% = +8, ≤35% = -10)
  5. VIX day-over-day % change: compares FRED VIXCLS to `fred_vix_prev` DB key; +20%+ DoD = -10pts; -15%+ collapse = +8pts. Bias scorer now 12+ signals.

### Completed in Aug 2026 Sessions ✅
- [x] `#market-analysis` channel refactor — 10 messages/day → 3 messages/day; no more duplicate conviction signals or contradicting bias labels (✅ Aug 11)
- [x] `daily_pulse.py` MARKET REGIME section removed; SimpleFIN cache fallback prevents $0.00 zeros on outage (✅ Aug 11)
- [x] DB hygiene — `db_tools.py --purge-stale`: dropped 3 dead tables, removed 11 orphaned keys; auto-prune of dated `global_state` keys >45 days added to daily maintenance (✅ Aug 11)
- [x] `market_scheduler.py` `market_intraday` + `macro_pm` entries removed (caused duplicate channel embeds); `scheduler.py --mode morning` standalone dispatch to #market-analysis removed (✅ Aug 11)
- [x] `market_analysis.py` morning shifted to 13:10 UTC (was 12:00 UTC) so `scheduler.py --mode morning` (12:50 UTC) writes SPY/QQQ POC/VAH/VAL to DB first; Overnight Market Structure section added; macro section now shows 10Y/2Y rates + unemployment; congressional trades filtered to 30-day recency window (✅ Aug 11)
- [x] IV tracker seeded — `iv_daily` table had 0 rows despite cron being wired; manual `python scheduler.py --mode store_daily_iv` seeded 23 tickers; usable baseline reached Aug 11 2026 (✅ Aug 11)
- [x] **Strategy 3 hardening** — Research backtest sweep (Aug 11): VIX resolution bonus (+7pts on backwardation→contango crossover, 88% win rate at +5d per 17yr backtest); P/C z-score weight reduced 15→8pts (index PCR weaker for index options per HarbourFront Quant 2025). DB keys: `tqqq_vix_backwardation_active`, `tqqq_vix_resolution_ts` (✅ Aug 11)
- [x] **Strategy 2 hardening** — VRP threshold raised 2→5pp in wheel screener (triple-confirmation filter: IVR + IV-HV spread + IV percentile per ORATS/QuantPedia/Schwab 2025); Kelly sizer upgraded to 126-day VIX percentile rank with 4 regime tiers (arXiv:2508.16598, Aug 2025). DB key: `vix_126d_history` (✅ Aug 11)
- [x] **Strategy 1 hardening** — Dark pool multi-session clustering: rolling 3-session window per ticker; single session = +8pts (was always +18); 2-of-3 sessions = full +18pts. Eliminates false positives from routine low-volume days. DB key: `dark_pool_session_hist_{ticker}` (✅ Aug 11)
- [x] Research journal entries — 7 informational findings stored in DB as `research_journal_2026-08-11_*` keys (MLPI validation, CEF discount env, PCR index weaker, VIX backwardation rarity, RO oversubscribe note, CEF half-life, wheel backtest IVR essential) (✅ Aug 11)

### Weekly Audit Cadence (ongoing discipline)
→ Full protocol and paste-ready weekend prompt: **see §0-F**

Quick reference — the 6 mandatory checks:
1. **monitor.py signals** — Review #cornerstone for ELEVATED/CRITICAL events this week
2. **Carry spread ≥ 5%?** — Sunday Pushover (personal_scorecard) surfaces this automatically
3. **Open wheel positions** — DTE countdown, earnings within 21 days?
4. **LEAP scorer** — Any CALL/PUT signal fire? Was VIXY z ≥ +1.5σ? VIX resolution bonus fire?
5. **CLM/CRF vs fair value** — CLM ≤ $7.51 / CRF ≤ $7.28 = accumulate zone (both active Aug 23 2026)
6. **October approaching?** — NAV lock month; heighten all CLM/CRF sensitivity; watch institutional exit detector

### Deployment Checklist (current — Aug 2026)
```bash
cd ~/scripts && git pull origin main
```
Restart in this order (after the Aug 23 2026 commit batch):
1. **`market_analysis.py` first** — critical SyntaxError fix restores morning brief + `market_analysis_bias` DB key
2. `monitor.py` — Path C intra-RO entry zone now live (active RO: CLM/CRF dodge active since Aug 14)
3. `market_scheduler.py` — GEX comment clarified; no functional changes
4. `tqqq.py` — VIX resolution bonus + P/C weight reduction from Aug 11

One-time (if not already done on PA):
```bash
python db_tools.py --seed-premiums   # CLM/CRF z-score mu/sigma
```
Env var (if not set):
```
PORTFOLIO_VALUE_APPROX=<your_value>  # required for Kelly sizing + personal scorecard
```

### Data Infrastructure
- [x] **IVR tracker usable baseline** — reached Aug 11 2026 (~30 days of daily IV stored). Full 52-week rank after 252 trading days (~Apr 2027). `vix_126d_history` also accumulating for Kelly regime detection.
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

**Gap 5 — Partially closed (Aug 2026): VRP ≥5pp filter now gates wheel entries**
The IV-HV spread ≥5pp filter catches the most egregious false positives (names with inflated IVR from historical spikes). The remaining gap: no delta reduction in genuine macro breakdown VIX regimes. The wheel scanner should cross-reference `classify_vix_regime()` and reduce delta to 0.15 in ELEVATED/CRITICAL VIX environments to lower assignment risk when the macro is genuinely stressed (not just high IV).

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
| Bidirectional LEAP cycle scorer (12+ signals, empirically calibrated) | Most servers just say "buy the dip" |
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

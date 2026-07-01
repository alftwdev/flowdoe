# Rockefeller Ecosystem — Improvement Blueprint
*Knowledge bank synthesis + channel-by-channel upgrade plan. Blueprint only — no code changes until approved.*

---

## Knowledge Bank Summary

### Todd Akin Cornerstone E-Guide (CLM CRF)

Key facts extracted that aren't yet fully wired into the ecosystem:

1. **3-share minimum rule** — Selling ALL shares loses CS DRIP status permanently. Must keep ≥3 shares before any RO play. Our current wording says "sell 99%" but never explicitly enforces the 3-share floor.
2. **30% premium threshold is the RO trigger** — Todd explicitly says the RO is "usually" announced when the NAV premium reaches 30%+, and can run as high as 50%. Our RO composite score tracks premium direction but has no hard "30% premium = ELEVATED WARNING" gate.
3. **Company announces completion** — The re-entry signal is *not* a price level — it's when Cornerstone announces "we are done, and it was a success." We currently only watch EDGAR N-2 filings (the launch signal); we have no "RO completion" detection.
4. **Springtime seasonality** — Todd pegs RO to spring (May historically), though it's been as late as October. We have `RO_FILING_SEASON = (2, 15, 4, 15)` (mid-Feb to mid-Apr) — this is slightly early vs Todd's "30% premium at any time."
5. **Seeking Alpha comments + press release page** — Todd watches two places: Cornerstone's own press release page and Seeking Alpha comments. We only scrape EDGAR.
6. **CS DRIP must be called in — not toggled** — Relevant educational context for #announcements content (many subscribers may have wrong DRIP).

### Todd Akin Margin E-Guide

Key facts relevant to the ecosystem:

1. **Maintenance rate discipline** — CLM/CRF are 30% maintenance (vs OXLC at 100%). During corrections, sell HIGH-maintenance positions first to raise equity fastest. Not in any current alert.
2. **Margin cap** — Todd uses 30% normally, 50% absolute max. Our CLAUDE.md says 25% — more conservative than Todd's, which is fine.
3. **SPY puts at 5% and 10% OTM** — Todd's own insurance approach. Aligns with our TQQQ puts (which are already 10% OTM) but SPY puts are cheaper and more appropriate for the dividend portfolio hedge vs TQQQ puts which are for the sniper leg.
4. **Todd's complementary fund list** — USA, BDJ, STK, DIVO, BST, EOS, SCHD alongside CLM/CRF. Some are already in our income channel (DIVO, SCHD). None are in the wheel universe. These could be added as diversifiers.
5. **IBKR doesn't support CS DRIP** — Confirmation that E*TRADE/Schwab/TD are the right brokers. Useful for #announcements education.

### Other Knowledge Bank Files

The `.txt` files in the knowledge bank are largely code dumps (Alpaca wheel bot, Chinese CTP futures framework, Pine Script indicators, ML training code, crypto wallet scanners). The strategic content lives entirely in Todd's PDFs above and in the user's CLAUDE.md.

The Recession Indicators PDF (Philosophical Economics) contains the GTT (Growth-Trend Timing) model — unemployment rate vs 12-month MA as recession filter. This could power a macro regime overlay.

---

## Channel Blueprints

---

### 1. #cornerstone (`monitor.py`)

**Current state:** N-2 EDGAR watcher, premium/RSI/NAV metrics, whale flow, 13D/G, dark pool, premium compression, macro correlation, RO composite score 0–100.

**Gaps identified from Todd Akin E-Guide:**

#### A. 30% Premium Hard Gate Alert
**What:** When CLM or CRF premium crosses 30% for the first time (from below), fire a dedicated "RO WATCH" alert before N-2 even drops. This is Todd's primary early-warning signal — the N-2 comes *after* 30% premium signals the probability.

**Implementation path:**
- Add `PREMIUM_RO_WATCH_THRESHOLD = 30.0` constant
- In `format_pulse_report()`, when premium ≥ 30% and no N-2 filed: insert `⚠️ PREMIUM AT 30%+ — RO WATCH ACTIVE` line
- Only alert once per crossing (debounce in DB: `cornerstone_30pct_watch_{ticker}`)
- Add +20 to `RO_SCORE_WEIGHTS` as a new `"premium_30pct_watch"` key (separate from existing `"premium_extreme"` which fires at 25%)

#### B. 3-Share Minimum Enforcement Alert
**What:** When RO is confirmed (N-2 detected), the sell instruction in the dispatch should explicitly say "Sell to 3 shares minimum to preserve CS DRIP status — do NOT go to zero."

**Implementation path:**
- In `dispatch_cornerstone_alert()` for CRITICAL tier with `sec_n2 = True`, add a line: `┣ DRIP Preserve: Keep ≥3 shares — full exit loses NAV DRIP permanently`
- No code change to detection logic — just the output template

#### C. RO Completion Signal (Re-Entry Trigger)
**What:** Currently we have no "RO is done — buy back now" signal. Todd says the re-entry trigger is when Cornerstone announces completion. We can't scrape their website easily, but we can proxy it:
- **Proxy signal**: Premium collapses back toward NAV (drops from 30%+ to <10%) AND price drops ≥15% from recent high. This pattern reliably marks the post-RO dip bottom.
- **Secondary proxy**: Check Seeking Alpha API (if available) or Reddit r/dividends for "CLM RO complete" mentions (not currently implemented — requires external scraping beyond Twelve Data scope)

**Implementation path (Twelve Data only):**
- New function `detect_ro_completion_dip()` in `monitor.py`
- Triggers when: (1) `tqqq_last_n2_detected` DB key is set for this ticker AND (2) premium drops below 10% after being above 20% AND (3) price is ≥10% below 60D high
- Dispatches a `🟢 RO DIP — REBUY ZONE` alert to #cornerstone with: price, current premium, estimated NAV, "Company may have announced RO completion — verify on cornerstone website before acting"
- Re-entry sizing note: "Rebuy position + DRIP income accumulated during hold period"

#### D. Seeking Alpha / Press Release Monitoring (Future Phase)
**What:** Todd's highest-conviction RO confirmation source is Cornerstone's own press release page. This requires either web scraping or an RSS feed — outside Twelve Data scope.
- **Option 1**: Add a Seeking Alpha RSS feed parser (Seeking Alpha has free RSS for public tickers). Cornerstone's SA symbol is `CLM` — watch for "Rights Offering" keyword in feed titles.
- **Option 2**: Manual check reminder — when N-2 is detected, the dispatch says "Monitor cornerstone website press releases at [URL] and Seeking Alpha CLM/CRF comments for 'RO complete' announcement"
- **Recommended**: Option 2 now, Option 1 if RSS is clean and stable

**Current functions that DO NOT need changes:** N-2 EDGAR watcher, whale flow, 13D/G, dark pool, premium compression, macro correlation — all solid.

---

### 2. #income (`scheduler.py --mode income` + `--mode wheel_signals`)

**Current state:** CC ETF/dividend pulse, Dividend Wheel v2 screener, ex-div radar, new income ETF radar, Tier 2 IV Rank screener, wheel position tracker.

**@easyincomeinvesting / @investingwithhenry philosophy:**
- Wheel on monthly-dividend stocks so assignment earns you dividend income while waiting for CC call-away
- Prefer stocks with predictable, stable dividends (not yield-chasing)
- Wheel on stocks you wouldn't mind owning 6-12 months
- Conservative strikes (0.20–0.25 delta) for safety, not premium maximization
- Assignment is not a failure — it's Phase 2 of the wheel

**Gaps / Improvements:**

#### A. Assignment Dividend Yield Display (Income Wheel Screener)
**What:** When the IV Rank screener fires a CSP setup on a monthly-div stock, the current output shows premium yield but NOT what the stock yields if assigned. Todd's framework says "the dividend pays you while you wait."

**Implementation path:**
- In `generate_tier2_iv_rank_alerts()`, when a candidate has `div_freq == "Monthly"`, append:
  - `┣ If Assigned: ~{monthly_div_per_share}/share/mo ({annual_yield:.1f}% annual)`
  - `┗ Total Return: Premium + Dividend = {combined:.1f}% annualized`
- This is currently partially done (we show `div_badge`) but doesn't show the actual dividend amount in the CSP setup block

#### B. Wheel Universe Tier Labels
**What:** The expanded WHEEL_UNIVERSE in analytics.py now has 30 stocks but they're mixed. For the income channel, separate them by:
- **Monthly Div Wheel** (preferred): AGNC, NLY, MAIN, O, PFLT, GAIN, F, SOFI, ET, MLPI, KQQQ, TDAQ
- **Standard Wheel**: INTC, PLTR, PFE, BMY, XOM, CVX, etc.

**Implementation path:**
- Add `"wheel_tier": "monthly_div" | "standard"` key to each entry in `WHEEL_UNIVERSE`
- In the screener output header: `⭐ MONTHLY DIV` badge before the ticker name when `wheel_tier == "monthly_div"`
- Sort monthly_div candidates first in the output

#### C. Phase Tracker in Wheel Position Output
**What:** When a wheel position is open, the DTE countdown should show which phase we're in and what to do next.

**Implementation path:**
- Extend `--mode wheel_position` display to show:
  - Phase 1 (CSP open): `CSP Phase — collecting premium`
  - Phase 2 (assigned, hold): `Assignment Phase — collecting dividends (est. ${div_per_share}/mo)`
  - Phase 3 (CC open): `Covered Call Phase — exit premium`
- DB stores phase alongside position; `--action assign` command transitions Phase 1 → 2

#### D. Monthly Div Calendar View
**What:** At start of each month, post a "Dividend Calendar" to #income — which held wheel positions pay dividends this month and when, so the user knows which positions to NOT call away before ex-div.

**Implementation path:**
- New function `generate_monthly_div_calendar()` in analytics.py
- Pulls ex-div dates for all open wheel positions from Twelve Data
- Output: list of tickers with ex-div dates, record dates, estimated payment amounts
- Fires on the 1st of each month via scheduler

---

### 3. #options (`tqqq.py`)

**Current state:** Directional TQQQ sniper (BTO calls/puts) + insurance put renewal clock. Single-dispatch-per-setup model. Gates: QQQ vs 21 EMA + z-score + macro_bull flag.

**TQQQ "wave riding" philosophy (per user + knowledge bank):** Single high-conviction entry, hold through the wave, exit only at 100% gain, 14 DTE, or 40% stop. Not scalping direction changes.

**Gaps / Improvements:**

#### A. Wave Position Duration Tracker
**What:** Once a BTO signal is dispatched, the user needs periodic "wave status" updates — not new signals, but check-ins on the open position.

**Implementation path:**
- New function `check_wave_position_status()` in `tqqq.py`, called in the main loop
- Only fires when `tqqq_open_position` is set
- Every 3 days (not daily — noise): post to #options:
  - `**TQQQ WAVE UPDATE — Day {N}**`
  - `┣ Entry: {contract} @ ${entry_price}`
  - `┣ Current TQQQ: ${current_price} ({pct_change:+.1f}% from entry)`
  - `┣ Days to Expiry: {dte}`
  - `┗ Status: RIDING | APPROACHING TARGET | 14DTE ALERT | STOP ZONE`
- Gate: only update if TQQQ has moved >3% from last update price (avoid noise on flat days)

#### B. Exit Signal Dispatcher
**What:** Currently `check_open_position_for_exit()` closes the DB position but doesn't dispatch a Discord message telling the user to act. User has to infer from silence.

**Implementation path:**
- In `check_open_position_for_exit()`, after clearing the position, call `self.dispatch_exit_signal(reason)` which posts:
  - `🏁 TQQQ WAVE EXIT — {reason}` (reason = "100% target", "14 DTE", "40% stop")
  - Position summary: entry → exit math
  - `┗ Next: Monitor for next wave setup. Insurance put renewal status: {put_dte} DTE remaining`

#### C. Conviction Score Breakdown in Signal
**What:** Current BTO dispatches show the conviction score but not what drove it. The "wave riding" philosophy means the user needs to trust the setup — show your work.

**Implementation path:**
- Add a `conviction_breakdown` dict to `evaluate_snipe()` output
- In the dispatch message, add a `┣ Conviction Drivers:` line: e.g. `EMA gate ✓ | RSI oversold ✓ | VIXY calm ✓ | Breadth 58% ✓`
- Keep it to one line on mobile

#### D. Regime Flip Alert (Separate from Signal)
**What:** When macro regime flips BULL→BEAR or BEAR→BULL, dispatch a brief regime change note — even if no new trade signal yet. This gives the user heads-up that a new wave direction may be forming.

**Implementation path:**
- Track `tqqq_last_macro_regime` in DB (BULL/BEAR)
- When it flips: post `⚡ REGIME FLIP: {old} → {new}` to #options with QQQ price, 21 EMA level, VIX proxy
- This is NOT a signal — explicitly say "Monitoring for wave setup..."

---

### 4. #announcements (`announcements.py` — TO BUILD)

**Current state:** Not yet built. CLAUDE.md says: weekly accuracy scorecard, teaser numbers, conversion engine for free tier.

**Design:**

#### A. Prediction Logging System
**What:** Each time a signal fires in any channel, log the prediction to a DB table with a grading window. Predictions are logged via `#market-analysis` (the single source of truth that synthesizes all feeds) — `#announcements` only publishes the weekly scorecard derived from those logs.

**DB schema (extend `database.py`):**
```
predictions table:
  id, channel, signal_type, ticker, predicted_direction, 
  predicted_target, signal_date, grade_date, 
  actual_outcome, score (1.0/0.5/0.0), graded_bool
```

**Signals to log:**
- `/NQ overnight direction` (from cross_asset.py) → graded next AM
- `CLM/CRF premium direction` (from monitor.py) → graded weekly
- `TQQQ put/call direction` (from tqqq.py) → graded at expiry or close
- `BTC direction` (from crypto.py when built) → graded weekly

#### B. Auto-Grader
**What:** Daily cron that grades yesterday's predictions against actual outcomes.

**Grading rules:**
- Directional call: 1.0 if correct, 0.0 if wrong, 0.5 if flat/within noise
- Premium call: 1.0 if premium moved in predicted direction by ≥1%
- Grade window: futures = next session close, options = weekly close, premium = 5-day

**Implementation:** New method `grade_pending_predictions()` in analytics.py, called by scheduler at EOD.

#### C. Weekly Scorecard Dispatcher
**What:** Every Sunday, dispatch to #announcements with the scorecard format from CLAUDE.md.

```
📊 WEEKLY ACCURACY SCORECARD — Week of [DATE]
Signal          | Predicted | Actual  | Score
/NQ direction   | Bullish   | +1.8%   | ✅
CLM premium     | Accum.    | +3.2%   | ✅
TQQQ direction  | [locked]  | [locked]| 🔒
BTC direction   | [locked]  | [locked]| 🔒
WEEK ACCURACY: 2/2 free signals — 100% 🎯 | MTD: {mtd}%
━━━━━━━━━━━━━━━━━━━━━━━━━
🔒 Pro signals locked — upgrade to see TQQQ + BTC calls
```

**Conversion mechanic:** Free users see 2 signals graded. Paid signals shown as 🔒 with result hidden but accuracy % includes them to show value.

#### D. Monthly Summary Post
**What:** First of each month, post a longer scorecard with MTD accuracy and top 3 calls of the month.

---

### 5. Macro Recession Guard (Cross-channel, NEW)

**From Philosophical Economics article in knowledge bank:**

The GTT (Growth-Trend Timing) model — when unemployment rate > 12-month MA AND price trend is down → go defensive. This is the cleanest macro regime filter available.

**Implementation path:**
- New function `check_recession_guard()` in analytics.py
- FRED data: UNRATE (monthly unemployment) vs 12-month MA
- If UNRATE > MA: recession risk ON → reduce TQQQ call sizing by 50%, no new margin draws, increase put size
- Dispatch to #market-analysis when mode flips
- This is a monthly-cadence signal (FRED updates monthly) — not real-time

**Twelve Data availability:** FRED data accessible via `analytics._fetch_fred_metric("UNRATE")` already — this is a low-cost add.

---

## What's Already Well-Implemented (Do Not Re-Do)

- N-2 EDGAR watcher → SEC filing detection is solid
- 13D/G large holder change detection
- Whale flow (direction-aware accumulation/distribution)
- Dark pool proxy (price drop on low public volume)
- Premium compression (intra-session CEF spread collapse)
- TQQQ single-dispatch model (BTO_CALL/PUT/MON_CALL/PUT states)
- Wheel v2 screener with IVR, delta, bid/ask filters
- EOD boundary-audit-driven lesson text
- WTREGEN unit fix (÷1000)
- CLM/CRF retry logic (2 attempts, 2s backoff)

---

## Priority Order for Implementation

| Priority | Feature | Channel | Effort |
|----------|---------|---------|--------|
| 1 | 30% premium RO Watch Gate | #cornerstone | Low |
| 2 | RO Completion Dip Detector | #cornerstone | Medium |
| 3 | 3-share minimum text in N-2 dispatch | #cornerstone | Trivial |
| 4 | Assignment dividend yield in CSP output | #income | Low |
| 5 | Wave position status updates (3-day) | #options | Low |
| 6 | Exit signal dispatcher | #options | Low |
| 7 | Prediction logging + auto-grader | #announcements | Medium |
| 8 | Weekly scorecard dispatcher | #announcements | Medium |
| 9 | Regime flip alert | #options | Low |
| 10 | Monthly div calendar | #income | Medium |
| 11 | Macro recession guard (UNRATE GTT) | cross-channel | Medium |
| 12 | Seeking Alpha RSS for RO completion | #cornerstone | Low-Med |

---

## What Requires Data Not Available on Twelve Data Venture

- True options flow / dark pool tape (Level 2 / tape data) — confirmed unavailable
- Seeking Alpha scraping (requires SA API or careful RSS parsing)
- Cornerstone press release page scraping (could use requests + BeautifulSoup as one-off)
- Real VIX (we use VIXY as proxy — confirmed limitation)
- Brokerage position auto-detection (E*TRADE API would solve this but not in scope)

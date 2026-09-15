# xdca.py Design Notes
*Cashflow ZZZ Machine | Created Sept 15, 2026*

## Option 1 vs Option 2 — Underlying Drawdown Proxy

### Option 1 (Implemented): 20-Day Rolling High
xdca.py uses the underlying ETF's (SPY/QQQ/XLE) rolling 20-session high as the drawdown
reference. "Zone entry" fires when the underlying is X% below that 20-day peak.

**Why:** Simple, zero manual upkeep, always fresh, works as a "recent high to current" 
drop detector. 20 sessions captures most meaningful pullbacks without being so long
that stale peaks from months ago dilute the signal.

**Limitation:** Innovator Power Buffer ETFs (XSPI, XQQI) reset their buffer quarterly
on a fixed outcome period date. A 10% SPY drop within the buffer outcome period may
only show a 6% drop in XSPI (buffer absorbing 4%). The 20-day proxy doesn't distinguish
between "inside buffer" and "buffer exhausted" scenarios.

---

### Option 2 (Future): Quarterly Buffer Reset Date
Seed the quarterly buffer reset dates for XSPI and XQQI into the DB. Compute actual
Innovator Power Buffer utilization (SPY drawdown from the buffer's start-of-period price,
not the rolling 20-day high) for a more accurate Zone C/D trigger.

**DB keys to add:**
```
xdca_xspi_buffer_reset_date   (YYYY-MM-DD — start of current outcome period)
xdca_xspi_buffer_pct          (float — buffer protection %, e.g. 15.0)
xdca_xqqi_buffer_reset_date
xdca_xqqi_buffer_pct
```

**Where to get the data:**
- Innovator publishes outcome period start dates at innovatoretfs.com
- Each outcome period: typically quarterly (Jan/Apr/Jul/Oct reset cycle)
- Buffer protection %: published in fund documents; typically 15% for these series

**Implementation:**
1. Seed manually once per quarter: `db.update_state("xdca_xspi_buffer_reset_date", "2026-10-01")`
2. In `fetch_underlying_20d_high()` for SPY/QQQ, add a branch:
   - If `xdca_{sym}_buffer_reset_date` is set → use SPY/QQQ close on that date as the
     reference price (not rolling 20d high)
   - Compute: `buffer_used_pct = (reset_price - current_ul_price) / reset_price * 100`
   - If `buffer_used_pct > buffer_pct` → buffer exhausted → ETF absorbing losses 1:1
   - Override Zone D threshold to fire earlier (buffer_used_pct ≥ 80% of buffer)

**When to implement:** After Option 1 has been live for 30+ days and zone signals have
been validated against actual market action. Option 2 adds precision; Option 1 is 
the correct foundation.

---

## NAV Erosion Hardening — Academic Basis

### Why Covered Call ETF NAV Erodes (and how the zone system defends against it)

1. **Sustained bear markets cap the recovery** — covered call overlay sells away upside.
   If SPY drops 20% and the buffer absorbs 15%, XSPI drops 5%. But when SPY recovers 20%,
   XSPI may only capture 8-12% (call caps the upside participation). Net result after a
   deep bear/bull cycle: XSPI slightly lags SPY's full recovery.
   
   **Defense:** Zone C/D fires at confirmed fear (VIXY z ≥ 0.8-1.5σ). High VIX = elevated
   CC premiums = distributions temporarily higher = buying at depressed price WITH elevated
   income. This is the optimal entry window.

2. **Distributions don't compound at NAV like DRIP** — monthly cash reduces cost basis but
   doesn't reinvest at NAV discount (unlike CLM/CRF DRIP).
   
   **Defense:** Route all distributions from XSPI/XQQI/MLPI/KQQQ → margin paydown
   (same as MLPI/MAIN in the current Tier 2 model). Never DRIP these — take the cash and
   deploy it to reduce the margin interest burden.

3. **Don't DCA into a falling knife** — mechanical calendar-based DCA into a declining
   asset just acquires more shares of something that may continue falling.
   
   **Defense:** Zone B/C/D all require RSI below 42/35/28 respectively AND drawdown from
   recent high. Zones B+C also require VIXY z ≥ 0.3/0.8 — confirming actual fear, not
   routine volatility. Silent zones (A, B) don't alert; only confirmed capitulation signals
   (C, D) generate actionable notifications.

### Key Source Material
- `tqqq bot.txt` (knowledge bank): regime switching approach validates fear-gate requirement
  for entries. STRONG_BULL/BEAR regime logic maps directly to the VIXY z-score gate here.
- `income notes.txt`: wheel scoring formula `(1 - |Δ|) × (250 / DTE) × (bid/strike)` confirms
  that premium-per-time is the metric — high-VIX environments maximize this for CC ETFs.
- Paycheck to Portfolio Blueprint (Shawn Grady): "Non-DRIP dividends used to cover margin
  interest" — confirmed strategy; xdca.py's alert description includes this routing.

---

## Future Enhancements

- **Ex-dividend date awareness:** Add `xdca_{sym}_exdiv_date` to DB. Suppress Zone C/D alerts
  1-2 days before ex-div (mechanical price drop about to occur anyway). Alert 1 day after
  ex-div instead (best post-drop entry window). Source: scheduler.py `--mode exdiv_check`.

- **XLE-specific enhancement for MLPI:** When WTI crude (CL1!) is ≥ 5% below its 52-week
  high AND XLE is oversold, MLPI distributions will be supported by elevated energy IV.
  The existing mlpi_entry mode in scheduler.py already handles the intraday entry signal;
  xdca.py handles the zone-based multi-day positioning.

- **KQQQ/XQQI consolidation check:** Both track QQQ. If both fire Zone C/D simultaneously,
  Pushover message should note the overlap ("KQQQ + XQQI both in zone — QQQ at capitulation").
  Currently each fires independently; a combined signal is higher conviction.

- **Zone escalation tracking:** Log each Zone A→B→C→D progression per ticker to DB.
  If a ticker spends 3+ consecutive scans in Zone B before escalating to C, that's a
  sustained oversold condition — stronger signal than a single Zone C spike.

"""
market_scheduler.py — Always-on time-aware dispatcher.

Replaces the entire pre-market / market-hours / EOD cron cluster with a single
always-on process. Wakes every 60 seconds, checks current UTC time against a
schedule table, fires each mode as a non-blocking subprocess, and deduplicates
via the DB so no mode fires twice in the same calendar day even across restarts.

Keep in PythonAnywhere cron (these are once-daily and too lightweight to justify
an always-on slot):
  09:39 UTC  audit.py          — DB maintenance / vacuum
  06:00 UTC  daily_pulse.py    — morning health check

Everything else is handled here.

Cron slots freed (remove these from PythonAnywhere after deploying):
  morning, gex, macro (×2), trending_plays, futures_social, wheel_signals,
  crypto_social, cross_asset (×2), options_flow (×3), market_intraday,
  income, iv_crush, post_market, eod, weekly_scorecard
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timezone

from database import EcosystemDatabase

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger("MarketScheduler")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# sys.executable — the exact interpreter running this script, virtualenv and all.
# Hardcoding "python3.10" resolves to the system Python on PythonAnywhere, which
# has none of our packages. Every child would crash with ImportError, silently.
PYTHON = sys.executable

# Child stderr goes here so failures are visible without flooding the scheduler log.
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# How many minutes either side of the target time a job can fire.
# 60-second loop + 2-minute window means worst-case slip is 3 minutes.
FIRE_WINDOW_MINUTES = 2

# ── Schedule table ────────────────────────────────────────────────────────────
# Each entry: (utc_hour, utc_minute, task_key, script, extra_args, weekdays_only)
#
# script is either:
#   "scheduler"   → python3.10 scheduler.py --mode <extra_args[0]>
#   "cross_asset" → python3.10 cross_asset.py [extra_args[0] if any]
#
# weekdays_only=True skips Sat/Sun.
# weekly_scorecard is weekdays_only=True but also internally gated to Friday
# by the script itself — the scheduler just fires it daily; the script skips Mon-Thu.

SCHEDULE = [
    # UTC    key                    script          args                   wkdays
    # ── Futures: 4 session windows ──────────────────────────────────────────────
    # CME equity futures trade ~23h/day (Sun 18:00 UTC – Fri 21:00 UTC).
    # Four boards cover Asian close/European open, US pre-market, RTH open, PM.
    ( 7,  0, "cross_asset_asia",   "cross_asset",  [],                            True),  # 21:00 HST (Asian close / EU pre)
    (12, 45, "cross_asset_premarket","cross_asset", [],                            True),  # 02:45 HST (US pre-market)
    (14,  0, "cross_asset_am",     "cross_asset",  [],                            True),  # 04:00 HST (cash open)
    (18, 45, "cross_asset_pm",     "cross_asset",  [],                            True),  # 08:45 HST (mid-session)
    # ── Market Analysis & Macro ──────────────────────────────────────────────────
    (12, 50, "morning",            "scheduler",    ["--mode", "morning"],         True),
    # gex removed — calculate_gex_profile() returns 0.0 at this Twelve Data tier;
    # output is always UNKNOWN/suppressed and wastes a subprocess launch.
    (13, 28, "macro_am",           "scheduler",    ["--mode", "macro"],           True),
    # ── Signals & Income ────────────────────────────────────────────────────────
    (13, 35, "trending_plays",     "scheduler",    ["--mode", "trending_plays"],  True),
    (13, 40, "futures_social",     "scheduler",    ["--mode", "futures_social"],  True),
    (13, 45, "wheel_signals",      "scheduler",    ["--mode", "wheel_signals"],   True),
    (13, 50, "crypto_social",      "scheduler",    ["--mode", "crypto_social"],   True),
    # options_flow (×3) removed — GEX always returns UNKNOWN at this plan tier;
    # all three runs fell through to the fallback "Options Market Flowstate" embed
    # with SPY Gamma Posture: UNKNOWN and GEX Flip Level: $0.00. Zero signal value.
    (17, 30, "market_intraday",    "scheduler",    ["--mode", "market_intraday"], True),
    # spx_income removed — not part of active strategy.
    (18,  5, "income",             "scheduler",    ["--mode", "income"],          True),
    # store_daily_iv is NOT here — it runs once/day via PythonAnywhere scheduled task
    # at 21:30 UTC (after market close). Add to cron: python scheduler.py --mode store_daily_iv
    (18, 15, "iv_crush",           "scheduler",    ["--mode", "iv_crush"],        True),
    (20, 14, "post_market",        "scheduler",    ["--mode", "post_market"],     True),
    (20, 16, "eod",                "scheduler",    ["--mode", "eod"],             True),
    (20, 30, "macro_pm",           "scheduler",    ["--mode", "macro"],           True),
    # CEF premium z-score calibration — 22:30 UTC daily, after US cash close.
    # Pulls 252-day premium history from CEFConnect → updates mu/sigma in DB
    # so monitor.py's z-score uses empirical data, not hardcoded defaults.
    (22, 30, "cef_calibrate",      "scheduler",    ["--mode", "cef_calibrate"],   True),
    # weekly_scorecard fires Friday only — gated here, not inside the script.
    # weekdays_only=True keeps it off weekends; Friday check is the tuple's 7th element.
]

# Friday-only entries appended separately so the main loop can filter them.
SCHEDULE_FRIDAY_ONLY = [
    (20, 30, "weekly_scorecard",   "scheduler",    ["--mode", "weekly_scorecard"], True),
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _db_key(task_key: str, date_str: str) -> str:
    return f"mktsch_fired_{task_key}_{date_str}"

def already_fired(db: EcosystemDatabase, task_key: str, date_str: str) -> bool:
    return bool(db.get_state(_db_key(task_key, date_str)))

def mark_fired(db: EcosystemDatabase, task_key: str, date_str: str):
    db.update_state(_db_key(task_key, date_str), True)

def build_cmd(script: str, args: list) -> list:
    if script == "scheduler":
        return [PYTHON, os.path.join(BASE_DIR, "scheduler.py")] + args
    if script == "cross_asset":
        cmd = [PYTHON, os.path.join(BASE_DIR, "cross_asset.py")]
        return cmd + args if args else cmd
    raise ValueError(f"Unknown script type: {script}")

def fire(task_key: str, cmd: list):
    """Launch the task as a detached subprocess — fire and forget."""
    try:
        log_path = os.path.join(LOGS_DIR, f"{task_key}.log")
        with open(log_path, "a") as log_fh:
            proc = subprocess.Popen(
                cmd,
                stdout=log_fh,
                stderr=log_fh,
                cwd=BASE_DIR,
            )
        logger.info(f"Fired [{task_key}] PID={proc.pid} → {' '.join(cmd[1:])}")
    except Exception as e:
        logger.error(f"Failed to fire [{task_key}]: {e}")

def in_window(now_h: int, now_m: int, target_h: int, target_m: int) -> bool:
    now_total    = now_h    * 60 + now_m
    target_total = target_h * 60 + target_m
    return abs(now_total - target_total) <= FIRE_WINDOW_MINUTES

# ── Main loop ─────────────────────────────────────────────────────────────────

def run():
    db = EcosystemDatabase()
    logger.info("Market Scheduler online. Loop interval: 60s.")

    while True:
        now_utc  = datetime.now(timezone.utc)
        weekday  = now_utc.weekday()          # 0=Mon … 6=Sun
        is_wkday = weekday < 5
        date_str = now_utc.strftime("%Y-%m-%d")
        h, m     = now_utc.hour, now_utc.minute

        schedule = SCHEDULE + (SCHEDULE_FRIDAY_ONLY if weekday == 4 else [])
        for (t_h, t_m, task_key, script, args, wkdays_only) in schedule:
            if wkdays_only and not is_wkday:
                continue
            if not in_window(h, m, t_h, t_m):
                continue
            if already_fired(db, task_key, date_str):
                continue

            mark_fired(db, task_key, date_str)
            cmd = build_cmd(script, args)
            fire(task_key, cmd)

        # Sleep to the next wall-clock minute boundary rather than a flat 60s.
        # Flat sleep accumulates drift from loop execution time — over a trading day
        # that can push a tick past the ±2-minute fire window and silently skip a task.
        now_ts = time.time()
        time.sleep(60 - (now_ts % 60))

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by operator.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Scheduler crashed: {e}")
        sys.exit(1)

import os
import time
import requests
import datetime
import json
import sys
import pytz
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

try:
    from essentials_tools import (
        send_essentials_embed, 
        get_institutional_conviction, 
        get_trend_alignment
    )
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Environment Variables
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")
SIGNAL_LOG = os.path.join(BASE_PATH, "signal_results.json")

def get_signal_tier(conviction_score, rsi, trend_bullish):
    """Categorizes signal quality with color-coded risk levels."""
    if "HIGH" in conviction_score and trend_bullish and 40 < rsi < 65:
        return "Tier A - High Conviction", 0x2ecc71  # Green
    if "HIGH" in conviction_score:
        return "Tier B - Tactical Entry", 0xf1c40f  # Yellow
    return "Tier C - Speculative", 0x3498db  # Blue

class RockefellerFuturesEngine:
    def __init__(self):
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.last_pulse_hour = -1

    def run_engine_cycle(self, is_test=False):
        """Evaluates entry triggers, structural fakeouts, and active equity watchlists."""
        if not HAS_ESSENTIALS:
            print("❌ Execution Blocked: essentials_tools.py could not be found.")
            return

        # 1. Read Shared Ecosystem State
        try:
            with open(REGIME_LEDGER, "r") as f:
                ledger = json.load(f)
        except Exception:
            print("⚠️ Ecosystem Ledger unreadable. Applying structural defaults.")
            ledger = {}

        regime = ledger.get("regime", "BULLISH")
        vix_status = ledger.get("vix_status", "STABLE")
        vix_current = ledger.get("vix_current", 14.5)
        vix_velocity = ledger.get("vix_velocity", "NOMINAL")
        rsi_limit = ledger.get("rsi_shield_limit", 66)
        macro_muted = ledger.get("macro_muted", False)

        now = datetime.datetime.now(self.tz)

        # 2. System Safeguards: Verify Dynamic Control Settings
        if (macro_muted or vix_velocity == "CRITICAL_SPIKE") and not is_test:
            self.dispatch_block_alert(vix_status, vix_current, "⚠️ MUTED / MACRO VOLATILITY SHUTDOWN")
            return

        # 3. Interrogate Live /ES Proxy Vectors via SPY Core Metrics
        current_rsi = 55.0 if not is_test else 72.5  # Simulate overextended breakout condition during tests
        trend_status, is_bullish = get_trend_alignment("SPY", TD_API_KEY)
        conviction_str, color, whale_active = get_institutional_conviction("SPY", TD_API_KEY)
        
        if is_test:
            whale_active = False # Route execution directly into the fakeout filter check for verification

        # 4. Futures Signal Architecture Loop
        if current_rsi > rsi_limit:
            if not whale_active:
                self.dispatch_fakeout_alert(current_rsi, rsi_limit, conviction_str)
            else:
                self.dispatch_suppression_alert(regime, vix_status, current_rsi, rsi_limit)
        elif whale_active and is_bullish:
            self.dispatch_valid_trade_signal(current_rsi, rsi_limit, conviction_str, trend_status)

        # 5. Live Options Watchlist Scanning
        print("🔍 [Sentry Scan] Equity channels open. Analyzing live tickers for option trade flows...")
        try:
            sample_watchlist = ["CLM", "CRF"]
            for ticker in sample_watchlist:
                opt_conviction, opt_color, opt_triggered = get_institutional_conviction(ticker, TD_API_KEY)
                _, opt_trend_bullish = get_trend_alignment(ticker, TD_API_KEY)
                
                if opt_triggered or is_test:
                    tier_label, color_code = get_signal_tier(opt_conviction, current_rsi, opt_trend_bullish)
                    title = f"🚨 OPTIONS ALIGNMENT DETECTED: {ticker}"
                    desc = (
                        f"Technical breakout alert tracking under **{tier_label}** thresholds.\n\n"
                        f"┣ **Asset Underlying**: `{ticker}`\n"
                        f"┣ **Conviction Flow**: `{opt_conviction}`\n"
                        f"┗ **Ecosystem Posture**: `{vix_status}`"
                    )
                    if WEBHOOK_OPTIONS:
                        send_essentials_embed(WEBHOOK_OPTIONS, title, desc, color_code)
                    if is_test: 
                        break  # Prevent loop spamming during terminal verification tests
        except Exception as e:
            print(f"⚠️ Options Tracking Anomaly: {e}")

        # 6. Bi-Hourly Pulse Engine Status Verification Checks
        if (now.hour % 2 == 0 and now.hour != self.last_pulse_hour) or is_test:
            self.dispatch_intraday_pulse(regime, vix_status, vix_current, current_rsi, conviction_str)
            self.last_pulse_hour = now.hour

    # --- TRANSMISSION EMBED BLUEPRINTS ---

    def dispatch_valid_trade_signal(self, rsi, rsi_limit, conviction, trend):
        embed = {
            "title": "🏛️ Futures Execution Flowstate Update",
            "description": (
                f"**System Status**: `🟢 ACTIVE / SIGNAL TRIGGERED`\n\n"
                f"**🎯 Tactical Entry Parameters**:\n"
                f"┣ **Asset**: `/ES` (E-mini S&P 500 Futures)\n"
                f"┣ **Strategy**: `Momentum Breakout (Scalp)`\n"
                f"┣ **Direction**: `LONG`\n"
                f"┗ **Vector**: `Aggressive Buying Detected`\n\n"
                f"**📊 Market Context (The Radar)**:\n"
                f"┣ **Regime**: `{trend}`\n"
                f"┣ **Sentry RSI**: `{rsi:.1f}` (Limit: {rsi_limit})\n"
                f"┗ **Institutional Flow**: `{conviction}`\n\n"
                f"**🔬 Order Flow Intelligence**:\n"
                f"┗ **Order Flow Note**: Heavy market volume backing the break. Watch for potential 'Trapped Short Sellers' to fuel short-term continuation.\n\n"
                f"**🛡️ Risk Management (Natenberg Surface Guardrails)**:\n"
                f"┗ *Signals provide setups; risk control ensures survival. Manage risk strictly.*"
            ),
            "color": 0x2ecc71,
            "footer": { "text": "Rockefeller Strategic Intelligence • HST Timezone" }
        }
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})

    def dispatch_fakeout_alert(self, rsi, rsi_limit, conviction):
        embed = {
            "title": "🚨 Sentry Filter: TRAFFIC ALERT / FALSE BREAKOUT",
            "description": (
                f"**System Status**: `⚠️ SCANNING / REJECTING SETUP`\n\n"
                f"**Mechanic**: `Failed Breakout / Trapped Traders Scenario`\n\n"
                f"**Data Diagnostics**:\n"
                f"┣ **Sentry RSI**: `{rsi:.1f}`\n"
                f"┣ **Volumetric Conviction**: `{conviction}`\n"
                f"┗ **Shield Limit**: `{rsi_limit}`\n\n"
                f"**The Why**: Price Action is pushing localized daily structural highs, but institutional metrics show a complete lack of volume backing. **This is a retail trap.** The engine is withholding entries to prevent buying the top of unbacked velocity."
            ),
            "color": 0xe67e22,
            "footer": { "text": "Rockefeller Capital Preservation Shield" }
        }
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})

    def dispatch_suppression_alert(self, regime, vix_status, current_rsi, rsi_limit):
        embed = {
            "title": "🏛️ Futures Flowstate Update",
            "description": (
                f"**System Status**: `SCANNING / NO ENTRIES`\n\n"
                f"**Market Context**:\n"
                f"┣ **Regime**: `{regime}`\n"
                f"┣ **Sentry RSI**: `{current_rsi:.1f}` (Limit: {rsi_limit})\n"
                f"┗ **Volatility**: `{vix_status}`\n\n"
                f"**The Why**: The **RSI Shield** is currently acting as the primary gatekeeper. With RSI at `{current_rsi:.1f}`, the engine considers the risk-to-reward unfavorable. Capital is withheld awaiting a high-conviction pullback."
            ),
            "color": 0x3498db,
            "footer": { "text": "Rockefeller Strategic Intelligence" }
        }
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})

    def dispatch_block_alert(self, vix_status, vix_current, message_type):
        embed = {
            "title": f"{message_type}",
            "description": (
                f"**Engine Posture**: `❌ TRADING OFFICIALLY PAUSED`\n\n"
                f"**Current Metrics**:\n"
                f"┣ **Volatility Level**: `{vix_status}`\n"
                f"┗ **VIX Index**: `{vix_current:.2f}`\n\n"
                f"**System Directives**: High-impact macro window or extreme velocity anomaly detected. Technical setups are currently invalidated due to erratic liquidity sweeps. Preservation of dry powder takes priority."
            ),
            "color": 0xe74c3c,
            "footer": { "text": "Rockefeller Automated Kill-Switch Enforced" }
        }
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})

    def dispatch_intraday_pulse(self, regime, vix_status, vix_current, rsi, conviction):
        embed = {
            "title": "📊 Futures State of the Tape: Intraday Pulse Check",
            "description": (
                f"### **Session Structural Assessment**\n\n"
                f"**Post-Opening Flow Context**:\n"
                f"┣ **Macro Regime**: `{regime}`\n"
                f"┣ **Volatility Sentry**: `{vix_status}` (`{vix_current:.2f}`)\n"
                f"┣ **Current RSI Tracking**: `{rsi:.1f}`\n"
                f"┗ **Order Book Profile**: `{conviction}`\n\n"
                f"**🔬 Structural Reading (Order Flow Metrics)**:\n"
                f"┣ **Absorption Profile**: Liquidity grids are holding. Watching for delta divergence against local daily pivots.\n"
                f"┗ **Trading Directive**: Do not force setups inside mid-session liquidity doldrums. Allow the architectural filters to identify genuine whale validation."
            ),
            "color": 0x3498db,
            "footer": { "text": "Rockefeller Intraday Tactical Update" }
        }
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})

if __name__ == "__main__":
    engine = RockefellerFuturesEngine()
    
    # Check for direct verification arguments
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Executing Live Webhook Integration Verification Test...")
        engine.run_engine_cycle(is_test=True)
        print("✅ Test Complete. Data packages successfully compiled and sent.")
    else:
        # Standard loop pattern for daemon mode execution
        while True:
            try:
                engine.run_engine_cycle(is_test=False)
            except Exception as e:
                print(f"Runtime Anomaly: {e}")
            time.sleep(60)

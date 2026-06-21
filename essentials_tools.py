import os
import io
import time
import json
import logging
import smtplib
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from email.message import EmailMessage
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from security import encode_canary

# Initialize Logger
logger = logging.getLogger("Essentials_Tools")
if not logger.handlers:
    ch = logging.StreamHandler()
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# --- DIAGNOSTICS & GATEKEEPERS ---
def benchmark_latency(func):
    """Decorator to track API execution speed for future-proofing."""
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logger.info(f"[Latency Benchmark] {func.__name__} executed in {(end - start):.4f} seconds.")
        return result
    return wrapper

def validate_payload_integrity(payload, expected_keys):
    """Ensures API data is intact before the ecosystem uses it."""
    if not payload or not isinstance(payload, dict):
        logger.error("Gatekeeper Check Failed: Payload is empty or invalid format.")
        return False
    
    missing_keys = [key for key in expected_keys if key not in payload]
    if missing_keys:
        logger.error(f"Gatekeeper Check Failed: Missing expected keys {missing_keys}")
        return False
    return True

# --- ALERTS & NOTIFICATIONS ---
def send_pushover_alert(message):
    token = os.getenv("PUSHOVER_APP_TOKEN")
    user = os.getenv("PUSHOVER_USER_KEY")
    if token and user:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": token, "user": user, "message": message, "title": "Rockefeller Alert"
            }, timeout=5)
            logger.info("Pushover alert dispatched successfully.")
        except Exception as e:
            logger.error(f"Pushover transmission failed: {e}")

def send_guardian_email(subject, body):
    sender = os.getenv("SENDER_EMAIL")
    pwd = os.getenv("EMAIL_APP_PASSWORD")
    work_email = os.getenv("WORK_EMAIL")
    if sender and pwd:
        try:
            msg = EmailMessage()
            msg.set_content(body)
            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = f"{sender}, {work_email}"
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
            logger.info("Guardian email dispatched successfully.")
        except Exception as e:
            logger.error(f"Guardian Email transmission failed: {e}")

@benchmark_latency
def send_essentials_embed(webhook_url, title, description, color=0x00ff00, user_id=None):
    """Dispatches a rich-text embed with an attached local thumbnail logo and invisible security canary."""
    if user_id is None:
        import hashlib
        user_id = int(hashlib.sha256(webhook_url.encode()).hexdigest()[:8], 16) & 0x7FFFFFFF
    canary_string = encode_canary(int(user_id))
    secured_description = f"{description}\n{canary_string}"
    logo_path = os.path.join(BASE_DIR, "ESSENTIALS - FOMO Logo.png")
    
    payload = {
        "embeds": [{
            "title": title,
            "description": secured_description,
            "color": color,
            "footer": {"text": "ESSENTIALS Macro-Quant Architecture | Data Secured"}
        }]
    }

    try:
        if os.path.exists(logo_path):
            payload["embeds"][0]["thumbnail"] = {"url": "attachment://logo.png"}
            with open(logo_path, "rb") as f:
                files = {
                    "payload_json": (None, json.dumps(payload)),
                    "file": ("logo.png", f, "image/png")
                }
                r = requests.post(webhook_url, files=files, timeout=5)
                r.raise_for_status()
        else:
            # Fallback if image doesn't exist locally (prevents catastrophic boundary crashes)
            r = requests.post(webhook_url, json=payload, timeout=5)
            r.raise_for_status()
            
        return True
    except Exception as e:
        logger.error(f"Discord secure dispatch failed: {e}")
        return False

def _stamp_chart_watermark(chart_bytes, trace_code):
    """
    Per-channel leak trace stamped directly into the chart pixels — the text canary only protects
    the embed description, so a screenshot cropped to just the image had zero protection until now.
    Deliberately a small, low-opacity corner tag rather than a full diagonal overlay (the old
    ocr.py/visuals.py approach, which existed but was never actually wired into any dispatch path):
    a visible-but-unobtrusive tag survives screenshotting/recompression (unlike LSB steganography,
    which doesn't) and doesn't degrade chart readability for legitimate subscribers.
    """
    try:
        from PIL import Image, ImageDraw
        img = Image.open(io.BytesIO(chart_bytes)).convert("RGBA")
        # Draw on a separate transparent layer and alpha-composite it — drawing fill alpha directly
        # onto the base image gets flattened away by convert("RGB") and renders fully opaque instead.
        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        tag = f"ESSENTIALS · {trace_code}"
        margin = 6
        bbox = draw.textbbox((0, 0), tag)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x, y = img.width - tw - margin - 4, img.height - th - margin - 4
        draw.text((x, y), tag, fill=(255, 255, 255, 100))
        img = Image.alpha_composite(img, overlay)
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logger.error(f"Chart watermark stamp failed, dispatching unstamped: {e}")
        return chart_bytes

@benchmark_latency
def send_essentials_embed_with_chart(webhook_url, title, description, chart_bytes, color=0x00ff00, user_id=None):
    """Same uniform embed format as send_essentials_embed, but attaches a generated chart PNG as the embed image."""
    if user_id is None:
        import hashlib
        user_id = int(hashlib.sha256(webhook_url.encode()).hexdigest()[:8], 16) & 0x7FFFFFFF
    canary_string = encode_canary(int(user_id))
    secured_description = f"{description}\n{canary_string}"
    trace_code = format(int(user_id) & 0xFFFFFF, "06X")
    chart_bytes = _stamp_chart_watermark(chart_bytes, trace_code)

    payload = {
        "embeds": [{
            "title": title,
            "description": secured_description,
            "color": color,
            "image": {"url": "attachment://chart.png"},
            "footer": {"text": f"ESSENTIALS Macro-Quant Architecture | Data Secured · {trace_code}"}
        }]
    }

    try:
        files = {
            "payload_json": (None, json.dumps(payload)),
            "file": ("chart.png", chart_bytes, "image/png")
        }
        r = requests.post(webhook_url, files=files, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Discord chart dispatch failed: {e}")
        return send_essentials_embed(webhook_url, title, description, color, user_id)

def generate_candlestick_chart(label, df, last_change=None, last_change_pct=None):
    """
    Finviz-style dark candlestick + volume snapshot. Expects df with columns:
    datetime (datetime64), open, high, low, close, volume — ascending order.
    No mplfinance dependency — hand-rolled to keep PythonAnywhere installs minimal.
    """
    df = df.copy().reset_index(drop=True)
    df['x'] = mdates.date2num(df['datetime'])
    width = (df['x'].diff().median() or 0.6) * 0.6
    has_volume = 'volume' in df.columns and df['volume'].sum() > 0

    if has_volume:
        fig, (ax, vol_ax) = plt.subplots(
            2, 1, figsize=(9, 5.5), dpi=120, sharex=True,
            gridspec_kw={"height_ratios": [4, 1], "hspace": 0.05}
        )
        axes = (ax, vol_ax)
    else:
        fig, ax = plt.subplots(figsize=(9, 5), dpi=120)
        axes = (ax,)

    fig.patch.set_facecolor("#0d1117")
    for a in axes:
        a.set_facecolor("#0d1117")
        a.tick_params(colors="white", labelsize=8)
        for spine in a.spines.values():
            spine.set_color("#30363d")
        a.grid(color="#21262d", linewidth=0.5)

    up_color, down_color = "#3fb950", "#f85149"
    for _, row in df.iterrows():
        color = up_color if row['close'] >= row['open'] else down_color
        ax.plot([row['x'], row['x']], [row['low'], row['high']], color=color, linewidth=0.8)
        body_low = min(row['open'], row['close'])
        body_height = max(abs(row['close'] - row['open']), (row['high'] - row['low']) * 0.01)
        ax.add_patch(plt.Rectangle((row['x'] - width / 2, body_low), width, body_height, color=color))
        if has_volume:
            vol_ax.bar(row['x'], row['volume'], width=width, color=color, alpha=0.7)

    last_price = df['close'].iloc[-1]
    title_suffix = ""
    if last_change is not None and last_change_pct is not None:
        arrow = "▲" if last_change >= 0 else "▼"
        title_suffix = f"  {arrow} {last_change:+.2f} ({last_change_pct:+.2f}%)"
    ax.set_title(f"{label}{title_suffix}", color="white", fontsize=12, loc="left")
    ax.axhline(last_price, color="#f1c40f", linewidth=0.8, linestyle=":")
    ax.text(df['x'].iloc[-1], last_price, f" {last_price:,.2f}", color="#0d1117",
            backgroundcolor="#f1c40f", fontsize=8, va="center")

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    fig.autofmt_xdate()
    fig.subplots_adjust(left=0.08, right=0.97, top=0.92, bottom=0.12)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()

def generate_line_comparison_chart(series_dict, title, lookback=90):
    """
    Normalized (rebased to 100) multi-line performance comparison — e.g. TSP funds G/F/C/S/I
    over the trailing N days, so members can see relative performance at a glance.
    series_dict: {label: pandas.Series of prices, ascending by date}
    """
    fig, ax = plt.subplots(figsize=(9, 5), dpi=120)
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")

    palette = ["#58a6ff", "#3fb950", "#f1c40f", "#f85149", "#a371f7", "#ff9f43", "#1abc9c"]
    for i, (label, series) in enumerate(series_dict.items()):
        s = series.tail(lookback).reset_index(drop=True)
        if s.empty or s.iloc[0] == 0:
            continue
        rebased = (s / s.iloc[0]) * 100
        ax.plot(rebased.index, rebased.values, label=label, color=palette[i % len(palette)], linewidth=1.5)

    ax.axhline(100, color="#30363d", linewidth=0.8, linestyle="--")
    ax.set_title(title, color="white", fontsize=11)
    ax.set_ylabel("Rebased to 100", color="white", fontsize=8)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#30363d")
    ax.legend(facecolor="#161b22", edgecolor="#30363d", labelcolor="white", fontsize=8, loc="best")
    ax.grid(color="#21262d", linewidth=0.5)
    fig.subplots_adjust(left=0.08, right=0.97, top=0.92, bottom=0.1)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()

# --- ANALYTICS & INSTITUTIONAL INDICATORS ---
def calculate_correlation(btc_prices, spy_prices):
    if not btc_prices or not spy_prices or len(btc_prices) != len(spy_prices): return 1.0
    df = pd.DataFrame({'BTC': btc_prices, 'SPY': spy_prices})
    return df['BTC'].corr(df['SPY'])

def get_trend_alignment(symbol, td_api_key, interval="1h"):
    url = f"https://api.twelvedata.com/supertrend?symbol={symbol}&interval={interval}&apikey={td_api_key}"
    try:
        response = requests.get(url, timeout=10).json()
        if "values" not in response or not response["values"]:
            return "NEUTRAL (Market Closed)", True
        latest = response['values'][0]
        curr_price = float(latest.get('close', 0))
        trend_val = float(latest.get('supertrend', 0))
        if curr_price == 0 or trend_val == 0: return "NEUTRAL", True
        is_bullish = curr_price > trend_val
        status = "🟢 BULLISH ALIGNMENT" if is_bullish else "🔴 BEARISH PRESSURE"
        return status, is_bullish
    except Exception as e:
        logger.error(f"Trend alignment computation failed for {symbol}: {e}")
        return "NEUTRAL", True

def get_institutional_conviction(symbol, td_api_key):
    url = f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={td_api_key}"
    try:
        r = requests.get(url, timeout=10).json()
        stats = r.get("statistics", {})
        vol = int(stats.get("volume", 0))
        avg_vol = int(stats.get("avg_volume_30_days", 1))
        
        if vol > (avg_vol * 1.5):
            return "⚡ HIGH (Whale Inflow)", 0x2ecc71, True
        return "NORMAL", 0x95a5a6, False
    except Exception as e:
        logger.error(f"Institutional conviction scan failed for {symbol}: {e}")
        return "NORMAL", 0x95a5a6, False

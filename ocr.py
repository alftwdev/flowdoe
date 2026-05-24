# visual_engine.py
import io
import matplotlib
matplotlib.use('Agg') # Crucial for headless cloud environments
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image, ImageDraw, ImageFont, ImageFilter

def create_radar_chart(ticker, rsi, trend, conviction, volume):
    """Generates a matplotlib radar chart and saves it to a byte buffer."""
    labels = np.array(['RSI Health', 'Trend Alignment', 'Inst. Conviction', 'Volume Surge'])
    stats = np.array([rsi, trend, conviction, volume])
    
    angles = np.linspace(0, 2*np.pi, len(labels), endpoint=False)
    stats = np.concatenate((stats,[stats[0]]))
    angles = np.concatenate((angles,[angles[0]]))
    
    fig, ax = plt.subplots(figsize=(4, 4), subplot_kw=dict(polar=True))
    fig.patch.set_alpha(0.0) # Transparent background
    ax.set_facecolor('#1e1e1e')
    
    ax.plot(angles, stats, color='#00ff00', linewidth=2)
    ax.fill(angles, stats, color='#00ff00', alpha=0.25)
    
    ax.set_thetagrids(angles[:-1] * 180/np.pi, labels, color='white', fontsize=10)
    ax.grid(color='#555555')
    ax.spines['polar'].set_color('#333333')
    
    # Save to memory buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', transparent=True, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return Image.open(buf)

def generate_essentials_payload(ticker, verdict_text, data_matrix):
    """Compiles the final image with branding, charts, and OCR defense."""
    # 1. Base Canvas (Dark Mode Institutional Vibe)
    width, height = 800, 500
    canvas = Image.new('RGB', (width, height), color='#121212')
    draw = ImageDraw.Draw(canvas)
    
    # 2. Fonts (Fallback to default if custom TTF is not uploaded)
    try:
        font_main = ImageFont.truetype("arialbd.ttf", 64)
        font_sub = ImageFont.truetype("arial.ttf", 24)
        font_data = ImageFont.truetype("courbd.ttf", 18)
    except IOError:
        font_main = ImageFont.load_default()
        font_sub = ImageFont.load_default()
        font_data = ImageFont.load_default()

    # 3. Branding: "ESSENTIALS / FEAR OF MISSING OUT"
    # We apply this as a semi-transparent background watermark
    watermark_layer = Image.new('RGBA', (width, height), (0,0,0,0))
    w_draw = ImageDraw.Draw(watermark_layer)
    w_draw.text((width//2, height//2 - 40), "ESSENTIALS", fill=(255,255,255, 25), font=font_main, anchor="mm")
    w_draw.text((width//2, height//2 + 20), "FEAR OF MISSING OUT", fill=(255,255,255, 25), font=font_sub, anchor="mm")
    watermark_layer = watermark_layer.rotate(15) # Diagonal tilt to break OCR bounding boxes
    canvas.paste(watermark_layer, (0,0), watermark_layer)

    # 4. Anti-OCR Noise Generation
    noise = np.random.randint(0, 30, (height, width, 3), dtype=np.uint8)
    noise_img = Image.fromarray(noise, 'RGB')
    canvas = Image.blend(canvas, noise_img, alpha=0.1)
    draw = ImageDraw.Draw(canvas) # Re-init draw after blend

    # 5. Header Overlay
    draw.text((30, 30), f"TICKER: {ticker}", fill="#FFFFFF", font=font_main)
    draw.text((30, 100), f"VERDICT: {verdict_text}", fill="#00FF00" if "PROCEED" in verdict_text else "#FF0000", font=font_sub)

    # 6. Insert Dynamic Chart
    # Normalized mock values based on data_matrix inputs (0-100 scale)
    chart_img = create_radar_chart(ticker, data_matrix['rsi_score'], 80, 90, 75) 
    canvas.paste(chart_img, (400, 80), chart_img)

    # 7. Print Matrix Data on Canvas
    matrix_text = (
        f"DATA MATRIX\n"
        f"-------------------\n"
        f"Spot Price: ${data_matrix['price']:.2f}\n"
        f"RSI (1D):   {data_matrix['rsi']:.1f}\n"
        f"Trend:      {data_matrix['trend']}\n"
        f"Regime:     {data_matrix['regime']}\n"
        f"VIX State:  {data_matrix['vix']}\n"
    )
    draw.text((30, 180), matrix_text, fill="#CCCCCC", font=font_data)

    # 8. Export to Bytes for Discord (No Disk Writing)
    final_buffer = io.BytesIO()
    canvas.save(final_buffer, format="PNG")
    final_buffer.seek(0)
    return final_buffer

def generate_generic_payload(title, description_text):
    """Compiles a non-ticker specific alert into a branded, OCR-resistant image."""
    width, height = 800, 500
    canvas = Image.new('RGB', (width, height), color='#121212')
    draw = ImageDraw.Draw(canvas)
    
    try:
        font_main = ImageFont.truetype("arialbd.ttf", 36)
        font_data = ImageFont.truetype("courbd.ttf", 18)
    except IOError:
        font_main = ImageFont.load_default()
        font_data = ImageFont.load_default()

    # Branding Watermark
    watermark_layer = Image.new('RGBA', (width, height), (0,0,0,0))
    w_draw = ImageDraw.Draw(watermark_layer)
    w_draw.text((width//2, height//2), "ESSENTIALS", fill=(255,255,255, 15), font=font_main, anchor="mm")
    watermark_layer = watermark_layer.rotate(15)
    canvas.paste(watermark_layer, (0,0), watermark_layer)

    # Anti-OCR Noise
    noise = np.random.randint(0, 30, (height, width, 3), dtype=np.uint8)
    noise_img = Image.fromarray(noise, 'RGB')
    canvas = Image.blend(canvas, noise_img, alpha=0.1)
    draw = ImageDraw.Draw(canvas)

    # Header & Body
    draw.text((30, 30), title, fill="#FFFFFF", font=font_main)
    
    # Handle text wrapping for description
    margin, offset = 30, 100
    for line in description_text.split('\n'):
        draw.text((margin, offset), line, fill="#00FF00", font=font_data)
        offset += 25 # line spacing

    final_buffer = io.BytesIO()
    canvas.save(final_buffer, format="PNG")
    final_buffer.seek(0)
    return final_buffer


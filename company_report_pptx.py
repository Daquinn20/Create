"""
Company Report — PowerPoint generator.

Modeled directly on the ASML three-statement case-study pptx script the user
shared. Reuses the same palette, header band, KPI cards, flagged statement
tables, and colored callout boxes.

Consumes the same report_data dict that company_report_backend._build_report_v2
returns. Exposes: generate_pptx_report(report_data) -> BytesIO
"""
from __future__ import annotations

import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Palette — matches user's ASML sample script exactly
# ─────────────────────────────────────────────────────────────────────────────
NAVY      = RGBColor(5, 38, 84)
BLUE      = RGBColor(20, 82, 145)
PALE_BLUE = RGBColor(231, 239, 248)
LIGHT_BLUE = RGBColor(165, 198, 232)
SOFT_BLUE  = RGBColor(218, 229, 241)

GREEN   = RGBColor(220, 242, 224)
GREEN_D = RGBColor(32, 120, 61)

RED   = RGBColor(250, 224, 224)
RED_D = RGBColor(183, 47, 47)

AMBER   = RGBColor(252, 241, 210)
AMBER_D = RGBColor(164, 112, 16)

WHITE = RGBColor(255, 255, 255)
DARK  = RGBColor(28, 38, 52)
GRAY  = RGBColor(100, 111, 124)
GRID  = RGBColor(211, 220, 230)
ROW   = RGBColor(248, 250, 252)


def tone_colors(tone: str) -> Tuple[RGBColor, RGBColor]:
    """Return (bg_fill, accent) for a tone label."""
    if tone in ("pos", "positive", "buy", "green"):
        return GREEN, GREEN_D
    if tone in ("neg", "negative", "sell", "red"):
        return RED, RED_D
    if tone in ("warn", "warning", "hold", "amber", "yellow"):
        return AMBER, AMBER_D
    return RGBColor(247, 249, 252), GRAY


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers (self-contained — no dependency on v2)
# ─────────────────────────────────────────────────────────────────────────────
def fmt_money(v: Optional[float], decimals: int = 1) -> str:
    if v is None or v == 0:
        return "N/A"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "N/A"
    sign = "-" if n < 0 else ""
    a = abs(n)
    if a >= 1e12:
        return f"{sign}${a/1e12:.{decimals}f}T"
    if a >= 1e9:
        return f"{sign}${a/1e9:.{decimals}f}B"
    if a >= 1e6:
        return f"{sign}${a/1e6:.{decimals}f}M"
    if a >= 1e3:
        return f"{sign}${a/1e3:.{decimals}f}K"
    return f"{sign}${a:,.0f}"


def fmt_pct(v: Optional[float], decimals: int = 1) -> str:
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "N/A"
    if -1.5 < n < 1.5:
        n *= 100.0
    return f"{n:.{decimals}f}%"


def fmt_ratio(v: Optional[float], decimals: int = 1, suffix: str = "x") -> str:
    if v is None or v == 0:
        return "N/A"
    try:
        return f"{float(v):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return "N/A"


# ─────────────────────────────────────────────────────────────────────────────
# Core shape / text helpers (adapted from user's sample)
# ─────────────────────────────────────────────────────────────────────────────
def clear_slide(slide) -> None:
    for shape in list(slide.shapes):
        sp = shape._element
        sp.getparent().remove(sp)


def add_text(slide, x, y, w, h, txt,
             size=12, bold=False, italic=False,
             color=DARK, align=PP_ALIGN.LEFT,
             anchor=MSO_ANCHOR.MIDDLE, font_name=None):
    """Add a text box at (x, y) with size (w, h) in INCHES."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.clear()
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = Inches(0)
    tf.margin_right = Inches(0)
    tf.margin_top = Inches(0)
    tf.margin_bottom = Inches(0)

    p = tf.paragraphs[0]
    r = p.add_run()
    r.text = str(txt) if txt is not None else ""
    r.font.size = Pt(size)
    r.font.bold = bold
    r.font.italic = italic
    r.font.color.rgb = color
    if font_name:
        r.font.name = font_name
    p.alignment = align
    return tb


def add_rich_text(slide, x, y, w, h, runs,
                  align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.MIDDLE):
    """runs = list of dicts {text, size, bold, color, italic}. Multiple runs in one paragraph."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.clear()
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = Inches(0)
    tf.margin_right = Inches(0)
    tf.margin_top = Inches(0)
    tf.margin_bottom = Inches(0)
    p = tf.paragraphs[0]
    p.alignment = align
    for r_spec in runs:
        r = p.add_run()
        r.text = r_spec.get("text", "")
        r.font.size = Pt(r_spec.get("size", 11))
        r.font.bold = bool(r_spec.get("bold"))
        r.font.italic = bool(r_spec.get("italic"))
        r.font.color.rgb = r_spec.get("color", DARK)
    return tb


def add_paragraph_block(slide, x, y, w, h, paragraphs,
                        size=10, color=DARK, bullet=False,
                        line_spacing=1.15, para_space_after=6):
    """Multi-paragraph text block for body copy."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.clear()
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.TOP
    tf.margin_left = Inches(0)
    tf.margin_right = Inches(0)
    tf.margin_top = Inches(0)
    tf.margin_bottom = Inches(0)
    for i, para in enumerate(paragraphs):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        prefix = "• " if bullet else ""
        r = p.add_run()
        r.text = f"{prefix}{para}"
        r.font.size = Pt(size)
        r.font.color.rgb = color
        p.line_spacing = line_spacing
        p.space_after = Pt(para_space_after)
    return tb


def header_band(prs, slide, title: str, subtitle: str = "", kicker: str = "COMPANY REPORT"):
    """Navy header strip across the top."""
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = WHITE

    band = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, Inches(0.92))
    band.fill.solid()
    band.fill.fore_color.rgb = NAVY
    band.line.fill.background()

    add_text(slide, 0.45, 0.10, 6.0, 0.20, kicker,
             size=9, bold=True, color=LIGHT_BLUE)
    add_text(slide, 0.45, 0.29, 9.6, 0.44, title,
             size=22, bold=True, color=WHITE)
    if subtitle:
        add_text(slide, 9.95, 0.23, 3.2, 0.36, subtitle,
                 size=9, color=SOFT_BLUE, align=PP_ALIGN.RIGHT)


def kpi_card(slide, x: float, y: float, w: float,
             label: str, value: str, sub: str = "",
             value_color: RGBColor = NAVY, height: float = 0.95):
    """Rounded rectangle KPI card. Value color drives the visual tone."""
    box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        Inches(x), Inches(y), Inches(w), Inches(height))
    box.fill.solid()
    box.fill.fore_color.rgb = RGBColor(247, 249, 252)
    box.line.color.rgb = GRID
    box.line.width = Emu(6350)  # ~0.5pt

    add_text(slide, x + 0.14, y + 0.05, w - 0.28, 0.22,
             label, size=8.5, bold=True, color=GRAY)
    add_text(slide, x + 0.14, y + 0.28, w - 0.28, 0.36,
             value, size=17, bold=True, color=value_color)
    if sub:
        add_text(slide, x + 0.14, y + 0.68, w - 0.28, 0.22,
                 sub, size=8, color=GRAY)


def kpi_card_from_dict(slide, x, y, w, kpi: Dict[str, Any], height=0.95):
    tone = kpi.get("tone", "")
    _, accent = tone_colors(tone)
    value_color = accent if tone in ("pos", "neg", "warn") else NAVY
    kpi_card(slide, x, y, w,
             kpi.get("label", ""), kpi.get("value", "N/A"),
             kpi.get("sub", ""), value_color, height=height)


def kpi_row(slide, x_start: float, y: float, total_width: float,
            kpis: List[Dict[str, Any]], gap: float = 0.15, height: float = 0.95):
    n = len(kpis)
    if n == 0:
        return
    card_w = (total_width - gap * (n - 1)) / n
    for i, k in enumerate(kpis):
        kpi_card_from_dict(slide, x_start + i * (card_w + gap), y, card_w, k, height=height)


def _add_cell_with_bold(cell, text: str, size: float = 9.2,
                        color=DARK, bold_default: bool = False,
                        align=None):
    """Populate a pptx table cell with **bold** markdown parsed into runs."""
    cell.text = ""
    p = cell.text_frame.paragraphs[0]
    if align is not None:
        p.alignment = align
    runs = _md_bold_runs(str(text) if text is not None else "",
                         base_size=size, color=color)
    if not runs:
        r = p.add_run()
        r.text = ""
        r.font.size = Pt(size)
        r.font.color.rgb = color
        r.font.bold = bold_default
        return
    for r_spec in runs:
        r = p.add_run()
        r.text = r_spec["text"]
        r.font.size = Pt(r_spec["size"])
        r.font.bold = bool(r_spec["bold"] or bold_default)
        r.font.color.rgb = r_spec["color"]


def statement_table(slide, x: float, y: float, w: float, h: float,
                    headers: List[str], rows: List[Dict[str, Any]],
                    first_col_frac: float = 0.44,
                    header_size: float = 9.5,
                    cell_size: float = 9.2,
                    tight: bool = False):
    """
    Financial statement table with flag-driven row shading.
    rows = [{label, cells, flag ('pos'|'neg'|'warn'|None), bold}]
    tight=True reduces cell margins for dense deep-dive tables.
    """
    if not rows:
        return None
    ncols = len(headers)
    nrows = 1 + len(rows)
    tbl_shape = slide.shapes.add_table(
        nrows, ncols, Inches(x), Inches(y), Inches(w), Inches(h))
    table = tbl_shape.table

    # Column widths
    table.columns[0].width = Inches(w * first_col_frac)
    other_w = w * (1 - first_col_frac) / max(1, ncols - 1)
    for c in range(1, ncols):
        table.columns[c].width = Inches(other_w)

    m_side = Inches(0.05 if tight else 0.08)
    m_vert = Inches(0.02 if tight else 0.03)
    m_vert_hdr = Inches(0.03 if tight else 0.04)

    # Header row
    for c, hdr in enumerate(headers):
        cell = table.cell(0, c)
        cell.text = ""
        cell.fill.solid()
        cell.fill.fore_color.rgb = NAVY
        cell.margin_left = m_side
        cell.margin_right = m_side
        cell.margin_top = m_vert_hdr
        cell.margin_bottom = m_vert_hdr
        p = cell.text_frame.paragraphs[0]
        r = p.add_run()
        r.text = str(hdr) if hdr else ""
        r.font.size = Pt(header_size)
        r.font.bold = True
        r.font.color.rgb = WHITE
        p.alignment = PP_ALIGN.LEFT if c == 0 else PP_ALIGN.RIGHT

    # Body rows
    for r_idx, row in enumerate(rows, start=1):
        label = row.get("label", "")
        cells = row.get("cells") or []
        flag = row.get("flag")
        bold = bool(row.get("bold"))
        for c in range(ncols):
            cell = table.cell(r_idx, c)
            cell.fill.solid()
            if flag in ("pos", "neg", "warn") and c >= 1:
                bg, _ = tone_colors(flag)
                cell.fill.fore_color.rgb = bg
            else:
                cell.fill.fore_color.rgb = WHITE if r_idx % 2 else ROW
            cell.margin_left = m_side
            cell.margin_right = m_side
            cell.margin_top = m_vert
            cell.margin_bottom = m_vert
            if c == 0:
                value = label
            else:
                idx = c - 1
                value = cells[idx] if idx < len(cells) else ""
            align = PP_ALIGN.LEFT if c == 0 else PP_ALIGN.RIGHT
            _add_cell_with_bold(cell, value, size=cell_size, color=DARK,
                                bold_default=bold, align=align)
    return table


def callout_box(slide, x: float, y: float, w: float, h: float,
                title: str, body: str, kind: str = "pos"):
    """Rounded colored box with uppercase title + body. Left accent border."""
    fill, accent = tone_colors(kind)

    box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        Inches(x), Inches(y), Inches(w), Inches(h))
    box.fill.solid()
    box.fill.fore_color.rgb = fill
    box.line.color.rgb = accent
    box.line.width = Emu(9525)  # ~0.75pt

    # Accent left bar
    bar = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(x), Inches(y), Inches(0.06), Inches(h))
    bar.fill.solid()
    bar.fill.fore_color.rgb = accent
    bar.line.fill.background()

    add_text(slide, x + 0.18, y + 0.10, w - 0.30, 0.22,
             (title or "").upper(), size=10, bold=True, color=accent,
             anchor=MSO_ANCHOR.TOP)
    # Body
    tb = slide.shapes.add_textbox(
        Inches(x + 0.18), Inches(y + 0.36),
        Inches(w - 0.32), Inches(h - 0.44))
    tf = tb.text_frame
    tf.clear()
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.TOP
    tf.margin_left = Inches(0)
    tf.margin_right = Inches(0)
    tf.margin_top = Inches(0)
    tf.margin_bottom = Inches(0)
    p = tf.paragraphs[0]
    r = p.add_run()
    r.text = body or ""
    r.font.size = Pt(9)
    r.font.color.rgb = DARK
    p.line_spacing = 1.2


def source_footer(slide, y: float, text: str,
                  prs_width_in: float = 13.333):
    """Small muted source line at bottom of slide area."""
    add_text(slide, 0.45, y, prs_width_in - 0.9, 0.20, text,
             size=8, italic=True, color=GRAY)


# ─────────────────────────────────────────────────────────────────────────────
# Slide-level page number footer
# ─────────────────────────────────────────────────────────────────────────────
def add_footer(prs, slide, page_num: int, company: str, symbol: str):
    """Thin footer at slide bottom."""
    w_in = prs.slide_width / 914400  # EMU → inches
    h_in = prs.slide_height / 914400
    y = h_in - 0.28
    # Divider line
    line = slide.shapes.add_connector(1, Inches(0.45), Inches(y), Inches(w_in - 0.45), Inches(y))
    line.line.color.rgb = GRID
    line.line.width = Emu(6350)
    add_text(slide, 0.45, y + 0.02, 5.0, 0.20,
             f"{company} ({symbol})", size=8, color=GRAY)
    add_text(slide, w_in - 3.5, y + 0.02, 3.0, 0.20,
             f"Page {page_num} · Generated {datetime.now().strftime('%b %d, %Y')}",
             size=8, color=GRAY, align=PP_ALIGN.RIGHT)


# ─────────────────────────────────────────────────────────────────────────────
# Matplotlib chart → PNG bytes (embedded into slides)
# ─────────────────────────────────────────────────────────────────────────────
def _chart_price_line(dates: List[str], closes: List[float],
                      width_in: float = 6.5, height_in: float = 2.2) -> Optional[io.BytesIO]:
    if not dates or not closes:
        return None
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    fig, ax = plt.subplots(figsize=(width_in, height_in))
    ax.plot(range(len(closes)), closes, color="#052654", linewidth=1.8)
    ax.fill_between(range(len(closes)), closes, min(closes),
                    color="#052654", alpha=0.10)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#D3DCE6")
    ax.tick_params(colors="#646F7C", labelsize=8)
    ax.grid(axis="y", color="#D3DCE6", linewidth=0.6, alpha=0.9)
    ax.set_axisbelow(True)
    if len(dates) > 6:
        step = max(1, len(dates) // 6)
        ticks = list(range(0, len(dates), step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([dates[i][:7] for i in ticks], rotation=0)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _p: f"${x:,.0f}"))
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=180, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf


def _chart_margins_line(periods: List[str],
                        gross: List[Optional[float]],
                        operating: List[Optional[float]],
                        net: List[Optional[float]],
                        n_actual: int,
                        width_in: float = 6.5,
                        height_in: float = 2.4) -> Optional[io.BytesIO]:
    if not periods:
        return None
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    fig, ax = plt.subplots(figsize=(width_in, height_in))
    xs = list(range(len(periods)))

    def _plot(series, color, label):
        act_x = xs[:n_actual]
        act_y = series[:n_actual]
        ax.plot(act_x, act_y, color=color, linewidth=1.8, label=label,
                marker="o", markersize=3.5)
        if n_actual < len(series):
            fwd_x = xs[n_actual - 1:]
            fwd_y = series[n_actual - 1:]
            ax.plot(fwd_x, fwd_y, color=color, linewidth=1.6, linestyle="--",
                    marker="o", markersize=3.5, alpha=0.85)

    _plot(gross, "#052654", "Gross")
    _plot(operating, "#145291", "Operating")
    _plot(net, "#20783D", "Net")

    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#D3DCE6")
    ax.tick_params(colors="#646F7C", labelsize=8.5)
    ax.grid(axis="y", color="#D3DCE6", linewidth=0.6, alpha=0.9)
    ax.set_axisbelow(True)
    ax.set_xticks(xs)
    ax.set_xticklabels(periods, rotation=0, fontsize=8)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _p: f"{x:.0f}%"))
    ax.legend(loc="best", frameon=False, fontsize=8, ncol=3)
    if n_actual < len(periods):
        ax.axvspan(n_actual - 0.5, len(periods) - 0.5,
                   color="#EEF2F7", alpha=0.6, zorder=0)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=180, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf


# ─────────────────────────────────────────────────────────────────────────────
# Data-fetch helpers (lazy backend import — same pattern as v2)
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_price_history(symbol: str, years: int = 5) -> Tuple[List[str], List[float]]:
    try:
        from company_report_backend import fmp_get
    except Exception:
        return [], []
    try:
        raw = fmp_get(f"historical-price-full/{symbol}", {"timeseries": years * 260})
    except Exception:
        return [], []
    if not raw or "historical" not in raw:
        return [], []
    bars = list(reversed(raw["historical"]))
    sampled = bars[::5]  # weekly
    return ([b.get("date", "") for b in sampled],
            [float(b.get("close", 0) or 0) for b in sampled])


def _fetch_peer_metrics(peers: List[Dict[str, Any]], max_peers: int = 5) -> List[Dict[str, Any]]:
    try:
        from company_report_backend import fmp_get
    except Exception:
        return peers[:max_peers]
    from datetime import datetime as _dt
    enriched = []
    for peer in peers[:max_peers]:
        sym = peer.get("symbol") or peer.get("ticker")
        if not sym:
            continue
        row = {"symbol": sym, "name": peer.get("name", sym),
               "market_cap": peer.get("market_cap"),
               "fwd_pe": None, "rev_growth_ttm": None, "op_margin": None}
        try:
            km = fmp_get(f"key-metrics-ttm/{sym}") or []
            if km:
                row["fwd_pe"] = km[0].get("peRatioTTM")
            ratios = fmp_get(f"ratios-ttm/{sym}") or []
            if ratios:
                op = ratios[0].get("operatingProfitMarginTTM")
                if op is not None:
                    row["op_margin"] = float(op) * 100.0
            inc = fmp_get(f"income-statement/{sym}", {"limit": 2}) or []
            if len(inc) >= 2:
                curr, prev = inc[0].get("revenue"), inc[1].get("revenue")
                if curr and prev:
                    row["rev_growth_ttm"] = (curr - prev) / abs(prev) * 100.0
        except Exception:
            pass
        enriched.append(row)
    return enriched


def _rank_risks(company_flags: List[str], general_risks: List[str],
                symbol: str, top_n: int = 5) -> List[Dict[str, str]]:
    combined = [r for r in (company_flags or []) if r] + [r for r in (general_risks or []) if r]
    if not combined:
        return []

    def _fallback():
        out = []
        for r in combined[:top_n]:
            parts = r.split(":", 1)
            if len(parts) == 2:
                out.append({"title": parts[0].strip(), "detail": parts[1].strip()})
            else:
                words = r.split()
                out.append({"title": " ".join(words[:6]).rstrip(".,;:"), "detail": r})
        return out

    try:
        from company_report_backend import anthropic_client
    except Exception:
        return _fallback()
    if anthropic_client is None:
        return _fallback()

    bullets = "\n".join(f"- {r}" for r in combined[:20])
    prompt = f"""You are ranking the most important investment risks for {symbol}.

Raw risk list:
{bullets}

Return exactly {top_n} risks, ranked most-to-least material. Deduplicate similar items.
Drop generic boilerplate unless it materially affects THIS company.

Format each risk as:
[N] TITLE: <2-6 word headline>
DETAIL: <one concrete sentence>

Nothing else."""
    try:
        resp = anthropic_client.messages.create(
            model="claude-sonnet-4-6", max_tokens=1200,
            messages=[{"role": "user", "content": prompt}])
        text = resp.content[0].text if resp.content else ""
    except Exception as e:
        logger.warning(f"risk ranking failed for {symbol}: {e}")
        return _fallback()

    parsed = []
    current = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("[") and "TITLE:" in line:
            if current.get("title"):
                parsed.append(current)
                current = {}
            current["title"] = line.split("TITLE:", 1)[1].strip()
        elif line.upper().startswith("DETAIL:"):
            current["detail"] = line.split(":", 1)[1].strip()
    if current.get("title"):
        parsed.append(current)
    parsed = [p for p in parsed if p.get("title") and p.get("detail")]
    return parsed[:top_n] if parsed else _fallback()


def _clean(text: str) -> str:
    if not text:
        return ""
    return str(text).replace("**", "").strip("- ").strip()


def _trim_paras(text: str, max_paras: int = 20) -> List[str]:
    """Split text into non-empty paragraphs, cap at max_paras."""
    if not text:
        return []
    return [_clean(p) for p in text.split("\n\n") if p.strip()][:max_paras]


# ─────────────────────────────────────────────────────────────────────────────
# Deep-dive markdown parser
# ─────────────────────────────────────────────────────────────────────────────
import re

_HEADER_RE = re.compile(r"^##\s+(\d+)\.\s+(.+?)\s*$", re.MULTILINE)
_SUB_PART_RE = re.compile(r"^##\s+(\d+)\.\s+(.+?)\s*$", re.MULTILINE)


def extract_subagent_part(memo: str, part_num: int) -> Optional[Dict[str, Any]]:
    """Extract a ## N. section from a sub-agent memo.
    Returns {'title': str, 'paragraphs': [str], 'tables': [table]} or None."""
    if not memo:
        return None
    matches = list(_SUB_PART_RE.finditer(memo))
    for i, m in enumerate(matches):
        if int(m.group(1)) == part_num:
            start = m.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(memo)
            body = memo[start:end].strip()
            paragraphs, tables = _extract_paragraphs_and_tables(body)
            return {"title": m.group(2).strip(),
                    "paragraphs": paragraphs, "tables": tables, "body": body}
    return None


def parse_deep_dive_sections(markdown_text: str) -> Dict[int, Dict[str, Any]]:
    """Split synthesis.analysis markdown by '## N. Title' headers.

    Returns {section_num: {'title': str, 'body': str, 'paragraphs': [str], 'tables': [table]}}
    where table = {'headers': [str], 'rows': [[str]]}.
    """
    if not markdown_text:
        return {}
    sections: Dict[int, Dict[str, Any]] = {}
    matches = list(_HEADER_RE.finditer(markdown_text))
    for i, m in enumerate(matches):
        num = int(m.group(1))
        title = m.group(2).strip()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(markdown_text)
        body = markdown_text[start:end].strip()
        paragraphs, tables = _extract_paragraphs_and_tables(body)
        sections[num] = {
            "title": title,
            "body": body,
            "paragraphs": paragraphs,
            "tables": tables,
        }
    return sections


def _preserve_md_clean(text: str) -> str:
    """Like _clean but PRESERVES **bold** markers — needed for deep-dive prose."""
    if not text:
        return ""
    return str(text).strip("- ").strip()


def _extract_paragraphs_and_tables(body: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Separate markdown body into prose paragraphs and structured tables.
    Preserves **bold** markup so the renderer can style important terms."""
    lines = body.split("\n")
    paragraphs: List[str] = []
    tables: List[Dict[str, Any]] = []
    buffer: List[str] = []
    i = 0

    def _flush_paragraph():
        if buffer:
            text = " ".join(x.strip() for x in buffer if x.strip())
            if text:
                paragraphs.append(_preserve_md_clean(text))
            buffer.clear()

    while i < len(lines):
        line = lines[i]
        # H3 sub-header: promote to bold-lead paragraph
        h3_match = re.match(r"^###\s+(.+?)\s*$", line)
        if h3_match:
            _flush_paragraph()
            paragraphs.append(f"**{h3_match.group(1).strip()}**")
            i += 1
            continue
        # Detect markdown table start: header line with | then divider line with dashes
        if "|" in line and i + 1 < len(lines) and re.match(r"^\s*\|?[\s\-|:]+\|?\s*$", lines[i + 1]):
            _flush_paragraph()
            header = [c.strip() for c in line.strip().strip("|").split("|")]
            i += 2  # skip divider
            rows = []
            while i < len(lines) and "|" in lines[i]:
                row = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                while len(row) < len(header):
                    row.append("")
                rows.append(row[:len(header)])
                i += 1
            if header and rows:
                tables.append({"headers": header, "rows": rows})
            continue
        # Blank line = paragraph break
        if not line.strip():
            _flush_paragraph()
        else:
            buffer.append(line)
        i += 1
    _flush_paragraph()
    return paragraphs, tables


# ─────────────────────────────────────────────────────────────────────────────
# Deep-dive slide helpers
# ─────────────────────────────────────────────────────────────────────────────
def _md_bold_runs(text: str, base_size: float = 10, color=None) -> List[Dict[str, Any]]:
    """Parse **bold** markdown into add_rich_text runs."""
    if color is None:
        color = DARK
    parts = re.split(r"(\*\*[^\*]+\*\*)", text)
    runs = []
    for p in parts:
        if not p:
            continue
        if p.startswith("**") and p.endswith("**"):
            runs.append({"text": p[2:-2], "size": base_size, "bold": True, "color": color})
        else:
            runs.append({"text": p, "size": base_size, "bold": False, "color": color})
    return runs


def _paragraph_block_bold(slide, x, y, w, h, paragraphs,
                          size=10, color=DARK, line_spacing=1.3,
                          para_space_after=6, bullet=False):
    """Multi-paragraph block that handles inline **bold**."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.clear()
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.TOP
    tf.margin_left = Inches(0)
    tf.margin_right = Inches(0)
    tf.margin_top = Inches(0)
    tf.margin_bottom = Inches(0)
    for i, para in enumerate(paragraphs):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        p.line_spacing = line_spacing
        p.space_after = Pt(para_space_after)
        prefix = "• " if bullet else ""
        text = f"{prefix}{para}"
        runs = _md_bold_runs(text, base_size=size, color=color)
        for r_spec in runs:
            r = p.add_run()
            r.text = r_spec["text"]
            r.font.size = Pt(r_spec["size"])
            r.font.bold = r_spec["bold"]
            r.font.color.rgb = r_spec["color"]
    return tb


def _md_table_to_pptx(slide, x, y, w, max_h, table: Dict[str, Any],
                       first_col_frac: float = 0.30,
                       expand_to_fill: bool = False,
                       header_size: float = 8.0,
                       cell_size: float = 7.5):
    """Render a parsed markdown table as a pptx statement_table (no flag colors).

    expand_to_fill: if True, stretch the table to consume max_h regardless of
    row count (avoids a compact table sitting in the middle of a blank slide).
    """
    headers = table.get("headers") or []
    rows_raw = table.get("rows") or []
    if not headers or not rows_raw:
        return None
    rows = []
    for r in rows_raw:
        label = r[0] if r else ""
        cells = r[1:]
        # Try to detect a "Severity 5" or high-risk row for tone
        flag = None
        joined = " ".join(str(c) for c in r).lower()
        if any(k in joined for k in ("bear", "sell", "avoid", "high risk", "severe")):
            flag = "neg"
        elif any(k in joined for k in ("bull", "buy", "creating value", "strong")):
            flag = "pos"
        rows.append({
            "label": label, "cells": cells, "flag": flag, "bold": False,
        })
    if expand_to_fill:
        h = max_h
        # Scale font up when rows are tall enough to warrant it
        nrows_total = 1 + len(rows)
        per_row = max_h / max(1, nrows_total)
        if per_row >= 0.55:
            cell_size = max(cell_size, 10.5)
            header_size = max(header_size, 11.0)
        elif per_row >= 0.42:
            cell_size = max(cell_size, 9.5)
            header_size = max(header_size, 10.0)
    else:
        row_h_est = 0.26
        h = min(max_h, 0.32 + row_h_est * len(rows))
    return statement_table(slide, x, y, w, h, headers, rows,
                           first_col_frac=first_col_frac,
                           header_size=header_size, cell_size=cell_size,
                           tight=not expand_to_fill)


def _compute_ttm(quarterly_data: List[Dict[str, Any]]) -> Optional[float]:
    """Sum trailing 4 quarters of revenue.  Returns None if <4 quarters."""
    if not quarterly_data or len(quarterly_data) < 4:
        return None
    total = 0.0
    for q in quarterly_data[:4]:
        r = q.get("revenue")
        if r is None:
            return None
        try:
            total += float(r)
        except (TypeError, ValueError):
            return None
    return total


def _parse_segments_from_ai_analysis(text: str) -> List[Dict[str, Any]]:
    """Parse the 'Segment Breakdown' block of an AI segment memo.
    Returns [{name, revenue_b, pct_total, is_sub}, ...] in display order.
    Handles two output shapes the AI has been observed to use:
      (a) markdown table rows: | Name | $XX.XB | YY.Y% | +ZZ% |
      (b) prose bullets:       **Name: $XX.XB quarterly (YY.Y% of total)**
                               - Sub Name: $XX.XB (YY.Y%)
    """
    if not text:
        return []
    # Prefer the "Quarterly Snapshot" region — it's presented as a markdown
    # table with clean $ amounts + %s + YoY.  Fall back to "Segment Breakdown"
    # (usually prose, with approximate "~$X" figures) only if no snapshot exists.
    lower = text.lower()
    start = -1
    for anchor in ("quarterly snapshot", "segment breakdown",
                   "segment revenue", "revenue by segment"):
        i = lower.find(anchor)
        if i >= 0:
            start = i
            break
    body = text[start:] if start >= 0 else text
    # Stop at the next H2 heading
    stop = re.search(r"\n---\s*\n|\n##\s+[^#]", body[100:])
    if stop:
        body = body[:100 + stop.start()]

    def _to_billions(v: float, unit: str) -> float:
        u = unit.upper()
        if u == "B":
            return v
        if u == "M":
            return v / 1000.0
        if u == "K":
            return v / 1_000_000.0
        return v

    results: List[Dict[str, Any]] = []
    seen_names = set()

    # ── (a) Markdown table rows ────────────────────────────────────────────
    # Row shape: | Name | $XX.XB | YY.Y% | (optional 4th col with YoY) |
    table_row_re = re.compile(
        r"^\s*\|\s*([^|]+?)\s*\|\s*\*{0,2}\$?\s*([\d.]+)\s*([BMK])\*{0,2}\s*\|"
        r"\s*\*{0,2}([\d.]+)\s*%\*{0,2}\s*\|"
        r"(?:\s*\*{0,2}([^|]*?)\*{0,2}\s*\|)?",
        re.IGNORECASE)
    yoy_extract_re = re.compile(r"([+\-]?\d+(?:\.\d+)?%|~?\d+(?:\.\d+)?[x×]?)", re.IGNORECASE)

    for line in body.splitlines():
        m = table_row_re.match(line)
        if not m:
            continue
        raw_name = m.group(1)
        # Strip markdown bold, arrow prefixes, dashes, whitespace
        name = re.sub(r"\*+", "", raw_name).strip()
        # Detect sub-item by leading arrow / indent marker
        is_sub = bool(re.match(r"^[\s]*[↳↪→⤷»]", name))
        clean_name = re.sub(r"^[↳↪→⤷»\s\-–—•*]+", "", name).strip()
        # Trim trailing bullet-adjacent dashes ("— AI Servers" → "AI Servers")
        clean_name = re.sub(r"^[\-–—]+\s*", "", clean_name).strip()
        if not clean_name or clean_name.lower() in ("segment", "metric", "category"):
            continue
        if clean_name.lower().startswith("total") and "dell" in clean_name.lower():
            continue
        if clean_name.lower() == "total":
            continue
        # Skip header separator (---) rows
        if set(clean_name) <= set("-: "):
            continue
        # Sub if the name is a KNOWN sub-segment word
        if not is_sub:
            sub_hints = ("ai server", "traditional server", "storage",
                          "commercial", "consumer", "networking")
            if any(h in clean_name.lower() for h in sub_hints) and "isg" not in clean_name.lower() and "csg" not in clean_name.lower():
                is_sub = True
        key = clean_name.lower()
        if key in seen_names:
            continue
        seen_names.add(key)

        # Optional 4th column → YoY / growth
        yoy_str = None
        col4 = m.group(5)
        if col4:
            col4_clean = col4.strip()
            ym = yoy_extract_re.search(col4_clean)
            if ym:
                v = ym.group(1)
                if "%" in v and not v.startswith(("+", "-")):
                    v = "+" + v
                yoy_str = v

        results.append({
            "name": clean_name,
            "revenue_b": _to_billions(float(m.group(2)), m.group(3)),
            "pct_total": float(m.group(4)),
            "is_sub": is_sub,
            "yoy": yoy_str,
        })

    if results:
        return results

    # ── (b) Prose bullet fallback (original format) ────────────────────────
    top_re = re.compile(
        r"\*\*([^:*]+?):\s*\$?\s*([\d.]+)\s*([BMK])\b[^*]*?\(([\d.]+)%",
        re.IGNORECASE)
    sub_re = re.compile(
        r"^\s*[-•]\s*([^:]+?):\s*\$?\s*([\d.]+)\s*([BMK])\b[^\(]*\(([\d.]+)%",
        re.IGNORECASE)
    for line in body.splitlines():
        m = top_re.search(line)
        if m:
            results.append({
                "name": m.group(1).strip(),
                "revenue_b": _to_billions(float(m.group(2)), m.group(3)),
                "pct_total": float(m.group(4)),
                "is_sub": False,
            })
            continue
        m = sub_re.match(line)
        if m and results:
            results.append({
                "name": m.group(1).strip(),
                "revenue_b": _to_billions(float(m.group(2)), m.group(3)),
                "pct_total": float(m.group(4)),
                "is_sub": True,
            })
    return results


def _build_decomposition_table(report: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Enriched segment table for the Business Model Decomposition slide.
    Columns: Segment | TTM $ | % of Total | Last Q $ | Notes (YoY / margin).
    Falls back to None if we don't have enough data to beat the deep-dive default.
    """
    rev = report.get("revenue_data", {}) or {}
    rh = report.get("recent_highlights", {}) or {}
    q_data = rh.get("quarterly_data") or []
    segs_meta = rev.get("segments") or []
    ai_text = ""
    for s in segs_meta:
        if isinstance(s, dict) and s.get("ai_analysis"):
            ai_text = s["ai_analysis"]
            break

    parsed = _parse_segments_from_ai_analysis(ai_text)
    if not parsed:
        return None

    ttm_total = _compute_ttm(q_data)
    if ttm_total is None:
        # Fall back to most recent full FY revenue
        hist = rev.get("historical_margins") or []
        annuals = [m for m in hist if str(m.get("period", "")).isdigit()
                   and len(str(m.get("period", ""))) == 4]
        if annuals:
            annuals = sorted(annuals, key=lambda m: str(m.get("period", "")))
            try:
                ttm_total = float(annuals[-1].get("revenue") or 0)
            except (TypeError, ValueError):
                ttm_total = None
    ttm_label = "TTM" if q_data and len(q_data) >= 4 else "FY Rev"
    ttm_period = ""
    if q_data and len(q_data) >= 4:
        ttm_period = f"({q_data[3].get('date','')} → {q_data[0].get('date','')})"

    latest_q = q_data[0] if q_data else {}
    latest_q_date = latest_q.get("date", "")
    try:
        latest_q_rev = float(latest_q.get("revenue") or 0)
    except (TypeError, ValueError):
        latest_q_rev = 0.0
    latest_q_label = latest_q.get("quarter") or (f"Latest Q ({latest_q_date})" if latest_q_date else "Last Q")

    # Extract YoY growth / operating margin per segment from the AI text.
    # Require the segment name to *lead* the line (after optional bullet) to
    # avoid mis-attributing numbers from adjacent bullets.
    yoy_map: Dict[str, str] = {}
    margin_map: Dict[str, str] = {}
    if ai_text:
        for line in ai_text.splitlines():
            stripped = re.sub(r"^\s*[-•*]\s*", "", line).lstrip("*").strip()
            head = stripped[:60].lower()
            for seg in parsed:
                key = seg["name"].split("(")[0].strip().lower()
                # Match on the first two words of the segment name so
                # "Traditional Servers" matches "Traditional Servers & Networking"
                short_key = " ".join(key.split()[:2])
                if not short_key or not head.startswith(short_key):
                    continue
                m = re.search(r"([+\-]?\d+(?:\.\d+)?)%\s*YoY", stripped)
                if m and seg["name"] not in yoy_map:
                    v = m.group(1)
                    if not v.startswith(("+", "-")):
                        v = "+" + v
                    yoy_map[seg["name"]] = f"{v}%"
                m2 = re.search(r"([\d.]+%)\s*(?:op(?:erating)?\s*margin|margin)", stripped, re.IGNORECASE)
                if m2 and seg["name"] not in margin_map:
                    margin_map[seg["name"]] = m2.group(1)
                break  # only credit the first (leading) segment on this line

        # Second pass — table rows in the memo like
        # "| Revenue Growth | **+89% YoY** |" apply to the *nearest preceding*
        # header (e.g. "### ISG Operating Performance:").  Walk once.
        current_header_seg = None
        for line in ai_text.splitlines():
            hm = re.match(r"^#{2,4}\s+(.+?)(?:\s+Operating\s+Performance)?:?\s*$", line, re.IGNORECASE)
            if hm:
                htext = hm.group(1).lower()
                for seg in parsed:
                    key = seg["name"].split("(")[0].strip().lower()
                    abbr = re.search(r"\(([^)]+)\)", seg["name"])
                    abbr_key = abbr.group(1).lower() if abbr else ""
                    if key and (key in htext or (abbr_key and abbr_key in htext)):
                        current_header_seg = seg["name"]
                        break
                continue
            if current_header_seg:
                if re.search(r"revenue\s*growth", line, re.IGNORECASE):
                    m = re.search(r"([+\-]?\d+(?:\.\d+)?)%\s*YoY", line)
                    if m and current_header_seg not in yoy_map:
                        v = m.group(1)
                        if not v.startswith(("+", "-")):
                            v = "+" + v
                        yoy_map[current_header_seg] = f"{v}%"
                if re.search(r"operating\s*margin", line, re.IGNORECASE):
                    m = re.search(r"\*?\*?([\d.]+)%\*?\*?", line)
                    if m and current_header_seg not in margin_map:
                        margin_map[current_header_seg] = m.group(1) + "%"

    # Assemble table
    def _fmt_b(v: Optional[float]) -> str:
        if v is None:
            return "—"
        if v >= 100:
            return f"${v:.0f}B"
        return f"${v:.1f}B"

    rows = []
    latest_q_total = sum(s["revenue_b"] for s in parsed if not s["is_sub"])
    if latest_q_total <= 0:
        latest_q_total = latest_q_rev / 1e9 if latest_q_rev else 0
    for seg in parsed:
        # TTM estimate: latest-quarter % of total applied to TTM total
        pct = seg["pct_total"]
        ttm_est_b = None
        if ttm_total:
            ttm_est_b = (pct / 100.0) * (ttm_total / 1e9)
        # Notes: prefer YoY from the parsed table row itself, fall back to
        # regex-parsed prose YoY, then to margin
        yoy = seg.get("yoy") or yoy_map.get(seg["name"], "")
        if yoy and "yoy" not in yoy.lower() and "%" in yoy:
            yoy_display = f"{yoy} YoY"
        elif yoy:
            yoy_display = yoy
        else:
            yoy_display = ""
        margin = margin_map.get(seg["name"], "")
        notes_bits = []
        if yoy_display:
            notes_bits.append(yoy_display)
        if margin:
            notes_bits.append(f"{margin} op margin")
        notes = "  ·  ".join(notes_bits) if notes_bits else "—"
        display_name = ("   ↳ " + seg["name"]) if seg["is_sub"] else seg["name"]
        rows.append([display_name,
                     _fmt_b(ttm_est_b),
                     f"{pct:.1f}%",
                     _fmt_b(seg["revenue_b"]),
                     notes])

    # Total row
    if ttm_total:
        rows.append(["Total",
                     _fmt_b(ttm_total / 1e9),
                     "100.0%",
                     _fmt_b(latest_q_total),
                     ""])

    headers = ["Segment",
               f"{ttm_label} Revenue",
               "% of Total",
               latest_q_label,
               "YoY / Margin"]
    return {
        "title": (f"Segment breakdown  ·  {ttm_period}" if ttm_period else "Segment breakdown"),
        "headers": headers,
        "rows": rows,
    }


def _chars_per_line(width_in: float, font_size_pt: float) -> int:
    """Approximate chars that fit on one line at a given width + font size."""
    chars_per_inch = 8.5 * (10.5 / max(1, font_size_pt))
    return max(20, int(width_in * chars_per_inch))


def _lines_per_slide(height_in: float, font_size_pt: float,
                     line_spacing: float) -> int:
    line_h_in = font_size_pt * line_spacing / 72.0
    return max(1, int(height_in / line_h_in))


def _paragraph_line_cost(text: str, chars_per_line: int,
                         para_gap_lines: float = 0.55) -> float:
    """How many 'lines' this paragraph consumes on the slide.
    Default para_gap_lines=0.55 matches ~6pt space_after at ~10pt font."""
    if not text:
        return 0.0
    stripped = text.replace("**", "")
    n = max(1, (len(stripped) + chars_per_line - 1) // chars_per_line)
    return n + para_gap_lines


def _fit_font_size(paragraphs: List[str], width_in: float, height_in: float,
                   base_size: float, line_spacing: float,
                   max_size: float) -> float:
    """Largest font size in [base_size, max_size] (0.5pt steps) that lets
    all paragraphs fit within height_in at width_in."""
    if not paragraphs:
        return base_size
    best = base_size
    steps = range(int(base_size * 2), int(max_size * 2) + 1)
    for s2 in steps:
        size = s2 / 2.0
        cpl = _chars_per_line(width_in, size)
        cost = sum(_paragraph_line_cost(p, cpl) for p in paragraphs)
        avail = _lines_per_slide(height_in, size, line_spacing)
        if cost <= avail:
            best = size
        else:
            break
    return best


def _split_paragraphs(paragraphs: List[str],
                      lines_per_slide: int,
                      chars_per_line: int,
                      first_slide_budget: Optional[int] = None
                      ) -> List[List[str]]:
    """Split paragraphs into consecutive groups that each fit in the line budget.
    Optionally the first slide has a smaller budget (e.g. because it also has a table)."""
    groups: List[List[str]] = []
    current: List[str] = []
    used = 0.0
    budget = first_slide_budget if (first_slide_budget is not None and not groups) else lines_per_slide
    for para in paragraphs:
        cost = _paragraph_line_cost(para, chars_per_line)
        if current and (used + cost > budget):
            groups.append(current)
            current = [para]
            used = cost
            budget = lines_per_slide  # continuations use full budget
        else:
            current.append(para)
            used += cost
    if current:
        groups.append(current)
    return groups


def _adaptive_group_and_size(paragraphs: List[str],
                             width_in: float,
                             height_in: float,
                             line_spacing: float = 1.30,
                             min_size: float = 9.0,
                             max_size: float = 12.0,
                             first_slide_budget_frac: Optional[float] = None
                             ) -> Tuple[List[List[str]], float]:
    """Per-section adaptive layout: pick the *fewest* slides and the *largest*
    professional font size that lets the content fit, and avoid orphan
    continuation slides (last slide < ~35% full while others are near full).

    Returns (groups, font_size).  Empty paragraphs are dropped so we never emit
    a blank continuation slide.

    first_slide_budget_frac: if the first slide shares vertical space with
    another element (e.g. a side table), pass e.g. 1.0 for no shrink or 0.6
    for 60% of the full budget on slide 1.
    """
    paragraphs = [p for p in paragraphs if p and p.strip()]
    if not paragraphs:
        return [], max_size

    def _try(size: float) -> List[List[str]]:
        cpl = _chars_per_line(width_in, size)
        budget = _lines_per_slide(height_in, size, line_spacing)
        first = int(budget * first_slide_budget_frac) if first_slide_budget_frac else None
        return _split_paragraphs(paragraphs, budget, cpl, first_slide_budget=first)

    sizes_desc = [s / 2.0 for s in range(int(max_size * 2), int(min_size * 2) - 1, -1)]

    # For each target slide count N (ascending), find the largest size that fits.
    for n_target in range(1, len(paragraphs) + 1):
        for size in sizes_desc:
            groups = _try(size)
            if len(groups) <= n_target:
                # Anti-orphan: if the last group is much emptier than the others
                # and dropping the font one notch would collapse into fewer slides,
                # prefer the tighter layout.
                if len(groups) > 1:
                    cpl = _chars_per_line(width_in, size)
                    budget = _lines_per_slide(height_in, size, line_spacing)
                    costs = [sum(_paragraph_line_cost(p, cpl) for p in g) for g in groups]
                    last_cost = costs[-1]
                    peer_max = max(costs[:-1]) if len(costs) > 1 else last_cost
                    if last_cost < budget * 0.35 and last_cost < peer_max * 0.5:
                        # Try progressively smaller sizes to collapse slide count.
                        for size2 in sizes_desc:
                            if size2 >= size:
                                continue
                            groups2 = _try(size2)
                            if len(groups2) < len(groups):
                                return groups2, size2
                return groups, size
        # else this n_target unreachable at any size — try a bigger N
    # Fallback
    return [paragraphs], min_size


def _parse_five_forces(part_body: str) -> List[Dict[str, str]]:
    """Parse Porter's Five Forces from the competitive_position sub-agent Part 1.
    Looks for ### Force Name — **RATING** patterns and grabs following prose."""
    if not part_body:
        return []
    # Split by ### headers
    chunks = re.split(r"(?=^###\s)", part_body, flags=re.MULTILINE)
    forces = []
    for chunk in chunks:
        m = re.match(r"^###\s+(.+?)\s*$", chunk, re.MULTILINE)
        if not m:
            continue
        header = m.group(1).strip()
        # Header looks like "Rivalry — **HIGH and INTENSIFYING**"
        # Split on em-dash / dash to get force name and rating
        parts = re.split(r"\s*[—–\-]\s*", header, maxsplit=1)
        force_name = parts[0].strip()
        rating = parts[1].strip() if len(parts) > 1 else ""
        rating_clean = rating.replace("**", "").strip()
        body_text = chunk[m.end():].strip()
        # Truncate body to first 2 sentences or ~280 chars
        sentences = re.split(r"(?<=[\.\?\!])\s+", body_text)
        summary = " ".join(sentences[:2])[:320]
        # Determine tone from rating text
        rating_upper = rating_clean.upper()
        if "HIGH" in rating_upper and ("INTENSIF" in rating_upper or "RISING" in rating_upper):
            tone = "neg"
        elif "HIGH" in rating_upper:
            tone = "warn"
        elif "LOW" in rating_upper:
            tone = "pos"
        elif "MEDIUM" in rating_upper:
            tone = "warn"
        else:
            tone = "warn"
        forces.append({
            "name": force_name,
            "rating": rating_clean,
            "summary": summary,
            "tone": tone,
        })
        if len(forces) >= 5:
            break
    return forces


def slide_five_forces(prs, forces: List[Dict[str, str]],
                     company: str, symbol: str) -> None:
    """Slide showing Porter's Five Forces as a 5-card row with rating + summary."""
    if not forces:
        return
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Competitive Position — Porter's Five Forces",
                subtitle=f"{company}  ·  {symbol}  ·  Industry structure",
                kicker="SECTION 4 — STRATEGIC DEEP DIVE")

    # 5-card row, each about 2.35" wide with 0.15 gap; fill vertical space
    n = len(forces)
    gap = 0.15
    card_w = (CONTENT_W - gap * (n - 1)) / n
    y = CONTENT_TOP + 0.1
    card_h = CONTENT_BOT - y - 0.05

    for i, f in enumerate(forces):
        x = CONTENT_LEFT + i * (card_w + gap)
        fill, accent = tone_colors(f["tone"])
        box = slide.shapes.add_shape(
            MSO_SHAPE.ROUNDED_RECTANGLE,
            Inches(x), Inches(y), Inches(card_w), Inches(card_h))
        box.fill.solid()
        box.fill.fore_color.rgb = fill
        box.line.color.rgb = accent
        # Force name (top)
        add_text(slide, x + 0.14, y + 0.10, card_w - 0.28, 0.34,
                 f["name"].upper(), size=10, bold=True, color=NAVY,
                 anchor=MSO_ANCHOR.TOP)
        # Rating (colored)
        add_text(slide, x + 0.14, y + 0.44, card_w - 0.28, 0.34,
                 f["rating"] or "—", size=11, bold=True, color=accent,
                 anchor=MSO_ANCHOR.TOP)
        # Body summary
        _paragraph_block_bold(slide, x + 0.14, y + 0.90, card_w - 0.28, card_h - 1.05,
                              [f["summary"]], size=9, color=DARK,
                              line_spacing=1.25, para_space_after=4)


def slide_deep_dive_section(prs, section_num: int, section: Dict[str, Any],
                            company: str, symbol: str) -> None:
    """Deep-dive section renderer with automatic overflow to (cont) slides.

    Layout modes:
      • prose + table  → first slide: prose (left) + first table (right).
                         Extra paragraphs spill onto continuation slides (full-width).
      • table-only     → one slide per table (up to 2 tables).
      • prose-only     → group paragraphs into slides using line-budget estimator.
    """
    title = section.get("title", "").replace("(Ranked)", "").strip()
    paragraphs = [p for p in (section.get("paragraphs") or []) if p and p.strip()]
    tables = section.get("tables") or []

    def _new_slide(cont: bool):
        s = _blank_slide(prs)
        clear_slide(s)
        header_band(prs, s, title + (" (cont)" if cont else ""),
                    subtitle=f"{company}  ·  {symbol}",
                    kicker=f"SECTION {section_num} — STRATEGIC DEEP DIVE")
        return s

    # ── Table-only ────────────────────────────────────────────────────────
    if tables and not paragraphs:
        for i, tbl in enumerate(tables[:2]):
            if not (tbl.get("headers") and tbl.get("rows")):
                continue
            s = _new_slide(cont=(i > 0))
            _md_table_to_pptx(s, CONTENT_LEFT, CONTENT_TOP, CONTENT_W,
                              CONTENT_BOT - CONTENT_TOP - 0.2, tbl,
                              first_col_frac=0.25,
                              expand_to_fill=True)
        return

    prose_h = CONTENT_BOT - CONTENT_TOP - 0.2
    line_spacing = 1.28

    # ── Prose + table (Section 4 case) ────────────────────────────────────
    if tables and paragraphs and tables[0].get("headers") and tables[0].get("rows"):
        left_w = CONTENT_W * 0.62 - 0.15
        right_w = CONTENT_W * 0.38 - 0.15
        right_x = CONTENT_LEFT + left_w + 0.30

        # Phase 1 — largest size (down to 8pt) that fits ALL prose on slide 1.
        first_size = None
        for size_x2 in range(int(11.5 * 2), int(8.0 * 2) - 1, -1):
            size = size_x2 / 2.0
            cpl = _chars_per_line(left_w, size)
            budget = _lines_per_slide(prose_h, size, line_spacing)
            cost = sum(_paragraph_line_cost(p, cpl) for p in paragraphs)
            if cost <= budget:
                first_size = size
                break

        if first_size is not None:
            first_group, overflow = paragraphs, []
        else:
            # Phase 2 — must split; pick natural ~10pt on slide 1, then check
            # if the overflow would be an orphan and shrink first_size to absorb.
            first_size = 10.0
            cpl_narrow = _chars_per_line(left_w, first_size)
            first_budget = _lines_per_slide(prose_h, first_size, line_spacing)
            first_group, current = [], []
            used = 0.0
            for para in paragraphs:
                c = _paragraph_line_cost(para, cpl_narrow)
                if not first_group and used + c > first_budget and current:
                    first_group = list(current)
                    current = [para]
                    used = c
                else:
                    current.append(para)
                    used += c
            if not first_group:
                first_group, overflow = current, []
            else:
                overflow = current
            if overflow:
                over_cpl = _chars_per_line(CONTENT_W, 11.0)
                over_budget = _lines_per_slide(prose_h, 11.0, line_spacing)
                over_cost = sum(_paragraph_line_cost(p, over_cpl) for p in overflow)
                if over_cost < over_budget * 0.45:
                    # Overflow would be an orphan — try to compress everything
                    # onto slide 1 by shrinking the narrow-column font.
                    for size_x2 in range(int(9.5 * 2), int(8.0 * 2) - 1, -1):
                        size2 = size_x2 / 2.0
                        cpl2 = _chars_per_line(left_w, size2)
                        budget2 = _lines_per_slide(prose_h, size2, line_spacing)
                        cost2 = sum(_paragraph_line_cost(p, cpl2) for p in paragraphs)
                        if cost2 <= budget2:
                            first_size = size2
                            first_group = paragraphs
                            overflow = []
                            break
        # Render first slide
        s = _new_slide(cont=False)
        _paragraph_block_bold(s, CONTENT_LEFT, CONTENT_TOP, left_w, prose_h,
                              first_group, size=first_size, color=DARK,
                              line_spacing=line_spacing,
                              para_space_after=max(3, int(first_size * 0.5)))
        tbl = tables[0]
        table_y = CONTENT_TOP - 0.05
        has_title = bool(tbl.get("title"))
        if has_title:
            add_text(s, right_x, table_y, right_w, 0.22,
                     tbl.get("title", ""), size=9.5, bold=True, color=NAVY)
        _md_table_to_pptx(s, right_x, table_y + (0.24 if has_title else 0),
                          right_w, CONTENT_BOT - table_y - (0.26 if has_title else 0.15),
                          tbl, first_col_frac=0.24)

        # Overflow prose (rare) — adaptive full-width
        if overflow:
            over_groups, over_size = _adaptive_group_and_size(
                overflow, CONTENT_W, prose_h,
                line_spacing=line_spacing, min_size=9.0, max_size=14.0)
            for g in over_groups:
                s2 = _new_slide(cont=True)
                _paragraph_block_bold(s2, CONTENT_LEFT, CONTENT_TOP, CONTENT_W, prose_h,
                                      g, size=over_size, color=DARK,
                                      line_spacing=line_spacing,
                                      para_space_after=max(4, int(over_size * 0.55)))
        return

    # ── Prose-only ────────────────────────────────────────────────────────
    if paragraphs:
        groups, size = _adaptive_group_and_size(
            paragraphs, CONTENT_W, prose_h,
            line_spacing=line_spacing, min_size=9.0, max_size=14.0)
        for idx, group in enumerate(groups):
            s = _new_slide(cont=(idx > 0))
            _paragraph_block_bold(s, CONTENT_LEFT, CONTENT_TOP, CONTENT_W, prose_h,
                                  group, size=size, color=DARK,
                                  line_spacing=line_spacing,
                                  para_space_after=max(4, int(size * 0.55)))


# ═════════════════════════════════════════════════════════════════════════════
# SLIDE BUILDERS
# ═════════════════════════════════════════════════════════════════════════════
SLIDE_W = 13.333
SLIDE_H = 7.5
CONTENT_LEFT = 0.45
CONTENT_RIGHT = 0.45
CONTENT_TOP = 1.10  # below header band
CONTENT_BOT = 6.95
CONTENT_W = SLIDE_W - CONTENT_LEFT - CONTENT_RIGHT


def _blank_slide(prs):
    layout = prs.slide_layouts[6]  # blank
    return prs.slides.add_slide(layout)


# ─────────────────────────────────────────────────────────────────────────────
# SLIDE 1 — Snapshot
# ─────────────────────────────────────────────────────────────────────────────
def slide_snapshot_with_brief(prs, report: Dict[str, Any],
                              section1: Optional[Dict[str, Any]] = None) -> None:
    """Slide 1: header + 6 KPI cards + Section 1 Strategic Investment Brief filling below.

    If section1 is None (deep dive unavailable), falls back to the old snapshot layout.
    """
    if section1 is None:
        # No deep dive available — fall back to original snapshot
        slide_snapshot(prs, report)
        return

    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    ta = report.get("technical_analysis", {}) or {}
    val = report.get("valuations", {}) or {}
    bs_m = report.get("balance_sheet_metrics", {}) or {}

    company = bo.get("company_name") or symbol
    sector = bo.get("sector") or "—"
    industry = bo.get("industry") or "—"
    date_str = datetime.now().strftime("%b %d, %Y")

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, f"{company}  ({symbol})",
                subtitle=f"{sector} · {industry}\nGenerated {date_str}")

    # KPI strip (6 cards) — same as before
    price_data = ta.get("price_data", {}) or {}
    current_price = price_data.get("current_price") or 0
    market_cap = bo.get("market_cap") or price_data.get("market_cap") or 0
    fwd_estimates = val.get("forward_estimates", {}) or {}
    fwd_pe = None
    if isinstance(fwd_estimates, dict) and fwd_estimates:
        try:
            first = fwd_estimates[sorted(fwd_estimates.keys())[0]]
            fwd_pe = first.get("forward_pe")
        except Exception:
            pass
    hist_val = val.get("historical", []) or []
    div_yield = None
    if hist_val:
        div_yield = hist_val[-1].get("dividend_yield")
        if div_yield and abs(div_yield) < 1:
            div_yield *= 100.0

    curr_bs = bs_m.get("current", {}) or {}
    ev_approx = None
    if market_cap:
        try:
            ev_approx = float(market_cap) + float(curr_bs.get("total_debt") or 0) - \
                        float(curr_bs.get("total_cash") or curr_bs.get("cash_and_equivalents") or 0)
        except (TypeError, ValueError):
            ev_approx = None

    year_high = price_data.get("year_high")
    year_low = price_data.get("year_low")
    pct_from_high = price_data.get("pct_from_52w_high")

    kpis = [
        {"label": "PRICE",
         "value": f"${current_price:,.2f}" if current_price else "N/A",
         "sub": f"{price_data.get('change_percent', 0):+.2f}% today" if price_data.get("change_percent") is not None else "",
         "tone": ""},
        {"label": "MARKET CAP", "value": fmt_money(market_cap, 1), "sub": "", "tone": ""},
        {"label": "ENTERPRISE VALUE", "value": fmt_money(ev_approx, 1), "sub": "", "tone": ""},
        {"label": "FWD P/E",
         "value": fmt_ratio(fwd_pe, 1) if fwd_pe else "N/A",
         "sub": "FY+1 consensus", "tone": ""},
        {"label": "DIV YIELD",
         "value": fmt_pct(div_yield, 2) if div_yield else "—", "sub": "", "tone": ""},
        {"label": "52W RANGE",
         "value": f"${year_low:,.0f}–${year_high:,.0f}" if year_low and year_high else "N/A",
         "sub": f"{pct_from_high:+.1f}% from high" if pct_from_high is not None else "",
         "tone": ""},
    ]
    kpi_row(slide, CONTENT_LEFT, CONTENT_TOP + 0.05, CONTENT_W, kpis,
            gap=0.14, height=0.95)

    # Strategic Investment Brief filling rest of the slide
    y = CONTENT_TOP + 1.15
    add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.32,
             "Strategic Investment Brief", size=14, bold=True, color=NAVY)
    y += 0.38

    paragraphs = section1.get("paragraphs") or []
    _paragraph_block_bold(slide, CONTENT_LEFT, y, CONTENT_W, CONTENT_BOT - y - 0.15,
                          paragraphs[:10], size=10.5, color=DARK,
                          line_spacing=1.35, para_space_after=8)


def slide_snapshot(prs, report: Dict[str, Any]) -> None:
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    ta = report.get("technical_analysis", {}) or {}
    val = report.get("valuations", {}) or {}
    rev = report.get("revenue_data", {}) or {}
    exec_sum = report.get("executive_summary", {}) or {}
    bs_m = report.get("balance_sheet_metrics", {}) or {}

    company = bo.get("company_name") or symbol
    sector = bo.get("sector") or "—"
    industry = bo.get("industry") or "—"
    date_str = datetime.now().strftime("%b %d, %Y")

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, f"{company}  ({symbol})",
                subtitle=f"{sector} · {industry}\nGenerated {date_str}")

    # KPI strip (6 cards)
    price_data = ta.get("price_data", {}) or {}
    current_price = price_data.get("current_price") or 0
    market_cap = bo.get("market_cap") or price_data.get("market_cap") or 0
    fwd_estimates = val.get("forward_estimates", {}) or {}
    fwd_pe = None
    fwd_eps_1 = None
    if isinstance(fwd_estimates, dict) and fwd_estimates:
        try:
            first = fwd_estimates[sorted(fwd_estimates.keys())[0]]
            fwd_pe = first.get("forward_pe")
            fwd_eps_1 = first.get("estimated_eps")
        except Exception:
            pass
    hist_val = val.get("historical", []) or []
    div_yield = None
    if hist_val:
        div_yield = hist_val[-1].get("dividend_yield")
        if div_yield and abs(div_yield) < 1:
            div_yield *= 100.0

    curr_bs = bs_m.get("current", {}) or {}
    ev_approx = None
    if market_cap:
        try:
            ev_approx = float(market_cap) + float(curr_bs.get("total_debt") or 0) - \
                        float(curr_bs.get("total_cash") or curr_bs.get("cash_and_equivalents") or 0)
        except (TypeError, ValueError):
            ev_approx = None

    year_high = price_data.get("year_high")
    year_low = price_data.get("year_low")
    pct_from_high = price_data.get("pct_from_52w_high")

    kpis = [
        {"label": "PRICE",
         "value": f"${current_price:,.2f}" if current_price else "N/A",
         "sub": f"{price_data.get('change_percent', 0):+.2f}% today" if price_data.get("change_percent") is not None else "",
         "tone": ""},
        {"label": "MARKET CAP", "value": fmt_money(market_cap, 1), "sub": "", "tone": ""},
        {"label": "ENTERPRISE VALUE", "value": fmt_money(ev_approx, 1), "sub": "", "tone": ""},
        {"label": "FWD P/E",
         "value": fmt_ratio(fwd_pe, 1) if fwd_pe else "N/A",
         "sub": "FY+1 consensus", "tone": ""},
        {"label": "DIV YIELD",
         "value": fmt_pct(div_yield, 2) if div_yield else "—", "sub": "", "tone": ""},
        {"label": "52W RANGE",
         "value": f"${year_low:,.0f}–${year_high:,.0f}" if year_low and year_high else "N/A",
         "sub": f"{pct_from_high:+.1f}% from high" if pct_from_high is not None else "",
         "tone": ""},
    ]
    kpi_row(slide, CONTENT_LEFT, CONTENT_TOP + 0.05, CONTENT_W, kpis,
            gap=0.14, height=0.95)

    # Verdict banner
    y = CONTENT_TOP + 1.15
    verdict = (exec_sum.get("verdict") or "Neutral").upper()
    reason = exec_sum.get("verdict_reason", "")
    if "POS" in verdict or "BUY" in verdict:
        v_label, v_fill = "BUY", GREEN_D
    elif "NEG" in verdict or "SELL" in verdict:
        v_label, v_fill = "SELL", RED_D
    else:
        v_label, v_fill = "HOLD", AMBER_D
    band = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(CONTENT_LEFT), Inches(y),
        Inches(CONTENT_W), Inches(0.75))
    band.fill.solid()
    band.fill.fore_color.rgb = v_fill
    band.line.fill.background()
    add_text(slide, CONTENT_LEFT, y + 0.06, CONTENT_W, 0.32,
             f"VERDICT: {v_label}", size=16, bold=True, color=WHITE,
             align=PP_ALIGN.CENTER)
    if reason:
        add_text(slide, CONTENT_LEFT + 0.5, y + 0.38, CONTENT_W - 1.0, 0.32,
                 reason, size=10, italic=True, color=WHITE, align=PP_ALIGN.CENTER)

    # 2-column body: bottom line (left) + forward view (right)
    body_y = y + 0.95
    col_w = (CONTENT_W - 0.4) / 2

    # LEFT — bottom line + strategic
    add_text(slide, CONTENT_LEFT, body_y, col_w, 0.28,
             "BOTTOM LINE", size=10, bold=True, color=NAVY)
    bottom = _clean(exec_sum.get("bottom_line") or "No bottom-line summary available.")
    add_paragraph_block(slide, CONTENT_LEFT, body_y + 0.32, col_w, 1.3,
                        [bottom], size=10, color=DARK, line_spacing=1.25)
    strategic = _clean(exec_sum.get("strategic_situation") or "")
    if strategic:
        add_text(slide, CONTENT_LEFT, body_y + 1.72, col_w, 0.28,
                 "STRATEGIC SITUATION", size=10, bold=True, color=NAVY)
        add_paragraph_block(slide, CONTENT_LEFT, body_y + 2.04, col_w, 1.6,
                            [strategic], size=10, color=DARK, line_spacing=1.25)

    # RIGHT — forward view mini-table
    right_x = CONTENT_LEFT + col_w + 0.4
    add_text(slide, right_x, body_y, col_w, 0.28,
             "FORWARD VIEW", size=10, bold=True, color=NAVY)

    est = rev.get("estimates", {}) or {}
    est_rows_list = []
    for k in ("year_1", "year_2"):
        if isinstance(est.get(k), dict):
            est_rows_list.append(est[k])
    if not est_rows_list:
        for k in sorted(est.keys())[:2]:
            if isinstance(est.get(k), dict):
                est_rows_list.append(est[k])

    def _e_label(idx, fallback):
        if idx < len(est_rows_list):
            lbl = est_rows_list[idx].get("period") or fallback
            if lbl and not str(lbl).upper().endswith("E"):
                lbl = f"{lbl}E"
            return lbl
        return fallback

    hist_margins = rev.get("historical_margins", []) or []
    annuals = [m for m in hist_margins if str(m.get("period", "")).isdigit() and len(str(m.get("period", ""))) == 4]
    last_actual = annuals[0] if annuals else (hist_margins[0] if hist_margins else {})
    la_period = last_actual.get("period", "Actual")

    e1_rev = est_rows_list[0].get("revenue") if len(est_rows_list) > 0 else None
    e2_rev = est_rows_list[1].get("revenue") if len(est_rows_list) > 1 else None
    e1_nm = est_rows_list[0].get("net_margin") if len(est_rows_list) > 0 else None
    e2_nm = est_rows_list[1].get("net_margin") if len(est_rows_list) > 1 else None

    fwd_eps_2 = None
    if isinstance(fwd_estimates, dict) and len(fwd_estimates) > 1:
        try:
            fwd_eps_2 = fwd_estimates[sorted(fwd_estimates.keys())[1]].get("estimated_eps")
        except Exception:
            pass

    fv_headers = ["Metric", str(la_period), _e_label(0, "FY+1E"), _e_label(1, "FY+2E")]
    fv_rows = [
        {"label": "Revenue", "cells": [fmt_money(last_actual.get("revenue"), 1),
                                       fmt_money(e1_rev, 1), fmt_money(e2_rev, 1)],
         "flag": None, "bold": True},
        {"label": "Net margin", "cells": [fmt_pct(last_actual.get("net_margin"), 1),
                                          fmt_pct(e1_nm, 1), fmt_pct(e2_nm, 1)],
         "flag": None, "bold": False},
        {"label": "EPS (diluted)", "cells": ["—",
                                             f"${fwd_eps_1:.2f}" if fwd_eps_1 else "N/A",
                                             f"${fwd_eps_2:.2f}" if fwd_eps_2 else "N/A"],
         "flag": None, "bold": False},
    ]
    statement_table(slide, right_x, body_y + 0.32, col_w, 1.5,
                    fv_headers, fv_rows, first_col_frac=0.34)

    # Price chart at bottom
    chart_y = body_y + 3.85
    dates, closes = _fetch_price_history(symbol, years=5)
    if dates and closes:
        png = _chart_price_line(dates, closes, width_in=CONTENT_W, height_in=2.0)
        if png:
            slide.shapes.add_picture(png, Inches(CONTENT_LEFT), Inches(chart_y),
                                     width=Inches(CONTENT_W), height=Inches(2.0))
            add_text(slide, CONTENT_LEFT, chart_y + 2.02, CONTENT_W, 0.20,
                     "5-year weekly close  ·  Source: FMP",
                     size=8, italic=True, color=GRAY, align=PP_ALIGN.RIGHT)


# ─────────────────────────────────────────────────────────────────────────────
# SLIDES 2-4 — Business & Positioning
# ─────────────────────────────────────────────────────────────────────────────
def slides_business_positioning(prs, report: Dict[str, Any]) -> None:
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    rev = report.get("revenue_data", {}) or {}
    adv = report.get("competitive_advantages", []) or []
    mgmt = report.get("management", []) or []
    comp_ai = report.get("competitive_analysis", {}) or {}
    company = bo.get("company_name") or symbol

    # ── Slide A: Business overview + segments ───────────────────────────────
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Business Overview", subtitle=f"{company}  ·  {symbol}",
                kicker="SECTION 1 — BUSINESS & POSITIONING")

    y = CONTENT_TOP
    # Description (full)
    add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.30,
             "What the company does", size=14, bold=True, color=NAVY)
    y += 0.35
    desc_paras = _trim_paras(bo.get("description") or "No description available.", max_paras=8)
    # Column split — text left ~60%, segments table right ~40%
    left_w = CONTENT_W * 0.60 - 0.15
    right_w = CONTENT_W * 0.40 - 0.15
    right_x = CONTENT_LEFT + left_w + 0.30
    add_paragraph_block(slide, CONTENT_LEFT, y, left_w, 5.5,
                        desc_paras, size=10, color=DARK, line_spacing=1.3,
                        para_space_after=6)

    # Segments table (right)
    segments = rev.get("segment_data") or []
    if segments:
        seg_map: Dict[str, float] = {}
        for s in segments:
            nm = s.get("name") or "Other"
            seg_map[nm] = seg_map.get(nm, 0) + float(s.get("revenue") or 0)
        total = sum(seg_map.values())
        if total > 0:
            add_text(slide, right_x, y, right_w, 0.28,
                     "Revenue by segment", size=11, bold=True, color=NAVY)
            sorted_segs = sorted(seg_map.items(), key=lambda kv: kv[1], reverse=True)[:10]
            seg_rows = [{"label": nm, "cells": [fmt_money(rv, 1), f"{rv/total*100:.1f}%"],
                         "flag": None, "bold": False} for nm, rv in sorted_segs]
            statement_table(slide, right_x, y + 0.33, right_w, 3.5,
                            ["Segment", "Revenue", "%"], seg_rows,
                            first_col_frac=0.52)

    # ── Slide B: Competitive position (moat/market/advantages) ──────────────
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Competitive Position", subtitle=f"{company}  ·  {symbol}",
                kicker="SECTION 1 — BUSINESS & POSITIONING")

    y = CONTENT_TOP
    # 2 columns: moat/advantages (left), market dynamics/industry (right)
    left_w = CONTENT_W * 0.5 - 0.15
    right_w = CONTENT_W * 0.5 - 0.15
    right_x = CONTENT_LEFT + left_w + 0.30

    # Left column
    left_paras = []
    moat = comp_ai.get("moat_analysis") or ""
    if moat:
        left_paras.append(("Moat Analysis", _trim_paras(moat, max_paras=6)))
    comp_pos = comp_ai.get("competitive_position") or ""
    if comp_pos:
        left_paras.append(("Market Position", _trim_paras(comp_pos, max_paras=4)))
    if adv:
        left_paras.append(("Competitive Advantages", [f"• {_clean(a)}" for a in adv[:8]]))

    cursor_y = y
    for title, paras in left_paras:
        add_text(slide, CONTENT_LEFT, cursor_y, left_w, 0.28,
                 title, size=12, bold=True, color=NAVY)
        cursor_y += 0.32
        # Estimate height needed
        h = min(2.5, 0.18 * sum(max(1, len(p) // 90 + 1) for p in paras) + 0.2)
        add_paragraph_block(slide, CONTENT_LEFT, cursor_y, left_w, h,
                            paras, size=9.5, color=DARK,
                            line_spacing=1.2, para_space_after=4)
        cursor_y += h + 0.18
        if cursor_y > CONTENT_BOT:
            break

    # Right column
    right_paras = []
    market_dyn = comp_ai.get("market_dynamics") or ""
    if market_dyn:
        right_paras.append(("Market Dynamics", _trim_paras(market_dyn, max_paras=6)))
    industry_a = comp_ai.get("industry_analysis") or ""
    if industry_a:
        right_paras.append(("Industry Analysis", _trim_paras(industry_a, max_paras=6)))

    cursor_y = y
    for title, paras in right_paras:
        add_text(slide, right_x, cursor_y, right_w, 0.28,
                 title, size=12, bold=True, color=NAVY)
        cursor_y += 0.32
        h = min(2.6, 0.18 * sum(max(1, len(p) // 90 + 1) for p in paras) + 0.2)
        add_paragraph_block(slide, right_x, cursor_y, right_w, h,
                            paras, size=9.5, color=DARK,
                            line_spacing=1.2, para_space_after=4)
        cursor_y += h + 0.18
        if cursor_y > CONTENT_BOT:
            break

    # ── Slide C: Competitors + peers + management ───────────────────────────
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Competitors, Peers & Management",
                subtitle=f"{company}  ·  {symbol}",
                kicker="SECTION 1 — BUSINESS & POSITIONING")

    y = CONTENT_TOP
    # Key competitors table (top half)
    key_comps_raw = comp_ai.get("key_competitors") or []
    if key_comps_raw:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "Key competitors", size=12, bold=True, color=NAVY)
        rows = []
        for c in key_comps_raw[:5]:
            parts = {}
            for chunk in c.split("|"):
                if ":" in chunk:
                    k, v = chunk.split(":", 1)
                    parts[k.strip().upper()] = v.strip()
            rows.append({
                "label": parts.get("COMPETITOR", "N/A"),
                "cells": [parts.get("TICKER", "—"),
                          parts.get("THREAT", "N/A"),
                          parts.get("STRENGTH", "N/A")],
                "flag": None, "bold": False,
            })
        statement_table(slide, CONTENT_LEFT, y + 0.32, CONTENT_W, 1.8,
                        ["Competitor", "Ticker", "Competitive Threat", "Their Strength"],
                        rows, first_col_frac=0.18)
        y += 2.3

    # Peer comparison table (mid)
    competition = report.get("competition") or []
    if competition:
        peers = _fetch_peer_metrics(competition, max_peers=5)
        peer_rows = []
        for p in peers:
            peer_rows.append({
                "label": f"{(p.get('name') or '')[:30]} ({p.get('symbol', '')})",
                "cells": [fmt_money(p.get("market_cap"), 1),
                          fmt_ratio(p.get("fwd_pe"), 1),
                          fmt_pct(p.get("rev_growth_ttm"), 1) if p.get("rev_growth_ttm") is not None else "N/A",
                          fmt_pct(p.get("op_margin"), 1) if p.get("op_margin") is not None else "N/A"],
                "flag": None, "bold": False,
            })
        if peer_rows:
            add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                     "Peer comparison (financial metrics)", size=12, bold=True, color=NAVY)
            statement_table(slide, CONTENT_LEFT, y + 0.32, CONTENT_W, 1.6,
                            ["Peer (Ticker)", "Mkt Cap", "Fwd P/E", "Rev Growth", "Op Margin"],
                            peer_rows, first_col_frac=0.30)
            y += 2.1

    # Management (bottom)
    if mgmt:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "Management", size=12, bold=True, color=NAVY)
        mgmt_rows = []
        for exec in mgmt[:6]:
            prior = _clean(exec.get("prior_employers") or "—")
            tenure = exec.get("tenure") or "N/A"
            pay = exec.get("pay")
            pay_str = fmt_money(pay, 1) if pay else "N/A"
            mgmt_rows.append({
                "label": exec.get("name", "—"),
                "cells": [exec.get("title", "—")[:40], str(tenure), pay_str, prior[:60]],
                "flag": None, "bold": False,
            })
        remaining = CONTENT_BOT - y - 0.35
        statement_table(slide, CONTENT_LEFT, y + 0.32, CONTENT_W,
                        min(2.5, remaining),
                        ["Name", "Title", "Tenure", "Pay", "Prior Employers"],
                        mgmt_rows, first_col_frac=0.20)


# ─────────────────────────────────────────────────────────────────────────────
# SLIDES 5-8 — Financials (ASML template)
# ─────────────────────────────────────────────────────────────────────────────
def _slide_statement_analysis(prs, report: Dict[str, Any],
                              section_key: str, slide_title: str,
                              subtitle_extra: str = "") -> None:
    """Generic ASML-style statement slide: KPI row + table + 3 callouts."""
    data = report.get(section_key, {}) or {}
    if not data.get("available"):
        return
    symbol = report.get("symbol", "")
    company = report.get("business_overview", {}).get("company_name") or symbol

    slide = _blank_slide(prs)
    clear_slide(slide)
    subtitle_parts = [f"{company} · {symbol}"]
    if subtitle_extra:
        subtitle_parts.append(subtitle_extra)
    if data.get("period_label"):
        subtitle_parts.append(data["period_label"])
    header_band(prs, slide, slide_title,
                subtitle="  ·  ".join(subtitle_parts),
                kicker="SECTION 2 — FINANCIALS")

    y = CONTENT_TOP
    # KPI row (4 cards)
    kpis = data.get("kpis") or []
    if kpis:
        kpi_row(slide, CONTENT_LEFT, y, CONTENT_W, kpis[:4], gap=0.15, height=0.95)
        y += 1.10

    # Full-width statement table on left ~65%, callout stack on right ~35%
    rows = data.get("rows") or []
    headers = data.get("column_headers") or []
    callouts = data.get("callouts") or []

    left_w = CONTENT_W * 0.66 - 0.15
    right_w = CONTENT_W * 0.34 - 0.15
    right_x = CONTENT_LEFT + left_w + 0.30

    # Table
    if rows and headers:
        table_h = min(CONTENT_BOT - y - 0.1, 0.38 + 0.30 * len(rows))
        statement_table(slide, CONTENT_LEFT, y, left_w, table_h,
                        headers, rows, first_col_frac=0.42)

    # Callout stack on right
    if callouts:
        picks = callouts[:3]
        callout_h = (CONTENT_BOT - y - 0.15 * (len(picks) - 1)) / len(picks)
        callout_h = min(callout_h, 1.9)
        cy = y
        for co in picks:
            body = co.get("body") or ""
            # Trim body if long
            if len(body) > 500:
                body = body[:497] + "..."
            callout_box(slide, right_x, cy, right_w, callout_h,
                        co.get("title", ""), body, co.get("kind", "warn"))
            cy += callout_h + 0.15


def slide_income_statement(prs, report):
    _slide_statement_analysis(prs, report, "income_statement_analysis",
                              "Income Statement",
                              subtitle_extra="Latest reported vs prior")


def slide_cash_flow(prs, report):
    _slide_statement_analysis(prs, report, "cash_flow_analysis",
                              "Cash Flow Statement",
                              subtitle_extra="Follow the cash")


def slide_balance_sheet(prs, report):
    _slide_statement_analysis(prs, report, "balance_sheet_analysis",
                              "Balance Sheet",
                              subtitle_extra="Current vs prior period")


def slide_revenue_margins(prs, report):
    """5-year actual + 2-year estimate revenue table, margin line chart, returns KPIs."""
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    company = bo.get("company_name") or symbol
    rev = report.get("revenue_data", {}) or {}
    val = report.get("valuations", {}) or {}
    km = report.get("key_metrics", {}) or {}

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Revenue & Margins",
                subtitle=f"{company}  ·  {symbol}  ·  5-year actual + 2-year estimate",
                kicker="SECTION 2 — FINANCIALS")

    hist = rev.get("historical_margins", []) or []
    annuals = [m for m in hist if str(m.get("period", "")).isdigit() and len(str(m.get("period", ""))) == 4]
    annuals_sorted = sorted(annuals, key=lambda m: str(m.get("period", "")))[-5:]

    est_raw = rev.get("estimates", {}) or {}
    est_list_all = []
    for k in ("year_1", "year_2"):
        if isinstance(est_raw.get(k), dict):
            est_list_all.append(est_raw[k])
    if not est_list_all:
        for k in sorted(est_raw.keys())[:2]:
            if isinstance(est_raw.get(k), dict):
                est_list_all.append(est_raw[k])

    fwd = val.get("forward_estimates", {}) or {}
    fwd_years = sorted(fwd.keys()) if isinstance(fwd, dict) else []

    # Trust the backend's `period` label (now derived from FMP's fiscal-year-end
    # date, e.g. 'FY2027E').  Defence in depth: drop any estimate whose implied
    # fiscal year is <= the latest reported historical (which would mean the FY
    # already has actuals and the estimate is stale).
    latest_hist_fy = None
    if annuals_sorted:
        try:
            latest_hist_fy = int(str(annuals_sorted[-1].get("period", "")).strip())
        except (TypeError, ValueError):
            latest_hist_fy = None

    def _est_fy_year(est_dict) -> Optional[int]:
        p = str(est_dict.get("period", "")).upper().replace("FY", "").replace("E", "").strip()
        try:
            return int(p)
        except ValueError:
            return None

    est_list = []
    est_labels = []
    for e in est_list_all:
        fy = _est_fy_year(e)
        lbl = e.get("period") or "FY+E"
        if lbl and not str(lbl).upper().endswith("E"):
            lbl = f"{lbl}E"
        if fy is None or latest_hist_fy is None or fy > latest_hist_fy:
            est_list.append(e)
            est_labels.append(lbl)
        # else stale — drop

    def _pct(v):
        if v is None:
            return "N/A"
        try:
            n = float(v)
        except (TypeError, ValueError):
            return "N/A"
        if -1.5 < n < 1.5:
            n *= 100.0
        return f"{n:.1f}%"

    # Historical headers: prefix with "FY" so the whole row reads as fiscal years
    hist_headers = [f"FY{m.get('period','')}" for m in annuals_sorted]
    headers = ["Metric"] + hist_headers + est_labels

    rev_cells = [fmt_money(m.get("revenue"), 1) for m in annuals_sorted] + \
                [fmt_money(e.get("revenue"), 1) for e in est_list]
    rev_vals = [m.get("revenue") for m in annuals_sorted] + [e.get("revenue") for e in est_list]
    growth_cells = ["—"]
    for i in range(1, len(rev_vals)):
        if rev_vals[i-1] in (None, 0) or rev_vals[i] is None:
            growth_cells.append("—")
        else:
            growth_cells.append(f"{(rev_vals[i] - rev_vals[i-1]) / abs(rev_vals[i-1]) * 100:+.1f}%")
    gm_cells = [_pct(m.get("gross_margin")) for m in annuals_sorted] + \
               [_pct(e.get("gross_margin")) for e in est_list]
    om_cells = [_pct(m.get("operating_margin")) for m in annuals_sorted] + \
               [_pct(e.get("operating_margin")) for e in est_list]
    nm_cells = [_pct(m.get("net_margin")) for m in annuals_sorted] + \
               [_pct(e.get("net_margin")) for e in est_list]
    eps_cells = ["—"] * len(annuals_sorted)
    for i, _ in enumerate(est_list):
        eps_val = None
        if i < len(fwd_years):
            eps_val = fwd.get(fwd_years[i], {}).get("estimated_eps")
        eps_cells.append(f"${eps_val:.2f}" if eps_val else "N/A")

    table_rows = [
        {"label": "Revenue", "cells": rev_cells, "flag": None, "bold": True},
        {"label": "  YoY growth", "cells": growth_cells, "flag": None, "bold": False},
        {"label": "Gross margin", "cells": gm_cells, "flag": None, "bold": False},
        {"label": "Operating margin", "cells": om_cells, "flag": None, "bold": False},
        {"label": "Net margin", "cells": nm_cells, "flag": None, "bold": False},
        {"label": "EPS (diluted)", "cells": eps_cells, "flag": None, "bold": False},
    ]
    y = CONTENT_TOP
    add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
             "5-year actual + 2-year consensus estimate", size=11, bold=True, color=NAVY)
    y += 0.32
    statement_table(slide, CONTENT_LEFT, y, CONTENT_W, 2.2,
                    headers, table_rows, first_col_frac=0.22)
    y += 2.35

    # Margins line chart (left) + returns KPIs (right)
    left_w = CONTENT_W * 0.60 - 0.15
    right_w = CONTENT_W * 0.40 - 0.15
    right_x = CONTENT_LEFT + left_w + 0.30

    if annuals_sorted:
        periods = hist_headers + est_labels

        def _norm(v):
            if v is None:
                return None
            try:
                n = float(v)
            except (TypeError, ValueError):
                return None
            if -1.5 < n < 1.5:
                n *= 100.0
            return n

        gross_s = [_norm(m.get("gross_margin")) for m in annuals_sorted] + \
                  [_norm(e.get("gross_margin")) for e in est_list]
        op_s = [_norm(m.get("operating_margin")) for m in annuals_sorted] + \
               [_norm(e.get("operating_margin")) for e in est_list]
        net_s = [_norm(m.get("net_margin")) for m in annuals_sorted] + \
                [_norm(e.get("net_margin")) for e in est_list]
        n_actual = len(annuals_sorted)
        png = _chart_margins_line(periods, gross_s, op_s, net_s, n_actual,
                                  width_in=left_w, height_in=2.3)
        if png:
            add_text(slide, CONTENT_LEFT, y, left_w, 0.28,
                     "Margin trajectory (dashed = estimate)", size=11, bold=True, color=NAVY)
            slide.shapes.add_picture(png, Inches(CONTENT_LEFT), Inches(y + 0.32),
                                     width=Inches(left_w), height=Inches(2.3))

    # Returns KPIs (right)
    fcf_margin = None
    if km.get("free_cash_flow") and km.get("revenue"):
        try:
            fcf_margin = float(km["free_cash_flow"]) / float(km["revenue"]) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            fcf_margin = None

    def _tone_for(v, good, bad):
        if v is None:
            return ""
        return "pos" if v > good else ("neg" if v < bad else "warn")

    ret_kpis = [
        {"label": "ROE (TTM)", "value": fmt_pct(km.get("roe"), 1),
         "sub": "", "tone": _tone_for(km.get("roe"), 15, 5)},
        {"label": "ROIC (TTM)", "value": fmt_pct(km.get("roic"), 1),
         "sub": "", "tone": _tone_for(km.get("roic"), 12, 6)},
        {"label": "ROA (TTM)", "value": fmt_pct(km.get("roa"), 1),
         "sub": "", "tone": _tone_for(km.get("roa"), 8, 2)},
        {"label": "FCF MARGIN", "value": fmt_pct(fcf_margin, 1) if fcf_margin else "N/A",
         "sub": "TTM", "tone": _tone_for(fcf_margin, 15, 3)},
    ]
    add_text(slide, right_x, y, right_w, 0.28,
             "Returns on capital", size=11, bold=True, color=NAVY)
    # 2x2 grid of KPIs
    card_w = (right_w - 0.15) / 2
    kpi_card_from_dict(slide, right_x, y + 0.32, card_w, ret_kpis[0], height=0.85)
    kpi_card_from_dict(slide, right_x + card_w + 0.15, y + 0.32, card_w, ret_kpis[1], height=0.85)
    kpi_card_from_dict(slide, right_x, y + 0.32 + 0.95, card_w, ret_kpis[2], height=0.85)
    kpi_card_from_dict(slide, right_x + card_w + 0.15, y + 0.32 + 0.95, card_w, ret_kpis[3], height=0.85)

    # WACC vs ROIC callout below returns
    wacc, roic = km.get("wacc"), km.get("roic")
    if wacc and roic:
        spread = float(roic) - float(wacc)
        if spread > 0:
            callout_box(slide, right_x, y + 0.32 + 1.95, right_w, 0.55,
                        "Value creation",
                        f"ROIC {roic:.1f}% vs WACC {wacc:.1f}% (+{spread:.1f}pp)", "pos")
        else:
            callout_box(slide, right_x, y + 0.32 + 1.95, right_w, 0.55,
                        "Value destruction",
                        f"ROIC {roic:.1f}% vs WACC {wacc:.1f}% ({spread:.1f}pp)", "neg")


# ─────────────────────────────────────────────────────────────────────────────
# SLIDES 9-10 — Valuation & Thesis
# ─────────────────────────────────────────────────────────────────────────────
def _val_tone_vs_history(curr, hist_vals, higher_is_expensive=True):
    if curr is None or curr == 0:
        return ""
    hist = [h for h in hist_vals if h and h > 0]
    if len(hist) < 3:
        return ""
    avg = sum(hist) / len(hist)
    if avg == 0:
        return ""
    ratio = curr / avg
    if higher_is_expensive:
        if ratio <= 0.85: return "pos"
        if ratio <= 1.15: return "warn"
        return "neg"
    else:
        if ratio >= 1.15: return "pos"
        if ratio >= 0.85: return "warn"
        return "neg"


def extract_description_section(description: str, section_name: str,
                                terminator_patterns: Optional[List[str]] = None
                                ) -> Optional[Dict[str, Any]]:
    """Extract a named section (e.g. 'Key Products Deep Dive') from
    business_overview.description. Section body runs until the next H3/H2/next
    Title-Case heading or explicit terminator.
    Returns {'title', 'paragraphs', 'tables', 'body'} or None."""
    if not description:
        return None
    # Case-insensitive start search
    lower = description.lower()
    idx = lower.find(section_name.lower())
    if idx < 0:
        return None
    # Move to line start
    line_start = description.rfind("\n", 0, idx) + 1
    # Find start of body (after the header line)
    body_start = description.find("\n", idx) + 1
    if body_start <= 0:
        return None
    # Find end: next markdown header (### or ## or --- separator)
    remaining = description[body_start:]
    end_re = re.compile(r"(?:^---\s*$|^##+\s+|^\*\*[A-Z][^\*]{2,}\*\*\s*\n)", re.MULTILINE)
    # Look for the first end marker that's plausibly a NEW section (not a bold product name)
    # A next "section" is typically ### or ---. Bold labels inside body are OK.
    end_re_strict = re.compile(r"^(?:---\s*$|##+\s+)", re.MULTILINE)
    m = end_re_strict.search(remaining)
    body = remaining[:m.start()].strip() if m else remaining.strip()
    if terminator_patterns:
        for term in terminator_patterns:
            i = body.lower().find(term.lower())
            if i > 0:
                body = body[:i].strip()
                break
    if not body:
        return None
    paragraphs, tables = _extract_paragraphs_and_tables(body)
    return {"title": section_name, "body": body,
            "paragraphs": paragraphs, "tables": tables}


def slide_subagent_addendum(prs, section_num: int, header_title: str,
                            addendum_title: str, part: Dict[str, Any],
                            company: str, symbol: str) -> None:
    """Slide rendering an addendum from a sub-agent memo (e.g. Part 1 Business Model
    Decomposition). Uses the same overflow logic as regular deep-dive sections."""
    if not part:
        return
    # Build a synthetic section dict and route through slide_deep_dive_section
    synthetic = {
        "title": f"{header_title} — {addendum_title}",
        "body": part.get("body", ""),
        "paragraphs": part.get("paragraphs") or [],
        "tables": part.get("tables") or [],
    }
    slide_deep_dive_section(prs, section_num, synthetic, company, symbol)


def slide_addenda_combined(prs, section_num: int, header_title: str,
                           addenda: List[Tuple[str, Dict[str, Any]]],
                           company: str, symbol: str,
                           report_data: Optional[Dict[str, Any]] = None) -> None:
    """Pack multiple sub-agent addenda into shared slides.  Each addendum gets a
    bold inline sub-heading so a short one doesn't burn its own slide.
    Addenda with tables are rendered separately (dedicated slide via the normal
    prose+table renderer) since tables need their own layout area.

    addenda: list of (subtitle, part_dict) tuples in display order.
    """
    if not addenda:
        return
    # Every addendum's prose flows into the combined block.  Addenda that carry
    # a real table also get a dedicated table-only slide first, so the table
    # gets full width without stealing prose area.
    prose_only: List[Tuple[str, Dict[str, Any]]] = []
    for title, part in addenda:
        if not part:
            continue
        tbls = part.get("tables") or []
        real_tables = [t for t in tbls if t.get("headers") and t.get("rows")]
        # For Business Model Decomposition, replace the (usually thin) deep-dive
        # table with an enriched TTM + last-quarter + %-of-total table built
        # from FMP quarterly data and the AI segment analysis.
        if (report_data is not None
                and "decomposition" in title.lower()
                and section_num == 2):
            enriched = _build_decomposition_table(report_data)
            if enriched:
                real_tables = [enriched] + [t for t in real_tables[1:]]
        for tbl in real_tables:
            s = _blank_slide(prs)
            clear_slide(s)
            header_band(prs, s, f"{header_title} — {title}",
                        subtitle=f"{company}  ·  {symbol}",
                        kicker=f"SECTION {section_num} — STRATEGIC DEEP DIVE")
            has_title = bool(tbl.get("title"))
            y = CONTENT_TOP
            if has_title:
                add_text(s, CONTENT_LEFT, y, CONTENT_W, 0.28,
                         tbl.get("title", ""), size=10, bold=True, color=NAVY)
                y += 0.32
            _md_table_to_pptx(s, CONTENT_LEFT, y, CONTENT_W,
                              CONTENT_BOT - y - 0.05, tbl,
                              first_col_frac=0.30, expand_to_fill=True)
        prose_only.append((title, part))

    if not prose_only:
        return

    # Flatten prose addenda into a single paragraph list, with each addendum's
    # subtitle rendered as a bold heading paragraph.  Track heading markers so
    # the slide title can name the first addendum on each page.
    flat: List[str] = []
    marker_prefix = "\x00H\x00"  # invisible sentinel so real content can't clash
    for title, part in prose_only:
        flat.append(f"{marker_prefix}{title.upper()}")
        for p in part.get("paragraphs") or []:
            if p and p.strip():
                flat.append(p)

    prose_h = CONTENT_BOT - CONTENT_TOP - 0.2
    line_spacing = 1.28
    groups, size = _adaptive_group_and_size(
        flat, CONTENT_W, prose_h,
        line_spacing=line_spacing, min_size=9.0, max_size=13.0)

    for idx, group in enumerate(groups):
        s = _blank_slide(prs)
        clear_slide(s)
        # Slide title: name the first addendum on this page (or "cont" if the
        # page starts mid-addendum).
        first_heading = next((p[len(marker_prefix):].title() for p in group
                              if p.startswith(marker_prefix)), None)
        starts_with_heading = group[0].startswith(marker_prefix) if group else False
        if first_heading and starts_with_heading:
            slide_title = f"{header_title} — Deep Dive"
        else:
            slide_title = f"{header_title} — Deep Dive (cont)"
        header_band(prs, s, slide_title,
                    subtitle=f"{company}  ·  {symbol}",
                    kicker=f"SECTION {section_num} — STRATEGIC DEEP DIVE")

        # Convert heading markers back to a bold, uppercase, navy heading via
        # markdown-bold syntax so the existing bold-run parser renders it.
        render_paras = []
        for p in group:
            if p.startswith(marker_prefix):
                heading = p[len(marker_prefix):]
                render_paras.append(f"**▸ {heading}**")
            else:
                render_paras.append(p)

        _paragraph_block_bold(s, CONTENT_LEFT, CONTENT_TOP, CONTENT_W, prose_h,
                              render_paras, size=size, color=DARK,
                              line_spacing=line_spacing,
                              para_space_after=max(4, int(size * 0.55)))


def slide_catalyst_calendar(prs, section6: Dict[str, Any],
                            company: str, symbol: str) -> None:
    """New slide from synthesis §6 Catalyst Calendar table."""
    if not section6:
        return
    tables = section6.get("tables") or []
    paragraphs = section6.get("paragraphs") or []
    if not (tables or paragraphs):
        return
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Catalyst Calendar",
                subtitle=f"{company}  ·  {symbol}  ·  Forward events that move the stock",
                kicker="SECTION 6 — STRATEGIC DEEP DIVE")
    y = CONTENT_TOP
    if tables:
        _md_table_to_pptx(slide, CONTENT_LEFT, y, CONTENT_W,
                          CONTENT_BOT - y - (0.5 if paragraphs else 0.2),
                          tables[0], first_col_frac=0.16)
        y += 3.2
    if paragraphs:
        prose_h = CONTENT_BOT - y - 0.15
        if prose_h > 0.5:
            size = _fit_font_size(paragraphs, CONTENT_W, prose_h,
                                  base_size=10, line_spacing=1.3, max_size=12)
            _paragraph_block_bold(slide, CONTENT_LEFT, y, CONTENT_W, prose_h,
                                  paragraphs, size=size, color=DARK,
                                  line_spacing=1.3, para_space_after=6)


def slide_scenario_analysis(prs, section7: Dict[str, Any],
                            company: str, symbol: str) -> None:
    """Slide from synthesis §7 Scenario Analysis (bear/base/bull with probability × 24mo)."""
    if not section7:
        return
    tables = section7.get("tables") or []
    paragraphs = section7.get("paragraphs") or []
    if not (tables or paragraphs):
        return
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Scenario Analysis",
                subtitle=f"{company}  ·  {symbol}  ·  Bear / Base / Bull — 24-month outlook",
                kicker="VALUATION EXTENSION")
    y = CONTENT_TOP
    if tables:
        _md_table_to_pptx(slide, CONTENT_LEFT, y, CONTENT_W,
                          CONTENT_BOT - y - (0.5 if paragraphs else 0.2),
                          tables[0], first_col_frac=0.14)
        y += 3.2
    if paragraphs:
        prose_h = CONTENT_BOT - y - 0.15
        if prose_h > 0.5:
            size = _fit_font_size(paragraphs, CONTENT_W, prose_h,
                                  base_size=10, line_spacing=1.3, max_size=12)
            _paragraph_block_bold(slide, CONTENT_LEFT, y, CONTENT_W, prose_h,
                                  paragraphs, size=size, color=DARK,
                                  line_spacing=1.3, para_space_after=6)


def slide_monitoring_kpis(prs, section10: Dict[str, Any],
                          company: str, symbol: str) -> None:
    """Closing slide from synthesis §10 Monitoring KPIs table."""
    if not section10:
        return
    tables = section10.get("tables") or []
    paragraphs = section10.get("paragraphs") or []
    if not (tables or paragraphs):
        return
    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Monitoring KPIs",
                subtitle=f"{company}  ·  {symbol}  ·  Track these to validate/invalidate the thesis",
                kicker="THESIS TRACKING DASHBOARD")
    y = CONTENT_TOP
    if tables:
        _md_table_to_pptx(slide, CONTENT_LEFT, y, CONTENT_W,
                          CONTENT_BOT - y - (0.5 if paragraphs else 0.2),
                          tables[0], first_col_frac=0.22)
        y += 3.2
    if paragraphs:
        prose_h = CONTENT_BOT - y - 0.15
        if prose_h > 0.5:
            size = _fit_font_size(paragraphs, CONTENT_W, prose_h,
                                  base_size=10, line_spacing=1.3, max_size=12)
            _paragraph_block_bold(slide, CONTENT_LEFT, y, CONTENT_W, prose_h,
                                  paragraphs, size=size, color=DARK,
                                  line_spacing=1.3, para_space_after=6)


def slide_valuation(prs, report):
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    company = bo.get("company_name") or symbol
    val = report.get("valuations", {}) or {}
    ta = report.get("technical_analysis", {}) or {}

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Valuation",
                subtitle=f"{company}  ·  {symbol}  ·  Current vs 5-year history",
                kicker="SECTION 3 — VALUATION & THESIS")

    curr = val.get("current", {}) or {}
    historical = val.get("historical", []) or []
    fwd = val.get("forward_estimates", {}) or {}
    fwd_first = None
    if fwd:
        try:
            fwd_first = fwd[sorted(fwd.keys())[0]]
        except Exception:
            fwd_first = None

    metric_defs = [
        ("P/E (TTM)", curr.get("pe_ratio"), "pe_ratio"),
        ("Fwd P/E (FY+1)", fwd_first.get("forward_pe") if fwd_first else None, "pe_ratio"),
        ("EV/EBITDA (TTM)", curr.get("ev_to_ebitda"), "ev_to_ebitda"),
        ("Fwd EV/EBITDA", fwd_first.get("forward_ev_ebitda") if fwd_first else None, "ev_to_ebitda"),
        ("P/S (TTM)", curr.get("price_to_sales"), "price_to_sales"),
        ("PEG (TTM)", curr.get("peg_ratio"), "peg_ratio"),
        ("P/FCF (TTM)", curr.get("price_to_fcf"), "price_to_fcf"),
    ]
    val_rows = []
    for label, cur_val, hist_key in metric_defs:
        h_vals = [h.get(hist_key) for h in historical[-5:] if h.get(hist_key) and h.get(hist_key) > 0]
        avg = sum(h_vals) / len(h_vals) if h_vals else None
        tone = _val_tone_vs_history(cur_val, h_vals, True)
        tone_label = {"pos": "CHEAP", "warn": "FAIR", "neg": "EXPENSIVE"}.get(tone, "—")
        val_rows.append({
            "label": label,
            "cells": [fmt_ratio(cur_val, 1) if cur_val else "N/A",
                      fmt_ratio(avg, 1) if avg else "N/A",
                      tone_label],
            "flag": tone if tone else None,
            "bold": False,
        })

    y = CONTENT_TOP
    left_w = CONTENT_W * 0.60 - 0.15
    right_w = CONTENT_W * 0.40 - 0.15
    right_x = CONTENT_LEFT + left_w + 0.30

    add_text(slide, CONTENT_LEFT, y, left_w, 0.28,
             "Multiples: current vs 5-year history", size=12, bold=True, color=NAVY)
    # Fill available vertical space so the table doesn't sit in a sparse block
    mult_h = CONTENT_BOT - (y + 0.32) - 0.05
    statement_table(slide, CONTENT_LEFT, y + 0.32, left_w, mult_h,
                    ["Metric", "Current", "5-yr avg", "vs history"],
                    val_rows, first_col_frac=0.34,
                    header_size=11.0, cell_size=10.5)

    # Forward return math (right)
    price_data = ta.get("price_data", {}) or {}
    current_price = price_data.get("current_price") or 0
    fy2_eps = None
    if fwd and len(fwd) > 1:
        try:
            fy2_eps = fwd[sorted(fwd.keys())[1]].get("estimated_eps")
        except Exception:
            pass
    if fy2_eps is None and fwd_first:
        fy2_eps = fwd_first.get("estimated_eps")
    ttm_pe = curr.get("pe_ratio")
    anchor = None
    for cand in (ttm_pe,):
        if cand and 5 < cand < 60:
            anchor = cand
            break
    if anchor is None:
        anchor = 15.0

    add_text(slide, right_x, y, right_w, 0.28,
             "Forward return math", size=12, bold=True, color=NAVY)
    if current_price and fy2_eps and fy2_eps > 0:
        add_text(slide, right_x, y + 0.32, right_w, 0.22,
                 f"Price ${current_price:.2f}  ·  FY+2 EPS ${fy2_eps:.2f}  ·  Anchor P/E {anchor:.1f}x",
                 size=9.0, color=GRAY)
        scen_rows = []
        for name, pe_mult in (("Bear", anchor * 0.75), ("Base", anchor), ("Bull", anchor * 1.25)):
            target = pe_mult * fy2_eps
            ret = (target - current_price) / current_price * 100
            tone = "pos" if ret > 5 else ("neg" if ret < -5 else "warn")
            scen_rows.append({
                "label": name,
                "cells": [f"{pe_mult:.1f}x", f"${target:.2f}", f"{ret:+.1f}%"],
                "flag": tone, "bold": name == "Base",
            })
        # Fill vertical space with taller scenario rows
        scen_h = CONTENT_BOT - (y + 0.62) - 0.05
        statement_table(slide, right_x, y + 0.62, right_w, scen_h,
                        ["Scenario", "Terminal P/E", "Target", "Return"],
                        scen_rows, first_col_frac=0.22,
                        header_size=11.0, cell_size=11.0)
    else:
        add_text(slide, right_x, y + 0.32, right_w, 0.28,
                 "Forward EPS estimate unavailable", size=10, italic=True, color=GRAY)


def slide_thesis(prs, report):
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    company = bo.get("company_name") or symbol
    exec_sum = report.get("executive_summary", {}) or {}
    thesis = report.get("investment_thesis", {}) or {}

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Investment Thesis",
                subtitle=f"{company}  ·  {symbol}",
                kicker="SECTION 3 — VALUATION & THESIS")

    y = CONTENT_TOP
    strategic = _clean(exec_sum.get("strategic_situation") or "")
    thesis_summary = thesis.get("summary") or ""
    if strategic:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "Strategic situation", size=12, bold=True, color=NAVY)
        add_paragraph_block(slide, CONTENT_LEFT, y + 0.32, CONTENT_W, 1.0,
                            [strategic], size=10, color=DARK, line_spacing=1.3)
        y += 1.4
    if thesis_summary:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "Thesis", size=12, bold=True, color=NAVY)
        add_paragraph_block(slide, CONTENT_LEFT, y + 0.32, CONTENT_W, 1.4,
                            _trim_paras(thesis_summary, 3), size=10, color=DARK,
                            line_spacing=1.3, para_space_after=4)
        y += 1.7

    # 3 columns: bulls / bears / watch
    bulls = thesis.get("bull_case") or exec_sum.get("key_positives") or []
    bears = thesis.get("bear_case") or exec_sum.get("key_concerns") or []
    catalysts = thesis.get("catalysts") or []
    key_watch = thesis.get("key_metrics_to_watch") or []
    watch = exec_sum.get("what_to_watch") or ""

    col_w = (CONTENT_W - 0.4) / 3
    remain = CONTENT_BOT - y - 0.3
    box_h = min(2.6, remain)

    # Bulls (green)
    b_box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        Inches(CONTENT_LEFT), Inches(y), Inches(col_w), Inches(box_h))
    b_box.fill.solid()
    b_box.fill.fore_color.rgb = GREEN
    b_box.line.color.rgb = GREEN_D
    add_text(slide, CONTENT_LEFT + 0.15, y + 0.1, col_w - 0.30, 0.28,
             "BULLS SAY", size=11, bold=True, color=GREEN_D)
    add_paragraph_block(slide, CONTENT_LEFT + 0.15, y + 0.42, col_w - 0.30, box_h - 0.5,
                        [f"• {_clean(b)}" for b in bulls[:5]],
                        size=9, color=DARK, line_spacing=1.25, para_space_after=4)

    # Bears (red)
    bx = CONTENT_LEFT + col_w + 0.2
    bx_box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        Inches(bx), Inches(y), Inches(col_w), Inches(box_h))
    bx_box.fill.solid()
    bx_box.fill.fore_color.rgb = RED
    bx_box.line.color.rgb = RED_D
    add_text(slide, bx + 0.15, y + 0.1, col_w - 0.30, 0.28,
             "BEARS SAY", size=11, bold=True, color=RED_D)
    add_paragraph_block(slide, bx + 0.15, y + 0.42, col_w - 0.30, box_h - 0.5,
                        [f"• {_clean(b)}" for b in bears[:5]],
                        size=9, color=DARK, line_spacing=1.25, para_space_after=4)

    # Watch (amber)
    wx = bx + col_w + 0.2
    w_box = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        Inches(wx), Inches(y), Inches(col_w), Inches(box_h))
    w_box.fill.solid()
    w_box.fill.fore_color.rgb = AMBER
    w_box.line.color.rgb = AMBER_D
    add_text(slide, wx + 0.15, y + 0.1, col_w - 0.30, 0.28,
             "WHAT TO WATCH", size=11, bold=True, color=AMBER_D)
    watch_items = []
    if watch:
        watch_items.append(_clean(watch))
    for c in catalysts[:4]:
        watch_items.append(f"• {_clean(c)}")
    for m in key_watch[:3]:
        watch_items.append(f"• {_clean(m)}")
    add_paragraph_block(slide, wx + 0.15, y + 0.42, col_w - 0.30, box_h - 0.5,
                        watch_items, size=9, color=DARK,
                        line_spacing=1.25, para_space_after=4)


# ─────────────────────────────────────────────────────────────────────────────
# SLIDE 11 — Risks
# ─────────────────────────────────────────────────────────────────────────────
def slide_risks(prs, report):
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    company = bo.get("company_name") or symbol
    risks = report.get("risks", {}) or {}
    company_flags = risks.get("company_red_flags") or risks.get("company_specific") or []
    general = risks.get("general_risks") or []

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Risks",
                subtitle=f"{company}  ·  {symbol}  ·  Ranked most-to-least material",
                kicker="SECTION 4 — RISKS")

    ranked = _rank_risks(company_flags, general, symbol, top_n=5)
    y = CONTENT_TOP
    if not ranked:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.3,
                 "No risk data available.", size=11, color=GRAY)
        return

    add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
             "Top 5 investor-material risks", size=12, bold=True, color=NAVY)
    y += 0.35

    for i, r in enumerate(ranked, 1):
        title = _clean(r.get("title", ""))
        detail = _clean(r.get("detail", ""))
        card_h = 0.90
        box = slide.shapes.add_shape(
            MSO_SHAPE.ROUNDED_RECTANGLE,
            Inches(CONTENT_LEFT), Inches(y),
            Inches(CONTENT_W), Inches(card_h))
        box.fill.solid()
        box.fill.fore_color.rgb = RED
        box.line.color.rgb = RED_D
        # Accent bar left
        bar = slide.shapes.add_shape(
            MSO_SHAPE.RECTANGLE,
            Inches(CONTENT_LEFT), Inches(y),
            Inches(0.08), Inches(card_h))
        bar.fill.solid()
        bar.fill.fore_color.rgb = RED_D
        bar.line.fill.background()
        # Number + title
        add_text(slide, CONTENT_LEFT + 0.20, y + 0.08, 0.6, 0.36,
                 f"{i}", size=22, bold=True, color=RED_D,
                 align=PP_ALIGN.LEFT)
        add_text(slide, CONTENT_LEFT + 0.80, y + 0.08, CONTENT_W - 1.0, 0.32,
                 title, size=12, bold=True, color=RED_D)
        add_text(slide, CONTENT_LEFT + 0.80, y + 0.42, CONTENT_W - 1.0, 0.44,
                 detail, size=9.5, color=DARK,
                 anchor=MSO_ANCHOR.TOP)
        y += card_h + 0.12


# ─────────────────────────────────────────────────────────────────────────────
# SLIDE 12 — Recent Quarters
# ─────────────────────────────────────────────────────────────────────────────
def slide_recent_quarters(prs, report):
    symbol = report.get("symbol", "")
    bo = report.get("business_overview", {}) or {}
    company = bo.get("company_name") or symbol
    rh = report.get("recent_highlights", {}) or {}
    quarterly = rh.get("quarterly_data", []) or []
    ai_summary = rh.get("ai_summary") or ""
    qoq = rh.get("qoq_commentary", []) or []

    slide = _blank_slide(prs)
    clear_slide(slide)
    header_band(prs, slide, "Recent Quarters",
                subtitle=f"{company}  ·  {symbol}",
                kicker="SECTION 5 — RECENT QUARTERS")

    y = CONTENT_TOP
    if quarterly:
        try:
            qs = sorted(quarterly, key=lambda q: q.get("date", ""), reverse=True)[:4]
        except Exception:
            qs = quarterly[:4]
        by_key = {q.get("quarter", ""): q for q in quarterly if q.get("quarter")}

        def _same_prior(qstr: str):
            parts = qstr.split()
            if len(parts) != 2 or not parts[1].isdigit():
                return None
            return by_key.get(f"{parts[0]} {int(parts[1]) - 1}")

        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "Last 4 quarters — beat/miss summary", size=12, bold=True, color=NAVY)
        rows = []
        for q in qs:
            rev = q.get("revenue")
            eps = q.get("eps")
            surprise = q.get("eps_surprise")
            prior = _same_prior(q.get("quarter") or "")
            yoy_str = "—"
            yoy_flag = None
            if prior and prior.get("revenue") and rev:
                yoy = (rev - prior["revenue"]) / abs(prior["revenue"]) * 100
                yoy_str = f"{yoy:+.1f}%"
                yoy_flag = "pos" if yoy > 0 else "neg"
            surp_str = f"{surprise:+.1f}%" if surprise is not None else "—"
            surp_flag = None
            if surprise is not None:
                surp_flag = "pos" if surprise > 0 else "neg"
            rows.append({
                "label": q.get("quarter") or q.get("date", "")[:10],
                "cells": [fmt_money(rev, 1), yoy_str,
                          f"${eps:.2f}" if eps else "N/A", surp_str],
                "flag": surp_flag, "bold": False,
            })
        statement_table(slide, CONTENT_LEFT, y + 0.32, CONTENT_W, 2.0,
                        ["Quarter", "Revenue", "Rev YoY", "EPS", "EPS Surprise"],
                        rows, first_col_frac=0.20)
        y += 2.5

    # AI summary block
    if ai_summary:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "Trend narrative", size=12, bold=True, color=NAVY)
        add_paragraph_block(slide, CONTENT_LEFT, y + 0.32, CONTENT_W,
                            CONTENT_BOT - y - 0.4,
                            _trim_paras(ai_summary, 8), size=10, color=DARK,
                            line_spacing=1.3, para_space_after=6)
    elif qoq:
        add_text(slide, CONTENT_LEFT, y, CONTENT_W, 0.28,
                 "QoQ commentary", size=12, bold=True, color=NAVY)
        add_paragraph_block(slide, CONTENT_LEFT, y + 0.32, CONTENT_W,
                            CONTENT_BOT - y - 0.4,
                            [f"• {_clean(l)}" for l in qoq[:8]],
                            size=10, color=DARK, line_spacing=1.3, para_space_after=4)


# ═════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════════
def generate_pptx_report(report_data: Dict[str, Any]) -> io.BytesIO:
    """Build the full company report PowerPoint deck.

    Deep-dive-driven order (when report_data['deep_dive']['synthesis']['status'] == 'success'):
      1. Snapshot with Section 1 (Strategic Investment Brief)
      2. Section 2: Business Model & Moat
      3. Section 3: Financial Quality
      4. Section 4: Competitive Position
      5. Section 5: Risk Stack (Ranked)
      then financials, valuation, recent quarters
    Fallback when no deep dive: original order (snapshot, business positioning, thesis, risks).
    """
    prs = Presentation()
    prs.slide_width = Inches(SLIDE_W)
    prs.slide_height = Inches(SLIDE_H)

    # Try to get deep dive
    deep_dive = report_data.get("deep_dive") or {}
    synthesis = deep_dive.get("synthesis") or {}
    dd_sections: Dict[int, Dict[str, Any]] = {}
    if synthesis.get("status") == "success":
        dd_sections = parse_deep_dive_sections(synthesis.get("analysis") or "")

    symbol = report_data.get("symbol", "")
    company = (report_data.get("business_overview") or {}).get("company_name") or symbol

    subagents = (deep_dive.get("subagents") or {})

    if dd_sections.get(1):
        # New deep-dive-driven flow
        slide_snapshot_with_brief(prs, report_data, section1=dd_sections.get(1))

        # Section 2 — synthesis, then combined addenda block
        if dd_sections.get(2):
            slide_deep_dive_section(prs, 2, dd_sections[2], company, symbol)
            moat_memo = subagents.get("business_moat", {}).get("analysis", "")
            desc = (report_data.get("business_overview") or {}).get("description", "")
            addenda_2 = []
            part1 = extract_subagent_part(moat_memo, 1)
            if part1:
                addenda_2.append(("Business Model Decomposition", part1))
            kp = extract_description_section(desc, "Key Products Deep Dive")
            if kp:
                addenda_2.append(("Key Products Deep Dive", kp))
            part3 = extract_subagent_part(moat_memo, 3)
            if part3:
                addenda_2.append(("Customer & Demand Durability", part3))
            slide_addenda_combined(prs, 2, "Business Model & Moat",
                                   addenda_2, company, symbol,
                                   report_data=report_data)

        # Section 3 — synthesis, then combined Cash Gen + Capital Structure
        if dd_sections.get(3):
            slide_deep_dive_section(prs, 3, dd_sections[3], company, symbol)
            fq_memo = subagents.get("financial_quality", {}).get("analysis", "")
            addenda_3 = []
            for pnum, ptitle in ((2, "Cash Generation"),
                                 (3, "Capital Structure")):
                part = extract_subagent_part(fq_memo, pnum)
                if part:
                    addenda_3.append((ptitle, part))
            slide_addenda_combined(prs, 3, "Financial Quality",
                                   addenda_3, company, symbol,
                                   report_data=report_data)

        # Section 4 — synthesis, then Porter's Five Forces card row
        if dd_sections.get(4):
            slide_deep_dive_section(prs, 4, dd_sections[4], company, symbol)
            cp_memo = subagents.get("competitive_position", {}).get("analysis", "")
            part1 = extract_subagent_part(cp_memo, 1)
            if part1:
                forces = _parse_five_forces(part1.get("body", ""))
                if forces:
                    slide_five_forces(prs, forces, company, symbol)

        # Section 5 — synthesis Risk Stack (no enrichment; table already comprehensive)
        if dd_sections.get(5):
            slide_deep_dive_section(prs, 5, dd_sections[5], company, symbol)

        # NEW: Catalyst Calendar (synthesis §6)
        if dd_sections.get(6):
            slide_catalyst_calendar(prs, dd_sections[6], company, symbol)
    else:
        # Fallback — original layout
        slide_snapshot(prs, report_data)
        slides_business_positioning(prs, report_data)
        slide_thesis(prs, report_data)
        slide_risks(prs, report_data)

    # Financials: revenue-margins overview + 3 statement analyses
    slide_revenue_margins(prs, report_data)
    slide_income_statement(prs, report_data)
    slide_cash_flow(prs, report_data)
    slide_balance_sheet(prs, report_data)

    slide_valuation(prs, report_data)
    # NEW: Scenario Analysis right after Valuation (synthesis §7)
    if dd_sections.get(7):
        slide_scenario_analysis(prs, dd_sections[7], company, symbol)

    slide_recent_quarters(prs, report_data)

    # NEW closing: Monitoring KPIs (synthesis §10)
    if dd_sections.get(10):
        slide_monitoring_kpis(prs, dd_sections[10], company, symbol)

    # Footers on every slide
    symbol = report_data.get("symbol", "")
    company = report_data.get("business_overview", {}).get("company_name") or symbol
    for i, slide in enumerate(prs.slides, 1):
        try:
            add_footer(prs, slide, i, company, symbol)
        except Exception as e:
            logger.debug(f"footer add failed on slide {i}: {e}")

    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)
    return buf

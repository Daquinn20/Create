"""
Evolution Fund + Disruption Index + S&P 500 technical screen runner.

Reads Evolution Fund positions, the local Disruption Index, and the S&P 500,
runs five technical screens (TLT, VCP Compression, Buy Trigger Daily,
Oversold, Williams %R Reversal) against each ticker list, and merges results
with the most recent TECG fundamental composite rankings. Outputs a
multi-sheet Excel report.

Usage:
  python screen_evolution_fund.py
  python screen_evolution_fund.py --workers 20
  python screen_evolution_fund.py --no-disruption    # skip Disruption
  python screen_evolution_fund.py --no-sp500         # skip S&P 500

Outputs:
  reports/evolution_screen_<timestamp>.xlsx
"""

from __future__ import annotations

import argparse
import os
import re
import smtplib
import sys
import types
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (Image, PageBreak, Paragraph, SimpleDocTemplate,
                                Spacer, Table, TableStyle)

load_dotenv()

LOGO_PATH = Path(__file__).parent / "company_logo.png"

# --- Paths ---
ROOT = Path(__file__).parent
EVOLUTION_FUND = Path(
    r"C:\Users\daqui\OneDrive\Documents\Kite Evolution\Evolution Fund DEC 2024.xlsx"
)
EVOLUTION_SHEET = "Fund 2023"
DISRUPTION_CSV = ROOT / "disruption_index.csv"
SP500_FMP_URL = "https://financialmodelingprep.com/api/v3/sp500_constituent"
COMPOSITE_DIR = Path(
    r"C:\Users\daqui\OneDrive\Documents\Targeted Equity Consulting Group\TECG FUNDAMENTAL COMPOSITE SCORE"
)
REPORTS_DIR = ROOT / "reports"

# --- Stub Streamlit before importing the screener ---
_fake_st = types.ModuleType("streamlit")


def _noop(*a, **k):
    return None


def _cache_data(*a, **k):
    if a and callable(a[0]):
        return a[0]
    return lambda fn: fn


_fake_st.set_page_config = _noop
_fake_st.cache_data = _cache_data
_fake_st.secrets = {}
_fake_st.warning = _noop
_fake_st.error = _noop
_fake_st.info = _noop
_fake_st.success = _noop
sys.modules["streamlit"] = _fake_st

from Technical_Screen_Quinn import DataFetcher, StockScreener  # noqa: E402
from tlt_engine_core import TLTEngine  # noqa: E402


def _read_excel_unlocked(path: Path, **kwargs) -> pd.DataFrame:
    """Read an Excel file even if it's open elsewhere (Excel/OneDrive lock)
    by copying to a temp file first when a PermissionError is raised."""
    import shutil
    import tempfile
    try:
        return pd.read_excel(path, **kwargs)
    except PermissionError:
        tmp = Path(tempfile.gettempdir()) / f"_tmp_{path.name}"
        shutil.copy2(path, tmp)
        try:
            return pd.read_excel(tmp, **kwargs)
        finally:
            try:
                tmp.unlink()
            except Exception:
                pass


def load_evolution_holdings() -> list[str]:
    fund = _read_excel_unlocked(EVOLUTION_FUND, sheet_name=EVOLUTION_SHEET, header=13)
    h = fund[fund["SYMBOL"].notna()].copy()
    h = h[h["SYMBOL"].astype(str).str.strip().str.lower() != "symbol"]
    h["SYMBOL"] = h["SYMBOL"].astype(str).str.strip().str.upper()
    h = h[h["SYMBOL"].str.match(r"^[A-Z]{1,5}$", na=False)]
    return sorted(set(h["SYMBOL"]) - {"CASH", "DATE"})


def load_disruption() -> list[str]:
    df = pd.read_csv(DISRUPTION_CSV)
    return sorted(set(df["Ticker"].astype(str).str.strip().str.upper()))


def load_sp500() -> list[str]:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        raise RuntimeError("FMP_API_KEY not set in .env — required for S&P 500 universe.")
    resp = requests.get(f"{SP500_FMP_URL}?apikey={api_key}", timeout=20)
    resp.raise_for_status()
    return sorted({str(item["symbol"]).strip().upper() for item in resp.json() if item.get("symbol")})


def find_latest_composite() -> Path:
    candidates = sorted(COMPOSITE_DIR.glob("fundamental_composite_*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"No fundamental_composite_*.xlsx in {COMPOSITE_DIR}")
    # Sort by the embedded YYYY-MM-DD if present, else by mtime
    date_pat = re.compile(r"(\d{4}-\d{2}-\d{2})")

    def keyfn(p: Path):
        m = date_pat.search(p.name)
        if m:
            return (m.group(1), p.stat().st_mtime)
        return ("", p.stat().st_mtime)

    return sorted(candidates, key=keyfn)[-1]


def load_composite(path: Path) -> pd.DataFrame:
    df = _read_excel_unlocked(path)
    df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
    return df


def screen_one(args):
    """Run all four per-stock screens for one ticker. Pickle-safe args bundle."""
    symbol, screener, spy_df, spy_close, tlt_engine = args
    out = {"Symbol": symbol}
    info_lookup: dict = {}

    # TLT
    try:
        r = screener._process_single_tlt(symbol, info_lookup, tlt_engine)
        if r:
            tier = re.search(r"(LEADER|SURGE|OVERSOLD|SPRING|DANGER|NEUTRAL)", str(r.get("Signal", "")))
            out["TLT_Tier"] = tier.group(1) if tier else ""
            out["TLT_Score"] = r.get("Score", "")
            out["RSI"] = r.get("RSI", "")
            out["LR_Ratio"] = r.get("LR Ratio", "")
            out["CMF"] = r.get("CMF", "")
            out["MRS"] = r.get("MRS", "")
            out["vs_MA200"] = r.get("vs MA200", "")
            out["vs_52w"] = r.get("vs 52wHigh", "")
    except Exception:
        pass

    # VCP
    try:
        r = screener._process_single_vcp(symbol, info_lookup, spy_df)
        if r:
            out["VCP_Grade"] = r.get("Grade", "")
            out["VCP_Score"] = r.get("Score", "")
    except Exception:
        pass

    # Buy Trigger (daily)
    try:
        r = screener._process_single_buy_trigger(symbol, info_lookup, spy_close, "daily")
        if r:
            out["BT_Grade"] = r.get("Grade", "")
            out["BT_Score"] = r.get("Score", "")
    except Exception:
        pass

    # Oversold
    try:
        r = screener._process_single_oversold(symbol, info_lookup)
        if r:
            out["OS_Grade"] = r.get("Grade", "")
            out["OS_Score"] = r.get("Score", "")
            out["RSI_2"] = r.get("RSI(2)", "")
            out["WPR"] = r.get("WPR", "")
    except Exception:
        pass

    # Williams %R Reversal trigger
    try:
        r = screener._process_single_wr_trigger(symbol, info_lookup)
        if r:
            out["WR_Grade"] = r.get("Grade", "")
            out["WR_Score"] = r.get("Score", "")
            out["WR_Path"] = r.get("WPR Path", "")
    except Exception:
        pass

    return out


def run_screens(tickers: list[str], label: str, workers: int) -> pd.DataFrame:
    fetcher = DataFetcher()
    screener = StockScreener(fetcher)

    print(f"\n[{label}] Fetching SPY benchmark...")
    spy_df = fetcher.get_historical_data("SPY", "2y")
    try:
        spy_close = yf.Ticker("SPY").history(period="2y")["Close"]
        if spy_close.index.tz is not None:
            spy_close.index = spy_close.index.tz_localize(None)
    except Exception:
        spy_close = spy_df["Close"] if spy_df is not None and "Close" in spy_df.columns else None

    tlt_engine = TLTEngine(benchmark_data=spy_df, mode="high_conviction")

    print(f"[{label}] Running 5 screens on {len(tickers)} tickers with {workers} workers...")
    rows = []
    done = 0
    args_list = [(t, screener, spy_df, spy_close, tlt_engine) for t in tickers]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(screen_one, a): a[0] for a in args_list}
        for fut in as_completed(futures):
            rows.append(fut.result())
            done += 1
            if done % 50 == 0 or done == len(tickers):
                print(f"  [{label}] {done}/{len(tickers)}")
    return pd.DataFrame(rows).sort_values("Symbol").reset_index(drop=True)


def merge_with_composite(screen_df: pd.DataFrame, composite: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "Symbol", "Sector", "Industry",
        "Fundamental Decile", "Business Decile", "Technicals (P5)",
        "1W %", "1M %", "3M %",
    ]
    fc = composite[[c for c in keep if c in composite.columns]].copy()
    return screen_df.merge(fc, on="Symbol", how="left")


def add_industry_rsi_percentile(*dfs: pd.DataFrame, min_peers: int = 4) -> None:
    """Compute each name's RSI percentile within its Industry across the combined
    universe of all supplied DataFrames, then write Industry_RSI_Pct and
    Industry_Peer_N columns back into every df in-place.

    Industries with fewer than ``min_peers`` members are left blank (too few
    peers for a meaningful rank).
    """
    frames = [d for d in dfs if d is not None and not d.empty]
    if not frames:
        return

    combined = pd.concat(
        [d[["Symbol", "RSI", "Industry"]] for d in frames if "RSI" in d.columns and "Industry" in d.columns],
        ignore_index=True,
    )
    # Dedupe by Symbol (a name can appear in multiple universes)
    combined = combined.sort_values("Industry", na_position="last").drop_duplicates("Symbol", keep="first")
    combined["RSI"] = pd.to_numeric(combined["RSI"], errors="coerce")
    valid = combined[combined["RSI"].notna() & combined["Industry"].notna() &
                     (combined["Industry"].astype(str).str.strip() != "")].copy()
    valid["Industry_Peer_N"] = valid.groupby("Industry")["Symbol"].transform("count")
    valid["Industry_RSI_Pct"] = (
        valid.groupby("Industry")["RSI"].rank(pct=True) * 100
    ).round(1)
    # Blank out ranks where the peer pool is too small
    valid.loc[valid["Industry_Peer_N"] < min_peers, "Industry_RSI_Pct"] = pd.NA

    lookup = valid.set_index("Symbol")[["Industry_RSI_Pct", "Industry_Peer_N"]]
    for d in frames:
        d["Industry_RSI_Pct"] = d["Symbol"].map(lookup["Industry_RSI_Pct"])
        d["Industry_Peer_N"] = d["Symbol"].map(lookup["Industry_Peer_N"])


def add_summary_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows with notable signals for the Top Signals sheet."""
    df = df.copy()
    flags = []
    for _, r in df.iterrows():
        f = []
        if r.get("TLT_Tier") in ("LEADER", "SURGE", "OVERSOLD", "SPRING"):
            f.append(f"TLT:{r['TLT_Tier']}")
        if r.get("TLT_Tier") == "DANGER":
            f.append("TLT:DANGER")
        if r.get("VCP_Grade") == "PASS":
            f.append(f"VCP:{r.get('VCP_Score','')}")
        if r.get("BT_Grade") == "PASS":
            f.append(f"BT:{r.get('BT_Score','')}")
        if r.get("OS_Grade") == "PASS":
            f.append(f"OS:{r.get('OS_Score','')}")
        if r.get("WR_Grade") == "PASS":
            path = r.get("WR_Path", "")
            f.append(f"WR:{r.get('WR_Score','')}{('/' + path) if path else ''}")
        flags.append(", ".join(f))
    df["Flags"] = flags
    return df


EMAIL_RECIPIENT_DEFAULT = "daquinn@targetedequityconsulting.com"


def _safe_num(v, default=None):
    """Coerce a value to float, return default if NaN/None/empty."""
    if v is None or v == "":
        return default
    try:
        f = float(v)
        if pd.isna(f):
            return default
        return f
    except (ValueError, TypeError):
        return default


def build_notes(row: pd.Series) -> str:
    """Generate a one-line interpretive note for a top-signal row.

    Rule-based templating that approximates the hand-curated Notes column
    (Symbol/Signal/FundDec/BizDec/Notes format the user prefers).
    """
    flags = str(row.get("Flags", ""))
    fd = _safe_num(row.get("Fundamental Decile"))
    bd = _safe_num(row.get("Business Decile"))
    m3 = _safe_num(row.get("3M %"))
    m1 = _safe_num(row.get("1M %"))
    vs52 = str(row.get("vs_52w", ""))
    mrs = _safe_num(row.get("MRS"))

    # Lead descriptor
    if fd is None:
        lead = "Not in composite — speculative add"
    elif fd >= 9 and (bd is None or bd >= 8):
        lead = "Top-decile fundamentals"
    elif fd >= 8 and bd is not None and bd <= 3:
        lead = f"Strong fund rank (Dec {int(fd)}) but BizDec {int(bd)} ←"
    elif fd >= 7:
        lead = f"Solid fundamentals (Dec {int(fd)})"
    elif fd >= 4:
        lead = f"Mid-tier fundamentals (Dec {int(fd)})"
    else:
        lead = f"Bottom-tier fundamentals (Dec {int(fd)}) — speculative"

    # Signal-specific addons
    addons = []
    if "VCP" in flags:
        # Compression — emphasize position vs 52w
        try:
            vs52_num = float(str(vs52).rstrip("%"))
            if -5 < vs52_num <= 0:
                addons.append("compression near 52w high")
            elif vs52_num <= -15:
                addons.append(f"deeper base ({vs52})")
            else:
                addons.append(f"compression forming ({vs52} from high)")
        except ValueError:
            addons.append("compression setup")
    if "BT" in flags:
        addons.append("fresh momentum trigger")
    if "WR:" in flags:
        if "/A" in flags:
            addons.append("Williams %R oversold, momentum turning")
        elif "/B" in flags:
            addons.append("Williams %R oversold-bounce reclaim")
        else:
            addons.append("Williams %R reversal trigger")
    if "TLT:DANGER" in flags:
        try:
            vs52_num = float(str(vs52).rstrip("%"))
            if vs52_num <= -20:
                addons.append(f"deep drawdown ({vs52})")
        except ValueError:
            pass
        if mrs is not None and mrs <= -1.5:
            addons.append("MRS deeply negative")

    # Momentum tail
    if m3 is not None and m3 >= 25:
        addons.append(f"+{m3:.0f}% 3M")
    elif m3 is not None and m3 <= -20:
        addons.append(f"{m3:.0f}% 3M")
    elif m1 is not None and m1 >= 10:
        addons.append(f"+{m1:.0f}% 1M")

    return lead + (" — " + ", ".join(addons) if addons else "")


_CELL_STYLE = ParagraphStyle(
    "cell", fontName="Helvetica", fontSize=9, leading=11, wordWrap="CJK"
)


_SCREEN_DEFINITIONS = [
    (
        "1. TLT Tier (Trend / Liquidity / Tape regime)",
        "Composite regime classifier that scores a name on its position within its own "
        "trend, its money-flow profile, and its strength relative to SPY. Each ticker "
        "is bucketed into one of five tiers: <b>LEADER</b> (high-conviction uptrend, "
        "outperforming the benchmark), <b>SURGE</b> (sharp expansion off a base), "
        "<b>SPRING</b> (early-trend reclaim), <b>OVERSOLD</b> (washed-out but stabilizing), "
        "<b>NEUTRAL</b> (no edge), and <b>DANGER</b> (broken trend with weakening internals). "
        "The TLT_Score is a 0&ndash;100 composite. <b>Flag fires</b> when the tier is "
        "non-NEUTRAL. DANGER is treated as a contrarian short signal and is broken out "
        "into its own table when it appears."
    ),
    (
        "2. VCP &mdash; Volatility Contraction Pattern",
        "A Minervini-style base-quality screen that looks for a stock building a tight, "
        "low-volume consolidation near its highs, with successively shallower pullbacks "
        "and contracting volatility. The grade is reported as <b>x/9</b>, where each "
        "point reflects a specific criterion (price within range of 52w high, MA stack, "
        "ATR contraction, volume dry-up, depth of last pullback, etc.). "
        "<b>PASS</b> requires a high score (typically 7+/9). A PASS implies a name is "
        "coiled for a breakout; the closer the price is to the 52w high while the score "
        "is high, the better the setup."
    ),
    (
        "3. Buy Trigger (Daily)",
        "A short-term momentum trigger that fires when a stock has just completed a "
        "tactical reclaim &mdash; price closing above a key moving average, MACD line "
        "above signal, and RSI above its 14-period SMA &mdash; with confirming volume "
        "and breadth. Grade is reported <b>x/5</b>. <b>PASS = 5/5</b> means every "
        "criterion is met today (a fresh trigger, not a stale one). WATCHLIST (4/5) "
        "means one criterion is missing and the name is a day or two away. This is the "
        "highest-frequency screen in the suite and produces the bulk of the daily flags."
    ),
    (
        "4. Oversold",
        "Mean-reversion screen designed to flag names that are stretched to the downside "
        "but showing early signs of stabilization. Grade is <b>x/6</b> and combines RSI(2) "
        "extremes, Williams %R, distance from a long-term MA, and a confirmation that "
        "the most recent bar isn&rsquo;t still falling. <b>PASS</b> = all six criteria. "
        "WATCHLIST means a name is close but hasn&rsquo;t fully turned. Useful for finding "
        "tactical long entries inside a broader uptrend, or short-cover candidates."
    ),
    (
        "5. Williams %R Reversal",
        "Two-path reversal trigger built on Williams %R(14). <b>Path A</b> requires the "
        "indicator to be deeply oversold (&lt; -80) <i>and</i> turning up &mdash; "
        "early-reversal momentum. <b>Path B</b> requires that %R has already reclaimed "
        "the -80 level after a recent oversold print &mdash; oversold-bounce confirmation. "
        "Grade is <b>x/4</b>; <b>PASS = 4/4</b> on either path, with the path letter "
        "appended (e.g. <b>WR:4/4/B</b>). Path A is the more aggressive entry; Path B is "
        "the more confirmed entry. Both are most useful on high-quality names that have "
        "pulled back inside an established uptrend."
    ),
    (
        "Reading the Flags column",
        "Flags concatenate every screen a row passed today. Example: "
        "<b>TLT:LEADER, BT:5/5, WR:4/4/B</b> means the name is a TLT Leader, fired a "
        "fresh Buy Trigger, and printed a Williams %R Path-B reversal. The more flags "
        "stacked, the higher the conviction. <b>TLT:DANGER</b> is the only bearish "
        "flag &mdash; it indicates a broken-trend name and is shown in a separate "
        "table for contrarian short consideration."
    ),
    (
        "RSI Rank column (peer-relative momentum)",
        "Each name's 14-day RSI is ranked against every other name in the same "
        "industry across the combined universe (Evolution + Disruption + "
        "S&amp;P 500). The <b>RSI Rank</b> column shows the percentile and the "
        "peer-pool size, e.g. <b>94 (n=46)</b> means the name sits at the 94th "
        "percentile of RSI within a 46-name industry pool. <b>100</b> = strongest "
        "RSI in its peer group; <b>0</b> = weakest. This adds a peer-relative "
        "read on top of the absolute RSI number: a name at RSI 55 may look "
        "mid-range in isolation but sit in the top decile of its industry, or "
        "vice versa. Industries with fewer than four peers are left blank."
    ),
    (
        "Fundamental and Business decile context",
        "Every signal row is merged against the TECG Fundamental Composite ranking. "
        "<b>FundDec</b> is the fundamental decile (10 = top 10% of the universe on the "
        "fundamental composite). <b>BizDec</b> is the business-quality decile (margins, "
        "returns on capital, growth durability). A high-decile fundamental name passing "
        "a momentum screen is the highest-quality combination; a low-decile name passing "
        "the same screen is treated as speculative and called out in the Notes column."
    ),
]


def _table_data(df: pd.DataFrame, max_rows: int = 30) -> list[list]:
    """Build a 6-column table: Symbol / Signal / FundDec / BizDec / RSI Rank / Notes.

    Signal and Notes are wrapped in Paragraphs so ReportLab word-wraps long
    text within the cell instead of overflowing the page.
    """
    header = ["Symbol", "Signal", "FundDec", "BizDec", "RSI Rank", "Notes"]
    rows = [header]
    for _, r in df.head(max_rows).iterrows():
        fd = _safe_num(r.get("Fundamental Decile"))
        bd = _safe_num(r.get("Business Decile"))
        pct = _safe_num(r.get("Industry_RSI_Pct"))
        peer_n = _safe_num(r.get("Industry_Peer_N"))
        if pct is None or peer_n is None:
            rsi_rank = ""
        else:
            rsi_rank = f"{int(round(pct))} (n={int(peer_n)})"
        rows.append([
            str(r.get("Symbol", "")),
            Paragraph(str(r.get("Flags", "")), _CELL_STYLE),
            "n/a" if fd is None else str(int(fd)),
            "n/a" if bd is None else str(int(bd)),
            rsi_rank,
            Paragraph(build_notes(r), _CELL_STYLE),
        ])
    return rows


def _styled_table(data: list[list]) -> Table:
    """Build a reportlab Table with consistent styling."""
    col_widths = [0.7 * inch, 1.1 * inch, 0.55 * inch, 0.55 * inch,
                  0.85 * inch, 3.35 * inch]
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e79")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cccccc")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t


def _peer_rsi_table_data(df: pd.DataFrame, max_rows: int = 60) -> list[list]:
    """Build a 5-column table: Symbol / Industry / RSI / Pct / Peers."""
    header = ["Symbol", "Industry", "RSI", "Ind %ile", "Peers"]
    rows = [header]
    for _, r in df.head(max_rows).iterrows():
        pct = _safe_num(r.get("Industry_RSI_Pct"))
        peer_n = _safe_num(r.get("Industry_Peer_N"))
        rsi = _safe_num(r.get("RSI"))
        rows.append([
            str(r.get("Symbol", "")),
            Paragraph(str(r.get("Industry", "") or ""), _CELL_STYLE),
            "" if rsi is None else f"{rsi:.1f}",
            "" if pct is None else f"{pct:.0f}",
            "" if peer_n is None else str(int(peer_n)),
        ])
    return rows


def _styled_peer_table(data: list[list]) -> Table:
    col_widths = [0.7 * inch, 3.4 * inch, 0.7 * inch, 0.8 * inch, 0.7 * inch]
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e79")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (2, 1), (4, -1), "RIGHT"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cccccc")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t


def generate_pdf(out_path: Path, evolution_top: pd.DataFrame,
                 disruption_top: pd.DataFrame, sp500_top: pd.DataFrame,
                 composite_file: str,
                 holdings_peer: pd.DataFrame | None = None) -> Path:
    """Render a single-PDF summary with logo + curated tables."""
    doc = SimpleDocTemplate(str(out_path), pagesize=letter,
                            topMargin=0.5 * inch, bottomMargin=0.5 * inch,
                            leftMargin=0.5 * inch, rightMargin=0.5 * inch)
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=16,
                        textColor=colors.HexColor("#1f4e79"), spaceAfter=6)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=12,
                        textColor=colors.HexColor("#1f4e79"), spaceBefore=12, spaceAfter=4)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=9, leading=12)

    story = []

    # Logo header
    if LOGO_PATH.exists():
        img = Image(str(LOGO_PATH), width=2.5 * inch, height=2.5 * inch * (1188 / 1836))
        img.hAlign = "CENTER"
        story.append(img)
        story.append(Spacer(1, 0.1 * inch))

    story.append(Paragraph("Evolution Fund — Daily Technical Screen", h1))
    story.append(Paragraph(
        f"Generated {datetime.now().strftime('%B %d, %Y at %H:%M')} &nbsp;|&nbsp; "
        f"Fundamental composite: {composite_file}", body))
    story.append(Spacer(1, 0.1 * inch))

    # Evolution Top Signals
    story.append(Paragraph("Evolution Holdings — Top Signals", h2))
    if evolution_top.empty:
        story.append(Paragraph("<i>No top signals on holdings today.</i>", body))
    else:
        story.append(_styled_table(_table_data(evolution_top)))

    # Disruption Top Signals — split into PASS-quality and DANGER
    danger = disruption_top[disruption_top["Flags"].astype(str).str.contains("DANGER", na=False)]
    bullish = disruption_top[~disruption_top["Flags"].astype(str).str.contains("DANGER", na=False)]

    if not bullish.empty:
        story.append(Paragraph("Disruption Index — Top Bullish Signals", h2))
        # Sort: highest fundamental decile first, then by Symbol
        bullish_sorted = bullish.sort_values(
            ["Fundamental Decile", "Symbol"], ascending=[False, True], na_position="last"
        )
        story.append(_styled_table(_table_data(bullish_sorted, max_rows=25)))

    if not danger.empty:
        story.append(Paragraph("Disruption Index — TLT DANGER (Contrarian Short Signals)", h2))
        story.append(_styled_table(_table_data(danger)))

    # S&P 500 Top Signals — same split
    if not sp500_top.empty:
        sp_danger = sp500_top[sp500_top["Flags"].astype(str).str.contains("DANGER", na=False)]
        sp_bullish = sp500_top[~sp500_top["Flags"].astype(str).str.contains("DANGER", na=False)]
        if not sp_bullish.empty:
            story.append(Paragraph("S&amp;P 500 — Top Bullish Signals", h2))
            sp_bullish_sorted = sp_bullish.sort_values(
                ["Fundamental Decile", "Symbol"], ascending=[False, True], na_position="last"
            )
            story.append(_styled_table(_table_data(sp_bullish_sorted, max_rows=25)))
        if not sp_danger.empty:
            story.append(Paragraph("S&amp;P 500 — TLT DANGER (Contrarian Short Signals)", h2))
            story.append(_styled_table(_table_data(sp_danger)))

    # Holdings — RSI vs Industry Peers
    if holdings_peer is not None and not holdings_peer.empty:
        peer_view = holdings_peer.dropna(subset=["Industry_RSI_Pct"]).sort_values(
            "Industry_RSI_Pct", ascending=False
        ).reset_index(drop=True)
        if not peer_view.empty:
            story.append(PageBreak())
            story.append(Paragraph("Holdings — RSI Percentile vs Industry Peers", h1))
            story.append(Paragraph(
                "Each holding's RSI ranked against every other name in the same "
                "industry across the combined universe (Evolution + Disruption + "
                "S&amp;P 500). 100 = strongest momentum in its peer group; 0 = "
                "weakest. Industries with fewer than 4 peers are blanked.",
                body))
            story.append(Spacer(1, 0.1 * inch))
            story.append(_styled_peer_table(_peer_rsi_table_data(peer_view)))

    # Appendix — what each setup means
    story.append(PageBreak())
    story.append(Paragraph("Appendix — Screen Definitions", h1))
    story.append(Paragraph(
        "Five technical screens run nightly against every name in the Evolution Fund, "
        "the Disruption Index, and the S&amp;P 500. A name surfaces on the Top Signals "
        "tables when it passes at least one screen. Below is what each setup measures, "
        "what a PASS means, and how to read the grade.",
        body))
    story.append(Spacer(1, 0.1 * inch))

    for title, blurb in _SCREEN_DEFINITIONS:
        story.append(Paragraph(title, h2))
        story.append(Paragraph(blurb, body))

    doc.build(story)
    return out_path


def send_report_email(xlsx_path: Path, summary_text: str, recipient: str,
                      extra_attachments: list[Path] | None = None) -> bool:
    """Email the Excel report as an attachment with a summary in the body."""
    user = os.getenv("EMAIL_ADDRESS")
    pwd = os.getenv("EMAIL_PASSWORD")
    if not user or not pwd:
        print("EMAIL_ADDRESS / EMAIL_PASSWORD not set in .env — skipping email.")
        return False
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = recipient
    msg["Subject"] = f"Evolution Fund Screen — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    msg.attach(MIMEText(summary_text, "plain"))

    attachments = [xlsx_path] + (list(extra_attachments) if extra_attachments else [])
    for path in attachments:
        if not path or not Path(path).exists():
            continue
        with open(path, "rb") as f:
            attach = MIMEBase("application", "octet-stream")
            attach.set_payload(f.read())
        encoders.encode_base64(attach)
        attach.add_header("Content-Disposition", f"attachment; filename={Path(path).name}")
        msg.attach(attach)

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--no-disruption", action="store_true",
                        help="Skip the Disruption Index scan.")
    parser.add_argument("--no-sp500", action="store_true",
                        help="Skip the S&P 500 scan.")
    parser.add_argument("--email", action="store_true",
                        help="Email the report when done (uses EMAIL_ADDRESS/EMAIL_PASSWORD from .env).")
    parser.add_argument("--recipient", default=EMAIL_RECIPIENT_DEFAULT,
                        help=f"Email recipient (default: {EMAIL_RECIPIENT_DEFAULT})")
    args = parser.parse_args()

    REPORTS_DIR.mkdir(exist_ok=True)

    # Inputs
    print(f"Evolution Fund:   {EVOLUTION_FUND}")
    holdings = load_evolution_holdings()
    print(f"  -> {len(holdings)} positions")

    if not args.no_disruption:
        print(f"Disruption Index: {DISRUPTION_CSV.name}")
        disruption = load_disruption()
        print(f"  -> {len(disruption)} tickers")
    else:
        disruption = []

    if not args.no_sp500:
        print("S&P 500: FMP sp500_constituent")
        try:
            sp500 = load_sp500()
            print(f"  -> {len(sp500)} tickers")
        except Exception as e:
            print(f"  ! Failed to fetch S&P 500 from FMP: {e}")
            sp500 = []
    else:
        sp500 = []

    composite_path = find_latest_composite()
    print(f"Fundamental composite: {composite_path.name}")
    composite = load_composite(composite_path)
    print(f"  -> {len(composite)} ranked stocks")

    # Run screens
    holdings_df = run_screens(holdings, "Evolution", args.workers)
    holdings_df = merge_with_composite(holdings_df, composite)
    holdings_df = add_summary_flags(holdings_df)

    if disruption:
        disruption_df = run_screens(disruption, "Disruption", args.workers)
        disruption_df = merge_with_composite(disruption_df, composite)
        disruption_df = add_summary_flags(disruption_df)
    else:
        disruption_df = pd.DataFrame()

    if sp500:
        sp500_df = run_screens(sp500, "SP500", args.workers)
        sp500_df = merge_with_composite(sp500_df, composite)
        sp500_df = add_summary_flags(sp500_df)
    else:
        sp500_df = pd.DataFrame()

    # RSI percentile rank vs industry peers, computed across the combined universe
    add_industry_rsi_percentile(holdings_df, disruption_df, sp500_df)

    # Top signals: anything with at least one notable flag, plus high decile + good tech
    def top_signals(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return df[df["Flags"].str.len() > 0].sort_values("Flags").reset_index(drop=True)

    top_holdings = top_signals(holdings_df)
    top_disruption = top_signals(disruption_df) if not disruption_df.empty else pd.DataFrame()
    top_sp500 = top_signals(sp500_df) if not sp500_df.empty else pd.DataFrame()

    # Write Excel
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = REPORTS_DIR / f"evolution_screen_{stamp}.xlsx"
    # Holdings sorted by industry RSI percentile (peer-relative momentum view)
    peer_cols = ["Symbol", "Sector", "Industry", "RSI", "Industry_RSI_Pct",
                 "Industry_Peer_N", "Flags", "Fundamental Decile", "Business Decile"]
    holdings_peer = holdings_df[[c for c in peer_cols if c in holdings_df.columns]].copy()
    holdings_peer = holdings_peer.sort_values(
        "Industry_RSI_Pct", ascending=False, na_position="last"
    ).reset_index(drop=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        holdings_df.to_excel(xw, sheet_name="Evolution Holdings", index=False)
        top_holdings.to_excel(xw, sheet_name="Evolution Top Signals", index=False)
        holdings_peer.to_excel(xw, sheet_name="Holdings RSI vs Peers", index=False)
        if not disruption_df.empty:
            disruption_df.to_excel(xw, sheet_name="Disruption Index", index=False)
            top_disruption.to_excel(xw, sheet_name="Disruption Top Signals", index=False)
        if not sp500_df.empty:
            sp500_df.to_excel(xw, sheet_name="S&P 500", index=False)
            top_sp500.to_excel(xw, sheet_name="S&P 500 Top Signals", index=False)
        # Metadata
        meta = pd.DataFrame({
            "Field": ["Run timestamp", "Evolution Fund file", "Disruption Index file",
                      "S&P 500 source", "Fundamental composite file",
                      "Holdings count", "Disruption count", "S&P 500 count"],
            "Value": [stamp, str(EVOLUTION_FUND), str(DISRUPTION_CSV),
                      "FMP sp500_constituent", str(composite_path),
                      len(holdings), len(disruption), len(sp500)],
        })
        meta.to_excel(xw, sheet_name="Run Info", index=False)

    print(f"\nExcel report: {out_path}")

    # Build PDF summary (curated, 1-2 pages, with logo)
    pdf_path = REPORTS_DIR / f"evolution_screen_{stamp}.pdf"
    try:
        generate_pdf(pdf_path, top_holdings, top_disruption, top_sp500,
                     composite_path.name, holdings_peer=holdings_peer)
        print(f"PDF summary:  {pdf_path}")
    except Exception as e:
        print(f"PDF generation failed: {e}")
        pdf_path = None

    # Build summary text (also used as email body)
    lines = [
        f"Run timestamp: {stamp}",
        f"Report file:   {out_path.name}",
        f"Fundamental composite: {composite_path.name}",
        "",
        "--- Evolution Holdings ---",
        f"  TLT non-NEUTRAL:    {(holdings_df['TLT_Tier'].fillna('NEUTRAL') != 'NEUTRAL').sum()}",
        f"  VCP PASS:           {(holdings_df.get('VCP_Grade') == 'PASS').sum()}",
        f"  Buy Trigger PASS:   {(holdings_df.get('BT_Grade') == 'PASS').sum()}",
        f"  Oversold PASS:      {(holdings_df.get('OS_Grade') == 'PASS').sum()}",
        f"  WR Reversal PASS:   {(holdings_df.get('WR_Grade') == 'PASS').sum()}",
    ]
    if not top_holdings.empty:
        lines.append("")
        lines.append("  Top-signal holdings: " + ", ".join(top_holdings["Symbol"].tolist()))
    if not disruption_df.empty:
        lines += [
            "",
            "--- Disruption Index ---",
            f"  TLT non-NEUTRAL:    {(disruption_df['TLT_Tier'].fillna('NEUTRAL') != 'NEUTRAL').sum()}",
            f"  VCP PASS:           {(disruption_df.get('VCP_Grade') == 'PASS').sum()}",
            f"  Buy Trigger PASS:   {(disruption_df.get('BT_Grade') == 'PASS').sum()}",
            f"  Oversold PASS:      {(disruption_df.get('OS_Grade') == 'PASS').sum()}",
            f"  WR Reversal PASS:   {(disruption_df.get('WR_Grade') == 'PASS').sum()}",
        ]
        if not top_disruption.empty:
            lines.append("")
            lines.append("  Top-signal Disruption names: " + ", ".join(top_disruption["Symbol"].head(20).tolist()))
    if not sp500_df.empty:
        lines += [
            "",
            "--- S&P 500 ---",
            f"  TLT non-NEUTRAL:    {(sp500_df['TLT_Tier'].fillna('NEUTRAL') != 'NEUTRAL').sum()}",
            f"  VCP PASS:           {(sp500_df.get('VCP_Grade') == 'PASS').sum()}",
            f"  Buy Trigger PASS:   {(sp500_df.get('BT_Grade') == 'PASS').sum()}",
            f"  Oversold PASS:      {(sp500_df.get('OS_Grade') == 'PASS').sum()}",
            f"  WR Reversal PASS:   {(sp500_df.get('WR_Grade') == 'PASS').sum()}",
        ]
        if not top_sp500.empty:
            lines.append("")
            lines.append("  Top-signal S&P 500 names: " + ", ".join(top_sp500["Symbol"].head(20).tolist()))
    summary_text = "\n".join(lines)
    print("\n" + summary_text)

    if args.email:
        try:
            extras = [pdf_path] if pdf_path else None
            sent = send_report_email(out_path, summary_text, args.recipient,
                                     extra_attachments=extras)
            print(f"\nEmail sent: {sent}")
        except Exception as e:
            print(f"\nEmail failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

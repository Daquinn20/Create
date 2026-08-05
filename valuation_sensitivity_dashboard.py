"""
5-Year Forward Earnings & Multiple Sensitivity Dashboard

Pulls FMP consensus EPS estimates for a ticker, lets the user override with
their own estimates, then builds a Present-Value sensitivity matrix:

    PV Today = (EPS_YearN * Terminal Multiple) / (1 + r)^N

Also shows upside/downside vs. the current market price for each cell.
"""

import os
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO

import numpy as np
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

load_dotenv()

def _secret(name: str) -> str:
    v = os.getenv(name)
    if v:
        return v
    try:
        val = st.secrets.get(name)
        return val or ""
    except Exception:
        return ""


FMP_API_KEY = _secret("FMP_API_KEY")
EMAIL_ADDRESS = _secret("EMAIL_ADDRESS")
EMAIL_PASSWORD = _secret("EMAIL_PASSWORD")
DEFAULT_RECIPIENT = "daquinn@targetedequityconsulting.com"
FMP_BASE = "https://financialmodelingprep.com/api/v3"
FMP_V4 = "https://financialmodelingprep.com/api/v4"

st.set_page_config(
    page_title="TECG - Valuation Sensitivity",
    page_icon="🎯",
    layout="wide",
)

st.markdown(
    """
    <style>
    .stApp { background-color: #000000; }
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div { color: #ffffff !important; }
    h1, h2, h3, h4 { color: #ffffff !important; }
    div[data-testid="stMetricValue"] { color: #17becf !important; font-size: 1.6rem !important; }
    div[data-testid="stMetricLabel"] { color: #cccccc !important; font-size: 0.95rem !important; }

     /* Sidebar: force black text on the light sidebar background */
      section[data-testid="stSidebar"],
      section[data-testid="stSidebar"] * { color: #000000 !important; }
      section[data-testid="stSidebar"] div[data-testid="stMetricValue"] { color: #0b5394 !important; }
      section[data-testid="stSidebar"] div[data-testid="stMetricLabel"] { color: #333333 !important; }

    /* Wider content area */
    .main .block-container { max-width: 1600px !important; padding-left: 2rem; padding-right: 2rem; }

    /* Section header pill */
    .section-header {
        background: linear-gradient(90deg, #17becf 0%, #1f77b4 100%);
        color: #ffffff !important;
        padding: 0.55rem 1rem;
        border-radius: 8px;
        font-size: 1.15rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        margin: 1.25rem 0 0.75rem 0;
    }

    /* Dataframe styling — larger, more legible cells */
    div[data-testid="stDataFrame"] { border: 1px solid #333333; border-radius: 8px; }
    div[data-testid="stDataFrame"] table { font-size: 15px !important; }
    div[data-testid="stDataFrame"] th {
        background-color: #1f2937 !important;
        color: #ffffff !important;
        font-weight: 700 !important;
        padding: 12px 14px !important;
        border-bottom: 2px solid #17becf !important;
    }
    div[data-testid="stDataFrame"] td { padding: 10px 14px !important; }

    /* Divider color */
    hr { border-color: #333333 !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


def section(title: str) -> None:
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)


@st.cache_data(ttl=900)
def fetch_consensus_estimates(ticker: str) -> pd.DataFrame:
    """Fetch annual analyst EPS estimates for the next several fiscal years."""
    if not FMP_API_KEY:
        raise RuntimeError("Missing FMP_API_KEY (env or Streamlit secrets)")

    url = f"{FMP_BASE}/analyst-estimates/{ticker}"
    resp = requests.get(url, params={"period": "annual", "limit": 10, "apikey": FMP_API_KEY}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    today = pd.Timestamp(datetime.now().date())
    df = df[df["date"] >= today - pd.Timedelta(days=30)]

    keep = [
        "date",
        "estimatedEpsAvg",
        "estimatedEpsLow",
        "estimatedEpsHigh",
        "numberAnalystsEstimatedEps",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["fiscal_year"] = df["date"].dt.year
    return df.reset_index(drop=True)


@st.cache_data(ttl=300)
def fetch_current_price(ticker: str) -> dict:
    """Fetch the latest quote for a ticker."""
    if not FMP_API_KEY:
        raise RuntimeError("Missing FMP_API_KEY (env or Streamlit secrets)")
    url = f"{FMP_BASE}/quote/{ticker}"
    resp = requests.get(url, params={"apikey": FMP_API_KEY}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return {}
    return data[0]


@st.cache_data(ttl=3600)
def fetch_wacc(ticker: str) -> dict:
    """Pull WACC + CAPM components from FMP's advanced DCF model."""
    if not FMP_API_KEY:
        raise RuntimeError("Missing FMP_API_KEY (env or Streamlit secrets)")
    url = f"{FMP_V4}/advanced_discounted_cash_flow"
    resp = requests.get(url, params={"symbol": ticker, "apikey": FMP_API_KEY}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return {}
    row = data[0]
    return {
        "wacc": row.get("wacc"),
        "costOfEquity": row.get("costOfEquity"),
        "costOfDebt": row.get("costofDebt"),
        "afterTaxCostOfDebt": row.get("afterTaxCostOfDebt"),
        "taxRate": row.get("taxRate"),
        "beta": row.get("beta"),
        "riskFreeRate": row.get("riskFreeRate"),
        "marketRiskPremium": row.get("marketRiskPremium"),
        "debtWeighting": row.get("debtWeighting"),
        "equityWeighting": row.get("equityWeighting"),
        "totalDebt": row.get("totalDebt"),
        "totalEquity": row.get("totalEquity"),
    }


def build_pv_matrix(eps_year_n: float, multiples: list[float], years: int, discount_rate: float) -> pd.DataFrame:
    """PV Today = (EPS_N * Multiple) / (1 + r)^N for each multiple."""
    discount = (1.0 + discount_rate) ** years
    rows = {f"{m:g}x": [(eps_year_n * m) / discount] for m in multiples}
    df = pd.DataFrame(rows, index=[f"${eps_year_n:,.2f}"])
    df.index.name = "EPS Year N"
    return df


def build_full_matrix(eps_values: list[float], multiples: list[float], years: int, discount_rate: float) -> pd.DataFrame:
    """Full grid: rows = EPS values, cols = multiples, values = PV Today."""
    discount = (1.0 + discount_rate) ** years
    data = {f"{m:g}x": [(eps * m) / discount for eps in eps_values] for m in multiples}
    df = pd.DataFrame(data, index=[f"${e:,.2f}" for e in eps_values])
    df.index.name = "EPS Year N"
    return df


def upside_matrix(pv_df: pd.DataFrame, current_price: float) -> pd.DataFrame:
    """Percent upside/downside vs. current price."""
    return (pv_df / current_price - 1.0) * 100.0


def send_report_email(
    recipient: str,
    ticker: str,
    pdf_bytes: bytes,
    headline_eps: float,
    discount_pct: float,
    years: int,
    current_price: float,
) -> tuple[bool, str]:
    """Email the valuation sensitivity PDF via Gmail SMTP."""
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return False, "EMAIL_ADDRESS / EMAIL_PASSWORD not configured in secrets."

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    msg = MIMEMultipart()
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = recipient
    msg["Subject"] = f"Valuation Sensitivity — {ticker} — {stamp}"

    body_lines = [
        f"Valuation sensitivity report for {ticker}",
        f"Run: {stamp}",
        "",
        f"Current price: ${current_price:,.2f}" if current_price else "Current price: n/a",
        f"Year-{years} EPS used: ${headline_eps:,.2f}",
        f"Discount rate: {discount_pct:.2f}%",
        f"Formula: PV = (EPS_{years} × Multiple) / (1 + {discount_pct:.2f}%)^{years}",
        "",
        "Full PDF attached.",
    ]
    msg.attach(MIMEText("\n".join(body_lines), "plain"))

    attach = MIMEBase("application", "pdf")
    attach.set_payload(pdf_bytes)
    encoders.encode_base64(attach)
    filename = f"{ticker}_valuation_sensitivity_{datetime.now():%Y%m%d_%H%M}.pdf"
    attach.add_header("Content-Disposition", f"attachment; filename={filename}")
    msg.attach(attach)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            s.send_message(msg)
        return True, f"Sent to {recipient}."
    except Exception as e:
        return False, f"SMTP error: {e}"


USABLE_WIDTH = 10.2 * inch  # landscape letter minus 0.4" margins each side


def _pv_table(df: pd.DataFrame, currency: bool = True, total_width: float = USABLE_WIDTH) -> Table:
    """Convert a PV or upside DataFrame to a styled reportlab Table filling the page width."""
    header = ["EPS Year N"] + list(df.columns)
    body = [header]
    for idx, row in df.iterrows():
        if currency:
            cells = [idx] + [f"${v:,.2f}" for v in row.values]
        else:
            cells = [idx] + [f"{v:+.1f}%" for v in row.values]
        body.append(cells)

    label_col_w = 1.4 * inch
    n_data_cols = len(df.columns)
    data_col_w = (total_width - label_col_w) / max(n_data_cols, 1)
    col_widths = [label_col_w] + [data_col_w] * n_data_cols

    tbl = Table(body, hAlign="LEFT", colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 11),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("LINEBELOW", (0, 0), (-1, 0), 1.5, colors.HexColor("#17becf")),
        ("FONTSIZE", (0, 1), (-1, -1), 11),
        ("TOPPADDING", (0, 1), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("LEFTPADDING", (0, 0), (0, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f3f4f6")]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BACKGROUND", (0, 1), (0, -1), colors.HexColor("#e5e7eb")),
        ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
    ]
    tbl.setStyle(TableStyle(style))
    return tbl


def build_pdf(
    ticker: str,
    price: float,
    quote: dict,
    consensus_df: pd.DataFrame,
    wacc_data: dict,
    discount_pct: float,
    years: int,
    discount_factor: float,
    used_wacc: bool,
    headline_eps: float,
    headline_df: pd.DataFrame,
    full_df: pd.DataFrame,
    eps_source_label: str,
) -> bytes:
    """Render the sensitivity report to a PDF and return the raw bytes."""
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(letter),
        leftMargin=0.4 * inch,
        rightMargin=0.4 * inch,
        topMargin=0.4 * inch,
        bottomMargin=0.4 * inch,
        title=f"{ticker} Valuation Sensitivity",
        author="Targeted Equity Consulting Group",
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleTECG", parent=styles["Title"], fontSize=20, spaceAfter=4, textColor=colors.HexColor("#111827")))
    styles.add(ParagraphStyle(
        name="H2TECG", parent=styles["Heading2"], fontSize=14,
        spaceBefore=14, spaceAfter=6,
        textColor=colors.white,
        backColor=colors.HexColor("#1f77b4"),
        borderPadding=(6, 8, 6, 8),
        leading=18,
    ))
    styles.add(ParagraphStyle(name="BodyTECG", parent=styles["BodyText"], fontSize=10, leading=13))
    styles.add(ParagraphStyle(name="SmallTECG", parent=styles["BodyText"], fontSize=9, textColor=colors.HexColor("#4b5563")))

    story: list = []

    # Title
    story.append(Paragraph(f"{ticker} — 5-Year Forward Earnings &amp; Multiple Sensitivity", styles["TitleTECG"]))
    story.append(Paragraph(
        f"Generated {datetime.now():%Y-%m-%d %H:%M} — Targeted Equity Consulting Group",
        styles["SmallTECG"],
    ))
    story.append(Spacer(1, 8))

    # Assumptions block
    mcap = quote.get("marketCap") or 0
    eps_ttm = quote.get("eps")
    assumptions_rows = [
        ["Ticker", ticker, "Current Price", f"${price:,.2f}" if price else "n/a"],
        ["Market Cap", f"${mcap/1e9:,.2f}B" if mcap else "n/a", "EPS TTM", f"{eps_ttm}" if eps_ttm is not None else "n/a"],
        [
            "Discount Rate",
            f"{discount_pct:.2f}%  ({'FMP WACC' if used_wacc else 'manual'})",
            "Years Forward",
            f"{years}",
        ],
        ["Discount Factor", f"{discount_factor:.6f}", "EPS Source", eps_source_label],
        ["Headline EPS (Year N)", f"${headline_eps:,.2f}", "Formula", f"PV = (EPS_{years} × Multiple) / (1 + {discount_pct:.2f}%)^{years}"],
    ]
    a_tbl = Table(
        assumptions_rows,
        colWidths=[1.7*inch, 2.5*inch, 1.7*inch, USABLE_WIDTH - 5.9*inch],
    )
    a_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#1f77b4")),
        ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#1f77b4")),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.white),
        ("TEXTCOLOR", (2, 0), (2, -1), colors.white),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
        ("FONTSIZE", (0, 0), (-1, -1), 11),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("ROWBACKGROUNDS", (1, 0), (-1, -1), [colors.white, colors.HexColor("#f3f4f6")]),
    ]))
    story.append(Paragraph("Assumptions", styles["H2TECG"]))
    story.append(a_tbl)
    story.append(Spacer(1, 8))

    # WACC breakdown
    if wacc_data.get("wacc") is not None:
        story.append(Paragraph("FMP WACC Breakdown", styles["H2TECG"]))
        w_rows = [
            ["WACC", f"{wacc_data.get('wacc'):.2f}%",
             "Cost of Equity", f"{wacc_data.get('costOfEquity') or 0:.2f}%",
             "After-tax Cost of Debt", f"{wacc_data.get('afterTaxCostOfDebt') or 0:.2f}%"],
            ["Beta", f"{wacc_data.get('beta') or 0:.2f}",
             "Risk-free Rate", f"{wacc_data.get('riskFreeRate') or 0:.2f}%",
             "Tax Rate", f"{wacc_data.get('taxRate') or 0:.2f}%"],
            ["Equity Weighting", f"{wacc_data.get('equityWeighting') or 0:.2f}%",
             "Debt Weighting", f"{wacc_data.get('debtWeighting') or 0:.2f}%",
             "", ""],
        ]
        w_col = USABLE_WIDTH / 6.0
        w_tbl = Table(w_rows, colWidths=[w_col]*6)
        w_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#1f77b4")),
            ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#1f77b4")),
            ("BACKGROUND", (4, 0), (4, -1), colors.HexColor("#1f77b4")),
            ("TEXTCOLOR", (0, 0), (0, -1), colors.white),
            ("TEXTCOLOR", (2, 0), (2, -1), colors.white),
            ("TEXTCOLOR", (4, 0), (4, -1), colors.white),
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
            ("FONTNAME", (4, 0), (4, -1), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
            ("FONTSIZE", (0, 0), (-1, -1), 11),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUNDS", (1, 0), (1, -1), [colors.white, colors.HexColor("#f3f4f6")]),
            ("ROWBACKGROUNDS", (3, 0), (3, -1), [colors.white, colors.HexColor("#f3f4f6")]),
            ("ROWBACKGROUNDS", (5, 0), (5, -1), [colors.white, colors.HexColor("#f3f4f6")]),
        ]))
        story.append(w_tbl)
        story.append(Spacer(1, 8))

    # Consensus estimates table
    if not consensus_df.empty:
        story.append(Paragraph("Consensus EPS Estimates (FMP)", styles["H2TECG"]))
        cons_rows = [["Fiscal Date", "EPS Avg", "EPS Low", "EPS High", "# Analysts"]]
        for _, r in consensus_df.iterrows():
            cons_rows.append([
                r["date"].strftime("%Y-%m-%d") if hasattr(r["date"], "strftime") else str(r["date"]),
                f"${r['estimatedEpsAvg']:,.2f}" if pd.notna(r.get('estimatedEpsAvg')) else "n/a",
                f"${r['estimatedEpsLow']:,.2f}" if pd.notna(r.get('estimatedEpsLow')) else "n/a",
                f"${r['estimatedEpsHigh']:,.2f}" if pd.notna(r.get('estimatedEpsHigh')) else "n/a",
                f"{int(r['numberAnalystsEstimatedEps'])}" if pd.notna(r.get('numberAnalystsEstimatedEps')) else "n/a",
            ])
        cons_col_w = [2.0*inch, 2.05*inch, 2.05*inch, 2.05*inch, USABLE_WIDTH - 8.15*inch]
        c_tbl = Table(cons_rows, hAlign="LEFT", colWidths=cons_col_w, repeatRows=1)
        c_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 11),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            ("LINEBELOW", (0, 0), (-1, 0), 1.5, colors.HexColor("#17becf")),
            ("FONTSIZE", (0, 1), (-1, -1), 11),
            ("TOPPADDING", (0, 1), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f3f4f6")]),
            ("ALIGN", (1, 0), (-1, -1), "CENTER"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("LEFTPADDING", (0, 0), (0, -1), 10),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(c_tbl)
        story.append(Spacer(1, 8))

    # Headline PV row
    story.append(Paragraph(
        f"Present Value — EPS ${headline_eps:,.2f} × Multiple, discounted {years}yr at {discount_pct:.2f}%",
        styles["H2TECG"],
    ))
    story.append(_pv_table(headline_df, currency=True))
    if price and price > 0:
        story.append(Spacer(1, 6))
        story.append(Paragraph(f"Upside / downside vs. current ${price:,.2f}:", styles["BodyTECG"]))
        story.append(_pv_table(upside_matrix(headline_df, price), currency=False))
    story.append(Spacer(1, 12))

    # Full sensitivity grid
    story.append(Paragraph("Full Sensitivity Grid — PV Today", styles["H2TECG"]))
    story.append(_pv_table(full_df, currency=True))
    if price and price > 0:
        story.append(Spacer(1, 8))
        story.append(Paragraph(f"Upside / Downside vs. ${price:,.2f}", styles["H2TECG"]))
        story.append(_pv_table(upside_matrix(full_df, price), currency=False))

    doc.build(story)
    return buf.getvalue()


# ============================================
# UI
# ============================================
st.title("5-Year Forward Earnings & Multiple Sensitivity")
st.caption("Discount future terminal value back to today: **PV = (EPS_N × Multiple) / (1 + r)^N**")

with st.sidebar:
    st.header("Inputs")
    ticker = st.text_input("Ticker", value="AAPL").strip().upper()
    fetch_btn = st.button("Fetch consensus + WACC", type="primary")

    st.divider()
    st.subheader("Discount")
    wacc_data = st.session_state.get("wacc_data", {})
    fmp_wacc = wacc_data.get("wacc")
    use_wacc = st.checkbox(
        "Use FMP WACC as discount rate",
        value=(fmp_wacc is not None),
        disabled=(fmp_wacc is None),
        help="Uncheck to enter a manual discount rate.",
    )
    if use_wacc and fmp_wacc is not None:
        discount_pct = float(fmp_wacc)
        st.metric("FMP WACC", f"{discount_pct:.2f}%")
    else:
        default_manual = float(fmp_wacc) if fmp_wacc is not None else 10.0
        discount_pct = st.number_input(
            "Manual discount rate (%)",
            min_value=0.0, max_value=30.0,
            value=default_manual, step=0.25,
        )
    years = st.number_input("Years forward (N)", min_value=1, max_value=15, value=5, step=1)
    discount_rate = discount_pct / 100.0
    discount_factor = 1.0 / ((1.0 + discount_rate) ** years)
    st.caption(f"Discount factor: **{discount_factor:.6f}**  (1 / (1+{discount_pct:g}%)^{years})")

    st.divider()
    st.subheader("Multiple range")
    mult_min = st.number_input("Min multiple", min_value=1.0, max_value=200.0, value=15.0, step=1.0)
    mult_max = st.number_input("Max multiple", min_value=1.0, max_value=300.0, value=35.0, step=1.0)
    mult_step = st.number_input("Step", min_value=0.5, max_value=25.0, value=5.0, step=0.5)

    st.divider()
    st.subheader("EPS grid")
    eps_min = st.number_input("Min EPS Year N", min_value=0.0, value=12.0, step=0.5)
    eps_max = st.number_input("Max EPS Year N", min_value=0.0, value=25.0, step=0.5)
    eps_step = st.number_input("EPS step", min_value=0.1, value=1.0, step=0.5)


# Fetch consensus + price + WACC
if fetch_btn or "consensus" not in st.session_state:
    if ticker:
        try:
            with st.spinner(f"Pulling FMP data for {ticker}..."):
                st.session_state["consensus"] = fetch_consensus_estimates(ticker)
                st.session_state["quote"] = fetch_current_price(ticker)
                try:
                    st.session_state["wacc_data"] = fetch_wacc(ticker)
                except Exception as wacc_err:
                    st.warning(f"WACC unavailable ({wacc_err}) — falling back to manual discount rate.")
                    st.session_state["wacc_data"] = {}
                st.session_state["ticker"] = ticker
                st.rerun()
        except Exception as e:
            st.error(f"FMP fetch failed: {e}")
            st.session_state["consensus"] = pd.DataFrame()
            st.session_state["quote"] = {}
            st.session_state["wacc_data"] = {}

consensus_df: pd.DataFrame = st.session_state.get("consensus", pd.DataFrame())
quote: dict = st.session_state.get("quote", {})
wacc_data: dict = st.session_state.get("wacc_data", {})
active_ticker: str = st.session_state.get("ticker", ticker)

# Top row: compact metrics only
section(f"{active_ticker} — Snapshot")
price = float(quote.get("price") or 0.0)
if price > 0:
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric(
        "Current Price", f"${price:,.2f}",
        delta=f"{quote.get('changesPercentage', 0):.2f}%" if quote.get("changesPercentage") is not None else None,
    )
    m2.metric("Market Cap", f"${(quote.get('marketCap') or 0)/1e9:,.2f}B")
    m3.metric("EPS TTM", f"{quote.get('eps')}" if quote.get("eps") is not None else "n/a")
    m4.metric("P/E TTM", f"{quote.get('pe'):.2f}" if quote.get("pe") is not None else "n/a")
    m5.metric(
        "FMP WACC" if wacc_data.get("wacc") is not None else "Discount Rate",
        f"{wacc_data.get('wacc'):.2f}%" if wacc_data.get("wacc") is not None else f"{discount_pct:.2f}%",
    )
else:
    st.info("No quote loaded — enter a ticker and click **Fetch consensus + WACC** in the sidebar.")

col_ovr1, col_ovr2 = st.columns([1, 4])
with col_ovr1:
    override_price = st.number_input("Override price (optional)", min_value=0.0, value=float(price), step=0.5)
current_price = override_price if override_price > 0 else price

# Consensus EPS — full width
section("Consensus EPS (FMP)")
if consensus_df.empty:
    st.info("No consensus estimates available.")
else:
    display = consensus_df.copy()
    display["date"] = display["date"].dt.strftime("%Y-%m-%d")
    display = display.rename(columns={
        "date": "Fiscal Date",
        "estimatedEpsAvg": "EPS Avg",
        "estimatedEpsLow": "EPS Low",
        "estimatedEpsHigh": "EPS High",
        "numberAnalystsEstimatedEps": "# Analysts",
    })
    display = display[[c for c in ["Fiscal Date", "EPS Avg", "EPS Low", "EPS High", "# Analysts"] if c in display.columns]]
    st.dataframe(
        display.style
            .format({"EPS Avg": "${:,.2f}", "EPS Low": "${:,.2f}", "EPS High": "${:,.2f}", "# Analysts": "{:.0f}"})
            .background_gradient(cmap="Blues", subset=["EPS Avg"]),
        hide_index=True, use_container_width=True,
        height=40 + 42 * len(display),
    )

# WACC breakdown — full width
if wacc_data.get("wacc") is not None:
    section(f"WACC Breakdown — FMP Model ({wacc_data['wacc']:.2f}%)")
    with st.expander("Show components", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("WACC", f"{wacc_data.get('wacc', 0):.2f}%")
        c2.metric("Cost of Equity", f"{wacc_data.get('costOfEquity') or 0:.2f}%")
        c3.metric("After-tax Cost of Debt", f"{wacc_data.get('afterTaxCostOfDebt') or 0:.2f}%")
        c4.metric("Tax Rate", f"{wacc_data.get('taxRate') or 0:.2f}%")
        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Beta", f"{wacc_data.get('beta') or 0:.2f}")
        c6.metric("Risk-free Rate", f"{wacc_data.get('riskFreeRate') or 0:.2f}%")
        c7.metric("Equity Weighting", f"{wacc_data.get('equityWeighting') or 0:.2f}%")
        c8.metric("Debt Weighting", f"{wacc_data.get('debtWeighting') or 0:.2f}%")
        st.caption(
            "WACC = (E/V × Cost of Equity) + (D/V × After-tax Cost of Debt).  "
            "Toggle **Use FMP WACC** off in the sidebar to enter your own rate."
        )

# EPS Year N: consensus vs custom
section(f"EPS in Year {years} — Consensus vs. Your Estimate")

consensus_eps_n = None
if not consensus_df.empty and len(consensus_df) >= years:
    consensus_eps_n = float(consensus_df.iloc[years - 1]["estimatedEpsAvg"])
    consensus_year = int(consensus_df.iloc[years - 1]["fiscal_year"])
    st.caption(f"Consensus for fiscal year **{consensus_year}** ({years} years out): **${consensus_eps_n:,.2f}**")
elif not consensus_df.empty:
    st.warning(f"FMP only returned {len(consensus_df)} forward year(s) — extend manually below.")

col_a, col_b = st.columns([1, 2])
with col_a:
    use_consensus = st.checkbox("Use consensus EPS for headline row", value=(consensus_eps_n is not None))
with col_b:
    default_custom = consensus_eps_n if consensus_eps_n else 20.0
    custom_eps = st.number_input("My EPS estimate (Year N)", min_value=0.0, value=float(default_custom), step=0.25)

headline_eps = consensus_eps_n if (use_consensus and consensus_eps_n is not None) else custom_eps

# Build multiples list
multiples = list(np.round(np.arange(mult_min, mult_max + 1e-9, mult_step), 2).tolist())
if not multiples:
    st.error("Multiple range is empty — check min/max/step.")
    st.stop()

# Headline row: PV for the single chosen EPS across all multiples
section(f"Present Value — EPS ${headline_eps:,.2f} × Multiple, discounted {years}yr @ {discount_pct:.2f}%")
headline_df = build_pv_matrix(headline_eps, multiples, years, discount_rate)
st.dataframe(
    headline_df.style.format("${:,.2f}").background_gradient(cmap="Greens", axis=1),
    use_container_width=True,
    height=110,
)
if current_price > 0:
    st.caption(f"**Upside / downside vs. current ${current_price:,.2f}:**")
    up_headline = upside_matrix(headline_df, current_price)
    st.dataframe(
        up_headline.style.format("{:+.1f}%").background_gradient(cmap="RdYlGn", axis=None),
        use_container_width=True,
        height=110,
    )

# Full sensitivity grid
section("Full Sensitivity Grid — Present Value Today")
if eps_max <= eps_min:
    st.error("EPS max must be > EPS min.")
    st.stop()
eps_values = list(np.round(np.arange(eps_min, eps_max + 1e-9, eps_step), 2).tolist())

full_df = build_full_matrix(eps_values, multiples, years, discount_rate)
grid_height = min(700, 50 + 42 * len(eps_values))
st.dataframe(
    full_df.style.format("${:,.2f}").background_gradient(cmap="Greens", axis=None),
    use_container_width=True,
    height=grid_height,
)

if current_price > 0:
    section(f"Upside / Downside vs. Current Price ${current_price:,.2f}")
    up_full = upside_matrix(full_df, current_price)
    st.dataframe(
        up_full.style.format("{:+.1f}%").background_gradient(cmap="RdYlGn", axis=None),
        use_container_width=True,
        height=grid_height,
    )

st.divider()

# Build PDF once (used by both download + email buttons)
eps_source_label = (
    f"Consensus (fiscal {int(consensus_df.iloc[years - 1]['fiscal_year'])})"
    if use_consensus and consensus_eps_n is not None else "Custom (user input)"
)
pdf_bytes = None
pdf_err_msg = None
try:
    pdf_bytes = build_pdf(
        ticker=active_ticker,
        price=current_price,
        quote=quote,
        consensus_df=consensus_df,
        wacc_data=wacc_data,
        discount_pct=discount_pct,
        years=int(years),
        discount_factor=discount_factor,
        used_wacc=bool(use_wacc and wacc_data.get("wacc") is not None),
        headline_eps=headline_eps,
        headline_df=headline_df,
        full_df=full_df,
        eps_source_label=eps_source_label,
    )
except Exception as pdf_err:
    pdf_err_msg = str(pdf_err)

# Downloads + email
col_dl1, col_dl2, col_dl3, col_dl4 = st.columns(4)
with col_dl1:
    st.download_button(
        "📄 PV grid (CSV)",
        data=full_df.to_csv().encode("utf-8"),
        file_name=f"{active_ticker}_pv_matrix_{datetime.now():%Y%m%d}.csv",
        mime="text/csv",
        use_container_width=True,
    )
with col_dl2:
    if current_price > 0:
        st.download_button(
            "📈 Upside grid (CSV)",
            data=upside_matrix(full_df, current_price).to_csv().encode("utf-8"),
            file_name=f"{active_ticker}_upside_matrix_{datetime.now():%Y%m%d}.csv",
            mime="text/csv",
            use_container_width=True,
        )
with col_dl3:
    if pdf_bytes:
        st.download_button(
            "📕 PDF report",
            data=pdf_bytes,
            file_name=f"{active_ticker}_valuation_sensitivity_{datetime.now():%Y%m%d}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    elif pdf_err_msg:
        st.error(f"PDF build failed: {pdf_err_msg}")
with col_dl4:
    email_disabled = not (EMAIL_ADDRESS and EMAIL_PASSWORD and pdf_bytes)
    if st.button(
        f"✉️ Email PDF to {DEFAULT_RECIPIENT.split('@')[0]}@…",
        type="primary",
        disabled=email_disabled,
        use_container_width=True,
        help=(
            f"Send the PDF report to {DEFAULT_RECIPIENT}"
            if not email_disabled else
            "Requires EMAIL_ADDRESS and EMAIL_PASSWORD in Streamlit secrets."
        ),
    ):
        with st.spinner("Sending..."):
            ok, msg_txt = send_report_email(
                recipient=DEFAULT_RECIPIENT,
                ticker=active_ticker,
                pdf_bytes=pdf_bytes,
                headline_eps=headline_eps,
                discount_pct=discount_pct,
                years=int(years),
                current_price=current_price,
            )
        if ok:
            st.success(f"✅ {msg_txt}")
        else:
            st.error(msg_txt)

st.caption(
    f"Formula: **PV = (EPS_{years} × Multiple) / (1 + {discount_pct:g}%)^{years}**  |  "
    f"Discount factor = **{discount_factor:.6f}**"
)

"""
Evolution Fund + Disruption Index technical screen runner.

Reads Evolution Fund positions and the local Disruption Index, runs four
technical screens (TLT, VCP Compression, Buy Trigger Daily, Oversold) against
both ticker lists, and merges results with the most recent TECG fundamental
composite rankings. Outputs a multi-sheet Excel report.

Usage:
  python screen_evolution_fund.py
  python screen_evolution_fund.py --workers 20
  python screen_evolution_fund.py --no-disruption    # holdings only

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
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

# --- Paths ---
ROOT = Path(__file__).parent
EVOLUTION_FUND = Path(
    r"C:\Users\daqui\OneDrive\Documents\Kite Evolution\Evolution Fund DEC 2024.xlsx"
)
EVOLUTION_SHEET = "Fund 2023"
DISRUPTION_CSV = ROOT / "disruption_index.csv"
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

    print(f"[{label}] Running 4 screens on {len(tickers)} tickers with {workers} workers...")
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
        flags.append(", ".join(f))
    df["Flags"] = flags
    return df


EMAIL_RECIPIENT_DEFAULT = "daquinn@targetedequityconsulting.com"


def send_report_email(xlsx_path: Path, summary_text: str, recipient: str) -> bool:
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

    with open(xlsx_path, "rb") as f:
        attach = MIMEBase("application", "octet-stream")
        attach.set_payload(f.read())
    encoders.encode_base64(attach)
    attach.add_header("Content-Disposition", f"attachment; filename={xlsx_path.name}")
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
                        help="Skip the Disruption Index scan (Evolution Fund only).")
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

    # Top signals: anything with at least one notable flag, plus high decile + good tech
    def top_signals(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return df[df["Flags"].str.len() > 0].sort_values("Flags").reset_index(drop=True)

    top_holdings = top_signals(holdings_df)
    top_disruption = top_signals(disruption_df) if not disruption_df.empty else pd.DataFrame()

    # Write Excel
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = REPORTS_DIR / f"evolution_screen_{stamp}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        holdings_df.to_excel(xw, sheet_name="Evolution Holdings", index=False)
        top_holdings.to_excel(xw, sheet_name="Evolution Top Signals", index=False)
        if not disruption_df.empty:
            disruption_df.to_excel(xw, sheet_name="Disruption Index", index=False)
            top_disruption.to_excel(xw, sheet_name="Disruption Top Signals", index=False)
        # Metadata
        meta = pd.DataFrame({
            "Field": ["Run timestamp", "Evolution Fund file", "Disruption Index file",
                      "Fundamental composite file", "Holdings count", "Disruption count"],
            "Value": [stamp, str(EVOLUTION_FUND), str(DISRUPTION_CSV),
                      str(composite_path), len(holdings), len(disruption)],
        })
        meta.to_excel(xw, sheet_name="Run Info", index=False)

    print(f"\nReport written: {out_path}")

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
        ]
        if not top_disruption.empty:
            lines.append("")
            lines.append("  Top-signal Disruption names: " + ", ".join(top_disruption["Symbol"].head(20).tolist()))
    summary_text = "\n".join(lines)
    print("\n" + summary_text)

    if args.email:
        try:
            sent = send_report_email(out_path, summary_text, args.recipient)
            print(f"\nEmail sent: {sent}")
        except Exception as e:
            print(f"\nEmail failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
PINK (Simplify Health Care ETF) daily holdings tracker.

Scrapes the current holdings table from Simplify's product page, snapshots it
to pink_holdings/YYYY-MM-DD.csv keyed by the page's "as of" date, diffs the
latest snapshot against the previous one, and emails a change report.

Usage:
    python pink_holdings_tracker.py            # scrape + save + print report
    python pink_holdings_tracker.py --email    # also email the report
"""

from __future__ import annotations

import argparse
import os
import re
import smtplib
import sys
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

URL = "https://www.simplify.us/etfs/pink-simplify-health-care-etf"
SNAPSHOT_DIR = Path(__file__).parent / "pink_holdings"
EMAIL_RECIPIENT_DEFAULT = "daquinn@targetedequityconsulting.com"


def scrape_holdings() -> tuple[pd.DataFrame, str]:
    """Return (holdings df, as_of_date string like 'MM/DD/YYYY')."""
    r = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    holdings_table = None
    for t in soup.find_all("table"):
        header = t.find("tr")
        if not header:
            continue
        cols = [c.get_text(strip=True).lower() for c in header.find_all(["th", "td"])]
        if cols[:4] == ["ticker", "name", "quantity", "weight"]:
            holdings_table = t
            break
    if holdings_table is None:
        raise RuntimeError("Holdings table not found on page")

    rows = []
    for tr in holdings_table.find_all("tr")[1:]:
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) < 4:
            continue
        ticker, name, qty_str, wt_str = cells[0], cells[1], cells[2], cells[3]
        try:
            qty = int(qty_str.replace(",", ""))
        except ValueError:
            continue
        try:
            weight = float(wt_str.replace("%", "").strip())
        except ValueError:
            weight = 0.0
        rows.append({"Ticker": ticker or "-", "Name": name, "Quantity": qty, "Weight": weight})

    df = pd.DataFrame(rows)

    as_of = ""
    m = re.search(r"[Hh]oldings.{0,200}?[Aa]s of[\s:]+(\d{1,2}/\d{1,2}/\d{2,4})", r.text, re.S)
    if m:
        as_of = m.group(1)
    else:
        m = re.search(r"[Aa]s of[\s:]+(\d{1,2}/\d{1,2}/\d{2,4})", soup.get_text())
        as_of = m.group(1) if m else datetime.now().strftime("%m/%d/%Y")

    parts = as_of.split("/")
    if len(parts[-1]) == 2:
        parts[-1] = "20" + parts[-1]
    as_of = "/".join(parts)

    return df, as_of


def snapshot_path(as_of: str) -> Path:
    dt = datetime.strptime(as_of, "%m/%d/%Y")
    return SNAPSHOT_DIR / f"{dt.strftime('%Y-%m-%d')}.csv"


def save_snapshot(df: pd.DataFrame, as_of: str) -> tuple[Path, bool]:
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    path = snapshot_path(as_of)
    newly_saved = not path.exists()
    if newly_saved:
        df.to_csv(path, index=False)
    return path, newly_saved


def load_prior_snapshot(current_path: Path) -> tuple[pd.DataFrame | None, str | None]:
    files = sorted(SNAPSHOT_DIR.glob("*.csv"))
    priors = [f for f in files if f.name < current_path.name]
    if not priors:
        return None, None
    prior_path = priors[-1]
    dt = datetime.strptime(prior_path.stem, "%Y-%m-%d")
    return pd.read_csv(prior_path), dt.strftime("%m/%d/%Y")


def build_diff(today: pd.DataFrame, prior: pd.DataFrame | None) -> pd.DataFrame:
    if prior is None:
        merged = today.copy()
        merged["T-1 shrs"] = pd.NA
        merged["chng Shrs"] = pd.NA
        merged["% chng in share"] = pd.NA
        return merged

    prior_slim = prior[["Ticker", "Quantity"]].rename(columns={"Quantity": "T-1 shrs"})
    merged = today.merge(prior_slim, on="Ticker", how="outer", indicator=True)

    merged["Quantity"] = merged["Quantity"].fillna(0).astype("Int64")
    merged["T-1 shrs"] = merged["T-1 shrs"].fillna(0).astype("Int64")
    merged["chng Shrs"] = (merged["Quantity"].astype(float) - merged["T-1 shrs"].astype(float)).astype("Int64")

    def pct(row):
        prev = row["T-1 shrs"]
        if pd.isna(prev) or prev == 0:
            return pd.NA if row["Quantity"] == 0 else float("inf")
        return (row["Quantity"] - prev) / prev * 100.0

    merged["% chng in share"] = merged.apply(pct, axis=1)
    return merged


def format_report(today_as_of: str, prior_as_of: str | None, diff: pd.DataFrame) -> str:
    lines = []
    lines.append(f"--- PINK Holdings - As of {today_as_of} ---")
    if prior_as_of:
        lines.append(f"Prior snapshot: {prior_as_of}")
    else:
        lines.append("Prior snapshot: (none - first run)")
    lines.append(f"Source: {URL}")
    lines.append("")

    if prior_as_of:
        present_today = diff["_merge"].isin(["both", "left_only"])
        new_positions = diff[diff["_merge"] == "left_only"]
        removed_positions = diff[diff["_merge"] == "right_only"]

        active = diff[present_today].copy()
        active["abs_pct"] = active["% chng in share"].abs().fillna(0)
        movers = active[active["abs_pct"] >= 3.0].sort_values("abs_pct", ascending=False)

        lines.append(f"--- Material Share Moves (|% chng| >= 3%) - {len(movers)} names ---")
        if len(movers):
            lines.append(_fmt_table(movers))
        else:
            lines.append("(none)")
        lines.append("")

        lines.append(f"--- New Positions - {len(new_positions)} ---")
        if len(new_positions):
            lines.append(_fmt_table(new_positions))
        else:
            lines.append("(none)")
        lines.append("")

        lines.append(f"--- Removed Positions - {len(removed_positions)} ---")
        if len(removed_positions):
            lines.append(_fmt_table(removed_positions, removed=True))
        else:
            lines.append("(none)")
        lines.append("")

    lines.append(f"--- Full Holdings ({len(diff[diff['_merge'] != 'right_only']) if '_merge' in diff.columns else len(diff)} positions) ---")
    full = diff[diff["_merge"] != "right_only"] if "_merge" in diff.columns else diff
    full_sorted = full.sort_values("Weight", ascending=False)
    lines.append(_fmt_table(full_sorted))

    return "\n".join(lines)


def _fmt_table(df: pd.DataFrame, removed: bool = False) -> str:
    header = f"{'%chng shrs':>10}  {'chng Shrs':>12}  {'Ticker':<6}  {'Name':<38}  {'Quantity':>12}  {'Weight':>7}  {'T-1 shrs':>12}"
    out = [header, "-" * len(header)]
    for _, r in df.iterrows():
        pct = r.get("% chng in share")
        if pd.isna(pct):
            pct_s = "NEW" if not removed else "OUT"
        elif pct == float("inf"):
            pct_s = "NEW"
        else:
            pct_s = f"{pct:+.2f}%"

        chng = r.get("chng Shrs")
        chng_s = "" if pd.isna(chng) else f"{int(chng):+,}"

        qty = r["Quantity"] if not removed else 0
        qty_s = f"{int(qty):,}" if not pd.isna(qty) else ""

        wt = r.get("Weight", 0)
        wt_s = f"{float(wt):.2f}%" if not pd.isna(wt) else ""

        t1 = r.get("T-1 shrs")
        t1_s = "" if pd.isna(t1) else f"{int(t1):,}"

        name = str(r.get("Name", ""))[:38]
        out.append(f"{pct_s:>10}  {chng_s:>12}  {str(r['Ticker']):<6}  {name:<38}  {qty_s:>12}  {wt_s:>7}  {t1_s:>12}")
    return "\n".join(out)


def send_email(report: str, csv_path: Path, as_of: str, recipient: str) -> bool:
    user = os.getenv("EMAIL_ADDRESS")
    pwd = os.getenv("EMAIL_PASSWORD")
    if not user or not pwd:
        print("EMAIL_ADDRESS / EMAIL_PASSWORD not set - skipping email.")
        return False

    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = recipient
    msg["Subject"] = f"PINK Holdings - {as_of}"
    msg.attach(MIMEText(report, "plain"))

    if csv_path.exists():
        with open(csv_path, "rb") as f:
            attach = MIMEBase("application", "octet-stream")
            attach.set_payload(f.read())
        encoders.encode_base64(attach)
        attach.add_header("Content-Disposition", f"attachment; filename={csv_path.name}")
        msg.attach(attach)

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)
    print(f"Emailed report to {recipient}")
    return True


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--email", action="store_true", help="Email the report after building it.")
    p.add_argument("--recipient", default=EMAIL_RECIPIENT_DEFAULT)
    args = p.parse_args()

    print(f"Fetching {URL} ...")
    today, as_of = scrape_holdings()
    print(f"Parsed {len(today)} holdings as of {as_of}")

    csv_path, newly_saved = save_snapshot(today, as_of)
    print(f"Snapshot: {csv_path.name} ({'saved' if newly_saved else 'already existed'})")

    prior, prior_as_of = load_prior_snapshot(csv_path)
    if prior is None:
        print("No prior snapshot found - first run; no diff to report.")

    diff = build_diff(today, prior)
    report = format_report(as_of, prior_as_of, diff)
    print()
    print(report)

    if args.email:
        send_email(report, csv_path, as_of, args.recipient)

    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
PINK (Simplify Health Care ETF) daily holdings tracker.

Scrapes the current holdings table from Simplify's product page, snapshots it
to pink_holdings/YYYY-MM-DD.csv keyed by the RUN DATE (calendar day the script
ran, not the page's "As of" date), diffs today's scrape against the most
recent prior snapshot, and emails a change report.

Keying by run date guarantees a snapshot every day the tracker fires, even
when Simplify hasn't rolled the "As of" date forward. When holdings are
byte-identical to the prior file, the report says so instead of pretending
to be a first run.

The saved CSV includes the change columns (T-1 shrs, chng Shrs, % chng in
share, Status) so each snapshot is self-describing. Rows marked
Status=REMOVED are excluded when the file is later used as a prior snapshot.

Usage:
    python pink_holdings_tracker.py            # scrape + save + print report
    python pink_holdings_tracker.py --email    # also email the report
"""

from __future__ import annotations

import argparse
import html
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

CSV_COLUMNS = [
    "Ticker", "Name", "Quantity", "Weight",
    "T-1 shrs", "chng Shrs", "% chng in share", "Status",
]


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


def snapshot_path(run_date: datetime) -> Path:
    return SNAPSHOT_DIR / f"{run_date.strftime('%Y-%m-%d')}.csv"


def save_snapshot(diff: pd.DataFrame, run_date: datetime) -> Path:
    """Write today's diff (positions + change columns + Status) to a run-date-keyed CSV."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    path = snapshot_path(run_date)

    out = diff.copy()
    if "_merge" in out.columns:
        out["Status"] = out["_merge"].map(
            {"both": "HELD", "left_only": "NEW", "right_only": "REMOVED"}
        )
    else:
        out["Status"] = "HELD"

    # Sort: held/new by weight desc, removed pinned at bottom.
    out["_sort_removed"] = (out["Status"] == "REMOVED").astype(int)
    out = out.sort_values(["_sort_removed", "Weight"], ascending=[True, False])

    if "% chng in share" in out.columns:
        out["% chng in share"] = pd.to_numeric(
            out["% chng in share"], errors="coerce"
        ).round(2)

    cols = [c for c in CSV_COLUMNS if c in out.columns]
    out[cols].to_csv(path, index=False)
    return path


def load_prior_snapshot(current_path: Path) -> tuple[pd.DataFrame | None, str | None]:
    """Return (prior df filtered to held/new positions, prior run-date 'YYYY-MM-DD') or (None, None)."""
    files = sorted(SNAPSHOT_DIR.glob("*.csv"))
    priors = [f for f in files if f.name < current_path.name]
    if not priors:
        return None, None
    prior_path = priors[-1]
    df = pd.read_csv(prior_path)
    if "Status" in df.columns:
        df = df[df["Status"] != "REMOVED"].copy()
    return df, prior_path.stem


def holdings_identical(today: pd.DataFrame, prior: pd.DataFrame) -> bool:
    """True iff the (Ticker, Quantity) pairs match exactly."""
    a = today[["Ticker", "Quantity"]].sort_values("Ticker").reset_index(drop=True)
    b = prior[["Ticker", "Quantity"]].sort_values("Ticker").reset_index(drop=True)
    return a.equals(b)


def build_diff(today: pd.DataFrame, prior: pd.DataFrame | None) -> pd.DataFrame:
    if prior is None:
        merged = today.copy()
        merged["T-1 shrs"] = pd.NA
        merged["chng Shrs"] = pd.NA
        merged["% chng in share"] = pd.NA
        return merged

    prior_slim = prior[["Ticker", "Name", "Quantity", "Weight"]].rename(
        columns={"Quantity": "T-1 shrs", "Name": "_prior_name", "Weight": "_prior_weight"}
    )
    merged = today.merge(prior_slim, on="Ticker", how="outer", indicator=True)

    # Fill Name/Weight from prior for removed positions (they're absent in today's scrape).
    merged["Name"] = merged["Name"].fillna(merged["_prior_name"])
    merged["Weight"] = merged["Weight"].fillna(merged["_prior_weight"])
    merged = merged.drop(columns=["_prior_name", "_prior_weight"])

    merged["Quantity"] = merged["Quantity"].fillna(0).astype("Int64")
    merged["T-1 shrs"] = merged["T-1 shrs"].fillna(0).astype("Int64")
    merged["chng Shrs"] = (
        merged["Quantity"].astype(float) - merged["T-1 shrs"].astype(float)
    ).astype("Int64")

    def pct(row):
        prev = row["T-1 shrs"]
        if pd.isna(prev) or prev == 0:
            return pd.NA if row["Quantity"] == 0 else float("inf")
        return (row["Quantity"] - prev) / prev * 100.0

    merged["% chng in share"] = merged.apply(pct, axis=1)
    return merged


def _row_display(r: pd.Series) -> dict:
    """Format one row's values for display (shared by text + html renderers)."""
    merge = r.get("_merge")
    if merge == "right_only":
        pct_s, status = "OUT", "removed"
    elif merge == "left_only":
        pct_s, status = "NEW", "new"
    else:
        pct = r.get("% chng in share")
        if pd.isna(pct):
            pct_s, status = "-", "neutral"
        elif pct == float("inf"):
            pct_s, status = "NEW", "new"
        else:
            pct_s = f"{pct:+.2f}%"
            status = "up" if pct > 0 else ("down" if pct < 0 else "neutral")

    chng = r.get("chng Shrs")
    chng_s = "" if pd.isna(chng) else f"{int(chng):+,}"

    qty = r.get("Quantity", 0)
    qty_s = f"{int(qty):,}" if not pd.isna(qty) else ""

    wt = r.get("Weight", 0)
    wt_s = f"{float(wt):.2f}%" if not pd.isna(wt) else ""

    t1 = r.get("T-1 shrs")
    t1_s = "" if pd.isna(t1) else f"{int(t1):,}"

    return {
        "pct": pct_s,
        "chng": chng_s,
        "ticker": str(r["Ticker"]),
        "name": str(r.get("Name", "") or ""),
        "qty": qty_s,
        "wt": wt_s,
        "t1": t1_s,
        "status": status,
    }


# ---------- plain-text report ----------

def format_report(page_as_of, run_date_str, prior_run_date, diff, no_changes=False):
    lines = []
    lines.append("--- PINK Holdings ---")
    lines.append(f"Run date: {run_date_str}")
    lines.append(f"Page 'As of': {page_as_of}")
    lines.append(
        f"Prior snapshot (run): {prior_run_date}" if prior_run_date
        else "Prior snapshot: (none - first run)"
    )
    lines.append(f"Source: {URL}")
    lines.append("")

    if no_changes:
        lines.append(f"--- No holdings changes vs prior snapshot ({prior_run_date}) ---")
        lines.append("(Simplify's page has not rolled forward since the last scrape.)")
        lines.append("")
    elif prior_run_date:
        present_today = diff["_merge"].isin(["both", "left_only"])
        new_positions = diff[diff["_merge"] == "left_only"]
        removed_positions = diff[diff["_merge"] == "right_only"]

        active = diff[present_today].copy()
        active["abs_pct"] = active["% chng in share"].abs().fillna(0)
        movers = active[active["abs_pct"] >= 3.0].sort_values("abs_pct", ascending=False)

        lines.append(f"--- Material Share Moves (|% chng| >= 3%) - {len(movers)} names ---")
        lines.append(_fmt_text_table(movers) if len(movers) else "(none)")
        lines.append("")

        lines.append(f"--- New Positions - {len(new_positions)} ---")
        lines.append(_fmt_text_table(new_positions) if len(new_positions) else "(none)")
        lines.append("")

        lines.append(f"--- Removed Positions - {len(removed_positions)} ---")
        lines.append(_fmt_text_table(removed_positions) if len(removed_positions) else "(none)")
        lines.append("")

    full = diff[diff["_merge"] != "right_only"] if "_merge" in diff.columns else diff
    full_sorted = full.sort_values("Weight", ascending=False)
    lines.append(f"--- Full Holdings ({len(full_sorted)} positions) ---")
    lines.append(_fmt_text_table(full_sorted))
    return "\n".join(lines)


def _fmt_text_table(df: pd.DataFrame) -> str:
    header = f"{'%chng shrs':>10}  {'chng Shrs':>12}  {'Ticker':<6}  {'Name':<38}  {'Quantity':>12}  {'Weight':>7}  {'T-1 shrs':>12}"
    out = [header, "-" * len(header)]
    for _, r in df.iterrows():
        v = _row_display(r)
        name = v["name"][:38]
        out.append(
            f"{v['pct']:>10}  {v['chng']:>12}  {v['ticker']:<6}  {name:<38}  {v['qty']:>12}  {v['wt']:>7}  {v['t1']:>12}"
        )
    return "\n".join(out)


# ---------- HTML report ----------

COLOR_UP = "#0a7d0a"
COLOR_DOWN = "#b30000"
COLOR_NEW = "#0a5a99"
COLOR_NEUTRAL = "#222"

# A row is an outlier if its % change deviates from the day's median held-position
# change by at least this many percentage points. Chosen so the fund's uniform
# daily NAV drift (typically 0.2-0.3pp) never colors anything, but a real
# position-level trim or add stands out.
OUTLIER_THRESHOLD_PP = 1.0

TABLE_STYLE = "border-collapse:collapse;font-size:12px;margin:4px 0 12px 0"
TH_BASE = "padding:6px 10px;border-bottom:2px solid #333;background:#f4f4f4;font-weight:bold;font-family:Arial,sans-serif"
TD_BASE = "padding:5px 10px;border-bottom:1px solid #eee;font-family:Arial,sans-serif"
MONO = "font-family:Consolas,'Courier New',monospace"


def _held_median_pct(diff: pd.DataFrame) -> float | None:
    """Median % change across positions held in both today and prior. None if unavailable."""
    if "_merge" not in diff.columns:
        return None
    held = diff[diff["_merge"] == "both"]
    if len(held) == 0:
        return None
    vals = pd.to_numeric(held["% chng in share"], errors="coerce").replace(
        [float("inf"), float("-inf")], pd.NA
    ).dropna()
    return float(vals.median()) if len(vals) else None


def _row_color(r: pd.Series, median_pct: float | None) -> tuple[str, str]:
    """Return (color, font-weight) for the % chg cell of one row."""
    merge = r.get("_merge")
    if merge == "left_only":
        return COLOR_NEW, "bold"
    if merge == "right_only":
        return COLOR_DOWN, "bold"

    if median_pct is None:
        return COLOR_NEUTRAL, "normal"
    pct = r.get("% chng in share")
    if pd.isna(pct) or pct == float("inf") or pct == float("-inf"):
        return COLOR_NEUTRAL, "normal"

    deviation = float(pct) - median_pct
    if abs(deviation) < OUTLIER_THRESHOLD_PP:
        return COLOR_NEUTRAL, "normal"
    return (COLOR_UP if deviation > 0 else COLOR_DOWN), "bold"


def _html_table(df: pd.DataFrame, median_pct: float | None = None) -> str:
    if len(df) == 0:
        return '<div style="color:#666;font-style:italic;margin:4px 0 12px 4px">(none)</div>'

    headers = [
        ("% Chg", "right"), ("Chg Shrs", "right"), ("Ticker", "left"),
        ("Name", "left"), ("Quantity", "right"), ("Weight", "right"),
        ("T-1 Shrs", "right"),
    ]

    out = [f'<table style="{TABLE_STYLE}"><thead><tr>']
    for label, align in headers:
        out.append(f'<th style="{TH_BASE};text-align:{align}">{html.escape(label)}</th>')
    out.append("</tr></thead><tbody>")

    for i, (_, r) in enumerate(df.iterrows()):
        v = _row_display(r)
        row_bg = "#fafafa" if i % 2 else "#ffffff"
        color, weight = _row_color(r, median_pct)

        pct_style = f"{TD_BASE};{MONO};text-align:right;color:{color};font-weight:{weight};background:{row_bg}"
        r_style = f"{TD_BASE};{MONO};text-align:right;background:{row_bg}"
        l_style = f"{TD_BASE};text-align:left;background:{row_bg}"
        ticker_style = f"{TD_BASE};text-align:left;font-weight:bold;background:{row_bg}"

        out.append("<tr>")
        out.append(f'<td style="{pct_style}">{html.escape(v["pct"])}</td>')
        out.append(f'<td style="{r_style}">{html.escape(v["chng"])}</td>')
        out.append(f'<td style="{ticker_style}">{html.escape(v["ticker"])}</td>')
        out.append(f'<td style="{l_style}">{html.escape(v["name"])}</td>')
        out.append(f'<td style="{r_style}">{html.escape(v["qty"])}</td>')
        out.append(f'<td style="{r_style}">{html.escape(v["wt"])}</td>')
        out.append(f'<td style="{r_style}">{html.escape(v["t1"])}</td>')
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def format_report_html(page_as_of, run_date_str, prior_run_date, diff, no_changes=False) -> str:
    parts = ['<div style="font-family:Arial,sans-serif;font-size:13px;color:#222;max-width:1100px">']
    parts.append('<h2 style="margin:0 0 8px 0;color:#111">PINK Holdings</h2>')

    parts.append('<div style="color:#555;font-size:12px;margin-bottom:10px">')
    parts.append(f'Run date: <b>{html.escape(run_date_str)}</b> &nbsp;|&nbsp; ')
    parts.append(f'Page &quot;As of&quot;: {html.escape(page_as_of)} &nbsp;|&nbsp; ')
    parts.append(
        f'Prior snapshot: {html.escape(prior_run_date)}' if prior_run_date
        else 'Prior snapshot: <i>(none &mdash; first run)</i>'
    )
    parts.append(f'<br>Source: <a href="{html.escape(URL)}">{html.escape(URL)}</a>')
    parts.append('</div>')

    median_pct = _held_median_pct(diff)
    if median_pct is not None:
        parts.append(
            f'<div style="color:#666;font-size:11px;margin:0 0 12px 0">'
            f'Median held-position change today: <b>{median_pct:+.2f}%</b>. '
            f'Rows highlighted when deviating &ge; {OUTLIER_THRESHOLD_PP:.1f}pp from median '
            f'(<span style="color:{COLOR_UP};font-weight:bold">green</span> = above, '
            f'<span style="color:{COLOR_DOWN};font-weight:bold">red</span> = below).'
            f'</div>'
        )

    if no_changes:
        parts.append(
            f'<p style="padding:10px;background:#fff8dc;border-left:4px solid #d4a017;margin:12px 0">'
            f'<b>No holdings changes vs prior snapshot ({html.escape(prior_run_date)}).</b><br>'
            f'<span style="color:#666;font-size:12px">Simplify&#39;s page has not rolled forward since the last scrape.</span>'
            f'</p>'
        )
    elif prior_run_date:
        present_today = diff["_merge"].isin(["both", "left_only"])
        new_positions = diff[diff["_merge"] == "left_only"]
        removed_positions = diff[diff["_merge"] == "right_only"]

        active = diff[present_today].copy()
        active["abs_pct"] = active["% chng in share"].abs().fillna(0)
        movers = active[active["abs_pct"] >= 3.0].sort_values("abs_pct", ascending=False)

        parts.append(f'<h3 style="margin:16px 0 4px 0;color:#111">Material Share Moves (|% chg| &ge; 3%) &mdash; {len(movers)}</h3>')
        parts.append(_html_table(movers, median_pct=median_pct))

        parts.append(f'<h3 style="margin:16px 0 4px 0;color:#111">New Positions &mdash; {len(new_positions)}</h3>')
        parts.append(_html_table(new_positions, median_pct=median_pct))

        parts.append(f'<h3 style="margin:16px 0 4px 0;color:#111">Removed Positions &mdash; {len(removed_positions)}</h3>')
        parts.append(_html_table(removed_positions, median_pct=median_pct))

    full = diff[diff["_merge"] != "right_only"] if "_merge" in diff.columns else diff
    full_sorted = full.sort_values("Weight", ascending=False)
    parts.append(f'<h3 style="margin:20px 0 4px 0;color:#111">Full Holdings ({len(full_sorted)})</h3>')
    parts.append(_html_table(full_sorted, median_pct=median_pct))

    parts.append('</div>')
    return "".join(parts)


# ---------- email ----------

def send_email(text_report: str, html_report: str, csv_path: Path, as_of: str, recipient: str) -> bool:
    user = os.getenv("EMAIL_ADDRESS")
    pwd = os.getenv("EMAIL_PASSWORD")
    if not user or not pwd:
        print("EMAIL_ADDRESS / EMAIL_PASSWORD not set - skipping email.")
        return False

    msg = MIMEMultipart("mixed")
    msg["From"] = user
    msg["To"] = recipient
    msg["Subject"] = f"PINK Holdings - {as_of}"

    body = MIMEMultipart("alternative")
    body.attach(MIMEText(text_report, "plain"))
    body.attach(MIMEText(html_report, "html"))
    msg.attach(body)

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

    run_date = datetime.now()
    run_date_str = run_date.strftime("%Y-%m-%d")

    print(f"Fetching {URL} ...")
    today, page_as_of = scrape_holdings()
    print(f"Parsed {len(today)} holdings; page 'As of' {page_as_of}")

    prior, prior_run_date = load_prior_snapshot(snapshot_path(run_date))
    no_changes = False
    if prior is None:
        print("No prior snapshot found - first run; no diff to report.")
    elif holdings_identical(today, prior):
        no_changes = True
        print(f"No holdings changes vs prior snapshot ({prior_run_date}).")

    diff = build_diff(today, prior)
    csv_path = save_snapshot(diff, run_date)
    print(f"Snapshot saved: {csv_path.name}")

    text_report = format_report(page_as_of, run_date_str, prior_run_date, diff, no_changes=no_changes)
    html_report = format_report_html(page_as_of, run_date_str, prior_run_date, diff, no_changes=no_changes)
    print()
    print(text_report)

    if args.email:
        send_email(text_report, html_report, csv_path, page_as_of, args.recipient)

    return 0


if __name__ == "__main__":
    sys.exit(main())

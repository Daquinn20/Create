"""Reconstruct the AI infrastructure PDF from the track-record DB + log file.

The original test run printed everything but didn't call generate_industry_pdf.
All rationale + trend text is in SQLite; FMP profiles are free to refetch.
"""

import os
import re
import sqlite3
import sys
from datetime import datetime

from industry_report_generator import (
    CompanyTrendPosition,
    WinnersLosersAnalysis,
    generate_industry_analysis,
    generate_industry_pdf,
    get_company_profile,
)

THEME = "AI Infrastructure / Neocloud GPU Hosting"
DB_PATH = "track_record.sqlite"
LOG_PATH = "test_run_ai_infra.log"
COMPANY_NAMES = {
    "NBIS": "Nebius Group",
    "CRWV": "CoreWeave",
    "IREN": "Iris Energy",
    "APLD": "Applied Digital",
    "CIFR": "Cipher Mining",
}

# Pull the synthesis "summary:" block from the log
summary = ""
if os.path.exists(LOG_PATH):
    with open(LOG_PATH, encoding="utf-8", errors="replace") as f:
        text = f.read()
    m = re.search(r"Summary:\s*\n\s*(.+?)\n\s*\n", text, re.DOTALL)
    if m:
        summary = m.group(1).strip()
print(f"Recovered summary ({len(summary)} chars): {summary[:100]!r}...")

# Pull picks from the most recent report only (synthesis agent)
con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
latest_report_id = con.execute(
    "SELECT report_id FROM reports ORDER BY run_date DESC, created_at DESC LIMIT 1"
).fetchone()["report_id"]
print(f"Using latest report: {latest_report_id[:8]}")
picks = con.execute(
    "SELECT ticker, direction, trend, thesis, conviction "
    "FROM picks WHERE agent = 'synthesis' AND report_id = ? "
    "ORDER BY direction, ticker",
    (latest_report_id,),
).fetchall()
con.close()

# Map conviction back to High/Medium/Low for the PDF
conv_label = {5: "High", 4: "High", 3: "Medium", 2: "Low", 1: "Low"}

def to_position(row, position_kind: str) -> CompanyTrendPosition:
    return CompanyTrendPosition(
        symbol=row["ticker"],
        company_name=COMPANY_NAMES.get(row["ticker"], row["ticker"]),
        position=position_kind,
        trend=row["trend"] or "",
        rationale=row["thesis"] or "",
        confidence=conv_label.get(row["conviction"], "Medium"),
    )

winners = [to_position(r, "winner") for r in picks if r["direction"] == "long"]
losers  = [to_position(r, "loser")  for r in picks if r["direction"] == "short"]
print(f"Picks recovered: {len(winners)} winners, {len(losers)} losers")

wl = WinnersLosersAnalysis(winners=winners, losers=losers, summary=summary)

# Fresh FMP profiles for cover page / valuation table
print("Fetching FMP profiles...")
companies = []
for sym in ["NBIS", "CRWV", "IREN", "APLD", "CIFR"]:
    p = get_company_profile(sym) or {}
    companies.append({
        "symbol": p.get("symbol", sym),
        "companyName": p.get("companyName", COMPANY_NAMES.get(sym, sym)),
        "marketCap": p.get("mktCap", 0) or 0,
        "price": p.get("price", 0) or 0,
        "beta": p.get("beta"),
        "sector": p.get("sector", "Technology"),
        "industry": p.get("industry", "N/A"),
        "volume": p.get("volAvg", 0) or 0,
        "description": p.get("description", ""),
        "exchange": p.get("exchangeShortName", ""),
        "website": p.get("website", ""),
    })

sector_data = {"sector": "Technology", "industry": THEME}

# The PDF body expects overview / trends / risks / outlook — generate them now
# (each is a ~1000-token Anthropic call, total ~$0.10).
print("Generating industry analysis sections (overview / trends / risks / outlook)...")
ai_analysis = generate_industry_analysis(THEME, companies, sector_data)
if summary:
    # Prefer the synthesis summary on the executive overview if the LLM gave us one
    ai_analysis["overview"] = summary + "\n\n" + ai_analysis.get("overview", "")
print(f"Sections populated: {[k for k, v in ai_analysis.items() if v]}")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = f"output/AI_Infrastructure_Report_{timestamp}.pdf"

print(f"Generating PDF to {out_path}...")
result_path = generate_industry_pdf(
    industry=THEME,
    companies=companies,
    sector_data=sector_data,
    ai_analysis=ai_analysis,
    output_path=out_path,
    winners_losers=wl,
)
print(f"Done: {result_path}")
sys.exit(0)

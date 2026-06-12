"""Smoke-test PDF generation for daily_brief_dashboard.create_pdf_report.

Feeds the function adversarial AI-summary strings (the kinds that caused
'paraparser: syntax error: parse ended with 1 unclosed tags para'),
runs the builder, and validates the resulting PDF.
"""
import os
import sys

# Silence streamlit set_page_config warning at import time
os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")

from daily_brief_dashboard import create_pdf_report, _sanitize_for_paragraph


def _stub_market(symbols):
    return {name: {"price": 100.0 + i, "change_pct": -0.5 + i * 0.1}
            for i, name in enumerate(symbols)}


# Hostile inputs — every one of these would have crashed the old code.
HOSTILE_AI_SUMMARY = "\n".join([
    "• **Fed signals 50bp cut** — markets rally",
    "• Nvidia P/E < 30 vs peers > 50 (cheap on relative basis)",  # raw <, >
    "• <b>Unclosed bold here triggers the exact error",            # unclosed <b>
    "• **bold with stray<b> closer** and a lonely </b>",          # broken pair + lone closer
    "• AT&T mentioned (raw ampersand)",                           # raw &
    "• *italic* and **bold** mixed with a < lonely angle",
])

HOSTILE_PORTFOLIO_SUMMARY = "\n".join([
    "NVDA: guidance > consensus by 8%",
    "TSLA: <break/> robotaxi delay",
    "**PLTR** earnings beat — margins < expected though",
])

NEWSLETTER_SUMMARIES = [
    {
        "sender": "Barron's & Co <test>",
        "subject": "Markets rally as P/E < historical avg",
        "summary": "**Bold lead** with <stray tag and **another unclosed",
    },
    {
        "sender": "I/O Fund",
        "subject": "AI capex > $200B in 2026",
        "summary": "Normal text, nothing tricky.",
    },
]

ECONOMIC_CALENDAR = [
    {"date": "2026-06-12", "event": "CPI YoY", "actual": "3.1%", "estimate": "3.2%", "previous": "3.4%"},
]

PREMARKET_MOVERS = [
    {"symbol": "NVDA", "changesPercentage": 2.3},
    {"symbol": "TSLA", "changesPercentage": -1.8},
]


def main():
    # First: unit-test the sanitizer directly on each hostile fragment
    print("== _sanitize_for_paragraph round-trip ==")
    for raw in HOSTILE_AI_SUMMARY.split("\n") + HOSTILE_PORTFOLIO_SUMMARY.split("\n"):
        out = _sanitize_for_paragraph(raw)
        # Quick balance check
        opens_b = out.count("<b>")
        closes_b = out.count("</b>")
        opens_i = out.count("<i>")
        closes_i = out.count("</i>")
        assert opens_b == closes_b, f"<b> unbalanced in: {raw!r} -> {out!r}"
        assert opens_i == closes_i, f"<i> unbalanced in: {raw!r} -> {out!r}"
        # All literal < that weren't intentional tags must be escaped
        print(f"  {raw[:60]!r:<65} -> {out[:70]!r}")
    print("  all balanced OK\n")

    # Now: full PDF build
    print("== create_pdf_report end-to-end ==")
    buf = create_pdf_report(
        index_data=_stub_market(["S&P 500", "NASDAQ"]),
        treasury_data=_stub_market(["US 10Y", "US 2Y"]),
        fx_data=_stub_market(["EUR/USD", "USD/JPY"]),
        commodity_data=_stub_market(["WTI Crude", "Gold"]),
        crypto_data=_stub_market(["BTC", "ETH"]),
        sector_data=[{"sector": "Tech", "changesPercentage": "1.2%"},
                     {"sector": "Energy", "changesPercentage": -0.4}],
        news=[],
        economic_calendar=ECONOMIC_CALENDAR,
        ai_summary=HOSTILE_AI_SUMMARY,
        premarket_movers=PREMARKET_MOVERS,
        portfolio_summary=HOSTILE_PORTFOLIO_SUMMARY,
        newsletter_summaries=NEWSLETTER_SUMMARIES,
    )

    data = buf.getvalue()
    assert data.startswith(b"%PDF-"), f"Not a PDF — header is {data[:8]!r}"
    assert data.rstrip().endswith(b"%%EOF"), "PDF missing %%EOF trailer"
    print(f"  PDF built OK: {len(data):,} bytes, header={data[:8]!r}")

    # Save it so the user can eyeball it
    out_path = os.path.join(os.path.dirname(__file__), "test_daily_brief.pdf")
    with open(out_path, "wb") as f:
        f.write(data)
    print(f"  wrote {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

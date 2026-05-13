"""End-to-end test of the industry-report pipeline on AI infrastructure names.

Universe: NBIS (Nebius), CRWV (CoreWeave), IREN (Iris Energy),
          APLD (Applied Digital), CIFR (Cipher Mining).
Focus:    deals with major AI players, current capacity, capacity upside.

Phase 1: web_research_industry()  ->  prints structured findings
Phase 2: run_all_agents_and_synthesize()  ->  prints final Winners/Losers
"""

import logging
import sys

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

from industry_report_generator import web_research_industry
from industry_report_dashboard import run_all_agents_and_synthesize

TICKERS = ["NBIS", "CRWV", "IREN", "APLD", "CIFR"]
TICKER_NAMES = {
    "NBIS": "Nebius Group",
    "CRWV": "CoreWeave",
    "IREN": "Iris Energy",
    "APLD": "Applied Digital",
    "CIFR": "Cipher Mining",
}

THEME = "AI Infrastructure / Neocloud GPU Hosting"
RESEARCH_PROMPT_THEME = (
    "AI infrastructure / neocloud GPU hosting providers (NBIS, CRWV, IREN, APLD, CIFR). "
    "Focus on (1) customer contracts with major AI players "
    "(Microsoft, Meta, OpenAI, Google, Anthropic, xAI, Oracle), "
    "(2) current data-center power capacity in MW and GPU fleet size, "
    "(3) announced or feasible additional capacity build-out (upside potential)."
)


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def main() -> int:
    section("PHASE 1 — Web research")
    findings = web_research_industry(
        theme=RESEARCH_PROMPT_THEME,
        tickers=TICKERS,
        lookback_days=120,
        max_searches=15,
    )
    print(findings.as_context_block())

    report_content = findings.as_context_block()
    if len(report_content) < 200:
        print("\nWeb research returned almost nothing — aborting before agent run.")
        print(f"Raw text (first 1000 chars): {findings.raw_text[:1000]!r}")
        return 1

    section(f"PHASE 2 — 11-agent synthesis ({len(report_content)} chars of research)")
    universe = pd.DataFrame(
        {"Symbol": TICKERS, "Name": [TICKER_NAMES[t] for t in TICKERS]}
    )

    def progress(msg: str, pct: float) -> None:
        print(f"  [{pct*100:4.0f}%] {msg}")

    user_directions = (
        "For every company you assess, address these three points explicitly:\n"
        "1. Named customer deals with major AI players (Microsoft, Meta, OpenAI, "
        "Google, Anthropic, xAI, Oracle) — cite the company and contract size if known.\n"
        "2. Current operating capacity in MW and approximate GPU count.\n"
        "3. Additional capacity in development or feasible to build (upside in MW)."
    )

    result = run_all_agents_and_synthesize(
        report_content=report_content,
        universe_df=universe,
        ai_provider="anthropic",
        progress_callback=progress,
        user_directions=user_directions,
        theme=THEME,
        sector="Technology",
    )

    section("FINAL SYNTHESIS — Winners / Losers")
    print(f"Summary:\n  {result.summary}\n")

    print(f"WINNERS ({len(result.winners)}):")
    for w in result.winners:
        print(f"  • {w.symbol} — {w.company_name}  [conf: {w.confidence}]")
        print(f"      Trend:     {w.trend}")
        print(f"      Rationale: {w.rationale}\n")

    print(f"LOSERS ({len(result.losers)}):")
    for l in result.losers:
        print(f"  • {l.symbol} — {l.company_name}  [conf: {l.confidence}]")
        print(f"      Trend:     {l.trend}")
        print(f"      Rationale: {l.rationale}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())

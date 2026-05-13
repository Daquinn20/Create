"""Diagnose why agents aren't emitting the ===PICKS_JSON=== marker.

Runs one agent (industry_analyst) at three max_tokens settings and reports:
  - stop_reason (max_tokens? end_turn?)
  - whether marker present
  - usage (input/output tokens)
  - tail of the response

If stop_reason == 'max_tokens' on the production setting (2500), token
budget is the cause. If 'end_turn' but no marker, the prompt is the cause.
"""

import os
from industry_report_generator import web_research_industry  # also loads .env
from industry_report_dashboard import ANALYSIS_AGENTS, PICKS_MARKER
import anthropic

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# Small, fixed research blob so we isolate the prompt issue from input variability
REPORT_CONTENT = """INDUSTRY OVERVIEW: AI neocloud GPU hosting is supply-constrained.
Five public players: NBIS, CRWV, IREN, APLD, CIFR.
- NBIS: Microsoft $19.4B + Meta $27B contracts.
- CRWV: $99.4B backlog, OpenAI/Meta/Anthropic anchor customers.
- IREN: Microsoft $9.7B + NVIDIA equity stake.
- APLD: $23B+ contracted across 3 hyperscalers.
- CIFR: Fluidstack/Google $3B contract."""

UNIVERSE = "- NBIS (Nebius)\n- CRWV (CoreWeave)\n- IREN (Iris Energy)\n- APLD (Applied Digital)\n- CIFR (Cipher Mining)"

AGENT_KEY = "industry_analyst"
agent = ANALYSIS_AGENTS[AGENT_KEY]

prompt = f"""{agent['prompt']}

RESEARCH REPORT:
{REPORT_CONTENT}

STOCK UNIVERSE TO CONSIDER:
{UNIVERSE}

Provide your analysis identifying specific stocks from the universe as potential winners or losers.

After your prose analysis, on a new line write exactly the marker `{PICKS_MARKER}` and then a JSON object with your concrete stock calls in this shape:
{{
  "picks": [
    {{"symbol": "TICKER", "direction": "long",    "rationale": "1-2 sentence reason", "confidence": "High",   "trend": "trend driving this call"}}
  ]
}}

Rules: only use tickers from the STOCK UNIVERSE; direction is "long" for winners, "short" for losers, "neutral" for mixed; confidence is High/Medium/Low; return the JSON with no code fences."""

for max_tok in [800, 2500, 4000]:
    print(f"\n{'=' * 70}\nmax_tokens={max_tok}\n{'=' * 70}")
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tok,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.content[0].text
    has_marker = PICKS_MARKER in text
    print(f"stop_reason: {resp.stop_reason}")
    print(f"usage:       in={resp.usage.input_tokens}  out={resp.usage.output_tokens}")
    print(f"marker present: {has_marker}")
    print(f"response length: {len(text)} chars")
    print(f"--- last 400 chars ---")
    print(text[-400:])

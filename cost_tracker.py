"""Running token-usage + cost tracker with a hard cap.

Used by the robust-report pipeline to abort before expensive stages
when accumulated spend would exceed the user's cap. Default cap is
generous; the dashboard's robust-mode entry point passes cost_cap_usd=12.

Pricing assumes Claude Sonnet 4.6 standard tier ($3/M input, $15/M output)
and Anthropic's web_search tool at ~$0.01 per search invocation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

INPUT_RATE_USD_PER_TOKEN = 3.0 / 1_000_000
OUTPUT_RATE_USD_PER_TOKEN = 15.0 / 1_000_000
WEB_SEARCH_RATE_USD = 0.01


class BudgetExceeded(RuntimeError):
    """Raised when CostTracker.require_remaining detects we're out of budget."""


@dataclass
class CostEvent:
    label: str
    input_tokens: int = 0
    output_tokens: int = 0
    web_searches: int = 0
    usd: float = 0.0


@dataclass
class CostTracker:
    cap_usd: float = 12.0
    input_tokens: int = 0
    output_tokens: int = 0
    web_searches: int = 0
    events: List[CostEvent] = field(default_factory=list)

    def total_usd(self) -> float:
        return (
            self.input_tokens * INPUT_RATE_USD_PER_TOKEN
            + self.output_tokens * OUTPUT_RATE_USD_PER_TOKEN
            + self.web_searches * WEB_SEARCH_RATE_USD
        )

    def remaining_usd(self) -> float:
        return self.cap_usd - self.total_usd()

    def record_anthropic(self, label: str, response: Any) -> CostEvent:
        """Pull usage + server-side web_search count off an Anthropic response.

        Returns the recorded event so callers can inspect input/output counts.
        """
        in_tok = out_tok = searches = 0
        usage = getattr(response, "usage", None)
        if usage is not None:
            in_tok = int(getattr(usage, "input_tokens", 0) or 0)
            out_tok = int(getattr(usage, "output_tokens", 0) or 0)
            server_tool_use = getattr(usage, "server_tool_use", None)
            if server_tool_use is not None:
                searches = int(getattr(server_tool_use, "web_search_requests", 0) or 0)

        event = CostEvent(
            label=label,
            input_tokens=in_tok,
            output_tokens=out_tok,
            web_searches=searches,
            usd=(
                in_tok * INPUT_RATE_USD_PER_TOKEN
                + out_tok * OUTPUT_RATE_USD_PER_TOKEN
                + searches * WEB_SEARCH_RATE_USD
            ),
        )
        self.input_tokens += in_tok
        self.output_tokens += out_tok
        self.web_searches += searches
        self.events.append(event)
        logger.info(
            f"[cost] {label}: in={in_tok} out={out_tok} searches={searches} "
            f"= ${event.usd:.3f} | running ${self.total_usd():.2f}/{self.cap_usd:.0f} "
            f"(remaining ${self.remaining_usd():.2f})"
        )
        return event

    def require_remaining(self, estimated_usd: float, stage_label: str) -> None:
        """Abort if running spend + estimated cost would exceed the cap."""
        if self.total_usd() + estimated_usd > self.cap_usd:
            raise BudgetExceeded(
                f"Stage '{stage_label}' would push spend to "
                f"${self.total_usd() + estimated_usd:.2f}, over cap of ${self.cap_usd:.2f}. "
                f"Aborting."
            )

    def summary(self) -> str:
        lines = [
            f"Cost summary (cap ${self.cap_usd:.2f}):",
            f"  Total spend: ${self.total_usd():.2f}",
            f"  Input tokens:  {self.input_tokens:,}",
            f"  Output tokens: {self.output_tokens:,}",
            f"  Web searches:  {self.web_searches}",
            f"  Calls: {len(self.events)}",
        ]
        for e in self.events:
            lines.append(f"    - {e.label:35s} ${e.usd:6.3f}  in={e.input_tokens:5d} out={e.output_tokens:5d} searches={e.web_searches}")
        return "\n".join(lines)

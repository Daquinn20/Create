"""SEC EDGAR companyfacts fetcher for authoritative historical financials.

Fetches XBRL-tagged data straight from filings so we don't inherit any
third-party data-vendor bugs.  Returns annuals in the same schema as
revenue_data.historical_margins so the caller can slot in as a drop-in.
"""
from __future__ import annotations

import html as _html
import logging
import re as _re
import requests
from functools import lru_cache
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SEC_BASE = "https://data.sec.gov"
# SEC requires a descriptive User-Agent identifying the requester.
UA = "TargetedEquityConsulting daquinn@targetedequityconsulting.com"

# XBRL tag priority — try each in order, take the first with data.  Different
# companies (and different fiscal years) use different tags for the same line.

# ── Income Statement (period-based / duration contexts) ────────────────────
REVENUE_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",  # post ASC-606 (most common)
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
]
COST_OF_REVENUE_TAGS = ["CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold"]
GROSS_PROFIT_TAGS = ["GrossProfit"]
RD_TAGS = ["ResearchAndDevelopmentExpense"]
SGA_TAGS = ["SellingGeneralAndAdministrativeExpense",
            "SellingAndMarketingExpense",  # fallback for split reporters
            "GeneralAndAdministrativeExpense"]
OPERATING_EXPENSES_TAGS = ["OperatingExpenses"]
OPERATING_INCOME_TAGS = ["OperatingIncomeLoss"]
INTEREST_EXPENSE_TAGS = ["InterestExpense", "InterestExpenseDebt"]
PRETAX_INCOME_TAGS = ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
                       "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"]
INCOME_TAX_TAGS = ["IncomeTaxExpenseBenefit"]
NET_INCOME_TAGS = ["NetIncomeLoss", "ProfitLoss"]
EPS_BASIC_TAGS = ["EarningsPerShareBasic"]
EPS_DILUTED_TAGS = ["EarningsPerShareDiluted"]
SHARES_DILUTED_TAGS = ["WeightedAverageNumberOfDilutedSharesOutstanding"]
SHARES_BASIC_TAGS = ["WeightedAverageNumberOfSharesOutstandingBasic"]

# ── Cash Flow (period-based) ───────────────────────────────────────────────
CFO_TAGS = ["NetCashProvidedByUsedInOperatingActivities"]
CFI_TAGS = ["NetCashProvidedByUsedInInvestingActivities"]
CFF_TAGS = ["NetCashProvidedByUsedInFinancingActivities"]
CAPEX_TAGS = ["PaymentsToAcquirePropertyPlantAndEquipment"]
DA_TAGS = ["DepreciationAndAmortization",
           "DepreciationDepletionAndAmortization",
           "DepreciationAmortizationAndAccretionNet"]
SBC_TAGS = ["ShareBasedCompensation"]
DIVIDENDS_PAID_TAGS = ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"]
BUYBACKS_TAGS = ["PaymentsForRepurchaseOfCommonStock",
                 "PaymentsForRepurchaseOfEquity"]
DEBT_REPAID_TAGS = ["RepaymentsOfLongTermDebt", "RepaymentsOfDebt"]
DEBT_ISSUED_TAGS = ["ProceedsFromIssuanceOfLongTermDebt", "ProceedsFromIssuanceOfDebt"]

# ── Balance Sheet (INSTANT contexts — no start date, only end/point-in-time)
TOTAL_ASSETS_TAGS = ["Assets"]
TOTAL_LIABILITIES_TAGS = ["Liabilities"]
TOTAL_EQUITY_TAGS = ["StockholdersEquity",
                      "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
CASH_TAGS = ["CashAndCashEquivalentsAtCarryingValue",
              "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
ST_INVESTMENTS_TAGS = ["ShortTermInvestments", "AvailableForSaleSecuritiesCurrent"]
CURRENT_ASSETS_TAGS = ["AssetsCurrent"]
CURRENT_LIABILITIES_TAGS = ["LiabilitiesCurrent"]
INVENTORY_TAGS = ["InventoryNet"]
RECEIVABLES_TAGS = ["AccountsReceivableNetCurrent", "ReceivablesNetCurrent"]
LT_DEBT_TAGS = ["LongTermDebtNoncurrent", "LongTermDebt"]
ST_DEBT_TAGS = ["LongTermDebtCurrent", "ShortTermBorrowings",
                "DebtCurrent", "CommercialPaper"]
GOODWILL_TAGS = ["Goodwill"]
INTANGIBLES_TAGS = ["IntangibleAssetsNetExcludingGoodwill",
                    "FiniteLivedIntangibleAssetsNet"]
PPE_TAGS = ["PropertyPlantAndEquipmentNet"]
RETAINED_EARNINGS_TAGS = ["RetainedEarningsAccumulatedDeficit"]


def _pad_cik(cik) -> str:
    """SEC requires 10-digit zero-padded CIK in the URL."""
    return str(cik).lstrip("0").zfill(10)


@lru_cache(maxsize=64)
def fetch_companyfacts(cik) -> Optional[Dict[str, Any]]:
    padded = _pad_cik(cik)
    url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{padded}.json"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=15)
        if r.status_code == 200:
            return r.json()
        logger.warning(f"SEC companyfacts CIK={padded}: HTTP {r.status_code}")
        return None
    except Exception as e:
        logger.warning(f"SEC companyfacts CIK={padded} error: {e}")
        return None


def _select_tag_values(facts: Dict[str, Any],
                       tag_candidates: List[str],
                       unit: str = "USD") -> List[Dict[str, Any]]:
    ns = facts.get("facts", {}).get("us-gaap", {})
    for tag in tag_candidates:
        if tag in ns:
            values = ns[tag].get("units", {}).get(unit) or []
            if values:
                return values
    return []


def _period_days(v: Dict[str, Any]) -> Optional[int]:
    from datetime import date
    try:
        s = v.get("start"); e = v.get("end")
        if not s or not e:
            return None
        ys, ms, ds = int(s[:4]), int(s[5:7]), int(s[8:10])
        ye, me, de = int(e[:4]), int(e[5:7]), int(e[8:10])
        return (date(ye, me, de) - date(ys, ms, ds)).days
    except Exception:
        return None


def _annual_by_end_date(values: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Take annual rows keyed by fiscal-year-END date.  Filters on period
    duration (~365 days) — the `fp=FY` flag alone is unreliable because some
    issuers file quarterly amendments with FY-tagged focus."""
    picked: Dict[str, Dict[str, Any]] = {}
    for v in values:
        if v.get("form") not in ("10-K", "10-K/A", "20-F", "40-F"):
            continue
        days = _period_days(v)
        if days is None or not (340 <= days <= 380):  # annual span (~365d ± tolerance)
            continue
        end = v.get("end")
        if not end:
            continue
        cur = picked.get(end)
        if cur is None or (cur.get("form") == "10-K/A" and v.get("form") == "10-K"):
            picked[end] = v
    return picked


def _quarterly_by_end_date(values: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Take single-quarter rows (~90d) keyed by fiscal-quarter-END date."""
    picked: Dict[str, Dict[str, Any]] = {}
    for v in values:
        if v.get("form") not in ("10-Q", "10-Q/A", "10-K", "10-K/A"):
            picked_form_ok = False
        else:
            picked_form_ok = True
        if not picked_form_ok:
            continue
        days = _period_days(v)
        if days is None or not (80 <= days <= 100):  # single quarter (~91d)
            continue
        end = v.get("end")
        if not end:
            continue
        cur = picked.get(end)
        if cur is None or (cur.get("form") == "10-Q/A" and v.get("form") == "10-Q"):
            picked[end] = v
    return picked


def _ytd9m_by_start_date(values: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Take 9-month YTD rows (~275d) from Q3 10-Q filings, keyed by START date.
    Used to derive Q4 = Annual - 9m YTD (they share the same start date)."""
    picked: Dict[str, Dict[str, Any]] = {}
    for v in values:
        if v.get("form") not in ("10-Q", "10-Q/A"):
            continue
        days = _period_days(v)
        if days is None or not (255 <= days <= 285):  # 9-month YTD (~273d)
            continue
        start = v.get("start")
        if not start:
            continue
        cur = picked.get(start)
        if cur is None or (cur.get("form") == "10-Q/A" and v.get("form") == "10-Q"):
            picked[start] = v
    return picked


def get_annual_financials(facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return list of annual rows sorted by fiscal-year end date, matching the
    schema of revenue_data.historical_margins:
        {period (str fy), date (end), revenue, gross_margin, operating_margin,
         net_margin, gross_profit, operating_income, net_income, eps_diluted,
         cfo, capex, source: 'SEC'}
    """
    if not facts:
        return []
    rev = _annual_by_end_date(_select_tag_values(facts, REVENUE_TAGS))
    gp  = _annual_by_end_date(_select_tag_values(facts, GROSS_PROFIT_TAGS))
    oi  = _annual_by_end_date(_select_tag_values(facts, OPERATING_INCOME_TAGS))
    ni  = _annual_by_end_date(_select_tag_values(facts, NET_INCOME_TAGS))
    eps = _annual_by_end_date(_select_tag_values(facts, EPS_DILUTED_TAGS, unit="USD/shares"))
    cfo = _annual_by_end_date(_select_tag_values(facts, CFO_TAGS))
    cap = _annual_by_end_date(_select_tag_values(facts, CAPEX_TAGS))

    # Only include years where we have at least revenue (drops empty legacy rows)
    end_dates = sorted(k for k in rev.keys() if rev[k].get("val"))
    out: List[Dict[str, Any]] = []
    for end in end_dates:
        rev_v = (rev.get(end) or {}).get("val")
        gp_v  = (gp.get(end)  or {}).get("val")
        oi_v  = (oi.get(end)  or {}).get("val")
        ni_v  = (ni.get(end)  or {}).get("val")

        def _pct(num, den):
            if num is None or not den:
                return None
            try:
                return float(num) / float(den) * 100.0
            except (TypeError, ValueError, ZeroDivisionError):
                return None

        # Period label = calendar year of fiscal-year-end date (matches Dell's
        # public naming: FY ending Jan 2026 = "FY2026", and matches FMP too).
        try:
            period = end[:4]
        except (TypeError, IndexError):
            period = ""

        out.append({
            "period": period,
            "date": end,
            "revenue": rev_v,
            "gross_profit": gp_v,
            "operating_income": oi_v,
            "net_income": ni_v,
            "gross_margin": _pct(gp_v, rev_v),
            "operating_margin": _pct(oi_v, rev_v),
            "net_margin": _pct(ni_v, rev_v),
            "eps_diluted": (eps.get(end) or {}).get("val"),
            "cfo": (cfo.get(end) or {}).get("val"),
            "capex": (cap.get(end) or {}).get("val"),
            "source": "SEC",
        })
    return out


def get_quarterly_financials(facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return single-quarter rows sorted by end date.  Same schema as annual.
    Q4 rows are DERIVED (Annual 10-K minus 9-month YTD 10-Q, aligned by start
    date) — Q4 is never filed as a discrete XBRL fact; it's implicit."""
    if not facts:
        return []
    # Regular Q1/Q2/Q3 rows (~91d single-quarter values from 10-Q)
    rev_q = _quarterly_by_end_date(_select_tag_values(facts, REVENUE_TAGS))
    gp_q  = _quarterly_by_end_date(_select_tag_values(facts, GROSS_PROFIT_TAGS))
    oi_q  = _quarterly_by_end_date(_select_tag_values(facts, OPERATING_INCOME_TAGS))
    ni_q  = _quarterly_by_end_date(_select_tag_values(facts, NET_INCOME_TAGS))
    eps_q = _quarterly_by_end_date(_select_tag_values(facts, EPS_DILUTED_TAGS, unit="USD/shares"))
    cfo_q = _quarterly_by_end_date(_select_tag_values(facts, CFO_TAGS))
    cap_q = _quarterly_by_end_date(_select_tag_values(facts, CAPEX_TAGS))

    # Annual (365d) + 9m YTD (275d) tables so we can derive Q4 = annual - ytd9m
    rev_ann = _annual_by_end_date(_select_tag_values(facts, REVENUE_TAGS))
    gp_ann  = _annual_by_end_date(_select_tag_values(facts, GROSS_PROFIT_TAGS))
    oi_ann  = _annual_by_end_date(_select_tag_values(facts, OPERATING_INCOME_TAGS))
    ni_ann  = _annual_by_end_date(_select_tag_values(facts, NET_INCOME_TAGS))
    cfo_ann = _annual_by_end_date(_select_tag_values(facts, CFO_TAGS))
    cap_ann = _annual_by_end_date(_select_tag_values(facts, CAPEX_TAGS))

    rev_ytd = _ytd9m_by_start_date(_select_tag_values(facts, REVENUE_TAGS))
    gp_ytd  = _ytd9m_by_start_date(_select_tag_values(facts, GROSS_PROFIT_TAGS))
    oi_ytd  = _ytd9m_by_start_date(_select_tag_values(facts, OPERATING_INCOME_TAGS))
    ni_ytd  = _ytd9m_by_start_date(_select_tag_values(facts, NET_INCOME_TAGS))
    cfo_ytd = _ytd9m_by_start_date(_select_tag_values(facts, CFO_TAGS))
    cap_ytd = _ytd9m_by_start_date(_select_tag_values(facts, CAPEX_TAGS))

    def _pct(num, den):
        if num is None or not den:
            return None
        try:
            return float(num) / float(den) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    def _sub(ann_val, ytd_val):
        if ann_val is None or ytd_val is None:
            return None
        try:
            return float(ann_val) - float(ytd_val)
        except (TypeError, ValueError):
            return None

    # Merge single-Q rows first
    end_dates = sorted(k for k in rev_q.keys() if rev_q[k].get("val"))
    out: List[Dict[str, Any]] = []
    for end in end_dates:
        rev_v = (rev_q.get(end) or {}).get("val")
        gp_v  = (gp_q.get(end)  or {}).get("val")
        oi_v  = (oi_q.get(end)  or {}).get("val")
        ni_v  = (ni_q.get(end)  or {}).get("val")
        out.append({
            "date": end,
            "revenue": rev_v,
            "gross_profit": gp_v,
            "operating_income": oi_v,
            "net_income": ni_v,
            "gross_margin": _pct(gp_v, rev_v),
            "operating_margin": _pct(oi_v, rev_v),
            "net_margin": _pct(ni_v, rev_v),
            "eps_diluted": (eps_q.get(end) or {}).get("val"),
            "cfo": (cfo_q.get(end) or {}).get("val"),
            "capex": (cap_q.get(end) or {}).get("val"),
            "source": "SEC",
        })

    # Derive Q4 rows: for each annual 10-K, find the matching 9m YTD (same
    # start date) and subtract.  End date of Q4 = end date of the annual.
    existing_ends = {r["date"] for r in out}
    for ann_end, ann_row in rev_ann.items():
        if ann_end in existing_ends:
            continue  # never overwrite a real filed quarter
        ann_start = ann_row.get("start")
        if not ann_start:
            continue
        ytd_rev = rev_ytd.get(ann_start)
        if not ytd_rev or not ytd_rev.get("val"):
            continue  # no matching 9m YTD row — can't derive
        # Sanity: YTD end must be earlier than annual end (a couple months earlier)
        try:
            from datetime import date
            ye, me, de = int(ann_end[:4]), int(ann_end[5:7]), int(ann_end[8:10])
            ys, ms, ds = int(ytd_rev["end"][:4]), int(ytd_rev["end"][5:7]), int(ytd_rev["end"][8:10])
            gap = (date(ye, me, de) - date(ys, ms, ds)).days
            if not (60 <= gap <= 120):  # Q4 span (~91d ± tolerance)
                continue
        except Exception:
            continue

        q4_rev = _sub(ann_row.get("val"), ytd_rev.get("val"))
        q4_gp  = _sub((gp_ann.get(ann_end)  or {}).get("val"), (gp_ytd.get(ann_start)  or {}).get("val"))
        q4_oi  = _sub((oi_ann.get(ann_end)  or {}).get("val"), (oi_ytd.get(ann_start)  or {}).get("val"))
        q4_ni  = _sub((ni_ann.get(ann_end)  or {}).get("val"), (ni_ytd.get(ann_start)  or {}).get("val"))
        q4_cfo = _sub((cfo_ann.get(ann_end) or {}).get("val"), (cfo_ytd.get(ann_start) or {}).get("val"))
        q4_cap = _sub((cap_ann.get(ann_end) or {}).get("val"), (cap_ytd.get(ann_start) or {}).get("val"))

        out.append({
            "date": ann_end,
            "revenue": q4_rev,
            "gross_profit": q4_gp,
            "operating_income": q4_oi,
            "net_income": q4_ni,
            "gross_margin": _pct(q4_gp, q4_rev),
            "operating_margin": _pct(q4_oi, q4_rev),
            "net_margin": _pct(q4_ni, q4_rev),
            "eps_diluted": None,  # EPS doesn't subtract cleanly (weighted avg shares changes)
            "cfo": q4_cfo,
            "capex": q4_cap,
            "source": "SEC-derived-Q4",
        })

    out.sort(key=lambda r: r.get("date", ""))
    return out


def _snapshot_by_end_date(values: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Balance-sheet items use INSTANT contexts (point-in-time, no start date).
    Key by end date; prefer values reported in the most recent filing."""
    picked: Dict[str, Dict[str, Any]] = {}
    for v in values:
        if v.get("form") not in ("10-K", "10-K/A", "10-Q", "10-Q/A"):
            continue
        if v.get("start"):  # snapshot facts have no start
            continue
        end = v.get("end")
        if not end:
            continue
        cur = picked.get(end)
        if cur is None:
            picked[end] = v
        else:
            # Prefer the row filed later (more recent restatement wins)
            if str(v.get("filed", "")) > str(cur.get("filed", "")):
                picked[end] = v
    return picked


def get_balance_sheet_snapshots(facts: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return balance-sheet snapshots keyed by end date (YYYY-MM-DD).
    Each snapshot is a dict with the standard fields (total_assets,
    total_liabilities, etc.) — values only, in raw dollars."""
    if not facts:
        return {}
    fields = {
        "total_assets": TOTAL_ASSETS_TAGS,
        "total_liabilities": TOTAL_LIABILITIES_TAGS,
        "total_equity": TOTAL_EQUITY_TAGS,
        "cash_and_equivalents": CASH_TAGS,
        "short_term_investments": ST_INVESTMENTS_TAGS,
        "current_assets": CURRENT_ASSETS_TAGS,
        "current_liabilities": CURRENT_LIABILITIES_TAGS,
        "inventory": INVENTORY_TAGS,
        "accounts_receivable": RECEIVABLES_TAGS,
        "long_term_debt": LT_DEBT_TAGS,
        "short_term_debt": ST_DEBT_TAGS,
        "goodwill": GOODWILL_TAGS,
        "intangible_assets": INTANGIBLES_TAGS,
        "ppe_net": PPE_TAGS,
        "retained_earnings": RETAINED_EARNINGS_TAGS,
    }
    per_field: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for fld, tags in fields.items():
        per_field[fld] = _snapshot_by_end_date(_select_tag_values(facts, tags))

    # Collect all end dates
    all_ends = set()
    for m in per_field.values():
        all_ends.update(m.keys())

    out: Dict[str, Dict[str, Any]] = {}
    for end in sorted(all_ends):
        row = {"date": end, "source": "SEC"}
        for fld, m in per_field.items():
            v = m.get(end)
            row[fld] = v.get("val") if v else None
        # Derived
        ta = row.get("total_assets") or 0
        tl = row.get("total_liabilities") or 0
        row["total_equity_derived"] = ta - tl if (ta and tl) else None
        cash = row.get("cash_and_equivalents") or 0
        sti = row.get("short_term_investments") or 0
        row["total_cash"] = cash + sti if (cash or sti) else None
        std = row.get("short_term_debt") or 0
        ltd = row.get("long_term_debt") or 0
        row["total_debt"] = std + ltd if (std or ltd) else None
        if row["total_debt"] is not None and cash:
            row["net_debt"] = row["total_debt"] - cash
        else:
            row["net_debt"] = None
        ca = row.get("current_assets")
        cl = row.get("current_liabilities")
        row["working_capital"] = (ca - cl) if (ca is not None and cl is not None) else None
        out[end] = row
    return out


def get_income_statement_rows(facts: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return period-based income-statement values keyed by end date.
    Covers both quarterly (~91d) and annual (~365d) contexts.
    Each row: {date, period_days, revenue, cost_of_revenue, gross_profit,
               rd, sga, operating_expenses, operating_income, interest_expense,
               pretax_income, income_tax, net_income, eps_basic, eps_diluted,
               shares_basic, shares_diluted, source}"""
    if not facts:
        return {}

    def _grab_period(tag_list, unit="USD"):
        """Return {end_date: {val, days, form}} filtered to annual or quarterly."""
        out = {}
        for v in _select_tag_values(facts, tag_list, unit=unit):
            if v.get("form") not in ("10-K", "10-K/A", "10-Q", "10-Q/A"):
                continue
            days = _period_days(v)
            if days is None:
                continue
            # Only accept clean annual or single-quarter durations
            if not ((80 <= days <= 100) or (340 <= days <= 380)):
                continue
            end = v.get("end")
            if not end:
                continue
            key = (end, "annual" if days >= 340 else "quarter")
            cur = out.get(key)
            if cur is None or str(v.get("filed", "")) > str(cur.get("filed", "")):
                out[key] = {"val": v.get("val"), "days": days, "form": v.get("form"),
                            "filed": v.get("filed")}
        return out

    tag_map = {
        "revenue": (REVENUE_TAGS, "USD"),
        "cost_of_revenue": (COST_OF_REVENUE_TAGS, "USD"),
        "gross_profit": (GROSS_PROFIT_TAGS, "USD"),
        "rd": (RD_TAGS, "USD"),
        "sga": (SGA_TAGS, "USD"),
        "operating_expenses": (OPERATING_EXPENSES_TAGS, "USD"),
        "operating_income": (OPERATING_INCOME_TAGS, "USD"),
        "interest_expense": (INTEREST_EXPENSE_TAGS, "USD"),
        "pretax_income": (PRETAX_INCOME_TAGS, "USD"),
        "income_tax": (INCOME_TAX_TAGS, "USD"),
        "net_income": (NET_INCOME_TAGS, "USD"),
        "eps_basic": (EPS_BASIC_TAGS, "USD/shares"),
        "eps_diluted": (EPS_DILUTED_TAGS, "USD/shares"),
        "shares_basic": (SHARES_BASIC_TAGS, "shares"),
        "shares_diluted": (SHARES_DILUTED_TAGS, "shares"),
    }
    per_field: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]] = {}
    for fld, (tags, unit) in tag_map.items():
        per_field[fld] = _grab_period(tags, unit=unit)

    # Group by (end, period_type)
    all_keys = set()
    for m in per_field.values():
        all_keys.update(m.keys())

    out: Dict[str, Dict[str, Any]] = {}
    for end, ptype in sorted(all_keys):
        key = f"{end}::{ptype}"
        row = {"date": end, "period_type": ptype, "source": "SEC"}
        for fld, m in per_field.items():
            v = m.get((end, ptype))
            row[fld] = v.get("val") if v else None
        out[key] = row
    return out


def get_cash_flow_rows(facts: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Same shape as get_income_statement_rows — period-based, annual + quarterly."""
    if not facts:
        return {}

    def _grab_period(tag_list, unit="USD"):
        out = {}
        for v in _select_tag_values(facts, tag_list, unit=unit):
            if v.get("form") not in ("10-K", "10-K/A", "10-Q", "10-Q/A"):
                continue
            days = _period_days(v)
            if days is None:
                continue
            if not ((80 <= days <= 100) or (340 <= days <= 380)):
                continue
            end = v.get("end")
            if not end:
                continue
            key = (end, "annual" if days >= 340 else "quarter")
            cur = out.get(key)
            if cur is None or str(v.get("filed", "")) > str(cur.get("filed", "")):
                out[key] = {"val": v.get("val"), "form": v.get("form")}
        return out

    tag_map = {
        "cfo": CFO_TAGS,
        "cfi": CFI_TAGS,
        "cff": CFF_TAGS,
        "capex": CAPEX_TAGS,
        "da": DA_TAGS,
        "sbc": SBC_TAGS,
        "dividends_paid": DIVIDENDS_PAID_TAGS,
        "buybacks": BUYBACKS_TAGS,
        "debt_repaid": DEBT_REPAID_TAGS,
        "debt_issued": DEBT_ISSUED_TAGS,
    }
    per_field: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]] = {}
    for fld, tags in tag_map.items():
        per_field[fld] = _grab_period(tags)

    all_keys = set()
    for m in per_field.values():
        all_keys.update(m.keys())

    out: Dict[str, Dict[str, Any]] = {}
    for end, ptype in sorted(all_keys):
        key = f"{end}::{ptype}"
        row = {"date": end, "period_type": ptype, "source": "SEC"}
        for fld, m in per_field.items():
            v = m.get((end, ptype))
            row[fld] = v.get("val") if v else None
        # Free cash flow derived
        cfo = row.get("cfo")
        capex = row.get("capex")
        if cfo is not None and capex is not None:
            row["fcf"] = cfo - capex  # capex is positive number → subtract
        else:
            row["fcf"] = None
        out[key] = row
    return out


def compare_annuals(sec_rows: List[Dict[str, Any]],
                    fmp_rows: List[Dict[str, Any]],
                    tolerance: float = 0.03) -> List[str]:
    """Return list of human-readable warnings where SEC and FMP disagree on
    revenue by more than `tolerance` (default 3%)."""
    warnings: List[str] = []
    fmp_by_year = {}
    for r in fmp_rows:
        p = str(r.get("period", "")).strip()
        if p.isdigit() and len(p) == 4:
            fmp_by_year[int(p)] = r
    for s in sec_rows:
        try:
            fy = int(s.get("period"))
        except (TypeError, ValueError):
            continue
        f = fmp_by_year.get(fy)
        if not f:
            continue
        s_rev = s.get("revenue")
        f_rev = f.get("revenue")
        if not (s_rev and f_rev):
            continue
        delta = abs(s_rev - f_rev) / s_rev
        if delta > tolerance:
            warnings.append(
                f"FY{fy}: SEC=${s_rev/1e9:.2f}B  FMP=${f_rev/1e9:.2f}B  "
                f"delta={delta*100:.1f}%")
    return warnings


# ─────────────────────────────────────────────────────────────────────────────
# LATEST FILING TEXT — 10-Q / 10-K risk factors + concentration disclosures
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def fetch_recent_filings(cik) -> List[Dict[str, Any]]:
    """Return recent filings metadata (form, filingDate, accession, primaryDocument),
    sorted newest-first, from SEC submissions endpoint."""
    padded = _pad_cik(cik)
    url = f"{SEC_BASE}/submissions/CIK{padded}.json"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=15)
        if r.status_code != 200:
            logger.warning(f"SEC submissions CIK={padded}: HTTP {r.status_code}")
            return []
        js = r.json()
    except Exception as e:
        logger.warning(f"SEC submissions CIK={padded} error: {e}")
        return []

    recent = (js.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []
    accessions = recent.get("accessionNumber") or []
    primary_docs = recent.get("primaryDocument") or []

    return [
        {"form": form, "filingDate": date,
         "accessionNumber": acc, "primaryDocument": doc}
        for form, date, acc, doc in zip(forms, dates, accessions, primary_docs)
    ]


def find_latest_filing(cik, form_types=("10-Q",)) -> Optional[Dict[str, Any]]:
    """Return the most recent filing of any of the given form types, or None."""
    for f in fetch_recent_filings(cik):
        if f.get("form") in form_types:
            return f
    return None


@lru_cache(maxsize=32)
def fetch_filing_text(cik, accession: str, primary_doc: str) -> str:
    """Fetch the filing's primary document and return it as plain text."""
    padded = _pad_cik(cik)
    acc_bare = accession.replace("-", "")
    url = (f"https://www.sec.gov/Archives/edgar/data/"
           f"{int(padded)}/{acc_bare}/{primary_doc}")
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        if r.status_code != 200:
            logger.warning(f"SEC filing fetch {url}: HTTP {r.status_code}")
            return ""
        return _strip_html_to_text(r.text)
    except Exception as e:
        logger.warning(f"SEC filing fetch {url} error: {e}")
        return ""


def _strip_html_to_text(html_content: str) -> str:
    """Strip HTML tags to plain text, preserving paragraph breaks."""
    text = _re.sub(r"<(script|style)[^>]*>.*?</\1>", " ",
                   html_content, flags=_re.DOTALL | _re.IGNORECASE)
    text = _re.sub(r"<(br|p|div|tr|li|h[1-6])[^>]*>", "\n",
                   text, flags=_re.IGNORECASE)
    text = _re.sub(r"<[^>]+>", " ", text)
    text = _html.unescape(text)
    text = _re.sub(r"[ \t\xa0]+", " ", text)
    text = _re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def extract_risk_factors(text: str, max_chars: int = 12000) -> str:
    """Extract Item 1A (Risk Factors) up to the next Item marker."""
    m = _re.search(r"item\s*1a\.?\s*risk\s*factors",
                   text, flags=_re.IGNORECASE)
    if not m:
        return ""
    start = m.end()
    end_m = _re.search(r"item\s*(1b|2|3|4|5)\.?\s*[a-z]",
                       text[start:], flags=_re.IGNORECASE)
    end = start + (end_m.start() if end_m else min(max_chars, len(text) - start))
    section = text[start:end].strip()
    if not section or len(section) < 200:
        return ""
    return section[:max_chars]


def extract_concentration_disclosures(text: str,
                                      window: int = 1800,
                                      max_hits: int = 8) -> List[str]:
    """Return context windows around customer/supplier/geographic concentration
    language. Deduplicates overlapping windows."""
    patterns = [
        r"concentration\s+of\s+(?:credit\s+)?risk",
        r"significant\s+customer",
        r"largest\s+customer",
        r"one\s+customer\s+accounted\s+for",
        r"customer\s+[A-F]\s+(?:accounted|represented)",
        r"end[-\s]*customer",
        r"\d{1,2}\s*%\s+of\s+(?:total\s+)?(?:net\s+)?revenue",
        r"\d{1,2}\s*%\s+of\s+(?:our\s+)?(?:total\s+)?net\s+sales",
        r"single\s+customer",
        r"top\s+(?:three|five|ten)\s+customers?",
    ]
    hits: List[str] = []
    seen_positions: List[int] = []
    for pat in patterns:
        for m in _re.finditer(pat, text, flags=_re.IGNORECASE):
            pos = m.start()
            if any(abs(pos - p) < window for p in seen_positions):
                continue
            seen_positions.append(pos)
            snippet_start = max(0, pos - window // 4)
            snippet_end = min(len(text), pos + window)
            hits.append(text[snippet_start:snippet_end].strip())
            if len(hits) >= max_hits:
                return hits
    return hits


def build_latest_filing_context(cik,
                                prefer_form: str = "10-Q",
                                risk_cap: int = 10000,
                                total_cap: int = 16000) -> str:
    """Fetch the latest 10-Q (or fall back to 10-K), extract risk factors and
    concentration disclosures, and format for injection into AI context.

    Returns empty string on any failure so callers can guard cheaply."""
    filing = find_latest_filing(cik, form_types=(prefer_form, "10-K"))
    if not filing:
        return ""

    text = fetch_filing_text(cik,
                             filing["accessionNumber"],
                             filing["primaryDocument"])
    if not text:
        return ""

    risks = extract_risk_factors(text, max_chars=risk_cap)
    conc_hits = extract_concentration_disclosures(text)

    if not risks and not conc_hits:
        return ""

    lines = [
        f"=== LATEST SEC FILING — {filing['form']} filed {filing['filingDate']} ===",
        "AUTHORITATIVE. Prefer figures/statements from this filing over any older",
        "reference for customer concentration, supplier concentration, geographic",
        "exposure, ownership, guidance, and risk factors. When these conflict with",
        "training-data facts, cite THIS filing.",
        "",
    ]
    if conc_hits:
        lines.append("--- Concentration & customer-dependency disclosures ---")
        for i, hit in enumerate(conc_hits, 1):
            lines.append(f"\n[Excerpt {i}]\n{hit}")
        lines.append("")
    if risks:
        lines.append("--- Item 1A. Risk Factors (as of this filing) ---")
        lines.append(risks)

    return "\n".join(lines)[:total_cap]

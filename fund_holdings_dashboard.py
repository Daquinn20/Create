"""
Fund Holdings Dashboard - review 13F-HR holdings and diff between periods.

Data source: SEC EDGAR (https://www.sec.gov/edgar/search/).
Look up by CIK (e.g., Ra Capital Management = 0001346824) or by fund name.
"""
from __future__ import annotations

import io
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
import streamlit as st

WATCHLIST_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "fund_watchlist.json")
DEFAULT_WATCHLIST = [
    {"cik": "0001346824", "label": "Ra Capital Management",       "category": "Healthcare", "biotech_only": False},
    {"cik": "0001583977", "label": "Cormorant Asset Management",  "category": "Healthcare", "biotech_only": False},
    {"cik": "0001263508", "label": "Baker Bros. Advisors",        "category": "Healthcare", "biotech_only": False},
    {"cik": "0001856083", "label": "Deep Track Capital",          "category": "Healthcare", "biotech_only": False},
    {"cik": "0001055951", "label": "OrbiMed Advisors",            "category": "Healthcare", "biotech_only": False},
    {"cik": "0001177719", "label": "Westfield Capital Management","category": "Healthcare", "biotech_only": True},
    {"cik": "0001082917", "label": "GW&K Investment Management",  "category": "Healthcare", "biotech_only": True},
    {"cik": "0001633313", "label": "Avoro Capital Advisors",      "category": "Healthcare", "biotech_only": False},
    {"cik": "0001009258", "label": "Deerfield Management",        "category": "Healthcare", "biotech_only": False},
    {"cik": "0001224962", "label": "Perceptive Advisors",         "category": "Healthcare", "biotech_only": False},
    {"cik": "0001536411", "label": "Duquesne Family Office",      "category": "Diversified", "biotech_only": False},
    {"cik": "0001387322", "label": "Whale Rock Capital Management","category": "Diversified", "biotech_only": False},
    {"cik": "0001569205", "label": "Fundsmith",                   "category": "Diversified", "biotech_only": False},
    {"cik": "0001135730", "label": "Coatue Management",           "category": "Diversified", "biotech_only": False},
    {"cik": "0001442891", "label": "Eventide Asset Management",   "category": "Diversified", "biotech_only": False},
]

# Case-insensitive substring match against issuer name. Broad enough to cover
# most life-sciences names (biotech + pharma) — used only for the "biotech only"
# filter on generalist funds like Westfield/GW&K.
BIOTECH_KEYWORDS = (
    "THERAPEUTIC", "PHARMA", "BIOSCIENCE", "BIOTECH", "BIOPHARMA",
    "MEDICINES", "GENOMIC", "ONCOLOGY", "IMMUNO", "VACCINE", "LIFE SCIENCES",
    "REGENERON", "MODERNA", "VERTEX", "BIOGEN", "GILEAD", "ILLUMINA",
    "ALNYLAM", "INCYTE", "EXELIXIS", "NEUROCRINE", "BEIGENE", "AMGEN",
    "VAXCYTE", "SAREPTA", "IONIS", "ARROWHEAD", "BEAM", "PRIME MEDICINE",
    "INTELLIA", "CRISPR", "EDITAS", "BLUEBIRD", "REPARE", "RELAY",
    "SUMMIT THERAPEUTICS", "ROIVANT", "STRUCTURE THERAPEUTICS",
)


def is_biotech_issuer(name: str) -> bool:
    if not isinstance(name, str):
        return False
    upper = name.upper()
    return any(kw in upper for kw in BIOTECH_KEYWORDS)


def load_watchlist() -> list[dict]:
    if not os.path.exists(WATCHLIST_PATH):
        save_watchlist(DEFAULT_WATCHLIST)
        return list(DEFAULT_WATCHLIST)
    try:
        with open(WATCHLIST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [e for e in data if e.get("cik")]
    except (json.JSONDecodeError, OSError):
        return list(DEFAULT_WATCHLIST)


def save_watchlist(entries: list[dict]) -> None:
    with open(WATCHLIST_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)

SEC_UA = "TECG Fund Holdings Dashboard daquinn@targetedequityconsulting.com"
HEADERS = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}

st.set_page_config(page_title="Fund Holdings (13F-HR)", layout="wide")


# --------------------------------------------------------------------------- #
# HTTP helpers                                                                #
# --------------------------------------------------------------------------- #
def _sec_get(url: str, params: Optional[dict] = None) -> requests.Response:
    """GET with SEC-mandated User-Agent + 100ms courtesy delay."""
    time.sleep(0.12)
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r


def _pad_cik(cik: str | int) -> str:
    return str(cik).lstrip("0").zfill(10)


# --------------------------------------------------------------------------- #
# EDGAR lookups                                                               #
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=3600, show_spinner=False)
def search_filers_by_name(name: str) -> pd.DataFrame:
    """Search EDGAR for filers matching a name that have filed 13F-HR."""
    url = "https://www.sec.gov/cgi-bin/browse-edgar"
    params = {
        "action": "getcompany",
        "company": name,
        "type": "13F-HR",
        "dateb": "",
        "owner": "include",
        "count": "40",
        "output": "atom",
    }
    r = _sec_get(url, params=params)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(r.content)
    rows = []
    for entry in root.findall("a:entry", ns):
        title = entry.findtext("a:title", default="", namespaces=ns)
        summary = entry.findtext("a:summary", default="", namespaces=ns) or ""
        # summary contains "CIK: 0001346824"
        m = re.search(r"CIK\D*(\d{4,10})", summary + " " + title)
        cik = m.group(1) if m else ""
        rows.append({"Name": title, "CIK": cik.zfill(10) if cik else ""})
    df = pd.DataFrame(rows).drop_duplicates(subset=["CIK"]).reset_index(drop=True)
    return df[df["CIK"] != ""]


@st.cache_data(ttl=3600, show_spinner=False)
def get_filer_submissions(cik: str) -> dict:
    """Fetch the filer's submissions JSON from data.sec.gov."""
    padded = _pad_cik(cik)
    r = _sec_get(f"https://data.sec.gov/submissions/CIK{padded}.json")
    return r.json()


def list_13f_filings(submissions: dict) -> pd.DataFrame:
    recent = submissions.get("filings", {}).get("recent", {})
    if not recent:
        return pd.DataFrame()
    df = pd.DataFrame(recent)
    keep_forms = {"13F-HR", "13F-HR/A"}
    df = df[df["form"].isin(keep_forms)].copy()
    df["filingDate"] = pd.to_datetime(df["filingDate"])
    df["reportDate"] = pd.to_datetime(df["reportDate"])
    df = df.sort_values("reportDate", ascending=False).reset_index(drop=True)
    return df[["form", "reportDate", "filingDate", "accessionNumber", "primaryDocument"]]


# --------------------------------------------------------------------------- #
# Information-table parsing                                                   #
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=6 * 3600, show_spinner=False)
def fetch_information_table(cik: str, accession_number: str) -> pd.DataFrame:
    """Locate and parse the informationTable XML for a 13F filing."""
    acc_clean = accession_number.replace("-", "")
    cik_int = int(cik)
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}"
    idx = _sec_get(f"{base}/index.json").json()
    xml_files = [i["name"] for i in idx.get("directory", {}).get("item", [])
                 if i["name"].lower().endswith(".xml")]

    info_xml = None
    for name in xml_files:
        # primary_doc is the cover page — skip if there is any other XML
        if "primary_doc" in name.lower() and len(xml_files) > 1:
            continue
        r = _sec_get(f"{base}/{name}")
        # Check root tag looks like an information table
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError:
            continue
        tag = root.tag.split("}", 1)[-1]
        if tag.lower() == "informationtable":
            info_xml = root
            break

    if info_xml is None:
        return pd.DataFrame()

    ns_uri = info_xml.tag.split("}")[0].strip("{") if "}" in info_xml.tag else ""
    ns = {"n": ns_uri} if ns_uri else {}
    q = (lambda tag: f"n:{tag}") if ns else (lambda tag: tag)

    rows = []
    for it in info_xml.findall(q("infoTable"), ns):
        def txt(path):
            el = it.find(q(path), ns) if "/" not in path else it.find(
                "/".join(q(p) for p in path.split("/")), ns
            )
            return (el.text or "").strip() if el is not None and el.text else ""

        shares_el = it.find(q("shrsOrPrnAmt"), ns)
        shares = ""
        share_type = ""
        if shares_el is not None:
            s = shares_el.find(q("sshPrnamt"), ns)
            t = shares_el.find(q("sshPrnamtType"), ns)
            shares = (s.text or "").strip() if s is not None and s.text else ""
            share_type = (t.text or "").strip() if t is not None and t.text else ""

        rows.append({
            "Issuer": txt("nameOfIssuer"),
            "Class": txt("titleOfClass"),
            "CUSIP": txt("cusip"),
            "Value": pd.to_numeric(txt("value"), errors="coerce"),
            "Shares": pd.to_numeric(shares, errors="coerce"),
            "ShareType": share_type,
            "PutCall": txt("putCall"),
            "Discretion": txt("investmentDiscretion"),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Aggregate duplicate rows (e.g., same CUSIP reported across multiple managers)
    df = (df.groupby(["Issuer", "Class", "CUSIP", "PutCall"], dropna=False, as_index=False)
            .agg({"Value": "sum", "Shares": "sum",
                  "ShareType": "first", "Discretion": "first"}))
    return df.sort_values("Value", ascending=False).reset_index(drop=True)


def scale_value_column(df: pd.DataFrame, report_date: pd.Timestamp) -> pd.DataFrame:
    """13F values switched from $thousands to whole dollars for periods ending
    on/after 2023-01-03 per SEC Form 13F amendment. Normalize to whole $."""
    if df.empty:
        return df
    out = df.copy()
    if report_date < pd.Timestamp("2023-01-03"):
        out["Value"] = out["Value"] * 1000
    return out


# --------------------------------------------------------------------------- #
# Diff logic                                                                  #
# --------------------------------------------------------------------------- #
@dataclass
class Diff:
    combined: pd.DataFrame
    new: pd.DataFrame
    sold: pd.DataFrame
    increased: pd.DataFrame
    decreased: pd.DataFrame
    unchanged: pd.DataFrame


def diff_holdings(curr: pd.DataFrame, prev: pd.DataFrame) -> Diff:
    # Merge on CUSIP + PutCall only. titleOfClass is filer-populated and often
    # inconsistent between periods (e.g. "SH" vs "Common Stock"); CUSIP already
    # uniquely identifies the security.
    key = ["CUSIP", "PutCall"]
    # Collapse curr/prev to one row per CUSIP+PutCall first (multiple share
    # classes for the same CUSIP would otherwise fan out on merge).
    def _collapse(df):
        return (df.groupby(key, dropna=False, as_index=False)
                  .agg({"Issuer": "first", "Class": "first",
                        "Value": "sum", "Shares": "sum",
                        "ShareType": "first", "Discretion": "first"}))
    curr_c = _collapse(curr)
    prev_c = _collapse(prev)
    c = curr_c.rename(columns={"Value": "Value_curr", "Shares": "Shares_curr"})
    p = prev_c.rename(columns={"Value": "Value_prev", "Shares": "Shares_prev",
                               "Issuer": "Issuer_prev", "Class": "Class_prev"})
    merged = c.merge(
        p[key + ["Value_prev", "Shares_prev", "Issuer_prev", "Class_prev"]],
        on=key, how="outer", indicator=True,
    )
    # Fill Issuer/Class from prev when the row exists only in prev
    merged["Issuer"] = merged["Issuer"].fillna(merged["Issuer_prev"])
    merged["Class"] = merged["Class"].fillna(merged["Class_prev"])
    merged = merged.drop(columns=["Issuer_prev", "Class_prev"])
    for col in ("Value_curr", "Value_prev", "Shares_curr", "Shares_prev"):
        merged[col] = merged[col].fillna(0)

    merged["ΔShares"] = merged["Shares_curr"] - merged["Shares_prev"]
    merged["ΔValue"] = merged["Value_curr"] - merged["Value_prev"]
    merged["ΔShares_%"] = pd.NA
    mask = merged["Shares_prev"] > 0
    merged.loc[mask, "ΔShares_%"] = (
        (merged.loc[mask, "Shares_curr"] - merged.loc[mask, "Shares_prev"])
        / merged.loc[mask, "Shares_prev"] * 100
    )

    new = merged[merged["_merge"] == "left_only"].copy()
    sold = merged[merged["_merge"] == "right_only"].copy()
    both = merged[merged["_merge"] == "both"].copy()
    increased = both[both["ΔShares"] > 0].copy()
    decreased = both[both["ΔShares"] < 0].copy()
    unchanged = both[both["ΔShares"] == 0].copy()

    display_cols = ["Issuer", "Class", "CUSIP", "PutCall",
                    "Shares_prev", "Shares_curr", "ΔShares", "ΔShares_%",
                    "Value_prev", "Value_curr", "ΔValue"]
    for d in (new, sold, increased, decreased, unchanged, merged):
        for col in display_cols:
            if col not in d.columns:
                d[col] = pd.NA

    return Diff(
        combined=merged[display_cols + ["_merge"]].sort_values("Value_curr", ascending=False),
        new=new[display_cols].sort_values("Value_curr", ascending=False),
        sold=sold[display_cols].sort_values("Value_prev", ascending=False),
        increased=increased[display_cols].sort_values("ΔValue", ascending=False),
        decreased=decreased[display_cols].sort_values("ΔValue", ascending=True),
        unchanged=unchanged[display_cols].sort_values("Value_curr", ascending=False),
    )


# --------------------------------------------------------------------------- #
# UI                                                                          #
# --------------------------------------------------------------------------- #
st.title("Fund Holdings Review (13F-HR)")
st.caption("Source: SEC EDGAR. Example: Ra Capital Management, CIK 0001346824.")

if "watchlist" not in st.session_state:
    st.session_state.watchlist = load_watchlist()

with st.sidebar:
    st.header("Filer")
    mode = st.radio("Lookup by", ["Saved", "Custom CIK", "Fund name"], horizontal=True)
    cik: Optional[str] = None
    current_label: Optional[str] = None

    biotech_only_flag = False
    if mode == "Saved":
        wl_all = st.session_state.watchlist
        if not wl_all:
            st.info("No saved funds yet. Add one via Custom CIK or Fund name.")
        else:
            categories = sorted({e.get("category", "Uncategorized") for e in wl_all})
            cat_options = ["All"] + categories
            cat_choice = st.selectbox("Category", cat_options, index=0)
            wl = wl_all if cat_choice == "All" else [
                e for e in wl_all if e.get("category", "Uncategorized") == cat_choice
            ]
            if not wl:
                st.info(f"No saved funds in category '{cat_choice}'.")
            else:
                idx = st.selectbox(
                    "Saved fund",
                    options=list(range(len(wl))),
                    format_func=lambda i: f"{wl[i]['label']} ({_pad_cik(wl[i]['cik'])})"
                        + (" [biotech only]" if wl[i].get("biotech_only") else ""),
                )
                cik = _pad_cik(wl[idx]["cik"])
                current_label = wl[idx]["label"]
                biotech_only_flag = bool(wl[idx].get("biotech_only", False))

    elif mode == "Custom CIK":
        cik_in = st.text_input("CIK", value="").strip()
        if cik_in:
            cik = _pad_cik(cik_in)

    else:  # Fund name
        name_in = st.text_input("Fund name", value="").strip()
        if name_in:
            with st.spinner("Searching EDGAR…"):
                try:
                    matches = search_filers_by_name(name_in)
                except Exception as e:
                    st.error(f"Search failed: {e}")
                    matches = pd.DataFrame()
            if matches.empty:
                st.warning("No 13F-HR filers matched.")
            else:
                label = st.selectbox(
                    "Match",
                    options=matches.index,
                    format_func=lambda i: f"{matches.at[i, 'Name']}  (CIK {matches.at[i, 'CIK']})",
                )
                cik = matches.at[label, "CIK"]
                current_label = matches.at[label, "Name"]

    st.divider()
    with st.expander("Manage saved funds", expanded=False):
        # Add current selection to the watchlist
        if cik:
            already_saved = any(_pad_cik(e["cik"]) == cik
                                for e in st.session_state.watchlist)
            if already_saved:
                st.caption(f"{cik} is already saved.")
            else:
                default_label = current_label or f"CIK {cik}"
                new_label = st.text_input("Label for saved entry",
                                          value=default_label, key="add_label")
                existing_cats = sorted({e.get("category", "Uncategorized")
                                        for e in st.session_state.watchlist})
                cat_choices = existing_cats + ["<new category>"]
                cat_pick = st.selectbox("Category", cat_choices, key="add_cat_pick")
                new_cat_name = ""
                if cat_pick == "<new category>":
                    new_cat_name = st.text_input("New category name",
                                                 value="", key="add_cat_new")
                add_biotech = st.checkbox("Biotech only (filter holdings)",
                                          value=False, key="add_biotech")
                if st.button("Add to saved funds", use_container_width=True):
                    cat_final = (new_cat_name.strip() if cat_pick == "<new category>"
                                 else cat_pick) or "Uncategorized"
                    st.session_state.watchlist.append({
                        "cik": cik,
                        "label": new_label.strip() or f"CIK {cik}",
                        "category": cat_final,
                        "biotech_only": add_biotech,
                    })
                    save_watchlist(st.session_state.watchlist)
                    st.success(f"Saved {new_label} ({cik}) → {cat_final}.")
                    st.rerun()

        # Remove
        if st.session_state.watchlist:
            wl = st.session_state.watchlist
            rm_idx = st.selectbox(
                "Remove saved fund",
                options=list(range(len(wl))),
                format_func=lambda i: f"{wl[i]['label']} ({_pad_cik(wl[i]['cik'])})",
                key="rm_idx",
            )
            if st.button("Remove", use_container_width=True):
                removed = st.session_state.watchlist.pop(rm_idx)
                save_watchlist(st.session_state.watchlist)
                st.success(f"Removed {removed['label']}.")
                st.rerun()

if not cik:
    st.info("Pick a saved fund, enter a custom CIK, or search a fund name in the sidebar.")
    st.stop()

# Filer header + filings list
try:
    subs = get_filer_submissions(cik)
except Exception as e:
    st.error(f"Failed to load submissions for CIK {cik}: {e}")
    st.stop()

st.subheader(f"{subs.get('name', 'Unknown filer')} — CIK {cik}")
filings = list_13f_filings(subs)
if filings.empty:
    st.warning("No 13F-HR filings found in recent submissions.")
    st.stop()

filings_display = filings.copy()
filings_display["Period"] = filings_display["reportDate"].dt.strftime("%Y-%m-%d")
filings_display["Filed"] = filings_display["filingDate"].dt.strftime("%Y-%m-%d")

if len(filings_display) < 3:
    st.warning("Filer has fewer than 3 13F filings — need at least 3 to compare "
               "current vs. 2 filings ago.")
    st.stop()

row_curr = filings_display.iloc[0]
row_prev = filings_display.iloc[2]  # 2 filings ago
period_curr = row_curr["Period"]
period_prev = row_prev["Period"]

st.markdown(f"Comparing **{period_curr}** (current) vs **{period_prev}** (2 filings ago)")

with st.spinner("Loading holdings…"):
    try:
        curr_df = fetch_information_table(cik, row_curr["accessionNumber"])
        prev_df = fetch_information_table(cik, row_prev["accessionNumber"])
    except Exception as e:
        st.error(f"Failed to fetch information table: {e}")
        st.stop()

curr_df = scale_value_column(curr_df, row_curr["reportDate"])
prev_df = scale_value_column(prev_df, row_prev["reportDate"])

if curr_df.empty:
    st.error(f"No information table found for {period_curr}.")
    st.stop()
if prev_df.empty:
    st.error(f"No information table found for {period_prev}.")
    st.stop()

# Biotech-only filter (per-fund preference from watchlist, overridable in UI)
biotech_filter = st.checkbox(
    "Biotech only (name-keyword match)",
    value=biotech_only_flag if mode == "Saved" else False,
    help="Filter holdings to issuers whose names match biotech/pharma keywords. "
         "Useful for generalist managers like Westfield/GW&K.",
)
if biotech_filter:
    before_curr, before_prev = len(curr_df), len(prev_df)
    curr_df = curr_df[curr_df["Issuer"].apply(is_biotech_issuer)].reset_index(drop=True)
    prev_df = prev_df[prev_df["Issuer"].apply(is_biotech_issuer)].reset_index(drop=True)
    st.caption(f"Filtered to biotech: {before_curr}→{len(curr_df)} (current), "
               f"{before_prev}→{len(prev_df)} (prior).")

# Summary metrics
aum_curr = float(curr_df["Value"].sum())
aum_prev = float(prev_df["Value"].sum())
m1, m2, m3, m4 = st.columns(4)
m1.metric("Positions (current)", len(curr_df),
          delta=len(curr_df) - len(prev_df))
m2.metric("Positions (2 filings ago)", len(prev_df))
m3.metric("AUM (current)", f"${aum_curr/1e9:,.2f}B",
          delta=f"${(aum_curr - aum_prev)/1e9:,.2f}B")
m4.metric("AUM (2 filings ago)", f"${aum_prev/1e9:,.2f}B")

diff = diff_holdings(curr_df, prev_df)

# Tabs: focused on largest / biggest change / new / sold out
tabs = st.tabs(["Largest positions", "Biggest changes", "New positions", "Sold out"])

with tabs[0]:
    st.caption(f"Top holdings as of {period_curr}, by reported value.")
    top_n = st.slider("Show top N", 10, min(100, len(curr_df)),
                      min(25, len(curr_df)), key="top_largest")
    largest = curr_df.head(top_n).copy()
    largest["% of AUM"] = (largest["Value"] / aum_curr * 100).round(2)
    st.dataframe(
        largest[["Issuer", "Class", "CUSIP", "Shares", "Value", "% of AUM", "PutCall"]],
        use_container_width=True, hide_index=True,
    )
    st.download_button("Download CSV",
                       curr_df.to_csv(index=False).encode("utf-8"),
                       f"{cik}_{period_curr}_holdings.csv", "text/csv")

with tabs[1]:
    st.caption(f"Positions held in both periods, ranked by |ΔValue| "
               f"({period_prev} → {period_curr}). Positive ΔValue = added, "
               f"negative = trimmed.")
    changed = pd.concat([diff.increased, diff.decreased], ignore_index=True)
    if changed.empty:
        st.info("No positions had share-count changes between the two filings.")
    else:
        changed["|ΔValue|"] = changed["ΔValue"].abs()
        changed = changed.sort_values("|ΔValue|", ascending=False).drop(columns="|ΔValue|")
        top_n = st.slider("Show top N", 10, min(100, len(changed)),
                          min(25, len(changed)), key="top_changed")
        st.dataframe(changed.head(top_n), use_container_width=True, hide_index=True)
        st.download_button("Download CSV",
                           changed.to_csv(index=False).encode("utf-8"),
                           f"{cik}_{period_prev}_vs_{period_curr}_changes.csv", "text/csv")

with tabs[2]:
    st.caption(f"Positions in {period_curr} that were NOT in {period_prev}.")
    if diff.new.empty:
        st.info("No new positions between the two filings.")
    else:
        new_display = diff.new[["Issuer", "Class", "CUSIP",
                                "Shares_curr", "Value_curr", "PutCall"]].copy()
        new_display["% of AUM"] = (new_display["Value_curr"] / aum_curr * 100).round(2)
        st.dataframe(new_display.sort_values("Value_curr", ascending=False),
                     use_container_width=True, hide_index=True)
        st.download_button("Download CSV",
                           diff.new.to_csv(index=False).encode("utf-8"),
                           f"{cik}_{period_curr}_new_positions.csv", "text/csv")

with tabs[3]:
    st.caption(f"Positions in {period_prev} that were fully exited by {period_curr}.")
    if diff.sold.empty:
        st.info("No sold-out positions between the two filings.")
    else:
        sold_display = diff.sold[["Issuer", "Class", "CUSIP",
                                  "Shares_prev", "Value_prev", "PutCall"]].copy()
        sold_display["% of prior AUM"] = (
            sold_display["Value_prev"] / aum_prev * 100
        ).round(2) if aum_prev else pd.NA
        st.dataframe(sold_display.sort_values("Value_prev", ascending=False),
                     use_container_width=True, hide_index=True)
        st.download_button("Download CSV",
                           diff.sold.to_csv(index=False).encode("utf-8"),
                           f"{cik}_{period_curr}_sold_positions.csv", "text/csv")

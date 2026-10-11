"""
HubSpot -> Google Sheets: raw lead table + native pivot tables
=============================================================================
Pulls one raw table from HubSpot and builds every other tab as a real Google
Sheets pivot table pointed at it. Rewritten from scratch; supersedes
leads_sales_pivots.py.

TABS PRODUCED
-------------
  1. Raw leads                22-column contact table + 2 Sale-Amount pivots
  2. Base pivot               COUNT of Record ID by Income Range x Year/Month,
                              for all owners then one block per rep
  3. Leads by source          Source x Year/Month in four income variants,
                              each condensed (top sources) and full
  4. LeadsQuotes analysis     Income Range x Qualified/Unqualified
  5. Sales by income analysis same as tab 3, filtered to the sale status
  6. Conversion Rate by Rep   all-owners totals + % (COUNTIFS, ignore the
                              slicers) then owner detail pivots (follow them)

FILTERS - WHAT CHANGED AND WHY
------------------------------
Earlier versions declared pivot filters through the API's `filterSpecs`
field. Those filters never appeared in the sheet, in three different
encodings (empty visibleValues + visibleByDefault, enumerated values, and a
mix). Hard filters written through the OLDER `criteria` field did work - the
sales pivots filtered correctly and showed up in the editor.

So this build uses `criteria` for everything, and a "dropdown" is simply a
criteria entry that lists EVERY value in that column: the filter is real, and
nothing is excluded. Same mechanism as the hard filters that already work,
which is the whole point.

CONFIRMED AGAINST YOUR DATA (not assumed)
-----------------------------------------
  * A SALE is Contact Status* = "Deal Closed-Won". The workbook's export said
    "Client (Sale)" but your portal's statuses were migrated since; the value
    is discovered from the data at run time, never hardcoded.
  * Months are written ZERO-PADDED ("01".."12"). A pivot group sorts its
    values alphabetically, so bare numbers come out 1, 10, 11, 12, 2, 3.
  * Years are written as TEXT. Pivot filters match by string, so a numeric
    2026 cell is never matched by the filter value "2026".
  * Sales are bucketed by the LEAD'S CREATED month, not the sale date - the
    workbook has one Month/Year pair, derived from Date Created.

SAFETY PROPERTIES
-----------------
  * Nothing is written until every pivot filter has been checked against the
    real data. A run that would produce empty pivots aborts first.
  * The raw tab is rewritten IN PLACE, keeping its sheet id, so pivots
    elsewhere are never orphaned - and it is trimmed to exactly the rows
    written, so a whole-column range (A:V) stays exact.
  * Pivot sizes are measured from the data, so pivots cannot overlap and
    render #REF!.
  * Tabs listed in PRESERVE_EXISTING_TABS are never touched once they exist.

SETUP:
  export HUBSPOT_ACCESS_TOKEN="pat-na1-xxxxxxxx"
  export GOOGLE_SERVICE_ACCOUNT_JSON='{...}'   (optional; falls back below)
  pip install requests pytz gspread google-auth --break-system-packages

USAGE:
  python hubspot_pivots.py
"""

import os
import sys
import json
import time
import calendar
from datetime import datetime, timedelta, timezone as dt_timezone
from concurrent.futures import ThreadPoolExecutor

import requests
import pytz
import gspread
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEARCH_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/contacts/search"
PROPERTIES_ENDPOINT = "https://api.hubapi.com/crm/v3/properties/contacts"

SPREADSHEET_ID = "16kZ7km-RPVqD1FK83q8N8ODuO5TNYFZvQjRAqZzoLro"

RAW_TAB_NAME = "Raw leads"
BASE_TAB_NAME = "Base pivot"
SOURCE_TAB_NAME = "Leads by source"
QUAL_TAB_NAME = "LeadsQuotes analysis"
SALES_TAB_NAME = "Sales by income analysis"
# Retired: this tab duplicated the Conversion tab's owner breakdown (in the
# source workbook the two pivots were byte-identical, same range A4:J62).
# The name is kept only so DELETE_RETIRED_TABS can clean it up.
PIVS_TAB_NAME = "Leads&Sales Pivs"

# Tabs written by an earlier version that should be removed on the next run
# rather than left behind showing stale numbers.
DELETE_RETIRED_TABS = [PIVS_TAB_NAME, "Leads analysis"]
CONVERSION_TAB_NAME = "Conversion Rate by Rep"

# Tabs left completely alone if they already exist, so hand-built pivots and
# filters survive. Empty by default now that filters are written by script.
PRESERVE_EXISTING_TABS = set()

REPORT_START_YEAR = 2025

# ---------------------------------------------------------------------------
# FREEZE HISTORICAL MONTHS
# ---------------------------------------------------------------------------
# True  = only the current month (plus REFRESH_TRAILING_MONTHS before it) is
#         fetched from HubSpot. Every older row is copied from the existing
#         'Raw leads' tab exactly as it was written by the last run.
# False = full refresh: every month since REPORT_START_YEAR is fetched again.
#         Use this for the one-off backfill, then switch back to True.
# The FULL_REFRESH environment variable (any non-empty value) also forces a
# full refresh for a single run without editing this file.
#
# TRADE-OFF: sales and statuses are bucketed by the lead's CREATED month, so
# a lead from a frozen month that later becomes a sale, gets qualified, or
# changes owner will NOT update in the sheet until the next full refresh.
FREEZE_HISTORICAL_MONTHS = True
REFRESH_TRAILING_MONTHS = 0   # 0 = current month only; 1 = also last month
TIMEZONE = "America/New_York"
CREATE_DATE_PROPERTY = "leconnex_create_date"

MAX_RESULTS_PER_QUERY = 9500
MONTH_FETCH_CONCURRENCY = 3
RAW_WRITE_CHUNK_ROWS = 2000

# Optional pin. None = discover the sale status from the data.
SALE_STATUS_VALUE = None

UNKNOWN_BAND_LABEL = "Unknown"
INCOME_BAND_ORDER = ["0-75", "150-250", "1M+", "250-500", "500-1M", "75-150", UNKNOWN_BAND_LABEL]
HIGH_INCOME_BANDS = ["75-150", "150-250", "250-500", "500-1M", "1M+"]
LEGACY_BAND_MAP = {
    "0-50": "0-75",
    "100-150": "75-150",
    "250+": "250-500",
    "50-100": UNKNOWN_BAND_LABEL,   # straddles 75k - not counted as High or Low
}

# Condensed Source pivots show only these. Verified: these four reproduce the
# workbook's 724 / 645 / 651 monthly totals for 2026 exactly.
TOP_SOURCES = ["AI/Chat GPT", "Google", "x", "y"]

MIN_LEADS_FOR_REP_BLOCK = 250

# Owners given rows in the Owner x Income conversion %% table. None = the
# rep list. Every owner would mean ~5,400 COUNTIFS over a 16k-row table.
PERCENT_BLOCK_OWNERS = None

# True  = the headline CONVERSION % is driven by the SLICERS, via two hidden
#         helper pivots (see percent_from_pivots).
# False = it is a COUNTIFS table driven by the dropdown cells, so it always
#         matches the LEADS and SALES tables printed beside it.
SLICER_DRIVEN_PERCENT = True
REP_BLOCK_OWNERS = None

# Service-account key is NOT stored in this file. Set GOOGLE_SERVICE_ACCOUNT_JSON
# (GitHub Actions secret, or a local environment variable) to the full JSON key.
_LOCAL_SERVICE_ACCOUNT_INFO_FALLBACK = None

# ---------------------------------------------------------------------------
# The raw table. Pivots address columns by index into this list, resolved
# through COL by header name, so the two can never drift apart.
#   kind="property" -> pulled from HubSpot
#   kind="month"/"year" -> derived from Date Created
# A property that will not resolve leaves its column blank and warns; it is
# never fatal, because one renamed field should not cost a 20-month fetch.
# ---------------------------------------------------------------------------
RAW_COLUMNS = [
    {"header": "Record ID",                                      "kind": "property", "override": "hs_object_id"},
    {"header": "Date Created* [DO NOT CHANGE]",                  "kind": "property", "override": CREATE_DATE_PROPERTY},
    {"header": "Month",                                          "kind": "month"},
    {"header": "Year",                                           "kind": "year"},
    {"header": "First Assigned Contact Owner (by name, not ID)", "kind": "property", "label": "First Assigned Contact Owner (by name, not ID)", "fallback": ["first assigned contact owner", "by name"]},
    {"header": "Source",                                         "kind": "property", "label": "Source*", "fallback": ["source*"]},
    {"header": "Income Range",                                   "kind": "property", "override": "your_income_range"},
    {"header": "Date stamp [Appointment Set]",                   "kind": "property", "label": "Date stamp [Appointment Set]", "fallback": ["date stamp", "appointment set"]},
    {"header": "Appointment Set Calculation",                    "kind": "property", "label": "Appointment Set Calculation", "fallback": ["appointment set calculation"]},
    {"header": "Date Met With (face-to-face)",                   "kind": "property", "label": "Date Met With (face-to-face)", "fallback": ["date met with"]},
    {"header": "Met With Client?",                               "kind": "property", "label": "Met With Client?", "fallback": ["met with client"]},
    {"header": "Met with within 60 days?",                       "kind": "property", "label": "Met with within 60 days?", "fallback": ["met with within 60"]},
    {"header": "Owner when Met with",                            "kind": "property", "label": "Owner when Met with", "fallback": ["owner when met with"]},
    {"header": "Qualified/Unqualified?*",                        "kind": "property", "label": "Qualified/Unqualified?*", "fallback": ["qualified/unqualified"]},
    {"header": "Became Unqualified Date",                        "kind": "property", "label": "Became Unqualified Date", "fallback": ["became unqualified date"]},
    {"header": "Unqualified Reason*",                            "kind": "property", "label": "Unqualified Reason*", "fallback": ["unqualified reason"]},
    {"header": "Contact Owner When Became Unqualified",          "kind": "property", "label": "Contact Owner When Became Unqualified", "fallback": ["contact owner when became unqualified"]},
    {"header": "Quote",                                          "kind": "property", "label": "Quote", "fallback": ["quote"]},
    {"header": "Contact Status*",                                "kind": "property", "label": "Contact Status*", "fallback": ["contact status"]},
    {"header": "Active Lead",                                    "kind": "property", "label": "Active Lead", "fallback": ["active lead"]},
    {"header": "Leconnex Sale Date",                             "kind": "property", "label": "Leconnex Sale Date", "fallback": ["sale date"]},
    {"header": "Leconnex Sale Amount",                           "kind": "property", "label": "Leconnex Sale Amount", "fallback": ["sale amount"]},
    # Composite key, derived. A pivot grouped on TWO fields renders its outer
    # label once per group with blanks beneath, so there is no row to MATCH on
    # for an owner's second and later bands. Grouping a helper pivot on this
    # single combined field sidesteps that entirely, which is what lets the
    # owner conversion % be driven by the slicers like everything else.
    {"header": "Owner | Income Range",                           "kind": "composite"},
]

COL = {c["header"]: i for i, c in enumerate(RAW_COLUMNS)}
C_RECORD_ID = COL["Record ID"]
C_MONTH     = COL["Month"]
C_YEAR      = COL["Year"]
C_OWNER     = COL["First Assigned Contact Owner (by name, not ID)"]
C_SOURCE    = COL["Source"]
C_INCOME    = COL["Income Range"]
C_QUAL      = COL["Qualified/Unqualified?*"]
C_STATUS    = COL["Contact Status*"]
C_ACTIVE    = COL["Active Lead"]
C_SALE_AMT  = COL["Leconnex Sale Amount"]
C_OWNER_INC = COL["Owner | Income Range"]
COMPOSITE_SEP = " | "


def load_service_account_info():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            sys.exit(f"ERROR: GOOGLE_SERVICE_ACCOUNT_JSON is set but isn't valid JSON: {e}")
    if _LOCAL_SERVICE_ACCOUNT_INFO_FALLBACK is None:
        sys.exit("ERROR: GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set - "
                 "set it to the full service account JSON key.")
    return _LOCAL_SERVICE_ACCOUNT_INFO_FALLBACK


def get_access_token():
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        sys.exit("ERROR: HUBSPOT_ACCESS_TOKEN not set.\n"
                 "  export HUBSPOT_ACCESS_TOKEN='pat-na1-xxxxxxxx'")
    return token


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

def request_with_retry(method, url, max_retries=5, **kwargs):
    """Retries 429 (honouring Retry-After), transient 5xx, and connection
    errors - the last of which raise before a response exists and would
    otherwise kill a long run over a one-second blip. 4xx bodies are printed,
    because HubSpot puts the real reason there and raise_for_status drops it."""
    kwargs.setdefault("timeout", 30)
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"  Network error ({exc.__class__.__name__}) - retry in {wait}s ({attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  Rate limited - retry in {wait}s ({attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            wait = 2 ** attempt
            print(f"  HubSpot {resp.status_code} - retry in {wait}s ({attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        if 400 <= resp.status_code < 500:
            try:
                body = resp.json()
            except ValueError:
                body = resp.text
            print(f"  HubSpot rejected the request ({resp.status_code}): {body}")
        resp.raise_for_status()
        return resp
    if last_exc:
        raise last_exc
    resp.raise_for_status()
    return resp


def sheets_retry(fn, *args, max_retries=5, **kwargs):
    """Same idea for gspread: a transient 503 from Google at the write step
    must not throw away a completed fetch."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and (status == 429 or status >= 500):
                wait = 2 ** attempt
                print(f"  Sheets {status} - retry in {wait}s ({attempt+1}/{max_retries})")
                time.sleep(wait)
                last_exc = exc
                continue
            raise
    raise last_exc


# ---------------------------------------------------------------------------
# Property resolution - non-fatal
# ---------------------------------------------------------------------------

def resolve_raw_columns(token):
    headers = {"Authorization": f"Bearer {token}"}
    props = request_with_retry("GET", PROPERTIES_ENDPOINT, headers=headers).json().get("results", [])
    by_label = {}
    for p in props:
        by_label.setdefault((p.get("label") or "").strip().lower(), []).append(p.get("name"))

    resolved, missing = {}, []
    for col in RAW_COLUMNS:
        if col["kind"] != "property":
            continue
        header = col["header"]
        if col.get("override"):
            resolved[header] = col["override"]
            continue
        hits = by_label.get(col["label"].strip().lower(), [])
        if len(hits) == 1:
            resolved[header] = hits[0]
            continue
        kws = [k.lower() for k in col.get("fallback", [])]
        subs = [p.get("name") for p in props
                if kws and all(k in (p.get("label") or "").lower() for k in kws)]
        if len(subs) == 1:
            resolved[header] = subs[0]
        else:
            resolved[header] = None
            missing.append(header)
        if len(hits) > 1:
            print(f"  '{col['label']}' is ambiguous ({hits}) - column left blank.")

    for col in RAW_COLUMNS:
        if col["kind"] == "property":
            name = resolved.get(col["header"])
            print(f"  {col['header'][:48]:<50} -> {name or '*** UNRESOLVED - blank column ***'}")
    if missing:
        print(f"\n  {len(missing)} column(s) blank: {missing}")
        print("  Pivots that don't use them are unaffected.")
    return resolved


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def utc_day_bounds_ms(year, month, day, end_of_day=False):
    ms = int(datetime(year, month, day, tzinfo=dt_timezone.utc).timestamp() * 1000)
    return ms + (24 * 60 * 60 * 1000 - 1) if end_of_day else ms


def local_today():
    return datetime.now(pytz.timezone(TIMEZONE)).date()


class MonthWindow:
    __slots__ = ("year", "month", "start_ms", "end_ms")

    def __init__(self, year, month, today):
        self.year, self.month = year, month
        last = calendar.monthrange(year, month)[1]
        end_day = today.day if (year == today.year and month == today.month) else last
        self.start_ms = utc_day_bounds_ms(year, month, 1)
        self.end_ms = utc_day_bounds_ms(year, month, end_day, end_of_day=True)


def build_month_windows(today):
    """Months are the FETCH unit - they keep each query far under the 10k cap
    and give natural concurrency. The report itself no longer aggregates by
    month; the pivots do that."""
    windows = []
    for year in range(REPORT_START_YEAR, today.year + 1):
        last_month = today.month if year == today.year else 12
        windows += [MonthWindow(year, m, today) for m in range(1, last_month + 1)]
    return windows


def _date_filters(start_ms, end_ms):
    return [{"propertyName": CREATE_DATE_PROPERTY, "operator": "GTE", "value": start_ms},
            {"propertyName": CREATE_DATE_PROPERTY, "operator": "LTE", "value": end_ms}]


def _probe(token, filters):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"filterGroups": [{"filters": filters}], "properties": [], "limit": 1}
    return request_with_retry("POST", SEARCH_ENDPOINT, headers=headers, json=body).json().get("total", 0)


def _paginate(token, filters, properties):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    out, after = [], None
    while True:
        body = {"filterGroups": [{"filters": filters}], "properties": properties, "limit": 100}
        if after:
            body["after"] = after
        data = request_with_retry("POST", SEARCH_ENDPOINT, headers=headers, json=body).json()
        out.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            return out


def fetch_capped(token, properties, start_ms, end_ms):
    """Splits and recurses if a range is at HubSpot's ~10,000-result ceiling,
    so a high-volume month can never silently truncate."""
    filters = _date_filters(start_ms, end_ms)
    total = _probe(token, filters)
    if total > MAX_RESULTS_PER_QUERY and end_ms > start_ms:
        mid = start_ms + (end_ms - start_ms) // 2
        print(f"  -> ~{total} contacts, over the cap - splitting")
        return (fetch_capped(token, properties, start_ms, mid)
                + fetch_capped(token, properties, mid + 1, end_ms))
    return _paginate(token, filters, properties)


def fetch_all(windows, token, properties, executor):
    def one(w):
        recs = fetch_capped(token, properties, w.start_ms, w.end_ms)
        print(f"{w.year}-{w.month:02d}  {len(recs)} contacts")
        return recs
    out = []
    for f in [executor.submit(one, w) for w in windows]:
        out.extend(f.result())
    return out


# ---------------------------------------------------------------------------
# Raw table construction
# ---------------------------------------------------------------------------

def parse_hs_date(value):
    """HubSpot returns dates as 'YYYY-MM-DD' or epoch ms depending on the
    property's type. Both handled; anything else returns None rather than
    guessing."""
    if value in (None, ""):
        return None
    s = str(value).strip()
    if s.isdigit() and len(s) >= 10:
        try:
            return datetime.fromtimestamp(int(s) / 1000, tz=dt_timezone.utc).date()
        except (ValueError, OverflowError):
            return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def as_number(value):
    if value in (None, ""):
        return ""
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return value


def normalize_owner_name(name):
    return " ".join((name or "").split()).lower()


def build_owner_roster(records, owner_prop):
    """Folds spelling variants of one person together ('Michelle Smith' /
    'michelle smith' / 'Michelle smith') and keeps the most-used form. Done on
    the RAW TABLE so every pivot inherits the fix - the workbook's own pivots
    split those variants into separate rows."""
    if not owner_prop:
        return {}
    variants = {}
    for rec in records:
        raw = (rec.get("properties", {}) or {}).get(owner_prop)
        raw = raw.strip() if raw else ""
        if not raw:
            continue
        key = normalize_owner_name(raw)
        variants.setdefault(key, {})
        variants[key][raw] = variants[key].get(raw, 0) + 1
    roster = {k: max(v.items(), key=lambda kv: kv[1])[0] for k, v in variants.items()}
    folded = sum(1 for v in variants.values() if len(v) > 1)
    print(f"Owner roster: {len(roster)} owners ({folded} had spelling variants folded).")
    return roster


def normalize_income(value):
    raw = (value or "").strip() if isinstance(value, str) else (value or "")
    if not raw:
        return UNKNOWN_BAND_LABEL
    if raw in INCOME_BAND_ORDER:
        return raw
    return LEGACY_BAND_MAP.get(raw, UNKNOWN_BAND_LABEL)


def build_raw_rows(records, resolved, roster):
    """Header + one row per contact, 22 columns.

    Month is ZERO-PADDED text and Year is text. Both are deliberate: pivot
    groups sort alphabetically (so bare 1..12 comes out 1, 10, 11, 12, 2) and
    pivot filters match by string (so a numeric 2026 cell never matches the
    filter value "2026")."""
    header = [c["header"] for c in RAW_COLUMNS]
    created_header = "Date Created* [DO NOT CHANGE]"
    created_prop = resolved.get(created_header)
    owner_header = "First Assigned Contact Owner (by name, not ID)"

    rows = []
    for rec in records:
        props = rec.get("properties", {}) or {}
        created = parse_hs_date(props.get(created_prop)) if created_prop else None
        row = []
        for col in RAW_COLUMNS:
            kind, name = col["kind"], col["header"]
            if kind == "composite":
                row.append("")          # filled in below, once the row exists
            elif kind == "month":
                row.append(f"{created.month:02d}" if created else "")
            elif kind == "year":
                row.append(str(created.year) if created else "")
            else:
                value = props.get(resolved.get(name)) if resolved.get(name) else None
                if name == created_header:
                    row.append(created.isoformat() if created else "")
                elif name == owner_header:
                    raw = value.strip() if value else ""
                    row.append(roster.get(normalize_owner_name(raw), raw) if raw else "")
                elif name == "Income Range":
                    row.append(normalize_income(value))
                elif name in ("Leconnex Sale Amount", "Record ID"):
                    row.append(as_number(value))
                else:
                    row.append("" if value is None else value)
        row[C_OWNER_INC] = f"{row[C_OWNER]}{COMPOSITE_SEP}{row[C_INCOME]}"
        rows.append(row)

    rows.sort(key=lambda r: (int(r[C_YEAR]) if str(r[C_YEAR]).isdigit() else 0,
                             int(r[C_MONTH]) if str(r[C_MONTH]).isdigit() else 0))
    return [header] + rows


def distinct_values(raw_rows, offset):
    """Every distinct value in one column, blanks included as "". Blanks must
    be listed: a contact with no owner still has to stay visible, or the
    filtered pivot's total silently disagrees with the base pivot."""
    return sorted({str(r[offset]) for r in raw_rows[1:]})


def resolve_sale_status(raw_rows):
    """Finds the Contact Status* value meaning 'made a sale' from the DATA.

    The workbook's export said "(emoji) Client (Sale)"; the live portal uses
    "Deal Closed-Won". Hardcoding either is how the sales pivots came back
    empty, so several shapes are tried in order and the choice is printed.
    NEGATIVE_HINTS keep 'No Sale' and 'Closed-Lost' from ever being picked."""
    if SALE_STATUS_VALUE:
        print(f"Sale status pinned by config: {SALE_STATUS_VALUE!r}")
        return SALE_STATUS_VALUE

    counts = {}
    for r in raw_rows[1:]:
        v = str(r[C_STATUS])
        if v.strip():
            counts[v] = counts.get(v, 0) + 1

    NEGATIVE = ("no sale", "not ", "lost", "cancel", "missed", "unqualified")
    RULES = [("client + sale", lambda s: "client" in s and "sale" in s),
             ("closed + won",  lambda s: "closed" in s and "won" in s),
             ("won",           lambda s: "won" in s)]
    candidates = {v: n for v, n in counts.items()
                  if not any(h in v.lower() for h in NEGATIVE)}
    for rule, test in RULES:
        hits = [v for v in candidates if test(v.lower())]
        if hits:
            best = max(hits, key=lambda v: candidates[v])
            print(f"Sale status resolved by rule '{rule}': {best!r} ({candidates[best]} rows).")
            return best

    print("*** No Contact Status* value looks like a sale. Values present: ***")
    for v, n in sorted(counts.items(), key=lambda kv: -kv[1])[:30]:
        print(f"    {n:>6}  {v!r}")
    return None


def choose_reps(raw_rows):
    if REP_BLOCK_OWNERS:
        return list(REP_BLOCK_OWNERS)
    counts = {}
    for r in raw_rows[1:]:
        if r[C_OWNER]:
            counts[r[C_OWNER]] = counts.get(r[C_OWNER], 0) + 1
    reps = sorted(((o, n) for o, n in counts.items() if n >= MIN_LEADS_FOR_REP_BLOCK),
                  key=lambda kv: (-kv[1], kv[0]))
    print(f"Rep blocks (>= {MIN_LEADS_FOR_REP_BLOCK} leads): "
          + ", ".join(f"{o} ({n})" for o, n in reps))
    return [o for o, _ in reps]


# ---------------------------------------------------------------------------
# Pivot construction
#
# FILTERS ARE WRITTEN THROUGH `criteria`, NOT `filterSpecs`.
# filterSpecs never rendered in the sheet across three separate encodings.
# criteria demonstrably works - the sales pivots filtered correctly with it.
# A "dropdown" here is just a criteria entry listing EVERY value in the
# column: a real filter, with nothing excluded.
# ---------------------------------------------------------------------------

def pivot_spec(raw_id, raw_rows_n, row_offsets, col_offsets, values, criteria=None,
               end_col=None):
    """end_col narrows the source range. A slicer only reaches pivots whose
    data range MATCHES its own, so giving one pivot a distinct range is how it
    gets its own slicer set while sitting on the same sheet as the others."""
    spec = {
        "source": {"sheetId": raw_id,
                   "startRowIndex": 0, "endRowIndex": raw_rows_n,
                   "startColumnIndex": 0,
                   "endColumnIndex": end_col or len(RAW_COLUMNS)},
        "rows": [{"sourceColumnOffset": o, "showTotals": True, "sortOrder": "ASCENDING"}
                 for o in row_offsets],
        "columns": [{"sourceColumnOffset": o, "showTotals": True, "sortOrder": "ASCENDING"}
                    for o in col_offsets],
        "values": [{"sourceColumnOffset": o, "summarizeFunction": fn, "name": nm}
                   for o, fn, nm in values],
        "valueLayout": "HORIZONTAL",
    }
    if criteria:
        spec["criteria"] = {str(off): {"visibleValues": [str(v) for v in vals]}
                            for off, vals in criteria.items()}
    return spec


def place_pivot(sheet_id, row, col, spec):
    return {"updateCells": {"rows": [{"values": [{"pivotTable": spec}]}],
                            "start": {"sheetId": sheet_id, "rowIndex": row, "columnIndex": col},
                            "fields": "pivotTable"}}


def place_title(sheet_id, row, col, text):
    return {"updateCells": {
        "rows": [{"values": [{"userEnteredValue": {"stringValue": text},
                              "userEnteredFormat": {
                                  "textFormat": {"bold": True, "fontSize": 11},
                                  "backgroundColor": {"red": 0.80, "green": 0.87, "blue": 0.98}}}]}],
        "start": {"sheetId": sheet_id, "rowIndex": row, "columnIndex": col},
        "fields": "userEnteredValue,userEnteredFormat"}}


def merged(*dicts):
    out = {}
    for d in dicts:
        if d:
            out.update(d)
    return out


PIVOT_GAP_ROWS = 4
PIVOT_SIZE_SAFETY = 6


def rows_matching(raw_rows, criteria):
    if not criteria:
        return raw_rows[1:]
    return [r for r in raw_rows[1:]
            if all(str(r[o]) in {str(v) for v in vals} for o, vals in criteria.items())]


def pivot_size(raw_rows, row_offsets, col_offsets, criteria=None, n_values=1):
    """Measured, not guessed. A pivot renders #REF! if it would overlap
    another, and its height depends entirely on how many distinct values the
    data happens to hold - 55 owners x 7 bands is ~440 rows, which no fixed
    constant was ever going to survive."""
    data = rows_matching(raw_rows, criteria)
    rkeys = {tuple(str(r[o]) for o in row_offsets) for r in data}
    ckeys = {tuple(str(r[o]) for o in col_offsets) for r in data}

    def outer(keys, offsets):
        """Number of first-level groups, i.e. how many subtotal lines the
        pivot adds. Guards the empty case: a filter that matches nothing
        leaves no keys at all, and indexing k[0] on an empty tuple used to
        raise IndexError and kill the run - a legitimately empty pivot must
        just size to zero, not crash."""
        if len(offsets) <= 1:
            return 0
        return len({k[0] for k in keys if k})

    outer_r, outer_c = outer(rkeys, row_offsets), outer(ckeys, col_offsets)
    rkeys = rkeys or {()}
    ckeys = ckeys or {()}
    height = (len(col_offsets) + 1) + len(rkeys) + outer_r + 1 + PIVOT_SIZE_SAFETY
    width = len(row_offsets) + (len(ckeys) + outer_c + 1) * n_values + PIVOT_SIZE_SAFETY
    return height, width


class TabLayout:
    """Stacks pivots down column A with measured gaps, so none can overlap."""

    def __init__(self, raw_rows, sheet_id, start_row=0):
        self.raw_rows, self.sheet_id = raw_rows, sheet_id
        self.requests, self.row, self.width = [], start_row, 1

    def add(self, title, raw_id, raw_rows_n, row_offsets, col_offsets, values,
            criteria=None, end_col=None):
        """criteria holds only the report's FIXED constraints. Interactive
        filtering is the slicers' job, so nothing here enumerates every value
        any more."""
        crit = dict(criteria or {})
        height, width = pivot_size(self.raw_rows, row_offsets, col_offsets,
                                   criteria, n_values=len(values))
        self.requests.append(place_title(self.sheet_id, self.row, 0, title))
        self.requests.append(place_pivot(
            self.sheet_id, self.row + 1, 0,
            pivot_spec(raw_id, raw_rows_n, row_offsets, col_offsets, values,
                       crit or None, end_col=end_col)))
        self.row += height + PIVOT_GAP_ROWS
        self.width = max(self.width, width)
        return self

    def result(self):
        return self.requests, self.row + 20, self.width + 5


COUNT_RECORDS = [(C_RECORD_ID, "COUNTA", "COUNT of Record ID")]

# ---------------------------------------------------------------------------
# SLICERS - the on-sheet filter controls.
#
# This is the thing that was actually wanted. A pivot's `criteria` filter is
# only visible inside the pivot editor panel; a SLICER is a control drawn on
# the sheet itself, with a dropdown listing every value, and it drives every
# pivot on that sheet whose source is the same data range.
#
# So the two mechanisms now do different jobs:
#   criteria -> fixed constraints the report defines (income band, source
#               shortlist, rep scope, the sale status)
#   slicer   -> the four controls you operate: Year, Month, Owner, Status
#
# Slicers float ON TOP of cells, so every tab leaves SLICER_BAND_ROWS blank
# at the top and the pivots start below that band.
# ---------------------------------------------------------------------------
SLICER_FIELDS = [C_YEAR, C_MONTH, C_OWNER, C_STATUS]

SLICER_BAND_ROWS = 5        # blank rows reserved at the top of each tab
SLICER_WIDTH_PX = 200
SLICER_HEIGHT_PX = 62
SLICER_GAP_PX = 12


def slicer_requests(sheet_id, raw_id, raw_rows_n, fields=None, anchor_col=0,
                    anchor_row=0, end_col=None):
    """One slicer per field, laid out left to right across the reserved band.

    columnIndex is relative to the slicer's dataRange, and the range starts at
    column 0 of the raw tab, so it is just the raw column offset.

    No filterCriteria is set, which means every value starts selected - the
    dropdown opens with everything ticked, ready to narrow."""
    reqs = []
    for i, off in enumerate(fields or SLICER_FIELDS):
        reqs.append({"addSlicer": {"slicer": {
            "spec": {
                "dataRange": {"sheetId": raw_id,
                              "startRowIndex": 0, "endRowIndex": raw_rows_n,
                              "startColumnIndex": 0,
                              "endColumnIndex": end_col or len(RAW_COLUMNS)},
                "columnIndex": off,
                "applyToPivotTables": True,
                "title": RAW_COLUMNS[off]["header"].split(" (")[0],
                "textFormat": {"bold": True},
            },
            "position": {"overlayPosition": {
                "anchorCell": {"sheetId": sheet_id, "rowIndex": anchor_row,
                               "columnIndex": anchor_col},
                "offsetXPixels": i * (SLICER_WIDTH_PX + SLICER_GAP_PX),
                "offsetYPixels": 4,
                "widthPixels": SLICER_WIDTH_PX,
                "heightPixels": SLICER_HEIGHT_PX,
            }},
        }}})
    return reqs


# ---------------------------------------------------------------------------
# Tab builders
# ---------------------------------------------------------------------------

def col_letter(index):
    """0-based column index -> A1 letter."""
    out = ""
    index += 1
    while index:
        index, rem = divmod(index - 1, 26)
        out = chr(65 + rem) + out
    return out


def place_values(sheet_id, row, col, rows_of_values, number_format=None, bold_first_row=False):
    """Writes a rectangular block of literal values / formulas."""
    fmt_base = {}
    if number_format:
        fmt_base["numberFormat"] = number_format
    grid = []
    for i, row_values in enumerate(rows_of_values):
        cells = []
        for v in row_values:
            cell = {}
            if isinstance(v, str) and v.startswith("="):
                cell["userEnteredValue"] = {"formulaValue": v}
            elif v == "":
                cell["userEnteredValue"] = {"stringValue": ""}
            elif isinstance(v, str):
                cell["userEnteredValue"] = {"stringValue": v}
            else:
                cell["userEnteredValue"] = {"numberValue": v}
            fmt = dict(fmt_base)
            if bold_first_row and i == 0:
                fmt["textFormat"] = {"bold": True}
                fmt.pop("numberFormat", None)
            if fmt:
                cell["userEnteredFormat"] = fmt
            cells.append(cell)
        grid.append({"values": cells})
    return {"updateCells": {
        "rows": grid,
        "start": {"sheetId": sheet_id, "rowIndex": row, "columnIndex": col},
        "fields": "userEnteredValue,userEnteredFormat"}}


def conversion_percent_block(sheet_id, raw_rows, leads_anchor_row, sales_anchor_row,
                             start_row, months):
    """A sales / leads percentage table, built from formulas that read the two
    pivots ABOVE it.

    Why formulas: a Sheets pivot cannot divide one pivot by another, so the
    rate cannot be a pivot value.

    Why they reference the pivots BY LABEL (INDEX + MATCH on the row label and
    the month header) rather than by cell position: the slicers resize the
    pivots. Filter to one month and the pivot loses columns; filter to one
    owner and it may lose income bands. Fixed offsets would then read the
    wrong cells silently. MATCH follows the labels, and IFERROR leaves a blank
    where a band or month has been filtered away.

    A pivot renders as:
        anchor      -> "COUNT of Record ID" | "Month"
        anchor + 1  -> "Income Range" | 01 | 02 | ... | Grand Total
        anchor + 2  -> first income band row
    which is where the +2 / +3 below come from (1-based for A1 notation)."""
    # The window around each pivot must stop before the NEXT block starts.
    # A window that overran into the pivot below would still look correct
    # today - MATCH takes the first hit, and that is the right one - but the
    # moment a slicer filtered a band out of LEADS, MATCH would fall through
    # and silently pick up the identically-labelled row from SALES.
    row_span = len(INCOME_BAND_ORDER) + 2          # bands + Grand Total + slack
    last_col = col_letter(len(months) + 2)         # months + Grand Total + slack

    def refs(anchor):
        hdr = anchor + 2           # 1-based row holding the month headers
        first = anchor + 3         # 1-based first data row
        last = first + row_span
        return (f"$A${first}:$A${last}",
                f"$B${hdr}:${last_col}${hdr}",
                f"$B${first}:${last_col}${last}")

    l_lab, l_hdr, l_body = refs(leads_anchor_row)
    s_lab, s_hdr, s_body = refs(sales_anchor_row)

    header = ["Income Range"] + months + ["Grand Total"]
    pct_header_row = start_row + 1                 # 0-based
    grid = [header]

    labels = INCOME_BAND_ORDER + ["Grand Total"]
    for i, band in enumerate(labels):
        r1 = pct_header_row + 1 + i + 1            # 1-based row of this output row
        cells = [band]
        for j in range(len(months) + 1):
            c1 = col_letter(1 + j)                 # B, C, ...
            hdr_cell = f"{c1}${pct_header_row + 1}"
            sales = (f"INDEX({s_body},MATCH($A{r1},{s_lab},0),"
                     f"MATCH({hdr_cell},{s_hdr},0))")
            leads = (f"INDEX({l_body},MATCH($A{r1},{l_lab},0),"
                     f"MATCH({hdr_cell},{l_hdr},0))")
            # N() turns a blank pivot cell into 0 so the division is defined;
            # a zero denominator still yields a blank rather than #DIV/0!.
            cells.append(f'=IFERROR(IF(N({leads})=0,"",N({sales})/N({leads})),"")')
        grid.append(cells)

    return [
        place_title(sheet_id, start_row, 0,
                    "CONVERSION % - sales / leads, follows the slicers above"),
        place_values(sheet_id, pct_header_row, 0, grid,
                     number_format={"type": "PERCENT", "pattern": "0.00%"},
                     bold_first_row=True),
    ]


# ---------------------------------------------------------------------------
# DROPDOWN CELLS - the filter that FORMULAS can see.
#
# A slicer filters pivot tables only. Nothing in a formula can read a slicer's
# selection, so the COUNTIFS tables could never respond to one. Data-validation
# cells solve that: the formulas reference the cell, so changing it recalculates
# them instantly.
#
# ALL_TOKEN is "*", COUNTIFS's match-anything wildcard. One caveat, stated
# plainly: "*" matches any TEXT, so a row whose value in that column is blank
# is not counted while the dropdown is on "*". Choose a specific value and the
# match is exact.
# ---------------------------------------------------------------------------
ALL_TOKEN = "*"


def dropdown_cells(sheet_id, raw_rows, fields, start_row=0, label_col=0, value_col=1):
    """Label + validated dropdown per field, stacked down the left of the band."""
    reqs = []
    for i, off in enumerate(fields):
        r = start_row + i
        values = [ALL_TOKEN] + [v for v in distinct_values(raw_rows, off) if v.strip()]
        reqs.append(place_values(sheet_id, r, label_col,
                                 [[RAW_COLUMNS[off]["header"].split(" (")[0]]],
                                 bold_first_row=True))
        reqs.append(place_values(sheet_id, r, value_col, [[ALL_TOKEN]]))
        reqs.append({"setDataValidation": {
            "range": {"sheetId": sheet_id, "startRowIndex": r, "endRowIndex": r + 1,
                      "startColumnIndex": value_col, "endColumnIndex": value_col + 1},
            "rule": {
                "condition": {"type": "ONE_OF_LIST",
                              "values": [{"userEnteredValue": v} for v in values]},
                "showCustomUi": True, "strict": True,
                "inputMessage": f"{ALL_TOKEN} = all values",
            }}})
    reqs.append(place_values(sheet_id, start_row + len(fields), label_col,
                             [[f'{ALL_TOKEN} = all values']]))
    return reqs


def dd_ref(value_col, row):
    return f"${col_letter(value_col)}${row + 1}"


def countifs_block(sheet_id, start_row, start_col, months, sale, extra=None):
    """LEADS, SALES and CONVERSION % laid out SIDE BY SIDE, left to right.

    All three share the same rows, so they read across as one wide strip
    rather than forcing a scroll to compare them.

    WHY FORMULAS AND NOT PIVOTS: a slicer filters every pivot on the sheet
    that shares its data range - there is no way to exempt one. These cells
    read the raw tab directly, so the slicers cannot touch them and they
    always show all-owners totals, which is the "don't add the selected owner
    into the table metrics" requirement.

    `extra` is a list of (raw_column_offset, cell_reference) pairs appended to
    every COUNTIFS, so the dropdown cells actually filter these tables. That is
    the whole reason they exist: a slicer cannot reach a formula.

    Returns (requests, rows_consumed, cols_consumed)."""
    inc, mon, sta = col_letter(C_INCOME), col_letter(C_MONTH), col_letter(C_STATUS)
    raw = f"'{RAW_TAB_NAME}'"
    bands = INCOME_BAND_ORDER
    width = 1 + len(months) + 1            # label + months + Grand Total
    gap = 2

    col_leads = start_col
    col_sales = start_col + width + gap
    col_pct = start_col + 2 * (width + gap)

    hdr_r = start_row + 2                  # 1-based header row
    first = hdr_r + 1                      # 1-based first band row
    last = first + len(bands) - 1
    gt = last + 1                          # 1-based Grand Total row

    def counts_grid(col0, with_status):
        grid = [["Income Range"] + months + ["Grand Total"]]
        for i, band in enumerate(bands):
            r1 = first + i
            cells = [band]
            for mm in months:
                crit = (f'{raw}!${inc}$2:${inc},"{band}",'
                        f'{raw}!${mon}$2:${mon},"{mm}"')
                for off, cell in (extra or []):
                    if with_status and off == C_STATUS:
                        continue          # status is pinned on the sales table
                    crit += f",{raw}!${col_letter(off)}$2:${col_letter(off)},{cell}"
                if with_status:
                    crit += f',{raw}!${sta}$2:${sta},"{sale}"'
                cells.append(f"=COUNTIFS({crit})")
            a = col_letter(col0 + 1)
            b = col_letter(col0 + len(months))
            cells.append(f"=SUM({a}{r1}:{b}{r1})")
            grid.append(cells)
        total = ["Grand Total"]
        for j in range(len(months) + 1):
            c = col_letter(col0 + 1 + j)
            total.append(f"=SUM({c}{first}:{c}{last})")
        grid.append(total)
        return grid

    pct_grid = [["Income Range"] + months + ["Grand Total"]]
    for i, band in enumerate(bands + ["Grand Total"]):
        r1 = first + i if i < len(bands) else gt
        cells = [band]
        for j in range(len(months) + 1):
            lc = col_letter(col_leads + 1 + j)
            sc = col_letter(col_sales + 1 + j)
            cells.append(f'=IFERROR(IF({lc}{r1}=0,"",{sc}{r1}/{lc}{r1}),"")')
        pct_grid.append(cells)

    num = {"type": "NUMBER", "pattern": "#,##0"}
    pct_fmt = {"type": "PERCENT", "pattern": "0.00%"}
    reqs = [
        place_title(sheet_id, start_row, col_leads, "LEADS - all owners (ignores the slicers)"),
        place_values(sheet_id, start_row + 1, col_leads, counts_grid(col_leads, False), num, True),
        place_title(sheet_id, start_row, col_sales, "SALES - all owners (ignores the slicers)"),
        place_values(sheet_id, start_row + 1, col_sales, counts_grid(col_sales, True), num, True),
        place_title(sheet_id, start_row, col_pct, "CONVERSION % - sales / leads"),
        place_values(sheet_id, start_row + 1, col_pct, pct_grid, pct_fmt, True),
    ]
    return reqs, (gt - start_row) + 2, col_pct + width - start_col


def base_tab(raw_rows, sheet_id, raw_id, n, reps, today, sale):
    """The base pivot, and the only place it lives: COUNT of Record ID by
    Income Range x Year/Month, all owners then one block per rep."""
    lay = TabLayout(raw_rows, sheet_id, start_row=SLICER_BAND_ROWS)
    for label, owner in [("All", None)] + [(r, r) for r in reps]:
        lay.add(label, raw_id, n, [C_INCOME], [C_YEAR, C_MONTH], COUNT_RECORDS,
                criteria={C_OWNER: [owner]} if owner else None)
    reqs, rows_needed, cols_needed = lay.result()
    return slicer_requests(sheet_id, raw_id, n) + reqs, rows_needed, cols_needed


# Leads-by-source slicers: Year and Owner only. Month stays as the column
# header, and Income Range is expressed as separate pinned pivots rather than
# a filter - so neither belongs on a slicer here.
# Income Range is a slicer here as well as being pinned on six of the eight
# pivots. With the slicer on "all values" (its default) nothing changes.
# Narrow it to one band and only the pivots that pin that same band - plus the
# two "All" pivots, which pin nothing - keep data; the rest pin a different
# band and a slicer selection against a conflicting pin leaves nothing. That
# is correct, but it reads like broken pivots if you are not expecting it.
SOURCE_TAB_SLICERS = [C_YEAR, C_OWNER, C_INCOME]

# RETIRED - giving one pivot its own slicers by building it over a narrower
# data range does NOT work. Sheets rejects the batch outright:
#
#   Invalid requests[N].addSlicer:
#   Two slicers can either have zero or all common rows.
#
# In other words every slicer on a sheet must cover either exactly the same
# rows as the others or none of them, so two sets over A..W and A..V on the
# same tab are illegal. The only reliable way to give a pivot its own slicers
# is to put it on its own sheet, where slicers are scoped by definition.
#
# So all eight pivots on this tab share one slicer set.

# Sales-by-income slicers: Income Range in place of Contact Status.
#
# Contact Status was never useful here - every pivot on that tab pins it to
# the sale value, so selecting any other status would have emptied all eight.
#
# Income Range is more useful but has one interaction worth knowing: the four
# variant pivots ALSO pin Income Range. With the slicer on "all values" (its
# default) they behave exactly as before. Narrow the slicer to one band and
# only the matching variant keeps data - the others pin a different band, and
# a slicer selection plus a conflicting pin leaves nothing. That is correct
# behaviour, not a fault, but it is easy to misread as broken pivots.
SALES_TAB_SLICERS = [C_YEAR, C_MONTH, C_OWNER, C_INCOME]


def source_tab(raw_rows, sheet_id, raw_id, n, reps, today, sale, sales=False,
               columns=None, slicer_fields=None):
    """Source x Month, once per income variant (All / 0-75 / Unknown / High
    Income), in a condensed top-sources version and a full-source version.

    The income variants ARE the income filter, expressed as separate pinned
    pivots - which is why Income Range is not offered as a slicer here.

    sales=True pins the sale status on every pivot.

    NOTE on slicers vs pinned criteria: a slicer filters the underlying data
    range, so a field CAN be both a column header and a slicer. Pinned
    `criteria` are what conflict with a grouping. That is why Month can sit on
    the header while Year and Owner sit on slicers - and why Month could be
    added back to the slicer list later without moving it off the header."""
    base = {C_STATUS: [sale]} if sales else None
    kind = "Sales" if sales else "Leads"
    cols = columns if columns is not None else [C_YEAR, C_MONTH]
    lay = TabLayout(raw_rows, sheet_id, start_row=SLICER_BAND_ROWS)
    variants = [("All", None),
                ("0-75", {C_INCOME: ["0-75"]}),
                ("Unknown", {C_INCOME: [UNKNOWN_BAND_LABEL]}),
                ("High Income", {C_INCOME: HIGH_INCOME_BANDS})]
    axis = " x ".join(RAW_COLUMNS[c]["header"] for c in cols)
    if TOP_SOURCES:
        for name, inc in variants:
            lay.add(f"{name} - {kind} by Source (top sources) x {axis}",
                    raw_id, n, [C_SOURCE], cols, COUNT_RECORDS,
                    criteria=merged(base, inc, {C_SOURCE: TOP_SOURCES}))
    for name, inc in variants:
        lay.add(f"{name} - {kind} by Source (all sources) x {axis}",
                raw_id, n, [C_SOURCE], cols, COUNT_RECORDS,
                criteria=merged(base, inc))

    reqs, rows_needed, cols_needed = lay.result()
    # ONE slicer set, covering every pivot on the tab - see the note on
    # SOURCE_TAB_SLICERS for why a second set is not possible here.
    return (slicer_requests(sheet_id, raw_id, n, fields=slicer_fields) + reqs,
            rows_needed, cols_needed)


def qual_tab(raw_rows, sheet_id, raw_id, n, reps, today, sale):
    """Income Range x Qualified/Unqualified - the breakdown unique to this
    tab. Despite the tab's name, no pivot in the source workbook filters on
    the Quote field."""
    lay = TabLayout(raw_rows, sheet_id, start_row=SLICER_BAND_ROWS)
    lay.add("Income Range x Qualified/Unqualified", raw_id, n,
            [C_INCOME, C_QUAL], [C_YEAR, C_MONTH], COUNT_RECORDS)
    reqs, rows_needed, cols_needed = lay.result()
    return slicer_requests(sheet_id, raw_id, n) + reqs, rows_needed, cols_needed


def owner_detail_percent_block(sheet_id, start_row, start_col, months, owners,
                               sale, extra=None):
    """Conversion % at the SAME granularity as the owner detail pivots beside
    it: one row per Owner x Income Range, columns by Month.

    Built with COUNTIFS against the raw tab rather than by dividing the two
    pivots. The pivots group on two levels, and Sheets renders the outer level
    (Owner) only once per group with blanks beneath, so there is no reliable
    row to MATCH on for the second and later bands of each owner - a
    label-based lookup would quietly return the wrong row.

    CONSEQUENCE, worth knowing: because these are formulas, the slicers do NOT
    filter them, while the two pivots beside them DO. Select an owner and the
    pivots shrink to that owner; this table keeps showing everyone. The
    numbers stay correct either way - it is the alignment that changes.

    Rows are limited to PERCENT_BLOCK_OWNERS to keep the formula count sane:
    every owner x band x month would be ~5,400 COUNTIFS on a 16k-row table."""
    inc, mon, sta, own = (col_letter(C_INCOME), col_letter(C_MONTH),
                          col_letter(C_STATUS), col_letter(C_OWNER))
    raw = f"'{RAW_TAB_NAME}'"

    grid = [["Owner", "Income Range"] + months + ["Grand Total"]]
    for owner in owners:
        for band in INCOME_BAND_ORDER:
            cells = [owner, band]
            for mm in months:
                base = (f'{raw}!${own}$2:${own},"{owner}",'
                        f'{raw}!${inc}$2:${inc},"{band}",'
                        f'{raw}!${mon}$2:${mon},"{mm}"')
                for off, cell in (extra or []):
                    base += f",{raw}!${col_letter(off)}$2:${col_letter(off)},{cell}"
                leads = f"COUNTIFS({base})"
                sales = f'COUNTIFS({base},{raw}!${sta}$2:${sta},"{sale}")'
                cells.append(f'=IFERROR(IF({leads}=0,"",{sales}/{leads}),"")')
            base_all = (f'{raw}!${own}$2:${own},"{owner}",'
                        f'{raw}!${inc}$2:${inc},"{band}"')
            for off, cell in (extra or []):
                base_all += f",{raw}!${col_letter(off)}$2:${col_letter(off)},{cell}"
            leads_all = f"COUNTIFS({base_all})"
            sales_all = f'COUNTIFS({base_all},{raw}!${sta}$2:${sta},"{sale}")'
            cells.append(f'=IFERROR(IF({leads_all}=0,"",{sales_all}/{leads_all}),"")')
            grid.append(cells)

    return [
        place_title(sheet_id, start_row, start_col,
                    "OWNER CONVERSION % - sales / leads by Owner & Income Range"),
        place_values(sheet_id, start_row + 1, start_col, grid,
                     number_format={"type": "PERCENT", "pattern": "0.00%"},
                     bold_first_row=True),
    ], len(grid) + 2


def hide_columns(sheet_id, start_col, end_col):
    """Hides the helper-pivot columns. They only exist to be read by formulas."""
    return {"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                  "startIndex": start_col, "endIndex": end_col},
        "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}}


def percent_from_pivots(sheet_id, start_row, start_col,
                        leads_anchor_row, leads_anchor_col,
                        sales_anchor_row, sales_anchor_col,
                        months, row_labels, title, helper_rows=None):
    """A % table that DOES follow the slicers, by reading two hidden pivots.

    THE TRICK: a slicer cannot touch a formula, but it does filter every pivot
    sharing its data range. So a pair of hidden helper pivots sit off to the
    right, get filtered by the slicer like any other pivot, and these formulas
    divide one by the other. The slicer reaches the percentage indirectly.

    The helpers deliberately use a SINGLE row field. A two-level pivot renders
    its outer label once per group with blanks beneath, leaving no row to MATCH
    on for the second and later members - which is exactly why the earlier
    label-lookup attempt could not be made reliable.

    A pivot renders as:
        anchor     -> "COUNT of Record ID" | "Month"
        anchor + 1 -> "<row field>"        | 01 | ... | Grand Total
        anchor + 2 -> first data row
    hence +2 / +3 below, in 1-based A1 terms."""
    # The window must cover the HELPER PIVOT's height, not this table's row
    # count. The composite helper has one row per Owner|Income combination in
    # the data - far more than the rows printed here - and sizing the window
    # to the printed rows would leave every key past the cut-off unmatched,
    # showing blank instead of a percentage.
    span = (helper_rows if helper_rows is not None else len(row_labels)) + 4

    def refs(arow, acol):
        hdr, first = arow + 2, arow + 3
        last = first + span
        lab = col_letter(acol)
        v0, v1 = col_letter(acol + 1), col_letter(acol + len(months) + 2)
        return (f"${lab}${first}:${lab}${last}",
                f"${v0}${hdr}:${v1}${hdr}",
                f"${v0}${first}:${v1}${last}")

    l_lab, l_hdr, l_body = refs(leads_anchor_row, leads_anchor_col)
    s_lab, s_hdr, s_body = refs(sales_anchor_row, sales_anchor_col)

    hdr_row_1 = start_row + 2
    two_col = row_labels and isinstance(row_labels[0], (list, tuple))
    head = (["Owner", "Income Range"] if two_col else ["Income Range"])
    grid = [head + months + ["Grand Total"]]
    for i, label in enumerate(row_labels):
        r1 = hdr_row_1 + 1 + i
        if two_col:
            # Match on the composite key, assembled from this row's own two
            # label cells - the helper pivot is grouped on that single field.
            a = f"${col_letter(start_col)}{r1}"
            b = f"${col_letter(start_col + 1)}{r1}"
            own = f'{a}&"{COMPOSITE_SEP}"&{b}'
            cells = list(label)
        else:
            own = f"${col_letter(start_col)}{r1}"
            cells = [label]
        off0 = 2 if two_col else 1
        for j in range(len(months) + 1):
            hcell = f"{col_letter(start_col + off0 + j)}${hdr_row_1}"
            lead = f"INDEX({l_body},MATCH({own},{l_lab},0),MATCH({hcell},{l_hdr},0))"
            sale = f"INDEX({s_body},MATCH({own},{s_lab},0),MATCH({hcell},{s_hdr},0))"
            cells.append(f'=IFERROR(IF(N({lead})=0,"",N({sale})/N({lead})),"")')
        grid.append(cells)

    return [
        place_title(sheet_id, start_row, start_col, title),
        place_values(sheet_id, start_row + 1, start_col, grid,
                     number_format={"type": "PERCENT", "pattern": "0.00%"},
                     bold_first_row=True),
    ]


def conversion_tab(raw_rows, sheet_id, raw_id, n, reps, today, sale):
    """ONE set of controls: the slicers. Everything on the tab responds to them.

    The dropdown cells that used to sit in A1:B3 are gone. They existed only
    because COUNTIFS tables cannot see a slicer - so those tables have been
    replaced by pivots, or by formulas that read pivots, and the slicers now
    reach every number here.

    Layout, left to right, in two strips:
      strip 1   LEADS | SALES | CONVERSION %          (Income Range x Month)
      strip 2   OWNER DETAIL leads | sales | OWNER %  (Owner & Income x Month)

    The two % tables are formulas because a pivot cannot divide another pivot.
    They read HIDDEN helper pivots parked off to the right, which the slicers
    filter like any other pivot - so the percentages follow the slicers too.

    The owner-level helper is grouped on the composite "Owner | Income Range"
    column rather than on two fields, because a two-level pivot renders its
    outer label once per group with blanks beneath, leaving no row to MATCH on
    for an owner's second and later bands."""
    months = [v for v in distinct_values(raw_rows, C_MONTH) if v.strip()]
    pct_owners = PERCENT_BLOCK_OWNERS or reps

    reqs = list(slicer_requests(sheet_id, raw_id, n, anchor_col=0))

    gap = 2
    top_row = SLICER_BAND_ROWS
    # Space strip 1 by the MEASURED PIVOT width, not by the width of the
    # formula table. The pivots are wider than the % table beside them, and
    # spacing on the narrower figure made LEADS overlap SALES, and SALES
    # overlap the % table - 96 colliding cells, which in the sheet is #REF!.
    ih, iw = pivot_size(raw_rows, [C_INCOME], [C_MONTH], None)
    tbl_w = max(iw, 1 + len(months) + 1)
    c_leads, c_sales, c_pct = 0, tbl_w + gap, 2 * (tbl_w + gap)

    # --- strip 1: leads / sales as visible pivots, % reading them ----------
    reqs.append(place_title(sheet_id, top_row, c_leads, "LEADS by Income Range x Month"))
    reqs.append(place_pivot(sheet_id, top_row + 1, c_leads,
                            pivot_spec(raw_id, n, [C_INCOME], [C_MONTH], COUNT_RECORDS)))
    reqs.append(place_title(sheet_id, top_row, c_sales, "SALES by Income Range x Month"))
    reqs.append(place_pivot(sheet_id, top_row + 1, c_sales,
                            pivot_spec(raw_id, n, [C_INCOME], [C_MONTH],
                                       COUNT_RECORDS, {C_STATUS: [sale]})))
    reqs += percent_from_pivots(
        sheet_id, top_row, c_pct,
        top_row + 1, c_leads, top_row + 1, c_sales,
        months, INCOME_BAND_ORDER, "CONVERSION % - sales / leads",
        helper_rows=len(distinct_values(raw_rows, C_INCOME)))

    # --- strip 2: owner detail + owner % -----------------------------------
    d_row = top_row + max(ih, len(INCOME_BAND_ORDER) + 4) + 3
    dh, dw = pivot_size(raw_rows, [C_OWNER, C_INCOME], [C_MONTH], None)
    o_leads, o_sales, o_pct = 0, dw + gap, 2 * (dw + gap)

    reqs.append(place_title(sheet_id, d_row, o_leads,
                            "OWNER DETAIL - LEADS by Owner & Income Range x Month"))
    reqs.append(place_pivot(sheet_id, d_row + 1, o_leads,
                            pivot_spec(raw_id, n, [C_OWNER, C_INCOME], [C_MONTH],
                                       COUNT_RECORDS)))
    reqs.append(place_title(sheet_id, d_row, o_sales,
                            "OWNER DETAIL - SALES by Owner & Income Range x Month"))
    reqs.append(place_pivot(sheet_id, d_row + 1, o_sales,
                            pivot_spec(raw_id, n, [C_OWNER, C_INCOME], [C_MONTH],
                                       COUNT_RECORDS, {C_STATUS: [sale]})))

    # hidden composite helpers, parked past everything visible
    visible_right = max(c_pct + tbl_w, o_pct + 2 + len(months) + 1)
    # Space the helpers by their MEASURED width. Spacing them by
    # len(months)+4 made them overlap each other by two columns, which is a
    # #REF! in the sheet and 420 colliding cells in the layout check.
    ch, cw = pivot_size(raw_rows, [C_OWNER_INC], [C_MONTH], None)
    h_leads = visible_right + 4
    h_sales = h_leads + cw + 2

    reqs.append(place_title(sheet_id, top_row, h_leads, "helper - owner leads (hidden)"))
    reqs.append(place_pivot(sheet_id, top_row + 1, h_leads,
                            pivot_spec(raw_id, n, [C_OWNER_INC], [C_MONTH], COUNT_RECORDS)))
    reqs.append(place_title(sheet_id, top_row, h_sales, "helper - owner sales (hidden)"))
    reqs.append(place_pivot(sheet_id, top_row + 1, h_sales,
                            pivot_spec(raw_id, n, [C_OWNER_INC], [C_MONTH],
                                       COUNT_RECORDS, {C_STATUS: [sale]})))

    labels = [(o, b) for o in pct_owners for b in INCOME_BAND_ORDER]
    reqs += percent_from_pivots(
        sheet_id, d_row, o_pct,
        top_row + 1, h_leads, top_row + 1, h_sales,
        months, labels,
        "OWNER CONVERSION % - sales / leads (follows the slicers)",
        helper_rows=len(distinct_values(raw_rows, C_OWNER_INC)))
    reqs.append(hide_columns(sheet_id, h_leads, h_sales + cw + 1))

    rows_needed = d_row + max(dh, len(labels) + 6) + 20
    cols_needed = h_sales + cw + 6
    return reqs, rows_needed, cols_needed


def raw_tab_pivots(raw_rows, sheet_id, raw_id, n, has_active):
    """The two Sale-Amount pivots the workbook keeps on the raw sheet, placed
    to the RIGHT of the 22 data columns."""
    col = len(RAW_COLUMNS) + 3
    reqs, row = [], 0
    vals = [(C_SALE_AMT, "COUNTA", "COUNTA of Sale Amount"),
            (C_SALE_AMT, "SUM", "SUM of Sale Amount")]
    cols = [C_ACTIVE] if has_active else []
    label = ("Sale Amount - COUNT & SUM by Owner/Income x Active Lead"
             if has_active else "Sale Amount - COUNT & SUM by Owner/Income")
    h, _ = pivot_size(raw_rows, [C_OWNER, C_INCOME], cols, None, n_values=2)
    reqs.append(place_title(sheet_id, row, col, label))
    reqs.append(place_pivot(sheet_id, row + 1, col,
                            pivot_spec(raw_id, n, [C_OWNER, C_INCOME], cols, vals)))
    row += h + PIVOT_GAP_ROWS
    h2, _ = pivot_size(raw_rows, [C_INCOME], [C_OWNER], None)
    reqs.append(place_title(sheet_id, row, col, "Sale Amount - AVERAGE by Income x Owner"))
    reqs.append(place_pivot(sheet_id, row + 1, col,
                            pivot_spec(raw_id, n, [C_INCOME], [C_OWNER],
                                       [(C_SALE_AMT, "AVERAGE", "AVERAGE of Sale Amount")])))
    return reqs, row + h2 + 20


# The single place that decides which pivots live on which tab. Reorder, drop
# or add entries here; main() just walks the list.
TAB_PLAN = [
    (BASE_TAB_NAME,       base_tab),
    (SOURCE_TAB_NAME,     lambda *a: source_tab(*a, sales=False,
                                                 columns=[C_MONTH],
                                                 slicer_fields=SOURCE_TAB_SLICERS)),
    (QUAL_TAB_NAME,       qual_tab),
    (SALES_TAB_NAME,      lambda *a: source_tab(*a, sales=True,
                                                 slicer_fields=SALES_TAB_SLICERS)),
    (CONVERSION_TAB_NAME, conversion_tab),
]


# ---------------------------------------------------------------------------
# Sheets plumbing
# ---------------------------------------------------------------------------

def open_spreadsheet():
    creds = Credentials.from_service_account_info(
        load_service_account_info(), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(creds)
    try:
        return sheets_retry(client.open_by_key, SPREADSHEET_ID)
    except gspread.exceptions.APIError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in (403, 404):
            sa = load_service_account_info().get("client_email", "(unknown)")
            sys.exit(f"ERROR: cannot open the spreadsheet ({status}).\n"
                     f"  https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit\n"
                     f"Share it with EDIT access to:\n  {sa}")
        raise


def recreate_tab(spreadsheet, name, rows, cols):
    try:
        sheets_retry(spreadsheet.del_worksheet, sheets_retry(spreadsheet.worksheet, name))
    except gspread.exceptions.WorksheetNotFound:
        pass
    return sheets_retry(spreadsheet.add_worksheet, title=name,
                        rows=max(rows, 50), cols=max(cols, 30))


def write_raw_tab(spreadsheet, raw_rows):
    """Reuses the existing sheet rather than deleting it, so its sheet id is
    stable and pivots elsewhere are never orphaned - deleting it is what wiped
    the workbook once. Then trims to exactly the rows written, so a
    whole-column range ('Raw leads'!A:V) contains the data and nothing else."""
    months = sorted({str(r[C_MONTH]) for r in raw_rows[1:] if str(r[C_MONTH]).strip()})
    print(f"  Month values, in the order the pivots will sort them: {months}")

    need_rows, need_cols = len(raw_rows) + 50, len(RAW_COLUMNS) + 40
    try:
        ws = sheets_retry(spreadsheet.worksheet, RAW_TAB_NAME)
        sheets_retry(ws.clear)
        if ws.row_count < need_rows or ws.col_count < need_cols:
            sheets_retry(ws.resize, rows=max(ws.row_count, need_rows),
                         cols=max(ws.col_count, need_cols))
        print(f"  reusing '{RAW_TAB_NAME}' (sheet id {ws.id} preserved)")
    except gspread.exceptions.WorksheetNotFound:
        ws = sheets_retry(spreadsheet.add_worksheet, title=RAW_TAB_NAME,
                          rows=need_rows, cols=need_cols)
        print(f"  created '{RAW_TAB_NAME}'")

    for start in range(0, len(raw_rows), RAW_WRITE_CHUNK_ROWS):
        chunk = raw_rows[start:start + RAW_WRITE_CHUNK_ROWS]
        sheets_retry(ws.update, values=chunk, range_name=f"A{start+1}", value_input_option="RAW")
        print(f"  wrote rows {start+1}-{start+len(chunk)}")

    keep = max(len(raw_rows), 120)
    if ws.row_count != keep:
        sheets_retry(ws.resize, rows=keep, cols=ws.col_count)
    print(f"Raw table: {len(raw_rows)-1} rows x {len(RAW_COLUMNS)} columns -> '{RAW_TAB_NAME}'.")
    return ws


def format_raw_tab(spreadsheet, ws):
    sid = ws.id
    sheets_retry(spreadsheet.batch_update, {"requests": [
        {"updateSheetProperties": {
            "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"}},
        {"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": len(RAW_COLUMNS)},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "backgroundColor": {"red": 0.20, "green": 0.26, "blue": 0.33},
                "wrapStrategy": "CLIP"}},
            "fields": "userEnteredFormat(textFormat,backgroundColor,wrapStrategy)"}},
        {"setBasicFilter": {"filter": {"range": {
            "sheetId": sid, "startRowIndex": 0,
            "startColumnIndex": 0, "endColumnIndex": len(RAW_COLUMNS)}}}},
    ]})


def validate(raw_rows, named_requests):
    """Every pivot filter is checked against real data BEFORE anything is
    written. A filter matching nothing renders as a blank grid with no error
    anywhere in Sheets, which is how three tabs once came back empty with no
    indication why. Only an unmatched SALE STATUS is fatal - an income or
    owner variant matching nothing is a legitimately empty pivot."""
    distinct = {i: {str(r[i]) for r in raw_rows[1:]} for i in range(len(RAW_COLUMNS))}
    problems = []
    for tab, reqs in named_requests:
        for req in reqs:
            if "addSlicer" in req:
                continue
            cells = req.get("updateCells", {})
            if cells.get("fields") != "pivotTable":
                continue
            spec = cells["rows"][0]["values"][0]["pivotTable"]
            for off, crit in (spec.get("criteria") or {}).items():
                off = int(off)
                missing = [v for v in crit["visibleValues"] if v not in distinct[off]]
                if missing:
                    problems.append((tab, RAW_COLUMNS[off]["header"], missing,
                                     sorted(distinct[off])[:6]))
    if not problems:
        print("Pre-flight: every pivot filter matches real values.")
        return []

    fatal = [p for p in problems if p[1] == RAW_COLUMNS[C_STATUS]["header"]]
    print("\n*** PRE-FLIGHT: filters matching nothing (those pivots render EMPTY) ***")
    seen = set()
    for tab, header, missing, sample in problems:
        key = (tab, header, tuple(missing))
        if key in seen:
            continue
        seen.add(key)
        sev = "FATAL" if header == RAW_COLUMNS[C_STATUS]["header"] else "warn "
        print(f"  [{sev}] [{tab}] {header!r} wants {missing[:4]}")
        print(f"           column contains e.g. {sample}")
    if not fatal:
        print("  None fatal - continuing.")
    print()
    return fatal


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def refresh_cutoff(today):
    """(year, month) of the first month that is fetched live."""
    y, m = today.year, today.month - REFRESH_TRAILING_MONTHS
    while m < 1:
        m += 12
        y -= 1
    return y, m


def read_frozen_raw_rows(spreadsheet, cutoff):
    """Returns the existing 'Raw leads' data rows created BEFORE the cutoff
    month, or None if they can't be reused safely (tab missing, empty, or
    its header doesn't match RAW_COLUMNS) - the caller then does a full
    refresh. Only the raw table's own columns are read: the Sale-Amount
    pivots placed to the right of the data are ignored. Values are read
    UNFORMATTED, so numbers come back as numbers and the zero-padded text
    months / text years come back as text, exactly as they were written."""
    last_col = col_letter(len(RAW_COLUMNS) - 1)
    try:
        resp = sheets_retry(spreadsheet.values_get, f"'{RAW_TAB_NAME}'!A:{last_col}",
                            params={"valueRenderOption": "UNFORMATTED_VALUE"})
    except Exception as exc:
        print(f"  Could not read '{RAW_TAB_NAME}' ({exc.__class__.__name__}) - full refresh instead.")
        return None
    values = resp.get("values", [])
    expected = [c["header"] for c in RAW_COLUMNS]
    if not values or [str(v) for v in values[0][:len(expected)]] != expected:
        print(f"  '{RAW_TAB_NAME}' is empty or its columns changed - full refresh instead.")
        return None

    kept, dropped = [], 0
    for row in values[1:]:
        row = list(row) + [""] * (len(expected) - len(row))
        row = row[:len(expected)]
        if str(row[C_RECORD_ID]).strip() == "":
            continue
        try:
            ym = (int(str(row[C_YEAR])), int(str(row[C_MONTH])))
        except ValueError:
            dropped += 1          # no created date - re-fetched only on a full refresh
            continue
        if ym < cutoff:
            row[C_YEAR], row[C_MONTH] = str(ym[0]), f"{ym[1]:02d}"   # keep them TEXT for pivot filters
            kept.append(row)
    print(f"  Reusing {len(kept)} frozen rows created before {cutoff[0]}-{cutoff[1]:02d}"
          + (f" ({dropped} rows without a created date dropped)" if dropped else "") + ".")
    return kept


def unify_owner_names(data_rows):
    """Re-folds owner spelling variants across frozen + freshly fetched rows
    together (same rule as build_owner_roster: most-used spelling wins), so a
    rep never splits into two pivot rows because their name was folded
    differently in an earlier run. Also rebuilds the Owner | Income key."""
    variants = {}
    for r in data_rows:
        name = str(r[C_OWNER]).strip()
        if name:
            key = normalize_owner_name(name)
            variants.setdefault(key, {})
            variants[key][name] = variants[key].get(name, 0) + 1
    canon = {k: max(v.items(), key=lambda kv: kv[1])[0] for k, v in variants.items()}
    for r in data_rows:
        name = str(r[C_OWNER]).strip()
        r[C_OWNER] = canon.get(normalize_owner_name(name), name) if name else ""
        r[C_OWNER_INC] = f"{r[C_OWNER]}{COMPOSITE_SEP}{r[C_INCOME]}"


def main():
    token = get_access_token()
    today = local_today()

    print("Checking Google Sheets access before fetching anything...")
    spreadsheet = open_spreadsheet()
    print(f"  OK - '{spreadsheet.title}'\n")

    print("Resolving the raw table's columns:")
    resolved = resolve_raw_columns(token)
    has_active = bool(resolved.get("Active Lead"))
    properties = sorted({v for v in resolved.values() if v})

    windows = build_month_windows(today)
    frozen_rows = None
    full_refresh = (not FREEZE_HISTORICAL_MONTHS) or bool(os.environ.get("FULL_REFRESH"))
    if full_refresh:
        print("\nFULL REFRESH - every month is fetched from HubSpot.")
    else:
        cutoff = refresh_cutoff(today)
        print(f"\nFreeze ON - months before {cutoff[0]}-{cutoff[1]:02d} come from the existing sheet.")
        frozen_rows = read_frozen_raw_rows(spreadsheet, cutoff)
        if frozen_rows is not None:
            windows = [w for w in windows if (w.year, w.month) >= cutoff]
    print(f"\nWindow: {windows[0].year}-{windows[0].month:02d} to "
          f"{windows[-1].year}-{windows[-1].month:02d} ({len(windows)} months fetched live)\n")
    with ThreadPoolExecutor(max_workers=MONTH_FETCH_CONCURRENCY) as ex:
        records = fetch_all(windows, token, properties, ex)
    print(f"\nFetched {len(records)} contacts.")

    roster = build_owner_roster(records, resolved.get("First Assigned Contact Owner (by name, not ID)"))
    raw_rows = build_raw_rows(records, resolved, roster)
    if frozen_rows:
        data_rows = frozen_rows + raw_rows[1:]
        unify_owner_names(data_rows)
        data_rows.sort(key=lambda r: (int(r[C_YEAR]) if str(r[C_YEAR]).isdigit() else 0,
                                      int(r[C_MONTH]) if str(r[C_MONTH]).isdigit() else 0))
        raw_rows = [raw_rows[0]] + data_rows
        print(f"Raw table: {len(frozen_rows)} frozen + {len(records)} fresh = {len(data_rows)} rows.")
    reps = choose_reps(raw_rows)
    sale = resolve_sale_status(raw_rows)
    if sale is None:
        sys.exit("Aborting BEFORE any write: no sale status could be resolved. "
                 "Nothing in the spreadsheet has been changed.")
    print(f"Rows counted as a sale: "
          f"{sum(1 for r in raw_rows[1:] if str(r[C_STATUS]) == sale)}")

    # VALIDATE FIRST, WRITE SECOND.
    preview = [(name, b(raw_rows, 0, 0, len(raw_rows), reps, today, sale)[0])
               for name, b in TAB_PLAN]
    if validate(raw_rows, preview):
        sys.exit("Aborting BEFORE any write - the sale filter matches nothing. "
                 "Nothing in the spreadsheet has been changed.")

    raw_ws = write_raw_tab(spreadsheet, raw_rows)
    format_raw_tab(spreadsheet, raw_ws)
    raw_id, n = raw_ws.id, len(raw_rows)

    existing = {w.title for w in sheets_retry(spreadsheet.worksheets)}
    for name in DELETE_RETIRED_TABS:
        if name in existing:
            sheets_retry(spreadsheet.del_worksheet, sheets_retry(spreadsheet.worksheet, name))
            print(f"Removed retired tab '{name}'.")
            existing.discard(name)
    total = 0
    for name, build in TAB_PLAN:
        if name in PRESERVE_EXISTING_TABS and name in existing:
            print(f"'{name}': PRESERVED - left as you built it.")
            continue
        _, need_rows, need_cols = build(raw_rows, 0, raw_id, n, reps, today, sale)
        ws = recreate_tab(spreadsheet, name, need_rows, need_cols)
        reqs, _, _ = build(raw_rows, ws.id, raw_id, n, reps, today, sale)
        sheets_retry(spreadsheet.batch_update, {"requests": reqs})
        placed = sum(1 for r in reqs
                     if "updateCells" in r and r["updateCells"]["fields"] == "pivotTable")
        slicers = sum(1 for r in reqs if "addSlicer" in r)
        total += placed
        print(f"'{name}': {placed} pivot(s) + {slicers} slicer(s), tab {need_rows}x{need_cols}")

    reqs, raw_need = raw_tab_pivots(raw_rows, raw_ws.id, raw_id, n, has_active)
    if raw_ws.row_count < raw_need:
        sheets_retry(raw_ws.resize, rows=raw_need, cols=raw_ws.col_count)
    sheets_retry(spreadsheet.batch_update, {"requests": reqs})
    total += sum(1 for r in reqs if r["updateCells"]["fields"] == "pivotTable")
    print(f"'{RAW_TAB_NAME}': Sale-Amount pivot(s) placed right of the data.")

    print(f"\nDone - {total} pivot tables, all sourced from '{RAW_TAB_NAME}'.")
    print(f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")


if __name__ == "__main__":
    main()

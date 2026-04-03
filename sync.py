"""
Pulls all data from the CallRail API and writes it to the local SQLite cache.
Called when the user clicks "Refresh Data" in the dashboard.

Flow:
  1. Resolve account ID
  2. GET /a/{account_id}/companies.json → upsert all companies
  3. For each company:
       GET calls        (paginated) → upsert
       GET form subs    (paginated) → upsert

Failure handling:
  - Per-company errors are caught and logged; one bad company never stops the rest.
  - Transient HTTP errors (429, 5xx) are retried up to MAX_RETRIES with back-off.
  - A list of failure strings is returned for the caller to surface in the UI.
"""

import time
import logging
from datetime import date
from requests.exceptions import HTTPError, Timeout, RequestException

from api_client import get_account_id, get_companies, get_calls, get_form_submissions, REQUEST_GAP
from db import upsert_companies, upsert_calls, upsert_forms, load_companies, purge_non_whitelisted_companies
from config import DATE_FROM, is_whitelisted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_CODES = {429, 500, 502, 503, 504}


def _with_retry(fn, *args, label: str = "", **kwargs):
    """Call fn(*args, **kwargs) with retry/back-off on transient failures."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            # Log the full error message which now includes the response body
            log.warning("HTTP %d for %r (attempt %d/%d): %s", status, label, attempt, MAX_RETRIES, exc)
            if status in RETRY_CODES and attempt < MAX_RETRIES:
                wait = 2 ** attempt
                log.info("Retrying in %ds…", wait)
                time.sleep(wait)
                continue
            return None
        except Timeout:
            log.warning("Timeout for %r (attempt %d/%d)", label, attempt, MAX_RETRIES)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
        except RequestException as exc:
            log.error("Request error for %r: %s", label, exc)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error("Giving up on %r after %d attempts", label, MAX_RETRIES)
    return None


def sync_all(progress_callback=None) -> list[str]:
    """
    Sync whitelisted companies and their calls/forms.
    Returns a list of failure messages (empty = all good).
    """
    today    = date.today().isoformat()
    failures = []

    def _cb(msg: str):
        log.info(msg)
        if progress_callback:
            progress_callback(msg)

    # ── 1. Resolve account ID ────────────────────────────────────────────────
    _cb("Resolving CallRail account ID…")
    try:
        account_id = get_account_id()
        _cb(f"Account ID: {account_id}")
    except Exception as exc:
        msg = f"Could not resolve account ID: {exc}"
        log.error(msg)
        failures.append(msg)
        return failures
    time.sleep(REQUEST_GAP)

    # ── 2. Companies ─────────────────────────────────────────────────────────
    _cb("Fetching companies…")
    companies = _with_retry(get_companies, account_id, label="companies")
    if not companies:
        failures.append("Failed to fetch companies — aborting sync.")
        return failures
    # Filter to whitelisted clients only
    all_count = len(companies)
    companies = [c for c in companies if is_whitelisted(c.get("name", ""))]
    skipped   = all_count - len(companies)
    upsert_companies(companies)
    # Remove any previously synced companies that are no longer whitelisted
    keep_ids = [c["id"] for c in companies]
    purge_non_whitelisted_companies(keep_ids)
    _cb(f"Found {all_count} companies in account; {len(companies)} match whitelist ({skipped} skipped).")
    time.sleep(REQUEST_GAP)

    # ── 3. Calls + forms per company ─────────────────────────────────────────
    total = len(companies)
    for i, company in enumerate(companies):
        cid  = company["id"]
        name = company.get("name", cid)
        _cb(f"[{i+1}/{total}] {name}")

        # Calls
        _cb(f"  Fetching calls for {name}…")
        calls = _with_retry(
            get_calls, account_id, cid, DATE_FROM, today,
            label=f"{name}/calls",
        )
        if calls is None:
            msg = f"[{i+1}/{total}] Failed to fetch calls for {name!r} — check log for HTTP error details"
            log.warning(msg)
            failures.append(msg)
        else:
            _cb(f"  → {len(calls)} calls")
            try:
                upsert_calls(calls)
            except Exception as exc:
                msg = f"[{i+1}/{total}] DB write failed for {name!r} calls: {exc}"
                log.error(msg)
                failures.append(msg)
        time.sleep(REQUEST_GAP)

        # Form submissions
        _cb(f"  Fetching form submissions for {name}…")
        forms = _with_retry(
            get_form_submissions, account_id, cid, DATE_FROM, today,
            label=f"{name}/forms",
        )
        if forms is None:
            msg = f"[{i+1}/{total}] Failed to fetch forms for {name!r}"
            log.warning(msg)
            failures.append(msg)
        else:
            _cb(f"  → {len(forms)} form submissions")
            try:
                upsert_forms(forms)
            except Exception as exc:
                msg = f"[{i+1}/{total}] DB write failed for {name!r} forms: {exc}"
                log.error(msg)
                failures.append(msg)
        time.sleep(REQUEST_GAP)

    if failures:
        _cb(f"Sync finished with {len(failures)} failure(s) — see log.")
    else:
        _cb("Sync complete — all companies updated.")

    return failures

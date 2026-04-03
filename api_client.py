"""
CallRail API client (v3).

Authentication: Token-based
  Authorization: Token token="YOUR_API_KEY"

Base URL: https://api.callrail.com/v3/

Endpoints used:
  GET /a/{account_id}/companies.json
      Returns all companies (clients) in the account.

  GET /a/{account_id}/calls.json
      Returns calls with pagination.
      Key params: date_range_start, date_range_end, fields, per_page, page

  GET /a/{account_id}/form_submissions.json
      Returns form submissions with pagination.
      Key params: date_range_start, date_range_end, fields, per_page, page

Rate limit: 20 requests/second → we use 0.06s gap between requests.
Pagination: response includes { "page", "per_page", "total_records", "data": [...] }
"""

import os
import time
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

BASE_URL = "https://api.callrail.com/v3"
REQUEST_GAP = 0.07   # ~14 req/s, safely under 20/s limit

log = logging.getLogger(__name__)


def _headers() -> dict:
    api_key = os.getenv("CALLRAIL_API_KEY", "").strip()
    if not api_key or api_key == "your_api_key_here":
        raise ValueError("CALLRAIL_API_KEY not set in .env")
    return {"Authorization": f'Token token="{api_key}"'}


def _get(url: str, params: dict | None = None) -> dict:
    """Make a single GET request, raise on HTTP errors."""
    resp = requests.get(url, headers=_headers(), params=params, timeout=30)
    if not resp.ok:
        # Include the response body in the exception so callers can log it
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:500]
        raise requests.HTTPError(
            f"HTTP {resp.status_code} — {body}",
            response=resp,
        )
    return resp.json()


def get_account_id() -> str:
    """
    Fetch the account ID by hitting the accounts endpoint.
    CallRail API URLs are scoped to /a/{account_id}/.
    """
    data = _get(f"{BASE_URL}/a.json")
    accounts = data.get("accounts", [])
    if not accounts:
        raise ValueError("No CallRail accounts found for this API key.")
    return accounts[0]["id"]


def get_companies(account_id: str) -> list[dict]:
    """Return all companies in the account."""
    url    = f"{BASE_URL}/a/{account_id}/companies.json"
    params = {"per_page": 100}
    all_companies = []
    page = 1
    while True:
        params["page"] = page
        data = _get(url, params)
        companies = data.get("companies", [])
        all_companies.extend(companies)
        total   = data.get("total_records", len(all_companies))
        per_pg  = data.get("per_page", 100)
        if len(all_companies) >= total or not companies:
            break
        page += 1
        time.sleep(REQUEST_GAP)
    return all_companies


def get_calls(account_id: str, company_id: str, date_from: str, date_to: str,
              progress_callback=None) -> list[dict]:
    """
    Fetch all calls for a company within the date range.
    Requests the 'tags' field explicitly so tags are included in each record.
    """
    url = f"{BASE_URL}/a/{account_id}/calls.json"
    params = {
        "company_id": company_id,
        "start_date": date_from,
        "end_date":   date_to,
        "fields":     "tags,source_name,referrer_domain,utm_source,utm_medium",
        "per_page":   250,
        "page":       1,
    }
    all_calls = []
    page = 1
    while True:
        params["page"] = page
        data = _get(url, params)
        calls = data.get("calls", [])
        # Inject company_id since the per-company filter means all records belong to it
        for c in calls:
            if "company" not in c or not isinstance(c.get("company"), dict):
                c["company_id"] = company_id
            else:
                c["company_id"] = c["company"]["id"]
        all_calls.extend(calls)
        total = data.get("total_records", len(all_calls))
        if progress_callback:
            progress_callback(f"  calls: {len(all_calls)}/{total}")
        if len(all_calls) >= total or not calls:
            break
        page += 1
        time.sleep(REQUEST_GAP)
    return all_calls


def get_form_submissions(account_id: str, company_id: str, date_from: str, date_to: str,
                          progress_callback=None) -> list[dict]:
    """
    Fetch all form submissions for a company within the date range.
    """
    url = f"{BASE_URL}/a/{account_id}/form_submissions.json"
    # form_submissions does NOT support source_name or referrer_domain as fields
    # (API returns 400). Valid extra fields: tags, utm_source, utm_medium, utm_campaign
    params = {
        "company_id": company_id,
        "start_date": date_from,
        "end_date":   date_to,
        "fields":     "tags,utm_source,utm_medium,utm_campaign",
        "per_page":   250,
        "page":       1,
    }
    all_forms = []
    page = 1
    while True:
        params["page"] = page
        data = _get(url, params)
        forms = data.get("form_submissions", [])
        for f in forms:
            if "company" not in f or not isinstance(f.get("company"), dict):
                f["company_id"] = company_id
            else:
                f["company_id"] = f["company"]["id"]
        all_forms.extend(forms)
        total = data.get("total_records", len(all_forms))
        if progress_callback:
            progress_callback(f"  forms: {len(all_forms)}/{total}")
        if len(all_forms) >= total or not forms:
            break
        page += 1
        time.sleep(REQUEST_GAP)
    return all_forms

"""
Quick debug script — run with: py debug_api.py
"""

import os, json, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

API_KEY = os.getenv("CALLRAIL_API_KEY", "").strip()
BASE    = "https://api.callrail.com/v3"
HEADERS = {"Authorization": f'Token token="{API_KEY}"'}

def get(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    print(f"  STATUS: {r.status_code}  URL: {r.url}")
    try:
        body = r.json()
        # Print just the metadata (not the full call list)
        meta = {k: v for k, v in body.items() if k not in ("calls", "form_submissions", "companies")}
        print(f"  META: {json.dumps(meta)}")
        # Print first record's date fields
        records = body.get("calls") or body.get("form_submissions") or body.get("companies") or []
        if records:
            first = records[0]
            date_fields = {k: v for k, v in first.items() if "time" in k or "date" in k or "at" in k.lower()}
            print(f"  FIRST RECORD DATE FIELDS: {json.dumps(date_fields)}")
            print(f"  FIRST RECORD KEYS: {list(first.keys())}")
    except Exception as e:
        print(f"  BODY: {r.text[:500]}")
    return r

print("=== 1. Accounts ===")
r = get(f"{BASE}/a.json")
if not r.ok:
    print("FAILED - check API key")
    exit()

acct_id = r.json()["accounts"][0]["id"]
print(f"Account ID: {acct_id}")

print("\n=== 2. First whitelisted company ===")
r2 = get(f"{BASE}/a/{acct_id}/companies.json", {"per_page": 100})
companies = r2.json().get("companies", [])
# Find one we know should be there
target = next((c for c in companies if "911" in c["name"] or "AGC" in c["name"]), companies[0] if companies else None)
if not target:
    print("No companies found")
    exit()
cid   = target["id"]
cname = target["name"]
print(f"Using: {cname} (id={cid})")

print("\n=== 3. Calls — NO date params (default) ===")
r3 = get(f"{BASE}/a/{acct_id}/calls.json", {
    "company_id": cid,
    "per_page": 5,
})

print("\n=== 4. Calls — date_range=custom + start/end ===")
r4 = get(f"{BASE}/a/{acct_id}/calls.json", {
    "company_id":       cid,
    "date_range":       "custom",
    "date_range_start": "2024-06-01",
    "date_range_end":   "2026-04-02",
    "per_page": 5,
    "fields": "tags",
})

print("\n=== 5. Calls — start_date / end_date (alternate param names) ===")
r5 = get(f"{BASE}/a/{acct_id}/calls.json", {
    "company_id": cid,
    "start_date": "2024-06-01",
    "end_date":   "2026-04-02",
    "per_page": 5,
    "fields": "tags",
})

print("\n=== 6. Form submissions — date_range=custom ===")
r6 = get(f"{BASE}/a/{acct_id}/form_submissions.json", {
    "company_id":       cid,
    "date_range":       "custom",
    "date_range_start": "2024-06-01",
    "date_range_end":   "2026-04-02",
    "per_page": 5,
    "fields": "tags",
})

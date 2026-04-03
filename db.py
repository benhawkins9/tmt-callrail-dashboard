"""
SQLite cache layer for CallRail data.

Schema
------
companies        — one row per CallRail company (client)
calls            — one row per call
form_submissions — one row per form submission
call_tags        — many-to-many: call_id → tag name
form_tags        — many-to-many: form_id → tag name
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "callrail_cache.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS companies (
                id   TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                time_zone TEXT
            );

            CREATE TABLE IF NOT EXISTS calls (
                id              TEXT PRIMARY KEY,
                company_id      TEXT NOT NULL REFERENCES companies(id),
                start_time      TEXT NOT NULL,
                duration        INTEGER,
                source          TEXT,
                source_type     TEXT,
                source_name     TEXT,
                utm_source      TEXT,
                utm_medium      TEXT,
                referrer_domain TEXT,
                recording_url   TEXT,
                direction       TEXT,
                answered        INTEGER
            );

            CREATE TABLE IF NOT EXISTS form_submissions (
                id         TEXT PRIMARY KEY,
                company_id TEXT NOT NULL REFERENCES companies(id),
                submitted_at TEXT NOT NULL,
                source      TEXT,
                source_type TEXT,
                form_name   TEXT,
                landing_page TEXT
            );

            CREATE TABLE IF NOT EXISTS call_tags (
                call_id TEXT NOT NULL,
                tag     TEXT NOT NULL,
                PRIMARY KEY (call_id, tag)
            );

            CREATE TABLE IF NOT EXISTS form_tags (
                form_id TEXT NOT NULL,
                tag     TEXT NOT NULL,
                PRIMARY KEY (form_id, tag)
            );

            CREATE INDEX IF NOT EXISTS idx_calls_company_time
                ON calls(company_id, start_time);
            CREATE INDEX IF NOT EXISTS idx_forms_company_time
                ON form_submissions(company_id, submitted_at);
            CREATE INDEX IF NOT EXISTS idx_call_tags_tag
                ON call_tags(tag);
            CREATE INDEX IF NOT EXISTS idx_form_tags_tag
                ON form_tags(tag);
        """)
        # Migrate existing DBs missing the new source columns on calls
        existing_calls = {r[1] for r in conn.execute("PRAGMA table_info(calls)")}
        for col in ["source_name", "utm_source", "utm_medium", "referrer_domain"]:
            if col not in existing_calls:
                conn.execute(f"ALTER TABLE calls ADD COLUMN {col} TEXT")
        # Migrate form_submissions too
        existing_forms = {r[1] for r in conn.execute("PRAGMA table_info(form_submissions)")}
        for col in ["source_name", "utm_source", "utm_medium", "referrer_domain"]:
            if col not in existing_forms:
                conn.execute(f"ALTER TABLE form_submissions ADD COLUMN {col} TEXT")


# ── writers ────────────────────────────────────────────────────────────────────

def upsert_companies(companies: list[dict]):
    with get_conn() as conn:
        conn.executemany(
            """INSERT INTO companies(id, name, time_zone)
               VALUES(:id, :name, :time_zone)
               ON CONFLICT(id) DO UPDATE SET
                   name=excluded.name,
                   time_zone=excluded.time_zone""",
            [
                {
                    "id":        c["id"],
                    "name":      c.get("name", ""),
                    "time_zone": c.get("time_zone") or "",
                }
                for c in companies
            ],
        )


def upsert_calls(calls: list[dict]):
    """Upsert call records and their tags."""
    if not calls:
        return
    with get_conn() as conn:
        conn.executemany(
            """INSERT INTO calls(id, company_id, start_time, duration,
                                  source, source_type, source_name, utm_source, utm_medium,
                                  referrer_domain, recording_url, direction, answered)
               VALUES(:id, :company_id, :start_time, :duration,
                      :source, :source_type, :source_name, :utm_source, :utm_medium,
                      :referrer_domain, :recording_url, :direction, :answered)
               ON CONFLICT(id) DO UPDATE SET
                   start_time=excluded.start_time,
                   duration=excluded.duration,
                   source=excluded.source,
                   source_type=excluded.source_type,
                   source_name=excluded.source_name,
                   utm_source=excluded.utm_source,
                   utm_medium=excluded.utm_medium,
                   referrer_domain=excluded.referrer_domain,
                   recording_url=excluded.recording_url,
                   direction=excluded.direction,
                   answered=excluded.answered""",
            [
                {
                    "id":             c["id"],
                    "company_id":     c["company"]["id"] if isinstance(c.get("company"), dict) else c.get("company_id", ""),
                    "start_time":     c.get("start_time", ""),
                    "duration":       c.get("duration") or 0,
                    "source":         c.get("source") or "",
                    "source_type":    c.get("source_type") or "",
                    "source_name":    c.get("source_name") or "",
                    "utm_source":     c.get("utm_source") or "",
                    "utm_medium":     c.get("utm_medium") or "",
                    "referrer_domain": c.get("referrer_domain") or "",
                    "recording_url":  c.get("recording") or "",
                    "direction":      c.get("direction") or "",
                    "answered":       1 if c.get("answered") else 0,
                }
                for c in calls
            ],
        )
        # Upsert tags — delete existing then re-insert to handle tag changes
        for c in calls:
            call_id = c["id"]
            tags = _extract_tags(c)
            if tags is not None:
                conn.execute("DELETE FROM call_tags WHERE call_id=?", (call_id,))
                conn.executemany(
                    "INSERT OR IGNORE INTO call_tags(call_id, tag) VALUES(?,?)",
                    [(call_id, t) for t in tags],
                )


def upsert_forms(forms: list[dict]):
    """Upsert form submission records and their tags."""
    if not forms:
        return
    with get_conn() as conn:
        conn.executemany(
            """INSERT INTO form_submissions(id, company_id, submitted_at,
                                            source, source_type, source_name,
                                            utm_source, utm_medium, referrer_domain,
                                            form_name, landing_page)
               VALUES(:id, :company_id, :submitted_at,
                      :source, :source_type, :source_name,
                      :utm_source, :utm_medium, :referrer_domain,
                      :form_name, :landing_page)
               ON CONFLICT(id) DO UPDATE SET
                   submitted_at=excluded.submitted_at,
                   source=excluded.source,
                   source_type=excluded.source_type,
                   source_name=excluded.source_name,
                   utm_source=excluded.utm_source,
                   utm_medium=excluded.utm_medium,
                   referrer_domain=excluded.referrer_domain,
                   form_name=excluded.form_name,
                   landing_page=excluded.landing_page""",
            [
                {
                    "id":             f["id"],
                    "company_id":     f["company"]["id"] if isinstance(f.get("company"), dict) else f.get("company_id", ""),
                    "submitted_at":   f.get("submitted_at") or "",
                    "source":         f.get("source") or "",
                    "source_type":    f.get("source_type") or "",
                    "source_name":    f.get("source_name") or "",
                    "utm_source":     f.get("utm_source") or "",
                    "utm_medium":     f.get("utm_medium") or "",
                    "referrer_domain": f.get("referrer_domain") or "",
                    "form_name":      f.get("form_name") or f.get("name") or "",
                    "landing_page":   f.get("landing_page_url") or "",
                }
                for f in forms
            ],
        )
        for f in forms:
            form_id = f["id"]
            tags = _extract_tags(f)
            if tags is not None:
                conn.execute("DELETE FROM form_tags WHERE form_id=?", (form_id,))
                conn.executemany(
                    "INSERT OR IGNORE INTO form_tags(form_id, tag) VALUES(?,?)",
                    [(form_id, t) for t in tags],
                )


def _extract_tags(record: dict) -> list[str] | None:
    """
    Pull tag names from a CallRail record.
    Tags come back as either:
      - list of strings: ["qualified lead", "closed/won"]
      - list of dicts:   [{"name": "qualified lead", ...}, ...]
    Returns None if the field is absent so callers can distinguish
    'no tag data' from 'empty tag list'.
    """
    raw = record.get("tags")
    if raw is None:
        return None
    result = []
    for t in raw:
        if isinstance(t, str):
            result.append(t.strip().lower())
        elif isinstance(t, dict):
            name = (t.get("name") or t.get("tag") or "").strip().lower()
            if name:
                result.append(name)
    return result


def purge_non_whitelisted_companies(keep_ids: list[str]):
    """Delete companies (and their calls/forms/tags) that are not in keep_ids."""
    if not keep_ids:
        return
    placeholders = ",".join("?" * len(keep_ids))
    with get_conn() as conn:
        # Find company IDs to delete
        to_delete = [
            r[0] for r in conn.execute(
                f"SELECT id FROM companies WHERE id NOT IN ({placeholders})",
                keep_ids,
            ).fetchall()
        ]
        if not to_delete:
            return
        del_ph = ",".join("?" * len(to_delete))
        conn.execute(f"DELETE FROM call_tags  WHERE call_id IN (SELECT id FROM calls WHERE company_id IN ({del_ph}))", to_delete)
        conn.execute(f"DELETE FROM form_tags  WHERE form_id IN (SELECT id FROM form_submissions WHERE company_id IN ({del_ph}))", to_delete)
        conn.execute(f"DELETE FROM calls             WHERE company_id IN ({del_ph})", to_delete)
        conn.execute(f"DELETE FROM form_submissions  WHERE company_id IN ({del_ph})", to_delete)
        conn.execute(f"DELETE FROM companies          WHERE id          IN ({del_ph})", to_delete)


# ── readers ────────────────────────────────────────────────────────────────────

def load_companies() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM companies ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def load_monthly_contacts(date_from: str, date_to: str) -> list[dict]:
    """
    Monthly count of calls + forms combined (no tag filter).
    Columns: month, contact_type ('call'|'form'), count
    """
    with get_conn() as conn:
        calls = conn.execute(
            """SELECT substr(start_time, 1, 7) AS month,
                      'call' AS contact_type,
                      COUNT(*) AS cnt
               FROM calls
               WHERE start_time BETWEEN ? AND ?
               GROUP BY month""",
            (date_from, date_to + "T23:59:59"),
        ).fetchall()
        forms = conn.execute(
            """SELECT substr(submitted_at, 1, 7) AS month,
                      'form' AS contact_type,
                      COUNT(*) AS cnt
               FROM form_submissions
               WHERE submitted_at BETWEEN ? AND ?
               GROUP BY month""",
            (date_from, date_to + "T23:59:59"),
        ).fetchall()
    return [dict(r) for r in calls] + [dict(r) for r in forms]


def load_monthly_tagged(date_from: str, date_to: str, tags: list[str]) -> list[dict]:
    """
    Monthly count of contacts that have at least one of the given tags.
    Also returns a separate row for contacts with NONE of the given tags ('other').
    Columns: month, category ('tagged'|'other'), cnt
    """
    if not tags:
        return []
    placeholders = ",".join("?" * len(tags))

    with get_conn() as conn:
        # Calls with at least one matching tag
        tagged_calls = conn.execute(
            f"""SELECT substr(c.start_time, 1, 7) AS month, COUNT(DISTINCT c.id) AS cnt
                FROM calls c
                JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND ct.tag IN ({placeholders})
                GROUP BY month""",
            [date_from, date_to + "T23:59:59"] + tags,
        ).fetchall()

        total_calls = conn.execute(
            """SELECT substr(start_time, 1, 7) AS month, COUNT(*) AS cnt
               FROM calls
               WHERE start_time BETWEEN ? AND ?
               GROUP BY month""",
            (date_from, date_to + "T23:59:59"),
        ).fetchall()

        # Forms with at least one matching tag
        tagged_forms = conn.execute(
            f"""SELECT substr(f.submitted_at, 1, 7) AS month, COUNT(DISTINCT f.id) AS cnt
                FROM form_submissions f
                JOIN form_tags ft ON ft.form_id = f.id
                WHERE f.submitted_at BETWEEN ? AND ?
                  AND ft.tag IN ({placeholders})
                GROUP BY month""",
            [date_from, date_to + "T23:59:59"] + tags,
        ).fetchall()

        total_forms = conn.execute(
            """SELECT substr(submitted_at, 1, 7) AS month, COUNT(*) AS cnt
               FROM form_submissions
               WHERE submitted_at BETWEEN ? AND ?
               GROUP BY month""",
            (date_from, date_to + "T23:59:59"),
        ).fetchall()

    # Merge into tagged / other per month
    from collections import defaultdict
    tagged_map: dict[str, int] = defaultdict(int)
    total_map:  dict[str, int] = defaultdict(int)

    for r in tagged_calls:
        tagged_map[r["month"]] += r["cnt"]
    for r in tagged_forms:
        tagged_map[r["month"]] += r["cnt"]
    for r in total_calls:
        total_map[r["month"]] += r["cnt"]
    for r in total_forms:
        total_map[r["month"]] += r["cnt"]

    rows = []
    for month in sorted(set(list(tagged_map.keys()) + list(total_map.keys()))):
        tagged_cnt = tagged_map.get(month, 0)
        total_cnt  = total_map.get(month, 0)
        other_cnt  = max(0, total_cnt - tagged_cnt)
        rows.append({"month": month, "category": "tagged",  "cnt": tagged_cnt})
        rows.append({"month": month, "category": "other",   "cnt": other_cnt})
    return rows


def load_monthly_by_tag(date_from: str, date_to: str, tags: list[str]) -> list[dict]:
    """
    Monthly count broken down by individual tag (for stacked bar).
    Columns: month, tag, cnt
    A contact with multiple matching tags is counted once per tag.
    """
    if not tags:
        return []
    placeholders = ",".join("?" * len(tags))
    with get_conn() as conn:
        call_rows = conn.execute(
            f"""SELECT substr(c.start_time, 1, 7) AS month, ct.tag, COUNT(DISTINCT c.id) AS cnt
                FROM calls c
                JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND ct.tag IN ({placeholders})
                GROUP BY month, ct.tag""",
            [date_from, date_to + "T23:59:59"] + tags,
        ).fetchall()
        form_rows = conn.execute(
            f"""SELECT substr(f.submitted_at, 1, 7) AS month, ft.tag, COUNT(DISTINCT f.id) AS cnt
                FROM form_submissions f
                JOIN form_tags ft ON ft.form_id = f.id
                WHERE f.submitted_at BETWEEN ? AND ?
                  AND ft.tag IN ({placeholders})
                GROUP BY month, ft.tag""",
            [date_from, date_to + "T23:59:59"] + tags,
        ).fetchall()

    from collections import defaultdict
    combined: dict[tuple, int] = defaultdict(int)
    for r in call_rows:
        combined[(r["month"], r["tag"])] += r["cnt"]
    for r in form_rows:
        combined[(r["month"], r["tag"])] += r["cnt"]

    return [{"month": m, "tag": t, "cnt": c} for (m, t), c in combined.items()]


# ── Lead source classification ─────────────────────────────────────────────────
# `source` and `source_type` are empty in the current data.
# We derive a clean channel label from utm_source + utm_medium + source_name:
#
#   source_name containing GMB/GBP keywords  → "Google My Business"
#   utm_source=google/adwords + medium=cpc/ppc/paid → "Google Ads"
#   utm_source=google + medium=search/organic  → "Google Organic"
#   utm_source=bing                            → "Bing"
#   utm_source facebook / medium social        → "Social Media"
#   utm_medium=referral or known referral sites→ "Referral"
#   utm_medium=direct / offline (non-GMB)      → "Direct"
#   anything else                              → NULL (excluded from charts)

def _src(tbl: str) -> str:
    """SQL CASE expression returning a clean lead-source label for table alias `tbl`."""
    sn = f"LOWER(COALESCE({tbl}.source_name,''))"
    us = f"LOWER(COALESCE({tbl}.utm_source,''))"
    um = f"LOWER(COALESCE({tbl}.utm_medium,''))"
    return (
        f"CASE"
        f" WHEN {sn} LIKE '%google my business%' OR {sn} LIKE '%google business profile%'"
        f"   OR {sn} LIKE '%gbp%' OR {sn} LIKE '%gmb%'"
        f"   OR {us} IN ('googlemybusiness','gmb') OR {um} = 'gmb'"
        f"   THEN 'Google My Business'"
        f" WHEN ({us} IN ('google','adwords') AND {um} IN ('cpc','ppc','paid','search-ads','display'))"
        f"   OR {us} = 'adwords'"
        f"   THEN 'Google Ads'"
        f" WHEN {us} = 'google' AND {um} IN ('search','organic')"
        f"   THEN 'Google Organic'"
        f" WHEN {us} = 'bing' THEN 'Bing'"
        f" WHEN {us} IN ('facebook','facebook.page','fb','ig','instagram','linkedin','linkedin.company','twitter')"
        f"   OR {um} IN ('social','social-media') THEN 'Social Media'"
        f" WHEN {um} IN ('referral','referral_profile')"
        f"   OR {us} IN ('cloudtango','clutch.co','themanifest.com','mspdatabase.com','chatgpt.com','local-listings')"
        f"   THEN 'Referral'"
        f" WHEN {um} = 'direct' OR {us} IN ('offline','direct','website') THEN 'Direct'"
        f" WHEN {um} = 'email' OR {us} IN ('email','email-signature') THEN 'Email'"
        f" WHEN {sn} LIKE '%website pool%' OR {sn} LIKE '%offline marketing%' THEN 'Website (Direct)'"
        f" ELSE NULL END"
    )


def load_source_breakdown(date_from: str, date_to: str, tags: list[str] | None = None) -> list[dict]:
    """
    Source breakdown for qualified contacts (or all if tags is None/empty).
    GMB variants unified. Unknown (no source_name) excluded entirely.
    Columns: source, cnt
    """
    ce = _src("c")
    fe = _src("f")
    with get_conn() as conn:
        if tags:
            ph = ",".join("?" * len(tags))
            call_rows = conn.execute(
                f"""SELECT ({ce}) AS channel, COUNT(DISTINCT c.id) AS cnt
                    FROM calls c
                    JOIN call_tags ct ON ct.call_id = c.id
                    WHERE c.start_time BETWEEN ? AND ?
                      AND ct.tag IN ({ph})
                      AND ({ce}) IS NOT NULL
                    GROUP BY channel""",
                [date_from, date_to + "T23:59:59"] + tags,
            ).fetchall()
            form_rows = conn.execute(
                f"""SELECT ({fe}) AS channel, COUNT(DISTINCT f.id) AS cnt
                    FROM form_submissions f
                    JOIN form_tags ft ON ft.form_id = f.id
                    WHERE f.submitted_at BETWEEN ? AND ?
                      AND ft.tag IN ({ph})
                      AND ({fe}) IS NOT NULL
                    GROUP BY channel""",
                [date_from, date_to + "T23:59:59"] + tags,
            ).fetchall()
        else:
            call_rows = conn.execute(
                f"""SELECT ({ce}) AS channel, COUNT(*) AS cnt
                    FROM calls c
                    WHERE c.start_time BETWEEN ? AND ?
                      AND ({ce}) IS NOT NULL
                    GROUP BY channel""",
                (date_from, date_to + "T23:59:59"),
            ).fetchall()
            form_rows = conn.execute(
                f"""SELECT ({fe}) AS channel, COUNT(*) AS cnt
                    FROM form_submissions f
                    WHERE f.submitted_at BETWEEN ? AND ?
                      AND ({fe}) IS NOT NULL
                    GROUP BY channel""",
                (date_from, date_to + "T23:59:59"),
            ).fetchall()

    from collections import defaultdict
    totals: dict[str, int] = defaultdict(int)
    for r in call_rows:
        totals[r["channel"]] += r["cnt"]
    for r in form_rows:
        totals[r["channel"]] += r["cnt"]
    return [{"source": s, "cnt": c} for s, c in totals.items()]


def load_source_by_month(date_from: str, date_to: str, tags: list[str]) -> list[dict]:
    """Source breakdown by month. Unknown excluded. Columns: month, source, cnt"""
    if not tags:
        return []
    ph = ",".join("?" * len(tags))
    ce = _src("c")
    fe = _src("f")
    with get_conn() as conn:
        call_rows = conn.execute(
            f"""SELECT substr(c.start_time, 1, 7) AS month,
                       ({ce}) AS channel, COUNT(DISTINCT c.id) AS cnt
                FROM calls c
                JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND ct.tag IN ({ph})
                  AND ({ce}) IS NOT NULL
                GROUP BY month, channel""",
            [date_from, date_to + "T23:59:59"] + tags,
        ).fetchall()
        form_rows = conn.execute(
            f"""SELECT substr(f.submitted_at, 1, 7) AS month,
                       ({fe}) AS channel, COUNT(DISTINCT f.id) AS cnt
                FROM form_submissions f
                JOIN form_tags ft ON ft.form_id = f.id
                WHERE f.submitted_at BETWEEN ? AND ?
                  AND ft.tag IN ({ph})
                  AND ({fe}) IS NOT NULL
                GROUP BY month, channel""",
            [date_from, date_to + "T23:59:59"] + tags,
        ).fetchall()

    from collections import defaultdict
    combined: dict[tuple, int] = defaultdict(int)
    for r in call_rows:
        combined[(r["month"], r["channel"])] += r["cnt"]
    for r in form_rows:
        combined[(r["month"], r["channel"])] += r["cnt"]
    return [{"month": m, "source": s, "cnt": c} for (m, s), c in combined.items()]


def load_scorecard_totals(date_from: str, date_to: str, pipeline_tags: list[str], closed_won_tag: str = "closed/won") -> dict:
    """
    Returns aggregate scorecard numbers.
    """
    if not pipeline_tags:
        return {"total_pipeline": 0, "total_closed_won": 0, "total_contacts": 0}

    placeholders_pipeline = ",".join("?" * len(pipeline_tags))

    with get_conn() as conn:
        # Total qualified pipeline contacts (calls)
        pipeline_calls = conn.execute(
            f"""SELECT COUNT(DISTINCT c.id) AS cnt
                FROM calls c
                JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND ct.tag IN ({placeholders_pipeline})""",
            [date_from, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchone()["cnt"]

        pipeline_forms = conn.execute(
            f"""SELECT COUNT(DISTINCT f.id) AS cnt
                FROM form_submissions f
                JOIN form_tags ft ON ft.form_id = f.id
                WHERE f.submitted_at BETWEEN ? AND ?
                  AND ft.tag IN ({placeholders_pipeline})""",
            [date_from, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchone()["cnt"]

        # Closed/won
        closed_calls = conn.execute(
            """SELECT COUNT(DISTINCT c.id) AS cnt
               FROM calls c
               JOIN call_tags ct ON ct.call_id = c.id
               WHERE c.start_time BETWEEN ? AND ?
                 AND ct.tag = ?""",
            (date_from, date_to + "T23:59:59", closed_won_tag),
        ).fetchone()["cnt"]

        closed_forms = conn.execute(
            """SELECT COUNT(DISTINCT f.id) AS cnt
               FROM form_submissions f
               JOIN form_tags ft ON ft.form_id = f.id
               WHERE f.submitted_at BETWEEN ? AND ?
                 AND ft.tag = ?""",
            (date_from, date_to + "T23:59:59", closed_won_tag),
        ).fetchone()["cnt"]

        # Total contacts
        total_calls = conn.execute(
            "SELECT COUNT(*) AS cnt FROM calls WHERE start_time BETWEEN ? AND ?",
            (date_from, date_to + "T23:59:59"),
        ).fetchone()["cnt"]

        total_forms = conn.execute(
            "SELECT COUNT(*) AS cnt FROM form_submissions WHERE submitted_at BETWEEN ? AND ?",
            (date_from, date_to + "T23:59:59"),
        ).fetchone()["cnt"]

    # Half-period comparison for % change
    import datetime
    d_from = datetime.date.fromisoformat(date_from)
    d_to   = datetime.date.today()
    mid    = d_from + (d_to - d_from) / 2
    mid_str = mid.isoformat()

    with get_conn() as conn:
        early_calls = conn.execute(
            f"""SELECT COUNT(DISTINCT c.id) AS cnt
                FROM calls c JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND ct.tag IN ({placeholders_pipeline})""",
            [date_from, mid_str + "T23:59:59"] + pipeline_tags,
        ).fetchone()["cnt"]
        early_forms = conn.execute(
            f"""SELECT COUNT(DISTINCT f.id) AS cnt
                FROM form_submissions f JOIN form_tags ft ON ft.form_id = f.id
                WHERE f.submitted_at BETWEEN ? AND ?
                  AND ft.tag IN ({placeholders_pipeline})""",
            [date_from, mid_str + "T23:59:59"] + pipeline_tags,
        ).fetchone()["cnt"]
        recent_calls = conn.execute(
            f"""SELECT COUNT(DISTINCT c.id) AS cnt
                FROM calls c JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND ct.tag IN ({placeholders_pipeline})""",
            [mid_str, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchone()["cnt"]
        recent_forms = conn.execute(
            f"""SELECT COUNT(DISTINCT f.id) AS cnt
                FROM form_submissions f JOIN form_tags ft ON ft.form_id = f.id
                WHERE f.submitted_at BETWEEN ? AND ?
                  AND ft.tag IN ({placeholders_pipeline})""",
            [mid_str, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchone()["cnt"]

    early  = early_calls  + early_forms
    recent = recent_calls + recent_forms
    pct_change = ((recent - early) / early * 100) if early else 0

    return {
        "total_pipeline":  pipeline_calls + pipeline_forms,
        "total_closed_won": closed_calls + closed_forms,
        "total_contacts":   total_calls + total_forms,
        "pct_change":       pct_change,
        "early_period":     early,
        "recent_period":    recent,
    }


def load_all_tags() -> list[str]:
    """Return all distinct tag names seen in the database."""
    with get_conn() as conn:
        call_tags = conn.execute("SELECT DISTINCT tag FROM call_tags ORDER BY tag").fetchall()
        form_tags = conn.execute("SELECT DISTINCT tag FROM form_tags ORDER BY tag").fetchall()
    tags = sorted({r["tag"] for r in call_tags} | {r["tag"] for r in form_tags})
    return tags


def load_call_duration_by_month(date_from: str, date_to: str, pipeline_tags: list[str]) -> list[dict]:
    """
    Average call duration (seconds) per month split by:
      qualified   — calls with at least one pipeline tag
      unqualified — calls with none of the pipeline tags
    Only counts calls where duration > 0.
    Returns list of {month, qualified_avg, unqualified_avg}.
    """
    if not pipeline_tags:
        return []
    ph = ",".join("?" * len(pipeline_tags))
    with get_conn() as conn:
        qual_rows = conn.execute(
            f"""SELECT substr(c.start_time, 1, 7) AS month,
                       AVG(c.duration) AS avg_dur
                FROM calls c
                JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND c.duration > 0
                  AND ct.tag IN ({ph})
                GROUP BY month""",
            [date_from, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchall()
        unqual_rows = conn.execute(
            f"""SELECT substr(c.start_time, 1, 7) AS month,
                       AVG(c.duration) AS avg_dur
                FROM calls c
                WHERE c.start_time BETWEEN ? AND ?
                  AND c.duration > 0
                  AND c.id NOT IN (
                      SELECT call_id FROM call_tags WHERE tag IN ({ph})
                  )
                GROUP BY month""",
            [date_from, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchall()

    qual_map   = {r["month"]: r["avg_dur"] for r in qual_rows}
    unqual_map = {r["month"]: r["avg_dur"] for r in unqual_rows}
    all_months = sorted(set(qual_map) | set(unqual_map))
    return [
        {
            "month":           m,
            "qualified_avg":   qual_map.get(m) or 0.0,
            "unqualified_avg": unqual_map.get(m) or 0.0,
        }
        for m in all_months
    ]


def load_duration_scorecard(date_from: str, date_to: str, pipeline_tags: list[str]) -> dict:
    """
    All-time average call duration (seconds) for qualified vs unqualified calls.
    Returns {qualified_avg, unqualified_avg}.
    """
    if not pipeline_tags:
        return {"qualified_avg": 0.0, "unqualified_avg": 0.0}
    ph = ",".join("?" * len(pipeline_tags))
    with get_conn() as conn:
        qual = conn.execute(
            f"""SELECT AVG(c.duration) AS avg_dur
                FROM calls c
                JOIN call_tags ct ON ct.call_id = c.id
                WHERE c.start_time BETWEEN ? AND ?
                  AND c.duration > 0
                  AND ct.tag IN ({ph})""",
            [date_from, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchone()
        unqual = conn.execute(
            f"""SELECT AVG(c.duration) AS avg_dur
                FROM calls c
                WHERE c.start_time BETWEEN ? AND ?
                  AND c.duration > 0
                  AND c.id NOT IN (
                      SELECT call_id FROM call_tags WHERE tag IN ({ph})
                  )""",
            [date_from, date_to + "T23:59:59"] + pipeline_tags,
        ).fetchone()
    return {
        "qualified_avg":   qual["avg_dur"]   or 0.0,
        "unqualified_avg": unqual["avg_dur"] or 0.0,
    }
